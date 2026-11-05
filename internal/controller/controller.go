// Package controller 按 ReplicaSet 的期望副本数对账其拥有的 Pod。
package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"mini-k8s/internal/api"
)

const prefix = "/api/mini/v1/"

// Controller 通过资源 API 观察并串行对账 ReplicaSet。
type Controller struct {
	base   string
	token  string
	client *http.Client
	resync time.Duration
}

// New 创建使用 HTTPS 和独立身份令牌的控制器客户端。
func New(server, tokenFile, caFile string, resync time.Duration) (*Controller, error) {
	u, err := url.Parse(server)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("server must be an HTTPS origin")
	}
	if resync <= 0 {
		return nil, errors.New("resync must be positive")
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	if strings.TrimSpace(string(token)) == "" {
		return nil, errors.New("token file is empty")
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file contains no certificates")
		}
	}
	return &Controller{base: strings.TrimSuffix(server, "/"), token: strings.TrimSpace(string(token)), resync: resync,
		client: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}}}, nil
}

func (c *Controller) request(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+prefix+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.client.Do(req)
}

func (c *Controller) call(ctx context.Context, method, path string, body, target any) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.request(ctx, method, path, body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("API %s %s: HTTP %d", method, path, resp.StatusCode)
	}
	if target != nil {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(target); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

type podList struct {
	Items           []api.Pod `json:"items"`
	ResourceVersion int64     `json:"resourceVersion"`
}
type replicaSetList struct {
	Items           []api.ReplicaSet `json:"items"`
	ResourceVersion int64            `json:"resourceVersion"`
}

func (c *Controller) listPods(ctx context.Context) (podList, error) {
	var list podList
	_, err := c.call(ctx, http.MethodGet, "pods", nil, &list)
	return list, err
}
func (c *Controller) listReplicaSets(ctx context.Context) (replicaSetList, error) {
	var list replicaSetList
	_, err := c.call(ctx, http.MethodGet, "replicasets", nil, &list)
	return list, err
}

// Run 启动单个对账工作者。Watch 只负责触发，定期全量读取用于修复漏事件。
func (c *Controller) Run(ctx context.Context) error {
	trigger := make(chan struct{}, 1)
	wake := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	go c.watchLoop(ctx, "pods", wake)
	go c.watchLoop(ctx, "replicasets", wake)
	ticker := time.NewTicker(c.resync)
	defer ticker.Stop()
	wake()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-trigger:
		}
		if err := c.reconcileAll(ctx); err != nil && ctx.Err() == nil {
			log.Printf("controller reconcile: %v", err)
		}
	}
}

func (c *Controller) watchLoop(ctx context.Context, kind string, wake func()) {
	for ctx.Err() == nil {
		// 先获取快照版本，再从该版本之后订阅；断线或历史压缩后重新 List。
		var list struct {
			ResourceVersion int64 `json:"resourceVersion"`
		}
		if _, err := c.call(ctx, http.MethodGet, kind, nil, &list); err == nil {
			wake()
			path := kind + "?watch=true&resourceVersion=" + strconv.FormatInt(list.ResourceVersion, 10)
			if resp, err := c.request(ctx, http.MethodGet, path, nil); err == nil {
				if resp.StatusCode == http.StatusOK {
					decoder := json.NewDecoder(resp.Body)
					for {
						var event struct {
							Type string `json:"type"`
						}
						if err := decoder.Decode(&event); err != nil {
							break
						}
						wake()
						if event.Type == "ERROR" {
							break
						}
					}
				}
				resp.Body.Close()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (c *Controller) reconcileAll(ctx context.Context) error {
	sets, err := c.listReplicaSets(ctx)
	if err != nil {
		return err
	}
	for _, rs := range sets.Items {
		if err := c.reconcile(ctx, rs); err != nil {
			log.Printf("controller ReplicaSet %s uid=%s: %v", rs.Metadata.Name, rs.Metadata.UID, err)
		}
	}
	return nil
}

// active 保守计算副本额度：已绑定且正在删除的 Pod 必须等清理确认后才释放。
func active(p api.Pod) bool {
	if p.Metadata.DeletionTimestamp != nil && p.NodeUID != "" {
		return true
	}
	return p.Status.Phase != "Succeeded" && p.Status.Phase != "Failed"
}

func (c *Controller) reconcile(ctx context.Context, rs api.ReplicaSet) error {
	if rs.Metadata.UID == "" {
		return errors.New("ReplicaSet has no UID")
	}
	// 未决操作必须先恢复；查询错误时不得继续删除、缩容或发起新操作。
	pending, err := c.pendingCreate(ctx, rs)
	if err != nil {
		return err
	}
	pods, err := c.listPods(ctx)
	if err != nil {
		return err
	}
	owned := make([]api.Pod, 0)
	usedNames := make(map[string]bool, len(pods.Items))
	for _, pod := range pods.Items {
		usedNames[pod.Metadata.Name] = true
		// 所有权只能由 UID 确认，不能认领标签相同的独立 Pod。
		if pod.Metadata.OwnerUID == rs.Metadata.UID && active(pod) {
			owned = append(owned, pod)
		}
	}
	if pending != nil {
		// 缩容、删除或已无缺口时，通过与提交竞争的 CAS 取消旧意图。
		// 本轮无论取消还是提交均结束，下一轮重新读取，不使用旧快照。
		if rs.Metadata.DeletionTimestamp != nil || pending.RSVersion != rs.Metadata.ResourceVersion || len(owned) >= rs.Spec.Replicas {
			return c.cancelCreate(ctx, rs, *pending)
		}
		return c.commitCreate(ctx, rs, *pending)
	}
	if rs.Metadata.DeletionTimestamp != nil {
		return c.deleteReplicaSetPods(ctx, rs, pods.Items)
	}
	if len(owned) > rs.Spec.Replicas {
		return c.scaleDown(ctx, owned, rs.Spec.Replicas)
	}
	if len(owned) >= rs.Spec.Replicas {
		return nil
	}
	return c.createPod(ctx, rs, usedNames, pods.ResourceVersion)
}

// deleteReplicaSetPods 等所有子 Pod 真正消失后再请求 API 完成删除。
// API 的 finalize 事务再次检查 UID、版本和子 Pod，防止与创建竞态。
func (c *Controller) deleteReplicaSetPods(ctx context.Context, rs api.ReplicaSet, pods []api.Pod) error {
	remaining := false
	for _, pod := range pods {
		if pod.Metadata.OwnerUID != rs.Metadata.UID {
			continue
		}
		remaining = true
		if pod.Metadata.DeletionTimestamp == nil {
			if err := c.deletePod(ctx, pod); err != nil {
				return err
			}
		}
	}
	if !remaining {
		_, err := c.call(ctx, http.MethodPost, "replicasets/"+url.PathEscape(rs.Metadata.Name)+"/finalize", map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion}, nil)
		return err
	}
	return nil
}

func (c *Controller) scaleDown(ctx context.Context, owned []api.Pod, desired int) error {
	excess := len(owned) - desired
	// 正在删除的实例仍占额度，但已纳入本轮缩容，不重复请求删除。
	for _, pod := range owned {
		if pod.Metadata.DeletionTimestamp != nil {
			excess--
		}
	}
	if excess <= 0 {
		return nil
	}
	// 优先删除未绑定 Pod；同一快照按版本和 UID 选择，避免依赖列表顺序。
	sort.Slice(owned, func(i, j int) bool {
		if (owned[i].NodeUID == "") != (owned[j].NodeUID == "") {
			return owned[i].NodeUID == ""
		}
		if owned[i].Metadata.ResourceVersion != owned[j].Metadata.ResourceVersion {
			return owned[i].Metadata.ResourceVersion > owned[j].Metadata.ResourceVersion
		}
		return owned[i].Metadata.UID < owned[j].Metadata.UID
	})
	for _, pod := range owned {
		if pod.Metadata.DeletionTimestamp != nil {
			continue
		}
		if err := c.deletePod(ctx, pod); err != nil {
			return err
		}
		excess--
		if excess == 0 {
			break
		}
	}
	return nil
}

func (c *Controller) createPod(ctx context.Context, rs api.ReplicaSet, usedNames map[string]bool, podListRevision int64) error {
	// 服务端持久化准备意图。结果未知时下一轮查询该 RS 的原意图，
	// 不以本地的新名称替换，也不通过原始 Pod 创建接口绕过事务。
	name := nextPodName(rs.Metadata.UID, usedNames)
	var operation createOperation
	_, err := c.call(ctx, http.MethodPost, operationPath(rs), map[string]any{
		"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podName": name, "podListRevision": podListRevision,
	}, &operation)
	if err != nil {
		return fmt.Errorf("prepare create unconfirmed; query pending next reconciliation: %w", err)
	}
	if err := validateOperation(rs, operation); err != nil {
		return err
	}
	switch operation.State {
	case "PREPARED":
		return c.commitCreate(ctx, rs, operation)
	case "COMMITTED":
		if operation.PodUID == "" {
			return errors.New("committed create operation has no Pod UID")
		}
		// 服务端重放了旧结果；本轮不再使用原快照决定是否补建。
		return nil
	case "CANCELLED":
		return nil
	default:
		return fmt.Errorf("unexpected prepared operation state %q", operation.State)
	}
}

// createOperation 是 API 持久化的创建意图；操作身份在控制器重启后仍有效。
type createOperation struct {
	UID         string `json:"uid"`
	RSVersion   int64  `json:"rsVersion"`
	OperationID int64  `json:"operationID"`
	PodName     string `json:"podName"`
	State       string `json:"state"`
	PodUID      string `json:"podUID"`
}

func operationPath(rs api.ReplicaSet) string {
	return "replicasets/" + url.PathEscape(rs.Metadata.Name) + "/create-operation"
}

func validateOperation(rs api.ReplicaSet, op createOperation) error {
	if op.UID != rs.Metadata.UID || op.RSVersion <= 0 || op.OperationID <= 0 || op.PodName == "" {
		return errors.New("create operation identity is invalid")
	}
	return nil
}

func (c *Controller) pendingCreate(ctx context.Context, rs api.ReplicaSet) (*createOperation, error) {
	var op createOperation
	status, err := c.call(ctx, http.MethodGet, operationPath(rs)+"?uid="+url.QueryEscape(rs.Metadata.UID), nil, &op)
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query pending create: %w", err)
	}
	if err := validateOperation(rs, op); err != nil {
		return nil, err
	}
	if op.State != "PREPARED" {
		return nil, fmt.Errorf("unexpected pending create state %q", op.State)
	}
	return &op, nil
}

func (c *Controller) commitCreate(ctx context.Context, rs api.ReplicaSet, op createOperation) error {
	var result createOperation
	_, err := c.call(ctx, http.MethodPut, operationPath(rs), map[string]any{
		"uid": rs.Metadata.UID, "operationID": op.OperationID, "podName": op.PodName,
	}, &result)
	if err != nil {
		// 响应丢失不代表提交失败；以原操作身份查询，不允许换 ID。
		path := operationPath(rs) + "?uid=" + url.QueryEscape(rs.Metadata.UID) + "&operationID=" + strconv.FormatInt(op.OperationID, 10)
		if _, queryErr := c.call(ctx, http.MethodGet, path, nil, &result); queryErr != nil {
			return fmt.Errorf("create outcome unknown: %w", err)
		}
	}
	if err := validateOperation(rs, result); err != nil {
		return err
	}
	if result.OperationID != op.OperationID || result.PodName != op.PodName || result.State != "COMMITTED" || result.PodUID == "" {
		return fmt.Errorf("create operation %d not confirmed committed", op.OperationID)
	}
	return nil
}

func (c *Controller) cancelCreate(ctx context.Context, rs api.ReplicaSet, op createOperation) error {
	var result createOperation
	_, err := c.call(ctx, http.MethodDelete, operationPath(rs), map[string]any{
		"uid": rs.Metadata.UID, "operationID": op.OperationID,
	}, &result)
	if err != nil {
		// 下轮重新查询；取消响应丢失或与提交竞争不能视作取消成功。
		return fmt.Errorf("cancel create outcome unknown: %w", err)
	}
	if err := validateOperation(rs, result); err != nil {
		return err
	}
	if result.OperationID != op.OperationID || result.PodName != op.PodName || result.State != "CANCELLED" {
		return fmt.Errorf("create operation %d not confirmed cancelled", op.OperationID)
	}
	return nil
}

func nextPodName(uid string, used map[string]bool) string {
	// 名称由 RS UID 和空位决定；同名 RS 重建后不会复用旧 UID 的名称空间。
	digest := sha256.Sum256([]byte(uid))
	base := "rs-" + hex.EncodeToString(digest[:8]) + "-"
	for i := 0; ; i++ {
		name := base + strconv.Itoa(i)
		if !used[name] {
			return name
		}
	}
}

func (c *Controller) deletePod(ctx context.Context, p api.Pod) error {
	// 同名资源可能已重建，附带 UID 让 API 拒绝删除新实例。
	_, err := c.call(ctx, http.MethodDelete, "pods/"+url.PathEscape(p.Metadata.Name)+"?uid="+url.QueryEscape(p.Metadata.UID), nil, nil)
	return err
}
