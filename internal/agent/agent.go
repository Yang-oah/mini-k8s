// Package agent 将当前节点的 Pod 期望状态与本地容器运行时对账。
package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"mini-k8s/internal/api"
	containerruntime "mini-k8s/internal/runtime"
)

const apiPrefix = "/api/mini/v1/"

// Runtime 是 Agent 需要的 UID 级运行时操作。
// 查询错误必须与实例不存在区分；Cleanup 仅在资源确实清理后返回 nil。
type Runtime interface {
	Ensure(context.Context, api.Pod) (containerruntime.Observation, error)
	Inspect(context.Context, string) (containerruntime.Observation, error)
	Cleanup(context.Context, string) error
	Logs(context.Context, string, int) (string, error)
}

type retryState struct {
	containerID string
	failures    int
	next        time.Time
	inFlight    bool
}

// Agent 以预注册节点的身份观察分配，并对删除中的 Pod 执行有界并发清理。
type Agent struct {
	base           string
	token          string
	node           string
	client         *http.Client
	runtime        Runtime
	resync         time.Duration
	retries        map[string]retryState
	statePath      string
	runtimeTimeout time.Duration
	runtimeGate    chan struct{}
	podMu          sync.Mutex
	podGates       map[string]*podGate
	retryMu        sync.Mutex
	nodeMu         sync.Mutex
	nodeUID        string
	readinessMu    sync.Mutex
	readiness      map[string]*readinessState
	readinessWake  chan struct{}
	readinessEpoch uint64
}

type apiError struct {
	status       int
	method, path string
}

type podGate struct {
	lock  chan struct{}
	users int
}

func (e *apiError) Error() string {
	return fmt.Sprintf("API %s %s: HTTP %d", e.method, e.path, e.status)
}

// New 创建节点 Agent；server 必须是 HTTPS origin，凭据从文件读取。
func New(server, node, tokenFile, caFile string, resync time.Duration, runtime Runtime, stateDir string) (*Agent, error) {
	u, err := url.Parse(server)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("server must be an HTTPS origin")
	}
	if node == "" || strings.ContainsAny(node, "/?#") || resync <= 0 || runtime == nil || !filepath.IsAbs(stateDir) {
		return nil, errors.New("node, runtime, positive resync and absolute state directory are required")
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
	a := &Agent{
		base: strings.TrimSuffix(server, "/"), token: strings.TrimSpace(string(token)), node: node,
		client:  &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}},
		runtime: runtime, resync: resync, retries: make(map[string]retryState),
		statePath: filepath.Join(stateDir, "retry-state.json"), runtimeTimeout: 30 * time.Second, runtimeGate: make(chan struct{}, 8),
		readiness: make(map[string]*readinessState), readinessWake: make(chan struct{}, 1),
	}
	if err := a.loadRetries(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *Agent) runtimeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.runtimeLimit())
}

func (a *Agent) runtimeLimit() time.Duration {
	timeout := a.runtimeTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return timeout
}

// 控制 socket 与对账共享 UID 互斥和总并发上限，等待也受同一 deadline 约束。
func (a *Agent) withRuntime(ctx context.Context, uid string, operation func(context.Context) error) error {
	callCtx, cancel := a.runtimeContext(ctx)
	defer cancel()
	a.podMu.Lock()
	if a.podGates == nil {
		a.podGates = make(map[string]*podGate)
	}
	gate := a.podGates[uid]
	if gate == nil {
		gate = &podGate{lock: make(chan struct{}, 1)}
		a.podGates[uid] = gate
	}
	gate.users++
	a.podMu.Unlock()
	defer func() {
		a.podMu.Lock()
		gate.users--
		if gate.users == 0 {
			delete(a.podGates, uid)
		}
		a.podMu.Unlock()
	}()
	select {
	case gate.lock <- struct{}{}:
		defer func() { <-gate.lock }()
	case <-callCtx.Done():
		return callCtx.Err()
	}
	select {
	case a.runtimeGate <- struct{}{}:
		defer func() { <-a.runtimeGate }()
		if err := callCtx.Err(); err != nil {
			return err
		}
		return operation(callCtx)
	case <-callCtx.Done():
		return callCtx.Err()
	}
}

func (a *Agent) call(ctx context.Context, method, path string, body, target any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+apiPrefix+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		a.invalidateReadiness()
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode >= 500 {
			a.invalidateReadiness()
		}
		return &apiError{status: resp.StatusCode, method: method, path: path}
	}
	if target != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(target)
	}
	return nil
}

// Run 持续重新读取节点与分配快照；控制面不可用时不清理或新建本地实例。
func (a *Agent) Run(ctx context.Context) error {
	defer a.invalidateReadiness()
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go a.runHeartbeats(heartbeatCtx)
	interval := a.resync
	if interval > readinessInterval {
		interval = readinessInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := a.reconcileAll(ctx); err != nil && ctx.Err() == nil {
			log.Printf("agent reconcile: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-a.readinessWake:
		}
	}
}

// 心跳与容器对账独立运行，单个运行时请求不能阻塞健康上报。
func (a *Agent) runHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := a.heartbeat(ctx); err != nil && ctx.Err() == nil {
			log.Printf("agent heartbeat: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// 每次读取最新版本和预留，避免心跳覆盖并发的绑定或清理事务。
func (a *Agent) heartbeat(ctx context.Context) error {
	var node api.Node
	path := "nodes/" + url.PathEscape(a.node)
	if err := a.call(ctx, http.MethodGet, path, nil, &node); err != nil {
		return err
	}
	if err := a.checkNodeUID(node.Metadata.UID); err != nil {
		return err
	}
	node.Status.Ready = true
	return a.call(ctx, http.MethodPut, path+"/status", struct {
		ResourceVersion int64          `json:"resourceVersion"`
		Status          api.NodeStatus `json:"status"`
	}{node.Metadata.ResourceVersion, node.Status}, nil)
}

// 相同名称的节点被重新创建后，旧 Agent 不得继续对其分配或心跳操作。
func (a *Agent) checkNodeUID(uid string) error {
	if uid == "" {
		return errors.New("registered node has no UID")
	}
	a.nodeMu.Lock()
	defer a.nodeMu.Unlock()
	if a.nodeUID == "" {
		a.nodeUID = uid
	}
	if a.nodeUID != uid {
		return errors.New("registered node UID changed; restart agent after verifying identity")
	}
	return nil
}

func (a *Agent) reconcileAll(ctx context.Context) error {
	var node api.Node
	if err := a.call(ctx, http.MethodGet, "nodes/"+url.PathEscape(a.node), nil, &node); err != nil {
		return err
	}
	if err := a.checkNodeUID(node.Metadata.UID); err != nil {
		return err
	}
	a.setReadinessEpoch(node.Status.Ready, node.Status.ReadinessEpoch)
	var list struct {
		Items []api.Pod `json:"items"`
	}
	if err := a.call(ctx, http.MethodGet, "pods", nil, &list); err != nil {
		return err
	}
	assigned := make(map[string]bool)
	var deleting, active []api.Pod
	for _, pod := range list.Items {
		if pod.NodeUID != node.Metadata.UID || pod.Metadata.UID == "" {
			continue
		}
		assigned[pod.Metadata.UID] = true
		if pod.Metadata.DeletionTimestamp != nil {
			deleting = append(deleting, pod)
		} else {
			active = append(active, pod)
		}
	}
	// 删除操作可能等待容器退出和 CNI；限制并发，避免一个 Pod 阻塞整个节点的缩容。
	const maxConcurrentCleanup = 6
	queue := make(chan api.Pod, len(deleting))
	for _, pod := range deleting {
		queue <- pod
	}
	close(queue)
	var cleanups sync.WaitGroup
	workers := min(maxConcurrentCleanup, len(deleting))
	for i := 0; i < workers; i++ {
		cleanups.Add(1)
		go func() {
			defer cleanups.Done()
			for pod := range queue {
				if ctx.Err() != nil {
					return
				}
				if err := a.reconcilePod(ctx, node.Metadata.UID, pod); err != nil && ctx.Err() == nil {
					log.Printf("agent pod uid=%s cleanup failed", pod.Metadata.UID)
				}
			}
		}()
	}
	// 清理上限小于运行时总并发，为正常 Pod 和控制请求保留容量。
	for _, pod := range active {
		if err := a.reconcilePod(ctx, node.Metadata.UID, pod); err != nil && ctx.Err() == nil {
			// 不记录 runtime 返回内容，避免镜像参数或环境变量进入日志。
			log.Printf("agent pod uid=%s reconciliation failed", pod.Metadata.UID)
		}
	}
	cleanups.Wait()
	a.pruneReadiness(assigned)
	return nil
}

func (a *Agent) reconcilePod(ctx context.Context, nodeUID string, pod api.Pod) error {
	if pod.Metadata.DeletionTimestamp != nil {
		a.forgetReadiness(pod.Metadata.UID)
		return a.cleanupPod(ctx, nodeUID, pod)
	}
	if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
		a.forgetReadiness(pod.Metadata.UID)
		return nil
	}
	var observation containerruntime.Observation
	err := a.withRuntime(ctx, pod.Metadata.UID, func(callCtx context.Context) error {
		var inspectErr error
		observation, inspectErr = a.runtime.Inspect(callCtx, pod.Metadata.UID)
		return inspectErr
	})
	if err != nil {
		a.forgetReadiness(pod.Metadata.UID)
		// CRI 状态不可确认时不得继续报告旧的就绪状态。
		_ = a.clearReady(ctx, pod)
		return err
	}
	if observation.Exited && observation.ExitReason == "" {
		observation.ExitReason = fmt.Sprintf("ExitCode %d", observation.ExitCode)
	}
	now := time.Now()
	state := a.retryFor(pod.Metadata.UID)
	if observation.Running {
		if state != (retryState{}) && !observation.StartedAt.IsZero() && now.Sub(observation.StartedAt) >= time.Minute {
			state = retryState{}
			if err := a.saveRetry(pod.Metadata.UID, state); err != nil {
				return err
			}
		}
		return a.reportStatus(ctx, pod, observation)
	}
	a.forgetReadiness(pod.Metadata.UID)
	if pod.Status.Ready {
		if err := a.clearReady(ctx, pod); err != nil {
			return err
		}
		// 清除 Ready 会推进资源版本；后续状态更新必须使用新版本。
		var current api.Pod
		if err := a.call(ctx, http.MethodGet, "pods/"+url.PathEscape(pod.Metadata.Name), nil, &current); err != nil {
			return err
		}
		if current.Metadata.UID != pod.Metadata.UID || current.NodeUID != nodeUID {
			return errors.New("pod identity or assignment changed")
		}
		pod = current
	}
	if observation.Exited && observation.ContainerID != "" && state.containerID != observation.ContainerID {
		if !observation.StartedAt.IsZero() && observation.FinishedAt.Sub(observation.StartedAt) >= time.Minute {
			state.failures = 0
		}
		state.containerID = observation.ContainerID
		state.failures++
		finished := observation.FinishedAt
		if finished.IsZero() {
			finished = now
		}
		deadline := finished.Add(backoff(state.failures))
		if state.next.Before(deadline) {
			state.next = deadline
		}
		if err := a.saveRetry(pod.Metadata.UID, state); err != nil {
			return err
		}
	}
	if now.Before(state.next) {
		return a.reportStatus(ctx, pod, observation)
	}
	// 新建或重启前再次确认分配和删除状态，避免使用过期快照主动启动。
	var current api.Pod
	if err := a.call(ctx, http.MethodGet, "pods/"+url.PathEscape(pod.Metadata.Name), nil, &current); err != nil {
		return err
	}
	if current.Metadata.UID != pod.Metadata.UID || current.NodeUID != nodeUID || current.Metadata.DeletionTimestamp != nil {
		return errors.New("pod identity or assignment changed")
	}
	previous := observation
	// 在启动前保守记录本次可能失败及其退避，进程中断也不会缩短等待。
	previousFailures := state.failures
	state.failures++
	state.inFlight = true
	state.next = time.Now().Add(a.runtimeLimit() + backoff(state.failures))
	if err := a.saveRetry(pod.Metadata.UID, state); err != nil {
		return err
	}
	err = a.withRuntime(ctx, current.Metadata.UID, func(callCtx context.Context) error {
		var ensureErr error
		observation, ensureErr = a.runtime.Ensure(callCtx, current)
		return ensureErr
	})
	if err != nil {
		state.inFlight = false
		state.next = time.Now().Add(backoff(state.failures))
		if saveErr := a.saveRetry(pod.Metadata.UID, state); saveErr != nil {
			return saveErr
		}
		// 已观察到退出时，即使重启失败也应清除之前的 Running/Ready。
		_ = a.reportStatus(ctx, current, previous)
		return err
	}
	if previous.Exited && previous.ExitReason != "" {
		// 即使本轮立即完成重启，也保留最近一次退出原因供诊断。
		observation.ExitReason = previous.ExitReason
	}
	state.failures = previousFailures
	state.next = time.Time{}
	state.inFlight = false
	if err := a.saveRetry(pod.Metadata.UID, state); err != nil {
		return err
	}
	return a.reportStatus(ctx, current, observation)
}

func backoff(failures int) time.Duration {
	if failures <= 1 {
		return time.Second
	}
	if failures >= 6 {
		return 30 * time.Second
	}
	return time.Second << (failures - 1)
}

func (a *Agent) reportStatus(ctx context.Context, pod api.Pod, observed containerruntime.Observation) error {
	status := pod.Status
	status.Ready, status.ReadinessEpoch = a.observedReady(ctx, pod, observed)
	status.PodIP = observed.PodIP
	if observed.Running {
		status.Phase = "Running"
		if observed.ExitReason != "" {
			status.Reason = observed.ExitReason
		}
	} else {
		status.Phase, status.Reason = "Pending", observed.ExitReason
	}
	if int64(observed.Attempt) > status.RestartCount {
		status.RestartCount = int64(observed.Attempt)
	}
	if reflect.DeepEqual(status, pod.Status) {
		return nil
	}
	return a.call(ctx, http.MethodPut, "pods/"+url.PathEscape(pod.Metadata.Name)+"/status", map[string]any{"resourceVersion": pod.Metadata.ResourceVersion, "status": status}, nil)
}

func (a *Agent) cleanupPod(ctx context.Context, nodeUID string, pod api.Pod) error {
	// 删除前再次读取，确保是同一 UID、仍属于本节点且已有删除标记。
	var current api.Pod
	path := "pods/" + url.PathEscape(pod.Metadata.Name)
	if err := a.call(ctx, http.MethodGet, path, nil, &current); err != nil {
		return err
	}
	if current.Metadata.UID != pod.Metadata.UID || current.NodeUID != nodeUID || current.Metadata.DeletionTimestamp == nil {
		return errors.New("pod identity or deletion state changed")
	}
	err := a.withRuntime(ctx, current.Metadata.UID, func(callCtx context.Context) error { return a.runtime.Cleanup(callCtx, current.Metadata.UID) })
	if err != nil {
		return err
	}
	// 仅在 runtime 确认完成后通知 API；API 原子删除 Pod 并释放预留。
	if err := a.call(ctx, http.MethodPost, path+"/cleanup", map[string]any{"uid": current.Metadata.UID, "resourceVersion": current.Metadata.ResourceVersion}, nil); err != nil {
		return err
	}
	return a.saveRetry(current.Metadata.UID, retryState{})
}

// Logs 按 UID 读取本地容器日志；调用者负责限制本机访问权限。
func (a *Agent) Logs(ctx context.Context, uid string, tail int) (string, error) {
	if uid == "" || tail < 0 {
		return "", errors.New("UID and nonnegative tail required")
	}
	var logs string
	err := a.withRuntime(ctx, uid, func(callCtx context.Context) error {
		var logsErr error
		logs, logsErr = a.runtime.Logs(callCtx, uid, tail)
		return logsErr
	})
	return logs, err
}

// CleanupStale 清理已被删除或重建的旧 UID；不能用于清理仍存在的同 UID Pod。
// 读取失败或未经授权时拒绝操作，避免把未知状态当作已删除。
func (a *Agent) CleanupStale(ctx context.Context, name, uid string) error {
	if name == "" || uid == "" || strings.ContainsAny(name, "/?#") {
		return errors.New("pod name and UID required")
	}
	var current api.Pod
	err := a.call(ctx, http.MethodGet, "pods/"+url.PathEscape(name), nil, &current)
	var response *apiError
	if err != nil && (!errors.As(err, &response) || response.status != http.StatusNotFound) {
		return err
	}
	if err == nil && current.Metadata.UID == uid {
		return errors.New("pod UID still exists")
	}
	// UID 不得属于本节点的其他名称，避免错误参数清理仍在运行的 Pod。
	var list struct {
		Items []api.Pod `json:"items"`
	}
	if err := a.call(ctx, http.MethodGet, "pods", nil, &list); err != nil {
		return err
	}
	for _, pod := range list.Items {
		if pod.Metadata.UID == uid {
			return errors.New("pod UID still assigned")
		}
	}
	var observed containerruntime.Observation
	err = a.withRuntime(ctx, uid, func(callCtx context.Context) error {
		var inspectErr error
		observed, inspectErr = a.runtime.Inspect(callCtx, uid)
		return inspectErr
	})
	if err != nil {
		return err
	}
	if observed.Exists && observed.PodName != name {
		return errors.New("runtime Pod name does not match")
	}
	return a.withRuntime(ctx, uid, func(callCtx context.Context) error { return a.runtime.Cleanup(callCtx, uid) })
}
