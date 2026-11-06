package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"mini-k8s/internal/api"
)

// replicaOperation is durable and deliberately contains no Pod environment.
type replicaOperation struct {
	UID         string `json:"uid"`
	OperationID int64  `json:"operationID"`
	PodName     string `json:"podName"`
	State       string `json:"state"`
	PodUID      string `json:"podUID"`
	RSName      string `json:"rsName"`
	RSVersion   int64  `json:"rsVersion"`
	CompletedAt int64  `json:"completedAt,omitempty"`
	GCVersion   int    `json:"gcVersion,omitempty"`
}

type replicaOperationRequest struct {
	UID             string `json:"uid"`
	OperationID     int64  `json:"operationID,omitempty"`
	ResourceVersion int64  `json:"resourceVersion,omitempty"`
	PodListRevision int64  `json:"podListRevision,omitempty"`
	PodName         string `json:"podName,omitempty"`
}

func operationPrefix(uid string) string    { return prefix + "replica-operations/" + uid + "/" }
func operationPending(uid string) string   { return operationPrefix(uid) + "pending" }
func operationSequence(uid string) string  { return operationPrefix(uid) + "sequence" }
func operationWatermark(uid string) string { return operationPrefix(uid) + "gc-watermark" }
func operationLive(uid string) string      { return operationPrefix(uid) + "live-count" }

const operationQuotaKey = prefix + "replica-operation-quota-v1"
const operationGlobalLimit int64 = 100000
const operationPerRSLimit int64 = 10000

// 计入 record、请求映射和反向索引；每个 UID 预留三个持久元数据键。
// 预留不自动释放，避免无界墓碑增长；已删除 RS 的元数据回收另行设计。
func operationUsage(raw []byte) (int64, error) {
	count, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || count < 0 {
		return 0, fmt.Errorf("invalid operation usage")
	}
	return count, nil
}
func operationAliases(uid string, id int64) string {
	return operationPrefix(uid) + "aliases/" + strconv.FormatInt(id, 10) + "/"
}
func operationAliasKey(uid string, id int64, requestKey string) string {
	return operationAliases(uid, id) + strings.TrimPrefix(requestKey, operationPrefix(uid)+"request/")
}
func operationKey(uid string, id int64) string {
	return operationPrefix(uid) + "record/" + strconv.FormatInt(id, 10)
}

// 同一请求无论响应是否丢失都映射到同一持久化结果；编码固定、无需存储 Pod 内容。
func operationRequestKey(name string, req replicaOperationRequest) string {
	body, _ := json.Marshal(struct {
		Name         string `json:"name"`
		UID          string `json:"uid"`
		Version      int64  `json:"version"`
		ListRevision int64  `json:"listRevision"`
		PodName      string `json:"podName"`
	}{name, req.UID, req.ResourceVersion, req.PodListRevision, req.PodName})
	digest := sha256.Sum256(body)
	return operationPrefix(req.UID) + "request/" + hex.EncodeToString(digest[:])
}

func validOperationUID(uid string) bool {
	if len(uid) != 32 {
		return false
	}
	_, err := hex.DecodeString(uid)
	return err == nil && strings.ToLower(uid) == uid
}

func encodeOperation(op replicaOperation) string { b, _ := json.Marshal(op); return string(b) }

// operationActive mirrors controller's conservative replica accounting.
func operationActive(p *api.Pod) bool {
	if p.Metadata.DeletionTimestamp != nil && p.NodeUID != "" {
		return true
	}
	return p.Status.Phase != "Succeeded" && p.Status.Phase != "Failed"
}

func (s *Server) replicaCreateOperation(w http.ResponseWriter, r *http.Request, name string, fail errorResponder) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var req replicaOperationRequest
	if r.Method == http.MethodGet {
		req.UID = r.URL.Query().Get("uid")
		if id := r.URL.Query().Get("operationID"); id != "" {
			parsed, err := strconv.ParseInt(id, 10, 64)
			if err != nil || parsed < 1 {
				fail(400, "Invalid", "invalid operationID")
				return
			}
			req.OperationID = parsed
		}
	} else if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil {
		fail(400, "Invalid", "invalid operation request")
		return
	}
	if !validOperationUID(req.UID) {
		fail(400, "Invalid", "invalid RS UID")
		return
	}
	setAuditUID(ctx, req.UID)
	switch r.Method {
	case http.MethodGet:
		if req.OperationID == 0 {
			pending, err := s.Client.Get(ctx, operationPending(req.UID))
			if err != nil {
				fail(503, "Unavailable", "storage unavailable")
				return
			}
			if len(pending.Kvs) == 0 {
				fail(404, "NotFound", "no pending operation")
				return
			}
			id, err := strconv.ParseInt(string(pending.Kvs[0].Value), 10, 64)
			if err != nil || id < 1 {
				fail(503, "Unavailable", "invalid operation index")
				return
			}
			req.OperationID = id
		}
		op, _, err := s.loadReplicaOperation(ctx, req.UID, req.OperationID)
		if err != nil {
			fail(503, "Unavailable", "storage unavailable")
			return
		}
		if op == nil {
			expired, err := s.replicaOperationExpired(ctx, req.UID, req.OperationID)
			if err != nil {
				fail(503, "Unavailable", "storage unavailable")
				return
			}
			if expired {
				fail(http.StatusGone, "Expired", "operation result is no longer retained")
				return
			}
			fail(404, "NotFound", "operation not found")
			return
		}
		if op.RSName != name {
			fail(404, "NotFound", "operation not found")
			return
		}
		if op.State == "EXPIRED" {
			fail(http.StatusGone, "Expired", "operation result is no longer retained")
			return
		}
		writeJSON(w, op)
	case http.MethodPost:
		s.prepareReplicaOperation(ctx, w, name, req, fail)
	case http.MethodPut:
		s.commitReplicaOperation(ctx, w, name, req, fail)
	case http.MethodDelete:
		s.cancelReplicaOperation(ctx, w, name, req, fail)
	default:
		fail(405, "MethodNotAllowed", "unsupported method")
	}
}

func (s *Server) loadReplicaOperation(ctx context.Context, uid string, id int64) (*replicaOperation, int64, error) {
	res, err := s.Client.Get(ctx, operationKey(uid, id))
	if err != nil {
		return nil, 0, err
	}
	if len(res.Kvs) == 0 {
		return nil, 0, nil
	}
	var op replicaOperation
	if err := json.Unmarshal(res.Kvs[0].Value, &op); err != nil || op.UID != uid || op.OperationID != id {
		return nil, 0, fmt.Errorf("invalid operation")
	}
	return &op, res.Kvs[0].ModRevision, nil
}

// 序号永久递增；找不到不高于水位的旧结果时，绝不能重新解释为新操作。
func (s *Server) replicaOperationExpired(ctx context.Context, uid string, id int64) (bool, error) {
	result, err := s.Client.Get(ctx, operationSequence(uid))
	if err != nil || len(result.Kvs) == 0 {
		return false, err
	}
	sequence, err := strconv.ParseInt(string(result.Kvs[0].Value), 10, 64)
	if err != nil || sequence < 0 {
		return false, fmt.Errorf("invalid operation sequence")
	}
	return id <= sequence, nil
}

func (s *Server) replyMissingReplicaOperation(ctx context.Context, uid string, id int64, fail errorResponder) {
	expired, err := s.replicaOperationExpired(ctx, uid, id)
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if expired {
		fail(http.StatusGone, "Expired", "operation result is no longer retained")
		return
	}
	fail(404, "NotFound", "operation not found")
}

func countActivePods(kvs []*mvccpb.KeyValue, uid string) (int, error) {
	count := 0
	for _, kv := range kvs {
		o, err := decode(kv.Value, "pods", kv.ModRevision)
		if err != nil {
			return 0, err
		}
		p := o.value.(*api.Pod)
		if p.Metadata.OwnerUID == uid && operationActive(p) {
			count++
		}
	}
	return count, nil
}

func (s *Server) prepareReplicaOperation(ctx context.Context, w http.ResponseWriter, name string, req replicaOperationRequest, fail errorResponder) {
	if req.ResourceVersion <= 0 || req.PodListRevision <= 0 || req.PodName == "" || req.OperationID != 0 {
		fail(400, "Invalid", "uid, resourceVersion, podListRevision and podName required")
		return
	}
	// 使用现有 Pod 校验规则检查名称，避免持久化无法创建的意图。
	unit := api.Resources{CPUMilli: 1, MemoryBytes: 1}
	test := api.Pod{
		APIVersion: api.Version, Kind: "Pod", Metadata: api.Metadata{Name: req.PodName},
		Spec: api.PodSpec{Image: "intent", RestartPolicy: "Always",
			Resources: api.ResourceRequirements{Requests: unit, Limits: unit}},
	}
	if err := api.ValidatePod(test); err != nil {
		fail(400, "Invalid", "invalid podName")
		return
	}
	rsKey := resourceKey("replicasets", name)
	pendingKey := operationPending(req.UID)
	seqKey := operationSequence(req.UID)
	marker := childCreationKey(req.UID)
	requestKey := operationRequestKey(name, req)
	watermarkKey := operationWatermark(req.UID)
	liveKey := operationLive(req.UID)
	for attempt := 0; attempt < 8; attempt++ {
		snap, err := s.Client.Txn(ctx).Then(
			clientv3.OpGet(rsKey), clientv3.OpGet(pendingKey), clientv3.OpGet(seqKey),
			clientv3.OpGet(resourceKey("pods", ""), clientv3.WithPrefix()),
			clientv3.OpGet(marker), clientv3.OpGet(requestKey), clientv3.OpGet(watermarkKey),
			clientv3.OpGet(liveKey), clientv3.OpGet(operationQuotaKey),
			clientv3.OpGet(prefix+"replica-operations/", clientv3.WithPrefix(), clientv3.WithLimit(1)),
		).Commit()
		if err != nil {
			fail(503, "Unavailable", "storage unavailable")
			return
		}
		if req.PodListRevision > snap.Header.Revision {
			fail(409, "Conflict", "pod list revision is ahead of storage")
			return
		}
		if mapping := snap.Responses[5].GetResponseRange().Kvs; len(mapping) > 0 {
			id, err := strconv.ParseInt(string(mapping[0].Value), 10, 64)
			if err != nil || id < 1 {
				fail(503, "Unavailable", "invalid request mapping")
				return
			}
			op, _, err := s.loadReplicaOperation(ctx, req.UID, id)
			if err != nil || op == nil || op.RSName != name {
				fail(503, "Unavailable", "invalid operation record")
				return
			}
			if op.State == "EXPIRED" {
				fail(http.StatusGone, "Expired", "operation result is no longer retained")
				return
			}
			writeJSON(w, op)
			return
		}
		var watermarkRevision int64
		if marks := snap.Responses[6].GetResponseRange().Kvs; len(marks) > 0 {
			watermark, parseErr := strconv.ParseInt(string(marks[0].Value), 10, 64)
			if parseErr != nil || watermark < 0 {
				fail(503, "Unavailable", "invalid operation retention watermark")
				return
			}
			if req.PodListRevision <= watermark {
				fail(http.StatusGone, "Expired", "pod snapshot is older than operation retention watermark")
				return
			}
			watermarkRevision = marks[0].ModRevision
		}
		rsKVs := snap.Responses[0].GetResponseRange().Kvs
		if len(rsKVs) != 1 {
			fail(409, "Conflict", "RS unavailable")
			return
		}
		rsObj, err := decode(rsKVs[0].Value, "replicasets", rsKVs[0].ModRevision)
		if err != nil {
			fail(503, "Unavailable", "invalid stored RS")
			return
		}
		rs := rsObj.value.(*api.ReplicaSet)
		if rs.Metadata.UID != req.UID || rs.Metadata.DeletionTimestamp != nil {
			fail(409, "Conflict", "RS identity or deletion mismatch")
			return
		}
		var seq, seqRev, markerRev, liveCount, liveRev, globalCount, globalRev int64
		if kvs := snap.Responses[2].GetResponseRange().Kvs; len(kvs) > 0 {
			seq, err = strconv.ParseInt(string(kvs[0].Value), 10, 64)
			if err != nil || seq < 0 || seq == math.MaxInt64 {
				fail(409, "Conflict", "operation sequence exhausted")
				return
			}
			seqRev = kvs[0].ModRevision
		}
		if kvs := snap.Responses[7].GetResponseRange().Kvs; len(kvs) > 0 {
			liveCount, err = operationUsage(kvs[0].Value)
			if err != nil {
				fail(503, "Unavailable", "invalid operation usage")
				return
			}
			liveRev = kvs[0].ModRevision
		}
		if kvs := snap.Responses[8].GetResponseRange().Kvs; len(kvs) > 0 {
			globalCount, err = operationUsage(kvs[0].Value)
			if err != nil {
				fail(503, "Unavailable", "invalid global operation usage")
				return
			}
			globalRev = kvs[0].ModRevision
		} else if len(snap.Responses[9].GetResponseRange().Kvs) > 0 {
			// 升级前数据没有配额计数，禁止默认为零；管理员须先迁移。
			fail(503, "Unavailable", "operation quota requires migration")
			return
		}
		if (seqRev == 0) != (liveRev == 0) {
			fail(503, "Unavailable", "operation usage requires migration")
			return
		}
		pending := snap.Responses[1].GetResponseRange().Kvs
		if len(pending) > 0 {
			id, err := strconv.ParseInt(string(pending[0].Value), 10, 64)
			if err != nil {
				fail(503, "Unavailable", "invalid operation index")
				return
			}
			op, _, err := s.loadReplicaOperation(ctx, req.UID, id)
			if err != nil || op == nil {
				fail(503, "Unavailable", "invalid operation record")
				return
			}
			if op.RSName != name {
				fail(409, "Conflict", "pending operation differs")
				return
			}
			if op.GCVersion != 1 {
				fail(503, "Unavailable", "pending operation requires retention migration")
				return
			}
			if liveCount > operationPerRSLimit-2 || globalCount > operationGlobalLimit-2 {
				fail(503, "Unavailable", "operation retention quota exhausted")
				return
			}
			// 不同准备请求指向已有意图时也须持久化映射，防止响应丢失后另分配操作。
			tx, err := s.Client.Txn(ctx).If(
				clientv3.Compare(clientv3.ModRevision(pendingKey), "=", pending[0].ModRevision),
				clientv3.Compare(clientv3.ModRevision(seqKey), "=", seqRev),
				clientv3.Compare(clientv3.Version(requestKey), "=", 0),
				clientv3.Compare(clientv3.ModRevision(watermarkKey), "=", watermarkRevision),
				clientv3.Compare(clientv3.ModRevision(liveKey), "=", liveRev),
				clientv3.Compare(clientv3.ModRevision(operationQuotaKey), "=", globalRev),
			).Then(
				clientv3.OpPut(requestKey, strconv.FormatInt(id, 10)),
				clientv3.OpPut(operationAliasKey(req.UID, id, requestKey), requestKey),
				clientv3.OpPut(liveKey, strconv.FormatInt(liveCount+2, 10)),
				clientv3.OpPut(operationQuotaKey, strconv.FormatInt(globalCount+2, 10)),
			).Commit()
			if err != nil {
				fail(503, "Unavailable", "storage unavailable")
				return
			}
			if tx.Succeeded {
				writeJSON(w, op)
				return
			}
			continue
		}
		if rs.Metadata.ResourceVersion != req.ResourceVersion {
			fail(409, "Conflict", "RS version changed")
			return
		}
		count, err := countActivePods(snap.Responses[3].GetResponseRange().Kvs, req.UID)
		if err != nil {
			fail(503, "Unavailable", "invalid stored Pod")
			return
		}
		if count >= rs.Spec.Replicas {
			fail(409, "Conflict", "no replica deficit")
			return
		}
		if kvs := snap.Responses[4].GetResponseRange().Kvs; len(kvs) > 0 {
			markerRev = kvs[0].ModRevision
		}
		if markerRev > req.PodListRevision {
			fail(409, "Conflict", "pod snapshot predates child creation")
			return
		}
		id := seq + 1
		cost := int64(3) // record、请求映射和反向索引
		if seqRev == 0 {
			cost += 3
		} // sequence、live-count、预留 watermark
		if liveCount > operationPerRSLimit-cost || globalCount > operationGlobalLimit-cost {
			fail(503, "Unavailable", "operation retention quota exhausted")
			return
		}
		op := replicaOperation{UID: req.UID, OperationID: id, PodName: req.PodName, State: "PREPARED", RSName: name, RSVersion: req.ResourceVersion, GCVersion: 1}
		tx, err := s.Client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(rsKey), "=", rs.Metadata.ResourceVersion),
			clientv3.Compare(clientv3.Version(pendingKey), "=", 0),
			clientv3.Compare(clientv3.ModRevision(seqKey), "=", seqRev),
			clientv3.Compare(clientv3.ModRevision(marker), "=", markerRev),
			clientv3.Compare(clientv3.Version(requestKey), "=", 0),
			clientv3.Compare(clientv3.ModRevision(watermarkKey), "=", watermarkRevision),
			clientv3.Compare(clientv3.ModRevision(liveKey), "=", liveRev),
			clientv3.Compare(clientv3.ModRevision(operationQuotaKey), "=", globalRev),
		).Then(
			clientv3.OpPut(seqKey, strconv.FormatInt(id, 10)),
			clientv3.OpPut(pendingKey, strconv.FormatInt(id, 10)),
			clientv3.OpPut(operationKey(req.UID, id), encodeOperation(op)),
			clientv3.OpPut(requestKey, strconv.FormatInt(id, 10)),
			clientv3.OpPut(operationAliasKey(req.UID, id, requestKey), requestKey),
			clientv3.OpPut(liveKey, strconv.FormatInt(liveCount+cost, 10)),
			clientv3.OpPut(operationQuotaKey, strconv.FormatInt(globalCount+cost, 10)),
		).Commit()
		if err != nil {
			fail(503, "Unavailable", "storage unavailable")
			return
		}
		if tx.Succeeded {
			writeJSON(w, op)
			return
		}
	}
	fail(409, "Conflict", "operation preparation conflicted")
}

func (s *Server) commitReplicaOperation(ctx context.Context, w http.ResponseWriter, name string, req replicaOperationRequest, fail errorResponder) {
	if req.OperationID < 1 || req.PodName == "" || req.ResourceVersion != 0 || req.PodListRevision != 0 {
		fail(400, "Invalid", "uid, operationID and podName required")
		return
	}
	op, rev, err := s.loadReplicaOperation(ctx, req.UID, req.OperationID)
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if op == nil {
		s.replyMissingReplicaOperation(ctx, req.UID, req.OperationID, fail)
		return
	}
	if op.RSName != name {
		fail(404, "NotFound", "operation not found")
		return
	}
	if op.State == "EXPIRED" {
		fail(http.StatusGone, "Expired", "operation result is no longer retained")
		return
	}
	if op.PodName != req.PodName {
		fail(409, "Conflict", "operation content differs")
		return
	}
	if op.State == "COMMITTED" {
		writeJSON(w, op)
		return
	}
	if op.State != "PREPARED" {
		fail(409, "Conflict", "operation not prepared")
		return
	}
	rsKey := resourceKey("replicasets", name)
	marker := childCreationKey(req.UID)
	pending := operationPending(req.UID)
	podKey := resourceKey("pods", op.PodName)
	snap, err := s.Client.Txn(ctx).Then(
		clientv3.OpGet(rsKey), clientv3.OpGet(resourceKey("pods", ""), clientv3.WithPrefix()),
		clientv3.OpGet(marker), clientv3.OpGet(pending), clientv3.OpGet(operationKey(req.UID, req.OperationID)),
	).Commit()
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	// 操作状态与 Pod、pending 必须来自同一快照。并发重试可能在首次读取
	// PREPARED 后完成提交，此时应重放结果，而不是把空 pending 当成冲突。
	current := snap.Responses[4].GetResponseRange().Kvs
	if len(current) != 1 {
		s.replyMissingReplicaOperation(ctx, req.UID, req.OperationID, fail)
		return
	}
	var snapshotOp replicaOperation
	if err := json.Unmarshal(current[0].Value, &snapshotOp); err != nil || snapshotOp.UID != req.UID || snapshotOp.OperationID != req.OperationID {
		fail(503, "Unavailable", "invalid operation record")
		return
	}
	if snapshotOp.RSName != name || snapshotOp.PodName != req.PodName {
		fail(409, "Conflict", "operation content differs")
		return
	}
	if snapshotOp.State == "COMMITTED" {
		writeJSON(w, &snapshotOp)
		return
	}
	if snapshotOp.State == "EXPIRED" {
		fail(http.StatusGone, "Expired", "operation result is no longer retained")
		return
	}
	if snapshotOp.State != "PREPARED" {
		fail(409, "Conflict", "operation not prepared")
		return
	}
	op, rev = &snapshotOp, current[0].ModRevision
	kvs := snap.Responses[0].GetResponseRange().Kvs
	if len(kvs) != 1 {
		fail(409, "Conflict", "RS unavailable")
		return
	}
	rsObj, err := decode(kvs[0].Value, "replicasets", kvs[0].ModRevision)
	if err != nil {
		fail(503, "Unavailable", "invalid stored RS")
		return
	}
	rs := rsObj.value.(*api.ReplicaSet)
	if rs.Metadata.UID != req.UID || rs.Metadata.ResourceVersion != op.RSVersion || rs.Metadata.DeletionTimestamp != nil {
		fail(409, "Conflict", "RS identity, version or deletion changed")
		return
	}
	count, err := countActivePods(snap.Responses[1].GetResponseRange().Kvs, req.UID)
	if err != nil {
		fail(503, "Unavailable", "invalid stored Pod")
		return
	}
	if count >= rs.Spec.Replicas {
		fail(409, "Conflict", "no replica deficit")
		return
	}
	var markerRev int64
	if m := snap.Responses[2].GetResponseRange().Kvs; len(m) > 0 {
		markerRev = m[0].ModRevision
	}
	index := snap.Responses[3].GetResponseRange().Kvs
	if len(index) != 1 || string(index[0].Value) != strconv.FormatInt(req.OperationID, 10) {
		fail(409, "Conflict", "operation no longer pending")
		return
	}
	pod := api.Pod{
		APIVersion: api.Version, Kind: "Pod",
		Metadata: api.Metadata{Name: op.PodName, Namespace: rs.Metadata.Namespace, UID: newUID(),
			OwnerUID: req.UID, Labels: rs.Spec.Template.Labels, Annotations: rs.Spec.Template.Annotations},
		Spec: rs.Spec.Template.Spec, Status: api.PodStatus{Phase: "Pending"},
	}
	if err := api.ValidatePod(pod); err != nil {
		fail(409, "Conflict", "invalid RS template")
		return
	}
	op.State = "COMMITTED"
	op.PodUID = pod.Metadata.UID
	op.CompletedAt = time.Now().UTC().UnixNano()
	obj := object{value: &pod, meta: &pod.Metadata}
	// Pod、结果和所有权标记同事务提交；所有快照检查均由 CAS 保护。
	tx, err := s.Client.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(rsKey), "=", op.RSVersion),
		clientv3.Compare(clientv3.ModRevision(operationKey(req.UID, req.OperationID)), "=", rev),
		clientv3.Compare(clientv3.ModRevision(pending), "=", index[0].ModRevision),
		clientv3.Compare(clientv3.ModRevision(marker), "=", markerRev),
		clientv3.Compare(clientv3.Version(podKey), "=", 0),
	).Then(
		clientv3.OpPut(podKey, encodeObject(obj)),
		clientv3.OpPut(marker, pod.Metadata.UID),
		clientv3.OpPut(operationKey(req.UID, req.OperationID), encodeOperation(*op)),
		clientv3.OpDelete(pending),
	).Commit()
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if tx.Succeeded {
		writeJSON(w, op)
		return
	}
	// 并发的相同提交可能已经成功，重读持久化结果再决定是否冲突。
	existing, _, err := s.loadReplicaOperation(ctx, req.UID, req.OperationID)
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if existing != nil && existing.State == "COMMITTED" && existing.RSName == name && existing.PodName == req.PodName {
		writeJSON(w, existing)
		return
	}
	fail(409, "Conflict", "operation commit conflicted")
}

func (s *Server) cancelReplicaOperation(ctx context.Context, w http.ResponseWriter, name string, req replicaOperationRequest, fail errorResponder) {
	if req.OperationID < 1 || req.PodName != "" || req.ResourceVersion != 0 || req.PodListRevision != 0 {
		fail(400, "Invalid", "uid and operationID required")
		return
	}
	op, rev, err := s.loadReplicaOperation(ctx, req.UID, req.OperationID)
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if op == nil {
		s.replyMissingReplicaOperation(ctx, req.UID, req.OperationID, fail)
		return
	}
	if op.RSName != name {
		fail(404, "NotFound", "operation not found")
		return
	}
	if op.State == "EXPIRED" {
		fail(http.StatusGone, "Expired", "operation result is no longer retained")
		return
	}
	if op.State == "CANCELLED" {
		writeJSON(w, op)
		return
	}
	if op.State != "PREPARED" {
		fail(409, "Conflict", "operation already committed")
		return
	}
	op.State = "CANCELLED"
	op.CompletedAt = time.Now().UTC().UnixNano()
	pending := operationPending(req.UID)
	tx, err := s.Client.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(operationKey(req.UID, req.OperationID)), "=", rev),
		clientv3.Compare(clientv3.Value(pending), "=", strconv.FormatInt(req.OperationID, 10)),
	).Then(
		clientv3.OpPut(operationKey(req.UID, req.OperationID), encodeOperation(*op)),
		clientv3.OpDelete(pending),
	).Commit()
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if tx.Succeeded {
		writeJSON(w, op)
		return
	}
	existing, _, err := s.loadReplicaOperation(ctx, req.UID, req.OperationID)
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if existing != nil && existing.State == "CANCELLED" {
		writeJSON(w, existing)
		return
	}
	fail(409, "Conflict", "operation cancellation conflicted")
}

// deletionOperationFence 让 RS 删除和待提交意图的撤销处于同一事务。
func (s *Server) deletionOperationFence(ctx context.Context, uid string) ([]clientv3.Cmp, []clientv3.Op, error) {
	pending := operationPending(uid)
	result, err := s.Client.Get(ctx, pending)
	if err != nil {
		return nil, nil, err
	}
	if len(result.Kvs) == 0 {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Version(pending), "=", 0)}, nil, nil
	}
	index := result.Kvs[0]
	id, err := strconv.ParseInt(string(index.Value), 10, 64)
	if err != nil || id < 1 {
		return nil, nil, fmt.Errorf("invalid pending operation")
	}
	op, rev, err := s.loadReplicaOperation(ctx, uid, id)
	if err != nil || op == nil || op.State != "PREPARED" {
		return nil, nil, fmt.Errorf("invalid pending record")
	}
	op.State = "CANCELLED"
	op.CompletedAt = time.Now().UTC().UnixNano()
	compares := []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(pending), "=", index.ModRevision),
		clientv3.Compare(clientv3.ModRevision(operationKey(uid, id)), "=", rev),
	}
	operations := []clientv3.Op{
		clientv3.OpPut(operationKey(uid, id), encodeOperation(*op)), clientv3.OpDelete(pending),
	}
	return compares, operations, nil
}
