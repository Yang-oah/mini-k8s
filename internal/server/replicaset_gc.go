package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"mini-k8s/internal/api"
)

// 回收是显式管理员操作；额外余量覆盖终态时间写入与事务提交间的请求时限。
const operationRetention = 24*time.Hour + 10*time.Second
const operationGCBatch = 16

// 所有页面使用同一 etcd revision；总数与单页大小均有界，超限保守拒绝。
const operationOwnerGCPageSize = 32
const operationOwnerGCScanLimit = 2048

var errOwnerGCReferenced = errors.New("owner UID is still referenced")
var errOwnerGCScanLimit = errors.New("owner collection scan limit exceeded")

// 首次核验后还要等待真实经过的时间；进程重启会重置等待，避免墙钟前跳导致提前回收。
// 只为已存在的终态记录建立条目，数量受操作记录总配额约束。
var operationGCObserved sync.Map

func operationGCObservationKey(op *replicaOperation) string {
	return fmt.Sprintf("%s/%d/%s/%d/%s", op.UID, op.OperationID, op.State, op.CompletedAt, op.PodUID)
}

// gcReplicaOperation 一次最多回收 16 个别名；可安全重试直到 complete=true。
// 先持久化过期状态与 revision 水位，再清理映射，防止迟到的准备请求被误认为新请求。
func (s *Server) gcReplicaOperation(w http.ResponseWriter, r *http.Request, name string, fail errorResponder) {
	var req replicaOperationRequest
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil || !validOperationUID(req.UID) || req.OperationID < 1 || req.ResourceVersion != 0 || req.PodListRevision != 0 || req.PodName != "" {
		fail(400, "Invalid", "uid and operationID required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	setAuditUID(ctx, req.UID)
	op, rev, err := s.loadReplicaOperation(ctx, req.UID, req.OperationID)
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
			writeJSON(w, map[string]any{"uid": req.UID, "operationID": req.OperationID, "complete": true})
			return
		}
		fail(404, "NotFound", "operation not found")
		return
	}
	if op.RSName != name {
		fail(404, "NotFound", "operation not found")
		return
	}
	if op.State == "PREPARED" {
		fail(409, "Conflict", "pending operations cannot be collected")
		return
	}
	if op.GCVersion != 1 || op.CompletedAt <= 0 {
		fail(409, "Conflict", "operation requires retention migration")
		return
	}
	if op.State != "EXPIRED" {
		if op.State != "COMMITTED" && op.State != "CANCELLED" {
			fail(409, "Conflict", "only terminal operations can be collected")
			return
		}
		key := operationGCObservationKey(op)
		observed, _ := operationGCObserved.LoadOrStore(key, time.Now())
		if time.Since(observed.(time.Time)) < operationRetention {
			fail(409, "Conflict", "operation retention observation window is incomplete")
			return
		}
		if time.Since(time.Unix(0, op.CompletedAt)) < operationRetention {
			fail(409, "Conflict", "operation remains inside the retry window")
			return
		}
		podKey, pendingKey, watermarkKey := resourceKey("pods", op.PodName), operationPending(req.UID), operationWatermark(req.UID)
		snapshot, err := s.Client.Txn(ctx).Then(clientv3.OpGet(operationKey(req.UID, req.OperationID)), clientv3.OpGet(podKey), clientv3.OpGet(pendingKey), clientv3.OpGet(watermarkKey)).Commit()
		if err != nil {
			fail(503, "Unavailable", "storage unavailable")
			return
		}
		current := snapshot.Responses[0].GetResponseRange().Kvs
		if len(current) != 1 || current[0].ModRevision != rev {
			fail(409, "Conflict", "operation changed")
			return
		}
		var podRev, pendingRev, watermarkRev int64
		if pods := snapshot.Responses[1].GetResponseRange().Kvs; len(pods) > 0 {
			podRev = pods[0].ModRevision
			obj, err := decode(pods[0].Value, "pods", podRev)
			if err != nil {
				fail(503, "Unavailable", "invalid stored Pod")
				return
			}
			if op.State == "COMMITTED" && obj.meta.UID == op.PodUID {
				fail(409, "Conflict", "committed Pod has not been reclaimed")
				return
			}
		}
		if pending := snapshot.Responses[2].GetResponseRange().Kvs; len(pending) > 0 {
			pendingRev = pending[0].ModRevision
			if string(pending[0].Value) == strconv.FormatInt(req.OperationID, 10) {
				fail(409, "Conflict", "operation remains pending")
				return
			}
		}
		watermark := snapshot.Header.Revision
		if current := snapshot.Responses[3].GetResponseRange().Kvs; len(current) > 0 {
			watermarkRev = current[0].ModRevision
			previous, err := strconv.ParseInt(string(current[0].Value), 10, 64)
			if err != nil || previous < 0 {
				fail(503, "Unavailable", "invalid operation retention watermark")
				return
			}
			if previous > watermark {
				watermark = previous
			}
		}
		op.State = "EXPIRED"
		tx, err := s.Client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(operationKey(req.UID, req.OperationID)), "=", rev),
			clientv3.Compare(clientv3.ModRevision(podKey), "=", podRev),
			clientv3.Compare(clientv3.ModRevision(pendingKey), "=", pendingRev),
			clientv3.Compare(clientv3.ModRevision(watermarkKey), "=", watermarkRev),
		).Then(clientv3.OpPut(operationKey(req.UID, req.OperationID), encodeOperation(*op)), clientv3.OpPut(watermarkKey, strconv.FormatInt(watermark, 10))).Commit()
		if err != nil {
			fail(503, "Unavailable", "storage unavailable")
			return
		}
		if !tx.Succeeded {
			fail(409, "Conflict", "operation changed during expiration")
			return
		}
	}
	complete, err := s.gcReplicaOperationBatch(ctx, req.UID, req.OperationID)
	if err != nil {
		fail(409, "Conflict", "operation collection needs retry")
		return
	}
	if complete {
		// 观察记录对应过期前的终态；清除两种合法终态的记录。
		for _, state := range []string{"COMMITTED", "CANCELLED"} {
			terminal := *op
			terminal.State = state
			operationGCObserved.Delete(operationGCObservationKey(&terminal))
		}
	}
	writeJSON(w, map[string]any{"uid": req.UID, "operationID": req.OperationID, "complete": complete})
}

// 回收限量批次中的映射和反向索引；计数与删除必须同事务提交。
func (s *Server) gcReplicaOperationBatch(ctx context.Context, uid string, id int64) (bool, error) {
	opKey, liveKey := operationKey(uid, id), operationLive(uid)
	aliases := operationAliases(uid, id)
	snapshot, err := s.Client.Txn(ctx).Then(
		clientv3.OpGet(opKey), clientv3.OpGet(liveKey), clientv3.OpGet(operationQuotaKey),
		clientv3.OpGet(aliases, clientv3.WithPrefix(), clientv3.WithLimit(operationGCBatch)),
	).Commit()
	if err != nil {
		return false, err
	}
	ops := snapshot.Responses[0].GetResponseRange().Kvs
	live := snapshot.Responses[1].GetResponseRange().Kvs
	quota := snapshot.Responses[2].GetResponseRange().Kvs
	if len(ops) != 1 || len(live) != 1 || len(quota) != 1 {
		return false, fmt.Errorf("missing retention metadata")
	}
	var op replicaOperation
	if err := api.DecodeStrict(strings.NewReader(string(ops[0].Value)), &op); err != nil || op.State != "EXPIRED" || op.UID != uid || op.OperationID != id {
		return false, fmt.Errorf("invalid expired operation")
	}
	liveCount, err := operationUsage(live[0].Value)
	if err != nil {
		return false, err
	}
	globalCount, err := operationUsage(quota[0].Value)
	if err != nil {
		return false, err
	}
	items := snapshot.Responses[3].GetResponseRange().Kvs
	compares := []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(opKey), "=", ops[0].ModRevision),
		clientv3.Compare(clientv3.ModRevision(liveKey), "=", live[0].ModRevision),
		clientv3.Compare(clientv3.ModRevision(operationQuotaKey), "=", quota[0].ModRevision),
	}
	var deletes []clientv3.Op
	for _, item := range items {
		digest := strings.TrimPrefix(string(item.Key), aliases)
		if len(digest) != 64 || strings.ToLower(digest) != digest {
			return false, fmt.Errorf("invalid alias")
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return false, err
		}
		key := operationPrefix(uid) + "request/" + digest
		if string(item.Value) != key {
			return false, fmt.Errorf("invalid reverse mapping")
		}
		compares = append(compares, clientv3.Compare(clientv3.ModRevision(string(item.Key)), "=", item.ModRevision), clientv3.Compare(clientv3.Value(key), "=", strconv.FormatInt(id, 10)))
		deletes = append(deletes, clientv3.OpDelete(key), clientv3.OpDelete(string(item.Key)))
	}
	complete := len(items) == 0
	removed := int64(2 * len(items))
	if complete {
		deletes = append(deletes, clientv3.OpDelete(opKey))
		removed++
	}
	if liveCount < removed+3 || globalCount < removed {
		return false, fmt.Errorf("invalid retention usage")
	}
	deletes = append(deletes, clientv3.OpPut(liveKey, strconv.FormatInt(liveCount-removed, 10)), clientv3.OpPut(operationQuotaKey, strconv.FormatInt(globalCount-removed, 10)))
	tx, err := s.Client.Txn(ctx).If(compares...).Then(deletes...).Commit()
	if err != nil {
		return false, err
	}
	if !tx.Succeeded {
		return false, fmt.Errorf("retention changed concurrently")
	}
	return complete, nil
}

// scanOperationOwnerReferences 按固定 revision 分页检查 UID，不把不同时间的
// 资源列表拼成一个快照。历史版本被压缩、超时或解码失败均直接返回错误。
func (s *Server) scanOperationOwnerReferences(ctx context.Context, uid string, revision int64, limit int) error {
	if revision <= 0 || limit <= 0 {
		return errors.New("invalid owner scan revision or limit")
	}
	seen := 0
	for _, kind := range []string{"replicasets", "pods"} {
		keyPrefix := resourceKey(kind, "")
		start, end := keyPrefix, clientv3.GetPrefixRangeEnd(keyPrefix)
		for {
			// 预算耗尽时仍读取一个键，以区分恰好达到上限和实际超限。
			pageLimit := operationOwnerGCPageSize
			if remaining := limit - seen; remaining < pageLimit {
				pageLimit = remaining + 1
			}
			result, err := s.Client.Get(ctx, start, clientv3.WithRange(end), clientv3.WithRev(revision), clientv3.WithLimit(int64(pageLimit)), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
			if err != nil {
				return err
			}
			seen += len(result.Kvs)
			if seen > limit {
				return errOwnerGCScanLimit
			}
			for _, kv := range result.Kvs {
				item, err := decode(kv.Value, kind, kv.ModRevision)
				if err != nil {
					return fmt.Errorf("invalid stored %s: %w", kind, err)
				}
				if (kind == "replicasets" && item.meta.UID == uid) || (kind == "pods" && item.meta.OwnerUID == uid) {
					return errOwnerGCReferenced
				}
			}
			if !result.More {
				break
			}
			if len(result.Kvs) == 0 {
				return errors.New("owner scan returned an empty page with more results")
			}
			// NUL 后缀严格排除上一页末键，不跳过其字节前缀相同的后续键。
			start = string(result.Kvs[len(result.Kvs)-1].Key) + "\x00"
		}
	}
	return nil
}

// gcReplicaOperationOwner 在所有记录回收之后释放一个已删除 RS 的保留元数据。
// 本入口只允许管理员显式执行；任一状态不明或快照超限时保守拒绝。
func (s *Server) gcReplicaOperationOwner(w http.ResponseWriter, r *http.Request, name string, fail errorResponder) {
	var req replicaOperationRequest
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil || !validOperationUID(req.UID) || req.OperationID != 0 || req.ResourceVersion != 0 || req.PodListRevision != 0 || req.PodName != "" {
		fail(400, "Invalid", "uid required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	setAuditUID(ctx, req.UID)
	rsKey := resourceKey("replicasets", name)
	markerKey := childCreationKey(req.UID)
	snapshot, err := s.Client.Txn(ctx).Then(
		clientv3.OpGet(operationPrefix(req.UID), clientv3.WithPrefix(), clientv3.WithLimit(4)),
		clientv3.OpGet(operationQuotaKey), clientv3.OpGet(markerKey), clientv3.OpGet(rsKey),
	).Commit()
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	keys := snapshot.Responses[0].GetResponseRange()
	if keys.More {
		fail(409, "Conflict", "operation records remain")
		return
	}
	if err := s.scanOperationOwnerReferences(ctx, req.UID, snapshot.Header.Revision, operationOwnerGCScanLimit); err != nil {
		if errors.Is(err, errOwnerGCReferenced) || errors.Is(err, errOwnerGCScanLimit) {
			fail(409, "Conflict", err.Error())
		} else {
			fail(503, "Unavailable", "owner snapshot unavailable; retry collection")
		}
		return
	}
	if len(keys.Kvs) == 0 {
		// 重复请求只确认当前没有元数据，不再扣减配额。
		writeJSON(w, map[string]any{"uid": req.UID, "complete": true})
		return
	}
	metadata := map[string]int64{}
	for _, kv := range keys.Kvs {
		metadata[string(kv.Key)] = kv.ModRevision
	}
	sequenceKey, liveKey, watermarkKey := operationSequence(req.UID), operationLive(req.UID), operationWatermark(req.UID)
	if len(metadata) != 3 || metadata[sequenceKey] == 0 || metadata[liveKey] == 0 || metadata[watermarkKey] == 0 {
		fail(409, "Conflict", "operation records remain or owner metadata is incomplete")
		return
	}
	var sequence, live, watermark int64
	for _, kv := range keys.Kvs {
		value, err := operationUsage(kv.Value)
		if err != nil {
			fail(503, "Unavailable", "invalid owner metadata")
			return
		}
		switch string(kv.Key) {
		case sequenceKey:
			sequence = value
		case liveKey:
			live = value
		case watermarkKey:
			watermark = value
		}
	}
	quota := snapshot.Responses[1].GetResponseRange().Kvs
	if sequence < 1 || live != 3 || watermark < 1 || len(quota) != 1 {
		fail(409, "Conflict", "owner metadata or quota is not ready for collection")
		return
	}
	global, err := operationUsage(quota[0].Value)
	if err != nil || global < 3 {
		fail(503, "Unavailable", "invalid global operation usage")
		return
	}
	var markerRevision, rsRevision int64
	if markers := snapshot.Responses[2].GetResponseRange().Kvs; len(markers) > 0 {
		markerRevision = markers[0].ModRevision
	}
	if current := snapshot.Responses[3].GetResponseRange().Kvs; len(current) > 0 {
		rsRevision = current[0].ModRevision
	}
	// 合法的子 Pod 创建必须校验原 RS 并推进 marker；先前快照之外的
	// 迟到事务将因 marker/RS/配额比较失败，不能在回收后复活旧 UID。
	tx, err := s.Client.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(sequenceKey), "=", metadata[sequenceKey]),
		clientv3.Compare(clientv3.ModRevision(liveKey), "=", metadata[liveKey]),
		clientv3.Compare(clientv3.ModRevision(watermarkKey), "=", metadata[watermarkKey]),
		clientv3.Compare(clientv3.Version(operationPending(req.UID)), "=", 0),
		clientv3.Compare(clientv3.ModRevision(operationQuotaKey), "=", quota[0].ModRevision),
		clientv3.Compare(clientv3.ModRevision(markerKey), "=", markerRevision),
		clientv3.Compare(clientv3.ModRevision(rsKey), "=", rsRevision),
	).Then(
		clientv3.OpDelete(sequenceKey), clientv3.OpDelete(liveKey), clientv3.OpDelete(watermarkKey),
		clientv3.OpPut(operationQuotaKey, strconv.FormatInt(global-3, 10)),
	).Commit()
	if err != nil {
		fail(503, "Unavailable", "storage unavailable")
		return
	}
	if !tx.Succeeded {
		fail(409, "Conflict", "owner or quota changed during collection")
		return
	}
	writeJSON(w, map[string]any{"uid": req.UID, "complete": true})
}
