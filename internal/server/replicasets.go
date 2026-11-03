package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var errOwnerUnavailable = errors.New("owner ReplicaSet missing or deleting")

func childCreationKey(ownerUID string) string { return prefix + "child-creation/" + ownerUID }

// createOwnedPod 在创建子 Pod 的同一事务中检查父对象版本。
// 删除标记或同名重建会改变父对象版本，因而不能产生孤儿 Pod。
func (s *Server) createOwnedPod(ctx context.Context, podKey string, pod object) (*clientv3.TxnResponse, error) {
	owners, err := s.Client.Get(ctx, resourceKey("replicasets", ""), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range owners.Kvs {
		owner, err := decode(kv.Value, "replicasets", kv.ModRevision)
		if err != nil {
			return nil, err
		}
		if owner.meta.UID != pod.meta.OwnerUID || owner.meta.DeletionTimestamp != nil {
			continue
		}
		// 创建标记与子对象一起写入。删除空 RS 时比较该标记，
		// 可检测快照读取之后并发提交的创建。
		return s.Client.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(podKey), "=", 0),
				clientv3.Compare(clientv3.ModRevision(string(kv.Key)), "=", kv.ModRevision)).
			Then(clientv3.OpPut(podKey, encodeObject(pod)),
				clientv3.OpPut(childCreationKey(pod.meta.OwnerUID), pod.meta.UID)).
			Commit()
	}
	return nil, errOwnerUnavailable
}

// childSnapshot 在同一读取版本上检查子 Pod 和最后一次创建标记。
func (s *Server) childSnapshot(ctx context.Context, ownerUID string) (bool, int64, error) {
	snapshot, err := s.Client.Txn(ctx).
		Then(clientv3.OpGet(resourceKey("pods", ""), clientv3.WithPrefix()),
			clientv3.OpGet(childCreationKey(ownerUID))).
		Commit()
	if err != nil {
		return false, 0, err
	}
	var markerRevision int64
	markers := snapshot.Responses[1].GetResponseRange().Kvs
	if len(markers) > 0 {
		markerRevision = markers[0].ModRevision
	}
	for _, kv := range snapshot.Responses[0].GetResponseRange().Kvs {
		pod, err := decode(kv.Value, "pods", kv.ModRevision)
		if err != nil {
			return false, 0, err
		}
		if pod.meta.OwnerUID == ownerUID {
			return true, markerRevision, nil
		}
	}
	return false, markerRevision, nil
}

func (s *Server) deleteReplicaSet(ctx context.Context, w http.ResponseWriter, rs object, respondError errorResponder) {
	hasChildren, markerRevision, err := s.childSnapshot(ctx, rs.meta.UID)
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	rsKey := resourceKey("replicasets", rs.meta.Name)
	compares := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(rsKey), "=", rs.meta.ResourceVersion)}
	var operations []clientv3.Op
	result := "deleted"
	if hasChildren {
		if rs.meta.DeletionTimestamp != nil {
			writeJSON(w, map[string]string{"result": "terminating"})
			return
		}
		// 有子 Pod 时保留 RS 和所有权，直到 Controller 确认全部回收。
		now := time.Now().UTC()
		rs.meta.DeletionTimestamp = &now
		operations = append(operations, clientv3.OpPut(rsKey, encodeObject(rs)))
		result = "terminating"
	} else {
		compares = append(compares, clientv3.Compare(clientv3.ModRevision(childCreationKey(rs.meta.UID)), "=", markerRevision))
		operations = append(operations, clientv3.OpDelete(rsKey), clientv3.OpDelete(childCreationKey(rs.meta.UID)),
			clientv3.OpPut(prefix+"replicaset-selector-revision", newUID()))
	}
	resultTxn, err := s.Client.Txn(ctx).If(compares...).Then(operations...).Commit()
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if !resultTxn.Succeeded {
		respondError(409, "Conflict", "concurrent ReplicaSet or child change")
		return
	}
	writeJSON(w, map[string]string{"result": result})
}

// finalizeReplicaSet 仅允许 Controller 在所有所属 Pod 已删除后移除 RS。
func (s *Server) finalizeReplicaSet(w http.ResponseWriter, r *http.Request, name string, respondError errorResponder) {
	var request struct {
		UID             string `json:"uid"`
		ResourceVersion int64  `json:"resourceVersion"`
	}
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &request); err != nil || request.UID == "" || request.ResourceVersion <= 0 {
		respondError(400, "Invalid", "uid and resourceVersion required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rsKey := resourceKey("replicasets", name)
	get, err := s.Client.Get(ctx, rsKey)
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if len(get.Kvs) == 0 {
		respondError(404, "NotFound", "ReplicaSet not found")
		return
	}
	rs, err := decode(get.Kvs[0].Value, "replicasets", get.Kvs[0].ModRevision)
	if err != nil {
		respondError(503, "Unavailable", "stored ReplicaSet invalid")
		return
	}
	setAuditUID(ctx, rs.meta.UID)
	if rs.meta.UID != request.UID || rs.meta.ResourceVersion != request.ResourceVersion || rs.meta.DeletionTimestamp == nil {
		respondError(409, "Conflict", "ReplicaSet identity, revision or deletion mismatch")
		return
	}
	hasChildren, markerRevision, err := s.childSnapshot(ctx, request.UID)
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if hasChildren {
		respondError(409, "Conflict", "ReplicaSet still has children")
		return
	}
	// 包括删除标记中的 Pod 在内的所有子对象都必须消失，才能释放 RS 名称。
	txn, err := s.Client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(rsKey), "=", request.ResourceVersion),
			clientv3.Compare(clientv3.ModRevision(childCreationKey(request.UID)), "=", markerRevision)).
		Then(clientv3.OpDelete(rsKey), clientv3.OpDelete(childCreationKey(request.UID)),
			clientv3.OpPut(prefix+"replicaset-selector-revision", newUID())).
		Commit()
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if !txn.Succeeded {
		respondError(409, "Conflict", "concurrent ReplicaSet or child change")
		return
	}
	writeJSON(w, map[string]string{"result": "deleted"})
}
