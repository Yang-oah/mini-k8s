package server

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (s *Server) create(ctx context.Context, w http.ResponseWriter, r *http.Request, kind string, who identity, respondError errorResponder) {
	o, _ := newObject(kind)
	if err := decodeBody(r, o); err != nil {
		respondError(400, "Invalid", err.Error())
		return
	}
	if err := validateObject(o); err != nil {
		respondError(400, "Invalid", err.Error())
		return
	}
	if o.meta.UID != "" || o.meta.ResourceVersion != 0 || o.meta.DeletionTimestamp != nil ||
		(o.meta.OwnerUID != "" && (who.role != "controller" || kind != "pods")) {
		respondError(400, "Invalid", "system metadata must be empty")
		return
	}
	if who.role == "controller" && o.meta.OwnerUID == "" {
		respondError(400, "Invalid", "controller-created Pod requires ownerUID")
		return
	}
	switch v := o.value.(type) {
	case *api.Pod:
		if v.NodeUID != "" || !reflect.DeepEqual(v.Status, api.PodStatus{}) {
			respondError(403, "Forbidden", "binding and status are system fields")
			return
		}
		v.Status.Phase = "Pending"
	case *api.ReplicaSet:
		if v.Status != (api.ReplicaSetStatus{}) {
			respondError(403, "Forbidden", "status is a system field")
			return
		}
	case *api.Node:
		if !reflect.DeepEqual(v.Status, api.NodeStatus{}) {
			respondError(403, "Forbidden", "status is a system field")
			return
		}
	case *api.Service:
		if v.Spec.ClusterIP != "" {
			respondError(403, "Forbidden", "clusterIP is a system field")
			return
		}
	}
	o.meta.UID = newUID()
	if o.meta.Namespace == "" {
		o.meta.Namespace = "default"
	}
	k := resourceKey(kind, o.meta.Name)
	var tx *clientv3.TxnResponse
	var err error
	if rs, ok := o.value.(*api.ReplicaSet); ok {
		// 对选择器和标记取一致快照，再在事务中比较标记版本，
		// 防止并发创建时两个重叠的选择器同时通过检查。
		marker := prefix + "replicaset-selector-revision"
		for attempt := 0; attempt < 5; attempt++ {
			snapshot, snapshotErr := s.Client.Txn(ctx).Then(clientv3.OpGet(resourceKey("replicasets", ""), clientv3.WithPrefix()), clientv3.OpGet(marker)).Commit()
			if snapshotErr != nil {
				respondError(503, "Unavailable", "storage unavailable")
				return
			}
			for _, item := range snapshot.Responses[0].GetResponseRange().Kvs {
				existing, decodeErr := decode(item.Value, "replicasets", item.ModRevision)
				if decodeErr != nil {
					respondError(503, "Unavailable", "invalid stored ReplicaSet")
					return
				}
				if selectorsOverlap(existing.value.(*api.ReplicaSet).Spec.Selector, rs.Spec.Selector) {
					respondError(409, "Conflict", "ReplicaSet selectors overlap")
					return
				}
			}
			var markerRevision int64
			markerKVs := snapshot.Responses[1].GetResponseRange().Kvs
			if len(markerKVs) > 0 {
				markerRevision = markerKVs[0].ModRevision
			}
			tx, err = s.Client.Txn(ctx).If(clientv3.Compare(clientv3.Version(k), "=", 0), clientv3.Compare(clientv3.ModRevision(marker), "=", markerRevision)).Then(clientv3.OpPut(k, encodeObject(o)), clientv3.OpPut(marker, o.meta.UID)).Commit()
			if err != nil || tx.Succeeded {
				break
			}
		}
	} else if who.role == "controller" {
		tx, err = s.createOwnedPod(ctx, k, o)
	} else {
		tx, err = s.Client.Txn(ctx).If(clientv3.Compare(clientv3.Version(k), "=", 0)).Then(clientv3.OpPut(k, encodeObject(o))).Commit()
	}
	if err != nil {
		if errors.Is(err, errOwnerUnavailable) {
			respondError(409, "Conflict", "owner ReplicaSet missing or deleting")
			return
		}
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if tx == nil || !tx.Succeeded {
		respondError(409, "Conflict", "resource already exists or concurrent selector change")
		return
	}
	setAuditUID(ctx, o.meta.UID)
	o.meta.ResourceVersion = tx.Header.Revision
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, o.value)
}

func (s *Server) update(ctx context.Context, w http.ResponseWriter, r *http.Request, kind, name string, respondError errorResponder) {
	next, _ := newObject(kind)
	if err := decodeBody(r, next); err != nil {
		respondError(400, "Invalid", err.Error())
		return
	}
	if next.meta.Name != name || next.meta.ResourceVersion <= 0 {
		respondError(400, "Invalid", "name and resourceVersion required")
		return
	}
	get, err := s.Client.Get(ctx, resourceKey(kind, name))
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if len(get.Kvs) == 0 {
		respondError(404, "NotFound", "resource not found")
		return
	}
	old, err := decode(get.Kvs[0].Value, kind, get.Kvs[0].ModRevision)
	if err != nil {
		respondError(503, "Unavailable", "stored resource invalid")
		return
	}
	setAuditUID(ctx, old.meta.UID)
	if next.meta.ResourceVersion != old.meta.ResourceVersion {
		respondError(409, "Conflict", "resourceVersion conflict")
		return
	}
	if err := validateObject(next); err != nil {
		respondError(400, "Invalid", err.Error())
		return
	}
	if next.meta.UID != old.meta.UID || next.meta.OwnerUID != old.meta.OwnerUID || next.meta.Namespace != old.meta.Namespace || !reflect.DeepEqual(next.meta.DeletionTimestamp, old.meta.DeletionTimestamp) {
		respondError(400, "Invalid", "system metadata is immutable")
		return
	}
	switch v := next.value.(type) {
	case *api.Pod:
		prior := old.value.(*api.Pod)
		if !reflect.DeepEqual(v.Status, prior.Status) {
			respondError(403, "Forbidden", "status is a system field")
			return
		}
		if err := api.ValidatePodUpdate(*prior, *v); err != nil {
			respondError(400, "Invalid", err.Error())
			return
		}
	case *api.ReplicaSet:
		prior := old.value.(*api.ReplicaSet)
		if v.Status != prior.Status {
			respondError(403, "Forbidden", "status is a system field")
			return
		}
		if err := api.ValidateReplicaSetUpdate(*prior, *v); err != nil {
			respondError(400, "Invalid", err.Error())
			return
		}
		previousMetadata, newMetadata := prior.Metadata, v.Metadata
		previousMetadata.ResourceVersion, newMetadata.ResourceVersion = 0, 0
		if !reflect.DeepEqual(previousMetadata, newMetadata) {
			respondError(400, "Invalid", "ReplicaSet metadata is immutable")
			return
		}
	case *api.Node:
		prior := old.value.(*api.Node)
		projectNodeHealth(prior, time.Now())
		if !reflect.DeepEqual(v.Status, prior.Status) {
			respondError(403, "Forbidden", "status is a system field")
			return
		}
	case *api.Service:
		if v.Spec.ClusterIP != old.value.(*api.Service).Spec.ClusterIP {
			respondError(403, "Forbidden", "clusterIP is a system field")
			return
		}
	}
	// 校验后仍需在写入事务中比较版本，防止覆盖并发更新。
	cas, err := s.Client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(resourceKey(kind, name)), "=", old.meta.ResourceVersion)).
		Then(clientv3.OpPut(resourceKey(kind, name), encodeObject(next))).
		Commit()
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if !cas.Succeeded {
		respondError(409, "Conflict", "resourceVersion conflict")
		return
	}
	next.meta.ResourceVersion = cas.Header.Revision
	writeJSON(w, next.value)
}

func (s *Server) delete(ctx context.Context, w http.ResponseWriter, r *http.Request, kind, name string, who identity, respondError errorResponder) {
	k := resourceKey(kind, name)
	get, err := s.Client.Get(ctx, k)
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if len(get.Kvs) == 0 {
		respondError(404, "NotFound", "resource not found")
		return
	}
	old, err := decode(get.Kvs[0].Value, kind, get.Kvs[0].ModRevision)
	if err != nil {
		respondError(503, "Unavailable", "stored resource invalid")
		return
	}
	setAuditUID(ctx, old.meta.UID)
	if who.role == "controller" && (old.meta.OwnerUID == "" || r.URL.Query().Get("uid") == "" || r.URL.Query().Get("uid") != old.meta.UID) {
		respondError(403, "Forbidden", "controller requires matching owned Pod UID")
		return
	}
	if kind == "replicasets" {
		s.deleteReplicaSet(ctx, w, old, respondError)
		return
	}
	op := clientv3.OpDelete(k)
	// 已绑定 Pod 必须保留删除标记，直到对应节点确认清理并释放预留。
	if p, ok := old.value.(*api.Pod); ok && p.NodeUID != "" {
		if old.meta.DeletionTimestamp == nil {
			now := time.Now().UTC()
			old.meta.DeletionTimestamp = &now
		}
		op = clientv3.OpPut(k, encodeObject(old))
	}
	res, err := s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(k), "=", old.meta.ResourceVersion)).Then(op).Commit()
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if !res.Succeeded {
		respondError(409, "Conflict", "resourceVersion conflict")
		return
	}
	writeJSON(w, map[string]string{"result": "deleted"})
}
