package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type bindRequest struct {
	NodeName        string `json:"nodeName"`
	NodeUID         string `json:"nodeUID"`
	PodUID          string `json:"podUID"`
	ResourceVersion int64  `json:"resourceVersion"`
}

func (s *Server) bind(w http.ResponseWriter, r *http.Request, name string, respondError errorResponder) {
	var req bindRequest
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil {
		respondError(400, "Invalid", err.Error())
		return
	}
	if req.NodeName == "" || req.NodeUID == "" || req.PodUID == "" || req.ResourceVersion <= 0 {
		respondError(400, "Invalid", "binding identifiers and version required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	for attempt := 0; attempt < 5; attempt++ {
		response, err := s.Client.Txn(ctx).Then(clientv3.OpGet(resourceKey("pods", name)), clientv3.OpGet(resourceKey("nodes", req.NodeName))).Commit()
		if err != nil {
			respondError(503, "Unavailable", "storage unavailable")
			return
		}
		pods, nodes := response.Responses[0].GetResponseRange().Kvs, response.Responses[1].GetResponseRange().Kvs
		if len(pods) == 0 || len(nodes) == 0 {
			respondError(404, "NotFound", "pod or node not found")
			return
		}
		po, err := decode(pods[0].Value, "pods", pods[0].ModRevision)
		if err != nil {
			respondError(503, "Unavailable", "invalid stored pod")
			return
		}
		no, err := decode(nodes[0].Value, "nodes", nodes[0].ModRevision)
		if err != nil {
			respondError(503, "Unavailable", "invalid stored node")
			return
		}
		p, n := po.value.(*api.Pod), no.value.(*api.Node)
		setAuditUID(ctx, p.Metadata.UID)
		if p.Metadata.UID != req.PodUID || n.Metadata.UID != req.NodeUID || p.Metadata.DeletionTimestamp != nil {
			respondError(409, "Conflict", "UID mismatch or deleting pod")
			return
		}
		// 相同目标的重复绑定返回现有结果，不再增加预留。
		if p.NodeUID == n.Metadata.UID {
			writeJSON(w, p)
			return
		}
		if p.NodeUID != "" || p.Metadata.ResourceVersion != req.ResourceVersion || !nodeAvailable(n, time.Now()) || n.Spec.Unschedulable {
			respondError(409, "Conflict", "pod or node cannot be bound")
			return
		}
		request := p.Spec.Resources.Requests
		reserved, capacity := n.Status.Reserved, n.Spec.Allocatable
		if request.CPUMilli > capacity.CPUMilli-reserved.CPUMilli || request.MemoryBytes > capacity.MemoryBytes-reserved.MemoryBytes {
			respondError(409, "Conflict", "insufficient resources")
			return
		}
		n.Status.Reserved.CPUMilli += request.CPUMilli
		n.Status.Reserved.MemoryBytes += request.MemoryBytes
		p.NodeUID = n.Metadata.UID
		p.Status.Reason = ""
		// 同时比较 Pod 和 Node 版本，并原子写入绑定与预留；冲突后重新读取。
		tx, err := s.Client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(resourceKey("pods", name)), "=", pods[0].ModRevision),
				clientv3.Compare(clientv3.ModRevision(resourceKey("nodes", req.NodeName)), "=", nodes[0].ModRevision)).
			Then(clientv3.OpPut(resourceKey("pods", name), encodeObject(po)),
				clientv3.OpPut(resourceKey("nodes", req.NodeName), encodeObject(no))).
			Commit()
		if err != nil {
			respondError(503, "Unavailable", "storage unavailable")
			return
		}
		if tx.Succeeded {
			p.Metadata.ResourceVersion = tx.Header.Revision
			writeJSON(w, p)
			return
		}
	}
	respondError(409, "Conflict", "concurrent binding conflict")
}

// schedulingStatus 只允许调度器更新尚未绑定的 Pending Pod 的调度原因。
func (s *Server) schedulingStatus(w http.ResponseWriter, r *http.Request, name string, respondError errorResponder) {
	var req struct {
		UID             string `json:"uid"`
		ResourceVersion int64  `json:"resourceVersion"`
		Reason          string `json:"reason"`
	}
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil {
		respondError(400, "Invalid", err.Error())
		return
	}
	switch req.Reason {
	case "InsufficientCPU", "InsufficientMemory", "InsufficientCPUAndMemory", "NoReadyNodes":
	default:
		respondError(400, "Invalid", "invalid scheduling reason")
		return
	}
	if req.UID == "" || req.ResourceVersion <= 0 {
		respondError(400, "Invalid", "UID and resourceVersion required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	key := resourceKey("pods", name)
	get, err := s.Client.Get(ctx, key)
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if len(get.Kvs) == 0 {
		respondError(404, "NotFound", "pod not found")
		return
	}
	o, err := decode(get.Kvs[0].Value, "pods", get.Kvs[0].ModRevision)
	if err != nil {
		respondError(503, "Unavailable", "invalid stored pod")
		return
	}
	p := o.value.(*api.Pod)
	if p.Metadata.UID != req.UID || p.Metadata.ResourceVersion != req.ResourceVersion || p.NodeUID != "" || p.Metadata.DeletionTimestamp != nil || p.Status.Phase != "Pending" {
		respondError(409, "Conflict", "pod is no longer eligible for scheduling status")
		return
	}
	if p.Status.Reason == req.Reason {
		writeJSON(w, p)
		return
	}
	p.Status.Reason = req.Reason
	tx, err := s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", req.ResourceVersion)).Then(clientv3.OpPut(key, encodeObject(o))).Commit()
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if !tx.Succeeded {
		respondError(409, "Conflict", "resourceVersion conflict")
		return
	}
	p.Metadata.ResourceVersion = tx.Header.Revision
	writeJSON(w, p)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, kind, name string, who identity, respondError errorResponder) {
	if kind != "pods" && kind != "nodes" {
		respondError(403, "Forbidden", "unsupported status")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
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
		respondError(503, "Unavailable", "invalid stored resource")
		return
	}
	allowed, err := s.agentCanRead(ctx, who, kind, old)
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if !allowed {
		respondError(403, "Forbidden", "resource not authorized")
		return
	}
	setAuditUID(ctx, old.meta.UID)
	var req struct {
		ResourceVersion int64           `json:"resourceVersion"`
		Status          json.RawMessage `json:"status"`
	}
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil {
		respondError(400, "Invalid", err.Error())
		return
	}
	if req.ResourceVersion != old.meta.ResourceVersion {
		respondError(409, "Conflict", "resourceVersion conflict")
		return
	}
	compares := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(resourceKey(kind, name)), "=", req.ResourceVersion)}
	switch v := old.value.(type) {
	case *api.Pod:
		var st api.PodStatus
		if err := api.DecodeStrict(strings.NewReader(string(req.Status)), &st); err != nil {
			respondError(400, "Invalid", err.Error())
			return
		}
		switch st.Phase {
		case "Pending", "Running", "Succeeded", "Failed", "Unknown":
		default:
			respondError(400, "Invalid", "unknown Pod phase")
			return
		}
		if st.RestartCount < v.Status.RestartCount || (st.Ready && st.Phase != "Running") {
			respondError(400, "Invalid", "invalid readiness or restart count")
			return
		}
		if (v.Status.Phase == "Succeeded" || v.Status.Phase == "Failed") && st.Phase != v.Status.Phase {
			respondError(409, "Conflict", "terminal Pod cannot change phase")
			return
		}
		if st.Ready {
			// 证明必须来自当前分配节点的当前就绪代次；与 Pod 一起比较节点版本，
			// 防止检查后节点失联或同名重建再提交旧探针结果。
			nodeKey := resourceKey("nodes", who.node)
			nodeResult, err := s.Client.Get(ctx, nodeKey)
			if err != nil {
				respondError(503, "Unavailable", "storage unavailable")
				return
			}
			if len(nodeResult.Kvs) != 1 {
				respondError(409, "Conflict", "assigned node no longer exists")
				return
			}
			nodeObject, err := decode(nodeResult.Kvs[0].Value, "nodes", nodeResult.Kvs[0].ModRevision)
			if err != nil {
				respondError(503, "Unavailable", "invalid stored node")
				return
			}
			node := nodeObject.value.(*api.Node)
			if v.Metadata.DeletionTimestamp != nil || v.NodeUID != node.Metadata.UID || !nodeAvailable(node, time.Now()) || node.Status.ReadinessEpoch == 0 || st.ReadinessEpoch != node.Status.ReadinessEpoch {
				respondError(409, "Conflict", "readiness proof is stale or node is unavailable")
				return
			}
			compares = append(compares, clientv3.Compare(clientv3.ModRevision(nodeKey), "=", nodeResult.Kvs[0].ModRevision))
		} else {
			// 非 Ready 状态不携带可被误用的旧证明，兼容旧 Agent。
			st.ReadinessEpoch = 0
		}
		v.Status = st
	case *api.Node:
		var st api.NodeStatus
		if err := api.DecodeStrict(strings.NewReader(string(req.Status)), &st); err != nil {
			respondError(400, "Invalid", err.Error())
			return
		}
		// 资源预留由绑定和清理事务维护，Agent 的状态上报不得修改。
		if st.Reserved != v.Status.Reserved {
			respondError(403, "Forbidden", "reservation is a system field")
			return
		}
		// 即使迟到心跳先于扫描器到达，也在同一次 CAS 中失效旧探针证明。
		now := time.Now().UTC()
		if v.Status.ReadinessEpoch == 0 || v.Status.Ready != st.Ready || (!nodeHeartbeatFresh(v, now) && v.Status.Condition != "Unknown") {
			if !advanceReadinessEpoch(v) {
				respondError(409, "Conflict", "node readiness epoch exhausted")
				return
			}
		}
		st.ReadinessEpoch = v.Status.ReadinessEpoch
		// 只信任控制面的接收时间，客户端时钟不能延长租期。
		st.LastHeartbeat = now
		if st.Ready {
			st.Condition = "Ready"
		} else {
			st.Condition = "NotReady"
		}
		v.Status = st
	}
	res, err := s.Client.Txn(ctx).If(compares...).Then(clientv3.OpPut(resourceKey(kind, name), encodeObject(old))).Commit()
	if err != nil {
		respondError(503, "Unavailable", "storage unavailable")
		return
	}
	if !res.Succeeded {
		respondError(409, "Conflict", "resourceVersion conflict")
		return
	}
	old.meta.ResourceVersion = res.Header.Revision
	writeJSON(w, old.value)
}

// cleanup 接收节点对本地实例已停止并清理的确认。
// Pod 删除与预留释放必须在同一事务中完成。
func (s *Server) cleanup(w http.ResponseWriter, r *http.Request, name string, who identity, respondError errorResponder) {
	var req struct {
		UID             string `json:"uid"`
		ResourceVersion int64  `json:"resourceVersion"`
	}
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil || req.UID == "" || req.ResourceVersion <= 0 {
		respondError(400, "Invalid", "uid and resourceVersion required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	for attempt := 0; attempt < 5; attempt++ {
		res, err := s.Client.Txn(ctx).Then(clientv3.OpGet(resourceKey("pods", name)), clientv3.OpGet(resourceKey("nodes", who.node))).Commit()
		if err != nil {
			respondError(503, "Unavailable", "storage unavailable")
			return
		}
		pods, nodes := res.Responses[0].GetResponseRange().Kvs, res.Responses[1].GetResponseRange().Kvs
		if len(pods) == 0 {
			writeJSON(w, map[string]string{"result": "already absent"})
			return
		}
		if len(nodes) == 0 {
			respondError(403, "Forbidden", "node not registered")
			return
		}
		po, err := decode(pods[0].Value, "pods", pods[0].ModRevision)
		if err != nil {
			respondError(503, "Unavailable", "invalid stored pod")
			return
		}
		no, err := decode(nodes[0].Value, "nodes", nodes[0].ModRevision)
		if err != nil {
			respondError(503, "Unavailable", "invalid stored node")
			return
		}
		p, n := po.value.(*api.Pod), no.value.(*api.Node)
		setAuditUID(ctx, p.Metadata.UID)
		if p.Metadata.UID != req.UID || p.Metadata.ResourceVersion != req.ResourceVersion || p.Metadata.DeletionTimestamp == nil || p.NodeUID != n.Metadata.UID {
			respondError(409, "Conflict", "pod identity, deletion or binding mismatch")
			return
		}
		requests := p.Spec.Resources.Requests
		if n.Status.Reserved.CPUMilli < requests.CPUMilli || n.Status.Reserved.MemoryBytes < requests.MemoryBytes {
			respondError(409, "Conflict", "invalid reservation")
			return
		}
		n.Status.Reserved.CPUMilli -= requests.CPUMilli
		n.Status.Reserved.MemoryBytes -= requests.MemoryBytes
		result, err := s.Client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(resourceKey("pods", name)), "=", pods[0].ModRevision),
				clientv3.Compare(clientv3.ModRevision(resourceKey("nodes", who.node)), "=", nodes[0].ModRevision)).
			Then(clientv3.OpDelete(resourceKey("pods", name)),
				clientv3.OpPut(resourceKey("nodes", who.node), encodeObject(no))).
			Commit()
		if err != nil {
			respondError(503, "Unavailable", "storage unavailable")
			return
		}
		if result.Succeeded {
			writeJSON(w, map[string]string{"result": "cleaned"})
			return
		}
	}
	respondError(409, "Conflict", "concurrent cleanup conflict")
}
