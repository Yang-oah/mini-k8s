package server

import (
	"context"
	"math"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 节点每 5 秒上报一次。18 秒判定失联，为一秒扫描周期及存储写入留出余量。
const nodeHeartbeatTimeout = 18 * time.Second

// bind 的整个存储事务最多允许 5 秒，留足余量避免提交时心跳已经过期。
const nodeBindingHeartbeatLimit = nodeHeartbeatTimeout - 5*time.Second

// advanceReadinessEpoch 在代次耗尽时保守拒绝恢复，避免旧证明因溢出重新有效。
func advanceReadinessEpoch(n *api.Node) bool {
	if n.Status.ReadinessEpoch == math.MaxUint64 {
		return false
	}
	n.Status.ReadinessEpoch++
	return true
}

func nodeHeartbeatFresh(n *api.Node, now time.Time) bool {
	return !n.Status.LastHeartbeat.IsZero() && !n.Status.LastHeartbeat.After(now) && now.Sub(n.Status.LastHeartbeat) < nodeHeartbeatTimeout
}

func nodeAvailable(n *api.Node, now time.Time) bool {
	return n.Status.Ready && nodeHeartbeatFresh(n, now) && now.Sub(n.Status.LastHeartbeat) < nodeBindingHeartbeatLimit
}

// projectNodeHealth 避免扫描延迟时读取到过期的 Ready；绑定也独立检查心跳。
func projectNodeHealth(n *api.Node, now time.Time) {
	if !nodeHeartbeatFresh(n, now) {
		n.Status.Ready = false
		n.Status.Condition = "Unknown"
	}
}

// 调度器视图还须遵守绑定的安全余量；13–18 秒之间节点对管理员仍然是 Ready，
// 但不能供调度器选择，避免反复撞到绑定端的心跳期限检查。
func projectNodeSchedulingHealth(n *api.Node, now time.Time) {
	projectNodeHealth(n, now)
	if n.Status.Ready && !nodeAvailable(n, now) {
		n.Status.Ready = false
		n.Status.Condition = "NotReady"
	}
}

// RunNodeHealthMonitor 主动将失联节点标为 Unknown。版本比较避免覆盖并发心跳或预留更新。
// 调用方负责绑定服务生命周期；存储异常时保持重试，不主动删除 Pod。
func (s *Server) RunNodeHealthMonitor(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		s.expireNodeHeartbeats(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) expireNodeHeartbeats(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	result, err := s.Client.Get(ctx, resourceKey("nodes", ""), clientv3.WithPrefix())
	if err != nil {
		return
	}
	now := time.Now()
	for _, kv := range result.Kvs {
		o, err := decode(kv.Value, "nodes", kv.ModRevision)
		if err != nil {
			continue
		}
		n := o.value.(*api.Node)
		if nodeHeartbeatFresh(n, now) || (!n.Status.Ready && n.Status.Condition == "Unknown") {
			continue
		}
		n.Status.Ready = false
		n.Status.Condition = "Unknown"
		// 即使代次耗尽也要将节点标为 Unknown；后续心跳不能恢复其就绪状态。
		advanceReadinessEpoch(n)
		_, _ = s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(string(kv.Key)), "=", kv.ModRevision)).Then(clientv3.OpPut(string(kv.Key), encodeObject(o))).Commit()
	}
}
