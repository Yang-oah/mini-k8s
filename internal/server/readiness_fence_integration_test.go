package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mini-k8s/internal/api"
)

// 心跳先于扫描器恢复时，旧探针也不得让工作负载重新 Ready。
func TestReadinessEpochFencesLateHeartbeat(t *testing.T) {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	nodeName, podName := "ready-node-"+suffix, "ready-pod-"+suffix
	s := testServer(t, nodeName)
	t.Cleanup(func() {
		for _, key := range []string{resourceKey("pods", podName), resourceKey("nodes", nodeName)} {
			if _, err := s.Client.Delete(context.Background(), key); err != nil {
				t.Errorf("delete isolated test key: %v", err)
			}
		}
	})
	response := apiRequest(t, s, "admin", http.MethodPost, "nodes", api.Node{
		APIVersion: api.Version, Kind: "Node", Metadata: api.Metadata{Name: nodeName},
		Spec: api.NodeSpec{PodCIDR: "10.29.0.0/24", Allocatable: api.Resources{CPUMilli: 1000, MemoryBytes: 1024}},
	})
	requireStatus(t, response, http.StatusCreated)
	node := decodeResponse[api.Node](t, response)
	heartbeat := func(n api.Node) api.Node {
		t.Helper()
		response := apiRequest(t, s, "agent", http.MethodPut, "nodes/"+nodeName+"/status", map[string]any{
			"resourceVersion": n.Metadata.ResourceVersion,
			"status":          api.NodeStatus{Ready: true, Reserved: n.Status.Reserved, ReadinessEpoch: ^uint64(0)},
		})
		requireStatus(t, response, http.StatusOK)
		return decodeResponse[api.Node](t, response)
	}
	node = heartbeat(node)
	if node.Status.ReadinessEpoch != 1 {
		t.Fatalf("server did not initialize its own epoch: %d", node.Status.ReadinessEpoch)
	}
	spec := testPodSpec()
	spec.Readiness = &api.TCPProbe{Port: 8080}
	response = apiRequest(t, s, "admin", http.MethodPost, "pods", api.Pod{APIVersion: api.Version, Kind: "Pod", Metadata: api.Metadata{Name: podName}, Spec: spec})
	requireStatus(t, response, http.StatusCreated)
	pod := decodeResponse[api.Pod](t, response)
	response = apiRequest(t, s, "scheduler", http.MethodPost, "pods/"+podName+"/bind", map[string]any{
		"nodeName": nodeName, "nodeUID": node.Metadata.UID, "podUID": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion,
	})
	requireStatus(t, response, http.StatusOK)
	pod = decodeResponse[api.Pod](t, response)
	proof := api.PodStatus{Phase: "Running", Ready: true, PodIP: "10.29.0.2", ReadinessEpoch: node.Status.ReadinessEpoch}
	writeProof := func(p api.Pod, st api.PodStatus) *httptest.ResponseRecorder {
		t.Helper()
		return apiRequest(t, s, "agent", http.MethodPut, "pods/"+podName+"/status", map[string]any{"resourceVersion": p.Metadata.ResourceVersion, "status": st})
	}
	response = writeProof(pod, proof)
	requireStatus(t, response, http.StatusOK)
	pod = decodeResponse[api.Pod](t, response)
	oldEpoch := proof.ReadinessEpoch

	// 模拟监控尚未扫描的失联节点，直接发送迟到心跳。
	node = decodeResponse[api.Node](t, apiRequest(t, s, "admin", http.MethodGet, "nodes/"+nodeName, nil))
	node.Status.LastHeartbeat = time.Now().Add(-20 * time.Second)
	encoded, err := json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client.Put(context.Background(), resourceKey("nodes", nodeName), string(encoded)); err != nil {
		t.Fatal(err)
	}
	node = decodeResponse[api.Node](t, apiRequest(t, s, "agent", http.MethodGet, "nodes/"+nodeName, nil))
	node = heartbeat(node)
	if node.Status.ReadinessEpoch <= oldEpoch {
		t.Fatalf("late heartbeat reused readiness epoch %d", oldEpoch)
	}
	requireStatus(t, writeProof(pod, proof), http.StatusConflict)
	proof.ReadinessEpoch = node.Status.ReadinessEpoch
	response = writeProof(pod, proof)
	requireStatus(t, response, http.StatusOK)

	// 后台扫描先发生时也必须推进代次并持久化 Unknown。
	node.Status.LastHeartbeat = time.Now().Add(-20 * time.Second)
	encoded, err = json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client.Put(context.Background(), resourceKey("nodes", nodeName), string(encoded)); err != nil {
		t.Fatal(err)
	}
	s.expireNodeHeartbeats(context.Background())
	stored, err := s.Client.Get(context.Background(), resourceKey("nodes", nodeName))
	if err != nil || len(stored.Kvs) != 1 {
		t.Fatalf("read node: %v", err)
	}
	var expired api.Node
	if err := json.Unmarshal(stored.Kvs[0].Value, &expired); err != nil {
		t.Fatal(err)
	}
	if expired.Status.Condition != "Unknown" || expired.Status.ReadinessEpoch <= proof.ReadinessEpoch {
		t.Fatalf("monitor failed to fence proof: %+v", expired.Status)
	}
}
