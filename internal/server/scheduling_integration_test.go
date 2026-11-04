package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"mini-k8s/internal/api"
)

func TestSchedulingReasonRequiresPendingVersionAndScheduler(t *testing.T) {
	name := fmt.Sprintf("g4-pending-%d", time.Now().UnixNano())
	s := testServer(t, "unused")
	created := apiRequest(t, s, "admin", http.MethodPost, "pods", api.Pod{
		APIVersion: api.Version, Kind: "Pod", Metadata: api.Metadata{Name: name}, Spec: testPodSpec(),
	})
	requireStatus(t, created, http.StatusCreated)
	t.Cleanup(func() {
		response := apiRequest(t, s, "admin", http.MethodDelete, "pods/"+name, nil)
		if response.Code != http.StatusOK && response.Code != http.StatusNotFound {
			t.Errorf("cleanup test pod: %s", response.Body.String())
		}
	})
	pod := decodeResponse[api.Pod](t, created)
	path := "pods/" + name + "/scheduling"
	body := map[string]any{"uid": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion, "reason": "InsufficientCPU"}
	requireStatus(t, apiRequest(t, s, "admin", http.MethodPut, path, body), http.StatusForbidden)
	requireStatus(t, apiRequest(t, s, "scheduler", http.MethodPut, path, body), http.StatusOK)
	// A stale version cannot overwrite a decision even if the Pod is still Pending.
	body["reason"] = "InsufficientMemory"
	requireStatus(t, apiRequest(t, s, "scheduler", http.MethodPut, path, body), http.StatusConflict)
	current := decodeResponse[api.Pod](t, apiRequest(t, s, "admin", http.MethodGet, "pods/"+name, nil))
	if current.Status.Phase != "Pending" || current.Status.Reason != "InsufficientCPU" || current.NodeUID != "" {
		t.Fatalf("scheduling request changed unrelated Pod fields: %+v", current)
	}
	body["resourceVersion"] = current.Metadata.ResourceVersion
	body["reason"] = "unexpected"
	requireStatus(t, apiRequest(t, s, "scheduler", http.MethodPut, path, body), http.StatusBadRequest)
}

func TestExpiredHeartbeatCannotBindAndMonitorPersistsUnknown(t *testing.T) {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	nodeName, podName := "g4-node-"+suffix, "g4-pod-"+suffix
	s := testServer(t, nodeName)
	nodeResponse := apiRequest(t, s, "admin", http.MethodPost, "nodes", api.Node{
		APIVersion: api.Version, Kind: "Node", Metadata: api.Metadata{Name: nodeName},
		Spec: api.NodeSpec{PodCIDR: "10.28.0.0/24", Allocatable: api.Resources{CPUMilli: 1000, MemoryBytes: 1024}},
	})
	requireStatus(t, nodeResponse, http.StatusCreated)
	node := decodeResponse[api.Node](t, nodeResponse)
	t.Cleanup(func() {
		for _, path := range []string{"pods/" + podName, "nodes/" + nodeName} {
			response := apiRequest(t, s, "admin", http.MethodDelete, path, nil)
			if response.Code != http.StatusOK && response.Code != http.StatusNotFound {
				t.Errorf("cleanup %s: %s", path, response.Body.String())
			}
		}
	})
	// 客户端时钟不能使已报告的心跳永久有效。
	future := time.Now().Add(24 * time.Hour)
	status := api.NodeStatus{Ready: true, LastHeartbeat: future}
	response := apiRequest(t, s, "agent", http.MethodPut, "nodes/"+nodeName+"/status", map[string]any{"resourceVersion": node.Metadata.ResourceVersion, "status": status})
	requireStatus(t, response, http.StatusOK)
	node = decodeResponse[api.Node](t, response)
	if node.Status.LastHeartbeat.After(time.Now().Add(time.Minute)) {
		t.Fatal("server trusted the client heartbeat timestamp")
	}
	// 安全余量窗口内，调度器不能选择节点；管理员仍观察到 Ready。
	node.Status.LastHeartbeat = time.Now().Add(-14 * time.Second)
	encoded, err := json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Client.Put(context.Background(), resourceKey("nodes", nodeName), string(encoded)); err != nil {
		t.Fatal(err)
	}
	adminView := decodeResponse[api.Node](t, apiRequest(t, s, "admin", http.MethodGet, "nodes/"+nodeName, nil))
	schedulerView := decodeResponse[api.Node](t, apiRequest(t, s, "scheduler", http.MethodGet, "nodes/"+nodeName, nil))
	if !adminView.Status.Ready || schedulerView.Status.Ready {
		t.Fatalf("unexpected node views: admin=%+v scheduler=%+v", adminView.Status, schedulerView.Status)
	}
	// 写入精确的过期状态以避免测试等待；保留节点身份和预留。
	node.Status.LastHeartbeat = time.Now().Add(-20 * time.Second)
	encoded, err = json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Client.Put(context.Background(), resourceKey("nodes", nodeName), string(encoded)); err != nil {
		t.Fatal(err)
	}
	podResponse := apiRequest(t, s, "admin", http.MethodPost, "pods", api.Pod{APIVersion: api.Version, Kind: "Pod", Metadata: api.Metadata{Name: podName}, Spec: testPodSpec()})
	requireStatus(t, podResponse, http.StatusCreated)
	pod := decodeResponse[api.Pod](t, podResponse)
	bind := map[string]any{"nodeName": nodeName, "nodeUID": node.Metadata.UID, "podUID": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion}
	requireStatus(t, apiRequest(t, s, "scheduler", http.MethodPost, "pods/"+podName+"/bind", bind), http.StatusConflict)
	s.expireNodeHeartbeats(context.Background())
	stored, err := s.Client.Get(context.Background(), resourceKey("nodes", nodeName))
	if err != nil || len(stored.Kvs) != 1 {
		t.Fatalf("read stored node: %v", err)
	}
	var persisted api.Node
	if err := json.Unmarshal(stored.Kvs[0].Value, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.Ready || persisted.Status.Condition != "Unknown" {
		t.Fatalf("monitor did not persist Unknown: %+v", persisted.Status)
	}
}
