package scheduler

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mini-k8s/internal/api"
)

func node(name string, cpu, memory, reservedCPU, reservedMemory int64) api.Node {
	return api.Node{Metadata: api.Metadata{Name: name, UID: name + "-uid"},
		Spec:   api.NodeSpec{Allocatable: api.Resources{CPUMilli: cpu, MemoryBytes: memory}},
		Status: api.NodeStatus{Ready: true, Reserved: api.Resources{CPUMilli: reservedCPU, MemoryBytes: reservedMemory}}}
}

func TestChooseNodeUsesPostBindCPUAndTieBreak(t *testing.T) {
	req := api.Resources{CPUMilli: 100, MemoryBytes: 100}
	tests := []struct {
		name  string
		nodes []api.Node
		want  string
	}{
		{"post-bind ratio", []api.Node{node("a", 1000, 1000, 100, 0), node("b", 2000, 1000, 100, 0)}, "b"},
		{"name tie break", []api.Node{node("b", 1000, 1000, 100, 0), node("a", 1000, 1000, 100, 0)}, "a"},
		{"memory filters lower CPU", []api.Node{node("a", 1000, 1000, 0, 950), node("b", 1000, 1000, 200, 0)}, "b"},
		{"integer overflow", []api.Node{node("b", math.MaxInt64, 1000, math.MaxInt64/2, 0), node("a", math.MaxInt64-1, 1000, math.MaxInt64/2, 0)}, "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := chooseNode(req, tt.nodes)
			if got == nil || got.Metadata.Name != tt.want || reason != "" {
				t.Fatalf("got %v, reason %q; want %s", got, reason, tt.want)
			}
		})
	}
}

func TestChooseNodeExplainsPending(t *testing.T) {
	req := api.Resources{CPUMilli: 100, MemoryBytes: 100}
	offline := node("off", 1000, 1000, 0, 0)
	offline.Status.Ready = false
	unsched := node("blocked", 1000, 1000, 0, 0)
	unsched.Spec.Unschedulable = true
	tests := []struct {
		name   string
		nodes  []api.Node
		reason string
	}{
		{"no ready", []api.Node{offline, unsched}, "NoReadyNodes"},
		{"cpu", []api.Node{node("n", 1000, 1000, 950, 0)}, "InsufficientCPU"},
		{"memory", []api.Node{node("n", 1000, 1000, 0, 950)}, "InsufficientMemory"},
		{"both", []api.Node{node("n", 1000, 1000, 950, 950)}, "InsufficientCPUAndMemory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := chooseNode(req, tt.nodes)
			if got != nil || reason != tt.reason {
				t.Fatalf("got %v, reason %q; want %q", got, reason, tt.reason)
			}
		})
	}
}

// API 负责最终的版本检查和原子预留，调度器不得把冲突当作成功或擅自重写状态。
func TestReconcileUsesLatestNodesAndToleratesBindingConflict(t *testing.T) {
	pods := []api.Pod{
		{Metadata: api.Metadata{Name: "a", UID: "pod-a", ResourceVersion: 11}, Spec: api.PodSpec{Resources: api.ResourceRequirements{Requests: api.Resources{CPUMilli: 100, MemoryBytes: 100}}}, Status: api.PodStatus{Phase: "Pending"}},
		{Metadata: api.Metadata{Name: "b", UID: "pod-b", ResourceVersion: 12}, Spec: api.PodSpec{Resources: api.ResourceRequirements{Requests: api.Resources{CPUMilli: 100, MemoryBytes: 100}}}, Status: api.PodStatus{Phase: "Pending"}},
	}
	listCount := 0
	var bodies []map[string]any
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Error("missing scheduler identity")
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case prefix + "pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": pods})
		case prefix + "nodes":
			listCount++
			n := node("n", 100, 100, 0, 0)
			if listCount == 2 {
				n.Status.Reserved = n.Spec.Allocatable
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []api.Node{n}})
		case prefix + "pods/a/bind", prefix + "pods/b/scheduling":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			bodies = append(bodies, body)
			if r.URL.Path == prefix+"pods/a/bind" {
				w.WriteHeader(http.StatusConflict)
			}
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	s := &Scheduler{base: server.URL, token: "token", client: server.Client(), interval: time.Second}
	if err := s.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if listCount != 2 || len(bodies) != 2 {
		t.Fatalf("node lists=%d, requests=%d", listCount, len(bodies))
	}
	if bodies[0]["nodeUID"] != "n-uid" || bodies[0]["podUID"] != "pod-a" || bodies[0]["resourceVersion"] != float64(11) {
		t.Fatalf("bad binding: %#v", bodies[0])
	}
	if bodies[1]["uid"] != "pod-b" || bodies[1]["reason"] != "InsufficientCPUAndMemory" {
		t.Fatalf("bad scheduling status: %#v", bodies[1])
	}
}
