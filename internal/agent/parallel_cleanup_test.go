package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mini-k8s/internal/api"
	containerruntime "mini-k8s/internal/runtime"
)

type blockingCleanupRuntime struct {
	entered   chan string
	release   chan struct{}
	inspected chan string
}

func (r *blockingCleanupRuntime) Ensure(context.Context, api.Pod) (containerruntime.Observation, error) {
	return containerruntime.Observation{}, nil
}
func (r *blockingCleanupRuntime) Inspect(_ context.Context, uid string) (containerruntime.Observation, error) {
	if r.inspected != nil {
		r.inspected <- uid
	}
	return containerruntime.Observation{Running: true, ContainerID: "running", PodIP: "10.1.0.2"}, nil
}

// 删除操作阻塞时，健康实例仍要继续检查，避免清理队列饿死 readiness。
func TestActivePodReconcilesWhileCleanupWaits(t *testing.T) {
	now := time.Now()
	deleting := api.Pod{Metadata: api.Metadata{Name: "deleted", UID: "old", ResourceVersion: 1, DeletionTimestamp: &now}, NodeUID: "node-uid"}
	active := api.Pod{Metadata: api.Metadata{Name: "active", UID: "live", ResourceVersion: 2}, NodeUID: "node-uid", Status: api.PodStatus{Phase: "Running", PodIP: "10.1.0.2"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, apiPrefix)
		switch {
		case r.Method == http.MethodGet && path == "nodes/node-a":
			_ = json.NewEncoder(w).Encode(api.Node{Metadata: api.Metadata{UID: "node-uid"}})
		case r.Method == http.MethodGet && path == "pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []api.Pod{deleting, active}})
		case r.Method == http.MethodGet && path == "pods/deleted":
			_ = json.NewEncoder(w).Encode(deleting)
		case r.Method == http.MethodPost && path == "pods/deleted/cleanup":
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	runtime := &blockingCleanupRuntime{entered: make(chan string, 1), release: make(chan struct{}), inspected: make(chan string, 1)}
	a := &Agent{base: server.URL, node: "node-a", client: server.Client(), runtime: runtime, retries: make(map[string]retryState), statePath: filepath.Join(t.TempDir(), "retry-state.json"), runtimeGate: make(chan struct{}, 8)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.reconcileAll(ctx) }()
	for _, signal := range []<-chan string{runtime.entered, runtime.inspected} {
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("active reconciliation was blocked by cleanup")
		}
	}
	close(runtime.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func (r *blockingCleanupRuntime) Cleanup(ctx context.Context, uid string) error {
	r.entered <- uid
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *blockingCleanupRuntime) Logs(context.Context, string, int) (string, error) {
	return "", nil
}

// 两个不同 UID 的真实清理必须同时开始，且确认完成之前不能通知 API 释放预留。
func TestReconcileCleansDistinctUIDsConcurrently(t *testing.T) {
	now := time.Now()
	pods := map[string]api.Pod{
		"a": {Metadata: api.Metadata{Name: "a", UID: "pod-a", ResourceVersion: 1, DeletionTimestamp: &now}, NodeUID: "node-uid"},
		"b": {Metadata: api.Metadata{Name: "b", UID: "pod-b", ResourceVersion: 2, DeletionTimestamp: &now}, NodeUID: "node-uid"},
	}
	var mu sync.Mutex
	releases := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, apiPrefix)
		switch {
		case r.Method == http.MethodGet && path == "nodes/node-a":
			_ = json.NewEncoder(w).Encode(api.Node{Metadata: api.Metadata{UID: "node-uid"}})
		case r.Method == http.MethodGet && path == "pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []api.Pod{pods["a"], pods["b"]}})
		case r.Method == http.MethodGet && strings.HasPrefix(path, "pods/"):
			_ = json.NewEncoder(w).Encode(pods[strings.TrimPrefix(path, "pods/")])
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/cleanup"):
			mu.Lock()
			releases++
			mu.Unlock()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	runtime := &blockingCleanupRuntime{entered: make(chan string, 2), release: make(chan struct{})}
	a := &Agent{base: server.URL, node: "node-a", client: server.Client(), runtime: runtime, retries: make(map[string]retryState), statePath: filepath.Join(t.TempDir(), "retry-state.json"), runtimeGate: make(chan struct{}, 8)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.reconcileAll(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-runtime.entered:
		case <-ctx.Done():
			t.Fatal("different UID cleanups did not run concurrently")
		}
	}
	mu.Lock()
	before := releases
	mu.Unlock()
	if before != 0 {
		t.Fatalf("released %d reservations before runtime cleanup finished", before)
	}
	close(runtime.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if releases != 2 {
		t.Fatalf("got %d cleanup confirmations, want 2", releases)
	}
}
