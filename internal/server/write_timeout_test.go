package server

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestOnlyResourceCollectionWatchBypassesRequestTimeout(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		watch        bool
	}{
		{http.MethodGet, "/api/mini/v1/pods?watch=true", true},
		{http.MethodGet, "/api/mini/v1/nodes?watch=true", true},
		{http.MethodGet, "/api/mini/v1/service-pool?watch=true", false},
		{http.MethodGet, "/api/mini/v1/pods/name?watch=true", false},
		{http.MethodGet, "/api/mini/v1/pods", false},
		{http.MethodPost, "/api/mini/v1/pods?watch=true", false},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if got := isWatchRequest(req); got != tc.watch {
				t.Fatalf("watch=%t, want %t", got, tc.watch)
			}
		})
	}
}

// 模拟连接可建立但存储不响应；普通读写应留出时间将 503 返回给 5 秒客户端。
func TestRequestsReturnUnavailableBeforeClientTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				<-stop
				_ = conn.Close()
			}()
		}
	}()
	store, err := clientv3.New(clientv3.Config{Endpoints: []string{listener.Addr().String()}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := httptest.NewServer(&Server{Client: store, Credentials: Credentials{
		Admin: "admin", Scheduler: "scheduler", Controller: "controller", Agents: map[string]string{"node": "agent"},
	}})
	t.Cleanup(server.Close)

	for _, tc := range []struct {
		name, method, path, token, body string
	}{
		{"apply preflight read", http.MethodGet, "services/service", "admin", ""},
		{"create", http.MethodPost, "nodes", "admin", `{"apiVersion":"mini.k8s/v1","kind":"Node","metadata":{"name":"node"},"spec":{"allocatable":{"cpuMilli":1000,"memoryBytes":1024},"podCIDR":"10.20.0.0/24"}}`},
		{"bind", http.MethodPost, "pods/pod/bind", "scheduler", `{"nodeName":"node","nodeUID":"node-uid","podUID":"pod-uid","resourceVersion":1}`},
		{"scheduling", http.MethodPut, "pods/pod/scheduling", "scheduler", `{"uid":"pod-uid","resourceVersion":1,"reason":"NoReadyNodes"}`},
		{"status", http.MethodPut, "nodes/node/status", "agent", `{"resourceVersion":1,"status":{"ready":true}}`},
		{"cleanup", http.MethodPost, "pods/pod/cleanup", "agent", `{"uid":"pod-uid","resourceVersion":1}`},
		{"finalize", http.MethodPost, "replicasets/rs/finalize", "controller", `{"uid":"rs-uid","resourceVersion":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request, err := http.NewRequest(tc.method, server.URL+"/api/mini/v1/"+tc.path, bytes.NewBufferString(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+tc.token)
			request.Header.Set("Content-Type", "application/json")
			client := &http.Client{Timeout: 5 * time.Second}
			started := time.Now()
			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("client timed out before receiving 503: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("got HTTP %d, want 503", response.StatusCode)
			}
			if elapsed := time.Since(started); elapsed >= 5*time.Second {
				t.Fatalf("HTTP 503 arrived after client budget: %v", elapsed)
			}
		})
	}
}
