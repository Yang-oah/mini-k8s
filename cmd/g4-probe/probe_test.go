package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProbeOnceRejectsWrongIdentity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = bufio.NewReader(conn).ReadString('\n')
		_, _ = fmt.Fprintln(conn, `{"identity":"wrong"}`)
	}()
	result := probeOnce(listener.Addr().String(), "wanted", time.Second, 1)
	if result.Error == "" || result.Identity != "wrong" || result.Local == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestSnapshotOmitsPodEnvironment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/mini/v1/nodes":
			_, _ = fmt.Fprint(w, `{"items":[{"metadata":{"name":"n1","uid":"node-uid"},"spec":{"allocatable":{"cpuMilli":1000,"memoryBytes":1024}},"status":{"ready":true}}]}`)
		case "/api/mini/v1/pods":
			_, _ = fmt.Fprint(w, `{"items":[{"metadata":{"name":"p1","uid":"pod-uid"},"nodeUID":"node-uid","spec":{"env":{"SECRET":"do-not-log"},"resources":{"requests":{"cpuMilli":100,"memoryBytes":64}}},"status":{"phase":"Pending"}}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	result := snapshot(&apiClient{base: server.URL, token: "secret-token", http: server.Client()})
	if len(result.Errors) != 0 || len(result.Nodes) != 1 || len(result.Pods) != 1 || result.Pods[0].Requests.CPUMilli != 100 {
		t.Fatalf("unexpected snapshot: %+v", result)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "do-not-log") || strings.Contains(string(encoded), "secret-token") {
		t.Fatal("snapshot leaked secret")
	}
}
