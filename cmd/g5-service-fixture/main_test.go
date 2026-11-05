package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestConnectChecksEndpointIdentity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_, _ = bufio.NewReader(conn).ReadString('\n')
			_ = json.NewEncoder(conn).Encode(map[string]string{"identity": "pod-a"})
			_ = conn.Close()
		}
	}()
	correct := connect(listener.Addr().String(), map[string]bool{"pod-a": true}, time.Second, false, 1)
	if correct.Error != "" || correct.Identity != "pod-a" || correct.Outcome != "success" {
		t.Fatalf("expected valid identity, got %+v", correct)
	}
	wrong := connect(listener.Addr().String(), map[string]bool{"pod-b": true}, time.Second, false, 2)
	if wrong.Error == "" {
		t.Fatalf("wrong endpoint was accepted: %+v", wrong)
	}
}

func TestBusinessConnectionLogsResult(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	output, restore := captureStdout(t)
	fixture := &fixture{identity: "pod-a"}
	done := make(chan struct{})
	go func() {
		fixture.handleBusiness(server)
		close(done)
	}()
	if _, err := io.WriteString(client, "nope\n"); err != nil {
		t.Fatal(err)
	}
	<-done
	text := output()
	restore()
	if !strings.Contains(text, `"kind":"error"`) || !strings.Contains(text, "unexpected request") || strings.Contains(text, "nope") {
		t.Fatalf("unexpected request log = %s", text)
	}

	server, client = net.Pipe()
	defer server.Close()
	defer client.Close()
	output, restore = captureStdout(t)
	done = make(chan struct{})
	go func() {
		fixture.handleBusiness(server)
		close(done)
	}()
	client.Close()
	<-done
	text = output()
	restore()
	if strings.TrimSpace(text) != "" {
		t.Fatalf("empty readiness connection was logged: %s", text)
	}
}

func captureStdout(t *testing.T) (func() string, func()) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = write
	return func() string {
			_ = write.Close()
			data, err := io.ReadAll(read)
			if err != nil {
				t.Fatal(err)
			}
			return string(data)
		}, func() {
			os.Stdout = old
			_ = write.Close()
			_ = read.Close()
		}
}

func TestNoEndpointMustNotAcceptListeningSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	result := connect(listener.Addr().String(), nil, time.Second, true, 1)
	if result.Error == "" {
		t.Fatalf("live connection was accepted as no endpoint: %+v", result)
	}
}
