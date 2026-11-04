package main

import (
	"bufio"
	"encoding/json"
	"net"
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
