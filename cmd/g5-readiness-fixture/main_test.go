package main

import (
	"net"
	"testing"
	"time"
)

func TestFixtureTogglesListeningWithoutReplacingProcess(t *testing.T) {
	f := &fixture{address: "127.0.0.1:0"}
	if got := f.set("on"); got.Error != "" || got.State != "on" || got.At.IsZero() {
		t.Fatalf("start: %+v", got)
	}
	address := f.listener.Addr().String()
	f.address = address
	defer func() { f.set("off") }()
	if c, err := net.DialTimeout("tcp", address, time.Second); err != nil {
		t.Fatalf("listener did not accept connections: %v", err)
	} else {
		_ = c.Close()
	}
	if got := f.set("off"); got.Error != "" || got.At.IsZero() {
		t.Fatalf("stop: %+v", got)
	}
	if c, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("closed listener accepted a connection")
	}
	if got := f.set("off"); got.Error != "" {
		t.Fatalf("repeated stop: %+v", got)
	}
	if got := f.set("on"); got.Error != "" {
		t.Fatalf("restart: %+v", got)
	}
	if c, err := net.DialTimeout("tcp", address, time.Second); err != nil {
		t.Fatalf("listener did not recover: %v", err)
	} else {
		_ = c.Close()
	}
}

func TestFixtureRejectsUnknownState(t *testing.T) {
	f := &fixture{address: "127.0.0.1:0"}
	if got := f.set("reset"); got.Error == "" || f.listener != nil {
		t.Fatalf("invalid state accepted: %+v", got)
	}
}
