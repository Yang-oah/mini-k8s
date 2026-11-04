//go:build linux

package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestControlSocketRefusesUnknownService(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.sock")
	foreign, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	if _, _, err := prepareControlSocket(path, dir); err == nil {
		t.Fatal("accepted an unowned socket")
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("foreign service was unlinked: %v", err)
	}
	_ = conn.Close()
}

func TestControlSocketRecoversOwnStaleInode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.sock")
	listener, _, err := prepareControlSocket(path, dir)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟进程崩溃：关闭监听器，但不执行正常的路径清理。
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, cleanup, err := prepareControlSocket(path, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	cleanup()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket remains after clean shutdown: %v", err)
	}
}

func TestControlSocketRejectsReplacedInode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.sock")
	listener, _, err := prepareControlSocket(path, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	foreign, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	if _, _, err := prepareControlSocket(path, dir); err == nil {
		t.Fatal("accepted a replacement socket")
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("replacement service was unlinked: %v", err)
	}
	_ = conn.Close()
}
