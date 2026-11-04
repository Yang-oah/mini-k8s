//go:build linux

package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type socketIdentity struct {
	Target  string `json:"target"`
	Pending string `json:"pending"`
	Device  uint64 `json:"device"`
	Inode   uint64 `json:"inode"`
}

func socketID(path string) (uint64, uint64, error) {
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return 0, 0, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return 0, 0, errors.New("existing path is not a Unix socket")
	}
	return uint64(st.Dev), st.Ino, nil
}

func syncSocketDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// removeOwnedSocket 同时核对持久化 inode 并拒绝移除仍可连接的服务。
func removeOwnedSocket(path string, owner socketIdentity) error {
	device, inode, err := socketID(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if device != owner.Device || inode != owner.Inode {
		return errors.New("socket identity does not match Agent ownership record")
	}
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return errors.New("owned control socket is still active")
	}
	if !errors.Is(err, unix.ECONNREFUSED) && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("cannot establish socket is inactive: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncSocketDir(filepath.Dir(path))
}

func recoverOwnedSocket(path, markerPath string) error {
	data, err := os.ReadFile(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		if _, err := os.Lstat(path); err == nil {
			return errors.New("existing control socket has no ownership record")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	var owner socketIdentity
	if err := json.Unmarshal(data, &owner); err != nil {
		return fmt.Errorf("invalid control socket ownership record: %w", err)
	}
	if owner.Target != path || filepath.Dir(owner.Pending) != filepath.Dir(path) || !strings.HasPrefix(filepath.Base(owner.Pending), ".mini-agent-") || owner.Inode == 0 {
		return errors.New("control socket ownership record does not match configuration")
	}
	if err := removeOwnedSocket(owner.Target, owner); err != nil {
		return err
	}
	if err := removeOwnedSocket(owner.Pending, owner); err != nil {
		return err
	}
	if err := os.Remove(markerPath); err != nil {
		return err
	}
	return syncSocketDir(filepath.Dir(markerPath))
}

// prepareControlSocket 先记录临时 socket 的 inode，再以不覆盖方式发布。
// 即使在发布前后崩溃，重启也只能清理明确属于该 Agent 的 inode。
func prepareControlSocket(path, stateDir string) (net.Listener, func(), error) {
	if !filepath.IsAbs(path) || !filepath.IsAbs(stateDir) {
		return nil, nil, errors.New("socket and state paths must be absolute")
	}
	markerPath := filepath.Join(stateDir, "control-socket-owner.json")
	if err := recoverOwnedSocket(path, markerPath); err != nil {
		return nil, nil, err
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, nil, err
	}
	pending := filepath.Join(filepath.Dir(path), ".mini-agent-"+hex.EncodeToString(random[:])+".sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: pending, Net: "unix"})
	if err != nil {
		return nil, nil, err
	}
	listener.SetUnlinkOnClose(false)
	var owner socketIdentity
	deferOnError := func() {
		_ = listener.Close()
		if owner.Inode != 0 {
			_ = removeOwnedSocket(pending, owner)
		}
	}
	device, inode, err := socketID(pending)
	if err != nil {
		deferOnError()
		return nil, nil, err
	}
	owner = socketIdentity{Target: path, Pending: pending, Device: device, Inode: inode}
	if err := os.Chmod(pending, 0600); err != nil {
		deferOnError()
		return nil, nil, err
	}
	marker, err := os.CreateTemp(stateDir, ".control-owner-*.tmp")
	if err != nil {
		deferOnError()
		return nil, nil, err
	}
	tmpMarker := marker.Name()
	defer os.Remove(tmpMarker)
	if err = marker.Chmod(0600); err == nil {
		err = json.NewEncoder(marker).Encode(owner)
	}
	if err == nil {
		err = marker.Sync()
	}
	closeErr := marker.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpMarker, markerPath)
	}
	if err == nil {
		err = syncSocketDir(stateDir)
	}
	if err != nil {
		deferOnError()
		return nil, nil, err
	}
	if err = unix.Renameat2(unix.AT_FDCWD, pending, unix.AT_FDCWD, path, unix.RENAME_NOREPLACE); err != nil {
		deferOnError()
		_ = os.Remove(markerPath)
		return nil, nil, err
	}
	if err = syncSocketDir(filepath.Dir(path)); err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	cleanup := func() {
		// 只清理仍与记录一致且不再活动的 socket；替换后的路径由其所有者处理。
		if removeOwnedSocket(path, owner) == nil && removeOwnedSocket(pending, owner) == nil {
			if os.Remove(markerPath) == nil {
				_ = syncSocketDir(stateDir)
			}
		}
	}
	return listener, cleanup, nil
}
