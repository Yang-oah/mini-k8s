package runtime

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestUIDLockSerializesSameUIDAndAllowsOtherUID(t *testing.T) {
	first, other := "pod-a", "pod-b"
	c := &Client{}
	unlock, err := c.lockPod(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.lockPod(ctx, first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same UID lock returned %v", err)
	}
	otherUnlock, err := c.lockPod(context.Background(), other)
	if err != nil {
		t.Fatalf("different UID lock failed: %v", err)
	}
	otherUnlock()
}

// Close 等待在途操作，但不能让新请求卡在等待关闭的互斥锁上。
func TestCloseRejectsNewOperationsAndWaitingUIDHonorsCancellation(t *testing.T) {
	c := &Client{}
	unlock, err := c.lockPod(context.Background(), "pod-a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	waitResult := make(chan error, 1)
	go func() {
		release, err := c.lockPod(waitCtx, "pod-a")
		if release != nil {
			release()
		}
		waitResult <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.podMu.Lock()
		gate := c.podGates["pod-a"]
		queued := gate != nil && gate.users == 2
		c.podMu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("UID waiter was not registered")
		}
		runtime.Gosched()
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	deadline = time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		closing := c.closed
		c.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not mark the client closed")
		}
		runtime.Gosched()
	}
	newCtx, cancelNew := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelNew()
	if _, err := c.lockPod(newCtx, "pod-b"); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("new operation did not immediately observe closed client: %v", err)
	}
	cancelWait()
	select {
	case err := <-waitResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued operation returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued operation did not honor cancellation")
	}
	select {
	case <-closed:
		t.Fatal("Close returned before the active operation completed")
	default:
	}
	unlock()
	unlock = nil
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after the active operation")
	}
}
