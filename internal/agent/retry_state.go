package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	goruntime "runtime"
	"time"
)

type retryRecord struct {
	ContainerID string    `json:"containerID"`
	Failures    int       `json:"failures"`
	Next        time.Time `json:"next"`
	InFlight    bool      `json:"inFlight"`
}

func (a *Agent) retryFor(uid string) retryState {
	a.retryMu.Lock()
	defer a.retryMu.Unlock()
	return a.retries[uid]
}

// loadRetries 拒绝损坏的状态，避免静默重置仍需执行的退避。
func (a *Agent) loadRetries() error {
	file, err := os.Open(a.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open retry state: %w", err)
	}
	defer file.Close()
	var records map[string]retryRecord
	decoder := json.NewDecoder(io.LimitReader(file, 8<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&records); err != nil {
		return fmt.Errorf("decode retry state: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("invalid retry state trailing data")
	}
	for uid, record := range records {
		if uid == "" || record.Failures < 0 {
			return errors.New("invalid retry state entry")
		}
		state := retryState{containerID: record.ContainerID, failures: record.Failures, next: record.Next, inFlight: record.InFlight}
		if state.inFlight {
			// 上次调用是否提交不可知；进程重启后至少重新等待完整退避。
			deadline := time.Now().Add(backoff(state.failures))
			if state.next.Before(deadline) {
				state.next = deadline
			}
		}
		a.retries[uid] = state
	}
	return nil
}

// saveRetry 先原子持久化再发布内存状态；不同 UID 的并发清理共用此文件。
func (a *Agent) saveRetry(uid string, state retryState) error {
	a.retryMu.Lock()
	defer a.retryMu.Unlock()
	records := make(map[string]retryRecord, len(a.retries)+1)
	for key, value := range a.retries {
		if key != uid {
			records[key] = retryRecord{ContainerID: value.containerID, Failures: value.failures, Next: value.next, InFlight: value.inFlight}
		}
	}
	if state != (retryState{}) {
		records[uid] = retryRecord{ContainerID: state.containerID, Failures: state.failures, Next: state.next, InFlight: state.inFlight}
	}
	dir := filepath.Dir(a.statePath)
	file, err := os.CreateTemp(dir, ".retry-*.tmp")
	if err != nil {
		return fmt.Errorf("create retry state: %w", err)
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return err
	}
	if err := json.NewEncoder(file).Encode(records); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, a.statePath); err != nil {
		return fmt.Errorf("commit retry state: %w", err)
	}
	if goruntime.GOOS != "windows" {
		directory, err := os.Open(dir)
		if err != nil {
			return err
		}
		err = directory.Sync()
		_ = directory.Close()
		if err != nil {
			return err
		}
	}
	if state == (retryState{}) {
		delete(a.retries, uid)
	} else {
		a.retries[uid] = state
	}
	return nil
}
