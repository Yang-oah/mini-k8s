package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
)

type creationIntent struct {
	Attempt uint32 `json:"attempt"`
}

func (c *Client) intentPath(uid, kind string) string {
	return filepath.Join(c.stateDir, uid+"-"+kind+".json")
}

// 创建 RPC 前先持久化意图；崩溃或响应丢失后，没有证据时不能重复创建。
func (c *Client) beginIntent(uid, kind string, attempt uint32) error {
	path := c.intentPath(uid, kind)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		return ErrUncertain
	}
	if err != nil {
		return err
	}
	data, err := json.Marshal(creationIntent{Attempt: attempt})
	if err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("persist creation intent: %w", err)
	}
	if closeErr != nil {
		return closeErr
	}
	return syncDir(c.stateDir)
}

func (c *Client) readIntent(uid, kind string) (*creationIntent, error) {
	data, err := os.ReadFile(c.intentPath(uid, kind))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var intent creationIntent
	if err := json.Unmarshal(data, &intent); err != nil {
		return nil, fmt.Errorf("invalid creation intent: %w", err)
	}
	return &intent, nil
}

func (c *Client) clearIntent(uid, kind string) error {
	if err := os.Remove(c.intentPath(uid, kind)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(c.stateDir)
}

func syncDir(path string) error {
	// 生产 Agent 仅在 Linux 启动；Windows 单元测试无需目录 fsync。
	if goruntime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
