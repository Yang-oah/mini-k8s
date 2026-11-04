package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxLogBytes = 4 << 20

// Logs 返回当前容器最近的 CRI 日志行，读取大小有界。
func (c *Client) Logs(ctx context.Context, uid string, tail int) (string, error) {
	if err := checkUID(uid); err != nil {
		return "", err
	}
	if tail < 1 || tail > 10000 {
		return "", errors.New("tail must be between 1 and 10000")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.snapshot(ctx, uid)
	if err != nil {
		return "", err
	}
	if err := c.reconcileIntents(uid, state); err != nil {
		return "", err
	}
	if state.containerStatus == nil {
		return "", os.ErrNotExist
	}
	// 日志路径由 UID 与 attempt 构造，不信任运行时返回的任意路径。
	root, err := filepath.EvalSymlinks(c.logDir)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, uid, "app", fmt.Sprintf("%d.log", state.observation.Attempt))
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("log path escapes configured root")
	}
	file, err := os.Open(resolved)
	if err != nil {
		return "", err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !stat.Mode().IsRegular() {
		return "", errors.New("log path is not a regular file")
	}
	offset := stat.Size() - maxLogBytes
	if offset < 0 {
		offset = 0
	}
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxLogBytes))
	if err != nil {
		return "", err
	}
	if offset > 0 {
		if index := strings.IndexByte(string(data), '\n'); index >= 0 {
			data = data[index+1:]
		} else {
			return "", nil
		}
	}
	lines := strings.SplitAfter(string(data), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > tail {
		lines = lines[len(lines)-tail:]
	}
	return strings.Join(lines, ""), nil
}
