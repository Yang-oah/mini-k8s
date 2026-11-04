// Package runtime 通过 CRI v1 管理本项目的容器与网络沙箱。
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	managedLabel = "mini-k8s.io/managed"
	podUIDLabel  = "mini-k8s.io/pod-uid"
)

var (
	// ErrUncertain 表示一次创建的结果尚无法确认，不允许再次创建。
	ErrUncertain = errors.New("CRI creation outcome is unconfirmed")
	validUID     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
)

// Observation 是特定 Pod UID 的本机 CRI 快照。查询失败不等于不存在。
type Observation struct {
	Exists      bool
	Running     bool
	Exited      bool
	PodIP       string
	PodName     string
	SandboxID   string
	ContainerID string
	ExitReason  string
	ExitCode    int32
	StartedAt   time.Time
	FinishedAt  time.Time
	Attempt     uint32
}

// Client 是本节点唯一的 CRI 执行者。调用方必须在退出时调用 Close。
type Client struct {
	conn     *grpc.ClientConn
	runtime  runtimev1.RuntimeServiceClient
	images   runtimev1.ImageServiceClient
	stateDir string
	logDir   string
	lock     *os.File
	mu       sync.Mutex
}

// New 连接本机 Unix socket 并取得 stateDir 的非阻塞进程锁。
// 同一节点的所有 Agent 必须使用相同的 stateDir。
func New(endpoint, stateDir, logDir string) (*Client, error) {
	if strings.HasPrefix(endpoint, "unix://") {
		endpoint = strings.TrimPrefix(endpoint, "unix://")
	}
	if !filepath.IsAbs(endpoint) || !filepath.IsAbs(stateDir) || !filepath.IsAbs(logDir) {
		return nil, errors.New("CRI socket, state and log paths must be absolute")
	}
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return nil, fmt.Errorf("create runtime state directory: %w", err)
	}
	if err := os.MkdirAll(logDir, 0750); err != nil {
		return nil, fmt.Errorf("create runtime log directory: %w", err)
	}
	lock, err := acquireLock(filepath.Join(stateDir, "agent.lock"))
	if err != nil {
		return nil, fmt.Errorf("acquire runtime lock: %w", err)
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", endpoint)
		}))
	if err != nil {
		_ = releaseLock(lock)
		return nil, fmt.Errorf("create CRI client: %w", err)
	}
	return &Client{conn: conn, runtime: runtimev1.NewRuntimeServiceClient(conn), images: runtimev1.NewImageServiceClient(conn), stateDir: stateDir, logDir: logDir, lock: lock}, nil
}

// Close 关闭 CRI 连接并释放节点互斥锁。
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var connErr, lockErr error
	if c.conn != nil {
		connErr = c.conn.Close()
		c.conn = nil
	}
	if c.lock != nil {
		lockErr = releaseLock(c.lock)
		c.lock = nil
	}
	return errors.Join(connErr, lockErr)
}

func labels(uid string) map[string]string {
	return map[string]string{managedLabel: "true", podUIDLabel: uid}
}

func logOperation(uid, action string, err error) {
	slog.Info("CRI operation", "podUID", uid, "action", action, "success", err == nil)
}
func matches(values map[string]string, uid string) bool {
	return values[managedLabel] == "true" && values[podUIDLabel] == uid
}
func checkUID(uid string) error {
	if !validUID.MatchString(uid) || uid == "." || uid == ".." {
		return errors.New("invalid Pod UID")
	}
	return nil
}
