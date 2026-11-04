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
	conn      *grpc.ClientConn
	runtime   runtimev1.RuntimeServiceClient
	images    runtimev1.ImageServiceClient
	stateDir  string
	logDir    string
	lock      *os.File
	mu        sync.Mutex
	active    sync.WaitGroup
	closeDone chan struct{}
	closeErr  error
	podMu     sync.Mutex
	podGates  map[string]*podGate
	closed    bool
}

type podGate struct {
	lock  chan struct{}
	users int
}

// lockPod 使相同 UID 的生命周期操作串行，不同 UID 可并发；关闭客户端会等待已开始的操作。
func (c *Client) lockPod(ctx context.Context, uid string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("runtime client is closed")
	}
	// 关闭在持有同一锁时禁止新登记，因此 Wait 与 Add 不会竞争。
	c.active.Add(1)
	c.mu.Unlock()
	c.podMu.Lock()
	if c.podGates == nil {
		c.podGates = make(map[string]*podGate)
	}
	gate := c.podGates[uid]
	if gate == nil {
		gate = &podGate{lock: make(chan struct{}, 1)}
		c.podGates[uid] = gate
	}
	gate.users++
	c.podMu.Unlock()
	releaseReference := func() {
		c.podMu.Lock()
		gate.users--
		if gate.users == 0 {
			delete(c.podGates, uid)
		}
		c.podMu.Unlock()
		c.active.Done()
	}
	select {
	case gate.lock <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-gate.lock
			releaseReference()
			return nil, err
		}
		return func() { <-gate.lock; releaseReference() }, nil
	case <-ctx.Done():
		releaseReference()
		return nil, ctx.Err()
	}
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
	if c.closeDone != nil {
		done := c.closeDone
		c.mu.Unlock()
		<-done
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	c.closed = true
	c.closeDone = make(chan struct{})
	done := c.closeDone
	c.mu.Unlock()
	// 不持有生命周期锁等待在途请求；新调用可立即观察到 closed。
	c.active.Wait()
	var connErr, lockErr error
	if c.conn != nil {
		connErr = c.conn.Close()
		c.conn = nil
	}
	if c.lock != nil {
		lockErr = releaseLock(c.lock)
		c.lock = nil
	}
	err := errors.Join(connErr, lockErr)
	c.mu.Lock()
	c.closeErr = err
	close(done)
	c.mu.Unlock()
	return err
}

func labels(uid string) map[string]string {
	return map[string]string{managedLabel: "true", podUIDLabel: uid}
}

func logOperation(uid, action string, err error) {
	slog.Info("CRI operation", "podUID", uid, "action", action, "success", err == nil)
}
func logTimedOperation(uid, action string, started time.Time, err error) {
	slog.Info("CRI operation", "podUID", uid, "action", action, "success", err == nil, "durationMs", time.Since(started).Milliseconds())
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
