package agent

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"mini-k8s/internal/api"
	containerruntime "mini-k8s/internal/runtime"
)

const readinessInterval = 2 * time.Second
const readinessTimeout = time.Second

// 每次探测属于一个具体容器与节点会话；旧结果不能应用于重建后的实例。
type readinessIdentity struct {
	containerID string
	ip          string
	port        int
	epoch       uint64
}

type readinessState struct {
	identity readinessIdentity
	ready    bool
	failures int
	cancel   context.CancelFunc
}

// setReadinessEpoch 只接受控制面最近一次返回的健康节点会话。
func (a *Agent) setReadinessEpoch(ready bool, epoch uint64) {
	if !ready {
		epoch = 0
	}
	a.readinessMu.Lock()
	defer a.readinessMu.Unlock()
	if a.readinessEpoch == epoch {
		return
	}
	a.clearReadinessLocked()
	a.readinessEpoch = epoch
}

// 控制连接失败时丢弃所有探测结果，恢复后必须重新读取会话并探测。
func (a *Agent) invalidateReadiness() {
	a.readinessMu.Lock()
	defer a.readinessMu.Unlock()
	a.clearReadinessLocked()
	a.readinessEpoch = 0
}

func (a *Agent) clearReadinessLocked() {
	for uid, state := range a.readiness {
		state.cancel()
		delete(a.readiness, uid)
	}
}

func (a *Agent) forgetReadiness(uid string) {
	a.readinessMu.Lock()
	defer a.readinessMu.Unlock()
	if state := a.readiness[uid]; state != nil {
		state.cancel()
		delete(a.readiness, uid)
	}
}

func (a *Agent) pruneReadiness(assigned map[string]bool) {
	a.readinessMu.Lock()
	defer a.readinessMu.Unlock()
	for uid, state := range a.readiness {
		if !assigned[uid] {
			state.cancel()
			delete(a.readiness, uid)
		}
	}
}

// 缺少探针时保持原有行为：Pod 可 Running，但不会被认作 Ready。
func (a *Agent) observedReady(ctx context.Context, pod api.Pod, observed containerruntime.Observation) (bool, uint64) {
	uid := pod.Metadata.UID
	if !observed.Running || observed.ContainerID == "" || net.ParseIP(observed.PodIP) == nil || pod.Spec.Readiness == nil || pod.Spec.Readiness.Port < 1 || pod.Spec.Readiness.Port > 65535 {
		a.forgetReadiness(uid)
		return false, 0
	}
	a.readinessMu.Lock()
	defer a.readinessMu.Unlock()
	if a.readinessEpoch == 0 {
		return false, 0
	}
	identity := readinessIdentity{containerID: observed.ContainerID, ip: observed.PodIP, port: pod.Spec.Readiness.Port, epoch: a.readinessEpoch}
	state := a.readiness[uid]
	if state == nil || state.identity != identity {
		if state != nil {
			state.cancel()
		}
		if a.readiness == nil {
			a.readiness = make(map[string]*readinessState)
		}
		probeCtx, cancel := context.WithCancel(ctx)
		state = &readinessState{identity: identity, cancel: cancel}
		a.readiness[uid] = state
		go a.probeLoop(probeCtx, uid, state)
	}
	if !state.ready {
		return false, 0
	}
	return true, identity.epoch
}

// 不持运行时锁或 API 锁做网络连接，每个 Pod 独立探测，避免串行超时。
func (a *Agent) probeLoop(ctx context.Context, uid string, state *readinessState) {
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	address := net.JoinHostPort(state.identity.ip, strconv.Itoa(state.identity.port))
	for {
		probeCtx, cancel := context.WithTimeout(ctx, readinessTimeout)
		conn, err := (&net.Dialer{}).DialContext(probeCtx, "tcp", address)
		if conn != nil {
			_ = conn.Close()
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		a.recordProbe(uid, state, err == nil)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *Agent) recordProbe(uid string, state *readinessState, success bool) {
	a.readinessMu.Lock()
	if a.readiness[uid] != state || state.identity.epoch != a.readinessEpoch {
		a.readinessMu.Unlock()
		return
	}
	before := state.ready
	previousFailures := state.failures
	event := updateProbeState(state, success)
	changed := before != state.ready
	failures, ready := state.failures, state.ready
	a.readinessMu.Unlock()
	if event != "" {
		// 只记录转折事件，不泄露探测地址、容器配置或凭据。
		log.Printf("readiness uid=%s event=%s failures=%d previous_failures=%d ready=%t", uid, event, failures, previousFailures, ready)
	}
	if changed {
		// 状态变化后重新读取 Pod 和 CRI，不能直接使用异步探测的旧快照上报。
		select {
		case a.readinessWake <- struct{}{}:
		default:
		}
	}
}

// 连续失败只在第一次和达到阈值时记录；恢复后重置计数。
func updateProbeState(state *readinessState, success bool) string {
	if success {
		previousFailures, wasReady := state.failures, state.ready
		state.failures = 0
		state.ready = true
		if previousFailures > 0 {
			return "recovered"
		}
		if !wasReady {
			return "ready"
		}
		return ""
	}
	if state.failures >= 2 {
		return ""
	}
	state.failures++
	if state.failures == 1 {
		return "first_failure"
	}
	state.ready = false
	return "failure_threshold"
}

func (a *Agent) clearReady(ctx context.Context, pod api.Pod) error {
	if !pod.Status.Ready {
		return nil
	}
	status := pod.Status
	status.Ready = false
	status.ReadinessEpoch = 0
	return a.call(ctx, http.MethodPut, "pods/"+url.PathEscape(pod.Metadata.Name)+"/status", map[string]any{"resourceVersion": pod.Metadata.ResourceVersion, "status": status}, nil)
}
