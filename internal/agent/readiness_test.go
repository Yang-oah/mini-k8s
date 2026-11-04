package agent

import (
	"context"
	"testing"

	"mini-k8s/internal/api"
	containerruntime "mini-k8s/internal/runtime"
)

func TestReadinessRequiresTwoConsecutiveFailures(t *testing.T) {
	state := &readinessState{identity: readinessIdentity{epoch: 3}, cancel: func() {}}
	a := &Agent{readiness: map[string]*readinessState{"pod": state}, readinessEpoch: 3, readinessWake: make(chan struct{}, 1)}
	a.recordProbe("pod", state, true)
	if !state.ready || state.failures != 0 {
		t.Fatal("first successful probe should make the current container ready")
	}
	a.recordProbe("pod", state, false)
	if !state.ready || state.failures != 1 {
		t.Fatal("one failed probe should not remove readiness")
	}
	a.recordProbe("pod", state, true)
	if !state.ready || state.failures != 0 {
		t.Fatal("success must reset the consecutive failure count")
	}
	a.recordProbe("pod", state, false)
	a.recordProbe("pod", state, false)
	if state.ready || state.failures != 2 {
		t.Fatal("two consecutive failures must remove readiness")
	}
}

func TestProbeTransitionLogsOnlyStateChanges(t *testing.T) {
	state := &readinessState{}
	for _, tc := range []struct {
		success  bool
		event    string
		failures int
		ready    bool
	}{
		{false, "first_failure", 1, false},
		{false, "failure_threshold", 2, false},
		{false, "", 2, false},
		{true, "recovered", 0, true},
		{true, "", 0, true},
		{false, "first_failure", 1, true},
		{true, "recovered", 0, true},
		{false, "first_failure", 1, true},
		{false, "failure_threshold", 2, false},
	} {
		if event := updateProbeState(state, tc.success); event != tc.event || state.failures != tc.failures || state.ready != tc.ready {
			t.Fatalf("success=%t event=%q failures=%d ready=%t; want %q %d %t", tc.success, event, state.failures, state.ready, tc.event, tc.failures, tc.ready)
		}
	}
}

func TestReadinessRejectsOldProbeAfterEpochOrContainerChange(t *testing.T) {
	old := &readinessState{identity: readinessIdentity{containerID: "old", ip: "10.0.0.1", port: 80, epoch: 1}, ready: true, cancel: func() {}}
	a := &Agent{readiness: map[string]*readinessState{"pod": old}, readinessEpoch: 1}
	a.setReadinessEpoch(true, 2)
	a.recordProbe("pod", old, true)
	if len(a.readiness) != 0 {
		t.Fatal("old session probe recreated state")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 不启动真实网络操作；验证新容器不能复用旧状态。
	pod := api.Pod{Metadata: api.Metadata{UID: "pod"}, Spec: api.PodSpec{Readiness: &api.TCPProbe{Port: 80}}}
	observation := containerruntime.Observation{Running: true, ContainerID: "new", PodIP: "10.0.0.1"}
	ready, epoch := a.observedReady(ctx, pod, observation)
	if ready || epoch != 0 {
		t.Fatal("new container must wait for a new successful probe")
	}
	current := a.readiness["pod"]
	a.recordProbe("pod", old, true)
	if current.ready {
		t.Fatal("late result for old container made the replacement ready")
	}
	a.recordProbe("pod", current, true)
	ready, epoch = a.observedReady(ctx, pod, observation)
	if !ready || epoch != 2 {
		t.Fatalf("current probe ready=%v epoch=%d, want true and 2", ready, epoch)
	}
	a.invalidateReadiness()
	if len(a.readiness) != 0 || a.readinessEpoch != 0 {
		t.Fatal("disconnection must discard all readiness proof")
	}
}
