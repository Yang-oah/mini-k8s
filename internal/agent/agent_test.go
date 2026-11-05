package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mini-k8s/internal/api"
	containerruntime "mini-k8s/internal/runtime"
)

type fakeRuntime struct {
	observed                          containerruntime.Observation
	ensuredResult                     *containerruntime.Observation
	inspectErr, ensureErr, cleanupErr error
	ensured, inspected, cleaned       []string
}

func (f *fakeRuntime) Inspect(_ context.Context, uid string) (containerruntime.Observation, error) {
	f.inspected = append(f.inspected, uid)
	return f.observed, f.inspectErr
}
func (f *fakeRuntime) Ensure(_ context.Context, pod api.Pod) (containerruntime.Observation, error) {
	f.ensured = append(f.ensured, pod.Metadata.UID)
	if f.ensuredResult != nil {
		return *f.ensuredResult, f.ensureErr
	}
	return f.observed, f.ensureErr
}
func (f *fakeRuntime) Cleanup(_ context.Context, uid string) error {
	f.cleaned = append(f.cleaned, uid)
	return f.cleanupErr
}
func (f *fakeRuntime) Logs(_ context.Context, _ string, _ int) (string, error) {
	return "test log", nil
}

type fakeAPI struct {
	node     api.Node
	pods     map[string]api.Pod
	statuses int
	cleanups []string
}

func (f *fakeAPI) RoundTrip(req *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	path := strings.TrimPrefix(req.URL.Path, apiPrefix)
	switch {
	case req.Method == http.MethodGet && path == "nodes/node-a":
		_ = json.NewEncoder(w).Encode(f.node)
	case req.Method == http.MethodGet && path == "pods":
		pods := make([]api.Pod, 0, len(f.pods))
		for _, p := range f.pods {
			pods = append(pods, p)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": pods})
	case req.Method == http.MethodGet && strings.HasPrefix(path, "pods/"):
		pod, ok := f.pods[strings.TrimPrefix(path, "pods/")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			break
		}
		_ = json.NewEncoder(w).Encode(pod)
	case req.Method == http.MethodPut && strings.HasSuffix(path, "/status"):
		var body struct {
			ResourceVersion int64         `json:"resourceVersion"`
			Status          api.PodStatus `json:"status"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			return nil, err
		}
		name := strings.TrimSuffix(strings.TrimPrefix(path, "pods/"), "/status")
		pod := f.pods[name]
		if body.ResourceVersion != pod.Metadata.ResourceVersion {
			w.WriteHeader(http.StatusConflict)
			break
		}
		pod.Status = body.Status
		pod.Metadata.ResourceVersion++
		f.pods[name] = pod
		f.statuses++
	case req.Method == http.MethodPost && strings.HasSuffix(path, "/cleanup"):
		var body struct {
			UID             string `json:"uid"`
			ResourceVersion int64  `json:"resourceVersion"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			return nil, err
		}
		name := strings.TrimSuffix(strings.TrimPrefix(path, "pods/"), "/cleanup")
		pod := f.pods[name]
		if body.UID != pod.Metadata.UID || body.ResourceVersion != pod.Metadata.ResourceVersion {
			w.WriteHeader(http.StatusConflict)
			break
		}
		f.cleanups = append(f.cleanups, body.UID)
		delete(f.pods, name)
	default:
		return nil, fmt.Errorf("unexpected request %s %s", req.Method, path)
	}
	return w.Result(), nil
}

func testAgent(t *testing.T, runtime Runtime, apiClient *fakeAPI) *Agent {
	t.Helper()
	return &Agent{base: "https://api.example", token: "agent", node: "node-a", client: &http.Client{Transport: apiClient}, runtime: runtime, resync: time.Second, retries: make(map[string]retryState), statePath: filepath.Join(t.TempDir(), "retry-state.json"), runtimeGate: make(chan struct{}, 1)}
}

func TestExitedWithoutFinishTimeBacksOffAndPersists(t *testing.T) {
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 1}, NodeUID: "node-uid"}
	apiClient := &fakeAPI{pods: map[string]api.Pod{"web": pod}}
	runtime := &fakeRuntime{observed: containerruntime.Observation{Exists: true, Exited: true, ContainerID: "old"}}
	a := testAgent(t, runtime, apiClient)
	if err := a.reconcilePod(context.Background(), "node-uid", pod); err != nil {
		t.Fatal(err)
	}
	if len(runtime.ensured) != 0 || a.retries[pod.Metadata.UID].failures != 1 || !a.retries[pod.Metadata.UID].next.After(time.Now()) {
		t.Fatal("missing finish time did not establish a backoff")
	}
	restarted := testAgent(t, runtime, apiClient)
	restarted.statePath = a.statePath
	if err := restarted.loadRetries(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcilePod(context.Background(), "node-uid", apiClient.pods["web"]); err != nil {
		t.Fatal(err)
	}
	if len(runtime.ensured) != 0 || restarted.retries[pod.Metadata.UID].failures != 1 {
		t.Fatal("restart shortened pending backoff")
	}
}

func TestRetryPersistenceAndStableReset(t *testing.T) {
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 1}, NodeUID: "node-uid"}
	apiClient := &fakeAPI{pods: map[string]api.Pod{"web": pod}}
	runtime := &fakeRuntime{observed: containerruntime.Observation{Exists: true, Running: true, ContainerID: "current", StartedAt: time.Now().Add(-10 * time.Second)}}
	a := testAgent(t, runtime, apiClient)
	state := retryState{containerID: "old", failures: 4, next: time.Now().Add(8 * time.Second)}
	if err := a.saveRetry(pod.Metadata.UID, state); err != nil {
		t.Fatal(err)
	}
	restarted := testAgent(t, runtime, apiClient)
	restarted.statePath = a.statePath
	if err := restarted.loadRetries(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcilePod(context.Background(), "node-uid", pod); err != nil {
		t.Fatal(err)
	}
	if restarted.retries[pod.Metadata.UID].failures != 4 {
		t.Fatal("running for less than a minute reset backoff")
	}
	runtime.observed.StartedAt = time.Now().Add(-61 * time.Second)
	if err := restarted.reconcilePod(context.Background(), "node-uid", apiClient.pods["web"]); err != nil {
		t.Fatal(err)
	}
	reloaded := testAgent(t, runtime, apiClient)
	reloaded.statePath = a.statePath
	if err := reloaded.loadRetries(); err != nil {
		t.Fatal(err)
	}
	if _, ok := reloaded.retries[pod.Metadata.UID]; ok {
		t.Fatal("stable running did not persist backoff reset")
	}
}

func TestInterruptedAttemptGetsFullBackoffAfterRestart(t *testing.T) {
	a := testAgent(t, &fakeRuntime{}, &fakeAPI{})
	if err := a.saveRetry("pod-1", retryState{failures: 3, next: time.Now().Add(-time.Hour), inFlight: true}); err != nil {
		t.Fatal(err)
	}
	restarted := testAgent(t, &fakeRuntime{}, &fakeAPI{})
	restarted.statePath = a.statePath
	before := time.Now()
	if err := restarted.loadRetries(); err != nil {
		t.Fatal(err)
	}
	if restarted.retries["pod-1"].next.Before(before.Add(4 * time.Second)) {
		t.Fatal("interrupted attempt did not receive full backoff after restart")
	}
}

type blockingRuntime struct {
	block   string
	ensured int
}

func (r *blockingRuntime) Inspect(ctx context.Context, _ string) (containerruntime.Observation, error) {
	if r.block == "inspect" {
		<-ctx.Done()
		return containerruntime.Observation{}, ctx.Err()
	}
	return containerruntime.Observation{}, nil
}
func (r *blockingRuntime) Ensure(ctx context.Context, _ api.Pod) (containerruntime.Observation, error) {
	r.ensured++
	if r.block == "ensure" {
		<-ctx.Done()
		return containerruntime.Observation{}, ctx.Err()
	}
	return containerruntime.Observation{}, nil
}
func (r *blockingRuntime) Cleanup(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}
func (r *blockingRuntime) Logs(ctx context.Context, _ string, _ int) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

func TestRuntimeTimeoutDoesNotConfirmCleanup(t *testing.T) {
	now := time.Now()
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 1, DeletionTimestamp: &now}, NodeUID: "node-uid"}
	apiClient := &fakeAPI{pods: map[string]api.Pod{"web": pod}}
	runtime := &blockingRuntime{}
	a := testAgent(t, runtime, apiClient)
	a.runtimeTimeout = 15 * time.Millisecond
	if err := a.reconcilePod(context.Background(), "node-uid", pod); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup returned %v", err)
	}
	if len(apiClient.cleanups) != 0 {
		t.Fatal("timed-out cleanup released reservation")
	}
	if _, err := a.Logs(context.Background(), pod.Metadata.UID, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("logs returned %v", err)
	}
}

func TestRuntimeTimeoutKeepsPendingBackoff(t *testing.T) {
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 1}, NodeUID: "node-uid"}
	apiClient := &fakeAPI{pods: map[string]api.Pod{"web": pod}}
	runtime := &blockingRuntime{block: "ensure"}
	a := testAgent(t, runtime, apiClient)
	a.runtimeTimeout = 15 * time.Millisecond
	if err := a.reconcilePod(context.Background(), "node-uid", pod); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ensure returned %v", err)
	}
	restarted := testAgent(t, runtime, apiClient)
	restarted.statePath = a.statePath
	if err := restarted.loadRetries(); err != nil {
		t.Fatal(err)
	}
	if restarted.retries[pod.Metadata.UID].failures != 1 || !restarted.retries[pod.Metadata.UID].next.After(time.Now()) {
		t.Fatal("timed out attempt was not persisted")
	}
	if err := restarted.reconcilePod(context.Background(), "node-uid", apiClient.pods["web"]); err != nil {
		t.Fatal(err)
	}
	if runtime.ensured != 1 {
		t.Fatal("timed-out operation retried before backoff")
	}
}

func TestWaitingForRuntimeLockHasDeadline(t *testing.T) {
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 1}, NodeUID: "node-uid"}
	runtime := &fakeRuntime{}
	a := testAgent(t, runtime, &fakeAPI{pods: map[string]api.Pod{"web": pod}})
	a.runtimeTimeout = 15 * time.Millisecond
	a.runtimeGate <- struct{}{}
	defer func() { <-a.runtimeGate }()
	if err := a.reconcilePod(context.Background(), "node-uid", pod); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting for runtime returned %v", err)
	}
	if len(runtime.inspected) != 0 {
		t.Fatal("runtime called without acquiring gate")
	}
}

func TestReconcileAdoptsOnlyLocalUID(t *testing.T) {
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 3}, NodeUID: "node-uid", Status: api.PodStatus{Phase: "Pending"}}
	remote := api.Pod{Metadata: api.Metadata{Name: "remote", UID: "pod-2"}, NodeUID: "other-node"}
	apiClient := &fakeAPI{node: api.Node{Metadata: api.Metadata{UID: "node-uid"}}, pods: map[string]api.Pod{"web": pod, "remote": remote}}
	runtime := &fakeRuntime{observed: containerruntime.Observation{Exists: true, Running: true, ContainerID: "container-1", PodIP: "10.0.0.2", Attempt: 2}}
	a := testAgent(t, runtime, apiClient)
	if err := a.reconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.inspected) != 1 || runtime.inspected[0] != "pod-1" || len(runtime.ensured) != 0 {
		t.Fatalf("inspected=%v ensured=%v", runtime.inspected, runtime.ensured)
	}
	if got := apiClient.pods["web"].Status; got.Phase != "Running" || got.RestartCount != 2 || got.PodIP != "10.0.0.2" {
		t.Fatalf("status=%+v", got)
	}
}

func TestCleanupRequiresConfirmedRuntimeAndMatchingUID(t *testing.T) {
	now := time.Now()
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "old", ResourceVersion: 4, DeletionTimestamp: &now}, NodeUID: "node-uid"}
	apiClient := &fakeAPI{pods: map[string]api.Pod{"web": pod}}
	runtime := &fakeRuntime{cleanupErr: errors.New("cleanup uncertain")}
	a := testAgent(t, runtime, apiClient)
	if err := a.reconcilePod(context.Background(), "node-uid", pod); err == nil {
		t.Fatal("unconfirmed cleanup accepted")
	}
	if len(apiClient.cleanups) != 0 {
		t.Fatal("released reservation before runtime cleanup")
	}
	runtime.cleanupErr = nil
	if err := a.reconcilePod(context.Background(), "node-uid", pod); err != nil {
		t.Fatal(err)
	}
	if len(apiClient.cleanups) != 1 || apiClient.cleanups[0] != "old" {
		t.Fatalf("cleanup=%v", apiClient.cleanups)
	}
	apiClient.pods["web"] = api.Pod{Metadata: api.Metadata{Name: "web", UID: "new", ResourceVersion: 5}, NodeUID: "node-uid"}
	if err := a.reconcilePod(context.Background(), "node-uid", pod); err == nil {
		t.Fatal("late cleanup accepted for recreated Pod")
	}
	if len(runtime.cleaned) != 2 {
		t.Fatalf("late cleanup reached runtime: %v", runtime.cleaned)
	}
}

func TestInspectionFailureDoesNotStartOrClean(t *testing.T) {
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 3}, NodeUID: "node-uid"}
	runtime := &fakeRuntime{inspectErr: errors.New("runtime unavailable")}
	a := testAgent(t, runtime, &fakeAPI{pods: map[string]api.Pod{"web": pod}})
	if err := a.reconcilePod(context.Background(), "node-uid", pod); err == nil {
		t.Fatal("expected inspection failure")
	}
	if len(runtime.ensured) != 0 || len(runtime.cleaned) != 0 {
		t.Fatal("acted on unknown runtime state")
	}
}

func TestRestartReportsPreviousExitReason(t *testing.T) {
	oldFinish := time.Now().Add(-2 * time.Second)
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 3}, NodeUID: "node-uid", Status: api.PodStatus{Phase: "Running", Ready: true}}
	apiClient := &fakeAPI{pods: map[string]api.Pod{"web": pod}}
	next := containerruntime.Observation{Exists: true, Running: true, ContainerID: "new", PodIP: "10.0.0.2", Attempt: 1}
	runtime := &fakeRuntime{observed: containerruntime.Observation{Exists: true, Exited: true, ContainerID: "old", ExitReason: "Error", StartedAt: oldFinish.Add(-time.Minute), FinishedAt: oldFinish}, ensuredResult: &next}
	a := testAgent(t, runtime, apiClient)
	if err := a.reconcilePod(context.Background(), "node-uid", pod); err != nil {
		t.Fatal(err)
	}
	if got := apiClient.pods["web"].Status; got.Phase != "Running" || got.Reason != "Error" || got.RestartCount != 1 || got.Ready {
		t.Fatalf("restarted status = %+v", got)
	}
}

func TestExitReasonIsLoggedOnce(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	oldFinish := time.Now().Add(-2 * time.Second)
	pod := api.Pod{Metadata: api.Metadata{Name: "web", UID: "pod-1", ResourceVersion: 3}, NodeUID: "node-uid", Status: api.PodStatus{Phase: "Running", Ready: true}}
	next := containerruntime.Observation{Exists: true, Running: true, ContainerID: "new", PodIP: "10.0.0.2", Attempt: 1}
	runtime := &fakeRuntime{observed: containerruntime.Observation{Exists: true, Exited: true, ContainerID: "old", ExitReason: "OOMKilled", ExitCode: 137, Attempt: 0, StartedAt: oldFinish.Add(-time.Minute), FinishedAt: oldFinish}, ensuredResult: &next}
	apiClient := &fakeAPI{pods: map[string]api.Pod{"web": pod}}
	a := testAgent(t, runtime, apiClient)
	if err := a.reconcilePod(context.Background(), "node-uid", pod); err != nil {
		t.Fatal(err)
	}
	if err := a.reconcilePod(context.Background(), "node-uid", apiClient.pods["web"]); err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	if strings.Count(text, "exited") != 1 || !strings.Contains(text, "uid=pod-1") || !strings.Contains(text, "container=old") || !strings.Contains(text, "reason=\"OOMKilled\"") || !strings.Contains(text, "exitCode=137") {
		t.Fatalf("exit log = %q", text)
	}
}

func TestStaleCleanupRefusesUIDStillAssignedElsewhere(t *testing.T) {
	apiClient := &fakeAPI{pods: map[string]api.Pod{
		"replacement": {Metadata: api.Metadata{Name: "replacement", UID: "new"}},
		"other":       {Metadata: api.Metadata{Name: "other", UID: "still-running"}},
	}}
	runtime := &fakeRuntime{}
	a := testAgent(t, runtime, apiClient)
	if err := a.CleanupStale(context.Background(), "replacement", "still-running"); err == nil {
		t.Fatal("UID assigned to another Pod was accepted")
	}
	if len(runtime.cleaned) != 0 {
		t.Fatalf("unsafe cleanup: %v", runtime.cleaned)
	}
	if err := a.CleanupStale(context.Background(), "replacement", "old"); err != nil {
		t.Fatal(err)
	}
	if len(runtime.cleaned) != 1 || runtime.cleaned[0] != "old" {
		t.Fatalf("cleanup = %v", runtime.cleaned)
	}
}

func TestBackoffSchedule(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, expected := range want {
		if got := backoff(i + 1); got != expected {
			t.Fatalf("backoff(%d) = %v, want %v", i+1, got, expected)
		}
	}
}
