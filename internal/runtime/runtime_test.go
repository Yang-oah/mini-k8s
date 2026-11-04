package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"testing"

	"mini-k8s/internal/api"

	"google.golang.org/grpc"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fakeCRI struct {
	runtimev1.RuntimeServiceClient
	sandbox                  *runtimev1.PodSandbox
	sandboxStatus            *runtimev1.PodSandboxStatus
	otherSandbox             *runtimev1.PodSandbox
	containers               []*runtimev1.Container
	statuses                 map[string]*runtimev1.ContainerStatus
	runCalls                 int
	createCalls              int
	stopCalls                int
	stoppedSandboxes         []string
	removedSandboxes         []string
	stopSandboxError         error
	removeSandboxError       error
	runError                 error
	commitSandboxBeforeError bool
}

func (f *fakeCRI) ListPodSandbox(_ context.Context, request *runtimev1.ListPodSandboxRequest, _ ...grpc.CallOption) (*runtimev1.ListPodSandboxResponse, error) {
	var result []*runtimev1.PodSandbox
	for _, sandbox := range []*runtimev1.PodSandbox{f.sandbox, f.otherSandbox} {
		if sandbox != nil && matches(sandbox.Labels, request.Filter.LabelSelector[podUIDLabel]) {
			result = append(result, sandbox)
		}
	}
	return &runtimev1.ListPodSandboxResponse{Items: result}, nil
}
func (f *fakeCRI) PodSandboxStatus(_ context.Context, _ *runtimev1.PodSandboxStatusRequest, _ ...grpc.CallOption) (*runtimev1.PodSandboxStatusResponse, error) {
	return &runtimev1.PodSandboxStatusResponse{Status: f.sandboxStatus}, nil
}
func (f *fakeCRI) ListContainers(_ context.Context, request *runtimev1.ListContainersRequest, _ ...grpc.CallOption) (*runtimev1.ListContainersResponse, error) {
	var found []*runtimev1.Container
	for _, container := range f.containers {
		if request.Filter.PodSandboxId != "" && container.PodSandboxId != request.Filter.PodSandboxId {
			continue
		}
		if request.Filter.LabelSelector != nil && !matches(container.Labels, request.Filter.LabelSelector[podUIDLabel]) {
			continue
		}
		found = append(found, container)
	}
	return &runtimev1.ListContainersResponse{Containers: found}, nil
}
func (f *fakeCRI) ContainerStatus(_ context.Context, request *runtimev1.ContainerStatusRequest, _ ...grpc.CallOption) (*runtimev1.ContainerStatusResponse, error) {
	return &runtimev1.ContainerStatusResponse{Status: f.statuses[request.ContainerId]}, nil
}
func (f *fakeCRI) RunPodSandbox(_ context.Context, request *runtimev1.RunPodSandboxRequest, _ ...grpc.CallOption) (*runtimev1.RunPodSandboxResponse, error) {
	f.runCalls++
	if f.runError == nil || f.commitSandboxBeforeError {
		f.sandbox = &runtimev1.PodSandbox{Id: "sandbox", Metadata: request.Config.Metadata, Labels: request.Config.Labels}
		f.sandboxStatus = &runtimev1.PodSandboxStatus{Id: "sandbox", Metadata: request.Config.Metadata, Labels: request.Config.Labels, State: runtimev1.PodSandboxState_SANDBOX_READY, Network: &runtimev1.PodSandboxNetworkStatus{Ip: "10.1.0.2"}}
	}
	if f.runError != nil {
		return nil, f.runError
	}
	return &runtimev1.RunPodSandboxResponse{PodSandboxId: "sandbox"}, nil
}
func (f *fakeCRI) CreateContainer(_ context.Context, request *runtimev1.CreateContainerRequest, _ ...grpc.CallOption) (*runtimev1.CreateContainerResponse, error) {
	f.createCalls++
	id := fmt.Sprintf("container-%d", request.Config.Metadata.Attempt)
	container := &runtimev1.Container{Id: id, PodSandboxId: request.PodSandboxId, Metadata: request.Config.Metadata, Labels: request.Config.Labels, State: runtimev1.ContainerState_CONTAINER_CREATED}
	f.containers = append(f.containers, container)
	f.statuses[id] = &runtimev1.ContainerStatus{Id: id, Metadata: container.Metadata, Labels: container.Labels, State: container.State}
	return &runtimev1.CreateContainerResponse{ContainerId: id}, nil
}
func (f *fakeCRI) StartContainer(_ context.Context, request *runtimev1.StartContainerRequest, _ ...grpc.CallOption) (*runtimev1.StartContainerResponse, error) {
	for _, container := range f.containers {
		if container.Id == request.ContainerId {
			container.State = runtimev1.ContainerState_CONTAINER_RUNNING
		}
	}
	f.statuses[request.ContainerId].State = runtimev1.ContainerState_CONTAINER_RUNNING
	return &runtimev1.StartContainerResponse{}, nil
}
func (f *fakeCRI) StopContainer(_ context.Context, _ *runtimev1.StopContainerRequest, _ ...grpc.CallOption) (*runtimev1.StopContainerResponse, error) {
	f.stopCalls++
	return &runtimev1.StopContainerResponse{}, nil
}
func (f *fakeCRI) StopPodSandbox(_ context.Context, request *runtimev1.StopPodSandboxRequest, _ ...grpc.CallOption) (*runtimev1.StopPodSandboxResponse, error) {
	f.stoppedSandboxes = append(f.stoppedSandboxes, request.PodSandboxId)
	if f.stopSandboxError != nil {
		return nil, f.stopSandboxError
	}
	return &runtimev1.StopPodSandboxResponse{}, nil
}
func (f *fakeCRI) RemovePodSandbox(_ context.Context, request *runtimev1.RemovePodSandboxRequest, _ ...grpc.CallOption) (*runtimev1.RemovePodSandboxResponse, error) {
	f.removedSandboxes = append(f.removedSandboxes, request.PodSandboxId)
	if f.sandbox != nil && f.sandbox.Id == request.PodSandboxId {
		f.sandbox, f.sandboxStatus = nil, nil
	}
	if f.otherSandbox != nil && f.otherSandbox.Id == request.PodSandboxId {
		f.otherSandbox = nil
	}
	if f.removeSandboxError != nil {
		return nil, f.removeSandboxError
	}
	return &runtimev1.RemovePodSandboxResponse{}, nil
}

type fakeImages struct{ runtimev1.ImageServiceClient }

func (fakeImages) ImageStatus(context.Context, *runtimev1.ImageStatusRequest, ...grpc.CallOption) (*runtimev1.ImageStatusResponse, error) {
	return &runtimev1.ImageStatusResponse{Image: &runtimev1.Image{Id: "image"}}, nil
}

func testRuntime(t *testing.T) (*Client, *fakeCRI) {
	t.Helper()
	dir := t.TempDir()
	fake := &fakeCRI{statuses: map[string]*runtimev1.ContainerStatus{}}
	return &Client{runtime: fake, images: fakeImages{}, stateDir: dir, logDir: dir}, fake
}

func testPod() api.Pod {
	return api.Pod{APIVersion: api.Version, Kind: "Pod", Metadata: api.Metadata{Name: "web", UID: "pod-uid"}, Spec: api.PodSpec{
		Image: "test-image", RestartPolicy: "Always", Resources: api.ResourceRequirements{Requests: api.Resources{CPUMilli: 50, MemoryBytes: 64}, Limits: api.Resources{CPUMilli: 100, MemoryBytes: 128}},
	}}
}

func TestContainerConfigUsesLimitsAndLabels(t *testing.T) {
	pod := testPod()
	pod.Spec.Env = map[string]string{"Z": "secret", "A": "value"}
	config, err := containerConfig(pod, 2)
	if err != nil {
		t.Fatal(err)
	}
	if config.Linux.Resources.CpuPeriod != 1000000 || config.Linux.Resources.CpuQuota != 100000 || config.Linux.Resources.MemoryLimitInBytes != 128 {
		t.Fatalf("incorrect resource limits: %+v", config.Linux.Resources)
	}
	if !matches(config.Labels, pod.Metadata.UID) || config.Metadata.Attempt != 2 || config.Envs[0].Key != "A" {
		t.Fatal("container identity or deterministic env order incorrect")
	}
	if config.Linux.SecurityContext != nil && config.Linux.SecurityContext.Privileged {
		t.Fatal("privileged container")
	}
	pod.Spec.Resources.Limits.CPUMilli = 1
	minimum, err := containerConfig(pod, 0)
	if err != nil || minimum.Linux.Resources.CpuQuota != 1000 {
		t.Fatalf("1m CPU limit rejected or misconfigured: %v", err)
	}
	pod.Spec.Resources.Limits.CPUMilli = int64(^uint64(0) >> 1)
	if _, err := containerConfig(pod, 0); err == nil {
		t.Fatal("overflowing CPU quota accepted")
	}
}

func TestEnsureRecoversLostSandboxResponseWithoutDuplicate(t *testing.T) {
	client, fake := testRuntime(t)
	fake.runError = errors.New("response lost")
	fake.commitSandboxBeforeError = true
	if _, err := client.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("expected uncertainty, got %v", err)
	}
	observation, err := client.Ensure(context.Background(), testPod())
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Running || observation.Attempt != 0 || fake.runCalls != 1 || fake.createCalls != 1 {
		t.Fatalf("unexpected observation: %+v, calls=%d/%d", observation, fake.runCalls, fake.createCalls)
	}
	if _, err := client.Ensure(context.Background(), testPod()); err != nil {
		t.Fatal(err)
	}
	if fake.createCalls != 1 {
		t.Fatal("repeated ensure created a second container")
	}
	// 重启后的新 client 也从 CRI 读取 attempt，而不是从内存计数。
	fake.containers[0].State = runtimev1.ContainerState_CONTAINER_EXITED
	fake.statuses["container-0"].State = runtimev1.ContainerState_CONTAINER_EXITED
	restarted := &Client{runtime: fake, images: fakeImages{}, stateDir: client.stateDir, logDir: client.logDir}
	observation, err = restarted.Ensure(context.Background(), testPod())
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Running || observation.Attempt != 1 || fake.createCalls != 2 {
		t.Fatalf("unexpected restart: %+v", observation)
	}
}

func TestUnknownCreationIsNotRetriedAfterRestart(t *testing.T) {
	client, fake := testRuntime(t)
	fake.runError = errors.New("response lost")
	if _, err := client.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("expected uncertainty, got %v", err)
	}
	restarted := &Client{runtime: fake, images: fakeImages{}, stateDir: client.stateDir, logDir: client.logDir}
	if _, err := restarted.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("expected uncertainty after restart, got %v", err)
	}
	if fake.runCalls != 1 {
		t.Fatalf("uncertain creation retried %d times", fake.runCalls)
	}
	if err := restarted.Cleanup(context.Background(), "pod-uid"); !errors.Is(err, ErrUncertain) {
		t.Fatalf("unsafe cleanup: %v", err)
	}
}

func TestCleanupKnownNotReadySandboxAfterLostResponse(t *testing.T) {
	client, fake := testRuntime(t)
	fake.runError = errors.New("response lost")
	fake.commitSandboxBeforeError = true
	if _, err := client.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("expected uncertain create: %v", err)
	}
	fake.sandboxStatus.State = runtimev1.PodSandboxState_SANDBOX_NOTREADY
	fake.otherSandbox = &runtimev1.PodSandbox{Id: "other-sandbox", Metadata: &runtimev1.PodSandboxMetadata{Name: "web", Uid: "other-uid"}, Labels: labels("other-uid")}
	restarted := &Client{runtime: fake, images: fakeImages{}, stateDir: client.stateDir, logDir: client.logDir}
	if _, err := restarted.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("not-ready create was retried or reported ready: %v", err)
	}
	if fake.runCalls != 1 {
		t.Fatalf("unexpected new sandbox creation: %d", fake.runCalls)
	}
	if err := restarted.Cleanup(context.Background(), "pod-uid"); err != nil {
		t.Fatalf("cleanup known sandbox: %v", err)
	}
	if len(fake.stoppedSandboxes) != 1 || fake.stoppedSandboxes[0] != "sandbox" || len(fake.removedSandboxes) != 1 || fake.removedSandboxes[0] != "sandbox" {
		t.Fatalf("wrong sandbox affected: stopped=%v removed=%v", fake.stoppedSandboxes, fake.removedSandboxes)
	}
	if fake.otherSandbox == nil {
		t.Fatal("same-name different UID sandbox was removed")
	}
	if intent, err := restarted.readIntent("pod-uid", "sandbox"); err != nil || intent != nil {
		t.Fatalf("intent not cleared after cleanup: %v, %v", intent, err)
	}
}

func TestCleanupRefusesNotReadySandboxWithUnknownContainerAttempt(t *testing.T) {
	client, fake := testRuntime(t)
	fake.runError = errors.New("response lost")
	fake.commitSandboxBeforeError = true
	if _, err := client.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("expected uncertain create: %v", err)
	}
	fake.sandboxStatus.State = runtimev1.PodSandboxState_SANDBOX_NOTREADY
	if err := client.beginIntent("pod-uid", "container", 1); err != nil {
		t.Fatal(err)
	}
	if err := client.Cleanup(context.Background(), "pod-uid"); !errors.Is(err, ErrUncertain) {
		t.Fatalf("unknown attempt must block cleanup: %v", err)
	}
	if len(fake.stoppedSandboxes) != 0 || fake.sandbox == nil {
		t.Fatal("sandbox was touched with an unknown container result")
	}
}

func TestCleanupNotReadyRejectsUnverifiedSandboxIdentity(t *testing.T) {
	client, fake := testRuntime(t)
	fake.runError, fake.commitSandboxBeforeError = errors.New("response lost"), true
	if _, err := client.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	fake.sandboxStatus.State = runtimev1.PodSandboxState_SANDBOX_NOTREADY
	fake.sandboxStatus.Labels = labels("another-uid")
	if err := client.Cleanup(context.Background(), "pod-uid"); err == nil {
		t.Fatal("sandbox with mismatched status labels accepted")
	}
	if len(fake.stoppedSandboxes) != 0 || fake.sandbox == nil {
		t.Fatal("unverified sandbox was modified")
	}
	if intent, err := client.readIntent("pod-uid", "sandbox"); err != nil || intent == nil {
		t.Fatalf("intent lost on identity mismatch: %v, %v", intent, err)
	}
}

func TestCleanupNotReadyRetriesAfterStopFailure(t *testing.T) {
	client, fake := testRuntime(t)
	fake.runError, fake.commitSandboxBeforeError = errors.New("response lost"), true
	if _, err := client.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	fake.sandboxStatus.State = runtimev1.PodSandboxState_SANDBOX_NOTREADY
	fake.stopSandboxError = errors.New("stop failed")
	if err := client.Cleanup(context.Background(), "pod-uid"); err == nil {
		t.Fatal("stop failure accepted")
	}
	if intent, err := client.readIntent("pod-uid", "sandbox"); err != nil || intent == nil {
		t.Fatalf("intent lost after stop failure: %v, %v", intent, err)
	}
	fake.stopSandboxError = nil
	if err := client.Cleanup(context.Background(), "pod-uid"); err != nil {
		t.Fatalf("retry cleanup: %v", err)
	}
	if fake.sandbox != nil {
		t.Fatal("sandbox remains after retry")
	}
}

func TestCleanupAbsentSandboxRetainsUnconfirmedIntent(t *testing.T) {
	client, fake := testRuntime(t)
	fake.runError, fake.commitSandboxBeforeError = errors.New("response lost"), true
	if _, err := client.Ensure(context.Background(), testPod()); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	fake.sandboxStatus.State = runtimev1.PodSandboxState_SANDBOX_NOTREADY
	fake.removeSandboxError = errors.New("remove response lost")
	if err := client.Cleanup(context.Background(), "pod-uid"); err == nil {
		t.Fatal("uncertain removal accepted")
	}
	if fake.sandbox != nil {
		t.Fatal("fake must remove sandbox before dropping response")
	}
	fake.removeSandboxError = nil
	if err := client.Cleanup(context.Background(), "pod-uid"); !errors.Is(err, ErrUncertain) {
		t.Fatalf("absent resource should retain uncertainty: %v", err)
	}
	if intent, err := client.readIntent("pod-uid", "sandbox"); err != nil || intent == nil {
		t.Fatalf("intent lost for unconfirmed sandbox: %v, %v", intent, err)
	}
}

func TestCleanupRejectsForeignContainer(t *testing.T) {
	client, fake := testRuntime(t)
	pod := testPod()
	config := client.sandboxConfig(pod)
	_, _ = fake.RunPodSandbox(context.Background(), &runtimev1.RunPodSandboxRequest{Config: config})
	fake.containers = []*runtimev1.Container{{Id: "foreign", PodSandboxId: "sandbox", Metadata: &runtimev1.ContainerMetadata{Name: "foreign"}, Labels: map[string]string{"other": "true"}}}
	if err := client.Cleanup(context.Background(), pod.Metadata.UID); err == nil {
		t.Fatal("cleanup accepted foreign container")
	}
	if fake.stopCalls != 0 {
		t.Fatal("foreign container was stopped")
	}
}

func TestLogsUsesUIDScopedPath(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("Windows 测试环境无法解析日志目录，正式日志测试使用 Linux")
	}
	client, fake := testRuntime(t)
	if _, err := client.Ensure(context.Background(), testPod()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(client.logDir, "pod-uid", "app", "0.log")
	if err := os.WriteFile(path, []byte("first\nsecond\nthird\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := client.Logs(context.Background(), "pod-uid", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got != "second\nthird\n" {
		t.Fatalf("unexpected logs %q", got)
	}
	if fake.createCalls != 1 {
		t.Fatal("log read changed runtime state")
	}
}
