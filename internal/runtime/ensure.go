package runtime

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"mini-k8s/internal/api"

	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Linux CFS 允许的最大周期为 1 秒，使 1m 对应其最小 1000 微秒配额。
const cpuPeriod = int64(1000000)

func (c *Client) sandboxConfig(pod api.Pod) *runtimev1.PodSandboxConfig {
	return &runtimev1.PodSandboxConfig{
		Metadata:     &runtimev1.PodSandboxMetadata{Name: pod.Metadata.Name, Namespace: "default", Uid: pod.Metadata.UID},
		Hostname:     pod.Metadata.Name,
		LogDirectory: filepath.Join(c.logDir, pod.Metadata.UID),
		Labels:       labels(pod.Metadata.UID),
		Linux: &runtimev1.LinuxPodSandboxConfig{SecurityContext: &runtimev1.LinuxSandboxSecurityContext{
			NamespaceOptions: &runtimev1.NamespaceOption{Network: runtimev1.NamespaceMode_POD, Pid: runtimev1.NamespaceMode_CONTAINER, Ipc: runtimev1.NamespaceMode_POD},
		}},
	}
}

func containerConfig(pod api.Pod, attempt uint32) (*runtimev1.ContainerConfig, error) {
	limits := pod.Spec.Resources.Limits
	if limits.CPUMilli <= 0 || limits.CPUMilli > math.MaxInt64/1000 || limits.MemoryBytes <= 0 {
		return nil, fmt.Errorf("invalid container limits")
	}
	quota := limits.CPUMilli * 1000
	keys := make([]string, 0, len(pod.Spec.Env))
	for key := range pod.Spec.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	envs := make([]*runtimev1.KeyValue, 0, len(keys))
	for _, key := range keys {
		envs = append(envs, &runtimev1.KeyValue{Key: key, Value: pod.Spec.Env[key]})
	}
	return &runtimev1.ContainerConfig{
		Metadata: &runtimev1.ContainerMetadata{Name: "app", Attempt: attempt},
		Image:    &runtimev1.ImageSpec{Image: pod.Spec.Image}, Command: pod.Spec.Command, Args: pod.Spec.Args, Envs: envs,
		Labels: labels(pod.Metadata.UID), LogPath: fmt.Sprintf("app/%d.log", attempt),
		Linux: &runtimev1.LinuxContainerConfig{Resources: &runtimev1.LinuxContainerResources{CpuPeriod: cpuPeriod, CpuQuota: quota, MemoryLimitInBytes: limits.MemoryBytes}},
	}, nil
}

// Ensure 认领 UID 对应的实例，或在确认不存在时创建；退出后的新容器使用递增 attempt。
func (c *Client) Ensure(ctx context.Context, pod api.Pod) (Observation, error) {
	if err := checkUID(pod.Metadata.UID); err != nil {
		return Observation{}, err
	}
	if err := api.ValidatePod(pod); err != nil {
		return Observation{}, err
	}
	if _, err := containerConfig(pod, 0); err != nil {
		return Observation{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	uid := pod.Metadata.UID
	state, err := c.snapshot(ctx, uid)
	if err != nil {
		return Observation{}, err
	}
	if err := c.reconcileIntents(uid, state); err != nil {
		return Observation{}, err
	}
	config := c.sandboxConfig(pod)
	if state.sandboxStatus == nil {
		if err := os.MkdirAll(filepath.Join(config.LogDirectory, "app"), 0750); err != nil {
			return Observation{}, err
		}
		if err := c.beginIntent(uid, "sandbox", 0); err != nil {
			return Observation{}, err
		}
		_, err := c.runtime.RunPodSandbox(ctx, &runtimev1.RunPodSandboxRequest{Config: config})
		logOperation(uid, "run-sandbox", err)
		if err != nil {
			return Observation{}, fmt.Errorf("run sandbox: %w: %v", ErrUncertain, err)
		}
		state, err = c.snapshot(ctx, uid)
		if err != nil {
			return Observation{}, err
		}
		if err = c.reconcileIntents(uid, state); err != nil {
			return Observation{}, err
		}
	}
	if state.sandboxStatus == nil || state.sandboxStatus.State != runtimev1.PodSandboxState_SANDBOX_READY {
		return Observation{}, errorsForUID("sandbox is not ready")
	}
	if state.observation.Running {
		return state.observation, nil
	}
	if state.containerStatus != nil && state.containerStatus.State == runtimev1.ContainerState_CONTAINER_CREATED {
		_, err := c.runtime.StartContainer(ctx, &runtimev1.StartContainerRequest{ContainerId: state.containerStatus.Id})
		logOperation(uid, "start-container", err)
		if err != nil {
			return Observation{}, fmt.Errorf("start container: %w", err)
		}
		state, err = c.snapshot(ctx, uid)
		return state.observation, err
	}
	var attempt uint32
	if state.containerStatus != nil {
		if !state.observation.Exited {
			return Observation{}, errorsForUID("container is not stopped")
		}
		if state.observation.Attempt == math.MaxUint32 {
			return Observation{}, fmt.Errorf("container attempt overflow")
		}
		attempt = state.observation.Attempt + 1
	}
	image := &runtimev1.ImageSpec{Image: pod.Spec.Image}
	imageStatus, err := c.images.ImageStatus(ctx, &runtimev1.ImageStatusRequest{Image: image})
	if err != nil {
		return Observation{}, fmt.Errorf("image status: %w", err)
	}
	if imageStatus.Image == nil {
		_, err = c.images.PullImage(ctx, &runtimev1.PullImageRequest{Image: image, SandboxConfig: config})
		logOperation(uid, "pull-image", err)
		if err != nil {
			return Observation{}, fmt.Errorf("pull image: %w", err)
		}
	}
	container, err := containerConfig(pod, attempt)
	if err != nil {
		return Observation{}, err
	}
	if err = c.beginIntent(uid, "container", attempt); err != nil {
		return Observation{}, err
	}
	_, err = c.runtime.CreateContainer(ctx, &runtimev1.CreateContainerRequest{PodSandboxId: state.sandboxStatus.Id, Config: container, SandboxConfig: config})
	logOperation(uid, "create-container", err)
	if err != nil {
		return Observation{}, fmt.Errorf("create container: %w: %v", ErrUncertain, err)
	}
	state, err = c.snapshot(ctx, uid)
	if err != nil {
		return Observation{}, err
	}
	if err = c.reconcileIntents(uid, state); err != nil {
		return Observation{}, err
	}
	if state.containerStatus == nil || state.containerStatus.Metadata.Attempt != attempt {
		return Observation{}, fmt.Errorf("container: %w", ErrUncertain)
	}
	_, err = c.runtime.StartContainer(ctx, &runtimev1.StartContainerRequest{ContainerId: state.containerStatus.Id})
	logOperation(uid, "start-container", err)
	if err != nil {
		return Observation{}, fmt.Errorf("start container: %w", err)
	}
	state, err = c.snapshot(ctx, uid)
	return state.observation, err
}
