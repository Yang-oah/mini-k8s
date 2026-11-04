package runtime

import (
	"context"
	"fmt"
	"time"

	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type snapshot struct {
	sandboxes       []*runtimev1.PodSandbox
	containers      []*runtimev1.Container
	sandboxStatus   *runtimev1.PodSandboxStatus
	containerStatus *runtimev1.ContainerStatus
	observation     Observation
}

func (c *Client) snapshot(ctx context.Context, uid string) (snapshot, error) {
	var result snapshot
	sandboxes, err := c.runtime.ListPodSandbox(ctx, &runtimev1.ListPodSandboxRequest{Filter: &runtimev1.PodSandboxFilter{LabelSelector: labels(uid)}})
	if err != nil {
		return result, fmt.Errorf("list sandboxes: %w", err)
	}
	containers, err := c.runtime.ListContainers(ctx, &runtimev1.ListContainersRequest{Filter: &runtimev1.ContainerFilter{LabelSelector: labels(uid)}})
	if err != nil {
		return result, fmt.Errorf("list containers: %w", err)
	}
	for _, sandbox := range sandboxes.Items {
		if sandbox == nil || !matches(sandbox.Labels, uid) || sandbox.Metadata == nil || sandbox.Metadata.Uid != uid {
			return result, errorsForUID("sandbox identity mismatch")
		}
		result.sandboxes = append(result.sandboxes, sandbox)
	}
	for _, container := range containers.Containers {
		if container == nil || !matches(container.Labels, uid) || container.Metadata == nil {
			return result, errorsForUID("container identity mismatch")
		}
		result.containers = append(result.containers, container)
	}
	if len(result.sandboxes) > 1 {
		return result, errorsForUID("multiple sandboxes found")
	}
	if len(result.sandboxes) == 0 && len(result.containers) != 0 {
		return result, errorsForUID("containers without sandbox")
	}
	result.observation.Exists = len(result.sandboxes) > 0 || len(result.containers) > 0
	if len(result.sandboxes) > 0 {
		sandbox := result.sandboxes[0]
		status, err := c.runtime.PodSandboxStatus(ctx, &runtimev1.PodSandboxStatusRequest{PodSandboxId: sandbox.Id})
		if err != nil {
			return result, fmt.Errorf("sandbox status: %w", err)
		}
		if status.Status == nil || status.Status.Id != sandbox.Id || !matches(status.Status.Labels, uid) || status.Status.Metadata == nil || status.Status.Metadata.Uid != uid || status.Status.Metadata.Name != sandbox.Metadata.Name {
			return result, errorsForUID("sandbox status identity mismatch")
		}
		result.sandboxStatus = status.Status
		result.observation.SandboxID = sandbox.Id
		result.observation.PodName = status.Status.Metadata.Name
		if status.Status.Network != nil {
			result.observation.PodIP = status.Status.Network.Ip
		}
	}
	var latest *runtimev1.Container
	var activeContainer *runtimev1.Container
	active := 0
	attempts := map[uint32]bool{}
	for _, container := range result.containers {
		if container.PodSandboxId != result.observation.SandboxID {
			return result, errorsForUID("container belongs to a different sandbox")
		}
		if attempts[container.Metadata.Attempt] {
			return result, errorsForUID("duplicate container attempt")
		}
		attempts[container.Metadata.Attempt] = true
		if container.State != runtimev1.ContainerState_CONTAINER_EXITED {
			active++
			activeContainer = container
		}
		if latest == nil || container.Metadata.Attempt > latest.Metadata.Attempt || (container.Metadata.Attempt == latest.Metadata.Attempt && container.CreatedAt > latest.CreatedAt) {
			latest = container
		}
	}
	if active > 1 {
		return result, errorsForUID("multiple active containers found")
	}
	if activeContainer != nil && activeContainer != latest {
		return result, errorsForUID("active container is not the latest attempt")
	}
	if latest != nil {
		status, err := c.runtime.ContainerStatus(ctx, &runtimev1.ContainerStatusRequest{ContainerId: latest.Id})
		if err != nil {
			return result, fmt.Errorf("container status: %w", err)
		}
		if status.Status == nil || status.Status.Id != latest.Id || !matches(status.Status.Labels, uid) || status.Status.Metadata == nil || status.Status.Metadata.Attempt != latest.Metadata.Attempt {
			return result, errorsForUID("container status identity mismatch")
		}
		if status.Status.State == runtimev1.ContainerState_CONTAINER_UNKNOWN {
			return result, errorsForUID("container state unknown")
		}
		result.containerStatus = status.Status
		result.observation.ContainerID = latest.Id
		result.observation.Running = status.Status.State == runtimev1.ContainerState_CONTAINER_RUNNING
		result.observation.Exited = status.Status.State == runtimev1.ContainerState_CONTAINER_EXITED
		result.observation.ExitReason = status.Status.Reason
		result.observation.ExitCode = status.Status.ExitCode
		result.observation.Attempt = status.Status.Metadata.Attempt
		if status.Status.StartedAt > 0 {
			result.observation.StartedAt = time.Unix(0, status.Status.StartedAt)
		}
		if status.Status.FinishedAt > 0 {
			result.observation.FinishedAt = time.Unix(0, status.Status.FinishedAt)
		}
	}
	return result, nil
}

func errorsForUID(message string) error { return fmt.Errorf("unsafe CRI state: %s", message) }

// reconcileIntents 只有发现对应实例时才解除待核实标记。
func (c *Client) reconcileIntents(uid string, state snapshot) error {
	sandboxIntent, err := c.readIntent(uid, "sandbox")
	if err != nil {
		return err
	}
	if sandboxIntent != nil {
		if state.sandboxStatus == nil || state.sandboxStatus.State != runtimev1.PodSandboxState_SANDBOX_READY {
			return fmt.Errorf("sandbox: %w", ErrUncertain)
		}
		if err := c.clearIntent(uid, "sandbox"); err != nil {
			return err
		}
	}
	return c.reconcileContainerIntent(uid, state)
}

// reconcileContainerIntent 只有按 UID 验证的 sandbox 中找到目标 attempt 才确认创建。
func (c *Client) reconcileContainerIntent(uid string, state snapshot) error {
	containerIntent, err := c.readIntent(uid, "container")
	if err != nil {
		return err
	}
	if containerIntent != nil {
		found := false
		for _, container := range state.containers {
			if container.Metadata.Attempt == containerIntent.Attempt {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("container: %w", ErrUncertain)
		}
		if err := c.clearIntent(uid, "container"); err != nil {
			return err
		}
	}
	return nil
}

// reconcileCleanupIntents 对已确认身份但尚未就绪的 sandbox 保留创建意图，
// 允许 Cleanup 清理它；只有全部资源确认消失后才能解除该意图。
func (c *Client) reconcileCleanupIntents(uid string, state snapshot) (bool, error) {
	intent, err := c.readIntent(uid, "sandbox")
	if err != nil {
		return false, err
	}
	if intent != nil && state.sandboxStatus != nil && state.sandboxStatus.State != runtimev1.PodSandboxState_SANDBOX_READY {
		if err := c.reconcileContainerIntent(uid, state); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, c.reconcileIntents(uid, state)
}

// Inspect 获取实例状态；CRI 查询失败或创建结果未知时返回错误。
func (c *Client) Inspect(ctx context.Context, uid string) (Observation, error) {
	if err := checkUID(uid); err != nil {
		return Observation{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.snapshot(ctx, uid)
	if err != nil {
		return Observation{}, err
	}
	if err := c.reconcileIntents(uid, state); err != nil {
		return Observation{}, err
	}
	return state.observation, nil
}
