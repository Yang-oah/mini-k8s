package runtime

import (
	"context"
	"fmt"
	"log/slog"

	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Cleanup 仅清理同时匹配项目标签和 UID 的实例；成功表示 CRI 已确认网络清理。
func (c *Client) Cleanup(ctx context.Context, uid string) (result error) {
	if err := checkUID(uid); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() { slog.Info("CRI cleanup", "podUID", uid, "success", result == nil) }()
	state, err := c.snapshot(ctx, uid)
	if err != nil {
		return err
	}
	pendingSandbox, err := c.reconcileCleanupIntents(uid, state)
	if err != nil {
		return err
	}
	if state.sandboxStatus == nil {
		return nil
	}
	// StopPodSandbox 会停止沙箱内所有容器：有非本项目容器时不能调用。
	all, err := c.runtime.ListContainers(ctx, &runtimev1.ListContainersRequest{Filter: &runtimev1.ContainerFilter{PodSandboxId: state.sandboxStatus.Id}})
	if err != nil {
		return fmt.Errorf("list sandbox containers: %w", err)
	}
	for _, container := range all.Containers {
		if container == nil || !matches(container.Labels, uid) || container.PodSandboxId != state.sandboxStatus.Id {
			return errorsForUID("sandbox contains a foreign container")
		}
	}
	for _, container := range all.Containers {
		_, err := c.runtime.StopContainer(ctx, &runtimev1.StopContainerRequest{ContainerId: container.Id, Timeout: 10})
		logOperation(uid, "stop-container", err)
		if err != nil {
			return fmt.Errorf("stop container: %w", err)
		}
		_, err = c.runtime.RemoveContainer(ctx, &runtimev1.RemoveContainerRequest{ContainerId: container.Id})
		logOperation(uid, "remove-container", err)
		if err != nil {
			return fmt.Errorf("remove container: %w", err)
		}
	}
	_, err = c.runtime.StopPodSandbox(ctx, &runtimev1.StopPodSandboxRequest{PodSandboxId: state.sandboxStatus.Id})
	logOperation(uid, "stop-sandbox", err)
	if err != nil {
		return fmt.Errorf("stop sandbox and release network: %w", err)
	}
	_, err = c.runtime.RemovePodSandbox(ctx, &runtimev1.RemovePodSandboxRequest{PodSandboxId: state.sandboxStatus.Id})
	logOperation(uid, "remove-sandbox", err)
	if err != nil {
		return fmt.Errorf("remove sandbox: %w", err)
	}
	remaining, err := c.snapshot(ctx, uid)
	if err != nil {
		return err
	}
	if remaining.observation.Exists {
		return errorsForUID("resources remain after cleanup")
	}
	if pendingSandbox {
		// 删除期间一直保留意图，任何部分失败都不能让 Ensure 盲目重建。
		if err := c.clearIntent(uid, "sandbox"); err != nil {
			return err
		}
	}
	return nil
}
