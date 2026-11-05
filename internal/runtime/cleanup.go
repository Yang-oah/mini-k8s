package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Cleanup 仅清理同时匹配项目标签和 UID 的实例；成功表示 CRI 已确认网络清理。
func (c *Client) Cleanup(ctx context.Context, uid string) (result error) {
	if err := checkUID(uid); err != nil {
		return err
	}
	unlock, err := c.lockPod(ctx, uid)
	if err != nil {
		return err
	}
	defer unlock()
	defer func() { slog.Info("CRI cleanup", "podUID", uid, "success", result == nil) }()
	started := time.Now()
	state, err := c.snapshot(ctx, uid)
	logTimedOperation(uid, "snapshot-before-cleanup", started, err)
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
	started = time.Now()
	all, err := c.runtime.ListContainers(ctx, &runtimev1.ListContainersRequest{Filter: &runtimev1.ContainerFilter{PodSandboxId: state.sandboxStatus.Id}})
	logTimedOperation(uid, "list-sandbox-containers", started, err)
	if err != nil {
		return fmt.Errorf("list sandbox containers: %w", err)
	}
	for _, container := range all.Containers {
		if container == nil || !matches(container.Labels, uid) || container.PodSandboxId != state.sandboxStatus.Id {
			return errorsForUID("sandbox contains a foreign container")
		}
	}
	for _, container := range all.Containers {
		started = time.Now()
		_, err := c.runtime.StopContainer(ctx, &runtimev1.StopContainerRequest{ContainerId: container.Id, Timeout: 10})
		logTimedOperation(uid, "stop-container", started, err)
		if err != nil {
			return fmt.Errorf("stop container: %w", err)
		}
		started = time.Now()
		_, err = c.runtime.RemoveContainer(ctx, &runtimev1.RemoveContainerRequest{ContainerId: container.Id})
		logTimedOperation(uid, "remove-container", started, err)
		if err != nil {
			return fmt.Errorf("remove container: %w", err)
		}
	}
	started = time.Now()
	_, err = c.runtime.StopPodSandbox(ctx, &runtimev1.StopPodSandboxRequest{PodSandboxId: state.sandboxStatus.Id})
	logTimedOperation(uid, "stop-sandbox", started, err)
	if err != nil {
		return fmt.Errorf("stop sandbox and release network: %w", err)
	}
	started = time.Now()
	_, err = c.runtime.RemovePodSandbox(ctx, &runtimev1.RemovePodSandboxRequest{PodSandboxId: state.sandboxStatus.Id})
	logTimedOperation(uid, "remove-sandbox", started, err)
	if err != nil {
		return fmt.Errorf("remove sandbox: %w", err)
	}
	started = time.Now()
	remaining, err := c.snapshot(ctx, uid)
	logTimedOperation(uid, "snapshot-after-cleanup", started, err)
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
