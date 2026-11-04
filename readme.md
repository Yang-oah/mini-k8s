# Mini K8s

一个用于学习容器编排的精简项目：使用 Go 实现声明式 API、控制器、调度器和节点 Agent，并使用 etcd 保存状态、通过 CRI 接入 containerd。项目是受限演示，不兼容 Kubernetes API 或 `kubectl`，也不用于生产环境。

## 项目文档

- [产品需求与边界](PRD.md)
- [分阶段实现计划](Plan.md)
- [各阶段验收标准](Gateway.md)
- [开发与代码阅读指南](docs/development.md)
- [G2 副本控制器说明](docs/g2.md)
- [G3 单节点 Agent 说明](docs/g3.md)

计划依次完成最小控制面（G1）、副本对账（G2）、单节点运行时（G3）、两节点调度（G4）和 Service 演示（G5）。每阶段必须通过对应 Gateway 才进入下一阶段；未运行或缺少真实环境的检查不能视为通过。

## 设计要点

- API 是唯一访问 etcd 的业务组件，使用资源版本和 CAS 处理并发更新，通过 List/Watch 同步状态。
- Controller 根据期望状态持续对账，按资源 UID 区分所有权，避免重复创建和误删。
- Agent 通过 CRI 操作 containerd，并复用 CNI；不直接调用 Docker API。
- CPU、内存预留与绑定原子提交；节点失联时保守处理尚未确认停止的实例。
- 控制面和运行时的恢复以可验证的真实环境测试为准。完整审计、灾备和生产级高可用等要求见 PRD，不能仅凭 demo 阶段通过而宣称完成。

初始设计记录：2026-11-03，howyangli。
