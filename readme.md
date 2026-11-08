# Mini K8s

Mini K8s 是一个用 Go 实现的容器编排实验。用 `mini` 提交应用声明后，Controller 管理副本，Scheduler 选择节点，Agent 通过 CRI 调用 containerd，Proxy 在 Worker 上配置 ClusterIP 转发。

首版使用一套控制面、单成员 etcd 和两台 Linux Worker，支持 `default` 命名空间下的无状态单容器 Pod、ReplicaSet 手动伸缩和单 TCP 端口 Service。项目使用自定义 API 和 CLI，面向学习和演示。与 Kubernetes 的能力对照和首版取舍见 [项目汇报](docs/mini-k8s-管理层汇报.md)。

## 验收与进度

- **G1～G5 已通过。** 验收覆盖 API 与存储、副本对账、真实容器运行、双节点调度和 Service。20 个 Pod 在最近一轮双节点测试中用时 7.3 秒全部 Ready，稳定运行 5 分钟后完成缩容和清理。 [验收结果与证据](docs/mini-k8s-验收结果与证据.md)
- **R1 已合并。** 副本创建采用操作 ID，响应丢失后可查询原结果。 [R1 设计与验收记录](docs/r1-reliability-design.md)
- **R2-A1 已通过验收，PR 待审阅。** 普通 API 请求过载返回 429 时，两台 Agent 仍完成心跳和测试 Pod 清理。后续工作包括 etcd 请求预算、控制器队列、List / Watch 缓存及节点隔离与恢复。 [R2 计划](docs/r2-implementation-plan.md) · [PR #9](https://github.com/Yang-oah/mini-k8s/pull/9) · [可靠性路线图](docs/reliability-roadmap.md)

## 设计要点

- API Server 直接读写 etcd，其他组件通过 API 读取和更新状态。资源版本和 CAS 事务保护并发写入。
- Controller 按资源 UID 对账。Scheduler 在同一事务里提交 Pod 绑定和 CPU、内存预留。
- 节点失联时，Scheduler 停止向它绑定新 Pod，Proxy 摘除它的 Service 端点。Controller 继续计入尚未确认停止的实例。

## 从哪里开始

| 想了解什么 | 文档 |
| --- | --- |
| 在两台 Worker 上运行和排障 | [两节点运行与排障指南](docs/operations.md) |
| 项目设计、Kubernetes 能力对照和 AI Coding 协作 | [项目汇报](docs/mini-k8s-管理层汇报.md) |
| 源码版本、逐项判定和原始记录 | [验收结果与证据](docs/mini-k8s-验收结果与证据.md) |
| 需求、阶段范围和通过条件 | [PRD](PRD.md) · [实施计划](Plan.md) · [验收标准](Gateway.md) |
| 阅读代码和组件设计 | [开发指南](docs/development.md) · [G2 副本控制器](docs/g2.md) · [G3 Agent](docs/g3.md) · [G4 双节点调度](docs/g4.md) · [G5 就绪探测](docs/g5-readiness.md) · [G5 Service](docs/g5-service.md) |

初始设计记录：2026-11-03，howyangli。
