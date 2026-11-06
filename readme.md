# Mini K8s

一个用于学习容器编排的精简项目：使用 Go 实现声明式 API、控制器、调度器和节点 Agent，并使用 etcd 保存状态、通过 CRI 接入 containerd。项目是受限演示，不兼容 Kubernetes API 或 `kubectl`，也不用于生产环境。

## 项目结构

```text
cmd/                  各组件的启动入口和验收用探针
internal/api/         资源模型与校验
internal/server/      API、etcd 持久化、调度绑定和状态管理
internal/controller/  ReplicaSet 对账
internal/scheduler/   节点选择与调度
internal/agent/       节点心跳、Pod 对账和就绪探测
internal/runtime/     CRI 容器生命周期及资源清理
internal/proxy/       Service 端点与 iptables 转发规则
internal/cli/         mini-k8s 命令行客户端
internal/acceptance/  真实 API 和运行时的阶段验收测试
scripts/acceptance/   各阶段验收与取证辅助脚本
examples/             示例资源清单
docs/                 开发、部署、排障和验收说明
artifacts/acceptance/ 验收运行产生的报告和证据
```

建议先从 `internal/api` 的资源模型读起，再按 API、Controller、Scheduler、Agent 和 Proxy 的数据流阅读；组件配置与双节点部署见[运行与排障指南](docs/operations.md)。

## 当前验收状态

G1–G5 是循序实施的 demo 阶段，验收依据是 [Gateway.md](Gateway.md)。已有 G1–G3 通过记录，以及同一源码校验值下 G4-01–06、G5-01–07 的脚本 PASS 记录；各轮证据与剩余交付条件见[验收总览](docs/acceptance-overview.md)。

当前 Service 实现会保留已删除 Service 的 ClusterIP 声明，以避免失联 Proxy 使用旧规则时地址复用；地址池不会自动回收，容量可能耗尽。节点失联、单成员 etcd、审计和灾备的限制见计划及运行文档，不应将 demo 验收与 PRD 全部能力完成等同。

## 快速开始

1. 本地构建和检查 Go 代码，见下节。
2. 准备 Linux Worker、CRI/CNI 和双向路由，按 [两节点运行与排障指南](docs/operations.md) 启动各组件。
3. 按运行指南部署 [ReplicaSet](examples/g5-replicaset.yaml) 和 [Service](examples/g5-service.yaml)，访问 ClusterIP，再缩容和清理示例资源。
4. 对照 [验收总览](docs/acceptance-overview.md) 选择场景、记录源码版本并保存证据。

## 开发与验证

项目使用 Go，依赖版本见 [go.mod](go.mod)。在仓库根目录可执行以下常规代码检查：

```sh
gofmt -l cmd internal
go vet ./...
go test ./...
go build ./...
```

这些命令不能替代真实环境验收。G3–G5 涉及 Linux、containerd CRI、CNI、网络及权限；启动组件、准备镜像和安全取证的步骤见[运行与排障指南](docs/operations.md)。`g4.sh` 仅提供预检，不单独判定 G4 通过；G5 的 API 采样脚本也不自动判定整门 Gate。运行验收前先核对脚本适用的节点、配置和影响范围，避免操作已有工作负载。

## 项目文档

- [产品需求与边界](PRD.md)
- [分阶段实现计划](Plan.md)
- [各阶段验收标准](Gateway.md)
- [验收总览与证据说明](docs/acceptance-overview.md)
- [开发与代码阅读指南](docs/development.md)
- [G1 控制面启动与操作说明](docs/g1.md)
- [G2 副本控制器说明](docs/g2.md)
- [G3 单节点 Agent 说明](docs/g3.md)
- [G4 两节点调度与验收说明](docs/g4.md)
- [G5 readiness 验收说明](docs/g5-readiness.md)
- [G5 Service 验收说明](docs/g5-service.md)
- [G5 证据核对与补采](docs/g5-evidence-reconciliation.md)
- [后续可靠性设计草案](docs/reliability-roadmap.md)
- [R1 详细设计与状态](docs/r1-reliability-design.md)
- [两节点运行与排障指南](docs/operations.md)（控制面和示例步骤已在当前源码上执行；Worker 步骤未在占用中的集群上再启动）

计划依次完成最小控制面（G1）、副本对账（G2）、单节点运行时（G3）、两节点调度（G4）和 Service 演示（G5）。每阶段按对应 Gateway 验收；未运行或缺少真实环境的检查不能视为通过。

## 设计要点

- API 是唯一访问 etcd 的业务组件，使用资源版本和 CAS 处理并发更新，通过 List/Watch 同步状态。
- Controller 根据期望状态持续对账，按资源 UID 区分所有权，避免重复创建和误删。
- Agent 通过 CRI 操作 containerd，并复用 CNI；不直接调用 Docker API。
- CPU、内存预留与绑定原子提交；节点失联时保守处理尚未确认停止的实例。
- 控制面和运行时的恢复以可验证的真实环境测试为准。完整审计、灾备和生产级高可用等要求见 PRD，不能仅凭 demo 阶段通过而宣称完成。

初始设计记录：2026-11-03，howyangli。
