# Agent 工作说明

本文件给在本仓库里改代码的代理使用。产品范围以 [PRD.md](PRD.md) 为准，demo 实施顺序以 [Plan.md](Plan.md) 为准，能否进入下一阶段只看 [Gateway.md](Gateway.md)。代码组织和本地检查见 [docs/development.md](docs/development.md)。本文不替代这些文档。

这是一个学习用的受限演示：Go 实现声明式 API、控制器、调度器和节点 Agent，etcd 保存状态，containerd CRI 运行容器。它不兼容 Kubernetes API 或 kubectl，也不用于生产。

## 动手前

1. 从 [验收总览](docs/acceptance-overview.md) 找到本次改动涉及的阶段与操作文档。
2. 再读要改的包，沿现有调用路径改，不另起一套存储、队列或运行时。
3. 核对 `artifacts/acceptance/` 里最新一份相关报告。历史 PASS 只说明当时的代码版本；改了共用 API、模型或持久化之后，受影响的 Gate 必须重跑。

## 当前进度

G1–G5 的阶段结论和受测版本记录在 [Gateway 第 9 节](Gateway.md#9-当前状态)。R1 的实现边界和验证记录见 [R1 详细设计](docs/r1-reliability-design.md)。

## 代码地图

| 路径 | 职责 |
| --- | --- |
| `cmd/mini-api` | 加载身份文件，连接 etcd，启动 HTTPS API。 |
| `cmd/mini-k8s` | CLI 入口。连接参数放在子命令之前。 |
| `cmd/mini-manager` | 单实例 Controller。按 owner UID 对账 ReplicaSet。 |
| `cmd/mini-scheduler` | 根据节点状态和可用资源调度 Pod。 |
| `cmd/mini-agent` | 本机 Agent。通过 CRI 处理已绑定到本节点的 Pod。 |
| `cmd/mini-proxy` | 读取 Service 端点并管理本机转发规则。 |
| `internal/api` | 资源模型、单位和校验。 |
| `internal/server` | 认证授权、字段归属、etcd 事务、List/Watch。ReplicaSet 子 Pod 的创建意图也写在这里。 |
| `internal/controller` | 副本决策。不选节点，不访问 CRI。子 Pod 创建走 ReplicaSet 的 create-operation，不直接 POST Pod。 |
| `internal/agent`、`internal/runtime` | 按 Pod UID 认领、创建、重启和清理容器。 |
| `internal/cli` | 严格 YAML 解码和 HTTPS 调用。 |
| `internal/acceptance` | G1/G2/G3 的真实进程验收。未设置环境变量时跳过，跳过不是通过。 |
| `scripts/acceptance` | 隔离 etcd、执行检查、记录证据、清理。 |
| `examples` | Pod、ReplicaSet 和 Service 的样例清单。 |

只有 API 访问 etcd。CLI、Controller 和 Agent 都走 HTTPS API。

## 必须保持的约束

- 名称用于查找；UID 区分同名重建。更新使用 `resourceVersion` 做 CAS，冲突交给调用方重读。
- 用户写 spec。Agent 只能更新自己的节点和已绑定 Pod 的授权状态。status 不能覆盖 spec。Pod 运行规格创建后不可原地修改。
- 绑定和资源预留在同一个事务里提交。重复绑定不得重复预留。清理按 UID 进行，确认停止后才释放预留。
- 创建结果不确定时先查询再决定，不盲目再创建一个实例。同一 ReplicaSet 串行对账。
- etcd 只监听回环地址。CRI socket 只在本机使用。日志记录身份、UID、动作和结果，不记录令牌、私钥或环境变量内容。
- 拒绝 privileged、hostNetwork、hostPID、hostPath。不要为了演示直接改 etcd。
- 不引入 controller-runtime、插件式调度框架或消息队列。运行时固定为 containerd，不并行维护 Docker 路径。
- G1–G5 通过只表示 demo 阶段完成，不表示 [PRD.md](PRD.md) 里的领导选举、审计和灾备已经完成。

## 修改方式

- 提交前对改过的 Go 文件执行 `gofmt`。导出的标识符使用以其名称开头的文档注释。
- 注释只解释并发、所有权、安全边界和代码里看不出的原因。
- 沿现有错误语义扩展。新增行为时补测试，并说明要重跑哪个 Gate。
- 验收目录里的证书、令牌和 etcd 数据只留在本机，不提交，不写进文档或日志。
- Windows 可以做 G1/G2 和单元测试。G3 及以后的 CRI/CNI 验收必须在真实 Linux Worker 上跑。Windows 上的交叉编译或跳过的测试不能写成 PASS。
- 证据写入新的 `artifacts/acceptance/<时间>-<阶段>/` 目录。不要改写已有报告来让旧记录看起来通过。

仓库根目录的本地检查：

```sh
gofmt -l cmd internal
go vet ./...
go test ./...
go build ./...
```

`go test ./...` 不会自行启动 etcd。真实 Gate 用 `scripts/acceptance/g1.sh`、`g2.sh`、`g3.sh`。G3 还要设置 `G3_TEST_IMAGE` 和 `G3_CNI_NETWORK`。缺少 containerd、CNI 或权限时，结果是 BLOCKED，不是 PASS。

模块要求 Go 1.23.0 或兼容版本。直接依赖的 etcd 客户端是 3.5.21，验收使用单成员 etcd 3.5.21。版本以 `go.mod`、`go.sum` 为准。

## 测试机

G3 及以后的验收在真实 Linux 测试机上跑。Windows 开发机只用于 G1/G2 和单元测试，不能代替测试机上的 CRI/CNI 检查。

测试机需要能编译本模块的 Go、单成员 etcd 3.5.21，以及 G3 起所需的 containerd CRI、CNI 和已拉到本地的固定 digest 镜像。具体安装位置以那台机器当时的环境为准，不写进本文。动手前先在测试机上确认 `go version`、`etcd --version` 和 CRI 是否可用。

在测试机上启动的 etcd 只绑定回环地址，不连已有集群。每轮验收使用独立数据目录，结束时停掉本轮进程并清掉临时数据。缺少运行时、网络或权限时结果记为 BLOCKED，不用模拟运行时放行。
