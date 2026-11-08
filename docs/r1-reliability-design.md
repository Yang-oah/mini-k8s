# R1 详细设计：容量基线与 RS 创建幂等

状态：R1 已通过 [PR #8](https://github.com/Yang-oah/mini-k8s/pull/8) 合并到 `master`（`77f47f1`）。合并范围的同版本 G1～G5、并发与故障评测通过。基线指标和后续运维边界见第 8 节。本文细化 [可靠性规划](reliability-roadmap.md) 的 R1。

已合并的实现支持创建操作的准备、提交、查询、取消和显式管理员回收。准备请求以 RS UID、版本、Pod 名称与 Pod 列表版本构造持久化身份。提交在同一事务写入 Pod 和结果。Controller 通过创建操作接口创建 RS 子 Pod。旧 Controller 与新版 API 不兼容，应先停止旧 Controller，再升级 API 和 Controller，并以新版本做回归验证。

**目前仍有边界：** 回收需管理员显式调用，尚无自动发现或周期回收；已有旧 schema 操作记录时新建意图会保守拒绝，尚无迁移工具，因此无人维护时全局配额仍可能耗尽。已删除 RS 的记录清理完后可显式回收其 UID 元数据并释放预算。回收前按同一个 etcd revision 分页检查 ReplicaSet 和 Pod，两类对象共用 2048 的扫描预算，每页最多 32 个；发现该 UID 仍被引用，或对象数超过预算，都返回冲突，不跳过检查。操作记录回收前，终态写入时间以及 API 进程首次观察到该终态之后的单调时间，都要超过 24 小时加 10 秒；API 重启会重新开始观察。etcd 离线恢复后必须重启 API，再按该规则重新观察。未经完整验证不得将这些限制视为已解决。

**上一轮验证：** 源码摘要 `09d68e75383a794bd5f0ead324d1f23d2bea9c781de0813e8142cd450017070c` 的隔离 G1/G2 已通过，证据位于测试机 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v6-jT6u1GHS/artifacts/acceptance/20261105T151659Z-g2-498302/`；同一快照的真实 etcd 操作/回收与身份测试重复 5 轮通过，Controller 与相关 Server 测试的 Go race 检查通过。此前并发提交修复在独立 etcd 下重复 20 轮通过。该快照的 G3-03 因停止 RPC 超时而未能验证重启；后续对 G3 测试器的修改需要在新的源码摘要下重新验证。G3、同版本 G4/G5 尚未完成，不能据此判定完整 R1 通过。

**G3 测试器修复后的验证：** 源码摘要 `919eb41dd1248db2c1adf5c0c38dcc597d534247c9017e02ea7a30c690e1a5bb` 的 G1/G2 及超时识别测试通过。真实 G3 的 01～05 全部通过，但首次进程守护检查遇到 EINTR，虽然随后重查通过，脚本仍报告 FAIL 并保留现场；证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v7-fxQu3Km4/artifacts/acceptance/20261105T154357Z-g3-940760/`。守护器已增加有界重试，须在新的源码摘要下重新执行完整 G3。停止 RPC 超时仍仅标记为结果未知；G3-03 仍须在原窗口内观察到同 UID 重启、计数增长和退出原因。

**G3 当前结果：** 源码摘要 `d9744f89f899b8bd3debfaa9cd3113b213103d4b7d415a4ba183d44f159f9d27` 的 G3-01～05、进程守护和隔离资源清理均通过，且同轮前置 G1/G2 通过。证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v8-jzrGGUC4/artifacts/acceptance/20261105T155315Z-g3-1178950/`。同版本 G4/G5 尚未复验；回收自动化和旧数据迁移仍未实现。

**更新后的验证：** 源码摘要 `4d6bcb2b61e61797a9329a67a389df46176922debce847c02ca7a7e60a222f5a` 的隔离 G1/G2 通过，证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5/artifacts/acceptance/20261106T014605Z-g2-1397784/`；真实 etcd 下相关 Server 并发与回收测试以 race 模式连续运行三轮通过。同一快照的 G3-01～05 和清理通过，证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5/artifacts/acceptance/20261106T015037Z-g3-1491037/`；G4-01～06 及两节点清理通过，证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/g4cur-20261106T024952Z/`。G5-01～07 同版本回归通过，证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/g5cur-20261106T032331Z/`；两侧租约、运行时和规则回到基线，地址 `10.96.0.15` 的声明仍为 retained。R1 专项完整验收尚未结束。

**R1 故障专项增量：** 在上述快照的隔离副本中添加验收测试，使用独立 etcd 与 Go race 检测，通过 20 轮提交与取消竞争，以及真实本地 HTTP 中提交成功后丢弃响应、按原操作身份恢复的测试。证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5/r1-faults-f2ol7Xj0/`，其中记录了原始快照与测试文件摘要。这组测试本身不覆盖真实进程重启或 etcd 中断。

**R1 PREPARED 恢复验证：** 真实独立 API、Controller 与隔离 etcd 中，将同一 RS 创建操作停在 PREPARED，再分别重启 Controller、重启 API、暂停并恢复 etcd。每项恢复后均使用原 operationID 提交，且只观察到一个相同 UID 的 Pod；证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5/r1-process-faults-XAVJgPca/`。API 重启和 etcd 暂停期间 Controller 停止，因而不代表 Controller 在存储故障期间持续重试或所有未知结果场景均已验证。

**当前版本隔离容量采样：** 证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5/r1-capacity-current-6pi0bjmc/`。单机隔离 API/etcd、零副本 RS 的首次并发创建出现 selector 修订竞争：20 个对象、1/8/32 并发首次 201 分别为 20/14/7 个；40 个对象分别为 40/20/10 个，其余返回 409。后续串行重试仅用于补齐读取样本，不改变首次失败统计。这不是吞吐保证，也不能代表双节点 Worker、Pod 收敛或持续负载结果；应在 R2 针对共享 selector 栅栏竞争评估改进。

**selector 竞争修复及回归：** 快照 `1a8537f578953716d9c2c9858b2f2421d2ff961c1ea8212b9c3e3991e9e401a8` 保留 selector 快照和 etcd CAS，遇到无关并发修改时在请求期限内有界退避重试。真实 etcd 和两个 Server 实例的并发/重叠/同名/取消测试以 race 模式重复五轮通过，证据为 `/home/lihaoyang/mini-k8s-g4/evidence/r1-selector-v10-I2MJLyAh/selector-race.log`。同环境的六档容量复测中，20/40 个不同 selector 在 1/8/32 并发下首次均返回 201；40 个对象、32 并发的成功写入 P95 为 1367.241ms。采样为单机零副本 RS，不能视为持续负载或容量承诺，证据目录 `/home/lihaoyang/mini-k8s-g4/evidence/r1-selector-v10-I2MJLyAh/r1-capacity-current-ozhondhu/`。同一新快照的 G1/G2、R1 竞争和进程故障专项通过；G3-01～05、进程身份及清理通过，证据目录 `/home/lihaoyang/mini-k8s-g4/evidence/r1-selector-v10-I2MJLyAh/artifacts/acceptance/20261106T063748Z-g3-4055336/`。G5 回归结果见下。

同一快照的 G4-01～06 与两节点清理通过；证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/g4cur-20261106T065127Z/`。G5-01～07 同版本回归及两节点清理通过，证据位于 `/home/lihaoyang/mini-k8s-g4/evidence/g5cur-20261106T070124Z/`；`10.96.0.18` 的 Service 地址声明仍为 retained。

## 1. 目标与边界

在单个活跃 Controller Manager、单控制面、单成员 etcd 的部署约束下，先测量容量基线，再确保 ReplicaSet 补建在并发调用、API 响应丢失和进程重启时不会因重复请求额外创建 Pod。etcd 是唯一持久化事实源，仅 API Server 直接访问。对每次创建给出成功、确定未创建或结果未知；未知时保守等待与对账，不换一个操作继续创建。

R1 只改 Controller 管理的 RS 子 Pod 创建路径和相关容量观测。用户 apply、调度绑定、CRI 创建、节点会话和审计补偿仍按各自现状及后续模块处理；不能宣称所有 API 都获得了完整幂等能力。不在本模块引入 Controller 多实例、领导选举、API Server 缓存或新依赖；以后支持多实例必须先有持久化领导租约和事务内代次栅栏。单成员 etcd 不提供高可用。

## 2. 现状与风险

- Controller 仍是单工作者。每轮按 RS UID 筛选全量 Pod，最多补建一个；Pod 名称仍由 RS UID 和空位决定。子 Pod 创建不再调用原始 `POST /pods`，改为准备并提交创建操作。删除 Pod 和 ReplicaSet finalize 仍走原有接口。
- 准备、提交、查询和取消只允许 Controller 身份。API 在同一事务里比较父 RS 的 UID、版本和未删除状态、Pod 名称不存在以及意图版本，并同时写入 Pod 与 `COMMITTED` 结果。已有的父子删除竞态保护必须保留。
- 请求已提交但响应丢失、旧请求仍在途、控制器重启以及同名资源被删除再创建时，单靠进程内状态或一次名称查询仍不足以作为结果证明。当前实现用持久化意图和原操作序号查询缩小这个窗口。存储不可达或查询失败时保持未知，不换一个新意图继续创建。

## 3. 必须保持的不变量

1. 同一个 `RS UID + 操作序号` 只能对应一种请求摘要和一个历史结果；相同身份不同内容为冲突。资源名称相同但 UID 不同是不同对象。
2. 每个 RS UID 最多一个未决创建意图；对应 RS 的对账串行。已绑定、正在清理且未终态的 Pod 仍占额度。决定缺口时以 API 强一致读取和当前已确认操作为依据，不能用旧缓存重复补建。
3. 创建 Pod、写入完成结果和更新现有子对象创建标记在同一个 etcd 事务内提交，事务同时校验意图版本、Pod 名称不存在、父 RS UID/版本及未删除状态。失败不得留下部分效果。
4. 在途操作不得因查询暂时返回“未找到 Pod”就被认定失败；取消也必须与创建事务通过同一意图版本做 CAS，确保取消与提交只能成功一个。
5. 结果重试返回历史 Pod UID 和创建结果；即使这个 Pod 后来已删除，也不能把历史成功解释成一次新的创建。若无法确定结果，阻止该 RS 的后续补建并保留诊断信息。

## 4. 操作记录与接口

创建操作只对 Controller 身份开放，回收只对管理员身份开放。记录不存 token、环境变量等敏感明文，日志只记录操作 ID、RS UID、Pod UID、版本与结果。下面的 HTTP 路径和动词是当前工作区的契约。etcd 键前缀是 `replica-operations/<RS UID>/`，具体键以 `internal/server/replicaset_operations.go` 为准。

### 4.1 身份与内容

- 服务端按 RS UID 分配严格递增的 `operationID`。它是 int64 序号，与 UID 分开保存，不拼成一个字符串，也不复用于同一 UID 的后续创建。为避免分配响应丢失导致第二个意图，分配和“该 RS 当前待处理意图”在事务中关联；重启后必须先按 RS UID 查询并恢复该意图。
- 记录包含：schema 版本、操作 ID、RS 名称/UID、预期 RS 版本、确定性 Pod 名称、规范化请求摘要、重建请求所需的受控引用、状态、创建时间和结果中的 Pod UID。不得将环境变量等敏感内容另行复制进操作记录；可由已保存且版本未变化的 RS 模板重建，模板改变则按竞争取消并重读。摘要覆盖会改变副作用的字段，序列化方式与默认值规则固定；不使用未经规范化的原始 JSON 字节。
- 路径可采用 `operations/rs/<RS UID>/<sequence>` 加每个 RS 的序号水位和待处理索引。精确 key 设计、记录字节上限、认证及敏感字段处理在实现前评审；不要求所有 Pod 名称永不复用，身份由 UID 和操作记录区分。

### 4.2 接口语义

| 操作 | 语义 |
| --- | --- |
| 为 RS 准备创建 | 强一致读取 RS 与子 Pod 后，事务校验 RS UID/版本并原子分配序号、记录 `PREPARED` 和该 RS 的待处理索引。已有未决意图时返回同一意图供恢复，不另分配。该步骤不创建 Pod。 |
| 提交创建 | 提交 `operationID` 和与意图相同的规范化内容；API 读取记录并执行第 3 节所列单事务。成功同时写 Pod 和 `COMMITTED` 结果。 |
| 查询单个操作 / RS 待处理操作 | 用强一致读取返回 `PREPARED`、`COMMITTED`、`CANCELLED` 或明确已过期/不存在。存储不可用时返回错误，不用空结果代替。返回结果按权限限制。 |
| 取消未提交意图 | 仅在重新读取后确认父 RS 已删除、UID 改变、期望缩容或其他确定不应创建时尝试；事务比较 `PREPARED` 版本后写 `CANCELLED` 并清除待处理索引。并发提交成功则取消冲突，须重读结果，不宣称取消成功。 |

当前 HTTP 契约：

| 方法 | 路径 | 身份 | 请求 |
| --- | --- | --- | --- |
| POST | `/api/mini/v1/replicasets/<名称>/create-operation` | controller | `uid`、`resourceVersion`、`podName`、`podListRevision`。这一步不创建 Pod。 |
| PUT | 同上 | controller | `uid`、`operationID`、`podName`。相同序号和相同内容重复调用返回原结果。 |
| GET | 同上 `?uid=` | controller | 该 UID 的未决意图。没有则返回 404。存储失败不用空对象代替。 |
| GET | 同上 `?uid=&operationID=` | controller | 查询该序号。 |
| DELETE | 同上 | controller | `uid`、`operationID`。只取消仍为 `PREPARED` 的意图。 |
| POST | `.../create-operation-gc` | admin | 见第 6 节。 |
| POST | `.../create-operation-owner-gc` | admin | 见第 6 节。 |

新提交成功返回创建结果。相同序号但 Pod 名称或父版本不同，返回冲突。已回收且低于水位的旧序号返回过期，不能当成新意图。etcd 不可用返回 503。客户端把响应丢失、超时和无法确认是否提交的错误当作未知，并用原序号查询。不能把 503 解释成事务一定没有提交。

## 5. 创建与恢复流程

1. Controller 强一致读取 RS 和其所有未终态子 Pod，按当前规则计算缺口；同时查询该 UID 的待处理操作。有未决操作先恢复，不能先创建新意图。
2. 存在缺口时准备意图。准备响应丢失后按 RS UID 强一致查询；若存储不可达或无法确认，则退避并暂停该 RS 的补建。服务端单个待处理索引是跨进程崩溃恢复的关键，而不是依赖本地内存。
3. 提交时服务端根据意图校验相同内容和当前父 RS 条件，以 CAS 原子写入 Pod、创建标记和 `COMMITTED` 结果并清除待处理索引。不同请求竞争或父 RS 被修改，返回冲突并重读，不盲目换名重试。
4. 响应丢失或 API/Controller 重启后，先查询原意图：`COMMITTED` 就确认原 Pod UID，`CANCELLED` 就重新读取并计算，`PREPARED` 则重新读取 RS 与相关 Pod 后以原 ID 重试或 CAS 取消；无法确认时保持未决。Pod 后续被删除也不改变历史 `COMMITTED` 的身份。
5. 明确处理 RS 同名重建：所有查询、记录和比较都使用原 UID；原 RS 删除后，旧意图不可在新 UID 上提交。对曾经提交的结果按历史返回，不据此修改新 RS。

准备与提交之间的缩容、RS 删除和并发重试必须纳入验证。仅有 Pod 名称查询或返回 404，不能证明旧的在途事务不会随后成功。

## 6. 保留、回收与过期重试

当前实现提供管理员接口 `POST /api/mini/v1/replicasets/<名称>/create-operation-gc`，请求体为 `{"uid":"<RS UID>","operationID":<序号>}`。首次观察终态时启动进程内单调计时并返回 409；经过完整观察窗口后重复调用，每次最多清理 16 个别名，响应 `complete=false` 表示尚需重试，`complete=true` 表示已完成。重启 API 会重新开始观察。不得通过手工删除 etcd 键绕过保留或配额检查；恢复快照后应先重启 API，再观察完整窗口。接口仅支持显式指定身份，尚无自动发现操作和定时回收。

全部操作记录回收后，可由管理员调用 `POST /api/mini/v1/replicasets/<名称>/create-operation-owner-gc`，请求体为 `{"uid":"<旧 RS UID>"}`。服务端核对该 UID 已无 RS、Pod、未决操作及历史记录后，原子回收该 UID 的元数据并释放预算；同名新 RS 使用独立 UID。检查使用固定的 etcd revision 分页，每页最多 32 个对象，两类对象共用 2048 个的扫描预算；超出预算或快照不可用时保守拒绝，不得跳过检查强行删除。

- `PREPARED` 不因普通 TTL 自动删除。不可确认的操作保持待核实。对可确定取消的意图先完成 CAS 终态化。未决操作和配额水位的独立告警列入 R4 运维观测。
- `COMMITTED` 至少保留到关联 Pod 已回收且超过保留窗口；`CANCELLED` 至少保留到相同窗口结束。当前代码把这个窗口定为 24 小时加 10 秒，并把单个 RS UID 的操作键用量上限定为 10000、全局上限定为 100000。这两个上限是拒绝新意图的实现阈值，不是压测得到的容量。
- 同一 RS UID 保留可持久化的单调序号高水位及已回收的连续终态水位；清除详细结果后，对旧序号返回明确“结果已过期”，禁止将其当成新意图执行。水位推进和记录删除需要 CAS，遇到未决/未满足保留期的空洞不能越过。
- RS 删除后不得让旧 UID 分配新意图；只有确认该 UID 无未决意图且所有保留期已结束，才可回收其详细记录及水位。此后旧 UID 的请求返回不存在或过期，不能根据同名新 RS 补建。UID 不复用是该方案的前提。
- 操作记录按有界批次回收。配额耗尽时，API 对需要新增记录的请求返回 503，现有容器与未决记录保留。记录配额水位、磁盘告警及磁盘写满后的恢复演练列入 R4。R1 已验证的故障范围见第 8 节。

## 7. 容量基线

先固定源码提交或源码树摘要、二进制摘要、镜像 digest、Go/etcd 版本、控制面与 Worker 规格、CNI、资源配额及背景负载。以 1、8、32 个并发客户端和 20、40 个对象作为初始测试点；对象规模、持续时间、预热和采样间隔由测试记录明确。这些输入是实验点，不是支持上限或 SLA。

记录请求数和状态码、超时/未知结果数、P50/P95/P99 延迟、RS 达到期望的时间、etcd 读写/事务请求量和延迟、Watch 数、内存、CPU、etcd 数据及磁盘增长。区分系统拒绝与客户端超时。首次报告只建立基线，不要求缓存优化；后续 R2 使用同一方法比较，禁止拿不同环境下的单次数据直接声称性能提升。

## 8. 增量 PR 和用户验收

各 PR 在前一部分验收通过后合并，使用独立分支与 PR，不直接推主分支；实现方提供脚本、故障注入与运行步骤，用户在 Linux 测试环境执行并回传证据。

| 顺序 | 交付 | 最小验收 |
| --- | --- | --- |
| R1-A | 基线采集、指标口径及现有路径梳理 | 固定环境执行基线，采集每类指标并确认测量过程无越权清理；不承诺容量目标。 |
| R1-B | 操作意图、原子提交与查询接口 | 真实 etcd 上并发重试同 ID；同内容只有一个 Pod UID，不同内容冲突；父 RS 删除或版本变化时无孤儿 Pod。 |
| R1-C | Controller 恢复、回收和过期处理 | 分别注入准备响应丢失、提交成功但响应丢失、提交与取消竞争、进程/API 重启、etcd 暂停；恢复后不重复创建。测试 Pod 删除后旧结果重试、RS 同名新 UID、记录清理后的旧序号、容量上限。 |

针对每轮保存源码与构建身份、操作 ID/RS UID/Pod UID、注入和恢复的时间、请求/响应、etcd/API 观察结果、最终 Pod 清单和清理结果。组件重启或 etcd 故障时，既有运行容器应维持原身份；验证结束只清理由该次测试创建、可确认 UID 的资源。针对变更影响运行必要的 G1/G2、调度及 G5 回归；回归范围由具体 diff 确定。

### 当前一次性验收清单

以源码摘要 `1a8537f578953716d9c2c9858b2f2421d2ff961c1ea8212b9c3e3991e9e401a8` 为准：

- **既有功能回归：通过。** G1～G5 同版本结果及证据见本文开头。G5 的双节点 20 Pod 发布和伸缩也是有效的收敛证据，不因单独的 API 基线采样没有 Worker 而失效。
- **R1-B 与 R1-C：已有覆盖。** 真实 etcd 下并发提交、同 ID 重放与冲突、提交与取消竞争及回收测试通过；响应丢失、重启及存储恢复测试的具体环境与范围见开头记录。真实进程的 PREPARED 故障测试在 API 重启和 etcd 暂停期间停止了 Controller，不能作为 Controller 在故障期间持续运行的证明；准备响应丢失由模拟 API 验证。
- **R1-A：基线已补采，存在指标缺口。** 当前版本的 20/40 个零副本 RS、1/8/32 客户端六档均完成；首次创建均为 201，读取均为 200。原始 etcd 指标、Watch 数、进程 CPU/内存、磁盘采样与资源限制记录保存在 `/home/lihaoyang/mini-k8s-g4/evidence/r1-selector-v10-I2MJLyAh/r1-capacity-current-xh5jxzts/`。本轮没有 Watch 负载，Watcher 仅为瞬时值；etcd 未暴露 `grpc_server_handling_seconds` 请求耗时直方图，因此不能声称已获得 etcd 请求延迟分布。规划没有指定持续压测分钟数或最低吞吐门槛，不因此新增硬性通过条件。
- **设计限制：持续保留。** 当前是单 Controller、单成员 etcd；操作记录回收需管理员显式调用，旧 schema 暂无迁移工具；缓存与全局并发控制在后续阶段。记录数／磁盘独立告警尚未确认实现，当前仅确认配额耗尽返回 503。上述限制不因 G1～G5 通过而消失，不能将其描述为已经实现。

**集中评测记录：** 复核上述源码摘要后，使用独立 etcd 重跑 Server/Controller race 测试通过（Service 地址测试因独占空 etcd 的前提而单独重跑通过）；提交与取消竞争、真实 HTTP 响应丢失及 PREPARED 进程故障测试通过。证据分别位于 `r1-selector-v10-I2MJLyAh/r1-evaluation-IUIbvU8z/`、`r1-selector-v10-I2MJLyAh/r1-service-evaluation-43Bad37b/`、`r1-selector-v10-I2MJLyAh/r1-faults-ZzzfksO8/` 和 `r1-selector-v10-I2MJLyAh/r1-process-faults-2aE6I6Ul/`，均相对于 `/home/lihaoyang/mini-k8s-g4/evidence/`。配额耗尽返回 503 已确认，独立阈值告警仍待 R4 实现。

## 9. 已知风险与尚未关闭的决定

- 单个 Controller 已经能按 RS UID 读取待处理意图，准备请求由服务端保证同一 UID 最多一个未决意图。这不能代替多个 Controller 实例所需的领导租约和事务代次栅栏。
- 强一致读后、准备前仍可能有并发变化；准备和提交均需版本比较，同一 RS 未决操作限制必须由服务端强制。这个约束不替代未来多控制器的领导代次栅栏。
- 服务端生成的保留水位和 UID 的不复用前提必须经迁移、重启和回滚验证；出现无法识别的历史状态时阻止不安全重试。
- 幂等只覆盖本模块指定副作用，不承诺物理节点隔离，也不承诺跨进程与网络的 exactly-once。
