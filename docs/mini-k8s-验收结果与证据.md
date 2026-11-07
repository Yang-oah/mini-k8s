# Mini K8s 验收结果与证据

更新日期：2026 年 11 月 7 日

本文汇总最近一轮 R2-A1 验收的结果，并列出原始记录的位置。测试使用一套控制面和两台 Linux Worker。验收结论适用于本次源码快照和测试环境。

## 一、源码与结论

本轮验收使用的 Go 源码 SHA-256 如下。

```text
b336e1c50cc7b2becc3520b6014c309c29b48d166dbc02664baaa9de4e930e82
```

摘要覆盖 `cmd`、`internal`、`go.mod` 和 `go.sum`。从 Git 提交 `de75e95c63407f1ef5f57730c637119c33341e79` 导出上述文件后，复算结果与验收摘要一致。代码交付对应 [R2-A1 PR #9](https://github.com/Yang-oah/mini-k8s/pull/9)。

G5 单独复验使用 Windows 入口 `scripts/acceptance/g5-live.ps1`。复核时，控制面留存的 `g5_live.py` 与上述 Git 提交导出的 Python 驱动逐字节一致，文件时间早于本轮预检。两者的 SHA-256 为 `75c6ee8cda4804d375b046ad6ef919c6285aca09d6f37b5478ffd41c09047954`。本次复核补记了 Python 驱动摘要，验收当时的 PowerShell 入口摘要未留档。

| 验收项 | 结果 | 内容 |
| --- | --- | --- |
| G1、G2 | PASS | 控制面、资源读写与副本对账；Go 测试、静态检查和构建 |
| G3 | PASS | Linux Worker 上的容器运行、恢复和清理 |
| G4-01～06 | PASS | 双节点调度、资源预留、失联处理、网络和伸缩 |
| G5-01～07 | PASS | 就绪探测、Service 转发、故障恢复、伸缩和清理 |
| R2-A1 双节点过载 | PASS | 普通请求返回 429 期间，两台节点心跳和测试 Pod 清理成功 |
| HTTP/2 慢请求体 | PASS | 真实 TLS、race 模式连续三轮通过 |

G5 预检曾因上一轮遗留应答文件而停止。文件已归档，修复脚本后在同一 Go 源码快照上单独重跑 G5。本轮 G5 的 `precheck.json` 记录了上述源码摘要，`g4-manifest.json` 指向本轮 G4 构建记录。

## 二、G4 双节点验收

| 编号 | 测试 | 结果 |
| --- | --- | --- |
| G4-01 | 双节点放置 | 两个副本分别在两台 Worker Running，用时 3.7 秒 |
| G4-02 | 并发资源请求 | CPU 场景 2 个 Running、4 个 InsufficientCPU；内存场景 2 个 Running、2 个 InsufficientMemory |
| G4-03 | 删除与预留释放 | 删除运行实例后，Pending Pod 在 2.9 秒后运行，预留只释放一次 |
| G4-04 | 节点控制连接中断 | 最后心跳后 18.20 秒变为 Unknown；连接恢复后容器和 sandbox 身份保持 |
| G4-05 | 跨节点通信 | 两个方向各 10 次新 TCP 连接通过，返回身份匹配 |
| G4-06 | 副本伸缩 | 2、4、1、0 四步均在 30 秒内收敛，node-b 容器、sandbox 和租约回到基线 |

## 三、G5 Service 验收

| 编号 | 测试 | 结果 |
| --- | --- | --- |
| G5-01 | 就绪与 Service | 两侧宿主机和两侧 Pod 各 20 次访问成功，窗口内访问日志及 ClusterIP 规则已保存 |
| G5-02 | 端点摘除与恢复 | 停止监听后不再命中目标；Ready 后 1.8 秒重新命中，删除后不再命中旧实例 |
| G5-03 | 无端点 | 四路各 20 次连接均在 3 秒内失败 |
| G5-04 | 组件重启与 etcd 中断 | etcd 暂停 10.1 秒；写请求在 4.011 秒返回 503，20 次业务连接成功，6.5 秒内恢复 |
| G5-05 | 节点失联与恢复 | 节点标记为 Unknown 后，失联端点被摘除。原 Pod 恢复监听后，脚本从首次确认该 Pod 已 Ready 且就绪代次与 Ready 节点一致开始计时，约 0.7 秒后首次重新命中该端点 |
| G5-06 | 20 Pod 与伸缩 | 7.3 秒全部 Ready，稳定 5 分钟；缩至 4 个和 0 个各用时 14.7 秒 |
| G5-07 | 清理 | 5.5 秒内对象、预留、两侧租约和转发规则回到测试基线 |

G5-07 后地址 `10.96.0.24` 对应声明仍为 retained，这是本轮记录的实际状态。

## 四、原始证据目录

以下路径位于控制面测试机 `10.128.176.165`。目录和文件是测试机上的原始记录。

| 内容 | 路径 |
| --- | --- |
| 本轮源码与 HTTP/2 日志 | `/home/lihaoyang/mini-k8s-g4/evidence/r2-a1-terra-final-uZp8BtGx/http2-race.log` |
| G1/G2 报告 | `/home/lihaoyang/mini-k8s-g4/evidence/r2-a1-terra-final-uZp8BtGx/artifacts/acceptance/20261106T102437Z-g2-2508512/G2.md` |
| G3 | `/home/lihaoyang/mini-k8s-g4/evidence/r2-a1-terra-final-uZp8BtGx/artifacts/acceptance/20261106T134228Z-g3-4126211/` |
| G4 | `/home/lihaoyang/mini-k8s-g4/evidence/g4cur-20261106T134648Z/` |
| R2-A1 过载探针 | `/home/lihaoyang/mini-k8s-g4/evidence/r2-a1-terra-final-uZp8BtGx/r2-a1-live-66ffa511d866/result.json` |
| G5 | `/home/lihaoyang/mini-k8s-g4/evidence/g5cur-20261107T121441Z/` |
| G5 遗留应答归档 | `/home/lihaoyang/mini-k8s-g4/evidence/g5-recovery-20261106T141606Z-531100/g5cur-ack.json` |

G4 和 G5 目录中的 `results.json` 保存各场景判定。G5 的 `g505.json` 记录端点重新命中用时 0.744 秒，`g506.json` 记录缩至 4 个和 0 个分别用时 14.714 秒和 14.692 秒。

## 五、结果的适用范围

- 测试结果适用于本轮源码快照和两台测试机的配置。20 Pod 结果属于功能与收敛验证。
- HTTP/2 慢请求体专项验证了清理请求能够进入处理器，真实节点心跳及 Pod 清理由双节点过载探针验证。
- R1 更早的并发采样和故障记录见 [R1 设计与验收记录](r1-reliability-design.md)。R2-A1 的历史失败、修复过程和不同快照见 [R2 实施与验收计划](r2-implementation-plan.md)。
- 本轮验收通过后，R2-A1 的 PR 和验收脚本修改仍需完成审阅与合并。
