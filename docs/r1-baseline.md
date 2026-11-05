# R1-A：只读 API 容量基线

本工具是 [R1 详细设计](r1-reliability-design.md) 的第一步，用于测量不同客户端并发下的 API 列表读取，不修改集群状态，不判定 Gate，不等同于完整的 etcd、写入或副本收敛基线。工具不保存响应正文和令牌；证据仍可能包含 API 地址及环境信息，发布前应检查。

## 准备

在与控制面 HTTPS 可连通的 Linux 主机准备 Python 3、API CA 和具有目标资源读取权限的令牌文件。沿用已核实的 `G5_API`、`G5_CA_FILE` 和 `G5_ADMIN_TOKEN_FILE` 环境变量；令牌只作为文件读取，不要把令牌本身写进命令或报告。URL 必须是与证书匹配的 HTTPS origin，脚本不跟随重定向，也不使用系统 HTTP 代理。

记录本轮源码版本或统一口径的源码树摘要、二进制摘要、Go/etcd/containerd/CNI 版本、集群 CPU/内存、已有对象和背景负载、镜像 digest 及测试时段。`--source-sha256` 和 `--image-digest` 为可选的操作者填写值，不是脚本独立核验结果。不要把不同版本或环境下的数据直接作为优化前后对比。

脚本只读取已有对象，不创建或删除工作负载。如果要比较 20 和 40 个对象，须由操作者在明确隔离的测试环境单独准备与清理，并记录对象组成和实际数量；不得为了测量改动已有业务。至少在单客户端试跑成功、确认系统资源充足后，再按需增加并发。

## 采集

证据目录必须是不存在的新目录，父目录应提前准备为操作者专属的私有目录。每个并发档位单独执行；例如在已确认的测试环境：

```sh
umask 077
mkdir -p "$HOME/r1-evidence"
for clients in 1 8 32; do
  python3 scripts/acceptance/r1-baseline.py \
    --server "$G5_API" --ca-file "$G5_CA_FILE" \
    --token-file "$G5_ADMIN_TOKEN_FILE" \
    --output "$HOME/r1-evidence/read-${clients}" \
    --concurrency "$clients" --requests-per-worker 20 --interval-ms 100 || break
done
```

上述数值只是初始实验点，不能视为系统容量保证；32 并发需要操作者确认测试环境能承受。每个 worker 总共执行 `--requests-per-worker` 次 GET，默认轮换 pods、nodes、replicasets、services；不是每类资源各执行 20 次。可重复传入 `--resource pods` 等参数限定资源，按实际授权选择。`--interval-ms 0` 会产生有界突发，请谨慎使用；采集发生认证失败、超时或过载时停止扩大负载并保留结果。

## 结果与核对

- `metadata.json`：参数、采集时段起点、操作者提供的版本信息及未采集项目。
- `requests.jsonl`：每次请求的资源类型、时间、状态码、耗时、数量或错误类别；没有响应正文和认证头。
- `summary.json`：请求总数、成功/错误数量、状态码、实际请求速率，以及**仅成功请求**的 P50/P95/P99；错误率必须与延迟一起看。

`--timeout-seconds` 是底层连接和读操作的 socket 超时，不是整个请求的严格墙钟时限；如果对端持续缓慢发送，单次请求可能更久。验收时应在受控网络中运行，并保留实际耗时。

退出码 `0` 表示本轮请求均成功，`1` 表示已采集但存在请求错误，`2` 表示参数或环境准备失败。任何退出码都不代表系统达到容量目标或 R1-A 已完成；`Gate: NOT_EVALUATED` 是预期标记。若脚本中断，可能只有部分文件或无汇总，保留原目录并另用新目录重试。

API 脚本未采集 etcd 内部请求量、事务延迟、Watch 数、存储占用、进程 CPU/内存、写入吞吐或 ReplicaSet 收敛；不要把客户端请求速率解释为 etcd QPS。完整 R1-A 基线要关联这些额外记录以及环境清单，再与后续变更按同口径比较。

## etcd 本机指标快照

在 etcd 所在的控制面主机，确认指标端点使用哪个本机地址和端口；下面的地址只适用于明确启用了此端点的环境。不要为了采集而将 etcd 端口暴露到网络。以下命令分别在 API 采样前后执行，输出文件必须不存在：

```sh
python3 scripts/acceptance/r1-etcd-snapshot.py \
  --url http://127.0.0.1:2379/metrics --output "$HOME/r1-evidence/etcd-before.json"

# 此处按上面的步骤执行选定档位的 API 采样。

python3 scripts/acceptance/r1-etcd-snapshot.py \
  --url http://127.0.0.1:2379/metrics --output "$HOME/r1-evidence/etcd-after.json"
```

脚本只接受数字回环地址，HTTPS 时可指定 `--ca-file`；它仅保存无标签的白名单指标与采集时间，不保存原始 metrics 文本。白名单包括 MVCC range、put、delete、txn 的累计次数，以及数据库大小、提交、磁盘和进程指标。缺失、带标签或存在歧义的指标分别标记，不推断为零。计数器和直方图的 count/sum 是累计时点值，只有确认进程未重启、指标含义一致并结合两次时间间隔后才能计算变化；环境中的其他流量也会影响差值。指标采集成功仍不是 Gate 通过。

纯本地脚本逻辑测试由验证者在隔离目录运行：

```sh
python3 scripts/acceptance/r1-baseline-test.py
python3 scripts/acceptance/r1-etcd-snapshot-test.py
```

测试结果、实际采集的路径和源码版本应一起记录；完成后仅由操作者清理本轮可确认的测试资源。
