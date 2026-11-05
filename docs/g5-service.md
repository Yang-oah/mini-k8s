# G5 Service：双节点分步验收

本说明用于 Service 子模块，不能单独证明整个 G5 通过。正式 G5 仍须满足 [Gateway.md](../Gateway.md) 的全部检查，并关联最终版本的 G1–G4 结果；缺少环境或证据时标记 `BLOCKED`，实际错误或超时标记 `FAIL`。本说明不自动更改机器路由、防火墙或已有资源。

## 前置条件和基线

- 两个独立 Linux Worker 的 containerd、CNI、Pod 间路由和独立的 Agent 正常，readiness 子模块已在相同代码版本验证。控制面 etcd、API、scheduler、controller 与两台 Proxy 均健康；明确每个进程的版本、PID 和连接方式，并核对时钟以便跨节点比较验收时间。
- Service 地址池应与两个 PodCIDR、宿主机网络、路由及已有 Service 地址范围不重叠。API 使用 `-service-cidr` 显式配置规范的 IPv4 `/16` 至 `/30` 网段；空值禁止创建 Service。为 Proxy 配置独立的 `proxy` 只读令牌；Proxy 参数为 `-server`、`-ca-file`、`-token-file`、`-resync`、`-state-dir`。
- 使用 CNI bridge 时，Pod 通过 ClusterIP 访问自身还需要该 Pod 对应的网桥端口允许 hairpin 转发；否则即使跨节点访问正常，自身方向仍可能超时。应在两侧核对测试 Pod 的 veth 归属和 hairpin 状态；持久配置可参考 [CNI bridge 插件文档](https://www.cni.dev/plugins/current/main/bridge/)的 `hairpinMode`。CNI 配置变更不会自动修改已创建的端口，需要通过安全重建或对已确认的端口单独调整并复核。
- 为避免失联 Proxy 的旧规则在地址复用后指向错误工作负载，当前实现会永久保留已删除 Service 的 ClusterIP 声明；它不会自动回收地址，池可能耗尽。管理员可只读访问 `GET /api/mini/v1/service-pool` 检查 `active`、`retained` 声明及剩余容量；不能把 `retained` 误报为已释放。
- 使用测试专用名称、标签及受控环境。开始前记录 UTC 时间、Service/Pod/Node/RS、Node UID 和 allocatable/reserved；两侧还需记录 CRI sandbox/container、CNI 租约文件名和专用规则与宿主机既有规则基线。租约基线必须在创建工作负载之前采集；不要以事后的空目录冒充基线。
- `iptables-save` 快照可能包含其他工作负载的地址及规则；保存到权限受限的本地证据目录，发布前脱敏。禁止为了测试 flush 共享表、批量删除容器或清空 CNI 目录。
- 所有测试容器使用预先导入两节点的同一个固定 digest。每次连接是全新 TCP 连接，不复用连接；控制 plane/API 不可达期间不承诺 Proxy 时限，不把分区节点的 Proxy 当作健康 Proxy。

API 启动时使用如 `-service-cidr 10.96.0.0/24` 的参数；实际网段必须由部署者依据现场路由选择。身份文件增加独立的 `proxy` 令牌后重启 API；令牌不得与管理员、调度器、控制器或任何 Agent 的令牌相同。将代理令牌和 CA 公钥证书以受保护方式分发到两个 Worker，不分发管理员令牌或 CA 私钥。两侧以本机规则管理所需权限分别运行：

```sh
go build -o mini-proxy ./cmd/mini-proxy
sudo ./mini-proxy -server "$G5_API" -ca-file "$G5_CA_FILE" \
  -token-file "$G5_PROXY_TOKEN_FILE" -resync 2s \
  -state-dir /var/lib/mini-k8s/proxy
```

`G5_API`、`G5_CA_FILE`、`G5_PROXY_TOKEN_FILE` 必须分别指向该 Worker 可访问的 HTTPS 地址及本地文件，证书须匹配 API 地址。两个 Worker 可以使用相同的 Proxy 只读令牌；状态目录则各自留在本机。保留 Proxy 的所有权文件以便进程重启后确认规则归属，不要手工删除或复用其他目录。规则更新出现错误时先检查 Proxy 日志和三张表的项目专用链，不能假定旧规则已被安全摘除。

运行 Proxy 期间，其他防火墙管理器不得并发修改项目专用链和指向它们的跳转。该实现会拒绝接管检查时已存在的未知规则，但不能保证阻止通过 nftables 等绕过 xtables 锁的外部并发修改；[xtables-nft 手册](https://man7.org/linux/man-pages/man8/xtables-nft.8.html)说明了 nft 后端的锁行为。规则更新会先设置前置阻断，但如果设置阻断的命令本身失败，旧规则可能继续生效。遇到归属不明或规则更新失败时应保留现场并停止验收。

参考 [G4 启动配置](g4.md#测试集群启动配置)设置 `G5_API`、`G5_CA_FILE`、`G5_ADMIN_TOKEN_FILE` 和 `G5_CLI`。以下证据目录为控制面上的新私有目录；路径按实际情况替换：

```sh
umask 077
G5_EVIDENCE="$(mktemp -d)"
mini() { "$G5_CLI" --server "$G5_API" --ca-file "$G5_CA_FILE" --token-file "$G5_ADMIN_TOKEN_FILE" "$@"; }
date -u +%FT%T.%NZ > "$G5_EVIDENCE/start.txt"
mini get nodes > "$G5_EVIDENCE/nodes-before.json"
mini get pods > "$G5_EVIDENCE/pods-before.json"
mini get rs > "$G5_EVIDENCE/rs-before.json"
mini get svc > "$G5_EVIDENCE/services-before.json"
```

分别在两侧 Worker 将 CRI 清单与已知 CNI 网络租约文件名保存到本地证据目录；CNI 路径需要替换成该 Worker 的实际网络。然后保存共享表完整只读基线，并只在本机安全保存：

```sh
date -u +%FT%T.%NZ
sudo crictl pods -o json
sudo crictl ps -a -o json
sudo find /var/lib/cni/networks/实际网络名 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort
sudo iptables-save -t nat
sudo iptables-save -t filter
sudo iptables-save -t mangle
```

上述命令需分别记录两侧输出、命令退出码和时间。规则完成核对前保留两侧 Proxy 日志；不要读取令牌或私钥并写入证据。

## 可识别、可控的测试应用

`cmd/g5-service-fixture` 默认不监听业务端口，只有容器内私有 Unix socket 能开关；它在收到 `probe` 请求后返回容器 hostname，并为每次成功业务请求输出时间、来源和目标身份。readiness 的空 TCP 连接不会记作业务访问。与 API Pod 名称和 CRI sandbox hostname 对照，避免仅靠地址猜测目标。

在可信构建机上为实际 Worker 架构构建并制作镜像；以下仅示意 `amd64`。将相同固定 digest 导入两侧，镜像制作与分发由操作者完成：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o g5-service-fixture ./cmd/g5-service-fixture
cat > Dockerfile.g5-service <<'EOF'
FROM scratch
COPY g5-service-fixture /g5-service-fixture
ENTRYPOINT ["/g5-service-fixture"]
EOF
```

创建两个唯一名字的独立测试 Pod：`g5-service-a`、`g5-service-b`，两者标签均为 `app: g5-service-test`，命令为 `["/g5-service-fixture", "serve", "-listen", ":8080"]`、`restartPolicy: Always`、`readiness: {port: 8080}`，镜像为上述固定 digest。根据当前容量选择 requests/limits，确保两个 Pod 在不同 Worker 且还留出客户端运行空间；自动调度，不得手工绑定。若分布不符合要求，调整受控资源或测试配置后重新建立基线，不能把两个 Pod 位于同一节点的结果当作双节点通过。

在两个 Pod 各自 Worker 上，根据 Pod UID 从 CRI 确认 container ID；以下 `G5_CONTAINER` 是当前 Worker 上经过核验的目标 ID。对两侧分别执行并保存命令输出与时间；初始不监听时不得 Ready，开启后须 Ready：

```sh
sudo crictl exec "$G5_CONTAINER" /g5-service-fixture set -state on
sudo crictl logs "$G5_CONTAINER"
```

从控制面记录 `mini get pods`、`mini get nodes`，检查两个 Pod 的 `Running`、`ready`、Pod IP、Node UID 和 readinessEpoch。节点健康、epoch 与 Pod 一致是端点有效的必要条件；只看历史 `Ready=true` 不足以判断。

创建以下 Service：将 `mini apply` 使用的 manifest 与实际分配的 ClusterIP、Service UID 及提交时间保存。`clusterIP` 是服务端管理字段，不在创建清单填写：

```yaml
apiVersion: mini.k8s/v1
kind: Service
metadata:
  name: g5-service-test
spec:
  selector: {app: g5-service-test}
  port: 8081
  targetPort: 8080
```

```sh
mini apply -f "$G5_SERVICE_MANIFEST"
mini get svc g5-service-test
```

记录此 Service 的 ClusterIP、UID 和资源版本；等待两个健康节点的 Proxy 收敛，记录端点集合并用两侧 Pod 的访问记录确认流量实际到达。规则文本有助于诊断；Gateway 要求保存端点和规则检查结果，但不要求每个流量场景都保存一份规则全文。不得只根据 API 中 Pod Ready 推断实际到达。

## 四个来源的真实 TCP 证据

在两侧宿主机各使用与上面镜像同一源码构建的 `g5-service-fixture`；容器侧可使用两个已核验 Pod 容器本身运行同一命令。将 `G5_CLUSTER_IP`、`G5_CONTAINER` 替换为实际值，并对 **宿主机 A、宿主机 B、Pod A、Pod B** 分别采集 20 次：

```sh
# 两个宿主机各执行一次；客户端二进制路径自行设置。
"$G5_FIXTURE" check -target "$G5_CLUSTER_IP:8081" -expect g5-service-a,g5-service-b -count 20 -timeout 2500ms

# 每个已核验的测试容器各执行一次。
sudo crictl exec "$G5_CONTAINER" /g5-service-fixture check \
  -target "$G5_CLUSTER_IP:8081" -expect g5-service-a,g5-service-b -count 20 -timeout 2500ms
```

每条 JSON 对应一次新的 TCP 连接，含起止时间、来源端口、实际目标身份及结果。四组都应 20/20 成功且身份属于两 Pod；检查四组的源端口、客户端退出码，并分别保存目标容器日志。整体日志必须证实流量实际到达两侧；不要求每一组或每 20 次都均匀分布。Pod 的 CRI hostname 与响应身份必须一致。保留会话级时间戳，避免重跑覆盖失败结果。

如需持续采样 API，可在控制面设置 `G5_PROBE` 为 G4 构建的 `g4-probe` 后执行 `bash scripts/acceptance/g5-service.sh`；脚本只采样 Node 和 Pod，输出采集状态与 `Gate: NOT_EVALUATED`，不会判定 G5。采集成功退出 0、采集失败退出 1、缺少输入退出 2。周期采样不能证明采样间隙内没有短暂错误；摘除时应辅以持续的新连接记录。

## 摘除、恢复与无端点

1. 两端健康时持续从至少健康 Worker 以及对应 Pod 向 Service 创建新连接并记录时间。对一个已核验容器执行 `set -state off`，从操作成功起 10 秒内，新的连接不得再命中该容器；另一端应保持可访问。保留 Agent 探针事件、API 就绪变化和后端实际访问记录；Proxy 规则变化可作为辅助证据。不能只检查 Ready 位或历史连接。
2. 对同一容器执行 `set -state on`，记录 Agent 的新成功探测；成功后 10 秒内该端点可重新进入规则，之后实际流量须可命中该身份。然后删除测试 Pod 并记录请求成功时间，10 秒内新连接不得再命中该旧 UID/容器；确认运行时回收，不能仅看同名 Pod。
3. 单独创建 selector 不匹配任何 Pod 的 Service，或让所有测试后端不监听并等待双方 Proxy 摘除。分别从两侧宿主机和 Pod 执行：

```sh
"$G5_FIXTURE" check -target "$G5_EMPTY_CLUSTER_IP:8081" -expect-no-endpoint -count 20 -timeout 2500ms
sudo crictl exec "$G5_CONTAINER" /g5-service-fixture check \
  -target "$G5_EMPTY_CLUSTER_IP:8081" -expect-no-endpoint -count 20 -timeout 2500ms
```

本辅助工具只将真实 `ECONNREFUSED` 视为明确拒绝；客户端自行超时、无路由、成功连接或其他错误都不算此工具的成功。Gateway 要求新连接在 3 秒内失败，须结合健康节点、Service 和端点状态排除网络环境故障；无端点规则文本可用于诊断，不是 Gateway 对 G5-03 单独规定的必需文件。其他快速失败方式应结合证据人工核对，不能把工具报错直接解释成 PASS。

## 控制连接中断与恢复

沿用 G4-04 的专用本地转发器方式，仅中断一侧 Agent 到 API 的控制连接，保持 Agent 进程、容器和数据面运行，并保留健康侧 Proxy 与 API 的连接。记录最后心跳、两侧 Agent/Proxy PID、Pod UID、容器和 sandbox ID；从故障前开始连续创建新连接并记录目标。

节点变 Unknown 后，健康侧 Proxy 必须在 5 秒内不再把新连接转发到失联节点。恢复连接前让旧容器保持不监听；恢复后记录 Node readinessEpoch、Pod readinessEpoch、Agent 探针及持续的新连接结果，验证旧 Ready 没有重新把该容器加入；Proxy 规则文本可作辅助诊断。随后开启监听，记录新探测成功时间，10 秒内允许重新加入并用实际新连接验证。无法单独断开控制连接、无法观察完整时间窗口或无法证明目标身份时应为 `BLOCKED`。

## 清理与最终判定

按本轮清单和 UID 删除测试 Pod 与 Service；如涉及 RS，先按该轮操作记录缩容并删除，不删除无关对象。确认删除请求时间后 30 秒内，收集两侧 CRI sandbox/container、CNI 租约、Node reserved、API Service/Pod/RS、专用规则和共享表差异。租约及预留须回到各自**测试前**基线，测试 Service 对应规则消失，其他程序的规则不变。只在可确认所有权后处理本项目残留；失败或身份不明时保留现场，不自动删规则或网络状态。

本子模块的证据不足不能宣称整个 G5 通过。G5 还需依次验证 Agent/manager/API 重启、etcd 短暂停止恢复、20 Pod 演示、全部资源及规则清理，并在最终代码版本复验 G1–G4。

## 节点只读取证辅助脚本

在两台 Worker 的测试操作**开始前**分别启动 `scripts/acceptance/g5-node-evidence.sh`。它需要 root 读取 CRI、CNI 租约及规则，不会创建或删除 Pod、租约和规则。为避免 root 执行可被普通用户替换的程序，脚本只使用可信系统路径；如 `crictl` 不在默认系统路径，可通过 `--tool-dir` 指定由 root 拥有、组和其他用户不可写的工具目录，目录祖先及所用工具也须满足该权限条件。若已有 `crictl` 不满足这些条件，请由管理员先核验来源和完整性，再在可信目录准备 root 所有的副本，不要盲目修改共享工具的所有者。输出路径的父目录及全部祖先必须由 root 拥有且不能被组或其他用户写入；请在两侧分别创建新的私有证据根，例如：

```sh
sudo mkdir -m 0700 /var/lib/mini-k8s-evidence
```

若目录已存在，先检查其所有者与权限，不要覆盖或修改其他人的目录。以下为 node-a 示例，node-b 需使用其实际 CNI 目录（例如 `/var/lib/cni/networks/mini-k8s-b`），并在各自的新终端中分别运行；CRI endpoint 也要与本机 Agent 一致：

```sh
sudo bash scripts/acceptance/g5-node-evidence.sh \
  --output /var/lib/mini-k8s-evidence/g5-run-a \
  --lease-dir /var/lib/cni/networks/mini-k8s \
  --cri-endpoint unix:///run/containerd/containerd.sock \
  --tool-dir /usr/local/bin --iptables full --interval 1
```

脚本默认采集项目管理的全部 CRI 对象；可用 `--pod-uid` 指定最多四个 UID。每个已退出容器会写入 `exits.txt`（指定 UID 时为 `exits-<uid>.txt`），包含容器 ID、`reason`、退出码、attempt、起止时间和内存上限，并在采集器标准错误首次打印 `exit-observed`。`reason=OOMKilled` 或退出码 137 时，另外保存该结束时间前后 30 秒的内核日志和 containerd 日志，文件为 `oom-kernel.out` 与 `oom-containerd.out`（指定 UID 时文件名带 `exits-<uid>`）。`session.txt` 的 `exit_observations` 和 `oom_observations` 按容器 ID 去重，同一容器留在后续样本中只计一次。这些文件只说明采样看到了什么，不表示 Gate 通过。需要容器访问日志时，核实本轮 container ID，再另外启动带 `--log-container <完整容器ID>` 的采集器；最多两个容器，不能仅凭名称猜测。可额外指定 `--stats` 为这些容器采集 CRI 瞬时资源统计，用于排查 OOM；统计不是连续内存峰值，不能单独确定 OOM 原因。日志和统计采集需在容器删除前结束：删除后读取可能失败，错误会如实记录。可使用另一个不带 `--log-container` 的采集器贯穿 G5-07 删除及其后 30 秒。

G5-07 使用 `--iptables full` 时，脚本只有在某轮租约、所选 CRI 范围及三张规则表都采样成功后才生成证据目录下的 `READY` 文件，并打印其路径。两侧均出现 `READY`、核对该样本和测试前基线，且确认采集器仍在运行后，才开始删除；文件仅表示某轮采样成功，不能证明之后每轮都成功或自动判定 Gate。默认不传 `--pod-uid` 才覆盖项目管理的所有 CRI 对象；指定 UID 时只覆盖指定范围。权限不足或可信工具不可用时先修复环境，不要先删除测试对象。

在独立终端用 Ctrl+C 结束相应采集器后，检查 `session.txt` 和每个 `sample-*/` 下的 `.meta`、`.err`、`.out`。完整 iptables 快照与容器日志可能包含无关敏感信息，仅保存在受保护的证据目录，发布前脱敏，不提交 Git。日志窗口有重叠，可能包含重复行；轮转、限额、截断或采样间隔缺口会影响完整性。每轮采样本身需要时间，`--interval 1` 不代表严格每秒取得一次结果。对 30 秒断言必须用操作时间、各样本时间和每项退出码核对覆盖范围；任何错误或证据缺口不能算通过。脚本退出 0 只表示采集命令未报告错误，并不表示 G5 通过。

## 全量验收时容易遗漏的证据

- 分别重启两台 Worker 的 Agent；每次先记录进程、Pod UID、sandbox 和容器 ID，确认恢复后实例不重复，再执行下一项。
- 在独立可恢复的 etcd 暂停场景中记录暂停及恢复的准确时间、写请求的起止时间和 HTTP 状态，以及期间独立业务连接；客户端超时不能视为已收到 503。
- 节点断连恢复测试应在重新开启监听**之前**启动连续 TCP 和 API 采样，并持续至少到新探测成功后 10 秒；同时记录目标实例身份。规则记录可辅助诊断，不能用后来补采的旧流量窗口代替。
- 20 个 Pod 缩容至 4 时，记录实际运行数量以及正在删除的对象，按 Gateway 核对 30 秒内实际数量收敛。API 列表总数和清理状态是重要诊断信息，但不把额外列表时限冒充 Gateway 的独立断言。
- 清理检查从删除操作成功时开始，持续采集两侧 CRI、节点预留、各自 CNI 租约及规则；30 秒内的有效结果须能与操作前基线比较。访问记录即使在容器删除后复制，只要能核实来源、身份、时间和完整性，也可用于评估；读取失败或内容不完整时不能推断通过。
