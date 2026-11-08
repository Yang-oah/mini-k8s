# 两节点运行与排障指南（G5 开发版）

本文面向隔离测试环境，说明当前代码的配置和运行方式。2026-11-05 用这份源码在控制面按下面的变量步骤建成一套只监听 `127.0.0.1:18443` 的临时集群：etcd 健康、六个程序完成构建、`mini get nodes` 成功，并完成 [examples/g5-replicaset.yaml](../examples/g5-replicaset.yaml) 与 [examples/g5-service.yaml](../examples/g5-service.yaml) 的创建、`scale rs g5-demo --replicas 0` 和删除。该临时目录随后已清掉，没有连接 `10.128.176.165:8443` 上的验收集群。Worker 上的 Agent 和 Proxy 命令与当前程序参数一致，但验收集群已经占用 CRI、状态目录和项目 iptables 链，这次没有再启动第二套。因此本文还不能单独作为 G5 通过证明。项目不是 Kubernetes API 的兼容实现，不适合生产环境。

## 准备环境

需要一个控制面和两个独立 Linux Worker；两个 Worker 可以是同一物理机上的独立虚拟机，但必须各自运行内核、containerd CRI 和 CNI。控制面不运行业务 Pod。各机需要相同源码版本的 Go 1.23 或兼容版本；控制面还需要 etcd、OpenSSL、Python 3，Worker 需要 `crictl`、`iptables-save`、`iptables-restore`、bridge CNI 和 host-local 等插件。确认 CRI socket、操作权限、镜像架构和 digest，按 [G3](g3.md) 的环境说明准备。

分配三个互不重叠的网段：每个 Worker 一个实际 CNI PodCIDR、一个 Service CIDR；它们都不能与任一宿主机地址或路由重叠。预配置两台节点到对端 PodCIDR 的路由，并确认 IP 转发及防火墙。使用 CNI bridge 时，Pod 访问自身 ClusterIP 需要其桥接端口启用 hairpin。应先确认端口属于本次 Pod；CNI 的 `hairpinMode` 配置仅对重新创建后的端口生效。不同 CNI 版本还可能限制 hairpin 与 promiscuous 配置的组合，修改前先检查本机插件文档和现有工作负载。

Proxy 使用本机 iptables 项目专用链。启动前保存两台 Worker 的三张表、CRI 容器和 sandbox、CNI 租约及节点资源基线，并在运行期间禁止其他管理程序修改项目专用链或引用它们的规则。规则更新错误、归属不明、API 断连均不能假定端点已安全摘除；已知并发限制见 [G5 Service 说明](g5-service.md)。不要 flush 共享表或清空 CNI 网络目录。

## 测试证书、身份和构建

以下步骤仅用于**新建的测试目录**。在控制面确认需要的端口未被其他服务占用，把示例 API 地址和网段替换为经过核对的实际值。目录包含私钥、令牌和数据，不提交到仓库，不发送给其他人。

```sh
umask 077
export MINI_K8S_DIR="$(mktemp -d)"
# 填入两个 Worker 实际连接的控制面 IPv4 地址。只在本机验证 API 时可用 127.0.0.1。192.0.2.10 是示例。
export G5_API_IP='192.0.2.10'
export G5_API_PORT='8443'
export G5_API="https://${G5_API_IP}:${G5_API_PORT}"
export G5_LISTEN="${G5_API_IP}:${G5_API_PORT}"
export G5_SERVICE_CIDR='10.96.0.0/24'
# 默认使用空闲的回环端口。已被其他 etcd 占用时改成未使用的回环端口，不要连接已有数据目录。
export G5_ETCD_CLIENT_URL='http://127.0.0.1:2379'
export G5_ETCD_PEER_URL='http://127.0.0.1:2380'

openssl req -x509 -newkey rsa:3072 -nodes -days 1 \
  -keyout "$MINI_K8S_DIR/ca.key" -out "$MINI_K8S_DIR/ca.pem" \
  -subj '/CN=mini-k8s test CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign'
openssl req -new -newkey rsa:3072 -nodes \
  -keyout "$MINI_K8S_DIR/server.key" -out "$MINI_K8S_DIR/server.csr" \
  -subj '/CN=mini-k8s-test'
printf 'subjectAltName=IP:127.0.0.1,IP:%s\nextendedKeyUsage=serverAuth\n' "$G5_API_IP" > "$MINI_K8S_DIR/server.ext"
openssl x509 -req -in "$MINI_K8S_DIR/server.csr" \
  -CA "$MINI_K8S_DIR/ca.pem" -CAkey "$MINI_K8S_DIR/ca.key" -CAcreateserial \
  -out "$MINI_K8S_DIR/server.pem" -days 1 -extfile "$MINI_K8S_DIR/server.ext"

python3 - <<'PY'
import json, os, pathlib, secrets
directory = pathlib.Path(os.environ['MINI_K8S_DIR'])
names = ('admin', 'scheduler', 'controller', 'proxy', 'node-a', 'node-b')
tokens = {name: secrets.token_hex(32) for name in names}
for name, token in tokens.items():
    with (directory / (name + '.token')).open('x', encoding='utf-8') as output:
        output.write(token + '\n')
auth = {name: tokens[name] for name in ('admin', 'scheduler', 'controller', 'proxy')}
auth['agents'] = {name: tokens[name] for name in ('node-a', 'node-b')}
with (directory / 'auth.json').open('x', encoding='utf-8') as output:
    json.dump(auth, output)
PY
```

API 地址必须匹配证书的 SAN；上面示例仅支持 IPv4 地址。如使用 DNS，需生成带相应 DNS SAN 的证书并设置对应 `G5_API`。节点令牌彼此不同，`proxy` 是只读身份。只把 CA 公钥证书、对应节点令牌和 Proxy 令牌分别安全分发给 Worker；不要分发 CA 私钥、服务端私钥、管理员或其他节点令牌。令牌内容不要放进命令参数或证据文件。

在控制面仓库根目录构建；两侧 Worker 也应构建相同源码版本及其架构的 `mini-agent` 和 `mini-proxy`，或从可信构建机分发二进制：

```sh
mkdir "$MINI_K8S_DIR/bin"
go build -o "$MINI_K8S_DIR/bin/mini-api" ./cmd/mini-api
go build -o "$MINI_K8S_DIR/bin/mini-k8s" ./cmd/mini-k8s
go build -o "$MINI_K8S_DIR/bin/mini-manager" ./cmd/mini-manager
go build -o "$MINI_K8S_DIR/bin/mini-scheduler" ./cmd/mini-scheduler
go build -o "$MINI_K8S_DIR/bin/mini-agent" ./cmd/mini-agent
go build -o "$MINI_K8S_DIR/bin/mini-proxy" ./cmd/mini-proxy
```

## 按顺序启动

先启动测试专用 etcd；当前 API 没有配置 etcd TLS 的参数，因此只在控制面本机回环监听，不向网络暴露。确认 `G5_ETCD_CLIENT_URL` 和 `G5_ETCD_PEER_URL` 的端口空闲后再执行：

```sh
etcd --data-dir "$MINI_K8S_DIR/etcd" \
  --listen-client-urls "$G5_ETCD_CLIENT_URL" \
  --advertise-client-urls "$G5_ETCD_CLIENT_URL" \
  --listen-peer-urls "$G5_ETCD_PEER_URL" \
  --initial-advertise-peer-urls "$G5_ETCD_PEER_URL" \
  --initial-cluster "default=${G5_ETCD_PEER_URL}" \
  > "$MINI_K8S_DIR/etcd.log" 2>&1 &
ETCD_PID=$!
etcdctl --endpoints="$G5_ETCD_CLIENT_URL" endpoint health
```

确认 etcd 健康后启动 API、manager 和 scheduler；`-service-cidr` 使用经现场确认的 IPv4 `/16` 到 `/30` 规范网段。空配置不能创建 Service。一旦地址池首次写入 etcd，同一个集群不能随意换成另一个池。

```sh
"$MINI_K8S_DIR/bin/mini-api" -listen "$G5_LISTEN" \
  -etcd "$G5_ETCD_CLIENT_URL" -tls-cert "$MINI_K8S_DIR/server.pem" \
  -tls-key "$MINI_K8S_DIR/server.key" -auth-file "$MINI_K8S_DIR/auth.json" \
  -service-cidr "$G5_SERVICE_CIDR" > "$MINI_K8S_DIR/api.log" 2>&1 &
API_PID=$!
"$MINI_K8S_DIR/bin/mini-manager" -server "$G5_API" -ca-file "$MINI_K8S_DIR/ca.pem" \
  -token-file "$MINI_K8S_DIR/controller.token" -resync 2s > "$MINI_K8S_DIR/manager.log" 2>&1 &
MANAGER_PID=$!
"$MINI_K8S_DIR/bin/mini-scheduler" -server "$G5_API" -ca-file "$MINI_K8S_DIR/ca.pem" \
  -token-file "$MINI_K8S_DIR/scheduler.token" -resync 2s > "$MINI_K8S_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$!
mini() { "$MINI_K8S_DIR/bin/mini-k8s" --server "$G5_API" --ca-file "$MINI_K8S_DIR/ca.pem" --token-file "$MINI_K8S_DIR/admin.token" "$@"; }
mini get nodes
```

管理员先注册 `node-a` 和 `node-b`，对应的令牌名必须与身份文件中的节点名一致。按 [G4 的节点注册示例](g4.md#测试集群启动配置)准备两份 Node 清单；清单中的 allocatable 必须符合实际可用容量，PodCIDR 必须与该 Worker 的 CNI 配置一致，不能与 Service CIDR 重叠。先 `mini apply -f <已核对节点清单>`，再在各 Worker 设置各自本地文件路径并启动 Agent、Proxy。下面的命令会占用该 Worker 的 CRI、状态目录和项目 iptables 链；已有 Agent 或 Proxy 在使用这些路径时，不要再启动第二套。

```sh
# 在每个 Worker 单独设置；API、CA、代理令牌路径按现场填写。
export G5_NODE='node-a'  # 另一台填写 node-b
export G5_API='https://实际控制面地址:8443'
export G5_CA_FILE='/安全目录/ca.pem'
export G5_NODE_TOKEN_FILE='/安全目录/本节点.token'
export G5_PROXY_TOKEN_FILE='/安全目录/proxy.token'
export G5_AGENT='/实际路径/mini-agent'
export G5_PROXY='/实际路径/mini-proxy'

sudo "$G5_AGENT" -server "$G5_API" -node "$G5_NODE" \
  -ca-file "$G5_CA_FILE" -token-file "$G5_NODE_TOKEN_FILE" \
  -cri-endpoint unix:///run/containerd/containerd.sock \
  -state-dir /var/lib/mini-k8s/agent -log-dir /var/log/mini-k8s -resync 2s

# 在该 Worker 的另一个终端运行。
sudo "$G5_PROXY" -server "$G5_API" -ca-file "$G5_CA_FILE" \
  -token-file "$G5_PROXY_TOKEN_FILE" -state-dir /var/lib/mini-k8s/proxy -resync 2s
```

以上前台进程在终端退出时会受影响；生产式托管不在本演示范围内。两侧的状态目录分别留在本机，权限须满足程序要求。不要删除或复用 Proxy 所有权文件；Proxy 退出不会自动移除已安装规则。启动后在控制面 `mini get nodes`，确认两节点 Ready 和心跳；查看各进程日志并核对证书、身份和 CRI。节点就绪不代表完整验收通过。

## 双 Worker 复现核对

以下是现场操作清单，尚未在本文所述的占用中集群上重新执行 Worker 启动。每次使用独立测试环境，先记录源码提交和源码摘要、二进制构建来源、两机架构、containerd/CNI 版本、时间和配置；不可用旧版本的验收结果替代本次结果。

1. 两台机器分别核对节点名、专属令牌、CRI endpoint、CNI 配置及实际 PodCIDR，验证到控制面 HTTPS 的证书与连通性，以及到对端 PodCIDR 的路由。确认镜像 digest 在两台均可用，记录原有 CRI、租约、资源预留和规则基线。
2. 控制面注册匹配的两个 Node，分别在各 Worker 启动对应 Agent 和 Proxy，记录进程身份与日志。确认两节点心跳、状态和 API 中的 PodCIDR；发生冲突或权限不足时停止并记录 `BLOCKED`，不要复用正在使用的状态目录或规则链。
3. 按下一节提交测试资源并记录 UID；确认各 Pod 的节点、CRI 身份和 Ready 状态，再从两侧宿主机和 Pod 分别建立新连接，核对目标实例。完整检查及故障注入按 [G4](g4.md) 和 [G5](g5-service.md) 执行，并记录每项操作、时间、结果与证据。
4. 按下节仅清理本轮资源，对照两侧基线核对 CRI、租约、预留和规则；保留日志、退出状态及清理结果。检查未执行或证据不足记为 `BLOCKED`，断言失败记为 `FAIL`，不能由节点 Ready 推断整体通过。

已按上述步骤配置验收环境时，可在 Windows 工作区一次启动 G1～G5。先把同一份源码放到 node-a，使用 Linux 计算摘要：

```bash
{ find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum
```

```powershell
$env:R1_SOURCE_DIR = '/path/to/source-snapshot'
$env:R1_EXPECTED_SHA = '<上一步输出的 64 位摘要>'
pwsh -File .\scripts\acceptance\run-live-gates.ps1
```

连接参数见 [共用配置](../scripts/acceptance/r1-live-config.ps1)。两侧 SSH 主机密钥需事先核对，运行中仍要在终端按提示输入 sudo 密码。入口先执行包含 G1/G2 的 G3，再依次执行 G4 和 G5，核对新证据的摘要、场景判定和运行锁。某关失败即停止，末尾的 `Summary` 是本机 JSON 汇总。排查后可用相同的配置单独运行对应入口，保留原始证据。该入口沿用现有验收集群的节点名、CRI/CNI 和故障注入约定。

验收 R2-A1 时加 `-IncludeR2A1`。它在 G4 通过后、G5 开始前对当前 API 执行过载探针，观察 429 窗口内两台 Agent 的心跳和测试 Pod 的清理，并把结果及证据路径写入同一份汇总。

## 20 Pod 和 Service 示例

[g5-replicaset.yaml](../examples/g5-replicaset.yaml) 使用 G5 要求的 20 副本、每 Pod requests 50m/64Mi 与 limits 100m/128Mi；[g5-service.yaml](../examples/g5-service.yaml) 按标签选择它们并将 TCP 8080 转发到目标 8080。ClusterIP 由 API 分配，不要在清单中填写。两个清单的名称和 `app` 标签必须保持一致，并保证测试名称和 selector 不与现有 RS 冲突。

**先编辑镜像占位符**：换成两个 Worker 均已预拉取的真实 `image@sha256:<digest>`。示例未指定 command，镜像默认进程必须在启动后监听 TCP 8080；若要用 G5 的身份验证工具，还必须按该工具的协议响应并能区分实例。请确认程序在 128Mi limit 内可靠运行。`g5-service-fixture` 默认不监听，直接作为上述示例镜像不会自动让 20 个 Pod Ready；它的手动开关用途见 [G5 Service 说明](g5-service.md)。

```sh
mini apply -f examples/g5-replicaset.yaml
mini apply -f examples/g5-service.yaml
mini get rs g5-demo
mini get pods
mini get svc g5-demo
```

记录提交时间、ClusterIP、RS/Service/Pod UID 和实际版本。先确认 Pod 的运行状态、各节点状态及 readinessEpoch，再按 [Gateway G5](../Gateway.md#7-g5service-与整体-demo) 从两侧宿主机和 Pod 执行新 TCP 连接与取证。`Running` 不等于 `Ready`；不配置 readiness 的 Pod 在当前实现中不会自动作为 Ready 端点。需确认端点分布，不能假设调度器一定均衡分布。

## 按 UID 排查与清理

- **Pod Pending、资源不足**：记录 Pod 的 reason、UID、绑定和资源请求，比较 `mini get nodes` 中两侧 allocatable/reserved、节点 Ready 和 unschedulable。确认后从 scheduler、controller、API 日志查证；不要手工绑定替代正式调度。
- **Pod Running 但 NotReady**：确认 readiness 端口、镜像启动行为、Pod IP、CRI 运行实例；在对应 Worker 根据 Pod UID 查询 `sudo crictl pods -o json`、`sudo crictl ps -a -o json` 后核对 ID，再用 `sudo crictl inspect <已核对容器ID>` 和 `sudo crictl logs <已核对容器ID>`。Agent 日志中的 `first_failure`、`failure_threshold`、`recovered` 可与 API 和容器时间线关联；旧容器或旧节点 epoch 的成功结果不能证明当前就绪。
- **OOMKilled/退出码 137**：先看 Agent 日志里的 `exited reason=`，以及节点采集器的 `exits.txt`、`oom-kernel.out` 和 `oom-containerd.out`。记录 Pod UID、节点、sandbox、新旧 container ID、退出码、内存上限和 cgroup 用量。区分本容器 memory cgroup 被打满与宿主机整体缺内存。2026-11-04 的 g5c-a 是容器自己的 64 MiB cgroup 被打满：被杀进程用户态约 18 MiB，同一 cgroup 还有第二个测试进程约 12 MiB，另有内核栈等内核内存；随后同限的新容器处理了更多请求且没有再次 OOM。因此不能把所有 Pod 的内存上限一律提高，也不能宣称 64 MiB 对单个测试进程一定不够。确认具体容器和负载后再决定是否调整该容器上限，并重新验收。
- **Service 访问失败**：先确认两个 Pod 是否 Ready、Pod 与 Node 的 `readinessEpoch` 相符、Service selector/ClusterIP/端口和两节点路由正确；在对应 Worker 核对相关 UID 的访问日志及 `iptables-save -t nat`、`-t filter`、`-t mangle` 项目专用链。Pod 自访问失败时检查该 Pod 对应 veth 的 hairpin；不要凭名称猜测或改动其他端口。保存规则前应考虑其包含其他工作负载的地址和规则，仅在受保护的证据目录留存。
- **Service 地址耗尽**：当前删除 Service 后保留该 ClusterIP 的声明，避免离线 Proxy 的旧规则误投到新工作负载，因此地址不会自动回收。管理员可只读访问 `/api/mini/v1/service-pool` 检查 `active`、`retained` 和剩余容量；不能把 retained 当成已释放。不要手工删除租约或重置池来掩盖问题。

完成测试时先记录本轮 RS、Service、Pod 的 UID 和两侧基线，再删除本轮 Service、将本轮 RS 缩到零：

```sh
mini delete -f examples/g5-service.yaml
mini scale rs g5-demo --replicas 0
mini get pods
mini get nodes
```

持续核对原 UID 对应的 API Pod、两侧 CRI container/sandbox、CNI 租约文件名和 reserved 已清理或回到测试前基线。确认后才删除该 RS：

```sh
mini delete -f examples/g5-replicaset.yaml
mini get rs
mini get svc
```

检查被删 Service 的专用转发规则已消失、其他程序规则不变；不要执行批量 `crictl rm`、批量删除网络目录、清空 iptables 表或删除不明所有权的链。失败或清理无法确认时保留 UID、时间和现场，不重复释放预留。最后仅停止本轮已记录且核实身份的进程；保留必要日志和证据，并按管理员的测试环境策略处理私有测试目录。有关断连、重启、计时和 30 秒清理窗口的操作和证据要求见 [G5 Service 验收说明](g5-service.md)。
