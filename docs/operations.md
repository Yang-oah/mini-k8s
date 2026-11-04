# 两节点运行与排障指南（G5 开发版）

本文面向隔离测试环境，说明当前代码的配置和运行方式；文档、示例和命令尚未在最终版本上完整复验，不能用作 G5 通过证明。G5 完成前仍需按 [Gateway.md](../Gateway.md) 在同一最终版本上验证 G1–G5。项目不是 Kubernetes API 的兼容实现，不适合生产环境。

## 准备环境

需要一个控制面和两个独立 Linux Worker；两个 Worker 可以是同一物理机上的独立虚拟机，但必须各自运行内核、containerd CRI 和 CNI。控制面不运行业务 Pod。各机需要相同源码版本的 Go 1.23 或兼容版本；控制面还需要 etcd、OpenSSL、Python 3，Worker 需要 `crictl`、`iptables-save`、`iptables-restore`、bridge CNI 和 host-local 等插件。确认 CRI socket、操作权限、镜像架构和 digest，按 [G3](g3.md) 的环境说明准备。

分配三个互不重叠的网段：每个 Worker 一个实际 CNI PodCIDR、一个 Service CIDR；它们都不能与任一宿主机地址或路由重叠。预配置两台节点到对端 PodCIDR 的路由，并确认 IP 转发及防火墙。使用 CNI bridge 时，Pod 访问自身 ClusterIP 需要其桥接端口启用 hairpin。应先确认端口属于本次 Pod；CNI 的 `hairpinMode` 配置仅对重新创建后的端口生效。不同 CNI 版本还可能限制 hairpin 与 promiscuous 配置的组合，修改前先检查本机插件文档和现有工作负载。

Proxy 使用本机 iptables 项目专用链。启动前保存两台 Worker 的三张表、CRI 容器和 sandbox、CNI 租约及节点资源基线，并在运行期间禁止其他管理程序修改项目专用链或引用它们的规则。规则更新错误、归属不明、API 断连均不能假定端点已安全摘除；已知并发限制见 [G5 Service 说明](g5-service.md)。不要 flush 共享表或清空 CNI 网络目录。

## 测试证书、身份和构建

以下步骤仅用于**新建的测试目录**。在控制面确认需要的端口未被其他服务占用，把示例 API 地址和网段替换为经过核对的实际值。目录包含私钥、令牌和数据，不提交到仓库，不发送给其他人。

```sh
umask 077
export MINI_K8S_DIR="$(mktemp -d)"
# 填入两个 Worker 实际连接的控制面 IPv4 地址；192.0.2.10 是示例。
export G5_API_IP='192.0.2.10'
export G5_API="https://${G5_API_IP}:8443"
export G5_LISTEN="${G5_API_IP}:8443"
export G5_SERVICE_CIDR='10.96.0.0/24'

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

先启动测试专用 etcd；当前 API 没有配置 etcd TLS 的参数，因此只在控制面本机回环监听，不向网络暴露。确认 `2379`、`2380` 空闲后再执行：

```sh
etcd --data-dir "$MINI_K8S_DIR/etcd" \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster default=http://127.0.0.1:2380 \
  > "$MINI_K8S_DIR/etcd.log" 2>&1 &
ETCD_PID=$!
etcdctl --endpoints=http://127.0.0.1:2379 endpoint health
```

确认 etcd 健康后启动 API、manager 和 scheduler；`-service-cidr` 使用经现场确认的 IPv4 `/16` 到 `/30` 规范网段。空配置不能创建 Service。一旦地址池首次写入 etcd，同一个集群不能随意换成另一个池。

```sh
"$MINI_K8S_DIR/bin/mini-api" -listen "$G5_LISTEN" \
  -etcd http://127.0.0.1:2379 -tls-cert "$MINI_K8S_DIR/server.pem" \
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

管理员先注册 `node-a` 和 `node-b`，对应的令牌名必须与身份文件中的节点名一致。按 [G4 的节点注册示例](g4.md#测试集群启动配置)准备两份 Node 清单；清单中的 allocatable 必须符合实际可用容量，PodCIDR 必须与该 Worker 的 CNI 配置一致，不能与 Service CIDR 重叠。先 `mini apply -f <已核对节点清单>`，再在各 Worker 设置各自本地文件路径并启动 Agent、Proxy：

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
- **OOMKilled/退出码 137**：先记录该 Pod UID、节点、sandbox 与新旧 container ID，核对 `crictl inspect` 中退出原因、退出码和时间，保留该实例 workload 日志与其 requests/limits；需要时由节点管理员按时间查看 containerd/内核日志。区分容器被 OOM 杀死与其他 SIGKILL 原因。此前 64Mi 相关案例根因尚未确定，不能宣称已修复或盲目扩大限制。确认原因和受控环境后再制定改动，并重新进行该场景与 G5 验收。
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
