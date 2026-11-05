# G5：TCP readiness 分步验收

本说明仅覆盖 G5 的 readiness 子模块，不代表 G5-01～07 全部通过：Service、Proxy 和整个 G5 的验收仍须按 [Gateway.md](../Gateway.md) 另行完成。已有 [G4 两节点环境](g4.md)须处于健康状态；准备独立的测试 Pod、固定 digest 镜像和证据目录。测试由操作者执行，脚本不会操作或清理现有工作负载。

## 通过依据

在真实 Linux Worker、containerd 和 CNI 上采集 API 状态、容器和网络侧证据，至少覆盖：

1. 容器 Running 但 TCP 未监听时保持 NotReady；首次 TCP 探测成功后变为 Ready。Pod 必须有真实 Pod IP，不能仅通过容器宿主机 localhost 替代。
2. 在同一 Running 容器中关闭 TCP：首次失败后不应立即从 Ready 变为 NotReady；连续第二次失败后不就绪。记录探测时间、失败次数和 API 转变时间，以核对约 2 秒间隔和每次 1 秒超时。
3. 恢复同一容器的监听，新的成功探测后重新 Ready；重复关闭、恢复不能因旧状态直接 Ready。
4. 在测试 Pod 曾经 Ready 后终止其容器。确认旧容器退出后旧 Ready 已失效；新容器实例在首次成功探测之前不能沿用旧 Ready。记录新旧 container ID、事件与 API 时间线；旧进程退出但新的监听尚未打开期间必须可观察。
5. 在保持容器和数据面运行的前提下中断 Agent 到 API 的控制连接，观察节点 Unknown；断连时保持 TCP 不监听。恢复控制连接后，即使旧 Pod 状态曾是 Ready，也不得在新探测成功前重新作为 Ready 使用。记录 Agent、Pod UID、容器 ID、Pod IP、节点状态及所有转变时刻。
6. 清理本次测试资源后，核对两侧 CRI sandbox/container、IP 租约和节点预留回归测试前基线。任何需保留以诊断的资源都先记录 UID 和运行时 ID。

若实现尚未提供探测失败次数、探测请求时间或当前容器身份的可验证证据，不能仅根据 `Ready` 的低频采样声称第 2 项或第 4、5 项通过；记录为 `BLOCKED` 并保留现场。实际断言错误或明确超时记 `FAIL`；环境、权限或证据缺失记 `BLOCKED`。不设置 readiness 的 Pod 行为必须遵照当前版本声明的语义，不得自行推断。

## 构建测试镜像

项目中 `cmd/g5-readiness-fixture` 是测试用工作负载：初始保持 TCP 关闭，`set` 只通过容器内 Unix socket 控制监听，不在网络端口上提供控制接口。它的每次实际切换返回 UTC JSON 事件。Unix socket 默认为容器内 `/g5-control/control.sock`，其父目录仅允许当前用户访问；业务端口示例为 8080。

以下只生成二进制和 Dockerfile；构建、传输和导入镜像由操作者按现场方式执行。目标架构不是 amd64 时修改 `GOARCH`，在两台 Worker 预置**同一个固定 digest**，不自动从公网拉取：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o g5-readiness-fixture ./cmd/g5-readiness-fixture
cat > Dockerfile.g5-readiness <<'EOF'
FROM scratch
COPY g5-readiness-fixture /g5-readiness-fixture
ENTRYPOINT ["/g5-readiness-fixture"]
EOF
```

此镜像仅用于受控测试。控制 socket 不应 bind-mount 到宿主机或暴露给其他容器。测试中只对本次 Pod 的已核对 container ID 执行操作，避免误操作共享节点。

## 创建测试工作负载并采样

控制面先准备已有的 API URL、CA、管理员令牌文件与 `mini-k8s`，参考 [G4 启动说明](g4.md#测试集群启动配置)。记录节点、Pod、CRI 和 host-local 租约基线；复制产物时避免复制令牌与私钥。以下变量的路径和镜像须替换成现场值：

```sh
export G5_API='https://控制面地址:8443'
export G5_CA_FILE='/实际路径/ca.pem'
export G5_ADMIN_TOKEN_FILE='/实际路径/admin.token'
export G5_PROBE='/实际路径/g4-probe'
export G5_CLI='/实际路径/mini-k8s'
export G5_TEST_IMAGE='已预拉取的镜像@sha256:真实digest'
umask 077
G5_EVIDENCE="$(mktemp -d)"
mini() { "$G5_CLI" --server "$G5_API" --ca-file "$G5_CA_FILE" --token-file "$G5_ADMIN_TOKEN_FILE" "$@"; }
mini get nodes > "$G5_EVIDENCE/nodes-before.json"
mini get pods > "$G5_EVIDENCE/pods-before.json"
```

使用独占测试环境和唯一名称，避免与现有工作负载冲突；示例的资源请求须确认可用：

```sh
cat > "$G5_EVIDENCE/pod.yaml" <<EOF
apiVersion: mini.k8s/v1
kind: Pod
metadata:
  name: g5-ready-manual
spec:
  image: $G5_TEST_IMAGE
  command: ["/g5-readiness-fixture", "serve", "-listen", ":8080"]
  restartPolicy: Always
  resources:
    requests: {cpuMilli: 100, memoryBytes: 33554432}
    limits: {cpuMilli: 200, memoryBytes: 67108864}
  readiness: {port: 8080}
EOF
mini apply -f "$G5_EVIDENCE/pod.yaml"
mini get pods g5-ready-manual
```

确认调度节点和 Pod UID 后，在该 Worker 用 `sudo crictl pods -o json` 与 `sudo crictl ps -a -o json` 按 UID 核对 sandbox、container ID、状态和 IP。不要根据名称或列表顺序猜测 ID；下文 `G5_CONTAINER` 是本次核对后的容器 ID。

在控制面另一个 shell 中启动 API 采样；在采样窗口内执行各次切换操作。采样工具 `g4-probe` 的构建方法见 [G4 文档](g4.md)。采样间隔只是请求间隔，并非原子/连续观察；每条记录有请求起止时间。根据实际操作时长调整 duration，避免采样中途结束：

```sh
export G5_OBSERVE_DURATION=180s G5_OBSERVE_INTERVAL=200ms
bash scripts/acceptance/g5-readiness.sh
```

采样结束后脚本输出证据目录、采集状态和 `Gate: NOT_EVALUATED`：它本身无法证明整个子模块或 G5 通过。采集成功退出 0、采集失败退出 1、缺少输入退出 2。将该目录中的 `api-series.jsonl`、采样错误与以下操作事件一起保存，确认采样贯穿实际变更窗口；不含完整窗口或有采样错误则不能判通过。

## 在同一容器中切换监听

在运行测试 Pod 的 Worker 上操作；下面各条 JSON 中 `at` 由 fixture 在监听操作完成后产生。分别重定向到证据文件并记录命令退出码。`sudo crictl logs "$G5_CONTAINER"` 可读 fixture 日志，供交叉核对；不要将无关容器日志收集到证据中。

```sh
# 先确认初始 Running、NotReady；再开始监听。
sudo crictl exec "$G5_CONTAINER" /g5-readiness-fixture set -state on

# 已 Ready 后关闭监听；等待并核对两次失败及 NotReady，再恢复监听。
sudo crictl exec "$G5_CONTAINER" /g5-readiness-fixture set -state off
sudo crictl exec "$G5_CONTAINER" /g5-readiness-fixture set -state on
```

两条切换命令不要紧挨着执行；先按需观察状态和探测证据。Agent 日志按 Pod UID 记录 `readiness` 事件：`first_failure`、`failure_threshold` 和 `recovered` 分别表示首次失败、达到两次连续失败和恢复成功；日志还包含失败计数与事件时间。保存这些事件并与 API 采样、fixture 切换时间关联，不能仅凭 fixture 的监听开关时间推断探测次数。若日志缺失或时间无法关联，可在 Worker 上捕获该 Pod IP 与端口的 TCP 探测请求并区分其他客户端，否则记录 `BLOCKED`。容器重建后重新核对并设置 `G5_CONTAINER`，新容器初始不监听。

对于控制连接中断，参照 G4-04 的专用本地转发器方法，只断开 Agent 到 API 的连接，保留 Agent 进程、容器和 Pod 网络；不要通过 SIGTERM Agent 来冒充连接中断。先准备可恢复方法并确认不会切断 SSH，记录转发器及 Agent PID、断开/恢复时间、API 心跳、CRI 身份和 fixture 事件。若无法安全注入或没有可验证的会话/探测证据，记录 `BLOCKED`。

## 清理与结果

使用同一测试清单执行 `mini delete -f "$G5_EVIDENCE/pod.yaml"`，等待对应 UID 不再存在且 CRI sandbox/container、CNI IP 租约和预留回到基线；异常时保留 UID、ID 与现场，不用宽范围清理命令。记录 `mini get nodes`、`mini get pods` 和两台 Worker 的 CRI/租约最终快照。

只在上述每项都有满足对应时序和身份要求的真实证据、清理完成并经操作者核对后，才能将 readiness 子模块标记通过；整个 G5 仍必须另行完成全部 Gateway 检查。
