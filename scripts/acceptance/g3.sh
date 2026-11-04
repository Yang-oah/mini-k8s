#!/usr/bin/env bash
set -uo pipefail

# G3 只能在本机真实 Linux Worker 上运行；不连接远程 CRI 或远程节点。
# G3_TEST_IMAGE 须预拉取并以 sha256 digest 指定，镜像内须有 sh 和 sleep；
# sh 须支持变量赋值、字符串拼接和 while 循环，用于 CPU 与 OOM 测试。
# G3_POD_CIDR 必须填写与实际 CNI 网络一致的 IPv4 CIDR。
# 当 CRI info 未提供 sandboxImage 时，G3_CONTAINERD_CONFIG 须指向目标 daemon 实际使用的绝对配置路径。
cd "$(dirname "$0")/../.." || exit 2
run_id="$(date -u +%Y%m%dT%H%M%SZ)-g3-$$"
out="$(pwd)/artifacts/acceptance/$run_id"
mkdir -p "$out" || exit 2
result=PASS
reason='All mandatory checks and cleanup passed.'
started="$(date -u +%FT%TZ)"
tmp=''
etcd_pid=''
cleaned=false
cleanup_result=NOT_RUN
preserve=false
g2_evidence=''
interrupted=false
execution_started=false
guard_completed=false

# 测试二进制内的 Linux pidfd 守护逻辑只回收本轮可执行文件和私有身份对应的进程。
run_guard() {
  if [[ -z "$tmp" || ! -x "$tmp/g3test" ]]; then return 1; fi
  G3_PROCESS_GUARD=1 "$tmp/g3test" '-test.run=^TestG3ProcessGuard$' -test.timeout=60s -test.v >>"$out/process-guard.log" 2>&1
}
on_signal() {
  trap '' INT TERM HUP
  interrupted=true
  preserve=true
  result=FAIL
  reason='G3 interrupted; process cleanup and recovery evidence retained.'
  if [[ -n "$tmp" && -x "$tmp/g3test" ]] && ! run_guard; then reason="$reason Process cleanup requires inspection."; fi
}
trap 'on_signal' INT TERM HUP

cleanup() {
  [[ "$cleaned" == true ]] && return 0
  local failed=false
  if [[ -n "$etcd_pid" ]]; then
    if kill -0 "$etcd_pid" 2>/dev/null && ! kill "$etcd_pid" 2>/dev/null; then failed=true; fi
    for _ in {1..50}; do
      if ! kill -0 "$etcd_pid" 2>/dev/null; then break; fi
      sleep 0.1
    done
    if kill -0 "$etcd_pid" 2>/dev/null; then failed=true; kill -KILL "$etcd_pid" 2>/dev/null || true; fi
    if ! kill -0 "$etcd_pid" 2>/dev/null; then wait "$etcd_pid" 2>/dev/null || :; fi
  fi
  # 已启动 G3 后失败或中断时保留 etcd 数据，供按 UID 恢复；只删除未使用或已通过的目录。
  if [[ -n "$tmp" && "$preserve" != true ]] && { ! rm -rf -- "$tmp" || [[ -e "$tmp" ]]; }; then failed=true; fi
  cleaned=true
  if [[ "$failed" == true ]]; then cleanup_result=FAIL; return 1; fi
  if [[ "$preserve" == true && -n "$tmp" ]]; then cleanup_result=RETAINED; else cleanup_result=PASS; fi
}
on_exit() {
  # 非信号异常退出也不能跳过子进程检查或删除恢复所需的 etcd 数据。
  if [[ "$execution_started" == true && "$guard_completed" != true ]]; then
    preserve=true
    if ! run_guard; then printf '%s\n' 'G3 process cleanup could not be confirmed; inspect process-guard.log.' >&2; fi
  fi
  cleanup || true
}
trap 'on_exit' EXIT

# 与 Go 的 net.ParseCIDR 对齐：四段十进制 IPv4，前缀 0–32，不接受前导零。
valid_pod_cidr() {
  local value="$1" part prefix
  [[ "$value" =~ ^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})/([0-9]{1,2})$ ]] || return 1
  local -a octets=("${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}" "${BASH_REMATCH[4]}")
  prefix="${BASH_REMATCH[5]}"
  for part in "${octets[@]}"; do
    [[ "$part" == 0 || "$part" != 0* ]] || return 1
    (( 10#$part <= 255 )) || return 1
  done
  [[ "$prefix" == 0 || "$prefix" != 0* ]] || return 1
  (( 10#$prefix <= 32 ))
}

# 不满足环境条件一律 BLOCKED；尤其不以 Windows 或 mock 代替 Linux CRI/CNI。
if [[ "$(uname -s)" != Linux ]]; then result=BLOCKED; reason='Linux worker is required.'; fi
if [[ "$result" == PASS ]]; then
  for tool in bash go gofmt etcd containerd crictl curl sha256sum find sort xargs grep sed tail mktemp date mkdir rm sleep id; do
    if ! command -v "$tool" >/dev/null 2>&1; then result=BLOCKED; reason="Missing dependency: $tool"; break; fi
  done
fi
if [[ "$result" == PASS && "$(id -u)" != 0 ]]; then result=BLOCKED; reason='Local root privileges are required for runtime and CNI verification.'; fi
cri="${G3_CRI_ENDPOINT:-unix:///run/containerd/containerd.sock}"
image="${G3_TEST_IMAGE:-}"
pod_cidr="${G3_POD_CIDR:-}"
containerd_config="${G3_CONTAINERD_CONFIG:-}"
cni_conf="${G3_CNI_CONF_DIR:-/etc/cni/net.d}"
cni_bin="${G3_CNI_BIN_DIR:-/opt/cni/bin}"
cni_network="${G3_CNI_NETWORK:-}"
cni_data="${G3_CNI_DATA_DIR:-/var/lib/cni/networks}"
if [[ "$result" == PASS && "$cri" != unix:///* ]]; then result=BLOCKED; reason='CRI endpoint must be a local absolute Unix socket.'; fi
if [[ "$result" == PASS && ! -S "${cri#unix://}" ]]; then result=BLOCKED; reason='Local CRI socket is unavailable.'; fi
if [[ "$result" == PASS && ! "$image" =~ @sha256:[a-f0-9]{64}$ ]]; then result=BLOCKED; reason='G3_TEST_IMAGE must be a pre-pulled image digest.'; fi
if [[ "$result" == PASS ]] && ! valid_pod_cidr "$pod_cidr"; then result=BLOCKED; reason='G3_POD_CIDR must be a valid IPv4 CIDR.'; fi
if [[ "$result" == PASS && ! "$cni_network" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then result=BLOCKED; reason='G3_CNI_NETWORK must identify the host-local network.'; fi
if [[ "$result" == PASS && "$cni_data" != /* ]]; then result=BLOCKED; reason='Host-local IP lease directory must be an absolute local path.'; fi
if [[ "$result" == PASS && ( ! -d "$cni_conf" || ! -x "$cni_bin/bridge" || ! -x "$cni_bin/host-local" ) ]]; then result=BLOCKED; reason='CNI configuration or bridge/host-local plugins are unavailable.'; fi
if [[ "$result" == PASS ]] && ! find "$cni_conf" -maxdepth 1 -type f \( -name '*.conf' -o -name '*.conflist' \) -print -quit | grep -q .; then result=BLOCKED; reason='No local CNI configuration found.'; fi
if [[ "$result" == PASS ]] && ! crictl --runtime-endpoint "$cri" --image-endpoint "$cri" version >"$out/cri-version.txt" 2>&1; then result=BLOCKED; reason='Local CRI is not reachable.'; fi
if [[ "$result" == PASS ]] && ! grep -qi containerd "$out/cri-version.txt"; then result=BLOCKED; reason='The CRI runtime is not containerd.'; fi
if [[ "$result" == PASS ]] && ! crictl --runtime-endpoint "$cri" --image-endpoint "$cri" inspecti "$image" >"$out/image-inspect.json" 2>&1; then result=BLOCKED; reason='Pinned test image is not already present locally.'; fi
if [[ "$result" == PASS && ! -f cmd/mini-agent/main.go ]]; then result=BLOCKED; reason='mini-agent is not implemented.'; fi
if [[ "$result" == PASS ]] && ! sha256sum "$cni_bin/bridge" "$cni_bin/host-local" >"$out/cni-plugins.sha256"; then result=BLOCKED; reason='Cannot record CNI plugin fingerprints.'; fi

if [[ "$result" == PASS ]]; then
  {
    printf 'started=%s\nimage=%s\npod_cidr=%s\ncri=%s\ncontainerd_config=%s\ncni_conf=%s\ncni_bin=%s\ncni_network=%s\ncni_data=%s\n' "$started" "$image" "$pod_cidr" "$cri" "$containerd_config" "$cni_conf" "$cni_bin" "$cni_network" "$cni_data"
    printf '%s\n' 'image_contract=pre-pulled digest with sh, sleep, shell variable concatenation and while loops'
    printf '%s\n' 'containerd_config_contract=when CRI omits sandboxImage, the supplied path must be the configuration used by the target daemon; this cannot be inferred from config dump alone'
    go version
    etcd --version
    containerd --version
    printf 'source_sha256='; { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum
  } > "$out/environment.txt"
  # 当前源码先通过 G2（G2 本身重跑 G1）；不依赖旧版本报告。
  unset G3_ETCD_ENDPOINT G3_AGENT_BINARY G3_MANAGER_BINARY G3_PROCESS_GUARD G3_PROCESS_TEST_BINARY G3_CRI_ENDPOINT G3_TEST_IMAGE G3_EVIDENCE_DIR G3_AGENT_LOG
  if bash scripts/acceptance/g2.sh >"$out/g2-prerequisite.log" 2>&1; then
    g2_evidence="$(sed -n 's/^Evidence: //p' "$out/g2-prerequisite.log" | tail -n 1)"
    if [[ -z "$g2_evidence" || ! -f "$g2_evidence/G2.md" ]] || ! grep -q '^- Result: PASS$' "$g2_evidence/G2.md"; then result=BLOCKED; reason='G2 did not produce a complete PASS report.'; fi
    if [[ "$result" == PASS ]]; then
      for check in 01 02 03 04 05 06; do
        if ! grep -Fq "| G2-$check | PASS |" "$g2_evidence/G2.md"; then result=BLOCKED; reason="G2-$check did not pass."; fi
      done
      if [[ "$(grep '^source_sha256=' "$out/environment.txt")" != "$(grep '^source_sha256=' "$g2_evidence/environment.txt")" ]]; then result=BLOCKED; reason='Source changed between G2 and G3.'; fi
    fi
  else result=BLOCKED; reason='Current-version G2 prerequisite did not pass.'; fi
fi
if [[ "$result" == PASS ]]; then
  tmp="$(mktemp -d)" || { result=BLOCKED; reason='Cannot create isolated etcd directory.'; }
fi
client_port="${G3_ETCD_CLIENT_PORT:-23793}"
peer_port="${G3_ETCD_PEER_PORT:-23803}"
endpoint="http://127.0.0.1:$client_port"
if [[ "$result" == PASS ]] && curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then result=BLOCKED; reason='Local G3 etcd endpoint already in use.'; fi
if [[ "$result" == PASS ]]; then
  etcd --name g3 --data-dir "$tmp/etcd" --listen-client-urls "$endpoint" --advertise-client-urls "$endpoint" \
    --listen-peer-urls "http://127.0.0.1:$peer_port" --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" \
    --initial-cluster "g3=http://127.0.0.1:$peer_port" >"$out/etcd.log" 2>&1 &
  etcd_pid=$!
  ready=false
  for _ in {1..100}; do
    if ! kill -0 "$etcd_pid" 2>/dev/null; then break; fi
    if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then ready=true; break; fi
    sleep 0.1
  done
  if [[ "$ready" != true ]]; then result=BLOCKED; reason='Isolated etcd failed to become ready.'; fi
fi
if [[ "$result" == PASS ]] && ! go build -o "$tmp/mini-agent" ./cmd/mini-agent >"$out/build-agent.log" 2>&1; then result=FAIL; reason='mini-agent build failed.'; fi
if [[ "$result" == PASS ]] && ! go build -o "$tmp/mini-manager" ./cmd/mini-manager >"$out/build-manager.log" 2>&1; then result=FAIL; reason='mini-manager build failed.'; fi
if [[ "$result" == PASS ]] && ! go test -c -o "$tmp/g3test" ./internal/acceptance >"$out/build-test.log" 2>&1; then result=FAIL; reason='G3 test binary build failed.'; fi
if [[ "$result" == PASS ]]; then
  export G3_ETCD_ENDPOINT="$endpoint" G3_AGENT_BINARY="$tmp/mini-agent" G3_MANAGER_BINARY="$tmp/mini-manager" G3_PROCESS_TEST_BINARY="$tmp/g3test" G3_CRI_ENDPOINT="$cri" G3_TEST_IMAGE="$image" G3_POD_CIDR="$pod_cidr" G3_EVIDENCE_DIR="$out" G3_AGENT_LOG="$out/agent.log" G3_CNI_NETWORK="$cni_network" G3_CNI_DATA_DIR="$cni_data" G3_CONTAINERD_CONFIG="$containerd_config"
  preserve=true
  execution_started=true
  "$tmp/g3test" '-test.run=^TestG3$' -test.timeout=300s -test.v >"$out/g3.log" 2>&1 &
  test_pid=$!
  if ! wait "$test_pid"; then result=FAIL; reason='G3 assertion, startup, interruption, or resource cleanup failed.'; fi
  # 测试超时可能跳过 Go cleanup：无论退出码均检查并回收确切的本轮进程。
  if run_guard; then guard_completed=true; else result=FAIL; reason="$reason Process identity or cleanup requires inspection."; fi
  if [[ "$interrupted" == true ]]; then result=FAIL; reason="$reason Execution was interrupted."; fi
  if [[ "$result" == PASS ]]; then
    if grep -q '^--- SKIP: TestG3[[:space:]]' "$out/g3.log"; then
      prerequisite_reason="$(sed -n 's/^[[:space:]]*g3_test\.go:[0-9][0-9]*: //p' "$out/g3.log" | tail -n 1)"
      result=BLOCKED
      reason="G3 setup skipped: ${prerequisite_reason:-see g3.log for the prerequisite reason.}"
    else
      for check in 01 02 03 04 05; do
        if ! grep -q -- "--- PASS: TestG3/G3-$check" "$out/g3.log"; then result=BLOCKED; reason="G3-$check was not executed successfully; see g3.log."; break; fi
      done
    fi
  fi
fi
if [[ "$result" == PASS ]]; then
  final_hash="$(printf 'source_sha256='; { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum)"
  if [[ "$final_hash" != "$(grep '^source_sha256=' "$out/environment.txt")" ]]; then result=FAIL; reason='Source changed during G3 acceptance.'; fi
fi
if [[ "$result" == PASS ]]; then preserve=false; fi
if ! cleanup; then result=FAIL; reason="$reason Local temporary cleanup failed."; fi
{
  printf '# G3 acceptance\n\n- Result: %s\n- Started (UTC): %s\n- Finished (UTC): %s\n- Command: scripts/acceptance/g3.sh\n' "$result" "$started" "$(date -u +%FT%TZ)"
  printf -- '- G2 evidence: %s\n- Details: %s\n\n| Check | Result | Evidence |\n| --- | --- | --- |\n' "$g2_evidence" "$reason"
  for check in 01 02 03 04 05; do
    if [[ -f "$out/g3.log" ]] && grep -q -- "--- PASS: TestG3/G3-$check" "$out/g3.log"; then state=PASS
    elif [[ -f "$out/g3.log" ]] && grep -q -- "--- FAIL: TestG3/G3-$check" "$out/g3.log"; then state=FAIL
    else state=NOT_RUN; fi
    printf '| G3-%s | %s | g3.log |\n' "$check" "$state"
  done
  printf '\n- Isolated etcd and temporary directory cleanup: %s\n' "$cleanup_result"
  if [[ "$cleanup_result" == RETAINED ]]; then printf -- '- Preserved etcd data and binaries: %s\n' "$tmp"; fi
  printf '%s\n' '- Process identity and cleanup: process-guard.log. Pod/runtime recovery: identities.json, leases.json and g3-state-*; see g3.log.' '- The CRI must report RuntimeReady and NetworkReady, and its configured sandbox image must already exist locally; missing conditions are BLOCKED.' '- If CRI omits sandboxImage, set G3_CONTAINERD_CONFIG to the absolute path used by the target containerd daemon, then retry; confirm that path independently because config dump cannot prove which file the daemon loaded.' '- SIGKILL or host loss cannot run the script trap; inspect the recorded state before retrying.'
} > "$out/G3.md"
printf '%s: %s\nEvidence: %s\n' "$result" "$reason" "$out"
case "$result" in PASS) exit 0;; FAIL) exit 1;; *) exit 2;; esac
