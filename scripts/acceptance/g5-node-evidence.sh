#!/usr/bin/env bash
set -uo pipefail
umask 077
# 不继承普通用户的命令搜索路径；自定义可执行文件只能通过受信任目录显式指定。
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH

# 只读节点采样器；不能自动判断 Gate。开始测试前启动，结束后发送 SIGTERM。
usage() {
  cat <<'EOF'
usage: sudo bash scripts/acceptance/g5-node-evidence.sh \
  --output /root-owned/private-parent/new-directory --lease-dir /absolute/cni/network-directory \
  --cri-endpoint unix:///absolute/socket [--pod-uid UID ...] \
  [--log-container CONTAINER_ID ...] [--iptables none|project|full] \
  [--stats] [--interval SECONDS] [--duration SECONDS] [--log-budget-bytes BYTES] \
  [--tool-dir /absolute/root-owned/tool-directory]

Default: project-managed CRI resources, no container logs or iptables, 1s interval,
unlimited duration. SIGINT/SIGTERM stops after in-flight reads complete. "project"
captures only MK8S chains and references; "full" captures entire iptables tables
and may include unrelated sensitive network information. Explicit container IDs
authorize timestamped, overlapping --since log windows; each container has an
8 MiB default cumulative budget and each sample has a 256 KiB cap. Truncation,
budget exhaustion and observed sampling gaps are recorded, never treated as PASS.
--stats also samples CRI statistics for each explicitly selected log container;
statistics can fail after that container is removed and are point-in-time only.
Output parent and every ancestor must be root-owned and not writable by group or
other; e.g. create /var/lib/mini-k8s-evidence with owner root and mode 0700 first.
Even exit 0 means collection completed without detected errors, not Gate PASS.
When --iptables full is enabled, READY is created after a complete successful
lease, CRI and three-table sample. Verify READY before starting deletion; it
proves neither baseline correctness nor later sampling/window coverage.
PATH is fixed to system directories. For tools installed elsewhere, --tool-dir
must resolve to a root-owned directory whose ancestors are also root-owned and
not writable by group/other; selected executables have the same restrictions.
EOF
}

out='' lease='' endpoint='' rules='none' interval=1 duration=0 log_budget=8388608 tool_dir='' stats=0
uids=() containers=()
while (($#)); do
  case "$1" in
    --help) usage; exit 0 ;;
    --stats) stats=1; shift ;;
    --output|--lease-dir|--cri-endpoint|--pod-uid|--log-container|--iptables|--interval|--duration|--log-budget-bytes|--tool-dir)
      (($# >= 2)) || { printf 'missing value for %s\n' "$1" >&2; exit 2; }
      case "$1" in
        --output) out="$2" ;;
        --lease-dir) lease="$2" ;;
        --cri-endpoint) endpoint="$2" ;;
        --pod-uid) uids+=("$2") ;;
        --log-container) containers+=("$2") ;;
        --iptables) rules="$2" ;;
        --interval) interval="$2" ;;
        --duration) duration="$2" ;;
        --log-budget-bytes) log_budget="$2" ;;
        --tool-dir) tool_dir="$2" ;;
      esac
      shift 2 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done

[[ $EUID -eq 0 ]] || { printf 'root is required to read CRI, leases and rules; use sudo\n' >&2; exit 2; }
[[ "$out" == /* && "$out" != / && "$lease" == /* && "$lease" != / ]] || { printf 'output and lease paths must be non-root absolute paths\n' >&2; exit 2; }
[[ "$endpoint" =~ ^unix:///[a-zA-Z0-9_./-]+$ && "$endpoint" != *..* ]] || { printf 'CRI endpoint must be a local absolute unix socket\n' >&2; exit 2; }
[[ "$rules" == none || "$rules" == project || "$rules" == full ]] || { printf 'invalid iptables mode\n' >&2; exit 2; }
[[ "$interval" =~ ^[0-9]{1,2}$ && "$duration" =~ ^[0-9]{1,6}$ ]] || { printf 'interval and duration must be integer seconds\n' >&2; exit 2; }
interval=$((10#$interval)); duration=$((10#$duration))
((interval >= 1 && interval <= 60 && duration <= 86400)) || { printf 'interval must be 1..60, duration 0..86400\n' >&2; exit 2; }
[[ "$log_budget" =~ ^[0-9]{1,8}$ ]] || { printf 'invalid log byte budget\n' >&2; exit 2; }
log_budget=$((10#$log_budget))
((log_budget >= 1024 && log_budget <= 16777216)) || { printf 'log budget must be 1024..16777216 bytes per container\n' >&2; exit 2; }
((${#uids[@]} <= 4 && ${#containers[@]} <= 2)) || { printf 'at most 4 UIDs and 2 log containers; omit UIDs to collect all project-managed CRI objects\n' >&2; exit 2; }
((stats == 0 || ${#containers[@]} > 0)) || { printf '%s\n' '--stats requires at least one --log-container' >&2; exit 2; }
declare -A seen_uids=() seen_containers=()
for uid in "${uids[@]}"; do
  [[ "$uid" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$ && "$uid" != *..* ]] || { printf 'invalid Pod UID\n' >&2; exit 2; }
  [[ -z "${seen_uids[$uid]+set}" ]] || { printf 'duplicate Pod UID\n' >&2; exit 2; }
  seen_uids[$uid]=1
done
for id in "${containers[@]}"; do
  [[ "$id" =~ ^[a-f0-9]{12,64}$ ]] || { printf 'container IDs must be lowercase hexadecimal\n' >&2; exit 2; }
  [[ -z "${seen_containers[$id]+set}" ]] || { printf 'duplicate container ID\n' >&2; exit 2; }
  seen_containers[$id]=1
done
command -v timeout >/dev/null && command -v realpath >/dev/null && command -v python3 >/dev/null || { printf 'timeout, realpath and python3 are required in the system path\n' >&2; exit 2; }

# 验证目录及每一层父目录，随后只使用 realpath 得到的路径，避免符号链接替换。
trusted_directory() {
  local ancestor="$1" owner mode
  while :; do
    [[ -d "$ancestor" && ! -L "$ancestor" ]] || return 1
    owner=$(stat -c '%u' -- "$ancestor") || return 1
    mode=$(stat -c '%a' -- "$ancestor") || return 1
    [[ "$owner" == 0 && "$mode" =~ ^[0-7]{3,4}$ ]] && (((8#$mode & 0022) == 0)) || return 1
    [[ "$ancestor" == / ]] && break
    ancestor=$(dirname -- "$ancestor")
  done
}

trusted_executable() {
  local input="$1" resolved owner mode
  [[ "$input" == /* ]] || return 1
  resolved=$(realpath -e -- "$input") || return 1
  [[ -f "$resolved" && -x "$resolved" && ! -L "$resolved" ]] || return 1
  owner=$(stat -c '%u' -- "$resolved") || return 1
  mode=$(stat -c '%a' -- "$resolved") || return 1
  [[ "$owner" == 0 && "$mode" =~ ^[0-7]{3,4}$ ]] && (((8#$mode & 0022) == 0)) || return 1
  trusted_directory "$(dirname -- "$resolved")" || return 1
  printf '%s\n' "$resolved"
}

if [[ -n "$tool_dir" ]]; then
  [[ "$tool_dir" == /* ]] || { printf 'tool directory must be absolute\n' >&2; exit 2; }
  tool_dir=$(realpath -e -- "$tool_dir") || { printf 'cannot resolve tool directory\n' >&2; exit 2; }
  trusted_directory "$tool_dir" || { printf 'tool directory and ancestors must be root-owned and not group/other writable\n' >&2; exit 2; }
fi
cri_candidate=''
if [[ -n "$tool_dir" && -e "$tool_dir/crictl" ]]; then
  cri_candidate="$tool_dir/crictl"
else
  cri_candidate=$(command -v crictl) || true
  if [[ -z "$cri_candidate" && -e /usr/local/bin/crictl ]]; then cri_candidate=/usr/local/bin/crictl; fi
fi
cri_tool=$(trusted_executable "$cri_candidate") || { printf 'trusted crictl is required: executable and ancestors must be root-owned and not group/other writable; install it in a trusted system directory or use --tool-dir\n' >&2; exit 2; }
iptables_tool=''
if [[ "$rules" != none ]]; then
  iptables_candidate=''
  if [[ -n "$tool_dir" && -e "$tool_dir/iptables-save" ]]; then iptables_candidate="$tool_dir/iptables-save"; else iptables_candidate=$(command -v iptables-save) || true; fi
  iptables_tool=$(trusted_executable "$iptables_candidate") || { printf 'trusted iptables-save executable is required\n' >&2; exit 2; }
fi
[[ ! -L "$lease" ]] || { printf 'lease directory must not be a symlink\n' >&2; exit 2; }
lease=$(realpath -e -- "$lease") || { printf 'cannot resolve lease directory\n' >&2; exit 2; }
[[ -d "$lease" && "$lease" != / && "$lease" != /proc/* && "$lease" != /sys/* && "$lease" != /dev/* ]] || { printf 'invalid lease directory\n' >&2; exit 2; }
# 输出目录必须全新创建，绝不覆盖已有证据；其父目录必须已存在。
parent=$(realpath -e -- "$(dirname -- "$out")") || { printf 'output parent must exist\n' >&2; exit 2; }
name=$(basename -- "$out")
[[ "$name" != . && "$name" != .. && "$name" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]] || { printf 'invalid output directory name\n' >&2; exit 2; }
out="$parent/$name"
[[ "$out" != "$lease" && "$out" != "$lease/"* ]] || { printf 'output must not be inside the lease directory\n' >&2; exit 2; }
# root 重定向只能位于可信目录：任何可由普通用户改名的祖先都会产生提权竞态。
trusted_directory "$parent" || { printf 'output ancestor must be root-owned and not group/other writable\n' >&2; exit 2; }
mkdir -m 0700 -- "$out" || { printf 'output directory already exists or cannot be created\n' >&2; exit 2; }

now() { date -u +'%Y-%m-%dT%H:%M:%S.%NZ'; }
stop=0 termination='duration'
trap 'stop=1; termination=SIGINT' INT
trap 'stop=1; termination=SIGTERM' TERM
lookback=$((interval + 5))
printf 'started=%s\ninterval_seconds=%s\nduration_seconds=%s\niptables=%s\nstats=%s\nlease_directory=%s\ncri_endpoint=%s\nlog_lookback_seconds=%s\nlog_budget_bytes_per_container=%s\nlog_sample_cap_bytes=262144\n' "$(now)" "$interval" "$duration" "$rules" "$stats" "$lease" "$endpoint" "$lookback" "$log_budget" >"$out/session.txt" || exit 1
printf '%s\n' 'Only project-managed CRI list results are captured; every command has stdout, stderr, timestamps and exit code. Exited containers are inspected and summarized in exits.txt, or exits-<pod-uid>.txt when --pod-uid is set. The first sighting is also printed once on stderr as exit-observed. OOMKilled or exit code 137 also saves a bounded kernel and containerd journal excerpt. session.txt counts distinct container IDs, not one row per sample. Sample start gaps are wall-clock observations, not proof of continuous coverage; compare the actual operation window and command timestamps. Log windows overlap and can still miss data because of rotation, process pauses or runtime errors; inspect metadata. Results require manual interpretation. Full iptables, inspect output and selected container logs may be sensitive.' >"$out/README.txt" || exit 1
printf 'Evidence: %s\n' "$out"

# 每次读操作有独立时限和结果文件；exit 124 是超时，绝不当作空清单。
capture() {
  local folder="$1" label="$2"; shift 2
  local started finished rc
  started=$(now)
  timeout -k 1s 3s "$@" >"$folder/$label.out" 2>"$folder/$label.err"
  rc=$?
  finished=$(now)
  printf 'start=%s\nfinish=%s\nexit=%s\n' "$started" "$finished" "$rc" >"$folder/$label.meta" || return 1
  return 0
}

# 窗口故意重叠以减少边界丢失；超过字节上限时明确标记，不能推断日志完整。
capture_log() {
  local folder="$1" id="$2" limit="$3" started finished source_rc head_rc size truncated=false
  local label="container-$id" statuses=()
  started=$(now)
  if ((limit == 0)); then
    : >"$folder/$label.out"
    printf 'per-container log budget exhausted\n' >"$folder/$label.err"
    source_rc=125; head_rc=0; truncated=true
  else
    timeout -k 1s 3s "$cri_tool" --runtime-endpoint "$endpoint" logs --timestamps --since "${lookback}s" "$id" 2>"$folder/$label.err" |
      head -c "$((limit + 1))" >"$folder/$label.out"
    statuses=("${PIPESTATUS[@]}")
    source_rc=${statuses[0]}; head_rc=${statuses[1]}
    size=$(wc -c <"$folder/$label.out")
    if ((size > limit)); then
      if ! head -c "$limit" "$folder/$label.out" >"$folder/$label.limited" || ! mv -- "$folder/$label.limited" "$folder/$label.out"; then
        source_rc=1
      fi
      truncated=true
    fi
  fi
  finished=$(now)
  local rc=0
  if ((source_rc != 0 || head_rc != 0)) || [[ "$truncated" == true ]]; then rc=1; fi
  printf 'start=%s\nfinish=%s\nexit=%s\nsource_exit=%s\nhead_exit=%s\ntruncated=%s\nlookback_seconds=%s\n' "$started" "$finished" "$rc" "$source_rc" "$head_rc" "$truncated" "$lookback" >"$folder/$label.meta" || return 1
  return 0
}

# 过滤只保留项目链、链声明和指向项目链的引用；full 模式需用户显式选择。
capture_rules() {
  local folder="$1" table="$2" started finished rc
  started=$(now)
  if [[ "$rules" == full ]]; then
    timeout -k 1s 3s "$iptables_tool" -t "$table" >"$folder/iptables-$table.out" 2>"$folder/iptables-$table.err"
    rc=$?
  else
    timeout -k 1s 3s "$iptables_tool" -t "$table" 2>"$folder/iptables-$table.err" |
      awk '/^:MK8S-/ || /^-A MK8S-/ || /(^| )(-j|-g|--jump|--goto) MK8S-/ {print}' >"$folder/iptables-$table.out"
    rc=$?
  fi
  finished=$(now)
  printf 'start=%s\nfinish=%s\nexit=%s\nmode=%s\n' "$started" "$finished" "$rc" "$rules" >"$folder/iptables-$table.meta" || return 1
  return 0
}

# 退出容器写出原因、退出码和内存上限。OOM 或 137 再保存短时间内核与 containerd 日志。
record_exits() {
  local folder="$1" list="$2" label="$3" started finished rc kernel_label containerd_label
  started=$(now)
  python3 - "$list" "$folder" "$cri_tool" "$endpoint" "$label" <<'PY'
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

list_path, folder_text, cri, endpoint, label = sys.argv[1:6]
folder = Path(folder_text)
source = Path(list_path)
if not source.is_file():
    raise SystemExit(1)
raw = source.read_text(encoding="utf-8")
try:
    data = json.loads(raw) if raw.strip() else {}
except json.JSONDecodeError:
    raise SystemExit(1)
containers = data.get("containers") if isinstance(data, dict) else None
if containers is None:
    containers = []
exited = []
for item in containers:
    if not isinstance(item, dict):
        continue
    state = str(item.get("state") or "")
    cid = str(item.get("id") or "")
    if state.endswith("EXITED") and 12 <= len(cid) <= 64 and all(c in "0123456789abcdef" for c in cid):
        exited.append(cid)
selected = exited[:4]
lines = []
earliest = None
latest = None
failed = False

def parse_time(value):
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, tail = text.split(".", 1)
        frac = tail
        zone = ""
        for index, char in enumerate(tail):
            if char in "+-":
                frac = tail[:index]
                zone = tail[index:]
                break
        text = head + "." + (frac + "000000")[:6] + zone
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

for cid in selected:
    proc = subprocess.run([cri, "--runtime-endpoint", endpoint, "inspect", "-o", "json", cid], capture_output=True, text=True, timeout=3)
    (folder / ("inspect-" + cid + ".out")).write_text(proc.stdout, encoding="utf-8")
    (folder / ("inspect-" + cid + ".err")).write_text(proc.stderr, encoding="utf-8")
    if proc.returncode != 0:
        failed = True
        lines.append("id=%s inspectExit=%s reason= exitCode= attempt= startedAt= finishedAt= memoryLimitBytes= oom=false captureKernel=false" % (cid, proc.returncode))
        continue
    try:
        status = json.loads(proc.stdout).get("status") or {}
    except json.JSONDecodeError:
        failed = True
        lines.append("id=%s inspectExit=1 reason= exitCode= attempt= startedAt= finishedAt= memoryLimitBytes= oom=false captureKernel=false" % cid)
        continue
    reason = str(status.get("reason") or "")
    code = status.get("exitCode")
    code_text = "" if code is None else str(code)
    metadata = status.get("metadata") or {}
    attempt = "" if not isinstance(metadata, dict) or metadata.get("attempt") is None else str(metadata.get("attempt"))
    started_at = str(status.get("startedAt") or "")
    finished_at = str(status.get("finishedAt") or "")
    resources = ((status.get("resources") or {}).get("linux") or {})
    limit = str(resources.get("memoryLimitInBytes") or "")
    oom = reason == "OOMKilled"
    capture = oom or code_text == "137"
    lines.append("id=%s inspectExit=0 reason=%s exitCode=%s attempt=%s startedAt=%s finishedAt=%s memoryLimitBytes=%s oom=%s captureKernel=%s" % (cid, reason, code_text, attempt, started_at, finished_at, limit, "true" if oom else "false", "true" if capture else "false"))
    if capture and finished_at:
        try:
            moment = parse_time(finished_at)
        except ValueError:
            continue
        if earliest is None or moment < earliest:
            earliest = moment
        if latest is None or moment > latest:
            latest = moment
(folder / (label + ".txt")).write_text(("".join(line + "\n" for line in lines)), encoding="utf-8")
if earliest is not None and latest is not None:
    (folder / ("kernel-since-" + label)).write_text((earliest - timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S UTC") + "\n", encoding="utf-8")
    (folder / ("kernel-until-" + label)).write_text((latest + timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S UTC") + "\n", encoding="utf-8")
if len(exited) > 4:
    (folder / (label + "-truncated")).write_text("exited=%s inspected=4\n" % len(exited), encoding="utf-8")
raise SystemExit(1 if failed else 0)
PY
  rc=$?
  finished=$(now)
  printf 'start=%s\nfinish=%s\nexit=%s\n' "$started" "$finished" "$rc" >"$folder/$label.meta" || return 1
  if [[ -f "$folder/kernel-since-$label" && -f "$folder/kernel-until-$label" ]]; then
    kernel_label=oom-kernel
    containerd_label=oom-containerd
    if [[ "$label" != exits ]]; then
      kernel_label="oom-kernel-$label"
      containerd_label="oom-containerd-$label"
    fi
    capture_journal "$folder" "$kernel_label" "$folder/kernel-since-$label" "$folder/kernel-until-$label" "" || return 1
    capture_journal "$folder" "$containerd_label" "$folder/kernel-since-$label" "$folder/kernel-until-$label" containerd || return 1
  fi
  return 0
}

capture_journal() {
  local folder="$1" label="$2" since_file="$3" until_file="$4" unit="$5" since until started finished source_rc head_rc rc truncated=false statuses=()
  since=$(<"$since_file")
  until=$(<"$until_file")
  since=${since//$'\n'/}
  until=${until//$'\n'/}
  started=$(now)
  if [[ -n "$unit" ]]; then
    timeout -k 1s 3s journalctl -u "$unit" --since "$since" --until "$until" --no-pager -o short-precise 2>"$folder/$label.err" | head -c 262144 >"$folder/$label.out"
  else
    timeout -k 1s 3s journalctl -k --since "$since" --until "$until" --no-pager -o short-precise 2>"$folder/$label.err" | head -c 262144 >"$folder/$label.out"
  fi
  statuses=("${PIPESTATUS[@]}")
  source_rc=${statuses[0]}
  head_rc=${statuses[1]}
  rc=0
  if ((head_rc != 0 || (source_rc != 0 && source_rc != 141))); then rc=1; fi
  if ((source_rc == 141)); then truncated=true; fi
  finished=$(now)
  printf 'start=%s\nfinish=%s\nexit=%s\nsource_exit=%s\nhead_exit=%s\ntruncated=%s\nsince=%s\nuntil=%s\n' "$started" "$finished" "$rc" "$source_rc" "$head_rc" "$truncated" "$since" "$until" >"$folder/$label.meta" || return 1
  return 0
}

start_epoch=$(date +%s)
sample=0
capture_errors=0
log_gaps=0
clock_regressions=0
previous_sample_ns=0
max_sample_interval_ns=0
first_sample_start=''
last_sample_start=''
last_sample_finish=''
ready_written=0
declare -A log_used=() seen_exits=()
for id in "${containers[@]}"; do log_used[$id]=0; done
while ((stop == 0)); do
  sample=$((sample + 1))
  printf -v seq '%06d' "$sample"
  folder="$out/sample-$seq"
  mkdir -m 0700 -- "$folder" || { printf 'cannot create sample directory\n' >&2; exit 1; }
  sample_stamp=$(date -u +'%Y-%m-%dT%H:%M:%S.%NZ|%s%N') || exit 1
  sample_start=${sample_stamp%%|*}
  sample_ns=${sample_stamp#*|}
  [[ "$sample_ns" =~ ^[0-9]{1,19}$ ]] || { printf 'invalid sampling timestamp\n' >&2; exit 1; }
  sample_ns=$((10#$sample_ns))
  interval_ns=NA
  if ((previous_sample_ns > 0)); then
    interval_ns=$((sample_ns - previous_sample_ns))
    if ((interval_ns < 0)); then
      clock_regressions=$((clock_regressions + 1))
    elif ((interval_ns > max_sample_interval_ns)); then
      max_sample_interval_ns=$interval_ns
    fi
    if ((${#containers[@]} > 0 && (interval_ns < 0 || interval_ns >= lookback * 1000000000))); then
      log_gaps=$((log_gaps + 1))
    fi
  fi
  [[ -n "$first_sample_start" ]] || first_sample_start="$sample_start"
  last_sample_start="$sample_start"
  previous_sample_ns=$sample_ns
  printf 'start=%s\nstart_epoch_ns=%s\nprevious_start_interval_ns=%s\n' "$sample_start" "$sample_ns" "$interval_ns" >"$folder/sample.meta" || exit 1
  if ((${#containers[@]} > 0)) && [[ "$interval_ns" != NA ]] && ((interval_ns < 0 || interval_ns >= lookback * 1000000000)); then
    printf 'log_gap=true\n' >>"$folder/sample.meta" || exit 1
  fi
  pids=()
  capture "$folder" leases find -P "$lease" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' & pids+=("$!")
  if ((${#uids[@]} == 0)); then
    (
      capture "$folder" cri-pods "$cri_tool" --runtime-endpoint "$endpoint" pods -o json --label mini-k8s.io/managed=true & pid_pods=$!
      capture "$folder" cri-containers "$cri_tool" --runtime-endpoint "$endpoint" ps -a -o json --label mini-k8s.io/managed=true & pid_containers=$!
      wait "$pid_pods"; pods_rc=$?
      wait "$pid_containers"; containers_rc=$?
      record_exits "$folder" "$folder/cri-containers.out" exits; exits_rc=$?
      ((pods_rc == 0 && containers_rc == 0 && exits_rc == 0))
    ) & pids+=("$!")
  else
    for uid in "${uids[@]}"; do
      # 同一 UID 的两次 CRI 查询串行，最多四个 UID 查询并行，减少取证扰动。
      (
        capture "$folder" "cri-pods-$uid" "$cri_tool" --runtime-endpoint "$endpoint" pods -o json --label mini-k8s.io/managed=true --label "mini-k8s.io/pod-uid=$uid"; first_rc=$?
        capture "$folder" "cri-containers-$uid" "$cri_tool" --runtime-endpoint "$endpoint" ps -a -o json --label mini-k8s.io/managed=true --label "mini-k8s.io/pod-uid=$uid"; second_rc=$?
        record_exits "$folder" "$folder/cri-containers-$uid.out" "exits-$uid"; exits_rc=$?
        ((first_rc == 0 && second_rc == 0 && exits_rc == 0))
      ) & pids+=("$!")
    done
  fi
  for id in "${containers[@]}"; do
    used=${log_used[$id]}
    remaining=$((log_budget - used))
    if ((remaining > 262144)); then remaining=262144; fi
    capture_log "$folder" "$id" "$remaining" & pids+=("$!")
    if ((stats != 0)); then capture "$folder" "stats-$id" "$cri_tool" --runtime-endpoint "$endpoint" stats -o json "$id" & pids+=("$!"); fi
  done
  if [[ "$rules" != none ]]; then
    for table in nat filter mangle; do capture_rules "$folder" "$table" & pids+=("$!"); done
  fi
  # 信号可能打断 wait；仍等待本轮有界只读任务退出，确保 meta 写入完整。
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null; worker_rc=$?
    # 信号可能使 wait 先返回，即使子进程稍后才正常退出；补收一次真实结果。
    if ((worker_rc > 128 && stop != 0)); then wait "$pid" 2>/dev/null; worker_rc=$?; fi
    if ((worker_rc != 0)); then capture_errors=$((capture_errors + 1)); fi
  done
  for meta in "$folder"/*.meta; do
    [[ "$meta" == "$folder/sample.meta" ]] && continue
    if ! grep -qx 'exit=0' "$meta"; then capture_errors=$((capture_errors + 1)); fi
  done
  # 删除前的就绪屏障：仅 full 模式覆盖无关规则比较所需的三张表。
  # READY 记录一次成功采样，不代表后续采样持续成功，也不是验收结果。
  if ((ready_written == 0)) && [[ "$rules" == full ]]; then
    required_meta=(leases iptables-nat iptables-filter iptables-mangle)
    if ((${#uids[@]} == 0)); then
      required_meta+=(cri-pods cri-containers)
    else
      for uid in "${uids[@]}"; do required_meta+=("cri-pods-$uid" "cri-containers-$uid"); done
    fi
    ready_sample=1
    for label in "${required_meta[@]}"; do
      if [[ ! -f "$folder/$label.meta" ]] || ! grep -qx 'exit=0' "$folder/$label.meta"; then
        ready_sample=0
        break
      fi
    done
    if ((ready_sample == 1)); then
      printf 'sample=%s\nsample_start=%s\nchecked=%s\ngate=NOT_EVALUATED\n' "$seq" "$sample_start" "$(now)" >"$out/.READY.tmp" || exit 1
      mv -- "$out/.READY.tmp" "$out/READY" || exit 1
      ready_written=1
      printf 'READY: required node samples succeeded; verify baseline and keep collecting: %s\n' "$out/READY"
    fi
  fi
  for id in "${containers[@]}"; do
    size=$(wc -c <"$folder/container-$id.out")
    used=${log_used[$id]}
    log_used[$id]=$((used + size))
  done
  shopt -s nullglob
  exit_logs=("$folder"/exits.txt "$folder"/exits-*.txt)
  shopt -u nullglob
  for exits in "${exit_logs[@]}"; do
    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ -n "$line" ]] || continue
      if [[ -z "${seen_exits[$line]+set}" ]]; then
        seen_exits[$line]=1
        printf 'exit-observed %s\n' "$line" >&2
      fi
    done <"$exits"
  done
  last_sample_finish=$(now)
  printf 'finish=%s\n' "$last_sample_finish" >>"$folder/sample.meta" || exit 1
  ((stop == 0)) || break
  if ((duration > 0 && $(date +%s) - start_epoch >= duration)); then break; fi
  sleep "$interval" & sleep_pid=$!
  if ! wait "$sleep_pid" 2>/dev/null && ((stop != 0)); then
    kill "$sleep_pid" 2>/dev/null || true
    wait "$sleep_pid" 2>/dev/null || true
  fi
done
exit_observations=0
oom_observations=0
shopt -s nullglob
exit_files=("$out"/sample-*/exits.txt "$out"/sample-*/exits-*.txt)
shopt -u nullglob
if ((${#exit_files[@]} > 0)); then
  # 同一退出容器会留在后续样本里；按容器 ID 计一次，避免把重复采样看成多次退出。
  exit_observations=$(grep -h '^id=' "${exit_files[@]}" | sed -n 's/^id=\([^ ]*\).*/\1/p' | sort -u | wc -l | tr -d ' ' || true)
  oom_observations=$(grep -h 'oom=true' "${exit_files[@]}" | sed -n 's/^id=\([^ ]*\).*/\1/p' | sort -u | wc -l | tr -d ' ' || true)
fi
result=CAPTURED_NOT_VERDICT
exit_code=0
if ((capture_errors > 0 || log_gaps > 0 || clock_regressions > 0 || sample == 0)); then result=CAPTURED_WITH_ERRORS_NOT_VERDICT; exit_code=1; fi
printf 'finished=%s\nsamples=%s\nfirst_sample_start=%s\nlast_sample_start=%s\nlast_sample_finish=%s\nmax_sample_interval_ns=%s\nclock_regressions=%s\ncapture_errors=%s\nlog_sampling_gaps=%s\nexit_observations=%s\noom_observations=%s\nwindow_coverage=NOT_ASSERTED\ntermination_reason=%s\nresult=%s\nexit_code=%s\n' "$(now)" "$sample" "$first_sample_start" "$last_sample_start" "$last_sample_finish" "$max_sample_interval_ns" "$clock_regressions" "$capture_errors" "$log_gaps" "$exit_observations" "$oom_observations" "$termination" "$result" "$exit_code" >>"$out/session.txt" || exit 1
exit "$exit_code"
