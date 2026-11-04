#!/usr/bin/env bash
set -euo pipefail

# 只用伪造的 crictl/iptables-save 和临时目录验证采集器，不访问真实 CRI 或规则。
[[ $EUID -eq 0 ]] || { printf 'run this isolated test with sudo\n' >&2; exit 2; }
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
# /tmp 可由普通用户写入；被测脚本要求输出目录祖先均为可信 root 目录。
tmp=$(mktemp -d /var/lib/g5-node-evidence-test.XXXXXXXX)
collector=''
cleanup() {
  if [[ -n "$collector" ]] && kill -0 "$collector" 2>/dev/null; then
    kill -TERM "$collector" 2>/dev/null || true
    wait "$collector" 2>/dev/null || true
  fi
  rm -rf -- "$tmp"
}
trap cleanup EXIT
mkdir -p "$tmp/bin" "$tmp/leases"
printf '%s\n' 'fake lease' >"$tmp/leases/10.88.0.1"
cat >"$tmp/bin/crictl" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$G5_TEST_ARGS"
case " $* " in
  *' logs '*) if [[ "${G5_TEST_LARGE_LOGS:-}" == 1 ]]; then printf '%02048d\n' 0; else printf 'test container log\n'; fi ;;
  *) printf '{"items":[]}\n' ;;
esac
EOF
cat >"$tmp/bin/iptables-save" <<'EOF'
#!/usr/bin/env bash
if [[ "${2:-}" == filter ]]; then printf 'permission denied\n' >&2; exit 4; fi
printf '%s\n' ':MK8S-SVC - [0:0]' '-A MK8S-SVC -j RETURN' '-A FORWARD -j UNRELATED'
EOF
cat >"$tmp/bin/date" <<'EOF'
#!/usr/bin/env bash
touch "$G5_TEST_UNTRUSTED_PATH_USED"
exit 99
EOF
chmod 0755 "$tmp/bin/crictl" "$tmp/bin/iptables-save" "$tmp/bin/date"
# 只有 --tool-dir 中被显式选定的工具可执行；普通 PATH 中的 date 必须被忽略。
export PATH="$tmp/bin:$PATH" G5_TEST_UNTRUSTED_PATH_USED="$tmp/path-used"
export G5_TEST_ARGS="$tmp/crictl-args"

if bash "$script_dir/g5-node-evidence.sh" --not-an-option >/dev/null 2>&1; then
  printf 'unknown option was accepted\n' >&2; exit 1
fi
if bash "$script_dir/g5-node-evidence.sh" --output "$tmp/existing" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --pod-uid pod-a --pod-uid pod-a >/dev/null 2>&1; then
  printf 'duplicate UID was accepted\n' >&2; exit 1
fi
if bash "$script_dir/g5-node-evidence.sh" --output "$tmp/excess" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --pod-uid a --pod-uid b --pod-uid c --pod-uid d --pod-uid e >/dev/null 2>&1; then
  printf 'too many concurrent UID queries were accepted\n' >&2; exit 1
fi

out="$tmp/evidence"
set +e
bash "$script_dir/g5-node-evidence.sh" --output "$out" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --pod-uid pod-a --log-container aaaaaaaaaaaa \
  --iptables project --stats --tool-dir "$tmp/bin" --duration 1 >/dev/null
collector_rc=$?
set -e
[[ $collector_rc -eq 1 ]] || { printf 'capture errors must return exit 1\n' >&2; exit 1; }
first="$out/sample-000001"
grep -qx 'exit=0' "$first/leases.meta"
grep -qx '10.88.0.1' "$first/leases.out"
grep -qx 'exit=4' "$first/iptables-filter.meta"
grep -qx 'permission denied' "$first/iptables-filter.err"
grep -q '^:MK8S-SVC ' "$first/iptables-nat.out"
if grep -q UNRELATED "$first/iptables-nat.out"; then
  printf 'project mode leaked unrelated rules\n' >&2; exit 1
fi
grep -qx 'test container log' "$first/container-aaaaaaaaaaaa.out"
grep -qx 'exit=0' "$first/stats-aaaaaaaaaaaa.meta"
grep -qx 'result=CAPTURED_WITH_ERRORS_NOT_VERDICT' "$out/session.txt"
grep -q -- '--label mini-k8s.io/managed=true --label mini-k8s.io/pod-uid=pod-a' "$G5_TEST_ARGS"
grep -q -- 'stats -o json aaaaaaaaaaaa' "$G5_TEST_ARGS"
[[ ! -e "$G5_TEST_UNTRUSTED_PATH_USED" ]] || { printf 'inherited PATH was used\n' >&2; exit 1; }
grep -qx 'previous_start_interval_ns=NA' "$first/sample.meta"
grep -q '^first_sample_start=20' "$out/session.txt"
grep -q '^last_sample_start=20' "$out/session.txt"
grep -q '^last_sample_finish=20' "$out/session.txt"
grep -Eq '^max_sample_interval_ns=[0-9]+$' "$out/session.txt"
grep -qx 'window_coverage=NOT_ASSERTED' "$out/session.txt"
if [[ -f "$out/sample-000002/sample.meta" ]]; then
  grep -Eq '^previous_start_interval_ns=[0-9]+$' "$out/sample-000002/sample.meta"
fi

# 输出超过预算时必须限制字节数并显式标记截断，不能把不完整日志当作成功。
capped="$tmp/capped"
set +e
G5_TEST_LARGE_LOGS=1 bash "$script_dir/g5-node-evidence.sh" --output "$capped" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --log-container aaaaaaaaaaaa --log-budget-bytes 1024 --tool-dir "$tmp/bin" --duration 1 >/dev/null
collector_rc=$?
set -e
[[ $collector_rc -eq 1 ]] || { printf 'truncated logs must return exit 1\n' >&2; exit 1; }
[[ $(wc -c <"$capped/sample-000001/container-aaaaaaaaaaaa.out") -le 1024 ]]
grep -qx 'truncated=true' "$capped/sample-000001/container-aaaaaaaaaaaa.meta"
grep -qx 'result=CAPTURED_WITH_ERRORS_NOT_VERDICT' "$capped/session.txt"

# SIGTERM 后应保留已完成样本和结束标记，而不是留下半写入的子进程。
signaled="$tmp/signaled"
bash "$script_dir/g5-node-evidence.sh" --output "$signaled" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --interval 1 --tool-dir "$tmp/bin" >"$tmp/signal.out" 2>"$tmp/signal.err" &
collector=$!
for _ in {1..100}; do
  [[ -f "$signaled/sample-000001/sample.meta" ]] && break
  sleep 0.05
done
[[ -f "$signaled/sample-000001/sample.meta" ]]
kill -TERM "$collector"
wait "$collector"
collector=''
grep -q '^finished=' "$signaled/session.txt"
grep -q '^finish=' "$signaled/sample-000001/sample.meta"
grep -qx 'termination_reason=SIGTERM' "$signaled/session.txt"

# root 不能向普通用户可替换的目录或租约目录写证据。
mkdir -m 0777 "$tmp/untrusted"
chmod 0777 "$tmp/untrusted"
if bash "$script_dir/g5-node-evidence.sh" --output "$tmp/untrusted/evidence" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --tool-dir "$tmp/bin" >/dev/null 2>&1; then
  printf 'group/other-writable output ancestor was accepted\n' >&2; exit 1
fi
if bash "$script_dir/g5-node-evidence.sh" --output "$tmp/leases/evidence" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --tool-dir "$tmp/bin" >/dev/null 2>&1; then
  printf 'output inside lease directory was accepted\n' >&2; exit 1
fi
chmod 0777 "$tmp/bin/crictl"
if bash "$script_dir/g5-node-evidence.sh" --output "$tmp/untrusted-tool" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --tool-dir "$tmp/bin" >/dev/null 2>&1; then
  printf 'group/other-writable tool was accepted\n' >&2; exit 1
fi
chmod 0755 "$tmp/bin/crictl"
ln -s /tmp "$tmp/untrusted-link"
if bash "$script_dir/g5-node-evidence.sh" --output "$tmp/untrusted-link-output" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --tool-dir "$tmp/untrusted-link" >/dev/null 2>&1; then
  printf 'tool directory resolving to untrusted ancestor was accepted\n' >&2; exit 1
fi

if bash "$script_dir/g5-node-evidence.sh" --output "$out" --lease-dir "$tmp/leases" \
  --cri-endpoint unix:///tmp/cri.sock --tool-dir "$tmp/bin" >/dev/null 2>&1; then
  printf 'existing evidence directory was overwritten\n' >&2; exit 1
fi
printf 'PASS: isolated collector behavior\n'
