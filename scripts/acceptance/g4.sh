#!/usr/bin/env bash
set -uo pipefail

# 本入口当前只做无副作用的双节点预检。真实运行时与 API 双侧断言尚未接入，
# 因此任何预检结果都不能报告 G4 PASS；具体验收要求见 docs/g4.md。
cd "$(dirname "$0")/../.." || exit 2
umask 077
out="$(pwd)/artifacts/acceptance/$(date -u +%Y%m%dT%H%M%SZ)-g4-preflight-$$"
mkdir -p "$out" || exit 2
started="$(date -u +%FT%TZ)"
reason='G4 real two-worker acceptance runner is not implemented; preflight cannot establish G4 PASS.'
worker_a="${G4_WORKER_A:-}"
worker_b="${G4_WORKER_B:-}"

# SSH 目标仅允许别名或 user@host，杜绝参数和远程命令注入。
valid_target() {
  [[ "$1" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.@-]*$ && "$1" != *@*@* ]]
}

inspect_worker() {
  local target="$1" output="$2"
  # 仅收集系统身份和命令可用性，不请求 sudo、不操作 CRI/CNI。
  ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=8 -- "$target" \
    'uname -s; cat /proc/sys/kernel/random/boot_id; command -v crictl' >"$output" 2>&1
}

if ! command -v ssh >/dev/null 2>&1; then
  reason='SSH is required for the read-only two-worker preflight.'
elif ! valid_target "$worker_a" || ! valid_target "$worker_b"; then
  reason='Set G4_WORKER_A and G4_WORKER_B to two SSH aliases or user@host targets.'
elif ! inspect_worker "$worker_a" "$out/worker-a.txt"; then
  reason='Cannot inspect worker A via non-interactive SSH; see worker-a.txt.'
elif ! inspect_worker "$worker_b" "$out/worker-b.txt"; then
  reason='Cannot inspect worker B via non-interactive SSH; see worker-b.txt.'
else
  mapfile -t a <"$out/worker-a.txt"
  mapfile -t b <"$out/worker-b.txt"
  if [[ "${a[0]:-}" != Linux || "${b[0]:-}" != Linux || -z "${a[1]:-}" || -z "${b[1]:-}" || -z "${a[2]:-}" || -z "${b[2]:-}" ]]; then
    reason='Both workers must be Linux hosts with readable boot IDs and crictl installed.'
  elif [[ "${a[1]}" == "${b[1]}" ]]; then
    reason='The two SSH targets appear to refer to the same running host.'
  fi
fi

{
  printf '# G4 preflight\n\n- Result: BLOCKED\n- Started (UTC): %s\n- Finished (UTC): %s\n' "$started" "$(date -u +%FT%TZ)"
  printf -- '- Details: %s\n\n' "$reason"
  printf '| Check | Result | Evidence |\n| --- | --- | --- |\n'
  for check in 01 02 03 04 05 06; do printf '| G4-%s | NOT_RUN | Real two-worker acceptance not executed |\n' "$check"; done
  printf '\n%s\n' '- This read-only preflight does not verify CRI access, routing, API behavior, workloads, or cleanup.'
} >"$out/G4.md"
printf 'BLOCKED: %s\nEvidence: %s\n' "$reason" "$out"
exit 2
