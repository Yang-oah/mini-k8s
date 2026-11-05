#!/usr/bin/env bash
set -uo pipefail

# 本工具只采样 API 状态；运行时、探测次数及跨节点恢复仍须人工核对。
# 没有完整证据之前绝不能将 G5 或 readiness 标记为 PASS。
cd "$(dirname "$0")/../.." || exit 2
umask 077
out="$(pwd)/artifacts/acceptance/$(date -u +%Y%m%dT%H%M%SZ)-g5-readiness-$$"
mkdir -p "$out" || exit 2
started="$(date -u +%FT%TZ)"
reason='API sampling does not establish probe counts, runtime identity or recovery; inspect the documented evidence.'
probe="${G5_PROBE:-}"
server="${G5_API:-}"
ca="${G5_CA_FILE:-}"
token="${G5_ADMIN_TOKEN_FILE:-}"
duration="${G5_OBSERVE_DURATION:-90s}"
interval="${G5_OBSERVE_INTERVAL:-200ms}"
result='NOT_RUN'

if [[ -z "$probe" || ! -x "$probe" || -z "$server" || ! -r "$ca" || ! -r "$token" ]]; then
  reason='Set G5_PROBE to the built g4-probe and G5_API, G5_CA_FILE, G5_ADMIN_TOKEN_FILE to the test cluster connection.'
else
  printf '%s\n' "$started" >"$out/observe-start.txt"
  # 输出会省略 Pod 环境变量，凭据从文件读取，不能将 token 内容写入命令行或证据。
  if "$probe" observe -server "$server" -ca-file "$ca" -token-file "$token" \
      -duration "$duration" -interval "$interval" >"$out/api-series.jsonl" 2>"$out/observe.err"; then
    result='CAPTURED'
  else
    result='FAILED'
    reason='API observation failed or was interrupted; inspect observe.err and preserve partial evidence.'
  fi
fi

{
  printf '# G5 readiness evidence collection\n\n'
  printf -- '- Collection: %s\n- Gate: NOT_EVALUATED\n- Started (UTC): %s\n- Finished (UTC): %s\n' "$result" "$started" "$(date -u +%FT%TZ)"
  printf -- '- Details: %s\n' "$reason"
  printf '\n%s\n' '- This collector does not decide readiness or the G5 gate. See docs/g5-readiness.md.'
} >"$out/G5-readiness.md"
printf 'Collection: %s; readiness/G5 gate: NOT_EVALUATED\n%s\nEvidence: %s\n' "$result" "$reason" "$out"
case "$result" in CAPTURED) exit 0 ;; FAILED) exit 1 ;; *) exit 2 ;; esac
