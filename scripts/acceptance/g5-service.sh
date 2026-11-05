#!/usr/bin/env bash
set -uo pipefail

# 只采集 API 时间序列；它不能代替双节点 CRI、TCP 和宿主机规则证据。
cd "$(dirname "$0")/../.." || exit 2
umask 077
out="$(pwd)/artifacts/acceptance/$(date -u +%Y%m%dT%H%M%SZ)-g5-service-$$"
mkdir -p "$out" || exit 2
started="$(date -u +%FT%TZ)"
probe="${G5_PROBE:-}"
server="${G5_API:-}"
ca="${G5_CA_FILE:-}"
token="${G5_ADMIN_TOKEN_FILE:-}"
duration="${G5_OBSERVE_DURATION:-300s}"
interval="${G5_OBSERVE_INTERVAL:-200ms}"
state='NOT_RUN'
reason='API sampling does not determine TCP routing, endpoint identity, cleanup or the G5 gate.'

if [[ -z "$probe" || ! -x "$probe" || -z "$server" || ! -r "$ca" || ! -r "$token" ]]; then
  reason='Set G5_PROBE, G5_API, G5_CA_FILE and G5_ADMIN_TOKEN_FILE to test-cluster values.'
elif "$probe" observe -server "$server" -ca-file "$ca" -token-file "$token" \
      -duration "$duration" -interval "$interval" >"$out/api-series.jsonl" 2>"$out/observe.err"; then
  state='CAPTURED'
else
  state='FAILED'
  reason='API observation failed or was interrupted; inspect observe.err and preserve partial evidence.'
fi

{
  printf '# G5 Service evidence collection\n\n'
  printf -- '- Collection: %s\n- Gate: NOT_EVALUATED\n- Started (UTC): %s\n- Finished (UTC): %s\n' "$state" "$started" "$(date -u +%FT%TZ)"
  printf -- '- Details: %s\n' "$reason"
  printf '\n%s\n' '- This collector does not decide Service or the G5 gate; see docs/g5-service.md.'
} >"$out/G5-service.md"
printf 'Collection: %s; G5 gate: NOT_EVALUATED\n%s\nEvidence: %s\n' "$state" "$reason" "$out"
case "$state" in CAPTURED) exit 0 ;; FAILED) exit 1 ;; *) exit 2 ;; esac
