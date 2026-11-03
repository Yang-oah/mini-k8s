#!/usr/bin/env bash
set -uo pipefail

# G2 仅验证真实 API/etcd 上的资源决策；不能替代真实容器链路的 G3。
cd "$(dirname "$0")/../.." || exit 2
for tool in bash go gofmt etcd curl sha256sum find sort xargs grep sed tail mktemp date mkdir rm sleep; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'BLOCKED: missing dependency %s\n' "$tool" >&2
    exit 2
  fi
done
run_id="$(date -u +%Y%m%dT%H%M%SZ)-g2-$$"
out="$(pwd)/artifacts/acceptance/$run_id"
mkdir -p "$out" || exit 2
tmp="$(mktemp -d)" || exit 2
etcd_pid=''
cleaned=false
cleanup_result=NOT_RUN
cleanup() {
  [[ "$cleaned" == true ]] && return 0
  local failed=false
  if [[ -n "$etcd_pid" ]]; then
    if kill -0 "$etcd_pid" 2>/dev/null && ! kill "$etcd_pid" 2>/dev/null; then failed=true; fi
    for _ in {1..50}; do
      if ! kill -0 "$etcd_pid" 2>/dev/null; then break; fi
      sleep 0.1
    done
    if kill -0 "$etcd_pid" 2>/dev/null; then
      failed=true
      kill -KILL "$etcd_pid" 2>/dev/null || true
    fi
    if ! kill -0 "$etcd_pid" 2>/dev/null; then wait "$etcd_pid" 2>/dev/null || :; fi
  fi
  if ! rm -rf -- "$tmp" || [[ -e "$tmp" ]]; then failed=true; fi
  cleaned=true
  if [[ "$failed" == true ]]; then cleanup_result=FAIL; return 1; fi
  cleanup_result=PASS
}
trap 'cleanup || true' EXIT
started="$(date -u +%FT%TZ)"
client_port="${G2_ETCD_CLIENT_PORT:-23792}"
peer_port="${G2_ETCD_PEER_PORT:-23802}"
endpoint="http://127.0.0.1:$client_port"
{
  printf 'started=%s\n' "$started"
  go version
  etcd --version
  printf 'source_sha256='; { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum
} > "$out/environment.txt"

result=PASS
reason='All mandatory checks and cleanup passed.'
g1_evidence=''
# G2 改动共享 API，因此必须在同一源码上先重跑真实 G1，而非信任旧报告。
unset G2_MANAGER_BINARY G2_MANAGER_LOG G1_ETCD_ENDPOINT G1_API_BINARY G1_API_LOG
if bash scripts/acceptance/g1.sh >"$out/g1-prerequisite.log" 2>&1; then
  g1_evidence="$(sed -n 's/^Evidence: //p' "$out/g1-prerequisite.log" | tail -n 1)"
  if [[ -z "$g1_evidence" || ! -f "$g1_evidence/G1.md" ]] || ! grep -q '^- Result: PASS$' "$g1_evidence/G1.md"; then
    result=BLOCKED; reason='G1 did not produce a complete PASS report.'
  fi
  if [[ "$result" == PASS ]]; then
    for id in 01 02 03 04 05 06; do
      if ! grep -Fq "| G1-$id | PASS |" "$g1_evidence/G1.md"; then result=BLOCKED; reason="G1-$id did not pass."; fi
    done
    if [[ "$(grep '^source_sha256=' "$out/environment.txt")" != "$(grep '^source_sha256=' "$g1_evidence/environment.txt")" ]]; then
      result=BLOCKED; reason='Source changed between G1 and G2.'
    fi
  fi
else
  result=BLOCKED; reason='Current-version G1 prerequisite did not pass; inspect g1-prerequisite.log.'
fi

# G2 使用独立 etcd 数据目录，避免触碰已有集群。
if [[ "$result" == PASS ]] && curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then
  result=BLOCKED; reason='G2 etcd endpoint is already occupied.'
fi
if [[ "$result" == PASS ]]; then
  etcd --name g2 --data-dir "$tmp/etcd" --listen-client-urls "$endpoint" \
    --advertise-client-urls "$endpoint" --listen-peer-urls "http://127.0.0.1:$peer_port" \
    --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" \
    --initial-cluster "g2=http://127.0.0.1:$peer_port" >"$out/etcd.log" 2>&1 &
  etcd_pid=$!
  ready=false
  for _ in {1..100}; do
    if ! kill -0 "$etcd_pid" 2>/dev/null; then break; fi
    if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then ready=true; break; fi
    sleep 0.1
  done
  if [[ "$ready" != true ]]; then result=BLOCKED; reason='Isolated etcd failed to become ready.'; fi
fi
if [[ "$result" == PASS ]]; then
  if ! go build -o "$tmp/mini-api" ./cmd/mini-api >"$out/build-api.log" 2>&1 || \
     ! go build -o "$tmp/mini-manager" ./cmd/mini-manager >"$out/build-manager.log" 2>&1; then
    result=FAIL; reason='API or manager build failed.'
  fi
fi
if [[ "$result" == PASS ]]; then
  export G1_ETCD_ENDPOINT="$endpoint" G1_API_BINARY="$tmp/mini-api" G1_API_LOG="$out/api.log"
  export G2_MANAGER_BINARY="$tmp/mini-manager" G2_MANAGER_LOG="$out/manager.log"
  if ! go test -count=1 -timeout=180s -run '^TestG2$' -v ./internal/acceptance >"$out/g2.log" 2>&1; then
    result=FAIL; reason='G2 assertions, startup, or resource cleanup failed; inspect g2.log.'
  fi
fi
if [[ "$result" == PASS ]]; then
  unset G1_ETCD_ENDPOINT G1_API_BINARY G1_API_LOG G2_MANAGER_BINARY G2_MANAGER_LOG
  if [[ -n "$(gofmt -l cmd internal)" ]]; then result=FAIL; reason='gofmt check failed.'; fi
  if ! go vet ./... >"$out/vet.log" 2>&1; then result=FAIL; reason='go vet failed.'; fi
  if ! go test -count=1 ./... >"$out/test.log" 2>&1; then result=FAIL; reason='go test failed.'; fi
  if ! go build ./... >"$out/build-all.log" 2>&1; then result=FAIL; reason='go build failed.'; fi
  final_hash="$(printf 'source_sha256='; { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum)"
  if [[ "$final_hash" != "$(grep '^source_sha256=' "$out/environment.txt")" ]]; then result=FAIL; reason='Source changed during acceptance.'; fi
fi
if ! cleanup; then
  if [[ "$result" == PASS ]]; then result=FAIL; fi
  reason="$reason Isolated etcd or temporary directory cleanup failed."
fi
{
  printf '# G2 acceptance\n\n- Result: %s\n- Started (UTC): %s\n- Finished (UTC): %s\n- Command: scripts/acceptance/g2.sh\n' "$result" "$started" "$(date -u +%FT%TZ)"
  printf -- '- Current-version G1 evidence: %s\n- Environment and source checksum: environment.txt\n- Details: %s\n\n' "$g1_evidence" "$reason"
  printf '| Check | Result | Evidence |\n| --- | --- | --- |\n'
  for id in 01 02 03 04 05 06; do
    if [[ -f "$out/g2.log" ]] && grep -q -- "--- PASS: TestG2/G2-$id" "$out/g2.log"; then state=PASS
    elif [[ -f "$out/g2.log" ]] && grep -q -- "--- FAIL: TestG2/G2-$id" "$out/g2.log"; then state=FAIL
    else state=NOT_RUN; fi
    printf '| G2-%s | %s | g2.log |\n' "$id" "$state"
  done
  printf '\n- Isolated etcd and temporary directory cleanup: %s\n' "$cleanup_result"
  printf '%s\n' '- Resource cleanup is verified by the test; see g2.log if it failed or did not run.' '- Scope: resource model reconciliation using simulated node status; real container recovery was not tested.'
} > "$out/G2.md"
printf '%s: %s\nEvidence: %s\n' "$result" "$reason" "$out"
case "$result" in PASS) exit 0;; FAIL) exit 1;; *) exit 2;; esac
