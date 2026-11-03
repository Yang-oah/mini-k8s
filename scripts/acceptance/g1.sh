#!/usr/bin/env bash
set -uo pipefail

# Runs only against a newly started, isolated etcd data directory. No existing
# cluster is touched; the spawned process and test directory belong to this run.
cd "$(dirname "$0")/../.." || exit 2
for tool in go gofmt etcd curl sha256sum find sort xargs grep mktemp date mkdir rm sleep; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'BLOCKED: missing dependency %s\n' "$tool" >&2
    exit 2
  fi
done
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
    # A process terminated by SIGTERM normally has a non-zero wait status.
    if ! kill -0 "$etcd_pid" 2>/dev/null; then wait "$etcd_pid" 2>/dev/null || :; fi
  fi
  if ! rm -rf -- "$tmp" || [[ -e "$tmp" ]]; then failed=true; fi
  cleaned=true
  if [[ "$failed" == true ]]; then cleanup_result=FAIL; return 1; fi
  cleanup_result=PASS
}
trap 'cleanup || true' EXIT
started="$(date -u +%FT%TZ)"
client_port="${G1_ETCD_CLIENT_PORT:-23791}"
peer_port="${G1_ETCD_PEER_PORT:-23801}"
endpoint="http://127.0.0.1:$client_port"
{
  printf 'started=%s\n' "$started"
  go version
  etcd --version
  printf 'source_sha256='; { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum
} > "$out/environment.txt"

if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then
  cleanup || true
  printf '# G1 acceptance\n\n- Result: BLOCKED\n- Reason: etcd endpoint is already occupied; choose unused ports.\n- Temporary directory cleanup: %s\n' "$cleanup_result" > "$out/G1.md"
  printf 'BLOCKED: etcd endpoint is already occupied. Evidence: %s\n' "$out" >&2
  exit 2
fi
etcd --name g1 --data-dir "$tmp/etcd" --listen-client-urls "$endpoint" \
  --advertise-client-urls "$endpoint" --listen-peer-urls "http://127.0.0.1:$peer_port" \
  --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" \
  --initial-cluster "g1=http://127.0.0.1:$peer_port" >"$out/etcd.log" 2>&1 &
etcd_pid=$!
result=PASS
reason='All mandatory checks and cleanup passed.'
ready=false
for _ in {1..100}; do
  if ! kill -0 "$etcd_pid" 2>/dev/null; then break; fi
  if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then ready=true; break; fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then result=BLOCKED; reason='Isolated etcd failed to become ready; inspect etcd.log.'; fi
if [[ "$result" == PASS ]]; then
  if ! go build -o "$tmp/mini-api" ./cmd/mini-api >"$out/build.log" 2>&1; then result=FAIL; reason='API build failed.'; fi
fi
if [[ "$result" == PASS ]]; then
  export G1_ETCD_ENDPOINT="$endpoint" G1_API_BINARY="$tmp/mini-api" G1_API_LOG="$out/api.log"
  if ! go test -count=1 -timeout=120s -v ./internal/acceptance >"$out/g1.log" 2>&1; then result=FAIL; reason='G1 assertions, startup, or cleanup failed; inspect g1.log.'; fi
fi
if [[ "$result" == PASS ]]; then
  unset G1_ETCD_ENDPOINT G1_API_BINARY G1_API_LOG
  if [[ -n "$(gofmt -l cmd internal)" ]]; then result=FAIL; reason='gofmt check failed.'; fi
  if ! go vet ./... >"$out/vet.log" 2>&1; then result=FAIL; reason='go vet failed.'; fi
  if ! go test -count=1 ./... >"$out/test.log" 2>&1; then result=FAIL; reason='go test failed.'; fi
  if ! go build ./... >"$out/build-all.log" 2>&1; then result=FAIL; reason='go build failed.'; fi
fi
if ! cleanup; then
  if [[ "$result" == PASS ]]; then result=FAIL; fi
  reason="$reason Isolated etcd or temporary directory cleanup failed."
fi
{
  printf '# G1 acceptance\n\n- Result: %s\n- Started (UTC): %s\n- Finished (UTC): %s\n- Command: scripts/acceptance/g1.sh\n- Environment and source checksum: environment.txt\n- Result details: %s\n\n' "$result" "$started" "$(date -u +%FT%TZ)" "$reason"
  printf '| Check | Result | Evidence |\n| --- | --- | --- |\n'
  for id in 01 02 03 04 05 06; do
    if [[ -f "$out/g1.log" ]] && grep -q -- "--- PASS: TestG1/G1-$id" "$out/g1.log"; then state=PASS
    elif [[ -f "$out/g1.log" ]] && grep -q -- "--- FAIL: TestG1/G1-$id" "$out/g1.log"; then state=FAIL
    else state=NOT_RUN; fi
    printf '| G1-%s | %s | g1.log |\n' "$id" "$state"
  done
  printf '\n- Isolated etcd process and temporary directory cleanup: %s\n' "$cleanup_result"
  printf '%s\n' '- Resource cleanup: verified by the acceptance test when it reaches cleanup; consult g1.log on failure or if not run.' '- Logs: api.log, etcd.log, g1.log and standard check logs.'
} > "$out/G1.md"
printf '%s: %s\nEvidence: %s\n' "$result" "$reason" "$out"
case "$result" in PASS) exit 0;; FAIL) exit 1;; *) exit 2;; esac
