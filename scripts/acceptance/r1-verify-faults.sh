#!/usr/bin/env bash
# Run additional fault tests in a copy so previously accepted source stays unchanged.
set -uo pipefail
source_dir="${R1_SOURCE_DIR:-/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5}"
expected_sha="${R1_EXPECTED_SHA:-4d6bcb2b61e61797a9329a67a389df46176922debce847c02ca7a7e60a222f5a}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
export PATH="/home/lihaoyang/.local/go/bin:/home/lihaoyang/.local/bin:$PATH"
for tool in go etcd curl ss sha256sum find sort xargs awk mktemp cp; do
  command -v "$tool" >/dev/null 2>&1 || { echo "BLOCKED: missing $tool"; exit 2; }
done
cd "$source_dir" || exit 2
actual_sha="$( { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum | awk '{print $1}')"
[[ "$actual_sha" == "$expected_sha" ]] || { echo 'BLOCKED: source checksum mismatch'; exit 2; }
for file in r1-race-integration.go.txt r1-http-lost-response.go.txt; do
  [[ -f "$script_dir/$file" ]] || { echo "BLOCKED: missing $file"; exit 2; }
done
out="$(mktemp -d "$source_dir/r1-faults-XXXXXXXX")" || exit 2
mkdir "$out/source"
cp -a cmd internal go.mod go.sum "$out/source/" || exit 2
cp "$script_dir/r1-race-integration.go.txt" "$out/source/internal/server/r1_acceptance_race_test.go"
cp "$script_dir/r1-http-lost-response.go.txt" "$out/source/internal/server/r1_acceptance_http_test.go"
{
  printf 'accepted_source_sha256=%s\n' "$actual_sha"
  sha256sum "$script_dir/r1-race-integration.go.txt" "$script_dir/r1-http-lost-response.go.txt"
  go version
  etcd --version
} >"$out/environment.txt"
client_port="${R1_ETCD_CLIENT_PORT:-23797}"
peer_port="${R1_ETCD_PEER_PORT:-23807}"
if ss -ltn | awk '{print $4}' | grep -Eq ":(${client_port}|${peer_port})$"; then
  echo "BLOCKED: isolated etcd port already in use. Evidence: $out"; exit 2
fi
etcd --name r1-faults --data-dir "$out/etcd-data" \
  --listen-client-urls "http://127.0.0.1:$client_port" --advertise-client-urls "http://127.0.0.1:$client_port" \
  --listen-peer-urls "http://127.0.0.1:$peer_port" --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" \
  --initial-cluster "r1-faults=http://127.0.0.1:$peer_port" >"$out/etcd.log" 2>&1 &
pid=$!
cleanup() { kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; }
trap cleanup EXIT
endpoint="http://127.0.0.1:$client_port"
ready=false
for _ in {1..30}; do
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
if [[ "$ready" != true ]]; then echo "BLOCKED: isolated etcd failed to start. Evidence: $out"; exit 2; fi
cd "$out/source" || exit 2
echo 'Running real-etcd commit/cancel and lost-response tests...'
if ! G2_ETCD_ENDPOINT="$endpoint" go test -race -count=1 -v -timeout=180s ./internal/server -run '^TestR1(AcceptanceCommitCancelRace|HTTPCommitResponseLost)$' >"$out/tests.log" 2>&1; then
  tail -30 "$out/tests.log"
  echo "FAIL: R1 fault tests. Evidence: $out"; exit 1
fi
if ! grep -q '^--- PASS: TestR1AcceptanceCommitCancelRace' "$out/tests.log" || ! grep -q '^--- PASS: TestR1HTTPCommitResponseLost' "$out/tests.log"; then
  echo "FAIL: required test result missing. Evidence: $out"; exit 1
fi
cleanup
trap - EXIT
echo 'PASS: real-etcd commit/cancel and lost-response tests passed.'
printf 'Evidence: %s\n' "$out"
