#!/usr/bin/env bash
set -uo pipefail
source_dir="${R1_SOURCE_DIR:-/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5}"
expected_sha="${R1_EXPECTED_SHA:-4d6bcb2b61e61797a9329a67a389df46176922debce847c02ca7a7e60a222f5a}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
export PATH="/home/lihaoyang/.local/go/bin:/home/lihaoyang/.local/bin:$PATH"
for tool in go etcd curl ss sha256sum find sort xargs awk mktemp cp; do
  command -v "$tool" >/dev/null 2>&1 || { echo "BLOCKED: missing $tool"; exit 2; }
done
cd "$source_dir" || exit 2
actual="$( { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum | awk '{print $1}')"
[[ "$actual" == "$expected_sha" ]] || { echo 'BLOCKED: source checksum mismatch'; exit 2; }
[[ -f "$script_dir/r1-process-faults.go.txt" ]] || { echo 'BLOCKED: missing test source'; exit 2; }
out="$(mktemp -d "$source_dir/r1-process-faults-XXXXXXXX")" || exit 2
mkdir "$out/source"
cp -a cmd internal go.mod go.sum "$out/source/" || exit 2
cp "$script_dir/r1-process-faults.go.txt" "$out/source/internal/acceptance/r1_process_faults_test.go" || exit 2
{ printf 'accepted_source_sha256=%s\n' "$actual"; sha256sum "$script_dir/r1-process-faults.go.txt"; go version; etcd --version; } >"$out/environment.txt"
client="${R1_ETCD_CLIENT_PORT:-23797}"
peer="${R1_ETCD_PEER_PORT:-23807}"
if ss -ltn | awk '{print $4}' | grep -Eq ":(${client}|${peer})$"; then echo "BLOCKED: isolated ports occupied. Evidence: $out"; exit 2; fi
etcd --name r1-process-faults-isolated --data-dir "$out/etcd-data" \
  --listen-client-urls "http://127.0.0.1:$client" --advertise-client-urls "http://127.0.0.1:$client" \
  --listen-peer-urls "http://127.0.0.1:$peer" --initial-advertise-peer-urls "http://127.0.0.1:$peer" \
  --initial-cluster "r1-process-faults-isolated=http://127.0.0.1:$peer" >"$out/etcd.log" 2>&1 &
pid=$!
cleanup() { kill -CONT "$pid" 2>/dev/null || :; kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; }
trap cleanup EXIT
endpoint="http://127.0.0.1:$client"
ready=false
for _ in {1..30}; do
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo "BLOCKED: isolated etcd unavailable. Evidence: $out"; exit 2; }
cd "$out/source" || exit 2
if ! go build -o "$out/mini-api" ./cmd/mini-api >"$out/build-api.log" 2>&1 || ! go build -o "$out/mini-manager" ./cmd/mini-manager >"$out/build-manager.log" 2>&1; then
  echo "FAIL: building API or manager. Evidence: $out"; exit 1
fi
sha256sum "$out/mini-api" "$out/mini-manager" >>"$out/environment.txt"
echo 'Testing PREPARED operation across actual process faults...'
if ! G1_ETCD_ENDPOINT="$endpoint" G1_API_BINARY="$out/mini-api" G1_API_LOG="$out/api.log" \
     G2_MANAGER_BINARY="$out/mini-manager" G2_MANAGER_LOG="$out/manager.log" R1_FAULT_ETCD_PID="$pid" \
     go test -count=1 -timeout=180s -v ./internal/acceptance -run '^TestR1ProcessFaults$' >"$out/tests.log" 2>&1; then
  tail -40 "$out/tests.log"; echo "FAIL: process fault tests. Evidence: $out"; exit 1
fi
for mode in controller-restart api-restart etcd-pause; do
  grep -q "^    --- PASS: TestR1ProcessFaults/$mode" "$out/tests.log" || { echo "FAIL: missing $mode result. Evidence: $out"; exit 1; }
done
cleanup
trap - EXIT
echo 'PASS: PREPARED identity survived isolated controller/API restarts and etcd pause.'
printf 'Evidence: %s\n' "$out"
