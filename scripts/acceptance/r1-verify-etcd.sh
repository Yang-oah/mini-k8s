#!/usr/bin/env bash
# 在独立 etcd 上复验 R1 的 Server、Controller 和 Service 事务测试。
set -uo pipefail

source_dir="${R1_SOURCE_DIR:-/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5}"
expected_sha="4d6bcb2b61e61797a9329a67a389df46176922debce847c02ca7a7e60a222f5a"
export PATH="/home/lihaoyang/.local/go/bin:/home/lihaoyang/.local/bin:$PATH"
for tool in go etcd curl ss sha256sum find sort xargs awk mktemp; do
  command -v "$tool" >/dev/null 2>&1 || { printf 'BLOCKED: missing %s\n' "$tool"; exit 2; }
done
cd "$source_dir" || { echo 'BLOCKED: source directory missing'; exit 2; }
actual_sha="$( { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum | awk '{print $1}')"
[[ "$actual_sha" == "$expected_sha" ]] || { echo 'BLOCKED: source checksum mismatch'; exit 2; }
out="$(mktemp -d "$source_dir/r1-etcd-check-XXXXXXXX")" || exit 2
pid=''
cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || return 1
    wait "$pid" 2>/dev/null || :
  fi
}
trap 'cleanup || true' EXIT
client_port="${R1_ETCD_CLIENT_PORT:-23797}"
peer_port="${R1_ETCD_PEER_PORT:-23807}"
if ss -ltn | awk '{print $4}' | grep -Eq ":(${client_port}|${peer_port})$"; then
  echo "BLOCKED: test port already in use. Evidence: $out"; exit 2
fi
etcd --name r1-isolated --data-dir "$out/data" \
  --listen-client-urls "http://127.0.0.1:$client_port" \
  --advertise-client-urls "http://127.0.0.1:$client_port" \
  --listen-peer-urls "http://127.0.0.1:$peer_port" \
  --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" \
  --initial-cluster "r1-isolated=http://127.0.0.1:$peer_port" >"$out/etcd.log" 2>&1 &
pid=$!
endpoint="http://127.0.0.1:$client_port"
ready=false
for _ in {1..30}; do
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
if [[ "$ready" != true ]]; then echo "BLOCKED: isolated etcd failed to start. Evidence: $out"; exit 2; fi
printf 'source_sha256=%s\n' "$actual_sha" >"$out/environment.txt"
go version >>"$out/environment.txt"
etcd --version >>"$out/environment.txt"
echo 'Running Server and Controller race tests against isolated etcd...'
if ! G2_ETCD_ENDPOINT="$endpoint" go test -race -count=1 -v -timeout=180s ./internal/server ./internal/controller >"$out/race.log" 2>&1; then
  echo "FAIL: Server/Controller tests. Evidence: $out"; exit 1
fi
# Service test requires an empty etcd, so it runs on a second fresh data directory.
cleanup || { echo "FAIL: etcd cleanup. Evidence: $out"; exit 1; }
pid=''
etcd --name r1-service --data-dir "$out/service-data" \
  --listen-client-urls "$endpoint" --advertise-client-urls "$endpoint" \
  --listen-peer-urls "http://127.0.0.1:$peer_port" \
  --initial-advertise-peer-urls "http://127.0.0.1:$peer_port" \
  --initial-cluster "r1-service=http://127.0.0.1:$peer_port" >"$out/service-etcd.log" 2>&1 &
pid=$!
ready=false
for _ in {1..30}; do
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  if curl --max-time 1 -fsS "$endpoint/health" >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
if [[ "$ready" != true ]]; then echo "BLOCKED: Service etcd failed to start. Evidence: $out"; exit 2; fi
if ! G5_SERVICE_ETCD_ENDPOINT="$endpoint" go test -race -count=1 -v -timeout=120s ./internal/server -run '^TestServiceAddressClaimsAreAtomicAndRetained$' >"$out/service-race.log" 2>&1; then
  echo "FAIL: Service address transaction test. Evidence: $out"; exit 1
fi
cleanup || { echo "FAIL: etcd cleanup. Evidence: $out"; exit 1; }
pid=''
echo 'PASS: isolated R1 Server/Controller and Service address tests passed.'
echo 'This result does not include G4/G5 live acceptance or the complete R1 gate.'
printf 'Evidence: %s\n' "$out"
