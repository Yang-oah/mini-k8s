#!/usr/bin/env bash
set -uo pipefail

# R1 当前快照的 G3 交互式入口；实际断言与清理仍由 g3.sh 执行。
# 可通过环境变量覆盖测试机相关参数，但源码必须匹配已验证的 R1 快照。
source_dir="${R1_SOURCE_DIR:-/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v8-jzrGGUC4}"
expected_sha="d9744f89f899b8bd3debfaa9cd3113b213103d4b7d415a4ba183d44f159f9d27"
image="${G3_TEST_IMAGE:-docker.io/library/busybox@sha256:5b3a16922a16714dac5c393ed0807e516e9f2b7b3d0602baef6b8e38a46aaf88}"
pod_cidr="${G3_POD_CIDR:-10.88.0.0/24}"
cni_network="${G3_CNI_NETWORK:-mini-k8s}"
containerd_config="${G3_CONTAINERD_CONFIG:-/etc/containerd/config.toml}"
cri="${G3_CRI_ENDPOINT:-unix:///run/containerd/containerd.sock}"
test_path="${R1_TEST_PATH:-/home/lihaoyang/.local/go/bin:/home/lihaoyang/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin}"

if [[ ! -d "$source_dir" || ! -f "$source_dir/scripts/acceptance/g3.sh" ]]; then
  printf 'BLOCKED: source directory or G3 script is missing: %s\n' "$source_dir" >&2
  exit 2
fi
cd "$source_dir" || exit 2
if ! command -v sha256sum >/dev/null 2>&1 || ! command -v sudo >/dev/null 2>&1; then
  printf 'BLOCKED: sha256sum and sudo are required.\n' >&2
  exit 2
fi
actual_sha="$( { find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum; } | sha256sum | awk '{print $1}')"
if [[ "$actual_sha" != "$expected_sha" ]]; then
  printf 'BLOCKED: source checksum differs from verified R1 snapshot.\nExpected: %s\nActual:   %s\n' "$expected_sha" "$actual_sha" >&2
  exit 2
fi

printf '%s\n' 'Source checksum verified.'
printf '%s\n' 'G3 uses real containerd/CNI, creates test-only resources, and runs G1/G2 first.'
printf '%s\n' 'sudo may prompt for your password; do not send it to anyone.'
printf 'Source: %s\nImage: %s\nCNI network: %s (%s)\n' "$source_dir" "$image" "$cni_network" "$pod_cidr"

sudo env \
  "PATH=$test_path" \
  "G3_TEST_IMAGE=$image" \
  "G3_POD_CIDR=$pod_cidr" \
  "G3_CNI_NETWORK=$cni_network" \
  "G3_CONTAINERD_CONFIG=$containerd_config" \
  "G3_CRI_ENDPOINT=$cri" \
  bash scripts/acceptance/g3.sh
result=$?
printf 'G3 script exit code: %s (0=PASS, 1=FAIL, 2=BLOCKED).\n' "$result"
printf '%s\n' 'Send the PASS/FAIL/BLOCKED output and Evidence path for review.'
exit "$result"
