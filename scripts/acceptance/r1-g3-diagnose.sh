#!/usr/bin/env bash
set -uo pipefail

# 只读取本次 G3 失败证据；不停止进程、不删除容器或租约。
evidence="${1:-/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v6-jT6u1GHS/artifacts/acceptance/20261105T152745Z-g3-679129}"
if [[ $EUID -ne 0 ]]; then
  printf '%s\n' '需要 sudo 读取本轮 Agent 日志、租约和 containerd 日志；不会修改资源。'
  exec sudo bash "$0" "$evidence"
fi
if [[ ! -r "$evidence/runtime-running.json" || ! -r "$evidence/leases.json" || ! -r "$evidence/agent.log" ]]; then
  printf 'BLOCKED: required evidence is unavailable in %s\n' "$evidence" >&2
  exit 2
fi

container_id=$(python3 - "$evidence/runtime-running.json" <<'PY'
import json, re, sys
with open(sys.argv[1], encoding="utf-8") as source:
    data = json.load(source)
value = data.get("container", {}).get("id", "")
if not re.fullmatch(r"[a-f0-9]{12,64}", value):
    raise SystemExit("invalid captured container ID")
print(value)
PY
) || exit 1

printf '\n%s\n' '=== 本轮 Agent 日志 ==='
cat "$evidence/agent.log" || exit 1
printf '\n%s\n' '=== 指定容器在 containerd 中的记录（UTC） ==='
if ! command -v journalctl >/dev/null 2>&1; then
  printf '%s\n' 'BLOCKED: journalctl is unavailable.'
else
  journal=$(mktemp) || exit 1
  trap 'rm -f -- "$journal"' EXIT
  if journalctl -u containerd --since '2026-11-05 15:29:50 UTC' --until '2026-11-05 15:30:20 UTC' --no-pager -o short-precise >"$journal" 2>&1; then
    if ! grep -F -- "$container_id" "$journal"; then
      printf '%s\n' '没有找到带完整容器 ID 的记录；不能据此判断 containerd 没有处理请求。'
    fi
  else
    printf '%s\n' 'BLOCKED: containerd journal could not be read.'
    cat "$journal"
  fi
fi
printf '\n%s\n' '=== 本轮租约文件核对 ==='
if ! python3 - "$evidence/leases.json" <<'PY'
import ipaddress, json, os, sys
from pathlib import Path

with open(sys.argv[1], encoding="utf-8") as source:
    leases = json.load(source)
if not isinstance(leases, dict) or not leases:
    raise SystemExit("BLOCKED: no recorded lease")
network = Path("/var/lib/cni/networks/mini-k8s").resolve(strict=True)
incomplete = False
for lease in leases.values():
    parsed = ipaddress.ip_address(lease["IP"])
    if parsed.version != 4:
        raise SystemExit("BLOCKED: unexpected lease address")
    address = str(parsed)
    path = Path(lease["Path"])
    if path.name != address or path.parent.resolve(strict=True) != network or path.is_symlink():
        raise SystemExit("BLOCKED: unexpected lease path")
    try:
        fields = path.read_text(encoding="utf-8").split()
    except FileNotFoundError:
        print(f"{address}: lease file absent")
        continue
    except OSError as exc:
        print(f"{address}: unreadable ({type(exc).__name__})")
        incomplete = True
        continue
    print(f"{address}: exists; belongs_to_test_sandbox={bool(fields and fields[0] == lease['SandboxID'])}")
if incomplete:
    raise SystemExit(1)
PY
then
  printf '%s\n' 'BLOCKED: lease checks are incomplete.' >&2
  exit 2
fi
printf '\n%s\n' '完成。请回传以上输出；若包含你不希望分享的信息，可先脱敏。'
