#!/usr/bin/env python3
# Same-version G5-01..G5-07 driver. Run only from scripts/acceptance/g5-live.ps1.
import hashlib
import json
import os
import shutil
import signal
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SRC = "/home/lihaoyang/mini-k8s-g5final"
HOME = "/home/lihaoyang/mini-k8s-g4"
BIN = HOME + "/bin"
LOGS = HOME + "/logs"
EV_PTR = HOME + "/g5cur-evidence.path"
REQ = HOME + "/g5cur-request.json"
ACK = HOME + "/g5cur-ack.json"
FWD_FLAG = HOME + "/g5cur-forwarder-down"
ETCD_FLAG = HOME + "/g5cur-etcd-down"
ETCD_ARGV = HOME + "/g5cur-etcd.json"
ROOT_REQ = HOME + "/g5cur-root-req.json"
ROOT_ACK = HOME + "/g5cur-root-ack.json"
ROOT_STOP = HOME + "/g5cur-root.stop"
EXPECTED = "4933ed318f8dcdb14c36cdc6d37d6696df4cfc59991b5ae3786198008c2e3fb8"
SERVICE_IMAGE = "docker.io/library/g5-service@sha256:5c28668e30e46b6f74a6ce67eed386d1d00df09ed65a6018166b2e03f26a4236"
PROBE_IMAGE = "docker.io/library/g4-probe@sha256:80caa5bfcece4648f735b36a8fbeb394a497bfe9390d8a72575dfc934d7a4a42"
CRICTL = "/usr/local/bin/crictl"
FWD = HOME + "/g4-tcp-forward.py"
FIXTURE = BIN + "/g5-service-fixture.g4cur"
PROXY_BIN = BIN + "/mini-proxy.g4cur"
ENDPOINTS = {
    "node-a": "unix:///run/containerd/containerd.sock",
    "node-b": "unix:///run/mini-k8s/containerd.sock",
}
LEASES = {
    "node-a": "/var/lib/cni/networks/mini-k8s",
    "node-b": "/var/lib/cni/networks/mini-k8s-b",
}
ROLES = ("mini-api", "mini-manager", "mini-scheduler", "mini-agent", "mini-proxy")
HOLD = 300

os.environ["PATH"] = "/home/lihaoyang/.local/go/bin:/home/lihaoyang/.local/bin:" + os.environ.get("PATH", "")
os.environ["GOPROXY"] = "https://goproxy.cn,direct"
os.environ["GOSUMDB"] = "sum.golang.google.cn"
os.environ["GOFLAGS"] = "-mod=readonly"
os.environ["CGO_ENABLED"] = "0"
os.environ["PYTHONIOENCODING"] = "utf-8"


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def say(msg):
    print(msg, flush=True)


def die(msg):
    say("FAIL " + msg)
    raise SystemExit(1)


def emit(obj):
    say("G4RESULT " + json.dumps(obj, ensure_ascii=False))
    if not obj.get("ok"):
        raise SystemExit(1)


def ev():
    with open(EV_PTR, encoding="utf-8") as handle:
        return handle.read().strip()


def write_json(name, obj):
    path = os.path.join(ev(), name)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(obj, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    os.replace(tmp, path)


def read_json(name):
    with open(os.path.join(ev(), name), encoding="utf-8") as handle:
        return json.load(handle)


def atomic_json(path, obj, mode=None):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(obj, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    if mode is not None:
        os.chmod(path, mode)


def payload():
    if len(sys.argv) < 3 or sys.argv[2] == "":
        return {}
    return json.loads(__import__("base64").b64decode(sys.argv[2]))


def procs():
    found = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            raw = open("/proc/%d/cmdline" % pid, "rb").read()
        except OSError:
            continue
        argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
        if not argv:
            continue
        base = os.path.basename(argv[0])
        role = None
        for item in ROLES:
            if base == item or base.startswith(item + "."):
                role = item
                break
        if FWD in argv:
            role = "forwarder"
        if "root-helper" in argv:
            role = "root-helper"
        if role:
            found.append((role, pid, argv))
    return found


def one(role):
    got = [item for item in procs() if item[0] == role]
    if len(got) != 1:
        die("%s 进程数量是 %d，期望 1" % (role, len(got)))
    return got[0]


def source_checksum():
    script = "find cmd internal -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum go.mod go.sum"
    out = subprocess.check_output(["bash", "-c", script], cwd=SRC)
    return hashlib.sha256(out).hexdigest()


def checksum_or_die():
    got = source_checksum()
    if got != EXPECTED:
        die("源码校验是 %s，期望 %s" % (got, EXPECTED))
    return got


def flag_map(argv):
    out = {}
    items = argv[1:]
    index = 0
    while index < len(items):
        if items[index].startswith("-") and index + 1 < len(items):
            out[items[index]] = items[index + 1]
            index += 2
        else:
            index += 1
    return out


def client_paths():
    _role, _pid, argv = one("mini-manager")
    flags = flag_map(argv)
    token_dir = os.path.dirname(flags["-token-file"])
    admin = os.path.join(token_dir, "admin.token")
    if not os.path.isfile(admin) or not os.path.isfile(flags["-ca-file"]):
        die("找不到 admin token 或 CA")
    return flags["-server"], flags["-ca-file"], admin


def assert_certs():
    _role, _pid, argv = one("mini-api")
    server = flag_map(argv).get("-tls-cert") or ""
    _server, ca, _admin = client_paths()
    verified = subprocess.run(["openssl", "verify", "-CAfile", ca, server], capture_output=True, text=True)
    if verified.returncode != 0:
        die("CA 无法验证服务器证书")
    for path, label in ((server, "服务器证书"), (ca, "CA")):
        remaining = subprocess.run(["openssl", "x509", "-in", path, "-noout", "-checkend", "21600"], capture_output=True, text=True)
        if remaining.returncode != 0:
            die(label + " 剩余有效期不足 6 小时")


CLIENT = {}


def api():
    if not CLIENT:
        server, ca, token_path = client_paths()
        with open(token_path, encoding="utf-8") as handle:
            token = handle.read().strip()
        if not token:
            die("admin token 是空的")
        CLIENT.update(server=server.rstrip("/"), ca=ca, token_path=token_path, token=token, ctx=ssl.create_default_context(cafile=ca))
    return CLIENT["server"], CLIENT["ca"], CLIENT["token_path"], CLIENT["token"], CLIENT["ctx"]


def api_get(resource):
    server, _ca, _path, token, ctx = api()
    req = urllib.request.Request(server + "/api/mini/v1/" + resource, headers={"Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=8) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as err:
        raise RuntimeError("API GET %s -> HTTP %s" % (resource, err.code)) from None


def pods():
    return api_get("pods").get("items") or []


def services():
    return api_get("services").get("items") or []


def replicasets():
    return api_get("replicasets").get("items") or []


def nodes():
    return {item["metadata"]["name"]: item for item in api_get("nodes").get("items") or []}


def mini(*args, timeout=30):
    server, ca, token_path, _token, _ctx = api()
    cmd = [BIN + "/mini-k8s.g4cur", "--server", server, "--ca-file", ca, "--token-file", token_path, *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "cli failed")[:500])
    return proc.stdout


def apply_text(text, delete=False):
    path = os.path.join(ev(), "next.yaml")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)
    mini("delete" if delete else "apply", "-f", path)


def pod_yaml(name, image, command, cpu, mem, limit_cpu, limit_mem, labels, readiness=None):
    lines = ["apiVersion: mini.k8s/v1", "kind: Pod", "metadata:", "  name: " + name, "  namespace: default", "  labels:"]
    for key, value in labels.items():
        lines.append("    %s: %s" % (key, value))
    lines += [
        "spec:",
        "  image: " + image,
        "  restartPolicy: Always",
        "  command: [%s]" % ", ".join(json.dumps(part) for part in command),
    ]
    if readiness:
        lines += ["  readiness:", "    port: %d" % readiness]
    lines += [
        "  resources:",
        "    requests:",
        "      cpuMilli: %d" % cpu,
        "      memoryBytes: %d" % mem,
        "    limits:",
        "      cpuMilli: %d" % limit_cpu,
        "      memoryBytes: %d" % limit_mem,
        "",
    ]
    return "\n".join(lines)


def service_yaml(name, selector, port, target):
    lines = [
        "apiVersion: mini.k8s/v1",
        "kind: Service",
        "metadata:",
        "  name: " + name,
        "  namespace: default",
        "spec:",
        "  selector:",
    ]
    for key, value in selector.items():
        lines.append("    %s: %s" % (key, value))
    lines += ["  port: %d" % port, "  targetPort: %d" % target, ""]
    return "\n".join(lines)


def rs_yaml(name, replicas, image, command, cpu, mem, limit_cpu, limit_mem):
    body = pod_yaml("ignored", image, command, cpu, mem, limit_cpu, limit_mem, {"app": name}, 8080)
    spec_body = body.splitlines()[body.splitlines().index("spec:") + 1:]
    indented = "\n".join("    " + line for line in spec_body if line.strip())
    return "\n".join([
        "apiVersion: mini.k8s/v1",
        "kind: ReplicaSet",
        "metadata:",
        "  name: " + name,
        "  namespace: default",
        "spec:",
        "  replicas: %d" % replicas,
        "  selector:",
        "    app: " + name,
        "  template:",
        "    labels:",
        "      app: " + name,
        "    spec:",
        indented,
        "",
    ])


def delete_names(kind, names):
    docs = ["apiVersion: mini.k8s/v1\nkind: %s\nmetadata:\n  name: %s\n  namespace: default\n" % (kind, name) for name in names]
    if docs:
        apply_text("\n---\n".join(docs), delete=True)


def node_of(pod, table):
    uid = pod.get("nodeUID") or ""
    for name, node in table.items():
        if node["metadata"]["uid"] == uid:
            return name
    return ""


def reserved(node):
    res = (node.get("status") or {}).get("reserved") or {}
    return int(res.get("cpuMilli") or 0), int(res.get("memoryBytes") or 0)


def aligned(pod, table):
    status = pod.get("status") or {}
    node = None
    for item in table.values():
        if item["metadata"]["uid"] == pod.get("nodeUID"):
            node = item
    if node is None:
        return False
    node_status = node.get("status") or {}
    return (
        status.get("phase") == "Running"
        and bool(status.get("ready"))
        and int(status.get("readinessEpoch") or 0) == int(node_status.get("readinessEpoch") or 0)
        and bool(node_status.get("ready"))
        and node_status.get("condition") == "Ready"
    )


def record(item, ok, detail):
    row = {"id": item, "ok": bool(ok), "detail": detail, "at": now_iso()}
    path = os.path.join(ev(), "results.json")
    rows = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            rows = json.load(handle)
    rows = [old for old in rows if old["id"] != item] + [row]
    write_json("results.json", rows)
    say("%s %s  %s" % (item, "PASS" if ok else "FAIL", detail))
    if not ok:
        raise SystemExit(1)


def spawn(argv, log_path, root=False):
    os.makedirs(LOGS, exist_ok=True)
    cmd = "setsid " + " ".join(json.dumps(part) for part in argv) + " >> " + json.dumps(log_path) + " 2>&1 < /dev/null &"
    if root and os.geteuid() != 0:
        subprocess.run(["sudo", "-n", "bash", "-c", cmd], check=True)
    else:
        subprocess.run(["bash", "-c", cmd], check=True)


def stop_pid(pid, root=False):
    killer = ["sudo", "-n", "kill"] if root and os.geteuid() != 0 else ["kill"]
    subprocess.run(killer + ["-TERM", str(pid)], check=False)
    for _ in range(50):
        if not os.path.exists("/proc/%d" % pid):
            return
        time.sleep(0.1)
    subprocess.run(killer + ["-KILL", str(pid)], check=False)
    for _ in range(20):
        if not os.path.exists("/proc/%d" % pid):
            return
        time.sleep(0.1)
    die("进程 %d 没有退出" % pid)


def cri(endpoint, args, use_sudo):
    cmd = [CRICTL, "--runtime-endpoint", endpoint, *args]
    if use_sudo and os.geteuid() != 0:
        cmd = ["sudo", "-n", *cmd]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "crictl failed")[-400:])
    if "-o" in args and "json" in args:
        return json.loads(proc.stdout or "null")
    return proc.stdout


def listed(obj):
    if isinstance(obj, list):
        return [item for item in obj if isinstance(item, dict)]
    if isinstance(obj, dict):
        for key in ("containers", "items", "pods"):
            if isinstance(obj.get(key), list):
                return [item for item in obj[key] if isinstance(item, dict)]
    return []


def record_ids(obj):
    return [item["id"] for item in listed(obj) if item.get("id")]


def listening(port):
    needle = "0100007F:%04X" % port
    try:
        with open("/proc/net/tcp", encoding="utf-8") as handle:
            for line in handle:
                parts = line.split()
                if len(parts) > 3 and parts[1].upper() == needle and parts[3] == "0A":
                    return True
    except OSError:
        return False
    return False


def ask_b(action, data=None, timeout=120):
    if os.path.exists(ACK):
        os.remove(ACK)
    rid = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
    atomic_json(REQ, {"id": rid, "action": action, "payload": data or {}})
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(ACK):
            try:
                with open(ACK, encoding="utf-8") as handle:
                    ack = json.load(handle)
            except (OSError, json.JSONDecodeError):
                time.sleep(0.2)
                continue
            if ack.get("id") == rid:
                if os.path.exists(REQ):
                    os.remove(REQ)
                if not ack.get("ok"):
                    die("node-b %s 失败: %s" % (action, str(ack.get("output", ""))[-500:]))
                for line in (ack.get("output") or "").splitlines():
                    if line.startswith("G4RESULT "):
                        return json.loads(line[len("G4RESULT "):])
                return {"ok": True, "output": ack.get("output") or ""}
        time.sleep(0.3)
    die("node-b 没有在 %d 秒内完成 %s" % (timeout, action))


def sandbox_pid(info):
    blob = info.get("info", info) if isinstance(info, dict) else info
    if isinstance(blob, str):
        blob = json.loads(blob)
    if isinstance(blob, dict) and str(blob.get("pid") or "").isdigit():
        return int(blob["pid"])
    raise RuntimeError("sandbox 没有 pid")


def as_root(args, input_text=None):
    cmd = list(args)
    if os.geteuid() != 0:
        cmd = ["sudo", "-n", *cmd]
    return subprocess.run(cmd, input=input_text, capture_output=True, text=True)


def set_hairpin(pid):
    pid = int(pid)
    # 只切换网络命名空间时，sysfs 仍可能属于宿主机；通过 netlink 查询容器端 veth。
    entered = as_root(["nsenter", "-t", str(pid), "-n", "ip", "-j", "-d", "link", "show", "dev", "eth0"])
    if entered.returncode != 0:
        raise RuntimeError("查询容器 eth0 失败: " + (entered.stderr or "")[-200:])
    try:
        links = json.loads(entered.stdout)
        link = links[0] if len(links) == 1 else {}
        if link.get("linkinfo", {}).get("info_kind") != "veth":
            raise ValueError("eth0 不是 veth")
        iflink = str(link.get("link_index") or "")
    except (ValueError, TypeError, IndexError, AttributeError) as err:
        raise RuntimeError("无法识别容器 eth0: " + str(err)) from err
    if not iflink.isdigit() or int(iflink) <= 0:
        raise RuntimeError("容器 eth0 缺少有效的宿主机对端索引")
    linked = as_root(["ip", "-o", "link"])
    if linked.returncode != 0:
        raise RuntimeError("ip link 失败: " + (linked.stderr or "")[-200:])
    iface = ""
    prefix = iflink + ":"
    for line in linked.stdout.splitlines():
        if line.startswith(prefix):
            iface = line.split(":", 2)[1].strip().split("@")[0]
            break
    if not iface.replace(".", "").replace("-", "").replace("_", "").isalnum():
        raise RuntimeError("找不到网桥端口")
    path = "/sys/class/net/%s/brport/hairpin_mode" % iface
    if as_root(["test", "-f", path]).returncode != 0:
        path = "/sys/class/net/%s/hairpin_mode" % iface
    if as_root(["test", "-f", path]).returncode != 0:
        raise RuntimeError("找不到 hairpin_mode")
    before = as_root(["cat", path])
    if before.returncode != 0:
        raise RuntimeError("读 hairpin 失败")
    wrote = as_root(["tee", path], "1\n")
    if wrote.returncode != 0:
        raise RuntimeError("写 hairpin 失败: " + (wrote.stderr or "")[-200:])
    after = as_root(["cat", path])
    if after.returncode != 0 or after.stdout.strip() != "1":
        raise RuntimeError(iface + " hairpin 仍是 " + after.stdout.strip())
    return {"iface": iface, "before": before.stdout.strip(), "after": after.stdout.strip()}


def rule_hits(ips, use_sudo=False):
    chunks = []
    for table in ("nat", "filter", "mangle"):
        cmd = ["/usr/sbin/iptables-save", "-t", table]
        if use_sudo and os.geteuid() != 0:
            cmd = ["sudo", "-n", *cmd]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError("iptables-save %s 失败" % table)
        chunks.append(proc.stdout)
    text = "\n".join(chunks)
    hits = {ip: 0 for ip in ips}
    matched = []
    for line in text.splitlines():
        found = [ip for ip in ips if ip and ip in line]
        if found:
            matched.append(line)
            for ip in found:
                hits[ip] += 1
    return hits, matched


def lease_names(node, use_sudo=False):
    path = LEASES[node]
    if use_sudo and os.geteuid() != 0:
        proc = subprocess.run(["sudo", "-n", "ls", "-A", path], capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or "ls failed")[-300:])
        return sorted(line for line in proc.stdout.splitlines() if line)
    return sorted(name for name in os.listdir(path) if name)


def cri_ids(node, use_sudo=False):
    endpoint = ENDPOINTS[node]
    ps = cri(endpoint, ["ps", "-a", "--label", "mini-k8s.io/managed=true", "-o", "json"], use_sudo)
    sandboxes = cri(endpoint, ["pods", "--label", "mini-k8s.io/managed=true", "-o", "json"], use_sudo)
    return record_ids(ps), record_ids(sandboxes)


def local_snapshot(node, ips, use_sudo=False):
    hits, matched = rule_hits(ips, use_sudo)
    containers, sandboxes = cri_ids(node, use_sudo)
    return {"leases": lease_names(node, use_sudo), "containers": containers, "sandboxes": sandboxes, "rules": hits, "matched": matched}


def container_id(node, uid, use_sudo):
    endpoint = ENDPOINTS[node]
    ps = cri(endpoint, ["ps", "--label", "mini-k8s.io/pod-uid=" + uid, "-o", "json"], use_sudo)
    ids = record_ids(ps)
    if len(ids) != 1:
        raise RuntimeError("%s 容器数量 %d" % (uid, len(ids)))
    return ids[0]


def exec_set(node, uid, state, use_sudo):
    cid = container_id(node, uid, use_sudo)
    endpoint = ENDPOINTS[node]
    out = cri(endpoint, ["exec", cid, "/g5-service-fixture", "set", "-state", state], use_sudo)
    parsed = None
    for line in str(out).splitlines():
        if line.startswith("{"):
            parsed = json.loads(line)
    if not parsed or parsed.get("kind") != state or parsed.get("error"):
        raise RuntimeError("set %s 失败" % state)
    return parsed


def run_check(target, expect, count, no_endpoint=False):
    cmd = [FIXTURE, "check", "-target", target, "-count", str(count), "-timeout", "2500ms"]
    if no_endpoint:
        cmd.append("-expect-no-endpoint")
    else:
        cmd += ["-expect", ",".join(expect)]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    attempts = []
    for line in (proc.stdout or "").splitlines():
        if line.startswith("{"):
            attempts.append(json.loads(line))
    return proc.returncode, attempts, (proc.stderr or "")[-300:]


def summarize(attempts, forbidden=()):
    ok = [item for item in attempts if item.get("outcome") == "success" and not item.get("error")]
    identities = sorted({item.get("identity") for item in ok if item.get("identity")})
    bad = [item for item in ok if item.get("identity") in forbidden]
    return {"n": len(attempts), "success": len(ok), "identities": identities, "forbiddenHits": len(bad)}


def precheck():
    os.umask(0o077)
    if shutil.which("go") is None:
        die("非交互 PATH 里没有 go")
    assert_certs()
    got = checksum_or_die()
    problems = []
    if pods():
        problems.append("已有 Pod")
    if replicasets():
        problems.append("已有 ReplicaSet")
    if services():
        problems.append("已有 Service")
    table = nodes()
    if set(table) != {"node-a", "node-b"}:
        problems.append("节点不是两台")
    for name, node in table.items():
        cpu, mem = reserved(node)
        status = node.get("status") or {}
        if status.get("condition") != "Ready" or not status.get("ready") or cpu or mem:
            problems.append(name + " 不是空闲 Ready")
    if os.path.basename(one("mini-api")[2][0]) != "mini-api.g4cur":
        problems.append("API 不是 g4cur")
    if os.path.basename(one("mini-agent")[2][0]) != "mini-agent.g4cur":
        problems.append("node-a Agent 不是 g4cur")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = HOME + "/evidence/g5cur-" + stamp
    os.makedirs(path, 0o700)
    with open(EV_PTR, "w", encoding="utf-8") as handle:
        handle.write(path + "\n")
    for stale in (REQ, ACK, FWD_FLAG, ETCD_FLAG, ROOT_REQ, ROOT_ACK, ROOT_STOP):
        if os.path.exists(stale):
            os.remove(stale)
    write_json("precheck.json", {"ok": not problems, "checksum": got, "problems": problems, "at": now_iso()})
    if problems:
        die("；".join(problems))
    say("源码校验通过，集群是空的。证据目录 " + path)


def build():
    checksum_or_die()
    os.makedirs(BIN, exist_ok=True)
    hashes = {}
    for name, pkg in (("mini-proxy", "./cmd/mini-proxy"), ("g5-service-fixture", "./cmd/g5-service-fixture"), ("mini-k8s", "./cmd/mini-k8s")):
        dest = BIN + "/" + name + ".g4cur"
        tmp = dest + ".new"
        say("编译 " + name)
        proc = subprocess.run(["go", "build", "-o", tmp, pkg], cwd=SRC, capture_output=True, text=True)
        if proc.returncode != 0:
            die(name + " 编译失败: " + (proc.stderr or "")[-800:])
        os.chmod(tmp, 0o755)
        os.replace(tmp, dest)
        hashes[name] = hashlib.sha256(open(dest, "rb").read()).hexdigest()
    write_json("build.json", {"ok": True, "sha256": hashes, "at": now_iso()})
    say("proxy、fixture 和 CLI 已写入 bin/*.g4cur")


def swap_proxy():
    _role, pid, argv = one("mini-proxy")
    if "https://10.128.176.165:8443" not in argv:
        die("proxy 没有连接 165 的 API")
    if not os.path.isfile(PROXY_BIN):
        die("没有 " + PROXY_BIN)
    new = argv[:]
    new[0] = PROXY_BIN
    stop_pid(pid, root=False)
    spawn(new, LOGS + "/proxy-g5cur.log", root=False)
    deadline = time.time() + 15
    while time.time() < deadline:
        got = [item for item in procs() if item[0] == "mini-proxy" and item[2][0] == PROXY_BIN]
        if len(got) == 1:
            say("proxy 已换成 g4cur，pid %d" % got[0][1])
            return got[0][1]
        time.sleep(0.3)
    die("proxy 没有用新程序启动")


def image_present(node):
    endpoint = ENDPOINTS[node]
    for image in (SERVICE_IMAGE, PROBE_IMAGE):
        proc = subprocess.run([CRICTL, "--runtime-endpoint", endpoint, "inspecti", image], capture_output=True, text=True)
        if proc.returncode != 0:
            die(node + " 缺少镜像 " + image)


def runtime_baseline(node):
    containers, sandboxes = cri_ids(node)
    leases = lease_names(node)
    if leases != ["last_reserved_ip.0", "lock"]:
        die(node + " 租约不是基线: " + " ".join(leases))
    if containers or sandboxes:
        die(node + " 还有 mini-k8s 容器")
    return {"leases": leases, "containers": containers, "sandboxes": sandboxes, "at": now_iso()}


def prepare_a():
    if os.geteuid() != 0:
        die("prepare-a 需要 sudo")
    image_present("node-a")
    pid = swap_proxy()
    report = runtime_baseline("node-a")
    write_json("baseline-a.json", report)
    os.chmod(os.path.join(ev(), "baseline-a.json"), 0o644)
    emit({"ok": True, "proxy": pid})


def prepare_b():
    if os.geteuid() != 0:
        die("prepare-b 需要 sudo")
    image_present("node-b")
    _role, _pid, argv = one("mini-agent")
    if "https://127.0.0.1:18443" not in argv:
        die("node-b Agent 没有走本机转发")
    pid = swap_proxy()
    report = runtime_baseline("node-b")
    with open("/tmp/g5cur-baseline-b.json", "w", encoding="utf-8") as handle:
        json.dump(report, handle)
    os.chmod("/tmp/g5cur-baseline-b.json", 0o644)
    for item in procs():
        if item[0] == "root-helper":
            stop_pid(item[1], root=False)
    if os.path.exists(ROOT_STOP):
        os.remove(ROOT_STOP)
    spawn([sys.executable, os.path.abspath(sys.argv[0]), "root-helper"], LOGS + "/g5cur-root-helper.log", root=False)
    deadline = time.time() + 10
    while time.time() < deadline:
        if any(item[0] == "root-helper" for item in procs()):
            emit({"ok": True, "proxy": pid})
            return
        time.sleep(0.2)
    die("node-b root helper 没有留下")


def root_helper():
    if os.geteuid() != 0:
        die("root-helper 需要 root")
    while not os.path.exists(ROOT_STOP):
        if not os.path.exists(ROOT_REQ):
            time.sleep(0.2)
            continue
        try:
            with open(ROOT_REQ, encoding="utf-8") as handle:
                req = json.load(handle)
        except (OSError, json.JSONDecodeError):
            time.sleep(0.1)
            continue
        action = req.get("action")
        data = req.get("payload") or {}
        try:
            if action == "hairpin":
                endpoint = ENDPOINTS["node-b"]
                sandboxes = cri(endpoint, ["pods", "--label", "mini-k8s.io/pod-uid=" + data["uid"], "-o", "json"], False)
                info = cri(endpoint, ["inspectp", "-o", "json", record_ids(sandboxes)[0]], False)
                result = {"ok": True, "hairpin": set_hairpin(sandbox_pid(info))}
            elif action == "snapshot":
                result = {"ok": True, "snapshot": local_snapshot("node-b", data.get("ips") or [])}
            else:
                result = {"ok": False, "error": "unknown root action"}
        except Exception as err:
            result = {"ok": False, "error": str(err)}
        atomic_json(ROOT_ACK, {"id": req.get("id"), "result": result}, 0o644)
        if os.path.exists(ROOT_REQ):
            os.remove(ROOT_REQ)
        time.sleep(0.1)


def root_call():
    data = payload()
    if os.path.exists(ROOT_ACK):
        os.remove(ROOT_ACK)
    rid = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
    atomic_json(ROOT_REQ, {"id": rid, "action": data.get("action"), "payload": data.get("payload") or {}})
    deadline = time.time() + 40
    while time.time() < deadline:
        if os.path.exists(ROOT_ACK):
            try:
                with open(ROOT_ACK, encoding="utf-8") as handle:
                    ack = json.load(handle)
            except (OSError, json.JSONDecodeError):
                time.sleep(0.2)
                continue
            if ack.get("id") == rid:
                os.remove(ROOT_ACK)
                emit(ack.get("result") or {"ok": False, "error": "empty"})
                return
        time.sleep(0.2)
    die("root helper 没有响应")


def host_check():
    data = payload()
    code, attempts, err = run_check(data["target"], data.get("expect") or [], int(data.get("count") or 20), bool(data.get("noEndpoint")))
    emit({"ok": True, "code": code, "attempts": attempts, "error": err})


def cri_set():
    data = payload()
    parsed = exec_set("node-b", data["uid"], data["state"], False)
    emit({"ok": True, "at": parsed.get("at"), "kind": parsed.get("kind")})


def cri_check():
    data = payload()
    cid = container_id("node-b", data["uid"], False)
    cmd = ["exec", cid, "/g5-service-fixture", "check", "-target", data["target"], "-count", str(data.get("count") or 20), "-timeout", "2500ms"]
    if data.get("noEndpoint"):
        cmd.append("-expect-no-endpoint")
    else:
        cmd += ["-expect", ",".join(data.get("expect") or [])]
    out = cri(ENDPOINTS["node-b"], cmd, False)
    attempts = [json.loads(line) for line in str(out).splitlines() if line.startswith("{")]
    failed = [item for item in attempts if item.get("error")]
    emit({"ok": True, "attempts": attempts, "error": "" if not failed else "check error"})


def proxy_name():
    _role, pid, argv = one("mini-proxy")
    emit({"ok": True, "argv0": os.path.basename(argv[0]), "pid": pid})


def stop_forwarder():
    got = [item for item in procs() if item[0] == "forwarder"]
    if len(got) != 1:
        die("转发进程数量是 %d" % len(got))
    stop_pid(got[0][1], root=False)
    deadline = time.time() + 5
    while time.time() < deadline:
        if not listening(18443):
            emit({"ok": True, "stopped": got[0][1]})
            return
        time.sleep(0.1)
    die("18443 仍在监听")


def start_forwarder():
    if listening(18443) and any(item[0] == "forwarder" for item in procs()):
        emit({"ok": True, "already": True})
        return
    spawn(["python3", FWD], LOGS + "/g5cur-forward.log", root=False)
    deadline = time.time() + 8
    while time.time() < deadline:
        if listening(18443):
            emit({"ok": True, "already": False})
            return
        time.sleep(0.2)
    die("转发没有重新监听")


def cri_empty():
    containers, sandboxes = cri_ids("node-b")
    emit({"ok": True, "empty": not containers and not sandboxes, "containers": containers, "sandboxes": sandboxes})


def snapshot_a(ips):
    return local_snapshot("node-a", ips, True)


def snapshot_b(ips):
    result = ask_b("root-call", {"action": "snapshot", "payload": {"ips": ips}}, timeout=40)
    if not result.get("ok"):
        die("node-b 快照失败: " + str(result.get("error", ""))[:300])
    return result["snapshot"]


def hairpin(node, uid):
    if node == "node-a":
        sandboxes = cri(ENDPOINTS["node-a"], ["pods", "--label", "mini-k8s.io/pod-uid=" + uid, "-o", "json"], True)
        info = cri(ENDPOINTS["node-a"], ["inspectp", "-o", "json", record_ids(sandboxes)[0]], True)
        return set_hairpin(sandbox_pid(info))
    result = ask_b("root-call", {"action": "hairpin", "payload": {"uid": uid}}, timeout=40)
    if not result.get("ok"):
        die("node-b hairpin 失败: " + str(result.get("error", ""))[:300])
    return result["hairpin"]


def wait_aligned(names, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        table = nodes()
        got = [pod for pod in pods() if pod["metadata"]["name"] in names and not pod["metadata"].get("deletionTimestamp")]
        if len(got) == len(names) and all(aligned(pod, table) for pod in got):
            return got, table
        time.sleep(0.4)
    return [], nodes()


def check_sources(target, expect, pod_uids):
    results = {}
    code, attempts, err = run_check(target, expect, 20)
    results["host-a"] = {"code": code, "error": err, "summary": summarize(attempts), "attempts": attempts}
    remote = ask_b("host-check", {"target": target, "expect": expect, "count": 20}, timeout=90)
    results["host-b"] = {"code": remote.get("code"), "summary": summarize(remote.get("attempts") or []), "attempts": remote.get("attempts") or []}
    for label, node, uid in pod_uids:
        if node == "node-a":
            cid = container_id("node-a", uid, True)
            out = cri(ENDPOINTS["node-a"], ["exec", cid, "/g5-service-fixture", "check", "-target", target, "-expect", ",".join(expect), "-count", "20", "-timeout", "2500ms"], True)
            attempts = [json.loads(line) for line in str(out).splitlines() if line.startswith("{")]
            results[label] = {"summary": summarize(attempts), "attempts": attempts}
        else:
            remote = ask_b("cri-check", {"uid": uid, "target": target, "expect": expect, "count": 20}, timeout=90)
            results[label] = {"summary": summarize(remote.get("attempts") or []), "attempts": remote.get("attempts") or []}
    return results


def covered(results, expect):
    seen = set()
    for item in results.values():
        summary = item["summary"]
        if summary["n"] != 20 or summary["success"] != 20:
            return False, seen
        seen.update(summary["identities"])
    return seen == set(expect), seen


def scenario_01():
    say("开始 G5-01")
    serve = ["/g5-service-fixture", "serve", "-listen", ":8080"]
    apply_text(pod_yaml("g5cur-a", SERVICE_IMAGE, serve, 1100, 67108864, 1100, 134217728, {"app": "g5cur"}, 8080))
    apply_text(pod_yaml("g5cur-b", SERVICE_IMAGE, serve, 1100, 67108864, 1100, 134217728, {"app": "g5cur"}, 8080))
    deadline = time.time() + 40
    placed = []
    while time.time() < deadline:
        table = nodes()
        placed = [pod for pod in pods() if pod["metadata"]["name"] in ("g5cur-a", "g5cur-b")]
        if len(placed) == 2 and all((pod.get("status") or {}).get("phase") == "Running" for pod in placed):
            if sorted(node_of(pod, table) for pod in placed) == ["node-a", "node-b"]:
                break
        time.sleep(0.4)
    else:
        record("G5-01", False, "两个 Pod 没有分别在两台机器上 Running")
    table = nodes()
    not_ready = [pod["metadata"]["name"] for pod in placed if (pod.get("status") or {}).get("ready")]
    if not_ready:
        record("G5-01", False, "开启监听前已经 Ready: " + " ".join(not_ready))
    for pod in placed:
        hairpin(node_of(pod, table), pod["metadata"]["uid"])
        if node_of(pod, table) == "node-a":
            exec_set("node-a", pod["metadata"]["uid"], "on", True)
        else:
            ask_b("cri-set", {"uid": pod["metadata"]["uid"], "state": "on"}, timeout=30)
    got, table = wait_aligned(["g5cur-a", "g5cur-b"], 20)
    if len(got) != 2:
        record("G5-01", False, "开启监听后 20 秒内没有两侧 Ready")
    apply_text(service_yaml("g5cur", {"app": "g5cur"}, 8081, 8080))
    deadline = time.time() + 15
    service = None
    while time.time() < deadline:
        service = next((item for item in services() if item["metadata"]["name"] == "g5cur"), None)
        if service and service["spec"].get("clusterIP"):
            break
        time.sleep(0.3)
    if not service or not service["spec"].get("clusterIP"):
        record("G5-01", False, "Service 没有分配 ClusterIP")
    time.sleep(3)
    target = service["spec"]["clusterIP"] + ":8081"
    expect = ["g5cur-a", "g5cur-b"]
    pod_uids = [( "pod-" + node_of(pod, table), node_of(pod, table), pod["metadata"]["uid"]) for pod in got]
    results = check_sources(target, expect, pod_uids)
    ok, seen = covered(results, expect)
    rules_a = snapshot_a([service["spec"]["clusterIP"]])["rules"]
    rules_b = snapshot_b([service["spec"]["clusterIP"]])["rules"]
    rules_ok = rules_a.get(service["spec"]["clusterIP"], 0) > 0 and rules_b.get(service["spec"]["clusterIP"], 0) > 0
    write_json("g501.json", {"service": service["metadata"]["uid"], "clusterIP": service["spec"]["clusterIP"], "results": results, "rulesA": rules_a, "rulesB": rules_b})
    if not ok or not rules_ok:
        record("G5-01", False, "四路覆盖 %s，规则计数 A=%s B=%s" % (sorted(seen), rules_a, rules_b))
    record("G5-01", True, "四路各 20 次成功，身份覆盖两台，ClusterIP 规则已出现")
    return service["spec"]["clusterIP"]


def sample_until(target, expect, seconds, forbidden):
    started = time.time()
    attempts = []
    while time.time() < started + seconds:
        _code, got, _err = run_check(target, expect, 1)
        for item in got:
            item["sampledAt"] = time.time()
        attempts.extend(got)
        time.sleep(0.2)
    late = [item for item in attempts if item["sampledAt"] >= started + seconds - 2]
    return attempts, late


def scenario_02(cluster_ip):
    say("开始 G5-02")
    target = cluster_ip + ":8081"
    table = nodes()
    pod_b = next(pod for pod in pods() if pod["metadata"]["name"] == "g5cur-b")
    off = ask_b("cri-set", {"uid": pod_b["metadata"]["uid"], "state": "off"}, timeout=30)
    off_at = time.time()
    attempts, late = sample_until(target, ["g5cur-a", "g5cur-b"], 12, ("g5cur-b",))
    late_hits = [item for item in late if item.get("identity") == "g5cur-b" and item.get("outcome") == "success"]
    other = [item for item in late if item.get("identity") == "g5cur-a" and item.get("outcome") == "success"]
    if late_hits or not other:
        record("G5-02", False, "停听 10 秒后仍命中 g5cur-b 或没有命中 g5cur-a")
    on = ask_b("cri-set", {"uid": pod_b["metadata"]["uid"], "state": "on"}, timeout=30)
    ready_at = None
    deadline = time.time() + 15
    while time.time() < deadline:
        table = nodes()
        current = next(pod for pod in pods() if pod["metadata"]["uid"] == pod_b["metadata"]["uid"])
        if aligned(current, table):
            ready_at = time.time()
            break
        time.sleep(0.3)
    if ready_at is None:
        record("G5-02", False, "重新监听后没有 Ready")
    hit_at = None
    while time.time() < ready_at + 10:
        _code, got, _err = run_check(target, ["g5cur-a", "g5cur-b"], 1)
        if any(item.get("identity") == "g5cur-b" and item.get("outcome") == "success" for item in got):
            hit_at = time.time()
            break
        time.sleep(0.2)
    rejoin = None if hit_at is None else hit_at - ready_at
    if hit_at is None or rejoin > 10:
        write_json("g502.json", {"off": off, "on": on, "rejoin": rejoin})
        record("G5-02", False, "Ready 后 10 秒内没有再命中 g5cur-b")
    delete_started = time.time()
    delete_names("Pod", ["g5cur-a"])
    delete_elapsed = time.time() - delete_started
    attempts2, late2 = sample_until(target, ["g5cur-a", "g5cur-b"], 12, ("g5cur-a",))
    late_old = [item for item in late2 if item.get("identity") == "g5cur-a" and item.get("outcome") == "success"]
    keep = [item for item in late2 if item.get("identity") == "g5cur-b" and item.get("outcome") == "success"]
    write_json("g502.json", {"off": off, "on": on, "rejoin": rejoin, "deleteElapsed": delete_elapsed, "lateOld": len(late_old), "lateKeep": len(keep)})
    if late_old or not keep:
        record("G5-02", False, "删除 g5cur-a 后 10 秒窗口仍命中旧身份或没有命中 g5cur-b")
    record("G5-02", True, "停听后 10 秒窗口不再命中 g5cur-b；Ready 后 %.1f 秒重新命中；删除 g5cur-a 后不再命中" % rejoin)


def scenario_03(cluster_ip):
    say("开始 G5-03")
    apply_text(service_yaml("g5cur-empty", {"app": "g5cur-none"}, 8081, 8080))
    deadline = time.time() + 15
    service = None
    while time.time() < deadline:
        service = next((item for item in services() if item["metadata"]["name"] == "g5cur-empty"), None)
        if service and service["spec"].get("clusterIP"):
            break
        time.sleep(0.3)
    if not service:
        record("G5-03", False, "空 Service 没有 ClusterIP")
    time.sleep(3)
    target = service["spec"]["clusterIP"] + ":8081"
    pod_b = next(pod for pod in pods() if pod["metadata"]["name"] == "g5cur-b")
    results = {}
    code, attempts, err = run_check(target, [], 20, True)
    results["host-a"] = {"code": code, "error": err, "summary": summarize(attempts), "attempts": attempts}
    remote = ask_b("host-check", {"target": target, "count": 20, "noEndpoint": True}, timeout=90)
    results["host-b"] = {"code": remote.get("code"), "summary": summarize(remote.get("attempts") or []), "attempts": remote.get("attempts") or []}
    remote = ask_b("cri-check", {"uid": pod_b["metadata"]["uid"], "target": target, "count": 20, "noEndpoint": True}, timeout=90)
    results["pod-b"] = {"summary": summarize(remote.get("attempts") or []), "attempts": remote.get("attempts") or []}
    ok = all(item["summary"]["n"] == 20 and item["summary"]["success"] == 0 and not any(row.get("error") and row.get("outcome") != "refused" for row in item["attempts"]) for item in results.values())
    refused = all(all(row.get("outcome") == "refused" and not row.get("error") for row in item["attempts"]) for item in results.values())
    write_json("g503.json", {"clusterIP": service["spec"]["clusterIP"], "results": {key: value["summary"] for key, value in results.items()}, "refused": refused})
    if not refused:
        record("G5-03", False, "无端点没有三路都立即拒绝")
    record("G5-03", True, "宿主机 A、宿主机 B、Pod B 各 20 次立即拒绝")


def restart_role(role, root, node_name=""):
    _found, pid, argv = one(role)
    if node_name and argv[argv.index("-node") + 1] != node_name:
        die(role + " 节点不匹配")
    before = pods()
    stop_pid(pid, root=root)
    spawn(argv, LOGS + "/" + role + "-g5cur-restart.log", root=root)
    deadline = time.time() + 40
    while time.time() < deadline:
        got = [item for item in procs() if item[0] == role and item[2] == argv]
        try:
            table = nodes()
            current = pods()
        except Exception:
            time.sleep(0.5)
            continue
        same = {(pod["metadata"]["uid"], pod["metadata"]["name"]) for pod in current} == {(pod["metadata"]["uid"], pod["metadata"]["name"]) for pod in before}
        ready = all((node.get("status") or {}).get("condition") == "Ready" for node in table.values())
        if len(got) == 1 and same and ready:
            return got[0][1]
        time.sleep(0.5)
    die(role + " 重启后没有恢复")


def scenario_04(cluster_ip):
    say("开始 G5-04")
    before = {(pod["metadata"]["uid"], (pod.get("status") or {}).get("restartCount") or 0) for pod in pods()}
    ask_b("nodeb-restart", timeout=40)
    restart_role("mini-agent", True, "node-a")
    restart_role("mini-manager", False)
    restart_role("mini-api", False)
    time.sleep(3)
    after = {(pod["metadata"]["uid"], (pod.get("status") or {}).get("restartCount") or 0) for pod in pods()}
    if before != after:
        record("G5-04", False, "重启后 Pod UID 或重启次数变了")
    etcd = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            raw = open("/proc/%s/cmdline" % name, "rb").read()
        except OSError:
            continue
        text = raw.replace(b"\0", b" ").decode("utf-8", "replace")
        if "etcd" in text and HOME + "/etcd" in text:
            etcd.append((int(name), [part.decode() for part in raw.split(b"\0") if part]))
    if len(etcd) != 1:
        die("etcd 进程数量是 %d" % len(etcd))
    atomic_json(ETCD_ARGV, {"argv": etcd[0][1]})
    with open(ETCD_FLAG, "w", encoding="utf-8") as handle:
        handle.write(now_iso() + "\n")
    stop_pid(etcd[0][0], root=False)
    pause_started = time.time()
    body = json.dumps({"apiVersion": "mini.k8s/v1", "kind": "Service", "metadata": {"name": "g5cur-503", "namespace": "default"}, "spec": {"selector": {"app": "g5cur-none"}, "port": 8081, "targetPort": 8080}}).encode()
    server, _ca, _path, token, ctx = api()
    req = urllib.request.Request(server + "/api/mini/v1/services", data=body, method="POST", headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    started = time.time()
    status = 0
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=8) as resp:
            status = resp.status
    except urllib.error.HTTPError as err:
        status = err.code
    elapsed = time.time() - started
    code, attempts, _err = run_check(cluster_ip + ":8081", ["g5cur-b"], 20)
    while time.time() < pause_started + 10:
        time.sleep(0.2)
    down_for = time.time() - pause_started
    # 从启动恢复开始计时：etcd 健康不代表 API 与控制循环已经恢复。
    recovery_started = time.monotonic()
    recover_etcd()
    recovery_error = ""
    recovered = False
    while time.monotonic() - recovery_started < 30:
        try:
            current = {(pod["metadata"]["uid"], (pod.get("status") or {}).get("restartCount") or 0) for pod in pods()}
            table = nodes()
            if current == before and set(table) == {"node-a", "node-b"} and all(
                (node.get("status") or {}).get("condition") == "Ready" for node in table.values()
            ):
                recovered = True
                break
            recovery_error = "Pod 身份、重启次数或节点状态尚未恢复"
        except Exception as err:
            recovery_error = str(err)
        time.sleep(0.5)
    recovery_seconds = time.monotonic() - recovery_started
    recovered = recovered and recovery_seconds <= 30
    business_ok = code == 0 and summarize(attempts)["success"] == 20
    write_json("g504.json", {"status": status, "elapsed": elapsed, "downFor": down_for, "business": summarize(attempts), "recovered": recovered, "recoverySeconds": recovery_seconds, "recoveryError": recovery_error})
    if status != 503 or elapsed >= 5 or not business_ok or down_for < 10 or not recovered:
        record("G5-04", False, "503=%s 用时 %.3f 秒，业务成功 %s，暂停 %.1f 秒，恢复=%s 用时 %.1f 秒：%s" % (status, elapsed, business_ok, down_for, recovered, recovery_seconds, recovery_error))
    record("G5-04", True, "组件重启后实例没增加；etcd 暂停 %.1f 秒，写请求 %.3f 秒返回 503，业务 20 次仍成功，%.1f 秒内恢复" % (down_for, elapsed, recovery_seconds))


def recover_etcd():
    if not os.path.exists(ETCD_FLAG):
        return
    with open(ETCD_ARGV, encoding="utf-8") as handle:
        argv = json.load(handle)["argv"]
    running = False
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            text = open("/proc/%s/cmdline" % name, "rb").read().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        if "etcd" in text and HOME + "/etcd" in text:
            running = True
    if not running:
        spawn(argv, LOGS + "/etcd-g5cur-restart.log", root=False)
    deadline = time.time() + 25
    while time.time() < deadline:
        proc = subprocess.run(["etcdctl", "--endpoints=http://127.0.0.1:2379", "endpoint", "health"], capture_output=True, text=True)
        if proc.returncode == 0:
            if os.path.exists(ETCD_FLAG):
                os.remove(ETCD_FLAG)
            say("etcd 已恢复")
            return
        time.sleep(0.4)
    die("etcd 没有恢复")


def nodeb_restart():
    if os.geteuid() == 0:
        die("node-b Agent 不能用 root 重启")
    _role, pid, argv = one("mini-agent")
    if "https://127.0.0.1:18443" not in argv or argv[argv.index("-node") + 1] != "node-b":
        die("node-b Agent 参数不对")
    if not any(item[0] == "forwarder" for item in procs()):
        die("转发不在，拒绝重启 Agent")
    stop_pid(pid, root=False)
    spawn(argv, LOGS + "/agent-b-g5cur-restart.log", root=False)
    deadline = time.time() + 15
    while time.time() < deadline:
        got = [item for item in procs() if item[0] == "mini-agent" and item[2] == argv]
        if len(got) == 1 and any(item[0] == "forwarder" for item in procs()):
            emit({"ok": True, "pid": got[0][1]})
            return
        time.sleep(0.3)
    die("node-b Agent 没有恢复")


def scenario_05(cluster_ip):
    say("开始 G5-05")
    pod_b = next(pod for pod in pods() if pod["metadata"]["name"] == "g5cur-b")
    target = cluster_ip + ":8081"
    samples = []
    stop = {"value": False}

    def loop():
        while not stop["value"]:
            try:
                table = nodes()
                current = pods()
                _code, got, _err = run_check(target, ["g5cur-b"], 1)
            except Exception as err:
                samples.append({"at": time.time(), "error": str(err)})
                time.sleep(0.2)
                continue
            samples.append({"at": time.time(), "nodes": table, "pods": current, "attempts": got})
            time.sleep(0.2)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    while not any("nodes" in sample for sample in samples):
        time.sleep(0.1)
    with open(FWD_FLAG, "w", encoding="utf-8") as handle:
        handle.write(now_iso() + "\n")
    ask_b("stop-forwarder", timeout=20)
    unknown_at = None
    heartbeat = None
    deadline = time.time() + 25
    while time.time() < deadline and not unknown_at:
        for sample in samples:
            node = (sample.get("nodes") or {}).get("node-b")
            if node and (node.get("status") or {}).get("condition") == "Unknown":
                unknown_at = sample["at"]
                heartbeat = (node.get("status") or {}).get("lastHeartbeat")
                break
        time.sleep(0.1)
    if not unknown_at:
        stop["value"] = True
        record("G5-05", False, "切断后没有看到 Unknown")
    time.sleep(max(0, unknown_at + 6 - time.time()))
    late = [item for sample in samples if sample["at"] >= unknown_at + 5 for item in sample.get("attempts") or []]
    late_hits = [item for item in late if item.get("identity") == "g5cur-b" and item.get("outcome") == "success"]
    ask_b("cri-set", {"uid": pod_b["metadata"]["uid"], "state": "off"}, timeout=30)
    ask_b("start-forwarder", timeout=20)
    if os.path.exists(FWD_FLAG):
        os.remove(FWD_FLAG)
    deadline = time.time() + 30
    while time.time() < deadline:
        table = nodes()
        if all((node.get("status") or {}).get("condition") == "Ready" for node in table.values()):
            break
        time.sleep(0.4)
    else:
        stop["value"] = True
        record("G5-05", False, "转发恢复后 30 秒内没有都 Ready")
    restored = next(pod for pod in pods() if pod["metadata"]["uid"] == pod_b["metadata"]["uid"])
    stale_ready = aligned(restored, nodes())
    quiet_hits = []
    until = time.time() + 6
    while time.time() < until:
        _code, got, _err = run_check(target, ["g5cur-b"], 1)
        quiet_hits.extend(item for item in got if item.get("outcome") == "success")
        time.sleep(0.3)
    on = ask_b("cri-set", {"uid": pod_b["metadata"]["uid"], "state": "on"}, timeout=30)
    ready_at = None
    deadline = time.time() + 15
    while time.time() < deadline:
        if aligned(next(pod for pod in pods() if pod["metadata"]["uid"] == pod_b["metadata"]["uid"]), nodes()):
            ready_at = time.time()
            break
        time.sleep(0.3)
    hit_at = None
    if ready_at:
        while time.time() < ready_at + 10:
            _code, got, _err = run_check(target, ["g5cur-b"], 1)
            if any(item.get("outcome") == "success" and item.get("identity") == "g5cur-b" for item in got):
                hit_at = time.time()
                break
            time.sleep(0.2)
    stop["value"] = True
    thread.join(timeout=3)
    write_json("g505.json", {"unknownAt": unknown_at, "heartbeat": heartbeat, "lateHits": len(late_hits), "staleReady": stale_ready, "quietHits": len(quiet_hits), "rejoin": None if not hit_at else hit_at - ready_at})
    if late_hits or stale_ready or quiet_hits or hit_at is None or hit_at - ready_at > 10:
        record("G5-05", False, "失联摘除或恢复后的旧 Ready 不符合")
    record("G5-05", True, "Unknown 后 5 秒不再命中 g5cur-b；恢复后未监听时没有加入；Ready 后 %.1f 秒重新命中" % (hit_at - ready_at))


def scenario_06():
    say("开始 G5-06")
    delete_names("Service", ["g5cur", "g5cur-empty"])
    delete_names("Pod", ["g5cur-b"])
    deadline = time.time() + 40
    while time.time() < deadline:
        table = nodes()
        if not pods() and not services() and all(reserved(node) == (0, 0) and (node.get("status") or {}).get("condition") == "Ready" for node in table.values()):
            break
        time.sleep(0.5)
    else:
        record("G5-06", False, "清理上一组 Service Pod 后没有回到空闲")
    apply_text(rs_yaml("g5cur-demo", 20, PROBE_IMAGE, ["/g4-probe", "serve", "-listen", ":8080"], 50, 67108864, 100, 134217728))
    apply_text(service_yaml("g5cur-demo", {"app": "g5cur-demo"}, 8080, 8080))
    started = time.time()
    ready = []
    while time.time() < started + 30:
        table = nodes()
        rs = next((item for item in replicasets() if item["metadata"]["name"] == "g5cur-demo"), None)
        if rs:
            owned = [pod for pod in pods() if pod["metadata"].get("ownerUID") == rs["metadata"]["uid"] and not pod["metadata"].get("deletionTimestamp")]
            ready = [pod for pod in owned if aligned(pod, table)]
            if len(owned) == 20 and len(ready) == 20:
                break
        time.sleep(0.4)
    else:
        record("G5-06", False, "30 秒内没有 20 个 Ready，当前 %d" % len(ready))
    ready_elapsed = time.time() - started
    service = next(item for item in services() if item["metadata"]["name"] == "g5cur-demo")
    names = sorted(pod["metadata"]["name"] for pod in ready)
    code, attempts, err = run_check(service["spec"]["clusterIP"] + ":8080", names, 20)
    first = summarize(attempts)
    counts = {(pod["metadata"]["uid"], (pod.get("status") or {}).get("restartCount") or 0) for pod in ready}
    hold_started = time.time()
    while time.time() < hold_started + HOLD:
        time.sleep(10)
        current = [pod for pod in pods() if pod["metadata"]["name"] in names]
        now_counts = {(pod["metadata"]["uid"], (pod.get("status") or {}).get("restartCount") or 0) for pod in current}
        if now_counts != counts or len(current) != 20:
            record("G5-06", False, "5 分钟内重启次数或 Pod 数量变了")
        if int(time.time() - hold_started) % 60 < 10:
            say("G5-06 已稳定 %d 秒" % int(time.time() - hold_started))
    code2, attempts2, err2 = run_check(service["spec"]["clusterIP"] + ":8080", names, 20)
    second = summarize(attempts2)
    marks = []
    rs_name_uid = next(item["metadata"]["uid"] for item in replicasets() if item["metadata"]["name"] == "g5cur-demo")
    for replicas in (4, 0):
        scale_started = time.time()
        for _ in range(3):
            try:
                mini("scale", "rs", "g5cur-demo", "--replicas", str(replicas))
                break
            except RuntimeError:
                time.sleep(0.4)
        else:
            record("G5-06", False, "scale %d 失败" % replicas)
        deadline = scale_started + 30
        while time.time() < deadline:
            owned = [pod for pod in pods() if pod["metadata"].get("ownerUID") == rs_name_uid and not pod["metadata"].get("deletionTimestamp") and (pod.get("status") or {}).get("phase") == "Running"]
            if len(owned) == replicas:
                marks.append({"replicas": replicas, "elapsed": time.time() - scale_started})
                break
            time.sleep(0.4)
        else:
            record("G5-06", False, "scale %d 没有在 30 秒内收敛" % replicas)
    write_json("g506.json", {"readyElapsed": ready_elapsed, "first": first, "second": second, "marks": marks, "clusterIP": service["spec"]["clusterIP"], "firstError": err, "secondError": err2})
    if code != 0 or code2 != 0 or first["success"] != 20 or second["success"] != 20:
        record("G5-06", False, "20 次连接没有全部成功")
    record("G5-06", True, "20 个 Pod %.1f 秒 Ready，稳定 5 分钟，缩到 4 用时 %.1f 秒，缩到 0 用时 %.1f 秒" % (ready_elapsed, marks[0]["elapsed"], marks[1]["elapsed"]))
    return service


def scenario_07(service):
    say("开始 G5-07")
    ip = service["spec"]["clusterIP"]
    uid = service["metadata"]["uid"]
    started = time.time()
    delete_names("Service", ["g5cur-demo"])
    delete_names("ReplicaSet", ["g5cur-demo"])
    deadline = started + 30
    last = {}
    while time.time() < deadline:
        table = nodes()
        snap_a = snapshot_a([ip])
        snap_b = snapshot_b([ip])
        pool = api_get("service-pool")
        claims = [item for item in pool.get("claims") or [] if item.get("ip") == ip]
        last = {"a": snap_a, "b": snap_b, "claims": claims, "pods": len(pods()), "services": len(services()), "rs": len(replicasets()), "reserved": {name: reserved(node) for name, node in table.items()}}
        clean = (
            last["pods"] == 0 and last["services"] == 0 and last["rs"] == 0
            and all(value == (0, 0) for value in last["reserved"].values())
            and snap_a["leases"] == ["last_reserved_ip.0", "lock"] and snap_b["leases"] == ["last_reserved_ip.0", "lock"]
            and not snap_a["containers"] and not snap_a["sandboxes"] and not snap_b["containers"] and not snap_b["sandboxes"]
            and snap_a["rules"].get(ip, 0) == 0 and snap_b["rules"].get(ip, 0) == 0
            and len(claims) == 1 and claims[0].get("state") == "retained" and claims[0].get("uid") == uid
        )
        if clean:
            elapsed = time.time() - started
            write_json("g507.json", {"elapsed": elapsed, "clusterIP": ip, "claim": claims[0], "available": pool.get("available"), "capacity": pool.get("capacity")})
            record("G5-07", True, "%.1f 秒内 Pod、RS、Service、预留、两侧租约和转发规则回到基线；%s 仍是 retained，没有释放" % (elapsed, ip))
            return
        time.sleep(0.5)
    write_json("g507.json", {"elapsed": time.time() - started, "last": last, "clusterIP": ip})
    record("G5-07", False, "30 秒内没有回到基线")


def recover_forwarder():
    if not os.path.exists(FWD_FLAG):
        return
    try:
        ask_b("start-forwarder", timeout=20)
        os.remove(FWD_FLAG)
        say("已重新拉起 node-b 转发")
    except SystemExit:
        say("node-b 转发没有自动恢复")


def sudo_refresh():
    while True:
        time.sleep(60)
        subprocess.run(["sudo", "-n", "-v"], check=False)


def driver():
    assert_certs()
    checksum_or_die()
    say("请输入 10.128.176.165 的 sudo 密码。输入时不会显示。")
    if subprocess.run(["sudo", "-v"]).returncode != 0:
        die("sudo 没有通过")
    refresher = threading.Thread(target=sudo_refresh, daemon=True)
    refresher.start()
    if os.path.basename(one("mini-proxy")[2][0]) != "mini-proxy.g4cur":
        die("node-a proxy 还不是 g4cur")
    remote = ask_b("proxy-name", timeout=20)
    if remote.get("argv0") != "mini-proxy.g4cur":
        die("node-b proxy 还不是 g4cur")
    if pods():
        die("开始前发现 Pod")
    try:
        cluster_ip = scenario_01()
        scenario_02(cluster_ip)
        scenario_03(cluster_ip)
        scenario_04(cluster_ip)
        scenario_05(cluster_ip)
        service = scenario_06()
        scenario_07(service)
        say("165 上的 G5 场景已跑完。")
    except SystemExit:
        recover_etcd()
        recover_forwarder()
        raise
    except Exception as err:
        recover_etcd()
        recover_forwarder()
        die(str(err))
    finally:
        recover_etcd()


def report():
    path = os.path.join(ev(), "results.json")
    if not os.path.exists(path):
        say("还没有场景结果")
        say("证据目录 " + ev())
        return
    with open(path, encoding="utf-8") as handle:
        rows = json.load(handle)
    say("")
    say("G5 结果")
    for row in rows:
        say("%s  %s  %s" % (row["id"], "PASS" if row["ok"] else "FAIL", row["detail"]))
    say("证据目录 " + ev())
    say("这只是这一轮的结果，脚本不会把 G5 记成通过。")


def main():
    actions = {
        "precheck": precheck,
        "build": build,
        "prepare-a": prepare_a,
        "prepare-b": prepare_b,
        "root-helper": root_helper,
        "root-call": root_call,
        "host-check": host_check,
        "cri-set": cri_set,
        "cri-check": cri_check,
        "proxy-name": proxy_name,
        "stop-forwarder": stop_forwarder,
        "start-forwarder": start_forwarder,
        "cri-empty": cri_empty,
        "nodeb-restart": nodeb_restart,
        "driver": driver,
        "report": report,
        "recover-etcd": recover_etcd,
    }
    if len(sys.argv) < 2 or sys.argv[1] not in actions:
        die("unknown command")
    actions[sys.argv[1]]()


if __name__ == "__main__":
    main()
