#!/usr/bin/env python3
# Same-version G4-01..G4-06 driver. Run only from scripts/acceptance/g4-live.ps1.
import base64
import hashlib
import json
import os
import shutil
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
EV_PTR = HOME + "/g4cur-evidence.path"
REQ = HOME + "/g4cur-request.json"
ACK = HOME + "/g4cur-ack.json"
FWD_FLAG = HOME + "/g4cur-forwarder-down"
EXPECTED = "4933ed318f8dcdb14c36cdc6d37d6696df4cfc59991b5ae3786198008c2e3fb8"
BUSYBOX = "docker.io/library/busybox@sha256:5b3a16922a16714dac5c393ed0807e516e9f2b7b3d0602baef6b8e38a46aaf88"
PROBE_IMAGE = "docker.io/library/g4-probe@sha256:80caa5bfcece4648f735b36a8fbeb394a497bfe9390d8a72575dfc934d7a4a42"
CRICTL = "/usr/local/bin/crictl"
FWD = HOME + "/g4-tcp-forward.py"
ENDPOINTS = {
    "node-a": "unix:///run/containerd/containerd.sock",
    "node-b": "unix:///run/mini-k8s/containerd.sock",
}
LEASES = {
    "node-a": "/var/lib/cni/networks/mini-k8s",
    "node-b": "/var/lib/cni/networks/mini-k8s-b",
}
ROLES = ("mini-api", "mini-manager", "mini-scheduler", "mini-agent", "mini-proxy")

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
    with open(EV_PTR, encoding="utf-8") as f:
        return f.read().strip()


def write_json(name, obj):
    path = os.path.join(ev(), name)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)


def read_json(name):
    with open(os.path.join(ev(), name), encoding="utf-8") as f:
        return json.load(f)


def atomic_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def payload():
    if len(sys.argv) < 3 or sys.argv[2] == "":
        return {}
    return json.loads(base64.b64decode(sys.argv[2]))


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
        argv = [a.decode("utf-8", "replace") for a in raw.split(b"\0") if a]
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


def flag_map(argv):
    out = {}
    items = argv[1:]
    i = 0
    while i < len(items):
        if items[i].startswith("-") and i + 1 < len(items):
            out[items[i]] = items[i + 1]
            i += 2
        else:
            i += 1
    return out


def client_paths():
    _role, _pid, argv = one("mini-manager")
    flags = flag_map(argv)
    token_dir = os.path.dirname(flags["-token-file"])
    admin = os.path.join(token_dir, "admin.token")
    if not os.path.isfile(admin) or not os.path.isfile(flags["-ca-file"]):
        names = sorted(x for x in os.listdir(token_dir) if x.endswith(".token"))
        die("找不到 admin token 或 CA。该目录的 token 文件名: " + " ".join(names))
    return flags["-server"], flags["-ca-file"], admin


def load_token(path):
    with open(path, encoding="utf-8") as f:
        token = f.read().strip()
    if not token:
        die("admin token 是空的")
    return token


CLIENT = {}


def api():
    if not CLIENT:
        server, ca, token_path = client_paths()
        CLIENT["server"] = server.rstrip("/")
        CLIENT["ca"] = ca
        CLIENT["token_path"] = token_path
        CLIENT["token"] = load_token(token_path)
        CLIENT["ctx"] = ssl.create_default_context(cafile=ca)
    return CLIENT["server"], CLIENT["ca"], CLIENT["token_path"], CLIENT["token"], CLIENT["ctx"]


def api_get(resource):
    server, _ca, _path, token, ctx = api()
    req = urllib.request.Request(
        server + "/api/mini/v1/" + resource,
        headers={"Authorization": "Bearer " + token},
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=4) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as err:
        raise RuntimeError("API GET %s -> HTTP %s" % (resource, err.code)) from None


def pods():
    data = api_get("pods")
    return data.get("items") or []


def nodes():
    data = api_get("nodes")
    return {item["metadata"]["name"]: item for item in data.get("items") or []}


def mini(*args, timeout=20):
    server, ca, token_path, _token, _ctx = api()
    cmd = [BIN + "/mini-k8s.g4cur", "--server", server, "--ca-file", ca, "--token-file", token_path, *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        text = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(text[:500])
    return proc.stdout


def apply_text(text, delete=False):
    path = os.path.join(ev(), "next.yaml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    mini("delete" if delete else "apply", "-f", path)


def pod_yaml(name, image, command, cpu, mem, limit_cpu=None, limit_mem=None, labels=None):
    limit_cpu = cpu if limit_cpu is None else limit_cpu
    limit_mem = mem if limit_mem is None else limit_mem
    lines = ["apiVersion: mini.k8s/v1", "kind: Pod", "metadata:", "  name: " + name]
    if labels:
        lines.append("  labels:")
        for key, value in labels.items():
            lines.append("    %s: %s" % (key, value))
    lines += [
        "spec:",
        "  image: " + image,
        "  restartPolicy: Always",
        "  command: [%s]" % ", ".join(json.dumps(part) for part in command),
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


def rs_yaml(name, replicas, image, command, cpu, mem, limit_cpu=None, limit_mem=None):
    body = pod_yaml("ignored", image, command, cpu, mem, limit_cpu, limit_mem)
    lines = body.splitlines()
    spec_body = lines[lines.index("spec:") + 1:]
    # pod spec 自身缩进 2 格；放进 template.spec 后再加 4 格。
    indented = "\n".join("    " + line for line in spec_body if line.strip())
    return "\n".join([
        "apiVersion: mini.k8s/v1",
        "kind: ReplicaSet",
        "metadata:",
        "  name: " + name,
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
    docs = []
    for name in names:
        docs.append("apiVersion: mini.k8s/v1\nkind: %s\nmetadata:\n  name: %s\n" % (kind, name))
    if docs:
        apply_text("\n---\n".join(docs), delete=True)


def owned(rs_uid):
    return [pod for pod in pods() if pod["metadata"].get("ownerUID") == rs_uid and not pod["metadata"].get("deletionTimestamp")]


def running_of(rs_uid):
    return [pod for pod in owned(rs_uid) if pod.get("status", {}).get("phase") == "Running"]


def node_of(pod, table):
    uid = pod.get("nodeUID") or ""
    for name, node in table.items():
        if node["metadata"]["uid"] == uid:
            return name
    return ""


def reserved(node):
    res = node.get("status", {}).get("reserved") or {}
    return int(res.get("cpuMilli") or 0), int(res.get("memoryBytes") or 0)


def quiet(timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        table = nodes()
        if pods():
            time.sleep(1)
            continue
        good = True
        for node in table.values():
            cpu, mem = reserved(node)
            ready = bool(node.get("status", {}).get("ready"))
            if cpu != 0 or mem != 0 or not ready or node.get("status", {}).get("condition") != "Ready":
                good = False
        if good and set(table) == {"node-a", "node-b"}:
            return table
        time.sleep(1)
    die("集群没有回到两台 Ready 且预留为 0。现场 Pod: " + " ".join(pod["metadata"]["name"] for pod in pods()))


def record(item, ok, detail):
    row = {"id": item, "ok": bool(ok), "detail": detail, "at": now_iso()}
    path = os.path.join(ev(), "results.json")
    rows = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
    rows = [old for old in rows if old["id"] != item] + [row]
    write_json("results.json", rows)
    say("%s %s  %s" % (item, "PASS" if ok else "FAIL", detail))
    if not ok:
        raise SystemExit(1)


def checksum_or_die():
    got = source_checksum()
    if got != EXPECTED:
        die("源码校验是 %s，期望 %s" % (got, EXPECTED))
    return got


def guard_snapshot():
    etcd = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            raw = open("/proc/%s/cmdline" % name, "rb").read().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        if "etcd" in raw and HOME + "/etcd" in raw:
            etcd.append(int(name))
    if len(etcd) != 1:
        die("etcd 进程数量是 %d" % len(etcd))
    proxy = one("mini-proxy")
    return {"etcd": etcd[0], "proxy": proxy[1]}


def assert_guard(saved):
    if not os.path.exists("/proc/%d" % saved["etcd"]) or not os.path.exists("/proc/%d" % saved["proxy"]):
        die("etcd 或 node-a proxy 的 pid 变了。脚本不会重启它们。")


def assert_certs():
    _role, _pid, argv = one("mini-api")
    flags = flag_map(argv)
    server = flags.get("-tls-cert") or ""
    ca = client_paths()[1]
    if not server or not os.path.isfile(server):
        die("找不到正在使用的服务器证书")
    verified = subprocess.run(["openssl", "verify", "-CAfile", ca, server], capture_output=True, text=True)
    if verified.returncode != 0:
        die("CA 无法验证服务器证书，没有改动进程")
    for path, label in ((server, "服务器证书"), (ca, "CA")):
        remaining = subprocess.run(["openssl", "x509", "-in", path, "-noout", "-checkend", "21600"], capture_output=True, text=True)
        if remaining.returncode != 0:
            die(label + " 剩余有效期不足 6 小时，没有改动进程")


def reload_same(role, log_name, root):
    _found, pid, argv = one(role)
    stop_pid(pid, root=root and os.geteuid() != 0)
    spawn(argv, LOGS + "/" + log_name, root=root and os.geteuid() != 0)
    deadline = time.time() + 15
    while time.time() < deadline:
        got = [item for item in procs() if item[0] == role and item[2] == argv]
        if len(got) == 1:
            say("%s 已重新读取证书，pid %d" % (role, got[0][1]))
            return
        time.sleep(0.3)
    die(role + " 没有用原来的参数重新启动")


def reload_root():
    if os.geteuid() == 0:
        die("请不要用 sudo 包裹整个命令")
    assert_certs()
    say("请输入 10.128.176.165 的 sudo 密码。输入时不会显示。")
    say("这一步只重启本机 root 身份的 Agent 和 proxy，让它们读取新证书。不更换程序，不停止 etcd。")
    if subprocess.run(["sudo", "-v"]).returncode != 0:
        die("sudo 没有通过，没有改动任何进程")
    etcd_pid = guard_snapshot()["etcd"]
    for role, log_name in (("mini-agent", "agent-a-g4cur.log"), ("mini-proxy", "proxy-a-reload.log")):
        _found, pid, _argv = one(role)
        if os.stat("/proc/%d" % pid).st_uid != 0:
            die(role + " 不是 root，拒绝在这一步重启")
        reload_same(role, log_name, root=True)
    if not os.path.exists("/proc/%d" % etcd_pid):
        die("etcd 不在了")
    deadline = time.time() + 40
    while time.time() < deadline:
        try:
            table = nodes()
        except Exception:
            time.sleep(1)
            continue
        ready = all(table.get(name, {}).get("status", {}).get("condition") == "Ready" for name in ("node-a", "node-b"))
        if ready:
            say("两台节点都是 Ready")
            return
        time.sleep(1)
    die("重新读取证书后，节点没有在 40 秒内回到 Ready")


def precheck():
    os.umask(0o077)
    if shutil.which("go") is None:
        die("非交互 PATH 里没有 go。需要 /home/lihaoyang/.local/go/bin")
    assert_certs()
    got = checksum_or_die()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = HOME + "/evidence/g4cur-" + stamp
    os.makedirs(path, 0o700)
    with open(EV_PTR, "w", encoding="utf-8") as f:
        f.write(path + "\n")
    for stale in (REQ, ACK, FWD_FLAG):
        if os.path.exists(stale):
            os.remove(stale)
    table = nodes()
    problems = []
    if pods():
        problems.append("已有 Pod: " + " ".join(pod["metadata"]["name"] for pod in pods()))
    if set(table) != {"node-a", "node-b"}:
        problems.append("节点不是 node-a 和 node-b")
    for name, node in table.items():
        cpu, mem = reserved(node)
        spec = node.get("spec", {})
        alloc = spec.get("allocatable") or {}
        if not node.get("status", {}).get("ready") or node.get("status", {}).get("condition") != "Ready":
            problems.append(name + " 不是 Ready")
        if cpu or mem:
            problems.append(name + " 预留不是 0")
        if int(alloc.get("cpuMilli") or 0) != 2000 or int(alloc.get("memoryBytes") or 0) != 1073741824:
            problems.append(name + " allocatable 与这套集群不一致")
    cidr = {name: (node.get("spec") or {}).get("podCIDR") for name, node in table.items()}
    if cidr.get("node-a") != "10.88.0.0/24" or cidr.get("node-b") != "10.89.0.0/24":
        problems.append("Pod CIDR 与这套集群不一致")
    for role in ("mini-api", "mini-manager", "mini-scheduler", "mini-agent", "mini-proxy"):
        got_role = [item for item in procs() if item[0] == role]
        if len(got_role) != 1:
            problems.append("%s 数量 %d" % (role, len(got_role)))
    if problems:
        write_json("precheck.json", {"ok": False, "problems": problems, "checksum": got})
        die("；".join(problems))
    saved = guard_snapshot()
    client_paths()
    write_json("guard.json", saved)
    write_json("precheck.json", {"ok": True, "checksum": got, "at": now_iso()})
    say("源码校验通过，集群是空的。证据目录 " + path)


def precheck_b():
    got = [item for item in procs() if item[0] == "mini-agent"]
    fwd = [item for item in procs() if item[0] == "forwarder"]
    proxy = [item for item in procs() if item[0] == "mini-proxy"]
    if len(got) != 1 or len(fwd) != 1 or len(proxy) != 1:
        die("node-b agent/forwarder/proxy 数量是 %d/%d/%d" % (len(got), len(fwd), len(proxy)))
    if not os.path.isfile(FWD):
        die("没有 " + FWD)
    emit({"ok": True, "agent": got[0][1], "forwarder": fwd[0][1], "proxy": proxy[0][1]})


def build():
    checksum_or_die()
    os.makedirs(BIN, exist_ok=True)
    os.makedirs(LOGS, exist_ok=True)
    targets = {
        "mini-api": "./cmd/mini-api",
        "mini-manager": "./cmd/mini-manager",
        "mini-scheduler": "./cmd/mini-scheduler",
        "mini-agent": "./cmd/mini-agent",
        "mini-k8s": "./cmd/mini-k8s",
        "g4-probe": "./cmd/g4-probe",
    }
    hashes = {}
    for name, pkg in targets.items():
        dest = BIN + "/" + name + ".g4cur"
        say("编译 " + name)
        proc = subprocess.run(["go", "build", "-o", dest, pkg], cwd=SRC, capture_output=True, text=True)
        if proc.returncode != 0:
            die(name + " 编译失败: " + (proc.stderr or "")[-800:])
        hashes[name] = hashlib.sha256(open(dest, "rb").read()).hexdigest()
    write_json("build.json", {"ok": True, "sha256": hashes, "at": now_iso()})
    say("六个程序已写入 bin/*.g4cur")


def spawn(argv, log_path, root=False):
    os.makedirs(LOGS, exist_ok=True)
    cmd = "setsid " + " ".join(json.dumps(part) for part in argv) + " >> " + json.dumps(log_path) + " 2>&1 < /dev/null &"
    if root and os.geteuid() != 0:
        subprocess.run(["sudo", "-n", "bash", "-c", cmd], check=True)
    else:
        subprocess.run(["bash", "-c", cmd], check=True)


def stop_pid(pid, root=False):
    killer = ["sudo", "-n", "kill"] if root else ["kill"]
    subprocess.run(killer + ["-TERM", str(pid)], check=False)
    for _ in range(50):
        if not os.path.exists("/proc/%d" % pid):
            return
        time.sleep(0.1)
    killer = ["sudo", "-n", "kill"] if root else ["kill"]
    subprocess.run(killer + ["-KILL", str(pid)], check=False)
    for _ in range(20):
        if not os.path.exists("/proc/%d" % pid):
            return
        time.sleep(0.1)
    die("进程 %d 没有退出" % pid)


def swap_control():
    assert_guard(read_json("guard.json"))
    order = ("mini-scheduler", "mini-manager", "mini-api")
    saved = {}
    for role in order:
        saved[role] = one(role)[2]
    for role in order:
        stop_pid(one(role)[1], root=False)
    logs = {"mini-api": "api-g4cur.log", "mini-manager": "manager-g4cur.log", "mini-scheduler": "scheduler-g4cur.log"}
    for role in ("mini-api", "mini-manager", "mini-scheduler"):
        argv = saved[role][:]
        argv[0] = BIN + "/" + role + ".g4cur"
        spawn(argv, LOGS + "/" + logs[role], root=False)
        time.sleep(0.4)
    deadline = time.time() + 25
    while time.time() < deadline:
        try:
            nodes()
            say("新的 API 已响应")
            return
        except Exception:
            time.sleep(0.5)
    die("新的 API 没有在 25 秒内响应。日志 " + LOGS + "/api-g4cur.log")


def nodea_agent():
    if os.geteuid() == 0:
        die("node-a 切换不要用 root 身份直接跑整个脚本")
    role, pid, argv = one("mini-agent")
    if "-node" not in argv or argv[argv.index("-node") + 1] != "node-a":
        die("165 上的 Agent 不是 node-a")
    new = argv[:]
    new[0] = BIN + "/mini-agent.g4cur"
    stop_pid(pid, root=True)
    spawn(new, LOGS + "/agent-a-g4cur.log", root=True)
    deadline = time.time() + 15
    while time.time() < deadline:
        got = [item for item in procs() if item[0] == "mini-agent" and item[2][0] == new[0]]
        if len(got) == 1:
            say("node-a Agent 已换成 g4cur，pid %d" % got[0][1])
            return
        time.sleep(0.3)
    die("node-a Agent 没有以新程序启动")


def nodeb_agent():
    if os.geteuid() == 0:
        die("node-b Agent 不能用 root 启动")
    binary = BIN + "/mini-agent.g4cur"
    if not os.access(binary, os.X_OK):
        die("node-b 上没有可执行的 " + binary)
    _role, pid, argv = one("mini-agent")
    if argv[argv.index("-node") + 1] != "node-b":
        die("125 上的 Agent 不是 node-b")
    if "https://127.0.0.1:18443" not in argv:
        die("node-b 没有走本机 18443 转发，拒绝替换")
    new = argv[:]
    new[0] = binary
    stop_pid(pid, root=False)
    spawn(new, LOGS + "/agent-b-g4cur.log", root=False)
    deadline = time.time() + 15
    while time.time() < deadline:
        got = [item for item in procs() if item[0] == "mini-agent" and item[2][0] == binary]
        if len(got) == 1 and any(item[0] == "forwarder" for item in procs()):
            emit({"ok": True, "pid": got[0][1]})
            return
        time.sleep(0.3)
    die("node-b Agent 没有启动，或转发进程消失了")


def ask_b(action, data=None, timeout=180):
    if os.path.exists(ACK):
        os.remove(ACK)
    rid = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
    atomic_json(REQ, {"id": rid, "action": action, "payload": data or {}})
    say("等待 node-b: " + action)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(ACK):
            try:
                with open(ACK, encoding="utf-8") as f:
                    ack = json.load(f)
            except (OSError, json.JSONDecodeError):
                time.sleep(0.2)
                continue
            if ack.get("id") == rid:
                if os.path.exists(REQ):
                    os.remove(REQ)
                if not ack.get("ok"):
                    die("node-b %s 失败: %s" % (action, str(ack.get("output", ""))[-500:]))
                text = ack.get("output") or ""
                for line in text.splitlines():
                    if line.startswith("G4RESULT "):
                        return json.loads(line[len("G4RESULT "):])
                return {"ok": True, "output": text}
        time.sleep(0.3)
    die("node-b 没有在 %d 秒内完成 %s" % (timeout, action))


def cri(endpoint, args, use_sudo):
    cmd = [CRICTL, "--runtime-endpoint", endpoint, *args]
    if use_sudo:
        cmd = ["sudo", "-n", *cmd]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "crictl failed")[-500:])
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


def runtime_report(node, use_sudo):
    endpoint = ENDPOINTS[node]
    images = {}
    for image in (BUSYBOX, PROBE_IMAGE):
        cmd = [CRICTL, "--runtime-endpoint", endpoint, "inspecti", image]
        if use_sudo:
            cmd = ["sudo", "-n", *cmd]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        images[image] = proc.returncode == 0
    ps = cri(endpoint, ["ps", "-a", "--label", "mini-k8s.io/managed=true", "-o", "json"], use_sudo)
    sandboxes = cri(endpoint, ["pods", "--label", "mini-k8s.io/managed=true", "-o", "json"], use_sudo)
    names = sudo_ls(LEASES[node]) if use_sudo else user_ls(LEASES[node])
    return {
        "images": images,
        "containers": record_ids(ps),
        "sandboxes": record_ids(sandboxes),
        "leases": names,
    }


def sudo_ls(path):
    proc = subprocess.run(["sudo", "-n", "ls", "-A", path], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or "ls failed")[-300:])
    return sorted(line for line in proc.stdout.splitlines() if line)


def user_ls(path):
    try:
        return sorted(os.listdir(path))
    except OSError as err:
        raise RuntimeError(str(err)) from None


def baseline(node, use_sudo):
    report = runtime_report(node, use_sudo)
    if report["leases"] != ["last_reserved_ip.0", "lock"]:
        die(node + " 租约不是基线: " + " ".join(report["leases"]))
    if report["containers"] or report["sandboxes"]:
        die(node + " 还有 mini-k8s 容器")
    if not all(report["images"].values()):
        die(node + " 缺少 busybox 或 g4-probe 镜像")
    return report


def baseline_b():
    if os.geteuid() != 0:
        die("baseline-b 需要 sudo")
    _role, pid, argv = one("mini-proxy")
    flags = flag_map(argv)
    if "https://10.128.176.165:8443" not in argv:
        die("node-b proxy 没有连接 165 上的 API，拒绝重启")
    remaining = subprocess.run(["openssl", "x509", "-in", flags["-ca-file"], "-noout", "-checkend", "21600"], capture_output=True, text=True)
    if remaining.returncode != 0:
        die("node-b 的 CA 剩余有效期不足 6 小时，没有重启 proxy")
    say("重新读取 node-b proxy 的 CA，pid %d" % pid)
    reload_same("mini-proxy", "proxy-b-reload.log", root=False)
    report = baseline("node-b", False)
    # Already root, so crictl and ls run directly. runtime_report(use_sudo=False) is correct here.
    with open("/tmp/g4cur-baseline-b.json", "w", encoding="utf-8") as f:
        json.dump({"ok": True, "report": report, "at": now_iso()}, f)
    os.chmod("/tmp/g4cur-baseline-b.json", 0o644)
    emit({"ok": True})


def cri_state(node, uid, use_sudo):
    endpoint = ENDPOINTS[node]
    ps = cri(endpoint, ["ps", "-a", "--label", "mini-k8s.io/pod-uid=" + uid, "-o", "json"], use_sudo)
    sandboxes = cri(endpoint, ["pods", "--label", "mini-k8s.io/pod-uid=" + uid, "-o", "json"], use_sudo)
    hosts = hostname_of(sandboxes)
    sids = record_ids(sandboxes)
    if sids and not hosts:
        info = cri(endpoint, ["inspectp", "-o", "json", sids[0]], use_sudo)
        hosts = hostname_of(info)
    running = any(str(item.get("state", "")).endswith("RUNNING") for item in listed(ps))
    return {
        "containers": record_ids(ps),
        "sandboxes": sids,
        "running": running and bool(record_ids(ps)),
        "hostname": hosts,
    }


def hostname_of(obj):
    found = []
    if isinstance(obj, dict):
        if isinstance(obj.get("hostname"), str):
            found.append(obj["hostname"])
        for value in obj.values():
            found.extend(hostname_of(value))
    elif isinstance(obj, list):
        for value in obj:
            found.extend(hostname_of(value))
    return found


def wait_running_rs(name, count, timeout):
    started = time.time()
    rs = api_get("replicasets/" + name)
    uid = rs["metadata"]["uid"]
    deadline = started + timeout
    while time.time() < deadline:
        table = nodes()
        got = running_of(uid)
        if len(got) == count and all(pod.get("status", {}).get("podIP") for pod in got):
            return uid, got, table, time.time() - started
        time.sleep(0.5)
    die("%s 在 %d 秒内没有 %d 个 Running" % (name, timeout, count))


def scenario_01():
    started = time.time()
    apply_text(rs_yaml("g4cur-place", 2, BUSYBOX, ["sleep", "7200"], 1100, 67108864, 1100, 134217728))
    uid, got, table, _elapsed = wait_running_rs("g4cur-place", 2, 30)
    elapsed = time.time() - started
    places = sorted(node_of(pod, table) for pod in got)
    ok = elapsed <= 30 and places == ["node-a", "node-b"]
    detail = "两台各一个 Running，用时 %.1f 秒" % elapsed
    for pod in got:
        node = node_of(pod, table)
        if node == "node-b":
            state = ask_b("cri-present", {"uid": pod["metadata"]["uid"]})
        elif node == "node-a":
            state = cri_state(node, pod["metadata"]["uid"], True)
        else:
            raise RuntimeError("Pod assigned to unexpected node: %s" % node)
        if not state.get("running"):
            ok = False
            detail = pod["metadata"]["name"] + " 没有对应的运行容器"
    write_json("g401.json", {"elapsed": elapsed, "pods": [compact_pod(pod, table) for pod in got]})
    if not ok:
        record("G4-01", False, detail)
    delete_names("ReplicaSet", ["g4cur-place"])
    quiet(60)
    record("G4-01", True, detail + "，随后已清理")


def compact_pod(pod, table):
    return {
        "name": pod["metadata"]["name"],
        "uid": pod["metadata"]["uid"],
        "node": node_of(pod, table),
        "phase": (pod.get("status") or {}).get("phase"),
        "reason": (pod.get("status") or {}).get("reason"),
        "podIP": (pod.get("status") or {}).get("podIP"),
    }


def probe_template(cpu, mem):
    return {
        "apiVersion": "mini.k8s/v1",
        "kind": "Pod",
        "metadata": {"name": "placeholder"},
        "spec": {
            "image": BUSYBOX,
            "restartPolicy": "Always",
            "command": ["sleep", "7200"],
            "resources": {
                "requests": {"cpuMilli": cpu, "memoryBytes": mem},
                "limits": {"cpuMilli": cpu, "memoryBytes": mem},
            },
        },
    }


def concurrent(prefix, count, cpu, mem):
    manifest = os.path.join(ev(), prefix + ".json")
    with open(manifest, "w", encoding="utf-8") as f:
        json.dump(probe_template(cpu, mem), f)
    out_path = os.path.join(ev(), prefix + "-create.jsonl")
    server, ca, token_path, _token, _ctx = api()
    cmd = [
        BIN + "/g4-probe.g4cur", "create", "-server", server, "-ca-file", ca,
        "-token-file", token_path, "-manifest", manifest, "-name-prefix", prefix, "-count", str(count),
    ]
    samples = []
    stop = {"value": False}

    def loop():
        while not stop["value"]:
            try:
                table = nodes()
                observed = time.time()
                current = pods()
                samples.append({"at": observed, "nodes": table, "pods": current})
            except Exception as err:
                samples.append({"at": time.time(), "error": str(err)})
            time.sleep(0.25)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    started = time.time()
    with open(out_path, "w", encoding="utf-8") as out:
        proc = subprocess.run(cmd, stdout=out, stderr=subprocess.PIPE, text=True, timeout=40)
    time.sleep(0.3)
    stop["value"] = True
    thread.join(timeout=3)
    creates = []
    with open(out_path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                creates.append(json.loads(line))
    return proc.returncode, creates, samples, started


def judge_capacity(samples, prefix, reason):
    first = {}
    over = False
    negative = False
    rebound = {}
    for sample in samples:
        if "pods" not in sample:
            continue
        for node in sample["nodes"].values():
            got_cpu, got_mem = reserved(node)
            alloc = node["spec"]["allocatable"]
            if got_cpu < 0 or got_mem < 0:
                negative = True
            if got_cpu > int(alloc["cpuMilli"]) or got_mem > int(alloc["memoryBytes"]):
                over = True
        for pod in sample["pods"]:
            if not pod["metadata"]["name"].startswith(prefix + "-"):
                continue
            uid = pod["metadata"]["uid"]
            node_uid = pod.get("nodeUID") or ""
            if uid in rebound and rebound[uid] and node_uid and rebound[uid] != node_uid:
                rebound[uid] = "CHANGED"
            elif node_uid:
                rebound[uid] = node_uid
            status = pod.get("status") or {}
            if status.get("phase") == "Pending" and status.get("reason") == reason and uid not in first:
                first[uid] = sample["at"]
    return first, over, negative, rebound


def scenario_02(prefix, count, cpu, mem, reason, running_count):
    code, creates, samples, _started = concurrent(prefix, count, cpu, mem)
    if code != 0 or any(item.get("error") for item in creates):
        record("G4-02", False, prefix + " 并发创建没有全部得到 UID，没有重试")
    deadline = time.time() + 30
    final = []
    while time.time() < deadline:
        table = nodes()
        current = pods()
        samples.append({"at": time.time(), "nodes": table, "pods": current})
        final = [pod for pod in current if pod["metadata"]["name"].startswith(prefix + "-")]
        running = [pod for pod in final if (pod.get("status") or {}).get("phase") == "Running"]
        pending = [pod for pod in final if (pod.get("status") or {}).get("phase") == "Pending" and (pod.get("status") or {}).get("reason") == reason]
        if len(final) == count and len(running) == running_count and len(pending) == count - running_count:
            break
        time.sleep(0.4)
    else:
        record("G4-02", False, prefix + " 没有形成 %d Running 和 %d %s" % (running_count, count - running_count, reason))
    first, over, negative, rebound = judge_capacity(samples, prefix, reason)
    late = []
    for item in creates:
        seen = first.get(item["uid"])
        if seen and seen - parse_rfc(item["started"]) > 10:
            late.append(item["name"])
    pending_uids = {pod["metadata"]["uid"] for pod in final if (pod.get("status") or {}).get("phase") == "Pending"}
    missing = [item["name"] for item in creates if item["uid"] in pending_uids and item["uid"] not in first]
    places = sorted(node_of(pod, nodes()) for pod in final if (pod.get("status") or {}).get("phase") == "Running")
    ok = not over and not negative and "CHANGED" not in rebound.values() and not late and not missing and places == ["node-a", "node-b"]
    detail = "%s: %d Running，%d 个 %s" % (prefix, running_count, count - running_count, reason)
    if not ok:
        detail += "；超额=%s 负数=%s 重复绑定=%s 超时=%s" % (over, negative, "CHANGED" in rebound.values(), late or missing)
        record("G4-02", False, detail)
    return final


def parse_rfc(value):
    value = value.replace("Z", "+00:00")
    if "." in value:
        head, tail = value.split(".", 1)
        frac, zone = tail.split("+", 1)
        value = head + "." + frac[:6] + "+" + zone
    return datetime.fromisoformat(value).timestamp()


def scenario_02_both():
    scenario_02("g4cur-cpu", 6, 1500, 33554432, "InsufficientCPU", 2)
    names = [pod["metadata"]["name"] for pod in pods() if pod["metadata"]["name"].startswith("g4cur-cpu-")]
    delete_names("Pod", names)
    quiet(60)
    scenario_02("g4cur-mem", 4, 50, 805306368, "InsufficientMemory", 2)
    record("G4-02", True, "CPU 6 个里 2 个 Running、4 个 InsufficientCPU；内存 4 个里 2 个 Running、2 个 InsufficientMemory")


def scenario_03():
    items = [pod for pod in pods() if pod["metadata"]["name"].startswith("g4cur-mem-")]
    table = nodes()
    running = [pod for pod in items if (pod.get("status") or {}).get("phase") == "Running"]
    pending = [pod for pod in items if (pod.get("status") or {}).get("phase") == "Pending"]
    chosen = next(pod for pod in running if node_of(pod, table) == "node-a")
    node = nodes()["node-a"]
    before_cpu, before_mem = reserved(node)
    ip = (chosen.get("status") or {}).get("podIP") or ""
    uid = chosen["metadata"]["uid"]
    started = time.time()
    delete_names("Pod", [chosen["metadata"]["name"]])
    saw_release = False
    negative = False
    too_low = False
    promoted = None
    deadline = started + 30
    while time.time() < deadline:
        table = nodes()
        cpu, mem = reserved(table["node-a"])
        if cpu < 0 or mem < 0:
            negative = True
        if mem < before_mem - 805306368 or cpu < before_cpu - 50:
            too_low = True
        if mem == before_mem - 805306368 and cpu == before_cpu - 50:
            saw_release = True
        current = {pod["metadata"]["uid"]: pod for pod in pods()}
        if uid not in current:
            for pod in pending:
                got = current.get(pod["metadata"]["uid"])
                if got and (got.get("status") or {}).get("phase") == "Running":
                    promoted = got
                    break
        if saw_release and promoted and uid not in current:
            break
        time.sleep(0.25)
    state = cri_state("node-a", uid, True)
    lease_gone = ip and ip not in sudo_ls(LEASES["node-a"])
    elapsed = time.time() - started
    ok = saw_release and promoted and not negative and not too_low and elapsed <= 30 and not state["containers"] and not state["sandboxes"] and lease_gone
    detail = "删除 %s 后 %.1f 秒，Pending Pod 运行，预留只释放一次" % (chosen["metadata"]["name"], elapsed)
    if not ok:
        record("G4-03", False, "释放=%s 新Pod=%s 负数=%s 过度释放=%s CRI残留=%s 租约残留=%s" % (
            saw_release, bool(promoted), negative, too_low, bool(state["containers"] or state["sandboxes"]), not lease_gone))
    names = [pod["metadata"]["name"] for pod in pods() if pod["metadata"]["name"].startswith("g4cur-mem-")]
    delete_names("Pod", names)
    quiet(60)
    record("G4-03", True, detail)


def scenario_04():
    apply_text(pod_yaml("g4cur-fill", BUSYBOX, ["sleep", "7200"], 1900, 33554432))
    apply_text(rs_yaml("g4cur-hold", 1, BUSYBOX, ["sleep", "7200"], 200, 33554432))
    deadline = time.time() + 30
    while time.time() < deadline:
        table = nodes()
        fill = [pod for pod in pods() if pod["metadata"]["name"] == "g4cur-fill"]
        rs = api_get("replicasets/g4cur-hold")
        hold = running_of(rs["metadata"]["uid"])
        if fill and (fill[0].get("status") or {}).get("phase") == "Running" and node_of(fill[0], table) == "node-a" and len(hold) == 1 and node_of(hold[0], table) == "node-b":
            break
        time.sleep(0.4)
    else:
        record("G4-04", False, "填充 Pod 或 hold Pod 没有按 node-a / node-b 运行")
    fill = next(pod for pod in pods() if pod["metadata"]["name"] == "g4cur-fill")
    rs = api_get("replicasets/g4cur-hold")
    hold = running_of(rs["metadata"]["uid"])[0]
    before = {
        "fill": cri_state("node-a", fill["metadata"]["uid"], True),
        "hold": ask_b("cri-present", {"uid": hold["metadata"]["uid"]}),
    }
    if not before["fill"]["running"] or not before["hold"].get("running"):
        record("G4-04", False, "切断前没有看到两侧容器")
    write_json("g404-before.json", {"fill": compact_pod(fill, nodes()), "hold": compact_pod(hold, nodes()), "cri": before})
    samples = []
    stop = {"value": False}

    def loop():
        while not stop["value"]:
            try:
                table = nodes()
                observed = time.time()
                current = pods()
                samples.append({"at": observed, "nodes": table, "pods": current})
            except Exception as err:
                samples.append({"at": time.time(), "error": str(err)})
            time.sleep(0.2)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    while not any("nodes" in sample for sample in samples):
        time.sleep(0.1)
    with open(FWD_FLAG, "w", encoding="utf-8") as f:
        f.write(now_iso() + "\n")
    ask_b("stop-forwarder")
    unknown_at = None
    heartbeat = None
    deadline = time.time() + 25
    while time.time() < deadline:
        for sample in samples:
            node = (sample.get("nodes") or {}).get("node-b")
            if node and (node.get("status") or {}).get("condition") == "Unknown":
                unknown_at = sample["at"]
                heartbeat = (node.get("status") or {}).get("lastHeartbeat")
                break
        if unknown_at:
            break
        time.sleep(0.1)
    if not unknown_at or not heartbeat:
        stop["value"] = True
        record("G4-04", False, "切断后 25 秒内没有看到 node-b Unknown")
    delta = unknown_at - parse_rfc(heartbeat)
    apply_text(pod_yaml("g4cur-after", BUSYBOX, ["sleep", "7200"], 100, 33554432))
    after = None
    hold_bad = False
    window_end = unknown_at + 30
    while time.time() < window_end:
        table = nodes()
        current = pods()
        hold_now = [pod for pod in current if pod["metadata"].get("ownerUID") == rs["metadata"]["uid"]]
        if [pod["metadata"]["uid"] for pod in hold_now] != [hold["metadata"]["uid"]] or hold_now[0]["metadata"].get("deletionTimestamp"):
            hold_bad = True
        node_b = table["node-b"]
        cpu, _mem = reserved(node_b)
        if cpu < 200:
            hold_bad = True
        got = [pod for pod in current if pod["metadata"]["name"] == "g4cur-after"]
        if got and (got[0].get("status") or {}).get("phase") == "Running":
            after = got[0]
        time.sleep(0.5)
    stop["value"] = True
    thread.join(timeout=3)
    after_node = node_of(after, nodes()) if after else ""
    ok = 18 <= delta <= 20 and after_node == "node-a" and not hold_bad
    write_json("g404-window.json", {"delta": delta, "heartbeat": heartbeat, "unknownAt": unknown_at, "afterNode": after_node, "holdBad": hold_bad})
    ask_b("start-forwarder")
    if os.path.exists(FWD_FLAG):
        os.remove(FWD_FLAG)
    ready_deadline = time.time() + 30
    while time.time() < ready_deadline:
        table = nodes()
        if all(node.get("status", {}).get("ready") and node.get("status", {}).get("condition") == "Ready" for node in table.values()):
            break
        time.sleep(0.5)
    else:
        record("G4-04", False, "转发恢复后 30 秒内两台没有都 Ready")
    after_cri = cri_state("node-a", fill["metadata"]["uid"], True)
    hold_cri = ask_b("cri-present", {"uid": hold["metadata"]["uid"]})
    same = before["fill"]["containers"] == after_cri["containers"] and before["hold"].get("containers") == hold_cri.get("containers")
    if not ok or not same:
        record("G4-04", False, "Unknown 延迟 %.2f 秒，after 在 %s，hold 被替换=%s，容器保持=%s" % (delta, after_node or "未运行", hold_bad, same))
    delete_names("Pod", ["g4cur-fill", "g4cur-after"])
    delete_names("ReplicaSet", ["g4cur-hold"])
    quiet(70)
    record("G4-04", True, "最后心跳后 %.2f 秒 Unknown，新 Pod 在 node-a，原 hold 容器保持，连接已恢复" % delta)


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
    spawn(["python3", FWD], LOGS + "/g4-tcp-forward.log", root=False)
    deadline = time.time() + 8
    while time.time() < deadline:
        if listening(18443):
            emit({"ok": True, "already": False})
            return
        time.sleep(0.2)
    die("转发没有重新监听 127.0.0.1:18443")


def listening(port):
    needle = "0100007F:%04X" % port
    try:
        with open("/proc/net/tcp", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) > 3 and parts[1].upper() == needle and parts[3] == "0A":
                    return True
    except OSError:
        return False
    return False


def scenario_05():
    apply_text(rs_yaml("g4cur-tcp", 2, PROBE_IMAGE, ["/g4-probe", "serve", "-listen", ":8080"], 1100, 67108864))
    _uid, got, table, elapsed = wait_running_rs("g4cur-tcp", 2, 30)
    if sorted(node_of(pod, table) for pod in got) != ["node-a", "node-b"]:
        record("G4-05", False, "两个探测 Pod 不在两台机器上")
    by_node = {node_of(pod, table): pod for pod in got}
    results = {}
    for source, dest in (("node-a", "node-b"), ("node-b", "node-a")):
        peer = by_node[dest]
        data = {
            "uid": by_node[source]["metadata"]["uid"],
            "sourceName": by_node[source]["metadata"]["name"],
            "peerIP": peer["status"]["podIP"],
            "peerName": peer["metadata"]["name"],
            "sourceIP": by_node[source]["status"]["podIP"],
        }
        if source == "node-a":
            results[source] = tcp_exec_local("node-a", data, True)
        else:
            results[source] = ask_b("tcp-exec", data, timeout=80)
    write_json("g405.json", {"elapsed": elapsed, "results": results, "pods": [compact_pod(pod, table) for pod in got]})
    ok = all(item.get("ok") for item in results.values())
    if not ok:
        record("G4-05", False, "双向 TCP 没有全部成功")
    delete_names("ReplicaSet", ["g4cur-tcp"])
    quiet(60)
    record("G4-05", True, "双向各 10 次新连接，身份匹配，用时 %.1f 秒" % elapsed)


def tcp_exec_local(node, data, use_sudo):
    endpoint = ENDPOINTS[node]
    own = cri_state(node, data["uid"], use_sudo)
    if not own["containers"]:
        return {"ok": False, "error": "no container"}
    if own["hostname"] and data.get("sourceName") and data["sourceName"] not in own["hostname"]:
        return {"ok": False, "error": "hostname mismatch", "hostname": own["hostname"]}
    cmd = [CRICTL, "--runtime-endpoint", endpoint, "exec", own["containers"][0],
           "/g4-probe", "check", "-target", data["peerIP"] + ":8080", "-expect", data["peerName"], "-count", "10"]
    if use_sudo:
        cmd = ["sudo", "-n", *cmd]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=50)
    lines = [json.loads(line) for line in proc.stdout.splitlines() if line.strip().startswith("{")]
    good = proc.returncode == 0 and len(lines) == 10 and all(not item.get("error") and item.get("identity") == data["peerName"] for item in lines)
    locals_ok = all(str(item.get("local", "")).startswith(data["sourceIP"] + ":") for item in lines)
    return {"ok": bool(good and locals_ok), "exit": proc.returncode, "lines": lines, "stderr": (proc.stderr or "")[-300:]}


def tcp_exec_b():
    result = tcp_exec_local("node-b", payload(), False)
    emit(result)


def cri_present_b():
    data = payload()
    state = cri_state("node-b", data["uid"], False)
    emit({"ok": True, "running": state["running"], "containers": state["containers"], "sandboxes": state["sandboxes"], "hostname": state["hostname"]})


def scenario_06():
    apply_text(rs_yaml("g4cur-scale", 2, BUSYBOX, ["sleep", "7200"], 50, 67108864, 100, 134217728))
    uid, _got, _table, elapsed = wait_running_rs("g4cur-scale", 2, 30)
    marks = [{"replicas": 2, "elapsed": elapsed}]
    for replicas in (4, 1, 0):
        started = time.time()
        for _ in range(3):
            try:
                mini("scale", "rs", "g4cur-scale", "--replicas", str(replicas))
                break
            except RuntimeError:
                time.sleep(0.4)
        else:
            record("G4-06", False, "scale %d 失败" % replicas)
        deadline = started + 30
        while time.time() < deadline:
            count = len(running_of(uid))
            if replicas == 0:
                count = len([pod for pod in pods() if pod["metadata"].get("ownerUID") == uid and (pod.get("status") or {}).get("phase") == "Running" and not pod["metadata"].get("deletionTimestamp")])
            if count == replicas:
                marks.append({"replicas": replicas, "elapsed": time.time() - started})
                break
            time.sleep(0.4)
        else:
            record("G4-06", False, "scale %d 没有在 30 秒内收敛" % replicas)
    delete_names("ReplicaSet", ["g4cur-scale"])
    quiet(70)
    final_a = baseline("node-a", True)
    final_b = ask_b("cri-empty")
    write_json("g406.json", {"marks": marks, "finalA": final_a, "finalBcri": final_b, "leasesB": "pending"})
    if final_a["leases"] != ["last_reserved_ip.0", "lock"] or final_a["containers"] or final_a["sandboxes"] or not final_b.get("empty"):
        record("G4-06", False, "node-a 或 node-b 运行时没有回到基线")
    record("G4-06", True, "2、4、1、0 都在 30 秒内收敛；node-b 租约由结束时的 sudo 再核对")


def cri_empty_b():
    endpoint = ENDPOINTS["node-b"]
    ps = cri(endpoint, ["ps", "-a", "--label", "mini-k8s.io/managed=true", "-o", "json"], False)
    sandboxes = cri(endpoint, ["pods", "--label", "mini-k8s.io/managed=true", "-o", "json"], False)
    emit({"ok": True, "empty": not record_ids(ps) and not record_ids(sandboxes), "containers": record_ids(ps), "sandboxes": record_ids(sandboxes)})


def final_b():
    if os.geteuid() != 0:
        die("final-b 需要 sudo")
    report = runtime_report("node-b", False)
    with open("/tmp/g4cur-final-b.json", "w", encoding="utf-8") as f:
        json.dump({"ok": True, "report": report, "at": now_iso()}, f)
    os.chmod("/tmp/g4cur-final-b.json", 0o644)
    emit({"ok": True, "leases": report["leases"], "containers": report["containers"], "sandboxes": report["sandboxes"]})


def report():
    path = os.path.join(ev(), "results.json")
    if not os.path.exists(path):
        say("还没有场景结果")
        say("证据目录 " + ev())
        return
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    say("")
    say("G4 已完成的判定")
    for row in rows:
        say("%s  %s  %s" % (row["id"], "PASS" if row["ok"] else "FAIL", row["detail"]))
    say("证据目录 " + ev())


def finish():
    rows = read_json("results.json")
    final_b_report = read_json("final-b.json")["report"]
    base = read_json("baseline-b.json")["report"]
    leases_ok = final_b_report["leases"] == base["leases"] == ["last_reserved_ip.0", "lock"]
    cri_ok = not final_b_report["containers"] and not final_b_report["sandboxes"]
    if not leases_ok or not cri_ok:
        for row in rows:
            if row["id"] == "G4-06":
                row["ok"] = False
                row["detail"] = "node-b 租约或容器没有回到基线: " + " ".join(final_b_report["leases"])
    write_json("results.json", rows)
    assert_guard(read_json("guard.json"))
    say("")
    say("G4 结果")
    all_ok = True
    for row in rows:
        say("%s  %s  %s" % (row["id"], "PASS" if row["ok"] else "FAIL", row["detail"]))
        all_ok = all_ok and row["ok"]
    say("证据目录 " + ev())
    say("这只是 G4 这一轮的结果，不会把 G5 记成通过。")
    if not all_ok:
        raise SystemExit(1)


def recover_forwarder():
    if not os.path.exists(FWD_FLAG):
        return
    try:
        ask_b("start-forwarder", timeout=30)
        os.remove(FWD_FLAG)
        say("已重新拉起 node-b 转发")
    except SystemExit:
        say("node-b 转发没有自动恢复，请在 125 上执行: python3 " + HOME + "/g4_live.py start-forwarder")


def driver():
    assert_certs()
    say("请输入 10.128.176.165 的 sudo 密码。输入时不会显示。")
    if subprocess.run(["sudo", "-v"]).returncode != 0:
        die("sudo 没有通过，没有改动任何进程")
    refresher = threading.Thread(target=sudo_refresh, daemon=True)
    refresher.start()
    try:
        checksum_or_die()
        if pods():
            die("开始前发现 Pod，没有重启组件")
        report = baseline("node-a", True)
        write_json("baseline-a.json", report)
        say("node-a 运行时基线通过，开始更换控制面")
        swap_control()
        nodea_agent()
        ask_b("nodeb-agent")
        quiet(40)
        say("两台 Agent 已换成 g4cur，开始 G4-01")
        scenario_01()
        say("开始 G4-02")
        scenario_02_both()
        say("开始 G4-03")
        scenario_03()
        say("开始 G4-04。切断转发是自动的，不要另外停进程。")
        scenario_04()
        say("开始 G4-05")
        scenario_05()
        say("开始 G4-06")
        scenario_06()
        assert_guard(read_json("guard.json"))
        say("165 上的检查已完成。接下来还要在 125 上核对租约。")
    except SystemExit:
        recover_forwarder()
        raise
    except Exception as err:
        recover_forwarder()
        die(str(err))


def sudo_refresh():
    while True:
        time.sleep(60)
        subprocess.run(["sudo", "-n", "-v"], check=False)


def main():
    if len(sys.argv) < 2:
        die("missing command")
    cmd = sys.argv[1]
    actions = {
        "precheck": precheck,
        "reload-root": reload_root,
        "precheck-b": precheck_b,
        "build": build,
        "driver": driver,
        "baseline-b": baseline_b,
        "nodeb-agent": nodeb_agent,
        "stop-forwarder": stop_forwarder,
        "start-forwarder": start_forwarder,
        "cri-present": cri_present_b,
        "cri-empty": cri_empty_b,
        "tcp-exec": tcp_exec_b,
        "final-b": final_b,
        "finish": finish,
        "report": report,
    }
    if cmd not in actions:
        die("unknown command " + cmd)
    actions[cmd]()


if __name__ == "__main__":
    main()
