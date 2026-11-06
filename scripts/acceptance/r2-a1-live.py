#!/usr/bin/env python3
"""Run the R2-A1 overload probe on node-a after a same-source G4 PASS."""

import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import socket
import time
import urllib.parse
import uuid

import g4_live as g4


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_for(check, message, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.2)
    raise RuntimeError(message)


def request(method, path, body=None):
    server, _, _, token, context = g4.api()
    address = urllib.parse.urlsplit(server)
    conn = http.client.HTTPSConnection(address.hostname, address.port, timeout=3, context=context)
    try:
        conn.request(method, "/api/mini/v1/" + path, body,
                     {"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        response = conn.getresponse()
        return response.status, json.loads(response.read() or "{}")
    finally:
        conn.close()


def hold_slot():
    server, _, _, token, context = g4.api()
    address = urllib.parse.urlsplit(server)
    stream = socket.create_connection((address.hostname, address.port or 443), timeout=2)
    try:
        stream = context.wrap_socket(stream, server_hostname=address.hostname)
        # An incomplete JSON object can never be applied, even if the probe aborts.
        stream.sendall((
            f"POST /api/mini/v1/pods HTTP/1.1\r\nHost: {address.netloc}\r\n"
            f"Authorization: Bearer {token}\r\nContent-Type: application/json\r\n"
            "Content-Length: 64\r\n\r\n{"
        ).encode())
        return stream
    except BaseException:
        stream.close()
        raise


def log_events(log, offset):
    log.seek(offset)
    for line in log:
        if "api request" in line:
            yield dict(re.findall(r"(\w+)=([^\s]+)", line))


def probe(report, holders, name, uid, owner):
    log_path = Path(g4.LOGS) / "api-g4cur.log"
    _, pid, argv = g4.one("mini-api")
    digest = hashlib.sha256(Path(f"/proc/{pid}/exe").read_bytes()).hexdigest()
    build = json.loads((Path(g4.ev()) / "build.json").read_text())
    require(digest == build["sha256"]["mini-api"], "running API does not match the G4 build")
    limit = int(g4.flag_map(argv).get("-max-inflight", "64"))
    require(1 <= limit <= 64, "unsupported live probe request budget")
    report.update(api_pid=pid, api_sha256=digest, ordinary_limit=limit, probes=[])
    # Open slots in parallel so that all holders remain within the 10-second body deadline.
    with log_path.open(encoding="utf-8") as log:
        log.seek(0, os.SEEK_END)
        offset = log.tell()
        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=limit) as pool:
            futures = [pool.submit(hold_slot) for _ in range(limit - 1)]
            for future in concurrent.futures.as_completed(futures):
                if future.exception() is None:
                    holders.append(future.result())
        require(len(holders) == limit - 1, "failed to open the test request holders")
        status, _ = request("DELETE", f"pods/{name}?uid={uid}")
        require(status == 200, f"UID-conditional deletion returned {status}")
        holders.append(hold_slot())
        require(time.monotonic() - started < 3, "request setup exceeded the saturation time budget")
        # A successful TLS send does not mean the server has admitted the request.
        # Establish saturation first and only then start the measured window.
        def saturated():
            status, result = request("GET", "pods")
            if status == 429 and "regular" in result.get("message", ""):
                return result
            require(status == 200, f"unexpected saturation probe status {status}")
            return None

        first_probe = wait_for(saturated, "ordinary request budget never filled", seconds=1)
        window_start = time.monotonic()
        report["probes"].append({"at": time.time(), "requestId": first_probe["requestId"], "status": 429})
        while True:
            status, result = request("GET", "pods")
            require(status == 429 and "regular" in result.get("message", ""),
                    f"ordinary saturation was not maintained (HTTP {status})")
            report["probes"].append({"at": time.time(), "requestId": result["requestId"], "status": status})
            if time.monotonic() - window_start >= 6:
                break
            time.sleep(0.2)
        first, last = (p["requestId"] for p in (report["probes"][0], report["probes"][-1]))

        def read_window():
            events = list(log_events(log, offset))
            ids = {event.get("requestId"): index for index, event in enumerate(events)}
            if first in ids and last in ids and ids[first] < ids[last]:
                return events, ids
            return None

        events, ids = wait_for(read_window, "missing bounding request IDs in API log", seconds=1)
        window = [e for e in events[ids[first]+1:ids[last]] if e.get("identity") == "agent"]
        heartbeats = {e["node"] for e in window if e.get("result") == "200" and
                      e.get("method") == "PUT" and e.get("path") == f"/api/mini/v1/nodes/{e.get('node')}/status"}
        cleanup = [e for e in window if e.get("result") == "200" and e.get("method") == "POST" and
                   e.get("path") == f"/api/mini/v1/pods/{name}/cleanup" and e.get("uid") == uid and e.get("node") == owner]
        report.update(heartbeats=sorted(heartbeats), cleanup_events=cleanup, agent_events=window)
        require({"node-a", "node-b"} <= heartbeats, "did not observe both heartbeats within the saturation window")
        require(cleanup, "did not observe this Pod cleanup within the saturation window")


def main():
    lock = Path(g4.HOME) / ".r1-live.lock"
    lock.mkdir(mode=0o700)
    evidence = Path(g4.SRC) / f"r2-a1-live-{uuid.uuid4().hex[:12]}"
    holders = []
    report = {"source_sha256": g4.EXPECTED, "result": "FAIL"}
    name = "r2-a1-" + uuid.uuid4().hex[:12]
    uid = None
    retain_lock = False
    try:
        evidence.mkdir(mode=0o700)
        require(g4.source_checksum() == g4.EXPECTED, "source digest mismatch")
        prior = Path(g4.ev())
        check = json.loads((prior / "precheck.json").read_text())
        results = json.loads((prior / "results.json").read_text())
        require(check["checksum"] == g4.EXPECTED and
                {r["id"] for r in results if r["ok"]} == {f"G4-{i:02d}" for i in range(1, 7)} and
                (prior / "final-b.json").is_file(), "same-source G4 with cleanup is required")
        require(all(not g4.api_get(kind).get("items") for kind in ("pods", "replicasets", "services")),
                "test cluster is not empty")
        nodes = g4.nodes()
        require(set(nodes) == {"node-a", "node-b"} and all(
            n["status"]["condition"] == "Ready" and g4.reserved(n) == (0, 0) for n in nodes.values()),
            "both nodes must be Ready with zero reservations")
        report.update(g4_evidence=str(prior), pod=name)
        retain_lock = True
        status, created = request("POST", "pods", g4.pod_yaml(name, g4.BUSYBOX, ["sleep", "300"], 50, 67108864))
        require(status == 201, f"test Pod creation returned {status}")
        uid = created["metadata"]["uid"]
        report["pod_uid"] = uid
        running = wait_for(
            lambda: next((p for p in g4.pods() if p["metadata"]["uid"] == uid and
                          p["status"]["phase"] == "Running"), None), "test Pod never ran")
        owner = next(n for n, item in nodes.items() if item["metadata"]["uid"] == running["nodeUID"])
        probe(report, holders, name, uid, owner)
        report["result"] = "PASS"
    except Exception as exc:
        report["error"] = str(exc)
    finally:
        for stream in holders:
            stream.close()
        if uid:
            try:
                status, _ = request("DELETE", f"pods/{name}?uid={uid}")
                require(status in (200, 404), f"test Pod cleanup returned {status}")
                wait_for(lambda: not any(p["metadata"]["uid"] == uid for p in g4.pods()),
                         "test Pod still present after saturation ended")
                require(all(g4.reserved(n) == (0, 0) for n in g4.nodes().values()),
                        "node reservations did not return to baseline")
                retain_lock = False
            except Exception as exc:
                report.update(result="FAIL", cleanup_error=str(exc))
        try:
            if evidence.is_dir():
                (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        finally:
            if not retain_lock:
                lock.rmdir()
    print(f"{report['result']}: {report.get('error', 'R2-A1 live overload evidence recorded')}")
    print(f"Evidence: {evidence}")
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
