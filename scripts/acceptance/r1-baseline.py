#!/usr/bin/env python3
"""Collect a bounded, read-only API baseline; this is not a Gate verdict."""

import argparse
import concurrent.futures
import http.client
import json
import math
import os
import platform
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


RESOURCES = ("pods", "nodes", "replicasets", "services")
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def bounded_int(lower, upper):
    def parse(value):
        try:
            number = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("must be an integer") from exc
        if not lower <= number <= upper:
            raise argparse.ArgumentTypeError(f"must be in {lower}..{upper}")
        return number
    return parse


def server_url(value):
    try:
        parsed = urllib.parse.urlsplit(value)
        # Force validation of malformed ports before any request is made.
        port = parsed.port
    except ValueError as exc:
        raise argparse.ArgumentTypeError("invalid server URL") from exc
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or
            parsed.password or parsed.path not in ("", "/") or parsed.query or parsed.fragment or
            (port is not None and not 1 <= port <= 65535)):
        raise argparse.ArgumentTypeError("server must be an https origin without credentials, path or query")
    return value.rstrip("/")


def digest(value):
    if not re.fullmatch(r"[a-f0-9]{64}", value):
        raise argparse.ArgumentTypeError("source digest must be 64 lowercase hexadecimal characters")
    return value


def image_digest(value):
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", value):
        raise argparse.ArgumentTypeError("image digest must be sha256: followed by 64 lowercase hex characters")
    return value


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward an authorization header to another destination."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[math.ceil(len(ordered) * fraction) - 1], 3)


def collect_one(opener, server, token, resource, timeout):
    started = utc_now()
    begin = time.monotonic()
    result = {"at": started, "resource": resource, "status": None,
              "result": "error", "error": None, "items": None}
    request = urllib.request.Request(
        server + "/api/mini/v1/" + resource,
        headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
        method="GET",
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            result["status"] = response.status
            body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                result["error"] = "response_too_large"
            else:
                try:
                    document = json.loads(body)
                    if not isinstance(document, dict) or not isinstance(document.get("items"), list):
                        result["error"] = "invalid_list_response"
                    else:
                        result["items"] = len(document["items"])
                        result["result"] = "ok"
                except (UnicodeDecodeError, json.JSONDecodeError):
                    result["error"] = "invalid_json"
    except urllib.error.HTTPError as exc:
        # Do not read or persist the server's error body or headers.
        result["status"] = exc.code
        result["error"] = "http_error"
        exc.close()
    except (socket.timeout, TimeoutError):
        result["error"] = "timeout"
    except urllib.error.URLError as exc:
        result["error"] = "timeout" if isinstance(exc.reason, (socket.timeout, TimeoutError)) else "connection_error"
    except (OSError, ssl.SSLError, http.client.HTTPException):
        result["error"] = "connection_error"
    result["latency_ms"] = round((time.monotonic() - begin) * 1000, 3)
    return result


def summarize(records, elapsed):
    ok = [r["latency_ms"] for r in records if r["result"] == "ok"]
    return {
        "requests": len(records),
        "successful": len(ok),
        "errors": len(records) - len(ok),
        "status_counts": dict(Counter(str(r["status"]) if r["status"] is not None else "no_response" for r in records)),
        "error_counts": dict(Counter(r["error"] for r in records if r["error"])),
        "latency_ms_success_only": {"p50": percentile(ok, 0.50), "p95": percentile(ok, 0.95), "p99": percentile(ok, 0.99)},
        "elapsed_seconds": round(elapsed, 3),
        "requests_per_second": round(len(records) / elapsed, 3) if elapsed > 0 else None,
    }


def parser():
    p = argparse.ArgumentParser(description="Bounded read-only API sampling; does not measure or decide a Gate.")
    p.add_argument("--server", required=True, type=server_url, help="HTTPS API origin")
    p.add_argument("--ca-file", required=True, type=Path, help="CA certificate file")
    p.add_argument("--token-file", required=True, type=Path, help="API token file; contents are never printed")
    p.add_argument("--output", required=True, type=Path, help="new private output directory; must not exist")
    p.add_argument("--concurrency", type=bounded_int(1, 32), default=1)
    p.add_argument("--requests-per-worker", type=bounded_int(1, 1000), default=20)
    p.add_argument("--interval-ms", type=bounded_int(0, 60000), default=100,
                   help="delay between requests per worker; 0 is a bounded burst")
    p.add_argument("--timeout-seconds", type=bounded_int(1, 30), default=5)
    p.add_argument("--resource", choices=RESOURCES, action="append", help="repeat to select resources; default all")
    p.add_argument("--source-sha256", type=digest, help="operator-supplied source snapshot digest")
    p.add_argument("--image-digest", type=image_digest, help="operator-supplied workload image digest")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    resources = args.resource or list(RESOURCES)
    if len(set(resources)) != len(resources):
        print("duplicate --resource", file=sys.stderr)
        return 2
    try:
        token = args.token_file.read_text(encoding="utf-8").strip()
        if not token or len(token) > 4096 or any(ch.isspace() for ch in token):
            raise ValueError("invalid token file")
        context = ssl.create_default_context(cafile=str(args.ca_file))
    except (OSError, ValueError, UnicodeError, ssl.SSLError):
        print("cannot read a valid token or CA file", file=sys.stderr)
        return 2
    os.umask(0o077)
    try:
        args.output.mkdir(mode=0o700)
    except OSError:
        print("cannot create new output directory; ensure its parent exists and path is unused", file=sys.stderr)
        return 2
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=context))
    metadata = {
        "started_utc": utc_now(), "server": args.server, "source_sha256_operator_supplied": args.source_sha256,
        "image_digest_operator_supplied": args.image_digest, "client_os": platform.platform(),
        "python": platform.python_version(), "concurrency": args.concurrency,
        "requests_per_worker": args.requests_per_worker, "timeout_seconds": args.timeout_seconds,
        "interval_ms": args.interval_ms,
        "resources": resources, "measurement": "read-only API list requests; not Gate verdict",
        "not_collected": ["etcd request counts and latency", "etcd database and disk usage", "API/etcd CPU and memory", "watch count", "write operations", "ReplicaSet convergence", "Go/etcd/containerd/CNI versions", "worker capacity and background load"],
    }
    with (args.output / "metadata.json").open("x", encoding="utf-8") as output:
        json.dump(metadata, output, ensure_ascii=False, indent=2)
        output.write("\n")

    def worker(index):
        records = []
        for i in range(args.requests_per_worker):
            if i and args.interval_ms:
                time.sleep(args.interval_ms / 1000)
            records.append(collect_one(opener, args.server, token, resources[(index + i) % len(resources)], args.timeout_seconds))
        return records

    begin = time.monotonic()
    records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for group in pool.map(worker, range(args.concurrency)):
            records.extend(group)
    elapsed = time.monotonic() - begin
    with (args.output / "requests.jsonl").open("x", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record, ensure_ascii=False) + "\n")
    summary = summarize(records, elapsed)
    summary.update({"finished_utc": utc_now(), "gate": "NOT_EVALUATED", "result": "COLLECTED" if not summary["errors"] else "COLLECTED_WITH_ERRORS"})
    with (args.output / "summary.json").open("x", encoding="utf-8") as output:
        json.dump(summary, output, ensure_ascii=False, indent=2)
        output.write("\n")
    print(f"{summary['result']}: {summary['requests']} requests; Gate NOT_EVALUATED; evidence: {args.output}")
    return 0 if not summary["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
