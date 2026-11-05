#!/usr/bin/env python3
"""Capture selected local etcd metrics; never interpret counters as rates."""

import argparse
import ipaddress
import json
import math
import os
import queue
import re
import socket
import ssl
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


MAX_BYTES = 2 * 1024 * 1024
# No label values or raw metrics are saved. These are instantaneous gauges or
# cumulative counters and histogram sums/counts, never derived throughput.
METRICS = (
    "etcd_mvcc_db_total_size_in_bytes",
    "etcd_mvcc_db_total_size_in_use_in_bytes",
    "etcd_mvcc_range_total",
    "etcd_mvcc_put_total",
    "etcd_mvcc_delete_total",
    "etcd_mvcc_txn_total",
    "etcd_server_has_leader",
    "etcd_server_leader_changes_seen_total",
    "etcd_server_proposals_committed_total",
    "etcd_server_proposals_failed_total",
    "etcd_disk_wal_fsync_duration_seconds_count",
    "etcd_disk_wal_fsync_duration_seconds_sum",
    "etcd_disk_backend_commit_duration_seconds_count",
    "etcd_disk_backend_commit_duration_seconds_sum",
    "process_cpu_seconds_total",
    "process_resident_memory_bytes",
)
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^\n]*\})?\s+(\S+)(?:\s+\d+)?\s*$')


def metrics_url(value):
    try:
        url = urllib.parse.urlsplit(value)
        host = ipaddress.ip_address(url.hostname or "")
        port = url.port
    except ValueError as exc:
        raise argparse.ArgumentTypeError("URL must use a numeric loopback address") from exc
    if (url.scheme not in ("http", "https") or not host.is_loopback or
            url.username is not None or url.password is not None or
            url.path != "/metrics" or url.query or url.fragment or
            port is None or not 1 <= port <= 65535 or
            (isinstance(host, ipaddress.IPv6Address) and host.scope_id is not None)):
        raise argparse.ArgumentTypeError("URL must be a loopback http(s) /metrics endpoint without credentials or query")
    return value


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def parse_metrics(raw):
    """Return finite unlabeled samples; refuse to guess how to combine labels."""
    values = {name: [] for name in METRICS}
    labelled = {name: False for name in METRICS}
    text = raw.decode("utf-8")
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = SAMPLE.fullmatch(line)
        if not match or match.group(1) not in values:
            continue
        name, labels, raw_value = match.groups()
        if labels not in (None, "{}"):
            labelled[name] = True
            continue
        try:
            value = float(raw_value)
        except ValueError:
            continue
        if math.isfinite(value):
            values[name].append(value)
    result = {}
    for name in METRICS:
        samples = values[name]
        if len(samples) == 1 and not labelled[name]:
            result[name] = {"status": "present", "value": samples[0]}
        elif len(samples) > 1 or (samples and labelled[name]):
            result[name] = {"status": "ambiguous", "value": None}
        elif labelled[name]:
            result[name] = {"status": "labelled_only", "value": None}
        else:
            result[name] = {"status": "missing", "value": None}
    return result


def fetch(opener, url, timeout):
    request = urllib.request.Request(url, headers={"Accept": "text/plain"}, method="GET")
    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                return None, "response_too_large"
            return parse_metrics(raw), None
    except urllib.error.HTTPError as exc:
        exc.close()
        return None, "http_error"
    except (socket.timeout, TimeoutError):
        return None, "timeout"
    except urllib.error.URLError as exc:
        return None, "timeout" if isinstance(exc.reason, (socket.timeout, TimeoutError)) else "connection_error"
    except (OSError, ssl.SSLError, UnicodeError):
        return None, "connection_or_decode_error"


def fetch_with_deadline(opener, url, timeout):
    """Enforce a wall-clock budget even if a peer trickles response bytes."""
    result = queue.Queue(maxsize=1)

    def read():
        try:
            result.put_nowait(fetch(opener, url, timeout))
        except Exception:
            result.put_nowait((None, "collection_error"))

    threading.Thread(target=read, daemon=True).start()
    try:
        return result.get(timeout=timeout)
    except queue.Empty:
        return None, "timeout"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Capture allowlisted local etcd metric values; never a Gate verdict")
    parser.add_argument("--url", type=metrics_url, required=True, help="numeric loopback http(s) URL ending in /metrics")
    parser.add_argument("--output", type=Path, required=True, help="new JSON file in a private existing directory")
    parser.add_argument("--ca-file", type=Path, help="CA certificate for HTTPS")
    parser.add_argument("--timeout-seconds", type=int, default=5, choices=range(1, 31), metavar="1..30")
    args = parser.parse_args(argv)
    try:
        context = ssl.create_default_context(cafile=str(args.ca_file) if args.ca_file else None)
    except (OSError, ValueError, ssl.SSLError):
        print("cannot load CA certificate", file=sys.stderr)
        return 2
    os.umask(0o077)
    try:
        output = args.output.open("x", encoding="utf-8")
    except OSError:
        print("cannot create new output file in an existing directory", file=sys.stderr)
        return 2
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=context))
    metrics, error = fetch_with_deadline(opener, args.url, args.timeout_seconds)
    result = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "endpoint": args.url,
        "result": "CAPTURED" if error is None else "CAPTURE_FAILED",
        "gate": "NOT_EVALUATED",
        "error": error,
        "interpretation": "Instantaneous snapshot; counters and histogram counts/sums are cumulative, not rates.",
        "metrics": metrics if metrics is not None else {name: {"status": "not_collected", "value": None} for name in METRICS},
    }
    with output:
        json.dump(result, output, ensure_ascii=False, indent=2, allow_nan=False)
        output.write("\n")
    print(f"{result['result']}; Gate NOT_EVALUATED; evidence: {args.output}")
    return 0 if error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
