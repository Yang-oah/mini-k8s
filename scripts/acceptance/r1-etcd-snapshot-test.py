#!/usr/bin/env python3
"""In-memory tests; never contact an etcd instance."""

import argparse
import importlib.util
import io
import json
import threading
import unittest
import urllib.error
from pathlib import Path


spec = importlib.util.spec_from_file_location("etcd_snapshot", Path(__file__).with_name("r1-etcd-snapshot.py"))
snapshot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(snapshot)


class Response:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, size):
        return self.body[:size]


class Opener:
    def __init__(self, body):
        self.body = body

    def open(self, request, timeout):
        if request.get_method() != "GET":
            raise AssertionError("expected GET")
        return Response(self.body)


class SnapshotTest(unittest.TestCase):
    def test_url_must_be_numeric_loopback_and_exact_path(self):
        for value in ("http://127.0.0.1:2379/metrics", "https://[::1]:2379/metrics"):
            self.assertEqual(snapshot.metrics_url(value), value)
        for value in ("http://10.0.0.1:2379/metrics", "http://localhost:2379/metrics",
                      "http://127.0.0.1:2379/metrics?q=secret", "http://u:p@127.0.0.1:2379/metrics",
                      "http://127.0.0.1:2379/other", "http://127.0.0.1/metrics", "ftp://127.0.0.1:2379/metrics"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                snapshot.metrics_url(value)

    def test_allowlist_and_ambiguous_labels(self):
        values = snapshot.parse_metrics(b'process_cpu_seconds_total 12.5\nprocess_resident_memory_bytes{secret="hidden"} 12\nprivate_metric 99\n')
        self.assertEqual(values["process_cpu_seconds_total"], {"status": "present", "value": 12.5})
        self.assertEqual(values["process_resident_memory_bytes"]["status"], "labelled_only")
        self.assertEqual(values["etcd_server_has_leader"]["status"], "missing")
        self.assertNotIn("secret", json.dumps(values))
        self.assertNotIn("private_metric", values)

    def test_duplicate_and_nonfinite_values_not_success(self):
        data = b'etcd_server_has_leader 1\netcd_server_has_leader 0\nprocess_cpu_seconds_total NaN\n'
        values = snapshot.parse_metrics(data)
        self.assertEqual(values["etcd_server_has_leader"]["status"], "ambiguous")
        self.assertEqual(values["process_cpu_seconds_total"]["status"], "missing")

    def test_errors_and_size_never_write_raw_content(self):
        metrics, error = snapshot.fetch(Opener(b'x' * (snapshot.MAX_BYTES + 1)), "http://127.0.0.1:2379/metrics", 1)
        self.assertIsNone(metrics)
        self.assertEqual(error, "response_too_large")
        class Failed:
            def open(self, request, timeout):
                raise urllib.error.HTTPError(request.full_url, 302, "redirect", {}, io.BytesIO(b"secret"))
        metrics, error = snapshot.fetch(Failed(), "http://127.0.0.1:2379/metrics", 1)
        self.assertIsNone(metrics)
        self.assertEqual(error, "http_error")
        self.assertIsNone(snapshot.NoRedirect().redirect_request(None, None, 302, "", {}, "http://evil/"))

    def test_wall_clock_deadline(self):
        blocked = threading.Event()
        class Slow:
            def open(self, request, timeout):
                blocked.wait(1)
                return Response(b"")
        metrics, error = snapshot.fetch_with_deadline(Slow(), "http://127.0.0.1:2379/metrics", 0.01)
        blocked.set()
        self.assertIsNone(metrics)
        self.assertEqual(error, "timeout")


if __name__ == "__main__":
    unittest.main()
