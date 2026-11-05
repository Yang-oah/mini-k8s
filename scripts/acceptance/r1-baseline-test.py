#!/usr/bin/env python3
"""Isolated tests for request handling; no server or real credentials are used."""

import importlib.util
import http.client
import io
from pathlib import Path
import unittest
import urllib.error


spec = importlib.util.spec_from_file_location("r1_baseline", Path(__file__).with_name("r1-baseline.py"))
baseline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(baseline)


class Response:
    status = 200

    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self, maximum):
        return self.body[:maximum]


class Opener:
    def __init__(self, body):
        self.body = body
        self.request = None

    def open(self, request, timeout):
        self.request = request
        return Response(self.body)


class BaselineTest(unittest.TestCase):
    def test_success_saves_count_only(self):
        opener = Opener(b'{"items":[{"spec":{"env":[{"value":"sensitive"}]}}]}')
        result = baseline.collect_one(opener, "https://example.test", "token-secret", "pods", 1)
        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["items"], 1)
        self.assertNotIn("sensitive", str(result))
        self.assertNotIn("token-secret", str(result))
        self.assertEqual(opener.request.get_method(), "GET")

    def test_rejects_redirect_and_http_body(self):
        class Failed:
            def open(self, request, timeout):
                raise urllib.error.HTTPError(request.full_url, 302, "redirect", {}, io.BytesIO(b"secret"))

        result = baseline.collect_one(Failed(), "https://example.test", "token-secret", "pods", 1)
        self.assertEqual((result["status"], result["error"]), (302, "http_error"))
        self.assertNotIn("secret", str(result))
        self.assertIsNone(baseline.NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.test"))

    def test_malformed_and_oversized_responses_fail(self):
        for body in (b'{}', b'not json', b'x' * (baseline.MAX_RESPONSE_BYTES + 1)):
            self.assertEqual(baseline.collect_one(Opener(body), "https://example.test", "x", "pods", 1)["result"], "error")

    def test_incomplete_http_response_is_recorded_as_error(self):
        class Interrupted:
            def open(self, request, timeout):
                raise http.client.IncompleteRead(b"partial secret")

        result = baseline.collect_one(Interrupted(), "https://example.test", "x", "pods", 1)
        self.assertEqual(result["error"], "connection_error")
        self.assertNotIn("partial secret", str(result))

    def test_url_and_limits(self):
        for url in ("http://example.test", "https://user:secret@example.test", "https://example.test/path", "https://example.test?token=x"):
            with self.assertRaises(Exception):
                baseline.server_url(url)
        self.assertEqual(baseline.server_url("https://example.test/"), "https://example.test")
        with self.assertRaises(Exception):
            baseline.bounded_int(1, 32)("33")

    def test_summary_excludes_errors_from_latency_percentiles(self):
        result = baseline.summarize([
            {"result": "ok", "status": 200, "error": None, "latency_ms": 10},
            {"result": "error", "status": 503, "error": "http_error", "latency_ms": 20},
        ], 1)
        self.assertEqual(result["latency_ms_success_only"]["p95"], 10)
        self.assertEqual(result["error_counts"], {"http_error": 1})


if __name__ == "__main__":
    unittest.main()
