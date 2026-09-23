"""Exercise the registry preflight against a local HTTP server (no Docker)."""

import base64
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "registry_tag_exists.py"
spec = importlib.util.spec_from_file_location("registry_tag_exists", SCRIPT)
registry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(registry)


class RegistryTagTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = patch.dict(os.environ, DOCKER_CONFIG=self.temp.name)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.requests = []
        self.respond = lambda path, headers: (200, {"name": "team/app", "tags": []}, {})
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append((self.path, dict(self.headers)))
                status, data, headers = owner.respond(self.path, self.headers)
                payload = data if isinstance(data, bytes) else json.dumps(data).encode()
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.01), daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.host = "127.0.0.1:" + str(self.server.server_port)
        self.url = "http://" + self.host

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def config(self, data):
        (Path(self.temp.name) / "config.json").write_text(json.dumps(data))

    def probe(self, tag="1.0.86"):
        return registry.tag_exists(self.url, "team/app", tag)

    def require_auth(self):
        def respond(path, headers):
            if not headers.get("Authorization"):
                return 401, {}, {"WWW-Authenticate": 'Basic realm="registry"'}
            return 200, {"name": "team/app", "tags": []}, {}
        self.respond = respond

    def test_absent_present_and_null_tags(self):
        for tags, expected in (([], False), (None, False), (["1.0.85"], False), (["1.0.86"], True)):
            with self.subTest(tags=tags):
                self.respond = lambda path, headers: (200, {"name": "team/app", "tags": tags}, {})
                self.assertIs(self.probe(), expected)
        self.assertTrue(all(path == "/v2/team/app/tags/list?n=1000" for path, _ in self.requests))

    def test_follows_pagination_before_reporting_absence(self):
        self.respond = lambda path, headers: (
            (200, {"name": "team/app", "tags": ["1.0.85"]},
             {"Link": '</v2/team/app/tags/list?n=1000&last=1.0.85>; rel="next"'})
            if "last=" not in path else
            (200, {"name": "team/app", "tags": ["1.0.86"]}, {})
        )
        self.assertTrue(self.probe())
        self.assertEqual(len(self.requests), 2)
        self.assertFalse(self.probe("1.0.87"))
        self.assertEqual(len(self.requests), 4)

    def test_missing_page_is_inconclusive(self):
        self.respond = lambda path, headers: (
            (200, {"name": "team/app", "tags": []},
             {"Link": '</v2/team/app/tags/list?last=next>; rel="next"'})
            if "last=" not in path else (500, {}, {})
        )
        self.assertIsNone(self.probe())

    def test_invalid_responses_never_claim_absence(self):
        for status, data in ((401, {}), (403, {}), (404, {}), (500, {}),
                             (200, b"not json"), (200, {}), (200, []),
                             (200, {"name": "other", "tags": []}),
                             (200, {"name": "team/app", "tags": "bad"}),
                             (200, {"name": "team/app", "tags": [7]})):
            with self.subTest(status=status, data=data):
                self.respond = lambda path, headers: (status, data, {})
                self.assertIsNone(self.probe())

    def test_unsafe_and_malformed_pagination_never_follows_link(self):
        for link in ('<https://example.invalid/tags>; rel="next"',
                     '</v2/other/tags/list>; rel="next"',
                     '</v2/team/app/tags/list?n=1000>; rel="next"',
                     '</v2/team/app/tags/list?last=a>; rel="prev"',
                     'not a link'):
            with self.subTest(link=link):
                self.requests.clear()
                self.respond = lambda path, headers: (200, {"name": "team/app", "tags": []}, {"Link": link})
                self.assertIsNone(self.probe())
                self.assertEqual(len(self.requests), 1)

    def test_reads_basic_auth_only_after_challenge(self):
        encoded = base64.b64encode(b"user:private-password").decode()
        self.config({"auths": {self.host: {"auth": encoded}}})
        self.require_auth()
        self.assertFalse(self.probe())
        self.assertNotIn("Authorization", self.requests[0][1])
        self.assertEqual(self.requests[1][1]["Authorization"], "Basic " + encoded)

    def test_credentials_helpers_and_store_keep_secret_off_arguments(self):
        for config in ({"credHelpers": {self.host: "test"}, "credsStore": "other"},
                       {"credsStore": "test"}):
            with self.subTest(config=config):
                self.requests.clear()
                self.config(config)
                self.require_auth()
                completed = subprocess.CompletedProcess([], 0, json.dumps({"Username": "user", "Secret": "private-password"}))
                with patch.object(registry.subprocess, "run", return_value=completed) as run:
                    self.assertFalse(self.probe())
                self.assertEqual(run.call_args.args[0], ["docker-credential-test", "get"])
                self.assertEqual(run.call_args.kwargs["input"], self.host + "\n")
                self.assertNotIn("private-password", str(run.call_args))
                self.assertIn("Authorization", self.requests[1][1])

    def test_credential_helper_failure_and_identity_tokens_fall_back(self):
        self.config({"credsStore": "test"})
        self.require_auth()
        with patch.object(registry.subprocess, "run", side_effect=FileNotFoundError):
            self.assertIsNone(self.probe())
        completed = subprocess.CompletedProcess([], 0, json.dumps({"Username": "<token>", "Secret": "token"}))
        with patch.object(registry.subprocess, "run", return_value=completed):
            self.assertIsNone(self.probe())

    def test_bearer_challenge_uses_docker_fallback(self):
        self.respond = lambda path, headers: (401, {}, {"WWW-Authenticate": 'Bearer realm="https://auth.invalid"'})
        with patch.object(registry.subprocess, "run") as run:
            self.assertIsNone(self.probe())
            run.assert_not_called()

    def test_redirect_is_not_followed_with_credentials(self):
        self.config({"auths": {self.host: {"auth": base64.b64encode(b"user:password").decode()}}})
        self.respond = lambda path, headers: (
            (401, {}, {"WWW-Authenticate": 'Basic realm="registry"'})
            if not headers.get("Authorization") else
            (302, {}, {"Location": self.url + "/should-not-be-followed"})
        )
        self.assertIsNone(self.probe())
        self.assertEqual(len(self.requests), 2)

    def test_cross_origin_pagination_does_not_forward_credentials(self):
        self.config({"auths": {self.host: {"auth": base64.b64encode(b"user:password").decode()}}})
        self.respond = lambda path, headers: (
            (401, {}, {"WWW-Authenticate": 'Basic realm="registry"'})
            if not headers.get("Authorization") else
            (200, {"name": "team/app", "tags": []},
             {"Link": '<http://example.invalid/v2/team/app/tags/list?last=a>; rel="next"'})
        )
        self.assertIsNone(self.probe())
        self.assertEqual(len(self.requests), 2)

    def test_cli_exit_codes_and_registry_ports(self):
        for result, status in ((True, 0), (False, 3), (None, 2)):
            with patch.object(registry.sys, "argv", ["check", "registry.test:5000/team/app:1.0.86"]), \
                    patch.object(registry, "tag_exists", return_value=result) as probe:
                self.assertEqual(registry.main(), status)
                probe.assert_called_once_with("https://registry.test:5000", "team/app", "1.0.86")

    def test_incomplete_http_response_is_inconclusive(self):
        with patch.object(registry, "build_opener") as build:
            build.return_value.open.side_effect = IncompleteRead(b"{", 100)
            self.assertIsNone(self.probe())


if __name__ == "__main__":
    unittest.main()
