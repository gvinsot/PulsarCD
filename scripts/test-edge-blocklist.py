#!/usr/bin/env python3
"""Validate real Traefik HTTP/HTTPS blocklist enforcement and removal.

Run ``python scripts/test-edge-blocklist.py`` with the backend Python dependencies
installed and Docker Desktop running Linux containers. Only the explicit local
``desktop-linux`` Docker context is used. An internal network and four temporary
containers are removed on completion; no ports are exposed and no production API
is called. The test image defaults to the deployed Traefik version.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "backend"))
from ip_blocklist import traefik_config  # noqa: E402


CLIENT_CODE = """
import http.client
import json
import ssl

result = {}
for protocol, port in [('http', 8080), ('https', 8443)]:
    if protocol == 'http':
        connection = http.client.HTTPConnection('edge', port, timeout=5)
    else:
        # The isolated test edge uses Traefik's default self-signed certificate.
        connection = http.client.HTTPSConnection(
            'edge', port, timeout=5, context=ssl._create_unverified_context()
        )
    connection.request('GET', '/legitimate-page', headers={'Host': 'blocklist.test'})
    response = connection.getresponse()
    response.read()
    result[protocol] = response.status
    connection.close()
print(json.dumps(result))
"""


def docker(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", "--context", "desktop-linux", *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if check and result.returncode:
        raise RuntimeError(result.stdout.strip())
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="traefik:v3.7.6")
    args = parser.parse_args()
    network = "pulsarcd-blocklist-test-" + uuid.uuid4().hex[:12]
    containers = []
    edge = None

    def start(suffix: str, *arguments: str) -> str:
        name = f"{network}-{suffix}"
        # Include failed starts in cleanup: docker may have created the container.
        containers.append(name)
        docker("run", "--detach", "--name", name, "--network", network, *arguments)
        return name

    def client_ip(name: str) -> str:
        inspected = json.loads(docker("inspect", name))[0]
        return inspected["NetworkSettings"]["Networks"][network]["IPAddress"]

    def check_stage(label: str, client_a: str, client_b: str, status_a: int) -> None:
        expected_a = {"http": status_a, "https": status_a}
        expected_b = {"http": 200, "https": 200}
        deadline = time.monotonic() + 30
        while True:
            try:
                actual_a = json.loads(docker("exec", client_a, "python", "-c", CLIENT_CODE))
                actual_b = json.loads(docker("exec", client_b, "python", "-c", CLIENT_CODE))
                if actual_a == expected_a and actual_b == expected_b:
                    print(f"PASS {label}: client A {actual_a}; client B {actual_b}", flush=True)
                    return
                detail = f"client A {actual_a}, expected {expected_a}; client B {actual_b}, expected {expected_b}"
            except (RuntimeError, json.JSONDecodeError) as error:
                detail = str(error)
            if time.monotonic() > deadline:
                raise AssertionError(f"{label}: {detail}")
            time.sleep(0.5)

    print(f"Runtime: {args.image}; Docker context desktop-linux", flush=True)
    with tempfile.TemporaryDirectory(prefix=network) as directory:
        temp = Path(directory)
        try:
            docker("network", "create", "--internal", network)
            client_a = start("client-a", "python:3.12-slim", "python", "-c", "import time; time.sleep(300)")
            client_b = start("client-b", "python:3.12-slim", "python", "-c", "import time; time.sleep(300)")
            ip_a, ip_b = client_ip(client_a), client_ip(client_b)
            print(f"Isolated sources: client A {ip_a}; client B {ip_b}", flush=True)

            def publish(targets: list[str]) -> None:
                # Exercise the actual pure generator. Private addresses are used
                # only inside this test network; no blocklist API is involved.
                staged = temp / "dynamic-next.json"
                staged.write_text(json.dumps(traefik_config(targets)), encoding="utf-8")
                staged.replace(temp / "dynamic.json")

            publish([])
            base = {"http": {"routers": {
                "test-http": {
                    "rule": "Host(`blocklist.test`)",
                    "entryPoints": ["web"], "service": "ping@internal",
                },
                "test-https": {
                    "rule": "Host(`blocklist.test`)",
                    "entryPoints": ["websecure"], "service": "ping@internal", "tls": {},
                },
            }}}
            static = {
                "global": {"checkNewVersion": False, "sendAnonymousUsage": False},
                "entryPoints": {"web": {"address": ":8080"}, "websecure": {"address": ":8443"}},
                "ping": {"manualRouting": True},
                "providers": {
                    "providersThrottleDuration": "100ms",
                    "file": {"filename": "/test-config/base.yml"},
                    "http": {
                        "endpoint": "http://configserver:8000/dynamic.json",
                        "pollInterval": "500ms", "pollTimeout": "5s",
                    },
                },
                "log": {"level": "INFO", "format": "json"},
            }
            # JSON is valid YAML, avoiding an additional test dependency.
            (temp / "base.yml").write_text(json.dumps(base), encoding="utf-8")
            (temp / "static.yml").write_text(json.dumps(static), encoding="utf-8")
            mount = f"type=bind,source={temp},target=/test-config,readonly"
            start(
                "config", "--network-alias", "configserver", "--mount", mount,
                "python:3.12-slim", "python", "-m", "http.server", "8000", "--directory", "/test-config",
            )
            edge = start(
                "edge", "--network-alias", "edge", "--mount", mount,
                args.image, "--configFile=/test-config/static.yml",
            )
            check_stage("initial empty list", client_a, client_b, 200)
            publish([ip_a])
            check_stage("ban active", client_a, client_b, 403)
            publish([])
            check_stage("ban removed", client_a, client_b, 200)

            errors = []
            for line in docker("logs", edge).splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("level") in ("error", "fatal", "panic"):
                    errors.append(line)
            if errors:
                raise AssertionError("Traefik configuration/runtime errors:\n" + "\n".join(errors))
            print("PASS: 12 HTTP/HTTPS checks; zero Traefik errors.", flush=True)
        except Exception:
            if edge is not None:
                print(docker("logs", edge, check=False))
            raise
        finally:
            for name in reversed(containers):
                docker("rm", "--force", name, check=False)
            docker("network", "rm", network, check=False)
            print("Temporary containers and network removed.", flush=True)


if __name__ == "__main__":
    main()
