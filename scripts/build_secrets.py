#!/usr/bin/env python3
"""BuildKit build secrets from the stack's devops/.env.build (and devops/.env).

Two sources, each secret readable in a Dockerfile with
``RUN --mount=type=secret,id=<id>`` (file ``/run/secrets/<id>``):

- ENV_FILE, the stack's devops/.env.build (the "Build" tab of the .env
  editor). It is parsed here, never sourced, so its values never reach the
  build's environment, compose interpolation or a log. Every ``<KEY>=value``
  becomes the secret ``<key>`` (lowercase), and the whole file the secret
  ``build_env``, to mount as a .env (``target=/src/.env``) where a build
  tool reads one.
- the environment: every variable ``BUILD_SECRET_<ID>`` (build-push.sh
  sources devops/.env with ``set -a`` first) becomes the secret ``<id>``.

In both, a ``_B64`` suffix (``<KEY>_B64=...``) means base64, decoded: binary
files such as an Android keystore, or multi-line text (a .env value is a
single line). An empty value defines no secret.

Each secret is written to a 0600 file in OUT_DIR, and a compose override that
declares them for every service with a ``build`` section is written to
OVERRIDE. The override is used for the build only (``docker compose -f
<compose> -f <override> build`` / ``docker buildx bake``), never by
``docker stack deploy``, so build secrets never become runtime secrets and
never reach an image layer. Values are never printed; only the ids are.

usage: build_secrets.py COMPOSE OUT_DIR OVERRIDE [ENV_FILE]
Prints the secret ids (comma separated) and writes nothing when there is none.
"""

from __future__ import annotations

import base64
import binascii
import os
import re
import sys

import yaml

PREFIX = "BUILD_SECRET_"
B64_SUFFIX = "_B64"
# Secret holding the whole devops/.env.build file.
FILE_SECRET = "build_env"
_ID = re.compile(r"^[a-z0-9][a-z0-9_]*$")
_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _add(secrets: dict[str, bytes], name: str, key: str, value: str) -> None:
    if key.endswith(B64_SUFFIX):
        key = key[: -len(B64_SUFFIX)]
        try:
            content = base64.b64decode("".join(value.split()), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"{name} is not valid base64") from exc
    else:
        content = value.encode("utf-8")
    sid = key.lower()
    if not _ID.match(sid):
        raise ValueError(f"{name}: invalid secret id {sid!r}")
    if sid in secrets:
        raise ValueError(f"build secret {sid!r} is defined twice")
    secrets[sid] = content


def collect(environ: dict[str, str], secrets: dict[str, bytes] | None = None) -> dict[str, bytes]:
    """Secret id -> content, from the BUILD_SECRET_* variables."""
    secrets = {} if secrets is None else secrets
    for name in sorted(environ):
        if not name.startswith(PREFIX) or not environ[name]:
            continue
        _add(secrets, name, name[len(PREFIX):], environ[name])
    return secrets


def parse_env_file(content: str) -> list[tuple[str, str]]:
    """``(key, value)`` assignments of a .env file, in order.

    ``KEY=value`` lines, with an optional ``export `` prefix; blank lines and
    ``#`` comments are skipped. Values are taken verbatim, without expansion
    or inline comments, once surrounding whitespace and one pair of matching
    quotes are removed. Errors name the line, never its value.
    """
    assignments = []
    for number, line in enumerate(content.splitlines(), 1):
        body = line.strip()
        if not body or body.startswith("#"):
            continue
        if body.startswith("export "):
            body = body[len("export "):].lstrip()
        key, sep, value = body.partition("=")
        key, value = key.strip(), value.strip()
        if not sep or not _KEY.match(key):
            raise ValueError(f".env.build line {number}: expected KEY=value")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        assignments.append((key, value))
    return assignments


def collect_file(path: str, secrets: dict[str, bytes] | None = None) -> dict[str, bytes]:
    """Secret id -> content, from devops/.env.build: one per key, plus the file."""
    secrets = {} if secrets is None else secrets
    with open(path, "rb") as f:
        raw = f.read()
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(".env.build is not UTF-8 text") from exc
    assignments = parse_env_file(content)
    for key, value in assignments:
        if value:
            _add(secrets, f".env.build {key}", key, value)
    if assignments:
        if FILE_SECRET in secrets:
            raise ValueError(f"build secret {FILE_SECRET!r} is reserved for the whole .env.build file")
        secrets[FILE_SECRET] = raw
    return secrets


def build_services(compose_path: str) -> list[str]:
    """Services of the compose file that have a build section."""
    with open(compose_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    services = data.get("services") or {}
    return [name for name, svc in services.items() if isinstance(svc, dict) and svc.get("build")]


def write(secrets: dict[str, bytes], services: list[str], out_dir: str, override: str) -> None:
    files = {}
    for sid, content in secrets.items():
        path = os.path.join(out_dir, sid)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(content)
        files[sid] = os.path.abspath(path)
    doc = {
        "services": {svc: {"build": {"secrets": sorted(secrets)}} for svc in services},
        "secrets": {sid: {"file": files[sid]} for sid in sorted(files)},
    }
    with open(override, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, sort_keys=False)


def main(argv: list[str]) -> int:
    if len(argv) not in (4, 5):
        print("usage: build_secrets.py COMPOSE OUT_DIR OVERRIDE [ENV_FILE]", file=sys.stderr)
        return 2
    compose, out_dir, override = argv[1:4]
    env_file = argv[4] if len(argv) == 5 else ""
    try:
        secrets = collect_file(env_file) if env_file else {}
        collect(dict(os.environ), secrets)
    except (OSError, ValueError) as exc:
        print(f"build secrets: {exc}", file=sys.stderr)
        return 1
    if not secrets:
        return 0
    services = build_services(compose)
    if services:
        write(secrets, services, out_dir, override)
    print(",".join(sorted(secrets)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
