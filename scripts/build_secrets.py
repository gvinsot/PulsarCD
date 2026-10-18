#!/usr/bin/env python3
"""BuildKit build secrets from the stack's devops/.env.

Every variable ``BUILD_SECRET_<ID>`` of the environment (build-push.sh sources
devops/.env with ``set -a`` first) becomes a BuildKit secret with id ``<id>``
(lowercase), readable in a Dockerfile with
``RUN --mount=type=secret,id=<id>`` (file ``/run/secrets/<id>``):

- ``BUILD_SECRET_<ID>=value``: the value, as text;
- ``BUILD_SECRET_<ID>_B64=...``: base64, decoded: binary files such as an
  Android keystore, or multi-line text (a .env value is a single line).

Each secret is written to a 0600 file in OUT_DIR, and a compose override that
declares them for every service with a ``build`` section is written to
OVERRIDE. The override is used for the build only (``docker compose -f
<compose> -f <override> build`` / ``docker buildx bake``), never by
``docker stack deploy``, so build secrets never become runtime secrets and
never reach an image layer. Values are never printed; only the ids are.

usage: build_secrets.py COMPOSE OUT_DIR OVERRIDE
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
_ID = re.compile(r"^[a-z0-9][a-z0-9_]*$")


def collect(environ: dict[str, str]) -> dict[str, bytes]:
    """Secret id -> content, from the BUILD_SECRET_* variables."""
    secrets: dict[str, bytes] = {}
    for name in sorted(environ):
        if not name.startswith(PREFIX) or not environ[name]:
            continue
        key = name[len(PREFIX):]
        value = environ[name]
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
    if len(argv) != 4:
        print("usage: build_secrets.py COMPOSE OUT_DIR OVERRIDE", file=sys.stderr)
        return 2
    compose, out_dir, override = argv[1:]
    try:
        secrets = collect(dict(os.environ))
    except ValueError as exc:
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
