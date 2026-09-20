"""Trusted build provenance for SwiftProof; never imported from a candidate repo."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


def run(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE, timeout=120).strip()


def root():
    return Path.home() / ".local/share/pulsarcd/swiftproof"


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, sort_keys=True)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def record(repo, version, commit, images, reuse=False):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", repo) or repo in (".", ".."):
        raise ValueError("Invalid repository")
    if not re.fullmatch(r"\d+\.\d+\.\d+", version) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Exact version and full commit required")
    if not images:
        raise ValueError("No images to record")
    mapping = {}
    for item in images:
        source, image = item.split("=", 1) if "=" in item else (item, item)
        manifest = json.loads(run("docker", "buildx", "imagetools", "inspect", "--format", "{{json .Manifest}}", image))
        digest = manifest.get("digest", "")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("Registry did not return an immutable image digest")
        mapping[source] = image.rsplit(":", 1)[0] + "@" + digest
    result = {"version": 1, "repo": repo, "release": version, "commit": commit, "images": mapping}
    path = root() / "builds" / repo / (version + ".json")
    if reuse:
        previous = json.loads(path.read_text(encoding="utf-8"))
        if previous != result:
            raise ValueError("Reused images have no matching build provenance; rebuild with --no-cache")
    else:
        atomic_json(path, result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("record")
    for field in ("repo", "version", "commit"):
        build.add_argument(field)
    build.add_argument("--reuse", action="store_true")
    build.add_argument("images", nargs="+")
    args = parser.parse_args()
    record(args.repo, args.version, args.commit, args.images, args.reuse)
