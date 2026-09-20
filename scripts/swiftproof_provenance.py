"""Trusted build/deploy provenance for SwiftProof; never imported from a candidate repo."""
import argparse
import hashlib
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


def pin(guard_path, compose_path, stack):
    import yaml
    guard_file = Path(guard_path).resolve()
    guard_file.relative_to((root() / "guards").resolve())
    guard = json.loads(guard_file.read_text(encoding="utf-8"))
    if run("git", "rev-parse", "HEAD") != guard["head"]:
        raise ValueError("Compose commit does not match the reviewed commit")
    current = root() / "deployed" / (guard["repo"] + ".json")
    deployed_hash = hashlib.sha256(current.read_bytes()).hexdigest() if current.exists() else None
    if deployed_hash != guard["deployed_hash"]:
        raise ValueError("Production baseline changed since review")
    compose_file = Path(compose_path)
    compose = yaml.safe_load(compose_file.read_text(encoding="utf-8"))
    mapping = dict(guard["images"])
    by_base = {}
    for source, digest in guard["images"].items():
        base = source.rsplit(":", 1)[0]
        by_base.setdefault(base, []).append((source, digest))
    for base, variants in by_base.items():
        if len(variants) == 1:
            mapping[base + ":" + guard["release"]] = variants[0][1]
        else:
            for source, digest in variants:
                mapping[source + "-" + guard["release"]] = digest
    services = {}
    for name, service in compose["services"].items():
        image = service.get("image", "")
        if image in mapping:
            image = mapping[image]
        elif not re.fullmatch(r"[^\s@$]+@sha256:[0-9a-f]{64}", image):
            raise ValueError("Every deployed image needs build provenance or an explicit digest")
        service["image"] = image
        services[stack + "_" + name] = image
    compose_file.write_text(yaml.safe_dump(compose, sort_keys=False), encoding="utf-8")
    guard["services"] = services
    atomic_json(guard_file, guard)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("record")
    for field in ("repo", "version", "commit"):
        build.add_argument(field)
    build.add_argument("--reuse", action="store_true")
    build.add_argument("images", nargs="+")
    deploy = commands.add_parser("pin")
    for field in ("guard", "compose", "stack"):
        deploy.add_argument(field)
    args = parser.parse_args()
    if args.command == "record":
        record(args.repo, args.version, args.commit, args.images, args.reuse)
    else:
        pin(args.guard, args.compose, args.stack)
