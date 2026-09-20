"""Standalone trusted worker streamed over SSH; only its JSON stdin carries job data.

Keep this module standard-library-only: the deployment host needs Python and the
installed SwiftProof binary, not PulsarCD's Python environment.
"""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import zipfile

LIMIT = 8 * 1024 * 1024


def command(*args, timeout=120, env=None):
    return subprocess.check_output(args, stderr=subprocess.PIPE, timeout=timeout, env=env).decode().strip()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def live_services(stack):
    ids = command("docker", "stack", "services", "--quiet", stack).split()
    if not ids:
        return {}
    specs = json.loads(command("docker", "service", "inspect", *ids))
    return {s["Spec"]["Name"]: s["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"] for s in specs}


def same_images(actual, expected):
    # Swarm may preserve the original tag before @sha256; digest and repository
    # still identify the exact image. Compare services as well as digest values.
    def normalized(image):
        name, sha = image.rsplit("@", 1)
        leaf = name.rsplit("/", 1)[-1]
        if ":" in leaf:
            name = name.rsplit(":", 1)[0]
        return name, sha
    try:
        return {k: normalized(v) for k, v in actual.items()} == {k: normalized(v) for k, v in expected.items()}
    except ValueError:
        return False


def inspect(request):
    repo, version = request["repo"], request["release"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", repo) or repo in (".", ".."):
        raise ValueError("Invalid repository")
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("SwiftProof requires an exact release version")
    root = Path.home() / ".local/share/pulsarcd/swiftproof"
    checkout = Path(request["repos_path"]).expanduser() / repo
    build = json.loads((root / "builds" / repo / (version + ".json")).read_text())
    if build.get("repo") != repo or build.get("release") != version or not build.get("images"):
        raise ValueError("Missing build provenance")
    head = command("git", "-C", str(checkout), "rev-parse", "--verify", "refs/tags/v" + version + "^{commit}")
    if head != build.get("commit") or not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("Release tag does not match the built commit")
    deployed_file = root / "deployed" / (repo + ".json")
    deployed = json.loads(deployed_file.read_text()) if deployed_file.exists() else None
    base = deployed["head"] if deployed else request.get("initial_baseline", "")
    if not re.fullmatch(r"[0-9a-f]{40}", base):
        raise ValueError("Set the verified production commit as initial_baseline before the first review")
    if deployed and request["action"] != "deployed" and not same_images(live_services(request["stack"]), deployed["services"]):
        raise ValueError("Live production images no longer match the recorded baseline")
    policy = command("git", "-C", str(checkout), "show", base + ":.swiftproof.json")
    binary = Path(request["binary"]).expanduser().resolve(strict=True)
    identity = {
        "repo": repo, "release": version, "base": base, "head": head,
        "images": build["images"], "binary_sha256": digest(binary.read_bytes()),
        "policy_sha256": digest(policy.encode()),
        "deployed_hash": digest(deployed_file.read_bytes()) if deployed else None,
    }
    return root, checkout, binary, policy, identity


def execute(request):
    root, checkout, binary, policy, identity = inspect(request)
    action = request["action"]
    if action == "inspect":
        return identity
    if request.get("identity") != identity:
        raise ValueError("Build, baseline, binary or policy changed since review")
    if action == "guard":
        key = request["id"]
        if not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError("Invalid review ID")
        path = root / "guards" / (key + ".json")
        save(path, identity)
        return {"path": str(path)}
    if action == "deployed":
        key = request["id"]
        if not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError("Invalid review ID")
        guard = json.loads((root / "guards" / (key + ".json")).read_text())
        if any(guard.get(k) != v for k, v in identity.items()):
            raise ValueError("Deployment guard changed")
        if not same_images(live_services(request["stack"]), guard["services"]):
            raise ValueError("Deployed images do not match reviewed digests")
        save(root / "deployed" / (identity["repo"] + ".json"), guard)
        return {"recorded": True}
    if action != "review":
        raise ValueError("Unknown action")
    with tempfile.TemporaryDirectory(prefix="pulsarcd-swiftproof-") as temporary:
        work = Path(temporary)
        cfg = json.loads(policy)
        reviewer = cfg.setdefault("reviewer", {})
        # The provider is inherited from PulsarCD; candidate text cannot select
        # an endpoint, model, key or bigger investigation budget.
        provider = request.get("provider")
        reviewer.update(endpoint=provider["endpoint"] if provider else "https://unused.invalid/v1",
                        model=provider["model"] if provider else "", api_key_env="SWIFTPROOF_JOB_TOKEN")
        reviewer["max_iterations"] = min(reviewer.get("max_iterations", 20), 20)
        reviewer["max_generated_tests"] = min(reviewer.get("max_generated_tests", 10), 10)
        reviewer["timeout_seconds"] = min(reviewer.get("timeout_seconds", 600), 600)
        reviewer["max_input_bytes"] = min(reviewer.get("max_input_bytes", 131072), 131072)
        config_file = work / "policy.json"
        config_file.write_text(json.dumps(cfg), encoding="utf-8")
        output = work / "report"
        env = {k: v for k, v in os.environ.items() if k.upper() in ("PATH", "HOME", "TMPDIR", "LANG", "DOCKER_HOST", "DOCKER_CONTEXT", "SYSTEMROOT", "WINDIR", "TEMP", "TMP")}
        if provider:
            env["SWIFTPROOF_JOB_TOKEN"] = provider["token"]
        args = [str(binary), "review", "--repo", str(checkout), "--base", identity["base"],
                "--head", identity["head"], "--exact", "--ci", "--config", str(config_file),
                "--reviewer=" + ("true" if provider else "false"), "--out", str(output)]
        with open(work / "run.log", "wb") as log:
            process = subprocess.run(args, stdout=log, stderr=log, env=env, timeout=1800)
        report_file = output / "confidence-report.json"
        if not report_file.is_file() or report_file.stat().st_size > LIMIT:
            raise ValueError("SwiftProof did not produce a bounded report; check binary, policy and sandbox image")
        report = json.loads(report_file.read_text(encoding="utf-8"))
        if (report.get("version") != 1 or report.get("exit_code") != process.returncode
                or report.get("change", {}).get("head_commit") != identity["head"]
                or report.get("change", {}).get("base_commit") != identity["base"]):
            raise ValueError("Report does not match the executed comparison")
        buffer = io.BytesIO()
        total = 0
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in output.rglob("*"):
                if path.is_symlink():
                    raise ValueError("Report artifacts must not contain symlinks")
                if path.is_file():
                    total += path.stat().st_size
                    if total > LIMIT:
                        raise ValueError("Report artifacts exceed the 8 MiB transfer budget")
                    archive.write(path, path.relative_to(output).as_posix())
        return {"identity": identity, "code": process.returncode, "tool_version": report["tool_version"],
                "archive": base64.b64encode(buffer.getvalue()).decode()}


if __name__ == "__main__":
    try:
        request = json.loads(sys.stdin.read(1024 * 1024))
        print(json.dumps({"ok": True, "result": execute(request)}))
    except Exception as error:
        # Never return command stdout/stderr or credentials from a provider.
        message = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(json.dumps({"ok": False, "error": message}))
        sys.exit(1)
