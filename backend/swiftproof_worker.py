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


def command(*args, timeout=120, env=None, failure=None):
    """Report what the administrator must fix; never the command's own stderr."""
    try:
        return subprocess.check_output(args, stderr=subprocess.PIPE, timeout=timeout, env=env).decode().strip()
    except subprocess.CalledProcessError:
        raise ValueError(failure or args[0] + " failed on the deployment host") from None
    except subprocess.TimeoutExpired:
        raise ValueError(args[0] + " timed out on the deployment host") from None


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
    unreadable = "Cannot read the live services of stack " + stack + " on the deployment host"
    ids = command("docker", "stack", "services", "--quiet", stack, failure=unreadable).split()
    if not ids:
        return {}
    try:
        specs = json.loads(command("docker", "service", "inspect", *ids, failure=unreadable))
        return {s["Spec"]["Name"]: s["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"] for s in specs}
    except (ValueError, KeyError, TypeError):
        raise ValueError(unreadable) from None


def released_commit(root, repo, stack, candidate):
    """Derive the production commit from the images Swarm actually runs.

    PulsarCD no longer records deployments: a deploy is just a deploy. The
    baseline is whichever build provenance explains the live production images.
    A release tag identifies a build exactly, so it outranks a digest match,
    which two builds can share when images were reused. The candidate release
    is never its own baseline. On a tie the lowest release wins: reviewing too
    large a diff is recoverable, reviewing too small a one is not.
    """
    builds = root / "builds" / repo
    if not builds.is_dir():
        return None
    tags, digests = set(), set()
    for image in live_services(stack).values():
        name, _, sha = image.partition("@")
        if sha:
            digests.add(sha)
        leaf = name.rsplit("/", 1)[-1]
        if ":" in leaf:
            tags.add(name)
    best = None
    for path in sorted(builds.glob("*.json")):
        try:
            build = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        release = build.get("release", "")
        if (build.get("repo") != repo or release == candidate
                or not re.fullmatch(r"\d+\.\d+\.\d+", release)
                or not re.fullmatch(r"[0-9a-f]{40}", build.get("commit", ""))):
            continue
        by_tag = by_digest = 0
        for source, pinned in build.get("images", {}).items():
            by_tag += source.rsplit(":", 1)[0] + ":" + release in tags
            by_digest += pinned.rsplit("@", 1)[-1] in digests
        if not by_tag and not by_digest:
            continue
        rank = (by_tag, by_digest, tuple(-int(part) for part in release.split(".")))
        if best is None or rank > best[0]:
            best = (rank, build["commit"])
    return best[1] if best else None


def inspect(request):
    repo, version = request["repo"], request["release"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", repo) or repo in (".", ".."):
        raise ValueError("Invalid repository")
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("SwiftProof requires an exact release version")
    root = Path.home() / ".local/share/pulsarcd/swiftproof"
    checkout = Path(request["repos_path"]).expanduser() / repo
    provenance = root / "builds" / repo / (version + ".json")
    if not provenance.is_file():
        raise ValueError("No build provenance for " + repo + " " + version
                         + ": rebuild this release with the current PulsarCD scripts")
    build = json.loads(provenance.read_text())
    if build.get("repo") != repo or build.get("release") != version or not build.get("images"):
        raise ValueError("Missing build provenance")
    head = command("git", "-C", str(checkout), "rev-parse", "--verify", "refs/tags/v" + version + "^{commit}",
                   failure="Tag v" + version + " is missing from the " + repo + " checkout on the deployment host")
    if head != build.get("commit") or not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("Release tag does not match the built commit")
    base = released_commit(root, repo, request["stack"], version) or request.get("initial_baseline", "")
    if not re.fullmatch(r"[0-9a-f]{40}", base):
        raise ValueError("No build provenance matches the images running in production: set the verified"
                         " production commit as the initial baseline for " + repo)
    policy = command("git", "-C", str(checkout), "show", base + ":.swiftproof.json",
                     failure="No .swiftproof.json in the baseline commit " + base[:12] + " of " + repo
                             + ": add the policy to the project, deploy it, then set that commit as the initial baseline")
    try:
        binary = Path(request["binary"]).expanduser().resolve(strict=True)
    except OSError:
        raise ValueError("No SwiftProof binary at " + str(request["binary"])
                         + " on the deployment host: run scripts/install-swiftproof.sh there") from None
    identity = {
        "repo": repo, "release": version, "base": base, "head": head,
        "images": build["images"], "binary_sha256": digest(binary.read_bytes()),
        "policy_sha256": digest(policy.encode()),
    }
    return checkout, binary, policy, identity


def execute(request):
    checkout, binary, policy, identity = inspect(request)
    action = request["action"]
    if action == "inspect":
        return identity
    if request.get("identity") != identity:
        raise ValueError("Build, baseline, binary or policy changed since review")
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
            try:
                process = subprocess.run(args, stdout=log, stderr=log, env=env, timeout=1800)
            except subprocess.TimeoutExpired:
                raise ValueError("SwiftProof did not finish within 30 minutes") from None
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
            # The binary's own console output is what explains a configuration
            # or execution failure that the report can only point at. Keep a
            # bounded tail so a noisy run cannot crowd out the evidence.
            tail = (work / "run.log").read_bytes()[-65536:]
            if total + len(tail) <= LIMIT:
                archive.writestr("pulsarcd-run.log", tail)
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
