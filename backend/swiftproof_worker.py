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
    if request.get("action") in ("plan", "prove"):
        return on_demand(request)
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
        provider = request.get("provider")
        output = work / "report"
        args = [str(binary), "review", "--repo", str(checkout), "--base", identity["base"],
                "--head", identity["head"], "--exact", "--ci", "--config", policy_file(work, policy, provider),
                "--reviewer=" + ("true" if provider else "false"), "--out", str(output)]
        code = run(args, work, provider)
        report = confidence_report(output, code, identity)
        return {"identity": identity, "code": code, "tool_version": report["tool_version"],
                "archive": archive(output, work)}


def policy_file(work, policy, provider):
    """Write the trusted policy with the provider and budgets PulsarCD imposes."""
    cfg = json.loads(policy)
    reviewer = cfg.setdefault("reviewer", {})
    # The provider is inherited from PulsarCD; candidate text cannot select
    # an endpoint, model, key or bigger investigation budget.
    reviewer.update(endpoint=provider["endpoint"] if provider else "https://unused.invalid/v1",
                    model=provider["model"] if provider else "", api_key_env="SWIFTPROOF_JOB_TOKEN")
    reviewer["max_iterations"] = min(reviewer.get("max_iterations", 20), 20)
    reviewer["max_generated_tests"] = min(reviewer.get("max_generated_tests", 10), 10)
    reviewer["timeout_seconds"] = min(reviewer.get("timeout_seconds", 600), 600)
    reviewer["max_input_bytes"] = min(reviewer.get("max_input_bytes", 131072), 131072)
    path = work / "policy.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return str(path)


def run(args, work, provider):
    env = {k: v for k, v in os.environ.items() if k.upper() in ("PATH", "HOME", "TMPDIR", "LANG", "DOCKER_HOST", "DOCKER_CONTEXT", "SYSTEMROOT", "WINDIR", "TEMP", "TMP")}
    if provider:
        env["SWIFTPROOF_JOB_TOKEN"] = provider["token"]
    with open(work / "run.log", "wb") as log:
        try:
            return subprocess.run(args, stdout=log, stderr=log, env=env, timeout=1800).returncode
        except subprocess.TimeoutExpired:
            raise ValueError("SwiftProof did not finish within 30 minutes") from None


def confidence_report(output, code, identity):
    report_file = output / "confidence-report.json"
    if not report_file.is_file() or report_file.stat().st_size > LIMIT:
        raise ValueError("SwiftProof did not produce a bounded report; check binary, policy and sandbox image")
    report = json.loads(report_file.read_text(encoding="utf-8"))
    if (report.get("version") != 1 or report.get("exit_code") != code
            or report.get("change", {}).get("head_commit") != identity["head"]
            or report.get("change", {}).get("base_commit") != identity["base"]):
        raise ValueError("Report does not match the executed comparison")
    return report


def archive(output, work):
    buffer = io.BytesIO()
    total = 0
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in output.rglob("*"):
            if path.is_symlink():
                raise ValueError("Report artifacts must not contain symlinks")
            if path.is_file():
                total += path.stat().st_size
                if total > LIMIT:
                    raise ValueError("Report artifacts exceed the 8 MiB transfer budget")
                bundle.write(path, path.relative_to(output).as_posix())
        # The binary's own console output is what explains a configuration
        # or execution failure that the report can only point at. Keep a
        # bounded tail so a noisy run cannot crowd out the evidence.
        tail = (work / "run.log").read_bytes()[-65536:]
        if total + len(tail) <= LIMIT:
            bundle.writestr("pulsarcd-run.log", tail)
    return base64.b64encode(buffer.getvalue()).decode()


# ---------------------------------------------------------------------------
# On-demand plans and proofs (the SwiftProof MCP server)
# ---------------------------------------------------------------------------
# These never build, tag or deploy anything and never touch the pipeline gate.
# They compare whatever commits the host checkout knows about, but the policy
# always comes from the commit production runs: a candidate cannot weaken the
# rules it is judged by.
REF = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}")


def resolve(checkout, ref):
    """Resolve a branch, tag, release or commit of the host checkout to a full SHA."""
    if not REF.fullmatch(ref) or ".." in ref or ref.endswith((".lock", "/", ".")):
        raise ValueError("Invalid Git reference " + repr(ref[:80]))
    candidates = ["refs/remotes/origin/" + ref, "refs/tags/" + ref]
    if re.fullmatch(r"\d+\.\d+\.\d+", ref):
        candidates.append("refs/tags/v" + ref)
    candidates.append("refs/heads/" + ref)
    if re.fullmatch(r"[0-9a-fA-F]{7,40}", ref):
        candidates.append(ref.lower())
    for candidate in candidates:
        try:
            return command("git", "-C", str(checkout), "rev-parse", "--verify", "--quiet", candidate + "^{commit}")
        except ValueError:
            continue
    raise ValueError("Unknown Git reference " + ref + " in the host checkout: push it first")


def on_demand(request):
    action, repo = request["action"], request["repo"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", repo) or repo in (".", ".."):
        raise ValueError("Invalid repository")
    root = Path.home() / ".local/share/pulsarcd/swiftproof"
    checkout = Path(request["repos_path"]).expanduser() / repo
    if not (checkout / ".git").exists():
        raise ValueError("No checkout of " + repo + " on the deployment host: build the project once first")
    warnings = []
    try:
        command("git", "-C", str(checkout), "fetch", "--quiet", "origin", timeout=300,
                failure="fetch")
    except ValueError:
        warnings.append("git fetch failed on the deployment host: the commits already known to its checkout were used")
    trusted = released_commit(root, repo, request["stack"], "") or request.get("initial_baseline", "")
    if not re.fullmatch(r"[0-9a-f]{40}", trusted):
        raise ValueError("No build provenance matches the images running in production: set the verified"
                         " production commit as the initial baseline for " + repo)
    policy = command("git", "-C", str(checkout), "show", trusted + ":.swiftproof.json",
                     failure="No .swiftproof.json in the production commit " + trusted[:12] + " of " + repo)
    try:
        binary = Path(request["binary"]).expanduser().resolve(strict=True)
    except OSError:
        raise ValueError("No SwiftProof binary at " + str(request["binary"])
                         + " on the deployment host: run scripts/install-swiftproof.sh there") from None
    tip = request.get("head") or ""
    head = resolve(checkout, tip) if tip else resolve(checkout, "HEAD")
    identity = {"repo": repo, "action": action, "policy_commit": trusted,
                "binary_sha256": digest(binary.read_bytes()), "policy_sha256": digest(policy.encode())}
    provider = request.get("provider")
    with tempfile.TemporaryDirectory(prefix="pulsarcd-swiftproof-") as temporary:
        work = Path(temporary)
        output = work / "report"
        config_file = policy_file(work, policy, provider)
        if action == "plan":
            if not provider:
                raise ValueError("A plan needs the LLM configured in PulsarCD")
            usage = command(str(binary), "help", failure="The SwiftProof binary on the deployment host does not run")
            if "swiftproof plan" not in usage:
                raise ValueError("The SwiftProof binary on the deployment host has no plan command:"
                                 " install a release that provides it")
            intent = request.get("intent", "")
            if not isinstance(intent, str) or not intent.strip() or len(intent.encode()) > 65536:
                raise ValueError("A plan needs an intent of at most 64 KiB")
            (work / "intent.md").write_text(intent, encoding="utf-8")
            identity.update(base=head, head=head)
            args = [str(binary), "plan", "--repo", str(checkout), "--base", head, "--ci",
                    "--config", config_file, "--intent-file", str(work / "intent.md"), "--out", str(output)]
            code = run(args, work, provider)
            plan_file = output / "PLAN.json"
            if code in (0, 2) and (not plan_file.is_file() or plan_file.stat().st_size > LIMIT):
                raise ValueError("SwiftProof did not produce a bounded plan")
            output.mkdir(exist_ok=True)
            return {"identity": identity, "code": code, "warnings": warnings, "archive": archive(output, work)}
        base = resolve(checkout, request["base"]) if request.get("base") else trusted
        identity.update(base=base, head=head)
        args = [str(binary), "review", "--repo", str(checkout), "--base", base, "--head", head,
                "--exact", "--ci", "--config", config_file,
                "--reviewer=" + ("true" if provider else "false"), "--out", str(output)]
        if request.get("plan"):
            (work / "PLAN.json").write_text(request["plan"], encoding="utf-8")
            args += ["--plan", str(work / "PLAN.json")]
        code = run(args, work, provider)
        report = confidence_report(output, code, identity)
        return {"identity": identity, "code": code, "tool_version": report["tool_version"],
                "warnings": warnings, "archive": archive(output, work)}

if __name__ == "__main__":
    try:
        request = json.loads(sys.stdin.read(2 * 1024 * 1024))
        print(json.dumps({"ok": True, "result": execute(request)}))
    except Exception as error:
        # Never return command stdout/stderr or credentials from a provider.
        message = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(json.dumps({"ok": False, "error": message}))
        sys.exit(1)
