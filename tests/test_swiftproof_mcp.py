"""On-demand SwiftProof plans and proofs: the SwiftProof MCP server."""
import asyncio
import base64
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock
import zipfile

import pytest

from backend import swiftproof as gate
from backend import swiftproof_jobs as jobs
from backend import swiftproof_worker as worker
from backend.config_file import LLMConfig, tool_denial_reason
from backend.pipeline_state import PipelineStateManager

# A stand-in for the SwiftProof CLI: records its arguments and writes the
# artifacts the real binary would, comparing the commits it was given.
FAKE_BINARY = r'''#!{python}
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
if args[0] == "help":
    print("swiftproof plan --intent-file FILE\nswiftproof review")
    sys.exit(0)
opts = {{args[i]: args[i + 1] for i in range(1, len(args) - 1) if args[i].startswith("--") and not args[i + 1].startswith("--")}}
out = Path(opts["--out"]); out.mkdir(parents=True)
policy = json.loads(Path(opts["--config"]).read_text())
(out / "args.json").write_text(json.dumps({{"args": args, "policy": policy, "token": os.environ.get("SWIFTPROOF_JOB_TOKEN")}}))
if args[0] == "plan":
    (out / "PLAN.json").write_text(json.dumps({{"format": "swiftproof-plan", "version": 1, "base_commit": opts["--base"],
        "assessment": {{"major": True, "categories": []}}, "contract": {{"files": ["a.py"]}}, "proposal": {{"summary": "s"}}}}))
    (out / "PLAN.md").write_text("# Plan\n")
    sys.exit(2)
report = {{"version": 1, "tool_version": "fake", "exit_code": 0,
          "change": {{"base_commit": opts["--base"], "head_commit": opts["--head"]}},
          "linter": [{{"id": "s1", "summary": "Sensitive path changed", "path": "a.py", "line": 1, "severity": "medium"}}]}}
if "--plan" in opts:
    report["plan_drift"] = {{"status": "conforming", "plan_sha256": "x"}}
(out / "confidence-report.json").write_text(json.dumps(report))
(out / "CONFIDENCE_REPORT.md").write_text("# Evidence\n")
'''


def git(cwd, *args):
    return subprocess.check_output(["git", "-C", str(cwd), *args], stderr=subprocess.PIPE).decode().strip()


@pytest.fixture
def host(tmp_path, monkeypatch):
    """A deployment host: an origin, its checkout, provenance root and binary."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(worker, "live_services", lambda *_: {})
    origin = tmp_path / "origin"
    origin.mkdir()
    git(origin, "init", "-q", "-b", "main")
    git(origin, "config", "user.email", "t@example.invalid")
    git(origin, "config", "user.name", "T")
    (origin / ".swiftproof.json").write_text(json.dumps({"version": 1, "reviewer": {"max_iterations": 99}}))
    (origin / "a.py").write_text("x = 1\n")
    git(origin, "add", ".")
    git(origin, "commit", "-q", "-m", "production")
    production = git(origin, "rev-parse", "HEAD")
    repos = tmp_path / "repos"
    repos.mkdir()
    subprocess.check_call(["git", "clone", "-q", str(origin), str(repos / "demo")])
    # Pushed after the host's last build: only a fetch makes it visible.
    git(origin, "checkout", "-q", "-b", "feature")
    (origin / "a.py").write_text("x = 2\n")
    (origin / ".swiftproof.json").write_text(json.dumps({"version": 1, "commands": {}, "weakened": True}))
    git(origin, "commit", "-q", "-am", "candidate")
    candidate = git(origin, "rev-parse", "HEAD")
    binary = tmp_path / "swiftproof"
    binary.write_text(FAKE_BINARY.format(python=sys.executable))
    binary.chmod(0o755)
    request = dict(repo="demo", repos_path=str(repos), stack="demo", binary=str(binary), initial_baseline=production)
    return SimpleNamespace(request=request, production=production, candidate=candidate, origin=origin)


def files(result):
    with zipfile.ZipFile(io.BytesIO(base64.b64decode(result["archive"]))) as bundle:
        return {name: bundle.read(name) for name in bundle.namelist()}


def test_proof_fetches_the_pushed_branch_and_keeps_the_production_policy(host):
    provider = {"endpoint": "http://127.0.0.1:1/v1", "model": "m", "token": "job-token"}
    result = worker.execute(dict(host.request, action="prove", head="feature", provider=provider))
    assert result["code"] == 0 and result["tool_version"] == "fake" and result["warnings"] == []
    assert result["identity"]["base"] == host.production and result["identity"]["head"] == host.candidate
    recorded = json.loads(files(result)["args.json"])
    args = recorded["args"]
    assert args[0] == "review" and "--exact" in args and "--ci" in args and "--reviewer=true" in args
    # The candidate weakened its policy; the production commit's policy is used,
    # with PulsarCD's budgets and provider.
    assert "weakened" not in recorded["policy"]
    assert recorded["policy"]["reviewer"]["max_iterations"] == 20
    assert recorded["policy"]["reviewer"]["model"] == "m" and recorded["token"] == "job-token"


def test_proof_accepts_explicit_base_commit_and_plan(host):
    plan = json.dumps({"format": "swiftproof-plan", "version": 1})
    result = worker.execute(dict(host.request, action="prove", head=host.candidate[:10], base="main", plan=plan))
    assert result["identity"]["base"] == host.production
    args = json.loads(files(result)["args.json"])["args"]
    assert "--plan" in args and "--reviewer=false" in args


@pytest.mark.parametrize("ref", ["--upload-pack=x", "a..b", "feature;id", "unknown-branch"])
def test_proof_refuses_invalid_or_unknown_references(host, ref):
    with pytest.raises(ValueError, match="Git reference"):
        worker.execute(dict(host.request, action="prove", head=ref))


def test_plan_needs_a_provider_and_an_intent_and_starts_from_the_default_branch(host):
    provider = {"endpoint": "http://127.0.0.1:1/v1", "model": "m", "token": "t"}
    with pytest.raises(ValueError, match="LLM"):
        worker.execute(dict(host.request, action="plan", intent="Add a flag"))
    with pytest.raises(ValueError, match="intent"):
        worker.execute(dict(host.request, action="plan", intent=" ", provider=provider))
    result = worker.execute(dict(host.request, action="plan", intent="Add a flag", provider=provider))
    assert result["code"] == 2
    bundle = files(result)
    assert json.loads(bundle["PLAN.json"])["base_commit"] == host.production
    assert json.loads(bundle["args.json"])["args"][0] == "plan"


def test_plan_explains_a_binary_without_the_plan_command(host, tmp_path):
    old = tmp_path / "old-swiftproof"
    old.write_text("#!/bin/sh\necho 'swiftproof review'\n")
    old.chmod(0o755)
    provider = {"endpoint": "http://127.0.0.1:1/v1", "model": "m", "token": "t"}
    with pytest.raises(ValueError, match="no plan command"):
        worker.execute(dict(host.request, binary=str(old), action="plan", intent="x", provider=provider))


def test_missing_checkout_and_baseline_name_what_to_fix(host):
    with pytest.raises(ValueError, match="No checkout"):
        worker.execute(dict(host.request, repo="other", action="prove"))
    with pytest.raises(ValueError, match="initial baseline"):
        worker.execute(dict(host.request, action="prove", initial_baseline=""))


# ---------------------------------------------------------------------------
# Jobs: asynchronous, persisted, never touching the pipeline gate
# ---------------------------------------------------------------------------
@pytest.fixture
def manager(tmp_path, monkeypatch):
    manager = PipelineStateManager(str(tmp_path / "data"))
    monkeypatch.setattr(PipelineStateManager, "_instance", manager)
    manager.set_transition_config("demo", "build_to_test", {"mode": "auto", "swiftproof_enabled": True})
    llm = LLMConfig(url="http://pulsar-provider:8000", model="existing-model", api_key="never-in-worker")
    monkeypatch.setattr(jobs, "load_config_file", lambda *_: SimpleNamespace(llm=llm))
    monkeypatch.setattr(gate, "request_for", lambda deployer, repo, release: {
        "repo": repo, "release": release, "repos_path": "/r", "stack": repo, "binary": "/b", "initial_baseline": ""})
    return manager


def archive(entries):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as bundle:
        for name, value in entries.items():
            bundle.writestr(name, value)
    return base64.b64encode(data.getvalue()).decode()


async def finished(job_id):
    for _ in range(200):
        job = jobs.result(job_id)
        if job["status"] not in ("queued", "running"):
            return job
        await asyncio.sleep(0.01)
    raise AssertionError("job did not finish")


async def test_plan_then_proof_with_drift_and_the_pipeline_gate_is_untouched(manager, monkeypatch):
    identity = {"repo": "demo", "base": "a" * 40, "head": "b" * 40}
    calls = []

    async def run(deployer, payload, llm=None, cancel_event=None):
        calls.append((payload, llm))
        if payload["action"] == "plan":
            plan = {"base_commit": "a" * 40, "assessment": {"major": True}, "contract": {"files": ["a.py"]}}
            return {"identity": identity, "code": 2, "archive": archive({"PLAN.json": json.dumps(plan), "PLAN.md": "# Plan"})}
        report = {"version": 1, "exit_code": 1, "change": {"base_commit": "a" * 40, "head_commit": "b" * 40},
                  "hypotheses": [{"id": "h1", "title": "Crash", "status": "REPRODUCED", "severity": "high", "path": "a.py", "line": 3}],
                  "plan_drift": {"status": "drifted"}}
        return {"identity": identity, "code": 1, "tool_version": "t", "warnings": [],
                "archive": archive({"confidence-report.json": json.dumps(report), "CONFIDENCE_REPORT.md": "# Evidence"})}

    monkeypatch.setattr(gate, "worker", run)
    plan = await jobs.start(None, "demo", "plan", intent="Add a flag")
    assert plan["status"] == "queued" and len(plan["params"]["intent_sha256"]) == 64
    plan = await finished(plan["id"])
    assert plan["status"] == "flagged" and plan["plan"]["assessment"]["major"] is True
    assert plan["markdown"] == "# Plan"
    assert calls[0][0]["intent"] == "Add a flag" and calls[0][1].model == "existing-model"
    assert "never-in-worker" not in json.dumps(calls[0][0])

    proof = await finished((await jobs.start(None, "demo", "prove", head="feature", plan_id=plan["id"]))["id"])
    assert proof["status"] == "blocked" and proof["code"] == 1
    assert proof["findings"][0]["status"] == "REPRODUCED" and proof["plan_drift"]["status"] == "drifted"
    assert json.loads(calls[1][0]["plan"])["contract"] == {"files": ["a.py"]}
    assert calls[1][0]["head"] == "feature" and "release" not in calls[1][0]

    assert manager.get_or_create("demo").swiftproof == {}
    assert [job["id"] for job in jobs.listing("demo")] == [proof["id"], plan["id"]]
    assert jobs.listing("other") == []


async def test_proof_honours_the_reviewer_choice_and_rejects_foreign_plans(manager, monkeypatch):
    seen = []

    async def run(deployer, payload, llm=None, cancel_event=None):
        seen.append(llm)
        raise ValueError("No SwiftProof binary at /b on the deployment host")

    monkeypatch.setattr(gate, "worker", run)
    manager.set_transition_config("demo", "build_to_test", {"mode": "auto", "swiftproof_reviewer": False})
    job = await finished((await jobs.start(None, "demo", "prove"))["id"])
    assert seen == [None] and job["params"]["reviewer"] is False
    assert job["status"] == "error" and "install" not in job["reason"] and "No SwiftProof binary" in job["reason"]
    with pytest.raises(ValueError, match="finished plan"):
        await jobs.start(None, "demo", "prove", plan_id=job["id"])
    with pytest.raises(ValueError, match="Invalid job id"):
        jobs.result("../../etc")


async def test_cancel_and_restart_are_reported(manager, monkeypatch):
    started = asyncio.Event()

    async def run(deployer, payload, llm=None, cancel_event=None):
        started.set()
        await cancel_event.wait()
        raise ValueError("SwiftProof cancelled or timed out")

    monkeypatch.setattr(gate, "worker", run)
    job = await jobs.start(None, "demo", "prove")
    await started.wait()
    jobs.cancel(job["id"])
    assert (await finished(job["id"]))["status"] == "cancelled"
    with pytest.raises(ValueError, match="not running"):
        jobs.cancel(job["id"])

    # A job recorded as running by a previous process is reported as interrupted.
    ghost = dict(job, id="f" * 32, status="running")
    jobs._write(ghost)
    assert jobs.result("f" * 32)["status"] == "interrupted"


async def test_tampered_archive_is_refused(manager, monkeypatch):
    async def run(deployer, payload, llm=None, cancel_event=None):
        return {"identity": {"base": "a" * 40, "head": "b" * 40}, "code": 0, "tool_version": "t",
                "archive": archive({"confidence-report.json": json.dumps(
                    {"version": 1, "exit_code": 0, "change": {"base_commit": "a" * 40, "head_commit": "b" * 40}})})}

    monkeypatch.setattr(gate, "worker", run)
    job = await finished((await jobs.start(None, "demo", "prove"))["id"])
    assert job["status"] == "passed"
    (jobs.directory(job["id"]) / "archive.zip").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="integrity"):
        jobs.result(job["id"])


# ---------------------------------------------------------------------------
# MCP surface
# ---------------------------------------------------------------------------
def test_swiftproof_server_is_admin_only_and_its_costly_tools_are_denied_to_the_agent():
    try:
        from backend import api
        from backend.mcp_auth import MCPAuthMiddleware
        from backend.mcp_server import mcp_swiftproof
    except Exception:
        pytest.skip("MCP support not installed in this environment")
    mounts = {r.path: r.app for r in api.app.routes if getattr(r, "path", "") == "/ai/swiftproof"}
    assert isinstance(mounts["/ai/swiftproof"], MCPAuthMiddleware)
    assert mounts["/ai/swiftproof"].require_admin is True
    names = {tool.name for tool in asyncio.run(mcp_swiftproof.list_tools())}
    assert names == {"swiftproof_status", "swiftproof_plan", "swiftproof_prove", "swiftproof_get_job",
                     "swiftproof_list_jobs", "swiftproof_cancel_job"}
    for name in ("swiftproof_plan", "swiftproof_prove", "swiftproof_cancel_job"):
        assert tool_denial_reason(name) is not None


async def test_mcp_tools_resolve_the_stack_and_validate_references(manager, monkeypatch):
    try:
        from backend import mcp_server
    except Exception:
        pytest.skip("MCP support not installed in this environment")
    import backend.api as api
    monkeypatch.setattr(api, "github_service", SimpleNamespace(get_starred_repos=AsyncMock(return_value=[
        {"name": "Demo", "owner": "o", "ssh_url": "git@github.com:o/Demo.git"}])))
    monkeypatch.setattr(mcp_server, "_get_deployer_and_host", lambda: (None, "host"))
    started = []

    async def start(deployer, repo, kind, **kwargs):
        started.append((repo, kind, kwargs))
        return {"id": "0" * 32, "status": "queued"}

    monkeypatch.setattr(jobs, "start", start)
    reply = json.loads(await mcp_server.swiftproof_prove("demo", head="feature/x"))
    assert reply["job_id"] == "0" * 32 and started[0][:2] == ("Demo", "prove")
    assert "error" in json.loads(await mcp_server.swiftproof_prove("demo", head="a;id"))
    assert "Unknown stack" in json.loads(await mcp_server.swiftproof_plan("nope", "intent"))["error"]
    status = json.loads(await mcp_server.swiftproof_status("demo"))
    assert status["repo"] == "Demo" and status["recent_jobs"] == []
