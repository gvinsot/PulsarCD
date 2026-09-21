"""Deployment authorization, immutable evidence and inherited provider integration."""
import asyncio
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock
import zipfile

import aiohttp
from aiohttp import web
import pytest
import yaml

from backend import swiftproof as gate
from backend import swiftproof_worker as worker
from backend.config_file import LLMConfig
from backend.pipeline_state import PipelineStateManager, PipelineEntry
from scripts import swiftproof_provenance as provenance


@pytest.fixture
def manager(tmp_path, monkeypatch):
    manager = PipelineStateManager(str(tmp_path))
    monkeypatch.setattr(PipelineStateManager, "_instance", manager)
    manager.set_transition_config("demo", "build_to_test", {"mode": "auto", "swiftproof_enabled": True})
    return manager


@pytest.fixture
def identity():
    return dict(repo="demo", release="1.0.1", base="a" * 40, head="b" * 40,
                images={"registry:5000/demo:latest": "registry:5000/demo@sha256:" + "c" * 64},
                binary_sha256="d" * 64, policy_sha256="e" * 64)


def reply(identity, code, report_extra=None):
    report = {"version": 1, "tool_version": "test", "exit_code": code,
              "change": {"base_commit": identity["base"], "head_commit": identity["head"]}}
    report.update(report_extra or {})
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr("confidence-report.json", json.dumps(report))
        archive.writestr("CONFIDENCE_REPORT.md", "# Evidence\nHuman review required")
    return dict(identity=identity, code=code, tool_version=report["tool_version"], archive=base64.b64encode(data.getvalue()).decode())


def mock_review(monkeypatch, identity, code, report_extra=None):
    calls = []
    async def run(deployer, payload, llm=None, cancel_event=None):
        calls.append((payload, llm))
        return identity if payload["action"] == "inspect" else reply(identity, code, report_extra)
    monkeypatch.setattr(gate, "worker", run)
    monkeypatch.setattr(gate, "request_for", lambda *args: {"repo": "demo", "release": "1.0.1"})
    llm = LLMConfig(url="http://pulsar-provider:8000", model="existing-model", api_key="never-in-worker")
    monkeypatch.setattr(gate, "load_config_file", lambda *_: SimpleNamespace(llm=llm))
    return calls


@pytest.mark.parametrize("code,status", [(0, "passed"), (1, "blocked"), (2, "needs_review"), (3, "error"), (4, "error")])
async def test_verdict_and_human_approval(manager, monkeypatch, identity, code, status):
    calls = mock_review(monkeypatch, identity, code)
    result = await gate.review(None, "demo", "1.0.1")
    assert result["status"] == status
    assert calls[1][1].model == "existing-model"
    assert "never-in-worker" not in json.dumps(calls[1][0])
    if code == 2:
        approved = gate.approve("demo", result["id"], "admin", "Reviewed the residual risk")
        assert approved["status"] == "approved"
        assert (await gate.review(None, "demo", "1.0.1"))["status"] == "approved"
    else:
        with pytest.raises(ValueError):
            gate.approve("demo", result["id"], "admin", "Cannot override")


async def test_new_digest_invalidates_approval(manager, monkeypatch, identity):
    calls = mock_review(monkeypatch, identity, 2)
    first = await gate.review(None, "demo", "1.0.1")
    gate.approve("demo", first["id"], "admin", "Reviewed")
    identity["images"]["registry:5000/demo:latest"] = "registry:5000/demo@sha256:" + "f" * 64
    second = await gate.review(None, "demo", "1.0.1")
    assert first["id"] != second["id"] and second["status"] == "needs_review"
    with pytest.raises(ValueError):
        gate.approve("demo", first["id"], "admin", "Stale")
    assert len(calls) == 4


async def test_report_tampering_regenerates_and_provider_failure_fails_closed(manager, monkeypatch, identity):
    calls = mock_review(monkeypatch, identity, 0)
    result = await gate.review(None, "demo", "1.0.1")
    gate.report_file(result["id"], "report.zip").write_bytes(b"changed")
    refreshed = await gate.review(None, "demo", "1.0.1")
    assert refreshed["status"] == "passed" and refreshed["id"] != result["id"]
    assert len(calls) == 4
    monkeypatch.setattr(gate, "worker", AsyncMock(side_effect=RuntimeError("secret-key")))
    failed = await gate.review(None, "demo", "1.0.1")
    assert failed["status"] == "error" and "secret-key" not in failed["reason"]


async def test_reviewer_can_be_disabled_without_changing_global_llm(manager, monkeypatch, identity):
    calls = mock_review(monkeypatch, identity, 0)
    manager.set_transition_config("demo", "build_to_test", {"swiftproof_reviewer": False})
    assert gate.enabled("demo")
    assert (await gate.review(None, "demo", "1.0.1"))["status"] == "passed"
    assert calls[1][1] is None


async def test_bridge_authentication_model_budget_and_chunked_response():
    received = []
    async def upstream(request):
        received.append((request.headers.get("Authorization"), await request.json()))
        response = web.StreamResponse(headers={"Content-Type": "application/json"})
        await response.prepare(request)
        await response.write(b'{"choices":')
        await asyncio.sleep(0.01)
        await response.write(b'[]}')
        await response.write_eof()
        return response
    app = web.Application()
    app.router.add_post("/v1/chat/completions", upstream)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        llm = LLMConfig(url=f"http://127.0.0.1:{runner.addresses[0][1]}", model="configured", api_key="provider-secret")
        async with gate.ProviderBridge(llm) as bridge, aiohttp.ClientSession() as client:
            url = f"http://127.0.0.1:{bridge.port}/v1/chat/completions"
            async with client.post(url, json={"messages": []}) as response:
                assert response.status == 401
            body = {"messages": [], "model": "candidate-model", "max_completion_tokens": 100000, "untrusted": "discard"}
            headers = {"Authorization": "Bearer " + bridge.token}
            async with client.post(url, json=body, headers=headers) as response:
                assert response.status == 200
                assert await response.json() == {"choices": []}
            assert received == [("Bearer provider-secret", {"messages": [], "model": "configured", "max_completion_tokens": 4096, "stream": False})]
            bridge.calls = 20
            async with client.post(url, json=body, headers=headers) as response:
                assert response.status == 429
    finally:
        await runner.cleanup()


async def test_deploy_is_only_a_deploy(manager, monkeypatch, identity):
    """QA and production deploys must not call, or be blocked by, SwiftProof."""
    from backend.github_service import StackDeployer
    from backend.config import GitHubConfig
    from backend import recovery
    deployer = StackDeployer(GitHubConfig())
    monkeypatch.setattr(recovery, "snapshot_envs", AsyncMock())
    monkeypatch.setattr(deployer, "_ensure_repo_cloned", AsyncMock(return_value=(True, "")))
    execute = AsyncMock(return_value=(True, "deployed"))
    monkeypatch.setattr(deployer, "_run_command", execute)
    review = AsyncMock(return_value={"status": "blocked", "reason": "Reproduced", "head": identity["head"]})
    monkeypatch.setattr(gate, "review", review)
    for qa in (True, False):
        result = await deployer.deploy("demo", "git@github.com:owner/demo.git", tag="v1.0.1", qa=qa)
        assert result["success"] and "gate_rejected" not in result
    review.assert_not_called()
    assert not hasattr(gate, "prepare_deploy") and not hasattr(gate, "record_deployed")
    command = execute.call_args.args[0]
    assert command.endswith(" v1.0.1") and "SWIFTPROOF_GUARD_FILE" not in command


def test_record_reused_image_requires_matching_commit(tmp_path, monkeypatch):
    monkeypatch.setattr(provenance, "root", lambda: tmp_path)
    monkeypatch.setattr(provenance, "run", lambda *args: json.dumps({"digest": "sha256:" + "c" * 64}))
    images = ["registry:5000/demo:latest=registry:5000/demo:1.0.1"]
    provenance.record("demo", "1.0.1", "a" * 40, images)
    provenance.record("demo", "1.0.1", "a" * 40, images, reuse=True)
    with pytest.raises(ValueError, match="Reused"):
        provenance.record("demo", "1.0.1", "b" * 40, images, reuse=True)


def test_worker_revalidates_identity_and_refuses_unknown_actions(tmp_path, monkeypatch, identity):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    root = tmp_path / ".local/share/pulsarcd/swiftproof"
    binary = tmp_path / "swiftproof"
    binary.write_bytes(b"trusted binary")
    worker.save(root / "builds/demo/1.0.1.json", {"repo": "demo", "release": "1.0.1", "commit": identity["head"], "images": identity["images"]})
    monkeypatch.setattr(worker, "command", lambda *args, **kwargs: '{"version":1}' if "show" in args else identity["head"])
    monkeypatch.setattr(worker, "live_services", lambda *_: {})
    request = dict(action="inspect", repo="demo", release="1.0.1", repos_path=str(tmp_path), stack="demo", binary=str(binary), initial_baseline=identity["base"])
    actual = worker.execute(request)
    assert actual["base"] == identity["base"] and "deployed_hash" not in actual
    with pytest.raises(ValueError, match="changed"):
        worker.execute(dict(request, action="review", identity=dict(actual, head="0" * 40)))
    for gone in ("guard", "deployed"):
        with pytest.raises(ValueError, match="Unknown action"):
            worker.execute(dict(request, action=gone, identity=actual, id="f" * 64))


def test_baseline_is_derived_from_the_images_production_runs(tmp_path, monkeypatch, identity):
    """A deploy records nothing: the baseline is whichever build is live."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    root = tmp_path / ".local/share/pulsarcd/swiftproof"
    binary = tmp_path / "swiftproof"
    binary.write_bytes(b"trusted binary")
    digest = "sha256:" + "c" * 64
    for release, commit in (("1.0.1", identity["head"]), ("1.0.0", "1" * 40), ("0.9.0", "2" * 40)):
        worker.save(root / "builds/demo" / (release + ".json"),
                    {"repo": "demo", "release": release, "commit": commit,
                     "images": {"registry:5000/demo:latest": "registry:5000/demo@" + digest}})
    monkeypatch.setattr(worker, "command", lambda *args, **kwargs: '{"version":1}' if "show" in args else identity["head"])
    request = dict(action="inspect", repo="demo", release="1.0.1", repos_path=str(tmp_path),
                   stack="demo", binary=str(binary), initial_baseline=identity["base"])

    # A release tag identifies the live build exactly.
    monkeypatch.setattr(worker, "live_services", lambda *_: {"demo_app": "registry:5000/demo:1.0.0"})
    assert worker.execute(request)["base"] == "1" * 40

    # A shared digest cannot tell 1.0.0 from 0.9.0; the lower release wins so
    # the review never sees a smaller diff than reality.
    monkeypatch.setattr(worker, "live_services", lambda *_: {"demo_app": "registry:5000/demo@" + digest})
    assert worker.execute(request)["base"] == "2" * 40

    # The candidate is never its own baseline, and an unexplained stack falls
    # back to the administrator's declared commit.
    monkeypatch.setattr(worker, "live_services", lambda *_: {"demo_app": "registry:5000/demo:1.0.1"})
    assert worker.execute(request)["base"] == identity["base"]
    monkeypatch.setattr(worker, "live_services", lambda *_: {"demo_app": "other:1.2.3"})
    assert worker.execute(request)["base"] == identity["base"]
    with pytest.raises(ValueError, match="initial baseline"):
        worker.execute(dict(request, initial_baseline=""))


def test_worker_setup_failures_name_what_to_fix_without_leaking_stderr(tmp_path, monkeypatch, identity):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(worker, "live_services", lambda *_: {})
    root = tmp_path / ".local/share/pulsarcd/swiftproof"
    request = dict(action="inspect", repo="demo", release="1.0.1", repos_path=str(tmp_path),
                   stack="demo", binary=str(tmp_path / "absent"), initial_baseline=identity["base"])
    with pytest.raises(ValueError, match="rebuild this release"):
        worker.execute(request)
    worker.save(root / "builds/demo/1.0.1.json",
                {"repo": "demo", "release": "1.0.1", "commit": identity["head"], "images": identity["images"]})

    def outputs(args, **kwargs):
        if "show" in args:
            raise subprocess.CalledProcessError(128, args, stderr=b"fatal: secret-token")
        return identity["head"].encode()

    monkeypatch.setattr(subprocess, "check_output", outputs)
    with pytest.raises(ValueError) as missing_policy:
        worker.execute(request)
    assert ".swiftproof.json" in str(missing_policy.value) and "secret-token" not in str(missing_policy.value)

    monkeypatch.setattr(subprocess, "check_output",
                        lambda args, **kwargs: b'{"version":1}' if "show" in args else identity["head"].encode())
    with pytest.raises(ValueError, match="install-swiftproof.sh"):
        worker.execute(request)

    # An unreadable Swarm is named as such, not as a decoding exception.
    monkeypatch.undo()
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(worker, "command",
                        lambda *args, **kwargs: identity["head"] if "rev-parse" in args else "not-json")
    with pytest.raises(ValueError, match="Cannot read the live services"):
        worker.execute(request)


def test_state_persistence_and_older_clients_preserve_gate(manager):
    manager.get_or_create("demo").swiftproof = {"id": "a", "status": "needs_review"}
    manager.get_or_create("demo").swiftproof_revision = 3
    manager.set_transition_config("demo", "build_to_test", {"mode": "manual"})
    restored = PipelineStateManager(str(manager._path.parent)).get("demo")
    assert restored.transition_configs["build_to_test"]["swiftproof_enabled"]
    assert restored.swiftproof["id"] == "a" and restored.swiftproof_revision == 3
    assert PipelineEntry.from_dict({}).swiftproof == {}


def test_settings_saved_before_the_move_are_migrated_not_lost():
    """A project configured while SwiftProof gated deployment keeps its review."""
    entry = PipelineEntry.from_dict({"transition_configs": {
        "test_to_deploy": {"mode": "manual", "qa_enabled": True,
                           "swiftproof_enabled": True, "swiftproof_initial_baseline": "a" * 40}}})
    assert entry.transition_configs["build_to_test"]["swiftproof_enabled"]
    assert entry.transition_configs["build_to_test"]["swiftproof_initial_baseline"] == "a" * 40
    assert entry.transition_configs["test_to_deploy"] == {"mode": "manual", "qa_enabled": True}


async def test_failed_setup_names_the_check_that_broke(manager, monkeypatch, identity):
    """A code 3/4 must say what to fix without opening the archive."""
    checks = [{"id": "check-1", "kind": "test", "status": "ERROR", "exit_code": 125,
               "output": "docker: Error response from daemon: No such image: demo-swiftproof:local\n"}]
    mock_review(monkeypatch, identity, 4, {"checks": checks})
    result = await gate.review(None, "demo", "1.0.1")
    assert result["status"] == "error"
    assert "test failed (exit 125)" in result["reason"] and "No such image" in result["reason"]

    manager.get_or_create("demo").swiftproof_revision += 1
    mock_review(monkeypatch, identity, 3, {"unverified": ["Reviewer incomplete: reviewer request failed"]})
    assert "Reviewer incomplete" in (await gate.review(None, "demo", "1.0.1"))["reason"]

    manager.get_or_create("demo").swiftproof_revision += 1
    mock_review(monkeypatch, identity, 0, {"checks": checks})
    assert (await gate.review(None, "demo", "1.0.1"))["reason"].endswith("blocking finding")


def test_api_config_rejects_coercion_and_readable_report(client, auth_headers, manager, monkeypatch, identity):
    import backend.api as api
    monkeypatch.setattr(api, "pipeline_state", manager)
    url = "/api/stacks/pipeline/demo/transition/build_to_test"
    assert client.put(url, headers=auth_headers, json={"swiftproof_enabled": "false"}).status_code == 400
    assert client.put(url, headers=auth_headers, json={"swiftproof_initial_baseline": "main"}).status_code == 400
    mock_review(monkeypatch, identity, 2)
    result = asyncio.run(gate.review(None, "demo", "1.0.1"))
    report_url = f'/api/stacks/pipeline/demo/swiftproof/{result["id"]}'
    assert client.get(report_url + "/report", headers=auth_headers).text.startswith("# Evidence")
    assert client.get(report_url.replace("/demo/", "/other/") + "/report", headers=auth_headers).status_code == 404
    assert client.post(report_url + "/approve", headers=auth_headers, json={"reason": ""}).status_code == 409
    assert client.post(report_url + "/approve", headers=auth_headers, json={"reason": "Evidence checked"}).status_code == 200
    assert client.post(report_url + "/retry", headers=auth_headers).status_code == 200
    assert manager.get("demo").swiftproof_revision == 1
    assert client.post(report_url + "/approve", headers=auth_headers, json={"reason": "Stale"}).status_code == 409


def test_report_findings_preserve_recorded_risk_evidence_and_coordinates():
    hypothesis = {"id": "hyp-1", "title": "Authorization bypass", "severity": "critical",
                  "status": "REPRODUCED", "rationale": "Candidate fails the baseline check",
                  "path": "auth.go", "line": 18, "evidence_ids": ["e-1", "missing"]}
    evidence = {"id": "e-1", "kind": "differential_test", "description": "Named regression test",
                "status": "REPRODUCED", "check_id": "candidate", "base_check_id": "base",
                "test_names": ["TestDenied"]}
    report = {
        "hypotheses": [hypothesis], "reproduced_issues": [hypothesis], "evidence": [evidence],
        "linter": [{"id": "signal-1", "kind": "auth", "path": "auth.go", "line": 8,
                    "end_line": 11, "side": "old", "severity": "high",
                    "summary": "Authorization body removed", "evidence": "A removed guard"}],
        "review_targets": [
            {"path": "auth.go", "start_line": 8, "end_line": 11, "side": "old",
             "severity": "high", "reasons": ["Authorization body removed"], "signal_ids": ["signal-1"]},
            {"path": "auth.go", "start_line": 18, "end_line": 18, "side": "new",
             "severity": "critical", "reasons": ["Authorization bypass"], "signal_ids": []},
            {"path": "legacy.go", "start_line": 2, "end_line": 4, "side": "new",
             "severity": "medium", "reasons": ["Historical target"], "signal_ids": []},
        ], "unverified": ["No coverage configured"],
    }
    findings = gate.report_findings(report)
    assert [item["kind"] for item in findings] == ["hypothesis", "signal", "review_target", "unverified"]
    assert len({item["id"] for item in findings}) == 4
    issue, signal, target, area = findings
    assert issue["status"] == "REPRODUCED" and issue["severity"] == "critical"
    assert issue["source_id"] == "hyp-1" and issue["evidence"][1] == evidence
    assert (issue["path"], issue["line"], issue["end_line"], issue["side"]) == ("auth.go", 18, 18, "new")
    assert (signal["path"], signal["line"], signal["end_line"], signal["side"]) == ("auth.go", 8, 11, "old")
    assert signal["status"] == target["status"] == ""
    assert signal["evidence"] == [{"description": "A removed guard"}]
    assert area["status"] == "UNVERIFIED" and area["severity"] == "" and area["line"] == 0


def test_report_findings_accept_empty_legacy_sections():
    assert gate.report_findings({}) == []
    assert gate.report_findings({"linter": None, "hypotheses": None, "review_targets": None,
                                "unverified": None, "evidence": None}) == []


async def test_v02_coverage_signals_are_navigable_without_changing_the_verdict(manager, monkeypatch, identity):
    coverage = {"status": "MEASURED", "added_lines": 2, "executed_lines": 1,
                "not_executed_lines": 1, "not_measured_lines": 0,
                "files": [{"path": "auth.go", "not_executed_lines": 1}]}
    signal = {"id": "uncovered-1", "kind": "uncovered_change", "path": "auth.go",
              "line": 42, "end_line": 42, "side": "new", "severity": "medium",
              "summary": "Added line was not executed", "evidence": "Recorded coverage count is zero"}
    mock_review(monkeypatch, identity, 0, {"tool_version": "v0.2.0", "coverage": coverage, "linter": [signal]})
    result = await gate.review(None, "demo", "1.0.1")
    report = gate.structured_report(result["id"], result)
    assert result["status"] == "passed"
    assert manager.get("demo").swiftproof["tool_version"] == "v0.2.0"
    assert report["result"]["tool_version"] == "v0.2.0"
    assert report["report"]["coverage"] == coverage
    finding, = report["findings"]
    assert (finding["path"], finding["line"], finding["side"]) == ("auth.go", 42, "new")
    assert finding["severity"] == "medium" and finding["status"] == ""
    assert finding["evidence"] == [{"description": "Recorded coverage count is zero"}]


def test_structured_report_api_preserves_raw_report_and_integrity(client, auth_headers, manager, monkeypatch, identity):
    import backend.api as api
    monkeypatch.setattr(api, "pipeline_state", manager)
    hypothesis = {"id": "hyp-1", "title": "Review <authorization>", "severity": "high",
                  "status": "UNVERIFIED", "path": "auth.go", "line": 12, "evidence_ids": []}
    mock_review(monkeypatch, identity, 2, {"hypotheses": [hypothesis], "checks": [
        {"id": "check-1", "kind": "test", "status": "FAIL", "exit_code": 1,
         "duration_ms": 123, "output": "failed\n  expected deny", "truncated": False}]})
    result = asyncio.run(gate.review(None, "demo", "1.0.1"))
    url = f'/api/stacks/pipeline/demo/swiftproof/{result["id"]}/report'
    response = client.get(url + "?format=json", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["result"]["release"] == "1.0.1"
    assert data["result"]["head"] == identity["head"]
    assert data["report"]["hypotheses"] == [hypothesis]
    assert data["report"]["checks"][0]["output"] == "failed\n  expected deny"
    assert data["findings"][0]["title"] == "Review <authorization>"
    assert data["markdown"] == client.get(url, headers=auth_headers).text
    assert client.get(url + "?download=true&format=json", headers=auth_headers).headers["content-type"] == "application/zip"
    assert client.get(url.replace("/demo/", "/other/") + "?format=json", headers=auth_headers).status_code == 404
    gate.report_file(result["id"], "report.zip").write_bytes(b"corrupt")
    assert client.get(url + "?format=json", headers=auth_headers).status_code == 404


@pytest.mark.parametrize("transition", ["build_to_test", "test_to_deploy"])
async def test_pipeline_agent_gate_without_llm_stops(client, manager, monkeypatch, transition):
    import backend.api as api
    manager.set_transition_config("demo", "build_to_test", {"mode": "auto", "swiftproof_enabled": False})
    manager.set_transition_config("demo", transition, {"mode": "agent"})
    monkeypatch.setattr(api, "pipeline_state", manager)
    monkeypatch.setattr(api, "llm_agent", None)
    deployer = SimpleNamespace(_ensure_repo_cloned=AsyncMock(return_value=(True, "")),
        has_build_config=AsyncMock(return_value=True), build=AsyncMock(return_value={"success": True}),
        test=AsyncMock(return_value={"success": True}), deploy=AsyncMock())
    monkeypatch.setattr(api, "_get_deployer_and_host", lambda: (deployer, "test-host"))
    tasks = []
    create_task = asyncio.create_task
    def capture(coro):
        task = create_task(coro)
        tasks.append(task)
        return task
    monkeypatch.setattr(api.asyncio, "create_task", capture)
    assert await api._trigger_pipeline("demo", "git@github.com:owner/demo.git", version="1.0.1")
    await asyncio.gather(*tasks)
    assert manager.get("demo").overall_status == "gate_rejected"
    assert "no LLM" in manager.get("demo").gates[-1].reason
    deployer.deploy.assert_not_called()
    if transition == "test_to_deploy":
        assert deployer.test.call_args.kwargs["tag"] == "v1.0.1"
    else:
        deployer.test.assert_not_called()


@pytest.mark.skipif(not os.environ.get("SWIFTPROOF_TEST_BINARY"), reason="Set SWIFTPROOF_TEST_BINARY for the real CLI integration")
@pytest.mark.parametrize("reviewer_enabled", [True, False])
async def test_real_cli_uses_inherited_provider_and_returns_bounded_artifacts(tmp_path, monkeypatch, identity, reviewer_enabled):
    repo = tmp_path / "repo"
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE).decode().strip()
    git("init")
    git("config", "user.email", "test@example.invalid")
    git("config", "user.name", "Test")
    (repo / "README.md").write_text("Before\n")
    git("add", ".")
    git("commit", "-m", "baseline")
    identity["base"] = git("rev-parse", "HEAD")
    (repo / "README.md").write_text("After\n")
    git("add", ".")
    git("commit", "-m", "candidate")
    identity["head"] = git("rev-parse", "HEAD")
    binary = Path(os.environ["SWIFTPROOF_TEST_BINARY"]).resolve()
    binary_version = subprocess.check_output([str(binary), "version"]).decode().strip().removeprefix("swiftproof ")
    monkeypatch.setattr(worker, "inspect", lambda _: (repo, binary,
        json.dumps({"version": 1, "commands": {}, "reviewer": {"model": "wrong-model"}}), identity))
    observed = []
    async def upstream(request):
        observed.append((request.headers.get("Authorization"), await request.json()))
        return web.json_response({"choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": "No hypothesis."}}]})
    app = web.Application()
    app.router.add_post("/v1/chat/completions", upstream)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        llm = LLMConfig(url=f"http://127.0.0.1:{runner.addresses[0][1]}", model="pulsar-model", api_key="upstream-secret")
        async with gate.ProviderBridge(llm) as bridge:
            request = dict(action="review", identity=identity,
                provider={"endpoint": f"http://127.0.0.1:{bridge.port}/v1", "model": llm.model, "token": bridge.token} if reviewer_enabled else None)
            result = await asyncio.to_thread(worker.execute, request)
            assert result["identity"] == identity and result["code"] in (0, 2)
            assert result["tool_version"] == binary_version
            if reviewer_enabled:
                assert len(observed) == 1 and observed[0][0] == "Bearer upstream-secret"
                assert observed[0][1]["model"] == "pulsar-model"
            else:
                assert observed == []
            with zipfile.ZipFile(io.BytesIO(base64.b64decode(result["archive"]))) as archive:
                # The binary's console output travels with the evidence so a
                # configuration failure can be diagnosed without the host.
                assert {"CONFIDENCE_REPORT.md", "pulsarcd-run.log"} <= set(archive.namelist())
                assert json.loads(archive.read("confidence-report.json"))["tool_version"] == binary_version
                for name in archive.namelist():
                    assert b"upstream-secret" not in archive.read(name)
                    assert bridge.token.encode() not in archive.read(name)
    finally:
        await runner.cleanup()
