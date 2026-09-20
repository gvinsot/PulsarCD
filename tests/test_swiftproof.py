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
    manager.set_transition_config("demo", "test_to_deploy", {"mode": "auto", "swiftproof_enabled": True})
    return manager


@pytest.fixture
def identity():
    return dict(repo="demo", release="1.0.1", base="a" * 40, head="b" * 40,
                images={"registry:5000/demo:latest": "registry:5000/demo@sha256:" + "c" * 64},
                binary_sha256="d" * 64, policy_sha256="e" * 64, deployed_hash=None)


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
    manager.set_transition_config("demo", "test_to_deploy", {"swiftproof_reviewer": False})
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


async def test_common_deploy_blocks_before_running_script(manager, monkeypatch):
    from backend.github_service import StackDeployer
    from backend.config import GitHubConfig
    from backend import recovery
    deployer = StackDeployer(GitHubConfig())
    monkeypatch.setattr(recovery, "snapshot_envs", AsyncMock())
    monkeypatch.setattr(deployer, "_ensure_repo_cloned", AsyncMock(return_value=(True, "")))
    execute = AsyncMock(return_value=(True, ""))
    monkeypatch.setattr(deployer, "_run_command", execute)
    monkeypatch.setattr(gate, "review", AsyncMock(return_value={"status": "needs_review", "reason": "Review required", "code": 2}))
    for qa in (False, True):
        result = await deployer.deploy("demo", "git@github.com:owner/demo.git", tag="v1.0.1", qa=qa)
        assert not result["success"] and result["gate_rejected"]
    execute.assert_not_called()


async def test_common_deploy_pins_sha_and_qa_does_not_advance_baseline(manager, monkeypatch, identity):
    from backend.github_service import StackDeployer
    from backend.config import GitHubConfig
    from backend import recovery
    deployer = StackDeployer(GitHubConfig())
    monkeypatch.setattr(recovery, "snapshot_envs", AsyncMock())
    monkeypatch.setattr(deployer, "_ensure_repo_cloned", AsyncMock(return_value=(True, "")))
    execute = AsyncMock(return_value=(True, "deployed"))
    monkeypatch.setattr(deployer, "_run_command", execute)
    monkeypatch.setattr(gate, "review", AsyncMock(return_value={"status": "passed", "reason": "Passed", "head": identity["head"]}))
    monkeypatch.setattr(gate, "prepare_deploy", AsyncMock(return_value={"path": "/trusted/guard.json"}))
    record = AsyncMock()
    monkeypatch.setattr(gate, "record_deployed", record)
    result = await deployer.deploy("demo", "git@github.com:owner/demo.git", tag="v1.0.1", qa=True)
    assert result["success"]
    command = execute.call_args.args[0]
    # The reviewed SHA must reach the script's commit argument: its branch
    # argument only resolves branches and tags, never a commit ID.
    assert command.endswith(" v1.0.1 " + identity["head"]) and "SWIFTPROOF_GUARD_FILE=/trusted/guard.json" in command
    record.assert_not_called()
    result = await deployer.deploy("demo", "git@github.com:owner/demo.git", tag="v1.0.1")
    assert result["success"]
    record.assert_awaited_once()


def test_pin_preserves_image_variants_and_rejects_changed_commit(tmp_path, monkeypatch, identity):
    monkeypatch.setattr(provenance, "root", lambda: tmp_path)
    monkeypatch.setattr(provenance, "run", lambda *args: identity["head"])
    guard = dict(identity, images={"registry:5000/demo:rocm": "registry:5000/demo@sha256:" + "c" * 64,
                                  "registry:5000/demo:cuda": "registry:5000/demo@sha256:" + "d" * 64})
    path = tmp_path / "guards" / "test.json"
    provenance.atomic_json(path, guard)
    compose = tmp_path / "compose.yml"
    compose.write_text(yaml.safe_dump({"services": {"a": {"image": "registry:5000/demo:rocm"},
                                                    "b": {"image": "registry:5000/demo:cuda-1.0.1"}}}))
    provenance.pin(path, compose, "demo")
    services = yaml.safe_load(compose.read_text())["services"]
    assert services["a"]["image"] != services["b"]["image"]
    assert json.loads(path.read_text())["services"]["demo_a"] == services["a"]["image"]
    monkeypatch.setattr(provenance, "run", lambda *args: "0" * 40)
    with pytest.raises(ValueError, match="commit"):
        provenance.pin(path, compose, "demo")


def test_record_reused_image_requires_matching_commit(tmp_path, monkeypatch):
    monkeypatch.setattr(provenance, "root", lambda: tmp_path)
    monkeypatch.setattr(provenance, "run", lambda *args: json.dumps({"digest": "sha256:" + "c" * 64}))
    images = ["registry:5000/demo:latest=registry:5000/demo:1.0.1"]
    provenance.record("demo", "1.0.1", "a" * 40, images)
    provenance.record("demo", "1.0.1", "a" * 40, images, reuse=True)
    with pytest.raises(ValueError, match="Reused"):
        provenance.record("demo", "1.0.1", "b" * 40, images, reuse=True)


def test_worker_revalidates_identity_and_records_only_actual_rollout(tmp_path, monkeypatch, identity):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    root = tmp_path / ".local/share/pulsarcd/swiftproof"
    binary = tmp_path / "swiftproof"
    binary.write_bytes(b"trusted binary")
    worker.save(root / "builds/demo/1.0.1.json", {"repo": "demo", "release": "1.0.1", "commit": identity["head"], "images": identity["images"]})
    monkeypatch.setattr(worker, "command", lambda *args, **kwargs: '{"version":1}' if "show" in args else identity["head"])
    request = dict(action="inspect", repo="demo", release="1.0.1", repos_path=str(tmp_path), stack="demo", binary=str(binary), initial_baseline=identity["base"])
    actual = worker.execute(request)
    with pytest.raises(ValueError, match="changed"):
        worker.execute(dict(request, action="guard", identity=dict(actual, head="0" * 40), id="f" * 64))
    guard_path = Path(worker.execute(dict(request, action="guard", identity=actual, id="f" * 64))["path"])
    guard = json.loads(guard_path.read_text())
    guard["services"] = {"demo_app": next(iter(identity["images"].values()))}
    worker.save(guard_path, guard)
    monkeypatch.setattr(worker, "live_services", lambda *_: {})
    with pytest.raises(ValueError, match="digests"):
        worker.execute(dict(request, action="deployed", identity=actual, id="f" * 64))
    monkeypatch.setattr(worker, "live_services", lambda *_: guard["services"])
    assert worker.execute(dict(request, action="deployed", identity=actual, id="f" * 64))["recorded"]
    assert worker.execute(request)["base"] == identity["head"]
    monkeypatch.setattr(worker, "live_services", lambda *_: {})
    with pytest.raises(ValueError, match="baseline"):
        worker.execute(request)


def test_worker_setup_failures_name_what_to_fix_without_leaking_stderr(tmp_path, monkeypatch, identity):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
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


def test_state_persistence_and_older_clients_preserve_gate(manager):
    manager.get_or_create("demo").swiftproof = {"id": "a", "status": "needs_review"}
    manager.get_or_create("demo").swiftproof_revision = 3
    manager.set_transition_config("demo", "test_to_deploy", {"mode": "manual"})
    restored = PipelineStateManager(str(manager._path.parent)).get("demo")
    assert restored.transition_configs["test_to_deploy"]["swiftproof_enabled"]
    assert restored.swiftproof["id"] == "a" and restored.swiftproof_revision == 3
    assert PipelineEntry.from_dict({}).swiftproof == {}


def test_api_config_rejects_coercion_and_readable_report(client, auth_headers, manager, monkeypatch, identity):
    import backend.api as api
    monkeypatch.setattr(api, "pipeline_state", manager)
    url = "/api/stacks/pipeline/demo/transition/test_to_deploy"
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
    manager.set_transition_config("demo", "test_to_deploy", {"mode": "auto", "swiftproof_enabled": False})
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
    monkeypatch.setattr(worker, "inspect", lambda _: (tmp_path, repo, binary,
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
                assert "CONFIDENCE_REPORT.md" in archive.namelist()
                assert json.loads(archive.read("confidence-report.json"))["tool_version"] == binary_version
                for name in archive.namelist():
                    assert b"upstream-secret" not in archive.read(name)
                    assert bridge.token.encode() not in archive.read(name)
    finally:
        await runner.cleanup()
