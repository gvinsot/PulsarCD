"""Automatic rollback after a deploy fails post-deploy verification."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from backend.config import GitHubConfig
from backend.deploy_rollback import (
    VERIFY_FAILED_MARKER,
    deploy_stage_with_rollback,
    deploy_with_rollback,
    describe_rollback,
    is_verification_failure,
    plan_rollback,
)
from backend.pipeline_state import PipelineStateManager


VERIFY_FAILED_OUTPUT = (
    "[INFO] Deploying stack: app\n"
    "[ERROR] app_web: 1 new task(s) failed (Failed: exit 1)\n"
    f"{VERIFY_FAILED_MARKER} check=converge stack=app version=2.0.0\n"
)


class FakeDeployer:
    """Records deploy calls and answers with scripted results, in order."""

    def __init__(self, *results, auto_rollback=True):
        self.results = list(results)
        self.calls = []
        self.config = GitHubConfig(auto_rollback=auto_rollback)

    async def deploy(self, repo_name, ssh_url, version="1.0", tag=None, qa=False,
                     output_callback=None, cancel_event=None):
        self.calls.append({"repo": repo_name, "version": version, "tag": tag, "qa": qa})
        result = dict(self.results.pop(0))
        result.setdefault("version", (tag or version or "").lstrip("v"))
        if output_callback:
            for line in result.get("output", "").splitlines():
                output_callback(line)
        return result


def ok(output="deployed"):
    return {"success": True, "output": output}


def verify_failed():
    return {"success": False, "output": VERIFY_FAILED_OUTPUT}


def run(coro):
    return asyncio.run(coro)


# ── Marker detection ──

def test_marker_must_start_a_line():
    assert is_verification_failure(VERIFY_FAILED_OUTPUT)
    assert is_verification_failure(f"  {VERIFY_FAILED_MARKER} check=health")
    assert not is_verification_failure("")
    assert not is_verification_failure(None)
    assert not is_verification_failure("[ERROR] Failed to pull image")
    # Quoted in the middle of an application log line: not ours.
    assert not is_verification_failure(f"app said: {VERIFY_FAILED_MARKER}")


# ── Decision ──

@pytest.mark.parametrize("kwargs, expected_target, reason_part", [
    (dict(enabled=True, failed_version="2.0.0", previous_version="1.9.0"), "1.9.0", "1.9.0"),
    (dict(enabled=True, failed_version="2.0.0", previous_version="v1.9.0"), "1.9.0", "1.9.0"),
    (dict(enabled=False, failed_version="2.0.0", previous_version="1.9.0"), None, "disabled"),
    (dict(enabled=True, failed_version="2.0.0", previous_version=None), None, "no previously"),
    (dict(enabled=True, failed_version="2.0.0", previous_version="v2.0.0"), None, "is the one that failed"),
    (dict(enabled=True, failed_version="2.0.0", previous_version="1.9.0", cancelled=True), None, "cancelled"),
])
def test_plan_rollback(kwargs, expected_target, reason_part):
    target, reason = plan_rollback(**kwargs)
    assert target == expected_target
    assert reason_part in reason


# ── deploy_with_rollback ──

def test_successful_deploy_is_untouched():
    deployer = FakeDeployer(ok())
    result = run(deploy_with_rollback(deployer, "app", "git@x:o/app.git",
                                      version="2.0.0", previous_version="1.9.0"))
    assert result["success"] is True
    assert "rollback" not in result
    assert len(deployer.calls) == 1


def test_failure_before_stack_deploy_does_not_roll_back():
    deployer = FakeDeployer({"success": False, "output": "[ERROR] Failed to pull image"})
    result = run(deploy_with_rollback(deployer, "app", "git@x:o/app.git",
                                      version="2.0.0", previous_version="1.9.0"))
    assert result["success"] is False
    assert "rollback" not in result
    assert len(deployer.calls) == 1


def test_verification_failure_redeploys_previous_version_once():
    lines = []
    deployer = FakeDeployer(verify_failed(), ok("rolled back fine"))
    result = run(deploy_with_rollback(deployer, "app", "git@x:o/app.git",
                                      version="2.0.0", tag="v2.0.0", qa=False,
                                      previous_version="1.9.0", output_callback=lines.append))
    assert result["success"] is False  # the requested version did not deploy
    assert deployer.calls[1] == {"repo": "app", "version": "1.9.0", "tag": None, "qa": False}
    rollback = result["rollback"]
    assert rollback["from_version"] == "2.0.0"
    assert rollback["to_version"] == "1.9.0"
    assert rollback["attempted"] is True and rollback["success"] is True
    # Summary first (what the LLM agent reads), rollback output last.
    assert result["output"].startswith(describe_rollback(rollback))
    assert "automatic rollback to 1.9.0 succeeded" in result["output"]
    assert result["output"].rstrip().endswith("rolled back fine")
    assert "=== Rolled back to 1.9.0 ===" in lines


def test_rollback_that_also_fails_verification_is_not_rolled_back_again():
    deployer = FakeDeployer(verify_failed(), verify_failed())
    result = run(deploy_with_rollback(deployer, "app", "git@x:o/app.git",
                                      version="2.0.0", previous_version="1.9.0"))
    assert len(deployer.calls) == 2  # no third deploy: no loop
    assert result["rollback"]["attempted"] is True
    assert result["rollback"]["success"] is False
    assert "automatic rollback to 1.9.0 FAILED" in result["output"]


def test_disabled_rollback_is_reported_not_attempted():
    deployer = FakeDeployer(verify_failed())
    result = run(deploy_with_rollback(deployer, "app", "git@x:o/app.git", version="2.0.0",
                                      previous_version="1.9.0", enabled=False))
    assert len(deployer.calls) == 1
    assert result["rollback"]["attempted"] is False
    assert "no automatic rollback" in result["output"]


def test_rollback_deploy_crash_is_reported():
    class Exploding(FakeDeployer):
        async def deploy(self, *args, **kwargs):
            if self.calls:
                raise RuntimeError("ssh down")
            return await super().deploy(*args, **kwargs)

    deployer = Exploding(verify_failed())
    result = run(deploy_with_rollback(deployer, "app", "git@x:o/app.git",
                                      version="2.0.0", previous_version="1.9.0"))
    assert result["rollback"]["success"] is False
    assert "RuntimeError: ssh down" in result["output"]


def test_cancelled_deploy_is_not_rolled_back():
    deployer = FakeDeployer(verify_failed())
    cancel = SimpleNamespace(is_set=lambda: True)
    result = run(deploy_with_rollback(deployer, "app", "git@x:o/app.git", version="2.0.0",
                                      previous_version="1.9.0", cancel_event=cancel))
    assert len(deployer.calls) == 1
    assert result["rollback"]["attempted"] is False


# ── Pipeline wiring ──

@pytest.fixture
def state(tmp_path):
    PipelineStateManager.reset_instance()
    mgr = PipelineStateManager(data_dir=str(tmp_path))
    yield mgr
    PipelineStateManager.reset_instance()


def test_stage_wrapper_uses_last_deployed_version_and_records_rollback(state, tmp_path):
    state.set_pipeline("app", "done", "success", "1.9.0")
    state.set_pipeline("app", "deploy", "running", "2.0.0")
    deployer = FakeDeployer(verify_failed(), ok())

    result = run(deploy_stage_with_rollback(deployer, state, "app", "git@x:o/app.git",
                                            version="2.0.0", tag="v2.0.0"))
    assert deployer.calls[1]["version"] == "1.9.0"
    state.set_pipeline("app", "deploy", "failed", "2.0.0")  # what api.py does next

    legacy = state.get_legacy("app")
    assert legacy["status"] == "failed"
    assert legacy["deployed_version"] == "1.9.0"
    assert legacy["last_rollback"]["stage"] == "deploy"
    assert legacy["last_rollback"]["to_version"] == "1.9.0"
    assert legacy["last_rollback"]["success"] is True
    assert result["rollback"] == {k: v for k, v in legacy["last_rollback"].items() if k != "stage"}

    # Persisted and reloaded.
    saved = json.loads((tmp_path / "pipeline_state.json").read_text(encoding="utf-8"))
    assert saved["app"]["last_rollback"]["from_version"] == "2.0.0"
    reloaded = PipelineStateManager(data_dir=str(tmp_path))
    assert reloaded.get("app").last_rollback["to_version"] == "1.9.0"


def test_stage_wrapper_reads_flag_from_deployer_config(state):
    state.set_pipeline("app", "done", "success", "1.9.0")
    deployer = FakeDeployer(verify_failed(), auto_rollback=False)
    result = run(deploy_stage_with_rollback(deployer, state, "app", "git@x:o/app.git",
                                            version="2.0.0"))
    assert len(deployer.calls) == 1
    assert "disabled" in result["rollback"]["reason"]
    assert state.get("app").last_rollback["attempted"] is False


def test_qa_stage_rolls_back_to_the_last_qa_version(state):
    state.set_pipeline("app", "done", "success", "1.5.0")
    state.set_pipeline("app", "qa", "success", "1.9.0")
    deployer = FakeDeployer(verify_failed(), ok())
    run(deploy_stage_with_rollback(deployer, state, "app", "git@x:o/app.git",
                                   version="2.0.0", qa=True))
    assert deployer.calls[1] == {"repo": "app", "version": "1.9.0", "tag": None, "qa": True}
    assert state.get("app").last_rollback["stage"] == "qa"


def test_first_deploy_has_nothing_to_roll_back_to(state):
    deployer = FakeDeployer(verify_failed())
    result = run(deploy_stage_with_rollback(deployer, state, "new-app", "git@x:o/new-app.git",
                                            version="1.0.0"))
    assert len(deployer.calls) == 1
    assert result["rollback"]["attempted"] is False
    assert "no previously deployed version" in result["rollback"]["reason"]


def test_failed_rollback_keeps_previous_current_version(state):
    state.set_pipeline("app", "done", "success", "1.9.0")
    state.record_rollback("app", "deploy", {"from_version": "2.0.0", "to_version": "1.9.0",
                                            "attempted": True, "success": False})
    assert state.get("app").stages["deploy"].current_version == "1.9.0"
    state.record_rollback("app", "build", {"attempted": True})  # ignored
    assert state.get("app").last_rollback["stage"] == "deploy"


def test_auto_rollback_defaults_on_and_reads_env(monkeypatch):
    from backend.config import Settings
    assert GitHubConfig().auto_rollback is True
    monkeypatch.setenv("PULSARCD_GITHUB__AUTO_ROLLBACK", "false")
    assert Settings().github.auto_rollback is False
