"""Tests for the build -> test -> (qa) -> deploy pipeline in backend/api.py.

The deployer, GitHub service and LLM agent are replaced by in-memory doubles;
the pipeline state is the real PipelineStateManager on a temporary directory,
so every assertion is made on what the UI and a restart would actually see.
"""

import asyncio
import types
from unittest.mock import AsyncMock, MagicMock

import pytest

import backend.api as api
from backend.pipeline_state import PipelineStateManager

SSH = "git@github.com:owner/app.git"


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------

class FakeDeployer:
    """Records every stage call and answers with configurable results."""

    def __init__(self, *, buildable=True, build_ok=True, test_ok=True, qa_ok=True,
                 deploy_ok=True, build_output=None, test_error=None, hold_build=None,
                 cancel_test=False):
        self.buildable = buildable
        self.build_ok = build_ok
        self.test_ok = test_ok
        self.qa_ok = qa_ok
        self.deploy_ok = deploy_ok
        self.build_output = build_output if build_output is not None else ["building", "Version: 1.0.5 (tag: v1.0.5)"]
        self.test_error = test_error
        self.hold_build = hold_build  # asyncio.Event the build waits on
        self.cancel_test = cancel_test
        self.calls = []

    def stages(self):
        return [c[0] for c in self.calls if c[0] != "clone"]

    def call(self, name):
        return next(c[1] for c in self.calls if c[0] == name)

    async def _ensure_repo_cloned(self, repo_name, ssh_url):
        self.calls.append(("clone", {"repo": repo_name}))
        return True, "Repository updated"

    async def has_build_config(self, repo_name):
        return self.buildable

    async def build(self, repo_name, ssh_url, version="1.0", tag=None,
                    output_callback=None, cancel_event=None):
        self.calls.append(("build", {"version": version, "tag": tag}))
        if self.hold_build is not None:
            await self.hold_build.wait()
        for line in self.build_output:
            output_callback(line)
        return {"success": self.build_ok, "output": "\n".join(self.build_output)}

    async def test(self, repo_name, ssh_url, tag=None, output_callback=None, cancel_event=None):
        self.calls.append(("test", {"tag": tag}))
        if self.test_error:
            raise self.test_error
        output_callback("tests ran")
        if self.cancel_test:
            cancel_event.set()
        return {"success": self.test_ok, "output": "tests ran"}

    async def deploy(self, repo_name, ssh_url, version="1.0", tag=None, qa=False,
                     output_callback=None, cancel_event=None):
        stage = "qa" if qa else "deploy"
        self.calls.append((stage, {"version": version, "tag": tag, "qa": qa}))
        output_callback(f"{stage} done")
        ok = self.qa_ok if qa else self.deploy_ok
        return {"success": ok, "output": f"{stage} output"}


@pytest.fixture
def env(monkeypatch, tmp_path):
    """Isolated pipeline module state; returns a namespace to configure."""
    mgr = PipelineStateManager(data_dir=str(tmp_path))
    monkeypatch.setattr(api, "pipeline_state", mgr)
    monkeypatch.setattr(api, "_auto_build_state", {})
    monkeypatch.setattr(api, "_background_actions", {})
    monkeypatch.setattr(api, "_buildable_cache", {})

    llm = MagicMock()
    llm.evaluate_gate = AsyncMock(return_value=(True, "looks good"))
    llm.handle_failure = AsyncMock()
    monkeypatch.setattr(api, "llm_agent", llm)

    ns = types.SimpleNamespace(state=mgr, llm=llm, deployer=FakeDeployer(), tmp_path=tmp_path)
    monkeypatch.setattr(api, "_get_deployer_and_host", lambda: (ns.deployer, "manager-1"))
    return ns


async def _run(repo="app", version="1.0.5", tag="v1.0.5", ssh_url=SSH):
    """Trigger a pipeline and wait for its background task to finish."""
    before = asyncio.all_tasks()
    started = await api._trigger_pipeline(repo, ssh_url, version=version, tag=tag)
    pending = asyncio.all_tasks() - before
    if pending:
        await asyncio.wait_for(asyncio.gather(*pending), timeout=5)
    return started


def _legacy(env, repo="app"):
    return env.state.get_legacy(repo)


def _actions_by_type():
    return {a.action_type: a for a in api._background_actions.values()}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    @pytest.mark.parametrize("lines,expected", [
        (["noise", "Version: 1.2.3 (tag: v1.2.3)"], "1.2.3"),
        (["Version: 1.0"], None),
        (["nothing here"], None),
        ([], None),
        (["Version: 2.0.0", "Version: 3.0.0"], "2.0.0"),
    ])
    def test_extract_version_from_output(self, lines, expected):
        assert api._extract_version_from_output(lines) == expected

    def test_build_lock_is_exclusive(self, env):
        assert api._acquire_build_lock("app") is True
        assert api._acquire_build_lock("app") is False
        api._release_build_lock("app")
        assert api._acquire_build_lock("app") is True

    def test_build_lock_does_not_invent_poller_fields(self, env):
        api._acquire_build_lock("app")
        assert api._auto_build_state["app"] == {"building": True}

    def test_release_unknown_repo_is_a_noop(self, env):
        api._release_build_lock("ghost")
        assert "ghost" not in api._auto_build_state


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

class TestSuccessPath:
    async def test_full_chain(self, env):
        assert await _run() is True

        assert env.deployer.stages() == ["build", "test", "deploy"]
        assert env.deployer.call("build") == {"version": "1.0.5", "tag": "v1.0.5"}
        assert env.deployer.call("test") == {"tag": "v1.0.5"}
        assert env.deployer.call("deploy") == {"version": "1.0.5", "tag": "v1.0.5", "qa": False}

        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("done", "success")
        assert legacy["deployed_version"] == "1.0.5"
        assert legacy["skip_build"] is False
        assert legacy["project_name"] == "app" and legacy["stack_name"] == "app"
        assert legacy["last_deployed_at"]
        assert legacy["qa_action_id"] is None
        for stage in ("build", "test", "deploy"):
            assert legacy["stages"][stage]["current_version"] == "1.0.5"

        actions = _actions_by_type()
        assert set(actions) == {"build", "test", "deploy"}
        assert all(a.status == "completed" for a in actions.values())
        assert legacy["build_action_id"] == actions["build"].id
        assert legacy["test_action_id"] == actions["test"].id
        assert legacy["deploy_action_id"] == actions["deploy"].id

        # Logs survive a restart, the lock is free again, nobody was paged
        reloaded = PipelineStateManager(data_dir=str(env.tmp_path))
        assert reloaded.get("app").stages["build"].last_log[-1].startswith("Version: 1.0.5")
        assert api._auto_build_state["app"]["building"] is False
        env.llm.handle_failure.assert_not_awaited()
        env.llm.evaluate_gate.assert_not_awaited()
        assert api._buildable_cache["app"] is True

    async def test_version_comes_from_build_output(self, env):
        """Auto-increment builds ("1.0") learn their version from the script."""
        env.deployer.build_output = ["Version: 1.0.42 (tag: v1.0.42)"]
        await _run(version=None, tag=None)
        assert env.deployer.call("build") == {"version": "1.0", "tag": None}
        assert env.deployer.call("test") == {"tag": "v1.0.42"}
        assert env.deployer.call("deploy")["tag"] == "v1.0.42"
        assert _legacy(env)["deployed_version"] == "1.0.42"

    async def test_unbuildable_repo_skips_build(self, env):
        env.deployer.buildable = False
        await _run()
        assert env.deployer.stages() == ["test", "deploy"]
        assert env.deployer.call("test") == {"tag": "v1.0.5"}
        legacy = _legacy(env)
        assert legacy["skip_build"] is True
        assert legacy["status"] == "success"
        assert legacy["build_action_id"] is None
        assert api._buildable_cache["app"] is False

    async def test_previous_gates_are_cleared(self, env):
        env.state.get_or_create("app")
        env.state.record_gate("app", "test_to_deploy", False, "old rejection")
        await _run()
        assert _legacy(env)["gates"] == []

    @pytest.mark.parametrize("mode", ["auto", "auto_with_success"])
    async def test_auto_modes_need_no_agent(self, env, mode):
        env.state.set_transition_config("app", "build_to_test", {"mode": mode})
        env.state.set_transition_config("app", "test_to_deploy", {"mode": mode})
        await _run()
        assert env.deployer.stages() == ["build", "test", "deploy"]
        env.llm.evaluate_gate.assert_not_awaited()


# ---------------------------------------------------------------------------
# Failure at each stage
# ---------------------------------------------------------------------------

class TestFailures:
    async def test_no_host_available(self, env, monkeypatch):
        def _no_host():
            raise RuntimeError("No host clients available")

        monkeypatch.setattr(api, "_get_deployer_and_host", _no_host)
        assert await _run() is False
        assert (_legacy(env)["stage"], _legacy(env)["status"]) == ("build", "failed")
        assert api._auto_build_state["app"]["building"] is False

    async def test_build_failure_stops_the_chain(self, env):
        env.deployer.build_ok = False
        await _run()
        assert env.deployer.stages() == ["build"]
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("build", "failed")
        assert legacy["deployed_version"] is None
        assert _actions_by_type()["build"].status == "failed"
        env.llm.handle_failure.assert_awaited_once()
        assert env.llm.handle_failure.await_args.args[:3] == ("build", "app", "1.0.5")
        assert api._auto_build_state["app"]["building"] is False
        assert env.state.get("app").stages["build"].last_log[-1].startswith("Version")

    async def test_test_failure_stops_before_deploy(self, env):
        env.deployer.test_ok = False
        await _run()
        assert env.deployer.stages() == ["build", "test"]
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("test", "failed")
        assert legacy["stages"]["build"]["status"] == "success"
        assert _actions_by_type()["test"].status == "failed"
        assert env.llm.handle_failure.await_args.args[0] == "test"
        assert env.state.get("app").stages["test"].last_log == ["tests ran"]

    async def test_cancelled_test_is_not_reported_as_an_incident(self, env):
        env.deployer.cancel_test = True
        await _run()
        assert env.deployer.stages() == ["build", "test"]
        assert _legacy(env)["status"] == "failed"
        assert _actions_by_type()["test"].status == "cancelled"
        env.llm.handle_failure.assert_not_awaited()

    async def test_deploy_failure(self, env):
        env.deployer.deploy_ok = False
        await _run()
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("deploy", "failed")
        assert legacy["deployed_version"] is None
        assert legacy["last_deployed_at"] is None
        assert _actions_by_type()["deploy"].status == "failed"
        assert env.llm.handle_failure.await_args.args[0] == "deploy"

    async def test_unexpected_exception_marks_current_stage_failed(self, env):
        env.deployer.test_error = RuntimeError("ssh exploded")
        await _run()
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("test", "failed")
        assert api._auto_build_state["app"]["building"] is False

    async def test_failure_notification_errors_do_not_break_the_pipeline(self, env):
        env.deployer.deploy_ok = False
        env.llm.handle_failure = AsyncMock(side_effect=RuntimeError("LLM down"))
        await _run()
        assert _legacy(env)["status"] == "failed"
        env.llm._report_system_error.assert_called_once()
        assert api._auto_build_state["app"]["building"] is False

    async def test_clone_error_before_build_check_is_tolerated(self, env):
        env.deployer._ensure_repo_cloned = AsyncMock(side_effect=OSError("ssh down"))
        await _run()
        assert _legacy(env)["status"] == "success"

    async def test_build_config_error_assumes_buildable(self, env):
        env.deployer.has_build_config = AsyncMock(side_effect=OSError("ssh down"))
        await _run()
        assert env.deployer.stages()[0] == "build"


# ---------------------------------------------------------------------------
# De-duplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    async def test_concurrent_trigger_is_refused_while_running(self, env):
        release = asyncio.Event()
        env.deployer.hold_build = release
        before = asyncio.all_tasks()
        assert await api._trigger_pipeline("app", SSH, version="1.0.5", tag="v1.0.5") is True
        await asyncio.sleep(0)  # let the build start and block
        assert _legacy(env)["status"] == "running"

        assert await api._trigger_pipeline("app", SSH, version="1.0.6", tag="v1.0.6") is False
        # Another repository is independent
        assert api._acquire_build_lock("other") is True
        api._release_build_lock("other")

        release.set()
        await asyncio.wait_for(asyncio.gather(*(asyncio.all_tasks() - before)), timeout=5)
        assert [c for c in env.deployer.calls if c[0] == "build"] == [
            ("build", {"version": "1.0.5", "tag": "v1.0.5"})
        ]
        assert _legacy(env)["deployed_version"] == "1.0.5"

        # Once finished, the next trigger goes through
        env.deployer.hold_build = None
        env.deployer.build_output = ["Version: 1.0.6 (tag: v1.0.6)"]
        assert await _run(version="1.0.6", tag="v1.0.6") is True
        assert _legacy(env)["deployed_version"] == "1.0.6"

    async def test_lock_is_released_after_gate_stop(self, env):
        env.state.set_transition_config("app", "build_to_test", {"mode": "manual"})
        await _run()
        assert api._auto_build_state["app"]["building"] is False
        assert await _run() is True


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

def _gate(env, transition, repo="app"):
    gates = [g for g in env.state.get(repo).gates if g.transition == transition]
    return gates


class TestBuildToTestGate:
    async def test_manual_stops_after_build(self, env):
        env.state.set_transition_config("app", "build_to_test", {"mode": "manual"})
        await _run()
        assert env.deployer.stages() == ["build"]
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("build", "gate_rejected")
        assert legacy["stages"]["build"]["current_version"] == "1.0.5"
        [gate] = _gate(env, "build_to_test")
        assert gate.approved is False and "Manual" in gate.reason
        assert gate.version == "1.0.5"

    async def test_agent_rejects(self, env):
        env.state.set_transition_config("app", "build_to_test", {"mode": "agent"})
        env.llm.evaluate_gate.return_value = (False, "warnings in build log")
        await _run()
        assert env.deployer.stages() == ["build"]
        assert _legacy(env)["status"] == "gate_rejected"
        assert env.llm.evaluate_gate.await_args.args[:3] == ("build_to_test", "app", "1.0.5")
        [gate] = _gate(env, "build_to_test")
        assert (gate.approved, gate.reason) == (False, "warnings in build log")

    async def test_agent_approves(self, env):
        env.state.set_transition_config("app", "build_to_test", {"mode": "agent"})
        await _run()
        assert env.deployer.stages() == ["build", "test", "deploy"]
        [gate] = _gate(env, "build_to_test")
        assert gate.approved is True

    async def test_agent_mode_without_llm_rejects(self, env, monkeypatch):
        monkeypatch.setattr(api, "llm_agent", None)
        env.state.set_transition_config("app", "build_to_test", {"mode": "agent"})
        await _run()
        assert env.deployer.stages() == ["build"]
        assert _legacy(env)["status"] == "gate_rejected"
        [gate] = _gate(env, "build_to_test")
        assert "no LLM configured" in gate.reason

    async def test_gate_is_skipped_when_build_is_skipped(self, env):
        env.deployer.buildable = False
        env.state.set_transition_config("app", "build_to_test", {"mode": "manual"})
        await _run()
        assert env.deployer.stages() == ["test", "deploy"]


class TestTestToDeployGate:
    async def test_manual_stops_after_test(self, env):
        env.state.set_transition_config("app", "test_to_deploy", {"mode": "manual"})
        await _run()
        assert env.deployer.stages() == ["build", "test"]
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("test", "gate_rejected")
        assert legacy["stages"]["test"]["current_version"] == "1.0.5"
        assert len(_gate(env, "test_to_deploy")) == 1

    async def test_agent_rejects_with_test_output(self, env):
        env.state.set_transition_config("app", "test_to_deploy", {"mode": "agent"})
        env.llm.evaluate_gate.return_value = (False, "coverage dropped")
        await _run()
        assert env.deployer.stages() == ["build", "test"]
        assert env.llm.evaluate_gate.await_args.args == ("test_to_deploy", "app", "1.0.5", "tests ran")
        assert _legacy(env)["status"] == "gate_rejected"

    async def test_agent_mode_without_llm_rejects(self, env, monkeypatch):
        monkeypatch.setattr(api, "llm_agent", None)
        env.state.set_transition_config("app", "test_to_deploy", {"mode": "agent"})
        await _run()
        assert env.deployer.stages() == ["build", "test"]
        assert _legacy(env)["status"] == "gate_rejected"


class TestQaStage:
    def _enable_qa(self, env, td_mode="auto", qd_mode=None):
        env.state.set_transition_config("app", "test_to_deploy", {"mode": td_mode, "qa_enabled": True})
        if qd_mode:
            env.state.set_transition_config("app", "qa_to_deploy", {"mode": qd_mode})

    async def test_qa_disabled_by_default(self, env):
        await _run()
        assert "qa" not in env.deployer.stages()
        assert _legacy(env)["stages"]["qa"]["status"] == "idle"

    async def test_qa_to_deploy_defaults_to_manual(self, env):
        self._enable_qa(env)
        await _run()
        assert env.deployer.stages() == ["build", "test", "qa"]
        assert env.deployer.call("qa") == {"version": "1.0.5", "tag": "v1.0.5", "qa": True}
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("qa", "gate_rejected")
        assert legacy["stages"]["qa"]["current_version"] == "1.0.5"
        assert legacy["qa_action_id"] == _actions_by_type()["qa-deploy"].id
        assert legacy["deployed_version"] is None
        [gate] = _gate(env, "qa_to_deploy")
        assert gate.approved is False and "after QA" in gate.reason

    async def test_qa_then_auto_production_deploy(self, env):
        self._enable_qa(env, qd_mode="auto")
        await _run()
        assert env.deployer.stages() == ["build", "test", "qa", "deploy"]
        assert env.deployer.call("deploy")["qa"] is False
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("done", "success")
        assert legacy["qa_action_id"] == _actions_by_type()["qa-deploy"].id
        assert legacy["deployed_version"] == "1.0.5"

    async def test_qa_failure_never_reaches_production(self, env):
        self._enable_qa(env, qd_mode="auto")
        env.deployer.qa_ok = False
        await _run()
        assert env.deployer.stages() == ["build", "test", "qa"]
        legacy = _legacy(env)
        assert (legacy["stage"], legacy["status"]) == ("qa", "failed")
        assert env.llm.handle_failure.await_args.args[0] == "qa"
        assert _actions_by_type()["qa-deploy"].status == "failed"

    async def test_agent_rejects_after_qa_records_one_decision(self, env):
        self._enable_qa(env, qd_mode="agent")
        env.llm.evaluate_gate.return_value = (False, "QA smoke test failed")
        await _run()
        assert env.deployer.stages() == ["build", "test", "qa"]
        assert env.llm.evaluate_gate.await_args.args == ("qa_to_deploy", "app", "1.0.5", "qa output")
        [gate] = _gate(env, "qa_to_deploy")
        assert (gate.approved, gate.reason) == (False, "QA smoke test failed")
        assert _legacy(env)["status"] == "gate_rejected"

    async def test_agent_approves_after_qa(self, env):
        self._enable_qa(env, qd_mode="agent")
        await _run()
        assert env.deployer.stages() == ["build", "test", "qa", "deploy"]
        assert _legacy(env)["status"] == "success"

    async def test_agent_mode_without_llm_stops_after_qa(self, env, monkeypatch):
        monkeypatch.setattr(api, "llm_agent", None)
        self._enable_qa(env, qd_mode="agent")
        await _run()
        assert env.deployer.stages() == ["build", "test", "qa"]
        [gate] = _gate(env, "qa_to_deploy")
        assert "no LLM configured" in gate.reason

    async def test_manual_test_gate_wins_over_qa(self, env):
        self._enable_qa(env, td_mode="manual", qd_mode="auto")
        await _run()
        assert env.deployer.stages() == ["build", "test"]


# ---------------------------------------------------------------------------
# HTTP endpoints around the pipeline
# ---------------------------------------------------------------------------

@pytest.fixture
def gh_mock(monkeypatch):
    m = MagicMock()
    m.is_configured = MagicMock(return_value=True)
    m.get_next_version = AsyncMock(return_value="1.0.8")
    m.create_tag = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(api, "github_service", m)
    return m


@pytest.fixture
def trigger_mock(monkeypatch):
    m = AsyncMock(return_value=True)
    monkeypatch.setattr(api, "_trigger_pipeline", m)
    return m


class TestTriggerEndpoint:
    URL = "/api/stacks/pipeline"

    def test_tag_flow(self, client, auth_headers, env, gh_mock, trigger_mock):
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": SSH, "tag": "v1.2.3"},
                           headers=auth_headers)
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"status": "started", "repo": "app", "tag": "v1.2.3", "version": "1.2.3"}
        trigger_mock.assert_awaited_once_with("app", SSH, version="1.2.3", tag="v1.2.3")

    def test_commit_flow_auto_tags(self, client, auth_headers, env, gh_mock, trigger_mock):
        sha = "a" * 40
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": SSH, "commit": sha},
                           headers=auth_headers)
        assert resp.status_code == 200, resp.text
        assert resp.json()["auto_tagged"] is True
        gh_mock.get_next_version.assert_awaited_once_with("owner", "app")
        gh_mock.create_tag.assert_awaited_once_with("owner", "app", "v1.0.8", sha)
        trigger_mock.assert_awaited_once_with("app", SSH, version="1.0.8", tag="v1.0.8")

    def test_tag_creation_failure(self, client, auth_headers, env, gh_mock, trigger_mock):
        gh_mock.create_tag.return_value = {"success": False, "error": "exists"}
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": SSH, "commit": "abc1234"},
                           headers=auth_headers)
        assert resp.status_code == 500
        trigger_mock.assert_not_awaited()

    @pytest.mark.parametrize("params,status", [
        ({}, 400),
        ({"tag": "latest"}, 400),
        ({"tag": "v1.2.3.4"}, 400),
    ])
    def test_bad_requests(self, client, auth_headers, env, gh_mock, trigger_mock, params, status):
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": SSH, **params},
                           headers=auth_headers)
        assert resp.status_code == status
        trigger_mock.assert_not_awaited()

    def test_unparseable_owner(self, client, auth_headers, env, gh_mock, trigger_mock):
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": "nonsense", "commit": "abc1234"},
                           headers=auth_headers)
        assert resp.status_code == 400

    def test_github_not_configured(self, client, auth_headers, env, gh_mock, trigger_mock):
        gh_mock.is_configured.return_value = False
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": SSH, "tag": "v1.0.0"},
                           headers=auth_headers)
        assert resp.status_code == 400

    def test_running_pipeline_conflicts(self, client, auth_headers, env, gh_mock, trigger_mock):
        env.state.set_pipeline("app", "test", "running", "1.0.0")
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": SSH, "tag": "v1.0.1"},
                           headers=auth_headers)
        assert resp.status_code == 409
        trigger_mock.assert_not_awaited()

    def test_held_build_lock_conflicts(self, client, auth_headers, env, gh_mock, trigger_mock):
        trigger_mock.return_value = False
        resp = client.post(self.URL, params={"repo_name": "app", "ssh_url": SSH, "tag": "v1.0.1"},
                           headers=auth_headers)
        assert resp.status_code == 409


class TestTransitionEndpoints:
    def _url(self, transition, repo="app"):
        return f"/api/stacks/pipeline/{repo}/transition/{transition}"

    def test_put_and_get_round_trip(self, client, auth_headers, env):
        resp = client.put(self._url("test_to_deploy"), json={"mode": "agent", "qa_enabled": True},
                          headers=auth_headers)
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"saved": True, "repo": "app", "transition": "test_to_deploy",
                               "mode": "agent", "qa_enabled": True}
        got = client.get(self._url("test_to_deploy"), headers=auth_headers).json()
        assert got["config"] == {"mode": "agent", "qa_enabled": True}
        assert got["last_decision"] is None

    def test_get_returns_last_decision_for_that_transition(self, client, auth_headers, env):
        env.state.get_or_create("app")
        env.state.record_gate("app", "qa_to_deploy", False, "first")
        env.state.record_gate("app", "build_to_test", True, "other")
        env.state.record_gate("app", "qa_to_deploy", True, "second")
        got = client.get(self._url("qa_to_deploy"), headers=auth_headers).json()
        assert got["last_decision"]["reason"] == "second"
        assert got["config"] == {}

    def test_qa_to_deploy_is_configurable(self, client, auth_headers, env):
        resp = client.put(self._url("qa_to_deploy"), json={"mode": "auto"}, headers=auth_headers)
        assert resp.status_code == 200
        assert env.state.get_transition_config("app", "qa_to_deploy") == {"mode": "auto"}

    @pytest.mark.parametrize("transition,body", [
        ("deploy_to_moon", {"mode": "auto"}),
        ("build_to_test", {"mode": "yolo"}),
    ])
    def test_rejects_invalid(self, client, auth_headers, env, transition, body):
        resp = client.put(self._url(transition), json=body, headers=auth_headers)
        assert resp.status_code == 400
        assert env.state.get("app") is None

    def test_get_rejects_invalid_transition(self, client, auth_headers, env):
        assert client.get(self._url("nope"), headers=auth_headers).status_code == 400

    def test_status_endpoint_lists_pipelines(self, client, auth_headers, env):
        env.state.set_pipeline("app", "build", "running", "1.0.0")
        data = client.get("/api/stacks/pipeline/status", headers=auth_headers).json()
        assert data["pipelines"]["app"]["status"] == "running"


# ---------------------------------------------------------------------------
# auto_build_poller
# ---------------------------------------------------------------------------

class _StopPoller(Exception):
    """Raised from the patched sleep to leave the poller's infinite loop."""


class World:
    """What GitHub looks like to the poller; tests mutate it between polls."""

    def __init__(self):
        self.pushed_at = "t0"
        self.sha = "sha0"
        self.untagged = 0
        self.latest_tag = {"name": "v1.0.5", "sha": "sha0"}
        self.fail_commits = False
        self.repo_calls = 0

    def service(self):
        m = MagicMock()
        m.is_configured = MagicMock(return_value=True)

        async def starred():
            return [{"name": "app", "owner": "owner", "ssh_url": SSH, "pushed_at": self.pushed_at}]

        async def commits(owner, name, per_page=1):
            self.repo_calls += 1
            if self.fail_commits:
                raise OSError("GitHub unreachable")
            return {"commits": [{"sha": self.sha}], "has_more": False}

        async def untagged(owner, name, limit=20):
            return {
                "untagged_commits": [{"sha": f"u{i}"} for i in range(self.untagged)],
                "latest_tag": self.latest_tag,
            }

        m.get_starred_repos = AsyncMock(side_effect=starred)
        m.get_repo_commits = AsyncMock(side_effect=commits)
        m.get_untagged_commits = AsyncMock(side_effect=untagged)
        m.get_next_version = AsyncMock(return_value="1.0.6")
        m.create_tag = AsyncMock(return_value={"success": True})
        return m


@pytest.fixture
def poller(env, monkeypatch):
    """Run auto_build_poller for a fixed number of polls.

    ``asyncio`` is swapped, in backend.api only, for a proxy whose sleep()
    returns at once and calls ``between(poll_index)`` after each poll.
    """
    world = World()
    gh = world.service()
    monkeypatch.setattr(api, "github_service", gh)
    trigger = AsyncMock(return_value=True)
    monkeypatch.setattr(api, "_trigger_pipeline", trigger)
    real_sleep = asyncio.sleep

    async def run(polls, between=None):
        calls = {"n": 0}

        async def fake_sleep(delay, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return  # initial start-up delay
            done = calls["n"] - 1  # polls completed so far
            if done >= polls:
                raise _StopPoller()
            if between:
                between(done - 1)
            await real_sleep(0)

        proxy = types.SimpleNamespace(**{n: getattr(asyncio, n) for n in dir(asyncio) if not n.startswith("__")})
        proxy.sleep = fake_sleep
        monkeypatch.setattr(api, "asyncio", proxy)
        try:
            with pytest.raises(_StopPoller):
                await api.auto_build_poller()
        finally:
            monkeypatch.setattr(api, "asyncio", asyncio)

    return types.SimpleNamespace(run=run, world=world, gh=gh, trigger=trigger, env=env)


class TestAutoBuildPoller:
    async def test_first_observation_only_records(self, poller):
        poller.world.untagged = 3
        await poller.run(polls=1)
        poller.trigger.assert_not_awaited()
        assert api._auto_build_state["app"] == {"last_sha": "sha0", "building": False, "untagged_commits": 3}

    async def test_new_untagged_commit_is_tagged_then_built(self, poller):
        w = poller.world

        def push(i):
            w.pushed_at, w.sha, w.untagged = "t1", "sha1", 1

        await poller.run(polls=2, between=push)
        poller.gh.create_tag.assert_awaited_once_with("owner", "app", "v1.0.6", "sha1")
        poller.trigger.assert_awaited_once_with("app", SSH, version="1.0.6", tag="v1.0.6")
        assert api._auto_build_state["app"]["last_sha"] == "sha1"

    async def test_new_tagged_commit_builds_latest_tag(self, poller):
        w = poller.world

        def push(i):
            w.pushed_at, w.sha = "t1", "sha1"
            w.latest_tag = {"name": "v2.0.0", "sha": "sha1"}

        await poller.run(polls=2, between=push)
        poller.gh.create_tag.assert_not_awaited()
        poller.trigger.assert_awaited_once_with("app", SSH, version="2.0.0", tag="v2.0.0")

    async def test_repo_without_tags_builds_with_auto_version(self, poller):
        w = poller.world
        w.latest_tag = None

        def push(i):
            w.pushed_at, w.sha = "t1", "sha1"

        await poller.run(polls=2, between=push)
        poller.trigger.assert_awaited_once_with("app", SSH)

    async def test_failed_auto_tag_does_not_build(self, poller):
        w = poller.world
        poller.gh.create_tag.return_value = {"success": False, "error": "exists"}

        def push(i):
            w.pushed_at, w.sha, w.untagged = "t1", "sha1", 1

        await poller.run(polls=2, between=push)
        poller.trigger.assert_not_awaited()

    async def test_manual_version_to_build_skips(self, poller):
        w = poller.world
        poller.env.state.set_transition_config("app", "version_to_build", {"mode": "manual"})

        def push(i):
            w.pushed_at, w.sha = "t1", "sha1"

        await poller.run(polls=3, between=push)
        poller.trigger.assert_not_awaited()
        assert api._auto_build_state["app"]["last_sha"] == "sha1"

    async def test_unchanged_pushed_at_skips_per_repo_calls(self, poller):
        await poller.run(polls=4)
        assert poller.world.repo_calls == 1
        poller.trigger.assert_not_awaited()

    async def test_manual_trigger_lock_counts_as_first_observation(self, poller):
        """A manual build before the first poll must not look like a new commit."""
        api._acquire_build_lock("app")
        api._release_build_lock("app")
        await poller.run(polls=1)
        poller.trigger.assert_not_awaited()
        assert api._auto_build_state["app"]["last_sha"] == "sha0"

    async def test_poller_survives_repo_listing_errors(self, poller):
        poller.gh.get_starred_repos = AsyncMock(side_effect=RuntimeError("boom"))
        await poller.run(polls=2)
        poller.trigger.assert_not_awaited()

    async def test_unconfigured_github_does_nothing(self, poller):
        poller.gh.is_configured.return_value = False
        await poller.run(polls=2)
        poller.gh.get_starred_repos.assert_not_awaited()
