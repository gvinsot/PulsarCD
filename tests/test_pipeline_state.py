"""Tests for backend/pipeline_state.py: persistence, reload, truncation, gates.

The pipeline state file is the only record of where every repository's
build/test/qa/deploy run stands, and of the per-project transition gates
(a lost "manual" gate silently re-enables automatic production deploys), so
these tests exercise it against a real file in a temporary directory.
"""

import asyncio
import json

import pytest

from backend.pipeline_state import (
    _MAX_GATE_DECISIONS,
    _MAX_LOG_LINES,
    GateDecision,
    PipelineEntry,
    PipelineStateManager,
    StageState,
)


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path


@pytest.fixture
def mgr(data_dir):
    return PipelineStateManager(data_dir=str(data_dir))


def _reload(data_dir) -> PipelineStateManager:
    """A second manager on the same directory, as after a process restart."""
    return PipelineStateManager(data_dir=str(data_dir))


def _state_file(data_dir):
    return data_dir / "pipeline_state.json"


# ---------------------------------------------------------------------------
# Serialisation round trips
# ---------------------------------------------------------------------------

class TestSerialisation:
    def test_stage_state_round_trip(self):
        stage = StageState(status="success", desired_version="1.2.3",
                           current_version="1.2.3", last_log=["a", "b"],
                           action_id="abc12345", updated_at="2026-01-01T00:00:00+00:00")
        clone = StageState.from_dict(stage.to_dict())
        assert clone.to_dict() == stage.to_dict()

    def test_stage_state_defaults_for_missing_keys(self):
        stage = StageState.from_dict({})
        assert stage.status == "idle"
        assert stage.desired_version is None
        assert stage.last_log == []
        assert stage.updated_at  # always stamped

    def test_gate_decision_round_trip(self):
        gate = GateDecision("test_to_deploy", False, "flaky tests", version="2.0.0")
        clone = GateDecision.from_dict(gate.to_dict())
        assert clone.to_dict() == gate.to_dict()

    def test_entry_round_trip_keeps_every_field(self):
        entry = PipelineEntry()
        entry.current_stage = "qa"
        entry.overall_status = "gate_rejected"
        entry.skip_build = True
        entry.project_name = "MyRepo"
        entry.stack_name = "myrepo"
        entry.gates = [GateDecision("qa_to_deploy", False, "manual")]
        entry.last_deployed_at = "2026-01-01T00:00:00+00:00"
        entry.transition_configs = {"test_to_deploy": {"mode": "manual", "qa_enabled": True}}
        entry.stages["qa"].status = "success"

        clone = PipelineEntry.from_dict(entry.to_dict())
        assert clone.to_dict() == entry.to_dict()

    def test_entry_from_legacy_file_without_qa_stage(self):
        """Files written before the QA stage existed must still load."""
        data = {
            "stages": {
                "build": {"status": "success", "current_version": "1.0.0"},
                "test": {"status": "success"},
                "deploy": {"status": "success", "current_version": "1.0.0"},
            },
            "current_stage": "done",
            "overall_status": "success",
        }
        entry = PipelineEntry.from_dict(data)
        assert entry.stages["qa"].status == "idle"
        assert entry.stages["deploy"].current_version == "1.0.0"
        assert entry.transition_configs == {}
        assert entry.gates == []

    def test_legacy_dict_omits_logs_but_keeps_statuses(self, mgr):
        mgr.set_stage("r", "build", "success", "1.0.0", action_id="b1",
                      log_lines=["line"] * 10)
        legacy = mgr.get_legacy("r")
        assert legacy["build_action_id"] == "b1"
        assert legacy["stages"]["build"]["status"] == "success"
        assert "last_log" not in legacy["stages"]["build"]

    def test_legacy_version_prefers_current_stage(self, mgr):
        mgr.set_stage("r", "build", "success", "1.0.0")
        mgr.set_stage("r", "test", "running", "1.0.1")
        assert mgr.get_legacy("r")["version"] == "1.0.1"

    def test_legacy_version_falls_back_to_latest_stage_with_version(self, mgr):
        entry = mgr.get_or_create("r")
        entry.stages["build"].desired_version = "3.0.0"
        entry.current_stage = "done"  # not a stage: no desired_version there
        assert mgr.get_legacy("r")["version"] == "3.0.0"

    def test_get_legacy_unknown_repo_is_empty(self, mgr):
        assert mgr.get_legacy("nope") == {}
        assert mgr.get("nope") is None
        assert "nope" not in mgr


# ---------------------------------------------------------------------------
# Persistence and reload
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_fresh_directory_starts_empty(self, mgr, data_dir):
        assert list(mgr.items()) == []
        assert not _state_file(data_dir).exists()

    def test_every_mutation_is_persisted_and_reloaded(self, mgr, data_dir):
        mgr.set_pipeline("repo", "build", "running", "1.0.0", build_id="b1")
        mgr.set_skip_build("repo", True)
        mgr.set_project_info("repo", "repo", "repo-stack")
        mgr.set_transition_config("repo", "test_to_deploy", {"mode": "manual", "qa_enabled": True})
        mgr.record_gate("repo", "test_to_deploy", False, "waiting", version="1.0.0")
        mgr.update_version("repo", "build", "1.0.1")

        reloaded = _reload(data_dir)
        assert reloaded.get("repo").to_dict() == mgr.get("repo").to_dict()
        assert reloaded.get_transition_config("repo", "test_to_deploy") == {
            "mode": "manual", "qa_enabled": True,
        }
        assert reloaded.find_repo_by_stack("REPO-STACK") == "repo"

    def test_file_is_utf8_json_keyed_by_repo(self, mgr, data_dir):
        mgr.set_stage("répo", "build", "failed", "1.0.0", log_lines=["échec ✗"])
        raw = json.loads(_state_file(data_dir).read_text(encoding="utf-8"))
        assert raw["répo"]["stages"]["build"]["last_log"] == ["échec ✗"]

    def test_corrupt_file_starts_fresh_instead_of_crashing(self, data_dir):
        _state_file(data_dir).write_text("{not json", encoding="utf-8")
        mgr = PipelineStateManager(data_dir=str(data_dir))
        assert list(mgr.items()) == []
        # And the next write replaces the corrupt file with a valid one
        mgr.set_skip_build("repo", False)
        assert "repo" in json.loads(_state_file(data_dir).read_text(encoding="utf-8"))

    def test_save_failure_is_logged_not_raised(self, mgr, data_dir, monkeypatch):
        """A full or read-only data volume must not break the pipeline itself."""
        from pathlib import Path

        def _boom(*args, **kwargs):
            raise OSError("No space left on device")

        monkeypatch.setattr(Path, "write_text", _boom)
        monkeypatch.setattr(Path, "write_bytes", _boom, raising=False)
        mgr.set_stage("repo", "build", "running", "1.0.0")  # must not raise
        assert mgr.get("repo").stages["build"].status == "running"

    def test_interrupted_write_keeps_previous_state_loadable(self, mgr, data_dir, monkeypatch):
        """A crash or full disk mid-write must not wipe every pipeline.

        A truncated file is unreadable, so the next start would begin from an
        empty state and forget every transition config -- including manual
        gates that hold production deploys back.
        """
        from pathlib import Path

        mgr.set_transition_config("repo", "test_to_deploy", {"mode": "manual"})
        real_write_text = Path.write_text

        def _partial_write(self, data, *args, **kwargs):
            real_write_text(self, data[: len(data) // 2], *args, **kwargs)
            raise OSError("No space left on device")

        monkeypatch.setattr(Path, "write_text", _partial_write)
        mgr.set_stage("repo", "build", "running", "1.0.1")  # save fails half-way
        monkeypatch.undo()

        reloaded = _reload(data_dir)
        assert reloaded.get_transition_config("repo", "test_to_deploy")["mode"] == "manual"

    def test_reset_clears_entry_and_persists(self, mgr, data_dir):
        mgr.set_pipeline("repo", "done", "success", "1.0.0")
        mgr.reset("repo")
        assert mgr.get("repo").overall_status == "idle"
        assert _reload(data_dir).get("repo").stages["deploy"].current_version is None

    def test_reset_unknown_repo_is_a_noop(self, mgr, data_dir):
        mgr.reset("ghost")
        assert "ghost" not in mgr
        assert not _state_file(data_dir).exists()

    def test_singleton_reset_for_tests(self, data_dir):
        PipelineStateManager.reset_instance()
        try:
            first = PipelineStateManager.get_instance(str(data_dir))
            assert PipelineStateManager.get_instance("/elsewhere") is first
        finally:
            PipelineStateManager.reset_instance()


# ---------------------------------------------------------------------------
# Log truncation
# ---------------------------------------------------------------------------

class TestLogTruncation:
    def _lines(self, n):
        return [f"line {i}" for i in range(n)]

    def test_set_stage_keeps_only_the_tail(self, mgr, data_dir):
        lines = self._lines(_MAX_LOG_LINES + 25)
        mgr.set_stage("repo", "build", "failed", "1.0.0", log_lines=lines)
        stored = _reload(data_dir).get("repo").stages["build"].last_log
        assert len(stored) == _MAX_LOG_LINES
        assert stored[0] == "line 25"
        assert stored[-1] == lines[-1]

    def test_done_stage_truncates_deploy_log(self, mgr):
        mgr.set_stage("repo", "done", "success", "1.0.0",
                      log_lines=self._lines(_MAX_LOG_LINES * 2))
        assert len(mgr.get("repo").stages["deploy"].last_log) == _MAX_LOG_LINES

    def test_update_log_truncates(self, mgr):
        mgr.get_or_create("repo")
        mgr.update_log("repo", "test", self._lines(_MAX_LOG_LINES + 1))
        log = mgr.get("repo").stages["test"].last_log
        assert len(log) == _MAX_LOG_LINES and log[0] == "line 1"

    def test_short_logs_are_kept_whole(self, mgr):
        mgr.set_stage("repo", "test", "success", "1.0.0", log_lines=["only"])
        assert mgr.get("repo").stages["test"].last_log == ["only"]

    def test_update_log_does_not_alias_caller_list(self, mgr):
        """The live BackgroundAction keeps appending to its own list."""
        mgr.get_or_create("repo")
        live = self._lines(3)
        mgr.update_log("repo", "build", live)
        live.append("later")
        assert mgr.get("repo").stages["build"].last_log == self._lines(3)

    def test_update_log_ignores_unknown_repo_and_stage(self, mgr, data_dir):
        mgr.update_log("ghost", "build", ["x"])
        assert "ghost" not in mgr
        mgr.get_or_create("repo")
        mgr.update_log("repo", "nonsense", ["x"])
        assert "nonsense" not in mgr.get("repo").stages


# ---------------------------------------------------------------------------
# Stage transitions
# ---------------------------------------------------------------------------

class TestStageTransitions:
    def test_success_sets_current_version(self, mgr):
        mgr.set_stage("repo", "build", "success", "1.0.0")
        stage = mgr.get("repo").stages["build"]
        assert stage.current_version == "1.0.0"
        assert mgr.get("repo").overall_status == "success"

    def test_failure_keeps_previous_current_version(self, mgr):
        mgr.set_stage("repo", "deploy", "success", "1.0.0")
        mgr.set_stage("repo", "deploy", "failed", "1.0.1")
        stage = mgr.get("repo").stages["deploy"]
        assert stage.current_version == "1.0.0"
        assert stage.desired_version == "1.0.1"

    def test_done_marks_deploy_success_and_timestamp(self, mgr):
        mgr.set_stage("repo", "done", "ignored", "2.0.0", action_id="d1")
        entry = mgr.get("repo")
        assert entry.current_stage == "done"
        assert entry.overall_status == "success"
        assert entry.last_deployed_at is not None
        assert entry.stages["deploy"].current_version == "2.0.0"
        assert entry.stages["deploy"].action_id == "d1"

    def test_set_pipeline_running_stamps_version_on_every_stage(self, mgr):
        mgr.set_pipeline("repo", "build", "running", "4.0.0")
        for stage in PipelineEntry.STAGES:
            assert mgr.get("repo").stages[stage].desired_version == "4.0.0"

    def test_set_pipeline_done_records_deploy(self, mgr):
        mgr.set_pipeline("repo", "done", "success", "1.2.3", deploy_id="d1")
        legacy = mgr.get_legacy("repo")
        assert legacy["stage"] == "done"
        assert legacy["deployed_version"] == "1.2.3"
        assert legacy["deploy_action_id"] == "d1"
        assert legacy["last_deployed_at"]

    def test_set_pipeline_qa_id(self, mgr):
        mgr.set_pipeline("repo", "qa", "running", "1.0.0", qa_id="q1")
        assert mgr.get_legacy("repo")["qa_action_id"] == "q1"
        mgr.set_pipeline("repo", "deploy", "running", "1.0.0", deploy_id="d1")
        assert mgr.get_legacy("repo")["qa_action_id"] == "q1"  # inherited

    def test_full_run_ends_in_done(self, mgr, data_dir):
        mgr.set_pipeline("repo", "build", "running", "1.0.0", build_id="b", test_id=None, deploy_id=None)
        mgr.set_stage("repo", "build", "success", "1.0.0", action_id="b")
        mgr.set_pipeline("repo", "test", "running", "1.0.0", test_id="t")
        mgr.set_pipeline("repo", "test", "success", "1.0.0", test_id="t")
        mgr.set_pipeline("repo", "deploy", "running", "1.0.0", deploy_id="d")
        mgr.set_pipeline("repo", "done", "success", "1.0.0", deploy_id="d")
        legacy = _reload(data_dir).get_legacy("repo")
        assert (legacy["stage"], legacy["status"]) == ("done", "success")
        assert [legacy[f"{s}_action_id"] for s in ("build", "test", "deploy")] == ["b", "t", "d"]
        assert legacy["stages"]["test"]["current_version"] == "1.0.0"


# ---------------------------------------------------------------------------
# Gates and transition configs
# ---------------------------------------------------------------------------

class TestGatesAndTransitions:
    def test_record_gate_requires_existing_entry(self, mgr):
        mgr.record_gate("ghost", "build_to_test", True, "ok")
        assert "ghost" not in mgr

    def test_record_gate_keeps_most_recent(self, mgr):
        mgr.get_or_create("repo")
        for i in range(_MAX_GATE_DECISIONS + 5):
            mgr.record_gate("repo", "build_to_test", i % 2 == 0, f"reason {i}")
        gates = mgr.get("repo").gates
        assert len(gates) == _MAX_GATE_DECISIONS
        assert gates[-1].reason == f"reason {_MAX_GATE_DECISIONS + 4}"
        assert gates[0].reason == "reason 5"

    def test_clear_gates(self, mgr, data_dir):
        mgr.get_or_create("repo")
        mgr.record_gate("repo", "build_to_test", False, "no")
        mgr.clear_gates("repo")
        assert _reload(data_dir).get("repo").gates == []

    @pytest.mark.parametrize("mode", ["auto", "auto_with_success", "agent", "manual"])
    def test_valid_modes_are_saved(self, mgr, mode):
        mgr.set_transition_config("repo", "build_to_test", {"mode": mode})
        assert mgr.get_transition_config("repo", "build_to_test") == {"mode": mode}

    def test_default_mode_is_auto_with_success(self, mgr):
        mgr.set_transition_config("repo", "build_to_test", {})
        assert mgr.get_transition_config("repo", "build_to_test")["mode"] == "auto_with_success"

    def test_invalid_transition_or_mode_is_ignored(self, mgr):
        mgr.set_transition_config("repo", "deploy_to_moon", {"mode": "auto"})
        mgr.set_transition_config("repo", "build_to_test", {"mode": "yolo"})
        assert "repo" not in mgr  # nothing was created
        assert mgr.get_transition_config("repo", "build_to_test") == {}

    def test_qa_enabled_only_on_test_to_deploy(self, mgr):
        mgr.set_transition_config("repo", "test_to_deploy", {"mode": "auto", "qa_enabled": 1})
        mgr.set_transition_config("repo", "build_to_test", {"mode": "auto", "qa_enabled": True})
        assert mgr.get_transition_config("repo", "test_to_deploy") == {"mode": "auto", "qa_enabled": True}
        assert mgr.get_transition_config("repo", "build_to_test") == {"mode": "auto"}

    def test_version_to_build_platforms(self, mgr):
        mgr.set_transition_config("repo", "version_to_build", {
            "mode": "manual", "multi_arch": True, "platforms": " linux/amd64,linux/arm64 ",
        })
        assert mgr.get_transition_config("repo", "version_to_build") == {
            "mode": "manual", "multi_arch": True, "platforms": "linux/amd64,linux/arm64",
        }

    def test_qa_to_deploy_is_accepted(self, mgr):
        mgr.set_transition_config("repo", "qa_to_deploy", {"mode": "auto"})
        assert mgr.get_transition_config("repo", "qa_to_deploy") == {"mode": "auto"}

    def test_reset_drops_transition_configs(self, mgr):
        mgr.set_transition_config("repo", "test_to_deploy", {"mode": "manual"})
        mgr.reset("repo")
        assert mgr.get_transition_config("repo", "test_to_deploy") == {}


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

class TestConcurrency:
    async def test_interleaved_pipelines_do_not_clobber_each_other(self, mgr, data_dir):
        """Many repos advancing through their stages at once, as the poller does."""
        repos = [f"repo{i}" for i in range(12)]

        async def _run(repo, version):
            mgr.set_pipeline(repo, "build", "running", version, build_id=f"b-{repo}")
            await asyncio.sleep(0)
            mgr.set_stage(repo, "build", "success", version, action_id=f"b-{repo}",
                          log_lines=[f"{repo} built"])
            await asyncio.sleep(0)
            mgr.set_pipeline(repo, "test", "running", version, test_id=f"t-{repo}")
            await asyncio.sleep(0)
            mgr.set_pipeline(repo, "test", "success", version, test_id=f"t-{repo}")
            await asyncio.sleep(0)
            mgr.set_pipeline(repo, "done", "success", version, deploy_id=f"d-{repo}")

        await asyncio.gather(*[_run(r, f"1.0.{i}") for i, r in enumerate(repos)])

        reloaded = _reload(data_dir)
        for i, repo in enumerate(repos):
            legacy = reloaded.get_legacy(repo)
            assert legacy["status"] == "success"
            assert legacy["deployed_version"] == f"1.0.{i}"
            assert legacy["build_action_id"] == f"b-{repo}"
            assert legacy["test_action_id"] == f"t-{repo}"
            assert legacy["deploy_action_id"] == f"d-{repo}"
            assert reloaded.get(repo).stages["build"].last_log == [f"{repo} built"]
