"""Restart durability of the agent actions queue and of pipeline runs.

Each "restart" is simulated by building a fresh ActionsQueue (or
PipelineStateManager) on the file the previous instance wrote.
"""

import asyncio
import json
from datetime import datetime, timedelta

import pytest

from backend.actions_queue import (
    REASON_EXPIRED,
    REASON_RESTART_EXPIRED,
    REASON_RESTART_IN_PROGRESS,
    ActionsQueue,
    ActionStatus,
    ActionType,
)
from backend.pipeline_state import INTERRUPTED_REASON, PipelineStateManager


def _run(coro):
    return asyncio.run(coro)


def _restart(path, **kwargs) -> ActionsQueue:
    queue = ActionsQueue(**kwargs)
    queue.enable_persistence(str(path))
    return queue


@pytest.fixture
def store(tmp_path):
    return tmp_path / "actions_queue.json"


# ---------------------------------------------------------------------------
# Actions queue
# ---------------------------------------------------------------------------

class TestActionsQueuePersistence:

    def test_in_memory_queue_writes_nothing(self, tmp_path):
        queue = ActionsQueue()
        _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        assert not queue.persistent
        assert list(tmp_path.iterdir()) == []

    def test_pending_action_survives_restart_and_is_delivered(self, store):
        queue = _restart(store)
        action = _run(queue.create_action("agent-a", ActionType.CONTAINER_ACTION,
                                          {"container_id": "c1", "action": "restart"}))

        restarted = _restart(store)
        delivered = _run(restarted.get_pending_actions("agent-a"))

        assert [a.id for a in delivered] == [action.id]
        assert delivered[0].payload == {"container_id": "c1", "action": "restart"}
        assert delivered[0].status == ActionStatus.IN_PROGRESS

    def test_delivered_action_is_not_handed_out_twice_across_restart(self, store):
        queue = _restart(store)
        _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        assert len(_run(queue.get_pending_actions("agent-a"))) == 1

        restarted = _restart(store)
        assert _run(restarted.get_pending_actions("agent-a")) == []

    def test_in_progress_action_is_failed_with_reason_on_restart(self, store):
        queue = _restart(store)
        action = _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        _run(queue.get_pending_actions("agent-a"))

        restarted = _restart(store)
        reloaded = _run(restarted.get_action(action.id))
        assert reloaded.status == ActionStatus.FAILED
        assert reloaded.success is False
        assert reloaded.reason == REASON_RESTART_IN_PROGRESS
        assert reloaded.completed_at is not None
        # A waiter on a resolved action returns at once instead of timing out.
        waited = _run(restarted.wait_for_action(action.id, timeout=0.01))
        assert waited.status == ActionStatus.FAILED

    def test_late_agent_result_supersedes_restart_failure(self, store):
        queue = _restart(store)
        action = _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        _run(queue.get_pending_actions("agent-a"))

        restarted = _restart(store)
        done = _run(restarted.complete_action(action.id, True, "uid=0", agent_id="agent-a"))
        assert done.status == ActionStatus.COMPLETED
        assert done.success is True
        assert done.reason is None

    def test_late_result_still_bound_to_owning_agent(self, store):
        queue = _restart(store)
        action = _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))

        restarted = _restart(store)
        assert _run(restarted.complete_action(action.id, True, "forged", agent_id="agent-b")) is None

    def test_pending_action_past_ttl_expires_on_restart(self, store):
        queue = _restart(store, action_timeout_seconds=60)
        action = _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        # Age the stored action beyond the TTL, as if the backend was down a while.
        raw = json.loads(store.read_text(encoding="utf-8"))
        raw["actions"][0]["created_at"] = (datetime.utcnow() - timedelta(seconds=120)).isoformat()
        store.write_text(json.dumps(raw), encoding="utf-8")

        restarted = _restart(store, action_timeout_seconds=60)
        reloaded = _run(restarted.get_action(action.id))
        assert reloaded.status == ActionStatus.EXPIRED
        assert reloaded.reason == REASON_RESTART_EXPIRED
        assert _run(restarted.get_pending_actions("agent-a")) == []

    def test_ttl_is_measured_from_original_creation(self, store):
        """A reload does not reset the clock: expiry still applies at poll time."""
        queue = _restart(store, action_timeout_seconds=60)
        action = _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        restarted = _restart(store, action_timeout_seconds=60)
        restarted._actions[action.id].created_at -= timedelta(seconds=61)

        assert _run(restarted.get_pending_actions("agent-a")) == []
        expired = _run(restarted.get_action(action.id))
        assert expired.status == ActionStatus.EXPIRED
        assert expired.reason == REASON_EXPIRED

    def test_results_are_not_written_to_disk(self, store):
        queue = _restart(store)
        action = _run(queue.create_action("agent-a", ActionType.GET_ENV, {"container_id": "c1"}))
        _run(queue.get_pending_actions("agent-a"))
        _run(queue.complete_action(action.id, True, "DB_PASSWORD=hunter2", agent_id="agent-a"))

        assert "hunter2" not in store.read_text(encoding="utf-8")
        # Still served from memory to the in-process waiter.
        assert _run(queue.get_action(action.id)).result == "DB_PASSWORD=hunter2"

    def test_agents_last_seen_survives_restart(self, store):
        queue = _restart(store)
        _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        _run(queue.get_pending_actions("agent-a"))

        restarted = _restart(store)
        assert [a.agent_id for a in _run(restarted.get_agents())] == ["agent-a"]
        assert _run(restarted.is_agent_online("agent-a"))

    def test_finished_actions_are_pruned_after_retention(self, store):
        queue = _restart(store, retention_seconds=300)
        old = _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        _run(queue.get_pending_actions("agent-a"))
        _run(queue.complete_action(old.id, True, "ok", agent_id="agent-a"))
        queue._actions[old.id].completed_at -= timedelta(seconds=301)

        _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))

        assert _run(queue.get_action(old.id)) is None
        stored_ids = [a["id"] for a in json.loads(store.read_text(encoding="utf-8"))["actions"]]
        assert old.id not in stored_ids

    @pytest.mark.parametrize("content", ["", "{not json", '{"version": 99, "actions": []}'])
    def test_unreadable_store_starts_empty(self, store, content):
        store.write_text(content, encoding="utf-8")
        queue = _restart(store)
        assert _run(queue.get_agents()) == []
        # And the next save replaces the bad file with a valid one.
        _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        assert json.loads(store.read_text(encoding="utf-8"))["version"] == 1

    def test_save_failure_keeps_queue_working(self, tmp_path):
        blocker = tmp_path / "not-a-dir"
        blocker.write_text("x", encoding="utf-8")
        queue = ActionsQueue()
        queue.enable_persistence(str(blocker / "actions_queue.json"))
        action = _run(queue.create_action("agent-a", ActionType.EXEC, {"command": ["id"]}))
        assert [a.id for a in _run(queue.get_pending_actions("agent-a"))] == [action.id]

    def test_poll_response_shape_is_unchanged(self, store):
        """The agent reads id/type/payload from each action of the poll response."""
        queue = _restart(store)
        _run(queue.create_action("agent-a", ActionType.GET_LOGS, {"container_id": "c1", "tail": 10}))
        restarted = _restart(store)
        dumped = [a.model_dump() for a in _run(restarted.get_pending_actions("agent-a"))]
        assert dumped[0]["type"] == ActionType.GET_LOGS
        assert dumped[0]["payload"] == {"container_id": "c1", "tail": 10}
        assert isinstance(dumped[0]["id"], str)


# ---------------------------------------------------------------------------
# Pipeline runs
# ---------------------------------------------------------------------------

class TestPipelineInterruptedByRestart:

    def test_running_stage_is_marked_failed_on_reload(self, tmp_path):
        mgr = PipelineStateManager(str(tmp_path))
        mgr.set_pipeline("repo", "build", "success", "1.2.0", build_id="b1")
        mgr.set_pipeline("repo", "test", "running", "1.2.0", test_id="t1")

        restored = PipelineStateManager(str(tmp_path))
        legacy = restored.get_legacy("repo")
        assert legacy["status"] == "failed"
        assert legacy["stage"] == "test"
        assert legacy["status_reason"] == INTERRUPTED_REASON
        assert legacy["stages"]["test"]["status"] == "failed"
        assert legacy["stages"]["build"]["status"] == "success"
        assert restored.get("repo").stages["test"].last_log[-1].endswith(INTERRUPTED_REASON)

        # Persisted: a second restart sees the same, already-resolved state.
        again = PipelineStateManager(str(tmp_path))
        assert again.get_legacy("repo")["status"] == "failed"

    def test_finished_runs_are_left_alone(self, tmp_path):
        mgr = PipelineStateManager(str(tmp_path))
        mgr.set_pipeline("ok", "done", "success", "1.0.0")
        mgr.set_pipeline("rejected", "test", "gate_rejected", "1.0.0")

        restored = PipelineStateManager(str(tmp_path))
        assert restored.get_legacy("ok")["status"] == "success"
        assert restored.get_legacy("rejected")["status"] == "gate_rejected"
        assert restored.get_legacy("ok")["status_reason"] is None

    def test_new_run_clears_the_interruption_reason(self, tmp_path):
        mgr = PipelineStateManager(str(tmp_path))
        mgr.set_pipeline("repo", "deploy", "running", "2.0.0")
        restored = PipelineStateManager(str(tmp_path))
        assert restored.get("repo").status_reason == INTERRUPTED_REASON

        restored.set_pipeline("repo", "build", "running", "2.0.1")
        assert restored.get("repo").status_reason is None
        assert restored.get_legacy("repo")["status"] == "running"

    def test_save_is_atomic_and_leaves_no_temp_files(self, tmp_path):
        mgr = PipelineStateManager(str(tmp_path))
        mgr.set_pipeline("repo", "build", "running", "1.0.0")
        assert [p.name for p in tmp_path.iterdir()] == ["pipeline_state.json"]
