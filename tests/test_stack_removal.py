"""Stack removal clears persisted runtime results and stale deployed tags."""

from unittest.mock import AsyncMock

import pytest

from backend import api
from backend.pipeline_state import PipelineStateManager
from backend.routes import containers, stacks


@pytest.fixture
def removal(client, monkeypatch, tmp_path):
    manager = PipelineStateManager(str(tmp_path))
    for repo in ("My_App", "other"):
        for stage in ("build", "test", "qa", "deploy"):
            manager.set_stage(repo, stage, "success", "1.2.3",
                              action_id=f"{repo}-{stage}", log_lines=["success"])
        manager.set_stage(repo, "done", "success", "1.2.3")
        manager.set_transition_config(repo, "test_to_deploy", {
            "mode": "manual", "qa_enabled": True,
        })
        manager.record_gate(repo, "test_to_deploy", True, "approved")
    manager.get("My_App").last_rollback = {"status": "success"}
    manager.get("My_App").status_reason = "old reason"
    monkeypatch.setattr(api, "pipeline_state", manager)
    host = AsyncMock()
    host.remove_stack.return_value = (True, "Removed")
    monkeypatch.setattr(api.collector, "clients", {"manager": host})
    monkeypatch.setattr(containers, "_get_swarm_manager_host", lambda: "manager")
    monkeypatch.setattr(stacks, "_deployed_tags_cache", {
        "tags": {"My_App": "1.2.3"}, "qa_tags": {}, "latest_built": {"My_App": "1.2.3"},
    })
    monkeypatch.setattr(stacks, "_deployed_tags_cache_time", 123)
    return manager, host, tmp_path


@pytest.mark.parametrize("metadata", [True, False])
def test_remove_clears_runtime_results_but_keeps_build_and_settings(
    client, auth_headers, removal, metadata,
):
    manager, host, data_dir = removal
    if metadata:
        manager.set_project_info("My_App", "My project", "custom-stack")
    stack = "custom-stack" if metadata else "my-app"
    before = manager.get("My_App").to_dict()
    other = manager.get("other").to_dict()

    response = client.post(f"/api/stacks/{stack}/remove", headers=auth_headers)
    assert response.status_code == 200
    host.remove_stack.assert_awaited_once_with(stack)
    assert stacks._deployed_tags_cache is None
    assert stacks._deployed_tags_cache_time is None

    reloaded = PipelineStateManager(str(data_dir))
    entry = reloaded.get("My_App")
    assert entry.stages["build"].to_dict() == before["stages"]["build"]
    assert entry.transition_configs == before["transition_configs"]
    assert entry.project_name == before["project_name"]
    assert entry.stack_name == before["stack_name"]
    assert entry.current_stage is None
    assert entry.overall_status == "idle"
    assert entry.last_deployed_at is None
    assert entry.last_rollback is None
    assert entry.status_reason is None
    assert entry.gates == []
    for stage in ("test", "qa", "deploy"):
        result = entry.stages[stage]
        assert result.status == "idle"
        assert result.current_version is None
        assert result.desired_version is None
        assert result.action_id is None
        assert result.last_log == []
    assert reloaded.get("other").to_dict() == other
    status = client.get("/api/stacks/pipeline/status", headers=auth_headers).json()
    assert status["pipelines"]["My_App"] == reloaded.get_legacy("My_App")
    assert status["pipelines"]["My_App"]["deployed_version"] is None


def test_failed_removal_preserves_state_and_cache(client, auth_headers, removal):
    manager, host, _ = removal
    before = manager.get("My_App").to_dict()
    cache = stacks._deployed_tags_cache
    host.remove_stack.return_value = (False, "Docker unavailable")

    response = client.post("/api/stacks/my-app/remove", headers=auth_headers)

    assert response.status_code == 500
    assert manager.get("My_App").to_dict() == before
    assert stacks._deployed_tags_cache == cache
    assert stacks._deployed_tags_cache_time == 123


def test_remove_without_pipeline_still_invalidates_tags(client, auth_headers, removal):
    manager, _, _ = removal
    before = manager.get_all_legacy()
    response = client.post("/api/stacks/untracked/remove", headers=auth_headers)
    assert response.status_code == 200
    assert manager.get_all_legacy() == before
    assert stacks._deployed_tags_cache is None
