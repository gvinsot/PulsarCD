"""Integration tests for the FastAPI backend — all infrastructure mocked."""

import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import backend.api as api_module
from backend.auth import create_token


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_action(action_type: str = "build", repo: str = "myrepo") -> tuple:
    """Create and register a BackgroundAction, return (action_id, action)."""
    from backend.api import BackgroundAction
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, action_type, repo)
    api_module._background_actions[action_id] = action
    return action_id, action


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_public(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"

    def test_health_no_auth_required(self, client):
        """Health endpoint must be reachable without a token."""
        resp = client.get("/api/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

class TestAuth:
    def test_login_valid(self, client):
        resp = client.post("/api/auth/login", json={"username": "testuser", "password": "testpass"})
        assert resp.status_code == 200
        data = resp.json()
        assert "token" in data

    def test_login_wrong_password(self, client):
        resp = client.post("/api/auth/login", json={"username": "testuser", "password": "wrong"})
        assert resp.status_code == 401

    def test_login_wrong_username(self, client):
        resp = client.post("/api/auth/login", json={"username": "nobody", "password": "testpass"})
        assert resp.status_code == 401

    def test_protected_without_token(self, client):
        resp = client.get("/api/containers")
        assert resp.status_code == 401

    def test_protected_with_invalid_token(self, client):
        resp = client.get("/api/containers", headers={"Authorization": "Bearer bogus"})
        assert resp.status_code == 401

    def test_protected_with_valid_token(self, client, auth_headers):
        resp = client.get("/api/containers", headers=auth_headers)
        # 200 or at least not 401/403
        assert resp.status_code != 401
        assert resp.status_code != 403

    def test_token_in_query_param(self, client, auth_token):
        """Tokens via query param are accepted (for SSE streams)."""
        resp = client.get(f"/api/health?token={auth_token}")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Action logs endpoint
# ---------------------------------------------------------------------------

class TestActionLogs:
    def test_unknown_action_returns_404(self, client, auth_headers):
        resp = client.get("/api/stacks/actions/nonexistent/logs", headers=auth_headers)
        assert resp.status_code == 404

    def test_action_logs_empty(self, client, auth_headers):
        action_id, _ = _fresh_action()
        resp = client.get(f"/api/stacks/actions/{action_id}/logs", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["action_id"] == action_id
        assert data["status"] == "running"
        assert data["lines"] == []
        assert data["total_lines"] == 0

    def test_action_logs_with_output(self, client, auth_headers):
        action_id, action = _fresh_action()
        action.append_output("line 1")
        action.append_output("line 2")
        resp = client.get(f"/api/stacks/actions/{action_id}/logs", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["lines"] == ["line 1", "line 2"]
        assert data["total_lines"] == 2

    def test_action_logs_offset(self, client, auth_headers):
        action_id, action = _fresh_action()
        for i in range(5):
            action.append_output(f"line {i}")
        resp = client.get(f"/api/stacks/actions/{action_id}/logs?offset=3", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["lines"] == ["line 3", "line 4"]
        assert data["offset"] == 3

    def test_action_logs_offset_beyond_end(self, client, auth_headers):
        action_id, action = _fresh_action()
        action.append_output("only line")
        resp = client.get(f"/api/stacks/actions/{action_id}/logs?offset=999", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["lines"] == []

    def test_action_status_completed(self, client, auth_headers):
        action_id, action = _fresh_action()
        action.status = "completed"
        resp = client.get(f"/api/stacks/actions/{action_id}/logs", headers=auth_headers)
        assert resp.json()["status"] == "completed"

    def test_action_logs_require_auth(self, client):
        action_id, _ = _fresh_action()
        resp = client.get(f"/api/stacks/actions/{action_id}/logs")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Action status endpoint
# ---------------------------------------------------------------------------

class TestActionStatus:
    def test_unknown_action_returns_404(self, client, auth_headers):
        resp = client.get("/api/stacks/actions/doesnotexist/status", headers=auth_headers)
        assert resp.status_code == 404

    def test_action_status_fields(self, client, auth_headers):
        action_id, action = _fresh_action("deploy", "testrepo")
        resp = client.get(f"/api/stacks/actions/{action_id}/status", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["action_id"] == action_id
        assert data["action_type"] == "deploy"
        assert data["repo"] == "testrepo"
        assert data["status"] == "running"
        assert "started_at" in data
        assert "elapsed_seconds" in data

    def test_action_status_require_auth(self, client):
        action_id, _ = _fresh_action()
        resp = client.get(f"/api/stacks/actions/{action_id}/status")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Pipeline state helpers
# ---------------------------------------------------------------------------

class TestPipelineHelpers:
    def _fresh_manager(self, tmp_path=None):
        """Create a fresh PipelineStateManager for testing."""
        from backend.pipeline_state import PipelineStateManager
        PipelineStateManager.reset_instance()
        import tempfile
        d = tmp_path or tempfile.mkdtemp()
        mgr = PipelineStateManager(data_dir=str(d))
        return mgr

    def test_set_pipeline_basic(self):
        mgr = self._fresh_manager()
        mgr.set_pipeline("repo1", "build", "running", "1.0.0", build_id="abc")
        state = mgr.get_legacy("repo1")
        assert state["stage"] == "build"
        assert state["status"] == "running"
        assert state["version"] == "1.0.0"
        assert state["build_action_id"] == "abc"
        assert state["test_action_id"] is None
        assert state["deploy_action_id"] is None

    def test_set_pipeline_inherits_previous_ids(self):
        mgr = self._fresh_manager()
        mgr.set_pipeline("repo2", "build", "success", "1.0.0", build_id="build-1")
        mgr.set_pipeline("repo2", "test", "running", "1.0.0", test_id="test-1")
        state = mgr.get_legacy("repo2")
        assert state["build_action_id"] == "build-1"  # preserved
        assert state["test_action_id"] == "test-1"    # newly set
        assert state["deploy_action_id"] is None      # preserved as None

    def test_set_pipeline_explicit_none_clears(self):
        mgr = self._fresh_manager()
        mgr.set_pipeline("repo3", "build", "success", "1.0.0", build_id="old-build")
        # Explicitly pass build_id=None to clear it (tag-based deploy scenario)
        mgr.set_pipeline("repo3", "deploy", "running", "1.0.0",
                         build_id=None, deploy_id="dep-1")
        state = mgr.get_legacy("repo3")
        assert state["build_action_id"] is None  # explicitly cleared

    def test_get_swarm_manager_host_none_when_no_hosts(self, client):
        # client fixture ensures settings is initialised via test lifespan
        original = api_module.settings.hosts
        api_module.settings.hosts = []
        try:
            result = api_module._get_swarm_manager_host()
            assert result is None
        finally:
            api_module.settings.hosts = original

    def test_get_swarm_manager_host_finds_manager(self, client):
        mock_host = MagicMock()
        mock_host.swarm_manager = True
        mock_host.name = "manager-node"
        original = api_module.settings.hosts
        api_module.settings.hosts = [mock_host]
        try:
            result = api_module._get_swarm_manager_host()
            assert result == "manager-node"
        finally:
            api_module.settings.hosts = original


# ---------------------------------------------------------------------------
# Config endpoint
# ---------------------------------------------------------------------------

class TestConfig:
    def test_config_requires_auth(self, client):
        resp = client.get("/api/config")
        assert resp.status_code == 401

    def test_config_returns_data(self, client, auth_headers):
        resp = client.get("/api/config", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "hosts" in data


# ---------------------------------------------------------------------------
# BackgroundAction class
# ---------------------------------------------------------------------------

class TestBackgroundAction:
    def test_initial_status(self):
        from backend.api import BackgroundAction
        action = BackgroundAction("test-id", "build", "myrepo")
        assert action.status == "running"
        assert action.output_lines == []
        assert action.result is None

    def test_append_output(self):
        from backend.api import BackgroundAction
        action = BackgroundAction("test-id", "build", "myrepo")
        action.append_output("hello")
        action.append_output("world")
        assert action.get_output() == "hello\nworld"

    def test_status_setter_triggers_event(self):
        from backend.api import BackgroundAction
        action = BackgroundAction("test-id", "build", "myrepo")
        assert not action.new_line_event.is_set()
        action.status = "completed"
        assert action.new_line_event.is_set()

    def test_cancel_event_initially_unset(self):
        from backend.api import BackgroundAction
        action = BackgroundAction("test-id", "build", "myrepo")
        assert not action.cancel_event.is_set()
