"""Integration tests for the FastAPI backend — all infrastructure mocked."""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
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

    def test_health_payload_is_minimal(self, client):
        """The only public endpoint must not disclose the deployment."""
        data = client.get("/api/health").json()
        assert set(data) <= {"status", "service", "opensearch"}
        assert data["service"] == "pulsarcd"
        # No version strings and no per-index document counts
        assert not [k for k in data if "version" in k or k.endswith("_docs")]

    def test_health_subpaths_are_not_exempt(self, client):
        """Only the exact /api/health path is public."""
        resp = client.get("/api/health/opensearch")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# OpenSearch probe (deep diagnostic, admin only)
# ---------------------------------------------------------------------------

def _fake_opensearch_for_probe():
    """Minimal OpenSearch double covering every call made by the probe."""
    fake_client = AsyncMock()
    fake_client.info.return_value = {
        "cluster_name": "test-cluster",
        "version": {"number": "2.11.0", "distribution": "opensearch"},
    }
    fake_client.cat.indices = AsyncMock(return_value=[])
    fake_client.index.return_value = {"result": "created", "_index": "logs", "_version": 1}
    fake_client.bulk.return_value = {"errors": False, "items": [{"index": {"status": 201}}]}
    fake_client.get.return_value = {"found": True}
    fake_client.delete.return_value = {}
    fake_client.search.return_value = {
        "hits": {
            "total": {"value": 42},
            "hits": [{"_source": {"message": "super-secret-production-log", "host": "prod-1"}}],
        }
    }
    fake_os = MagicMock()
    fake_os._client = fake_client
    fake_os.logs_index = "pulsarcd-logs"
    return fake_os


class TestOpenSearchProbe:
    """The deep probe writes documents and enumerates indices: admin only."""

    PROBE_PATH = "/api/admin/opensearch-probe"

    def test_probe_requires_authentication(self, client):
        resp = client.get(self.PROBE_PATH)
        assert resp.status_code == 401

    def test_probe_rejects_viewer(self, client):
        resp = client.get(self.PROBE_PATH, headers=_role_headers("viewer"))
        assert resp.status_code == 403
        assert resp.json()["detail"] == "Admin access required"

    def test_probe_reachable_by_admin(self, client):
        with patch.object(api_module, "opensearch", None):
            resp = client.get(self.PROBE_PATH, headers=_role_headers("admin"))
        assert resp.status_code == 200
        assert resp.json()["error"] == "OpenSearch client not configured"

    def test_probe_does_not_return_log_content(self, client):
        """Log lines are replaced by a count, even behind the admin gate."""
        with patch.object(api_module, "opensearch", _fake_opensearch_for_probe()):
            resp = client.get(self.PROBE_PATH, headers=_role_headers("admin"))
        assert resp.status_code == 200
        search_test = resp.json()["search_test"]
        assert search_test["status"] == "ok"
        assert search_test["total_docs"] == 42
        assert search_test["sample_count"] == 1
        assert "sample_hits" not in search_test
        assert "super-secret-production-log" not in resp.text


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


# ---------------------------------------------------------------------------
# Token transport: ?token= is restricted to the streaming endpoints
# ---------------------------------------------------------------------------

class TestTokenInQueryParam:
    """A JWT in the URL leaks into proxy logs, history and Referer headers."""

    def test_query_token_rejected_on_regular_route(self, client, auth_token):
        resp = client.get(f"/api/containers?token={auth_token}")
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Not authenticated"

    def test_query_token_rejected_on_admin_route(self, client, auth_token):
        resp = client.get(f"/api/admin/users?token={auth_token}")
        assert resp.status_code == 401

    def test_header_still_works_on_regular_route(self, client, auth_headers):
        resp = client.get("/api/containers", headers=auth_headers)
        assert resp.status_code == 200

    def test_query_token_accepted_on_sse_stream(self, client, auth_token):
        """EventSource cannot set headers: the log stream keeps accepting ?token=."""
        action_id, action = _fresh_action()
        action.status = "completed"  # makes the generator finish immediately
        resp = client.get(
            f"/api/stacks/actions/{action_id}/logs/stream?offset=0&token={auth_token}"
        )
        assert resp.status_code == 200
        assert '"type": "done"' in resp.text

    def test_sse_stream_without_token_is_rejected(self, client):
        action_id, action = _fresh_action()
        action.status = "completed"
        resp = client.get(f"/api/stacks/actions/{action_id}/logs/stream")
        assert resp.status_code == 401


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


# ---------------------------------------------------------------------------
# Role-based access control (viewer = read-only)
# ---------------------------------------------------------------------------

def _role_headers(role: str, username: str = "roletest") -> dict:
    """Build an Authorization header for a token carrying the given role."""
    token = create_token(username, api_module.settings.auth.jwt_secret, 1, role=role)
    return {"Authorization": f"Bearer {token}"}


class TestRoleBasedAccess:
    """A "viewer" token must not be able to trigger destructive actions."""

    # Mutating routes that must be admin-only.
    MUTATING_ROUTES = [
        ("post", "/api/stacks/build"),
        ("post", "/api/stacks/deploy"),
        ("post", "/api/stacks/test"),
        ("post", "/api/stacks/pipeline"),
        ("post", "/api/stacks/myrepo/remove"),
        ("post", "/api/services/mysvc/remove"),
        ("post", "/api/services/mysvc/update-image"),
        ("post", "/api/containers/action"),
        ("post", "/api/hosts/myhost/action"),
        ("post", "/api/tasks/create"),
        ("post", "/api/stacks/actions/abc123/cancel"),
        ("put", "/api/stacks/myrepo/env"),
        ("put", "/api/stacks/pipeline/myrepo/transition/build"),
    ]

    # Read-only routes that leak secrets or infrastructure details.
    ADMIN_ONLY_GET_ROUTES = [
        "/api/config",
        "/api/config/test",
        "/api/stacks/myrepo/env",
        "/api/containers/myhost/abc123/env",
    ]

    @pytest.mark.parametrize("method,path", MUTATING_ROUTES)
    def test_viewer_cannot_mutate(self, client, method, path):
        resp = getattr(client, method)(path, json={}, headers=_role_headers("viewer"))
        assert resp.status_code == 403, f"{method.upper()} {path} -> {resp.status_code}"
        assert resp.json()["detail"] == "Admin access required"

    @pytest.mark.parametrize("path", ADMIN_ONLY_GET_ROUTES)
    def test_viewer_cannot_read_sensitive_routes(self, client, path):
        resp = client.get(path, headers=_role_headers("viewer"))
        assert resp.status_code == 403, f"GET {path} -> {resp.status_code}"
        assert resp.json()["detail"] == "Admin access required"

    def test_viewer_can_still_read(self, client):
        """Read-only routes stay reachable for viewers."""
        resp = client.get("/api/containers", headers=_role_headers("viewer"))
        assert resp.status_code == 200

    def test_viewer_allowlisted_log_post_is_allowed(self, client):
        """POST log-search endpoints are read-only and remain open to viewers."""
        headers = _role_headers("viewer")

        resp = client.post("/api/logs/similar-count", json={}, headers=headers)
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

        resp = client.post("/api/logs/ai-analyze", json={}, headers=headers)
        assert resp.status_code == 200
        assert resp.json()["severity"] == "normal"

        # Missing question -> handler-level 400, proving the request was not blocked
        resp = client.post("/api/logs/ai-search", json={}, headers=headers)
        assert resp.status_code == 400

    def test_task_creation_is_not_allowlisted(self, client):
        """/api/tasks/create triggers the LLM agent: viewers must be rejected."""
        resp = client.post("/api/tasks/create", json={}, headers=_role_headers("viewer"))
        assert resp.status_code == 403

    def test_admin_passes_the_rbac_gate(self, client):
        """The same route reaches its handler with an admin token."""
        resp = client.post("/api/tasks/create", json={}, headers=_role_headers("admin"))
        # Handler-level validation error, not the middleware 403
        assert resp.status_code == 400

    def test_admin_can_read_sensitive_routes(self, client):
        resp = client.get("/api/config", headers=_role_headers("admin"))
        assert resp.status_code == 200
        assert "hosts" in resp.json()

    def test_viewer_cannot_reach_admin_prefix(self, client):
        resp = client.get("/api/admin/users", headers=_role_headers("viewer"))
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Agent API authentication (shared key vs per-agent keys)
# ---------------------------------------------------------------------------

def _agent_headers(key: str = "test-agent-key") -> dict:
    return {"Authorization": f"Bearer {key}"}


@pytest.fixture
def per_agent_keys():
    """Configure per-agent keys for the duration of a test."""
    auth = api_module.settings.auth
    previous = auth.agent_keys
    auth.agent_keys = {"agent-a": "key-a", "agent-b": "key-b"}
    yield auth.agent_keys
    auth.agent_keys = previous


class TestAgentSharedKey:
    """Default deployment: one shared key, accepted only on agent-side routes."""

    def test_poll_actions_with_agent_key(self, client):
        resp = client.get("/api/agent/actions?agent_id=agent-a", headers=_agent_headers())
        assert resp.status_code == 200
        assert resp.json()["agent_id"] == "agent-a"

    def test_poll_actions_without_key(self, client):
        resp = client.get("/api/agent/actions?agent_id=agent-a")
        assert resp.status_code == 401

    def test_poll_actions_with_wrong_key(self, client):
        resp = client.get("/api/agent/actions?agent_id=agent-a", headers=_agent_headers("nope"))
        assert resp.status_code == 401

    def test_system_error_with_agent_key(self, client):
        resp = client.post(
            "/api/agent/system-error",
            json={"agent_id": "agent-a", "error": "boom"},
            headers=_agent_headers(),
        )
        assert resp.status_code == 200


class TestAgentActionCreation:
    """POST /api/agent/action queues `exec`: it must require an admin JWT."""

    EXEC_PATH = "/api/agent/action?agent_id=agent-a&action_type=exec&container_id=c1&command=id"

    def test_agent_key_cannot_create_actions(self, client):
        """The fleet-wide agent key no longer grants remote command execution."""
        resp = client.post(self.EXEC_PATH, headers=_agent_headers())
        assert resp.status_code == 401

    def test_unauthenticated_cannot_create_actions(self, client):
        resp = client.post(self.EXEC_PATH)
        assert resp.status_code == 401

    def test_viewer_cannot_create_actions(self, client):
        resp = client.post(self.EXEC_PATH, headers=_role_headers("viewer"))
        assert resp.status_code == 403
        assert resp.json()["detail"] == "Admin access required"

    def test_admin_passes_the_gate(self, client):
        """An admin JWT reaches the handler (rejected there on the action type)."""
        resp = client.post(
            "/api/agent/action?agent_id=agent-a&action_type=bogus",
            headers=_role_headers("admin"),
        )
        assert resp.status_code == 400


class TestPerAgentKeys:
    """With per-agent keys, a key only works for the agent it belongs to."""

    def test_matching_key_is_accepted(self, client, per_agent_keys):
        resp = client.get("/api/agent/actions?agent_id=agent-a", headers=_agent_headers("key-a"))
        assert resp.status_code == 200

    def test_key_of_another_agent_is_rejected(self, client, per_agent_keys):
        """agent-a's key must not let it poll agent-b's action queue."""
        resp = client.get("/api/agent/actions?agent_id=agent-b", headers=_agent_headers("key-a"))
        assert resp.status_code == 401

    def test_shared_key_no_longer_sufficient(self, client, per_agent_keys):
        resp = client.get("/api/agent/actions?agent_id=agent-a", headers=_agent_headers())
        assert resp.status_code == 401

    def test_unknown_agent_id_is_rejected(self, client, per_agent_keys):
        resp = client.get("/api/agent/actions?agent_id=ghost", headers=_agent_headers("key-a"))
        assert resp.status_code == 401

    def test_result_is_bound_to_the_agent(self, client, per_agent_keys):
        resp = client.post(
            "/api/agent/result?agent_id=agent-b&action_id=x&success=true",
            headers=_agent_headers("key-a"),
        )
        assert resp.status_code == 401

    def test_body_agent_id_is_checked(self, client, per_agent_keys):
        """system-error carries agent_id in the body, not the query string."""
        resp = client.post(
            "/api/agent/system-error",
            json={"agent_id": "agent-b", "error": "boom"},
            headers=_agent_headers("key-a"),
        )
        assert resp.status_code == 401

    def test_body_agent_id_match_reaches_handler(self, client, per_agent_keys):
        """Reading the body in the middleware must not break the handler."""
        resp = client.post(
            "/api/agent/system-error",
            json={"agent_id": "agent-a", "error": "boom"},
            headers=_agent_headers("key-a"),
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Login rate limiting (per account + per client address)
# ---------------------------------------------------------------------------

LOGIN_PATH = "/api/auth/login"
GOOD_CREDENTIALS = {"username": "testuser", "password": "testpass"}


@pytest.fixture
def clean_login_attempts():
    """Isolate a test from the module-level rate-limit buckets."""
    api_module._login_attempts.clear()
    yield api_module._login_attempts
    api_module._login_attempts.clear()


class TestLoginRateLimiting:
    """Failures are counted per account, with a much higher per-address cap."""

    def test_account_locks_after_max_attempts(self, client, clean_login_attempts):
        for _ in range(api_module._LOGIN_MAX_ATTEMPTS):
            resp = client.post(LOGIN_PATH, json={"username": "testuser", "password": "wrong"})
            assert resp.status_code == 401
        # Even the right password is refused while the account bucket is full
        resp = client.post(LOGIN_PATH, json=GOOD_CREDENTIALS)
        assert resp.status_code == 429

    def test_locking_one_account_does_not_lock_the_others(self, client, clean_login_attempts):
        """Behind a proxy every request shares one address: no global lockout."""
        for _ in range(api_module._LOGIN_MAX_ATTEMPTS + 2):
            client.post(LOGIN_PATH, json={"username": "victim", "password": "wrong"})
        resp = client.post(LOGIN_PATH, json=GOOD_CREDENTIALS)
        assert resp.status_code == 200
        assert "token" in resp.json()

    def test_a_third_party_cannot_lock_an_account_out(self, client, clean_login_attempts,
                                                      monkeypatch):
        """The account counter must not be a remote lockout switch.

        Keyed on the username alone, five bad passwords per minute from any
        source refused the real account holder -- with the correct password, and
        before it was even checked, so the counter could never be cleared.
        """
        monkeypatch.setenv("PULSARCD_TRUST_PROXY_HEADERS", "true")
        for _ in range(api_module._LOGIN_MAX_ATTEMPTS + 3):
            resp = client.post(LOGIN_PATH,
                               json={"username": "testuser", "password": "wrong"},
                               headers={"X-Forwarded-For": "10.0.0.1"})
            assert resp.status_code in (401, 429)
        # The attacker's own source is locked...
        assert client.post(LOGIN_PATH, json=GOOD_CREDENTIALS,
                           headers={"X-Forwarded-For": "10.0.0.1"}).status_code == 429
        # ...the legitimate holder, from elsewhere, is not.
        resp = client.post(LOGIN_PATH, json=GOOD_CREDENTIALS,
                           headers={"X-Forwarded-For": "10.0.0.2"})
        assert resp.status_code == 200, resp.text
        assert "token" in resp.json()

    def test_client_address_ceiling_stops_an_account_sweep(self, client, clean_login_attempts):
        """One source trying many accounts still hits the global cap."""
        for i in range(api_module._LOGIN_MAX_ATTEMPTS_PER_CLIENT):
            client.post(LOGIN_PATH, json={"username": f"sweep{i}", "password": "wrong"})
        resp = client.post(LOGIN_PATH, json=GOOD_CREDENTIALS)
        assert resp.status_code == 429

    def test_successful_login_clears_the_account_bucket(self, client, clean_login_attempts):
        for _ in range(api_module._LOGIN_MAX_ATTEMPTS - 1):
            client.post(LOGIN_PATH, json={"username": "testuser", "password": "wrong"})
        assert client.post(LOGIN_PATH, json=GOOD_CREDENTIALS).status_code == 200
        for _ in range(api_module._LOGIN_MAX_ATTEMPTS - 1):
            client.post(LOGIN_PATH, json={"username": "testuser", "password": "wrong"})
        assert client.post(LOGIN_PATH, json=GOOD_CREDENTIALS).status_code == 200

    def test_forwarded_for_is_ignored_by_default(self, client, clean_login_attempts, monkeypatch):
        """The header is spoofable: it must not create per-client budgets."""
        monkeypatch.delenv("PULSARCD_TRUST_PROXY_HEADERS", raising=False)
        for i in range(api_module._LOGIN_MAX_ATTEMPTS_PER_CLIENT):
            client.post(
                LOGIN_PATH,
                json={"username": f"sweep{i}", "password": "wrong"},
                headers={"X-Forwarded-For": f"10.0.0.{i}"},
            )
        assert not [k for k in clean_login_attempts if k.startswith("ip:10.0.0.")]
        resp = client.post(LOGIN_PATH, json=GOOD_CREDENTIALS,
                           headers={"X-Forwarded-For": "10.0.0.250"})
        assert resp.status_code == 429

    def test_forwarded_for_is_used_when_trusted(self, client, clean_login_attempts, monkeypatch):
        monkeypatch.setenv("PULSARCD_TRUST_PROXY_HEADERS", "true")
        for i in range(api_module._LOGIN_MAX_ATTEMPTS_PER_CLIENT):
            client.post(
                LOGIN_PATH,
                json={"username": f"sweep{i}", "password": "wrong"},
                headers={"X-Forwarded-For": f"10.0.0.{i}, 172.16.0.1"},
            )
        assert "ip:10.0.0.1" in clean_login_attempts
        # Each forwarded client keeps its own budget, so this one is not capped
        resp = client.post(LOGIN_PATH, json=GOOD_CREDENTIALS,
                           headers={"X-Forwarded-For": "10.0.0.1"})
        assert resp.status_code == 200

    def test_expired_buckets_are_purged(self, client, clean_login_attempts):
        stale = time.time() - api_module._LOGIN_WINDOW_SECONDS - 1
        clean_login_attempts["ip:203.0.113.9"] = [stale]
        clean_login_attempts["user:ghost"] = [stale]
        client.post(LOGIN_PATH, json={"username": "testuser", "password": "wrong"})
        assert "ip:203.0.113.9" not in clean_login_attempts
        assert "user:ghost" not in clean_login_attempts

    def test_bucket_count_stays_bounded(self, client, clean_login_attempts, monkeypatch):
        monkeypatch.setattr(api_module, "_LOGIN_ATTEMPTS_MAX_KEYS", 8)
        for i in range(40):
            api_module._record_login_attempt(f"10.1.0.{i}", f"user{i}")
        assert len(clean_login_attempts) <= 8

    def test_non_string_credentials_are_rejected(self, client, clean_login_attempts):
        resp = client.post(LOGIN_PATH, json={"username": {"a": 1}, "password": "x"})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# JWT revocation (token epoch)
# ---------------------------------------------------------------------------

def _epoch_user_manager(epoch):
    """User manager double whose token_epoch_for returns `epoch`."""
    m = MagicMock()
    m.token_epoch_for = MagicMock(return_value=epoch)
    return m


def _token(username="bob", role="admin", epoch=0, claims=None):
    secret = api_module.settings.auth.jwt_secret
    if claims is not None:
        return jwt.encode(claims, secret, algorithm="HS256")
    return create_token(username, secret, 1, role=role, token_epoch=epoch)


class TestTokenRevocation:
    """A password/role change or a deletion must cut sessions already open."""

    def test_stale_epoch_is_rejected(self, client):
        headers = {"Authorization": f"Bearer {_token(epoch=4999)}"}
        with patch.object(api_module, "user_manager", _epoch_user_manager(5000)):
            resp = client.get("/api/containers", headers=headers)
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Token has been revoked"

    def test_current_epoch_is_accepted(self, client):
        headers = {"Authorization": f"Bearer {_token(epoch=5000)}"}
        with patch.object(api_module, "user_manager", _epoch_user_manager(5000)):
            resp = client.get("/api/containers", headers=headers)
        assert resp.status_code == 200

    def test_deleted_user_token_is_rejected(self, client):
        headers = {"Authorization": f"Bearer {_token(epoch=5000)}"}
        with patch.object(api_module, "user_manager", _epoch_user_manager(None)):
            resp = client.get("/api/containers", headers=headers)
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Token has been revoked"

    def test_token_issued_before_the_claim_existed_still_works(self, client):
        """Backward compatibility: no `epoch` claim reads as 0, which matches a
        users.json written before the field existed."""
        now = datetime.now(timezone.utc)
        legacy = _token(claims={
            "sub": "bob", "role": "admin",
            "iat": now, "exp": now + timedelta(hours=1),
        })
        with patch.object(api_module, "user_manager", _epoch_user_manager(0)):
            resp = client.get("/api/containers", headers={"Authorization": f"Bearer {legacy}"})
        assert resp.status_code == 200

    def test_legacy_token_rejected_once_the_account_is_revoked(self, client):
        now = datetime.now(timezone.utc)
        legacy = _token(claims={
            "sub": "bob", "role": "admin",
            "iat": now, "exp": now + timedelta(hours=1),
        })
        with patch.object(api_module, "user_manager", _epoch_user_manager(1)):
            resp = client.get("/api/containers", headers={"Authorization": f"Bearer {legacy}"})
        assert resp.status_code == 401

    def test_lookup_failure_does_not_lock_everyone_out(self, client):
        broken = MagicMock()
        broken.token_epoch_for = MagicMock(side_effect=OSError("users.json unreadable"))
        headers = {"Authorization": f"Bearer {_token(epoch=1)}"}
        with patch.object(api_module, "user_manager", broken):
            resp = client.get("/api/containers", headers=headers)
        assert resp.status_code == 200

    def test_login_carries_the_account_epoch(self, client):
        """The issued token must embed the account's current epoch."""
        from types import SimpleNamespace
        user = SimpleNamespace(username="testuser", role="admin", token_epoch=1234)
        um = MagicMock()
        um.authenticate = lambda u, p: user if (u, p) == ("testuser", "testpass") else None
        with patch.object(api_module, "user_manager", um):
            resp = client.post(LOGIN_PATH, json=GOOD_CREDENTIALS)
        assert resp.status_code == 200
        payload = jwt.decode(resp.json()["token"], api_module.settings.auth.jwt_secret,
                             algorithms=["HS256"])
        assert payload["epoch"] == 1234


class TestUserManagerTokenEpoch:
    """The epoch lives with the account and only moves on revocation."""

    def _manager(self, tmp_path, users=None):
        from backend.user_manager import UserManager
        path = tmp_path / "users.json"
        path.write_text(json.dumps(users if users is not None else [
            {"username": "admin", "password_hash": "hash-admin", "role": "admin"},
            {"username": "bob", "password_hash": "hash-bob", "role": "viewer"},
        ]), encoding="utf-8")
        return UserManager(path=str(path))

    def test_missing_field_reads_as_zero(self, tmp_path):
        """A users.json written before the upgrade keeps working."""
        mgr = self._manager(tmp_path)
        assert mgr.token_epoch_for("bob") == 0

    def test_unknown_user_has_no_epoch(self, tmp_path):
        mgr = self._manager(tmp_path)
        assert mgr.token_epoch_for("nobody") is None

    async def test_role_change_bumps_the_epoch(self, tmp_path):
        mgr = self._manager(tmp_path)
        await mgr.update_user("bob", role="admin")
        assert mgr.token_epoch_for("bob") > 0

    async def test_unchanged_role_does_not_bump(self, tmp_path):
        mgr = self._manager(tmp_path)
        await mgr.update_user("bob", role="viewer")
        assert mgr.token_epoch_for("bob") == 0

    async def test_password_change_bumps_the_epoch(self, tmp_path):
        mgr = self._manager(tmp_path)
        await mgr.update_user("bob", password="a-brand-new-password")
        assert mgr.token_epoch_for("bob") > 0

    async def test_epoch_is_persisted(self, tmp_path):
        from backend.user_manager import UserManager
        mgr = self._manager(tmp_path)
        await mgr.update_user("bob", role="admin")
        expected = mgr.token_epoch_for("bob")
        reloaded = UserManager(path=str(tmp_path / "users.json"))
        assert reloaded.token_epoch_for("bob") == expected

    async def test_deletion_removes_the_account(self, tmp_path):
        mgr = self._manager(tmp_path)
        await mgr.delete_user("bob")
        assert mgr.token_epoch_for("bob") is None


# ---------------------------------------------------------------------------
# Security headers
# ---------------------------------------------------------------------------

class TestSecurityHeaders:
    """Every response carries the baseline hardening headers."""

    def test_headers_on_authenticated_response(self, client, auth_headers):
        resp = client.get("/api/containers", headers=auth_headers)
        assert resp.headers["X-Content-Type-Options"] == "nosniff"
        assert resp.headers["X-Frame-Options"] == "DENY"
        assert resp.headers["Referrer-Policy"] == "no-referrer"
        assert "Content-Security-Policy" in resp.headers

    def test_headers_on_rejected_request(self, client):
        """The 401 produced by the auth middleware is covered too."""
        resp = client.get("/api/containers")
        assert resp.status_code == 401
        assert resp.headers["X-Content-Type-Options"] == "nosniff"
        assert "Content-Security-Policy" in resp.headers

    def test_csp_directives(self, client):
        csp = client.get("/api/health").headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "object-src 'none'" in csp
        assert "base-uri 'self'" in csp
        assert "script-src 'self' 'unsafe-inline'" in csp

    def test_csp_allows_the_same_origin_websocket(self, client):
        csp = client.get("/api/health").headers["Content-Security-Policy"]
        assert "connect-src 'self' ws://testserver wss://testserver" in csp

    def test_bogus_host_header_is_not_reflected(self, client):
        csp = client.get("/api/health", headers={"Host": "evil host <script>"}).headers[
            "Content-Security-Policy"]
        assert csp.endswith("connect-src 'self'")
        assert "evil host" not in csp

    def test_no_hsts_over_plain_http(self, client):
        resp = client.get("/api/health")
        assert "Strict-Transport-Security" not in resp.headers

    def test_hsts_behind_a_tls_terminating_proxy(self, client):
        resp = client.get("/api/health", headers={"X-Forwarded-Proto": "https"})
        assert resp.headers["Strict-Transport-Security"].startswith("max-age=")

    def test_sse_stream_keeps_its_streaming_headers(self, client, auth_token):
        action_id, action = _fresh_action()
        action.status = "completed"
        resp = client.get(f"/api/stacks/actions/{action_id}/logs/stream?token={auth_token}")
        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "no-cache"
        assert resp.headers["x-accel-buffering"] == "no"
        assert resp.headers["X-Content-Type-Options"] == "nosniff"


# ---------------------------------------------------------------------------
# Swarm workers refuse to run a shell command through the manager
# ---------------------------------------------------------------------------

class TestSwarmProxyShellRefusal:
    """A worker is reached through the manager's API, not through a shell.

    Nothing on the MCP surface runs shell commands any more, but the collector
    still exposes run_shell_command and routing a worker's command to the
    manager would report the manager's state as the worker's.
    """

    @pytest.mark.asyncio
    async def test_swarm_worker_refuses_instead_of_running_on_the_manager(self):
        from backend.host_client import SwarmProxyClient
        manager = MagicMock()
        manager.config = MagicMock(name="mgr")
        proxy = SwarmProxyClient(manager, "nodeid123456", "worker-2")
        success, message = await proxy.run_shell_command("docker ps")
        assert success is False
        assert "worker-2" in message
        manager.run_shell_command.assert_not_called()


# ---------------------------------------------------------------------------
# search_logs: the compose_services filter must reach OpenSearch
# ---------------------------------------------------------------------------

class TestSearchLogsServiceFilter:
    """Regression: compose_services was applied to the returned page only.

    It never reached the query, so `total` and the aggregations counted every
    service — and a size=0 aggregation request, which returns no hits to
    post-filter, was not filtered at all: the caller asked for one service,
    got the whole cluster back, and had no way to notice.
    """

    @pytest.mark.asyncio
    async def test_the_filter_reaches_the_search_query(self):
        try:
            from backend import mcp_server
        except Exception:
            pytest.skip("MCP support not installed in this environment")
        from backend.models import LogSearchResult

        recorded = {}

        class _OpenSearch:
            async def search_logs(self, query):
                recorded["query"] = query
                return LogSearchResult(total=0, hits=[], aggregations={})

        with patch.object(api_module, "opensearch", _OpenSearch()):
            await mcp_server.search_logs(
                compose_services="agent, swarm-manager", size=0, last_hours=1
            )

        assert recorded["query"].compose_services == ["agent", "swarm-manager"]

    @pytest.mark.asyncio
    async def test_the_query_builder_emits_a_terms_clause(self):
        from backend.models import LogSearchQuery
        from backend.opensearch_client import OpenSearchClient

        captured = {}

        class _Client:
            async def search(self, index, body):
                captured["body"] = body
                return {"hits": {"hits": [], "total": {"value": 0}}}

        client = OpenSearchClient.__new__(OpenSearchClient)
        client.logs_index = "pulsarcd-logs"
        client._client = _Client()

        await client.search_logs(LogSearchQuery(compose_services=["agent"]))

        filters = captured["body"]["query"]["bool"]["filter"]
        assert {"terms": {"compose_service": ["agent"]}} in filters

    @pytest.mark.asyncio
    async def test_no_service_means_no_clause(self):
        from backend.models import LogSearchQuery
        from backend.opensearch_client import OpenSearchClient

        captured = {}

        class _Client:
            async def search(self, index, body):
                captured["body"] = body
                return {"hits": {"hits": [], "total": {"value": 0}}}

        client = OpenSearchClient.__new__(OpenSearchClient)
        client.logs_index = "pulsarcd-logs"
        client._client = _Client()

        await client.search_logs(LogSearchQuery())

        filters = captured["body"]["query"]["bool"]["filter"]
        assert not any("compose_service" in str(f) for f in filters)
