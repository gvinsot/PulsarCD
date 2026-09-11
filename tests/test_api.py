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

class TestSignInConfig:
    """The login screen has to know which buttons to draw before it has a token."""

    def test_config_is_public(self, client):
        resp = client.get("/api/auth/config")
        assert resp.status_code == 200

    def test_config_describes_both_methods(self, client):
        data = client.get("/api/auth/config").json()
        assert data["google_enabled"] is True
        assert data["google_client_id"] == "test-client-id.apps.googleusercontent.com"
        assert data["password_login_enabled"] is True

    def test_config_carries_no_secret(self, client):
        """It is served to anyone who can reach the port."""
        body = client.get("/api/auth/config").text
        assert api_module.settings.auth.jwt_secret not in body
        assert api_module.settings.auth.agent_key not in body
        assert "password" not in body.replace("password_login_enabled", "")


def _google_claims(email="boss@example.com", **extra):
    claims = {"email": email, "email_verified": True, "sub": "google-123",
              "name": "Boss Person"}
    claims.update(extra)
    return claims


def _verifier(claims=None, error=None):
    """Google verifier double: verify() either returns claims or raises."""
    from backend.google_auth import GoogleTokenError
    m = MagicMock()
    m.enabled = True
    m.client_id = "test-client-id.apps.googleusercontent.com"
    if error is not None:
        m.verify = AsyncMock(side_effect=GoogleTokenError(error))
    else:
        m.verify = AsyncMock(return_value=claims or _google_claims())
    return m


class TestGoogleSignIn:
    """A verified Google identity plus an allowlist entry, or nothing."""

    def test_an_allowed_address_gets_a_token(self, client, clean_login_attempts):
        with patch.object(api_module, "google_verifier", _verifier()):
            resp = client.post("/api/auth/google", json={"credential": "any"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "boss@example.com"
        assert data["role"] == "admin"
        payload = jwt.decode(data["token"], api_module.settings.auth.jwt_secret,
                             algorithms=["HS256"])
        assert payload["sub"] == "boss@example.com"
        assert payload["role"] == "admin"
        assert payload["auth"] == "google"

    def test_the_token_works_on_the_api(self, client, clean_login_attempts):
        with patch.object(api_module, "google_verifier", _verifier()):
            token = client.post("/api/auth/google", json={"credential": "any"}).json()["token"]
        resp = client.get("/api/containers", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 200

    def test_a_viewer_address_gets_the_viewer_role(self, client, clean_login_attempts):
        verifier = _verifier(_google_claims(email="watcher@example.com"))
        with patch.object(api_module, "google_verifier", verifier):
            resp = client.post("/api/auth/google", json={"credential": "any"})
        assert resp.json()["role"] == "viewer"

    def test_an_address_outside_the_allowlist_is_refused(self, client, clean_login_attempts):
        """Having a Google account is not an access rule."""
        verifier = _verifier(_google_claims(email="stranger@example.com"))
        with patch.object(api_module, "google_verifier", verifier):
            resp = client.post("/api/auth/google", json={"credential": "any"})
        assert resp.status_code == 403
        assert "not authorised" in resp.json()["detail"]

    def test_a_rejected_credential_is_a_401(self, client, clean_login_attempts):
        with patch.object(api_module, "google_verifier", _verifier(error="bad signature")):
            resp = client.post("/api/auth/google", json={"credential": "forged"})
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Google sign-in failed"

    def test_the_failure_reason_does_not_leak(self, client, clean_login_attempts):
        """Why Google said no is an operator detail, not a caller's."""
        with patch.object(api_module, "google_verifier",
                          _verifier(error="audience mismatch: other-client-id")):
            resp = client.post("/api/auth/google", json={"credential": "forged"})
        assert "other-client-id" not in resp.text

    def test_sign_in_is_refused_when_google_is_not_configured(self, client,
                                                              clean_login_attempts):
        disabled = MagicMock()
        disabled.enabled = False
        with patch.object(api_module, "google_verifier", disabled):
            resp = client.post("/api/auth/google", json={"credential": "any"})
        assert resp.status_code == 403

    def test_a_non_string_credential_is_refused(self, client, clean_login_attempts):
        with patch.object(api_module, "google_verifier", _verifier()):
            resp = client.post("/api/auth/google", json={"credential": {"a": 1}})
        assert resp.status_code == 400

    def test_the_address_is_not_taken_from_the_request(self, client, clean_login_attempts):
        """Only the verified claims decide who signed in."""
        verifier = _verifier(_google_claims(email="watcher@example.com"))
        with patch.object(api_module, "google_verifier", verifier):
            resp = client.post("/api/auth/google",
                               json={"credential": "any", "email": "boss@example.com",
                                     "role": "admin"})
        assert resp.json()["email"] == "watcher@example.com"
        assert resp.json()["role"] == "viewer"

    def test_repeated_failures_are_rate_limited(self, client, clean_login_attempts):
        """The endpoint is unauthenticated and does public-key work per call."""
        with patch.object(api_module, "google_verifier", _verifier(error="nope")):
            for _ in range(api_module._LOGIN_MAX_ATTEMPTS_PER_CLIENT):
                client.post("/api/auth/google", json={"credential": "forged"})
            resp = client.post("/api/auth/google", json={"credential": "forged"})
        assert resp.status_code == 429

    def test_a_revoked_address_cannot_use_its_token(self, client, clean_login_attempts):
        """Removing an address from the allowlist cuts the session it opened."""
        with patch.object(api_module, "google_verifier", _verifier()):
            token = client.post("/api/auth/google", json={"credential": "any"}).json()["token"]
        gone = MagicMock()
        gone.token_epoch_for = MagicMock(return_value=None)
        with patch.object(api_module, "email_allowlist", gone):
            resp = client.get("/api/containers", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Token has been revoked"

    def test_a_google_token_is_not_resolved_against_local_accounts(self, client):
        """The two namespaces are separate; the `auth` claim picks the store."""
        token = create_token("testuser", api_module.settings.auth.jwt_secret, 1,
                             role="admin", token_epoch=0, auth_source="google")
        gone = MagicMock()
        gone.token_epoch_for = MagicMock(return_value=None)
        with patch.object(api_module, "email_allowlist", gone):
            resp = client.get("/api/containers", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 401


class TestAdminAllowlistApi:
    """Settings > Users manages Google addresses, not local accounts."""

    def test_listing_returns_the_addresses(self, client, auth_headers):
        data = client.get("/api/admin/users", headers=auth_headers).json()
        assert {u["email"] for u in data["users"]} == {"boss@example.com",
                                                       "watcher@example.com"}
        assert data["google_enabled"] is True

    def test_listing_reports_the_break_glass_account(self, client, auth_headers):
        """Otherwise the operator cannot tell the local login still exists."""
        data = client.get("/api/admin/users", headers=auth_headers).json()
        assert data["local_admin"] == {"username": "testuser", "role": "admin"}

    def test_listing_carries_no_password_material(self, client, auth_headers):
        body = client.get("/api/admin/users", headers=auth_headers).text
        assert "password_hash" not in body and "$2b$" not in body

    def test_adding_an_address(self, client, auth_headers):
        allowlist = MagicMock()
        allowlist.add = AsyncMock(return_value={"email": "new@example.com",
                                                "role": "viewer", "managed": False})
        with patch.object(api_module, "email_allowlist", allowlist):
            resp = client.post("/api/admin/users", headers=auth_headers,
                               json={"email": "new@example.com", "role": "viewer"})
        assert resp.status_code == 200
        allowlist.add.assert_awaited_once_with("new@example.com", "viewer")

    def test_an_address_is_required(self, client, auth_headers):
        resp = client.post("/api/admin/users", headers=auth_headers,
                           json={"email": "   ", "role": "viewer"})
        assert resp.status_code == 400

    def test_a_rejected_address_becomes_a_400(self, client, auth_headers):
        allowlist = MagicMock()
        allowlist.add = AsyncMock(side_effect=ValueError("'x' is not a valid email address"))
        with patch.object(api_module, "email_allowlist", allowlist):
            resp = client.post("/api/admin/users", headers=auth_headers,
                               json={"email": "x", "role": "viewer"})
        assert resp.status_code == 400
        assert "not a valid email" in resp.json()["detail"]

    def test_changing_a_role(self, client, auth_headers):
        allowlist = MagicMock()
        allowlist.set_role = AsyncMock(return_value={"email": "watcher@example.com",
                                                     "role": "admin", "managed": False})
        with patch.object(api_module, "email_allowlist", allowlist):
            resp = client.put("/api/admin/users/watcher@example.com",
                              headers=auth_headers, json={"role": "admin"})
        assert resp.status_code == 200
        allowlist.set_role.assert_awaited_once_with("watcher@example.com", "admin")

    def test_a_role_is_required_on_update(self, client, auth_headers):
        resp = client.put("/api/admin/users/watcher@example.com",
                          headers=auth_headers, json={})
        assert resp.status_code == 400

    def test_removing_an_address(self, client, auth_headers):
        allowlist = MagicMock()
        allowlist.remove = AsyncMock(return_value=True)
        with patch.object(api_module, "email_allowlist", allowlist):
            resp = client.delete("/api/admin/users/watcher@example.com",
                                 headers=auth_headers)
        assert resp.status_code == 200
        allowlist.remove.assert_awaited_once_with("watcher@example.com")

    def test_a_refused_removal_becomes_a_400(self, client, auth_headers):
        allowlist = MagicMock()
        allowlist.remove = AsyncMock(side_effect=ValueError("Cannot remove the last administrator"))
        with patch.object(api_module, "email_allowlist", allowlist):
            resp = client.delete("/api/admin/users/boss@example.com", headers=auth_headers)
        assert resp.status_code == 400

    @pytest.mark.parametrize("method,kwargs", [
        ("post", {"json": {"email": "new@example.com", "role": "admin"}}),
        ("put", {"json": {"role": "admin"}}),
        ("delete", {}),
    ])
    def test_viewers_cannot_change_the_allowlist(self, client, method, kwargs):
        """Granting access is the most privileged thing in the product."""
        path = "/api/admin/users" if method == "post" else "/api/admin/users/x@example.com"
        resp = getattr(client, method)(
            path, headers={"Authorization": f"Bearer {_token(role='viewer')}"}, **kwargs)
        assert resp.status_code == 403


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

    def test_login_carries_the_local_auth_source(self, client):
        token = client.post("/api/auth/login",
                            json={"username": "testuser", "password": "testpass"}).json()["token"]
        payload = jwt.decode(token, api_module.settings.auth.jwt_secret, algorithms=["HS256"])
        assert payload["auth"] == "local"

    def test_password_login_is_refused_when_no_account_is_provisioned(self, client):
        """PULSARCD_AUTH__PASSWORD unset means there is nothing to guess."""
        disabled = MagicMock()
        disabled.enabled = False
        with patch.object(api_module, "user_manager", disabled):
            resp = client.post("/api/auth/login",
                               json={"username": "admin", "password": "anything-at-all"})
        assert resp.status_code == 403
        assert "disabled" in resp.json()["detail"]

    def test_auth_me_reports_the_identity_source(self, client, auth_headers):
        data = client.get("/api/auth/me", headers=auth_headers).json()
        assert data["username"] == "testuser"
        assert data["auth"] == "local"

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


class TestAllowlistTokenEpoch:
    """The epoch lives with the address and only moves on revocation."""

    def _allowlist(self, tmp_path, entries=None, **kwargs):
        from backend.allowlist import EmailAllowlist
        path = tmp_path / "allowed_emails.json"
        path.write_text(json.dumps(entries if entries is not None else [
            {"email": "root@example.com", "role": "admin"},
            {"email": "bob@example.com", "role": "viewer"},
        ]), encoding="utf-8")
        return EmailAllowlist(path=str(path), **kwargs)

    def test_missing_field_reads_as_zero(self, tmp_path):
        """A file written before the upgrade keeps working."""
        allowlist = self._allowlist(tmp_path)
        assert allowlist.token_epoch_for("bob@example.com") == 0

    def test_unknown_address_has_no_epoch(self, tmp_path):
        allowlist = self._allowlist(tmp_path)
        assert allowlist.token_epoch_for("nobody@example.com") is None

    def test_lookup_is_case_insensitive(self, tmp_path):
        """Google's casing and the operator's typing must reach the same entry."""
        allowlist = self._allowlist(tmp_path)
        assert allowlist.role_for("Bob@Example.COM") == "viewer"

    async def test_role_change_bumps_the_epoch(self, tmp_path):
        allowlist = self._allowlist(tmp_path)
        await allowlist.set_role("bob@example.com", "admin")
        assert allowlist.token_epoch_for("bob@example.com") > 0

    async def test_unchanged_role_does_not_bump(self, tmp_path):
        allowlist = self._allowlist(tmp_path)
        await allowlist.set_role("bob@example.com", "viewer")
        assert allowlist.token_epoch_for("bob@example.com") == 0

    async def test_epoch_is_persisted(self, tmp_path):
        from backend.allowlist import EmailAllowlist
        allowlist = self._allowlist(tmp_path)
        await allowlist.set_role("bob@example.com", "admin")
        expected = allowlist.token_epoch_for("bob@example.com")
        reloaded = EmailAllowlist(path=str(tmp_path / "allowed_emails.json"))
        assert reloaded.token_epoch_for("bob@example.com") == expected

    async def test_removal_revokes_the_address(self, tmp_path):
        allowlist = self._allowlist(tmp_path)
        await allowlist.remove("bob@example.com")
        assert allowlist.token_epoch_for("bob@example.com") is None
        assert allowlist.role_for("bob@example.com") is None

    async def test_a_new_address_starts_above_any_old_token(self, tmp_path):
        """Re-adding an address must not resurrect the tokens it once had."""
        allowlist = self._allowlist(tmp_path)
        await allowlist.remove("bob@example.com")
        await allowlist.add("bob@example.com", "viewer")
        assert allowlist.token_epoch_for("bob@example.com") > 0

    async def test_the_last_admin_cannot_be_removed_or_demoted(self, tmp_path):
        allowlist = self._allowlist(tmp_path)
        with pytest.raises(ValueError):
            await allowlist.remove("root@example.com")
        with pytest.raises(ValueError):
            await allowlist.set_role("root@example.com", "viewer")

    @pytest.mark.parametrize("email", ["", "   ", "not-an-email", "a@b", "a b@c.d",
                                       "@example.com", "x@" + "y" * 300 + ".com"])
    async def test_invalid_addresses_are_refused(self, tmp_path, email):
        allowlist = self._allowlist(tmp_path)
        with pytest.raises(ValueError):
            await allowlist.add(email, "viewer")

    async def test_an_unknown_role_is_refused(self, tmp_path):
        allowlist = self._allowlist(tmp_path)
        with pytest.raises(ValueError):
            await allowlist.add("new@example.com", "superuser")


class TestAllowlistEnvironmentBootstrap:
    """The environment is reapplied on every boot and outranks the UI."""

    def _allowlist(self, tmp_path, **kwargs):
        from backend.allowlist import EmailAllowlist
        return EmailAllowlist(path=str(tmp_path / "allowed_emails.json"), **kwargs)

    def test_env_addresses_are_added_on_first_boot(self, tmp_path):
        allowlist = self._allowlist(tmp_path, admins=["boss@example.com"],
                                    viewers=["intern@example.com"])
        assert allowlist.role_for("boss@example.com") == "admin"
        assert allowlist.role_for("intern@example.com") == "viewer"

    def test_env_addresses_are_reported_as_managed(self, tmp_path):
        allowlist = self._allowlist(tmp_path, admins=["boss@example.com"])
        assert allowlist.is_managed("boss@example.com") is True
        assert allowlist.is_managed("someone@example.com") is False

    async def test_ui_additions_survive_a_reload(self, tmp_path):
        allowlist = self._allowlist(tmp_path, admins=["boss@example.com"])
        await allowlist.add("colleague@example.com", "viewer")
        reloaded = self._allowlist(tmp_path, admins=["boss@example.com"])
        assert reloaded.role_for("colleague@example.com") == "viewer"

    def test_env_wins_over_a_stored_role(self, tmp_path):
        """Demoting in the file must not survive a boot that says admin."""
        path = tmp_path / "allowed_emails.json"
        path.write_text(json.dumps([
            {"email": "boss@example.com", "role": "viewer", "token_epoch": 5},
        ]), encoding="utf-8")
        allowlist = self._allowlist(tmp_path, admins=["boss@example.com"])
        assert allowlist.role_for("boss@example.com") == "admin"
        # And the sessions the old role opened are cut.
        assert allowlist.token_epoch_for("boss@example.com") > 5

    async def test_a_managed_address_cannot_be_edited_from_the_ui(self, tmp_path):
        """Otherwise a restart would silently undo the change."""
        allowlist = self._allowlist(tmp_path, admins=["boss@example.com", "other@example.com"])
        with pytest.raises(ValueError, match="PULSARCD_AUTH__GOOGLE"):
            await allowlist.remove("boss@example.com")
        with pytest.raises(ValueError, match="PULSARCD_AUTH__GOOGLE"):
            await allowlist.set_role("boss@example.com", "viewer")

    def test_a_corrupt_file_falls_back_to_the_environment(self, tmp_path):
        (tmp_path / "allowed_emails.json").write_text("{not json", encoding="utf-8")
        allowlist = self._allowlist(tmp_path, admins=["boss@example.com"])
        assert allowlist.role_for("boss@example.com") == "admin"

    @pytest.mark.parametrize("raw,expected", [
        ("a@x.com,b@x.com", ["a@x.com", "b@x.com"]),
        ("a@x.com b@x.com", ["a@x.com", "b@x.com"]),
        ("a@x.com; b@x.com", ["a@x.com", "b@x.com"]),
        ('["a@x.com", "B@X.com"]', ["a@x.com", "b@x.com"]),
        ("  A@X.com  ", ["a@x.com"]),
        ("", []),
        ("[not json", []),
    ])
    def test_env_list_parsing(self, raw, expected):
        from backend.allowlist import parse_email_list
        assert parse_email_list(raw) == expected


class TestBreakGlassAdministrator:
    """The local account is provisioned from the environment, or not at all."""

    def _manager(self, tmp_path, monkeypatch, password, username="admin"):
        from backend.user_manager import UserManager
        monkeypatch.setenv("PULSARCD_AUTH__USERNAME", username)
        monkeypatch.setenv("PULSARCD_AUTH__PASSWORD", password)
        return UserManager(path=str(tmp_path / "users.json"))

    def test_a_strong_password_provisions_the_account(self, tmp_path, monkeypatch):
        mgr = self._manager(tmp_path, monkeypatch, "a-properly-long-secret")
        assert mgr.enabled is True
        assert mgr.authenticate("admin", "a-properly-long-secret") is not None

    @pytest.mark.parametrize("password", ["", "changeme", "short", "password"])
    def test_a_weak_password_provisions_nothing(self, tmp_path, monkeypatch, password):
        """No account beats an account with a password nobody will ever read."""
        mgr = self._manager(tmp_path, monkeypatch, password)
        assert mgr.enabled is False
        assert mgr.authenticate("admin", password) is None
        assert mgr.describe() is None

    def test_the_password_can_be_rotated_through_the_environment(self, tmp_path, monkeypatch):
        self._manager(tmp_path, monkeypatch, "the-first-long-password")
        rotated = self._manager(tmp_path, monkeypatch, "the-second-long-password")
        assert rotated.authenticate("admin", "the-first-long-password") is None
        assert rotated.authenticate("admin", "the-second-long-password") is not None

    def test_a_rotation_cuts_the_sessions_the_old_password_opened(self, tmp_path, monkeypatch):
        first = self._manager(tmp_path, monkeypatch, "the-first-long-password")
        before = first.token_epoch_for("admin")
        rotated = self._manager(tmp_path, monkeypatch, "the-second-long-password")
        assert rotated.token_epoch_for("admin") > before

    def test_an_unchanged_password_keeps_the_epoch(self, tmp_path, monkeypatch):
        """A restart must not sign the operator out of a live session."""
        first = self._manager(tmp_path, monkeypatch, "an-unchanging-password")
        before = first.token_epoch_for("admin")
        again = self._manager(tmp_path, monkeypatch, "an-unchanging-password")
        assert again.token_epoch_for("admin") == before

    def test_clearing_the_password_removes_the_stored_account(self, tmp_path, monkeypatch):
        """The file is a cache of the configuration, not a second place to edit."""
        self._manager(tmp_path, monkeypatch, "a-properly-long-secret")
        cleared = self._manager(tmp_path, monkeypatch, "")
        assert cleared.enabled is False
        assert json.loads((tmp_path / "users.json").read_text(encoding="utf-8")) == []

    def test_accounts_from_the_old_multi_user_file_are_dropped(self, tmp_path, monkeypatch):
        """Everyone but the break-glass admin signs in with Google now."""
        import bcrypt
        path = tmp_path / "users.json"
        path.write_text(json.dumps([
            {"username": "admin", "role": "admin", "token_epoch": 3,
             "password_hash": bcrypt.hashpw(b"a-properly-long-secret",
                                            bcrypt.gensalt()).decode()},
            {"username": "legacy", "password_hash": "x", "role": "viewer"},
        ]), encoding="utf-8")
        monkeypatch.setenv("PULSARCD_AUTH__USERNAME", "admin")
        monkeypatch.setenv("PULSARCD_AUTH__PASSWORD", "a-properly-long-secret")
        from backend.user_manager import UserManager
        mgr = UserManager(path=str(path))
        assert mgr.get_user("legacy") is None
        assert mgr.token_epoch_for("legacy") is None
        # The admin itself is untouched: same password, so the epoch survives.
        assert mgr.token_epoch_for("admin") == 3

    def test_a_corrupt_hash_is_rewritten_rather_than_crashing(self, tmp_path, monkeypatch):
        path = tmp_path / "users.json"
        path.write_text(json.dumps([
            {"username": "admin", "password_hash": "not-a-bcrypt-hash", "role": "admin"},
        ]), encoding="utf-8")
        monkeypatch.setenv("PULSARCD_AUTH__USERNAME", "admin")
        monkeypatch.setenv("PULSARCD_AUTH__PASSWORD", "a-properly-long-secret")
        from backend.user_manager import UserManager
        mgr = UserManager(path=str(path))
        assert mgr.authenticate("admin", "a-properly-long-secret") is not None


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
        assert "evil host" not in csp
        # No WebSocket source at all rather than one built from a bad host.
        assert "ws://" not in csp and "wss://" not in csp

    def test_csp_carries_the_google_sign_in_sources(self, client):
        """The GIS button is a cross-origin iframe with its own script and CSS."""
        csp = client.get("/api/health").headers["Content-Security-Policy"]
        assert "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net " \
               "https://accounts.google.com/gsi/client" in csp
        assert "https://accounts.google.com/gsi/style" in csp
        assert "frame-src https://accounts.google.com/gsi/" in csp
        assert "connect-src 'self' ws://testserver wss://testserver " \
               "https://accounts.google.com/gsi/" in csp
        # Widened, not duplicated: one script-src directive, or the browser
        # intersects them and the button stops loading.
        assert csp.count("script-src") == 1
        assert csp.count("style-src") == 1

    def test_csp_omits_google_when_sign_in_is_not_configured(self, client):
        """An unused allowance is still an allowance."""
        with patch.object(api_module, "google_verifier", None):
            csp = client.get("/api/health").headers["Content-Security-Policy"]
        assert "accounts.google.com" not in csp

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
