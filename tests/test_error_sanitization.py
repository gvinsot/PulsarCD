"""Tests for the global exception handler error sanitization (SEC-13).

Verifies that in production mode, error responses do NOT leak internal details
(stack trace, raw exception messages, SQL/Postgres errors), while in dev mode
the full stack is exposed for debugging.
"""

from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient

import backend.api as api_module


# ---------------------------------------------------------------------------
# Helpers — install ad-hoc routes that always raise so we can observe the
# behavior of the global exception handler.
# ---------------------------------------------------------------------------

_BOOM_ROUTES_INSTALLED = False


def _install_boom_routes():
    """Idempotently mount /api/_boom/* routes that raise unhandled exceptions."""
    global _BOOM_ROUTES_INSTALLED
    if _BOOM_ROUTES_INSTALLED:
        return

    SECRET_SQL = "ERROR: relation \"users\" does not exist at /var/lib/pg/secret"

    @api_module.app.get("/api/_boom/runtime")
    async def _boom_runtime():
        raise RuntimeError(SECRET_SQL)

    @api_module.app.get("/api/_boom/value")
    async def _boom_value():
        raise ValueError("bad input from caller")

    @api_module.app.get("/api/_boom/keyerror")
    async def _boom_key():
        d: dict = {}
        return d["missing-key"]

    @api_module.app.get("/api/_boom/permission")
    async def _boom_perm():
        raise PermissionError("not allowed")

    api_module._BOOM_SECRET = SECRET_SQL  # exposed for assertions
    _BOOM_ROUTES_INSTALLED = True


@pytest.fixture(scope="module")
def boom_client():
    """A TestClient that does NOT raise server exceptions — we want to inspect
    the JSON body returned by the exception handler, not let pytest swallow it."""
    _install_boom_routes()

    # Reuse the standard mocked lifespan from conftest by patching it inline.
    from tests.conftest import (
        _mock_settings, _mock_logstore, _mock_collector,
        _mock_github, _mock_user_manager,
    )

    mock_settings = _mock_settings()
    mock_store = _mock_logstore()
    mock_col = _mock_collector()
    mock_gh = _mock_github()
    mock_um = _mock_user_manager()

    @asynccontextmanager
    async def _test_lifespan(app):
        api_module.settings = mock_settings
        api_module.logstore = mock_store
        api_module.collector = mock_col
        api_module.github_service = mock_gh
        api_module.error_detector = None
        api_module.user_manager = mock_um
        yield

    api_module.app.router.lifespan_context = _test_lifespan

    with TestClient(api_module.app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture
def auth_headers(boom_client):
    resp = boom_client.post(
        "/api/auth/login",
        json={"username": "testuser", "password": "testpass"},
    )
    assert resp.status_code == 200, resp.text
    return {"Authorization": f"Bearer {resp.json()['token']}"}


# ---------------------------------------------------------------------------
# Production mode — must NOT leak internals
# ---------------------------------------------------------------------------

class TestProductionMode:
    def test_prod_hides_stack(self, boom_client, auth_headers, monkeypatch):
        # Force production: debug=False + neutral env
        api_module.settings.debug = False
        for var in ("PULSARCD_ENV", "ENVIRONMENT", "ENV", "PYTHON_ENV", "NODE_ENV"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("NODE_ENV", "production")

        resp = boom_client.get("/api/_boom/runtime", headers=auth_headers)
        assert resp.status_code == 500
        body = resp.json()

        # Generic message, no stack, no leak of the secret SQL string.
        assert body.get("error") == "Internal server error"
        assert "stack" not in body
        assert "message" not in body
        assert api_module._BOOM_SECRET not in resp.text
        assert "RuntimeError" not in resp.text
        assert "Traceback" not in resp.text

        # request_id is present and non-empty (correlatable with server logs)
        assert isinstance(body.get("request_id"), str) and body["request_id"]
        assert resp.headers.get("X-Request-ID") == body["request_id"]

    def test_prod_value_error_returns_400(self, boom_client, auth_headers, monkeypatch):
        api_module.settings.debug = False
        monkeypatch.setenv("NODE_ENV", "production")

        resp = boom_client.get("/api/_boom/value", headers=auth_headers)
        assert resp.status_code == 400
        body = resp.json()
        # 4xx is a client-class error — message is generic but not 500-flavored
        assert body.get("error") == "Request error"
        assert "bad input from caller" not in resp.text
        assert "request_id" in body

    def test_prod_keyerror_returns_404(self, boom_client, auth_headers, monkeypatch):
        api_module.settings.debug = False
        monkeypatch.setenv("NODE_ENV", "production")

        resp = boom_client.get("/api/_boom/keyerror", headers=auth_headers)
        assert resp.status_code == 404

    def test_prod_permission_returns_403(self, boom_client, auth_headers, monkeypatch):
        api_module.settings.debug = False
        monkeypatch.setenv("NODE_ENV", "production")

        resp = boom_client.get("/api/_boom/permission", headers=auth_headers)
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Development mode — full stack exposed for debugging
# ---------------------------------------------------------------------------

class TestDevelopmentMode:
    def test_dev_exposes_stack(self, boom_client, auth_headers, monkeypatch):
        # debug=True forces dev mode regardless of NODE_ENV
        api_module.settings.debug = True
        monkeypatch.setenv("NODE_ENV", "production")

        resp = boom_client.get("/api/_boom/runtime", headers=auth_headers)
        assert resp.status_code == 500
        body = resp.json()

        assert body.get("error") == "RuntimeError"
        assert "stack" in body and "Traceback" in body["stack"]
        assert "message" in body
        assert api_module._BOOM_SECRET in body["message"]
        assert "request_id" in body

        # cleanup
        api_module.settings.debug = False

    def test_dev_via_node_env(self, boom_client, auth_headers, monkeypatch):
        api_module.settings.debug = False
        monkeypatch.delenv("PULSARCD_ENV", raising=False)
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        monkeypatch.setenv("NODE_ENV", "development")

        resp = boom_client.get("/api/_boom/value", headers=auth_headers)
        assert resp.status_code == 400
        body = resp.json()
        # In dev mode, the original message should be exposed
        assert "bad input from caller" in body.get("message", "")
        assert "stack" in body


# ---------------------------------------------------------------------------
# Production is the safe-by-default
# ---------------------------------------------------------------------------

class TestDefaultIsProduction:
    def test_no_env_defaults_to_production(self, boom_client, auth_headers, monkeypatch):
        api_module.settings.debug = False
        for var in ("PULSARCD_ENV", "ENVIRONMENT", "ENV", "PYTHON_ENV", "NODE_ENV"):
            monkeypatch.delenv(var, raising=False)

        resp = boom_client.get("/api/_boom/runtime", headers=auth_headers)
        assert resp.status_code == 500
        body = resp.json()
        assert body.get("error") == "Internal server error"
        assert "stack" not in body
