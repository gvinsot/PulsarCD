"""Shared pytest fixtures for LogsCrawler tests.

All external infrastructure (OpenSearch, Collector, GitHub) is replaced with
AsyncMock objects so tests run without any real services.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Minimal mock factories
# ---------------------------------------------------------------------------

def _mock_settings():
    from backend.config import (
        AuthConfig, CollectorConfig, GitHubConfig,
        MCPConfig, OpenSearchConfig, SwarmConfig,
    )
    s = MagicMock()
    s.auth = AuthConfig(
        username="testuser",
        password="testpass",
        jwt_secret="test-jwt-secret-32-chars-minimum!",
        jwt_expiry_hours=1,
        agent_key="test-agent-key",
    )
    s.opensearch = OpenSearchConfig(hosts=["http://localhost:9200"])
    s.collector = CollectorConfig()
    s.github = GitHubConfig()
    s.swarm = SwarmConfig(secret_key="")
    s.mcp = MCPConfig(enabled=False)
    s.hosts = []
    s.debug = False
    return s


def _mock_opensearch():
    m = AsyncMock()
    m.get_dashboard_stats = AsyncMock(return_value={
        "total_logs": 0, "error_count": 0, "warn_count": 0,
        "info_count": 0, "containers": 0, "hosts": 0,
    })
    m.get_latest_container_stats = AsyncMock(return_value={})
    m.search_logs = AsyncMock(return_value={"hits": [], "total": 0})
    m.close = AsyncMock()
    return m


def _mock_collector():
    m = AsyncMock()
    m.get_all_containers = AsyncMock(return_value=[])
    m.clients = {}
    m._discovered_nodes = {}
    m.start = AsyncMock()
    m.stop = AsyncMock()
    return m


def _mock_github():
    m = AsyncMock()
    m.is_configured = MagicMock(return_value=False)
    m.close = AsyncMock()
    return m


# ---------------------------------------------------------------------------
# Patch app lifespan before creating the TestClient
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def client():
    """TestClient with all infrastructure mocked via a patched lifespan."""
    import backend.api as api_module

    mock_settings = _mock_settings()
    mock_os = _mock_opensearch()
    mock_col = _mock_collector()
    mock_gh = _mock_github()

    @asynccontextmanager
    async def _test_lifespan(app):
        # Inject mocks directly into the module's globals
        api_module.settings = mock_settings
        api_module.opensearch = mock_os
        api_module.collector = mock_col
        api_module.github_service = mock_gh
        api_module.error_detector = None
        yield
        # No teardown needed for mocks

    # Replace the lifespan on the app's router before TestClient starts it
    api_module.app.router.lifespan_context = _test_lifespan

    with TestClient(api_module.app, raise_server_exceptions=True) as c:
        yield c


@pytest.fixture(scope="session")
def auth_token(client):
    """Obtain a valid JWT for subsequent authenticated requests."""
    resp = client.post("/api/auth/login", json={"username": "testuser", "password": "testpass"})
    assert resp.status_code == 200, f"Login failed: {resp.text}"
    return resp.json()["token"]


@pytest.fixture(scope="session")
def auth_headers(auth_token):
    """Authorization header dict."""
    return {"Authorization": f"Bearer {auth_token}"}
