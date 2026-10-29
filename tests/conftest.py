"""Shared pytest fixtures for PulsarCD tests.

The API tests replace all external infrastructure (log store, Collector,
GitHub) with AsyncMock objects. The storage tests run against a real
VictoriaLogs instead (the ``vlogs`` fixture): the binary on PATH, started on a
temporary directory (the test image ships it), or an instance named by
VL_TEST_URL. Without either, those tests are skipped.
"""

import os
import shutil
import socket
import subprocess
import time
import urllib.request
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Real VictoriaLogs for the storage tests
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(f"{url}/health", timeout=2) as response:
            return response.status == 200
    except Exception:
        return False


@pytest.fixture(scope="session")
def vlogs(tmp_path_factory):
    """Base URL of a VictoriaLogs instance the tests may write to."""
    url = os.environ.get("VL_TEST_URL")
    if url:
        if not _healthy(url):
            pytest.skip(f"VL_TEST_URL={url} is not reachable")
        yield url.rstrip("/")
        return
    binary = shutil.which("victoria-logs") or shutil.which("victoria-logs-prod")
    if not binary:
        pytest.skip("no VictoriaLogs binary on PATH and no VL_TEST_URL")
    port = _free_port()
    process = subprocess.Popen(
        [binary, f"-storageDataPath={tmp_path_factory.mktemp('vlogs')}",
         f"-httpListenAddr=127.0.0.1:{port}", "-retentionPeriod=100y", "-loggerLevel=ERROR"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 20
    while not _healthy(url):
        if process.poll() is not None or time.monotonic() > deadline:
            process.kill()
            pytest.fail("VictoriaLogs did not start")
        time.sleep(0.1)
    yield url
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()


# ---------------------------------------------------------------------------
# Minimal mock factories
# ---------------------------------------------------------------------------

def _mock_settings():
    from backend.config import (
        AuthConfig, CollectorConfig, GitHubConfig,
        MCPConfig, PreviewConfig, SwarmConfig, VictoriaLogsConfig,
    )
    s = MagicMock()
    s.auth = AuthConfig(
        google_client_id="test-client-id.apps.googleusercontent.com",
        google_admins=["boss@example.com"],
        google_viewers=[],
        username="testuser",
        password="testpass",
        jwt_secret="test-jwt-secret-32-chars-minimum!",
        jwt_expiry_hours=1,
        agent_key="test-agent-key",
    )
    s.victorialogs = VictoriaLogsConfig(url="http://localhost:9428")
    s.collector = CollectorConfig()
    s.github = GitHubConfig()
    s.swarm = SwarmConfig(secret_key="")
    s.mcp = MCPConfig(enabled=False)
    # A real field lets monkeypatch restore it between feature-toggle tests;
    # a lazily created MagicMock attribute can be deleted during teardown.
    s.previews = PreviewConfig()
    s.hosts = []
    s.debug = False
    return s


def _mock_logstore():
    m = AsyncMock()
    m.get_dashboard_stats = AsyncMock(return_value={
        "total_logs": 0, "error_count": 0, "warn_count": 0,
        "info_count": 0, "containers": 0, "hosts": 0,
    })
    m.get_latest_container_stats = AsyncMock(return_value={})
    m.search_logs = AsyncMock(return_value={"hits": [], "total": 0})
    m.close = AsyncMock()
    m.health = AsyncMock(return_value=True)
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


def _mock_user_manager():
    """Break-glass administrator double that accepts testuser/testpass."""
    m = MagicMock()
    from types import SimpleNamespace
    test_user = SimpleNamespace(username="testuser", role="admin", token_epoch=0)

    def _authenticate(username, password):
        if username == "testuser" and password == "testpass":
            return test_user
        return None

    m.authenticate = _authenticate
    m.enabled = True
    m.describe = MagicMock(return_value={"username": "testuser", "role": "admin"})
    return m


def _mock_email_allowlist():
    """Allowlist double holding one admin and one viewer address."""
    m = MagicMock()
    entries = {"boss@example.com": "admin", "watcher@example.com": "viewer"}
    m.role_for = MagicMock(side_effect=lambda email: entries.get((email or "").lower()))
    m.token_epoch_for = MagicMock(
        side_effect=lambda email: 0 if (email or "").lower() in entries else None)
    m.admin_count = MagicMock(return_value=1)
    m.list_entries = MagicMock(return_value=[
        {"email": email, "role": role, "managed": role == "admin"}
        for email, role in entries.items()
    ])
    return m


def _mock_google_verifier(enabled=True):
    """Google verifier double; verify() is patched per test when needed."""
    m = MagicMock()
    m.enabled = enabled
    m.client_id = "test-client-id.apps.googleusercontent.com"
    return m


# ---------------------------------------------------------------------------
# Patch app lifespan before creating the TestClient
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def client(tmp_path_factory):
    """TestClient with all infrastructure mocked via a patched lifespan."""
    import backend.api as api_module
    from backend.ip_blocklist import IpBlocklist

    mock_settings = _mock_settings()
    mock_store = _mock_logstore()
    mock_col = _mock_collector()
    mock_gh = _mock_github()
    mock_um = _mock_user_manager()
    mock_allowlist = _mock_email_allowlist()
    mock_google = _mock_google_verifier()
    # The real store, on a throwaway file: it is pure local state, and the
    # endpoints are worth testing against what actually persists.
    blocklist = IpBlocklist(path=str(tmp_path_factory.mktemp("data") / "blocked_ips.json"))

    @asynccontextmanager
    async def _test_lifespan(app):
        # Inject mocks directly into the module's globals
        api_module.settings = mock_settings
        api_module.logstore = mock_store
        api_module.collector = mock_col
        api_module.github_service = mock_gh
        api_module.error_detector = None
        api_module.user_manager = mock_um
        api_module.email_allowlist = mock_allowlist
        api_module.google_verifier = mock_google
        api_module.ip_blocklist = blocklist
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
