"""Recovery drills against PostgreSQL when PULSARCD_TEST_DATABASE_URL is set."""

import asyncio
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest

from backend.backup_vault import BackupError, Vault
from backend.backup_cli import restore
from backend.config import GitHubConfig
from backend.github_service import StackDeployer
from backend import recovery, recovery_files

KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture
def vault():
    dsn = os.environ.get("PULSARCD_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set PULSARCD_TEST_DATABASE_URL for a disposable PostgreSQL database")
    value = Vault(dsn, KEY, "test-" + uuid.uuid4().hex)
    value.initialize()
    yield value
    with value.connect() as conn:
        conn.execute("DELETE FROM recovery_versions WHERE scope=%s", (value.scope,))


@pytest.fixture
def deployer(tmp_path, monkeypatch):
    # A loop-local lock also makes these tests independent across pytest loops.
    monkeypatch.setattr(recovery, "file_lock", asyncio.Lock())
    monkeypatch.delenv("PULSARCD_BACKUP__ENABLED", raising=False)
    return StackDeployer(GitHubConfig(repos_path=str(tmp_path)))


def test_invalid_key_rejected_without_leaking_it():
    with pytest.raises(BackupError) as exc:
        Vault("postgres://password-in-uri", "private-bad-key")
    assert "private-bad-key" not in str(exc.value)
    assert "password-in-uri" not in str(exc.value)


def test_ciphertext_only_dedup_history_and_rollback(vault):
    resource = "sample/devops/.env"
    first = vault.save("env", resource, b"TOKEN=secret\n\n", source="scan")
    assert vault.save("env", resource, b"TOKEN=secret\n\n", source="scan") == first
    pending = vault.save("env", resource, b"TOKEN=next\n", source="editor", state="pending")
    assert vault.read("env", resource) == b"TOKEN=secret\n\n"
    assert vault.read("env", resource, pending) == b"TOKEN=next\n"
    vault.mark_applied(pending)
    assert vault.read("env", resource) == b"TOKEN=next\n"
    assert vault.read("env", resource, first) == b"TOKEN=secret\n\n"
    assert len(vault.history("env", resource)) == 2
    with vault.connect() as conn:
        row = conn.execute("SELECT * FROM recovery_versions WHERE id=%s", (first,)).fetchone()
    assert b"TOKEN=secret" not in bytes(row["ciphertext"])
    assert "TOKEN=secret" not in json.dumps(row["metadata"])
    assert len(bytes(row["nonce"])) == 12


def test_tamper_wrong_key_and_cross_resource_rejected(vault):
    revision = vault.save("ssh", "id_ed25519", b"private-key", source="scan")
    with vault.connect() as conn:
        row = conn.execute("SELECT * FROM recovery_versions WHERE id=%s", (revision,)).fetchone()
    modified = copy.deepcopy(row)
    modified["metadata"]["resource"] = "id_rsa"
    with pytest.raises(BackupError):
        vault.decrypt(modified)
    modified = copy.deepcopy(row)
    modified["ciphertext"] = bytes(row["ciphertext"])[:-1] + bytes([row["ciphertext"][-1] ^ 1])
    with pytest.raises(BackupError):
        vault.decrypt(modified)
    wrong = Vault(vault.connection_string, base64.b64encode(b"z" * 32).decode(), vault.scope)
    with pytest.raises(BackupError):
        wrong.read("ssh", "id_ed25519")
    with pytest.raises(BackupError):
        vault.read("env", "somewhere/.env", revision)


async def test_backup_failure_prevents_editor_write(deployer, tmp_path, monkeypatch):
    path = tmp_path / "repo/devops/.env"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"OLD=value\n")
    def fail():
        raise BackupError("Database unavailable")
    monkeypatch.setattr(recovery, "get_vault", fail)
    ok, message = await deployer.save_env_file("repo", "NEW=value\n")
    assert not ok and "unavailable" in message
    assert path.read_bytes() == b"OLD=value\n"


async def test_editor_backs_up_previous_and_new_before_write(vault, deployer, tmp_path, monkeypatch):
    monkeypatch.setattr(recovery, "get_vault", lambda: vault)
    path = tmp_path / "repo/devops/.env"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"OLD=value\n\n")
    payload = "# preserved\r\nTOKEN='$(touch should-not-exist)'\r\n\r\n"
    ok, _ = await deployer.save_env_file("repo", payload, actor="admin@example.test")
    assert ok
    assert path.read_bytes() == payload.encode()
    versions = vault.history("env", "repo/devops/.env")
    assert len(versions) == 2
    assert versions[0]["state"] == "applied"
    assert versions[0]["actor"] == "admin@example.test"
    assert vault.read("env", "repo/devops/.env", versions[1]["id"]) == b"OLD=value\n\n"


async def test_failed_remote_write_leaves_recoverable_pending_revision(vault, deployer, monkeypatch):
    monkeypatch.setattr(recovery, "get_vault", lambda: vault)
    original = recovery.file_operation
    async def fail_write(deployer, operation, **kwargs):
        if operation == "write":
            raise BackupError("Disk unavailable")
        return await original(deployer, operation, **kwargs)
    monkeypatch.setattr(recovery, "file_operation", fail_write)
    ok, _ = await deployer.save_env_file("repo", "TOKEN=new\n")
    assert not ok
    versions = vault.history("env", "repo/devops/.env")
    assert len(versions) == 1 and versions[0]["state"] == "pending"
    assert vault.resources() == []
    assert vault.read("env", "repo/devops/.env", versions[0]["id"]) == b"TOKEN=new\n"


async def test_initial_scan_manual_changes_ssh_and_full_restore(vault, deployer, tmp_path, monkeypatch):
    monkeypatch.setattr(recovery, "get_vault", lambda: vault)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".env").write_bytes(b"ROOT=first\n\n")
    (repo / "devops").mkdir()
    (repo / "devops/.env").write_bytes(b"DEPLOY=one\n")
    ssh = tmp_path / "ssh-source"
    ssh.mkdir()
    (ssh / "id_ed25519").write_bytes(b"private-key\n")
    (ssh / "known_hosts").write_bytes(b"host key\n")
    monkeypatch.setenv("PULSARCD_BACKUP__SSH_PATH", str(ssh))
    await recovery.scan(deployer)
    await recovery.scan(deployer)
    assert len(vault.history("env", "repo/.env")) == 1
    (repo / ".env").write_bytes(b"ROOT=manual-change\n")
    await recovery.scan(deployer)
    (repo / ".env").unlink()
    await recovery.scan(deployer)
    assert len(vault.history("env", "repo/.env")) == 2
    destination = tmp_path / "recovered"
    assert restore(vault, destination, vault.resources()) == 4
    assert (destination / "env/repo/.env").read_bytes() == b"ROOT=manual-change\n"
    assert (destination / "env/repo/devops/.env").read_bytes() == b"DEPLOY=one\n"
    assert (destination / "ssh/id_ed25519").read_bytes() == b"private-key\n"
    with pytest.raises(FileExistsError):
        restore(vault, destination, vault.resources())


def test_cli_restore_without_server(vault, tmp_path):
    vault.save("env", "repo/devops/.env", b"TOKEN=recovery\n", source="scan")
    destination = tmp_path / "standalone"
    env = dict(os.environ, DATABASE_CONNECTION_STRING=vault.connection_string,
               ENCRYPTION_KEY=KEY, PULSARCD_BACKUP__SCOPE=vault.scope)
    result = subprocess.run([sys.executable, "-m", "backend.backup_cli", "restore-all",
                             "--output-dir", str(destination)], env=env, capture_output=True)
    assert result.returncode == 0, result.stderr.decode()
    assert b"TOKEN=recovery" not in result.stdout + result.stderr
    assert (destination / "env/repo/devops/.env").read_bytes() == b"TOKEN=recovery\n"


@pytest.mark.parametrize("resource", ["../escaped", "/etc/passwd", "repo/../../key", "C:/key", "..\\key"])
def test_restore_path_traversal_refused(tmp_path, resource):
    with pytest.raises(ValueError):
        recovery_files.safe_path(tmp_path, resource)


def test_atomic_write_refuses_concurrent_edit(tmp_path):
    path = tmp_path / ".env"
    path.write_bytes(b"MANUAL=edit")
    with pytest.raises(ValueError):
        recovery_files.handle(dict(root=str(tmp_path), resource=".env", operation="write",
                                   content=base64.b64encode(b"REPLACEMENT=x").decode(), expected=None))
    assert path.read_bytes() == b"MANUAL=edit"


def test_symlink_refused(tmp_path):
    target = tmp_path / "outside"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("Symlink creation requires privileges on this platform")
    with pytest.raises(ValueError):
        recovery_files.safe_path(tmp_path, "link/.env")


def test_viewer_cannot_access_recovery_metadata(client):
    from backend.auth import create_token
    from backend.api import settings
    token = create_token("roletest", settings.auth.jwt_secret, 1, role="viewer")
    headers = {"Authorization": f"Bearer {token}"}
    assert client.get("/api/admin/recovery/status", headers=headers).status_code == 403
    assert client.get("/api/admin/recovery/env/repo/history", headers=headers).status_code == 403
    assert client.post("/api/admin/recovery/env/repo/restore/id", headers=headers).status_code == 403


def test_admin_history_and_restore(vault, client, auth_headers, monkeypatch, tmp_path):
    from backend import backup_vault
    from backend.api import settings
    monkeypatch.setattr(backup_vault, "get_vault", lambda: vault)
    monkeypatch.setattr(recovery, "get_vault", lambda: vault)
    monkeypatch.setattr(settings.github, "repos_path", str(tmp_path))
    monkeypatch.setattr(recovery, "file_lock", asyncio.Lock())
    revision = vault.save("env", "repo/devops/.env", b"TOKEN=restore-me\n", source="scan")
    response = client.get("/api/admin/recovery/env/repo/history", headers=auth_headers)
    assert response.status_code == 200
    assert "restore-me" not in response.text
    response = client.post(f"/api/admin/recovery/env/repo/restore/{revision}", headers=auth_headers)
    assert response.status_code == 200, response.text
    assert "restore-me" not in response.text
    assert (tmp_path / "repo/devops/.env").read_bytes() == b"TOKEN=restore-me\n"


@pytest.mark.parametrize("method", ["build", "deploy"])
async def test_pipeline_stops_when_backup_fails(deployer, monkeypatch, method):
    from unittest.mock import AsyncMock
    deployer._ensure_docker_login = AsyncMock()
    deployer._ensure_repo_cloned = AsyncMock()
    monkeypatch.setattr(recovery, "snapshot_envs", AsyncMock(side_effect=BackupError("Backup unavailable")))
    result = await getattr(deployer, method)("repo", "git@github.com:owner/repo.git")
    assert not result["success"]
    assert "Backup unavailable" in result["output"]
    deployer._ensure_repo_cloned.assert_not_called()


async def test_secret_content_is_only_sent_on_stdin(deployer, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    connection = SimpleNamespace(run=AsyncMock(return_value=SimpleNamespace(
        stdout='{"written":true}', exit_status=0)))
    client = SimpleNamespace(connect=AsyncMock(return_value=connection))
    deployer._get_ssh_client = AsyncMock(return_value=client)
    encoded = base64.b64encode(b"SECRET=do-not-log-me\n").decode()
    await recovery.file_operation(deployer, "write", resource="repo/devops/.env",
                                  content=encoded, expected=None)
    call = connection.run.call_args
    assert "do-not-log-me" not in call.args[0]
    assert encoded not in call.args[0]
    assert json.loads(call.kwargs["input"])["content"] == encoded
