"""Env storage metadata comes from the host reading the selected file."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from backend import api, recovery, recovery_files
from backend.backup_vault import BackupError
from backend.config import GitHubConfig
from backend.github_service import StackDeployer


@pytest.mark.parametrize("env,filename", [("prod", ".env"), ("qa", ".env.qa"), ("build", ".env.build")])
@pytest.mark.parametrize("exists", [True, False])
def test_editor_reports_selected_file_storage(client, auth_headers, monkeypatch, tmp_path,
                                              env, filename, exists):
    monkeypatch.setattr(api.github_service, "is_configured", lambda: True)
    monkeypatch.setattr(api.settings.github, "repos_path", str(tmp_path))
    monkeypatch.setattr(recovery_files.socket, "gethostname", lambda: "build-node-2")
    if exists:
        path = tmp_path / "repo" / "devops" / filename
        path.parent.mkdir(parents=True)
        # An empty file exists, unlike an absent file with the same editor text.
        path.write_text("")

    async def operation(deployer, action, **kwargs):
        return recovery_files.handle(dict(root=deployer.config.repos_path,
                                          operation=action, **kwargs))

    monkeypatch.setattr(recovery, "file_operation", operation)
    response = client.get(f"/api/stacks/repo/env?env={env}", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == {
        "content": "", "repo": "repo", "env": env,
        "storage": {"node": "build-node-2", "exists": exists,
                    "resource": f"repo/devops/{filename}", "replica_targets": []},
    }


async def test_ssh_alias_is_not_reported_as_actual_node(monkeypatch):
    from types import SimpleNamespace
    import json

    process = SimpleNamespace(
        exit_status=0, close=MagicMock(),
        communicate=AsyncMock(return_value=(
            json.dumps({"hostname": "worker-3", "content": "WD0xCg=="}), "")))
    deployer = StackDeployer(GitHubConfig(ssh_host="dockerhost", build_hosts=(
        "primary=dockerhost,server-c=192.168.1.34,duplicate=192.168.1.34,server-d=192.168.1.92")))
    deployer._ssh_client = SimpleNamespace(connect=AsyncMock(
        return_value=SimpleNamespace(create_process=AsyncMock(return_value=process))))
    success, details = await deployer.get_env_file_details("repo", "qa")
    assert success
    assert details["storage"]["node"] == "worker-3"
    assert details["storage"]["replica_targets"] == ["server-c", "server-d"]
    assert details["content"] == "X=1\n"
    request = json.loads(process.communicate.call_args.args[0])
    assert request["resource"] == "repo/devops/.env.qa"
    # Preserve the API used by MCP callers, which only need file content.
    assert await deployer.get_env_file("repo", "qa") == (True, "X=1\n")


def test_unreachable_host_does_not_report_storage(client, auth_headers, monkeypatch):
    monkeypatch.setattr(api.github_service, "is_configured", lambda: True)
    monkeypatch.setattr(recovery, "file_operation", AsyncMock(side_effect=BackupError("unreachable")))
    response = client.get("/api/stacks/repo/env", headers=auth_headers)
    assert response.status_code == 500
    assert response.json() == {"detail": "Unable to read the environment file"}


async def test_file_details_validate_resource_before_remote_access(monkeypatch):
    operation = AsyncMock()
    monkeypatch.setattr(recovery, "file_operation", operation)
    deployer = StackDeployer(GitHubConfig())
    assert not (await deployer.get_env_file_details("../repo"))[0]
    assert not (await deployer.get_env_file_details("repo", "../env"))[0]
    operation.assert_not_called()
