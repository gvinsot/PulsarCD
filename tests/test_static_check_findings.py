"""Regression tests for the bugs the static checks (ruff, mypy) found."""

import asyncio
from types import SimpleNamespace
from unittest import mock

from backend.collector import Collector
from backend.config_file import PulsarConfig, TagCleanupConfig
from backend.tag_cleanup import TagCleaner


def test_mcp_import_error_survives_the_except_block():
    """The lifespan hook logs this name; an "except ... as" name is unbound."""
    import backend.api as api_module

    assert isinstance(api_module._mcp_import_error, str)
    if api_module._mcp_available:
        assert api_module._mcp_import_error == ""


def test_registry_cleanup_resolves_the_stack_name():
    """_repo_to_stack_name lives on StackDeployer; GitHubService raised AttributeError."""
    cleaner = TagCleaner(TagCleanupConfig(), github_service=None, registry_url="registry.local")
    deleted_calls = []

    async def fake_delete(repo_name, tag):
        deleted_calls.append((repo_name, tag))
        return True

    with mock.patch.object(cleaner, "_registry_list_repos",
                           mock.AsyncMock(return_value=["my-app/web", "my-app", "other/web"])), \
            mock.patch.object(cleaner, "_delete_registry_tag", side_effect=fake_delete):
        deleted = asyncio.run(cleaner._delete_registry_images("My_App", "1.2.3", "abcdef123"))

    assert deleted == ["my-app/web:1.2.3", "my-app/web:abcdef1", "my-app:1.2.3", "my-app:abcdef1"]
    assert ("other/web", "1.2.3") not in deleted_calls


def _swarm_collector(manager_client, exec_client):
    collector = Collector.__new__(Collector)
    collector._swarm_routing_enabled = True
    collector._swarm_manager_host = "manager"
    collector._swarm_manager_hostname = "manager-node"
    collector.clients = {"manager": manager_client}
    container = SimpleNamespace(labels={"com.docker.swarm.service.name": "app_web"})
    collector._find_container = mock.AsyncMock(return_value=container)
    collector._get_exec_client = mock.Mock(return_value=exec_client)
    return collector


def test_swarm_restart_uses_force_update_when_supported():
    manager = SimpleNamespace(force_update_service=mock.AsyncMock(return_value=(True, "updated")))
    collector = _swarm_collector(manager, exec_client=None)

    result = asyncio.run(collector.execute_action("worker", "abc", "restart"))

    assert result == (True, "updated")
    manager.force_update_service.assert_awaited_once_with("app_web")


def test_swarm_restart_on_ssh_manager_falls_back_to_container_restart():
    """swarm_routing allows an SSH manager, whose client has no force_update_service."""
    manager = SimpleNamespace()  # like SSHClient: no Swarm service API
    exec_client = SimpleNamespace(execute_container_action=mock.AsyncMock(return_value=(True, "restarted")))
    collector = _swarm_collector(manager, exec_client)

    result = asyncio.run(collector.execute_action("manager", "abc", "restart"))

    assert result == (True, "restarted")
    exec_client.execute_container_action.assert_awaited_once()


def test_settings_accepts_the_loaded_config_file():
    """pulsar_config is typed as PulsarConfig so attribute access type-checks."""
    from backend.config import Settings

    settings = Settings(pulsar_config=PulsarConfig())
    assert isinstance(settings.pulsar_config, PulsarConfig)
    assert Settings().pulsar_config is None
