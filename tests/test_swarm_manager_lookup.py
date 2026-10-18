"""One swarm-manager lookup (backend.config.find_swarm_manager) for every caller.

The terminal and the pipeline used to carry their own copy: the pipeline gave
None when no host was flagged, the terminal fell back to the first host, which
with several hosts opened an admin shell on an arbitrary machine.
"""

from unittest.mock import patch

import pytest

import backend.api as api_module
from backend.auth import create_token
from backend.config import HostConfig, find_swarm_manager
from backend.routes import terminal


def _host(name, manager=False):
    return HostConfig(name=name, swarm_manager=manager)


@pytest.fixture
def hosts(client):
    """Swap settings.hosts for the test and restore it afterwards."""
    original = api_module.settings.hosts

    def _set(value):
        api_module.settings.hosts = value

    yield _set
    api_module.settings.hosts = original


class TestFindSwarmManager:
    @pytest.mark.parametrize("fallback", [False, True])
    def test_flagged_manager_wins_over_list_order(self, fallback):
        manager = _host("b", manager=True)
        assert find_swarm_manager([_host("a"), manager, _host("c")],
                                  fallback_to_only_host=fallback) is manager

    @pytest.mark.parametrize("fallback", [False, True])
    def test_several_unflagged_hosts_give_none(self, fallback):
        assert find_swarm_manager([_host("a"), _host("b")],
                                  fallback_to_only_host=fallback) is None

    def test_single_unflagged_host_needs_the_fallback(self):
        only = _host("solo")
        assert find_swarm_manager([only]) is None
        assert find_swarm_manager([only], fallback_to_only_host=True) is only

    @pytest.mark.parametrize("fallback", [False, True])
    def test_no_hosts_give_none(self, fallback):
        assert find_swarm_manager([], fallback_to_only_host=fallback) is None


class TestPipelineCaller:
    def test_uses_the_shared_helper_without_fallback(self, hosts):
        manager = _host("mgr", manager=True)
        hosts([manager])
        with patch("backend.pipeline_runner.find_swarm_manager",
                   wraps=find_swarm_manager) as spy:
            assert api_module._get_swarm_manager_host() == "mgr"
        spy.assert_called_once_with(api_module.settings.hosts)

    def test_single_unflagged_host_is_not_a_manager(self, hosts):
        hosts([_host("solo")])
        assert api_module._get_swarm_manager_host() is None


class TestTerminalCaller:
    def test_uses_the_shared_helper_with_the_single_host_fallback(self, hosts):
        hosts([_host("solo")])
        with patch("backend.routes.terminal.find_swarm_manager",
                   wraps=find_swarm_manager) as spy:
            assert terminal._find_swarm_manager_config().name == "solo"
        spy.assert_called_once_with(api_module.settings.hosts,
                                    fallback_to_only_host=True)

    def test_flagged_manager_is_chosen(self, hosts):
        hosts([_host("a"), _host("mgr", manager=True)])
        assert terminal._find_swarm_manager_config().name == "mgr"

    def test_several_unflagged_hosts_do_not_fall_back_to_the_first(self, hosts):
        hosts([_host("first"), _host("second")])
        assert terminal._find_swarm_manager_config() is None

    def test_websocket_refuses_to_pick_an_arbitrary_host(self, client, hosts):
        hosts([_host("first"), _host("second")])
        token = create_token("alice", api_module.settings.auth.jwt_secret, 1, role="admin")
        async def _shell(websocket, *args):
            # Stand-in for a real shell: say so and hang up, so a regression
            # fails the assertion below instead of blocking receive_text.
            await websocket.send_text("shell opened")
            await websocket.close()

        with patch.object(terminal, "_handle_ssh_terminal", side_effect=_shell) as ssh, \
                patch.object(terminal, "_handle_local_terminal", side_effect=_shell) as local:
            with client.websocket_connect(f"/api/terminal/ws?token={token}") as ws:
                text = ws.receive_text()
        assert "No swarm manager host configured" in text
        ssh.assert_not_called()
        local.assert_not_called()
