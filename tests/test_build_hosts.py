"""Tests for backend/build_hosts.py: host choice, .env sync, pipeline routing."""

import base64
import types
from unittest.mock import AsyncMock, MagicMock

import pytest

import backend.api as api
from backend import build_hosts, recovery
from backend.background_actions import BackgroundAction
from backend.pipeline_state import PipelineStateManager
from tests.test_pipeline_chain import FakeDeployer, _run


@pytest.fixture(autouse=True)
def _reset():
    build_hosts.reset()
    yield
    build_hosts.reset()


class TestParse:
    @pytest.mark.parametrize("value,expected", [
        (None, []),
        ("", []),
        ("192.168.1.34", [("192.168.1.34", "192.168.1.34")]),
        ("server-c=192.168.1.34, server-d=10.0.0.9 ,", [("server-c", "192.168.1.34"), ("server-d", "10.0.0.9")]),
        ("=1.2.3.4,label=", []),
    ])
    def test_parse(self, value, expected):
        assert build_hosts.parse_build_hosts(value) == expected

    def test_non_string_is_empty(self):
        assert build_hosts.parse_build_hosts(object()) == []


class TestChoose:
    def test_least_loaded_primary_first_on_tie(self):
        assert build_hosts.choose("app", ["primary", "c"], {"primary": 0, "c": 0}) == "primary"
        assert build_hosts.choose("app", ["primary", "c"], {"primary": 2, "c": 0}) == "c"

    def test_affinity_kept_within_slack(self):
        build_hosts._affinity["app"] = "c"
        assert build_hosts.choose("app", ["primary", "c"], {"primary": 0, "c": 1}) == "c"

    def test_affinity_dropped_when_host_much_busier(self):
        build_hosts._affinity["app"] = "c"
        assert build_hosts.choose("app", ["primary", "c"], {"primary": 0, "c": 2}) == "primary"

    def test_affinity_dropped_when_host_unreachable(self):
        build_hosts._affinity["app"] = "c"
        assert build_hosts.choose("app", ["primary"], {"primary": 3}) == "primary"


def _config(build_hosts_value):
    from backend.config import GitHubConfig
    return GitHubConfig(ssh_host="dockerhost", ssh_user="gildas", build_hosts=build_hosts_value)


class TestPick:
    async def test_no_extra_host_returns_primary_without_probing(self, monkeypatch):
        async def probe(*_):
            raise AssertionError("no health check expected")
        monkeypatch.setattr(build_hosts, "_is_reachable", probe)
        primary = object()
        assert await build_hosts.pick_build_deployer("app", primary, "manager", _config(None)) == (primary, "manager")
        assert await build_hosts.pick_build_deployer("app", primary, "manager", None) == (primary, "manager")

    async def test_unreachable_secondary_is_skipped(self, monkeypatch):
        monkeypatch.setattr(api, "_background_actions", {})
        busy = BackgroundAction("a1", "build", "other")
        busy.host = "manager"
        api._background_actions["a1"] = busy

        async def probe(deployer, address):
            return address == "dockerhost"
        monkeypatch.setattr(build_hosts, "_is_reachable", probe)
        primary = object()
        assert await build_hosts.pick_build_deployer(
            "app", primary, "manager", _config("server-c=192.168.1.34")) == (primary, "manager")

    async def test_least_loaded_secondary_gets_the_job_and_env_source(self, monkeypatch):
        monkeypatch.setattr(api, "_background_actions", {})
        for i in range(2):
            a = BackgroundAction(f"a{i}", "build", "other")
            a.host = "manager"
            api._background_actions[a.id] = a

        async def probe(deployer, address):
            return True
        monkeypatch.setattr(build_hosts, "_is_reachable", probe)
        primary = object()
        deployer, label = await build_hosts.pick_build_deployer(
            "app", primary, "manager", _config("server-c=192.168.1.34"))
        assert label == "server-c"
        assert deployer.config.ssh_host == "192.168.1.34"
        assert deployer.config.ssh_user == "gildas"
        assert deployer.env_source is primary
        # Sticks to server-c for this repo while loads stay close
        assert build_hosts._affinity["app"] == "server-c"


class TestSyncEnvs:
    async def test_copies_only_changed_files_of_the_repo(self, monkeypatch):
        source, target = types.SimpleNamespace(name="src"), types.SimpleNamespace(name="dst")
        files = {
            "src": {"app/devops/.env": b"A=1", "app/devops/.env.qa": b"Q=1", "other/devops/.env": b"X"},
            "dst": {"app/devops/.env.qa": b"Q=1", "app/devops/.env": b"A=0"},
        }
        writes = []

        async def fake_operation(deployer, operation, **kw):
            store = files[deployer.name]
            if operation == "list":
                return {"files": sorted(store)}
            if operation == "read":
                v = store.get(kw["resource"])
                return {"content": None if v is None else base64.b64encode(v).decode()}
            if operation == "write":
                current = store.get(kw["resource"])
                expected = kw["expected"]
                assert (None if current is None else base64.b64encode(current).decode()) == expected
                store[kw["resource"]] = base64.b64decode(kw["content"])
                writes.append(kw["resource"])
                return {"written": True}
            raise AssertionError(operation)

        monkeypatch.setattr(recovery, "file_operation", fake_operation)
        assert await build_hosts.sync_repo_envs(source, target, "app") == 1
        assert writes == ["app/devops/.env"]
        assert files["dst"]["app/devops/.env"] == b"A=1"
        assert "other/devops/.env" not in files["dst"]


@pytest.fixture
def pipeline_env(monkeypatch, tmp_path):
    """Isolated pipeline state with a FakeDeployer as the primary (manager-1)."""
    mgr = PipelineStateManager(data_dir=str(tmp_path))
    monkeypatch.setattr(api, "pipeline_state", mgr)
    monkeypatch.setattr(api, "_auto_build_state", {})
    monkeypatch.setattr(api, "_background_actions", {})
    monkeypatch.setattr(api, "_buildable_cache", {})
    llm = MagicMock()
    llm.evaluate_gate = AsyncMock(return_value=(True, "looks good"))
    llm.handle_failure = AsyncMock()
    monkeypatch.setattr(api, "llm_agent", llm)
    ns = types.SimpleNamespace(state=mgr, deployer=FakeDeployer())
    monkeypatch.setattr(api, "_get_deployer_and_host", lambda: (ns.deployer, "manager-1"))
    return ns


class TestPipelineRouting:
    async def test_build_and_test_on_build_host_deploy_on_primary(self, pipeline_env, monkeypatch):
        env = pipeline_env
        builder = FakeDeployer()

        async def pick(repo_name, deployer, host_name, github_config):
            return builder, "server-c"
        monkeypatch.setattr(build_hosts, "pick_build_deployer", pick)

        assert await _run() is True
        assert builder.stages() == ["build", "test"]
        assert env.deployer.stages() == ["deploy"]
        actions = {a.action_type: a for a in api._background_actions.values()}
        assert actions["build"].host == "server-c"
        assert actions["test"].host == "server-c"
        assert actions["build"].result["host"] == "server-c"
        assert actions["deploy"].result["host"] == "manager-1"


class TestPrepareSecondaryHost:
    def _deployer(self, monkeypatch, primary_ok=True, clone_ok=True):
        primary = types.SimpleNamespace(
            _run_command=AsyncMock(return_value=(primary_ok, "git@github.com:me/PulsarCD.git\n")))
        d = build_hosts.deployer_for(_config("c=192.168.1.34"), "192.168.1.34", env_source=primary)
        d._ensure_docker_login = AsyncMock()
        d._ensure_repo_cloned = AsyncMock(return_value=(clone_ok, "msg"))
        sync = AsyncMock(return_value=2)
        monkeypatch.setattr(build_hosts, "sync_repo_envs", sync)
        return d, primary, sync

    async def test_primary_is_untouched(self, monkeypatch):
        from backend.github_service import StackDeployer
        d = StackDeployer(_config(None))
        await d._prepare_secondary_host("app")  # no env_source: nothing to do

    async def test_updates_scripts_then_envs(self, monkeypatch):
        d, primary, sync = self._deployer(monkeypatch)
        lines = []
        await d._prepare_secondary_host("app", lines.append)
        d._ensure_docker_login.assert_awaited_once()
        d._ensure_repo_cloned.assert_awaited_once_with("PulsarCD", "git@github.com:me/PulsarCD.git")
        sync.assert_awaited_once_with(primary, d, "app")
        assert "2 .env file(s) refreshed" in lines[-1]

    async def test_pulsarcd_build_skips_scripts_update(self, monkeypatch):
        d, primary, sync = self._deployer(monkeypatch)
        await d._prepare_secondary_host("PulsarCD")
        d._ensure_repo_cloned.assert_not_awaited()
        sync.assert_awaited_once()

    async def test_failure_is_logged_and_raised(self, monkeypatch):
        d, primary, sync = self._deployer(monkeypatch, clone_ok=False)
        lines = []
        with pytest.raises(RuntimeError):
            await d._prepare_secondary_host("app", lines.append)
        assert "not ready" in lines[-1]
        sync.assert_not_awaited()
