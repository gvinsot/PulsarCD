"""Preview environment manager and routes (backend/previews.py, backend/preview_routes.py).

Docker and the build host are replaced by a fake StackDeployer that records
every call, so the tests check exactly which commands a preview would run.
"""

from datetime import datetime, timedelta, timezone

import pytest

from backend.config import PreviewConfig
from backend.previews import PreviewError, PreviewManager, PreviewStore
from shared.preview_naming import preview_names

SSH_URL = "git@github.com:acme/shop.git"


class FakeDeployer:
    """Records build/deploy/command calls; ``stacks`` is what docker stack ls returns."""

    def __init__(self, stacks=(), build_ok=True, deploy_ok=True, ls_ok=True, rm_ok=True):
        self.stacks = list(stacks)
        self.build_ok = build_ok
        self.deploy_ok = deploy_ok
        self.ls_ok = ls_ok
        self.rm_ok = rm_ok
        self.builds = []
        self.deploys = []
        self.commands = []
        # stack -> images its services run (docker service ls); none by default
        self.images = {}
        self.service_ls_ok = True

    async def _run_command(self, command, output_callback=None, cancel_event=None):
        self.commands.append(command)
        if command.startswith("docker stack ls"):
            return (self.ls_ok, "\n".join(self.stacks) if self.ls_ok else "error")
        if command.startswith("docker service ls --filter"):
            stack = command.split("com.docker.stack.namespace=", 1)[1].split()[0].strip("'")
            return (self.service_ls_ok, "\n".join(self.images.get(stack, [])))
        if command.startswith("docker stack rm"):
            return (self.rm_ok, "Removing service" if self.rm_ok else "permission denied")
        return (False, "unexpected command")

    async def build(self, repo_name, ssh_url, version, branch=None, **kwargs):
        self.builds.append({"repo": repo_name, "version": version, "branch": branch})
        return {"success": self.build_ok, "output": "built"}

    async def deploy(self, repo_name, ssh_url, version, preview_branch=None, **kwargs):
        self.deploys.append({"repo": repo_name, "version": version,
                             "preview_branch": preview_branch, **kwargs})
        return {"success": self.deploy_ok, "output": "deployed"}


def make_manager(tmp_path, deployer=None, **config):
    deployer = deployer or FakeDeployer()
    manager = PreviewManager(PreviewStore(str(tmp_path / "previews.json")),
                             PreviewConfig(**config), deployer_factory=lambda: deployer)
    return manager, deployer


async def deploy(manager, repo="shop", branch="feature/login", ttl=None):
    names = manager.plan(repo, branch)
    await manager.check_can_deploy(repo, names)
    manager.begin(repo, SSH_URL, names, ttl, "act1")
    return names, await manager.run_deploy(repo, SSH_URL, names)


class TestDeploy:
    async def test_builds_branch_under_preview_tag_then_deploys_preview(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, result = await deploy(manager)
        assert result["success"]
        assert deployer.builds == [{"repo": "shop", "version": names.image_tag, "branch": "feature/login"}]
        assert deployer.deploys[0]["version"] == names.image_tag
        assert deployer.deploys[0]["preview_branch"] == "feature/login"
        assert not deployer.deploys[0].get("qa")
        record = manager.store.get(names.stack)
        assert record["status"] == "active"
        assert record["last_deployed_at"]
        assert names.stack.startswith("pv-") and names.stack.endswith("-shop")

    async def test_failed_build_skips_deploy(self, tmp_path):
        manager, deployer = make_manager(tmp_path, FakeDeployer(build_ok=False))
        names, result = await deploy(manager)
        assert not result["success"]
        assert deployer.deploys == []
        assert manager.store.get(names.stack)["status"] == "failed"

    async def test_stack_is_released_after_deploy(self, tmp_path):
        manager, _ = make_manager(tmp_path)
        names, _ = await deploy(manager)
        await manager.check_can_deploy("shop", names)  # not busy any more

    async def test_redeploy_same_branch_is_allowed(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, _ = await deploy(manager)
        deployer.stacks.append(names.stack)
        await deploy(manager)
        assert len(deployer.deploys) == 2

    async def test_running_stack_without_record_is_never_overwritten(self, tmp_path):
        """A production repo called e.g. 'pv-main-shop' must not be clobbered."""
        names = preview_names("main", "shop")
        manager, deployer = make_manager(tmp_path, FakeDeployer(stacks=[names.stack]))
        with pytest.raises(PreviewError) as exc:
            await manager.check_can_deploy("shop", manager.plan("shop", "main"))
        assert exc.value.status_code == 409
        assert deployer.builds == [] and deployer.deploys == []

    async def test_stack_recorded_for_another_branch_is_refused(self, tmp_path):
        manager, _ = make_manager(tmp_path)
        names = manager.plan("shop", "main")
        manager.store.upsert(names.stack, repo="shop", branch="other", created_at="2026-01-01T00:00:00+00:00")
        with pytest.raises(PreviewError) as exc:
            await manager.check_can_deploy("shop", names)
        assert exc.value.status_code == 409

    async def test_unknown_live_state_refuses_deploy(self, tmp_path):
        manager, _ = make_manager(tmp_path, FakeDeployer(ls_ok=False))
        with pytest.raises(PreviewError) as exc:
            await manager.check_can_deploy("shop", manager.plan("shop", "main"))
        assert exc.value.status_code == 503

    async def test_busy_stack_is_refused(self, tmp_path):
        manager, _ = make_manager(tmp_path)
        names = manager.plan("shop", "main")
        manager.begin("shop", SSH_URL, names, None, "a")
        with pytest.raises(PreviewError) as exc:
            await manager.check_can_deploy("shop", names)
        assert exc.value.status_code == 409

    async def test_limit(self, tmp_path):
        manager, _ = make_manager(tmp_path, max_active=1)
        await deploy(manager, branch="one")
        with pytest.raises(PreviewError) as exc:
            await manager.check_can_deploy("shop", manager.plan("shop", "two"))
        assert exc.value.status_code == 409

    async def test_begin_rechecks_busy_and_limit(self, tmp_path):
        """Two requests that both passed check_can_deploy cannot both reserve."""
        manager, _ = make_manager(tmp_path, max_active=1)
        names = manager.plan("shop", "main")
        other = manager.plan("shop", "other")
        await manager.check_can_deploy("shop", names)
        await manager.check_can_deploy("shop", other)
        manager.begin("shop", SSH_URL, names, None, "a")
        with pytest.raises(PreviewError) as exc:
            manager.begin("shop", SSH_URL, names, None, "b")
        assert exc.value.status_code == 409
        with pytest.raises(PreviewError) as exc:
            manager.begin("shop", SSH_URL, other, None, "c")
        assert exc.value.status_code == 409

    async def test_recorded_stack_taken_over_by_production_is_not_redeployed(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, _ = await deploy(manager, branch="main")
        deployer.stacks.append(names.stack)
        # A production repo literally named like the preview now runs there.
        deployer.images[names.stack] = ["registry.example.com/pv-main-shop/web:1.2.3@sha256:abc"]
        with pytest.raises(PreviewError) as exc:
            await manager.check_can_deploy("shop", names)
        assert exc.value.status_code == 409
        deployer.images[names.stack] = [f"registry.example.com/shop/web:{names.image_tag}@sha256:abc",
                                        "postgres:16"]
        await manager.check_can_deploy("shop", names)

    @pytest.mark.parametrize("repo,branch", [
        ("../etc", "main"), ("-x", "main"), ("shop", "$(reboot)"), ("shop", "-f"), ("shop", ""),
    ])
    def test_invalid_input_is_400(self, tmp_path, repo, branch):
        manager, _ = make_manager(tmp_path)
        with pytest.raises(PreviewError) as exc:
            manager.plan(repo, branch)
        assert exc.value.status_code == 400


class TestTeardown:
    async def test_removes_recorded_preview_with_docker_stack_rm(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, _ = await deploy(manager)
        result = await manager.teardown(names.stack)
        assert result["success"]
        assert deployer.commands[-1] == f"docker stack rm {names.stack}"
        assert manager.store.get(names.stack) is None

    @pytest.mark.parametrize("stack", ["shop", "qa-shop", "privatenetwork", "pv-x;rm -rf /", "PV-x-shop"])
    async def test_prod_qa_and_garbage_are_refused(self, tmp_path, stack):
        manager, deployer = make_manager(tmp_path)
        with pytest.raises(PreviewError) as exc:
            await manager.teardown(stack)
        assert exc.value.status_code == 400
        assert not any(c.startswith("docker stack rm") for c in deployer.commands)

    async def test_unrecorded_preview_shaped_stack_is_refused(self, tmp_path):
        manager, deployer = make_manager(tmp_path, FakeDeployer(stacks=["pv-main-shop"]))
        with pytest.raises(PreviewError) as exc:
            await manager.teardown("pv-main-shop")
        assert exc.value.status_code == 404
        assert not any(c.startswith("docker stack rm") for c in deployer.commands)

    async def test_tampered_record_cannot_target_another_repo(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        manager.store.upsert("pv-main-shop", repo="billing", branch="main")
        with pytest.raises(PreviewError):
            await manager.teardown("pv-main-shop")
        assert not any(c.startswith("docker stack rm") for c in deployer.commands)

    async def test_stack_not_running_the_preview_image_is_never_removed(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, _ = await deploy(manager)
        deployer.images[names.stack] = ["registry.example.com/other/web:2.0.1"]
        with pytest.raises(PreviewError) as exc:
            await manager.teardown(names.stack)
        assert exc.value.status_code == 409
        assert not any(c.startswith("docker stack rm") for c in deployer.commands)
        assert manager.store.get(names.stack)
        # Expiry skips it as well
        assert await manager.cleanup_expired(datetime.now(timezone.utc) + timedelta(days=3650)) == []
        assert not any(c.startswith("docker stack rm") for c in deployer.commands)

    async def test_variant_image_tag_counts_as_the_preview(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, _ = await deploy(manager)
        deployer.images[names.stack] = [f"registry.example.com/shop/speech:rocm-{names.image_tag}"]
        assert (await manager.teardown(names.stack))["success"]

    async def test_unlistable_services_refuse_teardown(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, _ = await deploy(manager)
        deployer.service_ls_ok = False
        with pytest.raises(PreviewError) as exc:
            await manager.teardown(names.stack)
        assert exc.value.status_code == 503
        assert not any(c.startswith("docker stack rm") for c in deployer.commands)

    async def test_docker_failure_keeps_the_record(self, tmp_path):
        manager, deployer = make_manager(tmp_path)
        names, _ = await deploy(manager)
        deployer.rm_ok = False
        with pytest.raises(PreviewError) as exc:
            await manager.teardown(names.stack)
        assert exc.value.status_code == 502
        assert manager.store.get(names.stack)["status"] == "failed"


class TestExpiry:
    async def test_expired_previews_are_removed(self, tmp_path):
        manager, deployer = make_manager(tmp_path, ttl_hours=24)
        old, _ = await deploy(manager, branch="old")
        fresh, _ = await deploy(manager, branch="fresh")
        never, _ = await deploy(manager, branch="pinned", ttl=0)
        past = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        for stack in (old.stack, never.stack):
            manager.store.upsert(stack, last_deployed_at=past)

        removed = await manager.cleanup_expired()
        assert removed == [old.stack]
        assert manager.store.get(fresh.stack) and manager.store.get(never.stack)
        assert f"docker stack rm {old.stack}" in deployer.commands

    async def test_ttl_zero_in_settings_means_never(self, tmp_path):
        manager, _ = make_manager(tmp_path, ttl_hours=0)
        names, _ = await deploy(manager)
        far = datetime.now(timezone.utc) + timedelta(days=3650)
        assert manager.expired(far) == []
        assert manager.store.get(names.stack)["ttl_hours"] == 0

    async def test_listing_reports_expiry_running_state_and_unmanaged(self, tmp_path):
        manager, deployer = make_manager(tmp_path, ttl_hours=10)
        names, _ = await deploy(manager)
        deployer.stacks = [names.stack, "pv-ghost-shop", "shop", "qa-shop"]
        data = await manager.list_previews()
        assert [p["stack"] for p in data["previews"]] == [names.stack]
        preview = data["previews"][0]
        assert preview["running"] is True
        assert preview["expires_at"]
        assert "ssh_url" not in preview
        assert data["unmanaged"] == ["pv-ghost-shop"]


class TestStore:
    def test_corrupt_file_reads_as_empty(self, tmp_path):
        path = tmp_path / "previews.json"
        path.write_text("{not json")
        assert PreviewStore(str(path)).all() == []

    def test_non_preview_keys_are_ignored(self, tmp_path):
        path = tmp_path / "previews.json"
        path.write_text('{"shop": {"stack": "shop"}, "pv-a-shop": {"stack": "pv-a-shop"}}')
        assert [r["stack"] for r in PreviewStore(str(path)).all()] == ["pv-a-shop"]


class TestRoutes:
    @pytest.fixture
    def preview_manager(self, tmp_path):
        from backend import preview_routes
        manager, deployer = make_manager(tmp_path)
        preview_routes.set_manager(manager)
        yield manager, deployer
        preview_routes.set_manager(None)

    def test_listing_requires_authentication(self, client, preview_manager):
        assert client.get("/api/previews").status_code == 401

    def test_listing(self, client, auth_headers, preview_manager):
        resp = client.get("/api/previews", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["previews"] == []

    def test_plan(self, client, auth_headers, preview_manager):
        resp = client.get("/api/previews/plan", params={"repo_name": "shop", "branch": "feature/x"},
                          headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == preview_names("feature/x", "shop")._asdict()

    def test_plan_rejects_bad_branch(self, client, auth_headers, preview_manager):
        resp = client.get("/api/previews/plan", params={"repo_name": "shop", "branch": "a..b"},
                          headers=auth_headers)
        assert resp.status_code == 400

    def test_teardown_refuses_production_stack(self, client, auth_headers, preview_manager):
        _, deployer = preview_manager
        resp = client.delete("/api/previews/shop", headers=auth_headers)
        assert resp.status_code == 400
        assert deployer.commands == []

    def test_teardown_unknown_preview_is_404(self, client, auth_headers, preview_manager):
        resp = client.delete("/api/previews/pv-main-shop", headers=auth_headers)
        assert resp.status_code == 404

    def test_deploy_requires_github(self, client, auth_headers, preview_manager):
        resp = client.post("/api/previews", params={"repo_name": "shop", "ssh_url": SSH_URL,
                                                    "branch": "main"}, headers=auth_headers)
        assert resp.status_code == 400

    def test_viewer_cannot_tear_down(self, client, preview_manager):
        from backend.auth import create_token
        import backend.api as api_module
        token = create_token("roletest", api_module.settings.auth.jwt_secret, 1, role="viewer")
        resp = client.delete("/api/previews/pv-main-shop", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 403


class TestDeployerCommand:
    """StackDeployer.deploy(preview_branch=...) calls deploy-service.sh --preview."""

    async def run_deploy(self, monkeypatch, version="pv-feature-x", **kwargs):
        from unittest.mock import AsyncMock
        from backend import recovery
        from backend.config import GitHubConfig
        from backend.github_service import StackDeployer

        deployer = StackDeployer(GitHubConfig(repos_path="/srv/repos"))
        deployer._ensure_repo_cloned = AsyncMock(return_value=(True, ""))
        deployer._run_command = AsyncMock(return_value=(True, "ok"))
        monkeypatch.setattr(recovery, "snapshot_envs", AsyncMock(return_value=0))
        result = await deployer.deploy("shop", SSH_URL, version, **kwargs)
        return result, deployer._run_command.await_args.args[0]

    async def test_preview_flag_and_branch_checkout(self, monkeypatch):
        result, command = await self.run_deploy(monkeypatch, preview_branch="feature/x")
        assert result["success"] and result["action"] == "preview-deploy"
        assert "bash deploy-service.sh --preview feature/x /srv/repos/shop pv-feature-x feature/x" in command
        assert "--qa" not in command

    async def test_regular_deploy_is_unchanged(self, monkeypatch):
        _, command = await self.run_deploy(monkeypatch, version="1.2.3")
        assert command.endswith("bash deploy-service.sh /srv/repos/shop 1.2.3 v1.2.3")
        _, command = await self.run_deploy(monkeypatch, version="1.2.3", qa=True)
        assert command.endswith("bash deploy-service.sh --qa /srv/repos/shop 1.2.3 v1.2.3")

    async def test_unsafe_branch_never_reaches_the_shell(self, monkeypatch):
        with pytest.raises(ValueError):
            await self.run_deploy(monkeypatch, preview_branch="$(id)")
