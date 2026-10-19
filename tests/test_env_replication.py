"""Editor saves replicate the selected file to every configured build host."""

import asyncio
import base64
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backend import build_hosts, recovery, recovery_files
from backend.backup_vault import BackupError
from backend.config import GitHubConfig
from backend.github_service import StackDeployer


@pytest.fixture
def hosts(monkeypatch, tmp_path):
    config = GitHubConfig(
        ssh_host="primary", build_hosts="server-c=secondary-c,server-d=secondary-d",
        repos_path=str(tmp_path / "primary"),
    )
    state = SimpleNamespace(
        primary=StackDeployer(config),
        roots={name: tmp_path / name for name in ("primary", "secondary-c", "secondary-d")},
        calls=[], hook=None,
    )

    async def operation(deployer, action, **kwargs):
        address = deployer.config.ssh_host
        state.calls.append((address, action, kwargs))
        if state.hook:
            await state.hook(address, action, kwargs)
        return recovery_files.handle(dict(root=state.roots[address], operation=action, **kwargs))

    monkeypatch.setattr(recovery, "file_operation", operation)
    monkeypatch.setattr(recovery, "get_vault", lambda: None)
    monkeypatch.setattr(recovery, "file_lock", asyncio.Lock())
    return state


def seed(hosts, address, resource, content):
    path = hosts.roots[address] / resource
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


@pytest.mark.parametrize("value,expected", [
    (None, []),
    ("", []),
    ("self=primary", []),
    ("self=primary, server-c=secondary-c,duplicate=secondary-c,secondary-d,server-d=secondary-d",
     [("server-c", "secondary-c"), ("secondary-d", "secondary-d")]),
])
def test_replica_targets_exclude_primary_and_deduplicate_addresses(value, expected):
    config = GitHubConfig(ssh_host="primary", build_hosts=value)
    assert build_hosts.env_replica_targets(config) == expected


@pytest.mark.parametrize("env,filename,other_filename", [
    ("prod", ".env", ".env.qa"), ("qa", ".env.qa", ".env"),
])
async def test_save_replicates_exact_selected_file_to_all_hosts(hosts, env, filename, other_filename):
    resource = f"repo/devops/{filename}"
    other_resource = f"repo/devops/{other_filename}"
    content = "# preserved\r\nTOKEN='$(not-a-command)'\r\nUNICODE=café\r\n\r\n"
    for address in hosts.roots:
        seed(hosts, address, resource, b"OLD=previous\n")
        seed(hosts, address, other_resource, b"OTHER=unchanged\n")
        seed(hosts, address, f"another-repo/devops/{filename}", b"UNRELATED=unchanged\n")

    ok, message = await hosts.primary.save_env_file("repo", content, env=env)

    assert ok, message
    assert "server-c" in message and "server-d" in message
    for root in hosts.roots.values():
        assert (root / resource).read_bytes() == content.encode("utf-8")
        assert (root / other_resource).read_bytes() == b"OTHER=unchanged\n"
        assert (root / f"another-repo/devops/{filename}").read_bytes() == b"UNRELATED=unchanged\n"
    assert all(kwargs["resource"] == resource for _, _, kwargs in hosts.calls)
    assert {address for address, action, _ in hosts.calls if action == "write"} == set(hosts.roots)


async def test_save_creates_missing_replica_directories(hosts):
    assert not any(root.exists() for root in hosts.roots.values())

    ok, message = await hosts.primary.save_env_file("new-repo", "INITIAL=1\n", env="qa")

    assert ok, message
    for root in hosts.roots.values():
        assert (root / "new-repo/devops/.env.qa").read_bytes() == b"INITIAL=1\n"
        assert not (root / "new-repo/devops/.env").exists()
        assert not list(root.rglob(".pulsarcd-*"))


async def test_primary_failure_never_attempts_replicas(hosts):
    async def fail_primary(address, action, kwargs):
        if address == "primary" and action == "write":
            raise BackupError("Primary disk unavailable")

    hosts.hook = fail_primary
    ok, message = await hosts.primary.save_env_file("repo", "TOKEN=new\n")

    assert not ok and "Primary disk unavailable" in message
    assert {address for address, _, _ in hosts.calls} == {"primary"}
    assert not any(root.exists() for root in hosts.roots.values())


async def test_partial_failure_attempts_other_hosts_and_can_retry_unchanged_content(hosts, capsys, caplog):
    secret = "never-expose-this-replication-secret"
    content = f"TOKEN={secret}\n"

    async def fail_one(address, action, kwargs):
        if address == "secondary-c":
            raise RuntimeError(f"SSH failed with credential {secret}")

    hosts.hook = fail_one
    ok, message = await hosts.primary.save_env_file("repo", content)

    assert not ok
    assert "saved on the primary" in message
    assert "server-c" in message and "server-d" not in message
    assert "retry" in message.lower()
    assert secret not in message
    assert (hosts.roots["primary"] / "repo/devops/.env").read_bytes() == content.encode()
    assert (hosts.roots["secondary-d"] / "repo/devops/.env").read_bytes() == content.encode()
    assert not (hosts.roots["secondary-c"] / "repo/devops/.env").exists()

    hosts.hook = None
    hosts.calls.clear()
    ok, message = await hosts.primary.save_env_file("repo", content)

    assert ok, message
    assert "server-c" in message and "server-d" in message
    for root in hosts.roots.values():
        assert (root / "repo/devops/.env").read_bytes() == content.encode()
    # A retry repairs the missing copy without rewriting an up-to-date replica.
    assert not any(address == "secondary-d" and action == "write"
                   for address, action, _ in hosts.calls)
    output = capsys.readouterr()
    assert secret not in output.out + output.err + caplog.text


async def test_unresponsive_replica_times_out_without_blocking_other_hosts(hosts, monkeypatch):
    monkeypatch.setattr(build_hosts, "_ENV_REPLICATION_TIMEOUT", 0.02)
    cancelled = asyncio.Event()

    async def hang_one(address, action, kwargs):
        if address == "secondary-c":
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    hosts.hook = hang_one
    ok, message = await asyncio.wait_for(hosts.primary.save_env_file("repo", "VALUE=1\n"), timeout=2)

    assert not ok
    assert "server-c" in message and "server-d" not in message
    assert cancelled.is_set()
    assert (hosts.roots["primary"] / "repo/devops/.env").read_bytes() == b"VALUE=1\n"
    assert (hosts.roots["secondary-d"] / "repo/devops/.env").read_bytes() == b"VALUE=1\n"


async def test_replica_write_refuses_concurrent_manual_edit(hosts):
    resource = "repo/devops/.env"
    seed(hosts, "secondary-c", resource, b"VALUE=previous\n")

    async def edit_after_read(address, action, kwargs):
        if address == "secondary-c" and action == "write":
            seed(hosts, address, resource, b"VALUE=manual-edit\n")

    hosts.hook = edit_after_read
    ok, message = await hosts.primary.save_env_file("repo", "VALUE=new\n")

    assert not ok
    assert "server-c" in message and "server-d" not in message
    assert (hosts.roots["secondary-c"] / resource).read_bytes() == b"VALUE=manual-edit\n"
    assert (hosts.roots["primary"] / resource).read_bytes() == b"VALUE=new\n"
    assert (hosts.roots["secondary-d"] / resource).read_bytes() == b"VALUE=new\n"


async def test_concurrent_saves_do_not_allow_old_replica_to_win(hosts):
    old_content, new_content = "VALUE=old\n", "VALUE=new\n"
    old_replica_started = asyncio.Event()
    release_old_replica = asyncio.Event()
    new_save_started = asyncio.Event()

    async def delay_old_copy(address, action, kwargs):
        if (address == "secondary-c" and action == "write"
                and base64.b64decode(kwargs["content"]) == old_content.encode()):
            old_replica_started.set()
            await release_old_replica.wait()

    async def save_new():
        new_save_started.set()
        return await hosts.primary.save_env_file("repo", new_content)

    hosts.hook = delay_old_copy
    older = asyncio.create_task(hosts.primary.save_env_file("repo", old_content))
    newer = None
    try:
        await asyncio.wait_for(old_replica_started.wait(), timeout=2)
        newer = asyncio.create_task(save_new())
        await new_save_started.wait()
        await asyncio.sleep(0)
        # The lock spans the replication, so even the primary's next write waits.
        assert not newer.done()
        assert (hosts.roots["primary"] / "repo/devops/.env").read_bytes() == old_content.encode()
    finally:
        release_old_replica.set()
        results = await asyncio.wait_for(asyncio.gather(*(t for t in (older, newer) if t)), timeout=2)

    assert all(ok for ok, _ in results), results
    for root in hosts.roots.values():
        assert (root / "repo/devops/.env").read_bytes() == new_content.encode()


async def test_encrypted_backup_is_marked_applied_before_replication(hosts, monkeypatch):
    vault = MagicMock()
    vault.save.return_value = "revision-id"
    monkeypatch.setattr(recovery, "get_vault", lambda: vault)

    async def check_backup_order(address, action, kwargs):
        if address != "primary":
            vault.mark_applied.assert_called_once_with("revision-id")
            assert (hosts.roots["primary"] / "repo/devops/.env").read_bytes() == b"VALUE=new\n"

    hosts.hook = check_backup_order
    ok, message = await hosts.primary.save_env_file("repo", "VALUE=new\n")

    assert ok, message
    assert "encrypted backup recorded" in message
    for root in hosts.roots.values():
        assert (root / "repo/devops/.env").read_bytes() == b"VALUE=new\n"


async def test_backup_status_failure_still_replicates_written_content(hosts, monkeypatch):
    vault = MagicMock()
    vault.save.return_value = "revision-id"
    vault.mark_applied.side_effect = BackupError("private database error details")
    monkeypatch.setattr(recovery, "get_vault", lambda: vault)

    ok, message = await hosts.primary.save_env_file("repo", "VALUE=new\n")

    assert not ok
    assert message == "File written and backed up, but application status is unconfirmed"
    vault.mark_applied.assert_called_once_with("revision-id")
    for root in hosts.roots.values():
        assert (root / "repo/devops/.env").read_bytes() == b"VALUE=new\n"


async def test_backup_status_and_replica_failures_report_both_after_attempting_all_hosts(hosts, monkeypatch):
    vault = MagicMock()
    vault.save.return_value = "revision-id"
    vault.mark_applied.side_effect = BackupError("private database error details")
    monkeypatch.setattr(recovery, "get_vault", lambda: vault)

    async def fail_one_replica(address, action, kwargs):
        if address == "secondary-c":
            raise BackupError("private SSH error details")

    hosts.hook = fail_one_replica
    ok, message = await hosts.primary.save_env_file("repo", "VALUE=new\n")

    assert not ok
    assert "replication failed on: server-c" in message
    assert "server-d" not in message
    assert "Encrypted backup application status is also unconfirmed." in message
    assert "private" not in message
    assert (hosts.roots["primary"] / "repo/devops/.env").read_bytes() == b"VALUE=new\n"
    assert (hosts.roots["secondary-d"] / "repo/devops/.env").read_bytes() == b"VALUE=new\n"
    assert not (hosts.roots["secondary-c"] / "repo/devops/.env").exists()


async def test_no_secondary_hosts_preserves_primary_only_save(hosts):
    hosts.primary.config.build_hosts = None

    ok, message = await hosts.primary.save_env_file("repo", "VALUE=local\n")

    assert ok, message
    assert {address for address, _, _ in hosts.calls} == {"primary"}
    assert (hosts.roots["primary"] / "repo/devops/.env").read_bytes() == b"VALUE=local\n"
