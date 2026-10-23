"""Pool of build hosts: where builds and tests run.

The primary build host is ``github.ssh_host``: it is also the swarm manager
that deploys, edits the stacks' ``.env`` files and answers every other
host-side request. ``github.build_hosts`` adds identical hosts that only run
builds and tests, so a busy or unreachable primary does not stop the pipeline.

Every host shares the primary's SSH user, key, port, host key policy and
repos path: they are clones of it. Images go through the registry, so a build
on one host deploys from another.

Choosing a host for a build or a test:

* only hosts that answer over SSH are candidates (checked at most every
  ``_HEALTH_TTL`` seconds);
* a repo stays on the host that built it last (its layer cache lives there)
  unless that host runs clearly more jobs than the least loaded one;
* otherwise the least loaded host wins, the primary on a tie.

The primary stays the single source of truth for ``.env`` files. Editor/MCP
saves replicate the edited file to every configured secondary. Before a
build or test, the repo's ``.env`` / ``.env.qa`` / ``.env.build`` files are also copied from
the primary (``sync_repo_envs``), catching up after failed replication or
manual edits. Host selection state is in memory: a restart only costs a
build without cache.
"""

import asyncio
import base64
import time
from typing import Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger()

# Seconds a reachability check result is reused.
_HEALTH_TTL = 30
# Seconds allowed for an SSH connection plus a trivial command.
_HEALTH_TIMEOUT = 8
# Bound the save delay when a secondary is unavailable, including SSH connect.
_ENV_REPLICATION_TIMEOUT = 15
# A repo leaves its usual host only when that host runs this many more jobs
# than the least loaded one: a cold layer cache costs more than a short wait.
_AFFINITY_SLACK = 1

_health: Dict[str, Tuple[bool, float]] = {}
_affinity: Dict[str, str] = {}


def parse_build_hosts(value: Optional[str]) -> List[Tuple[str, str]]:
    """Parse ``"label=address,address2"`` into ``[(label, address), ...]``.

    A bare address is its own label.
    """
    if not isinstance(value, str):
        return []
    hosts = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        label, sep, address = item.partition("=")
        if not sep:
            label, address = item, item
        label, address = label.strip(), address.strip()
        if label and address:
            hosts.append((label, address))
    return hosts


def deployer_for(github_config, address: str, env_source=None):
    """A StackDeployer whose commands run on ``address``."""
    from .github_service import StackDeployer
    deployer = StackDeployer(github_config.model_copy(update={"ssh_host": address}))
    deployer.env_source = env_source
    return deployer


def env_replica_targets(github_config) -> List[Tuple[str, str]]:
    """Configured secondaries, once per address, excluding the primary."""
    seen = {getattr(github_config, "ssh_host", None)}
    targets = []
    for label, address in parse_build_hosts(getattr(github_config, "build_hosts", None)):
        if address not in seen:
            seen.add(address)
            targets.append((label, address))
    return targets


async def replicate_env(source, resource: str, content: bytes) -> None:
    """Replicate one saved file; the caller must hold recovery.file_lock.

    Attempt every secondary, even after a failure. Never log exception text:
    it may contain credentials. A partial failure leaves the primary and
    successful replicas saved, and can be retried with the same content.
    """
    from .backup_vault import BackupError
    from .recovery import file_operation, read_env

    async def copy_to(address):
        target = deployer_for(source.config, address)
        current = await read_env(target, resource)
        if current != content:
            await file_operation(
                target, "write", resource=resource,
                content=base64.b64encode(content).decode(),
                expected=None if current is None else base64.b64encode(current).decode(),
            )

    async def replicate(label, address):
        try:
            await asyncio.wait_for(copy_to(address), timeout=_ENV_REPLICATION_TIMEOUT)
        except Exception:
            logger.warning("Environment replication failed", resource=resource, host=label)
            return label
        return None

    results = await asyncio.gather(*(
        replicate(label, address) for label, address in env_replica_targets(source.config)
    ))
    failed = [label for label in results if label is not None]
    if failed:
        raise BackupError("File saved on the primary, but replication failed on: "
                          + ", ".join(failed) + ". Save again to retry.")


async def _is_reachable(deployer, address: str) -> bool:
    cached = _health.get(address)
    now = time.monotonic()
    if cached and now - cached[1] < _HEALTH_TTL:
        return cached[0]
    try:
        client = await deployer._get_ssh_client()
        ok, _ = await asyncio.wait_for(client.run_shell_command("true"), timeout=_HEALTH_TIMEOUT)
    except Exception as e:
        logger.warning("Build host unreachable", host=address, error=str(e))
        ok = False
    _health[address] = (ok, now)
    return ok


def _running_jobs(label: str) -> int:
    from .api import _background_actions
    return sum(
        1 for a in _background_actions.values()
        if a.status == "running" and a.action_type in ("build", "test")
        and getattr(a, "host", None) == label
    )


def choose(repo_name: str, candidates: List[str], loads: Dict[str, int]) -> str:
    """Pick a host label among ``candidates`` (primary first) for ``repo_name``."""
    least = min(candidates, key=lambda h: loads.get(h, 0))
    preferred = _affinity.get(repo_name)
    if preferred in candidates and loads.get(preferred, 0) - loads.get(least, 0) <= _AFFINITY_SLACK:
        return preferred
    return least


async def pick_build_deployer(repo_name: str, primary_deployer, primary_label: str, github_config):
    """Return ``(deployer, host_label)`` for a build or test of ``repo_name``."""
    extra = parse_build_hosts(getattr(github_config, "build_hosts", None))
    if not extra:
        return primary_deployer, primary_label

    deployers = {primary_label: primary_deployer}
    for label, address in extra:
        deployers[label] = deployer_for(github_config, address, env_source=primary_deployer)

    labels = list(deployers)
    addresses = {primary_label: github_config.ssh_host}
    addresses.update({label: address for label, address in extra})
    reachable = await asyncio.gather(*(
        _is_reachable(deployers[label], addresses[label]) for label in labels
    ))
    candidates = [label for label, ok in zip(labels, reachable) if ok]
    if not candidates:
        # Fail on the primary with its own, explicit SSH error.
        return primary_deployer, primary_label

    loads = {label: _running_jobs(label) for label in candidates}
    label = choose(repo_name, candidates, loads)
    _affinity[repo_name] = label
    logger.info("Build host selected", repo=repo_name, host=label, loads=loads,
                unreachable=[l for l in labels if l not in candidates])
    return deployers[label], label


async def sync_repo_envs(source, target, repo_name: str) -> int:
    """Copy ``repo_name``'s ``.env`` files from ``source`` to ``target``.

    Returns the number of files written. Contents travel on stdin through
    ``file_operation``, never in a command line or a log.
    """
    from .recovery import file_operation, read_env, file_lock

    async with file_lock:
        listing = await file_operation(source, "list")
        resources = [r for r in listing["files"] if r.startswith(repo_name + "/")]
        written = 0
        for resource in resources:
            content = await read_env(source, resource)
            if content is None:
                continue
            current = await read_env(target, resource)
            if current == content:
                continue
            await file_operation(
                target, "write", resource=resource,
                content=base64.b64encode(content).decode(),
                expected=None if current is None else base64.b64encode(current).decode(),
            )
            written += 1
        return written


def reset():
    """Forget health and affinity (tests)."""
    _health.clear()
    _affinity.clear()
