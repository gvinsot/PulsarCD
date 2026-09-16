"""Backup orchestration shared by the editor, MCP and periodic reconciliation."""

import asyncio
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex

import structlog

from .backup_vault import BackupError, get_vault
from . import recovery_files

logger = structlog.get_logger()
# PulsarCD currently runs one backend replica. Serialize scans and editor writes
# across all StackDeployer instances; no local persistent state is needed.
file_lock = asyncio.Lock()
status = {"enabled": False, "last_success": None, "last_error": None, "files": 0}


async def file_operation(deployer, operation, **kwargs):
    """Pass sensitive input on stdin, never in command arguments or log callbacks."""
    request = dict(root=deployer.config.repos_path, operation=operation, **kwargs)
    code = Path(recovery_files.__file__).read_text(encoding="utf-8")
    command = "python3 -c " + shlex.quote(code)
    try:
        client = await deployer._get_ssh_client()
        connection = await client.connect() if client else None
        if connection:
            result = await connection.run(command, input=json.dumps(request),
                                          check=False, timeout=60)
            output, returncode = result.stdout, result.exit_status
        else:
            if deployer.host_client is not None and not client:
                raise BackupError("Recovery requires the configured build-host SSH connection")
            from .config import wrap_command_for_user
            import sys
            if os.name == "nt":
                process = await asyncio.create_subprocess_exec(
                    sys.executable, "-c", code, stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            else:
                process = await asyncio.create_subprocess_shell(
                    wrap_command_for_user(command), stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                stdout, _ = await asyncio.wait_for(
                    process.communicate(json.dumps(request).encode()), timeout=60)
            except BaseException:
                process.kill()
                await process.wait()
                raise
            output, returncode = stdout.decode(), process.returncode
        if returncode:
            raise BackupError("Recovery file operation failed; check permissions or concurrent edits")
        return json.loads(output)
    except BackupError:
        raise
    except Exception:
        raise BackupError("Unable to access recovery files on the build host") from None


async def read_env(deployer, resource):
    response = await file_operation(deployer, "read", resource=resource)
    value = response["content"]
    return None if value is None else base64.b64decode(value, validate=True)


async def save_env(deployer, repo_name, content, actor="operator"):
    resource = f"{repo_name}/devops/.env"
    async with file_lock:
        vault = await asyncio.to_thread(get_vault)
        previous = await read_env(deployer, resource)
        revision = None
        if vault:
            if previous is not None:
                await asyncio.to_thread(vault.save, "env", resource, previous,
                                        source="before-edit", actor=actor)
            revision = await asyncio.to_thread(vault.save, "env", resource, content,
                                             source="editor", actor=actor, state="pending")
        await file_operation(deployer, "write", resource=resource,
                             content=base64.b64encode(content).decode(),
                             expected=None if previous is None else base64.b64encode(previous).decode())
        if vault:
            try:
                await asyncio.to_thread(vault.mark_applied, revision)
            except BackupError:
                raise BackupError("File written and backed up, but application status is unconfirmed") from None
        return revision


async def snapshot_envs(deployer, repo_name=None):
    vault = await asyncio.to_thread(get_vault)
    if vault is None:
        return 0
    async with file_lock:
        response = await file_operation(deployer, "list")
        resources = response["files"]
        if repo_name is not None:
            resources = [r for r in resources if r.startswith(repo_name + "/")]
        for resource in resources:
            content = await read_env(deployer, resource)
            if content is None:
                raise BackupError("An environment file disappeared during the backup scan")
            await asyncio.to_thread(vault.save, "env", resource, content, source="scan")
        return len(resources)


def snapshot_ssh(vault):
    root = Path(os.environ.get("PULSARCD_BACKUP__SSH_PATH", "~/.ssh")).expanduser()
    if not root.is_dir() or root.is_symlink():
        raise BackupError("SSH backup directory is missing or is a symlink")
    count = 0
    try:
        def failed(error):
            raise error
        for directory, dirs, files in os.walk(root, onerror=failed, followlinks=False):
            if any((Path(directory) / name).is_symlink() for name in dirs):
                raise ValueError()
            for name in files:
                resource = (Path(directory) / name).relative_to(root).as_posix()
                path = recovery_files.safe_path(root, resource)
                # SSH agent/control sockets are ephemeral, not recovery material.
                if path.is_socket():
                    continue
                content = recovery_files.read_file(path)
                if content is None:
                    raise ValueError()
                vault.save("ssh", resource, content, source="scan")
                count += 1
        return count
    except BackupError:
        raise
    except Exception:
        raise BackupError("Unable to back up SSH files (permissions, symlink or file size)") from None


async def scan(deployer):
    vault = await asyncio.to_thread(get_vault)
    status["enabled"] = vault is not None
    if vault is None:
        return
    count = await snapshot_envs(deployer)
    count += await asyncio.to_thread(snapshot_ssh, vault)
    status.update(last_success=datetime.now(timezone.utc).isoformat(), last_error=None, files=count)


async def monitor(config):
    from .github_service import StackDeployer
    try:
        interval = max(10, int(os.environ.get("PULSARCD_BACKUP__INTERVAL_SECONDS", "60")))
    except ValueError:
        interval = 60
    deployer = StackDeployer(config)
    while True:
        try:
            await scan(deployer)
        except Exception:
            # Never log exception values: driver errors can contain credentials.
            status["last_error"] = "Recovery backup failed; check database, key and file access"
            logger.error("Recovery backup failed; check database, key and file access")
        await asyncio.sleep(interval)
