"""Backup recovery endpoints (/api/admin/recovery/*).

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

import asyncio

from fastapi import APIRouter, HTTPException, Request

from ..github_service import StackDeployer
from .stacks import _ENV_QUERY
from .. import api

router = APIRouter()


@router.get("/api/admin/recovery/status")
async def recovery_status():
    from ..recovery import status
    return dict(status)


@router.get("/api/admin/recovery/env/{repo_name}/history")
async def env_backup_history(repo_name: str, env: str = _ENV_QUERY):
    from ..backup_vault import get_vault, BackupError
    from ..github_service import _validate_repo_name
    from ..recovery import env_resource
    try:
        _validate_repo_name(repo_name)
        vault = await asyncio.to_thread(get_vault)
        if vault is None:
            raise HTTPException(503, "Encrypted backup is disabled")
        versions = await asyncio.to_thread(vault.history, "env", env_resource(repo_name, env))
        return {"versions": versions}
    except ValueError:
        raise HTTPException(400, "Invalid repository name") from None
    except BackupError as exc:
        raise HTTPException(503, str(exc)) from None


@router.get("/api/admin/recovery/env/{repo_name}/versions/{revision}")
async def env_backup_version(repo_name: str, revision: str, env: str = _ENV_QUERY):
    """Content of one backed-up version, for the editor's history panel.

    Admin-only like everything under /api/admin/ — the same audience that
    already reads the live .env through GET /api/stacks/{repo}/env.
    """
    from ..backup_vault import get_vault, BackupError
    from ..github_service import _validate_repo_name
    from ..recovery import env_resource
    try:
        _validate_repo_name(repo_name)
        vault = await asyncio.to_thread(get_vault)
        if vault is None:
            raise HTTPException(503, "Encrypted backup is disabled")
        content = await asyncio.to_thread(vault.read, "env", env_resource(repo_name, env), revision)
        return {"revision": revision, "content": content.decode("utf-8")}
    except (ValueError, UnicodeError):
        raise HTTPException(400, "Invalid repository or environment content") from None
    except BackupError as exc:
        raise HTTPException(503, str(exc)) from None


@router.post("/api/admin/recovery/env/{repo_name}/restore/{revision}")
async def restore_env_backup(repo_name: str, revision: str, request: Request,
                             env: str = _ENV_QUERY):
    from ..backup_vault import get_vault, BackupError
    from ..github_service import _validate_repo_name
    from ..recovery import env_resource
    try:
        _validate_repo_name(repo_name)
        vault = await asyncio.to_thread(get_vault)
        if vault is None:
            raise HTTPException(503, "Encrypted backup is disabled")
        content = await asyncio.to_thread(vault.read, "env", env_resource(repo_name, env), revision)
        success, message = await StackDeployer(api.settings.github).save_env_file(
            repo_name, content.decode("utf-8"), actor=getattr(request.state, "user", "operator"),
            env=env)
        if not success:
            raise HTTPException(503, message)
        return {"success": True, "message": message}
    except (ValueError, UnicodeError):
        raise HTTPException(400, "Invalid repository or environment content") from None
    except BackupError as exc:
        raise HTTPException(503, str(exc)) from None
