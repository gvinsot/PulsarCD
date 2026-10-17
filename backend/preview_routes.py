"""HTTP routes for per-branch preview environments (see backend/previews.py).

    GET    /api/previews                  recorded previews + unmanaged pv- stacks
    GET    /api/previews/plan             names a preview would get (no side effect)
    POST   /api/previews                  build + deploy a branch preview (background action)
    DELETE /api/previews/{stack}          docker stack rm a recorded preview
    POST   /api/previews/cleanup          remove expired previews now

Mutating routes are admin-only through the central role policy in
auth_middleware (every non-GET /api route). Deploys run as a regular
BackgroundAction, so the existing /api/stacks/actions/{id}/... status, log
stream and cancel endpoints work for them unchanged.

The route module reaches the API's globals (settings, background actions,
deployer selection) lazily through ``backend.api`` to avoid an import cycle.
"""

import asyncio
import os
import re
import traceback
import uuid
from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from .previews import PREVIEWS_FILE, PreviewError, PreviewManager, PreviewStore

logger = structlog.get_logger()

router = APIRouter(prefix="/api/previews", tags=["previews"])

_manager: Optional[PreviewManager] = None


def _api():
    from . import api
    return api


def get_manager() -> PreviewManager:
    """The process-wide PreviewManager, created on first use."""
    global _manager
    if _manager is None:
        api = _api()
        settings = api.settings
        store = PreviewStore(os.path.join(settings.data_dir, PREVIEWS_FILE))
        _manager = PreviewManager(
            store, settings.previews,
            deployer_factory=lambda: api._get_deployer_and_host()[0],
        )
    return _manager


def set_manager(manager: Optional[PreviewManager]) -> None:
    """Replace the manager (tests, shutdown)."""
    global _manager
    _manager = manager


def start_cleanup() -> None:
    """Start the TTL cleanup loop; called from the API lifespan."""
    try:
        get_manager().start()
    except Exception as e:
        logger.warning("Preview cleanup not started", error_type=type(e).__name__, error=str(e))


async def stop_cleanup() -> None:
    if _manager is not None:
        await _manager.stop()


def _require_enabled() -> PreviewManager:
    manager = get_manager()
    if not manager.config.enabled:
        raise HTTPException(status_code=404, detail="Preview environments are disabled")
    return manager


def _http(e: PreviewError) -> HTTPException:
    return HTTPException(status_code=e.status_code, detail=str(e))


@router.get("")
async def list_previews():
    """Recorded previews with running state and expiry, plus unmanaged pv- stacks."""
    manager = _require_enabled()
    return await manager.list_previews()


@router.get("/plan")
async def plan_preview(
    repo_name: str = Query(..., description="Repository name"),
    branch: str = Query(..., description="Branch to preview"),
):
    """Names (slug, stack, image tag, host prefix) a preview would get."""
    manager = _require_enabled()
    try:
        names = manager.plan(repo_name, branch)
    except PreviewError as e:
        raise _http(e)
    return names._asdict()


@router.post("")
async def deploy_preview(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    branch: str = Query(..., description="Branch to build and preview"),
    ttl_hours: Optional[int] = Query(default=None, ge=0, le=24 * 365,
                                     description="Hours without redeploy before removal (0 = never; default from settings)"),
):
    """Build ``branch`` under a branch-derived image tag and deploy it as a preview.

    Runs in the background; returns the action id and the preview's names.
    """
    api = _api()
    manager = _require_enabled()
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    try:
        names = manager.plan(repo_name, branch)
        from .github_service import _validate_ssh_url
        try:
            _validate_ssh_url(ssh_url)
        except ValueError as e:
            raise PreviewError(str(e), 400) from e
        # Same owner extraction as the build endpoint
        match = re.search(r'[:/]([^/]+)/[^/]+\.git$', ssh_url)
        if match:
            valid, error = await api.github_service.validate_branch(match.group(1), repo_name, branch)
            if not valid:
                raise PreviewError(error, 400)
        await manager.check_can_deploy(repo_name, names)
    except PreviewError as e:
        raise _http(e)

    action_id = str(uuid.uuid4())[:8]
    try:
        # Re-checked without awaiting: a concurrent request may have reserved
        # the stack (or the last slot) while this one awaited Docker above.
        manager.begin(repo_name, ssh_url, names, ttl_hours, action_id)
    except PreviewError as e:
        raise _http(e)
    action = api.BackgroundAction(action_id, "preview-deploy", repo_name)
    api._background_actions[action_id] = action

    async def _run():
        try:
            result = await manager.run_deploy(
                repo_name, ssh_url, names,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            action.result = result
            if action.cancel_event.is_set():
                action.status = "cancelled"
            else:
                action.status = "completed" if result.get("success") else "failed"
        except Exception as e:
            detail = f"{type(e).__name__}: {e}"
            logger.exception("Preview deploy failed", repo=repo_name, stack=names.stack,
                             error=str(e), traceback=traceback.format_exc())
            action.append_output(detail)
            action.result = {"success": False, "output": detail, "action": "preview-deploy",
                             "repo": repo_name, "stack": names.stack}
            action.status = "failed"

    action.task = asyncio.create_task(_run())
    return {"action_id": action_id, "action_type": "preview-deploy", "repo": repo_name,
            **names._asdict()}


@router.post("/cleanup")
async def cleanup_previews():
    """Remove every preview whose TTL has elapsed, now."""
    manager = _require_enabled()
    removed = await manager.cleanup_expired()
    return {"removed": removed}


@router.delete("/{stack}")
async def teardown_preview(stack: str):
    """Tear down a recorded preview (``docker stack rm``). Never touches prod or QA."""
    manager = _require_enabled()
    try:
        return await manager.teardown(stack)
    except PreviewError as e:
        raise _http(e)
