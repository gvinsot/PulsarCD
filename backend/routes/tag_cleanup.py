"""Registry tag cleanup endpoints (/api/admin/tag-cleanup/*).

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from fastapi import APIRouter, HTTPException, Query

from .. import api

router = APIRouter()


# ============== Tag Cleanup ==============

@router.post("/api/admin/tag-cleanup/run")
async def trigger_tag_cleanup(
    dry_run: bool = Query(default=None, description="Override dry_run setting for this run"),
):
    """Manually trigger a tag cleanup cycle."""
    if not api.tag_cleaner:
        raise HTTPException(status_code=400, detail="Tag cleanup not configured")

    # Temporarily override dry_run if requested
    original_dry_run = api.tag_cleaner._config.dry_run
    if dry_run is not None:
        api.tag_cleaner._config.dry_run = dry_run
    try:
        result = await api.tag_cleaner.run_cleanup()
    finally:
        api.tag_cleaner._config.dry_run = original_dry_run

    return result


@router.post("/api/admin/tag-cleanup/repo/{owner}/{repo}")
async def trigger_repo_tag_cleanup(
    owner: str,
    repo: str,
    dry_run: bool = Query(default=None, description="Override dry_run setting for this run"),
):
    """Manually trigger tag cleanup for a single repository."""
    if not api.tag_cleaner:
        raise HTTPException(status_code=400, detail="Tag cleanup not configured")

    original_dry_run = api.tag_cleaner._config.dry_run
    if dry_run is not None:
        api.tag_cleaner._config.dry_run = dry_run
    try:
        result = await api.tag_cleaner.run_cleanup_repo(owner, repo)
    finally:
        api.tag_cleaner._config.dry_run = original_dry_run

    return result


@router.get("/api/admin/tag-cleanup/config")
async def get_tag_cleanup_config():
    """Get current tag cleanup configuration."""
    if not api.settings.pulsar_config:
        raise HTTPException(status_code=400, detail="PulsarConfig not loaded")
    return api.settings.pulsar_config.tag_cleanup.model_dump()
