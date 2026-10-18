"""Pipeline endpoints (/api/stacks/pipeline*, /api/stacks/auto-build/status).

The chain itself runs in backend.pipeline_runner.  App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).

Tests patch _trigger_pipeline on backend.api, so it is called through it as
well.
"""

import re
from typing import Any, Dict

import structlog
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .. import run_history
from .. import api

logger = structlog.get_logger()

router = APIRouter()


@router.post("/api/stacks/pipeline")
async def trigger_pipeline_endpoint(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    tag: str = Query(default=None, description="Git tag to build and deploy (format: vX.X.X)"),
    commit: str = Query(default=None, description="Commit SHA to build (will be auto-tagged)"),
):
    """Trigger a full pipeline (Build → Test → Deploy) from a tag or commit.

    Either `tag` or `commit` must be provided. If `commit` is provided without a tag,
    the next version is computed automatically and the commit is tagged before building.
    """
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    if not tag and not commit:
        raise HTTPException(status_code=400, detail="Either 'tag' or 'commit' must be provided")

    # Check if pipeline is already running for this repo
    existing = api.pipeline_state.get_legacy(repo_name)
    if existing.get("status") == "running":
        raise HTTPException(status_code=409, detail=f"Pipeline already running for {repo_name}")

    if tag:
        # Existing flow: pipeline from a known tag
        if not re.match(r'^v?\d+(\.\d+){1,2}$', tag):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X (e.g., v1.0.5)"
            )
        version = tag.lstrip('v')
        started = await api._trigger_pipeline(repo_name, ssh_url, version=version, tag=tag)
        if not started:
            raise HTTPException(
                status_code=409,
                detail=f"A build is already running for {repo_name}"
            )
        return {"status": "started", "repo": repo_name, "tag": tag, "version": version}
    else:
        # New flow: auto-tag the commit, then run pipeline
        owner_match = re.search(r'[:/]([^/]+)/[^/]+\.git$', ssh_url)
        if not owner_match:
            raise HTTPException(status_code=400, detail="Cannot parse owner from ssh_url")
        owner = owner_match.group(1)

        next_version = await api.github_service.get_next_version(owner, repo_name)
        new_tag = f"v{next_version}"

        result = await api.github_service.create_tag(owner, repo_name, new_tag, commit)
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create tag {new_tag}: {result.get('error', 'unknown')}"
            )

        logger.info("Auto-tagged commit for pipeline", repo=repo_name, tag=new_tag, commit=commit[:7])
        run_history.set_commit(commit)
        started = await api._trigger_pipeline(repo_name, ssh_url, version=next_version, tag=new_tag)
        if not started:
            raise HTTPException(
                status_code=409,
                detail=f"A build is already running for {repo_name}"
            )
        return {"status": "started", "repo": repo_name, "tag": new_tag, "version": next_version, "auto_tagged": True}


@router.get("/api/stacks/pipeline/status")
async def get_pipeline_status():
    """Get pipeline state for all repos."""
    return {"pipelines": api.pipeline_state.get_all_legacy()}


@router.get("/api/stacks/pipeline/{repo_name}/transition/{transition}")
async def get_transition_config(repo_name: str, transition: str):
    """Get per-project transition config for a specific transition."""
    valid = {"version_to_build", "build_to_test", "test_to_deploy", "qa_to_deploy"}
    if transition not in valid:
        return JSONResponse({"error": "Invalid transition"}, status_code=400)
    config = api.pipeline_state.get_transition_config(repo_name, transition)
    # Also return last gate decision if available
    entry = api.pipeline_state.get(repo_name)
    last_decision = None
    if entry:
        for g in reversed(entry.gates):
            if g.transition == transition:
                last_decision = g.to_dict()
                break
    return {"repo": repo_name, "transition": transition, "config": config, "last_decision": last_decision}


@router.put("/api/stacks/pipeline/{repo_name}/transition/{transition}")
async def set_transition_config(repo_name: str, transition: str, request: Request):
    """Set per-project transition config for a specific transition.

    For test_to_deploy, accepts an optional ``qa_enabled`` boolean: when true,
    the pipeline runs an isolated QA deploy (stack prefixed ``qa-``, domains
    prefixed ``qa.``) before the production deploy.
    """
    valid = {"version_to_build", "build_to_test", "test_to_deploy", "qa_to_deploy"}
    if transition not in valid:
        return JSONResponse({"error": "Invalid transition"}, status_code=400)
    body = await request.json()
    mode = body.get("mode", "auto_with_success")
    valid_modes = {"auto", "auto_with_success", "agent", "manual"}
    if mode not in valid_modes:
        return JSONResponse({"error": f"Invalid mode. Valid: {', '.join(sorted(valid_modes))}"}, status_code=400)
    cfg: Dict[str, Any] = {"mode": mode}
    if transition == "test_to_deploy":
        cfg["qa_enabled"] = bool(body.get("qa_enabled", False))
    api.pipeline_state.set_transition_config(repo_name, transition, cfg)
    logger.info("Transition config updated", repo=repo_name, transition=transition,
                mode=mode, qa_enabled=cfg.get("qa_enabled"))
    return {"saved": True, "repo": repo_name, "transition": transition, **cfg}


@router.get("/api/stacks/auto-build/status")
async def get_auto_build_status():
    """Get auto-build state for all repos."""
    return {"state": api._auto_build_state}
