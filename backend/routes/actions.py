"""Manual build, test and deploy actions: start, status, logs and cancellation.

/api/stacks/build|deploy|test and /api/stacks/actions/*.  The status, log and
cancel routes serve every BackgroundAction, pipeline and preview ones too.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

import asyncio
import json
import re
import traceback
import uuid
from datetime import datetime
from typing import Any, Dict

import structlog
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from ..deploy_rollback import deploy_stage_with_rollback
from .. import run_history
from ..background_actions import BackgroundAction
from ..pipeline_runner import _notify_agent_failure, _set_pipeline
from .. import api

logger = structlog.get_logger()

router = APIRouter()


@router.post("/api/stacks/build")
async def build_stack(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    version: str = Query(default="1.0", description="Version tag"),
    branch: str = Query(default=None, description="Branch name to build from"),
    tag: str = Query(default=None, description="Git tag to build from (e.g., v1.0.5)"),
    commit: str = Query(default=None, description="Specific commit hash to build from"),
    no_cache: bool = Query(default=False, description="Force fresh build without Docker cache"),
):
    """Build a stack from a GitHub repository. Runs in background, returns action ID."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    owner = None
    if ssh_url:
        match = re.search(r'[:/]([^/]+)/[^/]+\.git$', ssh_url)
        if match:
            owner = match.group(1)

    # If tag is provided, derive version from it
    if tag:
        if not re.match(r'^v?\d+(\.\d+){1,2}$', tag):
            raise HTTPException(status_code=400, detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X")
        version = tag.lstrip('v')

    if branch and owner:
        is_valid, error_msg = await api.github_service.validate_branch(owner, repo_name, branch)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

    if commit and owner:
        if not re.match(r'^[a-fA-F0-9]{7,40}$', commit):
            raise HTTPException(status_code=400, detail=f"Invalid commit hash format: '{commit}'. Expected 7-40 hexadecimal characters.")
        is_valid, error_msg = await api.github_service.validate_commit(owner, repo_name, commit)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

    deployer, host_name = api._get_deployer_and_host()

    # Create background action
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "build", repo_name)
    api._background_actions[action_id] = action

    # Update pipeline state so all browsers see the build
    run_history.set_commit(commit)
    _set_pipeline(repo_name, "build", "running", version, build_id=action_id)

    async def _run_build():
        try:
            result = await deployer.build(
                repo_name, ssh_url, version, branch=branch, tag=tag, commit=commit,
                no_cache=no_cache,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            status = "success" if result.get("success") else "failed"
            _set_pipeline(repo_name, "build", status, version, build_id=action_id, log_lines=action.output_lines)
            if not result.get("success"):
                await _notify_agent_failure("build", repo_name, version, result.get("output", ""))
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background build failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": "build", "repo": repo_name}
            action.append_output(error_detail)
            _set_pipeline(repo_name, "build", "failed", version, build_id=action_id, log_lines=action.output_lines)
            await _notify_agent_failure("build", repo_name, version, error_detail)

    action.task = asyncio.create_task(_run_build())

    return {"action_id": action_id, "action_type": "build", "repo": repo_name}


@router.post("/api/stacks/deploy")
async def deploy_stack(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    version: str = Query(default="1.0", description="Version tag"),
    tag: str = Query(default=None, description="Specific tag to deploy (format: vX.X.X)"),
    qa: bool = Query(default=False, description="Deploy to the isolated QA environment (stack prefixed 'qa-')"),
):
    """Deploy a stack from a GitHub repository. Runs in background, returns action ID."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    if tag:
        if not re.match(r'^v?\d+(\.\d+){0,2}$', tag):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X (e.g., v1.0.5) or X.X.X"
            )

    deployer, host_name = api._get_deployer_and_host()

    # Create background action
    action_type = "qa-deploy" if qa else "deploy"
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, action_type, repo_name)
    api._background_actions[action_id] = action

    # Use the tag as display version when deploying a specific tag
    deploy_version = tag.lstrip('v') if tag else version
    # When deploying a specific tag (no prior build), clear build_action_id
    prev_build_id = api.pipeline_state.get_legacy(repo_name).get("build_action_id") if not tag else None

    pipeline_stage = "qa" if qa else "deploy"
    if qa:
        _set_pipeline(repo_name, pipeline_stage, "running", deploy_version, build_id=prev_build_id, qa_id=action_id)
    else:
        _set_pipeline(repo_name, pipeline_stage, "running", deploy_version, build_id=prev_build_id, deploy_id=action_id)

    async def _run_deploy():
        try:
            # Rolls back to the last good version if post-deploy verification fails
            result = await deploy_stage_with_rollback(
                deployer, api.pipeline_state, repo_name, ssh_url, version=version, tag=tag, qa=qa,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            if result.get("success"):
                if qa:
                    # Mark QA success first so current_version is updated, then
                    # apply the always-manual gate before production deploy.
                    _set_pipeline(repo_name, "qa", "success", deploy_version,
                                  qa_id=action_id, log_lines=action.output_lines)
                    api.pipeline_state.record_gate(repo_name, "qa_to_deploy", False,
                                               "Manual transition — waiting for user approval after QA",
                                               version=deploy_version)
                    _set_pipeline(repo_name, "qa", "gate_rejected", deploy_version,
                                  qa_id=action_id, log_lines=action.output_lines)
                else:
                    _set_pipeline(repo_name, "done", "success", deploy_version, deploy_id=action_id, log_lines=action.output_lines)
            else:
                _set_pipeline(repo_name, pipeline_stage, "failed", deploy_version,
                              deploy_id=None if qa else action_id,
                              qa_id=action_id if qa else None,
                              log_lines=action.output_lines)
                await _notify_agent_failure(pipeline_stage, repo_name, deploy_version, result.get("output", ""))
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background deploy failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": action_type, "repo": repo_name}
            action.append_output(error_detail)
            _set_pipeline(repo_name, pipeline_stage, "failed", deploy_version,
                          deploy_id=None if qa else action_id,
                          qa_id=action_id if qa else None,
                          log_lines=action.output_lines)
            await _notify_agent_failure(pipeline_stage, repo_name, deploy_version, error_detail)

    action.task = asyncio.create_task(_run_deploy())

    return {"action_id": action_id, "action_type": action_type, "repo": repo_name}


@router.post("/api/stacks/test")
async def test_stack(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    branch: str = Query(default=None, description="Branch name to test from"),
    tag: str = Query(default=None, description="Git tag to test from (e.g., v1.0.5)"),
    commit: str = Query(default=None, description="Specific commit hash to test from"),
):
    """Run tests for a stack. Runs in background, returns action ID."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    owner = None
    if ssh_url:
        match = re.search(r'[:/]([^/]+)/[^/]+\.git$', ssh_url)
        if match:
            owner = match.group(1)

    if branch and owner:
        is_valid, error_msg = await api.github_service.validate_branch(owner, repo_name, branch)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

    if tag and owner:
        if not re.match(r'^v?\d+(\.\d+){1,2}$', tag):
            raise HTTPException(status_code=400, detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X")

    if commit and owner:
        if not re.match(r'^[a-fA-F0-9]{7,40}$', commit):
            raise HTTPException(status_code=400, detail=f"Invalid commit hash format: '{commit}'. Expected 7-40 hexadecimal characters.")
        is_valid, error_msg = await api.github_service.validate_commit(owner, repo_name, commit)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

    deployer, host_name = api._get_deployer_and_host()

    # Create background action
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "test", repo_name)
    api._background_actions[action_id] = action

    # Use tag version if provided, otherwise keep previous version
    version = tag.lstrip('v') if tag else api.pipeline_state.get_legacy(repo_name).get("version", "")

    # Update pipeline state
    _set_pipeline(repo_name, "test", "running", version, test_id=action_id)

    async def _run_test():
        try:
            result = await deployer.test(
                repo_name, ssh_url, branch=branch, tag=tag, commit=commit,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            status = "success" if result.get("success") else "failed"
            _set_pipeline(repo_name, "test", status, version, test_id=action_id, log_lines=action.output_lines)
            # A cancelled test is the operator's doing, not an incident to investigate.
            if not result.get("success") and not action.cancel_event.is_set():
                await _notify_agent_failure("test", repo_name, version, result.get("output", ""))
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background test failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": "test", "repo": repo_name}
            action.append_output(error_detail)
            _set_pipeline(repo_name, "test", "failed", version, test_id=action_id, log_lines=action.output_lines)
            await _notify_agent_failure("test", repo_name, version, error_detail)

    action.task = asyncio.create_task(_run_test())

    return {"action_id": action_id, "action_type": "test", "repo": repo_name}


@router.get("/api/stacks/actions/{action_id}/status")
async def get_action_status(action_id: str) -> Dict[str, Any]:
    """Get the status of a background build/deploy/test action."""
    action = api._background_actions.get(action_id)
    if action:
        response = {
            "action_id": action.id,
            "action_type": action.action_type,
            "repo": action.repo_name,
            "status": action.status,
            "started_at": action.started_at.isoformat(),
            "elapsed_seconds": (datetime.utcnow() - action.started_at).total_seconds(),
        }
        if action.result:
            response["result"] = action.result
        return response

    # Fallback: reconstruct minimal status from persisted pipeline state
    if api.pipeline_state:
        for repo_name, entry in api.pipeline_state.items():
            for stage_name, stage in entry.stages.items():
                if stage.action_id == action_id:
                    return {
                        "action_id": action_id,
                        "action_type": stage_name,
                        "repo": repo_name,
                        "status": stage.status if stage.status and stage.status != "idle" else "unknown",
                        "restored": True,
                    }

    raise HTTPException(status_code=404, detail="Action not found")


@router.get("/api/stacks/actions/{action_id}/logs")
async def get_action_logs(
    action_id: str,
    offset: int = Query(default=0, ge=0, description="Line offset to start from"),
) -> Dict[str, Any]:
    """Get the streaming logs of a background build/deploy action.

    Falls back to persisted pipeline state logs when the action is no
    longer in memory (e.g. after a server restart).
    """
    action = api._background_actions.get(action_id)
    if action:
        lines = action.output_lines[offset:]
        return {
            "action_id": action.id,
            "status": action.status,
            "lines": lines,
            "offset": offset,
            "total_lines": len(action.output_lines),
        }

    # Fallback: look up the action_id in persisted pipeline state
    if api.pipeline_state:
        for repo_name, entry in api.pipeline_state.items():
            for stage_name, stage in entry.stages.items():
                if stage.action_id == action_id and stage.last_log:
                    lines = stage.last_log[offset:]
                    return {
                        "action_id": action_id,
                        "status": stage.status or "completed",
                        "lines": lines,
                        "offset": offset,
                        "total_lines": len(stage.last_log),
                        "restored": True,
                    }

    raise HTTPException(status_code=404, detail="Action not found")


@router.post("/api/stacks/actions/{action_id}/logs/themes")
async def group_action_test_themes(action_id: str, request: Request) -> Dict[str, Any]:
    """Suggest themes for parsed test metadata using the configured LLM."""
    from ..test_log_themes import MAX_THEME_PAYLOAD_BYTES, ThemeGroupingError, group_test_themes

    action = await get_action_status(action_id)
    if action["action_type"] != "test":
        raise HTTPException(status_code=400, detail="Themes are only available for test actions")

    # Allow JSON escaping overhead while bounding the request before parsing it.
    raw = bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw) > MAX_THEME_PAYLOAD_BYTES * 6 + 4096:
            raise HTTPException(status_code=400, detail="Test metadata is too large to group at once")
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        raise HTTPException(status_code=400, detail="A valid JSON object with entries is required")
    if not isinstance(body, dict) or "entries" not in body:
        raise HTTPException(status_code=400, detail="A JSON object with entries is required")

    try:
        return await group_test_themes(body["entries"])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ThemeGroupingError:
        raise HTTPException(status_code=503, detail="Test theme grouping is unavailable. Try again later.")


@router.get("/api/stacks/actions/{action_id}/logs/stream")
async def stream_action_logs(
    action_id: str,
    offset: int = Query(default=0, ge=0, description="Line offset to start from"),
):
    """Stream logs of a background action via Server-Sent Events."""
    action = api._background_actions.get(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    # One frame per line, deliberately: a cached older frontend only knows the
    # "line" type, and batching would make it silently drop every log line.
    # The client batches the DOM writes instead. Yield to the event loop every
    # _YIELD_EVERY lines so dumping a large backlog does not stall the server.
    _YIELD_EVERY = 500

    async def event_generator():
        cursor = offset
        while True:
            # Send any new lines
            if cursor < len(action.output_lines):
                pending = action.output_lines[cursor:]
                cursor += len(pending)
                for index, line in enumerate(pending, start=1):
                    data = json.dumps({"type": "line", "line": line})
                    yield f"data: {data}\n\n"
                    if index % _YIELD_EVERY == 0:
                        await asyncio.sleep(0)

            # If action is done, send final status and close
            if action.status != "running":
                data = json.dumps({"type": "done", "status": action.status})
                yield f"data: {data}\n\n"
                return

            # Clear before waiting; re-check after clear to avoid race
            action.new_line_event.clear()
            if cursor < len(action.output_lines) or action.status != "running":
                continue
            try:
                await asyncio.wait_for(action.new_line_event.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/api/stacks/actions/{action_id}/cancel")
async def cancel_action(action_id: str) -> Dict[str, Any]:
    """Cancel a running background build/deploy action."""
    action = api._background_actions.get(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")
    
    if action.status != "running":
        return {"success": False, "message": f"Action is already {action.status}"}
    
    action.cancel_event.set()
    action.append_output("[Cancellation requested...]")
    
    # Wait briefly for the task to acknowledge cancellation
    try:
        await asyncio.wait_for(asyncio.shield(action.task), timeout=5)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.warning("Error while waiting for action cancellation",
                       action_id=action_id, error=str(e))

    if action.status == "running":
        action.status = "cancelled"

    # Update pipeline state so UI reflects cancellation immediately
    kwargs = {}
    # A QA deployment runs as a "qa-deploy" action but is the "qa" pipeline stage.
    stage = "qa" if action.action_type == "qa-deploy" else action.action_type
    if stage == "build":
        kwargs["build_id"] = action_id
    elif stage == "test":
        kwargs["test_id"] = action_id
    elif stage == "qa":
        kwargs["qa_id"] = action_id
    elif stage == "deploy":
        kwargs["deploy_id"] = action_id
    elif action.action_type == "qa-deploy":
        kwargs["qa_id"] = action_id
    # A QA deploy action runs the pipeline's "qa" stage
    stage = "qa" if action.action_type == "qa-deploy" else action.action_type
    _set_pipeline(
        action.repo_name,
        stage,
        "failed",
        api.pipeline_state.get_legacy(action.repo_name).get("version", ""),
        log_lines=action.output_lines,
        **kwargs,
    )

    return {"success": True, "message": "Action cancelled"}
