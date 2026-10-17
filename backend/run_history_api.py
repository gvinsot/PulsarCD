"""HTTP endpoints for the pipeline run history (see run_history.py).

Read-only GET routes, so viewers can use them like the other pipeline and
action-log endpoints; the auth middleware applies as for any /api/ path.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from . import run_history
from .run_history import ALL_STATUSES, STAGES, TRIGGERS

logger = structlog.get_logger()

router = APIRouter(prefix="/api/pipeline", tags=["pipeline-history"])

_STAGE_PATTERN = "^(" + "|".join(STAGES) + ")$"
_STATUS_PATTERN = "^(" + "|".join(ALL_STATUSES) + ")$"
_TRIGGER_PATTERN = "^(" + "|".join(TRIGGERS) + ")$"


def _recorder() -> run_history.RunHistoryRecorder:
    rec = run_history.recorder
    if not rec.available:
        raise HTTPException(status_code=503, detail="Pipeline run history is not available")
    return rec


def _parse_bound(value: Optional[str], name: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid '{name}': expected an ISO 8601 date")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


@router.get("/runs")
async def list_pipeline_runs(
    repo: Optional[str] = Query(default=None, max_length=200),
    stage: Optional[str] = Query(default=None, pattern=_STAGE_PATTERN),
    status: Optional[str] = Query(default=None, pattern=_STATUS_PATTERN),
    trigger: Optional[str] = Query(default=None, pattern=_TRIGGER_PATTERN),
    since: Optional[str] = Query(default=None, description="ISO 8601 lower bound on start time"),
    until: Optional[str] = Query(default=None, description="ISO 8601 upper bound on start time"),
    hours: Optional[int] = Query(default=None, ge=1, le=24 * 3650,
                                 description="Shortcut for since = now - hours"),
    q: Optional[str] = Query(default=None, max_length=run_history.MAX_QUERY_LENGTH,
                             description="Full-text search in logs, error excerpts, version or commit"),
    page: int = Query(default=1, ge=1, le=10000),
    page_size: int = Query(default=20, ge=1, le=run_history.MAX_PAGE_SIZE),
) -> Dict[str, Any]:
    """List recorded build / test / qa / deploy runs, most recent first."""
    rec = _recorder()
    since_dt = _parse_bound(since, "since")
    if hours and not since_dt:
        since_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    try:
        return await rec.list_runs(
            repo=repo, stage=stage, status=status, trigger=trigger,
            since=since_dt, until=_parse_bound(until, "until"),
            q=q, page=page, page_size=page_size,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Pipeline run history query failed", error=str(e))
        raise HTTPException(status_code=502, detail="Pipeline run history query failed")


@router.get("/runs/repos")
async def list_pipeline_run_repos() -> Dict[str, List[str]]:
    """Repositories with at least one recorded run (for the UI filter)."""
    rec = _recorder()
    try:
        return {"repos": await rec.list_repos()}
    except Exception as e:
        logger.error("Pipeline run history repo listing failed", error=str(e))
        raise HTTPException(status_code=502, detail="Pipeline run history query failed")


@router.get("/runs/{run_id}")
async def get_pipeline_run(
    run_id: str,
    log_offset: int = Query(default=0, ge=-run_history.MAX_LOG_WINDOW * 50,
                            description="First log line; negative counts from the end"),
    log_limit: int = Query(default=5000, ge=1, le=run_history.MAX_LOG_WINDOW),
) -> Dict[str, Any]:
    """One run with a window of its stored log lines."""
    rec = _recorder()
    try:
        result = await rec.get_run(run_id, log_offset=log_offset, log_limit=log_limit)
    except Exception as e:
        logger.error("Pipeline run lookup failed", run_id=run_id, error=str(e))
        raise HTTPException(status_code=502, detail="Pipeline run history query failed")
    if result is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return result
