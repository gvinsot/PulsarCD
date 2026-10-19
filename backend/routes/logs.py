"""Log search and AI endpoints, plus manual agent task creation.

/api/logs/*, /api/ai/status and /api/tasks/create.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from ..models import (
    LogSearchQuery, LogSearchResult
)
from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ============== Logs Search ==============

@router.post("/api/logs/search", response_model=LogSearchResult)
async def search_logs(query: LogSearchQuery) -> LogSearchResult:
    """Search logs with filters."""
    return await api.logstore.search_logs(query)


@router.get("/api/logs/search")
async def search_logs_get(
    q: Optional[str] = Query(default=None, alias="query"),
    hosts: Optional[str] = Query(default=None),
    containers: Optional[str] = Query(default=None),
    compose_projects: Optional[str] = Query(default=None),
    levels: Optional[str] = Query(default=None),
    http_status_min: Optional[int] = Query(default=None),
    http_status_max: Optional[int] = Query(default=None),
    start_time: Optional[datetime] = Query(default=None),
    end_time: Optional[datetime] = Query(default=None),
    # Bounds mirror log_store.MAX_SEARCH_SIZE / MAX_SEARCH_FROM so an
    # out-of-range request is refused rather than silently truncated.
    size: int = Query(default=100, ge=1, le=500),
    from_: int = Query(default=0, ge=0, le=10000, alias="from"),
    sort_order: str = Query(default="desc", pattern="^(asc|desc)$"),
) -> LogSearchResult:
    """Search logs with GET parameters."""
    query = LogSearchQuery(
        query=q,
        hosts=hosts.split(",") if hosts else [],
        containers=containers.split(",") if containers else [],
        compose_projects=compose_projects.split(",") if compose_projects else [],
        levels=levels.split(",") if levels else [],
        http_status_min=http_status_min,
        http_status_max=http_status_max,
        start_time=start_time,
        end_time=end_time,
        size=size,
        from_=from_,
        sort_order=sort_order,
    )
    return await api.logstore.search_logs(query)


# ============== AI Query ==============

@router.post("/api/logs/ai-search")
async def ai_search_logs(request: Dict[str, str]) -> Dict[str, Any]:
    """Convert a natural-language question into search filters and run them."""
    from ..ai_service import get_ai_service
    
    natural_query = request.get("question", "")
    if not natural_query:
        raise HTTPException(status_code=400, detail="Question is required")
    
    ai = get_ai_service()
    
    # Get available metadata for better AI context (RAG)
    metadata = await api.logstore.get_available_metadata()
    
    # Convert natural language to query params with metadata context
    params = await ai.convert_to_query(natural_query, metadata)
    
    # Calculate time range
    start_time = None
    if params.get("time_range"):
        time_str = params["time_range"]
        now = datetime.utcnow()
        if time_str.endswith("m"):
            minutes = int(time_str[:-1])
            start_time = now - timedelta(minutes=minutes)
        elif time_str.endswith("h"):
            hours = int(time_str[:-1])
            start_time = now - timedelta(hours=hours)
        elif time_str.endswith("d"):
            days = int(time_str[:-1])
            start_time = now - timedelta(days=days)
    
    # Build and execute query
    query = LogSearchQuery(
        query=params.get("query"),
        hosts=params.get("hosts", []),
        containers=params.get("containers", []),
        compose_projects=params.get("compose_projects", []),
        levels=params.get("levels", []),
        http_status_min=params.get("http_status_min"),
        http_status_max=params.get("http_status_max"),
        start_time=start_time,
        size=100,
        # params comes from the LLM: anything but the two accepted values would
        # now raise a validation error, so fall back instead of returning a 500.
        sort_order=params.get("sort_order") if params.get("sort_order") in ("asc", "desc") else "desc",
    )
    
    result = await api.logstore.search_logs(query)
    
    return {
        "query_params": params,
        "result": result.model_dump(),
        "available_metadata": {
            "hosts": metadata.get("hosts", [])[:10],
            "containers": metadata.get("containers", [])[:20],
            "compose_projects": metadata.get("compose_projects", [])[:10],
        }
    }


@router.get("/api/ai/status")
async def get_ai_status() -> Dict[str, Any]:
    """Check AI service availability."""
    from ..ai_service import get_ai_service
    
    ai = get_ai_service()
    available = await ai.check_availability()
    
    return {
        "available": available,
        "model": ai.model,
        "vllm_url": ai.vllm_url,
    }


@router.post("/api/logs/similar-count")
async def get_similar_logs_count(request: Dict[str, Any]) -> Dict[str, Any]:
    """Count similar log messages in the last N hours."""
    message = request.get("message", "")
    container_name = request.get("container_name", "")
    hours = request.get("hours", 24)
    
    if not message:
        return {"count": 0}
    
    count = await api.logstore.count_similar_logs(message, container_name, hours)
    
    return {"count": count}


@router.post("/api/logs/ai-analyze")
async def analyze_log_message(request: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze a log message using AI to determine if it's normal or needs attention."""
    from ..ai_service import get_ai_service
    
    message = request.get("message", "")
    level = request.get("level", "")
    container_name = request.get("container_name", "")
    
    if not message:
        return {"severity": "normal", "assessment": "No message to analyze"}
    
    ai = get_ai_service()
    result = await ai.analyze_log(message, level, container_name)
    
    return result


# ============== Manual Task Creation ==============

@router.post("/api/tasks/create")
async def create_agent_task(request: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze logs via the LLM agent and create a task in PulsarTeam."""
    task = request.get("task", "").strip()
    project = request.get("project", "").strip()

    if not task:
        raise HTTPException(status_code=400, detail="task is required")
    if not project:
        raise HTTPException(status_code=400, detail="project is required")

    if not api.llm_agent:
        raise HTTPException(status_code=500, detail="LLM agent not configured")

    try:
        result = await api.llm_agent.handle_log_analysis(task, project)
        return {"ok": True, "response": result[:2000] if result else ""}
    except Exception as e:
        logger.error("Task creation via LLM agent failed", project=project, error=str(e))
        raise HTTPException(status_code=502, detail=f"LLM agent error: {e}")
