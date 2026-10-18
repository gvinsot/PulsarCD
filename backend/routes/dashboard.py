"""Dashboard endpoints (/api/dashboard/*): stats, time series and error lists.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from typing import Any, Dict, List

from fastapi import APIRouter, Query

from ..models import (
    ContainerStatus,
    DashboardStats, TimeSeriesPoint, TimeSeriesByHost
)
from .. import api

router = APIRouter()


# ============== Dashboard ==============

@router.get("/api/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get dashboard statistics."""
    stats = await api.opensearch.get_dashboard_stats()
    
    # Add container counts
    containers = await api.collector.get_all_containers()
    stats.total_containers = len(containers)
    stats.running_containers = len([c for c in containers if c.status == ContainerStatus.RUNNING])
    # Total hosts = configured hosts + discovered swarm nodes (each swarm node counts as a host)
    stats.total_hosts = len(api.collector.clients)
    stats.healthy_hosts = len(api.collector.clients)  # Simplistic health check
    
    return stats


@router.get("/api/dashboard/errors-timeseries", response_model=List[TimeSeriesPoint])
async def get_errors_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get error count time series."""
    return await api.opensearch.get_error_timeseries(hours=hours, interval=interval)


@router.get("/api/dashboard/http-4xx-timeseries", response_model=List[TimeSeriesPoint])
async def get_http_4xx_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get HTTP 4xx count time series."""
    return await api.opensearch.get_http_status_timeseries(400, 500, hours=hours, interval=interval)


@router.get("/api/dashboard/http-5xx-timeseries", response_model=List[TimeSeriesPoint])
async def get_http_5xx_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get HTTP 5xx count time series."""
    return await api.opensearch.get_http_status_timeseries(500, 600, hours=hours, interval=interval)


@router.get("/api/dashboard/http-requests-timeseries", response_model=List[TimeSeriesPoint])
async def get_http_requests_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get total HTTP requests count time series."""
    return await api.opensearch.get_http_requests_timeseries(hours=hours, interval=interval)


@router.get("/api/dashboard/cpu-timeseries", response_model=List[TimeSeriesPoint])
async def get_cpu_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get CPU usage time series."""
    return await api.opensearch.get_resource_timeseries("cpu_percent", hours=hours, interval=interval)


@router.get("/api/dashboard/gpu-timeseries", response_model=List[TimeSeriesPoint])
async def get_gpu_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get GPU usage time series."""
    return await api.opensearch.get_resource_timeseries("gpu_percent", hours=hours, interval=interval)


@router.get("/api/dashboard/memory-timeseries", response_model=List[TimeSeriesPoint])
async def get_memory_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get memory usage time series."""
    return await api.opensearch.get_resource_timeseries("memory_percent", hours=hours, interval=interval)


@router.get("/api/dashboard/cpu-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_cpu_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get CPU usage time series grouped by host."""
    return await api.opensearch.get_resource_timeseries_by_host("cpu_percent", hours=hours, interval=interval)


@router.get("/api/dashboard/gpu-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_gpu_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get GPU usage time series grouped by host."""
    return await api.opensearch.get_resource_timeseries_by_host("gpu_percent", hours=hours, interval=interval)


@router.get("/api/dashboard/memory-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_memory_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get memory usage time series grouped by host."""
    return await api.opensearch.get_resource_timeseries_by_host("memory_percent", hours=hours, interval=interval)


@router.get("/api/dashboard/vram-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_vram_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get VRAM usage time series grouped by host (percentage of total)."""
    # Use gpu_memory_used_mb / gpu_memory_total_mb * 100
    return await api.opensearch.get_vram_percent_timeseries_by_host(hours=hours, interval=interval)


@router.get("/api/dashboard/recurring-errors")
async def get_recurring_errors(limit: int = Query(default=5, ge=1, le=50)) -> List[Dict[str, Any]]:
    """Return the most recent notified recurring error patterns."""
    if not api.error_detector:
        return []
    return api.error_detector._notification_history[:limit]


@router.get("/api/dashboard/system-errors")
async def get_system_errors(limit: int = Query(default=20, ge=1, le=100)) -> List[Dict[str, Any]]:
    """Return recent system-level errors (LLM, MCP, infrastructure failures)."""
    if not api.llm_agent:
        return []
    return api.llm_agent.get_system_errors(limit=limit)
