"""Usage analytics endpoint (/api/usage/overview).

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).

Tests patch _get_router_stacks on backend.api, so it is called through it as
well.
"""

import time
from typing import Dict, Optional

import structlog
from fastapi import APIRouter, Query

from ..pipeline_runner import _get_swarm_manager_host
from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ============== Usage ==============
# Which endpoints the platform actually serves, for every stack or for one.
# The access index names the Traefik router that served each request; the
# stack behind it comes from the Swarm service labels, which change only on a
# deploy, so the map is fetched from the manager and kept for a while.

_ROUTER_STACKS_TTL = 120  # seconds
_router_stacks_cache: Dict[str, str] = {}
_router_stacks_fetched_at: float = 0.0


async def _get_router_stacks() -> Dict[str, str]:
    """Traefik router name -> stack, cached; empty if the manager is down."""
    global _router_stacks_cache, _router_stacks_fetched_at

    if time.time() - _router_stacks_fetched_at < _ROUTER_STACKS_TTL:
        return _router_stacks_cache

    manager_host = _get_swarm_manager_host()
    client = api.collector.clients.get(manager_host) if (api.collector and manager_host) else None
    if client is None or not hasattr(client, "get_traefik_routers"):
        return _router_stacks_cache

    try:
        routers = await client.get_traefik_routers()
    except Exception as e:
        # Keep the previous map rather than un-attributing every row because
        # one poll of the Docker API failed.
        logger.warning("Failed to map Traefik routers to stacks", error=str(e))
        return _router_stacks_cache

    if routers:
        _router_stacks_cache = routers
        _router_stacks_fetched_at = time.time()
    return _router_stacks_cache


@router.get("/api/usage/overview")
async def get_usage_overview(
    minutes: int = Query(default=60, ge=5, le=10080),
    include_internal: bool = Query(default=False),
    stack: Optional[str] = Query(default=None),
):
    """Endpoint and per-stack traffic statistics, from the Traefik access logs."""
    return await api.logstore.get_usage_overview(
        minutes=minutes, include_internal=include_internal,
        stack=stack or None, router_stacks=await api._get_router_stacks())
