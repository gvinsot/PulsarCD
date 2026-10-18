"""Public liveness probe (/api/health).

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from typing import Any, Dict

import structlog
from fastapi import APIRouter

from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ============== Health ==============

@router.get("/api/health")
async def health_check() -> Dict[str, Any]:
    """Public liveness probe: service status and OpenSearch reachability only.

    This is the single unauthenticated endpoint, so it must not disclose
    anything about the deployment.  Versions, index names and document counts
    live in the admin-only diagnostics (/api/admin/opensearch-status and
    /api/admin/opensearch-probe).
    """
    result: Dict[str, Any] = {"status": "healthy", "service": "pulsarcd"}

    if api.opensearch and api.opensearch._client:
        try:
            await api.opensearch._client.info()
            result["opensearch"] = "connected"
            # Coarse only ("ok" / "migrating" / "degraded"): details are in
            # /api/admin/opensearch-status.
            index_health = api.opensearch.index_health()
            if isinstance(index_health, str):
                result["indices"] = index_health
        except Exception as e:
            # The error type/message can leak host names or credentials: log it
            # server-side and expose only the coarse state.
            logger.warning("Health check: OpenSearch unreachable", error=str(e))
            result["opensearch"] = "error"
    else:
        result["opensearch"] = "not_configured"

    return result
