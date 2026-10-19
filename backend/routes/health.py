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
    """Public liveness probe: service status and VictoriaLogs reachability only.

    This is the single unauthenticated endpoint, so it must not disclose
    anything about the deployment.  Versions and record counts live in the
    admin-only diagnostics (/api/admin/victorialogs-status and
    /api/admin/victorialogs-probe).
    """
    result: Dict[str, Any] = {"status": "healthy", "service": "pulsarcd"}
    if api.logstore:
        # Never the error itself: it can carry host names or credentials.
        result["victorialogs"] = "connected" if await api.logstore.health() else "error"
    else:
        result["victorialogs"] = "not_configured"
    return result
