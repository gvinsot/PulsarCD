"""Admin endpoints for outbound notifications (see backend/notifier.py).

Mounted under /api/admin/, so auth_middleware already restricts every route
here to administrators.
"""

from typing import Any, Dict

from fastapi import APIRouter

from . import notifier

router = APIRouter(prefix="/api/admin/notifications", tags=["notifications"])


@router.get("")
async def notifications_status() -> Dict[str, Any]:
    """Channels, event switches and last delivery result. Never any secret."""
    return notifier.get_notifier().status()


@router.post("/test")
async def notifications_test() -> Dict[str, Any]:
    """Send a test notification to every configured channel and wait for it.

    Bypasses the event switches and the rate limit, so an operator can check a
    configuration at any time. Errors are reported per channel by type or HTTP
    status only: the full error text could contain the webhook URL (its secret).
    """
    current = notifier.get_notifier()
    if not current.enabled:
        return {"sent": False, "channels": {},
                "detail": "No notification channel is configured (PULSARCD_NOTIFY__*)."}
    results = await current.send_test()
    return {"sent": any(r["ok"] for r in results.values()), "channels": results}
