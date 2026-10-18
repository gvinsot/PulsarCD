"""Security endpoints (/api/security/*): access-log analytics and the edge blocklist.

GET /api/security/waf/traefik-config is what the edge Traefik polls; the auth
middleware authenticates it with the edge key (_EDGE_KEY_ROUTES).

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).

Tests patch _client_address on backend.api, so it is read through it as well.
"""

from fastapi import APIRouter, HTTPException, Query, Request

from shared.access_log import is_internal_ip, normalize_ip

from ..ip_blocklist import (
    covers as ip_blocklist_covers,
    validate_target as ip_blocklist_validate,
)
from .. import api

router = APIRouter()


# ============== Security ==============

@router.get("/api/security/overview")
async def get_security_overview(
    minutes: int = Query(default=60, ge=5, le=10080),
    include_internal: bool = Query(default=False),
):
    """Suspicious client IPs and hot endpoints, from the Traefik access logs."""
    overview = await api.opensearch.get_security_overview(
        minutes=minutes, include_internal=include_internal)
    # Which of those clients the edge is already refusing.  Resolved here
    # rather than in the browser because an address can be blocked through a
    # CIDR range that covers it, and the view has to say so instead of
    # offering to block it again.
    if api.ip_blocklist:
        for entry in overview.get("ips", []):
            entry["blocked"] = api.ip_blocklist.blocked_entry(entry.get("ip", ""))
    return overview


@router.get("/api/security/ips/{ip}")
async def get_security_ip_events(
    ip: str,
    minutes: int = Query(default=60, ge=5, le=10080),
):
    """Latest requests and WAF blocks of one client IP."""
    normalized = normalize_ip(ip)
    if normalized is None:
        raise HTTPException(status_code=400, detail="Invalid IP address")
    return {
        "ip": normalized,
        "internal": is_internal_ip(normalized),
        "blocked": api.ip_blocklist.blocked_entry(normalized) if api.ip_blocklist else None,
        "events": await api.opensearch.get_security_ip_events(normalized, minutes=minutes),
    }


# ---- Blocking a client at the edge -----------------------------------------
# A blocked address is refused by Traefik in front of Coraza: it reaches no
# WAF rule and no application.  The list lives here and the edge polls it (see
# backend/ip_blocklist.py); the two endpoints below are what the Security view
# calls, and the role policy in auth_middleware already reserves the mutating
# ones for administrators.


@router.get("/api/security/blocklist")
async def get_security_blocklist():
    """Addresses currently refused at the edge."""
    return {
        "entries": api.ip_blocklist.list_entries() if api.ip_blocklist else [],
        # False means blocking is inert: the edge has no key to poll with, so
        # an entry added here would never be enforced.  The view says so
        # rather than letting an operator believe an attacker is gone.
        "edge_configured": bool(api.settings and api.settings.auth.edge_key),
    }


@router.post("/api/security/blocklist")
async def add_security_block(request: Request):
    """Block a client address or CIDR range at the edge (admin only)."""
    body = await request.json()
    ip = body.get("ip", "")
    reason = body.get("reason", "")
    if not isinstance(ip, str) or not isinstance(reason, str):
        raise HTTPException(status_code=400, detail="Invalid request body")

    try:
        target = ip_blocklist_validate(ip)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Blocking the address the request comes from takes the unblock button
    # away with it: PulsarCD is published through the very proxy that would
    # start refusing this client.  The check is only meaningful behind a proxy
    # that sets X-Forwarded-For and is trusted to (PULSARCD_TRUST_PROXY_HEADERS);
    # otherwise the caller's address reads as Traefik's own, which is private
    # and can never be a block target anyway.
    caller = normalize_ip(api._client_address(request))
    if caller is not None and ip_blocklist_covers(target, caller):
        raise HTTPException(
            status_code=400,
            detail=(f"You are connecting from {caller}, which '{target}' covers. "
                    "Blocking it would lock you out of PulsarCD itself, and the "
                    "unblock button with it."),
        )

    try:
        return await api.ip_blocklist.add(
            target, reason=reason, blocked_by=getattr(request.state, "user", ""))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/api/security/blocklist")
async def remove_security_block(
    # A query parameter rather than a path one: a CIDR range carries a slash,
    # which no path segment can hold even percent-encoded.
    ip: str = Query(..., description="The blocked address or range to release"),
):
    """Stop refusing a client address at the edge (admin only)."""
    try:
        return await api.ip_blocklist.remove(ip)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/security/waf/traefik-config")
async def get_security_traefik_config():
    """Traefik dynamic configuration enforcing the blocklist.

    Polled by the edge's HTTP provider, which authenticates with
    PULSARCD_AUTH__EDGE_KEY (see _EDGE_KEY_ROUTES).  It always answers with a
    complete configuration -- an empty blocklist yields an empty one, which is
    how an unblock reaches the edge.
    """
    return api.ip_blocklist.traefik_config() if api.ip_blocklist else {}
