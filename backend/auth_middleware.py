"""Authentication middleware and the access policy it enforces on /api/.

Session JWTs with role-based access, agent keys, the edge key and signed
webhooks; token revocation; the client address used for rate limiting.
backend.api registers auth_middleware on the app.

App state (settings, the service clients, the action registries, ...) lives in
backend.api and is read as ``api.<name>`` at call time, so the lifespan hook
and tests patching backend.api reach this code too.
"""

import hmac
import os
import re
from typing import Any, Dict, Optional

import jwt
import structlog
from fastapi import Request
from fastapi.responses import JSONResponse

from .auth import AUTH_SOURCE_GOOGLE, AUTH_SOURCE_LOCAL, decode_token
from . import run_history

logger = structlog.get_logger()


# ============== Auth Middleware ==============

# Paths that don't require authentication
_AUTH_EXEMPT_PREFIXES = (
    "/api/auth/login",
    "/static/",
    "/ai",  # MCP has its own auth middleware
)
# Only the exact liveness probe is public.  "/api/health" is deliberately NOT a
# prefix: sub-paths must stay authenticated.
# The trailing-slash form is listed too -- it used to be covered by the old
# prefix rule and reaches the same route through Starlette's redirect, so an
# external probe configured with it would otherwise start reporting the service
# down with a 401.
#
# The two sign-in routes are exact entries, not a "/api/auth/" prefix: every
# other route under that path (starting with /api/auth/me) must stay behind the
# token check.
_AUTH_EXEMPT_EXACT = (
    "/", "/api/health", "/api/health/",
    "/api/auth/google",   # exchanges a Google ID token for a session token
    "/api/auth/config",   # what the sign-in screen must render (no secrets)
)

# ---- Role-based access policy ----------------------------------------------
# Two roles exist: "admin" (full access) and "viewer" (read-only).  The policy is
# enforced centrally in auth_middleware so that a new route is protected by
# default instead of relying on every handler remembering to check the role.
#
# Rules, in order:
#   1. Any request under /api/admin/ requires the admin role.
#   2. Any mutating request (POST/PUT/DELETE/PATCH) under /api/ requires the
#      admin role, unless it is an explicitly allowlisted read-only POST.
#   3. Some GET routes leak secrets or infrastructure details and are restricted
#      to admins even though they do not mutate anything.
#
# To open a new route to viewers, add it to one of the two structures below --
# and only after checking that it neither mutates state nor triggers a
# side effect (background job, LLM agent, remote command, ...).
_MUTATING_METHODS = frozenset({"POST", "PUT", "DELETE", "PATCH"})

# POST endpoints that are purely read-only: they use POST only to carry a JSON
# query body and never mutate state.  Deliberately excluded: /api/tasks/create,
# which runs the LLM agent and creates a task in an external system.
_READ_ONLY_POST_PATHS = frozenset({
    "/api/logs/search",         # log search with a JSON query body
    "/api/logs/ai-search",      # natural-language log search
    "/api/logs/similar-count",  # counts similar log messages
    "/api/logs/ai-analyze",     # severity assessment of a single log line
})

# GET endpoints restricted to admins (secrets / infrastructure disclosure).
# A compiled regex keeps path-parameter routes ("/api/stacks/{repo}/env")
# unambiguous instead of relying on string concatenation.
_ADMIN_ONLY_GET_RE = re.compile(
    r"^/api/(?:"
    r"config(?:/test)?"                # infrastructure and connectivity details
    r"|stacks/[^/]+/env"               # stack .env file content (secrets)
    r"|containers/[^/]+/[^/]+/env"     # container environment variables (secrets)
    r"|stacks/test-permissions/.*"     # probes the deployment's GitHub token
    r"|github/check-access"            # ditto, across every starred repository
    r")$"
)

# ---- Agent key policy ------------------------------------------------------
# Only the routes the host agents actually call authenticate with an agent key.
# Everything else under /api/agent/ falls through to the JWT + role policy
# above.  In particular POST /api/agent/action queues arbitrary actions --
# including `exec` in any container of any host -- so it requires an admin JWT
# like every other mutating route; it is called from the API/UI side, never by
# an agent.
_AGENT_KEY_ROUTES = frozenset({
    ("GET", "/api/agent/actions"),
    ("POST", "/api/agent/result"),
    ("POST", "/api/agent/system-error"),
})

# ---- Edge key policy -------------------------------------------------------
# The one route the edge Traefik polls, to fetch the IP blocklist it enforces.
# Its caller is a reverse proxy, not a person and not an agent: it holds no
# session and cannot be given a role, so it authenticates with its own shared
# key (PULSARCD_AUTH__EDGE_KEY).  The route is read-only and serves nothing but
# the blocked addresses, but it is reachable from the internet like the rest of
# /api, so it is never left open.
_EDGE_KEY_ROUTES = frozenset({
    ("GET", "/api/security/waf/traefik-config"),
})

# ---- Signed webhook policy -------------------------------------------------
# GitHub delivers push events without any session: the route authenticates the
# request itself by checking its HMAC signature against
# PULSARCD_GITHUB__WEBHOOK_SECRET (see github_webhook_endpoint).  Only this
# exact method and path bypass the JWT check; any other method on it, and every
# other path under /api/webhooks/, still requires a session.
_WEBHOOK_ROUTES = frozenset({
    ("POST", "/api/webhooks/github"),
})

# ---- Where a JWT may travel in the query string ----------------------------
# A token in the URL leaks into reverse-proxy access logs, browser history and
# Referer headers, so `?token=` is only honoured on the two endpoints whose
# client cannot set an Authorization header: the SSE action-log stream
# (EventSource) and the terminal WebSocket (browser WebSocket API).  Every
# other /api/ route requires the Authorization header.
# The WebSocket route is listed for completeness only: WebSocket scopes never
# reach an HTTP middleware, that handler validates the token itself.
_QUERY_TOKEN_PATH_RE = re.compile(
    r"^/api/(?:stacks/actions/[^/]+/logs/stream|terminal/ws)$"
)


def _agent_key_matches(presented: str, expected: str) -> bool:
    """Compare an agent key in constant time."""
    if not presented or not expected:
        return False
    # Encode first: compare_digest rejects str operands containing non-ASCII.
    return hmac.compare_digest(presented.encode("utf-8"), expected.encode("utf-8"))


async def _extract_agent_id(request: Request) -> Optional[str]:
    """Read the agent_id an agent request acts on (query param and JSON body).

    Both sources are read on a non-GET request and must agree.  The middleware
    authenticates one of them while the handler may read the other, so allowing
    them to differ would let an agent authenticate with its own key through the
    query string and then act under a peer's identity through the body.

    Reading the body here is safe: Starlette's BaseHTTPMiddleware caches it and
    replays it to the downstream handler.
    """
    query_agent_id = request.query_params.get("agent_id") or None
    body_agent_id = None
    if request.method.upper() != "GET":
        try:
            body = await request.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            value = body.get("agent_id")
            body_agent_id = str(value) if value else None
    if query_agent_id and body_agent_id and query_agent_id != body_agent_id:
        logger.warning(
            "Agent request carries two conflicting agent_id values",
            query_agent_id=query_agent_id,
            body_agent_id=body_agent_id,
        )
        return None
    return query_agent_id or body_agent_id


async def _authenticate_agent(request: Request, auth_header: str):
    """Authenticate an agent request; return an error response or None if valid.

    When settings.auth.agent_keys is configured the presented key must be the
    one registered for the agent_id the request acts on, so a compromised agent
    cannot poll or answer for another agent.  When it is empty the historical
    single shared key is accepted (see the startup warning in lifespan).
    """
    presented = auth_header[7:] if auth_header.startswith("Bearer ") else ""
    agent_keys = getattr(api.settings.auth, "agent_keys", None) or {}

    if agent_keys:
        agent_id = await _extract_agent_id(request)
        expected = agent_keys.get(agent_id) if agent_id else None
        if not expected or not _agent_key_matches(presented, expected):
            logger.warning(
                "Agent key rejected",
                path=request.url.path,
                method=request.method,
                agent_id=agent_id,
            )
            return JSONResponse(status_code=401, content={"detail": "Invalid agent key"})
        request.state.agent_id = agent_id
        return None

    if not _agent_key_matches(presented, api.settings.auth.agent_key):
        logger.warning("Agent key rejected", path=request.url.path, method=request.method)
        return JSONResponse(status_code=401, content={"detail": "Invalid agent key"})
    return None


def _authenticate_edge(auth_header: str):
    """Authenticate the edge proxy; return an error response or None if valid."""
    expected = getattr(getattr(api.settings, "auth", None), "edge_key", "") or ""
    if not expected:
        # Nothing to compare against: answering 401 would look like a wrong key
        # on the edge side, when the deployment simply never set one.
        logger.warning("Edge blocklist polled but PULSARCD_AUTH__EDGE_KEY is not set")
        return JSONResponse(
            status_code=503,
            content={"detail": "PULSARCD_AUTH__EDGE_KEY is not configured"},
        )
    presented = auth_header[7:] if auth_header.startswith("Bearer ") else ""
    if not _agent_key_matches(presented, expected):
        logger.warning("Edge key rejected")
        return JSONResponse(status_code=401, content={"detail": "Invalid edge key"})
    return None


# ---- Client address --------------------------------------------------------
# X-Forwarded-For / X-Real-IP are only trusted when PULSARCD_TRUST_PROXY_HEADERS
# opts in; see the login rate limiting in routes/auth.py for why.
_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})


def _trust_proxy_headers() -> bool:
    """Whether X-Forwarded-* headers are set by a trusted reverse proxy.

    Off by default: the headers are forgeable by any client when the app is
    reachable directly.
    """
    return os.environ.get("PULSARCD_TRUST_PROXY_HEADERS", "").strip().lower() in _TRUTHY_VALUES


def _client_address(request: Request) -> str:
    """Client address used to rate-limit logins."""
    if _trust_proxy_headers():
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded:
            # Left-most entry is the original client when the chain is trusted.
            candidate = forwarded.split(",")[0].strip()
            if candidate:
                return candidate[:64]
        real_ip = (request.headers.get("X-Real-IP") or "").strip()
        if real_ip:
            return real_ip[:64]
    return request.client.host if request.client else "unknown"


def _user_token_epoch(user: Any) -> int:
    """Read an account's revocation epoch, tolerating records without one."""
    try:
        return int(getattr(user, "token_epoch", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _identity_store(payload: Dict[str, Any]):
    """Return the store that owns a token's subject, or None if unavailable.

    The two namespaces are independent -- a Google address and a local username
    are looked up in different files -- so the ``auth`` claim decides which one
    answers.  A token minted before the claim existed carries none and is read
    as local, which is what it was.
    """
    if payload.get("auth") == AUTH_SOURCE_GOOGLE:
        return api.email_allowlist
    return api.user_manager


def _token_is_revoked(payload: Dict[str, Any]) -> bool:
    """Whether a decoded JWT was invalidated after it was issued.

    A role change, a removal from the allowlist and a break-glass password
    change all bump the identity's epoch (see allowlist / user_manager), so a
    token minted before that carries a lower value and is refused.  Tokens
    issued before the claim existed read as epoch 0 and stay valid until the
    identity is first revoked, which keeps a file written by an older version
    working as-is.
    """
    store = _identity_store(payload)
    if store is None:
        return False
    username = payload.get("sub") or ""
    try:
        current = store.token_epoch_for(username)
    except Exception as e:  # never lock everyone out on a storage hiccup
        logger.error("Token epoch lookup failed", user=username, error=str(e))
        return False
    if current is None:
        return True  # account deleted while its token was still within expiry
    if not isinstance(current, int):
        return False
    try:
        presented = int(payload.get("epoch", 0) or 0)
    except (TypeError, ValueError):
        return True
    return presented < current


def _route_path(scope: Dict[str, Any]) -> str:
    """Return the path the router will match, mirroring Starlette's own rule.

    Never derive the policy path from ``request.url``: that URL is rebuilt from
    the client-controlled Host header, so a request carrying ``Host: x?`` yields
    an empty ``url.path`` while the router still matches ``scope["path"]`` --
    every check below would be skipped on a fully routed request.  ``root_path``
    is stripped for the same reason in reverse: behind a proxy that mounts the
    app under a sub-path, the router matches the remainder, so the policy must
    look at the remainder too.
    """
    path = scope.get("path", "")
    root_path = scope.get("root_path", "")
    if root_path and path.startswith(root_path):
        return path[len(root_path):]
    return path


async def auth_middleware(request: Request, call_next):
    """Verify authentication on all /api/ endpoints."""
    path = _route_path(request.scope)

    # Skip auth for exempt paths
    if path in _AUTH_EXEMPT_EXACT or any(path.startswith(p) for p in _AUTH_EXEMPT_PREFIXES):
        return await call_next(request)

    # Only enforce auth on /api/ paths
    if not path.startswith("/api/"):
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")

    # Agent polling endpoints: validate the agent key.  Other /api/agent/ routes
    # (POST /api/agent/action) deliberately fall through to the JWT policy.
    if (request.method.upper(), path) in _AGENT_KEY_ROUTES:
        denied = await _authenticate_agent(request, auth_header)
        if denied is not None:
            return denied
        return await call_next(request)

    # Signed webhooks: the handler verifies the HMAC signature itself.
    if (request.method.upper(), path) in _WEBHOOK_ROUTES:
        return await call_next(request)

    # The edge proxy polling the blocklist it enforces: its own shared key.
    if (request.method.upper(), path) in _EDGE_KEY_ROUTES:
        denied = _authenticate_edge(auth_header)
        if denied is not None:
            return denied
        return await call_next(request)

    # All other /api/ endpoints: validate JWT Bearer token
    token = None
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
    elif request.query_params.get("token") and _QUERY_TOKEN_PATH_RE.match(path):
        # Only the two streaming clients that cannot set a header.
        token = request.query_params["token"]

    if not token:
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
    try:
        payload = decode_token(token, api.settings.auth.jwt_secret)
    except jwt.PyJWTError as e:
        logger.warning("JWT authentication failed", error=str(e), path=path)
        return JSONResponse(status_code=401, content={"detail": "Invalid or expired token"})

    if _token_is_revoked(payload):
        logger.warning("Revoked token rejected",
                       user=payload.get("sub", ""), path=path)
        return JSONResponse(status_code=401, content={"detail": "Token has been revoked"})

    request.state.user = payload.get("sub", "")
    request.state.role = payload.get("role", "viewer")
    run_history.set_actor(request.state.user)
    request.state.auth_source = payload.get("auth", AUTH_SOURCE_LOCAL)

    # Admin-only paths
    if path.startswith("/api/admin/") and getattr(request.state, "role", "") != "admin":
        return JSONResponse(status_code=403, content={"detail": "Admin access required"})

    # Role-based access control: viewers are read-only.  See the policy block
    # next to _AUTH_EXEMPT_PREFIXES for the rules and how to extend them.
    if getattr(request.state, "role", "") != "admin":
        method = request.method.upper()
        denied = False
        if method in _MUTATING_METHODS:
            denied = not (method == "POST" and path in _READ_ONLY_POST_PATHS)
        elif method == "GET" and _ADMIN_ONLY_GET_RE.match(path):
            denied = True
        if denied:
            logger.warning(
                "Access denied: admin role required",
                user=getattr(request.state, "user", ""),
                role=getattr(request.state, "role", ""),
                method=method,
                path=path,
            )
            return JSONResponse(status_code=403, content={"detail": "Admin access required"})

    return await call_next(request)


# Imported last: backend.api imports this module while it initializes, and
# ``api`` is only used at call time above.  This way the module can also be
# imported first, before backend.api.
from . import api  # noqa: E402
