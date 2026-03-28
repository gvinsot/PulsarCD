"""Authentication middleware for the MCP server.

Validates Bearer tokens on every MCP request before it reaches the tool handlers.
Accepts either a valid JWT (same tokens used by the web UI) or a dedicated MCP API key.

Token can be provided via:
- Authorization: Bearer <token> header (preferred)
- ?token=<token> query parameter (fallback for SSE clients that cannot set headers)
"""

import structlog
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from urllib.parse import parse_qs

logger = structlog.get_logger()


class MCPAuthMiddleware:
    """ASGI middleware that validates Bearer tokens for MCP requests."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Extract Authorization header from raw ASGI headers
        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")

        token = None
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        else:
            # Fallback: token via query parameter (SSE clients cannot set headers)
            qs = scope.get("query_string", b"").decode("utf-8", errors="ignore")
            params = parse_qs(qs)
            token_list = params.get("token")
            if token_list:
                token = token_list[0]

        if not token:
            response = JSONResponse(
                status_code=401,
                content={"error": "MCP authentication required. Provide a Bearer token or ?token= query parameter."},
            )
            await response(scope, receive, send)
            return

        # Lazy import to get current settings (from api.py, initialized in lifespan)
        from . import api as _api
        settings = _api.settings

        # Check 1: dedicated MCP API key
        if settings.mcp.api_key and token.strip() == settings.mcp.api_key.strip():
            await self.app(scope, receive, send)
            return

        # Check 2: valid JWT token
        try:
            from .auth import decode_token
            decode_token(token, settings.auth.jwt_secret)
            await self.app(scope, receive, send)
            return
        except Exception:
            pass

        expected = settings.mcp.api_key or ""
        logger.warning(
            "MCP auth failed",
            received=repr(token),
            expected=repr(expected),
        )
        response = JSONResponse(
            status_code=401,
            content={"error": "Invalid or expired token"},
        )
        await response(scope, receive, send)
