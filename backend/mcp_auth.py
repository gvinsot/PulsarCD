"""Authentication middleware for the MCP server.

Validates Bearer tokens on every MCP request before it reaches the tool handlers.
Accepts either a valid JWT (same tokens used by the web UI) or a dedicated MCP API key.
"""

import structlog
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

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

        if not auth_header.startswith("Bearer "):
            response = JSONResponse(
                status_code=401,
                content={"error": "MCP authentication required. Provide a Bearer token."},
            )
            await response(scope, receive, send)
            return

        token = auth_header[7:]

        # Lazy import to get current settings (from api.py, initialized in lifespan)
        from . import api as _api
        settings = _api.settings

        # Check 1: dedicated MCP API key
        if settings.mcp.api_key and token == settings.mcp.api_key:
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

        response = JSONResponse(
            status_code=401,
            content={"error": "Invalid or expired token"},
        )
        await response(scope, receive, send)
