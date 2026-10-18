"""Baseline security headers (CSP, HSTS, ...) attached to every HTTP response.

backend.api registers security_headers_middleware on the app; serve_frontend
builds its nonce-carrying policy with _csp_directives.
"""

import re
from typing import Optional

from fastapi import Request



# ============== Security Headers ==============

# Content-Security-Policy sources, kept in sync with frontend/index.html:
#   * scripts: files only.  Chart.js and xterm come from jsdelivr, pinned by
#     their integrity hashes.  There is no 'unsafe-inline': the markup carries
#     no inline script and no on* attribute, app.js wires every handler itself;
#   * styles: files only, style="" attributes included.  The single exception
#     is the <style> elements xterm's DOM renderer creates at run time, which
#     app.js stamps with the page's nonce (see serve_frontend);
#   * fonts: self-hosted under /static/fonts;
#   * images: the inline data: favicon.
# Anything not listed falls back to default-src 'self'.  The Google sign-in
# button needs more than this; see _GOOGLE_CSP_DIRECTIVES, which is merged in
# only on deployments that have it configured.
_CSP_DIRECTIVES = (
    "default-src 'self'",
    "script-src 'self' https://cdn.jsdelivr.net",
    "style-src 'self' https://cdn.jsdelivr.net",
    "font-src 'self'",
    "img-src 'self' data:",
    "frame-ancestors 'none'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self'",
)

# Google Identity Services, added only when Google sign-in is configured: the
# button is a cross-origin iframe that loads its own script and stylesheet and
# talks back to accounts.google.com. Each entry is the narrowest path Google
# documents, so this is not a blanket grant to the whole origin.
_GOOGLE_CSP_DIRECTIVES = (
    "script-src 'self' https://cdn.jsdelivr.net https://accounts.google.com/gsi/client",
    "style-src 'self' https://cdn.jsdelivr.net https://accounts.google.com/gsi/style",
    "frame-src https://accounts.google.com/gsi/",
)

_HSTS_VALUE = "max-age=31536000; includeSubDomains"

# host[:port] shapes only (IPv6 literals included) — the Host header is
# client-controlled and must never be echoed into a header unvalidated.
_HOST_HEADER_RE = re.compile(r"^[A-Za-z0-9._:\[\]-]{1,255}$")


def _connect_src(request: Request) -> str:
    """connect-src value: same-origin XHR/SSE plus the terminal WebSocket.

    'self' is meant to cover ws:/wss: on the page's own origin, but browsers
    disagreed on that for years; the origin's ws:// and wss:// forms are spelled
    out so the terminal keeps working, and the host is only echoed back when it
    has a plain host[:port] shape.
    """
    sources = ["'self'"]
    host = request.headers.get("host") or request.url.netloc
    if host and _HOST_HEADER_RE.match(host):
        sources += [f"ws://{host}", f"wss://{host}"]
    if api.google_verifier is not None and api.google_verifier.enabled:
        sources.append("https://accounts.google.com/gsi/")
    return "connect-src " + " ".join(sources)


def _csp_directives(request: Request, style_nonce: Optional[str] = None) -> str:
    """Build the policy for this response, widened only if Google sign-in is on.

    The Google entries are left out entirely on a deployment that does not use
    them: an unused allowance is still an allowance.  style_nonce is only given
    for the HTML page itself; no other response has markup to style.
    """
    directives = list(_CSP_DIRECTIVES)
    if api.google_verifier is not None and api.google_verifier.enabled:
        # Replace the base script-src/style-src with the widened forms and add
        # frame-src, which otherwise falls back to default-src 'self'.
        widened = {d.split(" ", 1)[0]: d for d in _GOOGLE_CSP_DIRECTIVES}
        directives = [widened.pop(d.split(" ", 1)[0], d) for d in directives]
        directives += list(widened.values())
    if style_nonce:
        directives = [f"{d} 'nonce-{style_nonce}'" if d.startswith("style-src ") else d
                      for d in directives]
    return "; ".join(directives + [_connect_src(request)])


def _request_is_https(request: Request) -> bool:
    """Whether the client's leg of the connection is TLS-protected."""
    if request.url.scheme in ("https", "wss"):
        return True
    forwarded = (request.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip().lower()
    return forwarded == "https"


async def security_headers_middleware(request: Request, call_next):
    """Attach the baseline security headers to every HTTP response.

    Registered last, so it wraps auth_middleware and the headers are also
    present on 401/403 responses.  Only response headers are touched: the SSE
    stream is not buffered, and WebSocket scopes never reach an HTTP
    middleware, so neither streaming path is affected.
    """
    response = await call_next(request)
    headers = response.headers
    headers.setdefault("Content-Security-Policy", _csp_directives(request))
    headers.setdefault("X-Content-Type-Options", "nosniff")
    headers.setdefault("X-Frame-Options", "DENY")
    headers.setdefault("Referrer-Policy", "no-referrer")
    if _request_is_https(request):
        headers.setdefault("Strict-Transport-Security", _HSTS_VALUE)
    return response


# Imported last: backend.api imports this module while it initializes, and
# ``api`` is only used at call time above.  This way the module can also be
# imported first, before backend.api.
from . import api  # noqa: E402
