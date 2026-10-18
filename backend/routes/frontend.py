"""Serves the single-page frontend (GET /).

backend.api mounts /static itself: a router cannot carry a mount.
"""

import hashlib
import os
import re
import secrets
from typing import Dict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from .. import api
from ..security_headers import _csp_directives

router = APIRouter()


_FRONTEND_DIR = "frontend"
_INDEX_PATH = os.path.join(_FRONTEND_DIR, "index.html")
_STATIC_DIR = os.path.abspath(os.path.join(_FRONTEND_DIR, "static"))

# href="/static/..." / src="/static/..." references, with or without a query
_ASSET_REF_RE = re.compile(r'(?P<attr>href|src)="(?P<path>/static/[^"?]+)(?:\?[^"]*)?"')

# url path -> (mtime_ns, size, version): re-hash an asset only when it changes
_asset_versions: Dict[str, tuple] = {}


def _asset_version(url_path: str) -> str:
    """Return the content hash used as the cache-busting version of an asset."""
    file_path = os.path.abspath(
        os.path.join(_STATIC_DIR, *url_path[len("/static/"):].split("/"))
    )
    # Paths come from our own index.html, but keep them inside the static dir
    try:
        if os.path.commonpath([_STATIC_DIR, file_path]) != _STATIC_DIR:
            return "0"
    except ValueError:  # different drives on Windows
        return "0"

    try:
        stat = os.stat(file_path)
    except OSError:
        return "0"

    cached = _asset_versions.get(url_path)
    if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return cached[2]

    try:
        with open(file_path, "rb") as f:
            version = hashlib.sha256(f.read()).hexdigest()[:12]
    except OSError:
        return "0"

    _asset_versions[url_path] = (stat.st_mtime_ns, stat.st_size, version)
    return version


# HTML comments, with the indentation before them and the line break after:
# they are notes for whoever edits index.html, not for whoever loads the page.
_HTML_COMMENT_RE = re.compile(r"[ \t]*<!--.*?-->[ \t]*(?:\r?\n)?", re.DOTALL)

# Where index.html asks for the per-response CSP nonce.
_CSP_NONCE_PLACEHOLDER = "__CSP_NONCE__"


# Markup that only exists while an optional feature is enabled:
#   <!-- feature:NAME[: note] --> ... <!-- /feature:NAME -->
# Disabled features are cut out of the page entirely (panel, script and
# stylesheet), so the UI never shows a section whose API answers 404.
_FEATURE_BLOCK_RE = re.compile(
    r"[ \t]*<!-- feature:(?P<name>[a-z_]+)(?::[^>]*)? -->.*?<!-- /feature:(?P=name) -->[ \t]*(?:\r?\n)?",
    re.DOTALL,
)


def _enabled_features() -> Dict[str, bool]:
    """Optional frontend features and whether the settings enable them."""
    previews = getattr(api.settings, "previews", None)
    return {"previews": bool(getattr(previews, "enabled", False))}


def _strip_disabled_features(html: str, features: Dict[str, bool]) -> str:
    """Drop the blocks of features that are not enabled (unknown ones too)."""
    return _FEATURE_BLOCK_RE.sub(
        lambda m: m.group(0) if features.get(m.group("name")) else "", html,
    )


def _render_index(style_nonce: str) -> str:
    """Read index.html, drop disabled features, strip its comments, stamp
    /static references and the nonce."""
    with open(_INDEX_PATH, encoding="utf-8") as f:
        html = f.read()
    html = _strip_disabled_features(html, _enabled_features())
    html = _HTML_COMMENT_RE.sub("", html)
    html = _ASSET_REF_RE.sub(
        lambda m: f'{m.group("attr")}="{m.group("path")}?v={_asset_version(m.group("path"))}"',
        html,
    )
    return html.replace(_CSP_NONCE_PLACEHOLDER, style_nonce)


@router.get("/")
async def serve_frontend(request: Request):
    """Serve the frontend.

    Each /static reference is stamped with a hash of that file's contents,
    replacing the version query that used to be maintained by hand: editing an
    asset without bumping it left browsers running a cached copy against an
    updated backend. The document itself must be revalidated for the new
    hashes to be picked up, hence no-cache.

    Every response draws a fresh style nonce and sends a policy naming it; the
    security headers middleware keeps that policy rather than its generic one.
    A nonce is only worth something while it is unpredictable, which no-cache
    also serves: this response has no validator, so each load is a new fetch
    and a new nonce.
    """
    nonce = secrets.token_urlsafe(18)
    return HTMLResponse(_render_index(nonce), headers={
        "Cache-Control": "no-cache",
        "Content-Security-Policy": _csp_directives(request, style_nonce=nonce),
    })
