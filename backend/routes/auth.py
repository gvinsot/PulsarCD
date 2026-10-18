"""Sign-in endpoints (/api/auth/*) and the login rate limiting they apply.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).

Tests patch _client_address and _LOGIN_ATTEMPTS_MAX_KEYS on backend.api, so
those are read through it as well.
"""

import time
from typing import Dict, List

import structlog
from fastapi import APIRouter, HTTPException, Request

from ..auth import AUTH_SOURCE_GOOGLE, AUTH_SOURCE_LOCAL, create_token
from ..google_auth import GoogleTokenError
from ..auth_middleware import _user_token_epoch
from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ---- Login rate limiting (in-memory) ---------------------------------------
# Failed logins are counted in two independent sliding-window buckets:
#   * per (account, client address), to stop credential stuffing against a
#     single user without letting a third party lock that user out;
#   * per client address, to keep one source from sweeping many accounts.
# The per-address ceiling is deliberately much higher than the per-account one:
# behind a reverse proxy every request carries the proxy's address, so a low
# per-address limit would let five bad passwords lock out the whole
# deployment.  The address is only read from X-Forwarded-For when
# PULSARCD_TRUST_PROXY_HEADERS opts in — the header is client-controlled
# otherwise, and trusting it blindly lets an attacker both dodge the limit and
# forge attempts against someone else's address.
_login_attempts: Dict[str, List[float]] = {}
_LOGIN_MAX_ATTEMPTS = 5                 # per account, per window
_LOGIN_MAX_ATTEMPTS_PER_CLIENT = 30     # per client address, per window
_LOGIN_WINDOW_SECONDS = 60
# Hard ceiling on the number of tracked buckets so that varying the username
# (or the forwarded address) cannot grow the map without bound.
_LOGIN_ATTEMPTS_MAX_KEYS = 4096


def _account_bucket(username: str, client_ip: str) -> str:
    """Bucket key for an (account, address) pair.

    Keying on the account ALONE would hand any unauthenticated client a targeted
    lockout: five bad passwords for "admin" every 60 seconds, from anywhere, and
    the real administrator is refused with a 429 before their correct password is
    even checked (the limit is evaluated ahead of authenticate(), and the counter
    is only cleared by a successful login). Pairing it with the address keeps the
    credential-stuffing protection for the attacker's own source while leaving
    every other client unaffected.  The username is truncated: it is
    attacker-chosen and must not be able to grow the key.
    """
    return "user:" + (username or "")[:128] + "|ip:" + client_ip


def _purge_login_attempts(now: float) -> None:
    """Drop buckets whose attempts all fell out of the sliding window."""
    expired = [
        key for key, attempts in _login_attempts.items()
        if not attempts or now - attempts[-1] >= _LOGIN_WINDOW_SECONDS
    ]
    for key in expired:
        _login_attempts.pop(key, None)


def _recent_attempts(key: str, now: float) -> List[float]:
    """Return the attempts of a bucket that are still inside the window."""
    attempts = [t for t in _login_attempts.get(key, []) if now - t < _LOGIN_WINDOW_SECONDS]
    if attempts:
        _login_attempts[key] = attempts
    else:
        _login_attempts.pop(key, None)
    return attempts


def _is_rate_limited(client_ip: str, username: str = "") -> bool:
    """Check whether a login attempt must be refused before verifying credentials."""
    now = time.time()
    _purge_login_attempts(now)
    if username and len(_recent_attempts(_account_bucket(username, client_ip), now)) >= _LOGIN_MAX_ATTEMPTS:
        return True
    return len(_recent_attempts("ip:" + client_ip, now)) >= _LOGIN_MAX_ATTEMPTS_PER_CLIENT


def _record_login_attempt(client_ip: str, username: str = "") -> None:
    """Record a failed login attempt in the client-address and account buckets."""
    now = time.time()
    _purge_login_attempts(now)
    keys = ["ip:" + client_ip]
    if username:
        keys.append(_account_bucket(username, client_ip))
    for key in keys:
        bucket = _login_attempts.get(key)
        if bucket is None:
            if len(_login_attempts) >= api._LOGIN_ATTEMPTS_MAX_KEYS:
                # Evict the least recently used bucket rather than failing to
                # record — never grow past the ceiling, never fail open.
                oldest = min(_login_attempts,
                             key=lambda k: _login_attempts[k][-1] if _login_attempts[k] else 0.0)
                _login_attempts.pop(oldest, None)
            bucket = _login_attempts.setdefault(key, [])
        bucket.append(now)


def _clear_login_attempts(username: str, client_ip: str) -> None:
    """Forget this account+address's failed attempts after a successful login."""
    _login_attempts.pop(_account_bucket(username, client_ip), None)


# ============== Auth Endpoints ==============

@router.get("/api/auth/config")
async def auth_config():
    """Describe the sign-in methods to the login screen.

    Public on purpose: it is read before anyone is authenticated, and it carries
    nothing secret.  The OAuth client id is a public identifier -- the browser
    has to hand it to Google -- and the two booleans only say which buttons to
    draw, which is already obvious from trying them.
    """
    return {
        "google_enabled": bool(api.google_verifier and api.google_verifier.enabled),
        "google_client_id": api.google_verifier.client_id if api.google_verifier else "",
        "password_login_enabled": bool(api.user_manager and api.user_manager.enabled),
    }


@router.post("/api/auth/google")
async def auth_google(request: Request):
    """Exchange a Google ID token for a PulsarCD session token.

    Three separate things have to hold, in this order: Google really signed this
    credential for this deployment (google_auth), the address it carries is
    allowed in (allowlist), and only then is a session token minted.

    A bad credential is a 401 and an unlisted address a 403, which names the
    address.  That distinction leaks nothing: reaching the 403 at all requires a
    Google-signed token for that address, so a caller only ever learns the
    status of an account they already control.  Why Google itself said no is NOT
    relayed -- that detail belongs in the server log, not in a reply to whoever
    sent the token.
    """
    client_ip = api._client_address(request)

    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid request body")
    credential = body.get("credential", "")
    if not isinstance(credential, str):
        raise HTTPException(status_code=400, detail="Invalid credential format")

    if not api.google_verifier or not api.google_verifier.enabled:
        raise HTTPException(status_code=403, detail="Google sign-in is not configured")

    # The address is unknown until the credential is verified, so this first
    # pass only applies the per-client ceiling; the per-account bucket is
    # checked below, once there is an account to speak of.
    if _is_rate_limited(client_ip):
        logger.warning("Google sign-in rate limited", client_ip=client_ip)
        raise HTTPException(status_code=429, detail="Too many sign-in attempts. Try again later.")

    try:
        claims = await api.google_verifier.verify(credential)
    except GoogleTokenError as e:
        _record_login_attempt(client_ip)
        logger.warning("Google credential rejected", client_ip=client_ip, error=str(e))
        raise HTTPException(status_code=401, detail="Google sign-in failed")

    email = claims["email"]
    if _is_rate_limited(client_ip, email):
        logger.warning("Google sign-in rate limited", email=email[:128], client_ip=client_ip)
        raise HTTPException(status_code=429, detail="Too many sign-in attempts. Try again later.")

    role = api.email_allowlist.role_for(email) if api.email_allowlist else None
    if role is None:
        _record_login_attempt(client_ip, email)
        # Logged in full: an address Google vouched for that is not allowed in
        # is exactly what an operator needs to see, either to add a colleague or
        # to notice someone probing.
        logger.warning("Google sign-in refused: address not allowed",
                       email=email[:128], client_ip=client_ip)
        raise HTTPException(
            status_code=403,
            detail=f"{email} is not authorised to access this deployment.")

    _clear_login_attempts(email, client_ip)
    token = create_token(
        email,
        api.settings.auth.jwt_secret,
        api.settings.auth.jwt_expiry_hours,
        role=role,
        token_epoch=api.email_allowlist.token_epoch_for(email) or 0,
        auth_source=AUTH_SOURCE_GOOGLE,
    )
    logger.info("Google sign-in", email=email, role=role, client_ip=client_ip)
    return {"token": token, "email": email, "role": role,
            "name": claims.get("name") or email}


@router.post("/api/auth/login")
async def auth_login(request: Request):
    """Authenticate the break-glass administrator and return a JWT token.

    This is not the normal way in -- everyone signs in with Google.  It exists
    so a Google outage or a misconfigured OAuth client does not lock the
    operator out of their own cluster, and it only works when
    PULSARCD_AUTH__PASSWORD provisioned an account (see user_manager).
    """
    client_ip = api._client_address(request)

    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid request body")
    username = body.get("username", "")
    password = body.get("password", "")
    if not isinstance(username, str) or not isinstance(password, str):
        raise HTTPException(status_code=400, detail="Invalid credentials format")

    if not api.user_manager or not api.user_manager.enabled:
        logger.warning("Password login attempted while disabled",
                       username=username[:128], client_ip=client_ip)
        raise HTTPException(
            status_code=403,
            detail="Password sign-in is disabled on this deployment. Use Google.")

    # Counted per account and per client address: the username is needed first,
    # so the body is parsed before the limit is applied.
    if _is_rate_limited(client_ip, username):
        logger.warning("Login rate limited", username=username[:128], client_ip=client_ip)
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")

    user = api.user_manager.authenticate(username, password)
    if user:
        _clear_login_attempts(username, client_ip)
        token = create_token(
            username,
            api.settings.auth.jwt_secret,
            api.settings.auth.jwt_expiry_hours,
            role=user.role,
            token_epoch=_user_token_epoch(user),
            auth_source=AUTH_SOURCE_LOCAL,
        )
        return {"token": token}

    _record_login_attempt(client_ip, username)
    logger.warning("Failed login attempt", username=username[:128], client_ip=client_ip)
    raise HTTPException(status_code=401, detail="Invalid credentials")


@router.get("/api/auth/me")
async def auth_me(request: Request):
    """Return the current authenticated user."""
    return {
        "username": getattr(request.state, "user", None),
        "role": getattr(request.state, "role", "viewer"),
        "auth": getattr(request.state, "auth_source", AUTH_SOURCE_LOCAL),
    }
