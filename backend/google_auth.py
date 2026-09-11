"""Google Sign-In (Google Identity Services) ID token verification.

The browser obtains an ID token from Google -- a JWT signed with an RS256 key
published at the JWKS endpoint below -- and posts it to ``POST /api/auth/google``.
This module turns that credential into a set of trusted claims, or raises
:class:`GoogleTokenError`.

Everything that must not be skipped happens here:

* the signature is verified against Google's *current* public keys, fetched over
  TLS and cached.  An unknown ``kid`` triggers a refresh, rate-limited so a
  stream of forged tokens cannot be turned into a request amplifier aimed at
  Google;
* ``alg`` is pinned to RS256.  The token header is read only to pick the key,
  never to pick the algorithm -- that is how ``alg: none`` and the
  "HS256 signed with the public key" trick get in;
* ``aud`` must equal the configured OAuth client id.  This is the claim that
  makes the token *ours*: without it, an ID token issued to any application in
  the world would be accepted here;
* ``iss`` must be Google, ``exp``/``iat`` must be current (small clock skew
  allowed), and the address must be present and carry ``email_verified`` -- an
  unverified address on a Google account proves nothing about who controls it.

The allowlist check deliberately does NOT live here.  This module only answers
"is this really Google, saying this is really that address"; whether that address
may sign in is ``backend/allowlist.py``.
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional

import aiohttp
import jwt
import structlog
from jwt.algorithms import RSAAlgorithm

logger = structlog.get_logger()

GOOGLE_JWKS_URL = "https://www.googleapis.com/oauth2/v3/certs"
# Google mints ID tokens with either spelling of the issuer.
GOOGLE_ISSUERS = frozenset({"accounts.google.com", "https://accounts.google.com"})

# How long a fetched key set is served before being refreshed.
_JWKS_TTL_SECONDS = 3600
# Floor between two fetches.  Without it, a flood of tokens bearing random
# `kid`s would make this service hammer Google on the attacker's behalf.
_JWKS_MIN_REFRESH_INTERVAL = 60
_JWKS_TIMEOUT_SECONDS = 10
# Tolerated clock difference between this host and Google on exp/iat.
_CLOCK_SKEW_SECONDS = 60
# A Google ID token is ~1 KB.  Anything far beyond that is not one, and parsing
# it is work an unauthenticated caller gets to ask for.
_MAX_CREDENTIAL_LENGTH = 8192


class GoogleTokenError(Exception):
    """The presented credential is not a usable Google ID token."""


class GoogleIdTokenVerifier:
    """Verifies Google ID tokens against Google's published signing keys.

    One instance is created at startup and reused: it owns the JWKS cache, so
    the common case costs no network round-trip at all.
    """

    def __init__(self, client_id: str, jwks_url: str = GOOGLE_JWKS_URL,
                 ttl_seconds: int = _JWKS_TTL_SECONDS):
        self._client_id = (client_id or "").strip()
        self._jwks_url = jwks_url
        self._ttl = ttl_seconds
        self._keys: Dict[str, Any] = {}
        self._fetched_at: Optional[float] = None
        self._last_attempt: Optional[float] = None
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        """Whether a client id is configured; without one, nothing is accepted."""
        return bool(self._client_id)

    @property
    def client_id(self) -> str:
        """The OAuth client id, which is public by design (the browser needs it)."""
        return self._client_id

    def _is_fresh(self, now: float) -> bool:
        return bool(self._keys) and self._fetched_at is not None \
            and now - self._fetched_at < self._ttl

    def _may_fetch(self, now: float) -> bool:
        return self._last_attempt is None \
            or now - self._last_attempt >= _JWKS_MIN_REFRESH_INTERVAL

    async def _fetch_jwks(self) -> None:
        """Replace the cached key set. Keeps the previous one on failure."""
        self._last_attempt = time.monotonic()
        try:
            timeout = aiohttp.ClientTimeout(total=_JWKS_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self._jwks_url) as response:
                    response.raise_for_status()
                    document = await response.json(content_type=None)
        except Exception as e:
            # Serving the previous keys through an outage is much better than
            # locking every user out; they stay valid well past our TTL.
            logger.error("Failed to fetch Google signing keys",
                         url=self._jwks_url, error=str(e))
            return

        keys: Dict[str, Any] = {}
        for entry in (document or {}).get("keys") or []:
            kid = entry.get("kid")
            if not kid or entry.get("kty") != "RSA":
                continue
            try:
                keys[kid] = RSAAlgorithm.from_jwk(json.dumps(entry))
            except Exception as e:
                logger.warning("Skipping unusable Google signing key",
                               kid=str(kid)[:64], error=str(e))
        if not keys:
            logger.error("Google signing key document contained no usable key",
                         url=self._jwks_url)
            return
        self._keys = keys
        self._fetched_at = time.monotonic()

    async def _key_for(self, kid: str):
        """Return the signing key for ``kid``, refreshing the cache if needed."""
        now = time.monotonic()
        if self._is_fresh(now) and kid in self._keys:
            return self._keys[kid]

        async with self._lock:
            # Another request may have refreshed while this one waited.
            now = time.monotonic()
            if self._is_fresh(now) and kid in self._keys:
                return self._keys[kid]
            # Either the cache aged out or Google rotated early; both are a
            # reason to refetch, and the floor below bounds how often.
            if self._may_fetch(now):
                await self._fetch_jwks()
            key = self._keys.get(kid)

        if key is None:
            if not self._keys:
                raise GoogleTokenError("Google signing keys are unavailable")
            raise GoogleTokenError("Google credential is signed with an unknown key")
        return key

    async def verify(self, credential: str) -> Dict[str, Any]:
        """Verify a Google ID token and return its claims.

        Raises GoogleTokenError on anything short of a fully valid token issued
        to this deployment's OAuth client for a verified address.
        """
        if not self.enabled:
            raise GoogleTokenError("Google sign-in is not configured")
        if not credential or not isinstance(credential, str):
            raise GoogleTokenError("Missing Google credential")
        if len(credential) > _MAX_CREDENTIAL_LENGTH:
            raise GoogleTokenError("Google credential is too large")

        try:
            header = jwt.get_unverified_header(credential)
        except jwt.PyJWTError as e:
            raise GoogleTokenError(f"Malformed Google credential: {e}") from e

        # Pinned, not read from the token: the header is attacker-controlled.
        if header.get("alg") != "RS256":
            raise GoogleTokenError("Unexpected Google credential algorithm")
        kid = header.get("kid")
        if not kid or not isinstance(kid, str):
            raise GoogleTokenError("Google credential carries no key id")

        key = await self._key_for(kid)
        try:
            claims = jwt.decode(
                credential,
                key,
                algorithms=["RS256"],
                audience=self._client_id,
                leeway=_CLOCK_SKEW_SECONDS,
                options={"require": ["exp", "iat", "aud", "iss", "sub"]},
            )
        except jwt.PyJWTError as e:
            raise GoogleTokenError(f"Google credential rejected: {e}") from e

        # PyJWT 2.8 takes a single issuer string; Google uses two spellings.
        if claims.get("iss") not in GOOGLE_ISSUERS:
            raise GoogleTokenError("Google credential was not issued by Google")

        email = claims.get("email")
        if not isinstance(email, str) or not email.strip():
            raise GoogleTokenError("Google credential carries no email address")
        # Google sends a JSON boolean; some libraries relay the string form.
        if claims.get("email_verified") not in (True, "true", "True"):
            raise GoogleTokenError("Google account email address is not verified")

        claims["email"] = email.strip().lower()
        return claims
