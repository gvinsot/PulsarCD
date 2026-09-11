"""Allowlist of Google addresses permitted to sign in to PulsarCD.

A verified Google identity only proves *who* someone is; this file decides
*whether* they get in, and with which role.  An address that is not listed is
refused even with a perfectly valid Google credential -- the deployment is
normally published on the public internet, where "has a Google account" is not
an access rule.

Two sources feed the list, and they do not have equal standing:

* the environment (``PULSARCD_AUTH__GOOGLE_ADMINS`` /
  ``PULSARCD_AUTH__GOOGLE_VIEWERS``) is reapplied on every boot and wins.  Its
  entries are reported as ``managed`` and cannot be edited or removed from the
  UI, because the next restart would bring them straight back -- an operator
  would think an account was revoked when it was not;
* the UI (Settings > Users) adds and removes everyone else, persisted to
  ``allowed_emails.json`` in the data directory, so onboarding a colleague does
  not need a redeploy.

Every entry carries a revocation epoch (see ``next_token_epoch``).  Changing a
role bumps it, which invalidates the sessions already open for that address;
removing the entry makes the lookup fail outright, which does the same.
"""

import asyncio
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import structlog
from pydantic import BaseModel

from .auth import next_token_epoch

logger = structlog.get_logger()

ROLES = ("admin", "viewer")

# Deliberately permissive: this is a sanity check on operator input, not an
# attempt to re-derive RFC 5322.  The address that actually matters is the one
# Google vouched for, and it is compared after the same normalisation.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s.]+(?:\.[^@\s.]+)+$")
_MAX_EMAIL_LENGTH = 254


def normalize_email(email: str) -> str:
    """Normalise an address for comparison and storage.

    Google reports addresses in a stable case, but an operator typing one into
    the UI will not: without this, ``Someone@Example.com`` in the allowlist would
    silently never match the ``someone@example.com`` Google sends.
    """
    return (email or "").strip().lower()


def parse_email_list(raw: str) -> List[str]:
    """Parse an env var holding addresses as JSON or a separated list.

    Accepts ``["a@b.c", "d@e.f"]`` as well as the comma/semicolon/whitespace
    separated forms an operator is far more likely to type into a .env file.
    """
    if not raw:
        return []
    value = raw.strip()
    if value.startswith("["):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [normalize_email(str(item)) for item in parsed if str(item).strip()]
            logger.warning("Google address list is not a JSON array; ignored")
            return []
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse Google address list as JSON", error=str(e))
            return []
    return [normalize_email(part) for part in re.split(r"[,;\s]+", value) if part.strip()]


class AllowedEmail(BaseModel):
    """One address permitted to sign in with Google."""
    email: str
    role: str = "viewer"
    # Revocation epoch carried by every JWT issued for this address.  A token
    # whose epoch is older than this value is refused, so bumping the field
    # invalidates every session already open.  Absent from a file written by an
    # older version: it then reads as 0.
    token_epoch: int = 0


class EmailAllowlist:
    """File-backed allowlist, bootstrapped from the environment on every load."""

    def __init__(self, path: str, admins: Iterable[str] = (),
                 viewers: Iterable[str] = ()):
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._entries: List[AllowedEmail] = []
        # Managed addresses are keyed by role so a boot can put them back the
        # way the environment says they should be.
        self._managed: Dict[str, str] = {}
        for email in viewers:
            normalized = normalize_email(email)
            if normalized:
                self._managed[normalized] = "viewer"
        # Admin wins if an address is listed in both, which is the safe reading
        # of a contradictory configuration only in the sense that it is the
        # explicit one -- it is logged below so it does not pass unnoticed.
        for email in admins:
            normalized = normalize_email(email)
            if normalized:
                if self._managed.get(normalized) == "viewer":
                    logger.warning("Address listed as both admin and viewer; "
                                   "admin wins", email=normalized)
                self._managed[normalized] = "admin"
        self._load()

    # ---- loading and persistence -------------------------------------------

    def _load(self) -> None:
        """Read the stored list, then reapply the environment on top of it."""
        stored: List[AllowedEmail] = []
        if self._path.exists():
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                for item in raw:
                    entry = AllowedEmail(**item)
                    entry.email = normalize_email(entry.email)
                    if entry.email and entry.role in ROLES:
                        stored.append(entry)
            except Exception as e:
                logger.error("Failed to parse the allowed addresses file, "
                             "starting from the environment only",
                             path=str(self._path), error=str(e))
                stored = []

        # Drop duplicates a hand-edited file may contain; first wins.
        self._entries = []
        seen = set()
        for entry in stored:
            if entry.email not in seen:
                seen.add(entry.email)
                self._entries.append(entry)

        changed = len(self._entries) != len(stored)
        for email, role in self._managed.items():
            existing = self._find(email)
            if existing is None:
                self._entries.append(AllowedEmail(
                    email=email, role=role, token_epoch=next_token_epoch()))
                changed = True
            elif existing.role != role:
                # The environment is authoritative; the sessions the old role
                # opened must not outlive the change.
                existing.role = role
                existing.token_epoch = next_token_epoch(existing.token_epoch)
                changed = True

        if changed:
            self._save_sync()
        logger.info("Google allowlist loaded", path=str(self._path),
                    count=len(self._entries), managed=len(self._managed))

    def _save_sync(self) -> None:
        """Write the list to disk (synchronous)."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            data = [e.model_dump() for e in self._entries]
            self._path.write_text(json.dumps(data, indent=2, ensure_ascii=False),
                                  encoding="utf-8")
        except Exception as e:
            logger.error("Failed to save the allowed addresses file",
                         path=str(self._path), error=str(e))
            raise

    async def _save(self) -> None:
        """Write the list to disk (async-safe)."""
        self._save_sync()

    def _find(self, email: str) -> Optional[AllowedEmail]:
        normalized = normalize_email(email)
        for entry in self._entries:
            if entry.email == normalized:
                return entry
        return None

    # ---- lookups -----------------------------------------------------------

    def is_managed(self, email: str) -> bool:
        """Whether this address comes from the environment rather than the UI."""
        return normalize_email(email) in self._managed

    def role_for(self, email: str) -> Optional[str]:
        """Return the role granted to an address, or None if it is not allowed."""
        entry = self._find(email)
        return entry.role if entry else None

    def token_epoch_for(self, email: str) -> Optional[int]:
        """Return the current revocation epoch of an address, or None.

        None means every token bearing that address must be refused: it was
        removed from the allowlist while its JWT was still within its expiry
        window.
        """
        entry = self._find(email)
        if entry is None:
            return None
        return int(getattr(entry, "token_epoch", 0) or 0)

    def admin_count(self) -> int:
        return sum(1 for e in self._entries if e.role == "admin")

    def list_entries(self) -> List[dict]:
        """List the allowed addresses, ordered admins first then alphabetically."""
        return sorted(
            ({"email": e.email, "role": e.role, "managed": self.is_managed(e.email)}
             for e in self._entries),
            key=lambda item: (item["role"] != "admin", item["email"]),
        )

    # ---- mutations ---------------------------------------------------------

    @staticmethod
    def _validate(email: str, role: str) -> str:
        normalized = normalize_email(email)
        if not normalized:
            raise ValueError("An email address is required")
        if len(normalized) > _MAX_EMAIL_LENGTH or not _EMAIL_RE.match(normalized):
            raise ValueError(f"'{email}' is not a valid email address")
        if role not in ROLES:
            raise ValueError(f"Invalid role: {role}")
        return normalized

    async def add(self, email: str, role: str = "viewer") -> dict:
        """Allow an address to sign in. Raises ValueError if already listed."""
        normalized = self._validate(email, role)
        async with self._lock:
            if self._find(normalized):
                raise ValueError(f"'{normalized}' is already allowed")
            self._entries.append(AllowedEmail(
                email=normalized, role=role,
                # Start above any epoch a token issued for a previous listing of
                # the same address could carry.
                token_epoch=next_token_epoch()))
            await self._save()
            logger.info("Address allowed", email=normalized, role=role)
            return {"email": normalized, "role": role, "managed": False}

    async def set_role(self, email: str, role: str) -> dict:
        """Change the role of an allowed address, cutting its open sessions."""
        normalized = self._validate(email, role)
        async with self._lock:
            entry = self._find(normalized)
            if entry is None:
                raise ValueError(f"'{normalized}' is not in the allowlist")
            if self.is_managed(normalized):
                raise ValueError(
                    f"'{normalized}' is configured through "
                    "PULSARCD_AUTH__GOOGLE_ADMINS/VIEWERS and cannot be changed "
                    "here; edit the environment and restart."
                )
            if entry.role != role:
                if entry.role == "admin" and self.admin_count() <= 1:
                    raise ValueError("Cannot demote the last administrator")
                entry.role = role
                entry.token_epoch = next_token_epoch(entry.token_epoch)
                await self._save()
                logger.info("Address role changed", email=normalized, role=role)
            return {"email": normalized, "role": role, "managed": False}

    async def remove(self, email: str) -> bool:
        """Revoke an address. Raises ValueError if unknown, managed or last admin.

        Removal alone cuts the address's live sessions: ``token_epoch_for``
        returns None for an unlisted address, and the API refuses every token
        whose subject it cannot resolve.
        """
        normalized = normalize_email(email)
        async with self._lock:
            entry = self._find(normalized)
            if entry is None:
                raise ValueError(f"'{normalized}' is not in the allowlist")
            if self.is_managed(normalized):
                raise ValueError(
                    f"'{normalized}' is configured through "
                    "PULSARCD_AUTH__GOOGLE_ADMINS/VIEWERS and would come back on "
                    "the next restart; remove it from the environment instead."
                )
            if entry.role == "admin" and self.admin_count() <= 1:
                raise ValueError("Cannot remove the last administrator")
            self._entries = [e for e in self._entries if e.email != normalized]
            await self._save()
            logger.info("Address revoked", email=normalized)
            return True
