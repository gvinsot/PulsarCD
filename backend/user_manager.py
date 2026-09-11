"""Break-glass local administrator for PulsarCD.

Signing in normally goes through Google: an ID token is verified
(``backend/google_auth.py``) and the address it carries is looked up in the
allowlist (``backend/allowlist.py``).  This module keeps exactly ONE local
account alive as a way back in when Google is unreachable, the OAuth client is
misconfigured, or the last allowlisted administrator locked themselves out.

It only exists when the operator provisions it explicitly:

    PULSARCD_AUTH__USERNAME=admin           (optional, defaults to "admin")
    PULSARCD_AUTH__PASSWORD=<12+ characters> (required: no password, no account)

With ``PULSARCD_AUTH__PASSWORD`` unset there is no local account at all and
``POST /api/auth/login`` answers 403 -- nothing to guess, nothing to stuff.  A
weak or placeholder value is refused the same way rather than replaced by a
generated one: on a Google-first deployment, a random password printed once into
the container logs is a live administrator account that nobody will ever read.

The environment is authoritative on every boot.  Changing the password rewrites
the stored hash and bumps the token epoch, which cuts the sessions the old
password opened; clearing it deletes the account outright.  This is deliberate:
the file is a cache of the configuration, not a second place to edit it, and the
previous behaviour (keep whatever the file says, ignore the environment forever)
made a password rotation look like it had worked when it had not.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import List, Optional

import bcrypt as _bcrypt
import structlog
from pydantic import BaseModel

from .auth import next_token_epoch as _next_token_epoch

logger = structlog.get_logger()

# Minimum length accepted for the break-glass password. Anything shorter,
# empty, or a well-known placeholder is refused and the account is not created.
MIN_ADMIN_PASSWORD_LENGTH = 12

# Placeholder values shipped in the sample compose/.env files.
_PLACEHOLDER_PASSWORDS = frozenset({"changeme", "change-me", "changemenow",
                                    "password", "admin", "pulsarcd", "secret"})


def weak_password_reason(password: str) -> Optional[str]:
    """Return why a password is unacceptable, or None if it is fine."""
    if not password:
        return "not set"
    if password.strip().lower() in _PLACEHOLDER_PASSWORDS:
        return "a well-known placeholder value"
    if len(password) < MIN_ADMIN_PASSWORD_LENGTH:
        return f"shorter than {MIN_ADMIN_PASSWORD_LENGTH} characters"
    return None


# Kept as a private alias: the bootstrap path already reads this name.
_weak_admin_password_reason = weak_password_reason


class User(BaseModel):
    """The break-glass account."""
    username: str
    password_hash: str
    role: str = "admin"
    # Revocation epoch carried by every JWT issued for this account.  A token
    # whose epoch is older than this value is refused, so bumping the field
    # invalidates every session already open.  Absent from a users.json written
    # by an older version: it then reads as 0.
    token_epoch: int = 0


class UserManager:
    """Holds the single break-glass administrator, reconciled from the env."""

    def __init__(self, path: str = "/data/users.json"):
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._users: List[User] = []
        self._load()

    @property
    def enabled(self) -> bool:
        """Whether a local account exists, i.e. whether password login works."""
        return bool(self._users)

    def _read_file(self) -> List[User]:
        """Read whatever accounts the data file holds, tolerating junk."""
        if not self._path.exists():
            return []
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return [User(**u) for u in raw]
        except Exception as e:
            logger.error("Failed to parse users file, starting fresh",
                         path=str(self._path), error=str(e))
            return []

    def _load(self) -> None:
        """Reconcile the stored account with the environment."""
        stored = self._read_file()
        username = (os.environ.get("PULSARCD_AUTH__USERNAME") or "admin").strip() or "admin"
        password = os.environ.get("PULSARCD_AUTH__PASSWORD", "")

        reason = weak_password_reason(password)
        if reason:
            if password:
                logger.error(
                    "PULSARCD_AUTH__PASSWORD rejected; the break-glass "
                    "administrator is DISABLED and password login answers 403. "
                    f"Set it to at least {MIN_ADMIN_PASSWORD_LENGTH} characters "
                    "(and not a placeholder) to provision it.",
                    username=username, reason=reason)
            else:
                logger.info(
                    "No break-glass administrator configured; sign-in is "
                    "Google-only. Set PULSARCD_AUTH__PASSWORD to provision one.")
            self._users = []
            if stored:
                # The environment used to define an account and no longer does:
                # the file must not keep a credential the operator has revoked.
                logger.warning("Removing the stored break-glass administrator",
                               path=str(self._path))
                self._save_sync()
            return

        previous = next((u for u in stored if u.username == username), None)
        if previous is not None and self._hash_matches(previous, password):
            # Unchanged: keep the stored epoch so a restart does not sign the
            # operator out of a session they opened a minute ago.
            self._users = [previous]
            if len(stored) != 1:
                logger.warning(
                    "Dropping extra accounts from the users file; only the "
                    "break-glass administrator is kept, everyone else signs in "
                    "with Google", path=str(self._path), dropped=len(stored) - 1)
                self._save_sync()
            return

        self._users = [User(
            username=username,
            password_hash=_bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode(),
            role="admin",
            # Bumped past the previous value so the sessions the old password
            # opened do not survive the rotation.
            token_epoch=_next_token_epoch(getattr(previous, "token_epoch", 0) if previous else 0),
        )]
        self._save_sync()
        logger.info("Break-glass administrator provisioned from the environment",
                    username=username, path=str(self._path),
                    replaced=previous is not None)

    @staticmethod
    def _hash_matches(user: User, password: str) -> bool:
        """Whether the stored hash already corresponds to this password."""
        try:
            return _bcrypt.checkpw(password.encode(), user.password_hash.encode())
        except (ValueError, TypeError):
            return False  # hand-edited or truncated hash: rewrite it

    def _save_sync(self) -> None:
        """Write the account to the JSON file (synchronous)."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            data = [u.model_dump() for u in self._users]
            self._path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            logger.error("Failed to save users file", path=str(self._path), error=str(e))
            raise

    def authenticate(self, username: str, password: str) -> Optional[User]:
        """Verify credentials and return the account if valid."""
        for user in self._users:
            if user.username == username:
                if self._hash_matches(user, password):
                    return user
                return None
        return None

    def get_user(self, username: str) -> Optional[User]:
        """Get the local account by username."""
        for user in self._users:
            if user.username == username:
                return user
        return None

    def token_epoch_for(self, username: str) -> Optional[int]:
        """Return the current token epoch of the account, or None if unknown.

        None means every token bearing that username must be refused: the
        account was removed while its JWT was still within its expiry window.
        """
        user = self.get_user(username)
        if user is None:
            return None
        return int(getattr(user, "token_epoch", 0) or 0)

    def describe(self) -> Optional[dict]:
        """Describe the break-glass account for the admin UI, or None."""
        if not self._users:
            return None
        user = self._users[0]
        return {"username": user.username, "role": user.role}
