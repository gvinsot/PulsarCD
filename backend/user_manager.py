"""File-based user management for PulsarCD.

Stores users in /data/users.json with bcrypt password hashing.
Auto-creates a default admin user on first boot.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import List, Optional

import structlog
from passlib.hash import bcrypt
from pydantic import BaseModel

logger = structlog.get_logger()


class User(BaseModel):
    """User account."""
    username: str
    password_hash: str
    role: str = "admin"  # "admin" or "viewer"


class UserManager:
    """File-based user CRUD with bcrypt authentication."""

    def __init__(self, path: str = "/data/users.json"):
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._users: List[User] = []
        self._load()

    def _load(self) -> None:
        """Load users from JSON file, create default admin if absent."""
        if self._path.exists():
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                self._users = [User(**u) for u in raw]
                logger.info("Users loaded", path=str(self._path), count=len(self._users))
                return
            except Exception as e:
                logger.error("Failed to parse users file, starting fresh",
                             path=str(self._path), error=str(e))

        # Auto-create default admin from env vars or fallback
        username = os.environ.get("PULSARCD_AUTH__USERNAME", "admin")
        password = os.environ.get("PULSARCD_AUTH__PASSWORD", "changeme")
        self._users = [User(
            username=username,
            password_hash=bcrypt.hash(password),
            role="admin",
        )]
        self._save_sync()
        logger.info("Default admin user created", username=username, path=str(self._path))

    def _save_sync(self) -> None:
        """Write users to JSON file (synchronous)."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            data = [u.model_dump() for u in self._users]
            self._path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            logger.error("Failed to save users file", path=str(self._path), error=str(e))
            raise

    async def _save(self) -> None:
        """Write users to JSON file (async-safe)."""
        self._save_sync()

    def authenticate(self, username: str, password: str) -> Optional[User]:
        """Verify credentials and return user if valid."""
        for user in self._users:
            if user.username == username:
                if bcrypt.verify(password, user.password_hash):
                    return user
                return None
        return None

    def get_user(self, username: str) -> Optional[User]:
        """Get user by username."""
        for user in self._users:
            if user.username == username:
                return user
        return None

    def list_users(self) -> List[dict]:
        """List all users (without password hashes)."""
        return [{"username": u.username, "role": u.role} for u in self._users]

    async def create_user(self, username: str, password: str, role: str = "viewer") -> dict:
        """Create a new user. Raises ValueError if username exists."""
        async with self._lock:
            if self.get_user(username):
                raise ValueError(f"User '{username}' already exists")
            if role not in ("admin", "viewer"):
                raise ValueError(f"Invalid role: {role}")

            user = User(
                username=username,
                password_hash=bcrypt.hash(password),
                role=role,
            )
            self._users.append(user)
            await self._save()
            logger.info("User created", username=username, role=role)
            return {"username": user.username, "role": user.role}

    async def update_user(self, username: str, password: Optional[str] = None, role: Optional[str] = None) -> dict:
        """Update an existing user. Raises ValueError if not found."""
        async with self._lock:
            user = self.get_user(username)
            if not user:
                raise ValueError(f"User '{username}' not found")
            if role is not None:
                if role not in ("admin", "viewer"):
                    raise ValueError(f"Invalid role: {role}")
                user.role = role
            if password is not None:
                user.password_hash = bcrypt.hash(password)
            await self._save()
            logger.info("User updated", username=username, role=user.role)
            return {"username": user.username, "role": user.role}

    async def delete_user(self, username: str) -> bool:
        """Delete a user. Raises ValueError if not found or last admin."""
        async with self._lock:
            user = self.get_user(username)
            if not user:
                raise ValueError(f"User '{username}' not found")

            admin_count = sum(1 for u in self._users if u.role == "admin")
            if user.role == "admin" and admin_count <= 1:
                raise ValueError("Cannot delete the last admin user")

            self._users = [u for u in self._users if u.username != username]
            await self._save()
            logger.info("User deleted", username=username)
            return True
