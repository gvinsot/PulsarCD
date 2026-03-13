"""JWT authentication helpers for PulsarCD."""

from datetime import datetime, timedelta, timezone

import jwt


def create_token(username: str, secret: str, expiry_hours: int = 24, role: str = "viewer") -> str:
    """Create a JWT token for the given username and role."""
    payload = {
        "sub": username,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def decode_token(token: str, secret: str) -> dict:
    """Decode and validate a JWT token. Raises jwt.PyJWTError on failure."""
    return jwt.decode(token, secret, algorithms=["HS256"])
