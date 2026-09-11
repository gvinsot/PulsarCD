"""JWT authentication helpers for PulsarCD.

Two identity sources mint the same kind of session token:

* ``google``  -- the normal path.  A Google ID token is verified
  (``backend/google_auth.py``) and the address it carries is looked up in the
  allowlist (``backend/allowlist.py``).  ``sub`` is the email address.
* ``local``   -- the break-glass administrator (``backend/user_manager.py``),
  provisioned from the environment and usually absent.  ``sub`` is a username.

The source is carried in the ``auth`` claim because the two namespaces are
independent: the API has to know which store to ask for the revocation epoch of
``sub``, and an address allowlisted for Google must never resolve against a
local account that happens to share its name (or the reverse).
"""

import time
from datetime import datetime, timedelta, timezone

import jwt

# Values of the ``auth`` claim.  A token minted before the claim existed carries
# none and is read as LOCAL, which is what it was.
AUTH_SOURCE_GOOGLE = "google"
AUTH_SOURCE_LOCAL = "local"


def create_token(subject: str, secret: str, expiry_hours: int = 24, role: str = "viewer",
                 token_epoch: int = 0, auth_source: str = AUTH_SOURCE_LOCAL) -> str:
    """Create a JWT token for the given subject and role.

    ``subject`` is the Google address for ``auth_source=google`` and the local
    username otherwise; ``auth_source`` tells the API which store owns it.

    ``token_epoch`` is the revocation epoch of the identity at issuance time (see
    ``next_token_epoch``).  The API rejects a token whose epoch is older than the
    identity's current one, which is how a role change, a removal from the
    allowlist or a break-glass password change cuts sessions that are already
    open.  Tokens issued before this field existed carry no ``epoch`` claim and
    are read as epoch 0, which stays valid for as long as the identity has never
    been revoked (its stored epoch is also 0).
    """
    payload = {
        "sub": subject,
        "role": role,
        "auth": auth_source,
        "epoch": int(token_epoch),
        "exp": datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def decode_token(token: str, secret: str) -> dict:
    """Decode and validate a JWT token. Raises jwt.PyJWTError on failure."""
    return jwt.decode(token, secret, algorithms=["HS256"])


def next_token_epoch(current: int = 0) -> int:
    """Return a strictly increasing revocation epoch.

    A wall-clock second is used as the base so that an identity recreated under
    a name that existed before does not restart from a value an old token could
    match; ``current + 1`` keeps the sequence strictly increasing when two
    revocations happen within the same second.
    """
    try:
        current_value = int(current)
    except (TypeError, ValueError):
        current_value = 0
    return max(int(time.time()), current_value + 1)
