"""GitHub push webhook: signature check, replay protection and payload parsing.

The HTTP route lives in ``backend.api`` (``POST /api/webhooks/github``) because
it must reach the same de-duplicated pipeline entry point as the auto-build
poller.  Everything here is pure and side-effect free so it can be unit tested
without the app:

* :func:`verify_signature` checks ``X-Hub-Signature-256`` (HMAC-SHA256 of the
  raw body keyed with the shared secret) in constant time;
* :class:`DeliveryCache` remembers recent ``X-GitHub-Delivery`` ids so a
  captured, correctly signed request cannot be replayed to re-run a pipeline;
* :func:`decode_body` / :func:`parse_push` turn the request into a
  :class:`PushEvent`, accepting both delivery content types GitHub offers.
"""

import hashlib
import hmac
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import parse_qs

# GitHub caps webhook payloads at 25 MB; anything larger is not from GitHub.
MAX_PAYLOAD_BYTES = 25 * 1024 * 1024

# A deleted branch or tag is reported with this "after" SHA.
ZERO_SHA = "0" * 40

_SIGNATURE_PREFIX = "sha256="


def verify_signature(secret: str, body: bytes, signature_header: Optional[str]) -> bool:
    """Return True if ``signature_header`` is the HMAC-SHA256 of ``body``.

    The comparison is constant-time.  An empty secret never validates, so a
    misconfigured deployment cannot accept unsigned requests by accident.
    """
    if not secret or not signature_header:
        return False
    if not signature_header.startswith(_SIGNATURE_PREFIX):
        return False
    presented = signature_header[len(_SIGNATURE_PREFIX):].strip().lower()
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(presented.encode("ascii", "replace"), expected.encode("ascii"))


class DeliveryCache:
    """Bounded, time-limited memory of processed ``X-GitHub-Delivery`` ids.

    In-memory on purpose: a restart forgets the ids, but every replay that
    gets past it still lands on the per-repo ``last_sha`` check, which refuses
    to rebuild a commit that was already handled.
    """

    def __init__(self, max_entries: int = 2048, ttl_seconds: float = 24 * 3600):
        self._max_entries = max_entries
        self._ttl = ttl_seconds
        self._seen: "OrderedDict[str, float]" = OrderedDict()

    def _prune(self, now: float):
        while self._seen:
            oldest_id, seen_at = next(iter(self._seen.items()))
            if now - seen_at <= self._ttl and len(self._seen) <= self._max_entries:
                break
            self._seen.pop(oldest_id)

    def check_and_add(self, delivery_id: str, now: Optional[float] = None) -> bool:
        """Record ``delivery_id``; return False if it was already recorded."""
        now = time.monotonic() if now is None else now
        self._prune(now)
        if delivery_id in self._seen:
            return False
        self._seen[delivery_id] = now
        self._prune(now)
        return True

    def discard(self, delivery_id: str):
        """Forget ``delivery_id`` so a redelivery after a failure is processed."""
        self._seen.pop(delivery_id, None)

    def __len__(self) -> int:
        return len(self._seen)

    def clear(self):
        self._seen.clear()


@dataclass(frozen=True)
class PushEvent:
    """The parts of a GitHub ``push`` payload the auto-build needs."""
    owner: str
    name: str
    full_name: str
    ref: str
    before: str
    after: str
    default_branch: str
    deleted: bool

    @property
    def branch(self) -> Optional[str]:
        """Branch name for a ``refs/heads/`` push, None for tags and others."""
        prefix = "refs/heads/"
        return self.ref[len(prefix):] if self.ref.startswith(prefix) else None

    @property
    def is_default_branch(self) -> bool:
        return bool(self.default_branch) and self.branch == self.default_branch


def decode_body(body: bytes, content_type: str) -> Dict[str, Any]:
    """Decode a webhook body sent as ``application/json`` or form-encoded.

    GitHub's form encoding carries the JSON document in a ``payload`` field.
    Raises ``ValueError`` when the body is not a JSON object.
    """
    text = body.decode("utf-8")
    if (content_type or "").split(";")[0].strip().lower() == "application/x-www-form-urlencoded":
        fields = parse_qs(text)
        if "payload" not in fields:
            raise ValueError("form-encoded webhook without a payload field")
        text = fields["payload"][0]
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("webhook payload is not a JSON object")
    return data


def parse_push(payload: Dict[str, Any]) -> Optional[PushEvent]:
    """Extract a :class:`PushEvent`, or None if the payload is not usable."""
    repo = payload.get("repository")
    if not isinstance(repo, dict):
        return None
    name = repo.get("name")
    full_name = repo.get("full_name")
    owner_info = repo.get("owner") or {}
    # Push payloads name the owner under "name" (legacy) and "login".
    owner = ((owner_info.get("login") or owner_info.get("name"))
             if isinstance(owner_info, dict) else None)
    if not owner and isinstance(full_name, str) and "/" in full_name:
        owner = full_name.split("/", 1)[0]
    ref = payload.get("ref")
    after = payload.get("after")
    if not all(isinstance(v, str) and v for v in (name, full_name, owner, ref, after)):
        return None
    return PushEvent(
        owner=owner,
        name=name,
        full_name=full_name,
        ref=ref,
        before=payload.get("before") or "",
        after=after,
        default_branch=repo.get("default_branch") or repo.get("master_branch") or "",
        deleted=bool(payload.get("deleted")) or after == ZERO_SHA,
    )
