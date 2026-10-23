"""Addresses PulsarCD's administrators are working from.

The automatic edge ban (auto_ban.py) bans whoever requests a probe path such
as /.git/config, on any host the edge serves. An administrator checking that
the WAF does block such a request is indistinguishable from a scanner, and the
ban then locks them out of PulsarCD itself -- the unblock button with it.

The auth middleware records the client address of every request an
administrator's session makes; the auto-ban leaves those addresses alone. The
WAF still refuses the probe itself: only the 24-hour edge ban is skipped.

Kept in memory: after a restart the open console re-records its address with
its next poll, within seconds. Bounded, so a flood of authenticated requests
from rotating addresses cannot grow it without limit.
"""

import ipaddress
import threading
import time
from typing import Dict, Optional

# Entries kept at most; the least recently seen go first.
MAX_ADDRESSES = 256

_lock = threading.Lock()
_seen: Dict[str, float] = {}


def _canonical(address: str) -> Optional[str]:
    try:
        return str(ipaddress.ip_address(str(address).strip()))
    except ValueError:
        return None


def note(address: str, now: Optional[float] = None) -> None:
    """Record that an administrator's session made a request from ``address``."""
    canonical = _canonical(address)
    if canonical is None:
        return
    with _lock:
        _seen.pop(canonical, None)  # re-inserted last: dicts keep insertion order
        _seen[canonical] = time.time() if now is None else now
        while len(_seen) > MAX_ADDRESSES:
            del _seen[next(iter(_seen))]


def seen_within(address, seconds: float, now: Optional[float] = None) -> bool:
    """Whether an administrator worked from ``address`` in the last ``seconds``."""
    canonical = _canonical(address)
    if canonical is None:
        return False
    with _lock:
        last = _seen.get(canonical)
    return last is not None and (time.time() if now is None else now) - last <= seconds


def reset() -> None:
    """Forget every address (tests)."""
    with _lock:
        _seen.clear()
