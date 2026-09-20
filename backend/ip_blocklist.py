"""Client addresses blocked at the edge, and the Traefik configuration for it.

The Security view answers "who is attacking"; this is the other half -- what an
operator does once a client has been identified.  A blocked address is refused
by Traefik itself, in front of Coraza: it never reaches the WAF, the rate
limiter or any application.

The enforcement is a single router matching every blocked address on both HTTP
entrypoints, at a priority above all application routers, chained to a
middleware that answers 403.  Traefik has no "deny" middleware, so the
idiomatic way to write one is an ``ipAllowList`` whose only allowed source is an
address no packet can come from.

Traefik reads that router through its HTTP provider, which polls
``GET /api/security/waf/traefik-config`` (see backend/api.py).  Nothing is
written on the edge node and no stack is redeployed, so a block takes effect
within one poll interval.  The provider is namespaced (``@http``), so a mistake
here can only break this one router -- never the file and swarm providers that
carry the real routes.

Everything is pure except :class:`IpBlocklist`'s file access, so the rendered
configuration can be checked without an edge.
"""

import asyncio
import ipaddress
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
from pydantic import BaseModel

from shared.access_log import normalize_ip

logger = structlog.get_logger()

# Names of the objects served to Traefik.  They land in the `http` provider's
# namespace, so they cannot collide with anything in dynamic.yml (`@file`) or
# with a router built from Docker labels (`@swarm`).
ROUTER_NAME = "pulsarcd-blocklist"
MIDDLEWARE_NAME = "pulsarcd-blocked"

# Above every application router (whose priority defaults to the length of its
# rule, a few dozen) and far below the ACME HTTP-01 challenge router Traefik
# registers at MaxInt -- a blocked client must not be able to keep a
# certificate from being renewed for everybody else.
ROUTER_PRIORITY = 1_000_000

ENTRYPOINTS = ["web", "websecure"]

# The one source the block middleware allows.  255.255.255.255 is the limited
# broadcast address: it is a syntactically valid sourceRange that can never be
# the source of a routed packet, so every client gets the 403.
DENY_SOURCE_RANGE = ["255.255.255.255/32"]

# A typo away from blocking the whole internet, this deployment included.  A /8
# is already 16 million addresses; anything broader is refused, and so is
# 0.0.0.0/0, which would otherwise pass every other check.
MIN_PREFIX_LENGTH = {4: 8, 6: 32}

# The rule is one `ClientIP()` clause per entry, parsed by Traefik on every
# configuration refresh.  A few hundred is harmless; tens of thousands would
# not be, and a list that long wants CIDR ranges instead.
MAX_ENTRIES = 500

MAX_REASON_LENGTH = 200


class BlockedIp(BaseModel):
    """One address or range refused at the edge."""
    ip: str
    reason: str = ""
    # ISO-8601 UTC, as the access index stores its timestamps.
    blocked_at: str = ""
    blocked_by: str = ""


def normalize_target(value: Any) -> Optional[str]:
    """Canonical form of a blockable address, or None if it is neither.

    A single address keeps its plain form (``1.2.3.4``) rather than becoming
    ``1.2.3.4/32``: it is what the Security view shows and what an operator
    typed, and Traefik's ``ClientIP`` matcher accepts both.
    """
    text = str(value or "").strip()
    if not text:
        return None
    single = normalize_ip(text)
    if single is not None:
        return single
    try:
        return str(ipaddress.ip_network(text, strict=False))
    except ValueError:
        return None


def validate_target(value: Any) -> str:
    """Normalise an address to block, refusing the ones that cut our own legs.

    Raises ValueError -- the API turns it into a 400 whose message is what the
    operator reads.
    """
    target = normalize_target(value)
    if target is None:
        raise ValueError(f"'{value}' is not an IP address or a CIDR range")

    network = ipaddress.ip_network(target, strict=False)
    if network.is_private or network.is_loopback or network.is_link_local:
        raise ValueError(
            f"'{target}' is an internal address. Blocking one would cut the "
            "cluster's own traffic (nodes pulling images, CI pushing to the "
            "registry, clients coming back through the router's NAT loopback), "
            "and the edge would refuse it for every service at once."
        )
    minimum = MIN_PREFIX_LENGTH[network.version]
    if network.prefixlen < minimum:
        raise ValueError(
            f"'{target}' covers {network.num_addresses} addresses. Blocks are "
            f"limited to /{minimum} and narrower so a typo cannot take the "
            "whole edge down."
        )
    return target


def normalize_reason(value: Any) -> str:
    return str(value or "").strip()[:MAX_REASON_LENGTH]


def _now() -> str:
    """UTC, second precision, with the Z suffix the access index uses."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def covers(target: str, ip: str) -> bool:
    """Whether a blocked entry applies to an address (it may be a range)."""
    address = normalize_ip(ip)
    if address is None:
        return False
    try:
        network = ipaddress.ip_network(target, strict=False)
    except ValueError:
        return False
    if network.version != ipaddress.ip_address(address).version:
        return False
    return ipaddress.ip_address(address) in network


def block_rule(targets: List[str]) -> str:
    """The router rule matching every blocked address.

    ``ClientIP`` takes one address or CIDR per clause, so they are OR-ed.  The
    values are validated addresses, which cannot contain the backquote that
    delimits them.
    """
    return " || ".join(f"ClientIP(`{target}`)" for target in targets)


def traefik_config(targets: List[str]) -> Dict[str, Any]:
    """Traefik dynamic configuration enforcing the blocklist.

    An empty blocklist returns an empty configuration rather than a router with
    an empty rule, which Traefik would refuse -- and refusing it would leave the
    previous rule in place, so an unblock would never take effect.
    """
    if not targets:
        return {"http": {}}
    return {
        "http": {
            "middlewares": {
                MIDDLEWARE_NAME: {"ipAllowList": {"sourceRange": list(DENY_SOURCE_RANGE)}},
            },
            "routers": {
                ROUTER_NAME: {
                    "rule": block_rule(targets),
                    "priority": ROUTER_PRIORITY,
                    "entryPoints": list(ENTRYPOINTS),
                    "middlewares": [MIDDLEWARE_NAME],
                    # No backend is ever reached: the middleware answers first.
                    "service": "noop@internal",
                    # Required to match on websecure. No certResolver: the
                    # router matches no domain, and Traefik picks the
                    # certificate from the SNI at handshake time, before any
                    # router is selected -- a blocked client still gets the
                    # real certificate of the host it asked for, then the 403.
                    "tls": {},
                },
            },
        }
    }


class IpBlocklist:
    """File-backed list of the addresses refused at the edge."""

    def __init__(self, path: str):
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._entries: List[BlockedIp] = []
        self._load()

    # ---- loading and persistence -------------------------------------------

    def _load(self) -> None:
        if not self._path.exists():
            logger.info("Edge blocklist is empty", path=str(self._path))
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception as e:
            # Starting with an empty list would silently unblock everyone, so
            # the failure is loud.  The file is only written by this class.
            logger.error("Failed to parse the edge blocklist; no address is "
                         "blocked until it is fixed",
                         path=str(self._path), error=str(e))
            return

        seen = set()
        for item in raw if isinstance(raw, list) else []:
            try:
                entry = BlockedIp(**item)
            except Exception:
                logger.warning("Ignoring a malformed blocklist entry", entry=str(item)[:120])
                continue
            target = normalize_target(entry.ip)
            if target is None or target in seen:
                continue
            seen.add(target)
            entry.ip = target
            self._entries.append(entry)
        logger.info("Edge blocklist loaded", path=str(self._path),
                    count=len(self._entries))

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = [e.model_dump() for e in self._entries]
        self._path.write_text(json.dumps(data, indent=2, ensure_ascii=False),
                              encoding="utf-8")

    # ---- lookups -----------------------------------------------------------

    def targets(self) -> List[str]:
        """The blocked addresses, in the order they were added."""
        return [e.ip for e in self._entries]

    def list_entries(self) -> List[dict]:
        """The blocklist, most recently blocked first.

        Insertion order reversed, not the timestamp: two addresses blocked in
        the same second would otherwise come back in an arbitrary order.
        """
        return [e.model_dump() for e in reversed(self._entries)]

    def blocked_entry(self, ip: str) -> Optional[dict]:
        """The entry that blocks an address, directly or through a range."""
        for entry in self._entries:
            if entry.ip == ip or covers(entry.ip, ip):
                return entry.model_dump()
        return None

    def traefik_config(self) -> Dict[str, Any]:
        return traefik_config(self.targets())

    # ---- mutations ---------------------------------------------------------

    async def add(self, ip: str, reason: str = "", blocked_by: str = "") -> dict:
        """Block an address or range. Raises ValueError if it is already blocked."""
        target = validate_target(ip)
        async with self._lock:
            if any(e.ip == target for e in self._entries):
                raise ValueError(f"'{target}' is already blocked")
            if len(self._entries) >= MAX_ENTRIES:
                raise ValueError(
                    f"The blocklist is full ({MAX_ENTRIES} entries). Remove "
                    "entries that no longer matter, or replace them with the "
                    "CIDR range that covers them."
                )
            entry = BlockedIp(
                ip=target,
                reason=normalize_reason(reason),
                blocked_at=_now(),
                blocked_by=blocked_by,
            )
            self._entries.append(entry)
            self._save()
        logger.info("Address blocked at the edge", ip=target, by=blocked_by,
                    reason=entry.reason)
        return entry.model_dump()

    async def remove(self, ip: str) -> dict:
        """Unblock an address. Raises ValueError if it is not blocked."""
        target = normalize_target(ip)
        if target is None:
            raise ValueError(f"'{ip}' is not an IP address or a CIDR range")
        async with self._lock:
            entry = next((e for e in self._entries if e.ip == target), None)
            if entry is None:
                # An address covered by a blocked range cannot be unblocked on
                # its own: saying so beats reporting "not blocked" to someone
                # who can see it being refused.
                covering = next((e for e in self._entries if covers(e.ip, target)), None)
                if covering is not None:
                    raise ValueError(
                        f"'{target}' is not blocked on its own; it is covered "
                        f"by the range '{covering.ip}'. Unblock that range."
                    )
                raise ValueError(f"'{target}' is not blocked")
            self._entries.remove(entry)
            self._save()
        logger.info("Address unblocked at the edge", ip=target)
        return entry.model_dump()
