"""Block external clients probing secrets or WordPress on our non-WP edge.

Only Docker-attributed Traefik access logs are trusted. The access analytics
index deliberately drops container provenance, so enforcement reads the same
requests from the raw logs index. It never trusts a client's forwarded header
or a Coraza message interpolating untrusted request text.
"""

import asyncio
import ipaddress
import json
import posixpath
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote

import structlog

from .config import AutoBanConfig
from .ip_blocklist import HTTP_ROUTER_NAME, ROUTER_NAME, IpBlocklist, _timestamp

logger = structlog.get_logger()

_ENV = re.compile(r"(?:^|/)\.env(?:[./;~]|$)", re.IGNORECASE)
_AUTH_JSON = re.compile(r"(?:^|/)auth\.json(?:[./;~]|$)", re.IGNORECASE)
_WORDPRESS = re.compile(
    r"(?:^|/)(?:(?:wp-admin|wp-content|wp-includes|wp-json)(?:[/;]|$)"
    r"|(?:wp-login|wp-config|wp-cron|wp-signup|wp-activate|wp-blog-header|"
    r"wp-load|wp-mail|wp-comments-post|wp-trackback|xmlrpc)\.php(?:[./;~]|$))",
    re.IGNORECASE,
)


def probe_kind(uri: str):
    """Classify only the path, with the WAF's two decodes and Win slashes."""
    path = str(uri).split("?", 1)[0]
    path = unquote(unquote(path)).replace("\\", "/")
    path = posixpath.normpath(path)
    if _ENV.search(path):
        return ".env"
    if _AUTH_JSON.search(path):
        return "auth.json"
    if _WORDPRESS.search(path):
        return "WordPress"
    return None


class AutoBanWorker:
    def __init__(self, opensearch, blocklist: IpBlocklist, config: AutoBanConfig,
                 checkpoint_path: str):
        self.opensearch = opensearch
        self.blocklist = blocklist
        self.config = config
        self._path = Path(checkpoint_path)
        self._task = None
        self._scan_lock = asyncio.Lock()
        self._exemptions = [ipaddress.ip_network(value) for value in config.exempt_cidrs]
        self._checkpoint = datetime.now(timezone.utc)
        self.initialization_error = None
        if self._path.exists():
            try:
                saved = _timestamp(json.loads(self._path.read_text(encoding="utf-8"))["through"])
                if saved is None:
                    raise ValueError("Invalid automatic-ban checkpoint")
                self._checkpoint = saved
            except Exception as exc:
                # Preserve the evidence and the API's manual controls. An
                # invalid cursor must not silently replay historical probes
                # or take the whole deployment control plane down.
                self.initialization_error = str(exc)
                logger.exception("Automatic edge bans disabled: unreadable checkpoint",
                                 path=str(self._path))

    def start(self):
        if self.initialization_error is not None:
            return False
        self._task = asyncio.create_task(self._run(), name="edge-auto-ban")
        return True

    async def stop(self):
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self):
        while True:
            try:
                await self.scan_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Automatic edge ban scan failed; checkpoint retained")
            await asyncio.sleep(self.config.poll_seconds)

    def _save_checkpoint(self, through):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(self._path.suffix + ".tmp")
        temporary.write_text(json.dumps({"through": through.isoformat()}), encoding="utf-8")
        temporary.replace(self._path)
        self._checkpoint = through

    async def _consume(self, source, now):
        if (source.get("compose_project") != self.config.traefik_project
                or source.get("compose_service") != self.config.traefik_service):
            return
        fields = source.get("parsed_fields")
        if not isinstance(fields, dict) or not all(
                key in fields for key in ("ClientHost", "DownstreamStatus", "RequestPath")):
            return
        # Denied requests must not perpetually refresh a client's ban.
        if str(fields.get("RouterName", "")).split("@", 1)[0] in (ROUTER_NAME, HTTP_ROUTER_NAME):
            return
        try:
            address = ipaddress.ip_address(str(fields["ClientHost"]).strip())
        except ValueError:
            return
        if not address.is_global or address.is_multicast or address.is_reserved:
            return
        if any(address.version == network.version and address in network
               for network in self._exemptions):
            return
        stamp = _timestamp(source.get("timestamp"))
        if stamp is None or stamp > now or stamp < now - timedelta(seconds=self.config.duration_seconds):
            return
        kind = probe_kind(fields["RequestPath"])
        if kind:
            await self.blocklist.add_automatic(
                str(address), f"Automatic probe: {kind}", stamp, self.config.duration_seconds)

    async def scan_once(self, now=None):
        """Read every matching log in bounded pages, including delayed writes.

        The durable checkpoint advances only after the entire scroll succeeds.
        Replay guards persist atomically with bans. A restart or partial scan
        can therefore retry safely, including after an operator unblocks an IP.
        """
        if self.initialization_error is not None:
            return
        async with self._scan_lock:
            now = now or datetime.now(timezone.utc)
            start = min(self._checkpoint, now) - timedelta(seconds=self.config.overlap_seconds)
            # After a long outage, already obsolete probes do not merit bans.
            start = max(start, now - timedelta(seconds=self.config.duration_seconds))
            body = {
                "size": self.config.batch_size,
                "sort": ["_doc"],
                "query": {"bool": {"filter": [
                    {"range": {"timestamp": {"gte": start.isoformat(), "lte": now.isoformat()}}},
                    {"term": {"compose_project": self.config.traefik_project}},
                    {"term": {"compose_service": self.config.traefik_service}},
                ]}},
                "_source": ["timestamp", "compose_project", "compose_service", "parsed_fields"],
            }
            scroll_id = None
            try:
                response = await self.opensearch._client.search(
                    index=self.opensearch.logs_index, body=body, scroll="2m", request_timeout=30)
                while True:
                    scroll_id = response.get("_scroll_id", scroll_id)
                    if response.get("timed_out") or response.get("_shards", {}).get("failed", 0):
                        raise RuntimeError("Incomplete automatic-ban log search")
                    hits = response.get("hits", {}).get("hits", [])
                    if not hits:
                        break
                    for hit in hits:
                        await self._consume(hit.get("_source", {}), now)
                    if not scroll_id:
                        raise RuntimeError("Automatic-ban log search returned no scroll cursor")
                    response = await self.opensearch._client.scroll(
                        scroll_id=scroll_id, scroll="2m", request_timeout=30)
                self._save_checkpoint(now)
                self.blocklist.prune_observed(now - timedelta(seconds=self.config.overlap_seconds))
                self.blocklist.targets()  # Expire bans even when no requests arrive.
            finally:
                if scroll_id:
                    try:
                        await self.opensearch._client.clear_scroll(scroll_id=scroll_id)
                    except Exception:
                        logger.warning("Could not release automatic-ban log scroll")
