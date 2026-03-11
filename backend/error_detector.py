"""Recurring error detector using zvec for similarity-based clustering.

Runs as a background task, periodically scanning recent ERROR/FATAL/CRITICAL logs
from OpenSearch, vectorizing them with zvec, and detecting recurring patterns.
When a recurring error pattern is confirmed (N occurrences across M services/containers),
it posts a repair task to the QWEN agent.
"""

import asyncio
import hashlib
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import structlog

logger = structlog.get_logger()

# Lazy-loaded zvec (optional dependency)
_zvec = None
_zvec_available = None


def _get_zvec():
    global _zvec, _zvec_available
    if _zvec_available is None:
        try:
            import zvec
            _zvec = zvec
            _zvec_available = True
            logger.info("zvec loaded successfully")
        except ImportError:
            _zvec_available = False
            logger.warning("zvec not installed — error detector will use text hashing fallback")
    return _zvec


# ---------------------------------------------------------------------------
# Text normalization — strip variable parts (timestamps, IDs, hex, IPs, paths)
# so that structurally identical errors produce the same fingerprint.
# ---------------------------------------------------------------------------

_STRIP_PATTERNS = [
    (re.compile(r'\b[0-9a-fA-F]{8,}\b'), '<HEX>'),          # hex IDs
    (re.compile(r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.\d]*Z?\b'), '<TS>'),  # timestamps
    (re.compile(r'\b\d+\.\d+\.\d+\.\d+(:\d+)?\b'), '<IP>'),  # IPs
    (re.compile(r'\b\d{5,}\b'), '<NUM>'),                      # large numbers
    (re.compile(r'/[a-zA-Z0-9_./-]{20,}'), '<PATH>'),         # long paths
    (re.compile(r'"[^"]{60,}"'), '<STR>'),                     # long strings
]


def normalize_message(msg: str) -> str:
    """Strip variable parts from a log message to get a structural fingerprint."""
    for pattern, replacement in _STRIP_PATTERNS:
        msg = pattern.sub(replacement, msg)
    return msg.strip()


def text_fingerprint(msg: str) -> str:
    """Fast hash-based fingerprint for a normalized message."""
    return hashlib.md5(normalize_message(msg).encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Recurring error pattern tracker
# ---------------------------------------------------------------------------

class ErrorPattern:
    """Tracks a recurring error pattern."""
    __slots__ = ('fingerprint', 'sample_message', 'services', 'count',
                 'first_seen', 'last_seen', 'notified')

    def __init__(self, fingerprint: str, message: str, service: str):
        self.fingerprint = fingerprint
        self.sample_message = message[:500]
        self.services: Set[str] = {service}
        self.count = 1
        self.first_seen = datetime.utcnow()
        self.last_seen = datetime.utcnow()
        self.notified = False

    def add_occurrence(self, service: str, message: str):
        self.count += 1
        self.last_seen = datetime.utcnow()
        self.services.add(service)
        # Keep the shortest sample (usually the most representative)
        if len(message) < len(self.sample_message):
            self.sample_message = message[:500]


class RecurringErrorDetector:
    """Background service that detects recurring error patterns in logs.

    Architecture:
    - Runs every `scan_interval` seconds (default 60s)
    - First scan: fetches the last `initial_lookback_hours` of errors (default 12h)
    - Subsequent scans: incremental — only fetches errors since the last scan
      (no double-counting from overlapping windows)
    - Normalizes messages and groups by fingerprint / zvec similarity
    - Patterns accumulate counts across scans and are evicted after
      `pattern_ttl_hours` of inactivity (default 12h)
    - When a pattern hits the threshold (count >= min_occurrences),
      posts a task to the QWEN agent; re-notifies at most once per hour
    """

    def __init__(
        self,
        opensearch_client,
        swarm_api_base: str,
        swarm_agent_name: str,
        swarm_secret_key: str,
        scan_interval: int = 60,
        initial_lookback_hours: int = 12,
        min_occurrences: int = 5,
        pattern_ttl_hours: int = 12,
        zvec_similarity_threshold: float = 0.92,
        zvec_db_path: str = "/tmp/logscrawler_zvec",
    ):
        self._opensearch = opensearch_client
        self._swarm_api_base = swarm_api_base
        self._swarm_agent_name = swarm_agent_name
        self._swarm_secret_key = swarm_secret_key
        self._scan_interval = scan_interval
        self._initial_lookback_hours = initial_lookback_hours
        self._min_occurrences = min_occurrences
        self._pattern_ttl_hours = pattern_ttl_hours
        self._similarity_threshold = zvec_similarity_threshold
        self._zvec_db_path = zvec_db_path

        # State
        self._patterns: Dict[str, ErrorPattern] = {}
        # fingerprint -> datetime of last notification; re-notify only after 1 hour
        self._notified_fingerprints: Dict[str, datetime] = {}
        # Persistent history of the last 20 notified patterns (survives the 1h eviction window)
        self._notification_history: List[dict] = []
        self._running = False
        self._last_scan_ts: Optional[datetime] = None

        # zvec collection (lazy init)
        self._zvec_collection = None
        self._zvec_dim = 0

    async def start(self):
        if self._running:
            return
        self._running = True
        logger.info("Recurring error detector starting",
                     interval=self._scan_interval,
                     initial_lookback_hours=self._initial_lookback_hours,
                     pattern_ttl_hours=self._pattern_ttl_hours,
                     threshold=self._min_occurrences)
        asyncio.create_task(self._scan_loop())

    async def stop(self):
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _scan_loop(self):
        # Small initial delay so the rest of the app starts first
        await asyncio.sleep(10)

        while self._running:
            try:
                await self._scan()
            except Exception as e:
                logger.error("Error detector scan failed", error=str(e))

            await asyncio.sleep(self._scan_interval)

    async def _scan(self):
        """One scan cycle: fetch new errors, fingerprint, detect patterns."""
        now = datetime.utcnow()

        if self._last_scan_ts is None:
            # First scan: bootstrap with the full initial lookback window
            since = now - timedelta(hours=self._initial_lookback_hours)
            logger.info("Error detector first scan", lookback_hours=self._initial_lookback_hours)
        else:
            # Incremental: only fetch errors that arrived since the last scan
            # to avoid double-counting errors across overlapping scan windows
            since = self._last_scan_ts

        # Query OpenSearch for new error logs
        errors = await self._fetch_recent_errors(since)
        if not errors:
            self._last_scan_ts = now
            return

        logger.debug("Error detector scanning",
                     error_count=len(errors),
                     since=since.isoformat(),
                     incremental=self._last_scan_ts is not None)

        # Group by fingerprint
        zvec = _get_zvec()
        cycle_fingerprints: Dict[str, List[dict]] = {}

        if zvec and self._zvec_collection is not None:
            # Use zvec similarity — group similar messages together
            cycle_fingerprints = await self._group_by_similarity(errors)
        else:
            # Fallback: group by text hash
            for err in errors:
                fp = text_fingerprint(err["message"])
                cycle_fingerprints.setdefault(fp, []).append(err)

        # Update pattern tracker
        for fp, occurrences in cycle_fingerprints.items():
            if fp in self._patterns:
                for occ in occurrences:
                    svc = occ.get("compose_project") or occ.get("container_name", "unknown")
                    self._patterns[fp].add_occurrence(svc, occ["message"])
            else:
                first = occurrences[0]
                svc = first.get("compose_project") or first.get("container_name", "unknown")
                self._patterns[fp] = ErrorPattern(fp, first["message"], svc)
                for occ in occurrences[1:]:
                    s = occ.get("compose_project") or occ.get("container_name", "unknown")
                    self._patterns[fp].add_occurrence(s, occ["message"])

        # Check thresholds and notify (with 1-hour cooldown per fingerprint)
        cooldown = timedelta(hours=1)
        for fp, pattern in list(self._patterns.items()):
            if pattern.count >= self._min_occurrences:
                last_notified = self._notified_fingerprints.get(fp)
                if last_notified is None or (now - last_notified) >= cooldown:
                    await self._notify_recurring_error(pattern)
                    self._notified_fingerprints[fp] = now

        # Evict patterns not seen within the TTL window
        cutoff = now - timedelta(hours=self._pattern_ttl_hours)
        stale = [fp for fp, p in self._patterns.items() if p.last_seen < cutoff]
        for fp in stale:
            del self._patterns[fp]
            self._notified_fingerprints.pop(fp, None)

        self._last_scan_ts = now

    # ------------------------------------------------------------------
    # OpenSearch queries
    # ------------------------------------------------------------------

    async def _fetch_recent_errors(self, since: datetime) -> List[dict]:
        """Fetch recent ERROR/FATAL/CRITICAL logs from OpenSearch."""
        try:
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"level": ["ERROR", "FATAL", "CRITICAL"]}},
                            {"range": {"timestamp": {"gte": since.isoformat()}}},
                        ]
                    }
                },
                "size": 2000,
                "sort": [{"timestamp": "desc"}],
                "_source": ["message", "container_name", "compose_project",
                            "compose_service", "host", "timestamp", "level"],
            }
            response = await self._opensearch._client.search(
                index=self._opensearch.logs_index, body=body
            )
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception as e:
            logger.error("Error detector: failed to fetch errors", error=str(e))
            return []

    # ------------------------------------------------------------------
    # zvec similarity grouping
    # ------------------------------------------------------------------

    def _init_zvec(self):
        """Initialize zvec collection for error message similarity."""
        zvec = _get_zvec()
        if not zvec:
            return

        try:
            # Use a simple character n-gram approach for vectorization
            # zvec stores vectors; we generate them from normalized text
            self._zvec_dim = 128
            schema = zvec.CollectionSchema(
                name="error_patterns",
                vectors=zvec.VectorSchema("embedding", zvec.DataType.VECTOR_FP32, self._zvec_dim),
            )
            path = Path(self._zvec_db_path)
            path.mkdir(parents=True, exist_ok=True)
            self._zvec_collection = zvec.create_and_open(
                path=str(path), schema=schema,
                memory_limit_mb=50,
            )
            logger.info("zvec collection initialized", path=str(path), dim=self._zvec_dim)
        except Exception as e:
            logger.warning("Failed to initialize zvec", error=str(e))
            self._zvec_collection = None

    def _text_to_vector(self, text: str) -> List[float]:
        """Convert normalized text to a fixed-size vector using character n-gram hashing.

        This is a lightweight alternative to embedding models — good enough for
        detecting structurally similar error messages without any ML dependency.
        """
        normalized = normalize_message(text)
        vec = [0.0] * self._zvec_dim

        # Character trigram hashing
        for i in range(len(normalized) - 2):
            trigram = normalized[i:i+3]
            idx = hash(trigram) % self._zvec_dim
            vec[idx] += 1.0

        # L2 normalize
        norm = sum(v * v for v in vec) ** 0.5
        if norm > 0:
            vec = [v / norm for v in vec]

        return vec

    async def _group_by_similarity(self, errors: List[dict]) -> Dict[str, List[dict]]:
        """Group errors by zvec cosine similarity."""
        zvec = _get_zvec()
        if not zvec or not self._zvec_collection:
            # Fallback
            groups: Dict[str, List[dict]] = {}
            for err in errors:
                fp = text_fingerprint(err["message"])
                groups.setdefault(fp, []).append(err)
            return groups

        groups: Dict[str, List[dict]] = {}
        fp_map: Dict[str, str] = {}  # zvec_id -> fingerprint

        for err in errors:
            vec = self._text_to_vector(err["message"])
            fp = text_fingerprint(err["message"])

            # Search for similar existing vectors
            try:
                results = self._zvec_collection.query(
                    zvec.VectorQuery("embedding", vector=vec),
                    topk=1
                )
                if results and results[0].score >= self._similarity_threshold:
                    # Merge into existing group
                    existing_fp = fp_map.get(results[0].id, fp)
                    groups.setdefault(existing_fp, []).append(err)
                    continue
            except Exception:
                pass

            # New pattern — insert into zvec
            doc_id = f"err_{fp}_{int(time.time() * 1000)}"
            try:
                self._zvec_collection.insert([
                    zvec.Doc(id=doc_id, vectors={"embedding": vec})
                ])
                fp_map[doc_id] = fp
            except Exception:
                pass

            groups.setdefault(fp, []).append(err)

        return groups

    # ------------------------------------------------------------------
    # QWEN agent notification
    # ------------------------------------------------------------------

    async def _notify_recurring_error(self, pattern: ErrorPattern):
        """Post a recurring error task to the QWEN agent."""
        if not self._swarm_secret_key:
            return

        services_list = ', '.join(sorted(pattern.services)[:10])
        duration = pattern.last_seen - pattern.first_seen
        duration_str = f"{int(duration.total_seconds())}s" if duration.total_seconds() < 3600 else f"{duration.total_seconds() / 3600:.1f}h"

        task_description = (
            f"RECURRING ERROR DETECTED\n"
            f"========================\n"
            f"Occurrences: {pattern.count} in {duration_str}\n"
            f"Affected services: {services_list}\n"
            f"Error sample:\n```\n{pattern.sample_message}\n```\n\n"
            f"3 possible actions:\n"
            f"1. CORRIGER: Investigate root cause and fix the issue\n"
            f"2. IGNORER: If this is a known/expected error, mark it as ignored\n"
            f"3. ALERTER ADMIN: If MCP is available, send a message to the admin for manual review\n"
        )

        # Safety cap: truncate to 128 KB
        max_bytes = 128 * 1024
        encoded = task_description.encode('utf-8')
        if len(encoded) > max_bytes:
            task_description = encoded[:max_bytes].decode('utf-8', errors='ignore') + '\n... [truncated]'

        url = f"{self._swarm_api_base}/agents/{self._swarm_agent_name}/tasks"
        headers = {
            "Authorization": f"Bearer {self._swarm_secret_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "task": task_description,
            "project": services_list.split(',')[0].strip(),
        }

        logger.info("Sending recurring error task to agent",
                    url=url, agent=self._swarm_agent_name,
                    count=pattern.count, services=services_list)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    body = await resp.text()
                    if resp.status in (200, 201):
                        logger.info("Recurring error task sent to agent",
                                    agent=self._swarm_agent_name,
                                    fingerprint=pattern.fingerprint,
                                    count=pattern.count,
                                    services=services_list)
                        pattern.notified = True
                        # Add to persistent history (keep last 20)
                        entry = {
                            "fingerprint": pattern.fingerprint,
                            "sample_message": pattern.sample_message,
                            "count": pattern.count,
                            "services": sorted(pattern.services),
                            "first_seen": pattern.first_seen.isoformat(),
                            "last_seen": pattern.last_seen.isoformat(),
                            "notified_at": datetime.utcnow().isoformat(),
                        }
                        # Update existing entry if same fingerprint, else prepend
                        self._notification_history = [
                            e for e in self._notification_history
                            if e["fingerprint"] != pattern.fingerprint
                        ]
                        self._notification_history.insert(0, entry)
                        self._notification_history = self._notification_history[:20]
                    else:
                        logger.error("Failed to send recurring error task",
                                     status=resp.status, url=url, response=body[:500])
        except Exception as e:
            logger.error("Error notifying agent of recurring error",
                         error_type=type(e).__name__, error=str(e))
