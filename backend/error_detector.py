"""Recurring error detector using zvec for similarity-based clustering.

Runs as a background task, periodically scanning recent ERROR/FATAL/CRITICAL logs
from OpenSearch, vectorizing them with zvec, and detecting recurring patterns.
When a recurring error pattern is confirmed (N occurrences across M services/containers),
it delegates to the LLM agent for investigation and remediation.
"""

import asyncio
import hashlib
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

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
        except Exception as e:
            _zvec_available = False
            logger.info("zvec not available, using text hashing fallback",
                        error=str(e), error_type=type(e).__name__)
    return _zvec


# Swarm container names: "{stack}_{service}.{slot}.{taskid}"
# e.g. "pulsarcd_agent.1.4sz1iuqpv26b1befmacmsawtr"
_SWARM_CONTAINER_RE = re.compile(r'^(.+?)_[^.]+\.\d+\.\w+$')

# ---------------------------------------------------------------------------
# Text normalization — strip variable parts (timestamps, IDs, hex, IPs, paths)
# so that structurally identical errors produce the same fingerprint.
# ---------------------------------------------------------------------------

_STRIP_PATTERNS = [
    (re.compile(r'\b[0-9a-fA-F]{8,}\b'), '<HEX>'),          # hex IDs
    (re.compile(r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.\d]*Z?\b'), '<TS>'),  # timestamps
    (re.compile(r'\b\d+\.\d+\.\d+\.\d+(:\d+)?\b'), '<IP>'),  # IPs
    (re.compile(r'\b\d+\b'), '<NUM>'),                         # any number (retry counts, timeouts, etc.)
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
    __slots__ = ('fingerprint', 'sample_message', 'services', 'compose_projects',
                 'count', 'first_seen', 'last_seen', 'notified')

    def __init__(self, fingerprint: str, message: str, service: str,
                 compose_project: str = None, timestamp: str = None):
        self.fingerprint = fingerprint
        self.sample_message = message[:500]
        self.services: Set[str] = {service}
        self.compose_projects: Set[str] = {compose_project} if compose_project else set()
        self.count = 1
        ts = self._parse_ts(timestamp)
        self.first_seen = ts
        self.last_seen = ts
        self.notified = False

    @staticmethod
    def _parse_ts(timestamp: str = None) -> datetime:
        """Parse an ISO timestamp string, falling back to utcnow()."""
        if timestamp:
            try:
                # Handle ISO format with or without Z suffix
                clean = timestamp.replace("Z", "+00:00")
                from datetime import timezone
                dt = datetime.fromisoformat(clean)
                return dt.replace(tzinfo=None) if dt.tzinfo else dt
            except (ValueError, TypeError):
                pass
        return datetime.utcnow()

    def add_occurrence(self, service: str, message: str,
                       compose_project: str = None, timestamp: str = None):
        self.count += 1
        ts = self._parse_ts(timestamp)
        if ts < self.first_seen:
            self.first_seen = ts
        if ts > self.last_seen:
            self.last_seen = ts
        self.services.add(service)
        if compose_project:
            self.compose_projects.add(compose_project)
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
      delegates to the LLM agent for investigation; re-notifies at most once per hour

    System errors (scan failures, OpenSearch errors, LLM delegation errors)
    are reported to the LLM agent's system error tracker when available.
    """

    # Messages matching these patterns are PulsarCD's own internal logs
    # and should not be detected as recurring errors (avoids self-detection loops).
    # Messages matching these patterns are PulsarCD's own internal logs
    # and should not be detected as recurring errors (avoids self-detection loops).
    # IMPORTANT: patterns must be specific enough to avoid filtering real app errors.
    # Use anchored phrases from PulsarCD's own structlog events, not broad substrings.
    _SELF_LOG_PATTERNS = [
        re.compile(r'Error detector scan', re.IGNORECASE),
        re.compile(r'Error pattern threshold', re.IGNORECASE),
        re.compile(r'LLM agent (handling|handled|error|skipped)', re.IGNORECASE),
        re.compile(r'Recurring error (detected|handled)', re.IGNORECASE),
        re.compile(r'MCP tool(s| call)', re.IGNORECASE),
        re.compile(r'Log collection (error|loop)', re.IGNORECASE),
        re.compile(r'Metrics collection error', re.IGNORECASE),
        re.compile(r'Node discovery error', re.IGNORECASE),
        re.compile(r'Failed to discover Swarm', re.IGNORECASE),
        re.compile(r'LLM gate (evaluation|decision)', re.IGNORECASE),
        re.compile(r'Pipeline state', re.IGNORECASE),
        re.compile(r'Context compaction', re.IGNORECASE),
        re.compile(r'Failed to poll actions', re.IGNORECASE),
        re.compile(r'zvec (unavailable|not available|loaded|collection)', re.IGNORECASE),
        re.compile(r'Failed to initialize zvec', re.IGNORECASE),
    ]

    def __init__(
        self,
        opensearch_client,
        llm_agent=None,
        github_service=None,
        pipeline_state: Optional[Dict] = None,
        scan_interval: int = 60,
        initial_lookback_hours: int = 12,
        min_occurrences: int = 10,
        pattern_ttl_hours: int = 12,
        zvec_similarity_threshold: float = 0.92,
        zvec_db_path: str = "/tmp/pulsarcd_zvec",
        burst_window_seconds: int = 10,
        exclude_compose_projects: Optional[List[str]] = None,
    ):
        self._opensearch = opensearch_client
        self._llm_agent = llm_agent
        self._github_service = github_service
        self._pipeline_state = pipeline_state
        self._scan_interval = scan_interval
        self._initial_lookback_hours = initial_lookback_hours
        self._min_occurrences = min_occurrences
        self._pattern_ttl_hours = pattern_ttl_hours
        self._similarity_threshold = zvec_similarity_threshold
        self._zvec_db_path = zvec_db_path
        self._burst_window_seconds = burst_window_seconds
        # Compose projects to exclude from error detection (e.g. PulsarCD's own stack)
        self._exclude_projects = exclude_compose_projects or ["pulsarcd"]

        # State
        self._patterns: Dict[str, ErrorPattern] = {}
        # fingerprint -> datetime of last notification; re-notify only after 1 hour
        self._notified_fingerprints: Dict[str, datetime] = {}
        # Persistent history of the last 20 notified patterns (survives the 1h eviction window)
        self._notification_history: List[dict] = []
        self._running = False
        self._last_scan_ts: Optional[datetime] = None
        self._scan_cycle: int = 0
        self._total_errors_found: int = 0
        self._total_notifications: int = 0

        # zvec collection (lazy init)
        self._zvec_collection = None
        self._zvec_dim = 0

    def get_status(self) -> dict:
        """Return detector internal state for diagnostics."""
        patterns_info = []
        for fp, p in sorted(self._patterns.items(), key=lambda x: -x[1].count):
            patterns_info.append({
                "fingerprint": fp,
                "count": p.count,
                "services": sorted(p.services),
                "compose_projects": sorted(p.compose_projects),
                "sample": p.sample_message[:150],
                "first_seen": p.first_seen.isoformat(),
                "last_seen": p.last_seen.isoformat(),
                "notified": p.notified,
            })
        return {
            "running": self._running,
            "scan_cycle": self._scan_cycle,
            "total_errors_found": self._total_errors_found,
            "total_notifications": self._total_notifications,
            "last_scan_ts": self._last_scan_ts.isoformat() if self._last_scan_ts else None,
            "active_patterns": len(self._patterns),
            "patterns": patterns_info[:20],
            "notification_history_count": len(self._notification_history),
            "config": {
                "scan_interval": self._scan_interval,
                "min_occurrences": self._min_occurrences,
                "initial_lookback_hours": self._initial_lookback_hours,
                "pattern_ttl_hours": self._pattern_ttl_hours,
                "exclude_compose_projects": self._exclude_projects,
            },
        }

    async def start(self):
        if self._running:
            return
        self._running = True
        self._init_zvec()
        logger.info("Recurring error detector starting",
                     interval=self._scan_interval,
                     initial_lookback_hours=self._initial_lookback_hours,
                     pattern_ttl_hours=self._pattern_ttl_hours,
                     threshold=self._min_occurrences,
                     zvec_enabled=self._zvec_collection is not None)
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
                if self._llm_agent and hasattr(self._llm_agent, '_report_system_error'):
                    self._llm_agent._report_system_error(
                        "error_detector", type(e).__name__, str(e),
                        action="scan")

            await asyncio.sleep(self._scan_interval)

    async def _scan(self):
        """One scan cycle: fetch new errors, fingerprint, detect patterns."""
        now = datetime.utcnow()
        self._scan_cycle += 1

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

        # Periodic INFO summary every 10 cycles (~10 minutes) for visibility
        if self._scan_cycle % 10 == 0:
            logger.info("Error detector periodic summary",
                        cycle=self._scan_cycle,
                        active_patterns=len(self._patterns),
                        total_errors_found=self._total_errors_found,
                        total_notifications=self._total_notifications,
                        patterns_above_threshold=sum(
                            1 for p in self._patterns.values()
                            if p.count >= self._min_occurrences
                        ),
                        errors_this_cycle=len(errors) if errors else 0,
                        since=since.isoformat())

        if not errors:
            self._last_scan_ts = now
            return

        self._total_errors_found += len(errors)

        # Deduplicate bursts: for each project, drop errors that are within
        # burst_window_seconds of a preceding error on the same project.
        errors = self._deduplicate_bursts(errors)

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
                    svc = self._extract_service_name(occ)
                    cp = occ.get("compose_project")
                    logger.debug("Error occurrence (existing pattern)",
                                 fingerprint=fp[:12], service=svc,
                                 compose_project=cp,
                                 container_name=occ.get("container_name"))
                    self._patterns[fp].add_occurrence(
                        svc, occ["message"], cp, occ.get("timestamp"))
            else:
                first = occurrences[0]
                svc = self._extract_service_name(first)
                cp = first.get("compose_project")
                logger.info("New error pattern detected",
                            fingerprint=fp[:12], service=svc,
                            compose_project=cp,
                            container_name=first.get("container_name"),
                            message_preview=first["message"][:120])
                self._patterns[fp] = ErrorPattern(
                    fp, first["message"], svc, cp, first.get("timestamp"))
                for occ in occurrences[1:]:
                    s = self._extract_service_name(occ)
                    self._patterns[fp].add_occurrence(
                        s, occ["message"], occ.get("compose_project"),
                        occ.get("timestamp"))

        # Check thresholds and notify (with 1-hour cooldown per fingerprint)
        cooldown = timedelta(hours=1)
        for fp, pattern in list(self._patterns.items()):
            if pattern.count >= self._min_occurrences:
                last_notified = self._notified_fingerprints.get(fp)
                if last_notified is None or (now - last_notified) >= cooldown:
                    self._total_notifications += 1
                    logger.info("Error pattern threshold reached, notifying",
                                fingerprint=fp,
                                count=pattern.count,
                                threshold=self._min_occurrences,
                                services=sorted(pattern.services),
                                compose_projects=sorted(pattern.compose_projects),
                                sample=pattern.sample_message[:200])
                    await self._notify_recurring_error(pattern)
                    self._notified_fingerprints[fp] = now
                else:
                    logger.debug("Error pattern above threshold but in cooldown",
                                 fingerprint=fp, count=pattern.count,
                                 next_allowed_in=str(cooldown - (now - last_notified)))

        # Evict patterns not seen within the TTL window
        cutoff = now - timedelta(hours=self._pattern_ttl_hours)
        stale = [fp for fp, p in self._patterns.items() if p.last_seen < cutoff]
        for fp in stale:
            del self._patterns[fp]
            self._notified_fingerprints.pop(fp, None)

        self._last_scan_ts = now

    @staticmethod
    def _extract_service_name(entry: dict) -> str:
        """Extract the most specific service name from a log entry.

        Priority: compose_service > container_name > compose_project.

        Swarm naming conventions handled:
        - compose_service is "stack_service" (e.g. "devops_nginx")
          → strip the stack prefix when compose_project matches
        - container_name is "stack_service.slot.taskid"
          (e.g. "devops_nginx.1.abc123") → extract "service" part
        """
        project = entry.get("compose_project")

        svc = entry.get("compose_service")
        if svc:
            if project and svc.startswith(project + "_"):
                svc = svc[len(project) + 1:]
            return svc

        cname = entry.get("container_name")
        if cname:
            # Swarm container names: "stack_service.slot.taskid"
            # Strip the stack prefix if present
            if project and cname.startswith(project + "_"):
                cname = cname[len(project) + 1:]
            # Strip ".slot.taskid" suffix (e.g. "nginx.1.abc123" → "nginx")
            dot = cname.find(".")
            if dot > 0:
                cname = cname[:dot]
            return cname

        return project or "unknown"

    # ------------------------------------------------------------------
    # Burst deduplication
    # ------------------------------------------------------------------

    def _deduplicate_bursts(self, errors: List[dict]) -> List[dict]:
        """Remove errors that are temporally too close to a preceding error on the same project.

        When a service crashes it can emit dozens of errors within seconds.
        Counting each one separately inflates the pattern count and triggers
        spurious notifications.  We keep only the first error per project within
        each burst_window_seconds sliding window.
        """
        window = timedelta(seconds=self._burst_window_seconds)
        # last accepted timestamp per project key
        last_seen_per_project: Dict[str, datetime] = {}
        result = []

        # Process in chronological order so the first event of a burst wins
        for err in reversed(errors):  # OpenSearch returns desc, reverse → asc
            project = err.get("compose_project") or err.get("container_name", "unknown")
            ts_raw = err.get("timestamp")
            try:
                ts = datetime.fromisoformat(ts_raw) if isinstance(ts_raw, str) else datetime.utcnow()
            except Exception:
                ts = datetime.utcnow()

            last = last_seen_per_project.get(project)
            if last is None or (ts - last) >= window:
                result.append(err)
                last_seen_per_project[project] = ts
            # else: drop — too close to the previous error on this project

        dropped = len(errors) - len(result)
        if dropped:
            logger.debug("Error detector: burst deduplication dropped errors",
                         original=len(errors), kept=len(result), dropped=dropped,
                         window_seconds=self._burst_window_seconds)

        # Restore descending order to match the rest of the pipeline
        result.reverse()
        return result

    # ------------------------------------------------------------------
    # OpenSearch queries
    # ------------------------------------------------------------------

    def _is_self_log(self, message: str) -> bool:
        """Check if a log message is from PulsarCD's own internal operations."""
        for pattern in self._SELF_LOG_PATTERNS:
            if pattern.search(message):
                return True
        return False

    @staticmethod
    def _fixup_compose_project(entry: dict) -> None:
        """Fix compose_project from container_name for Swarm containers.

        When compose files live in a 'devops/' directory, Docker may set
        com.docker.compose.project='devops' instead of the real stack name.
        Swarm container names always include the stack namespace, so we
        can reliably infer the correct value.
        """
        cn = entry.get("container_name", "")
        m = _SWARM_CONTAINER_RE.match(cn)
        if m:
            entry["compose_project"] = m.group(1)

    async def _fetch_recent_errors(self, since: datetime) -> List[dict]:
        """Fetch recent ERROR/FATAL/CRITICAL logs from OpenSearch.

        Excludes PulsarCD's own compose projects and internal log messages
        to avoid self-detection loops.
        """
        try:
            must_not = []
            # Exclude PulsarCD's own stacks
            if self._exclude_projects:
                must_not.append({"terms": {"compose_project": self._exclude_projects}})

            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"level": ["ERROR", "FATAL", "CRITICAL"]}},
                            {"range": {"timestamp": {"gte": since.isoformat()}}},
                        ],
                        "must_not": must_not,
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
            hits = [hit["_source"] for hit in response["hits"]["hits"]]
            total_hits = response["hits"].get("total", {})
            total_count = total_hits.get("value", len(hits)) if isinstance(total_hits, dict) else total_hits

            # Fix compose_project from Swarm container names (handles
            # 'devops' directory-name artifacts before exclusion filtering)
            for h in hits:
                self._fixup_compose_project(h)

            # Re-apply project exclusion after fixup (OpenSearch query only
            # excluded the original compose_project, not the corrected one)
            if self._exclude_projects:
                excl = set(self._exclude_projects)
                hits = [h for h in hits if h.get("compose_project") not in excl]

            # Filter out PulsarCD's own internal log messages by content
            filtered = [h for h in hits if not self._is_self_log(h.get("message", ""))]
            self_filtered = len(hits) - len(filtered)
            if hits:
                # Log compose_project distribution for debugging resolution
                cp_counts: Dict[str, int] = {}
                for h in filtered:
                    cp = h.get("compose_project", "(none)")
                    cp_counts[cp] = cp_counts.get(cp, 0) + 1
                logger.info("Error detector fetched errors from OpenSearch",
                            total_in_index=total_count,
                            returned=len(hits),
                            self_filtered=self_filtered,
                            kept=len(filtered),
                            compose_project_distribution=cp_counts,
                            since=since.isoformat())
            return filtered
        except Exception as e:
            logger.error("Error detector: failed to fetch errors", error=str(e))
            if self._llm_agent and hasattr(self._llm_agent, '_report_system_error'):
                self._llm_agent._report_system_error(
                    "opensearch", type(e).__name__, str(e),
                    action="fetch_errors")
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
                    # Extract fingerprint from doc_id ("err_<fp>_<timestamp>")
                    parts = results[0].id.split('_')
                    existing_fp = parts[1] if len(parts) >= 3 else fp
                    groups.setdefault(existing_fp, []).append(err)
                    continue
            except Exception as e:
                logger.warning("zvec similarity query failed", error=str(e))

            # New pattern — insert into zvec (fp embedded in doc_id for cross-scan lookup)
            doc_id = f"err_{fp}_{int(time.time() * 1000)}"
            try:
                self._zvec_collection.insert([
                    zvec.Doc(id=doc_id, vectors={"embedding": vec})
                ])
            except Exception as e:
                logger.warning("zvec embedding insert failed", error=str(e))

            groups.setdefault(fp, []).append(err)

        return groups

    # ------------------------------------------------------------------
    # QWEN agent notification
    # ------------------------------------------------------------------

    def _resolve_stacks(self, compose_projects: Set[str]) -> List[str]:
        """Resolve Docker compose project names to repo names via pipeline state.

        Docker compose_project values (e.g. "devops") correspond to the
        stack_name stored in each PipelineEntry, not to the repo name key.
        We match compose_project against entry.stack_name to find the
        canonical repo name.
        """
        if not compose_projects:
            logger.debug("_resolve_stacks: no compose_projects to resolve")
            return []
        if not self._pipeline_state:
            logger.warning("_resolve_stacks: no pipeline_state available, returning raw compose_projects",
                           compose_projects=sorted(compose_projects))
            return sorted(compose_projects)
        # Build a lookup: stack_name → repo_name
        # When stack_name is not set, fall back to repo_name.lower()
        # (stacks are named after their GitHub repo in lowercase)
        stack_to_repo = {}
        for repo_name, entry in self._pipeline_state.items():
            sn = getattr(entry, 'stack_name', None)
            if sn:
                stack_to_repo[sn.lower()] = repo_name
            else:
                stack_to_repo[repo_name.lower()] = repo_name
        logger.info("_resolve_stacks: lookup table built",
                    stack_to_repo=stack_to_repo,
                    compose_projects=sorted(compose_projects))
        resolved = []
        for cp in sorted(compose_projects):
            repo = stack_to_repo.get(cp.lower())
            if repo:
                logger.info("_resolve_stacks: matched",
                            compose_project=cp, resolved_repo=repo)
            else:
                logger.warning("_resolve_stacks: no match found",
                               compose_project=cp,
                               available_stacks=list(stack_to_repo.keys()))
            resolved.append(repo if repo else cp)
        return resolved

    async def _notify_recurring_error(self, pattern: ErrorPattern):
        """Record a recurring error in history and delegate to LLM agent."""
        # Always record in history so the dashboard panel is populated
        pattern.notified = True
        logger.info("_notify_recurring_error: resolving stacks",
                    fingerprint=pattern.fingerprint[:12],
                    compose_projects=sorted(pattern.compose_projects),
                    services=sorted(pattern.services),
                    count=pattern.count)
        stacks = self._resolve_stacks(pattern.compose_projects)
        logger.info("_notify_recurring_error: resolved stacks",
                    fingerprint=pattern.fingerprint[:12],
                    input_projects=sorted(pattern.compose_projects),
                    resolved_stacks=stacks)
        entry = {
            "fingerprint": pattern.fingerprint,
            "sample_message": pattern.sample_message,
            "count": pattern.count,
            "services": sorted(pattern.services),
            "stacks": stacks,
            "first_seen": pattern.first_seen.isoformat(),
            "last_seen": pattern.last_seen.isoformat(),
            "notified_at": datetime.utcnow().isoformat(),
            "delivered": None,  # None = not yet attempted, True = success, False = failed
        }
        self._notification_history = [
            e for e in self._notification_history
            if e["fingerprint"] != pattern.fingerprint
        ]
        self._notification_history.insert(0, entry)
        self._notification_history = self._notification_history[:20]

        # Delegate to LLM agent for investigation and action
        if not self._llm_agent:
            logger.debug("LLM agent not configured, skipping recurring error notification",
                         fingerprint=pattern.fingerprint, count=pattern.count)
            return

        try:
            result = await self._llm_agent.handle_recurring_error(pattern, resolved_stacks=stacks)
            if result:
                entry["agent_response"] = result[:2000]
                entry["delivered"] = True
                logger.info("LLM agent handled recurring error",
                            fingerprint=pattern.fingerprint,
                            count=pattern.count)
            else:
                entry["delivered"] = False
        except Exception as e:
            entry["delivered"] = False
            entry["delivery_error"] = str(e)[:200]
            logger.error("LLM agent error for recurring error",
                         fingerprint=pattern.fingerprint,
                         error_type=type(e).__name__, error=str(e))
            if self._llm_agent and hasattr(self._llm_agent, '_report_system_error'):
                self._llm_agent._report_system_error(
                    "agent", type(e).__name__, str(e),
                    action="notify_recurring_error",
                    fingerprint=pattern.fingerprint[:16])