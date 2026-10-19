"""Log, metrics and access-record store on VictoriaLogs.

Agents write container logs, metrics and Traefik access records straight to
VictoriaLogs (see agent/victorialogs_writer.py); the backend writes the same
records when it collects itself, and the pipeline run history
(run_history.py). Everything is appended; retention is VictoriaLogs'
``-retentionPeriod``. This module answers the UI, MCP and AI queries over
that data. Record layout and LogsQL helpers: shared/victorialogs.py.
"""

import asyncio
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any, Dict, List, Optional, Sequence

import structlog

from shared.access_log import build_access_doc
from shared.victorialogs import (
    KIND_ACCESS, KIND_CONTAINER_METRIC, KIND_HOST_METRIC, KIND_LOG, MAX_BUCKETS,
    VictoriaLogsClient, VictoriaLogsError, access_row, any_of, bucket_times, exact,
    integer, interval_seconds, log_row, metric_row, num, parse_time, quote, stream,
    text_filter,
)

from . import security_analytics as security
from . import usage_analytics as usage
from .config import VictoriaLogsConfig
from .models import (
    DashboardStats, LogEntry, LogSearchQuery, LogSearchResult, TimeSeriesByHost,
    TimeSeriesPoint,
)

logger = structlog.get_logger()

# --- Query cost / safety limits ------------------------------------------
# VictoriaLogs is reachable without authentication on the internal network,
# so every query built from user (or AI) input is bounded here as well.
DEFAULT_SEARCH_SIZE = 100
MAX_SEARCH_SIZE = 500          # max hits returned by a single search
MAX_RAW_QUERY_ROWS = 500       # rows returned by a raw MCP/AI LogsQL query
MAX_SEARCH_FROM = 10000        # deepest page offset accepted
MAX_SIMILAR_MESSAGE_LENGTH = 4096
MAX_SEARCH_HOURS = 24 * 365    # max time window accepted from a request body
MAX_RAW_QUERY_HOURS = 24 * 30  # time window of a raw LogsQL query
MAX_HISTOGRAM_BUCKETS = MAX_BUCKETS  # guards bucket-interval abuse
METADATA_WINDOW = timedelta(days=7)

ERROR_LEVELS = ("ERROR", "FATAL", "CRITICAL")

_LOG_FIELDS = ("_time, _msg, host, container_id, container_name, compose_project, "
               "compose_service, stream, level, http_status, parsed_fields")


def _clamp_int(value: Any, minimum: int, maximum: int, default: int) -> int:
    """Coerce a value to an int inside [minimum, maximum], or return default."""
    try:
        result = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(result, maximum))


def _safe_interval(interval: Any, default: str, hours: Any = 24) -> str:
    """Validate a bucket interval coming from a request parameter.

    Rejects anything that is not a plain "<number><unit>" value, and any
    interval that would generate more than MAX_HISTOGRAM_BUCKETS buckets over
    the requested window.
    """
    try:
        window_seconds = int(hours) * 3600
    except (TypeError, ValueError):
        window_seconds = 24 * 3600

    seconds = interval_seconds(interval) if isinstance(interval, str) else None
    if seconds and window_seconds / seconds <= MAX_HISTOGRAM_BUCKETS:
        return interval.strip()

    logger.warning("Rejected invalid histogram interval",
                   interval=str(interval)[:32], fallback=default)
    return default


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _log_id(host: str, container_id: str, timestamp: datetime, message: str) -> str:
    unique = f"{host}:{container_id}:{timestamp.isoformat()}:{message[:100]}"
    return hashlib.md5(unique.encode()).hexdigest()


def row_to_entry(row: Dict[str, Any]) -> LogEntry:
    """LogEntry from a ``kind=log`` row."""
    timestamp = parse_time(row.get("_time")) or _now()
    message = row.get("_msg") or ""
    try:
        parsed_fields = json.loads(row["parsed_fields"]) if row.get("parsed_fields") else {}
    except (TypeError, ValueError):
        parsed_fields = {}
    status = num(row.get("http_status"))
    return LogEntry(
        id=_log_id(row.get("host", ""), row.get("container_id", ""), timestamp, message),
        timestamp=timestamp,
        host=row.get("host") or "",
        container_id=row.get("container_id") or "",
        container_name=row.get("container_name") or "",
        compose_project=row.get("compose_project") or None,
        compose_service=row.get("compose_service") or None,
        stream=row.get("stream") or "stdout",
        message=message,
        level=row.get("level") or None,
        http_status=int(status) if status is not None else None,
        parsed_fields=parsed_fields if isinstance(parsed_fields, dict) else {},
    )


def log_filters(query: LogSearchQuery) -> str:
    """LogsQL filter for a log search (time range excluded: start/end args)."""
    parts = [stream(KIND_LOG)]
    if query.hosts:
        parts.append(any_of("host", query.hosts))
    if query.containers:
        parts.append(any_of("container_name", query.containers))
    if query.compose_projects:
        parts.append(any_of("compose_project", query.compose_projects))
    # Filtered in the query, not on the returned page: total and the
    # aggregations must count the same rows the hits come from.
    if query.compose_services:
        parts.append(any_of("compose_service", query.compose_services))
    if query.levels:
        parts.append(any_of("level", query.levels))
    if query.http_status_min is not None or query.http_status_max is not None:
        low = int(query.http_status_min) if query.http_status_min is not None else "-Inf"
        high = int(query.http_status_max) if query.http_status_max is not None else "Inf"
        parts.append(f"http_status:range[{low}, {high}]")
    text = text_filter(query.query)
    if text:
        parts.append(text)
    return " ".join(parts)


def _round(value: Any, digits: int = 1, default: Optional[float] = None) -> Optional[float]:
    result = num(value)
    return round(result, digits) if result is not None else default


def _similar_words(message: str) -> List[str]:
    """Key words of a log message, without its variable parts."""
    simplified = message
    # Remove ISO timestamps (2024-01-17T15:30:00.000Z)
    simplified = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.\d]*Z?', '', simplified)
    # Remove simple timestamps (15:30:00, 15:30:00.123)
    simplified = re.sub(r'\b\d{2}:\d{2}:\d{2}[.\d]*\b', '', simplified)
    # Remove dates (2024-01-17, 17/01/2024)
    simplified = re.sub(r'\b\d{4}[-/]\d{2}[-/]\d{2}\b', '', simplified)
    simplified = re.sub(r'\b\d{2}[-/]\d{2}[-/]\d{4}\b', '', simplified)
    # Remove UUIDs
    simplified = re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '', simplified, flags=re.I)
    # Remove hex strings (container IDs, hashes)
    simplified = re.sub(r'\b[0-9a-f]{12,}\b', '', simplified, flags=re.I)
    # Remove IPs
    simplified = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '', simplified)
    # Remove standalone numbers
    simplified = re.sub(r'\b\d+\b', '', simplified)
    # Remove ALL special characters - keep only alphanumeric and spaces
    simplified = re.sub(r'[^a-zA-Z0-9\s]', ' ', simplified)
    stop_words = {
        'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'her', 'was',
        'one', 'our', 'out', 'http', 'https', 'info', 'get', 'post', 'put', 'delete',
        'from', 'has', 'been', 'moved', 'will', 'that', 'this', 'with', 'have', 'your',
        'usr', 'local', 'lib', 'python', 'site', 'packages'  # Filter out path components
    }
    return [w.lower() for w in simplified.split() if len(w) >= 3 and w.lower() not in stop_words]


def similar_filter(words: Sequence[str]) -> Optional[str]:
    """Logs containing at least half (minimum 2) of the first six words."""
    key_words = list(dict.fromkeys(words))[:6]
    if len(key_words) < 2:
        return None
    needed = max(2, len(key_words) // 2)
    groups = [
        "(" + " AND ".join(f"_msg:i({quote(word)})" for word in group) + ")"
        for group in combinations(key_words, needed)
    ]
    return "(" + " OR ".join(groups) + ")"


class LogStore:
    """VictoriaLogs-backed store for logs, metrics and access records."""

    def __init__(self, config: VictoriaLogsConfig):
        self.config = config
        self.client = VictoriaLogsClient(
            config.url, timeout=config.timeout_seconds,
            username=config.username, password=config.password)

    async def initialize(self) -> None:
        """Fail when VictoriaLogs is unreachable (the caller retries)."""
        if not await self.client.health():
            raise VictoriaLogsError(f"VictoriaLogs not reachable at {self.config.url}")
        logger.warning("VictoriaLogs connection OK", url=self.config.url)

    async def close(self) -> None:
        await self.client.close()

    async def health(self) -> bool:
        return await self.client.health()

    async def _rows(self, query: str, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self.client.query(query, **kwargs)

    async def _run_queries(self, queries: Dict[str, str], **kwargs: Any) -> Dict[str, List[Dict[str, Any]]]:
        names = list(queries)
        results = await asyncio.gather(*(self._rows(queries[name], **kwargs) for name in names))
        return dict(zip(names, results))

    # ── Writes ──

    async def index_logs(self, entries: Sequence[Any]) -> int:
        """Append log lines; Traefik requests and WAF blocks become access records too."""
        if not entries:
            return 0
        docs = [e.model_dump() if hasattr(e, "model_dump") else dict(e) for e in entries]
        access = [access_row(doc) for doc in filter(None, (build_access_doc(d) for d in docs))]
        try:
            written = await self.client.insert([log_row(d) for d in docs], KIND_LOG)
            if access:
                await self.client.insert(access, KIND_ACCESS)
            return written
        except VictoriaLogsError as e:
            logger.error("Failed to index logs", error=str(e))
            return 0

    async def index_container_stats(self, stats: Any) -> None:
        await self.index_container_stats_batch([stats])

    async def index_container_stats_batch(self, stats: Sequence[Any]) -> None:
        rows = [metric_row(KIND_CONTAINER_METRIC, s.model_dump() if hasattr(s, "model_dump") else dict(s))
                for s in stats]
        try:
            await self.client.insert(rows, KIND_CONTAINER_METRIC)
        except VictoriaLogsError as e:
            logger.error("Failed to index container stats", error=str(e))

    async def index_host_metrics(self, metrics: Any) -> None:
        doc = metrics.model_dump() if hasattr(metrics, "model_dump") else dict(metrics)
        try:
            await self.client.insert([metric_row(KIND_HOST_METRIC, doc)], KIND_HOST_METRIC)
        except VictoriaLogsError as e:
            logger.error("Failed to index host metrics", error=str(e))

    # ── Latest metrics ──

    async def get_latest_container_stats(self) -> Dict[str, Dict[str, Any]]:
        """Latest stats of every container seen in the last 5 minutes.

        Returns {container_id: {cpu_percent, memory_percent, memory_usage_mb, ...}}.
        """
        query = (f"{stream(KIND_CONTAINER_METRIC)} | last 1 by (_time) partition by (container_id) "
                 "| fields container_id, cpu_percent, memory_percent, memory_usage_mb, "
                 "gpu_percent, gpu_memory_used_mb")
        try:
            rows = await self._rows(query, start=_now() - timedelta(minutes=5))
        except VictoriaLogsError as e:
            logger.error("Failed to get latest container stats", error=str(e))
            return {}
        return {
            row["container_id"]: {
                "cpu_percent": _round(row.get("cpu_percent"), default=0.0),
                "memory_percent": _round(row.get("memory_percent"), default=0.0),
                "memory_usage_mb": _round(row.get("memory_usage_mb"), default=0.0),
                "gpu_percent": _round(row.get("gpu_percent")),
                "gpu_memory_used_mb": _round(row.get("gpu_memory_used_mb")),
            }
            for row in rows if row.get("container_id")
        }

    async def get_latest_stats_for_container(self, container_id: str) -> Optional[Dict[str, Any]]:
        """Latest stats of one container (last 5 minutes), or None."""
        query = (f"{stream(KIND_CONTAINER_METRIC)} {exact('container_id', container_id)} "
                 "| fields _time, cpu_percent, memory_percent, memory_usage_mb, memory_limit_mb, "
                 "network_rx_bytes, network_tx_bytes, block_read_bytes, block_write_bytes")
        try:
            rows = await self._rows(query, start=_now() - timedelta(minutes=5), limit=1)
        except VictoriaLogsError as e:
            logger.error("Failed to get latest stats for container", container_id=container_id, error=str(e))
            return None
        if not rows:
            return None
        row = rows[0]
        return {
            "cpu_percent": num(row.get("cpu_percent")) or 0,
            "memory_percent": num(row.get("memory_percent")) or 0,
            "memory_usage_mb": num(row.get("memory_usage_mb")) or 0,
            "memory_limit_mb": num(row.get("memory_limit_mb")) or 0,
            "network_rx_bytes": integer(row.get("network_rx_bytes")),
            "network_tx_bytes": integer(row.get("network_tx_bytes")),
            "block_read_bytes": integer(row.get("block_read_bytes")),
            "block_write_bytes": integer(row.get("block_write_bytes")),
        }

    async def get_latest_host_metrics(self, host_name: str) -> Optional[Dict[str, Any]]:
        """Latest metrics of a host reported within the last day, or None."""
        try:
            rows = await self._rows(stream(KIND_HOST_METRIC, host=host_name),
                                    start=_now() - timedelta(days=1), limit=1)
        except VictoriaLogsError as e:
            logger.error("Failed to get latest host metrics", host=host_name, error=str(e))
            return None
        if not rows:
            return None
        row = rows[0]
        return {
            "cpu_percent": _round(row.get("cpu_percent"), default=0.0),
            "memory_percent": _round(row.get("memory_percent"), default=0.0),
            "memory_used_mb": _round(row.get("memory_used_mb"), default=0.0),
            "memory_total_mb": _round(row.get("memory_total_mb"), default=0.0),
            "gpu_percent": _round(row.get("gpu_percent")),
            "gpu_memory_used_mb": _round(row.get("gpu_memory_used_mb")),
            "gpu_memory_total_mb": _round(row.get("gpu_memory_total_mb")),
            "disk_total_gb": _round(row.get("disk_total_gb"), default=0.0),
            "disk_used_gb": _round(row.get("disk_used_gb"), default=0.0),
            "disk_percent": _round(row.get("disk_percent"), default=0.0),
            "timestamp": row.get("_time"),
        }

    # ── Log search ──

    async def search_logs(self, query: LogSearchQuery) -> LogSearchResult:
        """Search logs with filters: one page of hits, the total and facets."""
        filters = log_filters(query)
        # Cap the requested page here: LogSearchQuery is also built directly
        # by the MCP tools and internal callers.
        size = _clamp_int(query.size, 0, MAX_SEARCH_SIZE, DEFAULT_SEARCH_SIZE)
        from_ = _clamp_int(query.from_, 0, max(MAX_SEARCH_FROM - size, 0), 0)
        order = "desc" if query.sort_order != "asc" else ""
        window = {"start": query.start_time, "end": query.end_time}

        queries = {"total": f"{filters} | stats count() as n"}
        for name, field_name, limit in (("levels", "level", 10), ("hosts", "host", 50),
                                        ("containers", "container_name", 100),
                                        ("compose_projects", "compose_project", 100)):
            queries[name] = (f"{filters} {field_name}:* | stats by ({field_name}) count() as n "
                             f"| sort by (n desc) limit {limit}")
        if size:
            queries["hits"] = (f"{filters} | sort by (_time {order}) offset {from_} limit {size} "
                               f"| fields {_LOG_FIELDS}")
        try:
            results = await self._run_queries(queries, **window)
        except VictoriaLogsError as e:
            logger.error("Log search failed", error=str(e), query_snippet=filters[:300])
            return LogSearchResult(total=0, hits=[], aggregations={})

        aggregations = {}
        for name, field_name in (("levels", "level"), ("hosts", "host"),
                                 ("containers", "container_name"),
                                 ("compose_projects", "compose_project")):
            aggregations[name] = [{"key": row.get(field_name), "count": integer(row.get("n"))}
                                  for row in results[name]]
        total_rows = results["total"]
        return LogSearchResult(
            total=integer(total_rows[0].get("n")) if total_rows else 0,
            hits=[row_to_entry(row) for row in results.get("hits", [])],
            aggregations=aggregations,
        )

    async def count_similar_logs(self, message: str, container_name: str = "", hours: int = 24) -> int:
        """Count log messages similar to ``message`` over the last N hours.

        Similar: at least half (minimum 2) of its first six key words, once
        timestamps, ids, numbers and punctuation are removed.
        """
        # Both the message and the time window come straight from a request body.
        message = str(message)[:MAX_SIMILAR_MESSAGE_LENGTH]
        hours = _clamp_int(hours, 1, MAX_SEARCH_HOURS, 24)
        words_filter = similar_filter(_similar_words(message))
        if words_filter is None:
            return 0
        parts = [stream(KIND_LOG), words_filter]
        if container_name:
            parts.append(exact("container_name", container_name))
        try:
            rows = await self._rows(" ".join(parts) + " | stats count() as n",
                                    start=_now() - timedelta(hours=hours))
        except VictoriaLogsError as e:
            logger.error("Failed to count similar logs", error=str(e))
            return 0
        return integer(rows[0].get("n")) if rows else 0

    async def get_available_metadata(self) -> Dict[str, Any]:
        """Hosts, containers, compose projects/services and levels seen recently."""
        fields = {"hosts": ("host", 50), "containers": ("container_name", 200),
                  "compose_projects": ("compose_project", 50),
                  "compose_services": ("compose_service", 200), "levels": ("level", 10)}
        queries = {
            name: (f"{stream(KIND_LOG)} {field_name}:* | stats by ({field_name}) count() as n "
                   f"| sort by (n desc) limit {limit}")
            for name, (field_name, limit) in fields.items()
        }
        try:
            results = await self._run_queries(queries, start=_now() - METADATA_WINDOW)
        except VictoriaLogsError as e:
            error_msg = str(e)[:300]
            logger.error("Failed to get metadata for AI", error=error_msg)
            return {**{name: [] for name in fields}, "error": error_msg}
        return {name: [row.get(fields[name][0]) for row in rows] for name, rows in results.items()}

    async def run_logsql(self, query: str, *, start: Optional[datetime] = None,
                         end: Optional[datetime] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Run a caller-supplied LogsQL query (MCP tools) over the container logs.

        The query is confined to ``kind=log`` records by an extra stream
        filter VictoriaLogs applies to it and every subquery, whatever it
        says; its time window, row count and duration are bounded here.
        """
        end = end or _now()
        start = start or end - timedelta(hours=24)
        start = max(start, end - timedelta(hours=MAX_RAW_QUERY_HOURS))
        limit = _clamp_int(limit, 1, MAX_RAW_QUERY_ROWS, 100)
        rows: List[Dict[str, Any]] = []
        async for row in self.client.iter_query(query, start=start, end=end,
                                                extra_stream_filters={"kind": KIND_LOG}):
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    # ── Dashboard ──

    async def get_dashboard_stats(self) -> DashboardStats:
        """Error / HTTP counts over 24 h and average host usage over the last hour."""
        now = _now()
        logs_query = (
            f"{stream(KIND_LOG)} | stats "
            f"count() if ({any_of('level', ERROR_LEVELS)}) as errors, "
            f"count() if (level:=WARN) as warnings, "
            f"count() if (http_status:range[400, 500)) as http_4xx, "
            f"count() if (http_status:range[500, 600)) as http_5xx"
        )
        try:
            rows = await self._rows(logs_query, start=now - timedelta(days=1))
            counts = rows[0] if rows else {}
        except VictoriaLogsError as e:
            logger.error("Dashboard stats: logs query failed", error=str(e))
            counts = {}

        metrics_query = (
            f"{stream(KIND_HOST_METRIC)} | stats avg(cpu_percent) as cpu, avg(memory_percent) as memory, "
            "avg(gpu_percent) as gpu, avg(gpu_memory_used_mb) as vram_used, "
            "avg(gpu_memory_total_mb) as vram_total"
        )
        try:
            rows = await self._rows(metrics_query, start=now - timedelta(hours=1))
            averages = rows[0] if rows else {}
        except VictoriaLogsError as e:
            logger.error("Dashboard stats: host metrics query failed", error=str(e))
            averages = {}

        return DashboardStats(
            total_containers=0,  # Filled by the API
            running_containers=0,
            total_hosts=0,
            healthy_hosts=0,
            errors_24h=integer(counts.get("errors")),
            warnings_24h=integer(counts.get("warnings")),
            http_4xx_24h=integer(counts.get("http_4xx")),
            http_5xx_24h=integer(counts.get("http_5xx")),
            avg_cpu_percent=_round(averages.get("cpu"), 2, 0.0),
            avg_memory_percent=_round(averages.get("memory"), 2, 0.0),
            avg_gpu_percent=_round(averages.get("gpu"), 2),
            avg_vram_used_mb=_round(averages.get("vram_used"), 1),
            avg_vram_total_mb=_round(averages.get("vram_total"), 1),
        )

    async def _count_series(self, filters: str, hours: int, interval: str) -> List[TimeSeriesPoint]:
        """Zero-filled per-bucket counts of the rows matching ``filters``."""
        end = _now()
        start = end - timedelta(hours=hours)
        rows = await self._rows(f"{filters} | stats by (_time:{interval}) count() as n",
                                start=start, end=end)
        counts = {parse_time(row.get("_time")): integer(row.get("n")) for row in rows}
        return [TimeSeriesPoint(timestamp=t, value=counts.get(t, 0))
                for t in bucket_times(start, end, interval)]

    async def get_error_timeseries(self, hours: int = 24, interval: str = "1h") -> List[TimeSeriesPoint]:
        """Error count time series."""
        interval = _safe_interval(interval, "1h", hours)
        try:
            return await self._count_series(
                f"{stream(KIND_LOG)} {any_of('level', ERROR_LEVELS)}", hours, interval)
        except VictoriaLogsError as e:
            logger.error("Failed to get error timeseries", error=str(e))
            return []

    async def get_http_requests_timeseries(self, hours: int = 24, interval: str = "1h") -> List[TimeSeriesPoint]:
        """HTTP request count time series (any log line with an http_status)."""
        interval = _safe_interval(interval, "1h", hours)
        try:
            return await self._count_series(f"{stream(KIND_LOG)} http_status:*", hours, interval)
        except VictoriaLogsError as e:
            logger.error("Failed to get HTTP requests timeseries", error=str(e))
            return []

    async def get_http_status_timeseries(self, status_min: int, status_max: int, hours: int = 24,
                                         interval: str = "1h") -> List[TimeSeriesPoint]:
        """Count time series of HTTP statuses in [status_min, status_max)."""
        interval = _safe_interval(interval, "1h", hours)
        try:
            return await self._count_series(
                f"{stream(KIND_LOG)} http_status:range[{int(status_min)}, {int(status_max)})",
                hours, interval)
        except VictoriaLogsError as e:
            logger.error("Failed to get HTTP status timeseries", error=str(e))
            return []

    async def get_resource_timeseries(self, metric: str, hours: int = 24, interval: str = "15m") -> List[TimeSeriesPoint]:
        """Average host usage time series (cpu_percent, memory_percent or gpu_percent)."""
        interval = _safe_interval(interval, "15m", hours)
        end = _now()
        try:
            rows = await self._rows(
                f"{stream(KIND_HOST_METRIC)} | stats by (_time:{interval}) avg({_metric(metric)}) as v "
                "| sort by (_time)", start=end - timedelta(hours=hours), end=end)
        except VictoriaLogsError as e:
            logger.error("Failed to get resource timeseries", error=str(e))
            return []
        return [TimeSeriesPoint(timestamp=parse_time(row["_time"]), value=_round(row.get("v"), 2, 0.0))
                for row in rows]

    async def get_resource_timeseries_by_host(self, metric: str, hours: int = 24,
                                              interval: str = "15m") -> List[TimeSeriesByHost]:
        """Host usage time series, one per host."""
        interval = _safe_interval(interval, "15m", hours)
        end = _now()
        try:
            rows = await self._rows(
                f"{stream(KIND_HOST_METRIC)} | stats by (host, _time:{interval}) avg({_metric(metric)}) as v "
                "| sort by (host, _time)", start=end - timedelta(hours=hours), end=end)
        except VictoriaLogsError as e:
            logger.error("Failed to get resource timeseries by host", metric=metric, error=str(e))
            return []
        by_host: Dict[str, List[TimeSeriesPoint]] = {}
        for row in rows:
            by_host.setdefault(row.get("host", ""), []).append(
                TimeSeriesPoint(timestamp=parse_time(row["_time"]), value=_round(row.get("v"), 2, 0.0)))
        return [TimeSeriesByHost(host=host, data=data) for host, data in by_host.items()]

    async def get_vram_percent_timeseries_by_host(self, hours: int = 24,
                                                  interval: str = "15m") -> List[TimeSeriesByHost]:
        """VRAM usage % per host: average used / average total."""
        interval = _safe_interval(interval, "15m", hours)
        end = _now()
        try:
            rows = await self._rows(
                f"{stream(KIND_HOST_METRIC)} gpu_memory_total_mb:>0 "
                f"| stats by (host, _time:{interval}) avg(gpu_memory_used_mb) as used, "
                "avg(gpu_memory_total_mb) as total | sort by (host, _time)",
                start=end - timedelta(hours=hours), end=end)
        except VictoriaLogsError as e:
            logger.error("Failed to get VRAM timeseries by host", error=str(e))
            return []
        by_host: Dict[str, List[TimeSeriesPoint]] = {}
        for row in rows:
            used, total = num(row.get("used")) or 0, num(row.get("total")) or 0
            by_host.setdefault(row.get("host", ""), []).append(TimeSeriesPoint(
                timestamp=parse_time(row["_time"]),
                value=round(used / total * 100, 2) if total > 0 else 0))
        return [TimeSeriesByHost(host=host, data=data) for host, data in by_host.items()]

    async def get_container_metrics_timeseries(
        self, container_id: str, hours: int = 168, interval: str = "1h"
    ) -> Dict[str, List[Dict]]:
        """CPU %, memory % and error count time series of one container."""
        interval = _safe_interval(interval, "1h", hours)
        end = _now()
        window = {"start": end - timedelta(hours=hours), "end": end}
        container = exact("container_id", container_id)
        cpu_points: List[Dict] = []
        memory_points: List[Dict] = []
        error_points: List[Dict] = []
        try:
            rows = await self._rows(
                f"{stream(KIND_CONTAINER_METRIC)} {container} | stats by (_time:{interval}) "
                "avg(cpu_percent) as cpu, avg(memory_percent) as mem | sort by (_time)", **window)
            for row in rows:
                cpu_points.append({"timestamp": row["_time"], "value": _round(row.get("cpu"), 2, 0.0)})
                memory_points.append({"timestamp": row["_time"], "value": _round(row.get("mem"), 2, 0.0)})
        except VictoriaLogsError as e:
            logger.error("Failed to get container CPU/memory timeseries", container_id=container_id, error=str(e))
        try:
            rows = await self._rows(
                f"{stream(KIND_LOG)} {container} {any_of('level', ERROR_LEVELS)} "
                f"| stats by (_time:{interval}) count() as n | sort by (_time)", **window)
            error_points = [{"timestamp": row["_time"], "value": integer(row.get("n"))} for row in rows]
        except VictoriaLogsError as e:
            logger.error("Failed to get container error timeseries", container_id=container_id, error=str(e))
        return {"cpu": cpu_points, "memory": memory_points, "errors": error_points}

    async def get_error_counts_by_service(self, hours: int = 24) -> Dict[str, Any]:
        """Error/warning counts per compose_project for the last N hours."""
        levels = ERROR_LEVELS + ("WARN", "WARNING")
        try:
            rows = await self._rows(
                f"{stream(KIND_LOG)} {any_of('level', levels)} "
                "| stats by (compose_project, level) count() as n",
                start=_now() - timedelta(hours=hours))
        except VictoriaLogsError as e:
            logger.error("Failed to get error counts by service", error=str(e))
            return {"hours": hours, "services": []}
        projects: Dict[Optional[str], Dict[str, int]] = {}
        for row in rows:
            entry = projects.setdefault(row.get("compose_project") or None,
                                        {"errors": 0, "warnings": 0, "total": 0})
            count = integer(row.get("n"))
            entry["total"] += count
            entry["errors" if row.get("level") in ERROR_LEVELS else "warnings"] += count
        result = [{"compose_project": project, **counts} for project, counts in projects.items()]
        result.sort(key=lambda x: x["errors"] + x["warnings"], reverse=True)
        return {"hours": hours, "services": result}

    # ── Security & usage (access records) ──

    async def get_security_overview(self, minutes: int = security.DEFAULT_WINDOW_MINUTES,
                                    include_internal: bool = False) -> Dict[str, Any]:
        """Request volume, suspicious client IPs and hot endpoints (security view)."""
        minutes = security.clamp_window(minutes)
        now = _now()
        window = {"start": now - timedelta(minutes=minutes), "end": now}
        try:
            overview = await self._run_queries(
                security.build_overview_queries(minutes, include_internal), **window)
            details = await self._run_queries(
                security.build_detail_queries(minutes, include_internal, overview), **window)
            return security.summarize(overview, details, minutes=minutes,
                                      include_internal=include_internal, start=window["start"],
                                      end=now, generated_at=now)
        except VictoriaLogsError as e:
            logger.error("Failed to get security overview", error=str(e))
            return security.empty_overview(minutes, include_internal, now,
                                           error="Access data unavailable")

    async def get_security_ip_events(self, ip: str,
                                     minutes: int = security.DEFAULT_WINDOW_MINUTES) -> List[Dict[str, Any]]:
        """Latest requests and WAF blocks of one client IP, newest first."""
        start = _now() - timedelta(minutes=security.clamp_window(minutes))
        try:
            rows = await self._rows(security.build_ip_events_query(ip), start=start,
                                    limit=security.MAX_IP_EVENTS)
        except VictoriaLogsError as e:
            logger.error("Failed to get security IP events", ip=ip, error=str(e))
            return []
        return [security.event_from_row(row) for row in rows]

    async def get_usage_overview(self, minutes: int = usage.DEFAULT_WINDOW_MINUTES,
                                 include_internal: bool = False,
                                 stack: Optional[str] = None,
                                 router_stacks: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Endpoint, host and per-stack traffic statistics (usage view).

        router_stacks maps a Traefik router to the stack declaring it; it names
        the stacks that can be selected and attributes each request to one.
        Without it the view still works, with every row unattributed.
        """
        minutes = usage.clamp_window(minutes)
        now = _now()
        start = now - timedelta(minutes=minutes)
        router_stacks = router_stacks or {}
        stacks = sorted(set(router_stacks.values()))
        # A stack the map does not know has no router, so it gets an empty
        # filter: no request rather than every request.
        router_names = None
        if stack:
            router_names = [name for name, owner in router_stacks.items() if owner == stack]
        try:
            results = await self._run_queries(
                usage.build_usage_queries(minutes, include_internal, router_names),
                start=start, end=now)
            return usage.summarize(results, minutes=minutes, include_internal=include_internal,
                                   stack=stack, stacks=stacks, router_stacks=router_stacks,
                                   start=start, end=now, generated_at=now)
        except VictoriaLogsError as e:
            logger.error("Failed to get usage overview", error=str(e))
            return usage.empty_overview(minutes, include_internal, stack, stacks, now,
                                        error="Access data unavailable")

    # ── Diagnostics ──

    async def probe(self) -> Dict[str, Any]:
        """End-to-end write / flush / read check (admin diagnostics)."""
        return await self.client.probe(host="backend")

    async def status(self) -> Dict[str, Any]:
        """Reachability and per-kind volume over the last 24 hours (admin view)."""
        result: Dict[str, Any] = {"url": self.config.url, "reachable": await self.client.health()}
        if not result["reachable"]:
            return result
        try:
            rows = await self._rows("* | stats by (kind) count() as n, max(_time) as latest | sort by (kind)",
                                    start=_now() - timedelta(days=1))
            result["kinds"] = [{"kind": row.get("kind"), "records_24h": integer(row.get("n")),
                                "latest": row.get("latest")} for row in rows]
        except VictoriaLogsError as e:
            result["error"] = str(e)[:300]
        return result


def _metric(name: str) -> str:
    """Only the host metrics the dashboard charts may be averaged."""
    allowed = {"cpu_percent", "memory_percent", "gpu_percent", "disk_percent"}
    if name not in allowed:
        raise ValueError(f"Unknown metric: {name}")
    return name


