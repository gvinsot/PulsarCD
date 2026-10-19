"""VictoriaLogs writer for the agent: logs, access records and metrics.

Records are appended (VictoriaLogs has no document ids), so the agent must
not send a line twice: the collector drops lines it already sent, and at
startup it resumes after the newest line VictoriaLogs holds for each
container (see latest_log_times).
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import structlog

from shared.access_log import build_access_doc
from shared.victorialogs import (
    KIND_ACCESS, KIND_CONTAINER_METRIC, KIND_HOST_METRIC, KIND_LOG,
    VictoriaLogsClient, VictoriaLogsError, access_row, integer, log_row, metric_row,
    parse_time, stream,
)

from .config import VictoriaLogsConfig

logger = structlog.get_logger()

# How far back a restarting agent looks for the last line it sent.
RESUME_WINDOW = timedelta(days=7)


class VictoriaLogsWriter:
    """Writes this host's records to VictoriaLogs."""

    def __init__(self, config: VictoriaLogsConfig, host_name: str):
        self.config = config
        self.host_name = host_name
        self.client = VictoriaLogsClient(config.url, username=config.username,
                                         password=config.password)
        self._write_count = 0

    async def initialize(self) -> None:
        """Fail when VictoriaLogs is unreachable (the caller retries)."""
        if not await self.client.health():
            raise VictoriaLogsError(f"VictoriaLogs not reachable at {self.config.url}")
        logger.warning("Agent VictoriaLogs connection OK", url=self.config.url)

    async def close(self) -> None:
        await self.client.close()

    async def self_test(self) -> bool:
        """Write a self-test record, make it searchable and read it back."""
        result = await self.client.probe(self.host_name)
        if result.get("ok"):
            logger.warning("Self-test OK: VictoriaLogs write and read work", url=self.config.url)
        else:
            logger.critical("SELF-TEST FAILED — VictoriaLogs pipeline is broken!",
                            url=self.config.url, result=result)
        return bool(result.get("ok"))

    async def latest_log_times(self) -> Dict[str, datetime]:
        """Newest stored line per container of this host (naive UTC).

        Lets a restarted agent resume where it stopped instead of re-sending
        the tail of every container log.
        """
        rows = await self.client.query(
            f"{stream(KIND_LOG, host=self.host_name)} | stats by (container_id) max(_time) as latest",
            start=datetime.now(timezone.utc) - RESUME_WINDOW)
        out = {}
        for row in rows:
            latest = parse_time(row.get("latest"))
            if row.get("container_id") and latest:
                out[row["container_id"]] = latest.astimezone(timezone.utc).replace(tzinfo=None)
        return out

    async def count_recent_logs(self, minutes: int = 5) -> int:
        """Lines this host stored over the last minutes (-1 when unknown)."""
        try:
            rows = await self.client.query(
                f"{stream(KIND_LOG, host=self.host_name)} | stats count() as n",
                start=datetime.now(timezone.utc) - timedelta(minutes=minutes))
            return integer(rows[0].get("n")) if rows else 0
        except VictoriaLogsError:
            return -1

    async def index_logs(self, entries: List[Dict[str, Any]]) -> int:
        """Append log lines; Traefik requests and WAF blocks also become access records."""
        if not entries:
            return 0
        access = [access_row(doc) for doc in filter(None, (build_access_doc(e) for e in entries))]
        try:
            written = await self.client.insert([log_row(e) for e in entries], KIND_LOG)
        except VictoriaLogsError as e:
            logger.error("Failed to index logs", error=str(e))
            return 0
        if access:
            try:
                await self.client.insert(access, KIND_ACCESS)
            except VictoriaLogsError as e:
                logger.error("Failed to index access records", error=str(e))
        self._write_count += 1
        if self._write_count % 50 == 0:
            logger.warning("[periodic] Indexed logs", write_num=self._write_count, count=written,
                           access=len(access))
        return written

    async def index_metrics(self, host_metrics: Dict[str, Any],
                            container_stats: List[Dict[str, Any]]) -> None:
        """Append one host sample and every container's sample, two requests in all."""
        try:
            await self.client.insert([metric_row(KIND_HOST_METRIC, host_metrics)], KIND_HOST_METRIC)
            if container_stats:
                await self.client.insert(
                    [metric_row(KIND_CONTAINER_METRIC, stats) for stats in container_stats],
                    KIND_CONTAINER_METRIC)
        except VictoriaLogsError as e:
            logger.error("Failed to index metrics", error=str(e))
