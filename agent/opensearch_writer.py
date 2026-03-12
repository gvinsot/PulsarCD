"""OpenSearch writer for agent - writes logs and metrics directly to OpenSearch."""

import hashlib
from datetime import datetime
from typing import Any, Dict, List

import structlog
from opensearchpy import AsyncOpenSearch, helpers

from .config import OpenSearchConfig

logger = structlog.get_logger()


class OpenSearchWriter:
    """Async OpenSearch writer for direct data indexing."""

    def __init__(self, config: OpenSearchConfig):
        self.config = config
        self.logs_index = f"{config.index_prefix}-logs"
        self.metrics_index = f"{config.index_prefix}-metrics"
        self.host_metrics_index = f"{config.index_prefix}-host-metrics"

        auth = None
        if config.username and config.password:
            auth = (config.username, config.password)

        self._client = AsyncOpenSearch(
            hosts=config.hosts,
            http_auth=auth,
            use_ssl="https" in config.hosts[0] if config.hosts else False,
            verify_certs=False,
            ssl_show_warn=False,
        )

    async def initialize(self):
        """Ensure indices exist (create if needed)."""
        await self._ensure_logs_index()
        await self._ensure_metrics_index()
        await self._ensure_host_metrics_index()
        logger.debug("OpenSearch writer initialized")

    async def _ensure_logs_index(self):
        """Create logs index if it doesn't exist."""
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "host": {"type": "keyword"},
                    "container_id": {"type": "keyword"},
                    "container_name": {"type": "keyword"},
                    "compose_project": {"type": "keyword"},
                    "compose_service": {"type": "keyword"},
                    "stream": {"type": "keyword"},
                    "message": {"type": "text", "analyzer": "standard"},
                    "level": {"type": "keyword"},
                    "http_status": {"type": "integer"},
                    "parsed_fields": {"type": "object", "enabled": False},
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.refresh_interval": "5s",
            }
        }

        if not await self._client.indices.exists(index=self.logs_index):
            await self._client.indices.create(index=self.logs_index, body=mapping)
            logger.info("Created logs index", index=self.logs_index)

    async def _ensure_metrics_index(self):
        """Create container metrics index if it doesn't exist."""
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "host": {"type": "keyword"},
                    "container_id": {"type": "keyword"},
                    "container_name": {"type": "keyword"},
                    "cpu_percent": {"type": "float"},
                    "memory_usage_mb": {"type": "float"},
                    "memory_limit_mb": {"type": "float"},
                    "memory_percent": {"type": "float"},
                    "network_rx_bytes": {"type": "long"},
                    "network_tx_bytes": {"type": "long"},
                    "block_read_bytes": {"type": "long"},
                    "block_write_bytes": {"type": "long"},
                    "gpu_percent": {"type": "float"},
                    "gpu_memory_used_mb": {"type": "float"},
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            }
        }

        if not await self._client.indices.exists(index=self.metrics_index):
            await self._client.indices.create(index=self.metrics_index, body=mapping)
            logger.info("Created metrics index", index=self.metrics_index)

    async def _ensure_host_metrics_index(self):
        """Create host metrics index if it doesn't exist."""
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "host": {"type": "keyword"},
                    "cpu_percent": {"type": "float"},
                    "memory_total_mb": {"type": "float"},
                    "memory_used_mb": {"type": "float"},
                    "memory_percent": {"type": "float"},
                    "disk_total_gb": {"type": "float"},
                    "disk_used_gb": {"type": "float"},
                    "disk_percent": {"type": "float"},
                    "gpu_percent": {"type": "float"},
                    "gpu_memory_used_mb": {"type": "float"},
                    "gpu_memory_total_mb": {"type": "float"},
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            }
        }

        if not await self._client.indices.exists(index=self.host_metrics_index):
            await self._client.indices.create(index=self.host_metrics_index, body=mapping)
            logger.info("Created host metrics index", index=self.host_metrics_index)

    async def close(self):
        """Close the client."""
        await self._client.close()

    def _generate_log_id(self, entry: Dict[str, Any]) -> str:
        """Generate unique ID for log entry."""
        timestamp = entry.get("timestamp", "")
        if isinstance(timestamp, datetime):
            timestamp = timestamp.isoformat()
        unique_str = f"{entry.get('host')}:{entry.get('container_id')}:{timestamp}:{entry.get('message', '')[:100]}"
        return hashlib.md5(unique_str.encode()).hexdigest()

    async def index_logs(self, entries: List[Dict[str, Any]]):
        """Bulk index log entries."""
        if not entries:
            return

        actions = []
        for entry in entries:
            doc_id = self._generate_log_id(entry)
            doc = entry.copy()

            # Ensure timestamp is ISO format string
            if isinstance(doc.get("timestamp"), datetime):
                doc["timestamp"] = doc["timestamp"].isoformat()

            actions.append({
                "_index": self.logs_index,
                "_id": doc_id,
                "_source": doc,
            })

        try:
            success, failed = await helpers.async_bulk(
                self._client, actions, raise_on_error=False
            )
            if failed:
                logger.warning("Some logs failed to index", failed=len(failed))
            logger.debug("Indexed logs", count=success)
        except Exception as e:
            logger.error("Failed to index logs", error=str(e))

    async def index_container_stats(self, stats: Dict[str, Any]):
        """Index container statistics."""
        doc = stats.copy()
        timestamp = doc.get("timestamp", datetime.utcnow())
        if isinstance(timestamp, datetime):
            doc["timestamp"] = timestamp.isoformat()

        try:
            await self._client.index(
                index=self.metrics_index,
                body=doc,
            )
        except Exception as e:
            logger.error("Failed to index container stats", error=str(e))

    async def index_host_metrics(self, metrics: Dict[str, Any]):
        """Index host metrics.

        Uses auto-generated IDs to avoid version conflicts when multiple
        writers (agents, backend) index metrics for the same host concurrently.
        """
        doc = metrics.copy()
        timestamp = doc.get("timestamp", datetime.utcnow())
        if isinstance(timestamp, datetime):
            doc["timestamp"] = timestamp.isoformat()

        try:
            await self._client.index(
                index=self.host_metrics_index,
                body=doc,
            )
        except Exception as e:
            logger.error("Failed to index host metrics", error=str(e))
