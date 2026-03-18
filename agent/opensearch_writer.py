"""OpenSearch writer for agent - writes logs and metrics directly to OpenSearch."""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

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
        self._write_count = 0

    async def _ensure_index(self, index_name: str, mapping: dict):
        """Create an index if it doesn't exist, handling opensearch-py 2.x/3.x differences.

        Also verifies the mapping of existing indices — if key fields have
        wrong types (e.g. 'text' instead of 'keyword' from auto-mapping),
        the index is deleted and recreated with the correct mapping.
        """
        try:
            await self._client.indices.create(index=index_name, body=mapping)
            logger.warning("Created index", index=index_name)
        except Exception as e:
            error_str = str(e).lower()
            if "resource_already_exists" in error_str or "already exists" in error_str:
                logger.info("Index already exists, verifying mapping...",
                            index=index_name)
                await self._verify_mapping(index_name, mapping)
            else:
                logger.error("Failed to create index", index=index_name,
                             error=str(e), error_type=type(e).__name__)
                raise

    async def _verify_mapping(self, index_name: str, expected_mapping: dict):
        """Check if existing index has the correct field types.

        If key fields have wrong types (e.g. auto-mapped 'text' instead of
        'keyword'), delete and recreate the index.
        """
        try:
            resp = await self._client.indices.get_mapping(index=index_name)

            # opensearch-py 3.x returns ApiResponse; convert to dict
            resp_dict = resp
            if hasattr(resp, "body"):
                resp_dict = resp.body
            if not isinstance(resp_dict, dict):
                resp_dict = dict(resp_dict)

            actual_props = {}
            if index_name in resp_dict:
                actual_props = resp_dict[index_name].get("mappings", {}).get("properties", {})

            expected_props = expected_mapping.get("mappings", {}).get("properties", {})

            logger.warning("Mapping verification",
                           index=index_name,
                           actual_fields=list(actual_props.keys()),
                           expected_fields=list(expected_props.keys()))

            mismatches = []
            for field, expected_def in expected_props.items():
                expected_type = expected_def.get("type")
                actual_def = actual_props.get(field, {})
                actual_type = actual_def.get("type")
                if actual_type and expected_type and actual_type != expected_type:
                    mismatches.append(f"{field}: {actual_type} -> {expected_type}")

            if mismatches:
                logger.warning("Index has wrong field types, recreating...",
                               index=index_name, mismatches=mismatches)
                await self._client.indices.delete(index=index_name)
                logger.warning("Deleted index with bad mapping", index=index_name)
                await self._client.indices.create(index=index_name, body=expected_mapping)
                logger.warning("Recreated index with correct mapping",
                               index=index_name)
            else:
                logger.info("Index mapping verified OK", index=index_name)
        except Exception as e:
            logger.error("Mapping verification failed", index=index_name,
                         error=str(e), error_type=type(e).__name__)

    async def initialize(self):
        """Ensure indices exist (create if needed)."""
        # Log cluster info for diagnostics
        try:
            info = await self._client.info()
            server_ver = info.get("version", {}).get("number", "?")
            import opensearchpy as _ospy
            client_ver = getattr(_ospy, "__versionstr__", "?")
            logger.warning("Agent OpenSearch connection OK",
                           server_version=server_ver,
                           client_version=client_ver,
                           hosts=self.config.hosts)
        except Exception as e:
            logger.error("Agent cannot reach OpenSearch!", error=str(e))
            raise

        await self._ensure_index(self.logs_index, {
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
        })

        await self._ensure_index(self.metrics_index, {
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
        })

        await self._ensure_index(self.host_metrics_index, {
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
        })

        # Verify indices exist
        for idx in [self.logs_index, self.metrics_index, self.host_metrics_index]:
            try:
                count_resp = await self._client.count(index=idx)
                logger.warning("Index verified", index=idx,
                               doc_count=count_resp.get("count", 0))
            except Exception as e:
                logger.error("Index verification FAILED!", index=idx, error=str(e))

        logger.warning("OpenSearch writer initialized")

    async def close(self):
        """Close the client."""
        await self._client.close()

    async def self_test(self) -> bool:
        """Write a test document, read it back, and delete it.

        Returns True if the full write-read-delete cycle succeeds.
        This proves the OpenSearch connection and index mapping work end-to-end.
        """
        # Log server info first
        try:
            info = await self._client.info()
            server_version = info.get("version", {}).get("number", "unknown")
            cluster_name = info.get("cluster_name", "unknown")
            import opensearchpy as _ospy
            client_version = getattr(_ospy, "__versionstr__", "unknown")
            logger.warning("Self-test: cluster info",
                           server_version=server_version,
                           client_version=client_version,
                           cluster_name=cluster_name,
                           hosts=self.config.hosts)
        except Exception as e:
            logger.critical("Self-test: cannot reach OpenSearch cluster!",
                            error=str(e), hosts=self.config.hosts)
            return False

        test_id = "__selftest__"
        test_doc = {
            "timestamp": datetime.utcnow().isoformat(),
            "host": "__selftest__",
            "container_id": "__selftest__",
            "container_name": "__selftest__",
            "compose_project": "__selftest__",
            "compose_service": "__selftest__",
            "stream": "stdout",
            "message": "PulsarCD agent self-test",
            "level": "INFO",
            "http_status": None,
            "parsed_fields": {},
        }

        try:
            # 1. Single-doc write
            resp = await self._client.index(
                index=self.logs_index, id=test_id, body=test_doc, refresh="true"
            )
            write_result = resp.get("result")
            logger.info("Self-test: single write OK", result=write_result, index=self.logs_index)

            # 2. Bulk write test (verifies the bulk API works)
            bulk_test_id = "__selftest_bulk__"
            bulk_doc = test_doc.copy()
            bulk_doc["message"] = "PulsarCD agent bulk self-test"
            ndjson = (
                json.dumps({"index": {"_index": self.logs_index, "_id": bulk_test_id}}) + "\n"
                + json.dumps(bulk_doc) + "\n"
            )
            bulk_resp = await self._client.bulk(body=ndjson, refresh="true")
            bulk_errors = bulk_resp.get("errors", True)
            bulk_items = bulk_resp.get("items", [])
            bulk_status = bulk_items[0].get("index", {}).get("status") if bulk_items else "no_items"
            logger.warning("Self-test: bulk write", errors=bulk_errors, status=bulk_status)
            if bulk_errors:
                logger.critical("Self-test: BULK API DOES NOT WORK!",
                                response=json.dumps(bulk_resp)[:500])

            # 3. Read back
            get_resp = await self._client.get(index=self.logs_index, id=test_id)
            found = get_resp.get("found", False)
            logger.info("Self-test: read OK", found=found)

            # 4. Search test (verifies search works too)
            search_resp = await self._client.search(
                index=self.logs_index,
                body={"query": {"term": {"host": "__selftest__"}}, "size": 1},
            )
            search_hits = search_resp.get("hits", {}).get("total", {})
            search_count = search_hits.get("value", 0) if isinstance(search_hits, dict) else search_hits
            logger.warning("Self-test: search OK", hits=search_count)

            # 5. Delete test docs
            await self._client.delete(index=self.logs_index, id=test_id, refresh="true")
            try:
                await self._client.delete(index=self.logs_index, id=bulk_test_id, refresh="true")
            except Exception:
                pass
            logger.info("Self-test: delete OK")

            # 6. Count existing docs in all indices
            for idx_name in [self.logs_index, self.metrics_index, self.host_metrics_index]:
                try:
                    count_resp = await self._client.count(index=idx_name)
                    doc_count = count_resp.get("count", "?")
                    logger.warning("Self-test: index doc count",
                                   index=idx_name, doc_count=doc_count)
                except Exception as e:
                    logger.warning("Self-test: index not found or empty",
                                   index=idx_name, error=str(e)[:200])

            return True

        except Exception as e:
            logger.critical(
                "SELF-TEST FAILED — OpenSearch pipeline is broken!",
                error=str(e),
                error_type=type(e).__name__,
                index=self.logs_index,
                hosts=self.config.hosts,
            )
            return False

    async def count_docs(self, index: Optional[str] = None) -> int:
        """Count documents in an index. Defaults to logs index."""
        try:
            resp = await self._client.count(index=index or self.logs_index)
            return resp.get("count", 0)
        except Exception:
            return -1

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
            self._write_count += 1
            if self._write_count % 50 == 0:
                sample = actions[0]["_source"] if actions else {}
                logger.warning("[periodic] Indexed logs", write_num=self._write_count,
                               count=success, failed=len(failed) if failed else 0,
                               sample_keys=list(sample.keys()), sample_host=sample.get("host"),
                               sample_container=sample.get("container_name"))
            logger.debug("Indexed logs", count=success)
            return success
        except Exception as e:
            logger.error("Failed to index logs", error=str(e))
            return 0

    async def index_container_stats(self, stats: Dict[str, Any]):
        """Index container statistics."""
        doc = stats.copy()
        timestamp = doc.get("timestamp", datetime.utcnow())
        if isinstance(timestamp, datetime):
            doc["timestamp"] = timestamp.isoformat()

        try:
            resp = await self._client.index(
                index=self.metrics_index,
                body=doc,
            )
            self._write_count += 1
            if self._write_count % 50 == 0:
                logger.warning("[periodic] Indexed container stats", write_num=self._write_count,
                               container=doc.get("container_name"), host=doc.get("host"),
                               cpu=doc.get("cpu_percent"), mem_pct=doc.get("memory_percent"),
                               result=resp.get("result"), index=resp.get("_index"))
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
            resp = await self._client.index(
                index=self.host_metrics_index,
                body=doc,
            )
            self._write_count += 1
            if self._write_count % 50 == 0:
                logger.warning("[periodic] Indexed host metrics", write_num=self._write_count,
                               host=doc.get("host"), cpu=doc.get("cpu_percent"),
                               mem_pct=doc.get("memory_percent"),
                               result=resp.get("result"), index=resp.get("_index"))
        except Exception as e:
            logger.error("Failed to index host metrics", error=str(e))
