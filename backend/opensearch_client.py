"""OpenSearch client for log storage and querying."""

import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import structlog
from opensearchpy import AsyncOpenSearch, ConflictError, helpers

from .config import OpenSearchConfig
from .models import (
    ContainerStats, DashboardStats, HostMetrics, LogEntry,
    LogSearchQuery, LogSearchResult, TimeSeriesPoint
)

logger = structlog.get_logger()


class OpenSearchClient:
    """Async OpenSearch client for log operations."""
    
    def __init__(self, config: OpenSearchConfig):
        self.config = config
        self.logs_index = f"{config.index_prefix}-logs"
        self.metrics_index = f"{config.index_prefix}-metrics"
        self.host_metrics_index = f"{config.index_prefix}-host-metrics"
        
        # Parse hosts
        hosts = []
        for host_url in config.hosts:
            hosts.append(host_url)
        
        auth = None
        if config.username and config.password:
            auth = (config.username, config.password)
        
        self._client = AsyncOpenSearch(
            hosts=hosts,
            http_auth=auth,
            use_ssl="https" in config.hosts[0] if config.hosts else False,
            verify_certs=False,
            ssl_show_warn=False,
        )
    
    async def initialize(self):
        """Create indices and mappings."""
        await self._create_logs_index()
        await self._create_metrics_index()
        await self._create_host_metrics_index()
        logger.info("OpenSearch indices initialized")
    
    async def _create_logs_index(self):
        """Create logs index with mapping."""
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
    
    async def _create_metrics_index(self):
        """Create container metrics index."""
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
        else:
            # Verify container_id is mapped as keyword (not text).
            # If auto-mapped as text, aggregations fail; recreate the index.
            try:
                existing = await self._client.indices.get_mapping(index=self.metrics_index)
                props = existing.get(self.metrics_index, {}).get("mappings", {}).get("properties", {})
                cid_type = props.get("container_id", {}).get("type")
                if cid_type and cid_type != "keyword":
                    logger.warning("Metrics index has container_id mapped as %s, recreating", cid_type)
                    await self._client.indices.delete(index=self.metrics_index)
                    await self._client.indices.create(index=self.metrics_index, body=mapping)
                    logger.info("Recreated metrics index with correct mappings", index=self.metrics_index)
            except Exception as e:
                logger.warning("Could not verify metrics index mapping", error=str(e))
    
    async def _create_host_metrics_index(self):
        """Create host metrics index."""
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
    
    def _generate_log_id(self, entry: LogEntry) -> str:
        """Generate unique ID for log entry."""
        unique_str = f"{entry.host}:{entry.container_id}:{entry.timestamp.isoformat()}:{entry.message[:100]}"
        return hashlib.md5(unique_str.encode()).hexdigest()
    
    async def index_logs(self, entries: List[LogEntry]):
        """Bulk index log entries."""
        if not entries:
            return
        
        actions = []
        for entry in entries:
            doc_id = self._generate_log_id(entry)
            doc = entry.model_dump()
            doc["timestamp"] = entry.timestamp.isoformat()
            
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
    
    async def index_container_stats(self, stats: ContainerStats):
        """Index container statistics."""
        doc = stats.model_dump()
        doc["timestamp"] = stats.timestamp.isoformat()

        try:
            await self._client.index(
                index=self.metrics_index,
                body=doc,
            )
        except Exception as e:
            logger.error("Failed to index container stats", error=str(e))
    
    async def index_host_metrics(self, metrics: HostMetrics):
        """Index host metrics.

        Uses auto-generated IDs to avoid version conflicts when multiple
        writers (agents, backend) index metrics for the same host concurrently.
        """
        doc = metrics.model_dump()
        doc["timestamp"] = metrics.timestamp.isoformat()

        try:
            await self._client.index(
                index=self.host_metrics_index,
                body=doc,
            )
        except Exception as e:
            logger.error("Failed to index host metrics", error=str(e))
    
    async def get_latest_container_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get the latest stats for all containers (single aggregation query).
        
        Returns a dict: {container_id: {cpu_percent, memory_percent, memory_usage_mb}}
        """
        # Use top_hits aggregation to get the latest metric per container
        body = {
            "size": 0,
            "query": {
                "range": {
                    "timestamp": {
                        "gte": "now-5m"  # Only consider recent metrics
                    }
                }
            },
            "aggs": {
                "by_container": {
                    "terms": {
                        "field": "container_id",
                        "size": 1000  # Max containers to return
                    },
                    "aggs": {
                        "latest": {
                            "top_hits": {
                                "size": 1,
                                "sort": [{"timestamp": "desc"}],
                                "_source": ["cpu_percent", "memory_percent", "memory_usage_mb", "gpu_percent", "gpu_memory_used_mb", "container_id"]
                            }
                        }
                    }
                }
            }
        }
        
        try:
            response = await self._client.search(index=self.metrics_index, body=body)
            buckets = response.get("aggregations", {}).get("by_container", {}).get("buckets", [])
            
            result = {}
            for bucket in buckets:
                container_id = bucket["key"]
                hits = bucket.get("latest", {}).get("hits", {}).get("hits", [])
                if hits:
                    source = hits[0]["_source"]
                    result[container_id] = {
                        "cpu_percent": round(source.get("cpu_percent", 0) or 0, 1),
                        "memory_percent": round(source.get("memory_percent", 0) or 0, 1),
                        "memory_usage_mb": round(source.get("memory_usage_mb", 0) or 0, 1),
                        "gpu_percent": round(source.get("gpu_percent"), 1) if source.get("gpu_percent") is not None else None,
                        "gpu_memory_used_mb": round(source.get("gpu_memory_used_mb"), 1) if source.get("gpu_memory_used_mb") is not None else None,
                    }
            
            return result
        except Exception as e:
            logger.error("Failed to get latest container stats", error=str(e))
            return {}
    
    async def get_latest_stats_for_container(self, container_id: str) -> Optional[Dict[str, Any]]:
        """Get the latest stats for a specific container from OpenSearch.
        
        Args:
            container_id: The container ID to retrieve stats for.
            
        Returns:
            Dict with cpu_percent, memory_percent, memory_usage_mb, memory_limit_mb,
            network_rx_bytes, network_tx_bytes, block_read_bytes, block_write_bytes or None.
        """
        try:
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"container_id": container_id}},
                            {"range": {"timestamp": {"gte": "now-5m"}}}
                        ]
                    }
                },
                "size": 1,
                "sort": [{"timestamp": "desc"}],
                "_source": [
                    "cpu_percent", "memory_percent", "memory_usage_mb", "memory_limit_mb",
                    "network_rx_bytes", "network_tx_bytes", "block_read_bytes", "block_write_bytes",
                    "timestamp"
                ]
            }
            
            response = await self._client.search(index=self.metrics_index, body=body)
            hits = response.get("hits", {}).get("hits", [])
            
            if hits:
                source = hits[0]["_source"]
                return {
                    "cpu_percent": source.get("cpu_percent", 0) or 0,
                    "memory_percent": source.get("memory_percent", 0) or 0,
                    "memory_usage_mb": source.get("memory_usage_mb", 0) or 0,
                    "memory_limit_mb": source.get("memory_limit_mb", 0) or 0,
                    "network_rx_bytes": source.get("network_rx_bytes", 0) or 0,
                    "network_tx_bytes": source.get("network_tx_bytes", 0) or 0,
                    "block_read_bytes": source.get("block_read_bytes", 0) or 0,
                    "block_write_bytes": source.get("block_write_bytes", 0) or 0,
                }
            
            return None
        except Exception as e:
            logger.error("Failed to get latest stats for container", container_id=container_id, error=str(e))
            return None
    
    async def get_latest_host_metrics(self, host_name: str) -> Optional[Dict[str, Any]]:
        """Get the latest metrics for a specific host including GPU.
        
        Args:
            host_name: Name of the host
            
        Returns:
            Dict with cpu_percent, memory_percent, gpu_percent, disk_percent, etc. or None
        """
        try:
            body = {
                "query": {
                    "term": {"host": host_name}
                },
                "size": 1,
                "sort": [{"timestamp": "desc"}],
                "_source": [
                    "cpu_percent", "memory_percent", "memory_used_mb", "memory_total_mb",
                    "gpu_percent", "gpu_memory_used_mb", "gpu_memory_total_mb",
                    "disk_total_gb", "disk_used_gb", "disk_percent", "timestamp"
                ]
            }
            
            response = await self._client.search(index=self.host_metrics_index, body=body)
            hits = response.get("hits", {}).get("hits", [])
            
            if hits:
                source = hits[0]["_source"]
                return {
                    "cpu_percent": round(source.get("cpu_percent", 0) or 0, 1),
                    "memory_percent": round(source.get("memory_percent", 0) or 0, 1),
                    "memory_used_mb": round(source.get("memory_used_mb", 0) or 0, 1),
                    "memory_total_mb": round(source.get("memory_total_mb", 0) or 0, 1),
                    "gpu_percent": round(source.get("gpu_percent"), 1) if source.get("gpu_percent") is not None else None,
                    "gpu_memory_used_mb": round(source.get("gpu_memory_used_mb"), 1) if source.get("gpu_memory_used_mb") is not None else None,
                    "gpu_memory_total_mb": round(source.get("gpu_memory_total_mb"), 1) if source.get("gpu_memory_total_mb") is not None else None,
                    "disk_total_gb": round(source.get("disk_total_gb", 0) or 0, 1),
                    "disk_used_gb": round(source.get("disk_used_gb", 0) or 0, 1),
                    "disk_percent": round(source.get("disk_percent", 0) or 0, 1),
                    "timestamp": source.get("timestamp"),
                }
            
            return None
        except Exception as e:
            logger.error("Failed to get latest host metrics", host=host_name, error=str(e))
            return None
    
    async def search_logs(self, query: LogSearchQuery) -> LogSearchResult:
        """Search logs with filters."""
        must = []
        filter_clauses = []
        
        # Full-text search
        if query.query:
            must.append({
                "query_string": {
                    "query": query.query,
                    "default_field": "message",
                }
            })
        
        # Host filter
        if query.hosts:
            filter_clauses.append({"terms": {"host": query.hosts}})
        
        # Container filter
        if query.containers:
            filter_clauses.append({"terms": {"container_name": query.containers}})
        
        # Compose project filter
        if query.compose_projects:
            filter_clauses.append({"terms": {"compose_project": query.compose_projects}})
        
        # Level filter
        if query.levels:
            filter_clauses.append({"terms": {"level": query.levels}})
        
        # HTTP status range
        if query.http_status_min is not None or query.http_status_max is not None:
            range_query = {}
            if query.http_status_min is not None:
                range_query["gte"] = query.http_status_min
            if query.http_status_max is not None:
                range_query["lte"] = query.http_status_max
            filter_clauses.append({"range": {"http_status": range_query}})
        
        # Time range
        time_range = {}
        if query.start_time:
            time_range["gte"] = query.start_time.isoformat()
        if query.end_time:
            time_range["lte"] = query.end_time.isoformat()
        if time_range:
            filter_clauses.append({"range": {"timestamp": time_range}})
        
        # Build query
        es_query = {
            "bool": {
                "must": must if must else [{"match_all": {}}],
                "filter": filter_clauses,
            }
        }
        
        body = {
            "query": es_query,
            "sort": [{"timestamp": {"order": query.sort_order}}],
            "from": query.from_,
            "size": query.size,
            "aggs": {
                "levels": {"terms": {"field": "level", "size": 10}},
                "hosts": {"terms": {"field": "host", "size": 50}},
                "containers": {"terms": {"field": "container_name", "size": 100}},
                "compose_projects": {"terms": {"field": "compose_project", "size": 100}},
            }
        }
        
        try:
            response = await self._client.search(index=self.logs_index, body=body)
            
            hits = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                source["id"] = hit["_id"]
                if isinstance(source.get("timestamp"), str):
                    source["timestamp"] = datetime.fromisoformat(source["timestamp"].replace("Z", "+00:00"))
                hits.append(LogEntry(**source))
            
            aggregations = {}
            if "aggregations" in response:
                for key, agg in response["aggregations"].items():
                    aggregations[key] = [
                        {"key": bucket["key"], "count": bucket["doc_count"]}
                        for bucket in agg.get("buckets", [])
                    ]
            
            total = response["hits"]["total"]
            total_count = total["value"] if isinstance(total, dict) else total
            
            return LogSearchResult(
                total=total_count,
                hits=hits,
                aggregations=aggregations,
            )
        except Exception as e:
            logger.error("Log search failed",
                         error=str(e),
                         error_type=type(e).__name__,
                         index=self.logs_index,
                         query_snippet=str(body.get("query", ""))[:300])
            # Try a simple count to check if the index has data at all
            try:
                count_resp = await self._client.count(index=self.logs_index)
                doc_count = count_resp.get("count", "?")
                logger.error("Log search failed but index has docs",
                             index=self.logs_index, doc_count=doc_count)
            except Exception:
                logger.error("Log search: index count also failed",
                             index=self.logs_index)
            return LogSearchResult(total=0, hits=[], aggregations={})
    
    async def get_dashboard_stats(self) -> DashboardStats:
        """Get dashboard statistics."""
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)
        
        # Count errors and HTTP status codes in last 24h
        body = {
            "query": {
                "range": {
                    "timestamp": {"gte": yesterday.isoformat()}
                }
            },
            "size": 0,
            "aggs": {
                "errors": {
                    "filter": {"terms": {"level": ["ERROR", "FATAL", "CRITICAL"]}}
                },
                "warnings": {
                    "filter": {"term": {"level": "WARN"}}
                },
                "http_4xx": {
                    "filter": {"range": {"http_status": {"gte": 400, "lt": 500}}}
                },
                "http_5xx": {
                    "filter": {"range": {"http_status": {"gte": 500, "lt": 600}}}
                },
            }
        }
        
        try:
            response = await self._client.search(index=self.logs_index, body=body)
            aggs = response.get("aggregations", {})

            errors_24h = aggs.get("errors", {}).get("doc_count", 0)
            warnings_24h = aggs.get("warnings", {}).get("doc_count", 0)
            http_4xx = aggs.get("http_4xx", {}).get("doc_count", 0)
            http_5xx = aggs.get("http_5xx", {}).get("doc_count", 0)
        except Exception as e:
            logger.error("Dashboard stats: logs query failed", error=str(e), index=self.logs_index)
            errors_24h = warnings_24h = http_4xx = http_5xx = 0
        
        # Get average metrics from last hour
        one_hour_ago = now - timedelta(hours=1)
        metrics_body = {
            "query": {
                "range": {"timestamp": {"gte": one_hour_ago.isoformat()}}
            },
            "size": 0,
            "aggs": {
                "avg_cpu": {"avg": {"field": "cpu_percent"}},
                "avg_memory": {"avg": {"field": "memory_percent"}},
                "avg_gpu": {"avg": {"field": "gpu_percent"}},
                "avg_vram_used": {"avg": {"field": "gpu_memory_used_mb"}},
                "avg_vram_total": {"avg": {"field": "gpu_memory_total_mb"}},
            }
        }

        try:
            metrics_response = await self._client.search(index=self.host_metrics_index, body=metrics_body)
            metrics_aggs = metrics_response.get("aggregations", {})
            avg_cpu = metrics_aggs.get("avg_cpu", {}).get("value", 0) or 0
            avg_memory = metrics_aggs.get("avg_memory", {}).get("value", 0) or 0
            avg_gpu = metrics_aggs.get("avg_gpu", {}).get("value")  # Keep None if no GPU data
            avg_vram_used = metrics_aggs.get("avg_vram_used", {}).get("value")
            avg_vram_total = metrics_aggs.get("avg_vram_total", {}).get("value")
        except Exception as e:
            logger.error("Dashboard stats: host-metrics query failed", error=str(e), index=self.host_metrics_index)
            avg_cpu = avg_memory = 0
            avg_gpu = avg_vram_used = avg_vram_total = None

        return DashboardStats(
            total_containers=0,  # Will be filled by API
            running_containers=0,
            total_hosts=0,
            healthy_hosts=0,
            errors_24h=errors_24h,
            warnings_24h=warnings_24h,
            http_4xx_24h=http_4xx,
            http_5xx_24h=http_5xx,
            avg_cpu_percent=round(avg_cpu, 2),
            avg_memory_percent=round(avg_memory, 2),
            avg_gpu_percent=round(avg_gpu, 2) if avg_gpu is not None else None,
            avg_vram_used_mb=round(avg_vram_used, 1) if avg_vram_used is not None else None,
            avg_vram_total_mb=round(avg_vram_total, 1) if avg_vram_total is not None else None,
        )
    
    async def get_error_timeseries(self, hours: int = 24, interval: str = "1h") -> List[TimeSeriesPoint]:
        """Get error count time series."""
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"timestamp": {"gte": start.isoformat()}}},
                        {"terms": {"level": ["ERROR", "FATAL", "CRITICAL"]}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "fixed_interval": interval,
                    }
                }
            }
        }
        
        try:
            response = await self._client.search(index=self.logs_index, body=body)
            buckets = response.get("aggregations", {}).get("over_time", {}).get("buckets", [])
            
            return [
                TimeSeriesPoint(
                    timestamp=datetime.fromisoformat(bucket["key_as_string"].replace("Z", "+00:00")),
                    value=bucket["doc_count"]
                )
                for bucket in buckets
            ]
        except Exception as e:
            logger.error("Failed to get error timeseries", error=str(e))
            return []
    
    async def get_http_requests_timeseries(self, hours: int = 24, interval: str = "1h") -> List[TimeSeriesPoint]:
        """Get total HTTP requests count time series (any log with http_status)."""
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"timestamp": {"gte": start.isoformat()}}},
                        {"exists": {"field": "http_status"}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "fixed_interval": interval,
                    }
                }
            }
        }
        
        try:
            response = await self._client.search(index=self.logs_index, body=body)
            buckets = response.get("aggregations", {}).get("over_time", {}).get("buckets", [])
            
            return [
                TimeSeriesPoint(
                    timestamp=datetime.fromisoformat(bucket["key_as_string"].replace("Z", "+00:00")),
                    value=bucket["doc_count"]
                )
                for bucket in buckets
            ]
        except Exception as e:
            logger.error("Failed to get HTTP requests timeseries", error=str(e))
            return []
    
    async def get_http_status_timeseries(self, status_min: int, status_max: int, hours: int = 24, interval: str = "1h") -> List[TimeSeriesPoint]:
        """Get HTTP status count time series."""
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"timestamp": {"gte": start.isoformat()}}},
                        {"range": {"http_status": {"gte": status_min, "lt": status_max}}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "fixed_interval": interval,
                    }
                }
            }
        }
        
        try:
            response = await self._client.search(index=self.logs_index, body=body)
            buckets = response.get("aggregations", {}).get("over_time", {}).get("buckets", [])
            
            return [
                TimeSeriesPoint(
                    timestamp=datetime.fromisoformat(bucket["key_as_string"].replace("Z", "+00:00")),
                    value=bucket["doc_count"]
                )
                for bucket in buckets
            ]
        except Exception as e:
            logger.error("Failed to get HTTP status timeseries", error=str(e))
            return []
    
    async def get_resource_timeseries(self, metric: str, hours: int = 24, interval: str = "15m") -> List[TimeSeriesPoint]:
        """Get resource usage time series (cpu_percent or memory_percent)."""
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        
        body = {
            "query": {
                "range": {"timestamp": {"gte": start.isoformat()}}
            },
            "size": 0,
            "aggs": {
                "over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "fixed_interval": interval,
                    },
                    "aggs": {
                        "avg_value": {"avg": {"field": metric}}
                    }
                }
            }
        }
        
        try:
            response = await self._client.search(index=self.host_metrics_index, body=body)
            buckets = response.get("aggregations", {}).get("over_time", {}).get("buckets", [])
            
            return [
                TimeSeriesPoint(
                    timestamp=datetime.fromisoformat(bucket["key_as_string"].replace("Z", "+00:00")),
                    value=round(bucket["avg_value"]["value"] or 0, 2)
                )
                for bucket in buckets
            ]
        except Exception as e:
            logger.error("Failed to get resource timeseries", error=str(e))
            return []
    
    async def get_resource_timeseries_by_host(self, metric: str, hours: int = 24, interval: str = "15m") -> List[Dict]:
        """Get resource usage time series grouped by host."""
        from .models import TimeSeriesByHost, TimeSeriesPoint
        
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        
        body = {
            "query": {
                "range": {"timestamp": {"gte": start.isoformat()}}
            },
            "size": 0,
            "aggs": {
                "by_host": {
                    "terms": {
                        "field": "host",
                        "size": 50
                    },
                    "aggs": {
                        "over_time": {
                            "date_histogram": {
                                "field": "timestamp",
                                "fixed_interval": interval,
                            },
                            "aggs": {
                                "avg_value": {"avg": {"field": metric}}
                            }
                        }
                    }
                }
            }
        }
        
        try:
            response = await self._client.search(index=self.host_metrics_index, body=body)
            host_buckets = response.get("aggregations", {}).get("by_host", {}).get("buckets", [])
            
            result = []
            for host_bucket in host_buckets:
                host = host_bucket["key"]
                time_buckets = host_bucket.get("over_time", {}).get("buckets", [])
                
                data = [
                    TimeSeriesPoint(
                        timestamp=datetime.fromisoformat(bucket["key_as_string"].replace("Z", "+00:00")),
                        value=round(bucket["avg_value"]["value"] or 0, 2)
                    )
                    for bucket in time_buckets
                ]
                
                result.append(TimeSeriesByHost(host=host, data=data))
            
            return result
        except Exception as e:
            logger.error("Failed to get resource timeseries by host", metric=metric, error=str(e))
            return []
    
    async def get_vram_percent_timeseries_by_host(self, hours: int = 24, interval: str = "15m") -> List[Dict]:
        """Get VRAM usage percentage time series grouped by host.
        
        Calculates VRAM % as (gpu_memory_used_mb / gpu_memory_total_mb) * 100.
        """
        from .models import TimeSeriesByHost, TimeSeriesPoint
        
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"timestamp": {"gte": start.isoformat()}}},
                        {"exists": {"field": "gpu_memory_total_mb"}},
                        {"range": {"gpu_memory_total_mb": {"gt": 0}}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "by_host": {
                    "terms": {
                        "field": "host",
                        "size": 50
                    },
                    "aggs": {
                        "over_time": {
                            "date_histogram": {
                                "field": "timestamp",
                                "fixed_interval": interval,
                            },
                            "aggs": {
                                "avg_used": {"avg": {"field": "gpu_memory_used_mb"}},
                                "avg_total": {"avg": {"field": "gpu_memory_total_mb"}}
                            }
                        }
                    }
                }
            }
        }
        
        try:
            response = await self._client.search(index=self.host_metrics_index, body=body)
            host_buckets = response.get("aggregations", {}).get("by_host", {}).get("buckets", [])
            
            result = []
            for host_bucket in host_buckets:
                host = host_bucket["key"]
                time_buckets = host_bucket.get("over_time", {}).get("buckets", [])
                
                data = []
                for bucket in time_buckets:
                    used = bucket.get("avg_used", {}).get("value") or 0
                    total = bucket.get("avg_total", {}).get("value") or 0
                    vram_percent = (used / total * 100) if total > 0 else 0
                    
                    data.append(TimeSeriesPoint(
                        timestamp=datetime.fromisoformat(bucket["key_as_string"].replace("Z", "+00:00")),
                        value=round(vram_percent, 2)
                    ))
                
                if data:
                    result.append(TimeSeriesByHost(host=host, data=data))
            
            return result
        except Exception as e:
            logger.error("Failed to get VRAM timeseries by host", error=str(e))
            return []
    
    async def get_container_metrics_timeseries(
        self, container_id: str, hours: int = 168, interval: str = "1h"
    ) -> Dict[str, List[Dict]]:
        """Get CPU%, memory%, and error count time series for a specific container."""
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        time_range = {"range": {"timestamp": {"gte": start.isoformat()}}}
        container_filter = {"term": {"container_id": container_id}}

        # CPU & memory from metrics index
        perf_body = {
            "query": {"bool": {"filter": [time_range, container_filter]}},
            "size": 0,
            "aggs": {
                "over_time": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": interval},
                    "aggs": {
                        "avg_cpu": {"avg": {"field": "cpu_percent"}},
                        "avg_mem": {"avg": {"field": "memory_percent"}},
                    },
                }
            },
        }

        # Error count from logs index
        error_body = {
            "query": {
                "bool": {
                    "filter": [
                        time_range,
                        container_filter,
                        {"terms": {"level": ["ERROR", "FATAL", "CRITICAL"]}},
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "over_time": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": interval},
                }
            },
        }

        cpu_points: List[Dict] = []
        memory_points: List[Dict] = []
        error_points: List[Dict] = []

        try:
            resp = await self._client.search(index=self.metrics_index, body=perf_body)
            for b in resp.get("aggregations", {}).get("over_time", {}).get("buckets", []):
                ts = b["key_as_string"]
                cpu_points.append({"timestamp": ts, "value": round(b["avg_cpu"]["value"] or 0, 2)})
                memory_points.append({"timestamp": ts, "value": round(b["avg_mem"]["value"] or 0, 2)})
        except Exception as e:
            logger.error("Failed to get container CPU/memory timeseries", container_id=container_id, error=str(e))

        try:
            resp = await self._client.search(index=self.logs_index, body=error_body)
            for b in resp.get("aggregations", {}).get("over_time", {}).get("buckets", []):
                error_points.append({"timestamp": b["key_as_string"], "value": b["doc_count"]})
        except Exception as e:
            logger.error("Failed to get container error timeseries", container_id=container_id, error=str(e))

        return {"cpu": cpu_points, "memory": memory_points, "errors": error_points}

    async def count_similar_logs(self, message: str, container_name: str = "", hours: int = 24) -> int:
        """Count similar log messages in the last N hours.
        
        Uses a simplified similarity approach by extracting key terms from the message.
        """
        import re
        
        # Simplify the message: remove dynamic parts and special characters
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
        # Clean up extra spaces
        simplified = ' '.join(simplified.split())
        
        # Extract key words (at least 3 chars, filter out common/stop words)
        stop_words = {
            'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'her', 'was', 
            'one', 'our', 'out', 'http', 'https', 'info', 'get', 'post', 'put', 'delete',
            'from', 'has', 'been', 'moved', 'will', 'that', 'this', 'with', 'have', 'your',
            'usr', 'local', 'lib', 'python', 'site', 'packages'  # Filter out path components
        }
        words = [w.lower() for w in simplified.split() if len(w) >= 3 and w.lower() not in stop_words]
        
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        # If we have no useful words or too few, return 0 to avoid overly broad matches
        if len(words) < 2:
            return 0
        
        # Use match query with minimum_should_match for flexibility
        # At least 50% of words should match (minimum 2)
        key_words = words[:6]
        min_match = max(2, len(key_words) // 2)
        
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "message": {
                                    "query": ' '.join(key_words),
                                    "operator": "or",
                                    "minimum_should_match": f"{min_match}"
                                }
                            }
                        }
                    ],
                    "filter": [
                        {"range": {"timestamp": {"gte": cutoff.isoformat()}}}
                    ]
                }
            }
        }
        
        # Add container filter if provided
        if container_name:
            body["query"]["bool"]["filter"].append({"term": {"container_name": container_name}})
        
        try:
            response = await self._client.count(index=self.logs_index, body=body)
            return response.get("count", 0)
        except Exception as e:
            logger.error("Failed to count similar logs", error=str(e))
            return 0
    
    async def get_available_metadata(self) -> Dict[str, Any]:
        """Get available hosts, containers, compose projects and log levels for AI context."""
        body = {
            "size": 0,
            "aggs": {
                "hosts": {"terms": {"field": "host", "size": 50}},
                "containers": {"terms": {"field": "container_name", "size": 200}},
                "compose_projects": {"terms": {"field": "compose_project", "size": 50}},
                "compose_services": {"terms": {"field": "compose_service", "size": 200}},
                "levels": {"terms": {"field": "level", "size": 10}},
            }
        }
        
        try:
            response = await self._client.search(index=self.logs_index, body=body)
            aggs = response.get("aggregations", {})
            
            return {
                "hosts": [b["key"] for b in aggs.get("hosts", {}).get("buckets", [])],
                "containers": [b["key"] for b in aggs.get("containers", {}).get("buckets", [])],
                "compose_projects": [b["key"] for b in aggs.get("compose_projects", {}).get("buckets", [])],
                "compose_services": [b["key"] for b in aggs.get("compose_services", {}).get("buckets", [])],
                "levels": [b["key"] for b in aggs.get("levels", {}).get("buckets", [])],
            }
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)[:300]}"
            logger.error("Failed to get metadata for AI", error=error_msg,
                         index=self.logs_index)
            return {
                "hosts": [],
                "containers": [],
                "compose_projects": [],
                "compose_services": [],
                "levels": [],
                "error": error_msg,
            }

    async def run_logs_query(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a raw OpenSearch query against the logs index.

        Returns the raw OpenSearch response dict (hits, aggregations, total, etc.).
        Size and from are capped server-side to prevent abuse.
        """
        body.setdefault("size", 50)
        body["size"] = min(int(body["size"]), 500)
        body.setdefault("from", 0)
        try:
            return await self._client.search(index=self.logs_index, body=body)
        except Exception as e:
            logger.error("Raw logs query failed", error=str(e))
            raise

    async def get_error_counts_by_service(self, hours: int = 24) -> Dict[str, Any]:
        """Return error/warning counts per compose_project for the last N hours."""
        now = datetime.utcnow()
        start = now - timedelta(hours=hours)
        body = {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": start.isoformat()}}},
                        {"terms": {"level": ["ERROR", "FATAL", "CRITICAL", "WARN", "WARNING"]}},
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "by_project": {
                    "terms": {"field": "compose_project", "size": 100, "missing": "__none__"},
                    "aggs": {
                        "by_level": {"terms": {"field": "level", "size": 10}},
                    },
                }
            },
        }
        try:
            resp = await self._client.search(index=self.logs_index, body=body)
            result = []
            for bucket in resp.get("aggregations", {}).get("by_project", {}).get("buckets", []):
                project = bucket["key"] if bucket["key"] != "__none__" else None
                level_counts: Dict[str, int] = {}
                for lb in bucket.get("by_level", {}).get("buckets", []):
                    level_counts[lb["key"]] = lb["doc_count"]
                errors = sum(v for k, v in level_counts.items() if k in ("ERROR", "FATAL", "CRITICAL"))
                warnings = sum(v for k, v in level_counts.items() if k in ("WARN", "WARNING"))
                result.append({
                    "compose_project": project,
                    "errors": errors,
                    "warnings": warnings,
                    "total": bucket["doc_count"],
                })
            result.sort(key=lambda x: x["errors"] + x["warnings"], reverse=True)
            return {"hours": hours, "services": result}
        except Exception as e:
            logger.error("Failed to get error counts by service", error=str(e))
            return {"hours": hours, "services": []}

    async def cleanup_old_data(self, retention_days: int):
        """Delete data older than retention period."""
        cutoff = datetime.utcnow() - timedelta(days=retention_days)
        
        for index in [self.logs_index, self.metrics_index, self.host_metrics_index]:
            try:
                result = await self._client.delete_by_query(
                    index=index,
                    body={
                        "query": {
                            "range": {"timestamp": {"lt": cutoff.isoformat()}}
                        }
                    },
                    conflicts="proceed",
                )
                deleted = result.get("deleted", 0)
                conflicts = result.get("version_conflicts", 0)
                if conflicts:
                    logger.info("Cleaned up old data (some version conflicts ignored)",
                               index=index, deleted=deleted, conflicts=conflicts)
                else:
                    logger.info("Cleaned up old data", index=index, deleted=deleted)
            except ConflictError as e:
                # Version conflicts during cleanup are expected — new logs are being
                # indexed while old ones are deleted. Extract stats from the error body.
                import json
                try:
                    body = json.loads(str(e.info))
                    deleted = body.get("deleted", 0)
                    conflicts = body.get("version_conflicts", 0)
                    logger.info("Cleanup completed with version conflicts",
                               index=index, deleted=deleted, conflicts=conflicts)
                except Exception:
                    logger.info("Cleanup completed with minor version conflicts", index=index)
            except Exception as e:
                logger.error("Cleanup failed", index=index, error=str(e))
