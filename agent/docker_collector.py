"""Docker collector for the agent - collects containers, stats, and logs locally."""

import asyncio
import calendar
import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import structlog

from . import utils

logger = structlog.get_logger()


def _datetime_to_unix_utc(dt: datetime) -> int:
    """Convert a naive datetime (assumed UTC) to Unix timestamp.

    Python's ``datetime.timestamp()`` treats naive datetimes as *local* time,
    which silently shifts the value when the host timezone is not UTC.
    ``calendar.timegm`` always interprets the tuple as UTC.
    """
    return int(calendar.timegm(dt.timetuple()))


class DockerCollector:
    """Local Docker collector using Docker API."""

    # After this many consecutive empty collections for a container,
    # reset its ``since`` timestamp and fall back to tail-based fetch.
    _STALE_CYCLES_THRESHOLD = 20

    def __init__(self, docker_url: str, host_name: str):
        self.docker_url = docker_url
        self.host_name = host_name
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.BaseConnector] = None
        self._closing = False
        self._last_log_timestamp: Dict[str, datetime] = {}
        self._empty_cycles: Dict[str, int] = {}
        self._collection_cycle: int = 0

        # Determine connection type
        if docker_url.startswith("unix://"):
            socket_path = docker_url.replace("unix://", "")
            self._base_url = "http://localhost"
            self._connector = aiohttp.UnixConnector(path=socket_path)
            logger.info("Docker collector (socket)", socket=socket_path)
        else:
            self._base_url = docker_url.replace("tcp://", "http://")
            self._connector = None
            logger.info("Docker collector (TCP)", url=self._base_url)

    async def _get_session(self) -> Optional[aiohttp.ClientSession]:
        """Get or create aiohttp session."""
        if self._closing:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=self._connector)
        return self._session

    async def close(self):
        """Close the client session."""
        self._closing = True
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _request(self, method: str, endpoint: str, **kwargs) -> Tuple[Any, int]:
        """Make HTTP request to Docker API."""
        if self._closing:
            return None, 503

        session = await self._get_session()
        if session is None:
            return None, 503

        url = f"{self._base_url}{endpoint}"

        try:
            async with session.request(method, url, **kwargs) as response:
                if response.content_type == "application/json":
                    data = await response.json()
                else:
                    data = await response.text()
                return data, response.status
        except aiohttp.ClientError as e:
            if not self._closing:
                logger.error("Docker API request failed", endpoint=endpoint, error=str(e))
            return None, 500
        except Exception as e:
            if not self._closing:
                logger.error("Docker API request failed", endpoint=endpoint, error=str(e))
            return None, 500

    async def get_containers(self) -> List[Dict[str, Any]]:
        """Get list of all Docker containers."""
        data, status = await self._request("GET", "/containers/json?all=true")

        if status != 200 or not data:
            return []

        containers = []
        for c in data:
            try:
                container_id = c["Id"][:12]
                state = c.get("State", "").lower()
                labels = c.get("Labels", {}) or {}
                created_ts = c.get("Created", 0)
                created = datetime.fromtimestamp(created_ts) if created_ts else datetime.now()

                names = c.get("Names", ["/unknown"])
                name = names[0].lstrip("/") if names else "unknown"

                ports = {}
                for port in c.get("Ports", []):
                    private = f"{port.get('PrivatePort', '')}/{port.get('Type', 'tcp')}"
                    public = f"{port.get('IP', '')}:{port.get('PublicPort', '')}" if port.get('PublicPort') else None
                    if public:
                        ports[private] = public

                compose_project = (labels.get("com.docker.stack.namespace") or
                                   labels.get("com.docker.compose.project"))
                # com.docker.compose.service = plain service name (e.g. "nginx")
                # com.docker.swarm.service.name = "stack_service" (e.g. "pulsarcd_nginx")
                #   → strip the stack prefix so compose_service is always the plain name
                compose_service = labels.get("com.docker.compose.service")
                if not compose_service:
                    swarm_svc = labels.get("com.docker.swarm.service.name")
                    if swarm_svc and compose_project and swarm_svc.startswith(compose_project + "_"):
                        compose_service = swarm_svc[len(compose_project) + 1:]
                    else:
                        compose_service = swarm_svc

                containers.append({
                    "id": container_id,
                    "name": name,
                    "image": c.get("Image", "unknown"),
                    "status": state,
                    "created": created.isoformat(),
                    "host": self.host_name,
                    "compose_project": compose_project,
                    "compose_service": compose_service,
                    "ports": ports,
                    "labels": labels,
                })

            except Exception as e:
                logger.error("Failed to parse container", error=str(e))

        return containers

    async def get_container_pids(self, container_id: str) -> List[int]:
        """Get host PIDs for a running container via Docker API top endpoint."""
        data, status = await self._request("GET", f"/containers/{container_id}/top")
        if status != 200 or not data:
            return []
        pids = []
        processes = data.get("Processes") or []
        titles = data.get("Titles") or []
        try:
            pid_idx = titles.index("PID")
        except ValueError:
            pid_idx = 1  # Fallback: PID is usually the second column
        for proc in processes:
            if pid_idx < len(proc):
                try:
                    pids.append(int(proc[pid_idx]))
                except ValueError:
                    continue
        return pids

    async def get_container_stats(self, container_id: str, container_name: str) -> Optional[Dict[str, Any]]:
        """Get container resource statistics."""
        data, status = await self._request("GET", f"/containers/{container_id}/stats?stream=false")

        if status != 200 or not data:
            return None

        try:
            cpu_stats = data.get("cpu_stats", {})
            precpu_stats = data.get("precpu_stats", {})

            cpu_percent = 0.0
            percpu = cpu_stats.get("cpu_usage", {}).get("percpu_usage")
            num_cpus = cpu_stats.get("online_cpus") or (len(percpu) if percpu else 0) or 1

            cpu_usage = cpu_stats.get("cpu_usage", {})
            precpu_usage = precpu_stats.get("cpu_usage", {})

            cpu_delta = cpu_usage.get("total_usage", 0) - precpu_usage.get("total_usage", 0)

            if "system_cpu_usage" in cpu_stats and "system_cpu_usage" in precpu_stats:
                system_delta = cpu_stats["system_cpu_usage"] - precpu_stats["system_cpu_usage"]
                if system_delta > 0 and cpu_delta > 0:
                    cpu_percent = (cpu_delta / system_delta) * num_cpus * 100.0
            else:
                if cpu_delta > 0:
                    cpu_percent = (cpu_delta / 1e9) * 100.0 / num_cpus
                    cpu_percent = min(cpu_percent, 100.0 * num_cpus)

            memory_stats = data.get("memory_stats", {})
            memory_usage = memory_stats.get("usage", 0) / (1024 * 1024)
            memory_limit = memory_stats.get("limit", 0) / (1024 * 1024)

            if memory_limit > 1e12:
                memory_limit = memory_usage * 2 if memory_usage > 0 else 1024

            memory_percent = (memory_usage / memory_limit * 100) if memory_limit > 0 else 0

            networks = data.get("networks", {})
            net_rx = sum(n.get("rx_bytes", 0) for n in networks.values())
            net_tx = sum(n.get("tx_bytes", 0) for n in networks.values())

            blkio = data.get("blkio_stats", {}).get("io_service_bytes_recursive", []) or []
            block_read = sum(s.get("value", 0) for s in blkio if s.get("op", "").lower() == "read")
            block_write = sum(s.get("value", 0) for s in blkio if s.get("op", "").lower() == "write")

            return {
                "container_id": container_id,
                "container_name": container_name,
                "host": self.host_name,
                "timestamp": datetime.utcnow(),
                "cpu_percent": round(cpu_percent, 2),
                "memory_usage_mb": round(memory_usage, 2),
                "memory_limit_mb": round(memory_limit, 2),
                "memory_percent": round(memory_percent, 2),
                "network_rx_bytes": net_rx,
                "network_tx_bytes": net_tx,
                "block_read_bytes": block_read,
                "block_write_bytes": block_write,
            }

        except Exception as e:
            logger.error("Failed to parse container stats", container=container_id, error=str(e))
            return None

    async def get_host_metrics(self) -> Dict[str, Any]:
        """Get host-level metrics.

        Each metric source (Docker API, GPU, disk) is collected independently
        so that a failure in one does not prevent the others from being reported.
        """
        cpu_percent = 0.0
        memory_total_mb = 0.0
        memory_used_mb = 0.0

        # Docker API metrics (CPU + memory from container stats)
        try:
            data, status = await self._request("GET", "/info")
            if status == 200 and data:
                memory_total_mb = data.get("MemTotal", 0) / (1024 * 1024)
                containers = await self.get_containers()
                running = [c for c in containers if c.get("status") == "running"]

                for container in running[:10]:
                    stats = await self.get_container_stats(container["id"], container["name"])
                    if stats:
                        memory_used_mb += stats["memory_usage_mb"]
                        cpu_percent += stats["cpu_percent"]
        except Exception as e:
            logger.warning("Failed to collect Docker metrics", error=str(e))

        memory_percent = (memory_used_mb / memory_total_mb * 100) if memory_total_mb > 0 else 0

        # GPU metrics (independent - failure does not affect other metrics)
        gpu_percent = None
        gpu_mem_used = None
        gpu_mem_total = None
        try:
            gpu_percent, gpu_mem_used, gpu_mem_total = utils.get_gpu_metrics()
        except Exception as e:
            logger.warning("Failed to collect GPU metrics", error=str(e))

        # Disk metrics (independent)
        disk_total_gb = 0.0
        disk_used_gb = 0.0
        disk_percent = 0.0
        try:
            disk_total_gb, disk_used_gb, disk_percent = utils.get_disk_metrics()
        except Exception as e:
            logger.warning("Failed to collect disk metrics", error=str(e))

        return {
            "host": self.host_name,
            "timestamp": datetime.utcnow(),
            "cpu_percent": round(cpu_percent, 2),
            "memory_total_mb": round(memory_total_mb, 2),
            "memory_used_mb": round(memory_used_mb, 2),
            "memory_percent": round(memory_percent, 2),
            "disk_total_gb": disk_total_gb,
            "disk_used_gb": disk_used_gb,
            "disk_percent": disk_percent,
            "gpu_percent": gpu_percent,
            "gpu_memory_used_mb": gpu_mem_used,
            "gpu_memory_total_mb": gpu_mem_total,
        }

    async def get_container_logs(
        self,
        container_id: str,
        container_name: str,
        since: Optional[datetime] = None,
        tail: Optional[int] = 500,
        compose_project: Optional[str] = None,
        compose_service: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get container logs via Docker API."""
        params = ["timestamps=true", "stdout=true", "stderr=true"]

        if since:
            params.append(f"since={_datetime_to_unix_utc(since)}")
        elif tail:
            params.append(f"tail={tail}")

        endpoint = f"/containers/{container_id}/logs?{'&'.join(params)}"

        session = await self._get_session()
        if not session:
            return []

        url = f"{self._base_url}{endpoint}"

        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return []

                raw_data = await response.read()
                entries = self._parse_docker_logs(
                    raw_data, container_id, container_name,
                    compose_project, compose_service
                )
                return entries

        except Exception as e:
            logger.error("Failed to get container logs", container=container_id, error=str(e))
            return []

    def _parse_docker_logs(
        self,
        raw_data: bytes,
        container_id: str,
        container_name: str,
        compose_project: Optional[str],
        compose_service: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Parse Docker log stream format."""
        entries = []
        offset = 0

        while offset < len(raw_data):
            if offset + 8 > len(raw_data):
                break

            header = raw_data[offset:offset + 8]
            stream_type = header[0]
            size = int.from_bytes(header[4:8], byteorder='big')

            if offset + 8 + size > len(raw_data):
                break

            payload = raw_data[offset + 8:offset + 8 + size]
            offset += 8 + size

            try:
                line = payload.decode('utf-8', errors='replace').strip()
                if not line:
                    continue

                entry = self._parse_log_line(
                    line, container_id, container_name,
                    compose_project, compose_service,
                    "stderr" if stream_type == 2 else "stdout"
                )
                if entry:
                    entries.append(entry)

            except Exception:
                continue

        # Fallback: if no entries parsed, try plain text parsing
        if not entries and raw_data:
            try:
                text = raw_data.decode('utf-8', errors='replace')
                for line in text.strip().split('\n'):
                    if line.strip():
                        entry = self._parse_log_line(
                            line.strip(), container_id, container_name,
                            compose_project, compose_service, "stdout"
                        )
                        if entry:
                            entries.append(entry)
            except Exception:
                pass

        return entries

    def _parse_log_line(
        self,
        line: str,
        container_id: str,
        container_name: str,
        compose_project: Optional[str],
        compose_service: Optional[str],
        stream: str,
    ) -> Optional[Dict[str, Any]]:
        """Parse a log line with timestamp."""
        # Filter out known noise
        if utils.should_filter_log_line(line):
            return None

        # Extract timestamp and message
        timestamp, message = utils.extract_timestamp_and_message(line)

        # Strip ANSI escape codes from message — improves searchability,
        # display quality, and ensures level/status detection works correctly
        message = utils.strip_ansi(message)

        # Parse log level, HTTP status, and structured fields
        level, http_status, parsed_fields = utils.parse_log_message(message)

        return {
            "timestamp": timestamp,
            "host": self.host_name,
            "container_id": container_id,
            "container_name": container_name,
            "compose_project": compose_project,
            "compose_service": compose_service,
            "stream": stream,
            "message": message,
            "level": level,
            "http_status": http_status,
            "parsed_fields": parsed_fields,
        }

    async def collect_all_logs(self, tail: int = 500) -> List[Dict[str, Any]]:
        """Collect logs from all running containers."""
        containers = await self.get_containers()
        running = [c for c in containers if c.get("status") == "running"]

        self._collection_cycle += 1
        all_logs = []
        containers_with_logs = 0

        for container in running:
            container_key = container["id"]
            last_timestamp = self._last_log_timestamp.get(container_key)

            # Stale timestamp detection: if we've had too many consecutive
            # empty collections for this container, reset the timestamp
            # and fall back to tail-based fetch to recover.
            if last_timestamp and self._empty_cycles.get(container_key, 0) >= self._STALE_CYCLES_THRESHOLD:
                logger.warning(
                    "Resetting stale log timestamp",
                    container=container["name"],
                    container_id=container_key,
                    stale_since=last_timestamp.isoformat(),
                    empty_cycles=self._empty_cycles[container_key],
                )
                del self._last_log_timestamp[container_key]
                self._empty_cycles[container_key] = 0
                last_timestamp = None

            logs = await self.get_container_logs(
                container_id=container["id"],
                container_name=container["name"],
                since=last_timestamp,
                tail=tail if last_timestamp is None else None,
                compose_project=container.get("compose_project"),
                compose_service=container.get("compose_service"),
            )

            if logs:
                all_logs.extend(logs)
                containers_with_logs += 1
                newest_log = max(logs, key=lambda x: x["timestamp"])
                self._last_log_timestamp[container_key] = newest_log["timestamp"] + timedelta(milliseconds=1)
                self._empty_cycles[container_key] = 0
            else:
                self._empty_cycles[container_key] = self._empty_cycles.get(container_key, 0) + 1

        # Periodic diagnostic (every 10 cycles ≈ every 5 minutes at 30s interval)
        if self._collection_cycle % 10 == 0:
            logger.warning(
                "[diagnostic] Log collection status",
                cycle=self._collection_cycle,
                running_containers=len(running),
                containers_with_logs=containers_with_logs,
                total_logs=len(all_logs),
                tracked_containers=len(self._last_log_timestamp),
            )

        # Cleanup stale entries for containers that no longer exist
        running_ids = {c["id"] for c in running}
        stale_keys = [k for k in self._last_log_timestamp if k not in running_ids]
        for k in stale_keys:
            del self._last_log_timestamp[k]
            self._empty_cycles.pop(k, None)

        return all_logs

    async def collect_all_stats(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Collect host metrics and all container stats.

        Each part is collected independently so partial data is still returned.
        Per-container GPU metrics are collected by mapping nvidia-smi process PIDs
        to Docker container PIDs.
        """
        # Host metrics - always return at least a skeleton
        try:
            host_metrics = await self.get_host_metrics()
        except Exception as e:
            logger.error("Failed to collect host metrics, using defaults", error=str(e))
            host_metrics = {
                "host": self.host_name,
                "timestamp": datetime.utcnow(),
                "cpu_percent": 0.0,
                "memory_total_mb": 0.0,
                "memory_used_mb": 0.0,
                "memory_percent": 0.0,
                "disk_total_gb": 0.0,
                "disk_used_gb": 0.0,
                "disk_percent": 0.0,
                "gpu_percent": None,
                "gpu_memory_used_mb": None,
                "gpu_memory_total_mb": None,
            }

        # Collect per-process GPU metrics once (before iterating containers)
        gpu_processes = []
        try:
            gpu_processes = utils.get_gpu_process_metrics()
        except Exception as e:
            logger.debug("Per-process GPU metrics unavailable", error=str(e))

        # Build PID -> GPU metrics lookup
        gpu_by_pid: Dict[int, Dict] = {}
        for gp in gpu_processes:
            gpu_by_pid[gp["pid"]] = gp

        # Container stats - skip individual failures
        container_stats = []
        try:
            containers = await self.get_containers()
            running = [c for c in containers if c.get("status") == "running"]

            for container in running:
                try:
                    stats = await self.get_container_stats(container["id"], container["name"])
                    if stats:
                        # Enrich with per-container GPU metrics
                        if gpu_by_pid:
                            try:
                                pids = await self.get_container_pids(container["id"])
                                container_gpu_mem = 0.0
                                container_gpu_sm_values = []
                                for pid in pids:
                                    gm = gpu_by_pid.get(pid)
                                    if gm:
                                        container_gpu_mem += gm.get("gpu_memory_used_mb") or 0
                                        sm = gm.get("gpu_sm_percent")
                                        if sm is not None:
                                            container_gpu_sm_values.append(sm)
                                if container_gpu_mem > 0:
                                    stats["gpu_memory_used_mb"] = round(container_gpu_mem, 1)
                                if container_gpu_sm_values:
                                    stats["gpu_percent"] = round(sum(container_gpu_sm_values) / len(container_gpu_sm_values), 1)
                            except Exception as e:
                                logger.debug("Failed to get GPU for container",
                                             container=container.get("name"), error=str(e))
                        container_stats.append(stats)
                except Exception as e:
                    logger.warning("Failed to collect stats for container",
                                  container=container.get("name"), error=str(e))
        except Exception as e:
            logger.error("Failed to list containers for stats", error=str(e))

        return host_metrics, container_stats
