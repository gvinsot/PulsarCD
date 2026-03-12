"""Docker API client for direct Docker daemon communication."""

import asyncio
import aiohttp
import json
import re
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote

import structlog

from .config import HostConfig
from .models import (
    ContainerInfo, ContainerStats, ContainerStatus,
    HostMetrics, LogEntry, ContainerAction
)
from . import utils

logger = structlog.get_logger()


class DockerAPIClient:
    """Direct Docker API client (via socket or TCP)."""

    def __init__(self, host_config: HostConfig):
        self.config = host_config
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.BaseConnector] = None
        self._closing = False  # Flag to track graceful shutdown
        self._local_node_id: Optional[str] = None  # Cached local node ID for Swarm filtering

        docker_url = host_config.docker_url or "unix:///var/run/docker.sock"

        # Determine connection type
        if docker_url.startswith("unix://"):
            # Unix socket connection
            socket_path = docker_url.replace("unix://", "")
            self._base_url = "http://localhost"
            self._connector = aiohttp.UnixConnector(path=socket_path)
            logger.info("Docker API client (socket)", host=self.config.name, socket=socket_path)
        else:
            # TCP connection (http:// or tcp://)
            self._base_url = docker_url.replace("tcp://", "http://")
            self._connector = None
            logger.info("Docker API client (TCP)", host=self.config.name, url=self._base_url)
    
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
        # Skip requests if we're shutting down
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
            # Don't log errors during shutdown (session closed, server disconnected, etc.)
            if not self._closing:
                logger.error("Docker API request failed", endpoint=endpoint, error=str(e))
            return None, 500
        except Exception as e:
            if not self._closing:
                logger.error("Docker API request failed", endpoint=endpoint, error=str(e))
            return None, 500

    async def _get_local_node_id(self) -> Optional[str]:
        """Get the local node ID for Swarm filtering.

        Caches the result since node ID doesn't change.
        """
        if self._local_node_id:
            return self._local_node_id

        data, status = await self._request("GET", "/info")
        if status == 200 and data:
            swarm_info = data.get("Swarm", {})
            node_id = swarm_info.get("NodeID")
            if node_id:
                self._local_node_id = node_id
                logger.debug("Got local Swarm node ID", node_id=node_id[:12])
        return self._local_node_id

    async def _get_local_container_ids(self) -> set:
        """Get container IDs running on this specific Swarm node.

        Used to filter containers when swarm_autodiscover is enabled.
        """
        local_node_id = await self._get_local_node_id()
        if not local_node_id:
            return set()

        tasks = await self.get_swarm_tasks()
        local_container_ids = set()
        for task in tasks:
            # Match node ID (may be truncated)
            if (task["node_id"].startswith(local_node_id[:12]) or
                    local_node_id.startswith(task["node_id"][:12])):
                if task["container_id"]:
                    local_container_ids.add(task["container_id"][:12])

        return local_container_ids
    
    async def get_containers(self) -> List[ContainerInfo]:
        """Get list of all Docker containers.

        When swarm_autodiscover is enabled, only returns containers running on
        the local node (worker containers are handled by SwarmProxyClient).
        """
        data, status = await self._request("GET", "/containers/json?all=true")

        if status != 200 or not data:
            return []

        # If swarm_autodiscover is enabled, filter to only local node containers
        local_container_ids = None
        if self.config.swarm_autodiscover:
            local_container_ids = await self._get_local_container_ids()
            logger.debug("Filtering containers to local node",
                        local_count=len(local_container_ids))

        containers = []
        for c in data:
            try:
                container_id = c["Id"][:12]

                # Filter by local node if autodiscover is enabled
                if local_container_ids is not None and container_id not in local_container_ids:
                    continue

                # Parse status
                state = c.get("State", "").lower()
                try:
                    container_status = ContainerStatus(state)
                except ValueError:
                    container_status = ContainerStatus.EXITED

                # Parse labels
                labels = c.get("Labels", {}) or {}

                # Parse created timestamp
                created_ts = c.get("Created", 0)
                created = datetime.fromtimestamp(created_ts) if created_ts else datetime.now()

                # Parse name (remove leading /)
                names = c.get("Names", ["/unknown"])
                name = names[0].lstrip("/") if names else "unknown"

                # Parse ports
                ports = {}
                for port in c.get("Ports", []):
                    private = f"{port.get('PrivatePort', '')}/{port.get('Type', 'tcp')}"
                    public = f"{port.get('IP', '')}:{port.get('PublicPort', '')}" if port.get('PublicPort') else None
                    if public:
                        ports[private] = public

                # Get compose/stack project and service
                # Try Compose labels first, then Swarm stack labels
                compose_project = (labels.get("com.docker.compose.project") or
                                   labels.get("com.docker.stack.namespace"))
                compose_service = (labels.get("com.docker.compose.service") or
                                   labels.get("com.docker.swarm.service.name"))

                container = ContainerInfo(
                    id=container_id,
                    name=name,
                    image=c.get("Image", "unknown"),
                    status=container_status,
                    created=created,
                    host=self.config.name,
                    compose_project=compose_project,
                    compose_service=compose_service,
                    ports=ports,
                    labels=labels,
                )
                containers.append(container)

            except Exception as e:
                logger.error("Failed to parse container", error=str(e))

        return containers
    
    async def get_container_stats(self, container_id: str, container_name: str) -> Optional[ContainerStats]:
        """Get container resource statistics."""
        # stream=false for one-shot stats
        data, status = await self._request("GET", f"/containers/{container_id}/stats?stream=false")

        if status != 200 or not data:
            return None

        try:
            cpu_stats = data.get("cpu_stats", {})
            precpu_stats = data.get("precpu_stats", {})

            # Calculate CPU percentage
            # Handle different Docker versions and platforms (Linux, Windows, macOS)
            cpu_percent = 0.0
            num_cpus = cpu_stats.get("online_cpus") or len(cpu_stats.get("cpu_usage", {}).get("percpu_usage", [])) or 1

            cpu_usage = cpu_stats.get("cpu_usage", {})
            precpu_usage = precpu_stats.get("cpu_usage", {})

            cpu_delta = cpu_usage.get("total_usage", 0) - precpu_usage.get("total_usage", 0)

            # Try system_cpu_usage first (Linux with cgroup v1)
            if "system_cpu_usage" in cpu_stats and "system_cpu_usage" in precpu_stats:
                system_delta = cpu_stats["system_cpu_usage"] - precpu_stats["system_cpu_usage"]
                if system_delta > 0 and cpu_delta > 0:
                    cpu_percent = (cpu_delta / system_delta) * num_cpus * 100.0
            else:
                # Fallback for cgroup v2, Windows, macOS - use usage_in_kernelmode + usage_in_usermode
                # Or calculate based on time period if available
                if cpu_delta > 0:
                    # Estimate based on 1 second polling interval (stats are roughly 1s apart)
                    # CPU time is in nanoseconds, so divide by 1e9 to get seconds
                    cpu_percent = (cpu_delta / 1e9) * 100.0 / num_cpus
                    # Cap at reasonable value
                    cpu_percent = min(cpu_percent, 100.0 * num_cpus)

            # Memory stats
            memory_stats = data.get("memory_stats", {})
            memory_usage = memory_stats.get("usage", 0) / (1024 * 1024)  # MB
            memory_limit = memory_stats.get("limit", 0) / (1024 * 1024)  # MB

            # Handle unlimited memory (very large limit value)
            if memory_limit > 1e12:  # > 1 PB, essentially unlimited
                memory_limit = memory_usage * 2 if memory_usage > 0 else 1024  # Show relative usage

            memory_percent = (memory_usage / memory_limit * 100) if memory_limit > 0 else 0

            # Network stats
            networks = data.get("networks", {})
            net_rx = sum(n.get("rx_bytes", 0) for n in networks.values())
            net_tx = sum(n.get("tx_bytes", 0) for n in networks.values())

            # Block I/O stats
            blkio = data.get("blkio_stats", {}).get("io_service_bytes_recursive", []) or []
            block_read = sum(s.get("value", 0) for s in blkio if s.get("op", "").lower() == "read")
            block_write = sum(s.get("value", 0) for s in blkio if s.get("op", "").lower() == "write")

            return ContainerStats(
                container_id=container_id,
                container_name=container_name,
                host=self.config.name,
                timestamp=datetime.utcnow(),
                cpu_percent=round(cpu_percent, 2),
                memory_usage_mb=round(memory_usage, 2),
                memory_limit_mb=round(memory_limit, 2),
                memory_percent=round(memory_percent, 2),
                network_rx_bytes=net_rx,
                network_tx_bytes=net_tx,
                block_read_bytes=block_read,
                block_write_bytes=block_write,
            )

        except Exception as e:
            logger.error("Failed to parse container stats", container=container_id, error=str(e))
            return None
    
    async def get_host_metrics(self) -> HostMetrics:
        """Get host-level metrics (limited via Docker API).

        Each metric source is collected independently so that a failure
        in one (e.g. GPU not available) does not prevent the others.
        """
        cpu_percent = 0.0
        memory_total_mb = 0.0
        memory_used_mb = 0.0

        # Docker API metrics
        try:
            data, status = await self._request("GET", "/info")
            if status == 200 and data:
                memory_total_mb = data.get("MemTotal", 0) / (1024 * 1024)
                containers = await self.get_containers()
                running = [c for c in containers if c.status == ContainerStatus.RUNNING]

                for container in running[:10]:
                    try:
                        stats = await self.get_container_stats(container.id, container.name)
                        if stats:
                            memory_used_mb += stats.memory_usage_mb
                            cpu_percent += stats.cpu_percent
                    except Exception as e:
                        logger.warning("Failed to get stats for container",
                                      container=container.name, error=str(e))
        except Exception as e:
            logger.warning("Failed to collect Docker metrics", error=str(e))

        memory_percent = (memory_used_mb / memory_total_mb * 100) if memory_total_mb > 0 else 0

        # GPU metrics (independent)
        gpu_percent = None
        gpu_mem_used = None
        gpu_mem_total = None
        try:
            gpu_percent, gpu_mem_used, gpu_mem_total = await self._get_gpu_metrics()
        except Exception as e:
            logger.warning("Failed to collect GPU metrics", error=str(e))

        return HostMetrics(
            host=self.config.name,
            timestamp=datetime.utcnow(),
            cpu_percent=round(cpu_percent, 2),
            memory_total_mb=round(memory_total_mb, 2),
            memory_used_mb=round(memory_used_mb, 2),
            memory_percent=round(memory_percent, 2),
            disk_total_gb=0,
            disk_used_gb=0,
            disk_percent=0,
            gpu_percent=gpu_percent,
            gpu_memory_used_mb=gpu_mem_used,
            gpu_memory_total_mb=gpu_mem_total,
        )
    
    async def _get_gpu_metrics(self) -> tuple:
        """Try to get GPU metrics using nvidia-smi or rocm-smi."""
        # Try AMD GPU first (rocm-smi with CSV format - includes all info in one call)
        try:
            result = subprocess.run(
                ["rocm-smi", "--showuse", "--showmeminfo", "vram", "--csv"],
                capture_output=True,
                text=True,
                timeout=5
            )
            logger.debug("rocm-smi output", returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
            if result.returncode == 0 and result.stdout.strip():
                gpu_percent, gpu_mem_used, gpu_mem_total = utils.parse_rocm_smi_csv(result.stdout)
                if gpu_percent is not None or gpu_mem_used is not None:
                    return gpu_percent, gpu_mem_used, gpu_mem_total
        except FileNotFoundError:
            logger.debug("rocm-smi not found, trying nvidia-smi")
        except subprocess.TimeoutExpired:
            logger.warning("rocm-smi command timed out")
        except Exception as e:
            logger.warning("rocm-smi failed", error=str(e))
        
        # Fallback to NVIDIA GPU (nvidia-smi)
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"],
                capture_output=True,
                text=True,
                timeout=5
            )
            logger.debug("nvidia-smi output", returncode=result.returncode, stdout=result.stdout)
            if result.returncode == 0 and result.stdout.strip():
                return utils.parse_nvidia_smi_csv(result.stdout)
        except FileNotFoundError:
            logger.debug("nvidia-smi not found")
        except subprocess.TimeoutExpired:
            logger.warning("nvidia-smi command timed out")
        except Exception as e:
            logger.warning("nvidia-smi failed", error=str(e))
        
        return None, None, None
    
    async def get_container_logs(
        self,
        container_id: str,
        container_name: str,
        since: Optional[datetime] = None,
        tail: Optional[int] = 500,
        compose_project: Optional[str] = None,
        compose_service: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> List[LogEntry]:
        """Get container logs via Docker API.
        
        If task_id is provided (for Swarm containers), uses /tasks/{task_id}/logs
        which works for containers on any node in the swarm.
        Otherwise uses /containers/{container_id}/logs which only works locally.
        """
        params = ["timestamps=true", "stdout=true", "stderr=true"]
        
        if since:
            # Docker API uses Unix timestamp
            params.append(f"since={int(since.timestamp())}")
        elif tail:
            params.append(f"tail={tail}")
        
        # Use tasks API for swarm containers, containers API for local
        if task_id:
            endpoint = f"/tasks/{task_id}/logs?{'&'.join(params)}"
            logger.debug("Fetching swarm task logs", task_id=task_id, container=container_id)
        else:
            endpoint = f"/containers/{container_id}/logs?{'&'.join(params)}"
        
        session = await self._get_session()
        url = f"{self._base_url}{endpoint}"
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    # If task logs fail, try container logs as fallback
                    if task_id:
                        logger.debug("Task logs failed, trying container API", task_id=task_id)
                        return await self.get_container_logs(
                            container_id, container_name, since, tail,
                            compose_project, compose_service, task_id=None
                        )
                    return []
                
                # Docker logs come as a stream with header bytes
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
    ) -> List[LogEntry]:
        """Parse Docker log stream format."""
        entries = []
        offset = 0
        
        while offset < len(raw_data):
            # Docker log format: [8 bytes header][payload]
            # Header: [stream_type(1), 0, 0, 0, size(4)]
            if offset + 8 > len(raw_data):
                break
                
            header = raw_data[offset:offset + 8]
            stream_type = header[0]  # 1=stdout, 2=stderr
            size = int.from_bytes(header[4:8], byteorder='big')
            
            if offset + 8 + size > len(raw_data):
                # Fallback: try parsing as plain text
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
    ) -> Optional[LogEntry]:
        """Parse a log line with timestamp."""
        # Filter out known noise
        if utils.should_filter_log_line(line):
            return None
        
        # Extract timestamp and message
        timestamp, message = utils.extract_timestamp_and_message(line)
        
        # Parse log level, HTTP status, and structured fields
        level, http_status, parsed_fields = utils.parse_log_message(message)
        
        return LogEntry(
            timestamp=timestamp,
            host=self.config.name,
            container_id=container_id,
            container_name=container_name,
            compose_project=compose_project,
            compose_service=compose_service,
            stream=stream,
            message=message,
            level=level,
            http_status=http_status,
            parsed_fields=parsed_fields,
        )
    
    async def execute_container_action(self, container_id: str, action: ContainerAction) -> Tuple[bool, str]:
        """Execute an action on a container."""
        action_map = {
            ContainerAction.START: ("POST", f"/containers/{container_id}/start"),
            ContainerAction.STOP: ("POST", f"/containers/{container_id}/stop"),
            ContainerAction.RESTART: ("POST", f"/containers/{container_id}/restart"),
            ContainerAction.PAUSE: ("POST", f"/containers/{container_id}/pause"),
            ContainerAction.UNPAUSE: ("POST", f"/containers/{container_id}/unpause"),
            ContainerAction.REMOVE: ("DELETE", f"/containers/{container_id}?force=true"),
        }
        
        if action not in action_map:
            return False, f"Unknown action: {action}"
        
        method, endpoint = action_map[action]
        data, status = await self._request(method, endpoint)
        
        if status in (200, 204):
            return True, f"Action {action.value} completed successfully"
        else:
            error_msg = data if isinstance(data, str) else json.dumps(data) if data else "Unknown error"
            return False, error_msg

    async def remove_service(self, service_name: str) -> Tuple[bool, str]:
        """Remove a Docker Swarm service.
        
        Uses the Docker API DELETE /services/{id} endpoint.
        
        Args:
            service_name: The full service name (e.g., stackname_servicename)
            
        Returns:
            Tuple of (success, message)
        """
        from urllib.parse import quote
        safe_name = quote(service_name, safe='')
        data, status = await self._request("DELETE", f"/services/{safe_name}")
        
        if status == 200 or status == 204:
            return True, f"Service '{service_name}' removed successfully"
        elif status == 404:
            return False, f"Service '{service_name}' not found"
        else:
            error_msg = data if isinstance(data, str) else str(data)
            return False, f"Failed to remove service '{service_name}': {error_msg}"

    async def force_update_service(self, service_name: str) -> Tuple[bool, str]:
        """Force-update a Swarm service, effectively restarting all its tasks.
        
        Equivalent to `docker service update --force <service>`.
        This works by incrementing the ForceUpdate counter in the task template.
        
        Args:
            service_name: The full service name (e.g., stackname_servicename)
            
        Returns:
            Tuple of (success, message)
        """
        from urllib.parse import quote
        safe_name = quote(service_name, safe='')
        
        # First, get current service spec
        data, status = await self._request("GET", f"/services/{safe_name}")
        if status != 200 or not data:
            return False, f"Service '{service_name}' not found"
        
        try:
            version = data.get("Version", {}).get("Index")
            spec = data.get("Spec", {})
            
            # Increment ForceUpdate counter to trigger a rolling restart
            task_template = spec.get("TaskTemplate", {})
            current_force = task_template.get("ForceUpdate", 0)
            task_template["ForceUpdate"] = current_force + 1
            
            # Build registry auth header so workers can re-pull if needed
            # (same logic as update_service_image)
            extra_headers = {}
            try:
                import os, base64
                docker_config_path = os.path.expanduser("~/.docker/config.json")
                if os.path.exists(docker_config_path):
                    with open(docker_config_path, 'r') as _f:
                        _cfg = json.load(_f)
                    _auths = _cfg.get("auths", {})
                    if _auths:
                        _auth_payload = json.dumps({"auths": _auths}).encode()
                        extra_headers["X-Registry-Auth"] = base64.b64encode(_auth_payload).decode()
            except Exception:
                pass

            # Update the service
            request_kwargs = {"json": spec}
            if extra_headers:
                request_kwargs["headers"] = extra_headers
            update_data, update_status = await self._request(
                "POST",
                f"/services/{safe_name}/update?version={version}",
                **request_kwargs,
            )

            if update_status == 200:
                return True, f"Service '{service_name}' restarted successfully (force update)"
            else:
                error_msg = update_data if isinstance(update_data, str) else str(update_data)
                return False, f"Failed to restart service '{service_name}': {error_msg}"
        except Exception as e:
            return False, f"Failed to restart service '{service_name}': {e}"

    async def update_service_image(self, service_name: str, new_tag: str) -> Tuple[bool, str]:
        """Update a Swarm service's image tag.
        
        Updates the service to use a new image tag while keeping all other
        settings intact. This triggers a rolling update of the service.
        
        NOTE: For private registries, Docker must already be logged in on the
        Swarm manager. The --with-registry-auth flag (or X-Registry-Auth header)
        is used to propagate credentials to worker nodes.
        
        Args:
            service_name: The full service name (e.g., stackname_servicename)
            new_tag: The new image tag to use (e.g., "v1.2.3" or "latest")
            
        Returns:
            Tuple of (success, message)
        """
        import base64
        from urllib.parse import quote
        safe_name = quote(service_name, safe='')
        
        # Strip leading 'v' from tag if present (GitHub tags are v1.0.5, Docker images are 1.0.5)
        if new_tag.startswith('v'):
            new_tag = new_tag[1:]
        
        logger.info("[DOCKER-CLIENT] update_service_image called", service=service_name, new_tag=new_tag)
        
        # First, get current service spec
        data, status = await self._request("GET", f"/services/{safe_name}")
        logger.info("[DOCKER-CLIENT] Got service spec", service=service_name, status=status, has_data=bool(data))
        
        if status != 200 or not data:
            error_msg = f"Service '{service_name}' not found (status={status})"
            logger.error("[DOCKER-CLIENT] Service not found", service=service_name, status=status)
            return False, error_msg
        
        try:
            version = data.get("Version", {}).get("Index")
            spec = data.get("Spec", {})
            
            # Get current image and update the tag
            task_template = spec.get("TaskTemplate", {})
            container_spec = task_template.get("ContainerSpec", {})
            current_image = container_spec.get("Image", "")
            
            logger.info("[DOCKER-CLIENT] Current image", service=service_name, current_image=current_image)
            
            if not current_image:
                error_msg = f"Service '{service_name}' has no image configured"
                logger.error("[DOCKER-CLIENT] No image configured", service=service_name)
                return False, error_msg
            
            # Parse image name and replace tag
            # Handle formats: image:tag, image:tag@sha256:..., registry/image:tag
            # Remove any digest suffix first
            if "@sha256:" in current_image:
                current_image = current_image.split("@sha256:")[0]
            
            # Split image name and tag
            if ":" in current_image:
                image_base = current_image.rsplit(":", 1)[0]
            else:
                image_base = current_image
            
            new_image = f"{image_base}:{new_tag}"
            container_spec["Image"] = new_image
            
            logger.info("[DOCKER-CLIENT] New image", service=service_name, image_base=image_base, new_tag=new_tag, new_image=new_image)
            
            # Increment ForceUpdate to ensure the update is applied even if nothing else changed
            current_force = task_template.get("ForceUpdate", 0)
            task_template["ForceUpdate"] = current_force + 1
            logger.info("[DOCKER-CLIENT] Force update incremented", service=service_name, old_force=current_force, new_force=current_force + 1)
            
            # Build registry auth header for private registries
            # Docker expects base64-encoded JSON: {"username": "...", "password": "...", "serveraddress": "..."}
            # For images already in the store, this propagates credentials to workers
            headers = {}
            
            # Extract registry from image name
            registry = None
            if "/" in image_base:
                first_part = image_base.split("/")[0]
                # Check if it looks like a registry (contains '.' or ':' or is 'localhost')
                if "." in first_part or ":" in first_part or first_part == "localhost":
                    registry = first_part
                    logger.info("[DOCKER-CLIENT] Detected registry", registry=registry)
            
            # Try to get registry auth from Docker's config.json
            # This is what docker login stores and what --with-registry-auth uses
            try:
                import os
                docker_config_path = os.path.expanduser("~/.docker/config.json")
                if os.path.exists(docker_config_path):
                    with open(docker_config_path, 'r') as f:
                        docker_config = json.load(f)
                    
                    auths = docker_config.get("auths", {})
                    auth_key = registry or "https://index.docker.io/v1/"
                    
                    # Try exact match first, then try with/without https://
                    auth_entry = auths.get(auth_key) or auths.get(f"https://{registry}") or auths.get(registry)
                    
                    if auth_entry and auth_entry.get("auth"):
                        # The "auth" field is already base64 encoded "user:password"
                        auth_data = {
                            "identitytoken": "",  # For OAuth tokens
                            "registrytoken": ""   # For bearer tokens
                        }
                        
                        # Decode the auth to get username and password
                        try:
                            decoded = base64.b64decode(auth_entry["auth"]).decode('utf-8')
                            if ":" in decoded:
                                username, password = decoded.split(":", 1)
                                auth_data["username"] = username
                                auth_data["password"] = password
                                auth_data["serveraddress"] = registry or "https://index.docker.io/v1/"
                                
                                # Encode for X-Registry-Auth header
                                auth_json = json.dumps(auth_data)
                                headers["X-Registry-Auth"] = base64.b64encode(auth_json.encode()).decode()
                                logger.info("[DOCKER-CLIENT] Registry auth added", registry=registry, username=username[:3] + "***")
                        except Exception as e:
                            logger.warning("[DOCKER-CLIENT] Failed to decode registry auth", error=str(e))
                    else:
                        logger.info("[DOCKER-CLIENT] No registry auth found for", registry=auth_key)
            except Exception as e:
                logger.warning("[DOCKER-CLIENT] Could not read Docker config", error=str(e))
            
            # Update the service
            logger.info("[DOCKER-CLIENT] Sending service update request", service=service_name, version=version, has_auth=bool(headers))
            
            update_data, update_status = await self._request(
                "POST",
                f"/services/{safe_name}/update?version={version}",
                json=spec,
                headers=headers if headers else None,
            )
            
            logger.info("[DOCKER-CLIENT] Service update result", service=service_name, status=update_status, response=str(update_data)[:300] if update_data else '')
            
            if update_status == 200:
                success_msg = f"Service '{service_name}' updated to image '{new_image}'"
                logger.info("[DOCKER-CLIENT] Service updated successfully", service=service_name, new_image=new_image)
                return True, success_msg
            else:
                error_msg = update_data if isinstance(update_data, str) else str(update_data)
                logger.error("[DOCKER-CLIENT] Service update failed", service=service_name, status=update_status, error=error_msg[:500])
                return False, f"Failed to update service '{service_name}': {error_msg}"
        except Exception as e:
            import traceback
            error_detail = f"{type(e).__name__}: {e}"
            logger.error("[DOCKER-CLIENT] Exception in update_service_image", service=service_name, error=error_detail, traceback=traceback.format_exc())
            return False, f"Failed to update service '{service_name}': {error_detail}"

    async def get_service_logs(self, service_name: str, tail: int = 200) -> List[Dict[str, Any]]:
        """Get logs for a Docker Swarm service using the Docker API.
        
        Uses /services/{id}/logs endpoint which aggregates logs from all
        tasks/replicas of the service.
        
        Args:
            service_name: The full service name (e.g., stackname_servicename)
            tail: Number of log lines to retrieve
            
        Returns:
            List of log entry dicts with timestamp, message, stream, service
        """
        params = [
            "timestamps=true",
            "stdout=true",
            "stderr=true",
            f"tail={tail}",
        ]
        
        endpoint = f"/services/{quote(service_name, safe='')}/logs?{'&'.join(params)}"
        
        session = await self._get_session()
        if not session:
            return []
        
        url = f"{self._base_url}{endpoint}"
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error("Failed to get service logs", service=service_name, status=response.status)
                    return []
                
                raw_data = await response.read()
                logs = self._parse_service_logs(raw_data, service_name)
                
                # Detect if logs contain "incomplete log stream" / "has not been scheduled" error
                # This happens when the service's tasks are not running (e.g., not deployed)
                if logs and any(
                    "incomplete log stream" in (entry.get("message", ""))
                    or "has not been scheduled" in (entry.get("message", ""))
                    for entry in logs
                ):
                    # Return task status info instead of the unhelpful error
                    task_info = await self.get_service_tasks(service_name)
                    if task_info:
                        return {"type": "service_tasks", "tasks": task_info, "service": service_name}
                
                return logs
                
        except Exception as e:
            logger.error("Failed to get service logs", service=service_name, error=str(e))
            return []
    
    def _parse_service_logs(self, raw_data: bytes, service_name: str) -> List[Dict[str, Any]]:
        """Parse Docker service log stream format.
        
        Service logs use the same multiplexed stream format as container logs:
        [8 bytes header][payload] where header is [stream_type(1), 0, 0, 0, size(4)]
        """
        entries = []
        offset = 0
        
        while offset < len(raw_data):
            if offset + 8 > len(raw_data):
                break
            
            header = raw_data[offset:offset + 8]
            stream_type = header[0]  # 1=stdout, 2=stderr
            size = int.from_bytes(header[4:8], byteorder='big')
            
            if offset + 8 + size > len(raw_data):
                break
            
            payload = raw_data[offset + 8:offset + 8 + size]
            offset += 8 + size
            
            try:
                line = payload.decode('utf-8', errors='replace').strip()
                if not line:
                    continue
                
                # Parse timestamp if present (Docker adds timestamps)
                timestamp = None
                message = line
                if len(line) > 30 and (line[4] == '-' or line[:4].isdigit()):
                    # Try to extract ISO timestamp
                    space_idx = line.find(' ')
                    if space_idx > 20:
                        try:
                            timestamp = line[:space_idx]
                            message = line[space_idx + 1:]
                        except (ValueError, IndexError):
                            timestamp = None
                            message = line
                
                entry = {
                    "timestamp": timestamp or datetime.utcnow().isoformat() + "Z",
                    "message": message,
                    "stream": "stderr" if stream_type == 2 else "stdout",
                    "service": service_name,
                }
                entries.append(entry)
                
            except Exception:
                continue
        
        # Fallback: if no entries parsed with multiplexed format, try plain text
        if not entries and raw_data:
            try:
                text = raw_data.decode('utf-8', errors='replace')
                for line in text.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    
                    timestamp = None
                    message = line
                    if len(line) > 30 and (line[4] == '-' or line[:4].isdigit()):
                        space_idx = line.find(' ')
                        if space_idx > 20:
                            try:
                                timestamp = line[:space_idx]
                                message = line[space_idx + 1:]
                            except (ValueError, IndexError):
                                pass
                    
                    entries.append({
                        "timestamp": timestamp or datetime.utcnow().isoformat() + "Z",
                        "message": message,
                        "stream": "stdout",
                        "service": service_name,
                    })
            except Exception:
                pass
        
        return entries

    async def get_service_tasks(self, service_name: str) -> List[Dict[str, Any]]:
        """Get all tasks for a specific service (equivalent to docker service ps --no-trunc).
        
        Returns all tasks including failed, pending, and shutdown tasks,
        providing diagnostic info when a service is not running.
        
        Args:
            service_name: The full service name (e.g., stackname_servicename)
            
        Returns:
            List of task info dicts with id, state, desired_state, error, node, timestamps
        """
        import json as json_mod
        filters = json_mod.dumps({"service": [service_name]})
        data, status = await self._request("GET", f"/tasks?filters={quote(filters, safe='')}")
        
        if status != 200 or not data:
            return []
        
        # Fetch nodes for hostname resolution
        nodes = await self.get_swarm_nodes()
        node_hostnames = {n["id"]: n["hostname"] for n in nodes}
        # Also map full IDs
        nodes_raw, _ = await self._request("GET", "/nodes")
        full_node_map = {}
        if nodes_raw:
            for n in nodes_raw:
                full_node_map[n.get("ID", "")] = n.get("Description", {}).get("Hostname", "unknown")
        
        tasks = []
        for task in data:
            task_status = task.get("Status", {})
            container_status = task_status.get("ContainerStatus", {})
            task_spec = task.get("Spec", {})
            container_spec = task_spec.get("ContainerSpec", {})
            node_id = task.get("NodeID", "")
            
            task_info = {
                "id": task.get("ID", ""),
                "node": full_node_map.get(node_id, node_id[:12] if node_id else "(no node)"),
                "desired_state": task.get("DesiredState", "unknown"),
                "state": task_status.get("State", "unknown"),
                "error": task_status.get("Err", ""),
                "message": task_status.get("Message", ""),
                "image": container_spec.get("Image", "unknown"),
                "created_at": task.get("CreatedAt", ""),
                "updated_at": task.get("UpdatedAt", ""),
                "container_id": container_status.get("ContainerID", ""),
            }
            tasks.append(task_info)
        
        # Sort by updated_at descending (most recent first)
        tasks.sort(key=lambda t: t.get("updated_at", ""), reverse=True)
        
        return tasks

    async def get_swarm_stacks(self) -> Dict[str, List[str]]:
        """Get list of Docker Swarm stacks and their services.
        
        Returns:
            Dict mapping stack_name -> list of service names
        """
        # Docker API doesn't have stack endpoints, use subprocess
        import subprocess
        try:
            result = subprocess.run(
                ["docker", "stack", "ls", "--format", "{{.Name}}"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                return {}
            
            stacks = {}
            for stack_name in result.stdout.strip().split('\n'):
                stack_name = stack_name.strip()
                if not stack_name:
                    continue
                
                # Get services for this stack
                services_result = subprocess.run(
                    ["docker", "stack", "services", stack_name, "--format", "{{.Name}}"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if services_result.returncode == 0:
                    services = [s.strip() for s in services_result.stdout.strip().split('\n') if s.strip()]
                    stacks[stack_name] = services
                else:
                    stacks[stack_name] = []
            
            return stacks
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            return {}

    async def remove_stack(self, stack_name: str) -> Tuple[bool, str]:
        """Remove a Docker Swarm stack.
        
        Uses docker stack rm command since there's no direct Docker API endpoint for stacks.
        
        Args:
            stack_name: Name of the stack to remove
            
        Returns:
            Tuple of (success, message)
        """
        import subprocess
        try:
            result = subprocess.run(
                ["docker", "stack", "rm", stack_name],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return True, f"Stack '{stack_name}' removed successfully"
            else:
                error = result.stderr.strip() or result.stdout.strip()
                return False, f"Failed to remove stack '{stack_name}': {error}"
        except FileNotFoundError:
            return False, "Docker CLI not available"
        except subprocess.TimeoutExpired:
            return False, f"Timeout removing stack '{stack_name}'"

    async def exec_command(self, container_id: str, command: List[str]) -> Tuple[bool, str]:
        """Execute a command inside a container using Docker exec API.

        Args:
            container_id: The container ID
            command: Command as list of strings (e.g., ["printenv"])

        Returns:
            Tuple of (success, output/error)
        """
        # Step 1: Create exec instance
        exec_config = {
            "AttachStdout": True,
            "AttachStderr": True,
            "Cmd": command
        }

        data, status = await self._request(
            "POST",
            f"/containers/{container_id}/exec",
            json=exec_config
        )

        if status != 201 or not data:
            error_msg = data if isinstance(data, str) else json.dumps(data) if data else "Failed to create exec"
            return False, error_msg

        exec_id = data.get("Id")
        if not exec_id:
            return False, "No exec ID returned"

        # Step 2: Start exec and get output
        start_config = {
            "Detach": False,
            "Tty": False
        }

        session = await self._get_session()
        url = f"{self._base_url}/exec/{exec_id}/start"

        try:
            async with session.post(url, json=start_config) as response:
                if response.status != 200:
                    return False, f"Exec start failed with status {response.status}"

                # Read the output - Docker sends multiplexed stream
                output = await response.read()

                # Parse multiplexed stream (header: 8 bytes, then payload)
                result = []
                pos = 0
                while pos < len(output):
                    if pos + 8 > len(output):
                        break
                    # Header: 1 byte type, 3 bytes padding, 4 bytes size (big endian)
                    size = int.from_bytes(output[pos+4:pos+8], 'big')
                    pos += 8
                    if pos + size > len(output):
                        break
                    chunk = output[pos:pos+size].decode('utf-8', errors='replace')
                    result.append(chunk)
                    pos += size

                return True, ''.join(result)
        except Exception as e:
            logger.error("Exec command failed", container_id=container_id, error=str(e))
            return False, str(e)

    async def run_shell_command(self, command: str) -> Tuple[bool, str]:
        """Execute a shell command on the host.
        
        This runs the command locally using asyncio subprocess.
        For Docker API mode, commands are executed on the host running the LogsCrawler.
        
        Args:
            command: Shell command to run
            
        Returns:
            Tuple of (success, output)
        """
        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            output = stdout.decode('utf-8', errors='replace')
            if stderr:
                output += stderr.decode('utf-8', errors='replace')
            return proc.returncode == 0, output.strip()
        except Exception as e:
            logger.error("Shell command failed", command=command[:50], error=str(e))
            return False, str(e)

    # ============== Swarm-specific methods ==============

    async def get_swarm_nodes(self) -> List[Dict[str, Any]]:
        """Get all nodes in the Docker Swarm.

        Returns:
            List of node info dicts with id, hostname, role, status, availability
        """
        data, status = await self._request("GET", "/nodes")

        if status != 200 or not data:
            return []

        nodes = []
        for node in data:
            node_info = {
                "id": node.get("ID", "")[:12],
                "hostname": node.get("Description", {}).get("Hostname", "unknown"),
                "role": node.get("Spec", {}).get("Role", "worker"),
                "status": node.get("Status", {}).get("State", "unknown"),
                "availability": node.get("Spec", {}).get("Availability", "unknown"),
                "addr": node.get("Status", {}).get("Addr", ""),
                "engine_version": node.get("Description", {}).get("Engine", {}).get("EngineVersion", ""),
            }
            nodes.append(node_info)

        return nodes

    async def get_swarm_services(self) -> List[Dict[str, Any]]:
        """Get all services in the Docker Swarm.

        Returns:
            List of service info dicts
        """
        data, status = await self._request("GET", "/services")

        if status != 200 or not data:
            return []

        services = []
        for svc in data:
            spec = svc.get("Spec", {})
            service_info = {
                "id": svc.get("ID", "")[:12],
                "name": spec.get("Name", "unknown"),
                "image": spec.get("TaskTemplate", {}).get("ContainerSpec", {}).get("Image", "unknown"),
                "replicas": spec.get("Mode", {}).get("Replicated", {}).get("Replicas", 0),
                "stack": spec.get("Labels", {}).get("com.docker.stack.namespace", ""),
            }
            services.append(service_info)

        return services

    async def get_service_env(self, service_id: str) -> Optional[Dict[str, str]]:
        """Get environment variables from a Swarm service spec.
        
        This retrieves the env vars configured in the service definition,
        useful for Swarm containers where we can't exec into remote nodes.
        
        Args:
            service_id: The service ID (can be partial)
            
        Returns:
            Dict of env var name -> value, or None if not found
        """
        # Get service details
        data, status = await self._request("GET", f"/services/{service_id}")
        
        if status != 200 or not data:
            return None
        
        try:
            spec = data.get("Spec", {})
            container_spec = spec.get("TaskTemplate", {}).get("ContainerSpec", {})
            env_list = container_spec.get("Env", [])
            
            # Parse "KEY=VALUE" format
            env_vars = {}
            for env in env_list:
                if '=' in env:
                    key, _, value = env.partition('=')
                    env_vars[key] = value
            
            return env_vars
        except Exception as e:
            logger.error("Failed to parse service env", service_id=service_id, error=str(e))
            return None

    async def get_swarm_tasks(self, include_service_info: bool = False) -> List[Dict[str, Any]]:
        """Get all tasks (container instances) across the Swarm.

        This returns information about where each container is running,
        enabling routing commands to the correct node.

        Args:
            include_service_info: If True, fetch service details for each task
                                  (service name, image, labels). Slower but needed
                                  for building ContainerInfo without /containers API.

        Returns:
            List of task info dicts with container_id, node_id, service, status
        """
        data, status = await self._request("GET", "/tasks")

        if status != 200 or not data:
            return []

        # Fetch service info if needed (for building ContainerInfo)
        services_map = {}
        if include_service_info:
            services = await self.get_swarm_services()
            services_map = {s["id"]: s for s in services}

        tasks = []
        for task in data:
            task_status = task.get("Status", {})
            container_status = task_status.get("ContainerStatus", {})
            task_spec = task.get("Spec", {})
            container_spec = task_spec.get("ContainerSpec", {})

            service_id = task.get("ServiceID", "")[:12]
            service_info = services_map.get(service_id, {})

            task_info = {
                "id": task.get("ID", "")[:12],
                "node_id": task.get("NodeID", ""),
                "service_id": service_id,
                "container_id": container_status.get("ContainerID", "")[:12] if container_status.get("ContainerID") else None,
                "state": task_status.get("State", "unknown"),
                "desired_state": task.get("DesiredState", "unknown"),
                "slot": task.get("Slot", 0),
                # Additional info for building ContainerInfo
                "service_name": service_info.get("name", ""),
                "image": container_spec.get("Image") or service_info.get("image", "unknown"),
                "stack": service_info.get("stack", ""),
                "created": task_status.get("Timestamp", ""),
            }

            # Only include running tasks with a container
            if task_info["container_id"] and task_info["state"] == "running":
                tasks.append(task_info)

        return tasks

    async def get_node_containers(self, node_id: str) -> List[ContainerInfo]:
        """Get containers running on a specific Swarm node.

        This uses tasks API to find containers on a node, then gets their details.
        Useful for Swarm routing when you want to query containers on worker nodes.
        """
        tasks = await self.get_swarm_tasks()
        node_tasks = [t for t in tasks if t["node_id"].startswith(node_id)]

        containers = []
        for task in node_tasks:
            if task["container_id"]:
                # Get container details
                data, status = await self._request("GET", f"/containers/{task['container_id']}/json")
                if status == 200 and data:
                    try:
                        labels = data.get("Config", {}).get("Labels", {}) or {}
                        name = data.get("Name", "unknown").lstrip("/")

                        # Get compose/stack project and service
                        compose_project = (labels.get("com.docker.compose.project") or
                                           labels.get("com.docker.stack.namespace"))
                        compose_service = (labels.get("com.docker.compose.service") or
                                           labels.get("com.docker.swarm.service.name"))

                        container = ContainerInfo(
                            id=task["container_id"],
                            name=name,
                            image=data.get("Config", {}).get("Image", "unknown"),
                            status=ContainerStatus.RUNNING,
                            created=datetime.fromisoformat(data.get("Created", "").replace("Z", "+00:00")),
                            host=self.config.name,
                            compose_project=compose_project,
                            compose_service=compose_service,
                            ports={},
                            labels=labels,
                        )
                        containers.append(container)
                    except Exception as e:
                        logger.error("Failed to parse swarm container", task_id=task["id"], error=str(e))

        return containers

    async def get_all_swarm_containers(self) -> Dict[str, List[ContainerInfo]]:
        """Get all containers across all Swarm nodes, grouped by node hostname.

        This is the main method for Swarm routing - it discovers all containers
        in the swarm and their locations, so commands can be routed through
        the manager instead of requiring direct access to worker nodes.

        Note: This builds ContainerInfo from task/service data instead of calling
        /containers/{id}/json, which only works for containers on the local node.

        Returns:
            Dict mapping node hostname to list of containers on that node
        """
        nodes = await self.get_swarm_nodes()
        tasks = await self.get_swarm_tasks(include_service_info=True)

        # Build node_id -> hostname mapping
        node_hostnames = {n["id"]: n["hostname"] for n in nodes}

        # Group tasks by node
        containers_by_node: Dict[str, List[ContainerInfo]] = {}
        for hostname in node_hostnames.values():
            containers_by_node[hostname] = []

        for task in tasks:
            if not task["container_id"]:
                continue

            # Find node hostname
            node_hostname = None
            for node_id, hostname in node_hostnames.items():
                if task["node_id"].startswith(node_id) or node_id.startswith(task["node_id"]):
                    node_hostname = hostname
                    break

            if not node_hostname:
                continue

            try:
                # Build container name from service name and slot (like Docker does)
                service_name = task.get("service_name", "unknown")
                slot = task.get("slot", 0)
                container_name = f"{service_name}.{slot}.{task['id']}"

                # Parse created timestamp
                created_str = task.get("created", "")
                try:
                    created = datetime.fromisoformat(created_str.replace("Z", "+00:00")[:26]) if created_str else datetime.utcnow()
                except:
                    created = datetime.utcnow()

                # Use stack as compose_project and service_name as compose_service
                stack = task.get("stack", "")

                # Store task_id and service_id in labels for log retrieval
                labels = {
                    "com.docker.swarm.service.name": service_name,
                    "com.docker.swarm.task.id": task["id"],
                    "com.docker.swarm.service.id": task.get("service_id", ""),
                }
                if stack:
                    labels["com.docker.stack.namespace"] = stack
                    labels["com.docker.swarm.stack.namespace"] = stack

                container = ContainerInfo(
                    id=task["container_id"],
                    name=container_name,
                    image=task.get("image", "unknown"),
                    status=ContainerStatus.RUNNING,
                    created=created,
                    host=node_hostname,
                    compose_project=stack if stack else None,
                    compose_service=service_name,
                    ports={},
                    labels=labels,
                )
                containers_by_node[node_hostname].append(container)
            except Exception as e:
                logger.error("Failed to parse swarm container", task_id=task["id"], error=str(e))

        return containers_by_node
