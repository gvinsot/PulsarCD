"""SSH client for remote host operations."""

import asyncio
import json
import os
import re
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncssh
import structlog

from .config import HostConfig
from .models import (
    ContainerInfo, ContainerStats, ContainerStatus, 
    HostMetrics, LogEntry, ContainerAction
)
from . import utils

logger = structlog.get_logger()

# ---------- TOFU (Trust On First Use) helpers ----------
DEFAULT_KNOWN_HOSTS_PATH = str(Path.home() / ".ssh" / "known_hosts")


def _host_is_known(known_hosts_path: str, hostname: str, port: int) -> bool:
    """Check whether *hostname*:*port* already has an entry in *known_hosts_path*."""
    if not os.path.isfile(known_hosts_path):
        return False
    try:
        kh = asyncssh.read_known_hosts(known_hosts_path)
        result = kh.match(hostname, None, port)
        # result[0] = trusted host keys, result[1] = trusted CA keys
        return bool(result[0] or result[1])
    except Exception:
        return False


def _save_host_key(known_hosts_path: str, hostname: str, port: int, key) -> None:
    """Append a server host key to the known_hosts file (TOFU)."""
    try:
        Path(known_hosts_path).parent.mkdir(parents=True, exist_ok=True)
        key_data = key.export_public_key("openssh").decode("utf-8").strip()
        host_entry = f"[{hostname}]:{port}" if port and port != 22 else hostname
        with open(known_hosts_path, "a") as fh:
            fh.write(f"{host_entry} {key_data}\n")
        logger.info("TOFU: saved new host key", host=hostname, port=port,
                     file=known_hosts_path)
    except Exception as exc:
        logger.warning("TOFU: failed to save host key", host=hostname,
                       error=str(exc))


def resolve_known_hosts(
    ssh_known_hosts_path: Optional[str], hostname: str, port: int
):
    """Resolve the ``known_hosts`` parameter for :func:`asyncssh.connect` with
    Trust-On-First-Use semantics (equivalent to ``StrictHostKeyChecking=accept-new``).

    Returns ``(known_hosts_setting, save_key)`` where *save_key* indicates that
    the server key should be persisted after a successful connection.
    """
    if ssh_known_hosts_path:
        if ssh_known_hosts_path.lower() == "none":
            return None, False          # verification explicitly disabled
        return ssh_known_hosts_path, False  # use the caller-supplied file

    # Default path — TOFU behaviour
    kh_path = DEFAULT_KNOWN_HOSTS_PATH
    if _host_is_known(kh_path, hostname, port):
        return kh_path, False            # host already trusted → strict check
    return None, True                    # unknown host → accept & save later


def save_host_key_if_needed(
    conn: asyncssh.SSHClientConnection,
    save_key: bool,
    hostname: str,
    port: int,
) -> None:
    """Persist the server host key when *save_key* is ``True`` (TOFU)."""
    if not save_key:
        return
    server_key = conn.get_server_host_key()
    if server_key:
        _save_host_key(DEFAULT_KNOWN_HOSTS_PATH, hostname, port, server_key)


def is_localhost(hostname: str) -> bool:
    """Check if hostname refers to localhost."""
    return hostname.lower() in ("localhost", "127.0.0.1", "::1")


class SSHClient:
    """Async SSH client for Docker operations."""
    
    def __init__(self, host_config: HostConfig):
        self.config = host_config
        self._connection: Optional[asyncssh.SSHClientConnection] = None
        self._lock = asyncio.Lock()
        
        # Determine if we should run locally or via SSH:
        # - mode="local" forces local execution
        # - mode="ssh" forces SSH
        # - Otherwise, auto-detect based on hostname
        if host_config.mode == "local":
            self._is_local = True
        elif host_config.mode == "ssh":
            self._is_local = False
        else:
            self._is_local = is_localhost(host_config.hostname)
        
        if self._is_local:
            logger.info("Host configured as local (no SSH)", host=self.config.name)
        else:
            logger.info("Host configured for SSH", host=self.config.name, hostname=host_config.hostname)
        
    def _is_connection_open(self) -> bool:
        """Check if SSH connection is still open."""
        if self._connection is None:
            return False
        try:
            # asyncssh connections have a _transport attribute that is None when closed
            return self._connection._transport is not None and not self._connection._transport.is_closing()
        except Exception:
            return False
    
    async def connect(self) -> Optional[asyncssh.SSHClientConnection]:
        """Establish SSH connection (skipped for localhost)."""
        if self._is_local:
            return None
            
        async with self._lock:
            if not self._is_connection_open():
                known_hosts_setting, save_key = resolve_known_hosts(
                    self.config.ssh_known_hosts_path,
                    self.config.hostname,
                    self.config.port,
                )
                options = {
                    "host": self.config.hostname,
                    "port": self.config.port,
                    "username": self.config.username,
                    "known_hosts": known_hosts_setting,
                }
                
                if self.config.ssh_key_path:
                    key_path = Path(self.config.ssh_key_path).expanduser()
                    options["client_keys"] = [str(key_path)]
                    
                self._connection = await asyncssh.connect(**options)
                save_host_key_if_needed(
                    self._connection, save_key,
                    self.config.hostname, self.config.port,
                )
                logger.info("SSH connected", host=self.config.name)
                
            return self._connection
    
    async def disconnect(self):
        """Close SSH connection."""
        if self._is_local:
            return
            
        async with self._lock:
            if self._connection:
                try:
                    self._connection.close()
                    await self._connection.wait_closed()
                except Exception:
                    pass
                self._connection = None
                logger.info("SSH disconnected", host=self.config.name)
    
    async def close(self):
        """Alias for disconnect() to match HostClientProtocol."""
        await self.disconnect()
    
    async def run_command(self, command: str) -> Tuple[str, str, int]:
        """Execute command and return stdout, stderr, exit code."""
        if self._is_local:
            return await self._run_local_command(command)
        
        conn = await self.connect()
        result = await conn.run(command, check=False)
        return result.stdout or "", result.stderr or "", result.exit_status
    
    async def run_shell_command(self, command: str) -> Tuple[bool, str]:
        """Execute a shell command and return (success, output).
        
        This is a convenience wrapper around run_command for stack operations.
        """
        stdout, stderr, exit_code = await self.run_command(command)
        output = stdout + stderr if stderr else stdout
        return exit_code == 0, output.strip()
    
    async def _run_local_command(self, command: str) -> Tuple[str, str, int]:
        """Execute command locally using asyncio subprocess."""
        from .config import wrap_command_for_user
        try:
            proc = await asyncio.create_subprocess_shell(
                wrap_command_for_user(command),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            return (
                stdout.decode("utf-8", errors="replace"),
                stderr.decode("utf-8", errors="replace"),
                proc.returncode or 0,
            )
        except Exception as e:
            logger.error("Local command failed", command=command[:50], error=str(e))
            return "", str(e), 1
    
    async def get_containers(self) -> List[ContainerInfo]:
        """Get list of all Docker containers.

        Optimized to use a single docker inspect command for all containers
        instead of N separate commands per container.
        """
        # Get all container IDs first
        id_cmd = "docker ps -aq"
        id_stdout, _, id_code = await self.run_command(id_cmd)

        if id_code != 0 or not id_stdout.strip():
            return []

        container_ids = id_stdout.strip().split("\n")
        if not container_ids or container_ids == ['']:
            return []

        # Batch inspect all containers in one command (much faster than N commands)
        # Using JSON array output for all containers at once
        inspect_cmd = f"docker inspect {' '.join(shlex.quote(cid) for cid in container_ids)}"
        inspect_stdout, inspect_stderr, inspect_code = await self.run_command(inspect_cmd)

        if inspect_code != 0:
            logger.error("Failed to inspect containers", host=self.config.name, error=inspect_stderr)
            return []

        containers = []
        try:
            all_data = json.loads(inspect_stdout)

            for data in all_data:
                try:
                    # Parse status from State
                    state = data.get("State", {})
                    status_str = state.get("Status", "unknown").lower()
                    try:
                        status = ContainerStatus(status_str)
                    except ValueError:
                        status = ContainerStatus.EXITED

                    # Get labels from Config
                    labels = data.get("Config", {}).get("Labels", {}) or {}

                    # Parse created time
                    created_str = data.get("Created", "")
                    try:
                        created = datetime.fromisoformat(created_str.replace("Z", "+00:00")[:26])
                    except:
                        created = datetime.now()

                    # Parse name (remove leading /)
                    name = data.get("Name", "/unknown").lstrip("/")

                    # Parse ports from NetworkSettings
                    ports = {}
                    port_bindings = data.get("HostConfig", {}).get("PortBindings", {}) or {}
                    for container_port, host_bindings in port_bindings.items():
                        if host_bindings:
                            for binding in host_bindings:
                                host_port = f"{binding.get('HostIp', '')}:{binding.get('HostPort', '')}"
                                ports[container_port] = host_port

                    container = ContainerInfo(
                        id=data["Id"][:12],
                        name=name,
                        image=data.get("Config", {}).get("Image", "unknown"),
                        status=status,
                        created=created,
                        host=self.config.name,
                        compose_project=(labels.get("com.docker.stack.namespace") or
                                         labels.get("com.docker.compose.project")),
                        compose_service=labels.get("com.docker.compose.service"),
                        ports=ports,
                        labels=labels,
                    )
                    containers.append(container)

                except Exception as e:
                    logger.error("Failed to parse container", host=self.config.name, error=str(e))

        except json.JSONDecodeError as e:
            logger.error("Failed to parse docker inspect output", host=self.config.name, error=str(e))

        return containers
    
    async def get_container_stats(self, container_id: str, container_name: str) -> Optional[ContainerStats]:
        """Get container resource statistics."""
        cmd = f"docker stats {shlex.quote(container_id)} --no-stream --format '{{{{json .}}}}'"
        stdout, stderr, code = await self.run_command(cmd)
        
        if code != 0:
            return None
            
        try:
            data = json.loads(stdout.strip())
            
            # Parse CPU percentage
            cpu_str = data.get("CPUPerc", "0%").replace("%", "")
            cpu_percent = float(cpu_str) if cpu_str else 0.0
            
            # Parse memory
            mem_usage, mem_limit = self._parse_memory(data.get("MemUsage", "0B / 0B"))
            mem_perc_str = data.get("MemPerc", "0%").replace("%", "")
            mem_percent = float(mem_perc_str) if mem_perc_str else 0.0
            
            # Parse network I/O
            net_rx, net_tx = self._parse_io(data.get("NetIO", "0B / 0B"))
            
            # Parse block I/O
            block_r, block_w = self._parse_io(data.get("BlockIO", "0B / 0B"))
            
            return ContainerStats(
                container_id=container_id,
                container_name=container_name,
                host=self.config.name,
                timestamp=datetime.utcnow(),
                cpu_percent=cpu_percent,
                memory_usage_mb=mem_usage,
                memory_limit_mb=mem_limit,
                memory_percent=mem_percent,
                network_rx_bytes=net_rx,
                network_tx_bytes=net_tx,
                block_read_bytes=block_r,
                block_write_bytes=block_w,
            )
        except Exception as e:
            logger.error("Failed to parse stats", container=container_id, error=str(e))
            return None
    
    def _parse_memory(self, mem_str: str) -> Tuple[float, float]:
        """Parse memory usage string like '100MiB / 1GiB'."""
        return utils.parse_memory_string(mem_str)
    
    def _parse_io(self, io_str: str) -> Tuple[int, int]:
        """Parse I/O string like '100MB / 50MB'."""
        return utils.parse_io_string(io_str)
    
    async def get_host_metrics(self) -> HostMetrics:
        """Get host-level resource metrics."""
        # CPU usage
        cpu_cmd = "grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}'"
        cpu_out, _, _ = await self.run_command(cpu_cmd)
        cpu_percent = float(cpu_out.strip()) if cpu_out.strip() else 0.0
        
        # Memory
        mem_cmd = "free -m | grep Mem"
        mem_out, _, _ = await self.run_command(mem_cmd)
        mem_parts = mem_out.split()
        mem_total = float(mem_parts[1]) if len(mem_parts) > 1 else 0.0
        mem_used = float(mem_parts[2]) if len(mem_parts) > 2 else 0.0
        mem_percent = (mem_used / mem_total * 100) if mem_total > 0 else 0.0
        
        # Disk
        disk_cmd = "df -BG / | tail -1"
        disk_out, _, _ = await self.run_command(disk_cmd)
        disk_parts = disk_out.split()
        disk_total = float(disk_parts[1].replace("G", "")) if len(disk_parts) > 1 else 0.0
        disk_used = float(disk_parts[2].replace("G", "")) if len(disk_parts) > 2 else 0.0
        disk_percent = float(disk_parts[4].replace("%", "")) if len(disk_parts) > 4 else 0.0
        
        # GPU (AMD rocm-smi first, then NVIDIA nvidia-smi)
        gpu_percent, gpu_mem_used, gpu_mem_total = await self._get_gpu_metrics()
        
        return HostMetrics(
            host=self.config.name,
            timestamp=datetime.utcnow(),
            cpu_percent=cpu_percent,
            memory_total_mb=mem_total,
            memory_used_mb=mem_used,
            memory_percent=mem_percent,
            disk_total_gb=disk_total,
            disk_used_gb=disk_used,
            disk_percent=disk_percent,
            gpu_percent=gpu_percent,
            gpu_memory_used_mb=gpu_mem_used,
            gpu_memory_total_mb=gpu_mem_total,
        )
    
    async def _get_gpu_metrics(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Get GPU metrics using rocm-smi (AMD) or nvidia-smi (NVIDIA)."""
        # Try AMD GPU first (rocm-smi with CSV format - includes all info in one call)
        rocm_cmd = "rocm-smi --showuse --showmeminfo vram --csv 2>/dev/null"
        rocm_out, rocm_err, rocm_code = await self.run_command(rocm_cmd)
        logger.debug("rocm-smi output", returncode=rocm_code, stdout=rocm_out, stderr=rocm_err)
        
        if rocm_code == 0 and rocm_out.strip():
            gpu_percent, gpu_mem_used, gpu_mem_total = utils.parse_rocm_smi_csv(rocm_out)
            if gpu_percent is not None or gpu_mem_used is not None:
                return gpu_percent, gpu_mem_used, gpu_mem_total
        
        # Fallback to NVIDIA GPU
        nvidia_cmd = "nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits 2>/dev/null"
        nvidia_out, _, nvidia_code = await self.run_command(nvidia_cmd)
        logger.debug("nvidia-smi output", returncode=nvidia_code, stdout=nvidia_out)
        
        if nvidia_code == 0 and nvidia_out.strip():
            return utils.parse_nvidia_smi_csv(nvidia_out)
        
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
        """Get container logs.
        
        Args:
            container_id: Docker container ID
            container_name: Container name for logging
            since: If set, fetch all logs since this timestamp (ignores tail)
            tail: If since is None, limit to last N lines (initial fetch)
            compose_project: Optional compose project name
            compose_service: Optional compose service name
            task_id: Optional Swarm task ID (unused for SSH, included for API compatibility)
        """
        cmd = f"docker logs {shlex.quote(container_id)} --timestamps"
        if since:
            # Fetch ALL logs since timestamp - don't use tail to avoid missing logs
            cmd += f" --since {shlex.quote(since.isoformat())}"
        elif tail:
            # First fetch - limit to recent logs
            cmd += f" --tail {int(tail)}"
        # else: fetch all logs (no limit) - rare case
        cmd += " 2>&1"
        
        stdout, _, code = await self.run_command(cmd)
        
        if code != 0:
            return []
        
        entries = []
        for line in stdout.strip().split("\n"):
            if not line:
                continue
            entry = self._parse_log_line(
                line, container_id, container_name,
                compose_project, compose_service
            )
            if entry:
                entries.append(entry)
                
        return entries
    
    def _parse_log_line(
        self,
        line: str,
        container_id: str,
        container_name: str,
        compose_project: Optional[str],
        compose_service: Optional[str],
    ) -> Optional[LogEntry]:
        """Parse a log line with timestamp."""
        return utils.build_log_entry(
            line, self.config.name, container_id, container_name,
            compose_project, compose_service,
        )
    
    async def execute_container_action(self, container_id: str, action: ContainerAction) -> Tuple[bool, str]:
        """Execute an action on a container."""
        safe_id = shlex.quote(container_id)
        cmd_map = {
            ContainerAction.START: f"docker start {safe_id}",
            ContainerAction.STOP: f"docker stop {safe_id}",
            ContainerAction.RESTART: f"docker restart {safe_id}",
            ContainerAction.PAUSE: f"docker pause {safe_id}",
            ContainerAction.UNPAUSE: f"docker unpause {safe_id}",
            ContainerAction.REMOVE: f"docker rm -f {safe_id}",
        }
        
        cmd = cmd_map.get(action)
        if not cmd:
            return False, f"Unknown action: {action}"
        
        stdout, stderr, code = await self.run_command(cmd)
        
        if code == 0:
            return True, f"Action {action.value} completed successfully"
        else:
            return False, stderr or "Command failed"

    async def get_swarm_stacks(self) -> Dict[str, List[str]]:
        """Get list of Docker Swarm stacks and their services.
        
        Returns:
            Dict mapping stack_name -> list of service names
        """
        cmd = "docker stack ls --format '{{.Name}}'"
        stdout, stderr, code = await self.run_command(cmd)
        
        if code != 0:
            return {}
        
        stacks = {}
        for stack_name in stdout.strip().split('\n'):
            stack_name = stack_name.strip()
            if not stack_name:
                continue
            
            # Get services for this stack
            services_cmd = f"docker stack services {shlex.quote(stack_name)} --format '{{.Name}}'"
            services_out, _, services_code = await self.run_command(services_cmd)
            
            if services_code == 0:
                services = [s.strip() for s in services_out.strip().split('\n') if s.strip()]
                stacks[stack_name] = services
            else:
                stacks[stack_name] = []

        return stacks

    async def exec_command(self, container_id: str, command: List[str]) -> Tuple[bool, str]:
        """Execute a command inside a container using docker exec.

        Args:
            container_id: The container ID
            command: Command as list of strings (e.g., ["printenv"])

        Returns:
            Tuple of (success, output/error)
        """
        # Build command - properly quote each argument
        cmd_args = ' '.join(shlex.quote(arg) for arg in command)
        cmd = f"docker exec {shlex.quote(container_id)} {cmd_args}"

        stdout, stderr, code = await self.run_command(cmd)

        if code == 0:
            return True, stdout
        else:
            return False, stderr or "Command failed"

    async def remove_stack(self, stack_name: str) -> Tuple[bool, str]:
        """Remove a Docker Swarm stack."""
        cmd = f"docker stack rm {shlex.quote(stack_name)}"
        stdout, stderr, code = await self.run_command(cmd)
        
        if code == 0:
            return True, f"Stack '{stack_name}' removed successfully"
        else:
            return False, stderr or "Failed to remove stack"

    async def remove_service(self, service_name: str) -> Tuple[bool, str]:
        """Remove a Docker Swarm service."""
        cmd = f"docker service rm {shlex.quote(service_name)}"
        stdout, stderr, code = await self.run_command(cmd)
        
        if code == 0:
            return True, f"Service '{service_name}' removed successfully"
        else:
            return False, stderr or "Failed to remove service"

    async def update_service_image(self, service_name: str, new_tag: str) -> Tuple[bool, str]:
        """Update a Docker Swarm service's image tag.
        
        Uses --with-registry-auth to propagate registry credentials to all
        Swarm nodes so they can pull the new image from private registries.
        """
        # Strip leading 'v' from tag if present (GitHub tags are v1.0.5, Docker images are 1.0.5)
        if new_tag.startswith('v'):
            new_tag = new_tag[1:]
        
        logger.info("[SSH-CLIENT] update_service_image called", service=service_name, tag=new_tag)
        
        # Get current image
        get_image_cmd = f"docker service inspect {shlex.quote(service_name)} --format '{{{{.Spec.TaskTemplate.ContainerSpec.Image}}}}'"
        stdout, stderr, code = await self.run_command(get_image_cmd)
        logger.info("[SSH-CLIENT] Got current image", code=code, stdout=stdout[:200] if stdout else 'None', stderr=stderr[:200] if stderr else '')
        
        if code != 0:
            error_msg = f"Service '{service_name}' not found: {stderr or stdout}"
            logger.error("[SSH-CLIENT] Service not found", service=service_name, error=error_msg)
            return False, error_msg
        
        current_image = stdout.strip()
        if not current_image:
            error_msg = f"Service '{service_name}' has no image configured"
            logger.error("[SSH-CLIENT] No image configured", service=service_name)
            return False, error_msg
        
        logger.info("[SSH-CLIENT] Current image", current_image=current_image)
        
        # Remove digest if present (format: image:tag@sha256:...)
        if "@sha256:" in current_image:
            current_image = current_image.split("@sha256:")[0]
        
        # Get base image name without tag
        if ":" in current_image:
            image_base = current_image.rsplit(":", 1)[0]
        else:
            image_base = current_image
        
        new_image = f"{image_base}:{new_tag}"
        logger.info("[SSH-CLIENT] Updating to new image", image_base=image_base, new_tag=new_tag, new_image=new_image)
        
        # Update service with new image
        # IMPORTANT: --with-registry-auth propagates registry credentials to all swarm nodes
        # Without this, workers cannot pull images from private registries
        update_cmd = f"docker service update --image {shlex.quote(new_image)} --with-registry-auth --force {shlex.quote(service_name)}"
        logger.info("[SSH-CLIENT] Running update command", command=update_cmd)
        stdout, stderr, code = await self.run_command(update_cmd)
        
        output = stdout + stderr if stderr else stdout
        logger.info("[SSH-CLIENT] Update result", code=code, output=output[:500] if output else '')
        
        if code == 0:
            success_msg = f"Service '{service_name}' updated to image '{new_image}'"
            logger.info("[SSH-CLIENT] Service updated successfully", service=service_name, new_image=new_image)
            return True, success_msg
        else:
            error_msg = f"Failed to update service: {output}"
            logger.error("[SSH-CLIENT] Service update failed", service=service_name, error=error_msg)
            return False, error_msg

    async def get_service_tasks(self, service_name: str) -> List[Dict[str, Any]]:
        """Get all tasks for a service (equivalent to docker service ps --no-trunc).
        
        Returns task status information useful as a fallback when service logs
        are unavailable (e.g., service not fully deployed).
        """
        cmd = (
            f"docker service ps {shlex.quote(service_name)} --no-trunc "
            f"--format '{{{{.ID}}}}\t{{{{.Node}}}}\t{{{{.DesiredState}}}}\t{{{{.CurrentState}}}}\t{{{{.Error}}}}\t{{{{.Image}}}}'"
        )
        stdout, stderr, code = await self.run_command(cmd)
        
        if code != 0:
            logger.error("Failed to get service tasks", service=service_name, error=stderr)
            return []
        
        tasks = []
        for line in stdout.strip().split('\n'):
            if not line.strip():
                continue
            parts = line.split('\t')
            if len(parts) < 4:
                continue
            
            task_id = parts[0] if len(parts) > 0 else ''
            node = parts[1] if len(parts) > 1 else ''
            desired_state = parts[2] if len(parts) > 2 else ''
            current_state = parts[3] if len(parts) > 3 else ''
            error = parts[4] if len(parts) > 4 else ''
            image = parts[5] if len(parts) > 5 else ''
            
            # Parse state from current_state (e.g., "Running 2 hours ago" -> "running")
            state = current_state.split()[0].lower() if current_state else 'unknown'
            
            tasks.append({
                "id": task_id,
                "node": node,
                "desired_state": desired_state.lower(),
                "state": state,
                "error": error,
                "message": current_state,
                "image": image,
                "created_at": "",
                "updated_at": "",
                "container_id": "",
            })
        
        return tasks

    async def get_service_logs(self, service_name: str, tail: int = 200) -> List[dict]:
        """Get logs for a Docker Swarm service."""
        cmd = f"docker service logs --tail {int(tail)} --timestamps {shlex.quote(service_name)}"
        stdout, stderr, code = await self.run_command(cmd)
        
        if code != 0:
            logger.error("Failed to get service logs", service=service_name, error=stderr)
            return []
        
        logs = []
        for line in stdout.split('\n'):
            if not line.strip():
                continue
            # Parse timestamp and message
            # Format: service_name.1.xxx@node | 2024-01-01T00:00:00.123456789Z message
            try:
                if '|' in line:
                    prefix, rest = line.split('|', 1)
                    rest = rest.strip()
                    if rest and rest[0].isdigit():
                        ts_end = rest.find(' ')
                        if ts_end > 0:
                            timestamp = rest[:ts_end]
                            message = rest[ts_end+1:]
                        else:
                            timestamp = None
                            message = rest
                    else:
                        timestamp = None
                        message = rest
                else:
                    timestamp = None
                    message = line
                
                logs.append({
                    "timestamp": timestamp,
                    "message": message,
                    "service": service_name,
                    "stream": "stdout",
                })
            except Exception:
                logs.append({
                    "timestamp": None,
                    "message": line,
                    "service": service_name,
                    "stream": "stdout",
                })
        
        return logs
