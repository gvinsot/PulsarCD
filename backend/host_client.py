"""Unified host client interface."""

from typing import Dict, List, Optional, Tuple, Protocol, Any, TYPE_CHECKING
from datetime import datetime

import structlog

from .config import HostConfig
from .models import (
    ContainerInfo, ContainerStats, ContainerAction,
    HostMetrics, LogEntry
)

if TYPE_CHECKING:
    from .docker_client import DockerAPIClient

logger = structlog.get_logger()


class HostClientProtocol(Protocol):
    """Protocol for host clients (SSH or Docker API)."""
    
    config: HostConfig
    
    async def get_containers(self) -> List[ContainerInfo]: ...
    async def get_container_stats(self, container_id: str, container_name: str) -> Optional[ContainerStats]: ...
    async def get_host_metrics(self) -> HostMetrics: ...
    async def get_container_logs(
        self,
        container_id: str,
        container_name: str,
        since: Optional[datetime] = None,
        tail: Optional[int] = 500,
        compose_project: Optional[str] = None,
        compose_service: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> List[LogEntry]: ...
    async def execute_container_action(self, container_id: str, action: ContainerAction) -> Tuple[bool, str]: ...
    async def exec_command(self, container_id: str, command: List[str]) -> Tuple[bool, str]: ...
    async def remove_stack(self, stack_name: str) -> Tuple[bool, str]: ...
    async def remove_service(self, service_name: str) -> Tuple[bool, str]: ...
    async def update_service_image(self, service_name: str, new_tag: str) -> Tuple[bool, str]: ...
    async def get_swarm_stacks(self) -> Dict[str, List[str]]: ...
    async def get_service_logs(self, service_name: str, tail: int = 200) -> List[Any]: ...
    async def get_service_tasks(self, service_name: str) -> List[Any]: ...
    async def close(self) -> None: ...


class SwarmProxyClient:
    """Proxy client for Swarm worker nodes discovered via manager.

    This client wraps a Swarm manager's DockerAPIClient and filters/routes
    requests for a specific worker node. All Docker commands are executed
    through the manager's API, which routes them to the correct node.

    This eliminates the need for direct SSH access to worker nodes.
    """

    def __init__(self, manager_client: "DockerAPIClient", node_id: str, node_hostname: str):
        """Initialize proxy client for a Swarm worker node.

        Args:
            manager_client: The DockerAPIClient connected to the Swarm manager
            node_id: The Swarm node ID for this worker
            node_hostname: The hostname of the worker node
        """
        self._manager = manager_client
        self._node_id = node_id
        self._node_hostname = node_hostname

        # Create a virtual config for this node
        self.config = HostConfig(
            name=node_hostname,
            hostname=node_hostname,
            mode="swarm-proxy",
            swarm_manager=False,
        )
        logger.info("Created Swarm proxy client",
                   node=node_hostname, node_id=node_id[:12],
                   manager=manager_client.config.name)

    async def get_containers(self) -> List[ContainerInfo]:
        """Get containers running on this specific node.

        Uses task/service data from Swarm API instead of /containers/{id}/json
        which only works for containers on the local node.
        """
        from .models import ContainerStatus

        # Get tasks with service info for building ContainerInfo
        tasks = await self._manager.get_swarm_tasks(include_service_info=True)

        # Filter tasks for this node
        node_tasks = [
            t for t in tasks
            if t["node_id"].startswith(self._node_id[:12]) or self._node_id.startswith(t["node_id"][:12])
        ]

        if not node_tasks:
            return []

        containers = []
        for task in node_tasks:
            if not task.get("container_id"):
                continue

            try:
                # Build container name from service name and slot (like Docker does)
                service_name = task.get("service_name", "unknown")
                slot = task.get("slot", 0)
                container_name = f"{service_name}.{slot}.{task['id']}"

                # Parse created timestamp
                created_str = task.get("created", "")
                try:
                    created = datetime.fromisoformat(created_str.replace("Z", "+00:00")[:26]) if created_str else datetime.now()
                except:
                    created = datetime.now()

                # Use stack as compose_project
                stack = task.get("stack", "")

                container = ContainerInfo(
                    id=task["container_id"][:12],
                    name=container_name,
                    image=task.get("image", "unknown"),
                    status=ContainerStatus.RUNNING,
                    created=created,
                    host=self._node_hostname,
                    compose_project=stack if stack else None,
                    compose_service=service_name,
                    ports={},
                    labels={
                        "com.docker.swarm.service.name": service_name,
                        "com.docker.stack.namespace": stack,
                        "com.docker.swarm.stack.namespace": stack,
                        "com.docker.swarm.task.id": task["id"],
                        "com.docker.swarm.service.id": task.get("service_id", ""),
                    } if stack else {
                        "com.docker.swarm.service.name": service_name,
                        "com.docker.swarm.task.id": task["id"],
                        "com.docker.swarm.service.id": task.get("service_id", ""),
                    },
                )
                containers.append(container)
            except Exception as e:
                logger.error("Failed to parse container", container_id=task.get("container_id"), error=str(e))

        return containers

    async def get_container_stats(self, container_id: str, container_name: str) -> Optional[ContainerStats]:
        """Get container stats via manager."""
        return await self._manager.get_container_stats(container_id, container_name)

    async def get_host_metrics(self) -> HostMetrics:
        """Get host metrics (limited for proxy nodes).
        
        For autodiscovered Swarm nodes, we can't get full host metrics
        as the Docker API doesn't expose them for remote nodes.
        Returns basic info derived from node info if available.
        """
        # For Swarm proxy nodes, we have limited visibility
        # Container stats won't work for remote nodes, so return empty metrics
        return HostMetrics(
            host=self._node_hostname,
            timestamp=datetime.utcnow(),
            cpu_percent=0,  # Not available for remote Swarm nodes
            memory_total_mb=0,
            memory_used_mb=0,
            memory_percent=0,
            disk_total_gb=0,
            disk_used_gb=0,
            disk_percent=0,
            gpu_percent=None,  # Not available for remote Swarm nodes
            gpu_memory_used_mb=None,
            gpu_memory_total_mb=None,
        )

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
        """Get container logs via manager using tasks API for swarm containers."""
        return await self._manager.get_container_logs(
            container_id, container_name, since, tail,
            compose_project, compose_service, task_id
        )

    async def execute_container_action(self, container_id: str, action: ContainerAction) -> Tuple[bool, str]:
        """Execute action on container via manager."""
        return await self._manager.execute_container_action(container_id, action)

    async def exec_command(self, container_id: str, command: List[str]) -> Tuple[bool, str]:
        """Execute command in container via manager."""
        return await self._manager.exec_command(container_id, command)

    async def remove_stack(self, stack_name: str) -> Tuple[bool, str]:
        """Remove stack via manager."""
        return await self._manager.remove_stack(stack_name)

    async def remove_service(self, service_name: str) -> Tuple[bool, str]:
        """Remove service via manager."""
        return await self._manager.remove_service(service_name)

    async def update_service_image(self, service_name: str, new_tag: str) -> Tuple[bool, str]:
        """Update service image via manager.
        
        Delegates to the Swarm manager's client to perform the actual update.
        """
        logger.info("[SWARM-PROXY] update_service_image delegating to manager", 
                   service=service_name, tag=new_tag, 
                   manager=getattr(self._manager, 'config', {}).get('name', 'unknown'))
        result = await self._manager.update_service_image(service_name, new_tag)
        logger.info("[SWARM-PROXY] update_service_image result", 
                   service=service_name, success=result[0], message=result[1][:200] if result[1] else '')
        return result

    async def get_swarm_stacks(self) -> Dict[str, List[str]]:
        """Get swarm stacks via manager."""
        return await self._manager.get_swarm_stacks()

    async def get_service_logs(self, service_name: str, tail: int = 200) -> List[Any]:
        """Get service logs via manager."""
        return await self._manager.get_service_logs(service_name, tail)

    async def get_service_tasks(self, service_name: str) -> List[Any]:
        """Get service tasks via manager."""
        return await self._manager.get_service_tasks(service_name)

    async def close(self) -> None:
        """No-op for proxy client (manager handles connection)."""
        pass


def create_host_client(host_config: HostConfig) -> HostClientProtocol:
    """Factory function to create the appropriate client based on config."""

    mode = host_config.mode.lower()

    if mode == "docker":
        from .docker_client import DockerAPIClient
        logger.info("Creating Docker API client", host=host_config.name)
        return DockerAPIClient(host_config)

    elif mode == "local":
        from .ssh_client import SSHClient
        # Force local mode in SSH client
        host_config_copy = host_config.model_copy()
        host_config_copy.hostname = "localhost"
        logger.info("Creating local client", host=host_config.name)
        return SSHClient(host_config_copy)

    else:  # mode == "ssh" or default
        from .ssh_client import SSHClient
        logger.info("Creating SSH client", host=host_config.name, hostname=host_config.hostname)
        return SSHClient(host_config)
