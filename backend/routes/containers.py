"""Container and Swarm service endpoints.

/api/containers/*, /api/services/* and POST /api/stacks/{stack_name}/remove.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

import traceback
from typing import Any, Dict, List, Optional

import structlog
from fastapi import APIRouter, HTTPException, Path, Query, Request

from ..models import (
    ActionRequest, ActionResult, ContainerInfo, ContainerStatus
)
from ..github_service import StackDeployer
from ..actions_queue import ActionType
from ..pipeline_runner import _get_swarm_manager_host
from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ============== Containers ==============

@router.get("/api/containers", response_model=List[ContainerInfo])
async def list_containers(
    refresh: bool = Query(default=False),
    status: Optional[ContainerStatus] = Query(default=None),
    host: Optional[str] = Query(default=None),
    compose_project: Optional[str] = Query(default=None),
):
    """List all containers with optional filters."""
    containers = await api.collector.get_all_containers(refresh=refresh)
    
    # Apply filters
    if status:
        containers = [c for c in containers if c.status == status]
    if host:
        containers = [c for c in containers if c.host == host]
    if compose_project:
        containers = [c for c in containers if c.compose_project == compose_project]
    
    return containers


@router.get("/api/containers/states")
async def get_containers_states(
    refresh: bool = Query(default=True),
) -> List[Dict[str, Any]]:
    """Get lightweight container states (id, name, host, status, cpu, memory).
    
    This endpoint is designed for frequent polling to update UI without
    fetching the full container info and re-rendering everything.
    """
    containers = await api.collector.get_all_containers(refresh=refresh)

    # Fetch latest stats
    latest_stats = await api.opensearch.get_latest_container_stats()

    result = []
    for c in containers:
        stats = latest_stats.get(c.id, {})
        result.append({
            "id": c.id,
            "name": c.name,
            "host": c.host,
            "status": c.status.value,
            "image": c.image,
            "created": c.created.isoformat() if c.created else None,
            "cpu_percent": stats.get("cpu_percent", c.cpu_percent),
            "memory_percent": stats.get("memory_percent", c.memory_percent),
            "memory_usage_mb": stats.get("memory_usage_mb", c.memory_usage_mb),
            "labels": {
                k: v for k, v in c.labels.items()
                if k in (
                    "com.docker.swarm.stack.namespace",
                    "com.docker.swarm.service.name",
                )
            },
        })

    return result


@router.get("/api/containers/grouped")
async def list_containers_grouped(
    refresh: bool = Query(default=False),
    status: Optional[ContainerStatus] = Query(default=None),
    group_by: str = Query(default="host", description="Group by: 'host' or 'stack'"),
) -> Dict[str, Dict[str, List[ContainerInfo]]]:
    """List containers grouped by host and compose project, or by Docker Swarm stack."""
    containers = await api.collector.get_all_containers(refresh=refresh)
    
    if status:
        containers = [c for c in containers if c.status == status]
    
    # Fetch latest stats for all containers (single query)
    latest_stats = await api.opensearch.get_latest_container_stats()
    
    # Enrich containers with stats
    for container in containers:
        stats = latest_stats.get(container.id)
        if stats:
            container.cpu_percent = stats.get("cpu_percent")
            container.memory_percent = stats.get("memory_percent")
            container.memory_usage_mb = stats.get("memory_usage_mb")
            container.gpu_percent = stats.get("gpu_percent")
            container.gpu_memory_used_mb = stats.get("gpu_memory_used_mb")
    
    if group_by == "stack":
        # Group by Docker Swarm stack -> service
        swarm_manager_host = _get_swarm_manager_host()

        # Build stack_services_map only for name resolution (not pre-population)
        stack_services_map: Dict[str, List[str]] = {}
        if swarm_manager_host:
            manager_client = api.collector.clients.get(swarm_manager_host)
            if manager_client:
                try:
                    stack_services_map = await manager_client.get_swarm_stacks()
                except Exception as e:
                    logger.warning("Failed to get stacks from Swarm manager", host=swarm_manager_host, error=str(e))

        # Pre-populate from stack_services_map so services with 0 running replicas
        # (scaled down, starting up) are still shown in the UI.
        # docker stack services reflects the current desired state — removed services
        # are no longer listed there, so this won't show stale removed services.
        grouped: Dict[str, Dict[str, List[ContainerInfo]]] = {}
        for stack_name, services in stack_services_map.items():
            grouped[stack_name] = {}
            for service_full_name in services:
                grouped[stack_name][service_full_name] = []
        
        # Group containers by their stack
        for container in containers:
            # Try to get stack name from Swarm labels first
            stack_name = container.labels.get("com.docker.swarm.stack.namespace")
            service_name = container.labels.get("com.docker.swarm.service.name")
            
            # If no stack from labels, try to extract from container name
            # Docker Swarm container names: stack_service.replica_id or stack_service.replica_id.node_id
            # Example: myapp_web.1.abc123def456 -> stack: myapp, service: web
            if not stack_name and "." in container.name:
                main_part = container.name.split(".")[0]
                # Check if this matches any known stack pattern
                for known_stack in stack_services_map.keys():
                    if main_part.startswith(known_stack + "_"):
                        stack_name = known_stack
                        if not service_name:
                            service_name = main_part[len(known_stack) + 1:]
                        break
                
                # If still no stack found but has underscore pattern, try to extract
                if not stack_name and "_" in main_part:
                    # Try to match against known stacks by checking if prefix matches
                    for known_stack in stack_services_map.keys():
                        if main_part.startswith(known_stack):
                            stack_name = known_stack
                            if not service_name:
                                # Extract service name after stack prefix
                                remaining = main_part[len(known_stack):]
                                if remaining.startswith("_"):
                                    service_name = remaining[1:]
                                else:
                                    service_name = remaining
                            break
                    
                    # If still not found, assume first part before underscore is stack
                    if not stack_name:
                        parts = main_part.split("_", 1)
                        if len(parts) == 2:
                            potential_stack = parts[0]
                            # Check if this potential stack exists in our known stacks
                            if potential_stack in stack_services_map:
                                stack_name = potential_stack
                                service_name = parts[1] if not service_name else service_name
            
            # If we have stack info from manager, verify the stack exists
            if stack_name and stack_name in stack_services_map:
                # This is a confirmed Swarm stack
                if not service_name:
                    # Last resort: extract from container name
                    if "." in container.name:
                        main_part = container.name.split(".")[0]
                        if "_" in main_part and main_part.startswith(stack_name + "_"):
                            service_name = main_part[len(stack_name) + 1:]
                        else:
                            service_name = main_part.split("_", 1)[-1] if "_" in main_part else main_part
                    else:
                        service_name = container.name
            elif stack_name:
                # Has swarm label but stack not found in manager - might be stale
                # Still group it but use the stack name from label
                if not service_name:
                    if "." in container.name:
                        main_part = container.name.split(".")[0]
                        if "_" in main_part and main_part.startswith(stack_name + "_"):
                            service_name = main_part[len(stack_name) + 1:]
                        else:
                            service_name = main_part.split("_", 1)[-1] if "_" in main_part else main_part
                    else:
                        service_name = container.name
            else:
                # Not a Swarm stack, use compose project or standalone
                stack_name = container.compose_project or "_standalone"
                service_name = container.compose_service or \
                              container.name.split(".")[0] if "." in container.name else container.name
            
            # Ensure stack group exists
            if stack_name not in grouped:
                grouped[stack_name] = {}
            
            # Ensure service group exists
            if service_name not in grouped[stack_name]:
                grouped[stack_name][service_name] = []
            
            grouped[stack_name][service_name].append(container)
        
        # Keep all stacks from manager, even those with no containers
        # (services may exist but have 0 running replicas)
    else:
        # Group by host -> compose_project (default)
        grouped: Dict[str, Dict[str, List[ContainerInfo]]] = {}
        
        for container in containers:
            host = container.host
            project = container.compose_project or "_standalone"
            
            if host not in grouped:
                grouped[host] = {}
            if project not in grouped[host]:
                grouped[host][project] = []
            
            grouped[host][project].append(container)
    
    return grouped


@router.get("/api/containers/{host}/{container_id}/stats")
async def get_container_stats(host: str, container_id: str) -> Dict[str, Any]:
    """Get current stats for a container."""
    stats = await api.collector.get_container_stats(host, container_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Container not found")
    return stats


_PERIOD_MAP: Dict[str, tuple] = {
    "1h":  (1,   "1m"),
    "6h":  (6,   "5m"),
    "24h": (24,  "15m"),
    "7d":  (168, "1h"),
    "30d": (720, "6h"),
}


@router.get("/api/containers/{host}/{container_id}/metrics")
async def get_container_metrics(
    host: str,
    container_id: str,
    period: str = Query(default="7d"),
) -> Dict[str, Any]:
    """Return CPU%, memory% and error-count time series for a container."""
    hours, interval = _PERIOD_MAP.get(period, _PERIOD_MAP["7d"])
    if not api.opensearch:
        return {"cpu": [], "memory": [], "errors": []}
    return await api.opensearch.get_container_metrics_timeseries(container_id, hours, interval)


@router.get("/api/containers/{host}/{container_id}/logs")
async def get_container_logs(
    host: str,
    container_id: str,
    tail: int = Query(default=200, ge=1, le=10000)
) -> List[Dict[str, Any]]:
    """Get live logs for a container."""
    logs = await api.collector.get_container_logs_live(host, container_id, tail=tail)
    return logs


@router.get("/api/containers/{host}/{container_id}/env")
async def get_container_env(host: str, container_id: str, request: Request) -> Dict[str, Any]:
    """Get environment variables for a container by running printenv inside it."""
    if getattr(request.state, "role", "") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    env_data = await api.collector.get_container_env(host, container_id)
    if not env_data:
        raise HTTPException(status_code=404, detail="Container not found")
    return env_data


@router.post("/api/containers/action", response_model=ActionResult)
async def execute_container_action(request: ActionRequest) -> ActionResult:
    """Execute an action on a container (start, stop, restart, etc.).
    
    If an agent is online for the target host, the action is queued for the agent.
    Otherwise, the action is executed directly via the collector (SSH/Docker API).
    """
    # Check if an agent is online for this host.
    # The agent registers with its own hostname which may differ from the Docker node
    # hostname stored in ContainerInfo.host (e.g. FQDN vs short name, or configured
    # name vs actual hostname).  Try an exact match first, then fall back to a
    # case-insensitive short-hostname comparison across all online agents.
    agent_id = request.host
    agent_online = await api.actions_queue.is_agent_online(agent_id)

    if not agent_online:
        short_host = request.host.split('.')[0].lower()
        for agent in await api.actions_queue.get_agents():
            if await api.actions_queue.is_agent_online(agent.agent_id):
                if agent.agent_id.split('.')[0].lower() == short_host:
                    agent_id = agent.agent_id
                    agent_online = True
                    break

    if agent_online:
        # Use the actions queue - agent will pick up and execute the action
        action_obj = await api.actions_queue.create_action(
            agent_id=agent_id,
            action_type=ActionType.CONTAINER_ACTION,
            payload={
                "container_id": request.container_id,
                "action": request.action.value,
            },
        )
        
        # Wait for the action to complete (with timeout)
        completed_action = await api.actions_queue.wait_for_action(action_obj.id, timeout=30.0)
        
        if not completed_action:
            return ActionResult(
                success=False,
                message="Action timed out waiting for agent",
                container_id=request.container_id,
                action=request.action,
            )
        
        return ActionResult(
            success=completed_action.success or False,
            message=completed_action.result or "Action completed",
            container_id=request.container_id,
            action=request.action,
        )
    
    # No agent online - try direct execution via collector
    success, message = await api.collector.execute_action(
        request.host, 
        request.container_id, 
        request.action.value
    )
    
    return ActionResult(
        success=success,
        message=message,
        container_id=request.container_id,
        action=request.action,
    )


@router.post("/api/stacks/{stack_name}/remove")
async def remove_stack(stack_name: str, host: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """Remove a Docker Swarm stack."""
    # Normalize to Docker stack name (mirrors deploy-service.sh)
    stack_name = StackDeployer._repo_to_stack_name(stack_name)
    
    # If host specified, use it; otherwise try manager, then fallback to containers
    target_host = host or _get_swarm_manager_host()
    
    if not target_host:
        # Fallback: find host from containers belonging to this stack
        containers = await api.collector.get_all_containers(refresh=True)
        stack_containers = [
            c for c in containers 
            if c.labels.get("com.docker.swarm.stack.namespace") == stack_name
        ]
        if stack_containers:
            target_host = stack_containers[0].host
    
    if not target_host:
        raise HTTPException(status_code=404, detail=f"No host found to remove stack '{stack_name}'")
    
    # Execute stack removal
    client = api.collector.clients.get(target_host)
    if not client:
        raise HTTPException(status_code=404, detail=f"Host '{target_host}' not found")
    
    success, message = await client.remove_stack(stack_name)

    if success:
        # Reset pipeline state so the stack shows as undeployed
        repo_name = api.pipeline_state.find_repo_by_stack(stack_name)
        if repo_name:
            api.pipeline_state.reset(repo_name)
        return {"success": True, "message": message, "stack_name": stack_name}
    else:
        raise HTTPException(status_code=500, detail=message)


@router.post("/api/services/{service_name}/remove")
async def remove_service(
    service_name: str,
    host: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """Remove a Docker Swarm service."""
    target_host = host or _get_swarm_manager_host()
    if not target_host:
        # Fallback to first available client
        target_host = next(iter(api.collector.clients.keys()), None)
    
    if not target_host:
        raise HTTPException(status_code=404, detail="No host available")
    
    client = api.collector.clients.get(target_host)
    if not client:
        raise HTTPException(status_code=404, detail=f"Host '{target_host}' not found")
    
    success, message = await client.remove_service(service_name)
    
    if success:
        return {"success": True, "message": message, "service_name": service_name}
    else:
        raise HTTPException(status_code=500, detail=message)


@router.post("/api/services/{service_name}/update-image")
async def update_service_image(
    service_name: str,
    tag: str = Query(..., description="New image tag to deploy"),
    host: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """Update a Docker Swarm service's image tag.
    
    This allows deploying a new version of a single service without
    redeploying the entire stack. Uses --with-registry-auth to propagate
    registry credentials to all Swarm nodes.
    """
    logger.info("[API] update_service_image request", service=service_name, tag=tag, requested_host=host)
    
    target_host = host or _get_swarm_manager_host()
    if not target_host:
        # Fallback to first available client
        target_host = next(iter(api.collector.clients.keys()), None)
        logger.info("[API] Using fallback host", target_host=target_host)
    
    if not target_host:
        logger.error("[API] No host available for service update")
        raise HTTPException(status_code=404, detail="No host available")
    
    client = api.collector.clients.get(target_host)
    if not client:
        logger.error("[API] Host not found", target_host=target_host, available_hosts=list(api.collector.clients.keys()))
        raise HTTPException(status_code=404, detail=f"Host '{target_host}' not found")
    
    client_type = type(client).__name__
    logger.info("[API] Calling update_service_image", 
               service=service_name, tag=tag, host=target_host, client_type=client_type)
    
    try:
        success, message = await client.update_service_image(service_name, tag)
        
        logger.info("[API] update_service_image completed", 
                   service=service_name, tag=tag, success=success, 
                   message=message[:200] if message else '')
        
        if success:
            return {"success": True, "message": message, "service_name": service_name, "tag": tag}
        else:
            logger.error("[API] Service image update failed", 
                        service=service_name, tag=tag, error=message)
            raise HTTPException(status_code=500, detail=message)
    except HTTPException:
        raise
    except Exception as e:
        error_detail = f"{type(e).__name__}: {e}"
        logger.error("[API] Exception during service update",
                    service=service_name, tag=tag, error=error_detail,
                    traceback=traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_detail)


@router.get("/api/services/{service_name}/logs")
async def get_service_logs(
    service_name: str,
    tail: int = Query(default=200, ge=1, le=10000),
    host: Optional[str] = Query(default=None),
) -> Any:
    """Get logs for a Docker Swarm service.
    
    Uses the Docker API /services/{id}/logs endpoint to aggregate logs
    from all tasks/replicas of the given service.
    
    If the service tasks are not scheduled, returns task status info instead.
    """
    # Find a client that can access the service
    client = None
    if host:
        client = api.collector.clients.get(host)
    else:
        # Use the first available client (manager node)
        for name, c in api.collector.clients.items():
            client = c
            break
    
    if not client:
        raise HTTPException(status_code=404, detail="No host available")
    
    try:
        result = await client.get_service_logs(service_name, tail=tail)
        # If get_service_logs detected an unscheduled task, it returns a dict
        # with service task status info instead of a log list
        if isinstance(result, dict) and result.get("type") == "service_tasks":
            return result
        # Fallback: if no logs returned, try to show service task status
        # (equivalent to `docker service ps <service> --no-trunc`)
        if not result:
            if hasattr(client, "get_service_tasks"):
                tasks = await client.get_service_tasks(service_name)
                if tasks:
                    return {"type": "service_tasks", "tasks": tasks, "service": service_name}
        return result
    except Exception as e:
        logger.error("Failed to get container details", service=service_name, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/services/{service_name}/tasks")
async def get_service_tasks(
    service_name: str = Path(...),
    host: Optional[str] = Query(default=None),
) -> Any:
    """Get task status for a Docker Swarm service.

    Equivalent to: docker service ps <service_name> --no-trunc
    Returns all tasks including failed, pending, and shutdown tasks.
    """
    client = None
    if host:
        client = api.collector.clients.get(host)
    else:
        for name, c in api.collector.clients.items():
            client = c
            break

    if not client:
        raise HTTPException(status_code=404, detail="No host available")

    try:
        tasks = await client.get_service_tasks(service_name)
        return {"tasks": tasks, "service": service_name}
    except Exception as e:
        logger.error("Failed to get service tasks", service=service_name, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
