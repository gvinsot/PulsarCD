"""FastAPI REST API for LogsCrawler."""

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import jwt
import structlog
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from .auth import create_token, decode_token
from .collector import Collector
from .config import load_config, Settings
from .models import (
    ActionRequest, ActionResult, ContainerInfo, ContainerStatus,
    DashboardStats, LogSearchQuery, LogSearchResult, TimeSeriesPoint, TimeSeriesByHost
)
from .opensearch_client import OpenSearchClient
from .github_service import GitHubService, StackDeployer
from .actions_queue import actions_queue, ActionType, ActionStatus
try:
    from .mcp_server import mcp as mcp_server, get_mcp_app
    from .mcp_auth import MCPAuthMiddleware
    _mcp_available = True
except Exception as _mcp_err:
    _mcp_available = False

logger = structlog.get_logger()

# Global instances
settings: Settings = None
opensearch: OpenSearchClient = None
collector: Collector = None
github_service: GitHubService = None

# ============== Background Actions (Build/Deploy) ==============

class BackgroundAction:
    """Tracks a background build or deploy action."""
    
    def __init__(self, action_id: str, action_type: str, repo_name: str):
        self.id = action_id
        self.action_type = action_type  # "build" or "deploy"
        self.repo_name = repo_name
        self.status = "running"  # running, completed, failed, cancelled
        self.output_lines: List[str] = []
        self.result: Optional[Dict[str, Any]] = None
        self.started_at = datetime.utcnow()
        self.cancel_event = asyncio.Event()
        self.task: Optional[asyncio.Task] = None
    
    def append_output(self, line: str):
        self.output_lines.append(line)
    
    def get_output(self) -> str:
        return "\n".join(self.output_lines)

# Store of running/recent background actions
_background_actions: Dict[str, BackgroundAction] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global settings, opensearch, collector, github_service
    
    # Startup
    logger.info("Starting LogsCrawler API")

    settings = load_config()
    opensearch = OpenSearchClient(settings.opensearch)

    # Initialize OpenSearch with retry (wait for DNS/service to be ready)
    max_retries = 30
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            await opensearch.initialize()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(
                    "OpenSearch not ready, retrying...",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(e),
                )
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Failed to connect to OpenSearch after retries", error=str(e))
                raise
    
    collector = Collector(settings, opensearch)
    await collector.start()
    
    # Initialize GitHub service
    github_service = GitHubService(settings.github)

    # Start auto-build poller
    global _auto_build_task
    if github_service.is_configured():
        _auto_build_task = asyncio.create_task(auto_build_poller())
        logger.info("Auto-build poller started", interval=f"{AUTO_BUILD_POLL_INTERVAL}s")

    # Log MCP API key for configuration
    if _mcp_available and settings.mcp.enabled:
        logger.info("MCP server enabled", mcp_api_key=settings.mcp.api_key)
        async with mcp_server.session_manager.run():
            yield
    else:
        if not _mcp_available:
            logger.warning("MCP server not available", error=str(_mcp_err))
        yield

    # Shutdown
    logger.info("Shutting down LogsCrawler API")
    if _auto_build_task:
        _auto_build_task.cancel()
        try:
            await _auto_build_task
        except asyncio.CancelledError:
            pass
    await collector.stop()
    await opensearch.close()
    await github_service.close()


app = FastAPI(
    title="LogsCrawler API",
    description="Docker container log aggregation and monitoring",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount MCP server at /ai with its own authentication middleware
# The SDK serves internally on /mcp, so the full endpoint is /ai/mcp
if _mcp_available:
    app.mount("/ai", MCPAuthMiddleware(get_mcp_app()))

# CORS middleware - restricted to same-origin; only needed for dev/proxy setups
app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)


# ============== Auth Middleware ==============

# Paths that don't require authentication
_AUTH_EXEMPT_PREFIXES = (
    "/api/auth/login",
    "/api/health",
    "/static/",
    "/ai",  # MCP has its own auth middleware
)
_AUTH_EXEMPT_EXACT = ("/", "/api/health")

# Login rate limiting (in-memory)
_login_attempts: Dict[str, List[float]] = {}
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 60


def _is_rate_limited(client_ip: str) -> bool:
    """Check if a client IP is rate-limited for login attempts."""
    import time
    now = time.time()
    attempts = _login_attempts.get(client_ip, [])
    # Remove old attempts outside the window
    attempts = [t for t in attempts if now - t < _LOGIN_WINDOW_SECONDS]
    _login_attempts[client_ip] = attempts
    return len(attempts) >= _LOGIN_MAX_ATTEMPTS


def _record_login_attempt(client_ip: str):
    """Record a failed login attempt."""
    import time
    if client_ip not in _login_attempts:
        _login_attempts[client_ip] = []
    _login_attempts[client_ip].append(time.time())


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Verify authentication on all /api/ endpoints."""
    path = request.url.path

    # Skip auth for exempt paths
    if path in _AUTH_EXEMPT_EXACT or any(path.startswith(p) for p in _AUTH_EXEMPT_PREFIXES):
        return await call_next(request)

    # Only enforce auth on /api/ paths
    if not path.startswith("/api/"):
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")

    # Agent endpoints: validate shared agent key
    if path.startswith("/api/agent/"):
        if not auth_header.startswith("Bearer ") or auth_header[7:] != settings.auth.agent_key:
            return JSONResponse(status_code=401, content={"detail": "Invalid agent key"})
        return await call_next(request)

    # All other /api/ endpoints: validate JWT Bearer token
    if not auth_header.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

    token = auth_header[7:]
    try:
        payload = decode_token(token, settings.auth.jwt_secret)
        request.state.user = payload.get("sub", "")
    except jwt.PyJWTError:
        return JSONResponse(status_code=401, content={"detail": "Invalid or expired token"})

    return await call_next(request)


# ============== Auth Endpoints ==============

@app.post("/api/auth/login")
async def auth_login(request: Request):
    """Authenticate and return a JWT token."""
    client_ip = request.client.host if request.client else "unknown"

    if _is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")

    body = await request.json()
    username = body.get("username", "")
    password = body.get("password", "")

    if username == settings.auth.username and password == settings.auth.password:
        token = create_token(username, settings.auth.jwt_secret, settings.auth.jwt_expiry_hours)
        return {"token": token}

    _record_login_attempt(client_ip)
    logger.warning("Failed login attempt", username=username, client_ip=client_ip)
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/api/auth/me")
async def auth_me(request: Request):
    """Return the current authenticated user."""
    return {"username": getattr(request.state, "user", None)}


# ============== Dashboard ==============

@app.get("/api/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get dashboard statistics."""
    stats = await opensearch.get_dashboard_stats()
    
    # Add container counts
    containers = await collector.get_all_containers()
    stats.total_containers = len(containers)
    stats.running_containers = len([c for c in containers if c.status == ContainerStatus.RUNNING])
    # Total hosts = configured hosts + discovered swarm nodes (each swarm node counts as a host)
    stats.total_hosts = len(collector.clients)
    stats.healthy_hosts = len(collector.clients)  # Simplistic health check
    
    return stats


@app.get("/api/dashboard/errors-timeseries", response_model=List[TimeSeriesPoint])
async def get_errors_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get error count time series."""
    return await opensearch.get_error_timeseries(hours=hours, interval=interval)


@app.get("/api/dashboard/http-4xx-timeseries", response_model=List[TimeSeriesPoint])
async def get_http_4xx_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get HTTP 4xx count time series."""
    return await opensearch.get_http_status_timeseries(400, 500, hours=hours, interval=interval)


@app.get("/api/dashboard/http-5xx-timeseries", response_model=List[TimeSeriesPoint])
async def get_http_5xx_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get HTTP 5xx count time series."""
    return await opensearch.get_http_status_timeseries(500, 600, hours=hours, interval=interval)


@app.get("/api/dashboard/http-requests-timeseries", response_model=List[TimeSeriesPoint])
async def get_http_requests_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="1h")
):
    """Get total HTTP requests count time series."""
    return await opensearch.get_http_requests_timeseries(hours=hours, interval=interval)


@app.get("/api/dashboard/cpu-timeseries", response_model=List[TimeSeriesPoint])
async def get_cpu_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get CPU usage time series."""
    return await opensearch.get_resource_timeseries("cpu_percent", hours=hours, interval=interval)


@app.get("/api/dashboard/gpu-timeseries", response_model=List[TimeSeriesPoint])
async def get_gpu_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get GPU usage time series."""
    return await opensearch.get_resource_timeseries("gpu_percent", hours=hours, interval=interval)


@app.get("/api/dashboard/memory-timeseries", response_model=List[TimeSeriesPoint])
async def get_memory_timeseries(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get memory usage time series."""
    return await opensearch.get_resource_timeseries("memory_percent", hours=hours, interval=interval)


@app.get("/api/dashboard/cpu-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_cpu_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get CPU usage time series grouped by host."""
    return await opensearch.get_resource_timeseries_by_host("cpu_percent", hours=hours, interval=interval)


@app.get("/api/dashboard/gpu-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_gpu_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get GPU usage time series grouped by host."""
    return await opensearch.get_resource_timeseries_by_host("gpu_percent", hours=hours, interval=interval)


@app.get("/api/dashboard/memory-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_memory_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get memory usage time series grouped by host."""
    return await opensearch.get_resource_timeseries_by_host("memory_percent", hours=hours, interval=interval)


@app.get("/api/dashboard/vram-timeseries-by-host", response_model=List[TimeSeriesByHost])
async def get_vram_timeseries_by_host(
    hours: int = Query(default=24, ge=1, le=168),
    interval: str = Query(default="15m")
):
    """Get VRAM usage time series grouped by host (percentage of total)."""
    # Use gpu_memory_used_mb / gpu_memory_total_mb * 100
    return await opensearch.get_vram_percent_timeseries_by_host(hours=hours, interval=interval)


# ============== Containers ==============

@app.get("/api/containers", response_model=List[ContainerInfo])
async def list_containers(
    refresh: bool = Query(default=False),
    status: Optional[ContainerStatus] = Query(default=None),
    host: Optional[str] = Query(default=None),
    compose_project: Optional[str] = Query(default=None),
):
    """List all containers with optional filters."""
    containers = await collector.get_all_containers(refresh=refresh)
    
    # Apply filters
    if status:
        containers = [c for c in containers if c.status == status]
    if host:
        containers = [c for c in containers if c.host == host]
    if compose_project:
        containers = [c for c in containers if c.compose_project == compose_project]
    
    return containers


@app.get("/api/containers/states")
async def get_containers_states(
    refresh: bool = Query(default=True),
) -> List[Dict[str, Any]]:
    """Get lightweight container states (id, name, host, status, cpu, memory).
    
    This endpoint is designed for frequent polling to update UI without
    fetching the full container info and re-rendering everything.
    """
    containers = await collector.get_all_containers(refresh=refresh)

    # Fetch latest stats
    latest_stats = await opensearch.get_latest_container_stats()

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


@app.get("/api/containers/grouped")
async def list_containers_grouped(
    refresh: bool = Query(default=False),
    status: Optional[ContainerStatus] = Query(default=None),
    group_by: str = Query(default="host", description="Group by: 'host' or 'stack'"),
) -> Dict[str, Dict[str, List[ContainerInfo]]]:
    """List containers grouped by host and compose project, or by Docker Swarm stack."""
    containers = await collector.get_all_containers(refresh=refresh)
    
    if status:
        containers = [c for c in containers if c.status == status]
    
    # Fetch latest stats for all containers (single query)
    latest_stats = await opensearch.get_latest_container_stats()
    
    # Enrich containers with stats
    for container in containers:
        stats = latest_stats.get(container.id)
        if stats:
            container.cpu_percent = stats.get("cpu_percent")
            container.memory_percent = stats.get("memory_percent")
            container.memory_usage_mb = stats.get("memory_usage_mb")
    
    if group_by == "stack":
        # Group by Docker Swarm stack -> service
        # First, find the Swarm manager host
        swarm_manager_host = None
        for host_config in settings.hosts:
            if host_config.swarm_manager:
                swarm_manager_host = host_config.name
                break
        
        # Get stack information from manager if available
        stack_services_map: Dict[str, List[str]] = {}
        if swarm_manager_host:
            manager_client = collector.clients.get(swarm_manager_host)
            if manager_client:
                try:
                    stack_services_map = await manager_client.get_swarm_stacks()
                    logger.info("Retrieved stacks from Swarm manager", host=swarm_manager_host, stacks=list(stack_services_map.keys()))
                except Exception as e:
                    logger.warning("Failed to get stacks from Swarm manager", host=swarm_manager_host, error=str(e))
        
        # Initialize grouped structure with all known stacks and services from manager
        # This ensures stacks/services are shown even if they have no containers
        grouped: Dict[str, Dict[str, List[ContainerInfo]]] = {}
        for stack_name, services in stack_services_map.items():
            grouped[stack_name] = {}
            for service_full_name in services:
                # Use the full service name as key (e.g., "logscrawler_backend")
                # This matches what Docker returns in com.docker.swarm.service.name label
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


@app.get("/api/containers/{host}/{container_id}/stats")
async def get_container_stats(host: str, container_id: str) -> Dict[str, Any]:
    """Get current stats for a container."""
    stats = await collector.get_container_stats(host, container_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Container not found")
    return stats


@app.get("/api/containers/{host}/{container_id}/logs")
async def get_container_logs(
    host: str,
    container_id: str,
    tail: int = Query(default=200, ge=1, le=10000)
) -> List[Dict[str, Any]]:
    """Get live logs for a container."""
    logs = await collector.get_container_logs_live(host, container_id, tail=tail)
    return logs


@app.get("/api/containers/{host}/{container_id}/env")
async def get_container_env(host: str, container_id: str) -> Dict[str, Any]:
    """Get environment variables for a container by running printenv inside it."""
    env_data = await collector.get_container_env(host, container_id)
    if not env_data:
        raise HTTPException(status_code=404, detail="Container not found")
    return env_data


@app.post("/api/containers/action", response_model=ActionResult)
async def execute_container_action(request: ActionRequest) -> ActionResult:
    """Execute an action on a container (start, stop, restart, etc.).
    
    If an agent is online for the target host, the action is queued for the agent.
    Otherwise, the action is executed directly via the collector (SSH/Docker API).
    """
    # Check if an agent is online for this host
    agent_id = request.host  # Agent ID matches host name
    agent_online = await actions_queue.is_agent_online(agent_id)
    
    if agent_online:
        # Use the actions queue - agent will pick up and execute the action
        action_obj = await actions_queue.create_action(
            agent_id=agent_id,
            action_type=ActionType.CONTAINER_ACTION,
            payload={
                "container_id": request.container_id,
                "action": request.action.value,
            },
        )
        
        # Wait for the action to complete (with timeout)
        completed_action = await actions_queue.wait_for_action(action_obj.id, timeout=30.0)
        
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
    success, message = await collector.execute_action(
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


@app.post("/api/stacks/{stack_name}/remove")
async def remove_stack(stack_name: str, host: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """Remove a Docker Swarm stack."""
    # Normalize to Docker stack name (mirrors deploy-service.sh)
    stack_name = StackDeployer._repo_to_stack_name(stack_name)
    
    # Find the Swarm manager host to execute the removal
    manager_host = None
    for host_config in settings.hosts:
        if host_config.swarm_manager:
            manager_host = host_config.name
            break
    
    # If host specified, use it; otherwise try manager, then fallback to containers
    target_host = host or manager_host
    
    if not target_host:
        # Fallback: find host from containers belonging to this stack
        containers = await collector.get_all_containers(refresh=True)
        stack_containers = [
            c for c in containers 
            if c.labels.get("com.docker.swarm.stack.namespace") == stack_name
        ]
        if stack_containers:
            target_host = stack_containers[0].host
    
    if not target_host:
        raise HTTPException(status_code=404, detail=f"No host found to remove stack '{stack_name}'")
    
    # Execute stack removal
    client = collector.clients.get(target_host)
    if not client:
        raise HTTPException(status_code=404, detail=f"Host '{target_host}' not found")
    
    success, message = await client.remove_stack(stack_name)
    
    if success:
        return {"success": True, "message": message, "stack_name": stack_name}
    else:
        raise HTTPException(status_code=500, detail=message)


@app.post("/api/services/{service_name}/remove")
async def remove_service(
    service_name: str,
    host: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """Remove a Docker Swarm service."""
    # Find the Swarm manager host
    manager_host = None
    for host_config in settings.hosts:
        if host_config.swarm_manager:
            manager_host = host_config.name
            break
    
    target_host = host or manager_host
    if not target_host:
        # Fallback to first available client
        target_host = next(iter(collector.clients.keys()), None)
    
    if not target_host:
        raise HTTPException(status_code=404, detail="No host available")
    
    client = collector.clients.get(target_host)
    if not client:
        raise HTTPException(status_code=404, detail=f"Host '{target_host}' not found")
    
    success, message = await client.remove_service(service_name)
    
    if success:
        return {"success": True, "message": message, "service_name": service_name}
    else:
        raise HTTPException(status_code=500, detail=message)


@app.post("/api/services/{service_name}/update-image")
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
    
    # Find the Swarm manager host
    manager_host = None
    for host_config in settings.hosts:
        if host_config.swarm_manager:
            manager_host = host_config.name
            logger.info("[API] Found Swarm manager", manager=host_config.name, mode=host_config.mode)
            break
    
    target_host = host or manager_host
    if not target_host:
        # Fallback to first available client
        target_host = next(iter(collector.clients.keys()), None)
        logger.info("[API] Using fallback host", target_host=target_host)
    
    if not target_host:
        logger.error("[API] No host available for service update")
        raise HTTPException(status_code=404, detail="No host available")
    
    client = collector.clients.get(target_host)
    if not client:
        logger.error("[API] Host not found", target_host=target_host, available_hosts=list(collector.clients.keys()))
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
        import traceback
        error_detail = f"{type(e).__name__}: {e}"
        logger.error("[API] Exception during service update", 
                    service=service_name, tag=tag, error=error_detail, 
                    traceback=traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_detail)


@app.get("/api/services/{service_name}/logs")
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
        client = collector.clients.get(host)
    else:
        # Use the first available client (manager node)
        for name, c in collector.clients.items():
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
        raise HTTPException(status_code=500, detail=str(e))


# ============== Logs Search ==============

@app.post("/api/logs/search", response_model=LogSearchResult)
async def search_logs(query: LogSearchQuery) -> LogSearchResult:
    """Search logs with filters."""
    return await opensearch.search_logs(query)


@app.get("/api/logs/search")
async def search_logs_get(
    q: Optional[str] = Query(default=None, alias="query"),
    hosts: Optional[str] = Query(default=None),
    containers: Optional[str] = Query(default=None),
    compose_projects: Optional[str] = Query(default=None),
    levels: Optional[str] = Query(default=None),
    http_status_min: Optional[int] = Query(default=None),
    http_status_max: Optional[int] = Query(default=None),
    start_time: Optional[datetime] = Query(default=None),
    end_time: Optional[datetime] = Query(default=None),
    size: int = Query(default=100, ge=1, le=10000),
    from_: int = Query(default=0, alias="from"),
    sort_order: str = Query(default="desc"),
) -> LogSearchResult:
    """Search logs with GET parameters."""
    query = LogSearchQuery(
        query=q,
        hosts=hosts.split(",") if hosts else [],
        containers=containers.split(",") if containers else [],
        compose_projects=compose_projects.split(",") if compose_projects else [],
        levels=levels.split(",") if levels else [],
        http_status_min=http_status_min,
        http_status_max=http_status_max,
        start_time=start_time,
        end_time=end_time,
        size=size,
        from_=from_,
        sort_order=sort_order,
    )
    return await opensearch.search_logs(query)


# ============== AI Query ==============

@app.post("/api/logs/ai-search")
async def ai_search_logs(request: Dict[str, str]) -> Dict[str, Any]:
    """Convert natural language query to OpenSearch query and execute."""
    from .ai_service import get_ai_service
    
    natural_query = request.get("question", "")
    if not natural_query:
        raise HTTPException(status_code=400, detail="Question is required")
    
    ai = get_ai_service()
    
    # Get available metadata for better AI context (RAG)
    metadata = await opensearch.get_available_metadata()
    
    # Convert natural language to query params with metadata context
    params = await ai.convert_to_query(natural_query, metadata)
    
    # Calculate time range
    start_time = None
    if params.get("time_range"):
        time_str = params["time_range"]
        now = datetime.utcnow()
        if time_str.endswith("m"):
            minutes = int(time_str[:-1])
            start_time = now - timedelta(minutes=minutes)
        elif time_str.endswith("h"):
            hours = int(time_str[:-1])
            start_time = now - timedelta(hours=hours)
        elif time_str.endswith("d"):
            days = int(time_str[:-1])
            start_time = now - timedelta(days=days)
    
    # Build and execute query
    query = LogSearchQuery(
        query=params.get("query"),
        hosts=params.get("hosts", []),
        containers=params.get("containers", []),
        compose_projects=params.get("compose_projects", []),
        levels=params.get("levels", []),
        http_status_min=params.get("http_status_min"),
        http_status_max=params.get("http_status_max"),
        start_time=start_time,
        size=100,
        sort_order=params.get("sort_order", "desc"),
    )
    
    result = await opensearch.search_logs(query)
    
    return {
        "query_params": params,
        "result": result.model_dump(),
        "available_metadata": {
            "hosts": metadata.get("hosts", [])[:10],
            "containers": metadata.get("containers", [])[:20],
            "compose_projects": metadata.get("compose_projects", [])[:10],
        }
    }


@app.get("/api/ai/status")
async def get_ai_status() -> Dict[str, Any]:
    """Check AI service availability."""
    from .ai_service import get_ai_service
    
    ai = get_ai_service()
    available = await ai.check_availability()
    
    return {
        "available": available,
        "model": ai.model,
        "ollama_url": ai.ollama_url,
    }


@app.post("/api/logs/similar-count")
async def get_similar_logs_count(request: Dict[str, Any]) -> Dict[str, Any]:
    """Count similar log messages in the last N hours."""
    message = request.get("message", "")
    container_name = request.get("container_name", "")
    hours = request.get("hours", 24)
    
    if not message:
        return {"count": 0}
    
    count = await opensearch.count_similar_logs(message, container_name, hours)
    
    return {"count": count}


@app.post("/api/logs/ai-analyze")
async def analyze_log_message(request: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze a log message using AI to determine if it's normal or needs attention."""
    from .ai_service import get_ai_service
    
    message = request.get("message", "")
    level = request.get("level", "")
    container_name = request.get("container_name", "")
    
    if not message:
        return {"severity": "normal", "assessment": "No message to analyze"}
    
    ai = get_ai_service()
    result = await ai.analyze_log(message, level, container_name)
    
    return result


# ============== Hosts ==============

@app.get("/api/hosts")
async def list_hosts() -> List[Dict[str, Any]]:
    """List all hosts: configured hosts plus discovered Docker Swarm nodes.

    Each swarm node is exposed as a host so the Containers tab can show
    containers per node. Host count = configured + swarm nodes.
    """
    configured_names = {h.name for h in settings.hosts}
    result = [
        {
            "name": host.name,
            "hostname": host.hostname,
            "port": host.port,
            "username": host.username,
            "is_swarm_node": False,
        }
        for host in settings.hosts
    ]
    # Add discovered swarm nodes as hosts (so they appear in host list and Containers tab)
    for name, client in collector.clients.items():
        if name not in configured_names:
            result.append({
                "name": name,
                "hostname": client.config.hostname,
                "port": client.config.port,
                "username": client.config.username,
                "is_swarm_node": True,
            })
    return result


@app.get("/api/hosts/metrics")
async def get_hosts_metrics() -> Dict[str, Dict[str, Any]]:
    """Get latest metrics for all hosts including GPU and disk usage.
    
    Returns a dict keyed by host name with metrics:
    - cpu_percent, memory_percent, memory_used_mb, memory_total_mb
    - gpu_percent, gpu_memory_used_mb, gpu_memory_total_mb (if GPU available)
    - disk_total_gb, disk_used_gb, disk_percent
    """
    result = {}
    
    # Get latest host metrics from OpenSearch
    try:
        for host_name in collector.clients.keys():
            metrics = await opensearch.get_latest_host_metrics(host_name)
            if metrics:
                result[host_name] = metrics
    except Exception as e:
        logger.error("Failed to get host metrics", error=str(e))
    
    return result


@app.post("/api/hosts/{host_name}/action")
async def execute_host_action(host_name: str, request: Dict[str, str]) -> Dict[str, Any]:
    """Execute an action on a host (reboot or shutdown).
    
    Args:
        host_name: Name of the host to perform action on
        request: Dict with 'action' key: 'reboot' or 'shutdown'
    """
    action = request.get("action", "").lower()
    
    if action not in ("reboot", "shutdown"):
        raise HTTPException(status_code=400, detail="Invalid action. Must be 'reboot' or 'shutdown'")
    
    client = collector.clients.get(host_name)
    if not client:
        raise HTTPException(status_code=404, detail=f"Host '{host_name}' not found")
    
    try:
        if action == "reboot":
            command = "sudo reboot"
        else:
            command = "sudo shutdown -h now"
        
        # Execute the command
        success, output = await client.run_shell_command(command)
        
        # For reboot/shutdown, command may "fail" because connection drops
        # We consider it a success if it was sent
        return {
            "success": True,
            "message": f"{action.capitalize()} command sent to {host_name}",
            "host": host_name,
            "action": action,
            "output": output
        }
    except Exception as e:
        # Connection dropping during shutdown is expected
        if "disconnect" in str(e).lower() or "closed" in str(e).lower():
            return {
                "success": True,
                "message": f"{action.capitalize()} initiated on {host_name}",
                "host": host_name,
                "action": action
            }
        logger.error("Failed to execute host action", host=host_name, action=action, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to {action}: {str(e)}")


# ============== Health ==============

@app.get("/api/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy", "service": "logscrawler"}


# ============== Configuration ==============

@app.get("/api/config")
async def get_config() -> Dict[str, Any]:
    """Get current configuration (for debugging).

    Returns the loaded configuration including hosts, opensearch settings,
    and collector settings. Useful for verifying environment variables
    are being parsed correctly.
    """
    # Get list of configured hosts
    configured_hosts = [
        {
            "name": h.name,
            "hostname": h.hostname,
            "mode": h.mode,
            "docker_url": h.docker_url,
            "swarm_manager": h.swarm_manager,
            "swarm_routing": h.swarm_routing,
            "swarm_autodiscover": h.swarm_autodiscover,
        }
        for h in settings.hosts
    ]

    # Get list of active clients (including discovered nodes)
    active_clients = [
        {
            "name": name,
            "mode": client.config.mode,
            "hostname": client.config.hostname,
        }
        for name, client in collector.clients.items()
    ]

    return {
        "hosts": {
            "configured": configured_hosts,
            "active_clients": active_clients,
            "discovered_nodes": list(collector._discovered_nodes.keys()) if hasattr(collector, '_discovered_nodes') else [],
        },
        "opensearch": {
            "hosts": settings.opensearch.hosts,
            "index_prefix": settings.opensearch.index_prefix,
            "has_auth": bool(settings.opensearch.username),
        },
        "collector": {
            "log_interval_seconds": settings.collector.log_interval_seconds,
            "metrics_interval_seconds": settings.collector.metrics_interval_seconds,
            "log_lines_per_fetch": settings.collector.log_lines_per_fetch,
            "retention_days": settings.collector.retention_days,
        },
        "ai": {
            "model": settings.ai.model,
        },
        "swarm": {
            "manager_host": collector._swarm_manager_host if hasattr(collector, '_swarm_manager_host') else None,
            "routing_enabled": collector._swarm_routing_enabled if hasattr(collector, '_swarm_routing_enabled') else False,
            "autodiscover_enabled": collector._swarm_autodiscover_enabled if hasattr(collector, '_swarm_autodiscover_enabled') else False,
        }
    }


@app.get("/api/config/test")
async def test_config() -> Dict[str, Any]:
    """Test configuration by checking connectivity to all hosts.

    Returns status of each configured host including:
    - Connection status
    - Number of containers found
    - Any errors encountered
    """
    results = []

    for name, client in collector.clients.items():
        result = {
            "name": name,
            "mode": client.config.mode,
            "status": "unknown",
            "containers": 0,
            "error": None,
        }

        try:
            containers = await client.get_containers()
            result["status"] = "connected"
            result["containers"] = len(containers)
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        results.append(result)

    return {
        "hosts": results,
        "total_hosts": len(results),
        "connected": sum(1 for r in results if r["status"] == "connected"),
        "errors": sum(1 for r in results if r["status"] == "error"),
    }


# ============== Stacks (GitHub Integration) ==============

@app.get("/api/stacks/status")
async def get_stacks_status():
    """Get GitHub integration status."""
    return {
        "configured": github_service.is_configured(),
        "username": settings.github.username,
    }


@app.get("/api/stacks/repos")
async def get_starred_repos():
    """Get list of starred GitHub repositories."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    repos = await github_service.get_starred_repos()
    return {"repos": repos, "count": len(repos)}


@app.get("/api/stacks/{owner}/{repo}/branches")
async def get_repo_branches(owner: str, repo: str):
    """Get list of branches for a repository."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    branches = await github_service.get_repo_branches(owner, repo)
    return {"branches": branches, "count": len(branches)}


@app.get("/api/stacks/{owner}/{repo}/tags")
async def get_repo_tags(
    owner: str,
    repo: str,
    limit: int = Query(default=10, ge=1, le=100, description="Maximum number of tags to return"),
):
    """Get list of tags for a repository."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    tags_data = await github_service.get_repo_tags(owner, repo, limit)
    return tags_data


@app.get("/api/stacks/{owner}/{repo}/activity")
async def get_repo_activity(
    owner: str,
    repo: str,
    per_page: int = Query(default=30, ge=1, le=100, description="Commits per branch"),
):
    """Get activity data (commits from all branches, tags) for a repository."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    # Fetch branches and tags first
    branches, tags_data = await asyncio.gather(
        github_service.get_repo_branches(owner, repo),
        github_service.get_repo_tags(owner, repo, limit=50),
    )

    # Fetch commits from all branches in parallel
    commit_tasks = [
        github_service.get_repo_commits(owner, repo, branch=b["name"], per_page=per_page)
        for b in branches
    ]
    all_results = await asyncio.gather(*commit_tasks)

    # Collect any permission errors from the results
    errors = [r.get("error") for r in all_results if r.get("error")]

    # Deduplicate commits by SHA across all branches, track which branches contain each commit
    seen = {}
    commit_branches = {}  # sha -> list of branch names that contain this commit
    for branch, result in zip(branches, all_results):
        for c in result["commits"]:
            if c["sha"] not in seen:
                seen[c["sha"]] = c
            commit_branches.setdefault(c["sha"], []).append(branch["name"])

    # Sort by date descending
    commits = sorted(seen.values(), key=lambda c: c["date"], reverse=True)

    # Build maps: SHA -> branch/tag names
    branch_tip_map = {}
    for b in branches:
        branch_tip_map.setdefault(b["sha"], []).append(b["name"])

    tag_map = {}
    for t in tags_data.get("tags", []):
        tag_map.setdefault(t["sha"], []).append(t["name"])

    response = {
        "branches": branches,
        "tags": tags_data.get("tags", []),
        "commits": commits,
        "branch_tip_map": branch_tip_map,
        "tag_map": tag_map,
        "commit_branches": commit_branches,
        "default_branch": tags_data.get("default_branch", "main"),
    }
    if errors:
        response["error"] = errors[0]  # Surface first permission error to frontend
    return response


@app.get("/api/stacks/{owner}/{repo}/commits/{sha}/diff")
async def get_commit_diff(owner: str, repo: str, sha: str):
    """Get the diff for a specific commit."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    return await github_service.get_commit_diff(owner, repo, sha)


import re

def _get_deployer_and_host():
    """Helper to get deployer instance and host name."""
    if not collector.clients:
        raise HTTPException(status_code=500, detail="No host clients available")
    
    host_name = None
    host_client = None
    
    for name, client in collector.clients.items():
        if hasattr(client, 'config') and getattr(client.config, 'swarm_manager', False):
            host_name = name
            host_client = client
            break
    
    if not host_client:
        host_name, host_client = next(iter(collector.clients.items()))
    
    return StackDeployer(settings.github, host_client), host_name

@app.post("/api/stacks/build")
async def build_stack(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    version: str = Query(default="1.0", description="Version tag"),
    branch: str = Query(default=None, description="Branch name to build from"),
    commit: str = Query(default=None, description="Specific commit hash to build from"),
):
    """Build a stack from a GitHub repository. Runs in background, returns action ID."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    owner = None
    if ssh_url:
        match = re.search(r'[:/]([^/]+)/[^/]+\.git$', ssh_url)
        if match:
            owner = match.group(1)
    
    if branch and owner:
        is_valid, error_msg = await github_service.validate_branch(owner, repo_name, branch)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)
    
    if commit and owner:
        if not re.match(r'^[a-fA-F0-9]{7,40}$', commit):
            raise HTTPException(status_code=400, detail=f"Invalid commit hash format: '{commit}'. Expected 7-40 hexadecimal characters.")
        is_valid, error_msg = await github_service.validate_commit(owner, repo_name, commit)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)
    
    deployer, host_name = _get_deployer_and_host()
    
    # Create background action
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "build", repo_name)
    _background_actions[action_id] = action

    # Update pipeline state so all browsers see the build
    _pipeline_state[repo_name] = {
        "stage": "build", "status": "running",
        "build_action_id": action_id, "deploy_action_id": _pipeline_state.get(repo_name, {}).get("deploy_action_id"),
        "version": version,
    }

    async def _run_build():
        try:
            result = await deployer.build(
                repo_name, ssh_url, version, branch=branch, commit=commit,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            # Update pipeline state with result
            prev = _pipeline_state.get(repo_name, {})
            if result.get("success"):
                _pipeline_state[repo_name] = {
                    "stage": "build", "status": "success",
                    "build_action_id": action_id, "deploy_action_id": prev.get("deploy_action_id"),
                    "version": version,
                }
            else:
                _pipeline_state[repo_name] = {
                    "stage": "build", "status": "failed",
                    "build_action_id": action_id, "deploy_action_id": prev.get("deploy_action_id"),
                    "version": version,
                }
        except Exception as e:
            import traceback
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background build failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": "build", "repo": repo_name}
            action.append_output(error_detail)
            prev = _pipeline_state.get(repo_name, {})
            _pipeline_state[repo_name] = {
                "stage": "build", "status": "failed",
                "build_action_id": action_id, "deploy_action_id": prev.get("deploy_action_id"),
                "version": version,
            }

    action.task = asyncio.create_task(_run_build())

    return {"action_id": action_id, "action_type": "build", "repo": repo_name}


@app.post("/api/stacks/deploy")
async def deploy_stack(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    version: str = Query(default="1.0", description="Version tag"),
    tag: str = Query(default=None, description="Specific tag to deploy (format: vX.X.X)"),
):
    """Deploy a stack from a GitHub repository. Runs in background, returns action ID."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    if tag:
        if not re.match(r'^v?\d+(\.\d+){0,2}$', tag):
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X (e.g., v1.0.5) or X.X.X"
            )
    
    deployer, host_name = _get_deployer_and_host()
    
    # Create background action
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "deploy", repo_name)
    _background_actions[action_id] = action

    # Update pipeline state so all browsers see the deploy
    prev = _pipeline_state.get(repo_name, {})
    _pipeline_state[repo_name] = {
        "stage": "deploy", "status": "running",
        "build_action_id": prev.get("build_action_id"), "deploy_action_id": action_id,
        "version": prev.get("version") or version,
    }

    async def _run_deploy():
        try:
            result = await deployer.deploy(
                repo_name, ssh_url, version, tag=tag,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            # Update pipeline state with result
            prev = _pipeline_state.get(repo_name, {})
            if result.get("success"):
                _pipeline_state[repo_name] = {
                    "stage": "done", "status": "success",
                    "build_action_id": prev.get("build_action_id"), "deploy_action_id": action_id,
                    "version": prev.get("version") or version,
                }
            else:
                _pipeline_state[repo_name] = {
                    "stage": "deploy", "status": "failed",
                    "build_action_id": prev.get("build_action_id"), "deploy_action_id": action_id,
                    "version": prev.get("version") or version,
                }
        except Exception as e:
            import traceback
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background deploy failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": "deploy", "repo": repo_name}
            action.append_output(error_detail)
            prev = _pipeline_state.get(repo_name, {})
            _pipeline_state[repo_name] = {
                "stage": "deploy", "status": "failed",
                "build_action_id": prev.get("build_action_id"), "deploy_action_id": action_id,
                "version": prev.get("version") or version,
            }

    action.task = asyncio.create_task(_run_deploy())

    return {"action_id": action_id, "action_type": "deploy", "repo": repo_name}


@app.get("/api/stacks/actions/{action_id}/status")
async def get_action_status(action_id: str) -> Dict[str, Any]:
    """Get the status of a background build/deploy action."""
    action = _background_actions.get(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")
    
    response = {
        "action_id": action.id,
        "action_type": action.action_type,
        "repo": action.repo_name,
        "status": action.status,
        "started_at": action.started_at.isoformat(),
        "elapsed_seconds": (datetime.utcnow() - action.started_at).total_seconds(),
    }
    
    if action.result:
        response["result"] = action.result
    
    return response


@app.get("/api/stacks/actions/{action_id}/logs")
async def get_action_logs(
    action_id: str,
    offset: int = Query(default=0, ge=0, description="Line offset to start from"),
) -> Dict[str, Any]:
    """Get the streaming logs of a background build/deploy action."""
    action = _background_actions.get(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")
    
    lines = action.output_lines[offset:]
    
    return {
        "action_id": action.id,
        "status": action.status,
        "lines": lines,
        "offset": offset,
        "total_lines": len(action.output_lines),
    }


@app.post("/api/stacks/actions/{action_id}/cancel")
async def cancel_action(action_id: str) -> Dict[str, Any]:
    """Cancel a running background build/deploy action."""
    action = _background_actions.get(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")
    
    if action.status != "running":
        return {"success": False, "message": f"Action is already {action.status}"}
    
    action.cancel_event.set()
    action.append_output("[Cancellation requested...]")
    
    # Wait briefly for the task to acknowledge cancellation
    try:
        await asyncio.wait_for(asyncio.shield(action.task), timeout=5)
    except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
        pass
    
    if action.status == "running":
        action.status = "cancelled"
    
    return {"success": True, "message": "Action cancelled"}


@app.get("/api/stacks/{repo_name}/env")
async def get_stack_env(repo_name: str):
    """Get the .env file content for a stack."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    deployer = StackDeployer(settings.github, None)
    success, content = await deployer.get_env_file(repo_name)
    
    if not success:
        raise HTTPException(status_code=500, detail=content)
    
    return {"content": content, "repo": repo_name}


@app.put("/api/stacks/{repo_name}/env")
async def save_stack_env(repo_name: str, request: Request):
    """Save the .env file content for a stack."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    body = await request.json()
    content = body.get("content", "")
    
    deployer = StackDeployer(settings.github, None)
    success, message = await deployer.save_env_file(repo_name, content)
    
    if not success:
        raise HTTPException(status_code=500, detail=message)
    
    return {"success": True, "message": message, "repo": repo_name}


@app.get("/api/stacks/deployed-tags")
async def get_stacks_deployed_tags():
    """Get deployed image tags and latest built tags for all stacks."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    repos = await github_service.get_starred_repos()
    deployer = StackDeployer(settings.github, None)

    # Fetch all deployed tags in a single SSH command (avoids concurrent SSH issues)
    all_deployed = await deployer.get_all_deployed_stack_tags(
        [repo["name"] for repo in repos]
    )

    # Fetch latest built tags from GitHub in parallel
    async def get_latest(repo):
        return repo["name"], await github_service.get_latest_tag(repo["owner"], repo["name"])

    latest_results = await asyncio.gather(*[get_latest(r) for r in repos])

    deployed_tags = {name: tag for name, tag in all_deployed.items() if tag}
    latest_built = {name: tag for name, tag in latest_results if tag}

    return {"tags": deployed_tags, "latest_built": latest_built}


# ============== Pipeline (Auto Build → Test → Deploy) ==============

_auto_build_state = {}  # {repo_name: {"last_sha": str, "building": bool}}
_pipeline_state = {}  # {repo_name: {"stage": str, "status": str, "build_action_id": str|None, "deploy_action_id": str|None, "version": str}}
_auto_build_task = None
AUTO_BUILD_POLL_INTERVAL = 120  # seconds (was 20s, increased to avoid GitHub rate limits)

# Pipeline stages in order
PIPELINE_STAGES = ["build", "test", "deploy", "done"]


def _extract_version_from_output(lines: list) -> Optional[str]:
    """Parse the built version from build script output.

    Build script outputs: 'Version: 1.0.X (tag: v1.0.X)'
    """
    for line in lines:
        if "Version:" in line:
            m = re.search(r'Version:\s*(\d+\.\d+\.\d+)', line)
            if m:
                return m.group(1)
    return None


async def _trigger_pipeline(repo_name: str, ssh_url: str):
    """Trigger a full pipeline: build → test (skip) → deploy."""
    try:
        deployer, host_name = _get_deployer_and_host()
    except Exception as e:
        logger.error("Pipeline: no host available", repo=repo_name, error=str(e))
        _pipeline_state[repo_name] = {"stage": "build", "status": "failed", "build_action_id": None, "deploy_action_id": None, "version": None}
        _auto_build_state[repo_name]["building"] = False
        return

    _pipeline_state[repo_name] = {"stage": "build", "status": "running", "build_action_id": None, "deploy_action_id": None, "version": None}

    async def _run_pipeline():
        built_version = None
        try:
            # ── Step 1: Build ──
            build_id = str(uuid.uuid4())[:8]
            build_action = BackgroundAction(build_id, "build", repo_name)
            _background_actions[build_id] = build_action
            _pipeline_state[repo_name]["build_action_id"] = build_id

            logger.info("Pipeline: starting build", repo=repo_name, action_id=build_id)
            result = await deployer.build(
                repo_name, ssh_url, version="1.0",
                output_callback=build_action.append_output,
                cancel_event=build_action.cancel_event,
            )
            result["host"] = host_name
            result["auto_triggered"] = True
            build_action.result = result
            build_action.status = "completed" if result.get("success") else "failed"

            if not result.get("success"):
                _pipeline_state[repo_name] = {"stage": "build", "status": "failed", "build_action_id": build_id, "deploy_action_id": None, "version": None}
                return

            built_version = _extract_version_from_output(build_action.output_lines)
            _pipeline_state[repo_name]["version"] = built_version
            logger.info("Pipeline: build succeeded", repo=repo_name, version=built_version)

            # ── Step 2: Test (placeholder - auto-skip) ──
            _pipeline_state[repo_name] = {"stage": "test", "status": "running", "build_action_id": build_id, "deploy_action_id": None, "version": built_version}
            await asyncio.sleep(1)
            _pipeline_state[repo_name] = {"stage": "test", "status": "success", "build_action_id": build_id, "deploy_action_id": None, "version": built_version}

            # ── Step 3: Deploy ──
            _pipeline_state[repo_name] = {"stage": "deploy", "status": "running", "build_action_id": build_id, "deploy_action_id": None, "version": built_version}
            deploy_id = str(uuid.uuid4())[:8]
            deploy_action = BackgroundAction(deploy_id, "deploy", repo_name)
            _background_actions[deploy_id] = deploy_action
            _pipeline_state[repo_name]["deploy_action_id"] = deploy_id

            tag = f"v{built_version}" if built_version else None
            logger.info("Pipeline: starting deploy", repo=repo_name, action_id=deploy_id, tag=tag)
            deploy_result = await deployer.deploy(
                repo_name, ssh_url, version="1.0",
                tag=tag,
                output_callback=deploy_action.append_output,
                cancel_event=deploy_action.cancel_event,
            )
            deploy_result["host"] = host_name
            deploy_result["auto_triggered"] = True
            deploy_action.result = deploy_result
            deploy_action.status = "completed" if deploy_result.get("success") else "failed"

            if deploy_result.get("success"):
                _pipeline_state[repo_name] = {"stage": "done", "status": "success", "build_action_id": build_id, "deploy_action_id": deploy_id, "version": built_version}
                logger.info("Pipeline: deploy succeeded", repo=repo_name, version=built_version)
            else:
                _pipeline_state[repo_name] = {"stage": "deploy", "status": "failed", "build_action_id": build_id, "deploy_action_id": deploy_id, "version": built_version}

        except Exception as e:
            logger.exception("Pipeline failed", repo=repo_name, error=str(e))
            current = _pipeline_state.get(repo_name, {})
            _pipeline_state[repo_name] = {
                "stage": current.get("stage", "build"),
                "status": "failed",
                "build_action_id": current.get("build_action_id"),
                "deploy_action_id": current.get("deploy_action_id"),
                "version": built_version,
            }
        finally:
            if repo_name in _auto_build_state:
                _auto_build_state[repo_name]["building"] = False

    asyncio.create_task(_run_pipeline())
    logger.info("Pipeline triggered", repo=repo_name)


async def auto_build_poller():
    """Periodically check starred repos for new commits on default branch and trigger pipeline."""
    await asyncio.sleep(10)

    while True:
        try:
            if github_service and github_service.is_configured():
                repos = await github_service.get_starred_repos()

                for repo in repos:
                    owner, name, ssh_url = repo["owner"], repo["name"], repo["ssh_url"]

                    commits_data = await github_service.get_repo_commits(owner, name, per_page=1)
                    commits = commits_data.get("commits", [])
                    if not commits:
                        continue

                    latest_sha = commits[0]["sha"]
                    state = _auto_build_state.get(name)

                    if state is None:
                        _auto_build_state[name] = {"last_sha": latest_sha, "building": False}
                        continue

                    if latest_sha != state["last_sha"] and not state.get("building"):
                        logger.info("New commit detected, triggering pipeline",
                                    repo=name,
                                    old_sha=state["last_sha"][:7],
                                    new_sha=latest_sha[:7])
                        _auto_build_state[name]["building"] = True
                        _auto_build_state[name]["last_sha"] = latest_sha
                        await _trigger_pipeline(name, ssh_url)

        except Exception as e:
            logger.error("Auto-build poller error", error=str(e))

        await asyncio.sleep(AUTO_BUILD_POLL_INTERVAL)


@app.get("/api/stacks/pipeline/status")
async def get_pipeline_status():
    """Get pipeline state for all repos."""
    return {"pipelines": _pipeline_state}


@app.get("/api/stacks/auto-build/status")
async def get_auto_build_status():
    """Get auto-build state for all repos."""
    return {"state": _auto_build_state}


# ============== Agent API ==
# These endpoints are used by agents running on remote hosts

@app.get("/api/agent/actions")
async def get_agent_actions(agent_id: str = Query(..., description="Agent identifier")):
    """Get pending actions for an agent.

    Agents poll this endpoint to receive actions to execute.
    Actions are marked as in_progress when returned.
    """
    actions = await actions_queue.get_pending_actions(agent_id)
    return {
        "agent_id": agent_id,
        "actions": [action.model_dump() for action in actions],
    }


@app.post("/api/agent/result")
async def post_agent_result(
    agent_id: str = Query(..., description="Agent identifier"),
    action_id: str = Query(..., description="Action ID"),
    success: bool = Query(..., description="Whether action succeeded"),
    output: str = Query(default="", description="Action output"),
):
    """Report action result from an agent.

    Agents call this after executing an action to report the result.
    """
    action = await actions_queue.complete_action(action_id, success, output)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    return {"status": "ok", "action_id": action_id}


@app.get("/api/agents")
async def get_agents():
    """Get list of known agents and their status."""
    agents = await actions_queue.get_agents()
    result = []
    for agent in agents:
        is_online = await actions_queue.is_agent_online(agent.agent_id)
        result.append({
            "agent_id": agent.agent_id,
            "last_seen": agent.last_seen.isoformat(),
            "status": agent.status,
            "online": is_online,
        })
    return {"agents": result}


@app.post("/api/agent/action")
async def create_agent_action(
    agent_id: str = Query(..., description="Target agent identifier"),
    action_type: str = Query(..., description="Action type (container_action, exec, get_logs, get_env)"),
    container_id: Optional[str] = Query(default=None, description="Container ID for container actions"),
    action: Optional[str] = Query(default=None, description="Container action (start, stop, restart, etc.)"),
    command: Optional[str] = Query(default=None, description="Command to execute (for exec action)"),
    tail: Optional[int] = Query(default=100, description="Number of log lines (for get_logs action)"),
    wait: bool = Query(default=True, description="Wait for action to complete"),
    timeout: float = Query(default=30.0, description="Timeout in seconds when waiting"),
):
    """Create an action for an agent to execute.

    This is the main endpoint for the frontend/API to request actions on remote hosts.
    The action is queued and the agent will pick it up on next poll.

    If wait=True, the endpoint blocks until the action completes or times out.
    """
    # Validate action type
    try:
        action_type_enum = ActionType(action_type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid action type: {action_type}")

    # Build payload based on action type
    payload = {}
    if action_type_enum == ActionType.CONTAINER_ACTION:
        if not container_id or not action:
            raise HTTPException(status_code=400, detail="container_id and action required for container_action")
        payload = {"container_id": container_id, "action": action}

    elif action_type_enum == ActionType.EXEC:
        if not container_id or not command:
            raise HTTPException(status_code=400, detail="container_id and command required for exec")
        # Parse command string into list
        import shlex
        try:
            cmd_list = shlex.split(command)
        except ValueError:
            cmd_list = command.split()
        payload = {"container_id": container_id, "command": cmd_list}

    elif action_type_enum == ActionType.GET_LOGS:
        if not container_id:
            raise HTTPException(status_code=400, detail="container_id required for get_logs")
        payload = {"container_id": container_id, "tail": tail}

    elif action_type_enum == ActionType.GET_ENV:
        if not container_id:
            raise HTTPException(status_code=400, detail="container_id required for get_env")
        payload = {"container_id": container_id}

    # Create the action
    action_obj = await actions_queue.create_action(agent_id, action_type_enum, payload)

    if not wait:
        return {
            "action_id": action_obj.id,
            "status": action_obj.status,
            "message": "Action queued",
        }

    # Wait for action to complete
    completed_action = await actions_queue.wait_for_action(action_obj.id, timeout=timeout)

    if not completed_action:
        return {
            "action_id": action_obj.id,
            "status": "timeout",
            "message": "Action timed out waiting for agent",
        }

    return {
        "action_id": completed_action.id,
        "status": completed_action.status,
        "success": completed_action.success,
        "result": completed_action.result,
    }


# ============== Terminal WebSocket ==============

def _find_swarm_manager_config():
    """Find the swarm manager host config."""
    for host_config in settings.hosts:
        if host_config.swarm_manager:
            return host_config
    if settings.hosts:
        return settings.hosts[0]
    return None


@app.websocket("/api/terminal/ws")
async def terminal_websocket(
    websocket: WebSocket,
    cols: int = Query(default=80),
    rows: int = Query(default=24),
    token: str = Query(default=""),
):
    """Interactive terminal session on the swarm manager host."""
    # Validate JWT token before accepting the WebSocket
    try:
        if not token:
            await websocket.close(code=4001, reason="Missing token")
            return
        decode_token(token, settings.auth.jwt_secret)
    except jwt.PyJWTError:
        await websocket.close(code=4001, reason="Invalid token")
        return

    await websocket.accept()

    manager_config = _find_swarm_manager_config()
    if not manager_config:
        await websocket.send_text("\x1b[31mError: No host configured.\x1b[0m\r\n")
        await websocket.close()
        return

    session_id = str(uuid.uuid4())[:8]
    logger.info("Terminal session starting", session=session_id, host=manager_config.name, mode=manager_config.mode)

    try:
        if manager_config.mode == "ssh":
            await _handle_ssh_terminal(websocket, manager_config, cols, rows, session_id)
        else:
            await _handle_local_terminal(websocket, cols, rows, session_id)
    except WebSocketDisconnect:
        logger.info("Terminal session disconnected", session=session_id)
    except Exception as e:
        logger.error("Terminal session error", session=session_id, error=str(e))
        try:
            await websocket.send_text(f"\r\n\x1b[31mSession error: {e}\x1b[0m\r\n")
        except Exception:
            pass
    finally:
        logger.info("Terminal session ended", session=session_id)


async def _handle_ssh_terminal(websocket, host_config, cols, rows, session_id):
    """Interactive SSH terminal session via asyncssh PTY."""
    import asyncssh
    from pathlib import Path

    options = {
        "host": host_config.hostname,
        "port": host_config.port,
        "username": host_config.username,
        "known_hosts": None,
    }
    if host_config.ssh_key_path:
        key_path = Path(host_config.ssh_key_path).expanduser()
        options["client_keys"] = [str(key_path)]

    async with asyncssh.connect(**options) as conn:
        process = await conn.create_process(
            term_type="xterm-256color",
            term_size=(cols, rows),
            encoding=None,
        )

        logger.info("SSH PTY session opened", session=session_id, host=host_config.hostname)

        async def _read_stdout():
            try:
                while True:
                    data = await process.stdout.read(4096)
                    if not data:
                        break
                    await websocket.send_bytes(data)
            except Exception:
                pass

        async def _read_stderr():
            try:
                while True:
                    data = await process.stderr.read(4096)
                    if not data:
                        break
                    await websocket.send_bytes(data)
            except Exception:
                pass

        read_task = asyncio.create_task(_read_stdout())
        stderr_task = asyncio.create_task(_read_stderr())

        try:
            while True:
                message = await websocket.receive()

                if message["type"] == "websocket.disconnect":
                    break

                if "text" in message:
                    text = message["text"]
                    if text.startswith("{"):
                        try:
                            cmd = json.loads(text)
                            if cmd.get("type") == "resize":
                                process.change_terminal_size(cmd.get("cols", 80), cmd.get("rows", 24))
                                continue
                        except json.JSONDecodeError:
                            pass
                    process.stdin.write(text.encode("utf-8"))
                elif "bytes" in message:
                    process.stdin.write(message["bytes"])
        finally:
            read_task.cancel()
            stderr_task.cancel()
            process.close()
            try:
                await process.wait_closed()
            except Exception:
                pass


async def _handle_local_terminal(websocket, cols, rows, session_id):
    """Interactive local terminal session via PTY."""
    import pty
    import os
    import fcntl
    import struct
    import termios
    import signal

    master_fd, slave_fd = pty.openpty()

    winsize = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)

    if settings.run_user:
        shell_cmd = ["/bin/su", "-", settings.run_user]
    else:
        shell_cmd = ["/bin/bash", "--login"]

    process = await asyncio.create_subprocess_exec(
        *shell_cmd,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        preexec_fn=os.setsid,
        env={
            **os.environ,
            "TERM": "xterm-256color",
            "COLUMNS": str(cols),
            "LINES": str(rows),
        },
    )

    os.close(slave_fd)

    logger.info("Local PTY session opened", session=session_id, pid=process.pid)

    loop = asyncio.get_event_loop()

    async def _read_pty_output():
        try:
            while True:
                data = await loop.run_in_executor(None, os.read, master_fd, 4096)
                if not data:
                    break
                await websocket.send_bytes(data)
        except OSError:
            pass

    read_task = asyncio.create_task(_read_pty_output())

    try:
        while True:
            message = await websocket.receive()

            if message["type"] == "websocket.disconnect":
                break

            if "text" in message:
                text = message["text"]
                if text.startswith("{"):
                    try:
                        cmd = json.loads(text)
                        if cmd.get("type") == "resize":
                            winsize = struct.pack("HHHH", cmd.get("rows", 24), cmd.get("cols", 80), 0, 0)
                            fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)
                            os.kill(process.pid, signal.SIGWINCH)
                            continue
                    except json.JSONDecodeError:
                        pass
                os.write(master_fd, text.encode("utf-8"))
            elif "bytes" in message:
                os.write(master_fd, message["bytes"])
    finally:
        read_task.cancel()
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5)
        except Exception:
            process.kill()
        try:
            os.close(master_fd)
        except Exception:
            pass


# Serve static files (frontend)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")


@app.get("/")
async def serve_frontend():
    """Serve the frontend."""
    return FileResponse("frontend/index.html")
