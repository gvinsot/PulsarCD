"""FastAPI REST API for PulsarCD."""

import asyncio
import json
import re
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

import jwt
import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

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
from .pipeline_state import PipelineStateManager, _UNSET
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
error_detector = None
user_manager = None
llm_agent = None
pipeline_state: Optional[PipelineStateManager] = None


async def _notify_agent_failure(stage: str, repo_name: str, version: str, error_output: str):
    """Notify the LLM agent of a build/test/deploy failure so it can investigate and act."""
    if llm_agent is None:
        logger.debug("LLM agent not configured, skipping failure notification")
        return
    try:
        await llm_agent.handle_failure(stage, repo_name, version, error_output)
    except Exception as e:
        logger.error("LLM agent failure handling error",
                     stage=stage, repo=repo_name,
                     error_type=type(e).__name__, error=str(e))


# ============== Background Actions (Build/Deploy) ==============

class BackgroundAction:
    """Tracks a background build or deploy action."""
    
    def __init__(self, action_id: str, action_type: str, repo_name: str):
        self.id = action_id
        self.action_type = action_type  # "build" or "deploy"
        self.repo_name = repo_name
        self._status = "running"  # running, completed, failed, cancelled
        self.output_lines: List[str] = []
        self.result: Optional[Dict[str, Any]] = None
        self.started_at = datetime.utcnow()
        self.cancel_event = asyncio.Event()
        self.task: Optional[asyncio.Task] = None
        self.new_line_event = asyncio.Event()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        self.new_line_event.set()

    def append_output(self, line: str):
        self.output_lines.append(line)
        self.new_line_event.set()
    
    def get_output(self) -> str:
        return "\n".join(self.output_lines)

# Store of running/recent background actions
_background_actions: Dict[str, BackgroundAction] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global settings, opensearch, collector, github_service, error_detector, user_manager, llm_agent, pipeline_state

    # Startup
    logger.info("Starting PulsarCD API")

    settings = load_config()
    opensearch = OpenSearchClient(settings.opensearch)

    # Initialize persistent pipeline state
    pipeline_state = PipelineStateManager.get_instance(settings.data_dir)
    logger.info("Pipeline state manager initialized", data_dir=settings.data_dir)

    # Initialize user manager (file-based multi-user auth)
    from .user_manager import UserManager
    user_manager = UserManager(path=f"{settings.data_dir}/users.json")

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

    # Initialize LLM agent for error handling (replaces Swarm API notifications)
    if settings.pulsar_config:
        try:
            from .llm_agent import LLMAgent
            llm_agent = LLMAgent(
                config=settings.pulsar_config,
                mcp_api_key=settings.mcp.api_key,
                data_dir=settings.data_dir,
            )
            logger.info("LLM agent initialized for error handling")
        except Exception as e:
            logger.warning("Failed to initialize LLM agent", error=str(e))

    # Start recurring error detector (always — LLM agent notifications are optional
    # but the detection and history still run)
    from .error_detector import RecurringErrorDetector
    error_detector = RecurringErrorDetector(
        opensearch_client=opensearch,
        llm_agent=llm_agent,
        github_service=github_service,
        pipeline_state=pipeline_state,
    )
    await error_detector.start()
    logger.info("Recurring error detector started")

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
    logger.info("Shutting down PulsarCD API")
    if error_detector:
        await error_detector.stop()
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
    title="PulsarCD API",
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
    # Support token via query param for SSE (EventSource can't set headers)
    token = None
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
    elif request.query_params.get("token"):
        token = request.query_params["token"]

    if not token:
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
    try:
        payload = decode_token(token, settings.auth.jwt_secret)
        request.state.user = payload.get("sub", "")
        request.state.role = payload.get("role", "viewer")
    except jwt.PyJWTError:
        return JSONResponse(status_code=401, content={"detail": "Invalid or expired token"})

    # Admin-only paths
    if path.startswith("/api/admin/") and getattr(request.state, "role", "") != "admin":
        return JSONResponse(status_code=403, content={"detail": "Admin access required"})

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

    user = user_manager.authenticate(username, password) if user_manager else None
    if user:
        token = create_token(username, settings.auth.jwt_secret, settings.auth.jwt_expiry_hours, role=user.role)
        return {"token": token}

    _record_login_attempt(client_ip)
    logger.warning("Failed login attempt", username=username, client_ip=client_ip)
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/api/auth/me")
async def auth_me(request: Request):
    """Return the current authenticated user."""
    return {
        "username": getattr(request.state, "user", None),
        "role": getattr(request.state, "role", "viewer"),
    }


# ============== Admin: User Management ==============

@app.get("/api/admin/users")
async def admin_list_users():
    """List all users (admin only)."""
    return {"users": user_manager.list_users()}


@app.post("/api/admin/users")
async def admin_create_user(request: Request):
    """Create a new user (admin only)."""
    body = await request.json()
    username = body.get("username", "").strip()
    password = body.get("password", "")
    role = body.get("role", "viewer")

    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password are required")

    try:
        user = await user_manager.create_user(username, password, role)
        return user
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/api/admin/users/{username}")
async def admin_update_user(username: str, request: Request):
    """Update an existing user (admin only)."""
    body = await request.json()
    password = body.get("password")
    role = body.get("role")

    try:
        user = await user_manager.update_user(username, password=password, role=role)
        return user
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/admin/users/{username}")
async def admin_delete_user(username: str):
    """Delete a user (admin only)."""
    try:
        await user_manager.delete_user(username)
        return {"deleted": True}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============== Admin: Configuration ==============

@app.get("/api/admin/config")
async def admin_get_config():
    """Get current configuration (admin only)."""
    if settings.pulsar_config:
        return settings.pulsar_config.model_dump()
    return {}


@app.put("/api/admin/config")
async def admin_update_config(request: Request):
    """Update configuration file (admin only)."""
    from .config_file import PulsarConfig, save_config_file, _apply_env_overrides
    body = await request.json()
    try:
        new_config = PulsarConfig(**body)
        save_config_file(new_config, settings.data_dir)
        settings.pulsar_config = _apply_env_overrides(new_config)

        # Update LLM agent with new config
        if llm_agent:
            llm_agent.invalidate_tools_cache()
            llm_agent._error_handling = new_config.error_handling
            llm_agent._pipeline_gates = new_config.pipeline_gates
            llm_agent._mcp_servers = new_config.mcp_servers
            llm_agent._llm_url = new_config.llm.url.rstrip("/")
            llm_agent._llm_model = new_config.llm.model
            llm_agent._llm_api_key = new_config.llm.api_key

        return {"saved": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/admin/mcp-test")
async def admin_mcp_test(request: Request):
    """Test connectivity to an MCP server by calling tools/list."""
    body = await request.json()
    url = body.get("url", "").strip().rstrip("/")
    api_key = body.get("api_key", "").strip()

    if not url:
        raise HTTPException(status_code=400, detail="url is required")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {"jsonrpc": "2.0", "method": "tools/list", "id": 1}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                if "error" in data:
                    error = data["error"]
                    msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                    return {"ok": False, "error": msg}
                mcp_tools = data.get("result", {}).get("tools", [])
                tool_names = [t.get("name", "?") for t in mcp_tools]
                return {"ok": True, "tools": tool_names, "count": len(tool_names)}
    except aiohttp.ClientError as e:
        return {"ok": False, "error": f"Connection failed: {e}"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/admin/llm-test")
async def admin_llm_test(request: Request):
    """Test LLM agent with a user message (admin only). Uses current config."""
    if not llm_agent:
        raise HTTPException(status_code=503, detail="LLM agent not initialized")

    body = await request.json()
    user_message = body.get("message", "").strip()
    if not user_message:
        raise HTTPException(status_code=400, detail="Empty message")

    # Use the error handling instructions as system prompt context
    eh = llm_agent._error_handling
    system_prompt = (
        f"{eh.instructions}\n\n"
        f"You have access to MCP tools. Use them to answer the user's questions.\n"
        f"Respond concisely."
    )

    try:
        result = await llm_agent._run_agent(system_prompt, user_message)
        llm_agent._record("chat", message=user_message[:200], response=result[:500] if result else "")
        return {"response": result}
    except Exception as e:
        logger.error("LLM test error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/agent-history")
async def admin_agent_history(page: int = 1, page_size: int = 15):
    """Get LLM agent action history (admin only), paginated."""
    if not llm_agent:
        return {"history": [], "total": 0, "page": 1, "page_size": page_size, "total_pages": 0}
    all_history = llm_agent.get_history()
    total = len(all_history)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    return {"history": all_history[start:end], "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}


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


@app.get("/api/dashboard/recurring-errors")
async def get_recurring_errors(limit: int = Query(default=5, ge=1, le=50)) -> List[Dict[str, Any]]:
    """Return the most recent notified recurring error patterns."""
    if not error_detector:
        return []
    return error_detector._notification_history[:limit]


@app.get("/api/admin/error-detector-status")
async def get_error_detector_status():
    """Diagnostic endpoint: return the error detector's internal state."""
    if not error_detector:
        return {"error": "Error detector not initialized"}
    return error_detector.get_status()


@app.get("/api/admin/opensearch-status")
async def get_opensearch_status():
    """Diagnostic endpoint: check OpenSearch index health, doc counts, mappings, and samples."""
    result = {}

    # Cluster info
    try:
        import opensearchpy as _ospy
        result["_client_version"] = getattr(_ospy, "__versionstr__", "unknown")
    except Exception:
        pass
    try:
        info = await opensearch._client.info()
        result["_cluster_name"] = info.get("cluster_name")
        result["_server_version"] = info.get("version", {}).get("number")
    except Exception as e:
        result["_cluster_error"] = f"{type(e).__name__}: {str(e)[:300]}"

    for index_name in [opensearch.logs_index, opensearch.metrics_index, opensearch.host_metrics_index]:
        try:
            exists = await opensearch._client.indices.exists(index=index_name)
            if exists:
                count_resp = await opensearch._client.count(index=index_name)
                doc_count = count_resp.get("count", 0)

                # Get latest document
                latest = await opensearch._client.search(
                    index=index_name,
                    body={"size": 1, "sort": [{"timestamp": "desc"}]},
                )
                latest_hit = latest.get("hits", {}).get("hits", [])
                latest_doc = latest_hit[0]["_source"] if latest_hit else None
                latest_ts = latest_doc.get("timestamp") if latest_doc else None
                latest_host = latest_doc.get("host") if latest_doc else None

                # Get oldest document
                oldest = await opensearch._client.search(
                    index=index_name,
                    body={"size": 1, "sort": [{"timestamp": "asc"}], "_source": ["timestamp"]},
                )
                oldest_hit = oldest.get("hits", {}).get("hits", [])
                oldest_ts = oldest_hit[0]["_source"].get("timestamp") if oldest_hit else None

                # Get mapping (field types)
                mapping_resp = await opensearch._client.indices.get_mapping(index=index_name)
                mapping = mapping_resp.get(index_name, {}).get("mappings", {}).get("properties", {})
                field_types = {k: v.get("type", "?") for k, v in mapping.items()}

                # Get index settings
                settings_resp = await opensearch._client.indices.get_settings(index=index_name)
                idx_settings = settings_resp.get(index_name, {}).get("settings", {}).get("index", {})

                result[index_name] = {
                    "exists": True,
                    "doc_count": doc_count,
                    "oldest_timestamp": oldest_ts,
                    "latest_timestamp": latest_ts,
                    "latest_host": latest_host,
                    "latest_doc_sample": latest_doc,
                    "field_types": field_types,
                    "index_uuid": idx_settings.get("uuid"),
                    "refresh_interval": idx_settings.get("refresh_interval"),
                }
            else:
                result[index_name] = {"exists": False}
        except Exception as e:
            result[index_name] = {"exists": "unknown", "error": str(e)[:500]}
    return result


@app.post("/api/admin/opensearch-recreate-index")
async def recreate_opensearch_index(
    index: str = Query(..., description="Index name to recreate"),
):
    """Force delete and recreate an OpenSearch index with correct mapping.
    WARNING: This deletes all data in the index. Agents will repopulate it.
    """
    try:
        result = await opensearch.recreate_index(index)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
            container.gpu_percent = stats.get("gpu_percent")
            container.gpu_memory_used_mb = stats.get("gpu_memory_used_mb")
    
    if group_by == "stack":
        # Group by Docker Swarm stack -> service
        swarm_manager_host = _get_swarm_manager_host()

        # Build stack_services_map only for name resolution (not pre-population)
        stack_services_map: Dict[str, List[str]] = {}
        if swarm_manager_host:
            manager_client = collector.clients.get(swarm_manager_host)
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


@app.get("/api/containers/{host}/{container_id}/stats")
async def get_container_stats(host: str, container_id: str) -> Dict[str, Any]:
    """Get current stats for a container."""
    stats = await collector.get_container_stats(host, container_id)
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


@app.get("/api/containers/{host}/{container_id}/metrics")
async def get_container_metrics(
    host: str,
    container_id: str,
    period: str = Query(default="7d"),
) -> Dict[str, Any]:
    """Return CPU%, memory% and error-count time series for a container."""
    hours, interval = _PERIOD_MAP.get(period, _PERIOD_MAP["7d"])
    if not opensearch:
        return {"cpu": [], "memory": [], "errors": []}
    return await opensearch.get_container_metrics_timeseries(container_id, hours, interval)


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
    # Check if an agent is online for this host.
    # The agent registers with its own hostname which may differ from the Docker node
    # hostname stored in ContainerInfo.host (e.g. FQDN vs short name, or configured
    # name vs actual hostname).  Try an exact match first, then fall back to a
    # case-insensitive short-hostname comparison across all online agents.
    agent_id = request.host
    agent_online = await actions_queue.is_agent_online(agent_id)

    if not agent_online:
        short_host = request.host.split('.')[0].lower()
        for agent in await actions_queue.get_agents():
            if await actions_queue.is_agent_online(agent.agent_id):
                if agent.agent_id.split('.')[0].lower() == short_host:
                    agent_id = agent.agent_id
                    agent_online = True
                    break

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
    
    # If host specified, use it; otherwise try manager, then fallback to containers
    target_host = host or _get_swarm_manager_host()
    
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
        # Reset pipeline state so the stack shows as undeployed
        repo_name = pipeline_state.find_repo_by_stack(stack_name)
        if repo_name:
            pipeline_state.reset(repo_name)
        return {"success": True, "message": message, "stack_name": stack_name}
    else:
        raise HTTPException(status_code=500, detail=message)


@app.post("/api/services/{service_name}/remove")
async def remove_service(
    service_name: str,
    host: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """Remove a Docker Swarm service."""
    target_host = host or _get_swarm_manager_host()
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
    
    target_host = host or _get_swarm_manager_host()
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
        "vllm_url": ai.vllm_url,
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


# ============== Manual Task Creation ==============

@app.post("/api/tasks/create")
async def create_agent_task(request: Dict[str, Any]) -> Dict[str, Any]:
    """Create a task in PulsarTeam via its MCP server."""
    task = request.get("task", "").strip()
    project = request.get("project", "").strip()

    if not task:
        raise HTTPException(status_code=400, detail="task is required")
    if not project:
        raise HTTPException(status_code=400, detail="project is required")

    # Find the PulsarTeam MCP server in config
    pulsarteam_server = None
    mcp_servers = llm_agent._mcp_servers if llm_agent else []
    for server in mcp_servers:
        if server.name == "pulsarteam":
            pulsarteam_server = server
            break

    if not pulsarteam_server:
        raise HTTPException(status_code=500,
                            detail="PulsarTeam MCP server not configured (add a server named 'pulsarteam' in Settings)")

    url = pulsarteam_server.url.rstrip("/")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if pulsarteam_server.api_key:
        headers["Authorization"] = f"Bearer {pulsarteam_server.api_key}"

    # Call MCP tools/call with create_task
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "create_task",
            "arguments": {"task": task, "project": project},
        },
        "id": 1,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()

                if "error" in data:
                    error = data["error"]
                    error_msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                    logger.error("PulsarTeam MCP error", project=project, error=error_msg)
                    raise HTTPException(status_code=502, detail=f"PulsarTeam error: {error_msg}")

                result = data.get("result", {})
                content_parts = result.get("content", [])
                text_parts = [
                    p.get("text", "") if isinstance(p, dict) else str(p)
                    for p in content_parts
                ]
                response_text = "\n".join(text_parts)

                logger.info("Task created in PulsarTeam", project=project,
                            response_preview=response_text[:200])
                return {"ok": True, "response": response_text[:500]}

    except aiohttp.ClientError as e:
        logger.error("PulsarTeam MCP connection error", error=str(e), url=url)
        raise HTTPException(status_code=502, detail=f"Cannot reach PulsarTeam: {e}")


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
async def health_check() -> Dict[str, Any]:
    """Health check endpoint with OpenSearch connectivity status."""
    result: Dict[str, Any] = {"status": "healthy", "service": "pulsarcd"}

    # OpenSearch connectivity check (quick, no auth needed)
    if opensearch and opensearch._client:
        try:
            import opensearchpy as _ospy
            result["opensearch_client_version"] = getattr(_ospy, "__versionstr__", "unknown")
        except Exception:
            pass
        try:
            info = await opensearch._client.info()
            os_version = info.get("version", {}).get("number", "unknown")
            result["opensearch"] = "connected"
            result["opensearch_version"] = os_version
            # Quick doc counts
            for idx_name in [opensearch.logs_index, opensearch.metrics_index, opensearch.host_metrics_index]:
                try:
                    count_resp = await opensearch._client.count(index=idx_name)
                    result[f"{idx_name}_docs"] = count_resp.get("count", 0)
                except Exception as e:
                    result[f"{idx_name}_docs"] = f"error: {str(e)[:100]}"
        except Exception as e:
            result["opensearch"] = "error"
            result["opensearch_error"] = f"{type(e).__name__}: {str(e)[:200]}"
    else:
        result["opensearch"] = "not_configured"

    return result


@app.get("/api/health/opensearch")
async def opensearch_probe() -> Dict[str, Any]:
    """Deep OpenSearch diagnostic — no auth required.

    Tests:
    1. Cluster connectivity and version
    2. List ALL indices with doc counts
    3. Write a test doc (single + bulk)
    4. Read and delete the test doc
    5. Check index mappings
    """
    import json as _json
    result: Dict[str, Any] = {"timestamp": datetime.utcnow().isoformat()}

    if not opensearch or not opensearch._client:
        result["error"] = "OpenSearch client not configured"
        return result

    client = opensearch._client

    # 1. Cluster info
    try:
        info = await client.info()
        result["cluster"] = {
            "name": info.get("cluster_name"),
            "server_version": info.get("version", {}).get("number"),
            "distribution": info.get("version", {}).get("distribution"),
        }
        import opensearchpy as _ospy
        result["client_version"] = getattr(_ospy, "__versionstr__", "unknown")
    except Exception as e:
        result["cluster_error"] = f"{type(e).__name__}: {str(e)[:300]}"
        return result

    # 2. List all indices
    try:
        cat_resp = await client.cat.indices(format="json")
        result["indices"] = [
            {
                "index": idx.get("index"),
                "docs_count": idx.get("docs.count"),
                "store_size": idx.get("store.size"),
                "health": idx.get("health"),
                "status": idx.get("status"),
            }
            for idx in cat_resp
        ]
    except Exception as e:
        result["indices_error"] = f"{type(e).__name__}: {str(e)[:300]}"

    # 3. Write test doc (single)
    test_index = opensearch.logs_index
    test_id = "__probe_test__"
    test_doc = {
        "timestamp": datetime.utcnow().isoformat(),
        "host": "__probe__",
        "container_id": "__probe__",
        "container_name": "__probe__",
        "compose_project": "__probe__",
        "compose_service": "__probe__",
        "stream": "stdout",
        "message": "OpenSearch probe test",
        "level": "INFO",
    }

    try:
        write_resp = await client.index(
            index=test_index, id=test_id, body=test_doc, refresh="true"
        )
        result["write_test"] = {
            "status": "ok",
            "result": write_resp.get("result"),
            "index": write_resp.get("_index"),
            "version": write_resp.get("_version"),
        }
    except Exception as e:
        result["write_test"] = {"status": "error", "error": f"{type(e).__name__}: {str(e)[:300]}"}

    # 4. Bulk write test
    try:
        bulk_id = "__probe_bulk__"
        bulk_doc = test_doc.copy()
        bulk_doc["message"] = "OpenSearch probe bulk test"
        ndjson = (
            _json.dumps({"index": {"_index": test_index, "_id": bulk_id}}) + "\n"
            + _json.dumps(bulk_doc) + "\n"
        )
        bulk_resp = await client.bulk(body=ndjson, refresh="true")
        items = bulk_resp.get("items", [])
        item_status = items[0].get("index", {}).get("status") if items else None
        item_error = items[0].get("index", {}).get("error") if items else None
        result["bulk_test"] = {
            "status": "ok" if not bulk_resp.get("errors") else "error",
            "errors": bulk_resp.get("errors"),
            "item_status": item_status,
            "item_error": str(item_error)[:300] if item_error else None,
        }
    except Exception as e:
        result["bulk_test"] = {"status": "error", "error": f"{type(e).__name__}: {str(e)[:300]}"}

    # 5. Read back
    try:
        get_resp = await client.get(index=test_index, id=test_id)
        result["read_test"] = {"status": "ok", "found": get_resp.get("found")}
    except Exception as e:
        result["read_test"] = {"status": "error", "error": f"{type(e).__name__}: {str(e)[:300]}"}

    # 6. Cleanup
    for doc_id in [test_id, "__probe_bulk__"]:
        try:
            await client.delete(index=test_index, id=doc_id, refresh="true")
        except Exception:
            pass

    # 7. Check recent docs (any data at all?)
    try:
        search_resp = await client.search(
            index=test_index,
            body={"query": {"match_all": {}}, "size": 3, "sort": [{"timestamp": "desc"}]},
        )
        total = search_resp.get("hits", {}).get("total", {})
        total_val = total.get("value", total) if isinstance(total, dict) else total
        sample_hits = [
            {k: v for k, v in h.get("_source", {}).items() if k in ("timestamp", "host", "compose_project", "level", "message")}
            for h in search_resp.get("hits", {}).get("hits", [])
        ]
        result["search_test"] = {
            "status": "ok",
            "total_docs": total_val,
            "sample_hits": sample_hits,
        }
    except Exception as e:
        result["search_test"] = {"status": "error", "error": f"{type(e).__name__}: {str(e)[:300]}"}

    return result


@app.get("/api/github/check-access")
async def check_github_access():
    """Diagnostic: test GitHub API access for all starred repos."""
    if not github_service or not github_service.config.token:
        return {"error": "GitHub token not configured"}

    import aiohttp
    repos = await github_service.get_starred_repos()
    if not repos:
        return {"error": "No starred repos found (or token has no access)"}

    results = []
    session = await github_service._get_session()

    async def check_repo(repo):
        owner = repo["owner"]
        name = repo["name"]
        url = f"https://api.github.com/repos/{owner}/{name}/commits?per_page=1"
        try:
            async with session.get(url) as response:
                status = response.status
                if status == 200:
                    return {"repo": f"{owner}/{name}", "status": "ok", "code": 200}
                else:
                    error_text = await response.text()
                    return {"repo": f"{owner}/{name}", "status": "error", "code": status, "message": error_text[:200]}
        except Exception as e:
            return {"repo": f"{owner}/{name}", "status": "error", "message": str(e)}

    tasks = [check_repo(r) for r in repos]
    results = await asyncio.gather(*tasks)

    ok_count = sum(1 for r in results if r["status"] == "ok")
    error_count = len(results) - ok_count

    return {
        "total": len(results),
        "ok": ok_count,
        "errors": error_count,
        "repos": results
    }


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


@app.get("/api/stacks/test-permissions/{owner}/{repo}")
async def test_github_permissions(owner: str, repo: str):
    """Test GitHub token permissions for a specific repository.

    Returns detailed diagnostics about what the token can and cannot access.
    """
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    import aiohttp
    session = await github_service._get_session()
    results = {}

    # Test 1: Can we access the repo at all?
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}") as resp:
            results["repo_access"] = {"status": resp.status, "ok": resp.status == 200}
            if resp.status == 200:
                data = await resp.json()
                results["repo_access"]["default_branch"] = data.get("default_branch")
                results["repo_access"]["private"] = data.get("private")
            else:
                results["repo_access"]["error"] = (await resp.text())[:200]
            scopes = resp.headers.get("X-OAuth-Scopes", "none")
            results["token_scopes"] = scopes
            results["rate_limit_remaining"] = resp.headers.get("X-RateLimit-Remaining")
    except Exception as e:
        results["repo_access"] = {"status": 0, "ok": False, "error": str(e)}

    # Test 2: Can we list branches? (requires Contents: Read)
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}/branches", params={"per_page": 1}) as resp:
            results["branches_access"] = {"status": resp.status, "ok": resp.status == 200}
            if resp.status == 200:
                data = await resp.json()
                results["branches_access"]["count"] = len(data)
            else:
                results["branches_access"]["error"] = (await resp.text())[:200]
    except Exception as e:
        results["branches_access"] = {"status": 0, "ok": False, "error": str(e)}

    # Test 3: Can we list commits? (requires Contents: Read)
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}/commits", params={"per_page": 1}) as resp:
            results["commits_access"] = {"status": resp.status, "ok": resp.status == 200}
            if resp.status == 200:
                data = await resp.json()
                results["commits_access"]["count"] = len(data)
            else:
                results["commits_access"]["error"] = (await resp.text())[:200]
    except Exception as e:
        results["commits_access"] = {"status": 0, "ok": False, "error": str(e)}

    # Summary
    all_ok = all(r.get("ok") for r in results.values() if isinstance(r, dict) and "ok" in r)
    if all_ok:
        results["summary"] = "All permissions OK - token can access repo, branches, and commits."
    else:
        issues = []
        if not results.get("repo_access", {}).get("ok"):
            issues.append("Cannot access repository (check token has access to this repo)")
        if not results.get("branches_access", {}).get("ok"):
            issues.append("Cannot list branches (need 'Contents: Read' permission)")
        if not results.get("commits_access", {}).get("ok"):
            issues.append("Cannot list commits (need 'Contents: Read' permission)")
        results["summary"] = "Permission issues found: " + "; ".join(issues)

    return results


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


@app.get("/api/stacks/{owner}/{repo}/untagged-commits")
async def get_untagged_commits(
    owner: str,
    repo: str,
    limit: int = Query(default=10, ge=1, le=50, description="Maximum number of commits to check"),
):
    """Get recent commits that don't have a tag (not yet built/deployed)."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    return await github_service.get_untagged_commits(owner, repo, limit)


@app.get("/api/stacks/{owner}/{repo}/activity")
async def get_repo_activity(
    owner: str,
    repo: str,
    per_page: int = Query(default=30, ge=1, le=100, description="Commits per branch"),
):
    """Get activity data (commits from all branches, tags) for a repository."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    # Quick access check: verify we can reach the repo at all
    # This catches token issues early and gives clear error messages
    session = await github_service._get_session()
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}") as check_resp:
            if check_resp.status == 401:
                return {
                    "branches": [], "tags": [], "commits": [], "branch_tip_map": {},
                    "tag_map": {}, "commit_branches": {}, "default_branch": "main",
                    "error": "GitHub token is invalid or expired. Please update your GitHub token in settings.",
                }
            if check_resp.status == 404:
                return {
                    "branches": [], "tags": [], "commits": [], "branch_tip_map": {},
                    "tag_map": {}, "commit_branches": {}, "default_branch": "main",
                    "error": f"Repository '{owner}/{repo}' not found. The token may not have access to this repository.",
                }
            if check_resp.status == 403:
                error_text = await check_resp.text()
                return {
                    "branches": [], "tags": [], "commits": [], "branch_tip_map": {},
                    "tag_map": {}, "commit_branches": {}, "default_branch": "main",
                    "error": f"Access denied to '{owner}/{repo}'. GitHub says: {error_text[:200]}",
                }
            # Get the default branch from the repo metadata
            repo_data = await check_resp.json()
            repo_default_branch = repo_data.get("default_branch", "main")
    except Exception as e:
        logger.error("Failed to check repo access", repo=f"{owner}/{repo}", error=str(e))
        repo_default_branch = "main"

    # Fetch branches and tags first
    branches, tags_data = await asyncio.gather(
        github_service.get_repo_branches(owner, repo),
        github_service.get_repo_tags(owner, repo, limit=50),
    )

    branches_were_empty = not branches

    # Fetch commits from all branches in parallel
    # If branches list is empty (permissions issue, rate limit), fall back to default branch
    if branches:
        commit_tasks = [
            github_service.get_repo_commits(owner, repo, branch=b["name"], per_page=per_page)
            for b in branches
        ]
    else:
        # Fallback: fetch commits without specifying a branch (uses repo default)
        logger.warning("No branches available, falling back to default branch commits", repo=f"{owner}/{repo}")
        commit_tasks = [
            github_service.get_repo_commits(owner, repo, per_page=per_page)
        ]
        # Create a synthetic branch entry for the fallback
        branches = [{"name": tags_data.get("default_branch", repo_default_branch), "sha": "", "protected": False}]

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
        "default_branch": tags_data.get("default_branch", repo_default_branch),
    }
    if errors:
        response["error"] = errors[0]  # Surface first permission error to frontend
    elif not commits and branches_were_empty:
        # Both branches and commits failed - likely a permission issue
        response["error"] = (
            "Could not fetch repository data. Your GitHub token likely lacks the 'Contents: Read' permission. "
            "Go to GitHub Settings > Developer settings > Personal access tokens > Fine-grained tokens, "
            "edit your token and enable 'Contents: Read' under Repository permissions. "
            "Note: 'Administration: Read' alone is not sufficient to access branches and commits."
        )
    elif not commits:
        # Branches were found but no commits — something unexpected
        response["error"] = (
            f"No commits found on any branch ({len(branches)} branches checked). "
            "This may indicate a permission issue — ensure your GitHub token has 'Contents: Read' permission."
        )
    return response


@app.get("/api/stacks/{owner}/{repo}/commits/{sha}/diff")
async def get_commit_diff(owner: str, repo: str, sha: str):
    """Get the diff for a specific commit."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    return await github_service.get_commit_diff(owner, repo, sha)


def _get_swarm_manager_host() -> Optional[str]:
    """Return the swarm manager host name, or None if not configured."""
    for host_config in settings.hosts:
        if host_config.swarm_manager:
            return host_config.name
    return None


def _set_pipeline(
    repo_name: str,
    stage: str,
    status: str,
    version: str,
    build_id=_UNSET,
    test_id=_UNSET,
    deploy_id=_UNSET,
    log_lines: Optional[List[str]] = None,
) -> None:
    """Update pipeline state for repo_name (persisted to disk).

    Pass an explicit value (including None) to override; omit to inherit from the current state.
    If log_lines is provided, updates the last_log for the current stage.
    """
    pipeline_state.set_pipeline(
        repo_name, stage, status, version,
        build_id=build_id, test_id=test_id, deploy_id=deploy_id,
    )
    if log_lines and stage in ("build", "test", "deploy"):
        pipeline_state.update_log(repo_name, stage, log_lines)


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
    tag: str = Query(default=None, description="Git tag to build from (e.g., v1.0.5)"),
    commit: str = Query(default=None, description="Specific commit hash to build from"),
    no_cache: bool = Query(default=False, description="Force fresh build without Docker cache"),
):
    """Build a stack from a GitHub repository. Runs in background, returns action ID."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    owner = None
    if ssh_url:
        match = re.search(r'[:/]([^/]+)/[^/]+\.git$', ssh_url)
        if match:
            owner = match.group(1)

    # If tag is provided, derive version from it
    if tag:
        if not re.match(r'^v?\d+(\.\d+){1,2}$', tag):
            raise HTTPException(status_code=400, detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X")
        version = tag.lstrip('v')

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
    _set_pipeline(repo_name, "build", "running", version, build_id=action_id)

    async def _run_build():
        try:
            result = await deployer.build(
                repo_name, ssh_url, version, branch=branch, tag=tag, commit=commit,
                no_cache=no_cache,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            status = "success" if result.get("success") else "failed"
            _set_pipeline(repo_name, "build", status, version, build_id=action_id, log_lines=action.output_lines)
            if not result.get("success"):
                await _notify_agent_failure("build", repo_name, version, result.get("output", ""))
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background build failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": "build", "repo": repo_name}
            action.append_output(error_detail)
            _set_pipeline(repo_name, "build", "failed", version, build_id=action_id, log_lines=action.output_lines)
            await _notify_agent_failure("build", repo_name, version, error_detail)

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

    # Use the tag as display version when deploying a specific tag
    deploy_version = tag.lstrip('v') if tag else version
    # When deploying a specific tag (no prior build), clear build_action_id
    prev_build_id = pipeline_state.get_legacy(repo_name).get("build_action_id") if not tag else None

    _set_pipeline(repo_name, "deploy", "running", deploy_version, build_id=prev_build_id, deploy_id=action_id)

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
            if result.get("success"):
                _set_pipeline(repo_name, "done", "success", deploy_version, deploy_id=action_id, log_lines=action.output_lines)
            else:
                _set_pipeline(repo_name, "deploy", "failed", deploy_version, deploy_id=action_id, log_lines=action.output_lines)
                await _notify_agent_failure("deploy", repo_name, deploy_version, result.get("output", ""))
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background deploy failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": "deploy", "repo": repo_name}
            action.append_output(error_detail)
            _set_pipeline(repo_name, "deploy", "failed", deploy_version, deploy_id=action_id, log_lines=action.output_lines)
            await _notify_agent_failure("deploy", repo_name, deploy_version, error_detail)

    action.task = asyncio.create_task(_run_deploy())

    return {"action_id": action_id, "action_type": "deploy", "repo": repo_name}


@app.post("/api/stacks/test")
async def test_stack(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    branch: str = Query(default=None, description="Branch name to test from"),
    tag: str = Query(default=None, description="Git tag to test from (e.g., v1.0.5)"),
    commit: str = Query(default=None, description="Specific commit hash to test from"),
):
    """Run tests for a stack. Runs in background, returns action ID."""
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

    if tag and owner:
        if not re.match(r'^v?\d+(\.\d+){1,2}$', tag):
            raise HTTPException(status_code=400, detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X")

    if commit and owner:
        if not re.match(r'^[a-fA-F0-9]{7,40}$', commit):
            raise HTTPException(status_code=400, detail=f"Invalid commit hash format: '{commit}'. Expected 7-40 hexadecimal characters.")
        is_valid, error_msg = await github_service.validate_commit(owner, repo_name, commit)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

    deployer, host_name = _get_deployer_and_host()

    # Create background action
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "test", repo_name)
    _background_actions[action_id] = action

    # Use tag version if provided, otherwise keep previous version
    version = tag.lstrip('v') if tag else pipeline_state.get_legacy(repo_name).get("version", "")

    # Update pipeline state
    _set_pipeline(repo_name, "test", "running", version, test_id=action_id)

    async def _run_test():
        try:
            result = await deployer.test(
                repo_name, ssh_url, branch=branch, tag=tag, commit=commit,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            status = "success" if result.get("success") else "failed"
            _set_pipeline(repo_name, "test", status, version, test_id=action_id, log_lines=action.output_lines)
            if not result.get("success"):
                await _notify_agent_failure("test", repo_name, version, result.get("output", ""))
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background test failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": "test", "repo": repo_name}
            action.append_output(error_detail)
            _set_pipeline(repo_name, "test", "failed", version, test_id=action_id, log_lines=action.output_lines)
            await _notify_agent_failure("test", repo_name, version, error_detail)

    action.task = asyncio.create_task(_run_test())

    return {"action_id": action_id, "action_type": "test", "repo": repo_name}


@app.get("/api/stacks/actions/{action_id}/status")
async def get_action_status(action_id: str) -> Dict[str, Any]:
    """Get the status of a background build/deploy/test action."""
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


@app.get("/api/stacks/actions/{action_id}/logs/stream")
async def stream_action_logs(
    action_id: str,
    offset: int = Query(default=0, ge=0, description="Line offset to start from"),
):
    """Stream logs of a background action via Server-Sent Events."""
    action = _background_actions.get(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    async def event_generator():
        cursor = offset
        while True:
            # Send any new lines
            if cursor < len(action.output_lines):
                for line in action.output_lines[cursor:]:
                    data = json.dumps({"type": "line", "line": line})
                    yield f"data: {data}\n\n"
                cursor = len(action.output_lines)

            # If action is done, send final status and close
            if action.status != "running":
                data = json.dumps({"type": "done", "status": action.status})
                yield f"data: {data}\n\n"
                return

            # Clear before waiting; re-check after clear to avoid race
            action.new_line_event.clear()
            if cursor < len(action.output_lines) or action.status != "running":
                continue
            try:
                await asyncio.wait_for(action.new_line_event.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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


# ============== Build config detection ==============

_buildable_cache: Dict[str, bool] = {}  # {repo_name: has_build_config}


@app.get("/api/stacks/buildable")
async def get_stacks_buildable() -> Dict[str, bool]:
    """Return which stacks have build: directives in their docker-compose.swarm.yml."""
    if not github_service.is_configured():
        return {}
    if _buildable_cache:
        return _buildable_cache
    repos = await github_service.get_starred_repos()
    deployer, _ = _get_deployer_and_host()
    for repo in repos:
        name = repo["name"]
        try:
            _buildable_cache[name] = await deployer.has_build_config(name)
        except Exception:
            _buildable_cache[name] = True  # assume buildable on error
    return _buildable_cache


def invalidate_buildable_cache(repo_name: str = None):
    """Clear buildable cache (after clone/pull that may change compose file)."""
    if repo_name:
        _buildable_cache.pop(repo_name, None)
    else:
        _buildable_cache.clear()


# ============== Pipeline (Auto Build → Test → Deploy) ==============

_auto_build_state = {}  # {repo_name: {"last_sha": str, "building": bool, "untagged_commits": int}}
_auto_build_task = None


@app.post("/api/stacks/pipeline")
async def trigger_pipeline_endpoint(
    repo_name: str = Query(..., description="Repository name"),
    ssh_url: str = Query(..., description="SSH URL for cloning"),
    tag: str = Query(default=None, description="Git tag to build and deploy (format: vX.X.X)"),
    commit: str = Query(default=None, description="Commit SHA to build (will be auto-tagged)"),
):
    """Trigger a full pipeline (Build → Test → Deploy) from a tag or commit.

    Either `tag` or `commit` must be provided. If `commit` is provided without a tag,
    the next version is computed automatically and the commit is tagged before building.
    """
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    if not tag and not commit:
        raise HTTPException(status_code=400, detail="Either 'tag' or 'commit' must be provided")

    # Check if pipeline is already running for this repo
    existing = pipeline_state.get_legacy(repo_name)
    if existing.get("status") == "running":
        raise HTTPException(status_code=409, detail=f"Pipeline already running for {repo_name}")

    if tag:
        # Existing flow: pipeline from a known tag
        if not re.match(r'^v?\d+(\.\d+){1,2}$', tag):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tag format: '{tag}'. Expected format: vX.X.X (e.g., v1.0.5)"
            )
        version = tag.lstrip('v')
        await _trigger_pipeline(repo_name, ssh_url, version=version, tag=tag)
        return {"status": "started", "repo": repo_name, "tag": tag, "version": version}
    else:
        # New flow: auto-tag the commit, then run pipeline
        owner_match = re.search(r'[:/]([^/]+)/[^/]+\.git$', ssh_url)
        if not owner_match:
            raise HTTPException(status_code=400, detail="Cannot parse owner from ssh_url")
        owner = owner_match.group(1)

        next_version = await github_service.get_next_version(owner, repo_name)
        new_tag = f"v{next_version}"

        result = await github_service.create_tag(owner, repo_name, new_tag, commit)
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create tag {new_tag}: {result.get('error', 'unknown')}"
            )

        logger.info("Auto-tagged commit for pipeline", repo=repo_name, tag=new_tag, commit=commit[:7])
        await _trigger_pipeline(repo_name, ssh_url, version=next_version, tag=new_tag)
        return {"status": "started", "repo": repo_name, "tag": new_tag, "version": next_version, "auto_tagged": True}
AUTO_BUILD_POLL_INTERVAL = 30  # seconds – repo checks are batched so this is cheap

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


async def _trigger_pipeline(repo_name: str, ssh_url: str, version: str = None, tag: str = None):
    """Trigger a full pipeline: build → test → deploy.

    Args:
        repo_name: Repository name
        ssh_url: SSH URL for cloning
        version: Optional exact version (e.g., "1.0.5"). If provided, build uses this exact version.
        tag: Optional git tag (e.g., "v1.0.5"). If provided, build checks out this tag.
    """
    try:
        deployer, host_name = _get_deployer_and_host()
    except Exception as e:
        logger.error("Pipeline: no host available", repo=repo_name, error=str(e))
        _set_pipeline(repo_name, "build", "failed", version, build_id=None, test_id=None, deploy_id=None)
        if repo_name in _auto_build_state:
            _auto_build_state[repo_name]["building"] = False
        return

    # Check if this repo has build: directives in its compose file
    buildable = _buildable_cache.get(repo_name)
    if buildable is None:
        try:
            buildable = await deployer.has_build_config(repo_name)
            _buildable_cache[repo_name] = buildable
        except Exception:
            buildable = True  # assume buildable on error

    initial_stage = "build" if buildable else "deploy"
    _set_pipeline(repo_name, initial_stage, "running", version, build_id=None, test_id=None, deploy_id=None)
    pipeline_state.set_skip_build(repo_name, not buildable)

    # Set project and stack metadata
    stack_name = StackDeployer._repo_to_stack_name(repo_name)
    pipeline_state.set_project_info(repo_name, project_name=repo_name, stack_name=stack_name)
    # Clear previous gate decisions for new pipeline run
    pipeline_state.clear_gates(repo_name)

    # Determine build version param: exact "1.0.5" or auto-increment "1.0"
    build_version = version if version else "1.0"

    async def _run_pipeline():
        built_version = version
        build_id = None
        try:
            # ── Step 1: Build (skip if no build: in compose) ──
            if buildable:
                build_id = str(uuid.uuid4())[:8]
                build_action = BackgroundAction(build_id, "build", repo_name)
                _background_actions[build_id] = build_action
                pipeline_state.get_or_create(repo_name).stages["build"].action_id = build_id

                logger.info("Pipeline: starting build", repo=repo_name, action_id=build_id, version=build_version, tag=tag)
                result = await deployer.build(
                    repo_name, ssh_url, version=build_version,
                    tag=tag,
                    output_callback=build_action.append_output,
                    cancel_event=build_action.cancel_event,
                )
                result["host"] = host_name
                result["auto_triggered"] = True
                build_action.result = result
                build_action.status = "completed" if result.get("success") else "failed"

                if not result.get("success"):
                    _set_pipeline(repo_name, "build", "failed", version, build_id=build_id, test_id=None, deploy_id=None, log_lines=build_action.output_lines)
                    await _notify_agent_failure("build", repo_name, version or "", result.get("output", ""))
                    return

                # Extract version from build output, or use provided version
                built_version = _extract_version_from_output(build_action.output_lines) or version
                pipeline_state.update_version(repo_name, "build", built_version)
                pipeline_state.update_log(repo_name, "build", build_action.output_lines)
                # Mark build stage as success with current_version
                pipeline_state.set_stage(repo_name, "build", "success", built_version, action_id=build_id, log_lines=build_action.output_lines)
                logger.info("Pipeline: build succeeded", repo=repo_name, version=built_version)

                # ── Gate: Build → Test ──
                if llm_agent:
                    approved, reason = await llm_agent.evaluate_gate(
                        "build_to_test", repo_name, built_version or "",
                        result.get("output", "")
                    )
                    pipeline_state.record_gate(repo_name, "build_to_test", approved, reason)
                    if not approved:
                        logger.warning("Pipeline: gate build→test REJECTED",
                                       repo=repo_name, reason=reason[:200])
                        _set_pipeline(repo_name, "build", "gate_rejected", built_version,
                                      build_id=build_id, test_id=None, deploy_id=None, log_lines=build_action.output_lines)
                        return
                    logger.info("Pipeline: gate build→test approved", repo=repo_name, reason=reason[:100])

            else:
                logger.info("Pipeline: skipping build (no build: in compose)", repo=repo_name)

            # ── Step 2: Test ──
            test_id = str(uuid.uuid4())[:8]
            test_action = BackgroundAction(test_id, "test", repo_name)
            _background_actions[test_id] = test_action
            _set_pipeline(repo_name, "test", "running", built_version, build_id=build_id, test_id=test_id, deploy_id=None)

            logger.info("Pipeline: starting test", repo=repo_name, action_id=test_id, tag=tag)
            test_result = await deployer.test(
                repo_name, ssh_url,
                tag=tag,
                output_callback=test_action.append_output,
                cancel_event=test_action.cancel_event,
            )
            test_result["host"] = host_name
            test_result["auto_triggered"] = True
            test_action.result = test_result
            test_action.status = "completed" if test_result.get("success") else "failed"

            if not test_result.get("success"):
                _set_pipeline(repo_name, "test", "failed", built_version, build_id=build_id, test_id=test_id, deploy_id=None, log_lines=test_action.output_lines)
                logger.warning("Pipeline: test failed", repo=repo_name)
                await _notify_agent_failure("test", repo_name, built_version or "", test_result.get("output", ""))
                return

            _set_pipeline(repo_name, "test", "success", built_version, build_id=build_id, test_id=test_id, deploy_id=None, log_lines=test_action.output_lines)
            logger.info("Pipeline: test succeeded", repo=repo_name)

            # ── Gate: Test → Deploy ──
            if llm_agent:
                approved, reason = await llm_agent.evaluate_gate(
                    "test_to_deploy", repo_name, built_version or "",
                    test_result.get("output", "")
                )
                pipeline_state.record_gate(repo_name, "test_to_deploy", approved, reason)
                if not approved:
                    logger.warning("Pipeline: gate test→deploy REJECTED",
                                   repo=repo_name, reason=reason[:200])
                    _set_pipeline(repo_name, "test", "gate_rejected", built_version,
                                  build_id=build_id, test_id=test_id, deploy_id=None, log_lines=test_action.output_lines)
                    return
                logger.info("Pipeline: gate test→deploy approved", repo=repo_name, reason=reason[:100])

            # ── Step 3: Deploy ──
            deploy_id = str(uuid.uuid4())[:8]
            deploy_action = BackgroundAction(deploy_id, "deploy", repo_name)
            _background_actions[deploy_id] = deploy_action
            _set_pipeline(repo_name, "deploy", "running", built_version, build_id=build_id, test_id=test_id, deploy_id=deploy_id)

            deploy_tag = tag if tag else (f"v{built_version}" if built_version else None)
            logger.info("Pipeline: starting deploy", repo=repo_name, action_id=deploy_id, tag=deploy_tag)
            deploy_result = await deployer.deploy(
                repo_name, ssh_url, version=built_version,
                tag=deploy_tag,
                output_callback=deploy_action.append_output,
                cancel_event=deploy_action.cancel_event,
            )
            deploy_result["host"] = host_name
            deploy_result["auto_triggered"] = True
            deploy_action.result = deploy_result
            deploy_action.status = "completed" if deploy_result.get("success") else "failed"

            if deploy_result.get("success"):
                _set_pipeline(repo_name, "done", "success", built_version, build_id=build_id, test_id=test_id, deploy_id=deploy_id, log_lines=deploy_action.output_lines)
                logger.info("Pipeline: deploy succeeded", repo=repo_name, version=built_version)
            else:
                _set_pipeline(repo_name, "deploy", "failed", built_version, build_id=build_id, test_id=test_id, deploy_id=deploy_id, log_lines=deploy_action.output_lines)
                await _notify_agent_failure("deploy", repo_name, built_version or "", deploy_result.get("output", ""))

        except Exception as e:
            logger.exception("Pipeline failed", repo=repo_name, error=str(e))
            current_stage = pipeline_state.get_legacy(repo_name).get("stage", "build")
            _set_pipeline(repo_name, current_stage, "failed", built_version)
        finally:
            if repo_name in _auto_build_state:
                _auto_build_state[repo_name]["building"] = False

    asyncio.create_task(_run_pipeline())
    logger.info("Pipeline triggered", repo=repo_name)


async def auto_build_poller():
    """Periodically check starred repos for new commits on default branch and trigger pipeline.

    Optimisations:
    - Uses `pushed_at` from the starred-repos list to skip unchanged repos (0 extra API calls).
    - Checks remaining repos concurrently via asyncio.gather.
    """
    await asyncio.sleep(10)

    # Track last known pushed_at per repo to cheaply skip unchanged ones
    _pushed_at_cache: dict[str, str] = {}

    async def _check_repo(repo: dict):
        """Check a single repo for new commits. Designed to run concurrently."""
        owner, name, ssh_url = repo["owner"], repo["name"], repo["ssh_url"]

        try:
            commits_task = github_service.get_repo_commits(owner, name, per_page=1)
            untagged_task = github_service.get_untagged_commits(owner, name, limit=20)
            commits_data, untagged_data = await asyncio.gather(commits_task, untagged_task)
        except Exception as e:
            logger.warning("Failed to fetch commit data for repo", repo=name, error=str(e))
            return

        commits = commits_data.get("commits", [])
        if not commits:
            return

        latest_sha = commits[0]["sha"]
        untagged_count = len(untagged_data.get("untagged_commits", []))
        state = _auto_build_state.get(name)

        if state is None:
            _auto_build_state[name] = {
                "last_sha": latest_sha,
                "building": False,
                "untagged_commits": untagged_count,
            }
            if untagged_count > 0:
                logger.info("Repo initialized with untagged commits",
                            repo=name, untagged=untagged_count)
            return

        # Always update untagged count
        state["untagged_commits"] = untagged_count

        if latest_sha != state["last_sha"] and not state.get("building"):
            logger.info("New commit detected, triggering pipeline",
                        repo=name,
                        old_sha=state["last_sha"][:7],
                        new_sha=latest_sha[:7],
                        untagged=untagged_count)
            state["building"] = True
            state["last_sha"] = latest_sha

            latest_tag_info = untagged_data.get("latest_tag")
            if untagged_count > 0:
                try:
                    next_ver = await github_service.get_next_version(owner, name)
                    new_tag = f"v{next_ver}"
                    tag_result = await github_service.create_tag(owner, name, new_tag, latest_sha)
                    if tag_result.get("success"):
                        logger.info("Auto-tagged new commit", repo=name, tag=new_tag, sha=latest_sha[:7])
                        await _trigger_pipeline(name, ssh_url, version=next_ver, tag=new_tag)
                    else:
                        logger.error("Failed to auto-tag", repo=name, error=tag_result.get("error"))
                        state["building"] = False
                except Exception as tag_err:
                    logger.error("Auto-tag failed", repo=name, error=str(tag_err))
                    state["building"] = False
            elif latest_tag_info:
                tag_name = latest_tag_info.get("name", "")
                version = tag_name.lstrip("v")
                await _trigger_pipeline(name, ssh_url, version=version, tag=tag_name)
            else:
                await _trigger_pipeline(name, ssh_url)

    while True:
        try:
            if github_service and github_service.is_configured():
                repos = await github_service.get_starred_repos()

                # Fast-filter: skip repos whose pushed_at hasn't changed
                repos_to_check = []
                for repo in repos:
                    name = repo["name"]
                    pushed_at = repo.get("pushed_at", "")
                    prev = _pushed_at_cache.get(name)
                    if prev is not None and prev == pushed_at:
                        # No push since last poll – skip expensive per-repo calls
                        continue
                    _pushed_at_cache[name] = pushed_at
                    repos_to_check.append(repo)

                if repos_to_check:
                    logger.info("Checking repos for changes",
                                total=len(repos), checking=len(repos_to_check))
                    # Check all changed repos concurrently (batch)
                    await asyncio.gather(*[_check_repo(r) for r in repos_to_check])

        except Exception as e:
            logger.error("Auto-build poller error", error=str(e))

        await asyncio.sleep(AUTO_BUILD_POLL_INTERVAL)


@app.get("/api/stacks/pipeline/status")
async def get_pipeline_status():
    """Get pipeline state for all repos."""
    return {"pipelines": pipeline_state.get_all_legacy()}


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
