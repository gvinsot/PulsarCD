"""FastAPI REST API for PulsarCD."""

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import jwt
import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, ValidationError

from shared.access_log import is_internal_ip, normalize_ip

from .auth import AUTH_SOURCE_GOOGLE, AUTH_SOURCE_LOCAL, create_token, decode_token
from .allowlist import EmailAllowlist
from .auto_ban import AutoBanWorker
from . import notifier
from .notifications_api import router as notifications_router
from .ip_blocklist import (
    IpBlocklist,
    covers as ip_blocklist_covers,
    validate_target as ip_blocklist_validate,
)
from .google_auth import GoogleIdTokenVerifier, GoogleTokenError
from .collector import Collector
from .config import load_config, Settings
from .models import (
    ActionRequest, ActionResult, ContainerInfo, ContainerStatus,
    DashboardStats, LogSearchQuery, LogSearchResult, TimeSeriesPoint, TimeSeriesByHost
)
from .opensearch_client import OpenSearchClient
from .github_service import GitHubService, StackDeployer, close_shared_ssh_clients
from .actions_queue import actions_queue, ActionType, ActionStatus
from .pipeline_state import PipelineStateManager, _UNSET
from .deploy_rollback import deploy_stage_with_rollback
from . import run_history
from .run_history_api import router as run_history_router

# ---- Moved out of api.py; re-exported for callers and tests ---------------
from .background_actions import (
    BackgroundAction,
)
from .pipeline_runner import (
    AUTO_BUILD_POLL_INTERVAL,
    _SEEN_SHAS_KEEP,
    _acquire_build_lock,
    _advance_last_sha,
    _auto_build_poll_interval,
    _extract_version_from_output,
    _get_deployer_and_host,
    _get_swarm_manager_host,
    _github_webhook_secret,
    _notify_agent_failure,
    _release_build_lock,
    _run_history_log_source,
    _set_pipeline,
    _sha_already_handled,
    _trigger_pipeline,
    auto_build_poller,
)
from .auth_middleware import (
    _AGENT_KEY_ROUTES,
    _AUTH_EXEMPT_PREFIXES,
    _QUERY_TOKEN_PATH_RE,
    _READ_ONLY_POST_PATHS,
    _agent_key_matches,
    _client_address,
    _route_path,
    _token_is_revoked,
    auth_middleware,
)
from .security_headers import (
    _csp_directives,
    security_headers_middleware,
)
from .routes.auth import (
    _LOGIN_ATTEMPTS_MAX_KEYS,
    _LOGIN_MAX_ATTEMPTS,
    _LOGIN_MAX_ATTEMPTS_PER_CLIENT,
    _LOGIN_WINDOW_SECONDS,
    _login_attempts,
    _record_login_attempt,
)
from .routes.dashboard import (
    get_dashboard_stats,
    get_recurring_errors,
    get_system_errors,
)
from .routes.usage import (
    _get_router_stacks,
)
from .routes.containers import (
    execute_container_action,
    get_service_tasks,
    remove_service,
    remove_stack,
    update_service_image,
)
from .routes.stacks import (
    get_stacks_deployed_tags,
)
from .routes.actions import (
    cancel_action,
)
from .routes.pipeline import (
    trigger_pipeline_endpoint,
)
from .github_webhook import MAX_PAYLOAD_BYTES as _WEBHOOK_MAX_PAYLOAD_BYTES
from .routes.webhooks import (
    _WEBHOOK_LISTING_RETRY_DELAYS,
    _request_recheck,
    _webhook_deliveries,
)
from .routes import auth as auth_routes
from .routes import admin as admin_routes
from .routes import dashboard as dashboard_routes
from .routes import security as security_routes
from .routes import usage as usage_routes
from .routes import containers as containers_routes
from .routes import logs as logs_routes
from .routes import hosts as hosts_routes
from .routes import health as health_routes
from .routes import config as config_routes
from .routes import stacks as stacks_routes
from .routes import recovery as recovery_routes
from .routes import tag_cleanup as tag_cleanup_routes
from .routes import actions as actions_routes
from .routes import pipeline as pipeline_routes
from .routes import webhooks as webhooks_routes
try:
    from .mcp_server import mcp_read, mcp_actions, get_mcp_read_app, get_mcp_actions_app
    from .mcp_auth import MCPAuthMiddleware
    _mcp_available = True
    _mcp_import_error = ""
except Exception as exc:
    # Python unbinds an "except ... as" name when the block ends, so the
    # lifespan hook cannot read it later: keep the message in its own name.
    _mcp_available = False
    _mcp_import_error = str(exc)

logger = structlog.get_logger()

# Global instances
settings: Settings = None
opensearch: OpenSearchClient = None
collector: Collector = None
github_service: GitHubService = None
error_detector = None
user_manager = None
email_allowlist: Optional[EmailAllowlist] = None
ip_blocklist: Optional[IpBlocklist] = None
auto_ban_worker: Optional[AutoBanWorker] = None
google_verifier: Optional[GoogleIdTokenVerifier] = None
llm_agent = None
pipeline_state: Optional[PipelineStateManager] = None
tag_cleaner = None
_recovery_task = None

# Store of running/recent background actions
_background_actions: Dict[str, BackgroundAction] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global settings, opensearch, collector, github_service, error_detector, user_manager, llm_agent, pipeline_state
    global email_allowlist, google_verifier, ip_blocklist, auto_ban_worker

    # Startup
    logger.info("Starting PulsarCD API")

    settings = load_config()
    if not settings.auth.agent_keys:
        logger.warning(
            "No per-agent keys configured: the whole fleet shares a single "
            "agent key, so compromising one agent grants access to every "
            "agent endpoint. Set PULSARCD_AUTH__AGENT_KEYS to a JSON object "
            "mapping each agent_id to its own key."
        )
    opensearch = OpenSearchClient(settings.opensearch)

    # Initialize persistent pipeline state
    pipeline_state = PipelineStateManager.get_instance(settings.data_dir)
    logger.info("Pipeline state manager initialized", data_dir=settings.data_dir)
    # Survive restarts: reload queued agent actions, fail interrupted ones.
    actions_queue.enable_persistence(f"{settings.data_dir}/actions_queue.json")

    # Google sign-in: who Google says you are, then whether you are allowed in.
    google_verifier = GoogleIdTokenVerifier(settings.auth.google_client_id)
    email_allowlist = EmailAllowlist(
        path=f"{settings.data_dir}/allowed_emails.json",
        admins=settings.auth.google_admins,
        viewers=settings.auth.google_viewers,
    )
    # Addresses refused by the edge proxy (Security > Blocked at the edge).
    ip_blocklist = IpBlocklist(path=f"{settings.data_dir}/blocked_ips.json")
    if not settings.auth.edge_key:
        logger.warning(
            "PULSARCD_AUTH__EDGE_KEY is not set: the edge cannot poll the IP "
            "blocklist, so blocking an address from the Security view has no "
            "effect. Set the same value here and in the edge's "
            "providers.http.headers."
        )
    elif ip_blocklist.targets():
        logger.info("Edge blocklist enforced", count=len(ip_blocklist.targets()))

    if not google_verifier.enabled:
        logger.warning(
            "PULSARCD_AUTH__GOOGLE_CLIENT_ID is not set: Google sign-in is "
            "disabled and only the break-glass administrator can sign in."
        )
    elif not email_allowlist.admin_count():
        logger.warning(
            "Google sign-in is configured but no address has the admin role: "
            "set PULSARCD_AUTH__GOOGLE_ADMINS, or nobody will be able to "
            "administer the deployment through Google."
        )

    # Break-glass local administrator (usually absent, see user_manager).
    from .user_manager import UserManager
    user_manager = UserManager(path=f"{settings.data_dir}/users.json")
    if not user_manager.enabled and not google_verifier.enabled:
        logger.error(
            "No sign-in method is available: set PULSARCD_AUTH__GOOGLE_CLIENT_ID "
            "(plus PULSARCD_AUTH__GOOGLE_ADMINS) or PULSARCD_AUTH__PASSWORD."
        )

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

    await run_history.recorder.attach(opensearch, settings, log_source=_run_history_log_source)
    # Outbound notifications (ntfy / webhook / e-mail); a no-op when unset.
    notifier.configure(getattr(settings, "notify", None))

    if settings.autoban.enabled and settings.auth.edge_key:
        auto_ban_worker = AutoBanWorker(
            opensearch, ip_blocklist, settings.autoban,
            checkpoint_path=f"{settings.data_dir}/autoban_checkpoint.json")
        if auto_ban_worker.start():
            logger.info("Automatic edge bans enabled", duration_seconds=settings.autoban.duration_seconds)
        else:
            auto_ban_worker = None
    elif settings.autoban.enabled:
        logger.warning("Automatic edge bans disabled: PULSARCD_AUTH__EDGE_KEY is missing")
    
    # Initialize GitHub service
    github_service = GitHubService(settings.github)

    # Reconcile existing .env files and SSH material immediately, then every
    # minute. Editor writes are backed up synchronously before replacement.
    global _recovery_task
    if os.environ.get("PULSARCD_BACKUP__ENABLED", "false").lower() == "true":
        from .recovery import monitor, status as recovery_status
        recovery_status["enabled"] = True
        _recovery_task = asyncio.create_task(monitor(settings.github))

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
        logger.info("Auto-build poller started", interval=f"{_auto_build_poll_interval()}s",
                    webhook=bool(_github_webhook_secret()))

    # Start tag cleanup background task
    global tag_cleaner
    if settings.pulsar_config and github_service.is_configured():
        from .tag_cleanup import TagCleaner
        tag_cleaner = TagCleaner(
            cleanup_config=settings.pulsar_config.tag_cleanup,
            github_service=github_service,
            registry_url=settings.github.registry_url or "",
            registry_username=settings.github.registry_username or "",
            registry_password=settings.github.registry_password or "",
        )
        tag_cleaner.start()

    # Preview environments: TTL cleanup of stale branch previews
    from .preview_routes import start_cleanup as _start_preview_cleanup
    _start_preview_cleanup()

    # Log MCP API key for configuration
    if _mcp_available and settings.mcp.enabled:
        logger.info("MCP server enabled")
        async with mcp_read.session_manager.run(), mcp_actions.session_manager.run():
            yield
    else:
        if not _mcp_available:
            logger.warning("MCP server not available", error=_mcp_import_error)
        yield

    # Shutdown
    logger.info("Shutting down PulsarCD API")
    if auto_ban_worker:
        await auto_ban_worker.stop()
        auto_ban_worker = None
    if _recovery_task:
        _recovery_task.cancel()
        try:
            await _recovery_task
        except asyncio.CancelledError:
            pass
        _recovery_task = None
    if tag_cleaner:
        await tag_cleaner.stop()
    from .preview_routes import stop_cleanup as _stop_preview_cleanup
    await _stop_preview_cleanup()
    if error_detector:
        await error_detector.stop()
    if _auto_build_task:
        _auto_build_task.cancel()
        try:
            await _auto_build_task
        except asyncio.CancelledError:
            pass
    await notifier.get_notifier().close()
    await collector.stop()
    try:
        # Flush pending run-history writes before the client closes.
        await asyncio.wait_for(run_history.recorder.drain(), timeout=10)
    except Exception as e:
        logger.warning("Pipeline run history: pending writes not flushed", error=str(e))
    await opensearch.close()
    await github_service.close()
    await close_shared_ssh_clients()
    if llm_agent:
        await llm_agent.close()


app = FastAPI(
    title="PulsarCD API",
    description="Docker container log aggregation and monitoring",
    version="1.0.0",
    lifespan=lifespan,
)

# Per-branch preview environments (backend/preview_routes.py)
from .preview_routes import router as _preview_router  # noqa: E402
app.include_router(_preview_router)


# ============== Global Error Handling ==============

def _is_production() -> bool:
    """Return True when the API should hide internal error details from clients.

    Production mode hides stack traces and raw exception messages.
    Order of resolution:
    1. PULSARCD_DEBUG / settings.debug — when truthy, treat as dev (expose details)
    2. NODE_ENV / PYTHON_ENV / ENV / ENVIRONMENT containing "prod" — treat as prod
    3. Default: production (safe-by-default)
    """
    import os
    s = settings  # may be None during early startup
    if s is not None and getattr(s, "debug", False):
        return False
    env = (
        os.environ.get("PULSARCD_ENV")
        or os.environ.get("ENVIRONMENT")
        or os.environ.get("ENV")
        or os.environ.get("PYTHON_ENV")
        or os.environ.get("NODE_ENV")
        or ""
    ).lower()
    if env in ("dev", "development", "test", "testing", "local"):
        return False
    return True


# Common exception → HTTP status code mapping (used for unhandled exceptions).
_EXCEPTION_STATUS_MAP: Dict[type, int] = {
    ValueError: 400,
    TypeError: 400,
    KeyError: 404,
    LookupError: 404,
    FileNotFoundError: 404,
    PermissionError: 403,
    NotImplementedError: 501,
    TimeoutError: 504,
}


def _status_for_exception(exc: BaseException) -> int:
    """Resolve an HTTP status code for an unhandled exception."""
    for exc_type, code in _EXCEPTION_STATUS_MAP.items():
        if isinstance(exc, exc_type):
            return code
    return 500


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    """Sanitize unhandled exceptions to avoid leaking internals in production.

    - Generates a request_id for cross-referencing with server logs.
    - Logs the full exception (with stack trace) server-side.
    - In production: returns a generic message + request_id only.
    - In dev: returns the exception type, message and stack trace.
    """
    request_id = uuid.uuid4().hex
    status_code = _status_for_exception(exc)

    # Always log the full details server-side for forensics.
    logger.error(
        "Unhandled exception",
        request_id=request_id,
        path=request.url.path,
        method=request.method,
        status_code=status_code,
        exc_type=type(exc).__name__,
        exc_message=str(exc),
        stack=traceback.format_exc(),
    )

    if _is_production():
        body: Dict[str, Any] = {
            "error": "Internal server error" if status_code >= 500 else "Request error",
            "request_id": request_id,
        }
    else:
        body = {
            "error": type(exc).__name__,
            "message": str(exc),
            "stack": traceback.format_exc(),
            "request_id": request_id,
        }
    return JSONResponse(status_code=status_code, content=body, headers={"X-Request-ID": request_id})


# Mount MCP servers with authentication middleware
# Read MCP: /ai/mcp (stacks, containers, hosts, logs, tags, pipeline state,
#                    action status/logs, health summary)
# Actions MCP: /ai/actions/mcp (trigger_pipeline, build/test/deploy_stack,
#                    set_transition_config, cancel_action, get/set_stack_env,
#                    container_action, update_service_image, remove_service,
#                    remove_stack). Every tool added here must also be listed in
#                    config_file.DANGEROUS_TOOL_NAMES.
app.include_router(run_history_router)

if _mcp_available:
    app.mount("/ai/actions", MCPAuthMiddleware(get_mcp_actions_app(), require_admin=True))
    app.mount("/ai", MCPAuthMiddleware(get_mcp_read_app(), require_admin=False))

# CORS middleware - restricted to same-origin; only needed for dev/proxy setups
app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)


app.include_router(auth_routes.router)


app.middleware("http")(auth_middleware)


app.middleware("http")(security_headers_middleware)


app.include_router(admin_routes.router)


# ============== Admin: Notifications ==============

# GET /api/admin/notifications and POST /api/admin/notifications/test
app.include_router(notifications_router)


app.include_router(dashboard_routes.router)


app.include_router(security_routes.router)


app.include_router(usage_routes.router)


app.include_router(containers_routes.router)


app.include_router(logs_routes.router)


app.include_router(hosts_routes.router)


app.include_router(health_routes.router)


app.include_router(stacks_routes.router)


app.include_router(config_routes.router)


app.include_router(actions_routes.router)


app.include_router(recovery_routes.router)


app.include_router(tag_cleanup_routes.router)


# ============== Build config detection ==============

_buildable_cache: Dict[str, bool] = {}  # {repo_name: has_build_config}


# ============== Pipeline (Auto Build → Test → Deploy) ==============

_auto_build_state = {}  # {repo_name: {"last_sha": str, "building": bool, "untagged_commits": int}}
_auto_build_task = None


app.include_router(pipeline_routes.router)


app.include_router(webhooks_routes.router)


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


class AgentResult(BaseModel):
    """Result of an action, as agent/action_poller.py send_result posts it."""
    agent_id: str
    action_id: str
    success: bool
    output: str = ""


@app.post("/api/agent/result")
async def post_agent_result(request: Request):
    """Report action result from an agent.

    Agents call this after executing an action to report the result.  The action
    must belong to the reporting agent: the key was validated against this same
    agent_id, so the pair binds the result to the identity that was polled.

    The agent sends a JSON body; query parameters are still accepted for older
    callers.  The auth middleware rejects a request whose query and body name
    different agents (_extract_agent_id).
    """
    body: Any = None
    if request.headers.get("content-type", "").startswith("application/json"):
        try:
            body = await request.json()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid JSON body")
    source = body if isinstance(body, dict) else dict(request.query_params)
    try:
        result = AgentResult.model_validate(source)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors(include_url=False))
    agent_id, action_id = result.agent_id, result.action_id
    success, output = result.success, result.output

    action = await actions_queue.complete_action(action_id, success, output,
                                                 agent_id=agent_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    return {"status": "ok", "action_id": action_id}


@app.post("/api/agent/system-error")
async def post_agent_system_error(request: Request):
    """Receive a system error report from a host agent.

    The report is attributed to the identity the middleware authenticated, not
    to the agent_id the body names: with per-agent keys those two can differ,
    and the body is what ends up in the LLM agent's system-error store.
    """
    body = await request.json()
    claimed_agent_id = body.get("agent_id", "unknown")
    authenticated_agent_id = getattr(request.state, "agent_id", None)
    if authenticated_agent_id:
        if claimed_agent_id not in ("unknown", authenticated_agent_id):
            logger.warning("Agent system error rejected: agent_id mismatch",
                           authenticated=authenticated_agent_id,
                           claimed=claimed_agent_id)
            raise HTTPException(
                status_code=403,
                detail="agent_id does not match the authenticated agent",
            )
        agent_id = authenticated_agent_id
    else:
        agent_id = claimed_agent_id
    category = body.get("category", "host_agent")
    error_type = body.get("error_type", "Unknown")
    error = body.get("error", "")
    if llm_agent:
        llm_agent._report_system_error(
            category, error_type, error,
            agent_id=agent_id)
    logger.warning("Host agent system error reported",
                   agent_id=agent_id, category=category,
                   error_type=error_type, error=error[:200])
    return {"status": "ok"}


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

    Requires an admin JWT (enforced by auth_middleware): despite the /api/agent/
    prefix this route is not called by agents, and the `exec` action type runs
    an arbitrary command in any container of any host.

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
        payload = decode_token(token, settings.auth.jwt_secret)
        if _token_is_revoked(payload):
            logger.warning("Terminal WebSocket rejected: revoked token",
                           user=payload.get("sub", ""))
            await websocket.close(code=4001, reason="Token revoked")
            return
        if payload.get("role") != "admin":
            await websocket.close(code=4003, reason="Admin access required")
            return
    except jwt.PyJWTError as e:
        logger.warning("Terminal WebSocket auth failed", error=str(e))
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
    from .ssh_client import resolve_known_hosts, save_host_key_if_needed

    # An unknown or unverifiable host raises UnknownHostKeyError here. It is left
    # to propagate on purpose: the caller already relays str(e) to the terminal,
    # and that message names the known_hosts file and the ssh-keyscan command to
    # run, which is exactly what the operator needs.
    known_hosts_setting, save_key = resolve_known_hosts(
        host_config.ssh_known_hosts_path,
        host_config.hostname,
        host_config.port,
    )
    options = {
        "host": host_config.hostname,
        "port": host_config.port,
        "username": host_config.username,
        "known_hosts": known_hosts_setting,
    }
    if host_config.ssh_key_path:
        key_path = Path(host_config.ssh_key_path).expanduser()
        options["client_keys"] = [str(key_path)]

    async with asyncssh.connect(**options) as conn:
        save_host_key_if_needed(conn, save_key, host_config.hostname, host_config.port)
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

_FRONTEND_DIR = "frontend"
_INDEX_PATH = os.path.join(_FRONTEND_DIR, "index.html")
_STATIC_DIR = os.path.abspath(os.path.join(_FRONTEND_DIR, "static"))

# href="/static/..." / src="/static/..." references, with or without a query
_ASSET_REF_RE = re.compile(r'(?P<attr>href|src)="(?P<path>/static/[^"?]+)(?:\?[^"]*)?"')

# url path -> (mtime_ns, size, version): re-hash an asset only when it changes
_asset_versions: Dict[str, tuple] = {}


def _asset_version(url_path: str) -> str:
    """Return the content hash used as the cache-busting version of an asset."""
    file_path = os.path.abspath(
        os.path.join(_STATIC_DIR, *url_path[len("/static/"):].split("/"))
    )
    # Paths come from our own index.html, but keep them inside the static dir
    try:
        if os.path.commonpath([_STATIC_DIR, file_path]) != _STATIC_DIR:
            return "0"
    except ValueError:  # different drives on Windows
        return "0"

    try:
        stat = os.stat(file_path)
    except OSError:
        return "0"

    cached = _asset_versions.get(url_path)
    if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return cached[2]

    try:
        with open(file_path, "rb") as f:
            version = hashlib.sha256(f.read()).hexdigest()[:12]
    except OSError:
        return "0"

    _asset_versions[url_path] = (stat.st_mtime_ns, stat.st_size, version)
    return version


# HTML comments, with the indentation before them and the line break after:
# they are notes for whoever edits index.html, not for whoever loads the page.
_HTML_COMMENT_RE = re.compile(r"[ \t]*<!--.*?-->[ \t]*(?:\r?\n)?", re.DOTALL)

# Where index.html asks for the per-response CSP nonce.
_CSP_NONCE_PLACEHOLDER = "__CSP_NONCE__"


def _render_index(style_nonce: str) -> str:
    """Read index.html, strip its comments, stamp /static references and the nonce."""
    with open(_INDEX_PATH, encoding="utf-8") as f:
        html = f.read()
    html = _HTML_COMMENT_RE.sub("", html)
    html = _ASSET_REF_RE.sub(
        lambda m: f'{m.group("attr")}="{m.group("path")}?v={_asset_version(m.group("path"))}"',
        html,
    )
    return html.replace(_CSP_NONCE_PLACEHOLDER, style_nonce)


@app.get("/")
async def serve_frontend(request: Request):
    """Serve the frontend.

    Each /static reference is stamped with a hash of that file's contents,
    replacing the version query that used to be maintained by hand: editing an
    asset without bumping it left browsers running a cached copy against an
    updated backend. The document itself must be revalidated for the new
    hashes to be picked up, hence no-cache.

    Every response draws a fresh style nonce and sends a policy naming it; the
    security headers middleware keeps that policy rather than its generic one.
    A nonce is only worth something while it is unpredictable, which no-cache
    also serves: this response has no validator, so each load is a new fetch
    and a new nonce.
    """
    nonce = secrets.token_urlsafe(18)
    return HTMLResponse(_render_index(nonce), headers={
        "Cache-Control": "no-cache",
        "Content-Security-Policy": _csp_directives(request, style_nonce=nonce),
    })
