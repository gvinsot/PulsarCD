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
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, WebSocket, WebSocketDisconnect
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
from .routes import agents as agents_routes
from .routes import terminal as terminal_routes
from .routes import frontend as frontend_routes
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


app.include_router(agents_routes.router)


app.include_router(terminal_routes.router)


# Serve static files (frontend)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")


app.include_router(frontend_routes.router)
