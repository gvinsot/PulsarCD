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
    _auto_build_commit,
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
from .routes import auth as auth_routes
from .routes import admin as admin_routes
from .routes import dashboard as dashboard_routes
from .routes import security as security_routes
from .routes import usage as usage_routes
from .routes import containers as containers_routes
from .routes import logs as logs_routes
from .routes import hosts as hosts_routes
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


# ============== Health ==============

@app.get("/api/health")
async def health_check() -> Dict[str, Any]:
    """Public liveness probe: service status and OpenSearch reachability only.

    This is the single unauthenticated endpoint, so it must not disclose
    anything about the deployment.  Versions, index names and document counts
    live in the admin-only diagnostics (/api/admin/opensearch-status and
    /api/admin/opensearch-probe).
    """
    result: Dict[str, Any] = {"status": "healthy", "service": "pulsarcd"}

    if opensearch and opensearch._client:
        try:
            await opensearch._client.info()
            result["opensearch"] = "connected"
            # Coarse only ("ok" / "migrating" / "degraded"): details are in
            # /api/admin/opensearch-status.
            index_health = opensearch.index_health()
            if isinstance(index_health, str):
                result["indices"] = index_health
        except Exception as e:
            # The error type/message can leak host names or credentials: log it
            # server-side and expose only the coarse state.
            logger.warning("Health check: OpenSearch unreachable", error=str(e))
            result["opensearch"] = "error"
    else:
        result["opensearch"] = "not_configured"

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
            "model": settings.pulsar_config.llm.model if settings.pulsar_config else settings.ai.model,
            "url": settings.pulsar_config.llm.url if settings.pulsar_config else "",
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
    branch: str = Query(default=None, description="Branch name (defaults to repo default branch)"),
):
    """Get recent commits that don't have a tag (not yet built/deployed)."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    return await github_service.get_untagged_commits(owner, repo, limit, branch=branch)


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
    run_history.set_commit(commit)
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
    qa: bool = Query(default=False, description="Deploy to the isolated QA environment (stack prefixed 'qa-')"),
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
    action_type = "qa-deploy" if qa else "deploy"
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, action_type, repo_name)
    _background_actions[action_id] = action

    # Use the tag as display version when deploying a specific tag
    deploy_version = tag.lstrip('v') if tag else version
    # When deploying a specific tag (no prior build), clear build_action_id
    prev_build_id = pipeline_state.get_legacy(repo_name).get("build_action_id") if not tag else None

    pipeline_stage = "qa" if qa else "deploy"
    if qa:
        _set_pipeline(repo_name, pipeline_stage, "running", deploy_version, build_id=prev_build_id, qa_id=action_id)
    else:
        _set_pipeline(repo_name, pipeline_stage, "running", deploy_version, build_id=prev_build_id, deploy_id=action_id)

    async def _run_deploy():
        try:
            # Rolls back to the last good version if post-deploy verification fails
            result = await deploy_stage_with_rollback(
                deployer, pipeline_state, repo_name, ssh_url, version=version, tag=tag, qa=qa,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            if result.get("success"):
                if qa:
                    # Mark QA success first so current_version is updated, then
                    # apply the always-manual gate before production deploy.
                    _set_pipeline(repo_name, "qa", "success", deploy_version,
                                  qa_id=action_id, log_lines=action.output_lines)
                    pipeline_state.record_gate(repo_name, "qa_to_deploy", False,
                                               "Manual transition — waiting for user approval after QA",
                                               version=deploy_version)
                    _set_pipeline(repo_name, "qa", "gate_rejected", deploy_version,
                                  qa_id=action_id, log_lines=action.output_lines)
                else:
                    _set_pipeline(repo_name, "done", "success", deploy_version, deploy_id=action_id, log_lines=action.output_lines)
            else:
                _set_pipeline(repo_name, pipeline_stage, "failed", deploy_version,
                              deploy_id=None if qa else action_id,
                              qa_id=action_id if qa else None,
                              log_lines=action.output_lines)
                await _notify_agent_failure(pipeline_stage, repo_name, deploy_version, result.get("output", ""))
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.exception("Background deploy failed", repo=repo_name, error=str(e), traceback=traceback.format_exc())
            action.status = "failed"
            action.result = {"success": False, "output": error_detail, "action": action_type, "repo": repo_name}
            action.append_output(error_detail)
            _set_pipeline(repo_name, pipeline_stage, "failed", deploy_version,
                          deploy_id=None if qa else action_id,
                          qa_id=action_id if qa else None,
                          log_lines=action.output_lines)
            await _notify_agent_failure(pipeline_stage, repo_name, deploy_version, error_detail)

    action.task = asyncio.create_task(_run_deploy())

    return {"action_id": action_id, "action_type": action_type, "repo": repo_name}


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
    if action:
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

    # Fallback: reconstruct minimal status from persisted pipeline state
    if pipeline_state:
        for repo_name, entry in pipeline_state.items():
            for stage_name, stage in entry.stages.items():
                if stage.action_id == action_id:
                    return {
                        "action_id": action_id,
                        "action_type": stage_name,
                        "repo": repo_name,
                        "status": stage.status if stage.status and stage.status != "idle" else "unknown",
                        "restored": True,
                    }

    raise HTTPException(status_code=404, detail="Action not found")


@app.get("/api/stacks/actions/{action_id}/logs")
async def get_action_logs(
    action_id: str,
    offset: int = Query(default=0, ge=0, description="Line offset to start from"),
) -> Dict[str, Any]:
    """Get the streaming logs of a background build/deploy action.

    Falls back to persisted pipeline state logs when the action is no
    longer in memory (e.g. after a server restart).
    """
    action = _background_actions.get(action_id)
    if action:
        lines = action.output_lines[offset:]
        return {
            "action_id": action.id,
            "status": action.status,
            "lines": lines,
            "offset": offset,
            "total_lines": len(action.output_lines),
        }

    # Fallback: look up the action_id in persisted pipeline state
    if pipeline_state:
        for repo_name, entry in pipeline_state.items():
            for stage_name, stage in entry.stages.items():
                if stage.action_id == action_id and stage.last_log:
                    lines = stage.last_log[offset:]
                    return {
                        "action_id": action_id,
                        "status": stage.status or "completed",
                        "lines": lines,
                        "offset": offset,
                        "total_lines": len(stage.last_log),
                        "restored": True,
                    }

    raise HTTPException(status_code=404, detail="Action not found")


@app.post("/api/stacks/actions/{action_id}/logs/themes")
async def group_action_test_themes(action_id: str, request: Request) -> Dict[str, Any]:
    """Suggest themes for parsed test metadata using the configured LLM."""
    from .test_log_themes import MAX_THEME_PAYLOAD_BYTES, ThemeGroupingError, group_test_themes

    action = await get_action_status(action_id)
    if action["action_type"] != "test":
        raise HTTPException(status_code=400, detail="Themes are only available for test actions")

    # Allow JSON escaping overhead while bounding the request before parsing it.
    raw = bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw) > MAX_THEME_PAYLOAD_BYTES * 6 + 4096:
            raise HTTPException(status_code=400, detail="Test metadata is too large to group at once")
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        raise HTTPException(status_code=400, detail="A valid JSON object with entries is required")
    if not isinstance(body, dict) or "entries" not in body:
        raise HTTPException(status_code=400, detail="A JSON object with entries is required")

    try:
        return await group_test_themes(body["entries"])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ThemeGroupingError:
        raise HTTPException(status_code=503, detail="Test theme grouping is unavailable. Try again later.")


@app.get("/api/stacks/actions/{action_id}/logs/stream")
async def stream_action_logs(
    action_id: str,
    offset: int = Query(default=0, ge=0, description="Line offset to start from"),
):
    """Stream logs of a background action via Server-Sent Events."""
    action = _background_actions.get(action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    # One frame per line, deliberately: a cached older frontend only knows the
    # "line" type, and batching would make it silently drop every log line.
    # The client batches the DOM writes instead. Yield to the event loop every
    # _YIELD_EVERY lines so dumping a large backlog does not stall the server.
    _YIELD_EVERY = 500

    async def event_generator():
        cursor = offset
        while True:
            # Send any new lines
            if cursor < len(action.output_lines):
                pending = action.output_lines[cursor:]
                cursor += len(pending)
                for index, line in enumerate(pending, start=1):
                    data = json.dumps({"type": "line", "line": line})
                    yield f"data: {data}\n\n"
                    if index % _YIELD_EVERY == 0:
                        await asyncio.sleep(0)

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
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.warning("Error while waiting for action cancellation",
                       action_id=action_id, error=str(e))

    if action.status == "running":
        action.status = "cancelled"

    # Update pipeline state so UI reflects cancellation immediately
    kwargs = {}
    # A QA deployment runs as a "qa-deploy" action but is the "qa" pipeline stage.
    stage = "qa" if action.action_type == "qa-deploy" else action.action_type
    if stage == "build":
        kwargs["build_id"] = action_id
    elif stage == "test":
        kwargs["test_id"] = action_id
    elif stage == "qa":
        kwargs["qa_id"] = action_id
    elif stage == "deploy":
        kwargs["deploy_id"] = action_id
    elif action.action_type == "qa-deploy":
        kwargs["qa_id"] = action_id
    # A QA deploy action runs the pipeline's "qa" stage
    stage = "qa" if action.action_type == "qa-deploy" else action.action_type
    _set_pipeline(
        action.repo_name,
        stage,
        "failed",
        pipeline_state.get_legacy(action.repo_name).get("version", ""),
        log_lines=action.output_lines,
        **kwargs,
    )

    return {"success": True, "message": "Action cancelled"}


# env=prod is devops/.env; env=qa is devops/.env.qa, whose variables override
# devops/.env for the QA deployment only.
_ENV_QUERY = Query("prod", pattern="^(prod|qa)$")


@app.get("/api/stacks/{repo_name}/env")
async def get_stack_env(repo_name: str, env: str = _ENV_QUERY):
    """Get the .env (or .env.qa) file content for a stack."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    deployer = StackDeployer(settings.github, None)
    success, content = await deployer.get_env_file(repo_name, env=env)

    if not success:
        raise HTTPException(status_code=500, detail=content)

    return {"content": content, "repo": repo_name, "env": env}


@app.put("/api/stacks/{repo_name}/env")
async def save_stack_env(repo_name: str, request: Request, env: str = _ENV_QUERY):
    """Save the .env (or .env.qa) file content for a stack."""
    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    body = await request.json()
    content = body.get("content", "")

    deployer = StackDeployer(settings.github, None)
    success, message = await deployer.save_env_file(
        repo_name, content, actor=getattr(request.state, "user", "operator"), env=env)

    if not success:
        raise HTTPException(status_code=500, detail=message)

    return {"success": True, "message": message, "repo": repo_name, "env": env}


@app.get("/api/admin/recovery/status")
async def recovery_status():
    from .recovery import status
    return dict(status)


@app.get("/api/admin/recovery/env/{repo_name}/history")
async def env_backup_history(repo_name: str, env: str = _ENV_QUERY):
    from .backup_vault import get_vault, BackupError
    from .github_service import _validate_repo_name
    from .recovery import env_resource
    try:
        _validate_repo_name(repo_name)
        vault = await asyncio.to_thread(get_vault)
        if vault is None:
            raise HTTPException(503, "Encrypted backup is disabled")
        versions = await asyncio.to_thread(vault.history, "env", env_resource(repo_name, env))
        return {"versions": versions}
    except ValueError:
        raise HTTPException(400, "Invalid repository name") from None
    except BackupError as exc:
        raise HTTPException(503, str(exc)) from None


@app.post("/api/admin/recovery/env/{repo_name}/restore/{revision}")
async def restore_env_backup(repo_name: str, revision: str, request: Request,
                             env: str = _ENV_QUERY):
    from .backup_vault import get_vault, BackupError
    from .github_service import _validate_repo_name
    from .recovery import env_resource
    try:
        _validate_repo_name(repo_name)
        vault = await asyncio.to_thread(get_vault)
        if vault is None:
            raise HTTPException(503, "Encrypted backup is disabled")
        content = await asyncio.to_thread(vault.read, "env", env_resource(repo_name, env), revision)
        success, message = await StackDeployer(settings.github).save_env_file(
            repo_name, content.decode("utf-8"), actor=getattr(request.state, "user", "operator"),
            env=env)
        if not success:
            raise HTTPException(503, message)
        return {"success": True, "message": message}
    except (ValueError, UnicodeError):
        raise HTTPException(400, "Invalid repository or environment content") from None
    except BackupError as exc:
        raise HTTPException(503, str(exc)) from None


_deployed_tags_cache = None
_deployed_tags_cache_time = None
_DEPLOYED_TAGS_CACHE_TTL = 10  # seconds


@app.get("/api/stacks/deployed-tags")
async def get_stacks_deployed_tags():
    """Get deployed image tags and latest built tags for all stacks."""
    global _deployed_tags_cache, _deployed_tags_cache_time

    if not github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    # Return cached result if fresh enough
    now = asyncio.get_event_loop().time()
    if _deployed_tags_cache and _deployed_tags_cache_time and (now - _deployed_tags_cache_time) < _DEPLOYED_TAGS_CACHE_TTL:
        return _deployed_tags_cache

    repos = await github_service.get_starred_repos()
    deployer = StackDeployer(settings.github, None)

    # Fetch all deployed tags in a single SSH command (avoids concurrent SSH issues).
    # Returns both production and QA stack tags ("qa-<stack>_*" services).
    all_deployed, all_deployed_qa = await deployer.get_all_deployed_stack_tags_with_qa(
        [repo["name"] for repo in repos]
    )

    # Fetch latest built tags from GitHub in parallel
    async def get_latest(repo):
        return repo["name"], await github_service.get_latest_tag(repo["owner"], repo["name"])

    latest_results = await asyncio.gather(*[get_latest(r) for r in repos])

    deployed_tags = {name: tag for name, tag in all_deployed.items() if tag}
    deployed_qa_tags = {name: tag for name, tag in all_deployed_qa.items() if tag}
    latest_built = {name: tag for name, tag in latest_results if tag}

    result = {"tags": deployed_tags, "qa_tags": deployed_qa_tags, "latest_built": latest_built}
    _deployed_tags_cache = result
    _deployed_tags_cache_time = now
    return result


# ============== Tag Cleanup ==============

@app.post("/api/admin/tag-cleanup/run")
async def trigger_tag_cleanup(
    dry_run: bool = Query(default=None, description="Override dry_run setting for this run"),
):
    """Manually trigger a tag cleanup cycle."""
    if not tag_cleaner:
        raise HTTPException(status_code=400, detail="Tag cleanup not configured")

    # Temporarily override dry_run if requested
    original_dry_run = tag_cleaner._config.dry_run
    if dry_run is not None:
        tag_cleaner._config.dry_run = dry_run
    try:
        result = await tag_cleaner.run_cleanup()
    finally:
        tag_cleaner._config.dry_run = original_dry_run

    return result


@app.post("/api/admin/tag-cleanup/repo/{owner}/{repo}")
async def trigger_repo_tag_cleanup(
    owner: str,
    repo: str,
    dry_run: bool = Query(default=None, description="Override dry_run setting for this run"),
):
    """Manually trigger tag cleanup for a single repository."""
    if not tag_cleaner:
        raise HTTPException(status_code=400, detail="Tag cleanup not configured")

    original_dry_run = tag_cleaner._config.dry_run
    if dry_run is not None:
        tag_cleaner._config.dry_run = dry_run
    try:
        result = await tag_cleaner.run_cleanup_repo(owner, repo)
    finally:
        tag_cleaner._config.dry_run = original_dry_run

    return result


@app.get("/api/admin/tag-cleanup/config")
async def get_tag_cleanup_config():
    """Get current tag cleanup configuration."""
    if not settings.pulsar_config:
        raise HTTPException(status_code=400, detail="PulsarConfig not loaded")
    return settings.pulsar_config.tag_cleanup.model_dump()


# ============== Build config detection ==============

_buildable_cache: Dict[str, bool] = {}  # {repo_name: has_build_config}


@app.get("/api/stacks/buildable")
async def get_stacks_buildable(refresh: bool = Query(default=False)) -> Dict[str, bool]:
    """Return which stacks have build: directives in their docker-compose.swarm.yml."""
    if not github_service.is_configured():
        return {}
    if _buildable_cache and not refresh:
        return _buildable_cache
    _buildable_cache.clear()
    repos = await github_service.get_starred_repos()
    deployer, _ = _get_deployer_and_host()
    for repo in repos:
        name = repo["name"]
        ssh_url = repo.get("ssh_url", "")
        try:
            # Update local clone so newly added build: directives are detected
            if ssh_url:
                await deployer._ensure_repo_cloned(name, ssh_url)
            _buildable_cache[name] = await deployer.has_build_config(name)
        except Exception as e:
            logger.warning("Failed to check build config, assuming buildable",
                           repo=name, error=str(e))
            _buildable_cache[name] = True
    return _buildable_cache


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
        started = await _trigger_pipeline(repo_name, ssh_url, version=version, tag=tag)
        if not started:
            raise HTTPException(
                status_code=409,
                detail=f"A build is already running for {repo_name}"
            )
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
        run_history.set_commit(commit)
        started = await _trigger_pipeline(repo_name, ssh_url, version=next_version, tag=new_tag)
        if not started:
            raise HTTPException(
                status_code=409,
                detail=f"A build is already running for {repo_name}"
            )
        return {"status": "started", "repo": repo_name, "tag": new_tag, "version": next_version, "auto_tagged": True}


# ---- GitHub push webhook ----------------------------------------------------
# Signature checking, replay protection and parsing live in github_webhook.py;
# this route only maps a verified push onto _auto_build_commit.  The auth
# middleware lets this exact route through without a session (_WEBHOOK_ROUTES).

from .github_webhook import (  # noqa: E402  (kept next to its only user)
    MAX_PAYLOAD_BYTES as _WEBHOOK_MAX_PAYLOAD_BYTES,
    ZERO_SHA as _ZERO_SHA,
    DeliveryCache,
    PushEvent,
    decode_body as _decode_webhook_body,
    parse_push as _parse_push,
    verify_signature as _verify_webhook_signature,
)

_webhook_deliveries = DeliveryCache()
# Strong references to in-flight webhook builds (asyncio keeps only weak ones).
_webhook_tasks: set = set()


# Pause before each read of the branch listing.  GitHub can deliver a push
# before its REST API lists the commit; reading the tag state from that stale
# listing would build the previous release under the new commit's name.
_WEBHOOK_LISTING_RETRY_DELAYS = (0, 1, 2, 4, 8, 15)
# How long the poller keeps re-reading a repo after the webhook handed a
# commit back to it, even though the repo's pushed_at no longer changes.
_WEBHOOK_HANDBACK_RECHECK_SECONDS = 600


class _StaleListing(Exception):
    """GitHub's commit listing did not show the pushed commit as the head."""


def _request_recheck(state: dict, seconds: float = _WEBHOOK_HANDBACK_RECHECK_SECONDS):
    """Make the poller re-read this repo on every cycle for ``seconds``."""
    state["recheck_until"] = max(state.get("recheck_until", 0.0),
                                 time.monotonic() + seconds)


async def _read_pushed_head(owner: str, name: str, sha: str) -> Optional[dict]:
    """Read the tag state once GitHub's listing shows ``sha`` as the head.

    Returns None when a newer commit superseded ``sha`` meanwhile (its own
    delivery or the poller deals with it).  Raises _StaleListing when the
    listing never caught up.
    """
    seen_head = None
    for delay in _WEBHOOK_LISTING_RETRY_DELAYS:
        if delay:
            await asyncio.sleep(delay)
        if (_auto_build_state.get(name) or {}).get("last_sha") != sha:
            return None
        github_service.invalidate_repo_cache(owner, name)
        # Same arguments (and so the same cache entry) as the commit read
        # inside get_untagged_commits, so both describe one listing.
        commits_data = await github_service.get_repo_commits(owner, name, per_page=20)
        commits = commits_data.get("commits") or []
        seen_head = commits[0].get("sha") if commits else None
        if seen_head != sha:
            continue
        untagged_data = await github_service.get_untagged_commits(owner, name, limit=20)
        untagged = untagged_data.get("untagged_commits") or []
        if untagged and untagged[0].get("sha") != sha:
            continue
        return untagged_data
    raise _StaleListing(f"listing head is {(seen_head or 'unknown')[:7]}, not {sha[:7]}")


async def _run_webhook_build(owner: str, name: str, ssh_url: str, sha: str,
                             old_sha: Optional[str]):
    """Background half of a webhook push: read the tag state, then build."""
    # This task has its own context copy: label the runs it starts.
    run_history.set_trigger("webhook", actor="github-webhook", commit=sha)
    try:
        untagged_data = await _read_pushed_head(owner, name, sha)
        if untagged_data is None:
            logger.info("Webhook push superseded by a newer commit", repo=name, sha=sha[:7])
            return
        await _auto_build_commit(owner, name, ssh_url, sha, untagged_data,
                                 old_sha=old_sha, source="webhook")
    except Exception as e:
        logger.error("Webhook-triggered build failed", repo=name, sha=sha[:7], error=str(e))
        # Nothing was started: hand the commit back to the poller.
        state = _auto_build_state.get(name) or {}
        if state.get("last_sha") == sha and not state.get("building"):
            if old_sha:
                state["last_sha"] = old_sha
                if old_sha in state.get("seen_shas", []):
                    state["seen_shas"].remove(old_sha)
            else:
                state.pop("last_sha", None)
            # The poller may already have consumed this push's pushed_at.
            _request_recheck(state)


async def _handle_github_push(push: PushEvent) -> dict:
    """Decide what a verified push means for the auto-build.

    Everything up to claiming the commit runs without an ``await`` between the
    state check and ``_advance_last_sha``, so a concurrent poll can never pick
    the same commit up a second time.  The slow part (tagging, cloning) runs in
    the background: GitHub gives up on a delivery after 10 seconds.
    """
    def ignored(reason: str) -> dict:
        return {"status": "ignored", "reason": reason, "repo": push.full_name}

    if not github_service or not github_service.is_configured():
        return ignored("GitHub integration is not configured")
    if push.deleted:
        return ignored("ref was deleted")
    if push.branch is None:
        return ignored("not a branch push")
    if not push.is_default_branch:
        return ignored(f"push to '{push.branch}', not the default branch")

    repos = await github_service.get_starred_repos()
    wanted = push.full_name.lower()
    tracked = next((r for r in repos if (r.get("full_name") or "").lower() == wanted), None)
    if tracked is None:
        return ignored("repository is not tracked")

    # Owner, name and clone URL come from our own repo listing, not the payload.
    owner, name, ssh_url = tracked["owner"], tracked["name"], tracked["ssh_url"]
    state = _auto_build_state.setdefault(name, {"building": False})
    if _sha_already_handled(state, push.after):
        return ignored("commit already handled")

    if state.get("building"):
        # Leave the commit to the poller, which builds it once the running
        # pipeline releases the lock.  A repo the poller has not observed yet
        # gets the pre-push head as its baseline so the new one reads as new.
        if "last_sha" not in state and push.before and push.before != _ZERO_SHA:
            state["last_sha"] = push.before
        state["recheck"] = True
        # Also past a stale listing that still shows the pre-push head.
        _request_recheck(state)
        return {"status": "deferred", "repo": name, "sha": push.after,
                "reason": "a pipeline is already running for this repository"}

    # Baseline handed back to the poller if the build cannot start: without
    # one, the poller's first look would only record the pushed commit.
    old_sha = state.get("last_sha") or (
        push.before if push.before and push.before != _ZERO_SHA else None)
    _advance_last_sha(state, push.after)
    task = asyncio.create_task(_run_webhook_build(owner, name, ssh_url, push.after, old_sha))
    _webhook_tasks.add(task)
    task.add_done_callback(_webhook_tasks.discard)
    return {"status": "triggered", "repo": name, "sha": push.after}


@app.post("/api/webhooks/github")
async def github_webhook_endpoint(request: Request):
    """Receive GitHub push events and start the auto-build immediately.

    Authenticated by ``X-Hub-Signature-256`` against
    PULSARCD_GITHUB__WEBHOOK_SECRET; answers 404 while that secret is unset.
    ``X-GitHub-Delivery`` ids are remembered so a replayed delivery is a no-op.
    """
    secret = _github_webhook_secret()
    if not secret:
        raise HTTPException(status_code=404, detail="Not Found")

    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > _WEBHOOK_MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")
    body = await request.body()
    if len(body) > _WEBHOOK_MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")

    if not _verify_webhook_signature(secret, body, request.headers.get("X-Hub-Signature-256")):
        logger.warning("GitHub webhook rejected: invalid signature",
                       client=request.client.host if request.client else "",
                       delivery=request.headers.get("X-GitHub-Delivery", ""))
        raise HTTPException(status_code=401, detail="Invalid signature")

    event = request.headers.get("X-GitHub-Event", "")
    delivery = request.headers.get("X-GitHub-Delivery", "").strip()
    if not delivery:
        raise HTTPException(status_code=400, detail="Missing X-GitHub-Delivery header")
    if not _webhook_deliveries.check_and_add(delivery):
        logger.info("GitHub webhook delivery already processed", delivery=delivery,
                    github_event=event)
        return {"status": "duplicate", "delivery": delivery}

    try:
        payload = _decode_webhook_body(body, request.headers.get("content-type", ""))
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    if event == "ping":
        logger.info("GitHub webhook ping received", delivery=delivery,
                    hook_id=payload.get("hook_id"))
        return {"status": "pong", "delivery": delivery}
    if event != "push":
        return {"status": "ignored", "reason": f"event '{event}' is not handled",
                "delivery": delivery}

    push = _parse_push(payload)
    if push is None:
        raise HTTPException(status_code=400, detail="Invalid push payload")

    try:
        result = await _handle_github_push(push)
    except Exception:
        # Let GitHub's redelivery of this id through: nothing was claimed.
        _webhook_deliveries.discard(delivery)
        raise
    result["delivery"] = delivery
    logger.info("GitHub push webhook processed", delivery=delivery, ref=push.ref,
                sha=push.after[:7], **{k: v for k, v in result.items()
                                       if k in ("status", "repo", "reason")})
    return JSONResponse(status_code=202 if result["status"] == "triggered" else 200,
                        content=result)


@app.get("/api/stacks/pipeline/status")
async def get_pipeline_status():
    """Get pipeline state for all repos."""
    return {"pipelines": pipeline_state.get_all_legacy()}


@app.get("/api/stacks/pipeline/{repo_name}/transition/{transition}")
async def get_transition_config(repo_name: str, transition: str):
    """Get per-project transition config for a specific transition."""
    valid = {"version_to_build", "build_to_test", "test_to_deploy", "qa_to_deploy"}
    if transition not in valid:
        return JSONResponse({"error": "Invalid transition"}, status_code=400)
    config = pipeline_state.get_transition_config(repo_name, transition)
    # Also return last gate decision if available
    entry = pipeline_state.get(repo_name)
    last_decision = None
    if entry:
        for g in reversed(entry.gates):
            if g.transition == transition:
                last_decision = g.to_dict()
                break
    return {"repo": repo_name, "transition": transition, "config": config, "last_decision": last_decision}


@app.put("/api/stacks/pipeline/{repo_name}/transition/{transition}")
async def set_transition_config(repo_name: str, transition: str, request: Request):
    """Set per-project transition config for a specific transition.

    For test_to_deploy, accepts an optional ``qa_enabled`` boolean: when true,
    the pipeline runs an isolated QA deploy (stack prefixed ``qa-``, domains
    prefixed ``qa.``) before the production deploy.
    """
    valid = {"version_to_build", "build_to_test", "test_to_deploy", "qa_to_deploy"}
    if transition not in valid:
        return JSONResponse({"error": "Invalid transition"}, status_code=400)
    body = await request.json()
    mode = body.get("mode", "auto_with_success")
    valid_modes = {"auto", "auto_with_success", "agent", "manual"}
    if mode not in valid_modes:
        return JSONResponse({"error": f"Invalid mode. Valid: {', '.join(sorted(valid_modes))}"}, status_code=400)
    cfg: Dict[str, Any] = {"mode": mode}
    if transition == "test_to_deploy":
        cfg["qa_enabled"] = bool(body.get("qa_enabled", False))
    pipeline_state.set_transition_config(repo_name, transition, cfg)
    logger.info("Transition config updated", repo=repo_name, transition=transition,
                mode=mode, qa_enabled=cfg.get("qa_enabled"))
    return {"saved": True, "repo": repo_name, "transition": transition, **cfg}


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
