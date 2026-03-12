"""MCP (Model Context Protocol) server for PulsarCD.

Exposes PulsarCD functionality as MCP tools for AI agents.
Mounted on the existing FastAPI app at /mcp.
"""

import asyncio
import json
import re
import uuid
from datetime import datetime
from typing import Optional

import structlog
from mcp.server.fastmcp import FastMCP

logger = structlog.get_logger()

mcp = FastMCP(
    name="PulsarCD",
    instructions=(
        "PulsarCD is a DevOps monitoring platform. Use these tools to "
        "list stacks (GitHub repos), build/deploy Docker images, list containers "
        "and hosts, search and browse logs, get error summaries per service, "
        "and check build/deploy status.\n\n"
        "Typical log workflow:\n"
        "1. Call get_log_metadata() to discover available services, containers, and hosts.\n"
        "2. Call search_logs(github_project='myrepo', last_hours=24) to browse recent logs.\n"
        "3. For error counts per service: search_logs with opensearch_query and size=0 + aggs."
    ),
    stateless_http=True,
    json_response=True,
)


# ---------------------------------------------------------------------------
# Tool 1: list_stacks
# ---------------------------------------------------------------------------
@mcp.tool(description="List available stacks (starred GitHub repositories)")
async def list_stacks() -> str:
    """List all starred GitHub repos available as deployable stacks."""
    from .api import github_service

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})

    repos = await github_service.get_starred_repos()
    return json.dumps({"repos": repos, "count": len(repos)}, default=str)


# ---------------------------------------------------------------------------
# Tool 2: build_stack
# ---------------------------------------------------------------------------
@mcp.tool(
    description=(
        "Build a Docker image from a GitHub repository. "
        "Returns an action_id — use get_action_status to track progress."
    )
)
async def build_stack(
    repo_name: str,
    ssh_url: str,
    version: str = "1.0",
    branch: Optional[str] = None,
    commit: Optional[str] = None,
) -> str:
    """Build a stack image. Returns action_id for tracking."""
    from .api import github_service, _background_actions, BackgroundAction
    from .github_service import StackDeployer

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})

    # Validate branch/commit
    owner = None
    if ssh_url:
        match = re.search(r"[:/]([^/]+)/[^/]+\.git$", ssh_url)
        if match:
            owner = match.group(1)

    if branch and owner:
        is_valid, error_msg = await github_service.validate_branch(owner, repo_name, branch)
        if not is_valid:
            return json.dumps({"error": error_msg})

    if commit and owner:
        if not re.match(r"^[a-fA-F0-9]{7,40}$", commit):
            return json.dumps({"error": f"Invalid commit hash format: '{commit}'"})
        is_valid, error_msg = await github_service.validate_commit(owner, repo_name, commit)
        if not is_valid:
            return json.dumps({"error": error_msg})

    deployer, host_name = _get_deployer_and_host()

    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "build", repo_name)
    _background_actions[action_id] = action

    async def _run_build():
        try:
            result = await deployer.build(
                repo_name,
                ssh_url,
                version,
                branch=branch,
                commit=commit,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
        except Exception as e:
            action.status = "failed"
            action.result = {
                "success": False,
                "output": str(e),
                "action": "build",
                "repo": repo_name,
            }
            action.append_output(str(e))

    action.task = asyncio.create_task(_run_build())
    return json.dumps({"action_id": action_id, "action_type": "build", "repo": repo_name})


# ---------------------------------------------------------------------------
# Tool 3: deploy_stack
# ---------------------------------------------------------------------------
@mcp.tool(
    description=(
        "Deploy a stack to Docker Swarm. "
        "Returns an action_id — use get_action_status to track progress."
    )
)
async def deploy_stack(
    repo_name: str,
    ssh_url: str,
    version: str = "1.0",
    tag: Optional[str] = None,
) -> str:
    """Deploy a stack. Returns action_id for tracking."""
    from .api import github_service, _background_actions, BackgroundAction
    from .github_service import StackDeployer

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})

    if tag and not re.match(r"^v?\d+(\.\d+){0,2}$", tag):
        return json.dumps({"error": f"Invalid tag format: '{tag}'. Expected vX.X.X"})

    deployer, host_name = _get_deployer_and_host()

    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "deploy", repo_name)
    _background_actions[action_id] = action

    async def _run_deploy():
        try:
            result = await deployer.deploy(
                repo_name,
                ssh_url,
                version,
                tag=tag,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
        except Exception as e:
            action.status = "failed"
            action.result = {
                "success": False,
                "output": str(e),
                "action": "deploy",
                "repo": repo_name,
            }
            action.append_output(str(e))

    action.task = asyncio.create_task(_run_deploy())
    return json.dumps({"action_id": action_id, "action_type": "deploy", "repo": repo_name})


# ---------------------------------------------------------------------------
# Tool 4: list_containers
# ---------------------------------------------------------------------------
@mcp.tool(description="List all Docker containers and their states across all hosts")
async def list_containers(
    host: Optional[str] = None,
    status: Optional[str] = None,
) -> str:
    """List containers with optional host and status filters."""
    from .api import collector

    containers = await collector.get_all_containers(refresh=False)

    if host:
        containers = [c for c in containers if c.host == host]
    if status:
        containers = [c for c in containers if c.status.value == status]

    result = [
        {
            "id": c.id,
            "name": c.name,
            "image": c.image,
            "status": c.status.value,
            "host": c.host,
            "compose_project": c.compose_project,
            "compose_service": c.compose_service,
            "created": c.created.isoformat() if c.created else None,
        }
        for c in containers
    ]
    return json.dumps({"containers": result, "count": len(result)}, default=str)


# ---------------------------------------------------------------------------
# Tool 5: list_computers
# ---------------------------------------------------------------------------
@mcp.tool(description="List all monitored hosts/computers including discovered Swarm nodes")
async def list_computers() -> str:
    """List all hosts (configured + discovered swarm nodes)."""
    from .api import settings, collector

    configured_names = {h.name for h in settings.hosts}
    result = [
        {
            "name": h.name,
            "hostname": h.hostname,
            "mode": h.mode,
            "swarm_manager": h.swarm_manager,
            "is_swarm_node": False,
        }
        for h in settings.hosts
    ]
    for name, client in collector.clients.items():
        if name not in configured_names:
            result.append(
                {
                    "name": name,
                    "hostname": client.config.hostname,
                    "mode": client.config.mode,
                    "swarm_manager": False,
                    "is_swarm_node": True,
                }
            )
    return json.dumps({"hosts": result, "count": len(result)})


# ---------------------------------------------------------------------------
# Tool 6: get_log_metadata
# ---------------------------------------------------------------------------
@mcp.tool(
    description=(
        "Return all available hosts, containers, compose projects, compose services, "
        "and log levels present in the log store. Call this first to discover what "
        "services exist before querying logs."
    )
)
async def get_log_metadata() -> str:
    """Discover available hosts, services, containers and log levels."""
    from .api import opensearch

    if not opensearch:
        return json.dumps({"error": "OpenSearch not available"})

    meta = await opensearch.get_available_metadata()
    return json.dumps(meta)


# ---------------------------------------------------------------------------
# Tool 7: search_logs
# ---------------------------------------------------------------------------
_SEARCH_DOCS = """
Search logs stored in OpenSearch.

## Standard mode (use named parameters)

Parameters:
- query            Free-text search on the message field (Lucene syntax supported,
                   e.g. "connection refused", "timeout AND retry", "error*")
- github_project   GitHub repo name (case-insensitive) — matched against the
                   compose_project field. E.g. "MyApp" → searches compose_project="myapp"
- compose_services Comma-separated compose service names to filter on
- hosts            Comma-separated host names
- containers       Comma-separated container names
- levels           Comma-separated log levels: ERROR, FATAL, CRITICAL, WARN, INFO, DEBUG
- http_status_min  Lower bound of HTTP status code (e.g. 500 for server errors)
- http_status_max  Upper bound of HTTP status code (e.g. 599)
- last_hours       Shorthand time window: last N hours from now (1–720).
                   Ignored when start_time or end_time is provided.
- start_time       ISO 8601 start timestamp (e.g. "2024-01-15T10:00:00")
- end_time         ISO 8601 end timestamp
- sort_order       "desc" (newest first, default) or "asc" (chronological)
- size             Number of hits to return (1–200, default 50). Use 0 to get
                   aggregations only (no hits — useful for counts).
- from_offset      Pagination: skip first N results (default 0)

Response fields:
- total            Total number of matching documents
- returned         Number of hits in this response
- hits             List of log entries (timestamp, host, container, compose_project,
                   compose_service, level, http_status, message)
- aggregations     Breakdown counts by level, host, container, compose_project

## Raw OpenSearch mode (advanced)

Set opensearch_query to a JSON string containing a full OpenSearch request body.
All standard parameters above are IGNORED when this is set.
Size is capped at 500. The raw OpenSearch response is returned as-is.

Example — error counts per service over the last 24 hours:
  opensearch_query = '{
    "query": {"bool": {"filter": [
      {"range": {"timestamp": {"gte": "now-24h"}}},
      {"terms": {"level": ["ERROR","FATAL","CRITICAL"]}}
    ]}},
    "size": 0,
    "aggs": {"by_project": {"terms": {"field": "compose_project", "size": 50}}}
  }'

Available index fields: timestamp, host, container_name, container_id,
  compose_project, compose_service, level, message, http_status,
  network_rx_bytes, network_tx_bytes, stream.

Call get_log_metadata() first to discover valid values for host, compose_project, etc.
"""


@mcp.tool(description=_SEARCH_DOCS)
async def search_logs(
    query: Optional[str] = None,
    github_project: Optional[str] = None,
    compose_services: Optional[str] = None,
    hosts: Optional[str] = None,
    containers: Optional[str] = None,
    levels: Optional[str] = None,
    http_status_min: Optional[int] = None,
    http_status_max: Optional[int] = None,
    last_hours: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    sort_order: str = "desc",
    size: int = 50,
    from_offset: int = 0,
    opensearch_query: Optional[str] = None,
) -> str:
    """Search logs — standard filters or raw OpenSearch query."""
    from .api import opensearch
    from .models import LogSearchQuery

    if not opensearch:
        return json.dumps({"error": "OpenSearch not available"})

    # ── Raw OpenSearch passthrough ──────────────────────────────────────────
    if opensearch_query:
        try:
            body = json.loads(opensearch_query)
        except json.JSONDecodeError as exc:
            return json.dumps({"error": f"Invalid JSON in opensearch_query: {exc}"})
        try:
            raw = await opensearch.run_logs_query(body)
        except Exception as exc:
            return json.dumps({"error": str(exc)})
        # Return a clean subset of the raw response
        total = raw.get("hits", {}).get("total", {})
        total_count = total.get("value", total) if isinstance(total, dict) else total
        hits_out = [
            {k: v for k, v in h.get("_source", {}).items()}
            for h in raw.get("hits", {}).get("hits", [])
        ]
        aggs_out = {}
        for key, agg in raw.get("aggregations", {}).items():
            if "buckets" in agg:
                aggs_out[key] = [
                    {"key": b.get("key"), "count": b.get("doc_count")}
                    for b in agg["buckets"]
                ]
            else:
                aggs_out[key] = agg
        return json.dumps(
            {"total": total_count, "returned": len(hits_out), "hits": hits_out, "aggregations": aggs_out},
            default=str,
        )

    # ── Standard filtered search ────────────────────────────────────────────
    # Resolve github_project → compose_project (lowercased)
    projects = []
    if github_project:
        projects = [github_project.strip().lower()]

    # Time window: last_hours shorthand
    parsed_start = datetime.fromisoformat(start_time) if start_time else None
    parsed_end = datetime.fromisoformat(end_time) if end_time else None
    if last_hours is not None and parsed_start is None and parsed_end is None:
        from datetime import timedelta
        parsed_start = datetime.utcnow() - timedelta(hours=max(1, min(last_hours, 720)))

    search_query = LogSearchQuery(
        query=query,
        hosts=[h.strip() for h in hosts.split(",") if h.strip()] if hosts else [],
        containers=[c.strip() for c in containers.split(",") if c.strip()] if containers else [],
        compose_projects=projects,
        levels=[lv.strip().upper() for lv in levels.split(",") if lv.strip()] if levels else [],
        http_status_min=http_status_min,
        http_status_max=http_status_max,
        start_time=parsed_start,
        end_time=parsed_end,
        sort_order=sort_order if sort_order in ("asc", "desc") else "desc",
        size=min(max(size, 0), 200),
        **{"from": max(from_offset, 0)},
    )

    result = await opensearch.search_logs(search_query)

    # Post-filter by compose_service (not a LogSearchQuery field)
    hits_raw = result.hits
    if compose_services:
        svc_set = {s.strip() for s in compose_services.split(",") if s.strip()}
        hits_raw = [h for h in hits_raw if h.compose_service in svc_set]

    hits = [
        {
            "timestamp": h.timestamp.isoformat(),
            "host": h.host,
            "container": h.container_name,
            "compose_project": h.compose_project,
            "compose_service": h.compose_service,
            "level": h.level,
            "http_status": h.http_status,
            "message": h.message[:500],
        }
        for h in hits_raw
    ]
    return json.dumps(
        {
            "total": result.total,
            "returned": len(hits),
            "hits": hits,
            "aggregations": result.aggregations,
        },
        default=str,
    )


# ---------------------------------------------------------------------------
# Tool 8: get_action_status
# ---------------------------------------------------------------------------
@mcp.tool(
    description="Check the status of a background build or deploy action by its action_id"
)
async def get_action_status(action_id: str) -> str:
    """Get status of a build/deploy action."""
    from .api import _background_actions

    action = _background_actions.get(action_id)
    if not action:
        return json.dumps({"error": f"Action '{action_id}' not found"})

    response = {
        "action_id": action.id,
        "action_type": action.action_type,
        "repo": action.repo_name,
        "status": action.status,
        "started_at": action.started_at.isoformat(),
        "elapsed_seconds": (datetime.utcnow() - action.started_at).total_seconds(),
        "output_lines": len(action.output_lines),
        "last_output": action.output_lines[-5:] if action.output_lines else [],
    }
    if action.result:
        response["result"] = action.result

    return json.dumps(response, default=str)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _get_deployer_and_host():
    """Get a StackDeployer instance and the target host name."""
    from .api import collector, settings
    from .github_service import StackDeployer

    if not collector.clients:
        raise RuntimeError("No host clients available")

    host_name = None
    host_client = None

    for name, client in collector.clients.items():
        if hasattr(client, "config") and getattr(client.config, "swarm_manager", False):
            host_name = name
            host_client = client
            break

    if not host_client:
        host_name, host_client = next(iter(collector.clients.items()))

    return StackDeployer(settings.github, host_client), host_name


def get_mcp_app():
    """Return the ASGI app for the MCP server.

    The SDK serves internally on /mcp, and FastAPI mounts this at /ai,
    so the full endpoint is /ai/mcp.
    """
    return mcp.streamable_http_app()
