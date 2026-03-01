"""MCP (Model Context Protocol) server for LogsCrawler.

Exposes LogsCrawler functionality as MCP tools for AI agents.
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
    name="LogsCrawler",
    instructions=(
        "LogsCrawler is a DevOps monitoring platform. Use these tools to "
        "list stacks (GitHub repos), build/deploy Docker images, list containers "
        "and hosts, search logs, and check build/deploy status."
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
# Tool 6: search_logs
# ---------------------------------------------------------------------------
@mcp.tool(
    description=(
        "Search through collected logs. Supports free-text query and filters "
        "for host, container, compose project, log level, and time range. "
        "Comma-separate multiple values for hosts/containers/levels. "
        "Times must be ISO 8601 format."
    )
)
async def search_logs(
    query: Optional[str] = None,
    hosts: Optional[str] = None,
    containers: Optional[str] = None,
    compose_projects: Optional[str] = None,
    levels: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    size: int = 50,
) -> str:
    """Search logs with filters."""
    from .api import opensearch
    from .models import LogSearchQuery

    search_query = LogSearchQuery(
        query=query,
        hosts=hosts.split(",") if hosts else [],
        containers=containers.split(",") if containers else [],
        compose_projects=compose_projects.split(",") if compose_projects else [],
        levels=levels.split(",") if levels else [],
        start_time=datetime.fromisoformat(start_time) if start_time else None,
        end_time=datetime.fromisoformat(end_time) if end_time else None,
        size=min(size, 200),
    )

    result = await opensearch.search_logs(search_query)

    hits = [
        {
            "timestamp": h.timestamp.isoformat(),
            "host": h.host,
            "container": h.container_name,
            "level": h.level,
            "message": h.message[:500],
        }
        for h in result.hits
    ]
    return json.dumps(
        {"total": result.total, "returned": len(hits), "hits": hits},
        default=str,
    )


# ---------------------------------------------------------------------------
# Tool 7: get_action_status
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
    """Return the ASGI app for the MCP server (to be mounted on FastAPI at /mcp)."""
    return mcp.streamable_http_app(path="/mcp")
