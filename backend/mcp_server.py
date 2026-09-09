"""MCP (Model Context Protocol) servers for PulsarCD.

Exposes PulsarCD functionality as MCP tools for AI agents via two servers:
- mcp_read:    read-only tools mounted at /ai/mcp
- mcp_actions: build/deploy tools mounted at /ai/actions/mcp
"""

import asyncio
import json
import re
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import structlog
from mcp.server.fastmcp import FastMCP

logger = structlog.get_logger()

mcp_read = FastMCP(
    name="PulsarCD Read",
    instructions=(
        "PulsarCD is a DevOps monitoring platform. Use these tools to "
        "list stacks (GitHub repos), list containers and hosts, search and "
        "browse logs, get error summaries per service, and check "
        "build/deploy status.\n\n"
        "Typical log workflow:\n"
        "1. Call get_log_metadata() to discover available services, containers, and hosts.\n"
        "2. Call search_logs(github_project='myrepo', last_hours=24) to browse recent logs.\n"
        "3. For error counts per service: search_logs with opensearch_query and size=0 + aggs.\n\n"
        "Release workflow (read side):\n"
        "1. get_deployed_tags() shows what actually runs in production and QA "
        "versus the latest tag built.\n"
        "2. get_untagged_commits(repo_name) shows what is not shipped yet; "
        "get_next_version(repo_name) computes the version a build would take.\n"
        "3. get_pipeline_status(repo_name) and get_transition_config(repo_name) "
        "show where the pipeline stands and which gates are automatic.\n"
        "4. get_action_status / get_action_logs follow a running build, test or deploy.\n"
        "5. get_service_tasks(service) shows the Swarm task states, the "
        "docker service ps view: a deploy whose command returned successfully "
        "but never converged shows up here as failed or restarting tasks.\n"
        "6. get_health_summary() after a deploy tells you whether it broke anything."
    ),
    stateless_http=True,
    json_response=True,
)

mcp_actions = FastMCP(
    name="PulsarCD Actions",
    instructions=(
        "PulsarCD action tools: ship a stack through the pipeline, and operate "
        "what is already deployed.\n\n"
        "PREFER trigger_pipeline: it runs build -> test -> (QA) -> deploy as one "
        "tracked pipeline, tags the commit and honours the per-project gates. "
        "Use build_stack / test_stack / deploy_stack only to re-run a single "
        "stage, to promote a QA build to production, or to roll back "
        "(deploy_stack with the previous tag).\n\n"
        "Operating what already runs: container_action bounces a single "
        "container, update_service_image rolls one service to another tag, "
        "remove_service and remove_stack tear down. Check the outcome with "
        "get_service_tasks on the read server: an action that returned "
        "successfully has not necessarily converged.\n\n"
        "There is no shell tool here on purpose. Every tool is a named "
        "operation that records pipeline state and leaves an audit trail; a "
        "stack deployed by hand has neither.\n\n"
        "repo_name alone identifies a stack, ssh_url is resolved server-side "
        "from the starred repositories and only needs to be passed to override "
        "it.\n\n"
        "Each build/test/deploy tool accepts a version parameter (semver: "
        "MAJOR.MINOR or MAJOR.MINOR.PATCH, e.g. '1.0', '2.1.3'). "
        "Tags may optionally be prefixed with 'v' (e.g. 'v1.2.0'). "
        "All return an action_id, use get_action_status to track progress."
    ),
    stateless_http=True,
    json_response=True,
)


# ---------------------------------------------------------------------------
# Shared validation and repo resolution
# ---------------------------------------------------------------------------
# One convention for every tool: a version is X.Y or X.Y.Z, a tag is the same
# with an optional leading "v", a commit is a 7-40 hex SHA.
_VERSION_RE = re.compile(r"^v?\d+(\.\d+){1,2}$")
_TAG_RE = re.compile(r"^v?\d+(\.\d+){1,2}$")
_SHA_RE = re.compile(r"^[a-fA-F0-9]{7,40}$")

_VALID_TRANSITIONS = ("version_to_build", "build_to_test", "test_to_deploy")
_VALID_GATE_MODES = ("auto", "auto_with_success", "agent", "manual")


async def _resolve_repo(repo_name: str, ssh_url: Optional[str] = None):
    """Return ``(owner, ssh_url)`` for a starred repo.

    ``ssh_url`` is optional on every tool that used to require it. The server
    already knows the URL of every starred repo, and making the caller carry it
    allowed a ``repo_name``/``ssh_url`` pair that nothing cross-checked: the
    clone and the pipeline state could point at two different repositories.

    Raising instead of returning ``owner=None`` is deliberate. The previous code
    derived the owner with a regex over ``ssh_url`` and, when that regex did not
    match, ran ``if branch and owner:`` -- so a malformed URL silently disabled
    branch and commit validation instead of failing.
    """
    from .api import github_service

    repos = await github_service.get_starred_repos()
    wanted = (repo_name or "").strip().lower()
    match = next((r for r in repos if (r.get("name") or "").lower() == wanted), None)
    if not match:
        known = sorted(r.get("name", "") for r in repos)
        return _raise_unknown_stack(repo_name, known)

    owner = match.get("owner")
    known_url = match.get("ssh_url")
    if not owner or not known_url:
        raise ValueError(f"Stack '{repo_name}' has no owner or ssh_url in the GitHub listing")

    if ssh_url and ssh_url.strip().lower() != known_url.strip().lower():
        raise ValueError(
            f"ssh_url '{ssh_url}' does not match the registered URL for "
            f"'{repo_name}' ('{known_url}'). Omit ssh_url to use the registered one."
        )
    return owner, known_url


def _raise_unknown_stack(repo_name: str, known):
    listed = ", ".join(known[:30]) if known else "(none)"
    raise ValueError(f"Unknown stack '{repo_name}'. Known stacks: {listed}")


def _api_error(exc: Exception) -> str:
    """Render an api.py handler failure as the JSON error shape tools return."""
    detail = getattr(exc, "detail", None)
    if detail is not None:
        payload = {"error": str(detail)}
        status = getattr(exc, "status_code", None)
        if status is not None:
            payload["status_code"] = status
        return json.dumps(payload, default=str)
    return json.dumps({"error": f"{type(exc).__name__}: {exc}"})


# ---------------------------------------------------------------------------
# Tool 1: list_stacks
# ---------------------------------------------------------------------------
@mcp_read.tool(description="List available stacks (starred GitHub repositories)")
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
@mcp_actions.tool(
    description=(
        "Build a Docker image from a GitHub repository. Prefer trigger_pipeline "
        "unless you deliberately want to re-run the build stage alone.\n"
        "- ssh_url is resolved from the stack list when omitted.\n"
        "- version defaults to the next patch after the latest tag, so omitting "
        "it never silently overwrites an existing version.\n"
        "Returns an action_id — use get_action_status / get_action_logs to track it."
    )
)
async def build_stack(
    repo_name: str,
    version: Optional[str] = None,
    ssh_url: Optional[str] = None,
    branch: Optional[str] = None,
    commit: Optional[str] = None,
) -> str:
    """Build a stack image. Returns action_id for tracking."""
    from .api import github_service, _background_actions, BackgroundAction, _set_pipeline, _notify_agent_failure

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})

    try:
        owner, ssh_url = await _resolve_repo(repo_name, ssh_url)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    # Validation is unconditional now: the owner comes from the stack list
    # instead of a regex on a caller-supplied ssh_url, so a malformed URL can
    # no longer skip the branch and commit checks entirely.
    if branch:
        is_valid, error_msg = await github_service.validate_branch(owner, repo_name, branch)
        if not is_valid:
            return json.dumps({"error": error_msg})

    if commit:
        if not _SHA_RE.match(commit):
            return json.dumps({"error": f"Invalid commit hash format: '{commit}'"})
        is_valid, error_msg = await github_service.validate_commit(owner, repo_name, commit)
        if not is_valid:
            return json.dumps({"error": error_msg})

    if version is None:
        # No literal default: "1.0" used to be rebuilt -- and overwritten --
        # every time the caller omitted the argument.
        version = await github_service.get_next_version(owner, repo_name)
    elif not _VERSION_RE.match(version):
        return json.dumps(
            {"error": f"Invalid version format: '{version}'. Expected X.Y or X.Y.Z"}
        )
    version = version.lstrip("v")

    deployer, host_name = _get_deployer_and_host()

    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "build", repo_name)
    _background_actions[action_id] = action

    _set_pipeline(repo_name, "build", "running", version, build_id=action_id)

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
            status = "success" if result.get("success") else "failed"
            _set_pipeline(repo_name, "build", status, version, build_id=action_id, log_lines=action.output_lines)
            if not result.get("success"):
                await _notify_agent_failure("build", repo_name, version, result.get("output", ""))
        except Exception as e:
            action.status = "failed"
            action.result = {
                "success": False,
                "output": str(e),
                "action": "build",
                "repo": repo_name,
            }
            action.append_output(str(e))
            _set_pipeline(repo_name, "build", "failed", version, build_id=action_id, log_lines=action.output_lines)
            await _notify_agent_failure("build", repo_name, version, str(e))

    action.task = asyncio.create_task(_run_build())
    return json.dumps({
        "action_id": action_id,
        "action_type": "build",
        "repo": repo_name,
        "version": version,
    })


# ---------------------------------------------------------------------------
# Tool 3: deploy_stack
# ---------------------------------------------------------------------------
@mcp_actions.tool(
    description=(
        "Deploy a stack to Docker Swarm. Prefer trigger_pipeline unless you are "
        "re-deploying an already built version (that is also how you roll back: "
        "deploy_stack(repo_name, tag=<previous tag from list_tags>)).\n"
        "- ssh_url is resolved from the stack list when omitted.\n"
        "- qa=true deploys to the isolated QA environment instead of production "
        "(stack prefixed 'qa-', domains prefixed 'qa.'); production then waits "
        "for a manual approval.\n"
        "Returns an action_id — use get_action_status / get_action_logs to track it."
    )
)
async def deploy_stack(
    repo_name: str,
    version: Optional[str] = None,
    tag: Optional[str] = None,
    qa: bool = False,
    ssh_url: Optional[str] = None,
) -> str:
    """Deploy a stack. Returns action_id for tracking."""
    from .api import github_service, _background_actions, BackgroundAction, _set_pipeline, _notify_agent_failure, pipeline_state

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})

    if tag and not _TAG_RE.match(tag):
        return json.dumps({"error": f"Invalid tag format: '{tag}'. Expected vX.X.X"})
    if version and not _VERSION_RE.match(version):
        return json.dumps(
            {"error": f"Invalid version format: '{version}'. Expected X.Y or X.Y.Z"}
        )

    try:
        owner, ssh_url = await _resolve_repo(repo_name, ssh_url)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    if not tag and not version:
        # No literal default here either: falling back to "1.0" would quietly
        # roll production back to 1.0 whenever the caller forgot the argument.
        # Deploy what the pipeline last built, else the latest tag.
        version = pipeline_state.get_legacy(repo_name).get("version") or ""
        if not version:
            latest = await github_service.get_latest_tag(owner, repo_name)
            version = (latest or "").lstrip("v")
        if not version:
            return json.dumps({
                "error": f"No version to deploy for '{repo_name}': it has never been "
                         f"built and has no tag. Pass an explicit version or tag, or "
                         f"run trigger_pipeline first."
            })
    version = (version or "").lstrip("v")

    deployer, host_name = _get_deployer_and_host()

    action_type = "qa-deploy" if qa else "deploy"
    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, action_type, repo_name)
    _background_actions[action_id] = action

    deploy_version = tag.lstrip('v') if tag else version
    prev_build_id = pipeline_state.get_legacy(repo_name).get("build_action_id") if not tag else None

    # Mirrors POST /api/stacks/deploy: a QA deploy occupies the "qa" stage and
    # must not claim the deploy slot, otherwise the UI shows production as
    # updated while only the qa- stack moved.
    pipeline_stage = "qa" if qa else "deploy"
    if qa:
        _set_pipeline(repo_name, pipeline_stage, "running", deploy_version,
                      build_id=prev_build_id, qa_id=action_id)
    else:
        _set_pipeline(repo_name, pipeline_stage, "running", deploy_version,
                      build_id=prev_build_id, deploy_id=action_id)

    async def _run_deploy():
        try:
            result = await deployer.deploy(
                repo_name,
                ssh_url,
                version,
                tag=tag,
                qa=qa,
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
                    _set_pipeline(repo_name, "qa", "success", deploy_version,
                                  qa_id=action_id, log_lines=action.output_lines)
                    pipeline_state.record_gate(
                        repo_name, "qa_to_deploy", False,
                        "Manual transition — waiting for user approval after QA",
                        version=deploy_version,
                    )
                    _set_pipeline(repo_name, "qa", "gate_rejected", deploy_version,
                                  qa_id=action_id, log_lines=action.output_lines)
                else:
                    _set_pipeline(repo_name, "done", "success", deploy_version,
                                  deploy_id=action_id, log_lines=action.output_lines)
            else:
                _set_pipeline(repo_name, pipeline_stage, "failed", deploy_version,
                              deploy_id=None if qa else action_id,
                              qa_id=action_id if qa else None,
                              log_lines=action.output_lines)
                await _notify_agent_failure(pipeline_stage, repo_name, deploy_version, result.get("output", ""))
        except Exception as e:
            action.status = "failed"
            action.result = {
                "success": False,
                "output": str(e),
                "action": action_type,
                "repo": repo_name,
            }
            action.append_output(str(e))
            _set_pipeline(repo_name, pipeline_stage, "failed", deploy_version,
                          deploy_id=None if qa else action_id,
                          qa_id=action_id if qa else None,
                          log_lines=action.output_lines)
            await _notify_agent_failure(pipeline_stage, repo_name, deploy_version, str(e))

    action.task = asyncio.create_task(_run_deploy())
    return json.dumps({
        "action_id": action_id,
        "action_type": action_type,
        "repo": repo_name,
        "version": deploy_version,
    })


# ---------------------------------------------------------------------------
# Tool 3b: test_stack
# ---------------------------------------------------------------------------
@mcp_actions.tool(
    description=(
        "Run tests for a stack (executes the 'test' build target from "
        "docker-compose.swarm.yml). "
        "Optionally specify a version number to test. "
        "Returns an action_id — use get_action_status to track progress."
    )
)
async def test_stack(
    repo_name: str,
    version: Optional[str] = None,
    branch: Optional[str] = None,
    tag: Optional[str] = None,
    commit: Optional[str] = None,
    ssh_url: Optional[str] = None,
) -> str:
    """Run tests for a stack. Returns action_id for tracking."""
    from .api import github_service, _background_actions, BackgroundAction, _set_pipeline, _notify_agent_failure, pipeline_state

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})

    try:
        owner, ssh_url = await _resolve_repo(repo_name, ssh_url)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    # Unconditional, unlike the previous `if <x> and owner:` guards: a ssh_url
    # whose owner regex did not match used to disable every check below.
    if branch:
        is_valid, error_msg = await github_service.validate_branch(owner, repo_name, branch)
        if not is_valid:
            return json.dumps({"error": error_msg})

    if tag and not _TAG_RE.match(tag):
        return json.dumps({"error": f"Invalid tag format: '{tag}'. Expected vX.X.X"})

    if commit:
        if not _SHA_RE.match(commit):
            return json.dumps({"error": f"Invalid commit hash format: '{commit}'"})
        is_valid, error_msg = await github_service.validate_commit(owner, repo_name, commit)
        if not is_valid:
            return json.dumps({"error": error_msg})

    deployer, host_name = _get_deployer_and_host()

    action_id = str(uuid.uuid4())[:8]
    action = BackgroundAction(action_id, "test", repo_name)
    _background_actions[action_id] = action

    resolved_version = tag.lstrip('v') if tag else (version or pipeline_state.get_legacy(repo_name).get("version", ""))
    _set_pipeline(repo_name, "test", "running", resolved_version, test_id=action_id)

    async def _run_test():
        try:
            result = await deployer.test(
                repo_name,
                ssh_url,
                branch=branch,
                tag=tag,
                commit=commit,
                output_callback=action.append_output,
                cancel_event=action.cancel_event,
            )
            result["host"] = host_name
            action.result = result
            action.status = "completed" if result.get("success") else "failed"
            if action.cancel_event.is_set():
                action.status = "cancelled"
            status = "success" if result.get("success") else "failed"
            _set_pipeline(repo_name, "test", status, resolved_version, test_id=action_id, log_lines=action.output_lines)
            if not result.get("success"):
                await _notify_agent_failure("test", repo_name, resolved_version, result.get("output", ""))
        except Exception as e:
            action.status = "failed"
            action.result = {
                "success": False,
                "output": str(e),
                "action": "test",
                "repo": repo_name,
            }
            action.append_output(str(e))
            _set_pipeline(repo_name, "test", "failed", resolved_version, test_id=action_id, log_lines=action.output_lines)
            await _notify_agent_failure("test", repo_name, resolved_version, str(e))

    action.task = asyncio.create_task(_run_test())
    return json.dumps({
        "action_id": action_id,
        "action_type": "test",
        "repo": repo_name,
        "version": resolved_version,
    })


# ---------------------------------------------------------------------------
# Tool 4: list_containers
# ---------------------------------------------------------------------------
@mcp_read.tool(description="List all Docker containers and their states across all hosts")
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
@mcp_read.tool(description="List all monitored hosts/computers including discovered Swarm nodes")
async def list_computers() -> str:
    """List all hosts (configured + discovered swarm nodes).

    hostname and mode are deliberately NOT returned. This server is mounted with
    require_admin=False, so any viewer JWT reaches it, while the same
    information is admin-only on GET /api/config -- returning it here would
    reopen that disclosure through a side door. Every read tool targets a host by
    `name`, which is what this returns.
    """
    from .api import settings, collector

    configured_names = {h.name for h in settings.hosts}
    result = [
        {
            "name": h.name,
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
                    "swarm_manager": False,
                    "is_swarm_node": True,
                }
            )
    return json.dumps({"hosts": result, "count": len(result)})


# ---------------------------------------------------------------------------
# Tool 6: get_log_metadata
# ---------------------------------------------------------------------------
@mcp_read.tool(
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
- query            Free-text search on the message field only. Supported:
                   plain keywords, "quoted phrases", AND / OR / NOT, parentheses
                   and a trailing wildcard (e.g. "timeout AND retry", "error*").
                   NOT supported (silently matches nothing): field:value syntax,
                   leading wildcards (*term), regular expressions (/re/) and
                   fuzzy matching (term~2). Filter by field with hosts,
                   containers, compose_services, levels instead.
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
The raw OpenSearch response is returned as-is, with these limits enforced:
- "size" is capped at 500 and "from" at 10000 (deep pagination is refused);
- the request times out after 30s;
- any body containing script, script_fields, script_score, scripted_metric or
  runtime_mappings -- at any depth -- is refused with
  {"error": "Query construct not allowed: script"}. This also rules out the
  bucket_script / bucket_selector pipeline aggregations: compute ratios from
  the returned bucket counts instead of asking OpenSearch to evaluate them.

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


@mcp_read.tool(description=_SEARCH_DOCS)
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
        # Clamped, not validated: the caller is an LLM and an out-of-range
        # offset must degrade to the last reachable page, not raise.
        **{"from": min(max(from_offset, 0), 10000)},
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
_ACTION_STATUS_DOCS = (
    "Check the status of a background build / test / deploy action by its "
    "action_id. Only the last 5 output lines are included — call "
    "get_action_logs to actually read a failure. Falls back to the persisted "
    "pipeline state when the action is no longer in memory."
)


async def get_action_status(action_id: str) -> str:
    """Get status of a build/deploy action."""
    from .api import _background_actions, pipeline_state

    action = _background_actions.get(action_id)
    if action:
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

    # Same fallback as GET /api/stacks/actions/{id}/status: after a restart the
    # in-memory action is gone but the stage that ran it is still on disk.
    if pipeline_state:
        for repo_name, entry in pipeline_state.items():
            for stage_name, stage in entry.stages.items():
                if stage.action_id == action_id:
                    return json.dumps({
                        "action_id": action_id,
                        "action_type": stage_name,
                        "repo": repo_name,
                        "status": stage.status if stage.status and stage.status != "idle" else "unknown",
                        "restored": True,
                    }, default=str)

    return json.dumps({"error": f"Action '{action_id}' not found"})


# Registered on both servers on purpose: a client that only mounts the actions
# server (the tools that hand out action_ids) would otherwise be unable to
# follow its own jobs. Both are read-only, so this grants no extra privilege.
mcp_read.tool(description=_ACTION_STATUS_DOCS)(get_action_status)
mcp_actions.tool(description=_ACTION_STATUS_DOCS)(get_action_status)


# ---------------------------------------------------------------------------
# Tool 10: get_action_logs  (registered on both servers)
# ---------------------------------------------------------------------------
_ACTION_LOGS_DOCS = """
Read the captured output of a background build / test / deploy action.

get_action_status only carries the last 5 lines; use this tool to actually
diagnose a failure. Falls back to the log lines persisted in the pipeline state
when the action is no longer in memory (e.g. after a server restart).

Parameters:
- action_id  Id returned by build_stack / test_stack / deploy_stack.
- offset     First line to return (default 0). Poll with the previous
             total_lines to follow a running action incrementally.
- limit      Max lines to return (1-2000, default 200). When the requested
             slice is longer than limit the TAIL is returned, because a build
             failure is normally at the end; `offset` in the response says
             where the returned window actually starts.

Returns: action_id, status, lines, offset, returned, total_lines, truncated.
"""


async def get_action_logs(action_id: str, offset: int = 0, limit: int = 200) -> str:
    """Read the output of a background action."""
    from .api import _background_actions, pipeline_state

    offset = max(0, offset)
    limit = max(1, min(limit, 2000))

    lines = None
    status = None
    restored = False

    action = _background_actions.get(action_id)
    if action:
        lines = action.output_lines
        status = action.status
    elif pipeline_state:
        for repo_name, entry in pipeline_state.items():
            for stage_name, stage in entry.stages.items():
                if stage.action_id == action_id and stage.last_log:
                    lines = stage.last_log
                    status = stage.status or "completed"
                    restored = True
                    break
            if lines is not None:
                break

    if lines is None:
        return json.dumps({"error": f"Action '{action_id}' not found"})

    total = len(lines)
    window = lines[offset:]
    start = offset
    truncated = len(window) > limit
    if truncated:
        window = window[-limit:]
        start = total - len(window)

    payload = {
        "action_id": action_id,
        "status": status,
        "lines": window,
        "offset": start,
        "returned": len(window),
        "total_lines": total,
        "truncated": truncated,
    }
    if restored:
        payload["restored"] = True
    return json.dumps(payload, default=str)


mcp_read.tool(description=_ACTION_LOGS_DOCS)(get_action_logs)
mcp_actions.tool(description=_ACTION_LOGS_DOCS)(get_action_logs)


# ---------------------------------------------------------------------------
# Tool 11: list_actions
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "List recent background build / test / deploy actions, newest first. "
        "Use it to find an action_id you do not already hold — for instance the "
        "stages started by trigger_pipeline. Optional filters: status "
        "(running / completed / failed / cancelled) and repo_name."
    )
)
async def list_actions(
    status: Optional[str] = None,
    repo_name: Optional[str] = None,
    limit: int = 20,
) -> str:
    """List background actions currently held in memory."""
    from .api import _background_actions

    actions = list(_background_actions.values())
    if status:
        wanted = status.strip().lower()
        actions = [a for a in actions if (a.status or "").lower() == wanted]
    if repo_name:
        wanted_repo = repo_name.strip().lower()
        actions = [a for a in actions if (a.repo_name or "").lower() == wanted_repo]

    total = len(actions)
    actions.sort(key=lambda a: a.started_at, reverse=True)
    limit = max(1, min(limit, 100))

    result = [
        {
            "action_id": a.id,
            "action_type": a.action_type,
            "repo": a.repo_name,
            "status": a.status,
            "started_at": a.started_at.isoformat(),
            "elapsed_seconds": (datetime.utcnow() - a.started_at).total_seconds(),
            "output_lines": len(a.output_lines),
        }
        for a in actions[:limit]
    ]
    return json.dumps({"actions": result, "count": len(result), "total": total}, default=str)


# ---------------------------------------------------------------------------
# Tool 12: get_deployed_tags
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "Return, for every stack, the image tag actually running in production "
        "('tags'), the tag running in the QA environment ('qa_tags'), and the "
        "latest tag built on GitHub ('latest_built'). This is how you check "
        "whether a deploy really landed, and how you pick a rollback target: a "
        "stack whose deployed tag is behind latest_built has not shipped."
    )
)
async def get_deployed_tags() -> str:
    """Deployed vs built versions across all stacks."""
    from .api import get_stacks_deployed_tags

    try:
        data = await get_stacks_deployed_tags()
    except Exception as exc:
        return _api_error(exc)
    return json.dumps(data, default=str)


# ---------------------------------------------------------------------------
# Tool 13: list_tags
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "List the git tags of a stack, most recent first. Each tag is a version "
        "that was built. Use it to pick a rollback target for deploy_stack(tag=...)."
    )
)
async def list_tags(repo_name: str, limit: int = 10) -> str:
    """List a repository's tags."""
    from .api import github_service

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})
    try:
        owner, _ = await _resolve_repo(repo_name)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    limit = max(1, min(limit, 100))
    data = await github_service.get_repo_tags(owner, repo_name, limit)
    return json.dumps({"repo": repo_name, "owner": owner, **data}, default=str)


# ---------------------------------------------------------------------------
# Tool 14: get_next_version
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "Compute the version the next build would take: the latest tag with its "
        "patch incremented, or 1.0.0 when the repo has no tag yet. Call this "
        "before build_stack when you want to state the version explicitly."
    )
)
async def get_next_version(repo_name: str) -> str:
    """Next patch version for a stack."""
    from .api import github_service

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})
    try:
        owner, _ = await _resolve_repo(repo_name)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    latest = await github_service.get_latest_tag(owner, repo_name)
    next_version = await github_service.get_next_version(owner, repo_name)
    return json.dumps({
        "repo": repo_name,
        "latest_tag": latest,
        "next_version": next_version,
        "next_tag": f"v{next_version}",
    })


# ---------------------------------------------------------------------------
# Tool 15: get_untagged_commits
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "List the most recent commits that carry no tag, i.e. the work that has "
        "not been built or shipped yet. Stops at the first tagged commit. "
        "Returns untagged_commits and the latest_tag they sit on top of."
    )
)
async def get_untagged_commits(
    repo_name: str,
    limit: int = 10,
    branch: Optional[str] = None,
) -> str:
    """Commits not yet tagged (= not yet built)."""
    from .api import github_service

    if not github_service or not github_service.is_configured():
        return json.dumps({"error": "GitHub integration not configured"})
    try:
        owner, _ = await _resolve_repo(repo_name)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    limit = max(1, min(limit, 50))
    data = await github_service.get_untagged_commits(owner, repo_name, limit, branch=branch)
    return json.dumps({"repo": repo_name, "branch": branch, **data}, default=str)


# ---------------------------------------------------------------------------
# Tool 16: get_pipeline_status
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "Return the pipeline state — current stage, status, versions and the "
        "action_id of each stage (build / test / qa / deploy) — for one stack "
        "when repo_name is given, or for every stack otherwise. This is how you "
        "follow a run started by trigger_pipeline: it hands out no action_id of "
        "its own, the per-stage ids appear here as the run progresses."
    )
)
async def get_pipeline_status(repo_name: Optional[str] = None) -> str:
    """Pipeline state for one or every stack."""
    from .api import pipeline_state

    if repo_name:
        state = pipeline_state.get_legacy(repo_name)
        if not state:
            return json.dumps({"repo": repo_name, "pipeline": None,
                               "message": "No pipeline has run for this stack yet"})
        return json.dumps({"repo": repo_name, "pipeline": state}, default=str)

    return json.dumps({"pipelines": pipeline_state.get_all_legacy()}, default=str)


# ---------------------------------------------------------------------------
# Tool 17: get_transition_config
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "Read the gate configuration of a stack: which pipeline transitions are "
        "automatic and which need approval.\n"
        "Transitions: version_to_build, build_to_test, test_to_deploy.\n"
        "Modes: auto (always proceed), auto_with_success (proceed only if the "
        "previous stage succeeded), agent (the LLM agent decides), manual (a "
        "human must approve).\n"
        "test_to_deploy also carries qa_enabled: when true the pipeline deploys "
        "to the isolated QA environment first and then always waits for a manual "
        "approval before production.\n"
        "Omit `transition` to get all three, each with its last recorded gate "
        "decision. Check this before assuming a pipeline will run to completion "
        "on its own."
    )
)
async def get_transition_config(
    repo_name: str,
    transition: Optional[str] = None,
) -> str:
    """Read per-project pipeline gate configuration."""
    from .api import pipeline_state

    if transition and transition not in _VALID_TRANSITIONS:
        return json.dumps({
            "error": f"Invalid transition '{transition}'. "
                     f"Valid: {', '.join(_VALID_TRANSITIONS)}"
        })

    wanted = [transition] if transition else list(_VALID_TRANSITIONS)
    entry = pipeline_state.get(repo_name)

    out = {}
    for name in wanted:
        last_decision = None
        if entry:
            for gate in reversed(entry.gates):
                if gate.transition == name:
                    last_decision = gate.to_dict()
                    break
        out[name] = {
            "config": pipeline_state.get_transition_config(repo_name, name),
            "last_decision": last_decision,
        }

    return json.dumps({"repo": repo_name, "transitions": out}, default=str)


# ---------------------------------------------------------------------------
# Tool 18: get_health_summary
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "Post-deploy health check in one call: container and host counts, "
        "24h error / warning / HTTP 4xx / 5xx totals, average CPU, memory and "
        "GPU load, the recurring error patterns the detector last notified, and "
        "recent system-level errors. Call it after a deploy to see whether "
        "anything broke, then drill into a specific service with search_logs."
    )
)
async def get_health_summary(limit: int = 5) -> str:
    """Aggregate dashboard stats, recurring errors and system errors."""
    from .api import (
        get_dashboard_stats as _api_dashboard_stats,
        get_recurring_errors as _api_recurring_errors,
        get_system_errors as _api_system_errors,
    )

    limit = max(1, min(limit, 50))
    payload = {}

    try:
        stats = await _api_dashboard_stats()
        payload["stats"] = stats.model_dump() if hasattr(stats, "model_dump") else stats
    except Exception as exc:
        payload["stats_error"] = f"{type(exc).__name__}: {exc}"

    try:
        payload["recurring_errors"] = await _api_recurring_errors(limit)
    except Exception as exc:
        payload["recurring_errors_error"] = f"{type(exc).__name__}: {exc}"

    try:
        payload["system_errors"] = await _api_system_errors(limit)
    except Exception as exc:
        payload["system_errors_error"] = f"{type(exc).__name__}: {exc}"

    return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# Tool 19: trigger_pipeline  (actions)
# ---------------------------------------------------------------------------
_TRIGGER_PIPELINE_DOCS = """
Run the full PulsarCD pipeline for a stack: build -> test -> (QA) -> deploy.

This is the preferred way to ship. It is the only path that tags the commit,
records the version in the pipeline state, honours the per-project gates
(see get_transition_config) and leaves an auditable trail. Chaining
build_stack / test_stack / deploy_stack by hand bypasses the gates, and
update_service_image changes what runs without recording a version at all.

Provide exactly one of:
- tag     An existing tag to build and deploy (format vX.Y.Z or vX.Y).
- commit  A commit SHA. The next patch version is computed automatically, the
          commit is tagged with it, and the pipeline runs from that tag.

Returns {status, repo, tag, version} — no action_id: the pipeline creates one
action per stage as it advances. Follow it with get_pipeline_status(repo_name),
which reports the current stage and its action_id, then get_action_logs on that
id to read the output.

Refuses with a 409 error when a pipeline is already running for the stack.
"""


@mcp_actions.tool(description=_TRIGGER_PIPELINE_DOCS)
async def trigger_pipeline(
    repo_name: str,
    tag: Optional[str] = None,
    commit: Optional[str] = None,
    ssh_url: Optional[str] = None,
) -> str:
    """Trigger the full build -> test -> deploy pipeline."""
    from .api import trigger_pipeline_endpoint

    if bool(tag) == bool(commit):
        return json.dumps(
            {"error": "Provide exactly one of 'tag' or 'commit'"}
        )
    if tag and not _TAG_RE.match(tag):
        return json.dumps({"error": f"Invalid tag format: '{tag}'. Expected vX.Y.Z"})
    if commit and not _SHA_RE.match(commit):
        return json.dumps({"error": f"Invalid commit hash format: '{commit}'"})

    try:
        _owner, ssh_url = await _resolve_repo(repo_name, ssh_url)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    try:
        data = await trigger_pipeline_endpoint(
            repo_name=repo_name, ssh_url=ssh_url, tag=tag, commit=commit
        )
    except Exception as exc:
        return _api_error(exc)

    return json.dumps(
        {**data, "follow_with": f"get_pipeline_status(repo_name='{repo_name}')"},
        default=str,
    )


# ---------------------------------------------------------------------------
# Tool 21: set_transition_config  (actions)
# ---------------------------------------------------------------------------
@mcp_actions.tool(
    description=(
        "Set the gate mode of one pipeline transition.\n"
        "transition: version_to_build | build_to_test | test_to_deploy\n"
        "mode: auto | auto_with_success | agent | manual\n"
        "qa_enabled (test_to_deploy only): run an isolated QA deploy before "
        "production. Omit it to keep the current value — passing false disables "
        "QA. Read the current setting with get_transition_config first."
    )
)
async def set_transition_config(
    repo_name: str,
    transition: str,
    mode: str,
    qa_enabled: Optional[bool] = None,
) -> str:
    """Configure a pipeline gate."""
    from .api import pipeline_state

    if transition not in _VALID_TRANSITIONS:
        return json.dumps({
            "error": f"Invalid transition '{transition}'. "
                     f"Valid: {', '.join(_VALID_TRANSITIONS)}"
        })
    if mode not in _VALID_GATE_MODES:
        return json.dumps({
            "error": f"Invalid mode '{mode}'. Valid: {', '.join(_VALID_GATE_MODES)}"
        })

    config = {"mode": mode}
    if transition == "test_to_deploy":
        if qa_enabled is None:
            # Unlike the REST route, an omitted qa_enabled preserves the current
            # setting: silently turning QA off while only changing the mode
            # would push a version straight to production.
            current = pipeline_state.get_transition_config(repo_name, transition)
            qa_enabled = bool(current.get("qa_enabled", False))
        config["qa_enabled"] = bool(qa_enabled)

    pipeline_state.set_transition_config(repo_name, transition, config)
    logger.info("MCP transition config updated", repo=repo_name,
                transition=transition, mode=mode, qa_enabled=config.get("qa_enabled"))
    return json.dumps({
        "saved": True,
        "repo": repo_name,
        "transition": transition,
        "config": pipeline_state.get_transition_config(repo_name, transition),
    }, default=str)


# ---------------------------------------------------------------------------
# Tool 22: cancel_action  (actions)
# ---------------------------------------------------------------------------
@mcp_actions.tool(
    description=(
        "Cancel a running background build / test / deploy action by its "
        "action_id. A deploy that has already reached the Swarm may have "
        "applied part of its changes — check get_deployed_tags afterwards."
    )
)
async def cancel_action(action_id: str) -> str:
    """Cancel a running background action."""
    from .api import cancel_action as _api_cancel_action

    try:
        data = await _api_cancel_action(action_id)
    except Exception as exc:
        return _api_error(exc)
    return json.dumps({"action_id": action_id, **data}, default=str)


# ---------------------------------------------------------------------------
# Tool 23/24: get_stack_env / set_stack_env  (actions)
# ---------------------------------------------------------------------------
# Both live on the admin-only actions server, not the read server: a stack .env
# holds secrets, which is why GET /api/stacks/{repo}/env is admin-only too
# (_ADMIN_ONLY_GET_RE in api.py). Exposing them on the read server would hand
# every viewer JWT the deployment secrets.
#
# Neither tool ever returns a value. The MCP client is an LLM: whatever these
# tools return lands in a context window, gets summarised into a task, quoted
# in a chat answer, and -- because agent conversations are logged -- indexed in
# the very log store a viewer account can search. Reading the file whole was
# the shortest exfiltration path in the product, and no caller needed it: the
# only legitimate uses are "does KEY exist" and "set KEY to this", which the
# key listing and the patch below cover without a value ever coming back.
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _split_env_line(line: str):
    """Return ``(prefix, key, value)`` for an assignment line, else ``None``.

    ``prefix`` is the literal ``export `` when the line carries one, so a
    rewritten line keeps the shape the operator wrote.
    """
    body = line.strip()
    if not body or body.startswith("#"):
        return None
    prefix = ""
    if body.startswith("export "):
        prefix, body = "export ", body[len("export "):].lstrip()
    key, sep, value = body.partition("=")
    key = key.strip()
    if not sep or not _ENV_KEY_RE.match(key):
        return None
    return prefix, key, value


def _patch_env(content: str, updates: Dict[str, str], unset: List[str]):
    """Apply a key -> value patch to a .env file, preserving everything else.

    Comments, blank lines, ordering and unrelated variables survive untouched:
    only the lines whose key was named are rewritten. Returns
    ``(new_content, updated, added, removed)``.
    """
    drop = set(unset)
    out, updated, removed, seen = [], [], [], set()

    for line in (content or "").splitlines():
        parsed = _split_env_line(line)
        if parsed is None:
            out.append(line)
            continue
        prefix, key, _value = parsed
        if key in drop:
            removed.append(key)
            continue
        if key in updates:
            out.append(f"{prefix}{key}={updates[key]}")
            updated.append(key)
            seen.add(key)
            continue
        out.append(line)

    # A key the file did not define is appended rather than silently dropped.
    added = [k for k in updates if k not in seen]
    if added:
        if out and out[-1].strip():
            out.append("")
        out.extend(f"{k}={updates[k]}" for k in added)

    new_content = "\n".join(out)
    if new_content and not new_content.endswith("\n"):
        new_content += "\n"
    return new_content, sorted(set(updated)), added, sorted(set(removed))


@mcp_actions.tool(
    description=(
        "List the variables of the .env file a stack is deployed with.\n"
        "Values are never returned: each entry gives the key and the length of "
        "its value, which is what you need to check that a variable exists and "
        "that a set_stack_env write landed. A stack .env holds deployment "
        "secrets and this output ends up in a context window, so there is no "
        "option to reveal them -- read them in the web UI if you must.\n"
        "Change a variable with set_stack_env."
    )
)
async def get_stack_env(repo_name: str) -> str:
    """List a stack's .env variables, values redacted."""
    from .api import settings
    from .github_service import StackDeployer

    try:
        await _resolve_repo(repo_name)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    deployer = StackDeployer(settings.github, None)
    success, content = await deployer.get_env_file(repo_name)
    if not success:
        return json.dumps({"error": content})

    variables = []
    for line in (content or "").splitlines():
        parsed = _split_env_line(line)
        if parsed is None:
            continue
        _prefix, key, value = parsed
        variables.append({"key": key, "value": "<redacted>", "chars": len(value.strip())})

    return json.dumps({
        "repo": repo_name,
        "variables": variables,
        "count": len(variables),
        "note": "Values are redacted: use set_stack_env to change one.",
    })


@mcp_actions.tool(
    description=(
        "Patch the .env file a stack is deployed with. Only the keys you name "
        "are touched -- comments, ordering and every other variable are left "
        "exactly as they are.\n"
        "- updates: {\"KEY\": \"value\"} rewrites the key in place, or appends "
        "it when the file does not define it yet.\n"
        "- unset: [\"KEY\"] deletes those assignments.\n"
        "Values must be single-line and are written verbatim (quote them "
        "yourself if the value needs quotes). Takes effect on the next deploy: "
        "the running stack is not restarted."
    )
)
async def set_stack_env(
    repo_name: str,
    updates: Optional[Dict[str, str]] = None,
    unset: Optional[List[str]] = None,
) -> str:
    """Patch a stack's .env file, key by key."""
    from .api import settings
    from .github_service import StackDeployer

    updates = dict(updates or {})
    unset = list(unset or [])
    if not updates and not unset:
        return json.dumps({"error": "Nothing to do: pass 'updates' and/or 'unset'"})

    for key, value in updates.items():
        if not _ENV_KEY_RE.match(key or ""):
            return json.dumps({"error": f"Invalid variable name: '{key}'"})
        # A newline in a value would end the assignment and let the rest of the
        # string define further variables -- the .env equivalent of an
        # injection, reachable from any text the model was fed.
        if "\n" in str(value) or "\r" in str(value):
            return json.dumps({"error": f"Value for '{key}' must be single-line"})
        updates[key] = str(value)
    for key in unset:
        if not _ENV_KEY_RE.match(key or ""):
            return json.dumps({"error": f"Invalid variable name: '{key}'"})

    both = sorted(set(updates) & set(unset))
    if both:
        return json.dumps({"error": f"Keys in both updates and unset: {', '.join(both)}"})

    try:
        await _resolve_repo(repo_name)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    deployer = StackDeployer(settings.github, None)
    success, content = await deployer.get_env_file(repo_name)
    if not success:
        return json.dumps({"error": content})

    new_content, updated, added, removed = _patch_env(content, updates, unset)
    missing = [k for k in unset if k not in removed]

    if not updated and not added and not removed:
        return json.dumps({
            "success": True,
            "repo": repo_name,
            "unchanged": True,
            "message": (f"No variable matched: {', '.join(missing)}" if missing
                        else "Nothing to change"),
        })

    success, message = await deployer.save_env_file(repo_name, new_content)
    if not success:
        return json.dumps({"error": message})

    # Keys only: the values are the secrets this tool exists to keep out of the
    # log store.
    logger.info("MCP stack env patched", repo=repo_name,
                updated=updated, added=added, removed=removed)
    return json.dumps({
        "success": True,
        "repo": repo_name,
        "updated": updated,
        "added": added,
        "removed": removed,
        "not_found": missing,
        "message": message,
    })


# ---------------------------------------------------------------------------
# Tool 25: get_service_tasks  (read)
# ---------------------------------------------------------------------------
@mcp_read.tool(
    description=(
        "Task states of a Docker Swarm service, i.e. `docker service ps "
        "<service> --no-trunc`: running, pending, failed and shutdown tasks "
        "with their error messages.\n"
        "This is how you check that a deploy actually converged. "
        "get_action_status only says the deploy command returned; a service "
        "that cannot pull its image or crashes on boot shows up here as "
        "repeated failed tasks while the deploy still reads as 'completed'.\n"
        "service_name is the Swarm service name, normally '<stack>_<service>' "
        "-- list_containers shows the names in use."
    )
)
async def get_service_tasks(service_name: str, host: Optional[str] = None) -> str:
    """Read the Swarm task states of one service."""
    from .api import get_service_tasks as _api_get_service_tasks

    try:
        data = await _api_get_service_tasks(service_name=service_name, host=host)
    except Exception as exc:
        return _api_error(exc)
    return json.dumps(data, default=str)


# ---------------------------------------------------------------------------
# Tools 26-29: runtime operations  (actions)
# ---------------------------------------------------------------------------
# These replace the shell tool this server used to expose. A named operation
# can be authorised, logged and reasoned about; run_command could not -- it was
# arbitrary code execution on the Swarm manager, which made every other control
# on this server (the gates, the pipeline state, the agent denylist) advisory:
# one command bypassed all of them and left no version record behind.
_CONTAINER_ACTIONS = ("start", "stop", "restart", "pause", "unpause", "remove")


@mcp_actions.tool(
    description=(
        "Act on ONE container: start | stop | restart | pause | unpause | "
        "remove.\n"
        "- host and container_id come from list_containers.\n"
        "This bounces a single container. On a Swarm service the scheduler "
        "recreates the task from the same image, so use it to restart "
        "something wedged -- not to change what is deployed, which is "
        "deploy_stack (whole stack) or update_service_image (one service)."
    )
)
async def container_action(host: str, container_id: str, action: str) -> str:
    """Start / stop / restart / pause / unpause / remove a container."""
    from .api import execute_container_action
    from .models import ActionRequest, ContainerAction

    verb = (action or "").strip().lower()
    if verb not in _CONTAINER_ACTIONS:
        return json.dumps({
            "error": f"Invalid action '{action}'. Expected one of: {', '.join(_CONTAINER_ACTIONS)}"
        })

    try:
        result = await execute_container_action(
            ActionRequest(host=host, container_id=container_id, action=ContainerAction(verb))
        )
    except Exception as exc:
        return _api_error(exc)

    payload = result.model_dump() if hasattr(result, "model_dump") else result
    logger.info("MCP container action", host=host, container=container_id[:12],
                action=verb,
                success=payload.get("success") if isinstance(payload, dict) else None)
    return json.dumps(payload, default=str)


@mcp_actions.tool(
    description=(
        "Roll ONE Swarm service to another image tag (`docker service update "
        "--image`), without redeploying the whole stack.\n"
        "- service_name is normally '<stack>_<service>'; tag is the image tag.\n"
        "This does NOT go through the pipeline: no version is recorded, so "
        "get_deployed_tags keeps reporting the last deployed version until the "
        "next real deploy. Use it to unstick one service; ship a version with "
        "trigger_pipeline or deploy_stack."
    )
)
async def update_service_image(service_name: str, tag: str, host: Optional[str] = None) -> str:
    """Update the image tag of one Swarm service."""
    from .api import update_service_image as _api_update_service_image

    try:
        data = await _api_update_service_image(service_name=service_name, tag=tag, host=host)
    except Exception as exc:
        return _api_error(exc)
    logger.info("MCP service image updated", service=service_name, tag=tag, host=host)
    return json.dumps(data, default=str)


@mcp_actions.tool(
    description=(
        "Remove ONE Swarm service (`docker service rm`). The rest of its stack "
        "keeps running. Bring it back with deploy_stack, which recreates the "
        "whole stack."
    )
)
async def remove_service(service_name: str, host: Optional[str] = None) -> str:
    """Remove a Swarm service."""
    from .api import remove_service as _api_remove_service

    try:
        data = await _api_remove_service(service_name=service_name, host=host)
    except Exception as exc:
        return _api_error(exc)
    logger.info("MCP service removed", service=service_name, host=host)
    return json.dumps(data, default=str)


@mcp_actions.tool(
    description=(
        "Remove a whole deployed stack from the Swarm (`docker stack rm`): "
        "every service of the stack goes down.\n"
        "- stack_name accepts the repository name and normalises it to the "
        "stack name the deploy used.\n"
        "- host defaults to the Swarm manager.\n"
        "Images, git tags and pipeline history are untouched, so "
        "trigger_pipeline or deploy_stack brings the stack back."
    )
)
async def remove_stack(stack_name: str, host: Optional[str] = None) -> str:
    """Remove a deployed stack from the Swarm."""
    from .api import remove_stack as _api_remove_stack

    try:
        data = await _api_remove_stack(stack_name=stack_name, host=host)
    except Exception as exc:
        return _api_error(exc)
    # Not stack=: structlog reserves that key for exception stacks and would
    # render the value as a traceback block.
    logger.info("MCP stack removed", stack_name=stack_name, host=host)
    return json.dumps(data, default=str)


# ---------------------------------------------------------------------------
# Helpers
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


def get_mcp_read_app():
    """Return the ASGI app for the read-only MCP server.

    The SDK serves internally on /mcp, and FastAPI mounts this at /ai,
    so the full endpoint is /ai/mcp.
    """
    return mcp_read.streamable_http_app()


def get_mcp_actions_app():
    """Return the ASGI app for the actions MCP server.

    The SDK serves internally on /mcp, and FastAPI mounts this at /ai/actions,
    so the full endpoint is /ai/actions/mcp.
    """
    return mcp_actions.streamable_http_app()
