"""Agent API: the endpoints host agents poll and answer, plus queuing actions.

/api/agent/* and /api/agents.  The three routes agents call authenticate
with an agent key (_AGENT_KEY_ROUTES); the others take a session JWT.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from typing import Any, Optional

import structlog
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ValidationError

from ..actions_queue import ActionType
from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ============== Agent API ==
# These endpoints are used by agents running on remote hosts

@router.get("/api/agent/actions")
async def get_agent_actions(agent_id: str = Query(..., description="Agent identifier")):
    """Get pending actions for an agent.

    Agents poll this endpoint to receive actions to execute.
    Actions are marked as in_progress when returned.
    """
    actions = await api.actions_queue.get_pending_actions(agent_id)
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


@router.post("/api/agent/result")
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

    action = await api.actions_queue.complete_action(action_id, success, output,
                                                 agent_id=agent_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    return {"status": "ok", "action_id": action_id}


@router.post("/api/agent/system-error")
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
    if api.llm_agent:
        api.llm_agent._report_system_error(
            category, error_type, error,
            agent_id=agent_id)
    logger.warning("Host agent system error reported",
                   agent_id=agent_id, category=category,
                   error_type=error_type, error=error[:200])
    return {"status": "ok"}


@router.get("/api/agents")
async def get_agents():
    """Get list of known agents and their status."""
    agents = await api.actions_queue.get_agents()
    result = []
    for agent in agents:
        is_online = await api.actions_queue.is_agent_online(agent.agent_id)
        result.append({
            "agent_id": agent.agent_id,
            "last_seen": agent.last_seen.isoformat(),
            "status": agent.status,
            "online": is_online,
        })
    return {"agents": result}


@router.post("/api/agent/action")
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
    action_obj = await api.actions_queue.create_action(agent_id, action_type_enum, payload)

    if not wait:
        return {
            "action_id": action_obj.id,
            "status": action_obj.status,
            "message": "Action queued",
        }

    # Wait for action to complete
    completed_action = await api.actions_queue.wait_for_action(action_obj.id, timeout=timeout)

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
