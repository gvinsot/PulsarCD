"""Admin endpoints under /api/admin/ (see also recovery, tag_cleanup).

Allowed Google addresses, configuration, LLM and MCP connection tests, agent
history, error-detector status and the VictoriaLogs diagnostics.
auth_middleware restricts every /api/admin/ route to the admin role.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from typing import Any, Dict

import aiohttp

import structlog
from fastapi import APIRouter, HTTPException, Request

from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ============== Admin: Allowed Google Addresses ==============
#
# "Users" are Google addresses now: there is no local account to create, and no
# password to set.  Adding an address here lets whoever controls that Google
# account sign in; removing it cuts their sessions immediately (the revocation
# epoch, see allowlist.py).  Addresses that come from the environment are
# reported as `managed` and refuse to be edited here, because the next restart
# would reinstate them and the operator would think the change had stuck.


@router.get("/api/admin/users")
async def admin_list_users():
    """List the Google addresses allowed to sign in (admin only)."""
    return {
        "users": api.email_allowlist.list_entries() if api.email_allowlist else [],
        "google_enabled": bool(api.google_verifier and api.google_verifier.enabled),
        # The break-glass account, so the UI can say it exists rather than
        # leaving an operator wondering where the local login went.
        "local_admin": api.user_manager.describe() if api.user_manager else None,
    }


@router.post("/api/admin/users")
async def admin_add_user(request: Request):
    """Allow a Google address to sign in (admin only)."""
    body = await request.json()
    email = body.get("email", "")
    role = body.get("role", "viewer")

    if not isinstance(email, str) or not isinstance(role, str):
        raise HTTPException(status_code=400, detail="Invalid request body")
    if not email.strip():
        raise HTTPException(status_code=400, detail="An email address is required")

    try:
        return await api.email_allowlist.add(email, role)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/api/admin/users/{email}")
async def admin_update_user(email: str, request: Request):
    """Change the role of an allowed address (admin only)."""
    body = await request.json()
    role = body.get("role")
    if not isinstance(role, str) or not role:
        raise HTTPException(status_code=400, detail="A role is required")

    try:
        return await api.email_allowlist.set_role(email, role)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/api/admin/users/{email}")
async def admin_delete_user(email: str):
    """Revoke an allowed address, cutting its open sessions (admin only)."""
    try:
        await api.email_allowlist.remove(email)
        return {"deleted": True}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============== Admin: Configuration ==============

@router.get("/api/admin/config")
async def admin_get_config():
    """Get current configuration (admin only)."""
    if api.settings.pulsar_config:
        return api.settings.pulsar_config.model_dump()
    return {}


@router.put("/api/admin/config")
async def admin_update_config(request: Request):
    """Update configuration file (admin only)."""
    from ..config_file import PulsarConfig, save_config_file
    body = await request.json()
    try:
        new_config = PulsarConfig(**body)
        save_config_file(new_config, api.settings.data_dir)
        # Do NOT apply env overrides here: the user's explicit UI changes
        # take precedence over environment defaults.
        api.settings.pulsar_config = new_config

        # Update LLM agent with new config
        if api.llm_agent:
            api.llm_agent.invalidate_tools_cache()
            api.llm_agent._error_handling = new_config.error_handling
            api.llm_agent._pipeline_gates = new_config.pipeline_gates
            api.llm_agent._mcp_servers = new_config.mcp_servers
            api.llm_agent._llm_url = new_config.llm.url.rstrip("/")
            api.llm_agent._llm_chat_url = api.llm_agent._resolve_chat_url(new_config.llm.url)
            api.llm_agent._llm_model = new_config.llm.model
            api.llm_agent._llm_api_key = new_config.llm.api_key
            api.llm_agent._context_tokens = new_config.llm.context_tokens
            api.llm_agent._max_output_tokens = new_config.llm.max_output_tokens

        return {"saved": True}
    except Exception as e:
        logger.error("Failed to save config", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/admin/llm-connection-test")
async def admin_llm_connection_test(request: Request):
    """Quick connectivity check: send a minimal chat completion request."""
    body = await request.json()
    url = (body.get("url") or "").strip()
    model = (body.get("model") or "").strip()
    api_key = (body.get("api_key") or "").strip()

    if not url or not model:
        raise HTTPException(status_code=400, detail="url and model are required")

    from ..llm_agent import LLMAgent
    chat_url = LLMAgent._resolve_chat_url(url)

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "Reply with exactly: ok"}],
        "max_tokens": 16,
        "temperature": 0,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                chat_url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    body_text = await resp.text()
                    return {"ok": False, "error": f"HTTP {resp.status}: {body_text[:300]}"}
                data = await resp.json()
                content = (
                    data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content")
                ) or ""
                model_used = data.get("model", model)
                return {"ok": True, "model": model_used, "reply": (content or "").strip()}
    except aiohttp.ClientError as e:
        logger.warning("LLM connection test failed", url=chat_url, error=str(e))
        return {"ok": False, "error": f"Connection failed: {e}"}
    except Exception as e:
        logger.error("LLM connection test error", url=chat_url, error=str(e))
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@router.post("/api/admin/mcp-test")
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
                from ..llm_agent import _parse_mcp_response
                data = await _parse_mcp_response(resp)
                if "error" in data:
                    error = data["error"]
                    msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                    return {"ok": False, "error": msg}
                mcp_tools = data.get("result", {}).get("tools", [])
                tool_names = [t.get("name", "?") for t in mcp_tools]
                return {"ok": True, "tools": tool_names, "count": len(tool_names)}
    except aiohttp.ClientError as e:
        logger.warning("MCP connection test failed", url=url, error=str(e))
        return {"ok": False, "error": f"Connection failed: {e}"}
    except Exception as e:
        logger.error("MCP connection test error", url=url, error=str(e))
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@router.post("/api/admin/llm-test")
async def admin_llm_test(request: Request):
    """Test LLM agent with a user message (admin only). Uses current config."""
    if not api.llm_agent:
        raise HTTPException(status_code=503, detail="LLM agent not initialized")

    body = await request.json()
    user_message = body.get("message", "").strip()
    if not user_message:
        raise HTTPException(status_code=400, detail="Empty message")

    # Use the error handling instructions as system prompt context
    eh = api.llm_agent._error_handling
    system_prompt = (
        f"{eh.instructions}\n\n"
        f"You are chatting with a DevOps user. You have access to MCP tools "
        f"from PulsarCD (search_logs, list_containers, list_computers, "
        f"get_log_metadata, get_action_status, list_stacks) "
        f"and from PulsarTeam (create_task, etc.).\n"
        f"Infrastructure-mutating tools (trigger_pipeline, build_stack, "
        f"test_stack, deploy_stack, container_action, update_service_image, "
        f"remove_service, remove_stack) are NOT available here: the platform "
        f"policy refuses them unless error_handling.allow_dangerous_tools is "
        f"enabled.\n"
        f"Use the appropriate tools to answer questions. "
        f"For example, if the user asks about errors, call search_logs. "
        f"If they ask about container status, call list_containers.\n"
        f"Respond concisely."
    )

    try:
        result = await api.llm_agent._run_agent(system_prompt, user_message)
        tools_called = list(api.llm_agent._last_tools_called) if hasattr(api.llm_agent, '_last_tools_called') else []
        api.llm_agent._record("chat", message=user_message[:200], response=result[:500] if result else "",
                          tools_called=tools_called)
        return {"response": result}
    except Exception as e:
        logger.error("LLM test error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/admin/agent-history")
async def admin_agent_history(page: int = 1, page_size: int = 15):
    """Get LLM agent action history (admin only), paginated."""
    if not api.llm_agent:
        return {"history": [], "total": 0, "page": 1, "page_size": page_size, "total_pages": 0}
    all_history = api.llm_agent.get_history()
    total = len(all_history)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    return {"history": all_history[start:end], "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}


@router.get("/api/admin/error-detector-status")
async def get_error_detector_status():
    """Diagnostic endpoint: return the error detector's internal state."""
    if not api.error_detector:
        return {"error": "Error detector not initialized"}
    return api.error_detector.get_status()


@router.get("/api/admin/victorialogs-status")
async def get_victorialogs_status() -> Dict[str, Any]:
    """Diagnostic endpoint: VictoriaLogs reachability and records per kind over 24 h."""
    if not api.logstore:
        return {"error": "Log store not configured"}
    return await api.logstore.status()


@router.get("/api/admin/victorialogs-probe")
async def victorialogs_probe() -> Dict[str, Any]:
    """Write a self-test record, make it searchable and read it back -- admin only.

    Enforced by auth_middleware: every /api/admin/ route requires an admin JWT.
    The record is a ``kind=selftest`` one, never mixed with container logs.
    """
    if not api.logstore:
        return {"error": "Log store not configured"}
    return await api.logstore.probe()
