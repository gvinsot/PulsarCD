"""Admin endpoints under /api/admin/ (see also recovery, tag_cleanup).

Allowed Google addresses, configuration, LLM and MCP connection tests, agent
history, error-detector status and the OpenSearch diagnostics.
auth_middleware restricts every /api/admin/ route to the admin role.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from datetime import datetime
from typing import Any, Dict

import aiohttp

import structlog
from fastapi import APIRouter, HTTPException, Query, Request

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


@router.get("/api/admin/opensearch-status")
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
        info = await api.opensearch._client.info()
        result["_cluster_name"] = info.get("cluster_name")
        result["_server_version"] = info.get("version", {}).get("number")
    except Exception as e:
        result["_cluster_error"] = f"{type(e).__name__}: {str(e)[:300]}"

    for index_name in [api.opensearch.logs_index, api.opensearch.metrics_index, api.opensearch.host_metrics_index]:
        try:
            exists = await api.opensearch._client.indices.exists(index=index_name)
            if exists:
                count_resp = await api.opensearch._client.count(index=index_name)
                doc_count = count_resp.get("count", 0)

                # Get latest document
                latest = await api.opensearch._client.search(
                    index=index_name,
                    body={"size": 1, "sort": [{"timestamp": "desc"}]},
                )
                latest_hit = latest.get("hits", {}).get("hits", [])
                latest_doc = latest_hit[0]["_source"] if latest_hit else None
                latest_ts = latest_doc.get("timestamp") if latest_doc else None
                latest_host = latest_doc.get("host") if latest_doc else None

                # Get oldest document
                oldest = await api.opensearch._client.search(
                    index=index_name,
                    body={"size": 1, "sort": [{"timestamp": "asc"}], "_source": ["timestamp"]},
                )
                oldest_hit = oldest.get("hits", {}).get("hits", [])
                oldest_ts = oldest_hit[0]["_source"].get("timestamp") if oldest_hit else None

                # Get mapping (field types)
                # index_name is usually an alias: responses are keyed by the
                # concrete (versioned) index behind it.
                mapping_resp = await api.opensearch._client.indices.get_mapping(index=index_name)
                concrete = index_name if index_name in mapping_resp else next(iter(mapping_resp), index_name)
                mapping = mapping_resp.get(concrete, {}).get("mappings", {}).get("properties", {})
                field_types = {k: v.get("type", "?") for k, v in mapping.items()}

                # Get index settings
                settings_resp = await api.opensearch._client.indices.get_settings(index=index_name)
                idx_settings = settings_resp.get(concrete, {}).get("settings", {}).get("index", {})

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
                    "concrete_indices": sorted(mapping_resp),
                    "write_block": idx_settings.get("blocks", {}).get("write"),
                }
            else:
                result[index_name] = {"exists": False}
        except Exception as e:
            result[index_name] = {"exists": "unknown", "error": str(e)[:500]}
    result["_index_migrations"] = api.opensearch.index_status()
    return result


@router.post("/api/admin/opensearch-recreate-index")
async def recreate_opensearch_index(
    index: str = Query(..., description="Index name to recreate"),
):
    """Force delete and recreate an OpenSearch index with correct mapping.
    WARNING: This deletes all data in the index (every concrete index behind
    the alias). Mapping mismatches no longer need this: the backend migrates
    them in the background without data loss.
    """
    try:
        result = await api.opensearch.recreate_index(index)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to recreate index", index=index, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/admin/opensearch-probe")
async def opensearch_probe() -> Dict[str, Any]:
    """Deep OpenSearch diagnostic — admin only.

    Enforced by auth_middleware: every /api/admin/ route requires an admin JWT.
    The probe writes and deletes documents and enumerates every index, so it
    must never be reachable anonymously.

    Tests:
    1. Cluster connectivity and version
    2. List ALL indices with doc counts
    3. Write a test doc (single + bulk)
    4. Read and delete the test doc
    5. Check index mappings
    """
    import json as _json
    result: Dict[str, Any] = {"timestamp": datetime.utcnow().isoformat()}

    if not api.opensearch or not api.opensearch._client:
        result["error"] = "OpenSearch client not configured"
        return result

    client = api.opensearch._client

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
    test_index = api.opensearch.logs_index
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
        # Only report how many documents came back: returning production log
        # lines from a diagnostic endpoint adds nothing and leaks their content.
        result["search_test"] = {
            "status": "ok",
            "total_docs": total_val,
            "sample_count": len(search_resp.get("hits", {}).get("hits", [])),
        }
    except Exception as e:
        result["search_test"] = {"status": "error", "error": f"{type(e).__name__}: {str(e)[:300]}"}

    return result
