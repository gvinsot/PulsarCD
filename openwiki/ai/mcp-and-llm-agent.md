---
type: architecture
title: MCP servers and LLM agent
description: The two MCP servers PulsarCD exposes for reading state and changing deployments, how they authenticate, the built-in LLM agent that investigates failures under a restrictive tool policy with prompt injection defences, and natural language log search.
tags: [mcp, llm, ai, tools, prompt-injection, automation]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-1ff8f781e0487c7d85b80f37
    resource: repo://backend/ai_service.py
  - id: openwiki-source-1f07e88869e68e706effdb88
    resource: repo://backend/api.py
  - id: openwiki-source-a614c5fa89efac5c1c9d4216
    resource: repo://backend/config_file.py
  - id: openwiki-source-82a325da453da1db06378e6d
    resource: repo://backend/config.py
  - id: openwiki-source-bed4b2205cdc62988c2f6748
    resource: repo://backend/llm_agent.py
  - id: openwiki-source-7e9fe9900cc9f03f269a4feb
    resource: repo://backend/mcp_auth.py
  - id: openwiki-source-c9c8d0e951d558695dd62dcc
    resource: repo://backend/mcp_server.py
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# MCP servers and LLM agent

AI features come in three parts:

| Part | Direction | Code |
|---|---|---|
| MCP servers | External AI clients call PulsarCD | `backend/mcp_server.py`, `backend/mcp_auth.py` |
| LLM agent | PulsarCD calls a model, which uses MCP tools | `backend/llm_agent.py` |
| AI search | Translates a question into log filters | `backend/ai_service.py` |

`docs/MCP.md` is the tool reference. The model backend is an OpenAI-compatible vLLM endpoint. The README's mentions of Ollama are out of date for this code.

## MCP servers

| Server | Mount | Access | Contents |
|---|---|---|---|
| Read | `/ai/mcp` | Any authenticated user | Inventory, logs, tags, pipeline state, action status, Swarm tasks |
| Actions | `/ai/actions/mcp` | Admin JWT or MCP API key | Pipeline, build, test, deploy, stack env, runtime operations |

Both use `FastMCP` in stateless HTTP mode, and `get_action_status` and `get_action_logs` are on both.

**There is deliberately no shell tool.** Every tool is a named operation that records pipeline state and leaves an audit trail. The docstring of `mcp_auth.py` still mentions `run_command`, which has been retired, and it stays on the denied list for the LLM agent.

### How to ship with these tools

Prefer `trigger_pipeline`, which runs build, test, optional QA and deploy as one tracked pipeline, tags the commit and honours gates. Use `build_stack`, `test_stack` and `deploy_stack` only to rerun a stage, promote a QA build, or roll back by deploying a previous tag.

An action returning success doesn't mean it converged. Follow up with `get_service_tasks` and `get_health_summary`.

`repo_name` is enough to identify a stack. `_resolve_repo` looks it up among starred repositories and **rejects a mismatching `ssh_url`**, so a clone and its pipeline state can't refer to different repositories. An unknown stack fails with the list of known names.

Formats are validated: versions are `X.Y` or `X.Y.Z`, tags allow a leading `v`, commits are 7 to 40 hex characters.

### Stack environment tools

- `get_stack_env` returns keys with **values redacted**, plus their lengths.
- `set_stack_env` patches only named keys, and takes effect on the next deploy. `env` selects `prod` (`devops/.env`), `qa` (`devops/.env.qa`), or `build` (`devops/.env.build`, for BuildKit secrets). See [Project integration](../pipeline/project-integration-and-secrets.md).

### Authentication

`MCPAuthMiddleware` accepts only `Authorization: Bearer`. `?token=` was removed and is logged as a failure, because uvicorn would write it to an access log that any viewer can search in the log store.

| Token | Rights |
|---|---|
| JWT from web sign-in | `/ai/mcp` for any role; `/ai/actions/mcp` requires `admin`. Revocation is checked here, because `/ai` is exempt from the HTTP middleware |
| MCP API key, `PULSARCD_MCP__API_KEY` | **Full privilege on both servers**, with no role check. Treat it as a root credential |

If unset, the key is a random value generated at each boot in `config.py`. `docs/MCP.md` says it's printed in the logs at startup, but the current lifespan only logs that MCP is enabled, so **a generated key can't be retrieved. Set it explicitly** if you need it.

Failed authentications log a short SHA-256 fingerprint of the token, never the token.

## LLM agent

`LLMAgent` reacts to events by running an iterative tool-calling loop:

| Entry point | Trigger |
|---|---|
| `handle_failure` | A build, test or deploy failed, from `pipeline_runner.py` |
| `handle_recurring_error` | The [error detector](../observability/errors-and-notifications.md) found a recurring pattern |
| `handle_log_analysis` | A log analysis request |

Instructions come from `error_handling` in the PulsarCD config, per stage. The default flow investigates through `search_logs`, checks prior reports, and creates a task in PulsarTeam with a diagnosis if there isn't one. Default instructions say never to call `build_stack` or `deploy_stack`.

Each loop is limited to 25 iterations, and a dedup key imposes a one-hour cooldown per stage and repo. History and system errors are kept in bounded buffers, with history saved to `/data`.

### Tool policy

**The agent reads text anyone can influence: logs and build output.** An attacker able to trigger an error can write into its input, so tool access is restricted by platform code, not only by the prompt. `tool_denial_reason` in `config_file.py` applies:

| Tool kind | Default | How to allow |
|---|---|---|
| Investigation tools | Allowed | Empty `allowed_tools` means every non-dangerous tool |
| Name matches a dangerous keyword | Refused | List it in `allowed_tools`, or set `allow_dangerous_tools` |
| On `DANGEROUS_TOOL_NAMES` | Refused, **even if listed** | Only with `allow_dangerous_tools` |

`DANGEROUS_TOOL_NAMES` includes `build_stack`, `deploy_stack`, `test_stack`, `trigger_pipeline`, `set_transition_config`, `cancel_action`, `container_action`, `update_service_image`, `remove_service`, `remove_stack`, `get_stack_env`, `set_stack_env`, `create_tag` and `run_command`.

**When adding an MCP tool that changes anything, add it to this list.** A tool that matches no keyword would otherwise be available to the agent. `get_stack_env` is included because key names alone map the deployment. `set_transition_config` is included because switching a gate to `auto` would remove the human from the loop.

Because the MCP API key has full privilege, this policy, and not the key, is what limits the agent.

### Prompt injection defences

- Untrusted content is wrapped in `PULSARCD_UNTRUSTED_DATA` markers by `_wrap_untrusted`.
- Anything resembling the markers or the security notice's header is stripped first, so content can't close the block or impersonate policy.
- A security notice tells the model not to follow instructions inside the markers, and to report attempts as suspected injection.
- Inline values are sanitized by `_sanitize_inline_value`.

These reduce risk, but the tool policy is what enforces it.

### Context management

Tokens are estimated at 4 characters each. `compact_messages` progressively compresses the oldest tool results when nearing the budget, preserving the system prompt, latest user message and recent exchanges. It keeps the untrusted markers around compacted content. The initial prompt is limited to 64,000 tokens, or half the context, whichever is smaller. Output included in prompts is capped at 64 KB, favouring error lines.

## AI log search

`AIService` turns a natural language question into a JSON filter with query, levels, status range, hosts, containers, projects and time range. Exact names available in the log store are given to the model, as they must match exactly. If the model is unavailable or its reply is unusable, `_fallback_parse` applies simple heuristics. `analyze_log` explains individual log lines.

Configure it with `PULSARCD_VLLM_URL` and `PULSARCD_AI__MODEL`. In Swarm, the default URL is `vllm-dev-service`, from the AIFabric stack.

## Testing

`tests/test_context_compaction.py` covers compaction, `tests/test_security.py` covers access rules, and tests use doubles rather than real LLM or MCP calls.
