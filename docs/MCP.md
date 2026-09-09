# PulsarCD MCP Servers

PulsarCD exposes two [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) servers that let AI agents interact with the platform.

## Endpoints

| Server | URL | Description |
|--------|-----|-------------|
| **Read** | `/ai/mcp` | Read-only tools: stacks, containers, hosts, logs, tags and versions, pipeline state, action status and logs, health summary |
| **Actions** | `/ai/actions/mcp` | Write tools: trigger the pipeline, build / test / deploy, tag, configure gates, read and write a stack `.env`, run a shell command. **Admin JWT (or the MCP API key) only** |

A client that mounts only the actions server can still follow what it starts:
`get_action_status` and `get_action_logs` are registered on **both** servers.
Everything else on the read server requires mounting it.

## Authentication

The token **must** be sent in the `Authorization: Bearer <token>` header. The
`?token=` query-string fallback was removed: uvicorn writes the full request line
to its access log, and that log is indexed in the log store any `viewer` account
can search, so a token in the URL was a reusable credential sitting in a
searchable index. A streamable-HTTP MCP client always controls its headers.

Both servers accept two token types:

- **MCP API key** — dedicated key printed in the server logs at startup, or set via `PULSARCD_MCP__API_KEY`.
  This is a machine identity provisioned out of band: it carries full privilege on
  **both** servers and is not subject to the role check below. Treat it like a root
  credential, pin it explicitly rather than relying on the per-boot random value,
  and rotate it if it ever reaches a log.
- **JWT token** — the same token used by the web UI, with two conditions:
  - `/ai/actions/mcp` requires `role == "admin"`. A `viewer` JWT gets
    `403 {"error": "Admin role required for this MCP server"}` — its tools reach
    the Swarm manager over SSH. `/ai/mcp` accepts any authenticated role.
  - Revocation is enforced on both mounts: after a password change, a role change
    or an account deletion the token is refused with
    `401 {"error": "Token has been revoked"}`, exactly as on the HTTP API.

## Configuration

| Environment variable | Default | Description |
|---------------------|---------|-------------|
| `PULSARCD_MCP__ENABLED` | `true` | Enable or disable both MCP servers |
| `PULSARCD_MCP__API_KEY` | *(auto-generated)* | Set a fixed MCP API key. If empty, a random key is generated at startup and logged |

## Available Tools

### Read server (`/ai/mcp`)

| Tool | Description |
|------|-------------|
| `list_stacks` | List available stacks (starred GitHub repositories) |
| `list_containers` | List all Docker containers and their states across all hosts. Accepts optional `host` and `status` filters |
| `list_computers` | List all monitored hosts including discovered Swarm nodes. Returns names and the Swarm flag only: hostname/port/username are admin-only infrastructure detail and this server accepts any role |
| `get_log_metadata` | Discover available hosts, services, containers and log levels in the log store. Call this first before searching logs |
| `search_logs` | Search logs with filters (query, project, service, host, level, time range) or raw OpenSearch queries |
| `get_action_status` | Check the status of a background build/test/deploy action by its `action_id`. Falls back to the persisted pipeline state after a restart |
| `get_action_logs` | Read the captured output of an action (`offset` / `limit`, tail-biased). `get_action_status` only carries 5 lines — use this to diagnose a failure |
| `list_actions` | List recent background actions, newest first, with optional `status` and `repo_name` filters. Finds the `action_id` of a stage started by `trigger_pipeline` |
| `get_deployed_tags` | What actually runs: deployed production tags, QA tags, and the latest tag built per stack. Use it to confirm a deploy landed and to pick a rollback target |
| `list_tags` | Git tags of a stack, newest first — the versions available to `deploy_stack(tag=...)` |
| `get_next_version` | The version the next build would take (latest tag, patch incremented) |
| `get_untagged_commits` | Commits with no tag, i.e. what has not shipped yet |
| `get_pipeline_status` | Pipeline state (stage, status, versions, per-stage `action_id`) for one stack or all of them |
| `get_transition_config` | Gate configuration per transition (`version_to_build`, `build_to_test`, `test_to_deploy`), the `qa_enabled` flag, and the last gate decision |
| `get_health_summary` | Post-deploy check in one call: container/host counts, 24h errors, 4xx/5xx, CPU/memory/GPU load, recurring error patterns and recent system errors |

### Actions server (`/ai/actions/mcp`)

| Tool | Description |
|------|-------------|
| `trigger_pipeline` | **Preferred way to ship.** Runs build → test → (QA) → deploy as one pipeline from a `tag`, or from a `commit` which it tags with the next version first. Honours the per-project gates. Returns `{status, repo, tag, version}` — no `action_id`: follow it with `get_pipeline_status` |
| `build_stack` | Build a Docker image from a GitHub repository. `version` defaults to the next patch version rather than a literal. Returns an `action_id` |
| `test_stack` | Run the test suite for a stack. Returns an `action_id` |
| `deploy_stack` | Deploy a stack to Docker Swarm. `qa=true` targets the isolated QA environment. `deploy_stack(repo_name, tag=<previous>)` is also the rollback path. With neither `version` nor `tag`, deploys what the pipeline last built (else the latest tag) rather than a literal `1.0`. Returns an `action_id` |
| `cancel_action` | Cancel a running build/test/deploy by `action_id` |
| `create_tag` | Tag a commit. Only needed to tag without building — `trigger_pipeline(commit=...)` already tags what it ships |
| `set_transition_config` | Set a transition's gate `mode` (`auto`, `auto_with_success`, `agent`, `manual`) and `qa_enabled`. An omitted `qa_enabled` **preserves** the current value, unlike the REST route which resets it to false |
| `get_stack_env` / `set_stack_env` | Read and replace the `.env` used at deploy time. `set_stack_env` writes the file whole, so read it first and send it back complete. They live here, not on the read server, because a stack `.env` holds secrets — the same reason `GET /api/stacks/{repo}/env` is admin-only |
| `run_command` | Run a shell command on a host (the Swarm manager by default). This is arbitrary code execution on that node — it is why the whole server is admin-only. **Never deploy with it**: a stack deployed this way has no pipeline state, no version record and no audit trail |

Every tool on this server **except** the two read-only duplicates
(`get_action_status`, `get_action_logs`) is on the LLM agent's unconditional
denylist (`backend/config_file.py: DANGEROUS_TOOL_NAMES`): the agent refuses to
call them even when they are listed in `error_handling.allowed_tools`, unless
`error_handling.allow_dangerous_tools` is explicitly enabled. Any tool added to
this server must be added to that list too — a tool the agent can reach is a tool
a prompt injection in a log line can reach. `DANGEROUS_TOOL_KEYWORDS` is no
safety net: `trigger_pipeline`, `cancel_action`, `set_transition_config`,
`create_tag` and `*_stack_env` match none of its keywords.
`tests/test_security.py::test_every_privileged_mcp_tool_is_denied_by_default`
pins the two lists together.

`build_stack`, `test_stack` and `deploy_stack` run in the background and return an `action_id`. Use `get_action_status` / `get_action_logs` to track progress.

### Identifying a stack

`repo_name` alone identifies a stack on every tool. `ssh_url` is resolved
server-side from the starred repositories and only needs to be passed to
override it; when passed it must match the registered URL. It used to be
required, which allowed a `repo_name` / `ssh_url` pair that nothing
cross-checked — and worse, the owner was regex-parsed out of that URL, so a URL
the regex did not match silently skipped branch and commit validation instead of
failing.

## Client Configuration Examples

### Claude Desktop / Claude Code

Add both servers in your MCP settings:

```json
{
  "mcpServers": {
    "pulsarcd": {
      "type": "streamable-http",
      "url": "https://your-host:8000/ai/mcp",
      "headers": {
        "Authorization": "Bearer <your-mcp-api-key>"
      }
    },
    "pulsarcd-actions": {
      "type": "streamable-http",
      "url": "https://your-host:8000/ai/actions/mcp",
      "headers": {
        "Authorization": "Bearer <your-mcp-api-key>"
      }
    }
  }
}
```

### Typical Workflow

**Ship a change** — always through the pipeline, never through `run_command`:

1. `get_untagged_commits("myrepo")` — what has not shipped yet
2. `get_transition_config("myrepo")` — check which gates are automatic before assuming the run completes on its own
3. `trigger_pipeline("myrepo", commit="<sha>")` — tags with the next version and runs build → test → (QA) → deploy
4. `get_pipeline_status("myrepo")` — current stage and its `action_id`; `get_action_logs(action_id)` on failure
5. `get_deployed_tags()` — confirm the new version is actually running
6. `get_health_summary()` then `search_logs(github_project="myrepo", levels="ERROR", last_hours=1)` — confirm nothing broke

**Roll back**: `list_tags("myrepo")` to find the previous version, then
`deploy_stack("myrepo", tag="v1.2.3")`.

**Investigate logs**:

1. `get_log_metadata()` to discover available services and hosts
2. `search_logs(github_project="myrepo", last_hours=24)` to browse recent logs
