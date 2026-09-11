# PulsarCD MCP Servers

PulsarCD exposes two [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) servers that let AI agents interact with the platform.

## Endpoints

| Server | URL | Description |
|--------|-----|-------------|
| **Read** | `/ai/mcp` | Read-only tools: inventory, logs, tags, pipeline state, action status, Swarm task states |
| **Actions** | `/ai/actions/mcp` | Tools that change the deployment: pipeline, build/test/deploy, runtime operations, stack `.env`. **Admin JWT (or the MCP API key) only** |

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
- **JWT token** — the same token the web UI gets after signing in with Google,
  with two conditions:
  - `/ai/actions/mcp` requires `role == "admin"`. A `viewer` JWT gets
    `403 {"error": "Admin role required for this MCP server"}` — its tools change
    what runs on the Swarm. `/ai/mcp` accepts any authenticated role.
  - Revocation is enforced on both mounts: after a role change, a removal from
    the Google allowlist, or a break-glass password rotation, the token is
    refused with `401 {"error": "Token has been revoked"}`, exactly as on the
    HTTP API.

## Configuration

| Environment variable | Default | Description |
|---------------------|---------|-------------|
| `PULSARCD_MCP__ENABLED` | `true` | Enable or disable both MCP servers |
| `PULSARCD_MCP__API_KEY` | *(auto-generated)* | Set a fixed MCP API key. If empty, a random key is generated at startup and logged |

## Available Tools

### Read server (`/ai/mcp`)

#### Inventory

| Tool | Description |
|------|-------------|
| `list_stacks` | List available stacks (GitHub repositories) |
| `list_containers` | List all Docker containers and their states across all hosts. Accepts optional `host` and `status` filters |
| `list_computers` | List all monitored hosts including discovered Swarm nodes. Returns names and the Swarm flag only: hostname/port/username are admin-only infrastructure detail and this server accepts any role |
| `get_service_tasks` | Task states of one Swarm service — the `docker service ps` view, including failed and shutdown tasks. This is what tells you a deploy actually **converged**: `get_action_status` only says the deploy command returned |

#### Logs and health

| Tool | Description |
|------|-------------|
| `get_log_metadata` | Discover available hosts, services, containers and log levels in the log store. Call this first before searching logs |
| `search_logs` | Search logs with filters (query, project, service, host, level, time range) or raw OpenSearch queries |
| `get_health_summary` | Post-deploy check in one call: container/host counts, 24h error, warning and HTTP 4xx/5xx totals, CPU/memory/GPU load, recurring error patterns |

#### Releases and pipeline

| Tool | Description |
|------|-------------|
| `get_deployed_tags` | What actually runs in production and QA versus the latest tag built |
| `list_tags` | Recent tags of a repository |
| `get_untagged_commits` | Commits not shipped yet |
| `get_next_version` | The version a build would take |
| `get_pipeline_status` | Current stage of the pipeline and the action id running it |
| `get_transition_config` | Which gates are automatic, agent-evaluated or manual |
| `list_actions` | Recent background actions, newest first, filterable by status and repo |
| `get_action_status` | Status of a background action by `action_id` (also on the actions server) |
| `get_action_logs` | Output of a background action, with offset/limit paging (also on the actions server) |

### Actions server (`/ai/actions/mcp`)

#### Shipping a version

| Tool | Description |
|------|-------------|
| `trigger_pipeline` | **The preferred way to ship.** Runs build → test → (QA) → deploy as one tracked pipeline from a `tag` or a `commit`, tags the commit, records the version and honours the per-project gates |
| `build_stack` | Build a Docker image from a repository. Re-runs a single stage; `version` defaults to the next patch after the latest tag |
| `test_stack` | Run the test suite for a stack |
| `deploy_stack` | Deploy a stack to Docker Swarm. `qa=true` deploys to the isolated QA environment. This is also how you **roll back** (`tag=<previous tag>`) and how you **promote a QA build to production** |
| `cancel_action` | Cancel a running build/test/deploy |
| `set_transition_config` | Set the gate mode of one pipeline transition (`auto`, `auto_with_success`, `agent`, `manual`) and toggle the QA stage |

#### Operating what is already deployed

| Tool | Description |
|------|-------------|
| `container_action` | `start`, `stop`, `restart`, `pause`, `unpause` or `remove` **one** container |
| `update_service_image` | Roll one Swarm service to another image tag without redeploying the stack. Does **not** go through the pipeline: no version is recorded |
| `remove_service` | Remove one Swarm service |
| `remove_stack` | Remove a whole deployed stack (`docker stack rm`). Images, tags and pipeline history are untouched, so `deploy_stack` brings it back |

#### Stack configuration

| Tool | Description |
|------|-------------|
| `get_stack_env` | List the variables of a stack's `.env` — **keys and value lengths only**. Values are never returned |
| `set_stack_env` | Patch a stack's `.env` key by key (`updates` / `unset`). Comments, ordering and untouched variables survive |

### Two deliberate absences

**There is no shell tool.** `run_command` used to execute arbitrary commands on
the Swarm manager. Holding the MCP API key — a machine credential exempt from
the role check — therefore meant a shell on the node that owns every SSH key and
the Docker socket, and every other control on this server (the gates, the
pipeline state, the agent denylist) was advisory: one command bypassed all of
them and left no version record behind. The operations people actually ran
through it are now named tools that can each be authorised, logged and denied on
their own, and `get_service_tasks` covers the diagnostic use.

**There is no `create_tag`.** `trigger_pipeline(commit=…)` already tags what it
ships, which is the only tagging that leaves a coherent pipeline state.

### Agent denylist

Every tool on the actions server is on the LLM agent's unconditional denylist
(`backend/config_file.py: DANGEROUS_TOOL_NAMES`): the agent refuses to call them
even when they are listed in `error_handling.allowed_tools`, unless
`error_handling.allow_dangerous_tools` is explicitly enabled. Any tool added to
that server must be added to the list too — a tool the agent can reach is a tool
a prompt injection in a log line can reach. `tests/test_security.py` pins the two
lists together.

`build_stack`, `test_stack`, `deploy_stack` and the pipeline run in the
background and return an `action_id`. Use `get_action_status` / `get_action_logs`
to track progress.

## Client Configuration Examples

### Claude Code

```bash
claude mcp add --transport http pulsarcd-read \
  https://your-host/ai/mcp \
  --header "Authorization: Bearer <your-mcp-api-key>"

claude mcp add --transport http pulsarcd-actions \
  https://your-host/ai/actions/mcp \
  --header "Authorization: Bearer <your-mcp-api-key>"
```

Use the ASCII-only form of the key: non-ASCII bytes in an HTTP header value are
rejected by some clients.

### Claude Desktop

```json
{
  "mcpServers": {
    "pulsarcd": {
      "type": "streamable-http",
      "url": "https://your-host/ai/mcp",
      "headers": {
        "Authorization": "Bearer <your-mcp-api-key>"
      }
    },
    "pulsarcd-actions": {
      "type": "streamable-http",
      "url": "https://your-host/ai/actions/mcp",
      "headers": {
        "Authorization": "Bearer <your-mcp-api-key>"
      }
    }
  }
}
```

## Typical Workflow

### Ship a version

1. `get_untagged_commits(repo_name)` — what is not shipped yet.
2. `trigger_pipeline(repo_name, commit=…)` — build, test, QA, deploy under the project's gates.
3. `get_pipeline_status(repo_name)` — current stage and its `action_id`; `get_action_logs(action_id)` to read a failure.
4. After a QA deploy the pipeline stops on a manual gate: promote with `deploy_stack(repo_name, tag=…)`.
5. `get_service_tasks(service)` — check the rollout converged, then `get_health_summary()` and `search_logs(...)`.

### Roll back

`list_tags(repo_name)` → `deploy_stack(repo_name, tag=<previous tag>)` → `get_service_tasks(service)`.

### Investigate

1. `get_log_metadata()` to discover available services and hosts.
2. `search_logs(github_project="myrepo", last_hours=24)`.
3. `get_service_tasks(service)` when a service is restarting; `container_action(host, container_id, "restart")` to bounce one container.
