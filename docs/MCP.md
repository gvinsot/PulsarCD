# PulsarCD MCP Servers

PulsarCD exposes two [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) servers that let AI agents interact with the platform.

## Endpoints

| Server | URL | Description |
|--------|-----|-------------|
| **Read** | `/ai/mcp` | Read-only tools: list stacks, containers, hosts, search logs, check action status |
| **Actions** | `/ai/actions/mcp` | Write tools: build and deploy stacks |

## Authentication

Both servers accept two token types:

- **MCP API key** — dedicated key printed in the server logs at startup, or set via `PULSARCD_MCP__API_KEY`
- **JWT token** — the same token used by the web UI

The token can be provided via:

- `Authorization: Bearer <token>` header (preferred)
- `?token=<token>` query parameter (fallback for SSE clients that cannot set custom headers)

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
| `list_computers` | List all monitored hosts including discovered Swarm nodes |
| `get_log_metadata` | Discover available hosts, services, containers and log levels in the log store. Call this first before searching logs |
| `search_logs` | Search logs with filters (query, project, service, host, level, time range) or raw OpenSearch queries |
| `get_action_status` | Check the status of a background build or deploy action by its `action_id` |

### Actions server (`/ai/actions/mcp`)

| Tool | Description |
|------|-------------|
| `build_stack` | Build a Docker image from a GitHub repository. Returns an `action_id` |
| `deploy_stack` | Deploy a stack to Docker Swarm. Returns an `action_id` |

Both action tools run in the background and return an `action_id`. Use `get_action_status` on the read server to track progress.

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

1. Call `get_log_metadata()` to discover available services and hosts
2. Call `search_logs(github_project="myrepo", last_hours=24)` to browse recent logs
3. Call `build_stack(...)` to build, then `get_action_status(action_id)` to track progress
4. Call `deploy_stack(...)` to deploy once the build completes
