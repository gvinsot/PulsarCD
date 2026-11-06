---
type: architecture
title: System overview
description: How PulsarCD's backend, per-node agents, VictoriaLogs and unbundled frontend fit together, what the backend starts and keeps in memory, why it must run as a single replica, how configuration is layered, and what it can reach on the cluster.
tags: [architecture, fastapi, swarm, victorialogs, configuration]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-1f07e88869e68e706effdb88
    resource: repo://backend/api.py
  - id: openwiki-source-82a325da453da1db06378e6d
    resource: repo://backend/config.py
  - id: openwiki-source-8993656929d1491a964ebe3e
    resource: repo://backend/durable_json.py
  - id: openwiki-source-ae8c774b0dfd8e6bb9bb5d99
    resource: repo://backend/routes/__init__.py
  - id: openwiki-source-c4946ce70fb4d8531fa934a5
    resource: repo://devops/docker-compose.swarm.yml
  - id: openwiki-source-23775c3de52f3ab95a13cb8b
    resource: repo://README.md
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# System overview

PulsarCD is a self-hosted platform that builds, tests and deploys starred GitHub repositories to Docker Swarm, then monitors what runs there. It is one FastAPI backend, one agent per node, and one VictoriaLogs instance. **It also deploys itself and every other stack in this cluster**, so a fault here blocks releases everywhere.

<!-- openwiki: mermaid parse failed and this diagram was converted to a text fence so it does not break rendering. Fix the diagram source and restore the mermaid fence. Parser error: Heuristic: an unescaped angle bracket inside a label breaks rendering; rephrase the label. -->
```text
flowchart TB
  U[Browser] --> T[Traefik]
  AI[AI clients] --> T
  T --> B["swarm-manager: backend<br/>FastAPI + static UI, port 8000"]
  B -- "reads" --> V[(VictoriaLogs)]
  B -- "SSH: clone, build, deploy" --> M[Build and manager hosts]
  B -- "Docker socket: read Swarm" --> D1[Local Docker API]
  subgraph node["Every node"]
    A["agent, global mode"] --> D2[Docker API]
  end
  A -- "direct writes: logs, metrics" --> V
  A -- "polls actions, reports results" --> B
  EDGE[Traefik edge] -- "polls blocklist" --> B
  GH[GitHub] -- "push webhook" --> B
```

| Component | Code | Swarm service | Placement |
|---|---|---|---|
| Backend and UI | `backend/`, `frontend/` | `swarm-manager` | `BACKEND_NODE_HOSTNAME`, default `server-b` |
| Agent | `agent/`, `shared/` | `agent` | `mode: global` on amd64 and arm64 |
| Log store | | `victorialogs` v1.53.0 | `VICTORIALOGS_NODE_HOSTNAME`, default `server-d` |
| Test image | `Dockerfile`, `test` target | `test` | `replicas: 0`, build target only |

The stack name matters: other stacks reach it as `pulsarcd_swarm-manager`, and logs are matched by compose project.

## Backend layout

`backend/api.py` owns the app, its lifespan and the shared state. **Other modules read that state as `api.<name>` at call time**, so what the lifespan assigns, or a test patches, is what they see. Routers in `backend/routes/` follow one per domain, and a new route should copy how its neighbours access services, to avoid circular imports.

Router order matters: where two paths could match, the first registered wins, such as `/api/previews/plan` over `/api/previews/{stack}`.

Middleware wraps in this order, from outermost: `security_headers_middleware`, `auth_middleware`, then CORS, which allows no origins.

### What starts in the lifespan

1. Load settings, then `LogStore`, pipeline state and the action queue, which fails interrupted items.
2. Sign-in, email allowlist, IP blocklist and the optional break-glass user.
3. **Wait for VictoriaLogs**, up to 30 attempts 2 seconds apart.
4. Legacy collector, run history recorder, notifier and auto-ban worker.
5. GitHub service and, if enabled, the recovery backup monitor.
6. LLM agent if configured, then the recurring error detector.
7. Auto-build poller, tag cleanup and preview cleanup.
8. MCP session managers, then the app serves requests.

## One replica only

**`swarm-manager` must stay at `replicas: 1`.** The action queue and pipeline state live in this process and its `/data` files. Several replicas would each hold their own queue and overwrite each other's files.

| What survives a restart | How |
|---|---|
| Queued actions | `actions_queue.json`: delivered if within their 60-second TTL. Picked-up ones become `failed` |
| Pipeline state | `pipeline_state.json`: `running` entries become `failed` with `status_reason`, and don't resume by themselves. Run the pipeline again |
| Users, blocklist, config | Other files in `/data` |
| Background action results | **Lost** |

Writes go through `durable_json.atomic_write_json`, which uses a temporary file and `os.replace`, so a crash mid-write can't leave a truncated file. Reuse it for new persistent files.

A pipeline cut short by a restart may have left its work half done. Because PulsarCD deploys itself, deploying PulsarCD interrupts it, so check for other running pipelines first.

The restart policy is `any`: Swarm stops an unhealthy task with SIGTERM, uvicorn exits 0, and `on-failure` would leave the service at 0/1.

## Configuration

Settings come from environment variables layered over `/data/config.yml`, with the environment taking priority. The naming is `PULSARCD_<SECTION>__<KEY>`, as documented at the top of `backend/config.py`. Agents use `AGENT_*`.

A change can touch `config.py`, `config_file.py`, configuration routes, the Settings UI and the Swarm compose file.

| Unset value | Effect |
|---|---|
| `PULSARCD_AUTH__JWT_SECRET` | Random: **every restart signs everyone out** |
| `PULSARCD_AUTH__AGENT_KEY` | Random: agents with a fixed key can't authenticate |
| `PULSARCD_MCP__API_KEY` | Random, and not retrievable |
| `PULSARCD_AUTH__EDGE_KEY` | The blocklist endpoint answers 503, so blocking has no effect |

A configured secret shorter than `MIN_SECRET_LENGTH` **stops startup**. Secrets may also come from Docker secret files through `shared/secrets.py`.

## What the backend can reach

This container is a high-value target. The compose file's comments state so directly:

- **Docker socket**, read-only. The `:ro` flag protects the socket file, not the API behind it, so this is equivalent to root on the node.
- **SSH keys** from an operator account, used for builds and deployments on other hosts.
- **GitHub token and registry credentials.**
- **Every stack's `.env`**, through the build host.

It runs as root to reach the socket, with `cap_drop: ALL`, only `DAC_READ_SEARCH` added to read those keys, and `no-new-privileges`. The compose file lists safer options not yet applied, such as a socket proxy. Agents also mount the socket, but use a read-only root filesystem.

The header of `devops/docker-compose.swarm.yml` still says the backend has no access to Docker sockets, which its own volume list contradicts.

### Edge routing

Labels define routers on `TRAEFIK_HOST` for the main app, for WebSocket terminals and for SSE log streams. Comments say the last two bypass the WAF, but [the edge entrypoint now applies the WAF to every router](../security/edge-security-and-usage.md), and the comments predate that.

## Log store retention

VictoriaLogs has no authentication at all, so keep it on the `internal` network and never publish it on `0.0.0.0`. Retention defaults to 90 days and 50 GiB, and at the disk cap the oldest days are dropped whatever the retention says. This single retention applies to logs, metrics, access records and pipeline history.

## Where to go next

- [Build, test and deploy pipeline](../pipeline/build-test-deploy.md)
- [Actions and remote execution](../hosts/actions-and-remote-execution.md)
- [Log collection and search](../observability/log-collection-and-search.md)
- [Authentication and access control](../security/authentication-and-access.md)
