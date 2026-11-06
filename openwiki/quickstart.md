---
type: guide
title: PulsarCD quickstart
description: Entry point to the PulsarCD wiki, explaining what the self-hosted build, deploy and monitoring platform does, how to run and check it, what is risky to change, and which page answers which task.
tags: [quickstart, overview, navigation, ci-cd, monitoring]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-b3039cedc9a310b64508f1c4
    resource: repo://backend/pipeline_runner.py
  - id: openwiki-source-c4946ce70fb4d8531fa934a5
    resource: repo://devops/docker-compose.swarm.yml
  - id: openwiki-source-23775c3de52f3ab95a13cb8b
    resource: repo://README.md
  - id: openwiki-source-a5928e7ee39885995efdc170
    resource: repo://scripts/dev.sh
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# PulsarCD quickstart

PulsarCD builds starred GitHub repositories, tests them, deploys them to Docker Swarm, and monitors what runs, through one web UI and two MCP servers for AI clients. It's made of one FastAPI backend, one agent per node, and VictoriaLogs for all logs, metrics and history. See [System overview](architecture/system-overview.md).

**By default, a push to the default branch of a starred repo ships it.** It also deploys itself and every other stack in this cluster, and holds the Docker socket, SSH keys and `.env` files. So treat changes here, and admin access, as high stakes.

## Check a change

```bash
bash scripts/dev.sh setup     # once
bash scripts/dev.sh check     # static checks, shell syntax, Python and JS tests
bash scripts/dev.sh python tests/test_api.py -q
```

Don't use `scripts/test.sh` for this. It's the pipeline's tool for deployed projects, and it changes a Git checkout. Never validate by running a real deployment. See [Testing and checks](development/testing-and-checks.md).

## Run it

```bash
export PULSARCD_AUTH__GOOGLE_CLIENT_ID=<client-id>.apps.googleusercontent.com
export PULSARCD_AUTH__GOOGLE_ADMINS=you@example.com
docker-compose up -d
```

Open `http://localhost:5000`, and add that origin to the OAuth client's allowed JavaScript origins. In a cluster, PulsarCD deploys itself through `devops/docker-compose.swarm.yml`.

Without Docker, you need Python 3.11.18 or a later 3.11 release, and VictoriaLogs on the loopback interface. VictoriaLogs has no authentication, so never publish it.

## Find what you need

| Task | Page |
|---|---|
| Understand components, startup and state | [System overview](architecture/system-overview.md) |
| Follow or change a build or deploy | [Build, test and deploy pipeline](pipeline/build-test-deploy.md) |
| Onboard a project, or manage `.env` and secrets | [Project integration, stack env and secrets](pipeline/project-integration-and-secrets.md) |
| Deploy a branch separately | [Preview environments](pipeline/preview-environments.md) |
| Run things on hosts, or change agents | [Actions and remote execution](hosts/actions-and-remote-execution.md) |
| Work on log collection or search | [Log collection and search](observability/log-collection-and-search.md) |
| Change alerts | [Error detection and notifications](observability/errors-and-notifications.md) |
| Add a route, or change sign-in or roles | [Authentication and access control](security/authentication-and-access.md) |
| Change blocking, bans or analytics | [Edge security and usage analytics](security/edge-security-and-usage.md) |
| Add an MCP tool, or change the LLM agent | [MCP servers and LLM agent](ai/mcp-and-llm-agent.md) |
| Change the UI | [Frontend](frontend/ui-architecture.md) |

## Rules that are easy to break

1. **`swarm-manager` must stay at one replica.** Its queue and pipeline state are in its process and `/data` files.
2. **New `GET` routes are viewer-readable by default.** Add sensitive ones to `_ADMIN_ONLY_GET_RE`.
3. **New MCP tools that change anything must go in `DANGEROUS_TOOL_NAMES`**, or the LLM agent can reach them.
4. **New WebSocket handlers must authenticate themselves**, as HTTP middleware doesn't see them.
5. **Viewers can search all logs**, so never log secrets or put tokens in URLs.
6. **A changed `.env` secret isn't picked up automatically.** Docker secrets are immutable, and an existing one is reused until removed.
7. **Build LogsQL with the helpers in `shared/victorialogs.py`**, never by concatenation.
8. **`fastapi`, `starlette` and `PyJWT` are pinned on purpose.**
9. **The UI has no build step.** Script order in `index.html` matters, and the CSP forbids inline handlers and styles.
10. **Probe patterns in `backend/auto_ban.py` must match the WAF rules** in the edge stack's repository, and nothing enforces it.

## Documentation notes

`README.md` is long and largely accurate. Read it by section rather than in full. Its mentions of Ollama are out of date: the AI features use a vLLM endpoint. `docs/MCP.md` is the MCP reference, though it says the auto-generated API key is printed at startup, which current code doesn't do. `docs/PROMPT_PROJECTS.md` and `docs/RECOVERY.md`, in French, are the project and recovery guides.
