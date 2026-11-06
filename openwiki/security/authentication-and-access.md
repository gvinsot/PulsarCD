---
type: security
title: Authentication and access control
description: How people sign in with Google or a break-glass account, how session tokens are revoked, how the central middleware protects routes by default using two roles, which other credentials exist for agents, the edge, webhooks and MCP, and how headers and log redaction protect credentials.
tags: [security, authentication, google, roles, jwt, csp]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-5c1dcd33e1f8b6556f908228
    resource: repo://backend/allowlist.py
  - id: openwiki-source-7100139e910f5944b0451921
    resource: repo://backend/auth_middleware.py
  - id: openwiki-source-9d39a51fb1867de8482d70e3
    resource: repo://backend/auth.py
  - id: openwiki-source-c9957a25df0ca376162f25c4
    resource: repo://backend/google_auth.py
  - id: openwiki-source-fcffbe3e28749eaf9a39557c
    resource: repo://backend/main.py
  - id: openwiki-source-f6c953f83b38b5dce943d9ae
    resource: repo://backend/routes/auth.py
  - id: openwiki-source-9600acfef75e03bd87301993
    resource: repo://backend/security_headers.py
  - id: openwiki-source-4dc3938335a88be3d68a7a66
    resource: repo://backend/user_manager.py
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# Authentication and access control

**Anyone with an admin session has effective control of the cluster**, since PulsarCD deploys stacks, reads `.env` files and opens shells on a manager. It is normally published on the internet, so its policy denies by default.

## Credentials at a glance

| Credential | Used by | Where accepted |
|---|---|---|
| Session JWT, `Authorization: Bearer` | Browser | `/api/*` by role |
| Session JWT, `?token=` | SSE log stream, terminal WebSocket | Those two endpoints only |
| MCP API key | AI clients, LLM agent | `/ai/*`, full privilege |
| Agent key | Node agents | Three `/api/agent/*` routes |
| Edge key | Traefik | `GET /api/security/waf/traefik-config` only |
| HMAC signature | GitHub | `POST /api/webhooks/github` only |

## Signing in

### Google

Google is the normal path. The browser gets an ID token and posts it to `POST /api/auth/google`.

**Identity and permission are separate:**

1. `backend/google_auth.py` answers "is this really Google saying this is really that address?" The signature is checked against Google's current keys, `alg` is pinned to RS256, `aud` must equal the configured client ID, and `iss`, `exp` and `email_verified` are verified. The `aud` check is what stops an ID token issued to another application being accepted.
2. `backend/allowlist.py` answers "may that address sign in, and with which role?" An unlisted address is refused, since "has a Google account" isn't an access rule on the public internet.

| Source | Behaviour |
|---|---|
| `PULSARCD_AUTH__GOOGLE_ADMINS`, `_VIEWERS` | Reapplied on every boot and **win**. Shown as `managed`, and can't be edited in the UI |
| Settings > Users | Everyone else, in `allowed_emails.json` |

To remove an address listed in the environment, you must change the environment, or the next restart restores it.

### Break-glass administrator

`backend/user_manager.py` keeps one optional local account for when Google is unreachable or the last admin is locked out. It exists only if `PULSARCD_AUTH__PASSWORD` is set, with at least 12 characters. Without one, `POST /api/auth/login` answers 403, and a weak value is refused rather than replaced. The environment is authoritative at each boot. Passwords use bcrypt.

Failed logins are limited per account and address, 5 per minute, and per address, 30 per minute. The address comes from `X-Forwarded-For` only with `PULSARCD_TRUST_PROXY_HEADERS`, as the header is otherwise client-controlled.

## Sessions and revocation

`backend/auth.py` mints HS256 JWTs carrying `sub`, `role`, `auth` and `epoch`, with a 24-hour default expiry. The `auth` claim says which store owns `sub`, so an allowlisted address can't resolve to a local account with the same name.

**Tokens can be revoked** through a per-identity epoch: a token older than its identity's current epoch is refused. A role change, a removal from the allowlist, or a break-glass password change all cut open sessions.

- An unset `PULSARCD_AUTH__JWT_SECRET` is randomly generated, which signs everyone out on each restart. A short value stops startup.
- The UI keeps the token in `localStorage`. See [Frontend](../frontend/ui-architecture.md#authentication).

## Route policy

`auth_middleware.py` enforces the policy centrally, **so a new route is protected by default**:

1. Anything under `/api/admin/` needs `admin`.
2. Any `POST`, `PUT`, `DELETE` or `PATCH` under `/api/` needs `admin`, unless on `_READ_ONLY_POST_PATHS`.
3. Selected `GET`s need `admin` because they disclose secrets or infrastructure: `_ADMIN_ONLY_GET_RE` covers config, `.env` contents, container environments, GitHub token probes, and host information.
4. Other `GET`s are open to any signed-in user, including `viewer`.

| Role | Can |
|---|---|
| `admin` | Everything |
| `viewer` | Read dashboards, containers, stacks, **all logs**, pipeline status and history, security views |

**Rule 4 means a new GET is viewer-readable by default.** If it returns anything sensitive, add it to `_ADMIN_ONLY_GET_RE`.

To open a mutating route to viewers, add it to `_READ_ONLY_POST_PATHS` only after confirming no side effects. `/api/tasks/create` stays excluded as it runs the LLM agent.

**Exempt paths:** `/`, `/static/`, exact `/api/health`, `/api/auth/login`, `/api/auth/google` and `/api/auth/config`. `/ai` has its own middleware. Sub-paths of the health route stay authenticated.

WebSocket routes never reach HTTP middleware, so **a new WebSocket handler must authenticate for itself**, as `backend/routes/terminal.py` does.

Test new routes as unauthenticated, viewer and admin, and check their MCP callers.

## Machine credentials

**Agent keys:** see [Actions and remote execution](../hosts/actions-and-remote-execution.md#agent-identity). `POST /api/agent/action` isn't an agent route and needs an admin.

**Edge key:** Traefik has no session, so `PULSARCD_AUTH__EDGE_KEY` authenticates one read-only route. Unset, the route answers 503, and [blocking has no effect](edge-security-and-usage.md).

**Webhook:** only that method and path bypass the session check, and the handler verifies the signature.

**MCP:** see [MCP servers and LLM agent](../ai/mcp-and-llm-agent.md#authentication). `/ai` is exempt from `auth_middleware`, so `MCPAuthMiddleware` checks revocation for itself.

Comparisons use `hmac.compare_digest`.

## Tokens in URLs

A token in a URL leaks into proxy logs, browser history and Referer headers. **Here that matters more than usual, since container logs are searchable by any viewer**, so a logged token would be a credential anyone could find.

- `?token=` is honoured only on the SSE stream and terminal WebSocket, whose browser clients can't set headers.
- `backend/main.py` redacts `token`, `api_key`, `password` and `secret` query values from uvicorn log records before they're formatted.
- MCP refuses `?token=` outright.

Traefik's own access log is a separate matter, and the edge stack's logging decides what it records.

## Headers

`security_headers.py` sets a strict CSP on every response, including 401s and 403s, because it's the outermost middleware, plus HSTS. The Host header is client-controlled, so it's echoed into `connect-src` only if it has a plain `host[:port]` shape. See [Frontend](../frontend/ui-architecture.md#content-security-policy).

CORS allows no origins. It's the same-origin UI that calls the API.

## Error responses

Errors include an `X-Request-ID`, and tests in `tests/test_error_sanitization.py` check that internal details aren't exposed.

## Dependencies this relies on

**`fastapi`, `starlette` and `PyJWT` are pinned**, because the middleware and token decoding depend on their behaviour. Treat an upgrade as a security change, and rerun `tests/test_security.py`.
