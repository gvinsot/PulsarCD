---
type: guide
title: Preview environments
description: How PulsarCD deploys a branch as an isolated stack on its own hostname, how names are derived safely, the protections that stop a preview touching production or QA, expiry, and what DNS, TLS and stateful dependencies a preview needs.
tags: [previews, branches, deployment, traefik, dns]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-82a325da453da1db06378e6d
    resource: repo://backend/config.py
  - id: openwiki-source-5031d970187dc4735d5b2397
    resource: repo://backend/previews.py
  - id: openwiki-source-23775c3de52f3ab95a13cb8b
    resource: repo://README.md
  - id: openwiki-source-6655e701448cb97e10d38386
    resource: repo://scripts/build-push.sh
  - id: openwiki-source-57a9075a8ddf02747e7bf8e4
    resource: repo://scripts/deploy-service.sh
  - id: openwiki-source-b2f631f408558372510fd1db
    resource: repo://shared/preview_naming.py
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# Preview environments

A preview is a repository built from one branch and deployed beside production and QA, with its own stack, hosts and routers. It reuses the QA machinery in `scripts/deploy-service.sh`, called with `--preview <branch>`. Code is in `backend/previews.py`, `backend/preview_routes.py` and `shared/preview_naming.py`.

| | Production | QA | Preview of `feature/login` |
|---|---|---|---|
| Stack | `shop` | `qa-shop` | `pv-feature-login-df7c7a-shop` |
| Host | `example.com` | `qa.example.com` | `feature-login-df7c7a.example.com` |
| Image tag | `1.0.42` | `1.0.42` | `pv-feature-login-df7c7a` |

## Using it

| Request | Purpose |
|---|---|
| `GET /api/previews/plan?repo_name=&branch=` | Names it would get |
| `POST /api/previews?repo_name=&ssh_url=&branch=[&ttl_hours=]` | Build and deploy in the background |
| `GET /api/previews` | List recorded previews and unmanaged `pv-` stacks |
| `DELETE /api/previews/{stack}` | Tear down a recorded preview |
| `POST /api/previews/cleanup` | Remove expired ones now |

Mutating requests need an admin. The Stacks view has a matching section, removed when disabled.

## Before relying on previews

Read this first, because a preview may look isolated and not be:

**Stateful dependencies are shared by default.** A preview gets the production `.env` plus the `devops/.env.qa` overlay. **If `.env.qa` doesn't point databases and other dependencies elsewhere, the preview uses production's.** A branch with a migration could then change production data. Stack isolation doesn't extend to external services.

**DNS and TLS need setup.** Each preview answers on `<slug>.<TRAEFIK_HOST>`:

1. **DNS:** add a wildcard record for each apex you preview, such as `*.example.com`. A specific record still takes precedence.
2. **TLS:** either use a DNS-01 resolver for wildcard certificates, since HTTP-01 can't issue them, or rely on per-host certificates through HTTP-01, minding Let's Encrypt rate limits. The current edge configuration uses HTTP-01.
3. **Rules:** prefer `${TRAEFIK_HOST}` in Traefik rules. A literal `www.example.com` becomes `<slug>.www.example.com`, which a `*.example.com` certificate doesn't cover.

**Previews consume cluster resources**, as each is a full stack. `MAX_ACTIVE` limits recorded previews. Services pinned to GPU nodes, or using host-mode ports, may not be able to run twice.

## Names

`shared/preview_naming.py` is the single source of truth. The backend imports it, and the deploy script runs it as a script, so **they can't disagree on a stack name**.

- The slug is a DNS label of at most 24 characters.
- A branch that can't be used verbatim gets a 6-character hash of its exact name. This covers upper case, `/`, `_`, long names and reserved names such as `qa` and `www`. So `feature/x` and `feature-x` never share a stack.
- Stack names are capped at 48 characters.

## Build

The branch goes through the regular build path with version `pv-<slug>`. `build-push.sh` publishes that tag **only**: never the compose tag, never a release tag, and no Git tag. It always rebuilds, because the tag follows the branch.

Previews don't touch the repository's pipeline state.

## Protections

A preview can never overwrite or remove production or QA:

| Layer | Check |
|---|---|
| Names | Derived, never accepted from the client, and checked by `assert_preview_stack` |
| Deploy script | Refuses a name that doesn't start with `pv-`, or equals `<repo>` or `qa-<repo>` |
| API deploy | Refuses a running stack that isn't a recorded preview of the same repo and branch |
| API teardown | Accepts only stacks recorded in `<data_dir>/previews.json`, and checks again |
| Running check | Redeploys or removes a stack only while it runs an image tagged `pv-<slug>` |

So an unrelated stack that happens to start with `pv-` is listed as unmanaged and is never removed through this API.

## Hosts

`TRAEFIK_HOST` is always prefixed in preview mode, even if `.env.qa` sets it, and the same applies to `*_HOST` and `*_DOMAIN` variables referenced in a Traefik `rule` label. So a preview can't answer on a production or QA host. Other such variables follow the QA rules.

`process_qa_labels.py` prefixes router, service and middleware names and named networks, leaving shared ones such as `proxy` and provider references such as `global@file` alone.

## Expiry

| Variable | Default | Effect |
|---|---|---|
| `PULSARCD_PREVIEWS__ENABLED` | `true` | API, cleanup and UI section |
| `PULSARCD_PREVIEWS__TTL_HOURS` | 72 | Lifetime without a redeploy. `0` means never |
| `PULSARCD_PREVIEWS__CLEANUP_INTERVAL_MINUTES` | 30 | Check interval |
| `PULSARCD_PREVIEWS__MAX_ACTIVE` | 20 | Limit on recorded previews |

A preview that hasn't been redeployed within its TTL is removed by a background task. Redeploying resets the clock. `ttl_hours=0` on a request keeps it until torn down.

Teardown doesn't touch external resources a preview used. Cleaning up its data, if any, is your responsibility.

## Testing

`tests/test_previews.py`, `tests/test_preview_naming.py` and `tests/test_deploy_service_preview.py`.
