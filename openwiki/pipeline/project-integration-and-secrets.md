---
type: guide
title: Project integration, stack env and secrets
description: What a repository needs in order to be deployed by PulsarCD, how environment variables become Docker secrets by naming convention, why changed secrets are not picked up automatically, how QA overrides, build secrets and encrypted backups of env files work, and how old tags are cleaned.
tags: [onboarding, secrets, env, qa, backup, deployment]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-f9b9a2915e695519ad39edda
    resource: repo://backend/backup_vault.py
  - id: openwiki-source-a614c5fa89efac5c1c9d4216
    resource: repo://backend/config_file.py
  - id: openwiki-source-19933bcb2c8cf5cbd2ac4f31
    resource: repo://backend/github_service.py
  - id: openwiki-source-dbf2d65f93167fd927fbd876
    resource: repo://backend/tag_cleanup.py
  - id: openwiki-source-86a97cd4eb08e82f7aefe67a
    resource: repo://docs/PROMPT_PROJECTS.md
  - id: openwiki-source-ee1f3a327cce16d701880cbd
    resource: repo://docs/RECOVERY.md
  - id: openwiki-source-07ee66dbc36d3dac60d870ec
    resource: repo://scripts/build_secrets.py
  - id: openwiki-source-2c707e28f8c1f9d33f09f592
    resource: repo://scripts/process_qa_labels.py
  - id: openwiki-source-ed8ec59743428abf7ee12bce
    resource: repo://scripts/process_secrets.py
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# Project integration, stack env and secrets

A repository becomes deployable by being **starred on GitHub** and having a `devops/` folder. `docs/PROMPT_PROJECTS.md`, in French, is the full guide for project authors, including the edge infrastructure and data services.

```text
my-project/
├── devops/
│   ├── docker-compose.swarm.yml   # required
│   ├── docker-compose.pre.sh      # optional, before deploy
│   ├── docker-compose.post.sh     # optional, after a verified deploy
│   ├── .env.example               # committed, fake values
│   ├── .env                       # never committed
│   ├── .env.qa                    # optional, never committed
│   └── .env.build                 # optional, never committed
└── my-app/Dockerfile
```

Images with a `build:` section are built and pushed to the registry. A `test` service with `replicas: 0` is the convention for the test stage. Hooks run on the deploy host with `DEPLOY_VERSION` and `DEPLOY_STACK_NAME` in the environment. `STACK_NAME` isn't exported, as PulsarTeam's hook history shows.

## Secrets by naming convention

**A variable's name decides whether it becomes a secret.** At deploy, `scripts/process_secrets.py` looks through `environment:` blocks for names ending in one of these, case-insensitively:

`_SECRET`, `_KEY`, `_TOKEN`, `_PASSWORD`, `_CONNECTIONSTRING`, `_CONNECTION_STRING`

For a match, it:

1. Creates a Docker secret named `<stack>_<VAR>` from the `.env` value.
2. Removes the variable from `environment:`.
3. Mounts the secret at `/run/secrets/<VAR>`.
4. Declares it at top level as `external: true`.

**Names that don't match stay plain environment variables**, visible in `docker inspect`. `MONGODB_URI` and `DATABASE_URL` are common examples, so name them accordingly.

The application must read the file. `shared/secrets.py` shows one approach, copying secrets into the environment at startup without overwriting existing variables.

Other rules:

- An empty value creates no secret, and remains an environment variable.
- Don't define a key both ways.
- Secrets can also be declared explicitly at top level, with `external: true` and an optional `name:`.
- If PyYAML is unavailable, the script exits 0 **without changes**, so secrets stay as plain variables.
- **Never put secrets in build args.** They remain in image history.

### Changing a secret

**Docker secrets are immutable, and an existing secret is reused as is.** So editing a value in `.env` and redeploying **silently keeps the old value**.

To rotate one:

```bash
docker secret rm <stack>_<VAR>   # refused while a service uses it
# then redeploy
```

Swarm refuses removal while a service uses the secret, so that has to be handled first. This matters whenever a key seems not to have taken effect.

## The `.env` files

| File | Purpose | Used by |
|---|---|---|
| `devops/.env` | Production values | Deploy and builds |
| `devops/.env.qa` | QA-only overrides, used verbatim | QA and preview |
| `devops/.env.build` | Build-time secrets | Build only |

The primary build host holds them, and the editor UI and `set_stack_env` write there, with these guarantees:

- Reading or writing them via the API needs an admin, and the MCP read tool hides values.
- Content is limited to 4 MiB, and repo names are validated.
- A save replicates to each secondary build host over SSH stdin, with atomic replacement and `0600` permissions. Values never appear in command arguments or logs.
- If a copy fails, the file stays saved on the primary, and the UI lists failed servers. Saving again retries.
- Before a build or test on a secondary, files are synchronized from the primary.

**There is no failover if the primary is lost, and replication isn't a backup.** A direct edit over SSH isn't replicated until the next synchronization.

A change takes effect on the next deploy, without restarting anything, and is subject to the secret rule above.

## QA isolation

With `--qa`, `scripts/deploy-service.sh` prefixes the stack with `qa-` and `TRAEFIK_HOST` and any `*_HOST` or `*_DOMAIN` with `qa.`. Values in `.env.qa` override and aren't prefixed.

`scripts/process_qa_labels.py` avoids clashes on one cluster by prefixing local Traefik routers, services and middlewares, and named networks. Provider references such as `noop@internal` and shared networks such as `proxy` are kept. `QA_NETWORK_KEEP` extends that list. Hostnames in `rule=` labels are rewritten on the apex domain.

**Secrets are per stack, but anything else a QA stack shares is up to you.** PulsarTeam, for example, shares a database between its prod and QA.

## Build secrets

`scripts/build_secrets.py` handles secrets a build needs, such as a signing key:

| Source | Becomes |
|---|---|
| Each `KEY=value` in `.env.build` | Secret `key` in lowercase |
| The whole `.env.build` file | Secret `build_env`, to mount as a `.env` |
| `BUILD_SECRET_<ID>` in the environment | Secret `<id>` |
| A `_B64` suffix | Base64 decoded, for binary files |

Read one in a Dockerfile with `RUN --mount=type=secret,id=<id>`. The file is parsed and never sourced. Values go into `0600` files and a compose override used only for the build, so they don't become runtime secrets or image layers. Exclude `.env.build` from images too.

## Encrypted backups

`docs/RECOVERY.md`, in French, is the procedure. When enabled with `PULSARCD_BACKUP__ENABLED=true`, PulsarCD keeps encrypted versions of `.env` files and the mounted SSH directory in PostgreSQL. **It doesn't back up `/data`**, which holds users, the blocklist and pipeline state.

- Encryption is AES-256-GCM with a key derived from `ENCRYPTION_KEY`. Only authenticated ciphertext reaches PostgreSQL.
- A monitor reconciles files every minute, and editor saves back up synchronously first.
- `PULSARCD_BACKUP__ENV_HISTORY` keeps 10 versions per file by default.
- **Once enabled, a key or database error blocks `.env` edits through PulsarCD.**

**Keep a recovery copy of `ENCRYPTION_KEY` outside these backups.** Without it, they can't be decrypted, and `.env` files holding that key may be what you're trying to recover.

The feature uses `DATABASE_CONNECTION_STRING` or `PULSARCD_BACKUP__CONNECTION_STRING`, through the `postgresqlcluster_internal` network.

## Tag cleanup

`backend/tag_cleanup.py` removes old Git tags and their registry images. It is **off by default, and in dry-run when first enabled**, so that you can review its plan.

| Setting | Default |
|---|---|
| `enabled` | `false` |
| `dry_run` | `true` |
| `max_age_days` | 30 |
| `keep_latest_n` | 5 |
| `interval_hours` | 24 |

Deployed tags and the N most recent per repo are kept. Separately, the registry's own weekly cleanup in the PrivateNetwork stack deletes manifests that no Swarm service references, so an old version may disappear from the registry even with this disabled, and rollback or `deploy_stack` to it then fails.

## Testing

`tests/test_env_storage.py`, `test_env_replication.py`, `test_recovery.py`, `test_registry.py` and `test_build_push.py`. Vault tests need a disposable database in `PULSARCD_TEST_DATABASE_URL`.
