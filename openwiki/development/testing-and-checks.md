---
type: testing
title: Testing and checks
description: How to validate a PulsarCD change with scripts/dev.sh, what ruff, mypy and pip-audit enforce, how Python and JavaScript tests are organized, which tests skip without disposable services, how the test image gates a release, and why dependency versions are pinned.
tags: [testing, ci, pytest, ruff, mypy, dependencies]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-bb1ebe868e35e9e500714501
    resource: repo://Dockerfile
  - id: openwiki-source-19973c87ca458faa5d03fecc
    resource: repo://docs/DEVELOPMENT.md
  - id: openwiki-source-05ccef8d4cf1698187f20464
    resource: repo://pyproject.toml
  - id: openwiki-source-373640cd8a0886cee69db282
    resource: repo://requirements.txt
  - id: openwiki-source-fdcaaf4f0705620099c19c9a
    resource: repo://scripts/check_brotli.py
  - id: openwiki-source-a5928e7ee39885995efdc170
    resource: repo://scripts/dev.sh
  - id: openwiki-source-5fcb2acc3022831e05a5156f
    resource: repo://scripts/static-checks.sh
  - id: openwiki-source-2161525089da862e880baf7a
    resource: repo://scripts/test.sh
  - id: openwiki-source-5063b6aa8934c32dd8a94ee1
    resource: repo://tests/AGENTS.md
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# Testing and checks

**Use `scripts/dev.sh` to validate this repo.** Do not use `scripts/test.sh` for this: it is the pipeline's tool for deployed projects, and it modifies a Git checkout and runs Docker Compose. `dev.sh` never checks out or deploys anything.

```bash
bash scripts/dev.sh setup     # once; creates .venv from requirements-dev.txt, needs network
bash scripts/dev.sh check     # static checks, shell syntax, Python and JS tests

bash scripts/dev.sh python tests/test_api.py -q
bash scripts/dev.sh python tests/test_security.py -k viewer -q
bash scripts/dev.sh frontend tests/test_stack_env.cjs
bash scripts/dev.sh static
```

`check` continues after a failure, so one run reports every problem, and exits non-zero if any step failed. It finds `.venv` without activation. `PULSARCD_PYTHON` overrides the interpreter. The scripts target Bash on Linux or macOS, so on Windows use WSL.

## Static checks

`scripts/static-checks.sh`, configured in `pyproject.toml`:

| Tool | Role | Blocks? |
|---|---|---|
| `ruff check .` | Likely bugs only, never style | Yes |
| `mypy` on `backend`, `agent`, `shared` | Permissive: unknown attributes, bad calls | Yes |
| `pip-audit` | Known vulnerabilities | **No, warning only** |

`pip-audit` can't block because the pinned FastAPI, Starlette and PyJWT versions carry advisories that only a planned framework upgrade can clear. So it needs reading. `PULSARCD_SKIP_AUDIT=1` skips it offline; say so when reporting results.

## Python tests

`pytest.ini` sets `asyncio_mode = auto` and strict xfail. Tests are `tests/test_*.py`.

- **HTTP tests** use the `client` and `auth_headers` fixtures in `tests/conftest.py`, whose lifespan replaces infrastructure with doubles. They share session-level doubles, so **restore any change you make to services or global state**.
- For a new or changed route, test unauthenticated, viewer and admin access, and consider its MCP callers.
- **Bash scripts** are tested with fake executables on a temporary `PATH`. Keep this model: no real registry, cluster or remote repo.
- **Never validate by running a real deployment, restore or host action.**

### Tests that may be skipped

| Tests | Need | Otherwise |
|---|---|---|
| Storage | `victoria-logs` binary on PATH, or `VL_TEST_URL` | Skipped |
| Recovery vault | `PULSARCD_TEST_DATABASE_URL` | Skipped |

**Use only disposable instances, as these tests write data.** A skip doesn't prove anything, so read the `-ra` summary and report skipped tests with their cause. Don't add a skip or xfail to turn the suite green.

## JavaScript tests

`node --test tests/test_*.cjs`, with no npm dependencies. The frontend has no build step, so these tests load scripts or their logic directly. A missing Node counts as a failure, not a silent pass. See [Frontend](../frontend/ui-architecture.md).

## The release gate

Nothing in `.github/` tests this repo. **The check that matters is PulsarCD's own pipeline**, through the `Dockerfile` stages:

| Stage | Content |
|---|---|
| `base` | Python 3.11.18, requirements, `check_brotli.py`, `pip check`, an MCP import check |
| `test` | Dev dependencies, plus VictoriaLogs, Node, `static-checks.sh` **during the build** |
| `production` | No Node or dev dependencies |

So a lint or type error fails the test image build, and starting that image runs both test suites.

```bash
docker build --target test -t pulsarcd-test .
docker run --rm pulsarcd-test
```

A push triggers CI/CD, and a local check doesn't prove the remote deployment succeeded.

## Dependency pins

These pins are deliberate. Read the comments in `requirements.txt` before bumping them.

| Package | Pin | Why |
|---|---|---|
| `fastapi`, `starlette` | `0.109.0`, `>=0.35,<0.36` | Auth middleware depends on Starlette 0.35 behaviour |
| `PyJWT` | `2.8.0` | Newer versions may change `jwt.decode` behaviour |
| `pydantic` | `<2.14` | The compatible MCP version needs a module Pydantic removed. Lift with the framework upgrade |
| Python | 3.11.18 or later 3.11 | CVE-2026-115557. Updating packages doesn't fix the interpreter |

Runtime dependencies are in `requirements.txt` and `agent/requirements.txt`, and tools in `requirements-dev.txt`. Runtime ranges aren't a full lock of transitive dependencies.

`security-constraints.txt` sets minimum safe versions of optional decoders without installing them. `scripts/check_brotli.py` **fails the image build** if an unsafe Brotli decoder is present, and the Dockerfiles check Debian's `libbrotli1` separately. The reasoning is in `docs/CVE-2026-111497.md`.

## Conventions

- No frontend framework, bundler or new package manager for ordinary changes.
- Don't reformat unrelated neighbouring code.
- Never commit secrets, `.env`, SSH keys or `/data` content.
- Keep authorization checks, the CSP, log redaction and SSH host key verification intact.
- Update guides when commands, entry points or invariants change.
- Scoped instructions are in `backend/AGENTS.md`, `agent/AGENTS.md`, `frontend/AGENTS.md` and `tests/AGENTS.md`, and the workflow guide is `docs/DEVELOPMENT.md`.

## Leftover files

`.tmp-vlogs-commit/` holds a commit manifest and message from the OpenSearch to VictoriaLogs migration (commit `ab13889`). Nothing in the application uses it, and its name suggests it was committed by accident.
