#!/bin/bash
#
# Static Checks
# =============
# Lints and type-checks the Python code, then audits installed packages for
# known vulnerabilities. The Dockerfile "test" stage runs this script, so a
# lint or type error fails the test image build (and therefore test.sh).
#
# Usage:
#   scripts/static-checks.sh
#
# Checks (configured in pyproject.toml):
#   ruff check   - blocking. Likely-bug rules only (pyflakes, syntax, ...).
#   mypy         - blocking. Permissive: unknown attributes/names, bad calls.
#   pip-audit    - warning only. Audits the packages installed in the current
#                  environment. It does not block: requirements.txt pins
#                  FastAPI/Starlette and PyJWT on purpose (see the comments
#                  there) and those pins carry advisories that only the planned
#                  framework upgrade can clear; a newly published advisory, or
#                  an unreachable vulnerability database, would otherwise fail
#                  builds of unchanged code.
#
# Environment:
#   PULSARCD_SKIP_AUDIT=1  - skip pip-audit (offline builds)
#

set -u

cd "$(dirname "$0")/.."

FAILED=()

run_check() {
    local name="$1"
    shift
    echo "==> $name"
    if "$@"; then
        echo "--> $name: passed"
    else
        echo "--> $name: FAILED"
        FAILED+=("$name")
    fi
}

run_check "ruff" ruff check .
run_check "mypy" mypy

if [ "${PULSARCD_SKIP_AUDIT:-}" = "1" ]; then
    echo "==> pip-audit: skipped (PULSARCD_SKIP_AUDIT=1)"
else
    echo "==> pip-audit (warning only)"
    if pip-audit --progress-spinner off --timeout 30; then
        echo "--> pip-audit: no known vulnerabilities"
    else
        echo "--> pip-audit: WARNING - vulnerable packages reported above (or the audit could not run); not failing the build"
    fi
fi

if [ "${#FAILED[@]}" -gt 0 ]; then
    echo "Static checks failed: ${FAILED[*]}"
    exit 1
fi
echo "Static checks passed"
