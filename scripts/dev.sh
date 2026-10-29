#!/bin/bash
# Local, non-interactive development commands. Never checks out or deploys code.
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: bash scripts/dev.sh <command> [arguments]
  setup       Create .venv and install requirements-dev.txt (network required)
  check       Run static checks, shell syntax, Python and JavaScript tests
  test        Run Python and JavaScript tests
  python      Run pytest; remaining arguments select tests/options
  frontend    Run node --test; remaining arguments select tests/options
  static      Run ruff, mypy and the advisory dependency audit
  help        Show this message

Uses .venv automatically when present. PULSARCD_PYTHON overrides the interpreter.
PULSARCD_SKIP_AUDIT=1 disables the advisory network audit for offline checks.
Storage tests need a VictoriaLogs binary or VL_TEST_URL (disposable instance).
EOF
}

PYTHON="${PULSARCD_PYTHON:-python3}"
if [[ -z "${PULSARCD_PYTHON:-}" && -x .venv/bin/python ]]; then
    PYTHON="$PWD/.venv/bin/python"
fi
# static-checks.sh resolves tools from PATH; keep them in the same environment.
if [[ "$PYTHON" == */* ]]; then
    export PATH="$(dirname "$PYTHON"):$PATH"
fi

python_tests() {
    if [[ $# -eq 0 ]]; then
        "$PYTHON" -m pytest tests/ -q -ra
    else
        "$PYTHON" -m pytest "$@"
    fi
}

frontend_tests() {
    if ! command -v node >/dev/null 2>&1; then
        echo 'Node.js is required for frontend tests; install it before running check.' >&2
        return 1
    fi
    if [[ $# -eq 0 ]]; then
        node --test tests/test_*.cjs
    else
        node --test "$@"
    fi
}

shell_checks() {
    local file
    for file in scripts/*.sh; do
        bash -n "$file" || return 1
    done
}

# Keep independent checks running to report all failures in one agent iteration.
FAILED=()
run_check() {
    local name="$1"
    shift
    echo "==> $name"
    if "$@"; then
        echo "--> $name: passed"
    else
        FAILED+=("$name")
        echo "--> $name: FAILED"
    fi
}

COMMAND="${1:-help}"
if [[ $# -gt 0 ]]; then shift; fi
case "$COMMAND" in
    help|--help|-h) usage ;;
    setup|check|test|static)
        if [[ $# -gt 0 ]]; then
            echo "$COMMAND does not accept arguments; use python or frontend to select tests." >&2
            exit 2
        fi
        case "$COMMAND" in
            setup)
                "$PYTHON" -m venv .venv
                .venv/bin/python -m pip install -r requirements-dev.txt
                .venv/bin/python -m pip check
                ;;
            check|test)
                if [[ "$COMMAND" == check ]]; then
                    run_check static bash scripts/static-checks.sh
                    run_check shell shell_checks
                fi
                run_check python python_tests
                run_check frontend frontend_tests
                if [[ ${#FAILED[@]} -gt 0 ]]; then
                    echo "Checks failed: ${FAILED[*]}" >&2
                    exit 1
                fi
                ;;
            static) bash scripts/static-checks.sh ;;
        esac
        ;;
    python) python_tests "$@" ;;
    frontend) frontend_tests "$@" ;;
    *) usage >&2; exit 2 ;;
esac
