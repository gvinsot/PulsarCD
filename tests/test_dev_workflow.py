"""Exercise the development entry point without installing tools or deploying."""

import os
from pathlib import Path
import shutil
import subprocess

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "dev.sh"
BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(not BASH, reason="Bash is required")


@pytest.fixture
def workspace(tmp_path):
    # Spaces exercise quoting in both repository and executable paths.
    root = tmp_path / "agent workspace"
    scripts = root / "scripts"
    scripts.mkdir(parents=True)
    (scripts / "dev.sh").write_bytes(SCRIPT.read_bytes())
    (scripts / "static-checks.sh").write_text(
        '#!/bin/bash\necho static >> "$CALL_LOG"\nexit "${STATIC_EXIT:-0}"\n'
    )
    tests = root / "tests"
    tests.mkdir()
    (tests / "test_example.cjs").touch()
    bin_dir = root / "bin"
    bin_dir.mkdir()
    for tool, code in (("python", "PYTHON_EXIT"), ("node", "NODE_EXIT")):
        stub = bin_dir / tool
        stub.write_text(
            f'#!/bin/bash\nprintf "{tool}" >> "$CALL_LOG"\n'
            'printf " <%s>" "$@" >> "$CALL_LOG"\n'
            'printf "\\n" >> "$CALL_LOG"\n'
            f'exit "${{{code}:-0}}"\n'
        )
        stub.chmod(0o755)
    log = root / "calls.log"

    def run(*args, **overrides):
        env = dict(os.environ, CALL_LOG=str(log),
                   PULSARCD_PYTHON=str(bin_dir / "python"))
        env.update(overrides)
        # Prefix PATH inside Bash, also allowing paths with spaces.
        result = subprocess.run(
            [BASH, "-c", 'export PATH="$1:$PATH"; shift; bash "$@"',
             "dev-test", str(bin_dir), str(scripts / "dev.sh"), *args],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30,
        )
        return result, log.read_text().splitlines() if log.exists() else []

    return root, run


def test_check_runs_all_suites_from_outside_repository(workspace):
    _, run = workspace
    result, calls = run("check")
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls == ["static", "python <-m> <pytest> <tests/> <-q> <-ra>",
                     "node <--test> <tests/test_example.cjs>"]


@pytest.mark.parametrize("failure", ["STATIC_EXIT", "PYTHON_EXIT", "NODE_EXIT"])
def test_failures_are_blocking_and_other_checks_still_run(workspace, failure):
    _, run = workspace
    result, calls = run("check", **{failure: "1"})
    assert result.returncode == 1
    assert len(calls) == 3
    assert "Checks failed:" in result.stderr


def test_shell_syntax_failure_is_blocking(workspace):
    root, run = workspace
    (root / "scripts" / "broken.sh").write_text("if then\n")
    result, calls = run("check")
    assert result.returncode == 1
    assert "Checks failed: shell" in result.stderr
    assert len(calls) == 3


def test_python_selection_preserves_argument_boundaries_and_exit_code(workspace):
    _, run = workspace
    result, calls = run("python", "tests/test_api.py", "-k", "viewer or admin",
                        PYTHON_EXIT="5")
    assert result.returncode == 5
    assert calls == ["python <-m> <pytest> <tests/test_api.py> <-k> <viewer or admin>"]


def test_frontend_selection_preserves_exit_code(workspace):
    _, run = workspace
    result, calls = run("frontend", "tests/test_example.cjs", NODE_EXIT="7")
    assert result.returncode == 7
    assert calls == ["node <--test> <tests/test_example.cjs>"]


def test_missing_node_fails_instead_of_skipping_frontend(workspace):
    root, run = workspace
    bin_dir = root / "bin"
    (bin_dir / "node").unlink()
    for name in ("bash", "dirname"):
        (bin_dir / name).symlink_to(shutil.which(name))
    result, calls = run("frontend", PATH=str(bin_dir))
    assert result.returncode == 1
    assert "Node.js is required" in result.stderr
    assert not calls


def test_local_venv_is_selected_without_activation(workspace):
    root, run = workspace
    bin_dir = root / ".venv" / "bin"
    bin_dir.mkdir(parents=True)
    interpreter = bin_dir / "python"
    interpreter.write_text("#!/bin/bash\nexit 9\n")
    interpreter.chmod(0o755)
    result, calls = run("python", PULSARCD_PYTHON="")
    assert result.returncode == 9
    assert not calls


def test_test_command_omits_static_checks(workspace):
    _, run = workspace
    result, calls = run("test")
    assert result.returncode == 0
    assert len(calls) == 2
    assert "static" not in calls


@pytest.mark.parametrize("args", [("unknown",), ("check", "unexpected"),
                                 ("setup", "unexpected")])
def test_invalid_commands_fail_without_running_tools(workspace, args):
    _, run = workspace
    result, calls = run(*args)
    assert result.returncode == 2
    assert not calls


def test_help_has_no_side_effects(workspace):
    _, run = workspace
    result, calls = run("help")
    assert result.returncode == 0
    assert "Usage:" in result.stdout
    assert not calls
