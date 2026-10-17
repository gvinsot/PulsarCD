"""The configured Docker registry reaches the build/deploy/test scripts.

Covers host normalization, the ``REGISTRY=...`` prefix the backend adds to every
script invocation, and that no script hardcodes the registry anymore.
"""

import asyncio
from pathlib import Path
import re
import shlex
from unittest.mock import AsyncMock, patch

import pytest

from backend.config import GitHubConfig
from backend.github_service import StackDeployer
from backend.registry import (
    DEFAULT_SCRIPT_REGISTRY,
    normalize_registry_host,
    registry_env_prefix,
)
from backend.config_file import TagCleanupConfig
from backend.tag_cleanup import TagCleaner

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ("build-push.sh", "deploy-service.sh", "test.sh")
SSH_URL = "git@github.com:owner/my-app.git"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, ""),
    ("", ""),
    ("   ", ""),
    ("registry.example.com", "registry.example.com"),
    ("registry.example.com/", "registry.example.com"),
    ("https://registry.example.com/", "registry.example.com"),
    ("HTTP://localhost:5000", "localhost:5000"),
    (" registry.example.com:5000/team ", "registry.example.com:5000/team"),
])
def test_normalize_registry_host(raw, expected):
    assert normalize_registry_host(raw) == expected


def test_env_prefix_empty_when_unset():
    assert registry_env_prefix(None) == ""
    assert registry_env_prefix("") == ""


def test_env_prefix_quotes_value():
    assert registry_env_prefix("https://reg.example.com/") == "REGISTRY=reg.example.com "
    # Anything a shell could interpret stays inside a single word.
    prefix = registry_env_prefix("evil.com; rm -rf ~")
    assert shlex.split(prefix) == ["REGISTRY=evil.com; rm -rf ~"]


# ---------------------------------------------------------------------------
# StackDeployer passes REGISTRY to the scripts
# ---------------------------------------------------------------------------

def _run_action(registry_url, action):
    """Run a deployer action with every shell call captured; return the script command."""
    deployer = StackDeployer(GitHubConfig(repos_path="/srv/repos", registry_url=registry_url))
    commands = []

    async def fake_run(command, output_callback=None, cancel_event=None):
        commands.append(command)
        return True, ""

    async def go():
        with patch.object(deployer, "_run_command", side_effect=fake_run), \
             patch.object(deployer, "_ensure_docker_login", AsyncMock()), \
             patch.object(deployer, "_ensure_repo_cloned", AsyncMock(return_value=(True, ""))), \
             patch("backend.recovery.snapshot_envs", AsyncMock(return_value=0)):
            if action == "build":
                return await deployer.build("my-app", SSH_URL, version="1.2", branch="main")
            if action == "deploy":
                return await deployer.deploy("my-app", SSH_URL, version="1.2.3")
            return await deployer.test("my-app", SSH_URL, branch="main")

    result = asyncio.run(go())
    assert result["success"], result
    script_cmds = [c for c in commands if re.search(r"bash (build-push|deploy-service|test)\.sh", c)]
    assert len(script_cmds) == 1, commands
    return script_cmds[0]


@pytest.mark.parametrize("action, script", [
    ("build", "build-push.sh"),
    ("deploy", "deploy-service.sh"),
    ("test", "test.sh"),
])
def test_script_receives_configured_registry(action, script):
    cmd = _run_action("https://registry.example.com:5000/", action)
    # The assignment sits right before the script so it survives SSH and `su -c`.
    assert f"&& REGISTRY=registry.example.com:5000 bash {script} " in cmd


@pytest.mark.parametrize("action", ["build", "deploy", "test"])
def test_script_uses_its_default_when_registry_unset(action):
    cmd = _run_action(None, action)
    assert "REGISTRY=" not in cmd


def test_command_exports_registry_to_bash(tmp_path):
    """The generated prefix really sets the variable for the invoked script."""
    import shutil
    import subprocess
    bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(bash) if bash.exists() else shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required")
    (tmp_path / "show.sh").write_text('echo "[$REGISTRY]"\n', newline="\n")
    cmd = f"cd {shlex.quote(tmp_path.as_posix())} && {registry_env_prefix('reg.example.com')}bash show.sh"
    out = subprocess.run([bash, "-c", cmd], capture_output=True, text=True, timeout=30)
    assert out.stdout.strip() == "[reg.example.com]", out.stderr


# ---------------------------------------------------------------------------
# Tag cleanup
# ---------------------------------------------------------------------------

def test_tag_cleanup_normalizes_registry_url():
    cleaner = TagCleaner(TagCleanupConfig(), github_service=None,
                         registry_url="https://registry.example.com/")
    assert cleaner._registry_url == "registry.example.com"


# ---------------------------------------------------------------------------
# Scripts no longer hardcode the registry
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("script", SCRIPTS)
def test_scripts_take_registry_from_environment(script):
    text = (ROOT / "scripts" / script).read_text(encoding="utf-8")
    assert f'REGISTRY="${{REGISTRY:-{DEFAULT_SCRIPT_REGISTRY}}}"' in text
    # The default appears only in that fallback (and in header comments).
    code_lines = [l for l in text.splitlines()
                  if DEFAULT_SCRIPT_REGISTRY in l and not l.lstrip().startswith("#")]
    assert code_lines == [f'REGISTRY="${{REGISTRY:-{DEFAULT_SCRIPT_REGISTRY}}}"']


def test_backend_does_not_hardcode_registry():
    offenders = [p.name for p in (ROOT / "backend").glob("*.py")
                 if p.name != "registry.py" and DEFAULT_SCRIPT_REGISTRY in p.read_text(encoding="utf-8")]
    assert offenders == []
