"""The static-check gate: its configuration, its build wiring and its script.

The script runs with fake ruff/mypy/pip-audit commands, so these tests need
Bash but none of the real tools. The real tools run in the Dockerfile "test"
stage, which fails the build when they report an error.
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "static-checks.sh"
PYPROJECT = ROOT / "pyproject.toml"
DOCKERFILE = ROOT / "Dockerfile"
GIT_BASH = Path("C:/Program Files/Git/bin/bash.exe")
BASH = str(GIT_BASH) if GIT_BASH.exists() else shutil.which("bash")


class StaticCheckConfigTests(unittest.TestCase):
    def setUp(self):
        if not PYPROJECT.is_file():
            self.skipTest("pyproject.toml is not part of this test environment")
        self.config = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["tool"]

    def test_ruff_keeps_the_bug_finding_rules(self):
        lint = self.config["ruff"]["lint"]
        # F821 (undefined name) found a NameError in the API startup path.
        self.assertIn("F", lint["select"])
        self.assertIn("E9", lint["select"])
        self.assertNotIn("F821", lint.get("ignore", []))

    def test_mypy_is_permissive_but_checks_attributes_and_calls(self):
        mypy = self.config["mypy"]
        self.assertTrue(mypy["ignore_missing_imports"])
        self.assertEqual(set(mypy["files"]), {"backend", "agent", "shared"})
        disabled = set(mypy["disable_error_code"])
        # These codes found an AttributeError in tag cleanup and an unbound
        # variable read; disabling them would hollow out the gate.
        for code in ("attr-defined", "name-defined", "call-arg", "misc"):
            self.assertNotIn(code, disabled)
        self.assertNotIn("strict", mypy)


class StaticCheckBuildWiringTests(unittest.TestCase):
    def setUp(self):
        if not DOCKERFILE.is_file():
            self.skipTest("Dockerfile is not part of this test environment")
        text = DOCKERFILE.read_text(encoding="utf-8")
        start = text.index("AS test")
        self.test_stage = text[start:text.index("AS production")]

    def test_test_stage_runs_the_script_at_build_time(self):
        self.assertIn("COPY pyproject.toml", self.test_stage)
        self.assertIn("COPY scripts/static-checks.sh", self.test_stage)
        self.assertRegex(self.test_stage, r"RUN .*bash scripts/static-checks\.sh")

    def test_tool_versions_are_pinned(self):
        for tool in ("ruff", "mypy", "pip-audit"):
            self.assertRegex(self.test_stage, rf'"{tool}==[0-9][^"]*"')


@unittest.skipUnless(BASH, "Bash is required")
class StaticCheckScriptTests(unittest.TestCase):
    def run_script(self, *, ruff=0, mypy=0, audit=0, skip_audit=False):
        self.assertTrue(SCRIPT.is_file(), f"Script missing from test environment: {SCRIPT}")
        with tempfile.TemporaryDirectory(prefix="static checks ") as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            script = root / "scripts" / "static-checks.sh"
            script.write_bytes(SCRIPT.read_bytes().replace(b"\r\n", b"\n"))
            bin_dir = root / "bin"
            bin_dir.mkdir()
            log = root / "calls.log"
            for name, code in (("ruff", ruff), ("mypy", mypy), ("pip-audit", audit)):
                stub = bin_dir / name
                stub.write_text(
                    f'#!/bin/bash\necho "{name} $*" >> "$CALL_LOG"\nexit {code}\n', newline="\n"
                )
                stub.chmod(0o755)
            env = dict(os.environ)
            env["CALL_LOG"] = str(log)
            env.pop("PULSARCD_SKIP_AUDIT", None)
            if skip_audit:
                env["PULSARCD_SKIP_AUDIT"] = "1"
            # Add stubs inside Bash so Git Bash receives a POSIX PATH.
            command = 'export PATH="$(cd "$1" && pwd):$PATH"; bash "$2"'
            result = subprocess.run(
                [BASH, "-c", command, "static-checks", str(bin_dir), str(script)],
                cwd=root, env=env, capture_output=True, text=True, timeout=60,
            )
            calls = log.read_text().splitlines() if log.exists() else []
            return result, calls

    def test_all_checks_pass(self):
        result, calls = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual([call.split()[0] for call in calls], ["ruff", "mypy", "pip-audit"])
        self.assertIn("Static checks passed", result.stdout)

    def test_ruff_failure_fails_but_mypy_still_runs(self):
        result, calls = self.run_script(ruff=1)
        self.assertEqual(result.returncode, 1)
        self.assertIn("mypy", [call.split()[0] for call in calls])
        self.assertIn("Static checks failed: ruff", result.stdout)

    def test_mypy_failure_fails(self):
        result, _ = self.run_script(mypy=1)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Static checks failed: mypy", result.stdout)

    def test_audit_findings_only_warn(self):
        result, _ = self.run_script(audit=1)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("pip-audit: WARNING", result.stdout)

    def test_audit_can_be_skipped_offline(self):
        result, calls = self.run_script(skip_audit=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("pip-audit", [call.split()[0] for call in calls])


if __name__ == "__main__":
    unittest.main()
