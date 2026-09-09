"""Run the build script with fake git/Docker commands, without a registry or daemon.

Also runnable directly: python tests/test_build_push.py
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build-push.sh"
GIT_BASH = Path("C:/Program Files/Git/bin/bash.exe")
BASH = str(GIT_BASH) if GIT_BASH.exists() else shutil.which("bash")


@unittest.skipUnless(BASH, "Bash is required")
class BuildPushTests(unittest.TestCase):
    def run_build(self, *, existing="", failure="", platforms="", no_cache=False):
        self.assertTrue(SCRIPT.is_file(), f"Build script missing from test environment: {SCRIPT}")
        with tempfile.TemporaryDirectory(prefix="build push ") as directory:
            root = Path(directory)
            repo = root / "repo"
            (repo / ".git").mkdir(parents=True)
            (repo / "devops").mkdir()
            (repo / "devops" / "docker-compose.swarm.yml").write_text('''services:
  stt-server:
    image: ${REGISTRY}/speech:rocm
    build:
      context: ../speech
  tts-server:
    image: ${REGISTRY}/speech:cuda-arm64
    x-platforms: linux/arm64
    build:
      context: ../speech
      args:
        BASE_IMAGE: cuda-base
        TORCH_INDEX_URL: ""
  web:
    image: ${REGISTRY}/web:latest
    build:
      context: ../web
''')
            bin_dir = root / "bin"
            bin_dir.mkdir()
            git = bin_dir / "git"
            git.write_text('''#!/bin/bash
case "$*" in
  "rev-parse --abbrev-ref HEAD") echo main ;;
  "rev-parse --short HEAD") echo abc123 ;;
  "rev-parse HEAD") echo abc123456 ;;
esac
exit 0
''', newline="\n")
            docker = bin_dir / "docker"
            docker.write_text('''#!/bin/bash
printf '%s\t' "$@" >> "$DOCKER_LOG"
printf '\n' >> "$DOCKER_LOG"
case "$*" in
  "buildx inspect"*) echo 'Platforms: linux/amd64, linux/arm64*' ;;
  "manifest inspect "*) [ "$3" = "$EXISTING_IMAGE" ]; exit $? ;;
  "buildx bake "*)
    allowed=false
    for arg in "$@"; do
      [ "$arg" = "--allow=fs.read=$(cd .. && pwd)" ] && allowed=true
    done
    if [ "$allowed" != true ]; then
      echo 'ERROR: additional privileges requested: Read access to path ..' >&2
      exit 1
    fi
    [ "$FAIL_COMMAND" != bake ]; exit $? ;;
  "buildx imagetools create "*) [ "$FAIL_COMMAND" != tag ]; exit $? ;;
  "push "*) [ "$FAIL_COMMAND" != push ]; exit $? ;;
esac
exit 0
''', newline="\n")
            git.chmod(0o755)
            docker.chmod(0o755)
            log = root / "docker.log"
            env = os.environ.copy()
            env.update(DOCKER_LOG=log.as_posix(), EXISTING_IMAGE=existing,
                       FAIL_COMMAND=failure, BUILD_PLATFORMS=platforms)
            # Add stubs inside Bash so Git Bash receives a POSIX PATH.
            command = 'export PATH="$(cd "$1" && pwd):$PATH"; cd "$2"; bash "$3" "$PWD" 1.2.3 "" "" "$4"'
            result = subprocess.run(
                [BASH, "-c", command, "test", bin_dir.as_posix(), repo.as_posix(),
                 SCRIPT.as_posix(), "--no-cache" if no_cache else ""],
                env=env, capture_output=True, text=True, timeout=30,
            )
            calls = [line.rstrip("\t").split("\t") for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_arm_build_uses_compose_builder_platform_and_exact_tag(self):
        result, calls = self.run_build()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        bake, = [c for c in calls if c[:2] == ["buildx", "bake"]]
        self.assertIn("pulsarcd-multiarch", bake)
        self.assertIn("tts-server.platform=linux/arm64", bake)
        self.assertIn("tts-server.tags=registry.methodinfo.fr/speech:cuda-arm64", bake)
        self.assertIn("--push", bake)
        self.assertEqual(bake[-1], "tts-server")
        self.assertTrue(bake[bake.index("-f") + 1].endswith("devops/docker-compose.swarm.yml"))
        compose, = [c for c in calls if c[0] == "compose"]
        self.assertEqual(compose[-2:], ["stt-server", "web"])
        aliases = [c for c in calls if c[:3] == ["buildx", "imagetools", "create"]]
        self.assertTrue(aliases)
        for alias in aliases:
            self.assertEqual(alias[-1], "registry.methodinfo.fr/speech:cuda-arm64")
            self.assertIn("speech:cuda-arm64-", alias[4])
        self.assertIn(["tag", "registry.methodinfo.fr/speech:rocm",
                       "registry.methodinfo.fr/speech:rocm-1.2.3"], calls)
        self.assertIn(["tag", "registry.methodinfo.fr/web:latest",
                       "registry.methodinfo.fr/web:1.2.3"], calls)

    def test_existing_rocm_release_does_not_skip_arm(self):
        result, calls = self.run_build(existing="registry.methodinfo.fr/speech:rocm-1.2.3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(c[:2] == ["buildx", "bake"] for c in calls))
        compose, = [c for c in calls if c[0] == "compose"]
        self.assertNotIn("stt-server", compose)

    def test_bake_can_read_parent_context_with_spaces(self):
        result, calls = self.run_build()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        bake, = [c for c in calls if c[:2] == ["buildx", "bake"]]
        compose_path = bake[bake.index("-f") + 1]
        repo_path = compose_path.rsplit("/devops/", 1)[0]
        self.assertIn(" ", repo_path)
        self.assertEqual([arg for arg in bake if arg.startswith("--allow")],
                         [f"--allow=fs.read={repo_path}"])

    def test_per_service_arm_platform_overrides_global(self):
        result, calls = self.run_build(platforms="linux/amd64")
        self.assertEqual(result.returncode, 0, result.stderr)
        arm, = [c for c in calls if c[:2] == ["buildx", "bake"] and c[-1] == "tts-server"]
        self.assertIn("tts-server.platform=linux/arm64", arm)

    def test_no_cache_forces_existing_image_to_build(self):
        result, calls = self.run_build(existing="registry.methodinfo.fr/speech:cuda-arm64-1.2.3", no_cache=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        bake, = [c for c in calls if c[:2] == ["buildx", "bake"]]
        self.assertIn("--no-cache", bake)

    def test_build_or_publication_failures_cannot_report_success(self):
        for failure in ("bake", "tag", "push"):
            with self.subTest(failure=failure):
                result, _ = self.run_build(failure=failure)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertNotIn("Build Complete!", result.stdout)


if __name__ == "__main__":
    unittest.main()
