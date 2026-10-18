"""Run the build script with fake git/Docker commands, without a registry or daemon.

Also runnable directly: python tests/test_build_push.py
"""

import base64
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build-push.sh"
GIT_BASH = Path("C:/Program Files/Git/bin/bash.exe")
BASH = str(GIT_BASH) if GIT_BASH.exists() else shutil.which("bash")


@unittest.skipUnless(BASH, "Bash is required")
class BuildPushTests(unittest.TestCase):
    def run_build(self, *, existing="", failure="", platforms="", no_cache=False, tag_status=2,
                  registry=None, version="1.2.3", extra_env=None, secrets_copy=None):
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
  "buildx inspect"*)
    printf '%s\\n' 'Name: pulsarcd-multiarch' 'Nodes:' \\
      'Name: pulsarcd-multiarch0' 'Platforms: linux/amd64, linux/arm64*' ;;
  "exec buildx_buildkit_pulsarcd-multiarch0 cat /etc/resolv.conf")
    echo 'nameserver 192.0.2.53' ;;
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
  "compose "*)
    # compose -f COMPOSE -f OVERRIDE build ...: keep the build-secrets override.
    if [ "$4" = "-f" ] && [ -n "$SECRETS_COPY" ]; then cp -r "$(dirname "$5")" "$SECRETS_COPY"; fi ;;
esac
exit 0
''', newline="\n")
            git.chmod(0o755)
            docker.chmod(0o755)
            # Keep registry-list probes local to this script integration test.
            # HTTP/auth/pagination behavior is tested against a real local server
            # in test_registry_tag_exists.py.
            python = bin_dir / "python3"
            python.write_text('''#!/bin/bash
case "$1" in
  */build_secrets.py) exec "$REAL_PYTHON" "$@" ;;
  */registry_tag_exists.py)
    printf 'tag-check\t%s\t\n' "$2" >> "$DOCKER_LOG"
    exit "$STUB_TAG_STATUS" ;;
esac
exit 0
''', newline="\n")
            python.chmod(0o755)
            log = root / "docker.log"
            env = os.environ.copy()
            env.update(DOCKER_LOG=log.as_posix(), EXISTING_IMAGE=existing,
                       FAIL_COMMAND=failure, BUILD_PLATFORMS=platforms,
                       STUB_TAG_STATUS=str(tag_status),
                       REAL_PYTHON=Path(sys.executable).as_posix(),
                       SECRETS_COPY=secrets_copy or "")
            for name in [n for n in env if n.startswith("BUILD_SECRET_")]:
                env.pop(name)
            env.update(extra_env or {})
            # The registry comes from the environment (PulsarCD exports it);
            # without it the script must keep its historical default.
            for name in ("REGISTRY", "REGISTRY_URL", "DOCKER_REGISTRY_URL"):
                env.pop(name, None)
            if registry is not None:
                env["REGISTRY"] = registry
            # Add stubs inside Bash so Git Bash receives a POSIX PATH.
            command = 'export PATH="$(cd "$1" && pwd):$PATH"; cd "$2"; bash "$3" "$PWD" "$5" "" "" "$4"'
            result = subprocess.run(
                [BASH, "-c", command, "test", bin_dir.as_posix(), repo.as_posix(),
                 SCRIPT.as_posix(), "--no-cache" if no_cache else "", version],
                env=env, capture_output=True, text=True, timeout=30,
            )
            calls = [line.rstrip("\t").split("\t") for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_build_secrets_reach_every_build_through_a_build_only_override(self):
        keystore = bytes(range(256))
        with tempfile.TemporaryDirectory() as copy_root:
            copy = Path(copy_root) / "secrets"
            result, calls = self.run_build(
                extra_env={
                    "BUILD_SECRET_KEYSTORE_B64": base64.b64encode(keystore).decode(),
                    "BUILD_SECRET_KEYSTORE_ENV": "KEY_ALIAS=release",
                },
                secrets_copy=copy.as_posix(),
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Build secrets: keystore,keystore_env", result.stdout)
            self.assertNotIn("KEY_ALIAS=release", result.stdout + result.stderr)
            compose, = [c for c in calls if c[0] == "compose"]
            self.assertEqual(compose[1], "-f")
            self.assertEqual(compose[3], "-f")
            self.assertTrue(compose[4].endswith("compose.build-secrets.yml"))
            bake, = [c for c in calls if c[:2] == ["buildx", "bake"]]
            self.assertEqual([a for a in bake if a.endswith("compose.build-secrets.yml")], [compose[4]])
            override = yaml.safe_load((copy / "compose.build-secrets.yml").read_text())
            self.assertEqual(set(override["services"]), {"stt-server", "tts-server", "web"})
            self.assertEqual(override["services"]["web"]["build"]["secrets"], ["keystore", "keystore_env"])
            self.assertEqual((copy / "keystore").read_bytes(), keystore)
            self.assertEqual((copy / "keystore_env").read_text(), "KEY_ALIAS=release")

    def test_no_build_secret_keeps_the_single_compose_file(self):
        result, calls = self.run_build()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        compose, = [c for c in calls if c[0] == "compose"]
        self.assertEqual(compose.count("-f"), 1)

    def test_invalid_base64_build_secret_fails_the_build(self):
        result, calls = self.run_build(extra_env={"BUILD_SECRET_KEYSTORE_B64": "not base64!"})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid build secrets", result.stdout + result.stderr)
        self.assertFalse([c for c in calls if c[0] == "compose"])

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

    def test_registry_from_environment_replaces_default(self):
        result, calls = self.run_build(registry="https://registry.example.com:5000/")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Registry: registry.example.com:5000", result.stdout)
        bake, = [c for c in calls if c[:2] == ["buildx", "bake"]]
        self.assertIn("tts-server.tags=registry.example.com:5000/speech:cuda-arm64", bake)
        self.assertIn(["tag", "registry.example.com:5000/web:latest",
                       "registry.example.com:5000/web:1.2.3"], calls)
        self.assertFalse(any("registry.methodinfo.fr" in arg for c in calls for arg in c))

    def test_existing_rocm_release_does_not_skip_arm(self):
        result, calls = self.run_build(existing="registry.methodinfo.fr/speech:rocm-1.2.3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(c[:2] == ["buildx", "bake"] for c in calls))
        compose, = [c for c in calls if c[0] == "compose"]
        self.assertNotIn("stt-server", compose)

    def test_absent_tag_builds_without_missing_manifest_probe(self):
        result, calls = self.run_build(tag_status=3)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(any(c[:2] == ["manifest", "inspect"] for c in calls))
        self.assertTrue(any(c[:2] == ["buildx", "bake"] for c in calls))
        compose, = [c for c in calls if c[0] == "compose"]
        self.assertEqual(compose[-2:], ["stt-server", "web"])

    def test_listed_tag_still_requires_valid_manifest(self):
        result, calls = self.run_build(tag_status=0, existing="registry.methodinfo.fr/speech:rocm-1.2.3")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len([c for c in calls if c[:2] == ["manifest", "inspect"]]), 3)
        compose, = [c for c in calls if c[0] == "compose"]
        self.assertNotIn("stt-server", compose)
        self.assertIn("web", compose)  # Listed but unavailable manifests must build.

    def test_inconclusive_or_broken_tag_helper_falls_back_to_docker(self):
        for status in (1, 2, 42, 127):
            with self.subTest(status=status):
                result, calls = self.run_build(tag_status=status, existing="registry.methodinfo.fr/speech:rocm-1.2.3")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(len([c for c in calls if c[:2] == ["manifest", "inspect"]]), 3)
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
        self.assertFalse(any(c[0] == "tag-check" or c[:2] == ["manifest", "inspect"] for c in calls))

    def test_build_or_publication_failures_cannot_report_success(self):
        for failure in ("bake", "tag", "push"):
            with self.subTest(failure=failure):
                result, _ = self.run_build(failure=failure)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertNotIn("Build Complete!", result.stdout)

    def test_failed_bake_reports_builder_dns_without_masking_failure(self):
        result, calls = self.run_build(failure="bake")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("nameserver 192.0.2.53", result.stdout)
        self.assertIn(["exec", "buildx_buildkit_pulsarcd-multiarch0",
                       "cat", "/etc/resolv.conf"], calls)
        self.assertFalse(any("--format" in c for c in calls if c[:2] == ["buildx", "inspect"]))

    def test_preview_build_publishes_only_the_preview_tag(self):
        """A branch preview must never push a compose or release tag production uses."""
        result, calls = self.run_build(version="pv-feature-x", tag_status=0,
                                       existing="registry.methodinfo.fr/web:pv-feature-x")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        # The preview tag moves with the branch: an existing image never skips the build
        self.assertFalse(any(c[0] == "tag-check" or c[:2] == ["manifest", "inspect"] for c in calls))
        bake, = [c for c in calls if c[:2] == ["buildx", "bake"]]
        self.assertIn("tts-server.tags=registry.methodinfo.fr/speech:cuda-arm64-pv-feature-x", bake)
        self.assertFalse(any(c[:3] == ["buildx", "imagetools", "create"] for c in calls))
        pushes = [c[1] for c in calls if c[0] == "push"]
        self.assertEqual(sorted(pushes), ["registry.methodinfo.fr/speech:rocm-pv-feature-x",
                                          "registry.methodinfo.fr/web:pv-feature-x"])
        self.assertIn(["tag", "registry.methodinfo.fr/web:latest",
                       "registry.methodinfo.fr/web:pv-feature-x"], calls)
        self.assertIn("Skipping git tagging (preview build)", result.stdout)
        self.assertNotIn("Creating git tag", result.stdout)

    def test_malformed_preview_tag_is_refused(self):
        for version in ("pv-", "pv-Feature", "pv-x;id", "pv--"):
            with self.subTest(version=version):
                result, calls = self.run_build(version=version)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertFalse(any(c[:2] == ["buildx", "bake"] for c in calls))


if __name__ == "__main__":
    unittest.main()
