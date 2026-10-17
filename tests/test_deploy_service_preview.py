"""Run deploy-service.sh in --qa and --preview mode with a fake Docker CLI.

The fake records the stack name, the compose file and the environment that
`docker stack deploy` would see, so the tests check the isolation of a preview
from production and QA without a swarm.
"""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from shared.preview_naming import preview_names  # noqa: E402

SCRIPT = ROOT / "scripts" / "deploy-service.sh"
GIT_BASH = Path("C:/Program Files/Git/bin/bash.exe")
BASH = str(GIT_BASH) if GIT_BASH.exists() else shutil.which("bash")

COMPOSE = """services:
  web:
    image: registry.methodinfo.fr/shop/web:latest
    deploy:
      labels:
        - traefik.enable=true
        - traefik.http.routers.shop.rule=Host(`${TRAEFIK_HOST}`)
        - traefik.http.routers.shop.service=shop
        - traefik.http.routers.www.rule=Host(`www.example.com`)
        - traefik.http.services.shop.loadbalancer.server.port=80
networks:
  shop-net:
    name: shop-net
"""

DOCKER_STUB = r"""#!/bin/bash
printf '%s\t' "$@" >> "$DOCKER_LOG"
printf '\n' >> "$DOCKER_LOG"
if [ "$1 $2" = "stack deploy" ]; then
    cp "$4" "$CAPTURE_DIR/compose.yml"
    # docker stack deploy -c <file> <stack> ...
    printf '%s\n' "$5" > "$CAPTURE_DIR/stack_arg"
    printf '%s\n' "$TRAEFIK_HOST" > "$CAPTURE_DIR/traefik_host"
    printf '%s\n' "$DEPLOY_ENV" > "$CAPTURE_DIR/deploy_env"
fi
case "$1 $2" in
    "service inspect") exit 1 ;;
esac
exit 0
"""


@unittest.skipUnless(BASH, "Bash is required")
class DeployServicePreviewTests(unittest.TestCase):
    def deploy(self, *flags, env_qa=None, version="pv-x"):
        with tempfile.TemporaryDirectory(prefix="deploy-preview-") as directory:
            root = Path(directory)
            repo = root / "shop"
            (repo / "devops").mkdir(parents=True)
            (repo / "devops" / "docker-compose.swarm.yml").write_text(COMPOSE, newline="\n")
            (repo / "devops" / ".env").write_text("TRAEFIK_HOST=example.com\n", newline="\n")
            if env_qa is not None:
                (repo / "devops" / ".env.qa").write_text(env_qa, newline="\n")
            bin_dir = root / "bin"
            bin_dir.mkdir()
            docker = bin_dir / "docker"
            docker.write_text(DOCKER_STUB, newline="\n")
            docker.chmod(0o755)
            # The interpreter running the tests (with PyYAML), whatever python3
            # resolves to on this machine.
            python3 = bin_dir / "python3"
            python3.write_text(f'#!/bin/bash\nexec "{Path(sys.executable).as_posix()}" "$@"\n', newline="\n")
            python3.chmod(0o755)
            capture = root / "capture"
            capture.mkdir()
            env = os.environ.copy()
            env.update(DOCKER_LOG=(root / "docker.log").as_posix(), CAPTURE_DIR=capture.as_posix())
            # Paths go through cd/pwd so a Windows bash sees POSIX paths.
            command = ('export PATH="$(cd "$1" && pwd):$PATH"; cd "$2"; script="$3"; shift 3; '
                       'bash "$script" "$@" "$PWD/shop" "$VERSION"')
            env["VERSION"] = version
            result = subprocess.run(
                [BASH, "-c", command, "deploy", bin_dir.as_posix(), root.as_posix(),
                 SCRIPT.as_posix(), *flags],
                env=env, capture_output=True, text=True, timeout=60,
            )
            captured = {}
            for name in ("stack_arg", "traefik_host", "deploy_env"):
                path = capture / name
                captured[name] = path.read_text().strip() if path.exists() else None
            compose_path = capture / "compose.yml"
            captured["compose"] = yaml.safe_load(compose_path.read_text()) if compose_path.exists() else None
            return result, captured

    def labels(self, captured):
        return captured["compose"]["services"]["web"]["deploy"]["labels"]

    def test_preview_gets_its_own_stack_host_and_routers(self):
        names = preview_names("feature/Login", "shop")
        result, captured = self.deploy("--preview", "feature/Login", version=names.image_tag)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(captured["stack_arg"], names.stack)
        self.assertEqual(captured["traefik_host"], f"{names.slug}.example.com")
        self.assertEqual(captured["deploy_env"], "preview")
        labels = self.labels(captured)
        prefix = f"pv-{names.slug}-"
        self.assertIn(f"traefik.http.routers.{prefix}shop.service={prefix}shop", labels)
        self.assertIn(f"traefik.http.routers.{prefix}www.rule=Host(`{names.slug}.www.example.com`)", labels)
        self.assertEqual(captured["compose"]["networks"]["shop-net"]["name"], f"{prefix}shop-net")
        image = captured["compose"]["services"]["web"]["image"]
        self.assertEqual(image, f"registry.methodinfo.fr/shop/web:{names.image_tag}")

    def test_preview_never_reuses_the_qa_host_set_by_env_qa(self):
        names = preview_names("main", "shop")
        result, captured = self.deploy("--preview", "main", env_qa="TRAEFIK_HOST=qa.example.com\n",
                                       version=names.image_tag)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(captured["traefik_host"], "main.qa.example.com")
        self.assertEqual(captured["stack_arg"], "pv-main-shop")

    def test_reserved_branch_cannot_take_the_qa_host(self):
        result, captured = self.deploy("--preview", "qa")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotEqual(captured["stack_arg"], "qa-shop")
        self.assertTrue(captured["stack_arg"].startswith("pv-qa-"))
        self.assertNotEqual(captured["traefik_host"], "qa.example.com")

    def test_unsafe_branch_is_refused_before_docker(self):
        for branch in ("$(id)", "a..b", "-x"):
            with self.subTest(branch=branch):
                result, captured = self.deploy(f"--preview={branch}")
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIsNone(captured["stack_arg"])

    def test_qa_and_preview_are_exclusive(self):
        result, captured = self.deploy("--qa", "--preview", "main")
        self.assertEqual(result.returncode, 1)
        self.assertIsNone(captured["stack_arg"])

    def test_qa_mode_is_unchanged(self):
        result, captured = self.deploy("--qa", version="1.2.3")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(captured["stack_arg"], "qa-shop")
        self.assertEqual(captured["traefik_host"], "qa.example.com")
        self.assertEqual(captured["deploy_env"], "qa")
        self.assertIn("traefik.http.routers.qa-www.rule=Host(`qa.www.example.com`)", self.labels(captured))

    def test_production_mode_is_unchanged(self):
        result, captured = self.deploy(version="1.2.3")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(captured["stack_arg"], "shop")
        self.assertEqual(captured["traefik_host"], "example.com")


if __name__ == "__main__":
    unittest.main()
