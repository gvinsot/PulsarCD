"""Post-deploy verification scripts, run against a fake docker/curl on PATH.

Covers scripts/verify-deploy.sh (convergence and health rules) and its wiring
into scripts/deploy-service.sh (exit code, marker line, step ordering), without
a swarm, a registry or a network.

Also runnable directly: python tests/test_deploy_verify.py
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
VERIFY = SCRIPTS / "verify-deploy.sh"
DEPLOY = SCRIPTS / "deploy-service.sh"
GIT_BASH = Path("C:/Program Files/Git/bin/bash.exe")
BASH = str(GIT_BASH) if GIT_BASH.exists() else shutil.which("bash")

MARKER = "PULSARCD_DEPLOY_VERIFICATION_FAILED"
IMAGE = "registry.methodinfo.fr/app:42"
OLD_IMAGE = "registry.methodinfo.fr/app:41"

# Fake docker: answers come from files in $STUB_DIR. For "service inspect" and
# "service ps", <kind>_<service>.<n> is used on the n-th call when it exists,
# else <kind>_<service>, so a test can script how a service evolves over polls.
DOCKER_STUB = r'''#!/bin/bash
printf '%s\t' "$@" >> "$STUB_DIR/calls.log"
printf '\n' >> "$STUB_DIR/calls.log"
answer() {
    local key="$1" count_file="$STUB_DIR/count_$1" n=0
    [ -f "$count_file" ] && n=$(cat "$count_file")
    n=$((n + 1))
    echo "$n" > "$count_file"
    if [ -f "$STUB_DIR/$key.$n" ]; then cat "$STUB_DIR/$key.$n"; return 0; fi
    if [ -f "$STUB_DIR/$key" ]; then cat "$STUB_DIR/$key"; return 0; fi
    return 1
}
case "$1 $2" in
  "stack services") cat "$STUB_DIR/services" 2>/dev/null; exit 0 ;;
  "stack ps") cat "$STUB_DIR/baseline" 2>/dev/null; exit 0 ;;
  "stack deploy") exit 0 ;;
  "service inspect") answer "inspect_$3"; exit $? ;;
  "service ps") answer "ps_$3"; exit $? ;;
  "service logs") echo "app: boom, exiting"; exit 0 ;;
  "service update") exit 0 ;;
  "pull "*|"tag "*) exit 0 ;;
esac
case "$1" in
  pull|tag) exit 0 ;;
  images) echo "sha256:deadbeef"; exit 0 ;;
  image) echo "registry.methodinfo.fr/app@sha256:cafe"; exit 0 ;;
esac
exit 0
'''

# Fake curl: prints the next status code listed in $STUB_DIR/curl_statuses
# (the last one repeats), in the shape of the -w format it was given. The body
# is $STUB_DIR/curl_body when present, written to the -o file if one is given.
CURL_STUB = r'''#!/bin/bash
echo "$*" >> "$STUB_DIR/curl.log"
# Linux refuses to start any program whose environment holds one variable over
# 128 KiB ("Argument list too long"); Windows does not, so record them instead.
awk 'BEGIN { for (k in ENVIRON) if (length(ENVIRON[k]) > 131072) print k }' >> "$STUB_DIR/big_env"
n=0
[ -f "$STUB_DIR/count_curl" ] && n=$(cat "$STUB_DIR/count_curl")
n=$((n + 1)); echo "$n" > "$STUB_DIR/count_curl"
status=$(sed -n "${n}p" "$STUB_DIR/curl_statuses")
[ -z "$status" ] && status=$(tail -n 1 "$STUB_DIR/curl_statuses")
out=""
prev=""
for arg in "$@"; do
  [ "$prev" = "-o" ] && out="$arg"
  prev="$arg"
done
if [ -n "$out" ]; then
  if [ -f "$STUB_DIR/curl_body" ]; then cp "$STUB_DIR/curl_body" "$out"; else printf 'body' > "$out"; fi
fi
case "$*" in
  *HTTP_STATUS*)
    if [ -f "$STUB_DIR/curl_body" ]; then cat "$STUB_DIR/curl_body"; else printf 'body'; fi
    printf '\nHTTP_STATUS:%s' "$status" ;;
  *) printf '%s' "$status" ;;
esac
'''

# deploy-service.sh runs process_secrets.py through python3: just copy.
PYTHON_STUB = r'''#!/bin/bash
case "$1" in
  */process_secrets.py) cp "$2" "$3" ;;
esac
exit 0
'''


def inspect_line(mode="2", image=IMAGE, update_state="", started="", restart="any"):
    """One line as produced by verify-deploy.sh's `service inspect` template."""
    update = f"{update_state}|{started}" if update_state else ""
    return f"{mode}|{image}@sha256:abc|{restart}|{update}\n"


def task(task_id, state="Running 5 seconds ago", desired="Running", image=IMAGE, error=""):
    return f"{task_id}|{image}@sha256:abc|{desired}|{state}|{error}\n"


@unittest.skipUnless(BASH, "Bash is required")
class _StubbedBash(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="deploy verify ")
        self.root = Path(self._tmp.name)
        self.stub = self.root / "stub"
        self.stub.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, body in (("docker", DOCKER_STUB), ("curl", CURL_STUB), ("python3", PYTHON_STUB)):
            path = self.bin / name
            path.write_text(body, newline="\n")
            path.chmod(0o755)
        self.env = os.environ.copy()
        self.env.update(
            STUB_DIR=self.stub.as_posix(),
            DEPLOY_CONVERGE_INTERVAL="0",
            DEPLOY_CONVERGE_STABLE="0",
            DEPLOY_CONVERGE_TIMEOUT="5",
            DEPLOY_HEALTHCHECK_INTERVAL="0",
            DEPLOY_HEALTHCHECK_TIMEOUT="0",
        )

    def tearDown(self):
        self._tmp.cleanup()

    def write(self, name, content):
        (self.stub / name).write_text(content, newline="\n")

    def services(self, *names):
        self.write("services", "".join(f"{n}\n" for n in names))

    def bash(self, command, *args, env=None):
        full_env = dict(self.env, **(env or {}))
        # Prepend the stubs inside Bash so Git Bash receives a POSIX PATH.
        script = 'export PATH="$(cd "$1" && pwd):$PATH"; shift; ' + command
        return subprocess.run(
            [BASH, "-c", script, "test", self.bin.as_posix(), *args],
            env=full_env, capture_output=True, text=True, timeout=60,
        )


class ConvergeTests(_StubbedBash):
    def converge(self, baseline=None, start_epoch="", env=None):
        baseline_path = ""
        if baseline is not None:
            baseline_path = (self.stub / "baseline_file").as_posix()
            self.write("baseline_file", "".join(f"{t}\n" for t in baseline))
        return self.bash('bash "$1" converge app "$2" "$3"',
                         VERIFY.as_posix(), baseline_path, start_epoch, env=env)

    def test_all_replicas_running_new_image_converges(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("2", update_state="completed", started="1000"))
        self.write("ps_app_web", task("t1") + task("t2") + task("t0", "Shutdown 1 minute ago", "Shutdown", OLD_IMAGE))
        result = self.converge()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("converged", result.stdout)

    def test_crash_looping_new_task_fails_fast_with_logs(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web",
                   task("t2", "Starting 1 second ago")
                   + task("t1", "Failed 3 seconds ago", "Shutdown", error="task: non-zero exit (1)"))
        result = self.converge(baseline=["t0"])
        self.assertEqual(result.returncode, 3, result.stdout)
        self.assertIn("1 new task(s) failed", result.stdout)
        self.assertIn("non-zero exit (1)", result.stdout)
        self.assertIn("app: boom, exiting", result.stdout)

    def test_rejected_task_counts_as_failure(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web", task("t1", "Rejected 2 seconds ago", "Shutdown", error="No such image"))
        result = self.converge(baseline=[])
        self.assertEqual(result.returncode, 3, result.stdout)
        self.assertIn("Rejected: No such image", result.stdout)

    def test_failures_from_before_the_deploy_are_ignored(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web",
                   task("t2") + task("t1", "Failed 2 hours ago", "Shutdown", OLD_IMAGE, "old crash"))
        result = self.converge(baseline=["t1"])
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_failure_budget_is_configurable(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web", task("t2") + task("t1", "Failed 9 seconds ago", "Shutdown", error="oom"))
        result = self.converge(baseline=[], env={"DEPLOY_CONVERGE_MAX_FAILURES": "1"})
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_one_shot_service_that_completed_converges(self):
        # e.g. a migration service: replicas 1, restart_policy condition none
        self.services("app_migrate")
        self.write("inspect_app_migrate", inspect_line("1", restart="none"))
        self.write("ps_app_migrate",
                   task("m1", "Complete 3 seconds ago", "Shutdown")
                   + task("m0", "Complete 2 days ago", "Shutdown", OLD_IMAGE))
        result = self.converge(baseline=["m0"])
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_one_shot_service_still_pending_does_not_converge(self):
        self.services("app_migrate")
        self.write("inspect_app_migrate", inspect_line("1", restart="on-failure"))
        self.write("ps_app_migrate",
                   task("m1", "Preparing 1 second ago")
                   + task("m0", "Complete 2 days ago", "Shutdown"))
        result = self.converge(baseline=["m0"], env={"DEPLOY_CONVERGE_TIMEOUT": "0"})
        self.assertEqual(result.returncode, 3, result.stdout)

    def test_completed_task_with_restart_any_does_not_converge(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web", task("t1", "Complete 3 seconds ago", "Shutdown"))
        result = self.converge(baseline=[], env={"DEPLOY_CONVERGE_TIMEOUT": "0"})
        self.assertEqual(result.returncode, 3, result.stdout)

    def test_waits_for_rolling_update_then_converges(self):
        self.services("app_web")
        self.write("inspect_app_web.1", inspect_line("2", update_state="updating", started="2000"))
        self.write("inspect_app_web", inspect_line("2", update_state="completed", started="2000"))
        # Start-first rollout: one old task still running next to the new ones.
        self.write("ps_app_web.1", task("n1") + task("o1", image=OLD_IMAGE) + task("o2", image=OLD_IMAGE))
        self.write("ps_app_web.2", task("n1") + task("n2", "Starting 1 second ago") + task("o2", image=OLD_IMAGE))
        self.write("ps_app_web", task("n1") + task("n2"))
        result = self.converge(baseline=["o1", "o2"], start_epoch="2000")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("waiting: app_web", result.stdout)

    def test_old_image_still_running_is_not_converged(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web", task("o1", image=OLD_IMAGE))
        result = self.converge(env={"DEPLOY_CONVERGE_TIMEOUT": "0"})
        self.assertEqual(result.returncode, 3, result.stdout)
        self.assertIn("did not converge within 0s", result.stdout)
        self.assertIn("0/1 running", result.stdout)

    def test_stability_window_catches_a_late_crash(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web.1", task("n1"))
        self.write("ps_app_web", task("n2", "Starting 1 second ago")
                   + task("n1", "Failed 1 second ago", "Shutdown", error="exit 137"))
        result = self.converge(baseline=[], env={"DEPLOY_CONVERGE_STABLE": "30"})
        self.assertEqual(result.returncode, 3, result.stdout)
        self.assertIn("exit 137", result.stdout)

    def test_zero_replicas_converge_once_old_tasks_are_gone(self):
        self.services("app_worker")
        self.write("inspect_app_worker", inspect_line("0"))
        self.write("ps_app_worker", task("o1", "Shutdown 2 seconds ago", "Shutdown", OLD_IMAGE))
        result = self.converge(baseline=["o1"])
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_global_service_needs_every_scheduled_task_running(self):
        self.services("app_agent")
        self.write("inspect_app_agent", inspect_line("global"))
        self.write("ps_app_agent.1", task("a1") + task("a2", "Preparing 1 second ago") + task("a3"))
        self.write("ps_app_agent", task("a1") + task("a2") + task("a3"))
        result = self.converge(baseline=[])
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("2/3 running", result.stdout)

    def test_job_service_is_only_checked_for_failures(self):
        self.services("app_migrate", "app_web")
        self.write("inspect_app_migrate", inspect_line("job"))
        self.write("ps_app_migrate", task("j1", "Complete 3 seconds ago", "Complete"))
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web", task("w1"))
        result = self.converge(baseline=[])
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_swarm_rollback_started_by_this_deploy_fails(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1", update_state="rollback_completed", started="5000"))
        self.write("ps_app_web", task("o1", image=IMAGE))
        result = self.converge(baseline=[], start_epoch="5000")
        self.assertEqual(result.returncode, 3, result.stdout)
        self.assertIn("swarm update rollback_completed", result.stdout)

    def test_stale_paused_update_from_an_earlier_deploy_is_ignored(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1", update_state="paused", started="1000"))
        self.write("ps_app_web", task("t1"))
        result = self.converge(baseline=[], start_epoch="5000")
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_stack_without_services_fails(self):
        self.services()
        result = self.converge()
        self.assertEqual(result.returncode, 3, result.stdout)

    def test_invalid_tunable_falls_back_to_default(self):
        self.services("app_web")
        self.write("inspect_app_web", inspect_line("1"))
        self.write("ps_app_web", task("t1"))
        result = self.converge(env={"DEPLOY_CONVERGE_TIMEOUT": "3m"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Waiting up to 180s", result.stdout)
        self.assertIn("Ignoring invalid DEPLOY_CONVERGE_TIMEOUT", result.stderr)


class HealthTests(_StubbedBash):
    def test_health_retries_until_2xx(self):
        self.write("curl_statuses", "502\n503\n200\n")
        result = self.bash('bash "$1" health https://app.example.com/health', VERIFY.as_posix(),
                           env={"DEPLOY_HEALTHCHECK_TIMEOUT": "30"})
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("HTTP 200", result.stdout)

    def test_health_fails_with_its_own_code(self):
        self.write("curl_statuses", "500\n")
        result = self.bash('bash "$1" health https://app.example.com/health', VERIFY.as_posix())
        self.assertEqual(result.returncode, 4, result.stdout)
        self.assertIn("answered HTTP 500", result.stdout)


class DeployServiceVerificationTests(_StubbedBash):
    """deploy-service.sh end to end, on a non-git project folder."""

    def setUp(self):
        super().setUp()
        self.repo = self.root / "repo"
        devops = self.repo / "devops"
        devops.mkdir(parents=True)
        (devops / "docker-compose.swarm.yml").write_text('''services:
  web:
    image: registry.methodinfo.fr/app:latest
    deploy:
      labels:
        - "traefik.http.routers.app.rule=Host(`app.example.com`)"
''', newline="\n")
        # The post-deploy hook leaves a trace so tests can tell whether it ran.
        (devops / "docker-compose.post.sh").write_text(
            'touch "$STUB_DIR/post_ran"\n', newline="\n")
        self.services("repo_web")

    def deploy(self, env=None):
        return self.bash('cd "$1"; bash "$2" "$PWD" 42', self.repo.as_posix(),
                         DEPLOY.as_posix(), env=env)

    def test_converged_deploy_succeeds_and_runs_post_hook(self):
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1"))
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Deployment Complete", result.stdout)
        self.assertNotIn(MARKER, result.stdout)
        self.assertTrue((self.stub / "post_ran").exists())
        # The baseline is captured before the deploy is submitted.
        calls = (self.stub / "calls.log").read_text().splitlines()
        ps = next(i for i, c in enumerate(calls) if c.startswith("stack\tps"))
        deploy = next(i for i, c in enumerate(calls) if c.startswith("stack\tdeploy"))
        self.assertLess(ps, deploy)

    def test_crash_loop_exits_3_with_marker_and_skips_post_hook(self):
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1", "Failed 1 second ago", "Shutdown", error="exit 1"))
        result = self.deploy()
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn(f"{MARKER} check=converge stack=repo version=42", result.stdout)
        self.assertNotIn("Deployment Complete", result.stdout)
        self.assertFalse((self.stub / "post_ran").exists())
        self.assertFalse((self.repo / "devops" / "docker-compose.swarm.deploy.yml").exists())

    def test_verification_can_be_disabled(self):
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1", "Failed 1 second ago", "Shutdown", error="exit 1"))
        result = self.deploy(env={"DEPLOY_VERIFY": "false"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("skipping the convergence check", result.stdout)

    def test_health_path_is_probed_on_the_traefik_host(self):
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1"))
        self.write("curl_statuses", "503\n")
        result = self.deploy(env={"DEPLOY_HEALTHCHECK_PATH": "healthz", "TRAEFIK_HOST": "web.example.com"})
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn(f"{MARKER} check=health", result.stdout)
        self.assertIn("https://web.example.com/healthz", (self.stub / "curl.log").read_text())

    def test_health_path_falls_back_to_compose_host_rule(self):
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1"))
        self.write("curl_statuses", "200\n")
        result = self.deploy(env={"DEPLOY_HEALTHCHECK_PATH": "/health", "TRAEFIK_HOST": ""})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("https://app.example.com/health", (self.stub / "curl.log").read_text())

    def test_traefik_resync_still_uses_the_collected_hosts(self):
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1"))
        self.write("inspect_privatenetwork_traefik", "exists\n")
        self.write("curl_statuses", "502\n")
        result = self.deploy(env={"TRAEFIK_HOST": "web.example.com"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Bad Gateway from web.example.com", result.stdout)
        calls = (self.stub / "calls.log").read_text()
        self.assertIn("service\tupdate\t--force\tprivatenetwork_traefik", calls)

    def test_a_large_page_does_not_break_the_commands_after_the_probe(self):
        # Sourcing devops/.env turns on allexport for the rest of the script.
        # A probed page kept in a variable was then exported, and a page over
        # Linux's 128 KiB per-variable limit made every later command fail
        # with "Argument list too long" (PulsarCD's own index.html did).
        (self.repo / "devops" / ".env").write_text("APP_NAME=web\n", newline="\n")
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1"))
        self.write("inspect_privatenetwork_traefik", "exists\n")
        self.write("curl_statuses", "200\n")
        self.write("curl_body", "<html>" + "x" * 200_000 + "</html>")
        # The health check runs curl again after the probe: it sees the
        # environment every later command gets.
        result = self.deploy(env={"TRAEFIK_HOST": "web.example.com", "DEPLOY_HEALTHCHECK_PATH": "/health"})
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        self.assertNotIn("Argument list too long", output)
        self.assertIn("Got HTTP 200 from web.example.com", result.stdout)
        self.assertIn("/health", (self.stub / "curl.log").read_text())
        self.assertEqual((self.stub / "big_env").read_text(), "")

    def test_a_bad_gateway_page_is_still_detected_without_a_502(self):
        self.write("inspect_repo_web", inspect_line("1"))
        self.write("ps_repo_web", task("t1"))
        self.write("inspect_privatenetwork_traefik", "exists\n")
        self.write("curl_statuses", "200\n")
        self.write("curl_body", "<h1>502 Bad Gateway</h1>")
        result = self.deploy(env={"TRAEFIK_HOST": "web.example.com"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Bad Gateway from web.example.com (HTTP 200", result.stdout)
        self.assertIn("service\tupdate\t--force\tprivatenetwork_traefik",
                      (self.stub / "calls.log").read_text())


if __name__ == "__main__":
    unittest.main()
