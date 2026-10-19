"""Pipeline execution: the build -> test -> (QA) -> deploy chain and the auto-build.

Holds what the pipeline routes, the push webhook, the MCP tools and the
lifespan hook share: pipeline-state updates, the per-repo build lock, the
deployer lookup, the chain itself (_trigger_pipeline) and the auto-build
entry point used by both the poller and the webhook.

App state (settings, the service clients, the action registries, ...) lives in
backend.api and is read as ``api.<name>`` at call time, so the lifespan hook
and tests patching backend.api reach this code too.

Tests patch _trigger_pipeline, _get_deployer_and_host and asyncio (for the
poller's sleeps) on backend.api, so those are called through it as well.
"""

import asyncio
import re
import time
import uuid
from typing import List, Optional

import structlog
from fastapi import HTTPException

from . import build_hosts
from . import notifier
from .config import find_swarm_manager
from .github_service import StackDeployer
from .pipeline_state import _UNSET
from .deploy_rollback import deploy_stage_with_rollback
from . import run_history
from .background_actions import BackgroundAction

logger = structlog.get_logger()


async def _notify_agent_failure(stage: str, repo_name: str, version: str, error_output: str):
    """Notify the LLM agent of a build/test/deploy failure so it can investigate and act."""
    if api.llm_agent is None:
        logger.debug("LLM agent not configured, skipping failure notification")
        return
    try:
        await api.llm_agent.handle_failure(stage, repo_name, version, error_output)
    except Exception as e:
        logger.error("LLM agent failure handling error",
                     stage=stage, repo=repo_name,
                     error_type=type(e).__name__, error=str(e))
        api.llm_agent._report_system_error(
            "agent", type(e).__name__, str(e),
            action="notify_agent_failure", stage=stage, repo=repo_name)


def _get_swarm_manager_host() -> Optional[str]:
    """Return the swarm manager host name, or None if not configured."""
    manager = find_swarm_manager(api.settings.hosts)
    return manager.name if manager else None


def _set_pipeline(
    repo_name: str,
    stage: str,
    status: str,
    version: str,
    build_id=_UNSET,
    test_id=_UNSET,
    qa_id=_UNSET,
    deploy_id=_UNSET,
    log_lines: Optional[List[str]] = None,
) -> None:
    """Update pipeline state for repo_name (persisted to disk).

    Pass an explicit value (including None) to override; omit to inherit from the current state.
    If log_lines is provided, updates the last_log for the current stage.
    """
    api.pipeline_state.set_pipeline(
        repo_name, stage, status, version,
        build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=deploy_id,
    )
    # "done" is how a successful deploy is recorded; its log is the deploy's.
    log_stage = "deploy" if stage == "done" else stage
    if log_lines and log_stage in ("build", "test", "qa", "deploy"):
        api.pipeline_state.update_log(repo_name, log_stage, log_lines)
    run_history.recorder.observe_pipeline(
        repo_name, stage, status, version,
        action_ids={"build": build_id, "test": test_id, "qa": qa_id, "deploy": deploy_id},
        log_lines=log_lines,
    )
    # Failures (any stage) and production deploys; fire-and-forget, deduplicated.
    # A cancelled action is recorded as "failed" in pipeline state, but an
    # operator cancelling it is not an incident: no notification then (the
    # LLM-agent failure path skips cancellations the same way).
    if not _involves_cancelled_action(build_id, test_id, qa_id, deploy_id):
        notifier.notify_pipeline_stage(repo_name, stage, status, version)


def _run_history_log_source(repo_name: str, stage: str, action_id: Optional[str]) -> Optional[List[str]]:
    """Log lines of a run that closed without passing them (pipeline exception path)."""
    if not action_id and api.pipeline_state is not None:
        entry = api.pipeline_state.get(repo_name)
        if entry and stage in entry.stages:
            action_id = entry.stages[stage].action_id
    action = api._background_actions.get(action_id) if action_id else None
    return list(action.output_lines) if action else None


def _involves_cancelled_action(*action_ids) -> bool:
    """True when one of the given background action ids was cancelled."""
    for action_id in action_ids:
        if action_id is _UNSET or not action_id:
            continue
        action = api._background_actions.get(action_id)
        if action is not None and (action.cancel_event.is_set() or action.status == "cancelled"):
            return True
    return False


def _get_deployer_and_host():
    """Helper to get deployer instance and host name."""
    if not api.collector.clients:
        raise HTTPException(status_code=500, detail="No host clients available")

    host_name = None
    host_client = None

    for name, client in api.collector.clients.items():
        if hasattr(client, 'config') and getattr(client.config, 'swarm_manager', False):
            host_name = name
            host_client = client
            break

    if not host_client:
        host_name, host_client = next(iter(api.collector.clients.items()))

    return StackDeployer(api.settings.github, host_client), host_name


async def _get_build_deployer(repo_name: str):
    """Deployer and host label for a build or test of repo_name.

    Same as _get_deployer_and_host() unless extra build hosts are configured;
    deploys always use _get_deployer_and_host().
    """
    deployer, host_name = api._get_deployer_and_host()
    return await build_hosts.pick_build_deployer(
        repo_name, deployer, host_name, getattr(api.settings, "github", None))


def invalidate_buildable_cache(repo_name: str = None):
    """Clear buildable cache (after clone/pull that may change compose file)."""
    if repo_name:
        api._buildable_cache.pop(repo_name, None)
    else:
        api._buildable_cache.clear()


def _acquire_build_lock(repo_name: str) -> bool:
    """Atomically claim the build slot for ``repo_name``.

    Returns True if the caller now owns the slot, False if a build is
    already in progress. Single-threaded asyncio makes the check-and-set
    safe as long as no ``await`` runs between them.

    Only the ``building`` flag is initialized here — ``last_sha`` and
    ``untagged_commits`` are owned by ``auto_build_poller`` and must stay
    absent until the poller has actually observed the repo, so a manual
    trigger never tricks the poller into thinking the SHA changed.
    """
    state = api._auto_build_state.get(repo_name)
    if state is None:
        api._auto_build_state[repo_name] = {"building": True}
        return True
    if state.get("building"):
        return False
    state["building"] = True
    return True


def _release_build_lock(repo_name: str):
    if repo_name in api._auto_build_state:
        api._auto_build_state[repo_name]["building"] = False


AUTO_BUILD_POLL_INTERVAL = 10  # seconds – repo checks are batched so this is cheap

# Pipeline stages in order
PIPELINE_STAGES = ["build", "test", "deploy", "done"]


def _extract_version_from_output(lines: list) -> Optional[str]:
    """Parse the built version from build script output.

    Build script outputs: 'Version: 1.0.X (tag: v1.0.X)'
    """
    for line in lines:
        if "Version:" in line:
            m = re.search(r'Version:\s*(\d+\.\d+\.\d+)', line)
            if m:
                return m.group(1)
    return None


async def _trigger_pipeline(repo_name: str, ssh_url: str, version: str = None, tag: str = None) -> bool:
    """Trigger a full pipeline: build → test → deploy.

    Args:
        repo_name: Repository name
        ssh_url: SSH URL for cloning
        version: Optional exact version (e.g., "1.0.5"). If provided, build uses this exact version.
        tag: Optional git tag (e.g., "v1.0.5"). If provided, build checks out this tag.

    Returns:
        True if the pipeline was started, False if a build was already
        running for this repo (de-duplication).
    """
    # Acquire the per-repo build lock first so any caller (API endpoint,
    # webhook, auto-poller, manual rebuild) is de-duplicated centrally.
    if not _acquire_build_lock(repo_name):
        logger.warning("Pipeline already running, skipping duplicate trigger",
                       repo=repo_name, version=version, tag=tag)
        return False
    run_history.begin_pipeline()

    try:
        deployer, host_name = api._get_deployer_and_host()
    except Exception as e:
        logger.error("Pipeline: no host available", repo=repo_name, error=str(e))
        _set_pipeline(repo_name, "build", "failed", version, build_id=None, test_id=None, deploy_id=None)
        _release_build_lock(repo_name)
        return False

    # Update local clone BEFORE checking build config, so that newly added
    # build: directives in docker-compose.swarm.yml are picked up.
    try:
        await deployer._ensure_repo_cloned(repo_name, ssh_url)
    except Exception as e:
        logger.warning("Pipeline: failed to update repo clone before build check",
                       repo=repo_name, error=str(e))

    # Check if this repo has build: directives in its compose file.
    invalidate_buildable_cache(repo_name)
    try:
        buildable = await deployer.has_build_config(repo_name)
        api._buildable_cache[repo_name] = buildable
    except Exception as e:
        logger.warning("Failed to check build config, assuming buildable",
                       repo=repo_name, error=str(e))
        buildable = True

    initial_stage = "build" if buildable else "test"
    _set_pipeline(repo_name, initial_stage, "running", version, build_id=None, test_id=None, deploy_id=None)
    api.pipeline_state.set_skip_build(repo_name, not buildable)

    # Set project and stack metadata
    stack_name = StackDeployer._repo_to_stack_name(repo_name)
    api.pipeline_state.set_project_info(repo_name, project_name=repo_name, stack_name=stack_name)
    # Clear previous gate decisions for new pipeline run
    api.pipeline_state.clear_gates(repo_name)

    # Determine build version param: exact "1.0.5" or auto-increment "1.0"
    build_version = version if version else "1.0"

    async def _run_pipeline():
        built_version = version
        build_id = None
        try:
            # Build and test run on the build host picked for this repo; QA and
            # deploy stay on the primary (swarm manager) deployer.
            build_deployer, build_host = await _get_build_deployer(repo_name)

            # ── Step 1: Build (skip if no build: in compose) ──
            if buildable:
                build_id = str(uuid.uuid4())[:8]
                build_action = BackgroundAction(build_id, "build", repo_name)
                build_action.host = build_host
                api._background_actions[build_id] = build_action
                api.pipeline_state.get_or_create(repo_name).stages["build"].action_id = build_id

                logger.info("Pipeline: starting build", repo=repo_name, action_id=build_id,
                            version=build_version, tag=tag)
                result = await build_deployer.build(
                    repo_name, ssh_url, version=build_version,
                    tag=tag,
                    output_callback=build_action.append_output,
                    cancel_event=build_action.cancel_event,
                )
                result["host"] = build_host
                result["auto_triggered"] = True
                build_action.result = result
                build_action.status = "completed" if result.get("success") else "failed"

                if not result.get("success"):
                    _set_pipeline(repo_name, "build", "failed", version, build_id=build_id, test_id=None, deploy_id=None, log_lines=build_action.output_lines)
                    await _notify_agent_failure("build", repo_name, version or "", result.get("output", ""))
                    return

                # Extract version from build output, or use provided version
                built_version = _extract_version_from_output(build_action.output_lines) or version
                api.pipeline_state.update_version(repo_name, "build", built_version)
                api.pipeline_state.update_log(repo_name, "build", build_action.output_lines)
                # Mark build stage as success with current_version
                api.pipeline_state.set_stage(repo_name, "build", "success", built_version, action_id=build_id, log_lines=build_action.output_lines)
                run_history.recorder.observe(repo_name, "build", "success", built_version,
                                             action_id=build_id, log_lines=build_action.output_lines)
                logger.info("Pipeline: build succeeded", repo=repo_name, version=built_version)

                # ── Gate: Build → Test ──
                _bt_cfg = api.pipeline_state.get_transition_config(repo_name, "build_to_test")
                _bt_mode = _bt_cfg.get("mode", "auto_with_success") if _bt_cfg else "auto_with_success"
                if _bt_mode == "manual":
                    logger.info("Pipeline: build→test manual gate, stopping", repo=repo_name)
                    api.pipeline_state.record_gate(repo_name, "build_to_test", False, "Manual transition — waiting for user approval", version=built_version)
                    _set_pipeline(repo_name, "build", "gate_rejected", built_version,
                                  build_id=build_id, test_id=None, deploy_id=None, log_lines=build_action.output_lines)
                    return
                elif _bt_mode == "agent":
                    if api.llm_agent:
                        approved, reason = await api.llm_agent.evaluate_gate(
                            "build_to_test", repo_name, built_version or "",
                            result.get("output", "")
                        )
                        api.pipeline_state.record_gate(repo_name, "build_to_test", approved, reason, version=built_version)
                        if not approved:
                            logger.warning("Pipeline: gate build→test REJECTED",
                                           repo=repo_name, reason=reason[:200])
                            _set_pipeline(repo_name, "build", "gate_rejected", built_version,
                                          build_id=build_id, test_id=None, deploy_id=None, log_lines=build_action.output_lines)
                            return
                        logger.info("Pipeline: gate build→test approved", repo=repo_name, reason=reason[:100])
                    else:
                        api.pipeline_state.record_gate(repo_name, "build_to_test", False, "Agent gate unavailable: no LLM configured", version=built_version)
                        _set_pipeline(repo_name, "build", "gate_rejected", built_version, build_id=build_id)
                        return
                # else: "auto" or "auto_with_success" — proceed (success already checked above)

            else:
                logger.info("Pipeline: skipping build (no build: in compose)", repo=repo_name)

            # ── Step 2: Test ──
            test_id = str(uuid.uuid4())[:8]
            test_action = BackgroundAction(test_id, "test", repo_name)
            test_action.host = build_host
            test_action.task = asyncio.current_task()
            api._background_actions[test_id] = test_action
            _set_pipeline(repo_name, "test", "running", built_version, build_id=build_id, test_id=test_id, deploy_id=None)

            logger.info("Pipeline: starting test", repo=repo_name, action_id=test_id, tag=tag)
            test_result = await build_deployer.test(
                repo_name, ssh_url,
                tag=(f"v{built_version}" if buildable and built_version else tag),
                output_callback=test_action.append_output,
                cancel_event=test_action.cancel_event,
            )
            test_result["host"] = build_host
            test_result["auto_triggered"] = True
            test_action.result = test_result

            if not test_result.get("success") or test_action.cancel_event.is_set():
                test_result["success"] = False
                test_action.status = "cancelled" if test_action.cancel_event.is_set() else "failed"
                _set_pipeline(repo_name, "test", "failed", built_version, build_id=build_id, test_id=test_id, deploy_id=None, log_lines=test_action.output_lines)
                logger.warning("Pipeline: test failed", repo=repo_name)
                if not test_action.cancel_event.is_set():
                    await _notify_agent_failure("test", repo_name, built_version or "", test_result.get("output", ""))
                return

            test_action.status = "completed"
            _set_pipeline(repo_name, "test", "success", built_version, build_id=build_id, test_id=test_id, deploy_id=None, log_lines=test_action.output_lines)
            logger.info("Pipeline: test succeeded", repo=repo_name)

            _td_cfg = api.pipeline_state.get_transition_config(repo_name, "test_to_deploy")
            _td_mode = _td_cfg.get("mode", "auto_with_success") if _td_cfg else "auto_with_success"
            if _td_mode == "manual":
                logger.info("Pipeline: test→deploy manual gate, stopping", repo=repo_name)
                api.pipeline_state.record_gate(repo_name, "test_to_deploy", False, "Manual transition — waiting for user approval", version=built_version)
                _set_pipeline(repo_name, "test", "gate_rejected", built_version,
                              build_id=build_id, test_id=test_id, deploy_id=None, log_lines=test_action.output_lines)
                return
            elif _td_mode == "agent":
                if api.llm_agent:
                    approved, reason = await api.llm_agent.evaluate_gate(
                        "test_to_deploy", repo_name, built_version or "",
                        test_result.get("output", "")
                    )
                    api.pipeline_state.record_gate(repo_name, "test_to_deploy", approved, reason, version=built_version)
                    if not approved:
                        logger.warning("Pipeline: gate test→deploy REJECTED",
                                       repo=repo_name, reason=reason[:200])
                        _set_pipeline(repo_name, "test", "gate_rejected", built_version,
                                      build_id=build_id, test_id=test_id, deploy_id=None, log_lines=test_action.output_lines)
                        return
                    logger.info("Pipeline: gate test→deploy approved", repo=repo_name, reason=reason[:100])
                else:
                    api.pipeline_state.record_gate(repo_name, "test_to_deploy", False, "Agent gate unavailable: no LLM configured", version=built_version)
                    _set_pipeline(repo_name, "test", "gate_rejected", built_version, build_id=build_id, test_id=test_id)
                    return
            # else: "auto" or "auto_with_success" — proceed (success already checked above)

            deploy_tag = tag if tag else (f"v{built_version}" if built_version else None)

            # ── Optional Step 2.5: QA deploy (when test_to_deploy.qa_enabled) ──
            qa_id = None
            qa_enabled = bool(_td_cfg.get("qa_enabled", False)) if _td_cfg else False
            if qa_enabled:
                qa_id = str(uuid.uuid4())[:8]
                qa_action = BackgroundAction(qa_id, "qa-deploy", repo_name)
                api._background_actions[qa_id] = qa_action
                _set_pipeline(repo_name, "qa", "running", built_version,
                              build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=None)

                logger.info("Pipeline: starting QA deploy", repo=repo_name, action_id=qa_id, tag=deploy_tag)
                qa_result = await deploy_stage_with_rollback(
                    deployer, api.pipeline_state, repo_name, ssh_url, version=built_version,
                    tag=deploy_tag, qa=True,
                    output_callback=qa_action.append_output,
                    cancel_event=qa_action.cancel_event,
                )
                qa_result["host"] = host_name
                qa_result["auto_triggered"] = True
                qa_action.result = qa_result
                qa_action.status = "completed" if qa_result.get("success") else "failed"

                if not qa_result.get("success") or qa_action.cancel_event.is_set():
                    qa_result["success"] = False
                    qa_action.status = "cancelled" if qa_action.cancel_event.is_set() else "failed"
                    _set_pipeline(repo_name, "qa", "failed", built_version,
                                  build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=None,
                                  log_lines=qa_action.output_lines)
                    logger.warning("Pipeline: QA deploy failed", repo=repo_name)
                    if not qa_action.cancel_event.is_set():
                        await _notify_agent_failure("qa", repo_name, built_version or "", qa_result.get("output", ""))
                    return

                _set_pipeline(repo_name, "qa", "success", built_version,
                              build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=None,
                              log_lines=qa_action.output_lines)
                logger.info("Pipeline: QA deploy succeeded", repo=repo_name, version=built_version)

                # ── Gate: QA → Deploy (configurable, defaults to manual) ──
                _qd_cfg = api.pipeline_state.get_transition_config(repo_name, "qa_to_deploy")
                _qd_mode = _qd_cfg.get("mode", "manual") if _qd_cfg else "manual"
                _qd_stop_reason = None
                if _qd_mode == "manual":
                    _qd_stop_reason = "Manual transition — waiting for user approval after QA"
                elif _qd_mode == "agent":
                    if api.llm_agent:
                        approved, reason = await api.llm_agent.evaluate_gate(
                            "qa_to_deploy", repo_name, built_version or "",
                            qa_result.get("output", "")
                        )
                        api.pipeline_state.record_gate(repo_name, "qa_to_deploy", approved, reason, version=built_version)
                        if not approved:
                            _qd_stop_reason = ""  # already recorded
                    else:
                        _qd_stop_reason = "Agent gate unavailable: no LLM configured"
                # else: "auto" / "auto_with_success" — QA succeeded, proceed to production
                if _qd_stop_reason is not None:
                    logger.info("Pipeline: qa→deploy gate stopping", repo=repo_name, mode=_qd_mode)
                    if _qd_stop_reason:
                        api.pipeline_state.record_gate(repo_name, "qa_to_deploy", False, _qd_stop_reason,
                                                   version=built_version)
                    _set_pipeline(repo_name, "qa", "gate_rejected", built_version,
                                  build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=None,
                                  log_lines=qa_action.output_lines)
                    return

            # ── Step 3: Deploy ──
            deploy_id = str(uuid.uuid4())[:8]
            deploy_action = BackgroundAction(deploy_id, "deploy", repo_name)
            api._background_actions[deploy_id] = deploy_action
            _set_pipeline(repo_name, "deploy", "running", built_version,
                          build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=deploy_id)

            logger.info("Pipeline: starting deploy", repo=repo_name, action_id=deploy_id, tag=deploy_tag)
            deploy_result = await deploy_stage_with_rollback(
                deployer, api.pipeline_state, repo_name, ssh_url, version=built_version,
                tag=deploy_tag,
                output_callback=deploy_action.append_output,
                cancel_event=deploy_action.cancel_event,
            )
            deploy_result["host"] = host_name
            deploy_result["auto_triggered"] = True
            deploy_action.result = deploy_result
            deploy_action.status = "completed" if deploy_result.get("success") else "failed"

            if deploy_result.get("success"):
                _set_pipeline(repo_name, "done", "success", built_version, build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=deploy_id, log_lines=deploy_action.output_lines)
                logger.info("Pipeline: deploy succeeded", repo=repo_name, version=built_version)
            else:
                _set_pipeline(repo_name, "deploy", "failed", built_version, build_id=build_id, test_id=test_id, qa_id=qa_id, deploy_id=deploy_id, log_lines=deploy_action.output_lines)
                await _notify_agent_failure("deploy", repo_name, built_version or "", deploy_result.get("output", ""))

        except Exception as e:
            logger.exception("Pipeline failed", repo=repo_name, error=str(e))
            current_stage = api.pipeline_state.get_legacy(repo_name).get("stage", "build")
            _set_pipeline(repo_name, current_stage, "failed", built_version)
        finally:
            _release_build_lock(repo_name)

    asyncio.create_task(_run_pipeline())
    logger.info("Pipeline triggered", repo=repo_name)
    return True


# ---- Shared auto-build entry point (poller and push webhook) ----------------

_SEEN_SHAS_KEEP = 20


def _github_webhook_secret() -> str:
    """The push webhook secret, or "" when the webhook is not configured."""
    return getattr(getattr(api.settings, "github", None), "webhook_secret", None) or ""


def _auto_build_poll_interval() -> int:
    """Seconds between poller runs: longer once the webhook delivers pushes."""
    if _github_webhook_secret():
        configured = getattr(api.settings.github, "webhook_poll_interval", 0) or 0
        return max(AUTO_BUILD_POLL_INTERVAL, int(configured))
    return AUTO_BUILD_POLL_INTERVAL


def _advance_last_sha(state: dict, sha: str):
    """Move ``last_sha`` to ``sha``, remembering the SHA it replaces.

    The webhook usually learns about a commit before GitHub's REST listings do,
    so the poller can read the previous head again after the webhook advanced
    ``last_sha``.  Remembering recent heads keeps that stale read from looking
    like a new commit and rebuilding it.
    """
    old = state.get("last_sha")
    if old and old != sha:
        seen = state.setdefault("seen_shas", [])
        if old not in seen:
            seen.append(old)
            del seen[:-_SEEN_SHAS_KEEP]
    state["last_sha"] = sha


def _sha_already_handled(state: dict, sha: str) -> bool:
    """Whether ``sha`` is the current or a recent head already acted upon."""
    return sha == state.get("last_sha") or sha in state.get("seen_shas", ())


async def _auto_build_commit(owner: str, name: str, ssh_url: str, sha: str,
                             untagged_data: dict, old_sha: Optional[str] = None,
                             source: str = "poller") -> bool:
    """Build ``sha``, the new head of ``name``'s default branch.

    The single auto-build path shared by the poller and the push webhook: it
    honours the version->build gate, advances ``last_sha``, auto-tags untagged
    commits and hands over to ``_trigger_pipeline``, whose build lock
    de-duplicates concurrent triggers.  Callers have checked that ``sha`` is
    new and that no build is running.  Returns True if a pipeline started.
    """
    state = api._auto_build_state.setdefault(name, {"building": False})
    untagged_count = len(untagged_data.get("untagged_commits", []))
    state["untagged_commits"] = untagged_count

    _vb_cfg = api.pipeline_state.get_transition_config(name, "version_to_build")
    _vb_mode = _vb_cfg.get("mode", "auto_with_success") if _vb_cfg else "auto_with_success"
    if _vb_mode == "manual":
        logger.info("Auto-build skipped (version→build set to manual)", repo=name, source=source)
        _advance_last_sha(state, sha)
        return False

    logger.info("New commit detected, triggering pipeline",
                repo=name,
                old_sha=(old_sha or "")[:7],
                new_sha=sha[:7],
                untagged=untagged_count,
                source=source)
    # Advance last_sha so the next poll iteration doesn't re-evaluate
    # the same commit. The actual build lock is owned by _trigger_pipeline.
    _advance_last_sha(state, sha)

    latest_tag_info = untagged_data.get("latest_tag")
    if untagged_count > 0:
        try:
            next_ver = await api.github_service.get_next_version(owner, name)
            new_tag = f"v{next_ver}"
            tag_result = await api.github_service.create_tag(owner, name, new_tag, sha)
            if tag_result.get("success"):
                logger.info("Auto-tagged new commit", repo=name, tag=new_tag, sha=sha[:7])
                return await api._trigger_pipeline(name, ssh_url, version=next_ver, tag=new_tag)
            logger.error("Failed to auto-tag", repo=name, error=tag_result.get("error"))
        except Exception as tag_err:
            logger.error("Auto-tag failed", repo=name, error=str(tag_err))
        return False
    if latest_tag_info:
        tag_name = latest_tag_info.get("name", "")
        version = tag_name.lstrip("v")
        return await api._trigger_pipeline(name, ssh_url, version=version, tag=tag_name)
    return await api._trigger_pipeline(name, ssh_url)


async def auto_build_poller():
    """Periodically check starred repos for new commits on default branch and trigger pipeline.

    Optimisations:
    - Uses `pushed_at` from the starred-repos list to skip unchanged repos (0 extra API calls).
    - Checks remaining repos concurrently via asyncio.gather.
    """
    run_history.set_trigger("auto", actor="auto-build")
    # Both sleeps go through backend.api's asyncio, which tests replace to
    # drive the loop one poll at a time.
    await api.asyncio.sleep(10)

    # Track last known pushed_at per repo to cheaply skip unchanged ones
    _pushed_at_cache: dict[str, str] = {}
    _repo_sem = asyncio.Semaphore(3)

    async def _check_repo(repo: dict) -> bool:
        """Check a single repo for new commits. Designed to run concurrently."""
        async with _repo_sem:
            return await _check_repo_inner(repo)

    async def _check_repo_inner(repo: dict) -> bool:
        """Return False when the repo must be checked again on the next poll."""
        owner, name, ssh_url = repo["owner"], repo["name"], repo["ssh_url"]

        try:
            commits_task = api.github_service.get_repo_commits(owner, name, per_page=1)
            untagged_task = api.github_service.get_untagged_commits(owner, name, limit=20)
            commits_data, untagged_data = await asyncio.gather(commits_task, untagged_task)
        except Exception as e:
            logger.warning("Failed to fetch commit data for repo", repo=name, error=str(e))
            return False

        commits = commits_data.get("commits", [])
        if not commits:
            # Only a rate-limited fetch is retried on the next poll. Other API
            # errors are persistent (GitHub answers 409 for an empty repo,
            # 403/404 for missing permissions): retrying them every poll
            # would burn the API quota until the next push.
            return "rate limit" not in str(commits_data.get("error", "")).lower()

        latest_sha = commits[0]["sha"]
        untagged_count = len(untagged_data.get("untagged_commits", []))
        state = api._auto_build_state.get(name)

        # First observation of this repo (or only the build lock has touched it
        # so far via a manual trigger) — record the SHA without firing a build.
        if state is None or "last_sha" not in state:
            if state is None:
                api._auto_build_state[name] = {
                    "last_sha": latest_sha,
                    "building": False,
                    "untagged_commits": untagged_count,
                }
            else:
                state["last_sha"] = latest_sha
                state["untagged_commits"] = untagged_count
            if untagged_count > 0:
                logger.info("Repo initialized with untagged commits",
                            repo=name, untagged=untagged_count)
            return True

        # Always update untagged count
        state["untagged_commits"] = untagged_count

        if _sha_already_handled(state, latest_sha):
            state.pop("recheck", None)
            return True
        if state.get("building"):
            # Keep checking this repo on every poll, even though its pushed_at
            # will not change again, so the commit is built once the running
            # pipeline releases the lock.
            state["recheck"] = True
            return False
        state.pop("recheck", None)
        await _auto_build_commit(owner, name, ssh_url, latest_sha, untagged_data,
                                 old_sha=state["last_sha"], source="poller")
        return True

    while True:
        try:
            if api.github_service and api.github_service.is_configured():
                repos = await api.github_service.get_starred_repos()

                # Fast-filter: skip repos whose pushed_at hasn't changed
                repos_to_check = []
                for repo in repos:
                    name = repo["name"]
                    pushed_at = repo.get("pushed_at", "")
                    prev = _pushed_at_cache.get(name)
                    _st = api._auto_build_state.get(name) or {}
                    recheck = (_st.get("recheck")
                               or _st.get("recheck_until", 0.0) > time.monotonic())
                    if prev is not None and prev == pushed_at and not recheck:
                        # No push since last poll – skip expensive per-repo calls
                        continue
                    repos_to_check.append(repo)

                if repos_to_check:
                    logger.info("Checking repos for changes",
                                total=len(repos), checking=len(repos_to_check))
                    # Check all changed repos concurrently (batch)
                    settled = await asyncio.gather(*[_check_repo(r) for r in repos_to_check])
                    # Only remember pushed_at once a repo's latest commit has
                    # been handled, so a failed fetch or a commit that landed
                    # during a build is retried on the next poll.
                    for repo, done in zip(repos_to_check, settled):
                        if done:
                            _pushed_at_cache[repo["name"]] = repo.get("pushed_at", "")

        except Exception as e:
            logger.error("Auto-build poller error", error=str(e))

        await api.asyncio.sleep(_auto_build_poll_interval())


# Imported last: backend.api imports this module while it initializes, and
# ``api`` is only used at call time above.  This way the module can also be
# imported first, before backend.api.
from . import api  # noqa: E402
