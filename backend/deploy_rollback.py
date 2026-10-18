"""Automatic rollback after a deploy that failed post-deploy verification.

scripts/deploy-service.sh waits for the stack to converge (and optionally for
an HTTP health check) once ``docker stack deploy`` has run. When that check
fails the new version IS deployed -- typically crash-looping -- and the script
prints a line starting with ``VERIFY_FAILED_MARKER``. Only that case is rolled
back: any earlier failure (checkout, image pull, pre-deploy hook, ...) aborts
before the swarm is touched, so the previous version is still running.

The rollback redeploys the last version recorded as successfully deployed for
the same stage (``current_version`` in pipeline_state). It is attempted at most
once per deploy: the rollback deploy calls the deployer directly, never this
module again, so a previous version that also fails verification is reported
instead of triggering another rollback.
"""

import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple

import structlog

from . import notifier

logger = structlog.get_logger()

# Printed by deploy-service.sh (fail_verification) -- keep the two in sync.
VERIFY_FAILED_MARKER = "PULSARCD_DEPLOY_VERIFICATION_FAILED"

_MARKER_RE = re.compile(rf"^\s*{VERIFY_FAILED_MARKER}\b(.*)$", re.MULTILINE)


def is_verification_failure(output: Optional[str]) -> bool:
    """Whether a deploy's output says it failed post-deploy verification."""
    return bool(output) and _MARKER_RE.search(output) is not None


def _normalize_version(version: Optional[str]) -> str:
    return (version or "").strip().lstrip("v")


def plan_rollback(
    *,
    enabled: bool,
    failed_version: str,
    previous_version: Optional[str],
    cancelled: bool = False,
) -> Tuple[Optional[str], str]:
    """Decide whether to roll back, and to which version.

    Returns:
        (version to redeploy or None, human-readable reason)
    """
    if not enabled:
        return None, "automatic rollback is disabled (PULSARCD_GITHUB__AUTO_ROLLBACK=false)"
    if cancelled:
        return None, "the deploy was cancelled"
    previous = _normalize_version(previous_version)
    if not previous:
        return None, "no previously deployed version is known"
    if previous == _normalize_version(failed_version):
        # Redeploying the version that just failed would only fail again.
        return None, f"the last deployed version ({previous}) is the one that failed"
    return previous, f"rolling back to the last successfully deployed version {previous}"


def describe_rollback(rollback: Optional[Dict[str, Any]]) -> str:
    """One-line summary for logs and the LLM agent failure notification."""
    if not rollback:
        return ""
    if not rollback.get("attempted"):
        return (f"Post-deploy verification failed for {rollback.get('from_version')}; "
                f"no automatic rollback: {rollback.get('reason')}.")
    outcome = "succeeded" if rollback.get("success") else "FAILED"
    return (f"Post-deploy verification failed for {rollback.get('from_version')}; "
            f"automatic rollback to {rollback.get('to_version')} {outcome}.")


def _emit(output_callback: Optional[Callable[[str], None]], line: str) -> None:
    if output_callback:
        output_callback(line)


async def deploy_with_rollback(
    deployer,
    repo_name: str,
    ssh_url: str,
    *,
    version: str,
    tag: Optional[str] = None,
    qa: bool = False,
    previous_version: Optional[str] = None,
    enabled: bool = True,
    output_callback: Optional[Callable[[str], None]] = None,
    cancel_event=None,
) -> Dict[str, Any]:
    """Run ``deployer.deploy`` and roll back if post-deploy verification fails.

    The returned dict is the original deploy's result: ``success`` stays False
    after a rollback (the requested version did not deploy). When verification
    failed, ``result["rollback"]`` describes what happened:
    ``{from_version, to_version, attempted, success, reason, qa, at}``;
    ``result["output"]`` then starts with a one-line summary (what the LLM
    agent failure notification sees first) and ends with the rollback
    deploy's output.
    """
    result = await deployer.deploy(
        repo_name, ssh_url, version, tag=tag, qa=qa,
        output_callback=output_callback, cancel_event=cancel_event,
    )
    if result.get("success") or not is_verification_failure(result.get("output")):
        return result

    failed_version = result.get("version") or _normalize_version(tag) or version
    cancelled = bool(cancel_event is not None and cancel_event.is_set())
    target, reason = plan_rollback(
        enabled=enabled, failed_version=failed_version,
        previous_version=previous_version, cancelled=cancelled,
    )
    rollback: Dict[str, Any] = {
        "from_version": failed_version,
        "to_version": target,
        "attempted": target is not None,
        "success": False,
        "reason": reason,
        "qa": qa,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    result["rollback"] = rollback

    if target is None:
        logger.warning("Deploy failed verification, no automatic rollback",
                       repo=repo_name, version=failed_version, qa=qa, reason=reason)
        _emit(output_callback, "")
        _emit(output_callback, f"=== Automatic rollback skipped: {reason} ===")
        result["output"] = f"{describe_rollback(rollback)}\n\n{result.get('output', '')}"
        return result

    logger.warning("Deploy failed verification, rolling back",
                   repo=repo_name, version=failed_version, rollback_to=target, qa=qa)
    _emit(output_callback, "")
    _emit(output_callback,
          f"=== Post-deploy verification failed for {failed_version}: {reason} ===")

    # Straight to the deployer: a rollback is never itself rolled back.
    try:
        rollback_result = await deployer.deploy(
            repo_name, ssh_url, target, tag=None, qa=qa,
            output_callback=output_callback, cancel_event=cancel_event,
        )
    except Exception as e:  # the deployer normally reports errors in its result
        rollback_result = {"success": False, "output": f"{type(e).__name__}: {e}"}

    rollback["success"] = bool(rollback_result.get("success"))
    result["output"] = (
        f"{describe_rollback(rollback)}\n\n{result.get('output', '')}\n\n"
        f"=== Rollback to {target} ===\n{rollback_result.get('output', '')}"
    )
    if rollback["success"]:
        logger.info("Automatic rollback succeeded", repo=repo_name, version=target, qa=qa)
        _emit(output_callback, f"=== Rolled back to {target} ===")
    else:
        logger.error("Automatic rollback failed", repo=repo_name, version=target, qa=qa)
        _emit(output_callback, f"=== Rollback to {target} FAILED: the stack needs attention ===")
    return result


async def deploy_stage_with_rollback(
    deployer,
    pipeline_state,
    repo_name: str,
    ssh_url: str,
    *,
    version: str,
    tag: Optional[str] = None,
    qa: bool = False,
    enabled: Optional[bool] = None,
    output_callback: Optional[Callable[[str], None]] = None,
    cancel_event=None,
) -> Dict[str, Any]:
    """Pipeline wrapper: roll back to the stage's last good version, and record it.

    The previous version is the ``current_version`` of the "deploy" (or "qa")
    stage, i.e. the last version that stage deployed successfully. Callers keep
    updating the stage status themselves from ``result["success"]``.
    ``enabled`` defaults to the deployer's GitHubConfig.auto_rollback.
    """
    if enabled is None:
        enabled = bool(getattr(getattr(deployer, "config", None), "auto_rollback", True))
    stage = "qa" if qa else "deploy"
    previous_version = None
    entry = pipeline_state.get(repo_name) if pipeline_state is not None else None
    if entry is not None and stage in entry.stages:
        previous_version = entry.stages[stage].current_version

    result = await deploy_with_rollback(
        deployer, repo_name, ssh_url,
        version=version, tag=tag, qa=qa,
        previous_version=previous_version, enabled=enabled,
        output_callback=output_callback, cancel_event=cancel_event,
    )
    rollback = result.get("rollback")
    if rollback and pipeline_state is not None:
        pipeline_state.record_rollback(repo_name, stage, rollback)
    if rollback and rollback.get("attempted"):
        reason = "post-deploy verification failed"
        if not rollback.get("success"):
            reason += "; the rollback deploy FAILED too, the stack needs attention"
        notifier.notify_rollback(
            f"{repo_name} (QA)" if qa else repo_name,
            rollback.get("from_version") or version,
            rollback.get("to_version") or "",
            reason=reason,
        )
    return result
