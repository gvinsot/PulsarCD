"""GitHub push webhook (POST /api/webhooks/github).

Signature checking, replay protection and parsing live in github_webhook.py;
this route only maps a verified push onto _auto_build_commit, the auto-build
entry point shared with the poller (pipeline_runner).  The auth middleware
lets this exact route through without a session (_WEBHOOK_ROUTES).

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).

Tests patch _webhook_deliveries, _WEBHOOK_LISTING_RETRY_DELAYS and
_WEBHOOK_MAX_PAYLOAD_BYTES on backend.api, so those are read through it as
well.
"""

import asyncio
import time
from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from .. import run_history
from ..github_webhook import (
    ZERO_SHA as _ZERO_SHA,
    DeliveryCache,
    PushEvent,
    decode_body as _decode_webhook_body,
    parse_push as _parse_push,
    verify_signature as _verify_webhook_signature,
)
from ..pipeline_runner import (
    _advance_last_sha,
    _auto_build_commit,
    _github_webhook_secret,
    _sha_already_handled,
)
from .. import api

logger = structlog.get_logger()

router = APIRouter()


_webhook_deliveries = DeliveryCache()
# Strong references to in-flight webhook builds (asyncio keeps only weak ones).
_webhook_tasks: set = set()


# Pause before each read of the branch listing.  GitHub can deliver a push
# before its REST API lists the commit; reading the tag state from that stale
# listing would build the previous release under the new commit's name.
_WEBHOOK_LISTING_RETRY_DELAYS = (0, 1, 2, 4, 8, 15)
# How long the poller keeps re-reading a repo after the webhook handed a
# commit back to it, even though the repo's pushed_at no longer changes.
_WEBHOOK_HANDBACK_RECHECK_SECONDS = 600


class _StaleListing(Exception):
    """GitHub's commit listing did not show the pushed commit as the head."""


def _request_recheck(state: dict, seconds: float = _WEBHOOK_HANDBACK_RECHECK_SECONDS):
    """Make the poller re-read this repo on every cycle for ``seconds``."""
    state["recheck_until"] = max(state.get("recheck_until", 0.0),
                                 time.monotonic() + seconds)


async def _read_pushed_head(owner: str, name: str, sha: str) -> Optional[dict]:
    """Read the tag state once GitHub's listing shows ``sha`` as the head.

    Returns None when a newer commit superseded ``sha`` meanwhile (its own
    delivery or the poller deals with it).  Raises _StaleListing when the
    listing never caught up.
    """
    seen_head = None
    for delay in api._WEBHOOK_LISTING_RETRY_DELAYS:
        if delay:
            await asyncio.sleep(delay)
        if (api._auto_build_state.get(name) or {}).get("last_sha") != sha:
            return None
        api.github_service.invalidate_repo_cache(owner, name)
        # Same arguments (and so the same cache entry) as the commit read
        # inside get_untagged_commits, so both describe one listing.
        commits_data = await api.github_service.get_repo_commits(owner, name, per_page=20)
        commits = commits_data.get("commits") or []
        seen_head = commits[0].get("sha") if commits else None
        if seen_head != sha:
            continue
        untagged_data = await api.github_service.get_untagged_commits(owner, name, limit=20)
        untagged = untagged_data.get("untagged_commits") or []
        if untagged and untagged[0].get("sha") != sha:
            continue
        return untagged_data
    raise _StaleListing(f"listing head is {(seen_head or 'unknown')[:7]}, not {sha[:7]}")


async def _run_webhook_build(owner: str, name: str, ssh_url: str, sha: str,
                             old_sha: Optional[str]):
    """Background half of a webhook push: read the tag state, then build."""
    # This task has its own context copy: label the runs it starts.
    run_history.set_trigger("webhook", actor="github-webhook", commit=sha)
    try:
        untagged_data = await _read_pushed_head(owner, name, sha)
        if untagged_data is None:
            logger.info("Webhook push superseded by a newer commit", repo=name, sha=sha[:7])
            return
        await _auto_build_commit(owner, name, ssh_url, sha, untagged_data,
                                 old_sha=old_sha, source="webhook")
    except Exception as e:
        logger.error("Webhook-triggered build failed", repo=name, sha=sha[:7], error=str(e))
        # Nothing was started: hand the commit back to the poller.
        state = api._auto_build_state.get(name) or {}
        if state.get("last_sha") == sha and not state.get("building"):
            if old_sha:
                state["last_sha"] = old_sha
                if old_sha in state.get("seen_shas", []):
                    state["seen_shas"].remove(old_sha)
            else:
                state.pop("last_sha", None)
            # The poller may already have consumed this push's pushed_at.
            _request_recheck(state)


async def _handle_github_push(push: PushEvent) -> dict:
    """Decide what a verified push means for the auto-build.

    Everything up to claiming the commit runs without an ``await`` between the
    state check and ``_advance_last_sha``, so a concurrent poll can never pick
    the same commit up a second time.  The slow part (tagging, cloning) runs in
    the background: GitHub gives up on a delivery after 10 seconds.
    """
    def ignored(reason: str) -> dict:
        return {"status": "ignored", "reason": reason, "repo": push.full_name}

    if not api.github_service or not api.github_service.is_configured():
        return ignored("GitHub integration is not configured")
    if push.deleted:
        return ignored("ref was deleted")
    if push.branch is None:
        return ignored("not a branch push")
    if not push.is_default_branch:
        return ignored(f"push to '{push.branch}', not the default branch")

    repos = await api.github_service.get_starred_repos()
    wanted = push.full_name.lower()
    tracked = next((r for r in repos if (r.get("full_name") or "").lower() == wanted), None)
    if tracked is None:
        return ignored("repository is not tracked")

    # Owner, name and clone URL come from our own repo listing, not the payload.
    owner, name, ssh_url = tracked["owner"], tracked["name"], tracked["ssh_url"]
    state = api._auto_build_state.setdefault(name, {"building": False})
    if _sha_already_handled(state, push.after):
        return ignored("commit already handled")

    if state.get("building"):
        # Leave the commit to the poller, which builds it once the running
        # pipeline releases the lock.  A repo the poller has not observed yet
        # gets the pre-push head as its baseline so the new one reads as new.
        if "last_sha" not in state and push.before and push.before != _ZERO_SHA:
            state["last_sha"] = push.before
        state["recheck"] = True
        # Also past a stale listing that still shows the pre-push head.
        _request_recheck(state)
        return {"status": "deferred", "repo": name, "sha": push.after,
                "reason": "a pipeline is already running for this repository"}

    # Baseline handed back to the poller if the build cannot start: without
    # one, the poller's first look would only record the pushed commit.
    old_sha = state.get("last_sha") or (
        push.before if push.before and push.before != _ZERO_SHA else None)
    _advance_last_sha(state, push.after)
    task = asyncio.create_task(_run_webhook_build(owner, name, ssh_url, push.after, old_sha))
    _webhook_tasks.add(task)
    task.add_done_callback(_webhook_tasks.discard)
    return {"status": "triggered", "repo": name, "sha": push.after}


@router.post("/api/webhooks/github")
async def github_webhook_endpoint(request: Request):
    """Receive GitHub push events and start the auto-build immediately.

    Authenticated by ``X-Hub-Signature-256`` against
    PULSARCD_GITHUB__WEBHOOK_SECRET; answers 404 while that secret is unset.
    ``X-GitHub-Delivery`` ids are remembered so a replayed delivery is a no-op.
    """
    secret = _github_webhook_secret()
    if not secret:
        raise HTTPException(status_code=404, detail="Not Found")

    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > api._WEBHOOK_MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")
    body = await request.body()
    if len(body) > api._WEBHOOK_MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")

    if not _verify_webhook_signature(secret, body, request.headers.get("X-Hub-Signature-256")):
        logger.warning("GitHub webhook rejected: invalid signature",
                       client=request.client.host if request.client else "",
                       delivery=request.headers.get("X-GitHub-Delivery", ""))
        raise HTTPException(status_code=401, detail="Invalid signature")

    event = request.headers.get("X-GitHub-Event", "")
    delivery = request.headers.get("X-GitHub-Delivery", "").strip()
    if not delivery:
        raise HTTPException(status_code=400, detail="Missing X-GitHub-Delivery header")
    if not api._webhook_deliveries.check_and_add(delivery):
        logger.info("GitHub webhook delivery already processed", delivery=delivery,
                    github_event=event)
        return {"status": "duplicate", "delivery": delivery}

    try:
        payload = _decode_webhook_body(body, request.headers.get("content-type", ""))
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    if event == "ping":
        logger.info("GitHub webhook ping received", delivery=delivery,
                    hook_id=payload.get("hook_id"))
        return {"status": "pong", "delivery": delivery}
    if event != "push":
        return {"status": "ignored", "reason": f"event '{event}' is not handled",
                "delivery": delivery}

    push = _parse_push(payload)
    if push is None:
        raise HTTPException(status_code=400, detail="Invalid push payload")

    try:
        result = await _handle_github_push(push)
    except Exception:
        # Let GitHub's redelivery of this id through: nothing was claimed.
        api._webhook_deliveries.discard(delivery)
        raise
    result["delivery"] = delivery
    logger.info("GitHub push webhook processed", delivery=delivery, ref=push.ref,
                sha=push.after[:7], **{k: v for k, v in result.items()
                                       if k in ("status", "repo", "reason")})
    return JSONResponse(status_code=202 if result["status"] == "triggered" else 200,
                        content=result)
