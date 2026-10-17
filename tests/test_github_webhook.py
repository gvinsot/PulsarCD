"""Tests for the GitHub push webhook (POST /api/webhooks/github).

Covers the pure helpers in backend.github_webhook (signature, replay cache,
payload parsing), the route's auth exemption, and how a verified push maps onto
the shared auto-build entry point and its last_sha bookkeeping.
"""

import asyncio
import hashlib
import hmac
import json
import time
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import urlencode

import pytest

from backend.config import GitHubConfig
from backend.github_webhook import (
    DeliveryCache,
    ZERO_SHA,
    decode_body,
    parse_push,
    verify_signature,
)

SECRET = "webhook-test-secret"
URL = "/api/webhooks/github"
OLD_SHA = "a" * 40
NEW_SHA = "b" * 40


def _sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _push_payload(full_name="octo/app", ref="refs/heads/main", after=NEW_SHA,
                  before=OLD_SHA, default_branch="main", deleted=False) -> dict:
    owner, name = full_name.split("/")
    return {
        "ref": ref,
        "before": before,
        "after": after,
        "deleted": deleted,
        "repository": {
            "name": name,
            "full_name": full_name,
            "owner": {"name": owner, "login": owner},
            "default_branch": default_branch,
        },
    }


_delivery_counter = 0


def _post(client, payload, event="push", delivery=None, secret=SECRET, signature=None,
          raw: bytes = None):
    global _delivery_counter
    body = raw if raw is not None else json.dumps(payload).encode()
    if delivery is None:
        _delivery_counter += 1
        delivery = f"delivery-{_delivery_counter}"
    headers = {"Content-Type": "application/json", "X-GitHub-Event": event}
    if delivery:
        headers["X-GitHub-Delivery"] = delivery
    sig = signature if signature is not None else _sign(body, secret)
    if sig:
        headers["X-Hub-Signature-256"] = sig
    return client.post(URL, content=body, headers=headers)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestVerifySignature:
    def test_valid_signature(self):
        body = b'{"a": 1}'
        assert verify_signature(SECRET, body, _sign(body))

    def test_uppercase_hex_accepted(self):
        body = b"x"
        sig = _sign(body)
        assert verify_signature(SECRET, body, "sha256=" + sig[7:].upper())

    def test_wrong_secret(self):
        body = b"x"
        assert not verify_signature(SECRET, body, _sign(body, "other"))

    def test_tampered_body(self):
        assert not verify_signature(SECRET, b"y", _sign(b"x"))

    @pytest.mark.parametrize("header", [None, "", "sha1=abcd", "sha256=", "garbage",
                                        "sha256=éé"])
    def test_malformed_header(self, header):
        assert not verify_signature(SECRET, b"x", header)

    def test_empty_secret_never_validates(self):
        body = b"x"
        assert not verify_signature("", body, _sign(body, ""))

    def test_uses_constant_time_compare(self, monkeypatch):
        import backend.github_webhook as mod
        calls = []
        real = hmac.compare_digest
        monkeypatch.setattr(mod.hmac, "compare_digest",
                            lambda a, b: calls.append(1) or real(a, b))
        verify_signature(SECRET, b"x", _sign(b"x"))
        assert calls


class TestDeliveryCache:
    def test_first_seen_then_duplicate(self):
        cache = DeliveryCache()
        assert cache.check_and_add("d1")
        assert not cache.check_and_add("d1")
        assert cache.check_and_add("d2")

    def test_expires_after_ttl(self):
        cache = DeliveryCache(ttl_seconds=10)
        assert cache.check_and_add("d1", now=100.0)
        assert not cache.check_and_add("d1", now=105.0)
        assert cache.check_and_add("d1", now=111.0)

    def test_discard(self):
        cache = DeliveryCache()
        cache.check_and_add("d1")
        cache.discard("d1")
        cache.discard("never-seen")
        assert cache.check_and_add("d1")

    def test_bounded_size(self):
        cache = DeliveryCache(max_entries=3)
        for i in range(10):
            cache.check_and_add(f"d{i}", now=float(i))
        assert len(cache) == 3
        # The oldest were evicted, the newest are still remembered.
        assert not cache.check_and_add("d9", now=10.0)


class TestParsing:
    def test_parse_push(self):
        push = parse_push(_push_payload())
        assert push.owner == "octo" and push.name == "app"
        assert push.branch == "main" and push.is_default_branch
        assert push.after == NEW_SHA and not push.deleted

    def test_parse_push_tag_ref(self):
        push = parse_push(_push_payload(ref="refs/tags/v1.0.0"))
        assert push.branch is None and not push.is_default_branch

    def test_parse_push_deleted(self):
        assert parse_push(_push_payload(after=ZERO_SHA)).deleted

    def test_parse_push_owner_from_full_name(self):
        payload = _push_payload()
        payload["repository"]["owner"] = {}
        assert parse_push(payload).owner == "octo"

    @pytest.mark.parametrize("payload", [{}, {"repository": "x"}, {"repository": {}},
                                         {"ref": "refs/heads/main", "repository": {"name": "a"}}])
    def test_parse_push_invalid(self, payload):
        assert parse_push(payload) is None

    def test_decode_json(self):
        assert decode_body(b'{"zen": "hi"}', "application/json") == {"zen": "hi"}

    def test_decode_form(self):
        body = urlencode({"payload": json.dumps({"zen": "hi"})}).encode()
        assert decode_body(body, "application/x-www-form-urlencoded") == {"zen": "hi"}

    @pytest.mark.parametrize("body,ctype", [(b"[1]", "application/json"),
                                            (b"not json", "application/json"),
                                            (b"other=1", "application/x-www-form-urlencoded")])
    def test_decode_invalid(self, body, ctype):
        with pytest.raises(ValueError):
            decode_body(body, ctype)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def webhook(client, monkeypatch):
    """Configure the webhook secret and isolate auto-build state per test."""
    import backend.api as api

    gh_config = GitHubConfig(token="t", webhook_secret=SECRET)
    monkeypatch.setattr(api.settings, "github", gh_config)

    gh = MagicMock()
    gh.is_configured = MagicMock(return_value=True)
    gh.get_starred_repos = AsyncMock(return_value=[{
        "name": "app", "full_name": "octo/app", "owner": "octo",
        "ssh_url": "git@github.com:octo/app.git", "pushed_at": "",
    }])
    # GitHub's listing already shows the pushed commit as the head.
    gh.get_repo_commits = AsyncMock(return_value={"commits": [{"sha": NEW_SHA}]})
    gh.get_untagged_commits = AsyncMock(return_value={
        "untagged_commits": [], "latest_tag": {"name": "v1.2.3"}})
    gh.get_next_version = AsyncMock(return_value="1.2.4")
    gh.create_tag = AsyncMock(return_value={"success": True})
    gh.invalidate_repo_cache = MagicMock()
    monkeypatch.setattr(api, "github_service", gh)

    trigger = AsyncMock(return_value=True)
    monkeypatch.setattr(api, "_trigger_pipeline", trigger)

    ps = MagicMock()
    ps.get_transition_config = MagicMock(return_value=None)
    monkeypatch.setattr(api, "pipeline_state", ps)

    state = {}
    monkeypatch.setattr(api, "_auto_build_state", state)
    # Retry the listing immediately instead of waiting for GitHub to catch up.
    monkeypatch.setattr(api, "_WEBHOOK_LISTING_RETRY_DELAYS", (0, 0, 0))
    monkeypatch.setattr(api, "_webhook_deliveries", DeliveryCache())

    return {"api": api, "gh": gh, "trigger": trigger, "state": state,
            "pipeline_state": ps, "config": gh_config}


def _wait_for(predicate, timeout=3.0):
    """The build runs as a background task on the TestClient's event loop."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class TestAuthAndConfig:
    def test_disabled_without_secret(self, client):
        # The default test settings carry no webhook secret.
        resp = _post(client, {"zen": "hi"}, event="ping")
        assert resp.status_code == 404

    def test_exempt_from_session_auth(self, webhook, client):
        # No Authorization header at all, yet the request reaches the handler.
        resp = _post(client, {"zen": "hi"}, event="ping")
        assert resp.status_code == 200
        assert resp.json()["status"] == "pong"

    def test_exemption_is_post_only(self, webhook, client):
        assert client.get(URL).status_code == 401

    def test_exemption_is_exact_path(self, webhook, client):
        assert client.post("/api/webhooks/github/x", json={}).status_code == 401
        assert client.post("/api/webhooks/gitlab", json={}).status_code == 401

    def test_bad_signature(self, webhook, client):
        resp = _post(client, _push_payload(), signature=_sign(b"other body"))
        assert resp.status_code == 401
        webhook["gh"].get_starred_repos.assert_not_awaited()

    def test_wrong_secret(self, webhook, client):
        resp = _post(client, _push_payload(), secret="not-the-secret")
        assert resp.status_code == 401

    def test_missing_signature(self, webhook, client):
        resp = _post(client, _push_payload(), signature="")
        assert resp.status_code == 401

    def test_missing_delivery_header(self, webhook, client):
        resp = _post(client, _push_payload(), delivery="")
        assert resp.status_code == 400

    def test_payload_too_large(self, webhook, client, monkeypatch):
        monkeypatch.setattr(webhook["api"], "_WEBHOOK_MAX_PAYLOAD_BYTES", 10)
        resp = _post(client, _push_payload())
        assert resp.status_code == 413

    def test_invalid_json(self, webhook, client):
        resp = _post(client, None, raw=b"not json")
        assert resp.status_code == 400

    def test_form_encoded_ping(self, webhook, client):
        body = urlencode({"payload": json.dumps({"zen": "hi"})}).encode()
        resp = client.post(URL, content=body, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "form-ping",
            "X-Hub-Signature-256": _sign(body),
        })
        assert resp.status_code == 200 and resp.json()["status"] == "pong"


class TestEvents:
    def test_ping(self, webhook, client):
        resp = _post(client, {"zen": "Keep it simple.", "hook_id": 1}, event="ping")
        assert resp.status_code == 200
        assert resp.json()["status"] == "pong"

    def test_other_event_ignored(self, webhook, client):
        resp = _post(client, {"action": "opened"}, event="pull_request")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ignored"
        webhook["trigger"].assert_not_awaited()

    def test_push_triggers_pipeline(self, webhook, client):
        webhook["state"]["app"] = {"last_sha": OLD_SHA, "building": False}
        resp = _post(client, _push_payload())
        assert resp.status_code == 202
        assert resp.json()["status"] == "triggered"
        assert _wait_for(lambda: webhook["trigger"].await_count == 1)
        args, kwargs = webhook["trigger"].await_args
        assert args == ("app", "git@github.com:octo/app.git")
        assert kwargs == {"version": "1.2.3", "tag": "v1.2.3"}
        webhook["gh"].invalidate_repo_cache.assert_called_once_with("octo", "app")
        # The poller's bookkeeping now points at the pushed commit.
        assert webhook["state"]["app"]["last_sha"] == NEW_SHA

    def test_push_auto_tags_untagged_commit(self, webhook, client):
        webhook["gh"].get_untagged_commits.return_value = {
            "untagged_commits": [{"sha": NEW_SHA}], "latest_tag": {"name": "v1.2.3"}}
        resp = _post(client, _push_payload())
        assert resp.status_code == 202
        assert _wait_for(lambda: webhook["trigger"].await_count == 1)
        webhook["gh"].create_tag.assert_awaited_once_with("octo", "app", "v1.2.4", NEW_SHA)
        assert webhook["trigger"].await_args.kwargs == {"version": "1.2.4", "tag": "v1.2.4"}

    def test_push_unknown_repo_ignored(self, webhook, client):
        resp = _post(client, _push_payload(full_name="stranger/other"))
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ignored" and "not tracked" in body["reason"]
        assert webhook["state"] == {}
        webhook["trigger"].assert_not_awaited()

    def test_push_repo_match_is_case_insensitive(self, webhook, client):
        resp = _post(client, _push_payload(full_name="Octo/App"))
        assert resp.status_code == 202

    def test_push_other_branch_ignored(self, webhook, client):
        resp = _post(client, _push_payload(ref="refs/heads/feature"))
        assert resp.json()["status"] == "ignored"
        webhook["gh"].get_starred_repos.assert_not_awaited()

    def test_tag_push_ignored(self, webhook, client):
        # PulsarCD's own auto-tags come back as tag pushes: never a build.
        resp = _post(client, _push_payload(ref="refs/tags/v1.2.4"))
        assert resp.json()["status"] == "ignored"

    def test_branch_deletion_ignored(self, webhook, client):
        resp = _post(client, _push_payload(after=ZERO_SHA, deleted=True))
        assert resp.json()["status"] == "ignored"

    def test_github_not_configured(self, webhook, client):
        webhook["gh"].is_configured.return_value = False
        resp = _post(client, _push_payload())
        assert resp.json()["status"] == "ignored"

    def test_invalid_push_payload(self, webhook, client):
        resp = _post(client, {"repository": {}})
        assert resp.status_code == 400


class TestDeduplication:
    def test_replayed_delivery_is_noop(self, webhook, client):
        first = _post(client, _push_payload(), delivery="same-id")
        assert first.status_code == 202
        assert _wait_for(lambda: webhook["trigger"].await_count == 1)
        replay = _post(client, _push_payload(after="c" * 40), delivery="same-id")
        assert replay.status_code == 200
        assert replay.json()["status"] == "duplicate"
        time.sleep(0.05)
        assert webhook["trigger"].await_count == 1

    def test_bad_signature_does_not_burn_delivery_id(self, webhook, client):
        assert _post(client, _push_payload(), delivery="d-x",
                     secret="wrong").status_code == 401
        assert _post(client, _push_payload(), delivery="d-x").status_code == 202

    def test_server_error_releases_delivery_id(self, webhook, client):
        webhook["gh"].get_starred_repos.side_effect = RuntimeError("GitHub down")
        with pytest.raises(RuntimeError):
            _post(client, _push_payload(), delivery="d-retry")
        webhook["gh"].get_starred_repos.side_effect = None
        assert _post(client, _push_payload(), delivery="d-retry").status_code == 202

    def test_same_commit_new_delivery_not_rebuilt(self, webhook, client):
        webhook["state"]["app"] = {"last_sha": NEW_SHA, "building": False}
        resp = _post(client, _push_payload())
        assert resp.json()["status"] == "ignored"
        assert "already handled" in resp.json()["reason"]
        webhook["trigger"].assert_not_awaited()

    def test_build_running_defers_to_poller(self, webhook, client):
        webhook["state"]["app"] = {"last_sha": OLD_SHA, "building": True}
        resp = _post(client, _push_payload())
        assert resp.json()["status"] == "deferred"
        state = webhook["state"]["app"]
        # last_sha is left behind so the poller sees the commit as new, and the
        # recheck flag makes it look again even though pushed_at won't change.
        assert state["last_sha"] == OLD_SHA
        assert state["recheck"] is True
        webhook["trigger"].assert_not_awaited()

    def test_deferred_unobserved_repo_gets_baseline(self, webhook, client):
        webhook["state"]["app"] = {"building": True}  # only the lock touched it
        _post(client, _push_payload())
        assert webhook["state"]["app"]["last_sha"] == OLD_SHA

    def test_manual_gate_skips_build_but_advances(self, webhook, client):
        webhook["pipeline_state"].get_transition_config.return_value = {"mode": "manual"}
        webhook["state"]["app"] = {"last_sha": OLD_SHA, "building": False}
        resp = _post(client, _push_payload())
        assert resp.status_code == 202
        assert _wait_for(lambda: webhook["gh"].get_untagged_commits.await_count == 1)
        time.sleep(0.05)
        webhook["trigger"].assert_not_awaited()
        assert webhook["state"]["app"]["last_sha"] == NEW_SHA

    def test_failure_before_trigger_hands_commit_back(self, webhook, client):
        webhook["gh"].get_untagged_commits.side_effect = RuntimeError("GitHub down")
        webhook["state"]["app"] = {"last_sha": OLD_SHA, "building": False}
        _post(client, _push_payload())
        assert _wait_for(lambda: webhook["state"]["app"]["last_sha"] == OLD_SHA)
        api = webhook["api"]
        assert not api._sha_already_handled(webhook["state"]["app"], NEW_SHA)


    def test_stale_listing_does_not_build_previous_release(self, webhook, client):
        """GitHub delivered the push before its REST listing shows the commit.

        The listing still has the previous (tagged) head: building now would
        ship the previous release and mark the new commit as built.
        """
        webhook["gh"].get_repo_commits.return_value = {"commits": [{"sha": OLD_SHA}]}
        webhook["state"]["app"] = {"last_sha": OLD_SHA, "building": False}
        resp = _post(client, _push_payload())
        assert resp.status_code == 202
        state = webhook["state"]["app"]
        assert _wait_for(lambda: state["last_sha"] == OLD_SHA and "recheck_until" in state)
        webhook["trigger"].assert_not_awaited()
        assert webhook["gh"].get_repo_commits.await_count == 3  # one per retry
        # Handed back: the poller re-reads the repo until it builds the commit.
        assert state["recheck_until"] > time.monotonic()
        assert not webhook["api"]._sha_already_handled(state, NEW_SHA)

    def test_listing_catching_up_builds_pushed_commit(self, webhook, client):
        webhook["gh"].get_repo_commits.side_effect = [
            {"commits": [{"sha": OLD_SHA}]}, {"commits": [{"sha": NEW_SHA}]}]
        webhook["gh"].get_untagged_commits.return_value = {
            "untagged_commits": [{"sha": NEW_SHA}], "latest_tag": {"name": "v1.2.3"}}
        webhook["state"]["app"] = {"last_sha": OLD_SHA, "building": False}
        _post(client, _push_payload())
        assert _wait_for(lambda: webhook["trigger"].await_count == 1)
        webhook["gh"].create_tag.assert_awaited_once_with("octo", "app", "v1.2.4", NEW_SHA)

    def test_listing_error_does_not_build_previous_release(self, webhook, client):
        # get_repo_commits reports API errors as an empty listing.
        webhook["gh"].get_repo_commits.return_value = {"commits": [], "error": "Rate limited"}
        webhook["state"]["app"] = {"last_sha": OLD_SHA, "building": False}
        _post(client, _push_payload())
        state = webhook["state"]["app"]
        assert _wait_for(lambda: state["last_sha"] == OLD_SHA and "recheck_until" in state)
        webhook["trigger"].assert_not_awaited()

    def test_superseded_push_stops_quietly(self, webhook, client):
        api = webhook["api"]
        state = {"last_sha": OLD_SHA, "building": False}
        webhook["state"]["app"] = state

        async def newer_push_lands(*a, **k):
            api._advance_last_sha(state, "c" * 40)  # a newer delivery claimed C
            return {"commits": [{"sha": OLD_SHA}]}
        webhook["gh"].get_repo_commits.side_effect = newer_push_lands
        _post(client, _push_payload())
        assert _wait_for(lambda: webhook["gh"].get_repo_commits.await_count >= 1)
        time.sleep(0.05)
        assert webhook["gh"].get_repo_commits.await_count == 1
        assert state["last_sha"] == "c" * 40  # not handed back over the newer commit
        webhook["trigger"].assert_not_awaited()

    def test_handback_for_unobserved_repo_keeps_pre_push_baseline(self, webhook, client):
        # The poller has not looked at the repo yet: handing back by dropping
        # last_sha would make its first look record the commit without a build.
        webhook["gh"].get_repo_commits.return_value = {"commits": []}
        _post(client, _push_payload())
        assert _wait_for(lambda: "recheck_until" in webhook["state"].get("app", {}))
        assert webhook["state"]["app"]["last_sha"] == OLD_SHA
        webhook["trigger"].assert_not_awaited()


class TestPollerBookkeeping:
    def test_advance_remembers_previous_heads(self):
        from backend.api import _advance_last_sha, _sha_already_handled
        state = {"last_sha": OLD_SHA}
        _advance_last_sha(state, NEW_SHA)
        assert state["last_sha"] == NEW_SHA
        # A stale REST listing that still shows the old head is not "new".
        assert _sha_already_handled(state, OLD_SHA)
        assert _sha_already_handled(state, NEW_SHA)
        assert not _sha_already_handled(state, "c" * 40)

    def test_seen_shas_bounded(self):
        from backend.api import _SEEN_SHAS_KEEP, _advance_last_sha
        state = {}
        for i in range(_SEEN_SHAS_KEEP + 10):
            _advance_last_sha(state, f"{i:040d}")
        assert len(state["seen_shas"]) == _SEEN_SHAS_KEEP

    def test_poll_interval_longer_with_webhook(self, webhook):
        api = webhook["api"]
        webhook["config"].webhook_poll_interval = 300
        assert api._auto_build_poll_interval() == 300
        webhook["config"].webhook_poll_interval = 1  # never faster than the default
        assert api._auto_build_poll_interval() == api.AUTO_BUILD_POLL_INTERVAL
        webhook["config"].webhook_secret = None
        assert api._auto_build_poll_interval() == api.AUTO_BUILD_POLL_INTERVAL

    def test_poller_does_not_rebuild_webhook_commit(self, webhook):
        """Run the poller for one cycle after a webhook build: no second build."""
        api = webhook["api"]
        gh = webhook["gh"]
        api._auto_build_state["app"] = {"last_sha": NEW_SHA, "building": False,
                                        "seen_shas": [OLD_SHA]}
        gh.get_starred_repos.return_value = [{
            "name": "app", "full_name": "octo/app", "owner": "octo",
            "ssh_url": "git@github.com:octo/app.git", "pushed_at": "t1"}]

        async def run(listed_sha):
            gh.get_repo_commits = AsyncMock(return_value={"commits": [{"sha": listed_sha}]})
            sleeps = []

            async def fake_sleep(_):
                sleeps.append(1)
                if len(sleeps) > 1:  # initial delay, then one full cycle
                    raise asyncio.CancelledError

            orig = api.asyncio.sleep
            api.asyncio.sleep = fake_sleep
            try:
                with pytest.raises(asyncio.CancelledError):
                    await api.auto_build_poller()
            finally:
                api.asyncio.sleep = orig

        # Both the fresh listing and a stale one showing the previous head.
        asyncio.run(run(NEW_SHA))
        asyncio.run(run(OLD_SHA))
        webhook["trigger"].assert_not_awaited()
        # A genuinely new commit is still built by the fallback poller.
        asyncio.run(run("c" * 40))
        assert webhook["trigger"].await_count == 1

    def test_poller_rechecks_commit_pushed_during_build(self, webhook):
        api = webhook["api"]
        gh = webhook["gh"]
        api._auto_build_state["app"] = {"last_sha": OLD_SHA, "building": True}
        gh.get_starred_repos.return_value = [{
            "name": "app", "full_name": "octo/app", "owner": "octo",
            "ssh_url": "git@github.com:octo/app.git", "pushed_at": "t1"}]
        gh.get_repo_commits = AsyncMock(return_value={"commits": [{"sha": NEW_SHA}]})

        cycles = []

        async def fake_sleep(_):
            cycles.append(1)
            if len(cycles) == 2:
                # First cycle saw the build running; let it finish.
                api._auto_build_state["app"]["building"] = False
            if len(cycles) > 2:
                raise asyncio.CancelledError

        orig = api.asyncio.sleep
        api.asyncio.sleep = fake_sleep
        try:
            with pytest.raises(asyncio.CancelledError):
                asyncio.run(api.auto_build_poller())
        finally:
            api.asyncio.sleep = orig
        # pushed_at never changed between the cycles, yet the commit was built.
        assert webhook["trigger"].await_count == 1
        assert "recheck" not in api._auto_build_state["app"]

    def test_poller_rechecks_after_handback_past_stale_listing(self, webhook):
        """After a hand-back the poller keeps reading the repo even when its
        first read is still stale and pushed_at never changes again."""
        api = webhook["api"]
        gh = webhook["gh"]
        api._auto_build_state["app"] = {"last_sha": OLD_SHA, "building": False}
        api._request_recheck(api._auto_build_state["app"])
        gh.get_starred_repos.return_value = [{
            "name": "app", "full_name": "octo/app", "owner": "octo",
            "ssh_url": "git@github.com:octo/app.git", "pushed_at": "t1"}]
        gh.get_repo_commits = AsyncMock(side_effect=[
            {"commits": [{"sha": OLD_SHA}]},  # stale on the first cycle
            {"commits": [{"sha": NEW_SHA}]},
        ])

        cycles = []

        async def fake_sleep(_):
            cycles.append(1)
            if len(cycles) > 2:
                raise asyncio.CancelledError

        orig = api.asyncio.sleep
        api.asyncio.sleep = fake_sleep
        try:
            with pytest.raises(asyncio.CancelledError):
                asyncio.run(api.auto_build_poller())
        finally:
            api.asyncio.sleep = orig
        assert webhook["trigger"].await_count == 1
        assert api._auto_build_state["app"]["last_sha"] == NEW_SHA
