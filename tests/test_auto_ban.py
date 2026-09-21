"""Automatic sanctions: real store, bounded log traversal, replay and trust."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from backend import ip_blocklist as bl
from backend.auto_ban import AutoBanWorker, probe_kind
from backend.config import AutoBanConfig, Settings


@pytest.mark.parametrize("path,kind", [
    ("/.env", ".env"), ("/API/.ENV.production?x=1", ".env"),
    ("/app/%252eenv~", ".env"), ("/a%255c.env", ".env"),
    ("/.env;v=1", ".env"), ("/a/../.env.bak", ".env"),
    ("/auth.json", "auth.json"), ("/app/auth.json", "auth.json"),
    ("/.composer/auth.json", "auth.json"), ("/AUTH.JSON", "auth.json"),
    ("/%61uth%2ejson", "auth.json"), ("/%2561uth%252ejson", "auth.json"),
    ("/auth.json.bak", "auth.json"), ("/auth.json~", "auth.json"),
    ("/auth.json;v=1", "auth.json"), ("/auth.json/PATH_INFO", "auth.json"),
    ("/app%255cauth.json", "auth.json"), ("/a/../auth.json", "auth.json"),
    ("/blog/wp-admin", "WordPress"), ("/WP-ADMIN/", "WordPress"),
    ("/sub/wp-login.php?next=/", "WordPress"),
    ("/wp-config.php.bak", "WordPress"), ("/wp-json;v=1", "WordPress"),
    ("/blog/xmlrpc.php", "WordPress"), ("/x%255cwp-includes/x", "WordPress"),
    ("/", None), ("/environment", None), ("/.environment", None),
    ("/wp-admin-guide", None), ("/wp-login.phpx", None),
    ("/search?q=/.env", None), ("/search?next=/wp-admin", None),
    ("/docs/my.env", None), ("/wp-admin/../normal", None),
    ("/oauth.json", None), ("/auth.json-guide", None), ("/authentication.json", None),
    ("/api?file=auth.json", None), ("/auth.json/../normal", None),
])
def test_paths(path, kind):
    assert probe_kind(path) == kind


@pytest.mark.parametrize("name", ["wp-admin", "wp-content", "wp-includes", "wp-json",
    "wp-login.php", "wp-config.php", "wp-cron.php", "wp-signup.php", "wp-activate.php",
    "wp-blog-header.php", "wp-load.php", "wp-mail.php", "wp-comments-post.php",
    "wp-trackback.php", "xmlrpc.php"])
def test_every_wordpress_path(name):
    assert probe_kind(f"/sub/{name}") == "WordPress"


@pytest.fixture
def clock_now():
    now = [datetime(2026, 9, 21, 12, 0, 0, 900000, tzinfo=timezone.utc)]
    with patch.object(bl, "_now", side_effect=lambda: now[0].isoformat()):
        yield now


@pytest.fixture
def store(tmp_path):
    return bl.IpBlocklist(str(tmp_path / "blocked_ips.json"))


def log(now, ip="45.9.12.7", path="/.env", **fields):
    return {"timestamp": now.isoformat(), "compose_project": "privatenetwork",
            "compose_service": "traefik", "parsed_fields": {
                "ClientHost": ip, "DownstreamStatus": 403, "RequestPath": path, **fields}}


def worker(tmp_path, store, now, config=None):
    os = SimpleNamespace(_client=AsyncMock(), logs_index="test-logs")
    result = AutoBanWorker(os, store, config or AutoBanConfig(), str(tmp_path / "cursor.json"))
    result._checkpoint = now
    return result


def page(*entries, scroll="cursor"):
    return {"_scroll_id": scroll, "hits": {"hits": [{"_source": entry} for entry in entries]}}


async def test_auth_json_probe_creates_auditable_24h_ban(tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0])
    w.opensearch._client.search.return_value = page(
        log(clock_now[0], path="/.composer/auth.json"))
    w.opensearch._client.scroll.return_value = page()
    await w.scan_once(clock_now[0])
    entry = store.blocked_entry("45.9.12.7")
    assert entry["reason"] == "Automatic probe: auth.json"
    assert entry["blocked_by"] == "automatic:probe"
    assert datetime.fromisoformat(entry["expires_at"].replace("Z", "+00:00")) == (
        clock_now[0] + timedelta(hours=24))


async def test_expiry_all_reads_persists_manual_and_replay_guards(store, tmp_path, clock_now):
    observed = clock_now[0] - timedelta(seconds=1)
    await store.add("8.8.8.8", "manual")
    entry = await store.add_automatic("45.9.12.7", "probe", observed, 60)
    assert entry["blocked_by"] == "automatic:probe"
    assert entry["expires_at"]
    clock_now[0] += timedelta(seconds=61)
    for lookup in (lambda s: s.targets(), lambda s: s.list_entries(),
                   lambda s: s.blocked_entry("45.9.12.7"), lambda s: s.traefik_config()):
        reloaded = bl.IpBlocklist(str(tmp_path / "blocked_ips.json"))
        lookup(reloaded)
        assert reloaded.targets() == ["8.8.8.8"]
        assert await reloaded.add_automatic("45.9.12.7", "probe", observed, 60) is None


async def test_no_refresh_and_existing_manual_ban_preserved(store, clock_now):
    initial = await store.add_automatic("45.9.12.7", "probe", clock_now[0], 60)
    clock_now[0] += timedelta(seconds=20)
    assert await store.add_automatic("45.9.12.7", "probe", clock_now[0], 60) is None
    assert store.blocked_entry("45.9.12.7")["expires_at"] == initial["expires_at"]
    await store.add("8.8.8.8", "manual", "admin")
    await store.add_automatic("8.8.8.8", "probe", clock_now[0], 60)
    assert store.blocked_entry("8.8.8.8")["expires_at"] is None
    assert store.blocked_entry("8.8.8.8")["blocked_by"] == "admin"


@pytest.mark.parametrize("target", ["45.9.12.7", "45.9.12.0/24"])
async def test_manual_release_ignores_delayed_subsecond_probe_after_restart(
        store, tmp_path, clock_now, target):
    await store.add(target)
    await store.remove(target)
    reloaded = bl.IpBlocklist(str(tmp_path / "blocked_ips.json"))
    old = clock_now[0] - timedelta(microseconds=100)
    assert await reloaded.add_automatic("45.9.12.7", "old", old, 60) is None
    fresh = clock_now[0] + timedelta(microseconds=100)
    assert await reloaded.add_automatic("45.9.12.7", "fresh", fresh, 60)


async def test_scans_every_page_and_clears_scroll_and_persists_cursor(tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0], AutoBanConfig(batch_size=1))
    w.opensearch._client.search.return_value = page(log(clock_now[0], "45.9.12.7"))
    w.opensearch._client.scroll.side_effect = [page(log(clock_now[0], "8.8.8.8")), page()]
    await w.scan_once(clock_now[0])
    assert store.targets() == ["45.9.12.7", "8.8.8.8"]
    assert w.opensearch._client.scroll.await_count == 2
    w.opensearch._client.clear_scroll.assert_awaited_once_with(scroll_id="cursor")
    body = w.opensearch._client.search.call_args.kwargs["body"]
    assert body["size"] == 1 and body["sort"] == ["_doc"]
    assert {"term": {"compose_service": "traefik"}} in body["query"]["bool"]["filter"]
    assert json.loads((tmp_path / "cursor.json").read_text())["through"] == clock_now[0].isoformat()


async def test_failed_scan_retains_checkpoint_and_retry_does_not_undo_manual_unblock(
        tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0] - timedelta(seconds=60))
    old_checkpoint = w._checkpoint
    w.opensearch._client.search.return_value = page(log(clock_now[0] - timedelta(seconds=1)))
    w.opensearch._client.scroll.side_effect = RuntimeError("unavailable")
    with pytest.raises(RuntimeError):
        await w.scan_once(clock_now[0])
    assert w._checkpoint == old_checkpoint
    await store.remove("45.9.12.7")
    w.opensearch._client.scroll.side_effect = None
    w.opensearch._client.scroll.return_value = page()
    await w.scan_once(clock_now[0])
    assert store.targets() == []


@pytest.mark.parametrize("ip", ["192.168.1.50", "127.0.0.1", "::1", "fd00::1",
    "169.254.1.1", "100.64.0.1", "224.0.0.1", "2001:db8::1", "not-an-ip", "45.9.12.0/24"])
async def test_nonpublic_or_nonaddress_never_banned(ip, tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0])
    await w._consume(log(clock_now[0], ip), clock_now[0])
    assert store.targets() == []


async def test_exemptions_headers_provenance_and_blocked_responses(tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0], AutoBanConfig(exempt_cidrs=["45.9.12.0/24"]))
    await w._consume(log(clock_now[0]), clock_now[0])
    await w._consume(log(clock_now[0], "10.0.0.1", **{"request_X-Forwarded-For": "8.8.8.8"}), clock_now[0])
    forged = log(clock_now[0], "8.8.8.8")
    forged["compose_service"] = "application"
    await w._consume(forged, clock_now[0])
    for router in (bl.ROUTER_NAME, bl.HTTP_ROUTER_NAME):
        await w._consume(log(clock_now[0], "8.8.8.8", RouterName=router + "@http"), clock_now[0])
    assert store.targets() == []
    await w._consume(log(clock_now[0], "2a01:e0a::1", path="/wp-admin"), clock_now[0])
    assert store.targets() == ["2a01:e0a::1"]


@pytest.mark.parametrize("ip", ["162.158.222.234", "104.23.221.84", "2606:4700::1234"])
async def test_cloudflare_proxy_peers_are_exempt_by_default(ip, tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0])
    await w._consume(log(clock_now[0], ip, path="/auth.json"), clock_now[0])
    assert store.targets() == []


async def test_startup_releases_only_exempt_automatic_bans_and_persists_guards(
        tmp_path, store, clock_now):
    old = clock_now[0] - timedelta(seconds=1)
    for ip in ("162.158.222.234", "104.23.221.84", "2606:4700::1234", "8.8.8.8", "45.138.12.22"):
        await store.add_automatic(ip, "probe", old, 86400)
    await store.add("162.158.222.235", "manual", "admin")
    await store.add("104.24.0.0/14", "manual range", "admin")
    reloaded = bl.IpBlocklist(str(tmp_path / "blocked_ips.json"))
    w = worker(tmp_path, reloaded, clock_now[0], AutoBanConfig(exempt_cidrs=["8.8.8.8"]))
    # Startup reconciliation must not depend on OpenSearch availability.
    queried = asyncio.Event()
    async def unavailable(*args, **kwargs):
        queried.set()
        raise RuntimeError("index unavailable")
    w.opensearch._client.search.side_effect = unavailable
    assert w.start()
    await asyncio.wait_for(queried.wait(), timeout=2)
    await w.stop()
    persisted = bl.IpBlocklist(str(tmp_path / "blocked_ips.json"))
    assert set(persisted.targets()) == {"45.138.12.22", "162.158.222.235", "104.24.0.0/14"}
    assert persisted.blocked_entry("162.158.222.235")["blocked_by"] == "admin"
    assert persisted.blocked_entry("45.138.12.22")["blocked_by"] == "automatic:probe"
    assert "162.158.222.234" in persisted._auto_observed
    assert await persisted.add_automatic("162.158.222.234", "old probe", old, 86400) is None
    # Fresh Cloudflare requests remain exempt, not merely skipped as replays.
    await w._consume(log(clock_now[0], "162.158.222.234"), clock_now[0])
    assert reloaded.blocked_entry("162.158.222.234") is None


async def test_cloudflare_exclusion_can_be_explicitly_disabled(tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0], AutoBanConfig(exclude_cloudflare=False))
    await w._consume(log(clock_now[0], "162.158.222.234"), clock_now[0])
    assert store.targets() == ["162.158.222.234"]


async def test_full_blocklist_keeps_event_retryable_and_expired_capacity_is_reclaimed(store, clock_now):
    with patch.object(bl, "MAX_ENTRIES", 1):
        await store.add_automatic("8.8.8.8", "probe", clock_now[0], 60)
        with pytest.raises(ValueError, match="full"):
            await store.add_automatic("45.9.12.7", "probe", clock_now[0], 60)
        assert "45.9.12.7" not in store._auto_observed
        clock_now[0] += timedelta(seconds=61)
        assert await store.add_automatic("45.9.12.7", "probe", clock_now[0], 60)
        assert store.targets() == ["45.9.12.7"]


def test_autoban_environment_and_validation(monkeypatch):
    monkeypatch.setenv("PULSARCD_AUTOBAN__ENABLED", "true")
    monkeypatch.setenv("PULSARCD_AUTOBAN__DURATION_SECONDS", "3600")
    monkeypatch.setenv("PULSARCD_AUTOBAN__EXEMPT_CIDRS", '["8.8.8.8"]')
    config = Settings().autoban
    assert config.enabled and config.duration_seconds == 3600
    assert config.exempt_cidrs == ["8.8.8.8/32"]
    assert config.exclude_cloudflare is True
    monkeypatch.setenv("PULSARCD_AUTOBAN__EXCLUDE_CLOUDFLARE", "false")
    assert Settings().autoban.exclude_cloudflare is False
    with pytest.raises(ValueError):
        AutoBanConfig(exempt_cidrs=["bad-network"])


async def test_failed_atomic_save_rolls_back_guard_and_can_retry(store, tmp_path, clock_now):
    with patch.object(Path, "replace", side_effect=OSError("disk unavailable")):
        with pytest.raises(OSError):
            await store.add_automatic("45.9.12.7", "probe", clock_now[0], 60)
    assert store.targets() == [] and store._auto_observed == {}
    assert await store.add_automatic("45.9.12.7", "probe", clock_now[0], 60)
    reloaded = bl.IpBlocklist(str(tmp_path / "blocked_ips.json"))
    assert reloaded.targets() == ["45.9.12.7"]


async def test_corrupt_checkpoint_disables_only_worker_keeps_manual_controls(tmp_path, store, clock_now):
    (tmp_path / "cursor.json").write_text("invalid json", encoding="utf-8")
    w = worker(tmp_path, store, clock_now[0])
    assert w.initialization_error
    assert w.start() is False
    await w.scan_once(clock_now[0])
    w.opensearch._client.search.assert_not_awaited()
    await store.add("45.9.12.7", "manual")
    assert store.targets() == ["45.9.12.7"]
    assert (tmp_path / "cursor.json").read_text() == "invalid json"


@pytest.mark.parametrize("failure", [{"timed_out": True}, {"_shards": {"failed": 1}}])
async def test_incomplete_search_never_advances_checkpoint(tmp_path, store, clock_now, failure):
    before = clock_now[0] - timedelta(seconds=10)
    w = worker(tmp_path, store, before)
    w.opensearch._client.search.return_value = {**page(log(clock_now[0])), **failure}
    with pytest.raises(RuntimeError, match="Incomplete"):
        await w.scan_once(clock_now[0])
    assert w._checkpoint == before and store.targets() == []
    assert not (tmp_path / "cursor.json").exists()
