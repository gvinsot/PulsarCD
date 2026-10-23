"""Automatic sanctions: real store, bounded log traversal, replay and trust."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

import pytest

from backend import ip_blocklist as bl
from backend.auto_ban import AutoBanWorker, probe_kind, scanner_agent
from backend.config import AutoBanConfig, Settings


@pytest.mark.parametrize("path,kind", [
    ("/.env", ".env"), ("/API/.ENV.production?x=1", ".env"),
    ("/app/%252eenv~", ".env"), ("/a%255c.env", ".env"),
    ("/.env;v=1", ".env"), ("/a/../.env.bak", ".env"),
    ("/docs/my.env", ".env"), ("/production.ENV", ".env"),
    ("/config/app.env~", ".env"), ("/x%252eenv", ".env"),
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
    ("/docs/my.envelope", None), ("/app.env.js", None), ("/wp-admin/../normal", None),
    ("/oauth.json", None), ("/auth.json-guide", None), ("/authentication.json", None),
    ("/api?file=auth.json", None), ("/auth.json/../normal", None),
])
def test_paths(path, kind):
    assert probe_kind(path) == kind


@pytest.mark.parametrize("path,kind", [
    ("/.git/config", "VCS metadata"), ("/app/.git/HEAD", "VCS metadata"), ("/.git", "VCS metadata"),
    ("/.svn/entries", "VCS metadata"), ("/.hg/hgrc", "VCS metadata"),
    ("/.vscode/sftp.json", "VCS metadata"), ("/.DS_Store", "VCS metadata"),
    ("/%252egit/config", "VCS metadata"),
    ("/.htpasswd", "credentials file"), ("/.htaccess.bak", "credentials file"),
    ("/.git-credentials", "credentials file"), ("/.aws/credentials", "credentials file"),
    ("/home/.ssh/id_rsa", "credentials file"), ("/id_ed25519.pub", "credentials file"),
    ("/.npmrc", "credentials file"), ("/../../etc/passwd", "credentials file"),
    ("/proc/self/environ", "credentials file"),
    ("/phpmyadmin/index.php", "admin/exploit endpoint"), ("/PhpMyAdmin", "admin/exploit endpoint"),
    ("/cgi-bin/luci", "admin/exploit endpoint"), ("/boaform/admin/formLogin", "admin/exploit endpoint"),
    ("/HNAP1/", "admin/exploit endpoint"), ("/server-status", "admin/exploit endpoint"),
    ("/_ignition/execute-solution", "admin/exploit endpoint"),
    ("/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php", "admin/exploit endpoint"),
    ("/phpinfo.php", "admin/exploit endpoint"), ("/uploads/shell.php", "admin/exploit endpoint"),
    ("/manager/html", "admin/exploit endpoint"), ("/actuator/env", "admin/exploit endpoint"),
    ("/dump.sql", "backup dump"), ("/db/prod.SQL.gz", "backup dump"),
    ("/backup.zip", "backup dump"), ("/www.tar.gz", "backup dump"),
    ("/.github/workflows", None), ("/.gitignore", None), ("/.well-known/acme-challenge/x", None),
    ("/docs/git/config", None), ("/htaccess-guide", None), ("/api/ssh/keys", None),
    ("/etc/passwords-guide", None), ("/admin", None), ("/actuator/health", None),
    ("/manager", None), ("/information.php", None), ("/sql/schema", None),
    ("/sqlite.db", None), ("/backup", None), ("/my-backup.zip", None),
    ("/api?file=.git/config", None), ("/v2/library/sql/manifests/latest", None),
])
def test_extended_paths(path, kind):
    assert probe_kind(path) == kind


@pytest.mark.parametrize("agent,tool", [
    ("sqlmap/1.8#stable (https://sqlmap.org)", "sqlmap"),
    ("Mozilla/5.0 (compatible; Nmap Scripting Engine; https://nmap.org/book/nse.html)", "nmap"),
    ("Mozilla/5.0 zgrab/0.x", "zgrab"), ("Nuclei - Open-source project", "nuclei"),
    ("Mozilla/5.0 (X11; Linux x86_64) Firefox/131.0", None), ("", None), (None, None),
])
def test_scanner_agent(agent, tool):
    assert scanner_agent(agent) == tool


async def test_scanner_user_agent_bans_on_any_path(tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0])
    w.logstore.client.rows = rows(
        log(clock_now[0], path="/", **{"request_User-Agent": "sqlmap/1.8"}),
        log(clock_now[0], ip="45.9.12.8", path="/", **{"request_User-Agent": "Mozilla/5.0"}))
    await w.scan_once(clock_now[0])
    assert store.blocked_entry("45.9.12.7")["reason"] == "Automatic probe: scanner sqlmap"
    assert store.blocked_entry("45.9.12.8") is None


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


class FakeLogClient:
    """Streams rows like VictoriaLogsClient.iter_query; records the queries."""

    def __init__(self):
        self.rows = []
        self.error = None
        self.fail_after = None
        self.on_call = None
        self.calls = []

    async def iter_query(self, query, **kwargs):
        self.calls.append((query, kwargs))
        if self.on_call:
            self.on_call()
        for count, row in enumerate(self.rows):
            if self.error is not None and self.fail_after is not None and count >= self.fail_after:
                raise self.error
            yield row
        if self.error is not None and self.fail_after is None:
            raise self.error


def worker(tmp_path, store, now, config=None):
    logstore = SimpleNamespace(client=FakeLogClient())
    result = AutoBanWorker(logstore, store, config or AutoBanConfig(), str(tmp_path / "cursor.json"))
    result._checkpoint = now
    return result


def rows(*entries):
    """Log entries as VictoriaLogs returns them (parsed_fields is a JSON string)."""
    return [{"_time": e["timestamp"], "compose_project": e["compose_project"],
             "compose_service": e["compose_service"], "parsed_fields": json.dumps(e["parsed_fields"])}
            for e in entries]


async def test_auth_json_probe_creates_auditable_24h_ban(tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0])
    w.logstore.client.rows = rows(log(clock_now[0], path="/.composer/auth.json"))
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


async def test_scans_every_row_and_persists_cursor(tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0])
    w.logstore.client.rows = rows(log(clock_now[0], "45.9.12.7"), log(clock_now[0], "8.8.8.8"))
    await w.scan_once(clock_now[0])
    assert store.targets() == ["45.9.12.7", "8.8.8.8"]
    (query, kwargs), = w.logstore.client.calls
    assert query.startswith('{kind="log"} compose_project:="privatenetwork" compose_service:="traefik"')
    # The window covers the checkpoint minus the overlap, through now included
    assert kwargs["start"] == clock_now[0] - timedelta(seconds=300)
    assert kwargs["end"] > clock_now[0]
    assert json.loads((tmp_path / "cursor.json").read_text())["through"] == clock_now[0].isoformat()


async def test_failed_scan_retains_checkpoint_and_retry_does_not_undo_manual_unblock(
        tmp_path, store, clock_now):
    w = worker(tmp_path, store, clock_now[0] - timedelta(seconds=60))
    old_checkpoint = w._checkpoint
    probe = log(clock_now[0] - timedelta(seconds=1))
    w.logstore.client.rows = rows(probe, probe)
    w.logstore.client.error, w.logstore.client.fail_after = RuntimeError("unavailable"), 1
    with pytest.raises(RuntimeError):
        await w.scan_once(clock_now[0])
    assert w._checkpoint == old_checkpoint
    await store.remove("45.9.12.7")
    w.logstore.client.error = None
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
    # Startup reconciliation must not depend on the log store's availability.
    queried = asyncio.Event()
    w.logstore.client.on_call = queried.set
    w.logstore.client.error = RuntimeError("store unavailable")
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
    assert w.logstore.client.calls == []
    await store.add("45.9.12.7", "manual")
    assert store.targets() == ["45.9.12.7"]
    assert (tmp_path / "cursor.json").read_text() == "invalid json"


async def test_interrupted_scan_never_advances_checkpoint(tmp_path, store, clock_now):
    before = clock_now[0] - timedelta(seconds=10)
    w = worker(tmp_path, store, before)
    w.logstore.client.rows = rows(log(clock_now[0]))
    w.logstore.client.error, w.logstore.client.fail_after = RuntimeError("Incomplete scan"), 0
    with pytest.raises(RuntimeError, match="Incomplete"):
        await w.scan_once(clock_now[0])
    assert w._checkpoint == before and store.targets() == []
    assert not (tmp_path / "cursor.json").exists()
