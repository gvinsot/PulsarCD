"""Outbound notifications: payloads, rate limiting, delivery and secrecy."""

import asyncio
import json
import smtplib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import web
from fastapi.testclient import TestClient

from backend import notifier as nt
from backend.config import NotifyConfig, Settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_notifier():
    """Each test starts from (and leaves) an unconfigured notifier."""
    nt.configure(None)
    yield
    nt.configure(None)


class Receiver:
    """A real local HTTP server recording what the channels POST."""

    def __init__(self, status=200, delay=0.0):
        self.status = status
        self.delay = delay
        self.requests = []
        self._runner = None
        self.url = ""

    async def _handle(self, request):
        self.requests.append({
            "path": request.path,
            "headers": dict(request.headers),
            "json": await request.json(),
        })
        if self.delay:
            await asyncio.sleep(self.delay)
        return web.json_response({}, status=self.status)

    async def __aenter__(self):
        app = web.Application()
        app.router.add_post("/{tail:.*}", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *exc):
        await self._runner.cleanup()


async def _drain():
    """Wait for the fire-and-forget deliveries scheduled so far."""
    tasks = list(nt.get_notifier()._tasks)
    if tasks:
        await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class TestConfig:
    def test_disabled_by_default(self):
        n = nt.Notifier(NotifyConfig())
        assert n.channels() == []
        assert not n.enabled
        assert n.event_enabled("pipeline_failure")
        assert not n.event_enabled("deploy_success")  # opt-in

    def test_env_nesting(self, monkeypatch):
        monkeypatch.setenv("PULSARCD_NOTIFY__NTFY_URL", "https://ntfy.example")
        monkeypatch.setenv("PULSARCD_NOTIFY__NTFY_TOPIC", "ops")
        monkeypatch.setenv("PULSARCD_NOTIFY__ON_DEPLOY_SUCCESS", "true")
        monkeypatch.setenv("PULSARCD_NOTIFY__EMAIL_TO", "a@example.com, b@example.com")
        s = Settings()
        assert s.notify.ntfy_url == "https://ntfy.example"
        assert s.notify.on_deploy_success is True
        assert nt.email_recipients(s.notify.email_to) == ["a@example.com", "b@example.com"]

    def test_swarm_compose_defaults_load(self, monkeypatch):
        """The empty/explicit defaults devops/docker-compose.swarm.yml passes."""
        import re
        from pathlib import Path
        compose = Path(__file__).resolve().parent.parent / "devops" / "docker-compose.swarm.yml"
        found = re.findall(r"- (PULSARCD_NOTIFY__\w+)=\$\{\1:-([^}]*)\}", compose.read_text())
        assert len(found) >= 10
        for name, default in found:
            monkeypatch.setenv(name, default)
        s = Settings()
        assert not nt.Notifier(s.notify).enabled
        assert s.notify.smtp_port == 587

    def test_channels_require_complete_targets(self):
        assert nt.Notifier(NotifyConfig(ntfy_url="https://n.example")).channels() == []
        assert nt.Notifier(NotifyConfig(smtp_host="smtp", email_from="x@y")).channels() == []
        full = nt.Notifier(NotifyConfig(
            ntfy_url="https://n.example", ntfy_topic="t", webhook_url="https://h.example/x",
            smtp_host="smtp", email_from="x@y", email_to="z@y"))
        assert full.channels() == ["ntfy", "webhook", "email"]

    def test_configure_ignores_non_config(self):
        # The API tests run with a MagicMock settings object.
        assert not nt.configure(SimpleNamespace()).enabled

    def test_status_never_exposes_secrets(self):
        cfg = NotifyConfig(
            ntfy_url="https://ntfy.example", ntfy_topic="secret-topic-name",
            ntfy_token="tk_supersecret",
            webhook_url="https://discord.com/api/webhooks/123/webhooktoken",
            smtp_host="smtp.example", smtp_password="smtppass", email_from="a@b",
            email_to="c@d")
        dumped = json.dumps(nt.Notifier(cfg).status())
        for secret in ("secret-topic-name", "tk_supersecret", "webhooktoken", "smtppass", "/api/webhooks"):
            assert secret not in dumped
        assert '"format": "discord"' in dumped


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------

class TestPayloads:
    def _n(self, **kw):
        base = dict(event=nt.PIPELINE_FAILURE, title="app: build failed",
                    message="The build stage failed.", severity="error")
        base.update(kw)
        return nt.Notification(**base)

    @pytest.mark.parametrize("url,fmt", [
        ("https://discord.com/api/webhooks/1/x", "discord"),
        ("https://canary.discordapp.com/api/webhooks/1/x", "discord"),
        ("https://hooks.slack.com/services/T/B/X", "slack"),
        ("https://chat.example/hooks/abc", "generic"),
        ("https://hooks.slack.com.evil.example/x", "generic"),
    ])
    def test_auto_format(self, url, fmt):
        assert nt.webhook_format_for(url, "auto") == fmt

    def test_explicit_format_wins(self):
        assert nt.webhook_format_for("https://discord.com/api/webhooks/1/x", "slack") == "slack"

    def test_discord_disables_mentions(self):
        p = nt.webhook_payload(self._n(title="@everyone deploy"), "discord")
        assert p["allowed_mentions"] == {"parse": []}
        assert "@everyone deploy" in p["content"]
        assert len(p["content"]) <= 2000

    def test_slack_and_generic(self):
        assert "text" in nt.webhook_payload(self._n(), "slack")
        g = nt.webhook_payload(self._n(url="https://cd.example"), "generic")
        assert g["event"] == "pipeline_failure" and g["severity"] == "error"
        assert g["text"] == g["content"] and "https://cd.example" in g["text"]

    def test_ntfy_payload(self):
        p = nt.ntfy_payload(self._n(url="https://cd.example"), "ops")
        assert p["topic"] == "ops" and p["priority"] == 4
        assert p["click"] == "https://cd.example"

    def test_text_is_sanitized_and_bounded(self):
        n = self._n(title="a\x1b[31mb" + "x" * 500, message="m" * 5000)
        assert "\x1b" not in n.title
        assert len(n.title) <= nt.MAX_TITLE_CHARS
        assert len(n.message) <= nt.MAX_MESSAGE_CHARS

    def test_email_message(self):
        msg = nt.email_message(self._n(), "cd@example.com", ["a@example.com", "b@example.com"])
        assert msg["Subject"] == "[PulsarCD] app: build failed"
        assert msg["To"] == "a@example.com, b@example.com"

    @pytest.mark.parametrize("raw,leak", [
        ("DB_PASSWORD=hunter2 failed", "hunter2"),
        ("connect postgres://user:s3cr3t@db:5432 refused", "s3cr3t"),
        ("Authorization: Bearer abcdef123456 rejected", "abcdef123456"),
        ('{"api_key": "AKIA-something"} bad', "AKIA-something"),
        ("token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig expired", "eyJhbGciOiJIUzI1NiJ9"),
        ("key 0123456789abcdef0123456789abcdef0123 invalid", "0123456789abcdef0123456789abcdef"),
    ])
    def test_redact(self, raw, leak):
        assert leak not in nt.redact(raw)


# ---------------------------------------------------------------------------
# Event helpers and rate limiting
# ---------------------------------------------------------------------------

class TestEvents:
    def _capture(self, **cfg):
        n = nt.configure(NotifyConfig(webhook_url="https://hook.example/x", **cfg))
        sent = []
        n.deliver = AsyncMock(side_effect=lambda notification: sent.append(notification) or {})
        return n, sent

    async def test_unconfigured_is_noop(self):
        assert nt.notify_pipeline_stage("app", "build", "failed", "1.0") is False

    async def test_pipeline_failure_and_success(self):
        _, sent = self._capture(on_deploy_success=True)
        assert nt.notify_pipeline_stage("app", "test", "failed", "1.2.3")
        assert nt.notify_pipeline_stage("app", "done", "success", "1.2.3")
        assert not nt.notify_pipeline_stage("app", "build", "running", "1.2.3")
        assert not nt.notify_pipeline_stage("app", "build", "success", "1.2.3")
        await _drain()
        assert [s.event for s in sent] == ["pipeline_failure", "deploy_success"]
        assert sent[0].title == "app: test failed"
        assert "v1.2.3" in sent[1].title

    async def test_deploy_success_is_opt_in(self):
        _, sent = self._capture()
        assert not nt.notify_pipeline_stage("app", "done", "success", "1.0")

    async def test_event_switch(self):
        self._capture(on_pipeline_failure=False)
        assert not nt.notify_pipeline_stage("app", "build", "failed", "1.0")

    async def test_duplicates_are_suppressed(self):
        _, sent = self._capture()
        assert nt.notify_pipeline_stage("app", "deploy", "failed", "1.0")
        assert not nt.notify_pipeline_stage("app", "deploy", "failed", "1.0")
        assert nt.notify_pipeline_stage("app", "deploy", "failed", "1.1")  # new version
        assert nt.notify_auto_ban("203.0.113.9", "Automatic probe: .env")
        assert not nt.notify_auto_ban("203.0.113.9", "Automatic probe: .env")
        await _drain()
        assert len(sent) == 3

    async def test_dedupe_window_expires(self):
        n, _ = self._capture(dedupe_seconds=60)
        note = lambda: nt.Notification(event="auto_ban", title="t", message="m", key="k")
        assert n._admit(note(), now=1000.0)
        assert not n._admit(note(), now=1059.0)
        assert n._admit(note(), now=1061.0)

    async def test_hourly_cap_reports_dropped(self):
        n, sent = self._capture(max_per_hour=2, dedupe_seconds=0)
        for ip in ("198.51.100.1", "198.51.100.2", "198.51.100.3", "198.51.100.4"):
            nt.notify_auto_ban(ip, "probe")
        await _drain()
        assert len(sent) == 2
        # Once the window frees up, the next message mentions the dropped ones.
        n._window.clear()
        nt.notify_auto_ban("198.51.100.5", "probe")
        await _drain()
        assert "2 earlier notification(s) dropped" in sent[-1].message

    async def test_recurring_error_sample_opt_in_and_redacted(self):
        _, sent = self._capture()
        nt.notify_recurring_error("fp1", 12, ["api"], ["shop"], "DB_PASSWORD=hunter2 boom")
        await _drain()
        assert "Sample" not in sent[0].message and "hunter2" not in sent[0].message
        assert "12 times in shop" in sent[0].message

        _, sent = self._capture(include_error_samples=True)
        nt.notify_recurring_error("fp2", 3, ["api"], [], "DB_PASSWORD=hunter2 boom")
        await _drain()
        assert "Sample:" in sent[0].message and "hunter2" not in sent[0].message

    def test_without_event_loop_never_raises(self):
        self._capture()
        assert nt.notify_pipeline_stage("app", "build", "failed", "1.0") is False

    async def test_helpers_never_raise(self):
        n, _ = self._capture()
        with patch.object(n, "submit", side_effect=RuntimeError("boom")):
            assert nt.notify_auto_ban("203.0.113.1", "x") is False


# ---------------------------------------------------------------------------
# Delivery over real HTTP
# ---------------------------------------------------------------------------

class TestDelivery:
    async def test_ntfy_and_webhook(self):
        async with Receiver() as rx:
            n = nt.configure(NotifyConfig(
                ntfy_url=rx.url + "/", ntfy_topic="ops", ntfy_token="tk_abc",
                webhook_url=rx.url + "/hook", webhook_format="discord",
                public_url="https://cd.example"))
            assert nt.notify_pipeline_stage("shop", "deploy", "failed", "2.0.0")
            await _drain()
        by_path = {r["path"]: r for r in rx.requests}
        assert by_path["/"]["json"]["topic"] == "ops"
        assert by_path["/"]["headers"]["Authorization"] == "Bearer tk_abc"
        assert by_path["/"]["json"]["click"] == "https://cd.example"
        assert "shop: deploy failed" in by_path["/hook"]["json"]["content"]
        assert n.last_results["channels"] == {"ntfy": {"ok": True}, "webhook": {"ok": True}}
        assert n.sent_count == 1

    async def test_http_error_is_reported_without_url(self, capsys):
        async with Receiver(status=404) as rx:
            n = nt.configure(NotifyConfig(webhook_url=rx.url + "/api/webhooks/1/SECRETTOKEN"))
            results = await n.send_test()
        assert results == {"webhook": {"ok": False, "error": "HTTP 404"}}
        assert "SECRETTOKEN" not in capsys.readouterr().out

    async def test_unreachable_target_reports_type_only(self, capsys):
        n = nt.configure(NotifyConfig(webhook_url="http://127.0.0.1:1/SECRETTOKEN",
                                      timeout_seconds=2))
        results = await n.send_test()
        assert results["webhook"]["ok"] is False
        assert "SECRETTOKEN" not in results["webhook"]["error"]
        assert "SECRETTOKEN" not in capsys.readouterr().out

    async def test_timeout(self):
        async with Receiver(delay=3) as rx:
            n = nt.configure(NotifyConfig(webhook_url=rx.url + "/x", timeout_seconds=0.5))
            results = await n.send_test()
        assert results["webhook"]["ok"] is False

    async def test_submit_returns_before_delivery(self):
        async with Receiver(delay=0.5) as rx:
            n = nt.configure(NotifyConfig(webhook_url=rx.url + "/x"))
            loop = asyncio.get_running_loop()
            start = loop.time()
            assert nt.notify_auto_ban("203.0.113.7", "probe")
            assert loop.time() - start < 0.1
            await n.close()

    async def test_email(self):
        sent = []

        class FakeSMTP:
            def __init__(self, host, port, timeout):
                self.calls = [("connect", host, port)]

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                sent.append(self.calls)

            def starttls(self, context):
                self.calls.append(("starttls",))

            def login(self, user, password):
                self.calls.append(("login", user))

            def send_message(self, msg):
                self.calls.append(("send", msg["To"], msg["Subject"]))

        n = nt.configure(NotifyConfig(
            smtp_host="smtp.example", smtp_username="u", smtp_password="p",
            email_from="cd@example.com", email_to="ops@example.com"))
        with patch.object(nt.smtplib, "SMTP", FakeSMTP):
            results = await n.send_test()
        assert results == {"email": {"ok": True}}
        assert sent[0] == [("connect", "smtp.example", 587), ("starttls",), ("login", "u"),
                           ("send", "ops@example.com", "[PulsarCD] PulsarCD test notification")]

    async def test_email_auth_failure(self):
        class Refusing:
            def __init__(self, *a, **kw):
                raise smtplib.SMTPAuthenticationError(535, b"bad credentials for p")

        n = nt.configure(NotifyConfig(smtp_host="smtp.example", email_from="a@b",
                                      email_to="c@d", smtp_starttls=False))
        with patch.object(nt.smtplib, "SMTP", Refusing):
            results = await n.send_test()
        assert results == {"email": {"ok": False, "error": "SMTP 535"}}


# ---------------------------------------------------------------------------
# Integration with the event sources
# ---------------------------------------------------------------------------

class TestSources:
    async def test_auto_ban_notifies_new_bans_only(self, tmp_path):
        from backend import ip_blocklist as bl
        from backend.auto_ban import AutoBanWorker
        from backend.config import AutoBanConfig

        store = bl.IpBlocklist(str(tmp_path / "blocked.json"))
        worker = AutoBanWorker(SimpleNamespace(), store, AutoBanConfig(exclude_cloudflare=False),
                               checkpoint_path=str(tmp_path / "cp.json"))
        now = datetime.now(timezone.utc)
        source = {
            "compose_project": "privatenetwork", "compose_service": "traefik",
            "timestamp": now.isoformat(),
            "parsed_fields": {"ClientHost": "8.8.8.8", "DownstreamStatus": 404,
                              "RequestPath": "/.env", "RouterName": "app@docker"},
        }
        with patch("backend.auto_ban.notify_auto_ban") as notify:
            await worker._consume(source, now)
            await worker._consume(source, now)  # replay: no second ban
        notify.assert_called_once()
        assert notify.call_args.args[0] == "8.8.8.8"
        assert ".env" in notify.call_args.args[1]

    async def test_recurring_error_notifies(self):
        from backend.error_detector import ErrorPattern, RecurringErrorDetector

        detector = RecurringErrorDetector(opensearch_client=AsyncMock(), llm_agent=None)
        pattern = ErrorPattern("abc123", "Connection refused", "api", "shop")
        pattern.count = 7
        with patch("backend.error_detector.notify_recurring_error") as notify:
            await detector._notify_recurring_error(pattern)
        notify.assert_called_once()
        assert notify.call_args.args[:3] == ("abc123", 7, ["api"])


# ---------------------------------------------------------------------------
# Admin API
# ---------------------------------------------------------------------------

class TestAdminApi:
    def test_status_and_test_endpoints_unconfigured(self, client, auth_headers):
        r = client.get("/api/admin/notifications", headers=auth_headers)
        assert r.status_code == 200 and r.json()["enabled"] is False
        r = client.post("/api/admin/notifications/test", headers=auth_headers)
        assert r.status_code == 200
        assert r.json()["sent"] is False and "PULSARCD_NOTIFY__" in r.json()["detail"]

    def test_test_endpoint_delivers(self, client, auth_headers):
        n = nt.configure(NotifyConfig(webhook_url="https://hook.example/secret"))
        with patch.object(n, "_send_webhook", AsyncMock()) as send:
            r = client.post("/api/admin/notifications/test", headers=auth_headers)
        assert r.json() == {"sent": True, "channels": {"webhook": {"ok": True}}}
        send.assert_awaited_once()
        assert "secret" not in client.get("/api/admin/notifications", headers=auth_headers).text

    def test_requires_authentication(self, client):
        assert client.get("/api/admin/notifications").status_code == 401
        assert client.post("/api/admin/notifications/test").status_code == 401
