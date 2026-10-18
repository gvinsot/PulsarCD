"""Outbound notifications: ntfy, a Discord/Slack-compatible webhook and SMTP.

Until now pipeline failures only reached the LLM agent, and automatic edge bans
or recurring error patterns were only visible in the UI. This module pushes
those events to a human, through whichever channels PULSARCD_NOTIFY__* enables
(see NotifyConfig in backend/config.py). With nothing configured it is a no-op.

Guarantees, because callers sit in the middle of the build/deploy pipeline and
the auto-ban scanner:

- Non-blocking: the ``notify_*`` helpers are synchronous, schedule delivery as a
  background task and return immediately. Every channel has a timeout and every
  error is swallowed (and logged without its message, see below).
- Rate-limited: the same event key is sent at most once per ``dedupe_seconds``
  and at most ``max_per_hour`` notifications leave per rolling hour. Dropped
  notifications are counted and reported in the next one that goes out.
- No secrets: tokens, webhook URLs and SMTP passwords are never logged nor
  returned by ``status()``. Delivery errors are logged by exception type and
  HTTP status only, because an aiohttp error message embeds the request URL,
  and a Discord/Slack webhook URL *is* its credential. Messages never contain
  build output or .env values; recurring error samples are opt-in and redacted.
"""

import asyncio
import functools
import re
import smtplib
import ssl
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Deque, Dict, List, Optional, Set
from urllib.parse import urlsplit

import aiohttp
import structlog

from .config import NotifyConfig

logger = structlog.get_logger()

# Event kinds, matching the on_<kind> switches of NotifyConfig.
PIPELINE_FAILURE = "pipeline_failure"
DEPLOY_SUCCESS = "deploy_success"
ROLLBACK = "rollback"
AUTO_BAN = "auto_ban"
RECURRING_ERROR = "recurring_error"
TEST = "test"

EVENT_KINDS = (PIPELINE_FAILURE, DEPLOY_SUCCESS, ROLLBACK, AUTO_BAN, RECURRING_ERROR)

# Discord rejects content over 2000 characters; keep every channel well below.
MAX_MESSAGE_CHARS = 1500
MAX_TITLE_CHARS = 200
MAX_SAMPLE_CHARS = 200

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
_LINE_BREAKS = re.compile(r"[\r\n\t  \x85]+")

# Redaction applied to (opt-in) log samples. Deliberately broad: a false
# positive costs a few characters of context, a false negative leaks a secret.
_REDACTIONS = (
    # scheme://user:password@host
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@"), r"\1[REDACTED]@"),
    # Authorization: Bearer xxx / Basic xxx
    (re.compile(r"(?i)\b(bearer|basic|token)\s+[A-Za-z0-9._~+/=-]{6,}"), r"\1 [REDACTED]"),
    # key=value / key: value for secret-looking keys (also FOO_PASSWORD=...)
    (re.compile(
        r"(?i)([A-Za-z0-9_.-]*(?:pass(?:word|wd)?|secret|token|api[_-]?key|apikey|"
        r"access[_-]?key|private[_-]?key|credential|auth|session|cookie|dsn)[A-Za-z0-9_.-]*"
        r"[\"']?\s*[:=]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&]+)"),
     r"\1[REDACTED]"),
    # JWTs
    (re.compile(r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*"), "[REDACTED]"),
    # Long opaque tokens (hex / base64-ish runs of 32+ characters)
    (re.compile(r"\b[A-Za-z0-9+/_-]{32,}={0,2}"), "[REDACTED]"),
)


def redact(text: str) -> str:
    """Mask credentials that commonly appear in log lines."""
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def _clean(text: Any, limit: int) -> str:
    """Single-purpose sanitizer: str, no control characters, bounded length."""
    text = _CONTROL_CHARS.sub(" ", str(text if text is not None else ""))
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return text


def _host_of(url: str) -> str:
    """Return only the host part of a URL, for display. Never the path/query."""
    try:
        return urlsplit(url).hostname or ""
    except ValueError:
        return ""


@dataclass
class Notification:
    """One message, rendered identically by every channel."""
    event: str
    title: str
    message: str
    severity: str = "info"  # info | warning | error
    # Deduplication key; defaults to event + title.
    key: str = ""
    url: str = ""
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))

    def __post_init__(self):
        # One line: the title becomes an e-mail Subject header, where a line
        # break makes EmailMessage raise (and would be header injection).
        self.title = _clean(_LINE_BREAKS.sub(" ", str(self.title or "")), MAX_TITLE_CHARS)
        self.message = _clean(self.message, MAX_MESSAGE_CHARS)
        if not self.key:
            self.key = f"{self.event}:{self.title}"


class ChannelError(Exception):
    """A delivery failure whose text is safe to log and to return to an admin."""


# ---------------------------------------------------------------------------
# Payload builders (pure, unit-tested)
# ---------------------------------------------------------------------------

_NTFY_PRIORITY = {"info": 3, "warning": 4, "error": 4}
_NTFY_TAGS = {
    PIPELINE_FAILURE: ["rotating_light"],
    DEPLOY_SUCCESS: ["white_check_mark"],
    ROLLBACK: ["leftwards_arrow_with_hook"],
    AUTO_BAN: ["no_entry"],
    RECURRING_ERROR: ["warning"],
    TEST: ["bell"],
}
_EMOJI = {"info": "ℹ️", "warning": "⚠️", "error": "\U0001f6a8"}


def ntfy_payload(n: Notification, topic: str) -> Dict[str, Any]:
    """JSON publish body (https://docs.ntfy.sh/publish/#publish-as-json).

    JSON rather than headers: header values must be latin-1, a title need not.
    """
    payload: Dict[str, Any] = {
        "topic": topic,
        "title": n.title,
        "message": n.message,
        "priority": _NTFY_PRIORITY.get(n.severity, 3),
        "tags": _NTFY_TAGS.get(n.event, []),
    }
    if n.url:
        payload["click"] = n.url
    return payload


def webhook_format_for(url: str, configured: str) -> str:
    """Resolve "auto" to the payload shape the target expects."""
    if configured and configured != "auto":
        return configured
    host = _host_of(url).lower()
    if host.endswith("discord.com") or host.endswith("discordapp.com"):
        return "discord"
    if host == "hooks.slack.com":
        return "slack"
    return "generic"


def _slack_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def webhook_payload(n: Notification, fmt: str) -> Dict[str, Any]:
    """Discord, Slack or generic JSON body for one notification."""
    link = f"\n{n.url}" if n.url else ""
    emoji = _EMOJI.get(n.severity, "")
    if fmt == "discord":
        return {
            "username": "PulsarCD",
            "content": f"{emoji} **{n.title}**\n{n.message}{link}"[:2000],
            # Repo names or log samples must not be able to ping @everyone.
            "allowed_mentions": {"parse": []},
        }
    if fmt == "slack":
        # Slack parses <...> as links and mentions (<!channel>, <@U123>):
        # escape its three control characters so the text cannot ping anyone.
        return {"text": f"{emoji} *{_slack_escape(n.title)}*\n{_slack_escape(n.message)}{link}"}
    # Generic: carries both "text" (Slack, Mattermost, Rocket.Chat, Teams
    # connectors) and "content" (Discord-like) plus structured fields.
    return {
        "text": f"{n.title}\n{n.message}{link}",
        "content": f"{n.title}\n{n.message}{link}",
        "event": n.event,
        "title": n.title,
        "message": n.message,
        "severity": n.severity,
        "timestamp": n.timestamp,
        "url": n.url or None,
        "source": "pulsarcd",
    }


def email_recipients(raw: str) -> List[str]:
    """Split the comma/semicolon/whitespace separated email_to setting."""
    return [r for r in re.split(r"[,;\s]+", raw or "") if r]


def email_message(n: Notification, sender: str, recipients: List[str]) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = f"[PulsarCD] {n.title}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    body = n.message + (f"\n\n{n.url}" if n.url else "")
    msg.set_content(f"{body}\n\n-- \nPulsarCD, {n.timestamp}\n")
    return msg


# ---------------------------------------------------------------------------
# Notifier
# ---------------------------------------------------------------------------

class Notifier:
    """Delivers notifications to the configured channels, rate-limited."""

    def __init__(self, config: Optional[NotifyConfig] = None):
        self.config = config if isinstance(config, NotifyConfig) else NotifyConfig()
        self._last_sent: Dict[str, float] = {}
        self._window: Deque[float] = deque()
        self._suppressed = 0
        self._tasks: Set[asyncio.Task] = set()
        self.last_results: Dict[str, Any] = {}
        self.sent_count = 0

    # ---- configuration ----------------------------------------------------

    def channels(self) -> List[str]:
        """Names of the channels whose target is fully configured."""
        c = self.config
        names = []
        if c.ntfy_url and c.ntfy_topic:
            names.append("ntfy")
        if c.webhook_url:
            names.append("webhook")
        if c.smtp_host and c.email_from and email_recipients(c.email_to):
            names.append("email")
        return names

    @property
    def enabled(self) -> bool:
        return bool(self.channels())

    def event_enabled(self, event: str) -> bool:
        return bool(getattr(self.config, f"on_{event}", False))

    def status(self) -> Dict[str, Any]:
        """Configuration summary for the admin UI. Contains no secret."""
        c = self.config
        return {
            "enabled": self.enabled,
            "channels": {
                "ntfy": {
                    "configured": "ntfy" in self.channels(),
                    "server": _host_of(c.ntfy_url),
                    # The topic name is a credential on a public ntfy server.
                    "topic_set": bool(c.ntfy_topic),
                    "token_set": bool(c.ntfy_token),
                },
                "webhook": {
                    "configured": "webhook" in self.channels(),
                    "host": _host_of(c.webhook_url),
                    "format": webhook_format_for(c.webhook_url, c.webhook_format)
                    if c.webhook_url else c.webhook_format,
                },
                "email": {
                    "configured": "email" in self.channels(),
                    "server": c.smtp_host,
                    "port": c.smtp_port,
                    "recipients": len(email_recipients(c.email_to)),
                    "tls": "ssl" if c.smtp_ssl else ("starttls" if c.smtp_starttls else "none"),
                },
            },
            "events": {kind: self.event_enabled(kind) for kind in EVENT_KINDS},
            "include_error_samples": c.include_error_samples,
            "dedupe_seconds": c.dedupe_seconds,
            "max_per_hour": c.max_per_hour,
            "sent": self.sent_count,
            "suppressed": self._suppressed,
            "last_results": self.last_results,
        }

    # ---- rate limiting ----------------------------------------------------

    def _admit(self, n: Notification, now: Optional[float] = None) -> bool:
        """Apply dedupe and the hourly cap. Records the send when admitted."""
        now = time.monotonic() if now is None else now
        dedupe = self.config.dedupe_seconds
        last = self._last_sent.get(n.key)
        if dedupe and last is not None and now - last < dedupe:
            logger.debug("Notification deduplicated", notify_event=n.event)
            return False
        while self._window and now - self._window[0] >= 3600:
            self._window.popleft()
        if len(self._window) >= self.config.max_per_hour:
            self._suppressed += 1
            if self._suppressed == 1 or self._suppressed % 50 == 0:
                logger.warning("Notification rate limit reached, dropping",
                               notify_event=n.event, suppressed=self._suppressed)
            return False
        self._window.append(now)
        self._last_sent[n.key] = now
        # Bound memory: forget keys whose dedupe window has passed.
        if len(self._last_sent) > 2048:
            horizon = now - max(dedupe, 1)
            self._last_sent = {k: t for k, t in self._last_sent.items() if t >= horizon}
        return True

    # ---- delivery ---------------------------------------------------------

    def submit(self, n: Notification) -> bool:
        """Fire-and-forget: schedule delivery and return immediately.

        Returns True when the notification was scheduled. Never raises.
        """
        try:
            if not self.enabled or not self.event_enabled(n.event):
                return False
            if not self._admit(n):
                return False
            if self._suppressed:
                n.message = _clean(
                    f"{n.message}\n({self._suppressed} earlier notification(s) dropped by the rate limit)",
                    MAX_MESSAGE_CHARS)
                self._suppressed = 0
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.debug("No running event loop, notification dropped", notify_event=n.event)
                return False
            task = loop.create_task(self.deliver(n), name=f"notify-{n.event}")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            return True
        except Exception as e:  # Never break the caller.
            logger.warning("Notification scheduling failed", error_type=type(e).__name__)
            return False

    async def deliver(self, n: Notification) -> Dict[str, Dict[str, Any]]:
        """Send to every configured channel concurrently; never raises."""
        senders = {"ntfy": self._send_ntfy, "webhook": self._send_webhook,
                   "email": self._send_email}
        names = self.channels()
        timeout = self.config.timeout_seconds

        async def _one(name: str) -> Dict[str, Any]:
            try:
                await asyncio.wait_for(senders[name](n), timeout=timeout + 1)
                return {"ok": True}
            except asyncio.TimeoutError:
                error = "timed out"
            except ChannelError as e:
                error = str(e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Type only: messages of aiohttp/smtplib errors embed URLs/hosts.
                error = type(e).__name__
            logger.warning("Notification delivery failed", channel=name,
                           notify_event=n.event, error=error)
            return {"ok": False, "error": error}

        results = dict(zip(names, await asyncio.gather(*(_one(name) for name in names))))
        if any(r["ok"] for r in results.values()):
            self.sent_count += 1
        self.last_results = {
            "event": n.event,
            "at": n.timestamp,
            "channels": results,
        }
        logger.info("Notification delivered", notify_event=n.event,
                    channels={k: v["ok"] for k, v in results.items()})
        return results

    async def send_test(self) -> Dict[str, Dict[str, Any]]:
        """Send a test message to every channel, bypassing switches and limits."""
        n = Notification(
            event=TEST,
            title="PulsarCD test notification",
            message="Notifications are configured correctly on this PulsarCD instance.",
            severity="info",
            url=self.config.public_url,
        )
        return await self.deliver(n)

    async def _post_json(self, url: str, payload: Dict[str, Any],
                         headers: Optional[Dict[str, str]] = None) -> None:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload, headers=headers or {},
                                    allow_redirects=False) as response:
                if response.status >= 300:
                    raise ChannelError(f"HTTP {response.status}")

    async def _send_ntfy(self, n: Notification) -> None:
        c = self.config
        headers = {"Authorization": f"Bearer {c.ntfy_token}"} if c.ntfy_token else {}
        # JSON publishing posts to the server root, the topic goes in the body.
        await self._post_json(c.ntfy_url.rstrip("/") + "/", ntfy_payload(n, c.ntfy_topic), headers)

    async def _send_webhook(self, n: Notification) -> None:
        c = self.config
        fmt = webhook_format_for(c.webhook_url, c.webhook_format)
        await self._post_json(c.webhook_url, webhook_payload(n, fmt))

    async def _send_email(self, n: Notification) -> None:
        c = self.config
        msg = email_message(n, c.email_from, email_recipients(c.email_to))
        await asyncio.to_thread(self._smtp_send, msg)

    def _smtp_send(self, msg: EmailMessage) -> None:
        c = self.config
        context = ssl.create_default_context()
        try:
            if c.smtp_ssl:
                client = smtplib.SMTP_SSL(c.smtp_host, c.smtp_port,
                                          timeout=c.timeout_seconds, context=context)
            else:
                client = smtplib.SMTP(c.smtp_host, c.smtp_port, timeout=c.timeout_seconds)
            with client:
                if not c.smtp_ssl and c.smtp_starttls:
                    client.starttls(context=context)
                if c.smtp_username:
                    client.login(c.smtp_username, c.smtp_password)
                client.send_message(msg)
        except smtplib.SMTPResponseException as e:
            raise ChannelError(f"SMTP {e.smtp_code}") from None

    async def close(self) -> None:
        """Give in-flight deliveries a moment, then cancel them (shutdown)."""
        tasks = list(self._tasks)
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=2)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


# ---------------------------------------------------------------------------
# Process-wide instance and event helpers
# ---------------------------------------------------------------------------

_notifier = Notifier()


def get_notifier() -> Notifier:
    return _notifier


def configure(config: Optional[NotifyConfig]) -> Notifier:
    """Install the configuration loaded at startup. Returns the notifier."""
    global _notifier
    _notifier = Notifier(config)
    if _notifier.enabled:
        logger.info("Outbound notifications enabled", channels=_notifier.channels(),
                    events=[k for k in EVENT_KINDS if _notifier.event_enabled(k)])
    return _notifier


def _never_raise(func):
    """Event helpers run inside the pipeline and the scanners: never break them."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> bool:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning("Notification skipped", helper=func.__name__,
                           error_type=type(e).__name__)
            return False
    return wrapper


def _link(path: str = "") -> str:
    base = _notifier.config.public_url.rstrip("/")
    return f"{base}{path}" if base else ""


@_never_raise
def notify_pipeline_stage(repo: str, stage: str, status: str, version: str = "") -> bool:
    """Map a pipeline state transition to a notification, if it deserves one.

    Called on every transition; only ``failed`` (any stage) and the final
    ``done``/``success`` of a production deploy produce a message.
    """
    version_text = f" v{str(version).lstrip('v')}" if version else ""
    if status == "failed":
        stage_name = "QA deploy" if stage == "qa" else stage
        return _notifier.submit(Notification(
            event=PIPELINE_FAILURE,
            title=f"{repo}: {stage_name} failed",
            message=f"The {stage_name} stage of {repo}{version_text} failed. "
                    f"Open PulsarCD for the logs.",
            severity="error",
            key=f"{PIPELINE_FAILURE}:{repo}:{stage}:{version}",
            url=_link(),
        ))
    if stage == "done" and status == "success":
        return _notifier.submit(Notification(
            event=DEPLOY_SUCCESS,
            title=f"{repo}{version_text} deployed",
            message=f"{repo}{version_text} was deployed to production.",
            severity="info",
            key=f"{DEPLOY_SUCCESS}:{repo}:{version}",
            url=_link(),
        ))
    return False


@_never_raise
def notify_rollback(repo: str, from_version: str, to_version: str, reason: str = "") -> bool:
    """For an automatic rollback (no such mechanism exists yet; ready for it)."""
    reason_text = f" Reason: {reason}." if reason else ""
    return _notifier.submit(Notification(
        event=ROLLBACK,
        title=f"{repo} rolled back to v{str(to_version).lstrip('v')}",
        message=f"{repo} was rolled back from v{str(from_version).lstrip('v')} "
                f"to v{str(to_version).lstrip('v')}.{reason_text}",
        severity="warning",
        key=f"{ROLLBACK}:{repo}:{from_version}:{to_version}",
        url=_link(),
    ))


@_never_raise
def notify_auto_ban(ip: str, reason: str, expires_at: str = "") -> bool:
    """An address was blocked at the edge by the automatic probe scanner."""
    until = f" until {expires_at}" if expires_at else ""
    return _notifier.submit(Notification(
        event=AUTO_BAN,
        title=f"Edge ban: {ip}",
        message=f"{ip} was automatically blocked at the edge{until}. {reason}",
        severity="warning",
        key=f"{AUTO_BAN}:{ip}",
        url=_link(),
    ))


@_never_raise
def notify_recurring_error(fingerprint: str, count: int, services: List[str],
                           stacks: List[str], sample: str = "") -> bool:
    """A recurring error pattern was confirmed by the error detector."""
    where = ", ".join(stacks or services or ["unknown"])
    service_text = ", ".join(services[:5]) + (" ..." if len(services) > 5 else "")
    message = (f"An error recurred {count} times in {where} "
               f"(services: {service_text or 'unknown'}).")
    if sample and _notifier.config.include_error_samples:
        message += f"\nSample: {_clean(redact(sample), MAX_SAMPLE_CHARS)}"
    return _notifier.submit(Notification(
        event=RECURRING_ERROR,
        title=f"Recurring error in {where}",
        message=message,
        severity="warning",
        key=f"{RECURRING_ERROR}:{fingerprint}",
        url=_link(),
    ))
