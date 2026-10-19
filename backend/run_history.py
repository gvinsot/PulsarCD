"""Persistent pipeline run history and build logs in VictoriaLogs.

``pipeline_state`` only keeps the *current* state of each repository and the
last few hundred log lines per stage, so "what was deployed before the outage"
is lost as soon as the next run starts.  This module records every
build / test / qa / deploy run and keeps its full log next to it, chunked so
long build logs stay searchable. Both are append-only, like everything in
VictoriaLogs:

* ``kind=pipeline_run``     one record per state of a stage run (repo, stage,
                            version, commit, trigger, actor, timings, status,
                            error excerpt). A run is written when it starts and
                            again when it ends; its current state is its latest
                            record (``_time``, then ``seq``).
* ``kind=pipeline_run_log`` the run's log, ``chunk_lines`` lines per record,
                            keyed by ``run_id``, stamped with the run's start.

Retention is VictoriaLogs' own (-retentionPeriod).

The recorder is fed from the existing pipeline code paths through a single
call, :meth:`RunHistoryRecorder.observe`, which receives the same
``(repo, stage, status, version)`` transitions ``_set_pipeline`` already
publishes.  A transition to ``running`` opens a run, a terminal status closes
it.  Writes are fire-and-forget background tasks: history is an audit trail,
it must never slow down or break a deployment.

Who/what started a run travels in context variables (``set_trigger``,
``set_actor``, ``begin_pipeline``): asyncio copies them into the background
tasks the pipeline spawns, so the endpoint, the auto-build poller or the MCP
tool that started a run only has to declare itself once.
"""

import asyncio
import contextvars
import itertools
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import structlog

from shared.victorialogs import (
    KIND_PIPELINE_RUN, KIND_PIPELINE_RUN_LOG, any_of, exact, integer, num,
    stream, text_filter,
)

logger = structlog.get_logger()

# Stages recorded in history; "done" is pipeline_state's name for a
# successful deploy and "qa-deploy" the action type of a QA deployment.
STAGES = ("build", "test", "qa", "deploy")
_STAGE_ALIASES = {"done": "deploy", "qa-deploy": "qa"}

TERMINAL_STATUSES = ("success", "failed", "gate_rejected", "cancelled")
ALL_STATUSES = ("running",) + TERMINAL_STATUSES + ("interrupted",)
TRIGGERS = ("manual", "auto", "webhook", "mcp")

# Marker cancel_action() appends to an action's output.
_CANCEL_MARKER = "[Cancellation requested...]"

MAX_PAGE_SIZE = 100
MAX_FROM = 10000                 # deepest offset of a run list page
MAX_LOG_WINDOW = 20000           # lines returned by a single get_run call
MAX_QUERY_LENGTH = 512
MAX_MATCHING_RUNS = 500          # runs a log full-text search can select
ERROR_EXCERPT_LINES = 25
ERROR_EXCERPT_CHARS = 4000
_FINISHED_MEMORY = 512           # action ids remembered to drop duplicate closes

# Record fields of a run and how to read them back (VictoriaLogs returns
# strings and omits empty values).
_RUN_FIELDS: Dict[str, str] = {
    "run_id": "str", "pipeline_id": "str", "repo": "str", "stage": "str",
    "version": "str", "commit": "str", "trigger": "str", "actor": "str",
    "action_id": "str", "status": "str", "timestamp": "str", "started_at": "str",
    "finished_at": "str", "duration_seconds": "float", "error_excerpt": "str",
    "log_line_count": "int", "log_stored_lines": "int", "log_truncated": "bool",
}
# Orders the records of one run written within the same instant.
_SEQ = itertools.count()
_LATEST_PER_RUN = "last 1 by (_time, seq) partition by (run_id)"

_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_COMMIT_RE = re.compile(r"(?i)\b(?:commit|HEAD is now at)[:\s]+([0-9a-f]{7,40})\b")
_ERROR_HINT_RE = re.compile(r"(?i)\b(error|failed|failure|fatal|exception|traceback|denied)\b")
# A pytest -v result line for a test that did not fail. Its name may still
# contain "failed" or "error" (test_plan_rollback[...is the one that failed]).
_PASSING_TEST_RE = re.compile(r"::\S.*\s(PASSED|SKIPPED|XFAIL|XPASS)\b")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# Credentials a build/deploy script may echo. Run logs are kept for months and
# are full-text searchable by viewers, so they are masked before being stored.
_SECRET_PATTERNS = (
    # GitHub tokens (classic and fine-grained)
    (re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"), "[REDACTED]"),
    # user:password@ in URLs
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s/:@]+:)[^\s/@]+@"), r"\1[REDACTED]@"),
    # Authorization headers / bearer tokens
    (re.compile(r"(?i)\b(authorization:\s*(?:bearer|basic|token)\s+)\S+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)\b(bearer\s+)(?!\[REDACTED\])[A-Za-z0-9._~+/=-]{16,}"), r"\1[REDACTED]"),
    # KEY=value / key: value where the key names a secret
    (re.compile(
        r"(?i)\b([A-Z0-9_.-]*(?:password|passwd|secret|token|api[_-]?key|access[_-]?key"
        r"|private[_-]?key)[A-Z0-9_.-]*\s*[=:]\s*)(?!\[REDACTED\])(\"[^\"]*\"|'[^']*'|[^\s\"']+)"),
     r"\1[REDACTED]"),
)

# ── Trigger context ─────────────────────────────────────────────────────────

_trigger_var: contextvars.ContextVar[str] = contextvars.ContextVar("run_trigger", default="manual")
_actor_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("run_actor", default=None)
_commit_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("run_commit", default=None)
_pipeline_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("run_pipeline", default=None)


def set_trigger(trigger: str, actor: Optional[str] = None, commit: Optional[str] = None) -> None:
    """Declare what started the runs created from the current context.

    ``trigger`` is one of :data:`TRIGGERS`; unknown values fall back to
    "manual".  Background tasks created afterwards inherit the values.
    """
    _trigger_var.set(trigger if trigger in TRIGGERS else "manual")
    if actor is not None:
        _actor_var.set(actor)
    if commit is not None:
        _commit_var.set(commit)


def set_actor(actor: Optional[str]) -> None:
    """Record the authenticated user of the current request (auth middleware)."""
    if actor:
        _actor_var.set(actor)


def set_commit(commit: Optional[str]) -> None:
    """Record the commit the runs started from the current context build."""
    if commit:
        _commit_var.set(commit.lower())


def begin_pipeline() -> str:
    """Start a new full-pipeline run: stage runs created from here share an id."""
    pipeline_id = uuid.uuid4().hex[:12]
    _pipeline_var.set(pipeline_id)
    return pipeline_id


# ── Pure helpers ────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_stage(stage: Optional[str]) -> Optional[str]:
    """Map pipeline_state / action-type stage names onto history stages."""
    if not stage:
        return None
    stage = _STAGE_ALIASES.get(stage, stage)
    return stage if stage in STAGES else None


def strip_ansi(line: str) -> str:
    return _ANSI_RE.sub("", line)


def redact_secrets(line: str) -> str:
    """Mask credentials a script printed (tokens, URL passwords, KEY=secret)."""
    for pattern, repl in _SECRET_PATTERNS:
        line = pattern.sub(repl, line)
    return line


def extract_commit(lines: Iterable[str]) -> Optional[str]:
    """Return the last commit SHA a build/deploy script printed, if any.

    build-push.sh ends with "Commit: <sha>", deploy-service.sh prints
    "Using commit: <sha>" and git itself "HEAD is now at <sha>".
    """
    found = None
    for line in lines:
        for match in _COMMIT_RE.finditer(strip_ansi(str(line))):
            found = match.group(1).lower()
    return found


def error_excerpt(lines: List[str]) -> str:
    """Short excerpt explaining a failure: the error lines near the end of the log.

    Prefers lines that look like errors among the last 200, falling back to
    the plain tail when none does.
    """
    clean = [strip_ansi(str(line)).rstrip() for line in lines[-200:]]
    clean = [line for line in clean if line.strip()]
    hits = [line for line in clean
            if _ERROR_HINT_RE.search(line) and not _PASSING_TEST_RE.search(line)]
    picked = (hits or clean)[-ERROR_EXCERPT_LINES:]
    text = "\n".join(picked)
    if len(text) > ERROR_EXCERPT_CHARS:
        text = text[-ERROR_EXCERPT_CHARS:]
    return text


def chunk_lines(lines: List[str], size: int, first_line: int = 0) -> List[Tuple[int, List[str]]]:
    """Split ``lines`` into ``(line_start, chunk)`` pairs of at most ``size`` lines."""
    size = max(1, int(size))
    return [(first_line + i, lines[i:i + size]) for i in range(0, len(lines), size)]


def _clamp(value: Any, minimum: int, maximum: int, default: int) -> int:
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _epoch_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def build_runs_query(
    repo: Optional[str] = None,
    stage: Optional[str] = None,
    status: Optional[str] = None,
    trigger: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    run_ids: Optional[List[str]] = None,
    text: Optional[str] = None,
) -> str:
    """LogsQL selecting the current state of the matching runs (pure, unit-tested).

    Fields a run never changes (repo, stage) filter its records; the others
    filter its latest record only, since its earlier ones still say
    "running".
    """
    parts = [stream(KIND_PIPELINE_RUN, repo=repo)]
    if stage:
        parts.append(exact("stage", normalize_stage(stage) or stage))
    query = " ".join(parts) + f" | {_LATEST_PER_RUN}"
    after: List[str] = []
    if status:
        after.append(exact("status", status))
    if trigger:
        after.append(exact("trigger", trigger))
    if since or until:
        low = _epoch_ms(since) if since else "-Inf"
        high = _epoch_ms(until) if until else "Inf"
        after.append(f"started_ms:range[{low}, {high}]")
    if text:
        # A run matches a free-text query through its error excerpt, its
        # metadata, or any of its log chunks (run_ids resolved beforehand).
        alternatives = [exact("version", text), exact("commit", text.lower()), exact("run_id", text)]
        excerpt = text_filter(text, "error_excerpt", default_op="AND")
        if excerpt:
            alternatives.insert(0, excerpt)
        if run_ids:
            alternatives.append(any_of("run_id", run_ids))
        after.append("(" + " OR ".join(alternatives) + ")")
    if after:
        query += " | filter " + " ".join(after)
    return query


def run_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """A run record as the API returns it: every field, typed, None when empty."""
    run: Dict[str, Any] = {}
    for name, kind in _RUN_FIELDS.items():
        value = row.get(name)
        if value is None or value == "":
            run[name] = False if kind == "bool" else (0 if kind == "int" else None)
        elif kind == "float":
            run[name] = num(value)
        elif kind == "int":
            run[name] = integer(value)
        elif kind == "bool":
            run[name] = str(value).lower() == "true"
        else:
            run[name] = value
    return run


def run_row(doc: Dict[str, Any]) -> Dict[str, Any]:
    """The record appended for one state of a run."""
    started = _parse_time(doc.get("started_at"))
    row = {key: value for key, value in doc.items() if key in _RUN_FIELDS and value is not None}
    row.update({
        "_time": _now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "_msg": " ".join(str(doc.get(k) or "") for k in ("repo", "stage", "status", "version")).strip(),
        "kind": KIND_PIPELINE_RUN,
        "seq": next(_SEQ),
    })
    if started:
        row["started_ms"] = _epoch_ms(started)
    return row


def _matching_lines(message: str, text: str, limit: int = 5) -> List[str]:
    """Lines of a log chunk that contain one of the query's words."""
    words = [w.lower() for w in re.findall(r"[\w.:/-]{2,}", text)
             if w.upper() not in ("AND", "OR", "NOT")]
    if not words:
        return []
    out = []
    for line in message.split("\n"):
        low = line.lower()
        if any(w in low for w in words):
            out.append(line[:500])
            if len(out) >= limit:
                break
    return out


# ── Recorder ────────────────────────────────────────────────────────────────

class _OpenRun:
    """A stage run that has started and not finished yet (in memory only)."""

    __slots__ = ("doc",)

    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc


LogSource = Callable[[str, str, Optional[str]], Optional[List[str]]]


class RunHistoryRecorder:
    """Records pipeline stage runs and their logs in VictoriaLogs."""

    def __init__(self):
        self._client = None                 # VictoriaLogsClient, set by attach()
        self.enabled = True
        self.max_log_lines = 20000
        self.chunk_size = 200
        self._open: Dict[Tuple[str, str], _OpenRun] = {}
        self._finished_actions: Dict[str, None] = {}
        self._tails: Dict[str, asyncio.Task] = {}
        self._tasks: set = set()
        self._log_source: Optional[LogSource] = None

    # ── Wiring ──

    def configure(self, config: Any = None) -> None:
        """Apply RunHistoryConfig values (no I/O)."""
        if config is not None:
            self.enabled = bool(getattr(config, "enabled", True))
            self.max_log_lines = _clamp(getattr(config, "max_log_lines", 20000), 100, 1_000_000, 20000)
            self.chunk_size = _clamp(getattr(config, "log_chunk_lines", 200), 10, 5000, 200)

    async def attach(self, logstore: Any, settings: Any,
                     log_source: Optional[LogSource] = None) -> None:
        """Bind to the application's VictoriaLogs client.

        Failures are logged and leave the recorder detached: deployments keep
        working, they are just not recorded.
        """
        self.configure(getattr(settings, "run_history", None))
        self._log_source = log_source
        if not self.enabled:
            logger.info("Pipeline run history disabled")
            return
        self._client = logstore.client
        await self._mark_interrupted()
        logger.info("Pipeline run history enabled")

    @property
    def available(self) -> bool:
        return self.enabled and self._client is not None

    async def _mark_interrupted(self) -> None:
        """Runs still 'running' at startup were cut short by a restart.

        Each gets a new, final record; nothing is rewritten.
        """
        try:
            rows = await self._client.query(build_runs_query(status="running"))
            if rows:
                closed = [run_row({**run_from_row(row), "status": "interrupted"}) for row in rows]
                await self._client.insert(closed, KIND_PIPELINE_RUN)
                logger.warning("Pipeline runs interrupted by a restart closed", count=len(closed))
        except Exception as e:
            logger.warning("Could not close interrupted pipeline runs", error=str(e))

    # ── Observation (sync, called from the pipeline code paths) ──

    def observe_pipeline(self, repo: str, stage: str, status: str, version: Optional[str],
                         action_ids: Optional[Dict[str, Any]] = None,
                         log_lines: Optional[List[str]] = None) -> None:
        """Adapter for ``_set_pipeline``: picks the stage's id out of its kwargs."""
        hist_stage = normalize_stage(stage)
        action_id = None
        if hist_stage and action_ids:
            candidate = action_ids.get(hist_stage)
            if isinstance(candidate, str) and candidate:
                action_id = candidate
        self.observe(repo, stage, status, version, action_id=action_id, log_lines=log_lines)

    def observe(self, repo: str, stage: str, status: str, version: Optional[str],
                action_id: Optional[str] = None,
                log_lines: Optional[List[str]] = None) -> None:
        """Record a pipeline transition. Never raises."""
        try:
            self._observe(repo, stage, status, version, action_id, log_lines)
        except Exception as e:
            logger.warning("Pipeline run history: observe failed",
                           repo=repo, stage=stage, status=status, error=str(e))

    def _observe(self, repo, stage, status, version, action_id, log_lines) -> None:
        if not self.enabled or not repo:
            return
        if stage == "done":
            status = "success"
        hist_stage = normalize_stage(stage)
        if not hist_stage:
            return
        key = (repo, hist_stage)
        current = self._open.get(key)

        if status == "running":
            if current is not None:
                same = (not action_id or not current.doc.get("action_id")
                        or current.doc["action_id"] == action_id)
                if same:
                    # Same run announced again (pipeline sets "running" before
                    # it knows the action id): just enrich it.
                    if action_id:
                        current.doc["action_id"] = action_id
                    if version:
                        current.doc["version"] = version
                    return
                self._finish(key, current, "interrupted", None)
            self._start(key, repo, hist_stage, version, action_id)
            return

        if status not in TERMINAL_STATUSES:
            return

        if current is None:
            # gate_rejected follows a success already recorded; a failure
            # reported twice (task + cancel endpoint) is dropped by action id.
            if status != "failed" or (action_id and action_id in self._finished_actions):
                return
            self._start(key, repo, hist_stage, version, action_id, persist=False)
            current = self._open[key]
        if action_id:
            current.doc["action_id"] = action_id
        if version:
            current.doc["version"] = version
        self._finish(key, current, status, log_lines)

    def _start(self, key, repo, stage, version, action_id, persist: bool = True) -> None:
        started = _now()
        doc = {
            "run_id": uuid.uuid4().hex[:16],
            "pipeline_id": _pipeline_var.get(),
            "repo": repo,
            "stage": stage,
            "version": version or None,
            "commit": _commit_var.get(),
            "trigger": _trigger_var.get(),
            "actor": _actor_var.get(),
            "action_id": action_id,
            "status": "running",
            "timestamp": started.isoformat(),
            "started_at": started.isoformat(),
            "finished_at": None,
            "duration_seconds": None,
            "error_excerpt": None,
            "log_line_count": 0,
            "log_stored_lines": 0,
            "log_truncated": False,
        }
        self._open[key] = _OpenRun(doc)
        if persist:
            self._schedule(doc["run_id"], self._write_run(dict(doc)))

    def _finish(self, key, run: _OpenRun, status: str, log_lines: Optional[List[str]]) -> None:
        self._open.pop(key, None)
        doc = run.doc
        if log_lines is None and self._log_source is not None:
            try:
                log_lines = self._log_source(doc["repo"], doc["stage"], doc.get("action_id"))
            except Exception:
                log_lines = None
        lines = [redact_secrets(strip_ansi(str(line))) for line in (log_lines or [])]
        if status == "failed" and any(_CANCEL_MARKER in line for line in lines):
            status = "cancelled"

        finished = _now()
        started = _parse_time(doc["started_at"]) or finished
        doc["status"] = status
        doc["finished_at"] = finished.isoformat()
        doc["duration_seconds"] = round(max(0.0, (finished - started).total_seconds()), 3)
        doc["commit"] = doc.get("commit") or extract_commit(lines)
        doc["log_line_count"] = len(lines)
        stored = lines[-self.max_log_lines:]
        doc["log_stored_lines"] = len(stored)
        doc["log_truncated"] = len(stored) < len(lines)
        if status in ("failed", "cancelled", "interrupted"):
            doc["error_excerpt"] = error_excerpt(lines) or None

        if doc.get("action_id"):
            self._finished_actions[doc["action_id"]] = None
            while len(self._finished_actions) > _FINISHED_MEMORY:
                self._finished_actions.pop(next(iter(self._finished_actions)))

        first_line = len(lines) - len(stored)
        logger.info("Pipeline run recorded", repo=doc["repo"], stage=doc["stage"],
                    status=status, version=doc.get("version"), run_id=doc["run_id"],
                    duration=doc["duration_seconds"])
        self._schedule(doc["run_id"], self._write_finished(dict(doc), stored, first_line))

    # ── Background writes ──

    def _schedule(self, run_id: str, coro) -> None:
        """Run ``coro`` after the previous write of the same run (keeps order)."""
        if not self.available:
            coro.close()
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            coro.close()
            return
        previous = self._tails.get(run_id)

        async def _chained():
            if previous is not None:
                try:
                    await previous
                except Exception:
                    pass
            await coro

        task = loop.create_task(_chained())
        self._tails[run_id] = task
        self._tasks.add(task)

        def _done(t, run_id=run_id):
            self._tasks.discard(t)
            if self._tails.get(run_id) is t:
                del self._tails[run_id]

        task.add_done_callback(_done)

    async def drain(self) -> None:
        """Wait for pending writes (tests and graceful shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
            # Awaiting already-finished tasks does not yield: let their done
            # callbacks (which empty self._tasks) run.
            await asyncio.sleep(0)

    async def _write_run(self, doc: Dict[str, Any]) -> None:
        try:
            await self._client.insert([run_row(doc)], KIND_PIPELINE_RUN)
        except Exception as e:
            logger.warning("Pipeline run history: write failed",
                           run_id=doc.get("run_id"), error=str(e))

    async def _write_finished(self, doc: Dict[str, Any], lines: List[str], first_line: int) -> None:
        if lines:
            await self._write_logs(doc, lines, first_line)
        await self._write_run(doc)

    async def _write_logs(self, doc: Dict[str, Any], lines: List[str], first_line: int) -> None:
        rows = []
        for number, (line_start, chunk) in enumerate(chunk_lines(lines, self.chunk_size, first_line)):
            row = {
                # A chunk is stamped with its run's start: a run's log lies
                # in the time window the run does.
                "_time": doc["started_at"],
                "_msg": "\n".join(strip_ansi(line) for line in chunk),
                "kind": KIND_PIPELINE_RUN_LOG,
                "run_id": doc["run_id"],
                "repo": doc["repo"],
                "stage": doc["stage"],
                "version": doc.get("version"),
                "chunk": number,
                "line_start": line_start,
                "line_end": line_start + len(chunk),
            }
            rows.append({key: value for key, value in row.items() if value is not None})
        try:
            await self._client.insert(rows, KIND_PIPELINE_RUN_LOG)
        except Exception as e:
            logger.warning("Pipeline run history: log write failed",
                           run_id=doc["run_id"], error=str(e))

    # ── Queries ──

    def _require(self) -> None:
        if not self.available:
            raise RuntimeError("Pipeline run history is not available")

    @staticmethod
    def _log_scope(repo: Optional[str], stage: Optional[str]) -> str:
        parts = [stream(KIND_PIPELINE_RUN_LOG, repo=repo)]
        if stage:
            parts.append(exact("stage", normalize_stage(stage) or stage))
        return " ".join(parts)

    async def _run_ids_matching_logs(self, text: str, repo: Optional[str],
                                     stage: Optional[str],
                                     since: Optional[datetime] = None,
                                     until: Optional[datetime] = None) -> List[str]:
        words = text_filter(text, default_op="AND")
        if not words:
            return []
        # A log chunk's _time is its run's started_at. Most recent runs
        # first, so a broad query keeps the newest matches.
        rows = await self._client.query(
            f"{self._log_scope(repo, stage)} {words} | stats by (run_id) max(_time) as latest "
            f"| sort by (latest desc) limit {MAX_MATCHING_RUNS}",
            start=since, end=until)
        return [row["run_id"] for row in rows if row.get("run_id")]

    async def list_runs(
        self,
        repo: Optional[str] = None,
        stage: Optional[str] = None,
        status: Optional[str] = None,
        trigger: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        q: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """Paginated run list, most recent first."""
        self._require()
        page = _clamp(page, 1, 100000, 1)
        page_size = _clamp(page_size, 1, MAX_PAGE_SIZE, 20)
        start = max(0, min((page - 1) * page_size, MAX_FROM - page_size))
        text = (q or "").strip()[:MAX_QUERY_LENGTH] or None
        run_ids = await self._run_ids_matching_logs(text, repo, stage, since, until) if text else None
        query = build_runs_query(repo, stage, status, trigger, since, until, run_ids, text)
        totals, rows = await asyncio.gather(
            self._client.query(f"{query} | stats count() as n"),
            self._client.query(f"{query} | sort by (started_ms desc) offset {start} limit {page_size}"),
        )
        runs = [run_from_row(row) for row in rows]
        # Overlay in-memory state for runs still open in this process.
        open_ids = {r.doc["run_id"] for r in self._open.values()}
        for run in runs:
            if run.get("status") == "running" and run.get("run_id") not in open_ids:
                run["status_note"] = "not running in this process"
        total = integer(totals[0].get("n")) if totals else 0
        return {"runs": runs, "total": total, "page": page, "page_size": page_size}

    async def get_run(self, run_id: str, log_offset: int = 0,
                      log_limit: int = 5000) -> Optional[Dict[str, Any]]:
        """One run with a window of its log lines, or None when unknown.

        A negative ``log_offset`` counts from the end of the log (``-200`` =
        the last 200 lines), the useful view of a failed build.
        """
        self._require()
        if not _RUN_ID_RE.match(run_id or ""):
            return None
        rows = await self._client.query(
            f"{stream(KIND_PIPELINE_RUN)} {exact('run_id', run_id)} | {_LATEST_PER_RUN}")
        if not rows:
            return None
        run = run_from_row(rows[0])
        stored = int(run.get("log_stored_lines") or 0)
        total = int(run.get("log_line_count") or 0)
        first_stored = total - stored
        log_limit = _clamp(log_limit, 1, MAX_LOG_WINDOW, 5000)
        try:
            log_offset = int(log_offset)
        except (TypeError, ValueError):
            log_offset = 0
        if log_offset < 0:
            log_offset = max(first_stored, total + log_offset)
        log_offset = max(first_stored, log_offset)
        end = min(total, log_offset + log_limit)

        lines: List[str] = []
        if stored and end > log_offset:
            chunks = await self._client.query(
                f"{stream(KIND_PIPELINE_RUN_LOG, repo=run.get('repo'))} {exact('run_id', run_id)} "
                f"line_start:<{end} line_end:>{log_offset} | sort by (chunk) | fields _msg, line_start")
            for chunk_row in chunks:
                chunk = (chunk_row.get("_msg") or "").split("\n")
                chunk_start = integer(chunk_row.get("line_start"))
                lo = max(0, log_offset - chunk_start)
                hi = max(0, end - chunk_start)
                lines.extend(chunk[lo:hi])
        return {
            "run": run,
            "logs": {
                "offset": log_offset,
                "lines": lines,
                "total": total,
                "first_available": first_stored,
                "truncated": bool(run.get("log_truncated")),
            },
        }

    async def search_logs(self, q: str, repo: Optional[str] = None,
                          stage: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Full-text search across stored run logs; returns matching lines per chunk."""
        self._require()
        text = (q or "").strip()[:MAX_QUERY_LENGTH]
        words = text_filter(text, default_op="AND") if text else None
        if not words:
            return []
        limit = _clamp(limit, 1, 100, 20)
        rows = await self._client.query(
            f"{self._log_scope(repo, stage)} {words} | sort by (_time desc, chunk) limit {limit} "
            "| fields _time, _msg, run_id, repo, stage, version, line_start")
        return [{
            "run_id": row.get("run_id"),
            "repo": row.get("repo"),
            "stage": row.get("stage"),
            "version": row.get("version"),
            "timestamp": row.get("_time"),
            "line_start": integer(row.get("line_start")),
            "lines": _matching_lines(row.get("_msg") or "", text),
        } for row in rows]

    async def list_repos(self) -> List[str]:
        """Repositories that have at least one recorded run."""
        self._require()
        rows = await self._client.query(
            f"{stream(KIND_PIPELINE_RUN)} | stats by (repo) count() as n | sort by (repo)")
        return [row["repo"] for row in rows if row.get("repo")]


recorder = RunHistoryRecorder()
