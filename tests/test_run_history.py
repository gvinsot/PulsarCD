"""Tests for the persistent pipeline run history (backend/run_history.py)."""

import asyncio
import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backend import run_history
from backend.config import RunHistoryConfig
from backend.run_history import (
    RunHistoryRecorder, build_runs_query, chunk_lines, error_excerpt,
    extract_commit, normalize_stage,
)
from shared.victorialogs import KIND_PIPELINE_RUN, KIND_PIPELINE_RUN_LOG


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class FakeClient:
    """Stand-in for VictoriaLogsClient: records inserts, answers queries."""

    def __init__(self):
        self.inserted = []          # (kind, rows) per insert call
        self.query = AsyncMock(return_value=[])

    async def insert(self, rows, kind):
        self.inserted.append((kind, list(rows)))
        return len(rows)


class FakeStore:
    def __init__(self):
        self.client = FakeClient()


def _settings(**history):
    return SimpleNamespace(run_history=RunHistoryConfig(**history))


async def _attached(log_source=None, **history):
    rec = RunHistoryRecorder()
    fake = FakeStore()
    await rec.attach(fake, _settings(**history), log_source=log_source)
    return rec, fake


def _indexed_docs(fake):
    """Run records appended, in order."""
    return [row for kind, rows in fake.client.inserted if kind == KIND_PIPELINE_RUN for row in rows]


def _log_batches(fake):
    """Log chunk records, one list per insert."""
    return [rows for kind, rows in fake.client.inserted if kind == KIND_PIPELINE_RUN_LOG]


def _queries(fake):
    return [c.args[0] for c in fake.client.query.await_args_list]


@pytest.fixture(autouse=True)
def _reset_context():
    """Context variables must not leak between tests."""
    tokens = [
        run_history._trigger_var.set("manual"),
        run_history._actor_var.set(None),
        run_history._commit_var.set(None),
        run_history._pipeline_var.set(None),
    ]
    yield
    for var, token in zip(
        (run_history._trigger_var, run_history._actor_var,
         run_history._commit_var, run_history._pipeline_var),
        tokens,
    ):
        var.reset(token)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    @pytest.mark.parametrize("raw,expected", [
        ("build", "build"), ("test", "test"), ("qa", "qa"), ("deploy", "deploy"),
        ("done", "deploy"), ("qa-deploy", "qa"), ("idle", None), ("", None), (None, None),
    ])
    def test_normalize_stage(self, raw, expected):
        assert normalize_stage(raw) == expected

    def test_extract_commit_takes_the_last_one_and_strips_ansi(self):
        lines = [
            "\x1b[0;34m[INFO]\x1b[0m Checking out commit: abc1234",
            "HEAD is now at 1234567 Some message",
            "\x1b[0;32m[SUCCESS]\x1b[0m Commit: DEADBEEF12",
        ]
        assert extract_commit(lines) == "deadbeef12"
        assert extract_commit(["no sha here", "Commit: xyz"]) is None

    def test_error_excerpt_prefers_error_lines(self):
        lines = ["step 1", "ERROR: npm install failed", "step 3", "cleanup"]
        assert error_excerpt(lines) == "ERROR: npm install failed"

    def test_error_excerpt_ignores_passing_tests_named_after_failures(self):
        lines = [
            "tests/test_x.py::test_plan[kwargs4-None-is the one that failed] PASSED [ 19%]",
            "tests/test_x.py::test_error_path SKIPPED (no docker)       [ 20%]",
            "tests/test_x.py::test_real_bug FAILED                      [ 21%]",
            "  Tests Failed! (exit code: 1)",
        ]
        assert error_excerpt(lines).split("\n") == lines[2:]

    def test_error_excerpt_falls_back_to_tail(self):
        lines = [f"line {i}" for i in range(100)]
        excerpt = error_excerpt(lines).split("\n")
        assert excerpt[-1] == "line 99"
        assert len(excerpt) == run_history.ERROR_EXCERPT_LINES

    def test_chunk_lines(self):
        chunks = chunk_lines([str(i) for i in range(5)], 2, first_line=10)
        assert chunks == [(10, ["0", "1"]), (12, ["2", "3"]), (14, ["4"])]

    def test_build_runs_query_filters(self):
        query = build_runs_query(repo="r", stage="done", status="failed", trigger="auto")
        # Fixed fields select records, the others the latest record of each run
        selection, latest = query.split(" | last 1 by (_time, seq) partition by (run_id)")
        assert selection == '{kind="pipeline_run", repo="r"} stage:="deploy"'
        assert latest == ' | filter status:="failed" trigger:="auto"'

    def test_build_runs_query_text_matches_logs_and_metadata(self):
        query = build_runs_query(text="OOMKilled", run_ids=["a", "b"])
        text = query.split("| filter ", 1)[1]
        assert 'error_excerpt:i("OOMKilled")' in text
        assert 'version:="OOMKilled"' in text and 'commit:="oomkilled"' in text
        assert 'run_id:in("a", "b")' in text
        assert text.count(" OR ") == 4

    def test_build_runs_query_time_window_uses_the_start(self):
        from datetime import datetime, timezone
        since = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert "started_ms:range[1767225600000, Inf]" in build_runs_query(since=since)

    def test_run_from_row_types_the_fields(self):
        run = run_history.run_from_row({"run_id": "x", "duration_seconds": "1.5",
                                        "log_line_count": "26", "log_truncated": "false"})
        assert run["duration_seconds"] == 1.5 and run["log_line_count"] == 26
        assert run["log_truncated"] is False and run["error_excerpt"] is None
        assert run["log_stored_lines"] == 0


# ---------------------------------------------------------------------------
# Recorder lifecycle
# ---------------------------------------------------------------------------

class TestRecorder:
    async def test_attach_closes_interrupted_runs_with_a_new_record(self):
        rec = RunHistoryRecorder()
        fake = FakeStore()
        fake.client.query = AsyncMock(return_value=[
            {"run_id": "r1", "repo": "app", "stage": "build", "status": "running",
             "started_at": "2026-01-01T00:00:00+00:00"}])
        await rec.attach(fake, _settings())
        assert rec.available
        assert 'filter status:="running"' in _queries(fake)[0]
        (closed,) = _indexed_docs(fake)
        assert (closed["run_id"], closed["status"]) == ("r1", "interrupted")
        assert closed["started_ms"] == 1767225600000

    async def test_query_failure_at_attach_is_not_fatal(self):
        rec = RunHistoryRecorder()
        fake = FakeStore()
        fake.client.query = AsyncMock(side_effect=RuntimeError("store down"))
        await rec.attach(fake, _settings())
        assert rec.available
        # Observing still works and never raises.
        rec.observe("repo", "build", "running", "1.0.0", action_id="a1")
        rec.observe("repo", "build", "success", "1.0.0", action_id="a1", log_lines=["ok"])
        await rec.drain()

    async def test_disabled_by_config(self):
        rec, fake = await _attached(enabled=False)
        assert not rec.available
        assert fake.client.query.await_count == 0

    async def test_successful_run_is_recorded_with_logs(self):
        rec, fake = await _attached(log_chunk_lines=10)
        run_history.set_trigger("manual", actor="alice@example.com")
        rec.observe("myrepo", "build", "running", "1.2.3", action_id="b1")
        lines = [f"line {i}" for i in range(25)] + ["[SUCCESS] Commit: abcdef1"]
        rec.observe("myrepo", "build", "success", "1.2.3", action_id="b1", log_lines=lines)
        await rec.drain()

        docs = _indexed_docs(fake)
        assert [d["status"] for d in docs] == ["running", "success"]
        assert docs[0]["seq"] < docs[1]["seq"]
        final = docs[-1]
        assert final["kind"] == KIND_PIPELINE_RUN
        assert final["run_id"] == docs[0]["run_id"]
        assert final["repo"] == "myrepo" and final["stage"] == "build"
        assert final["version"] == "1.2.3"
        assert final["trigger"] == "manual" and final["actor"] == "alice@example.com"
        assert final["action_id"] == "b1"
        assert final["commit"] == "abcdef1"
        assert final["log_line_count"] == 26 and final["log_truncated"] is False
        assert final["duration_seconds"] >= 0
        assert final.get("error_excerpt") is None

        (chunks,) = _log_batches(fake)
        assert len(chunks) == 3  # 26 lines in chunks of 10
        assert [c["line_start"] for c in chunks] == [0, 10, 20]
        assert chunks[-1]["line_end"] == 26
        assert {c["run_id"] for c in chunks} == {final["run_id"]}
        assert [c["chunk"] for c in chunks] == [0, 1, 2]
        # A run's log is stamped with the run's start
        assert {c["_time"] for c in chunks} == {final["started_at"]}

    async def test_writes_of_one_run_keep_their_order(self):
        """The 'running' write must never land after the final one."""
        rec, fake = await _attached()
        gate = asyncio.Event()
        order = []

        async def slow_insert(rows, kind):
            if kind == KIND_PIPELINE_RUN:
                if rows[0]["status"] == "running":
                    await gate.wait()
                order.append(rows[0]["status"])
            return len(rows)

        fake.client.insert = slow_insert
        rec.observe("r", "test", "running", "1.0.0", action_id="t1")
        rec.observe("r", "test", "failed", "1.0.0", action_id="t1", log_lines=["boom"])
        await asyncio.sleep(0)
        gate.set()
        await rec.drain()
        assert order == ["running", "failed"]

    async def test_failure_keeps_error_excerpt(self):
        rec, fake = await _attached()
        rec.observe("r", "deploy", "running", "2.0.0", action_id="d1")
        rec.observe("r", "deploy", "failed", "2.0.0", action_id="d1",
                    log_lines=["pulling", "Error response from daemon: no such image", "done"])
        await rec.drain()
        final = _indexed_docs(fake)[-1]
        assert final["status"] == "failed"
        assert "no such image" in final["error_excerpt"]

    async def test_done_closes_deploy_as_success(self):
        rec, fake = await _attached()
        rec.observe("r", "deploy", "running", "2.0.0", action_id="d1")
        rec.observe("r", "done", "success", "2.0.0", action_id="d1", log_lines=["ok"])
        await rec.drain()
        final = _indexed_docs(fake)[-1]
        assert (final["stage"], final["status"]) == ("deploy", "success")

    async def test_cancel_marker_marks_run_cancelled(self):
        rec, fake = await _attached()
        rec.observe("r", "build", "running", "1.0", action_id="b1")
        rec.observe("r", "build", "failed", "1.0", action_id="b1",
                    log_lines=["building", "[Cancellation requested...]"])
        await rec.drain()
        assert _indexed_docs(fake)[-1]["status"] == "cancelled"

    async def test_duplicate_failure_for_same_action_is_dropped(self):
        """The build task and cancel_action() both report the same failure."""
        rec, fake = await _attached()
        rec.observe("r", "build", "running", "1.0", action_id="b1")
        rec.observe("r", "build", "failed", "1.0", action_id="b1", log_lines=["x"])
        rec.observe("r", "build", "failed", "1.0", action_id="b1", log_lines=["x"])
        await rec.drain()
        assert [d["status"] for d in _indexed_docs(fake)] == ["running", "failed"]

    async def test_gate_rejected_after_success_is_not_a_new_run(self):
        rec, fake = await _attached()
        rec.observe("r", "test", "running", "1.0", action_id="t1")
        rec.observe("r", "test", "success", "1.0", action_id="t1", log_lines=["ok"])
        rec.observe("r", "test", "gate_rejected", "1.0", action_id="t1", log_lines=["ok"])
        await rec.drain()
        assert [d["status"] for d in _indexed_docs(fake)] == ["running", "success"]

    async def test_failure_without_a_start_is_recorded(self):
        """E.g. 'no host available' fails the pipeline before any stage runs."""
        rec, fake = await _attached()
        rec.observe("r", "build", "failed", "1.0")
        await rec.drain()
        docs = _indexed_docs(fake)
        assert len(docs) == 1 and docs[0]["status"] == "failed"
        assert docs[0]["duration_seconds"] == 0

    async def test_running_reannounced_with_action_id_enriches_the_same_run(self):
        rec, fake = await _attached()
        rec.observe("r", "test", "running", "1.0")                  # pipeline start
        rec.observe("r", "test", "running", "1.0", action_id="t1")  # stage start
        rec.observe("r", "test", "success", "1.0", action_id="t1", log_lines=["ok"])
        await rec.drain()
        docs = _indexed_docs(fake)
        assert len({d["run_id"] for d in docs}) == 1
        assert docs[-1]["action_id"] == "t1"

    async def test_new_run_interrupts_a_still_open_one(self):
        rec, fake = await _attached()
        rec.observe("r", "deploy", "running", "1.0", action_id="d1")
        rec.observe("r", "deploy", "running", "1.1", action_id="d2")
        await rec.drain()
        statuses = {(d["action_id"], d["status"]) for d in _indexed_docs(fake)}
        assert ("d1", "interrupted") in statuses
        assert ("d2", "running") in statuses

    async def test_log_source_fills_missing_logs(self):
        calls = []

        def source(repo, stage, action_id):
            calls.append((repo, stage, action_id))
            return ["from action", "ERROR boom"]

        rec, fake = await _attached(log_source=source)
        rec.observe("r", "build", "running", "1.0", action_id="b1")
        rec.observe("r", "build", "failed", "1.0")  # pipeline exception path
        await rec.drain()
        assert calls == [("r", "build", "b1")]
        final = _indexed_docs(fake)[-1]
        assert final["log_line_count"] == 2
        assert final["error_excerpt"] == "ERROR boom"

    async def test_long_logs_keep_the_tail(self):
        rec, fake = await _attached(max_log_lines=100, log_chunk_lines=50)
        rec.observe("r", "build", "running", "1.0", action_id="b1")
        rec.observe("r", "build", "success", "1.0", action_id="b1",
                    log_lines=[f"l{i}" for i in range(250)])
        await rec.drain()
        final = _indexed_docs(fake)[-1]
        assert final["log_line_count"] == 250
        assert final["log_stored_lines"] == 100
        assert final["log_truncated"] is True
        (chunks,) = _log_batches(fake)
        assert [c["line_start"] for c in chunks] == [150, 200]
        assert chunks[0]["_msg"].split("\n")[0] == "l150"

    async def test_observe_pipeline_picks_the_stage_action_id(self):
        from backend.pipeline_state import _UNSET
        rec, fake = await _attached()
        rec.observe_pipeline("r", "qa", "running", "1.0",
                             action_ids={"build": "b1", "test": _UNSET, "qa": "q1", "deploy": None})
        await rec.drain()
        assert _indexed_docs(fake)[0]["action_id"] == "q1"
        assert _indexed_docs(fake)[0]["stage"] == "qa"

    async def test_trigger_context_reaches_background_tasks(self):
        rec, fake = await _attached()
        run_history.set_trigger("auto", actor="auto-build")
        pipeline_id = run_history.begin_pipeline()
        run_history.set_commit("ABC1234")

        async def pipeline_task():
            rec.observe("r", "build", "running", "1.0", action_id="b1")

        await asyncio.create_task(pipeline_task())
        await rec.drain()
        doc = _indexed_docs(fake)[0]
        assert doc["trigger"] == "auto"
        assert doc["actor"] == "auto-build"
        assert doc["pipeline_id"] == pipeline_id
        assert doc["commit"] == "abc1234"

    def test_unknown_trigger_falls_back_to_manual(self):
        run_history.set_trigger("bogus")
        assert run_history._trigger_var.get() == "manual"

    async def test_nothing_is_ever_deleted(self):
        """Retention is VictoriaLogs' own: the recorder only appends."""
        assert not hasattr(RunHistoryRecorder, "cleanup")


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def _answer(count=0, runs=(), logs=(), matching=()):
    """Fake query results keyed on the shape of the LogsQL asked."""
    async def query(q, **kwargs):
        if q.startswith('{kind="pipeline_run_log"'):
            if "stats by (run_id)" in q:
                return [{"run_id": r} for r in matching]
            return list(logs)
        if q.endswith("| stats count() as n"):
            return [{"n": str(count)}]
        return list(runs)
    return AsyncMock(side_effect=query)


class TestQueries:
    async def test_list_runs_paginates_and_sorts(self):
        rec, fake = await _attached()
        fake.client.query = _answer(count=42, runs=[{"run_id": "x", "status": "success"}])
        result = await rec.list_runs(repo="r", page=3, page_size=10)
        page_query = _queries(fake)[1]
        assert page_query.endswith("| sort by (started_ms desc) offset 20 limit 10")
        assert result["total"] == 42 and (result["page"], result["page_size"]) == (3, 10)
        assert result["runs"][0]["run_id"] == "x" and result["runs"][0]["status"] == "success"

    async def test_list_runs_page_size_is_capped(self):
        rec, fake = await _attached()
        result = await rec.list_runs(page_size=10_000)
        assert result["page_size"] == run_history.MAX_PAGE_SIZE

    async def test_list_runs_text_query_searches_logs_first(self):
        rec, fake = await _attached()
        fake.client.query = _answer(matching=["run-1"])
        await rec.list_runs(q="OOMKilled")
        log_query, *run_queries = _queries(fake)
        assert log_query.startswith('{kind="pipeline_run_log"}')
        assert '_msg:i("OOMKilled")' in log_query
        assert all('run_id:in("run-1")' in q for q in run_queries)

    async def test_stale_running_run_is_flagged(self):
        rec, fake = await _attached()
        fake.client.query = _answer(count=1, runs=[{"run_id": "gone", "status": "running"}])
        result = await rec.list_runs()
        assert result["runs"][0]["status_note"] == "not running in this process"

    async def test_get_run_returns_log_window(self):
        rec, fake = await _attached()
        run = {"run_id": "abc", "repo": "app", "log_line_count": "250", "log_stored_lines": "250",
               "log_truncated": "false"}
        chunk = {"line_start": "200", "_msg": "\n".join(f"l{i}" for i in range(200, 250))}
        fake.client.query = AsyncMock(side_effect=[[run], [chunk]])
        result = await rec.get_run("abc", log_offset=-10, log_limit=5)
        assert result["logs"]["offset"] == 240
        assert result["logs"]["lines"] == ["l240", "l241", "l242", "l243", "l244"]
        run_query, chunk_query = _queries(fake)
        assert 'run_id:="abc"' in run_query and "last 1 by (_time, seq)" in run_query
        assert "line_start:<245 line_end:>240" in chunk_query
        # Every chunk of the window, whatever the chunk size
        assert "limit" not in chunk_query

    async def test_get_run_spanning_chunks(self):
        rec, fake = await _attached()
        run = {"run_id": "abc", "log_line_count": "4", "log_stored_lines": "4"}
        chunks = [{"line_start": "0", "_msg": "a\nb"}, {"line_start": "2", "_msg": "c\nd"}]
        fake.client.query = AsyncMock(side_effect=[[run], chunks])
        result = await rec.get_run("abc", log_offset=1, log_limit=2)
        assert result["logs"]["lines"] == ["b", "c"]

    async def test_get_run_not_found(self):
        rec, fake = await _attached()
        assert await rec.get_run("missing") is None
        queried = fake.client.query.await_count
        assert await rec.get_run("../etc") is None  # rejected before querying
        assert fake.client.query.await_count == queried

    async def test_search_logs_returns_matching_lines(self):
        rec, fake = await _attached()
        fake.client.query = _answer(logs=[{"run_id": "r1", "repo": "app", "stage": "build",
                                           "line_start": "0", "_msg": "ok\nOOMKilled worker\nok",
                                           "_time": "2026-01-01T00:00:00Z"}])
        hits = await rec.search_logs("oomkilled", repo="app")
        assert hits[0]["run_id"] == "r1" and hits[0]["timestamp"] == "2026-01-01T00:00:00Z"
        assert hits[0]["lines"] == ["OOMKilled worker"]

    async def test_log_search_is_time_bounded_and_newest_first(self):
        from datetime import datetime, timezone
        rec, fake = await _attached()
        fake.client.query = _answer()
        since = datetime(2026, 1, 1, tzinfo=timezone.utc)
        await rec.list_runs(q="OOMKilled", since=since)
        log_call = fake.client.query.await_args_list[0]
        assert log_call.kwargs["start"] == since
        assert "sort by (latest desc)" in log_call.args[0]

    async def test_queries_require_attachment(self):
        rec = RunHistoryRecorder()
        with pytest.raises(RuntimeError):
            await rec.list_runs()


class TestAgainstVictoriaLogs:
    """Append-only history, written and read back by a real VictoriaLogs."""

    async def test_lifecycle(self, vlogs):
        from backend.config import VictoriaLogsConfig
        from backend.log_store import LogStore

        store = LogStore(VictoriaLogsConfig(url=vlogs))
        repo = f"repo-{uuid.uuid4().hex[:8]}"
        rec = RunHistoryRecorder()
        await rec.attach(store, _settings(log_chunk_lines=10))
        run_history.set_trigger("manual", actor="tester")
        rec.observe(repo, "build", "running", "1.2.3", action_id="a1")
        await rec.drain()
        rec.observe(repo, "build", "failed", "1.2.3", action_id="a1",
                    log_lines=[f"line {i}" for i in range(25)] + ["ERROR: kaboom", "Commit: abcdef1234"])
        rec.observe(repo, "test", "running", "1.2.3", action_id="t1")
        await rec.drain()
        await store.client.flush()

        runs = (await rec.list_runs(repo=repo))["runs"]
        assert [(r["stage"], r["status"]) for r in runs] == [("test", "running"), ("build", "failed")]
        build = runs[1]
        assert build["log_line_count"] == 27 and build["commit"] == "abcdef1234"
        assert (await rec.list_runs(repo=repo, status="failed"))["total"] == 1
        assert (await rec.list_runs(repo=repo, status="running"))["total"] == 1
        assert (await rec.list_runs(repo=repo, q="kaboom"))["total"] == 1      # error excerpt
        assert (await rec.list_runs(repo=repo, q="line"))["total"] == 1        # log chunks
        got = await rec.get_run(build["run_id"], log_offset=-2)
        assert got["logs"]["lines"] == ["ERROR: kaboom", "Commit: abcdef1234"]
        got = await rec.get_run(build["run_id"], log_offset=8, log_limit=4)
        assert got["logs"]["lines"] == ["line 8", "line 9", "line 10", "line 11"]
        assert (await rec.search_logs("kaboom", repo=repo))[0]["lines"] == ["ERROR: kaboom"]
        assert repo in await rec.list_repos()

        # A restart closes the run left open, by appending its final state
        restarted = RunHistoryRecorder()
        await restarted.attach(store, _settings())
        await store.client.flush()
        runs = (await restarted.list_runs(repo=repo))["runs"]
        assert [(r["stage"], r["status"]) for r in runs] == [("test", "interrupted"), ("build", "failed")]
        await store.close()


# ---------------------------------------------------------------------------
# Wiring into api.py
# ---------------------------------------------------------------------------

class TestApiWiring:
    def test_set_pipeline_feeds_the_recorder(self, monkeypatch, tmp_path):
        import backend.api as api_module
        from backend.pipeline_state import PipelineStateManager

        observed = []
        monkeypatch.setattr(api_module, "pipeline_state", PipelineStateManager(data_dir=str(tmp_path)))
        monkeypatch.setattr(run_history.recorder, "observe",
                            lambda *a, **kw: observed.append((a, kw)))
        api_module._set_pipeline("repo", "deploy", "running", "1.0.0", deploy_id="d1")
        api_module._set_pipeline("repo", "done", "success", "1.0.0", deploy_id="d1", log_lines=["ok"])
        assert observed[0] == (("repo", "deploy", "running", "1.0.0"),
                               {"action_id": "d1", "log_lines": None})
        assert observed[1] == (("repo", "done", "success", "1.0.0"),
                               {"action_id": "d1", "log_lines": ["ok"]})

    def test_log_source_resolves_through_pipeline_state(self, monkeypatch, tmp_path):
        import backend.api as api_module
        from backend.pipeline_state import PipelineStateManager

        mgr = PipelineStateManager(data_dir=str(tmp_path))
        mgr.set_pipeline("repo", "build", "running", "1.0", build_id="zz1")
        monkeypatch.setattr(api_module, "pipeline_state", mgr)
        action = api_module.BackgroundAction("zz1", "build", "repo")
        action.append_output("hello")
        monkeypatch.setitem(api_module._background_actions, "zz1", action)
        assert api_module._run_history_log_source("repo", "build", None) == ["hello"]
        assert api_module._run_history_log_source("repo", "build", "nope") is None


class TestEndpoints:
    @pytest.fixture
    def stub_recorder(self, monkeypatch):
        rec = RunHistoryRecorder()
        rec._client = object()  # mark available; queries are stubbed below
        rec.list_runs = AsyncMock(return_value={"runs": [], "total": 0, "page": 1, "page_size": 20})
        rec.list_repos = AsyncMock(return_value=["a", "b"])
        rec.get_run = AsyncMock(return_value={"run": {"run_id": "abc"}, "logs": {"lines": []}})
        monkeypatch.setattr(run_history, "recorder", rec)
        return rec

    def test_requires_auth(self, client):
        assert client.get("/api/pipeline/runs").status_code == 401

    def test_unavailable_is_503(self, client, auth_headers, monkeypatch):
        monkeypatch.setattr(run_history, "recorder", RunHistoryRecorder())
        resp = client.get("/api/pipeline/runs", headers=auth_headers)
        assert resp.status_code == 503

    def test_list_runs_passes_filters(self, client, auth_headers, stub_recorder):
        resp = client.get(
            "/api/pipeline/runs?repo=app&stage=deploy&status=failed&trigger=auto"
            "&q=oom&page=2&page_size=5&since=2026-01-01T00:00:00Z",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        kwargs = stub_recorder.list_runs.await_args.kwargs
        assert kwargs["repo"] == "app" and kwargs["stage"] == "deploy"
        assert kwargs["status"] == "failed" and kwargs["trigger"] == "auto"
        assert kwargs["q"] == "oom" and kwargs["page"] == 2 and kwargs["page_size"] == 5
        assert kwargs["since"].year == 2026

    def test_hours_shortcut(self, client, auth_headers, stub_recorder):
        resp = client.get("/api/pipeline/runs?hours=24", headers=auth_headers)
        assert resp.status_code == 200
        assert stub_recorder.list_runs.await_args.kwargs["since"] is not None

    @pytest.mark.parametrize("query", [
        "stage=prod", "status=weird", "trigger=cron", "page_size=1000", "since=yesterday",
    ])
    def test_rejects_invalid_filters(self, client, auth_headers, stub_recorder, query):
        resp = client.get(f"/api/pipeline/runs?{query}", headers=auth_headers)
        assert resp.status_code in (400, 422)

    def test_repos(self, client, auth_headers, stub_recorder):
        resp = client.get("/api/pipeline/runs/repos", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == {"repos": ["a", "b"]}

    def test_get_run(self, client, auth_headers, stub_recorder):
        resp = client.get("/api/pipeline/runs/abc?log_offset=-100&log_limit=100",
                          headers=auth_headers)
        assert resp.status_code == 200
        stub_recorder.get_run.assert_awaited_with("abc", log_offset=-100, log_limit=100)

    def test_get_run_404(self, client, auth_headers, stub_recorder):
        stub_recorder.get_run.return_value = None
        resp = client.get("/api/pipeline/runs/zzz", headers=auth_headers)
        assert resp.status_code == 404

    def test_query_failure_is_502_without_details(self, client, auth_headers, stub_recorder):
        stub_recorder.list_runs.side_effect = RuntimeError("secret cluster detail")
        resp = client.get("/api/pipeline/runs", headers=auth_headers)
        assert resp.status_code == 502
        assert "secret" not in resp.text


# ---------------------------------------------------------------------------
# MCP read tools
# ---------------------------------------------------------------------------

class TestMcpTools:
    @pytest.fixture
    def stub_recorder(self, monkeypatch):
        rec = RunHistoryRecorder()
        rec._client = object()
        rec.list_runs = AsyncMock(return_value={"runs": [{"run_id": "r1"}], "total": 1,
                                                "page": 1, "page_size": 20})
        rec.get_run = AsyncMock(return_value={"run": {"run_id": "r1"}, "logs": {"lines": ["x"]}})
        monkeypatch.setattr(run_history, "recorder", rec)
        return rec

    async def test_get_pipeline_history(self, stub_recorder):
        from backend.mcp_server import get_pipeline_history
        out = json.loads(await get_pipeline_history(repo_name="app", stage="deploy",
                                                    last_hours=6, query="oom"))
        assert out["runs"] == [{"run_id": "r1"}] and out["last_hours"] == 6
        kwargs = stub_recorder.list_runs.await_args.kwargs
        assert kwargs["repo"] == "app" and kwargs["stage"] == "deploy" and kwargs["q"] == "oom"
        assert kwargs["since"] is not None

    async def test_get_pipeline_history_validates(self, stub_recorder):
        from backend.mcp_server import get_pipeline_history
        assert "error" in json.loads(await get_pipeline_history(stage="prod"))
        assert "error" in json.loads(await get_pipeline_history(status="weird"))

    async def test_get_pipeline_run(self, stub_recorder):
        from backend.mcp_server import get_pipeline_run
        out = json.loads(await get_pipeline_run("r1"))
        assert out["run"]["run_id"] == "r1"
        stub_recorder.get_run.assert_awaited_with("r1", log_offset=-200, log_limit=200)

    async def test_unavailable(self, monkeypatch):
        from backend.mcp_server import get_pipeline_history
        monkeypatch.setattr(run_history, "recorder", RunHistoryRecorder())
        assert "error" in json.loads(await get_pipeline_history())

    def test_tool_names_are_not_treated_as_dangerous(self):
        """The default LLM-agent policy must let it read the history."""
        from backend.config_file import is_dangerous_tool
        assert not is_dangerous_tool("get_pipeline_history")
        assert not is_dangerous_tool("get_pipeline_run")


# ---------------------------------------------------------------------------
# Review fixes
# ---------------------------------------------------------------------------

class TestRedaction:
    @pytest.mark.parametrize("raw,expected", [
        ("git clone https://bot:s3cr3t@github.com/o/r.git",
         "git clone https://bot:[REDACTED]@github.com/o/r.git"),
        ("token ghp_abcdefghijklmnopqrstuvwxyz0123 used", "token [REDACTED] used"),
        ("DB_PASSWORD=hunter2 rest", "DB_PASSWORD=[REDACTED] rest"),
        ('api_key: "a b c"', "api_key: [REDACTED]"),
        ("Authorization: Bearer abc.def", "Authorization: Bearer [REDACTED]"),
        ("Step 3/10 : RUN npm ci", "Step 3/10 : RUN npm ci"),
        ("[SUCCESS] Commit: abcdef1", "[SUCCESS] Commit: abcdef1"),
    ])
    def test_redact_secrets(self, raw, expected):
        assert run_history.redact_secrets(raw) == expected

    async def test_stored_logs_and_excerpt_are_redacted(self):
        rec, fake = await _attached()
        rec.observe("r", "deploy", "running", "1.0", action_id="d9")
        rec.observe("r", "deploy", "failed", "1.0", action_id="d9",
                    log_lines=["export REGISTRY_PASSWORD=topsecret", "ERROR: login failed"])
        await rec.drain()
        final = _indexed_docs(fake)[-1]
        assert "topsecret" not in (final.get("error_excerpt") or "")
        (chunks,) = _log_batches(fake)
        assert "topsecret" not in chunks[0]["_msg"]

    async def test_cancel_marker_anywhere_in_log(self):
        rec, fake = await _attached()
        rec.observe("r", "build", "running", "1.0", action_id="b9")
        lines = ["[Cancellation requested...]"] + [f"cleanup {i}" for i in range(100)]
        rec.observe("r", "build", "failed", "1.0", action_id="b9", log_lines=lines)
        await rec.drain()
        assert _indexed_docs(fake)[-1]["status"] == "cancelled"


class TestCancelWiring:
    def test_cancel_qa_deploy_reports_the_qa_stage(self, client, auth_headers, monkeypatch, tmp_path):
        import backend.api as api_module
        from backend.pipeline_state import PipelineStateManager

        monkeypatch.setattr(api_module, "pipeline_state", PipelineStateManager(data_dir=str(tmp_path)))
        seen = []
        monkeypatch.setattr(
            run_history.recorder, "observe",
            lambda *a, **kw: seen.append((a, kw, run_history._actor_var.get())))
        action = api_module.BackgroundAction("qa1", "qa-deploy", "repo")
        monkeypatch.setitem(api_module._background_actions, "qa1", action)

        resp = client.post("/api/stacks/actions/qa1/cancel", headers=auth_headers)
        assert resp.status_code == 200, resp.text
        (args, kwargs, actor), = seen
        assert args[1] == "qa" and args[2] == "failed"
        assert kwargs["action_id"] == "qa1"
        # The authenticated user reaches the recorder through the auth middleware.
        assert actor == "testuser"
