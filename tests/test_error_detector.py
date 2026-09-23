"""Unit tests for backend/error_detector.py — no infrastructure needed."""

from datetime import datetime, timedelta
import json
from unittest import mock

import pytest

from backend.error_detector import (
    ErrorPattern,
    RecurringErrorDetector,
    normalize_message,
    text_fingerprint,
)


def _make_detector(**kwargs):
    """Create a RecurringErrorDetector with dummy config (no real services)."""
    defaults = dict(
        opensearch_client=None,
        llm_agent=None,
        github_service=None,
    )
    defaults.update(kwargs)
    return RecurringErrorDetector(**defaults)


def _err(project: str, message: str, ts: datetime) -> dict:
    return {
        "compose_project": project,
        "container_name": f"{project}_web",
        "message": message,
        "timestamp": ts.isoformat(),
        "level": "ERROR",
    }


# ── _deduplicate_bursts ──────────────────────────────────────────────────────

class TestDeduplicateBursts:
    def test_burst_collapsed_to_one(self):
        """5 errors from same project within 3 seconds → 1 kept."""
        d = _make_detector(burst_window_seconds=10)
        now = datetime(2024, 1, 1, 12, 0, 0)
        errors = [_err("myapp", f"error #{i}", now + timedelta(seconds=i)) for i in range(5)]
        errors_desc = list(reversed(errors))  # OpenSearch returns desc
        result = d._deduplicate_bursts(errors_desc)
        assert len(result) == 1

    def test_spread_errors_all_kept(self):
        """5 errors 15s apart → all kept (window=10s)."""
        d = _make_detector(burst_window_seconds=10)
        now = datetime(2024, 1, 1, 12, 0, 0)
        errors = [_err("myapp", f"error #{i}", now + timedelta(seconds=i * 15)) for i in range(5)]
        errors_desc = list(reversed(errors))
        result = d._deduplicate_bursts(errors_desc)
        assert len(result) == 5

    def test_different_projects_independent(self):
        """Burst from project A does not suppress project B errors."""
        d = _make_detector(burst_window_seconds=10)
        now = datetime(2024, 1, 1, 12, 0, 0)
        errors = [
            _err("projectA", "error", now),
            _err("projectA", "error", now + timedelta(seconds=1)),
            _err("projectB", "error", now + timedelta(seconds=2)),
            _err("projectB", "error", now + timedelta(seconds=3)),
        ]
        errors_desc = list(reversed(errors))
        result = d._deduplicate_bursts(errors_desc)
        # One from each project
        assert len(result) == 2
        projects = {r.get("compose_project") for r in result}
        assert projects == {"projectA", "projectB"}

    def test_empty_list(self):
        d = _make_detector()
        assert d._deduplicate_bursts([]) == []

    def test_order_preserved_descending(self):
        """Output should remain in descending order (latest first)."""
        d = _make_detector(burst_window_seconds=5)
        now = datetime(2024, 1, 1, 12, 0, 0)
        errors = [
            _err("app", "err A", now),
            _err("app", "err B", now + timedelta(seconds=10)),
            _err("app", "err C", now + timedelta(seconds=20)),
        ]
        errors_desc = list(reversed(errors))
        result = d._deduplicate_bursts(errors_desc)
        assert len(result) == 3
        # First element should be the latest
        ts0 = result[0]["timestamp"]
        ts1 = result[1]["timestamp"]
        ts2 = result[2]["timestamp"]
        assert ts0 > ts1 > ts2

    def test_window_boundary_exact(self):
        """Error exactly at window boundary (== window) should be kept."""
        d = _make_detector(burst_window_seconds=10)
        now = datetime(2024, 1, 1, 12, 0, 0)
        errors = [
            _err("app", "err A", now),
            _err("app", "err B", now + timedelta(seconds=10)),  # exactly at boundary
        ]
        errors_desc = list(reversed(errors))
        result = d._deduplicate_bursts(errors_desc)
        assert len(result) == 2

    def test_invalid_timestamp_falls_back(self):
        """Errors with unparseable timestamps should not crash."""
        d = _make_detector(burst_window_seconds=10)
        errors = [
            {"compose_project": "app", "container_name": "app_web", "message": "err",
             "timestamp": "not-a-date", "level": "ERROR"},
        ]
        result = d._deduplicate_bursts(errors)
        assert len(result) == 1


# ── ErrorPattern ─────────────────────────────────────────────────────────────

class TestErrorPattern:
    def test_initial_state(self):
        p = ErrorPattern("fp1", "some error message", "myapp")
        assert p.count == 1
        assert "myapp" in p.services
        assert p.notified is False

    def test_add_occurrence(self):
        p = ErrorPattern("fp1", "error message long", "svc1")
        p.add_occurrence("svc2", "shorter")
        assert p.count == 2
        assert "svc2" in p.services
        # Should keep shortest sample
        assert p.sample_message == "shorter"

    def test_sample_truncated_to_500(self):
        p = ErrorPattern("fp1", "x" * 600, "svc")
        assert len(p.sample_message) == 500


# ── normalize_message (error_detector version) ───────────────────────────────

class TestNormalizeMessage:
    def test_retry_counts_equal(self):
        assert normalize_message("retry 4/5") == normalize_message("retry 5/5")

    def test_connection_ids_equal(self):
        assert normalize_message("conn id=abc12345") == normalize_message("conn id=def67890")

    def test_different_errors_not_equal(self):
        assert normalize_message("timeout") != normalize_message("disk full")


# ── _fixup_compose_project ───────────────────────────────────────────────────

class TestFixupComposeProject:
    def test_swarm_container_name_overrides_devops(self):
        """Swarm container name 'pulsarcd_agent.1.abc' → compose_project='pulsarcd'."""
        entry = {"compose_project": "devops", "container_name": "pulsarcd_agent.1.4sz1iuqpv26b"}
        RecurringErrorDetector._fixup_compose_project(entry)
        assert entry["compose_project"] == "pulsarcd"

    def test_swarm_container_hyphenated_stack(self):
        """Stack with hyphens: 'art-retrainer_web.2.xyz' → 'art-retrainer'."""
        entry = {"compose_project": "devops", "container_name": "art-retrainer_web.2.abc123def"}
        RecurringErrorDetector._fixup_compose_project(entry)
        assert entry["compose_project"] == "art-retrainer"

    def test_non_swarm_container_not_modified(self):
        """Non-Swarm container name without .slot.taskid → no change."""
        entry = {"compose_project": "myapp", "container_name": "myapp_web_1"}
        RecurringErrorDetector._fixup_compose_project(entry)
        assert entry["compose_project"] == "myapp"

    def test_no_container_name(self):
        """Missing container_name → no change."""
        entry = {"compose_project": "devops"}
        RecurringErrorDetector._fixup_compose_project(entry)
        assert entry["compose_project"] == "devops"

    def test_correct_project_stays(self):
        """Already correct compose_project is overridden by container name (always wins)."""
        entry = {"compose_project": "pulsarcd", "container_name": "pulsarcd_agent.1.abc123"}
        RecurringErrorDetector._fixup_compose_project(entry)
        assert entry["compose_project"] == "pulsarcd"


# ── _get_zvec fallback ──────────────────────────────────────────────────────

class TestGetZvecFallback:
    def test_runtime_error_caught(self):
        """Non-ImportError exceptions (e.g. OSError from native binary) must be caught."""
        import backend.error_detector as ed
        # Reset global state
        ed._zvec = None
        ed._zvec_available = None
        with mock.patch.dict("sys.modules", {"zvec": None}):
            # Importing a module set to None in sys.modules raises ImportError,
            # but we want to simulate a RuntimeError from native code loading.
            pass

        # Simulate a RuntimeError during import (e.g. "Prebuilt binary not found")
        ed._zvec = None
        ed._zvec_available = None
        original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

        def mock_import(name, *args, **kwargs):
            if name == "zvec":
                raise RuntimeError("Prebuilt binary not found for linux-x64")
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=mock_import):
            result = ed._get_zvec()

        assert result is None
        assert ed._zvec_available is False

        # Reset for other tests
        ed._zvec = None
        ed._zvec_available = None

    def test_fallback_uses_text_hashing(self):
        """When zvec is unavailable, scan should use text_fingerprint fallback."""
        d = _make_detector()
        # Ensure zvec path returns None
        import backend.error_detector as ed
        ed._zvec = None
        ed._zvec_available = False
        # The scan code checks `zvec and self._zvec_collection`
        # With _zvec_available=False, _get_zvec() returns None → fallback path
        assert ed._get_zvec() is None

        # Reset
        ed._zvec = None
        ed._zvec_available = None

    def test_self_log_patterns_include_zvec(self):
        """ZVEC messages must be in the self-log filter to prevent self-detection loops."""
        d = _make_detector()
        assert d._is_self_log("zvec not available, using text hashing fallback")
        assert d._is_self_log("zvec loaded successfully")
        assert d._is_self_log("zvec collection initialized")
        assert d._is_self_log("Failed to initialize zvec")
        # Real app errors should NOT match
        assert not d._is_self_log("Connection refused by database")


# ── _is_benign ───────────────────────────────────────────────────────────────

# Real lines from a distribution 3.1.1 registry, trimmed to the fields the
# patterns rely on.
_REFERRERS_FALLBACK = (
    'time="2026-09-20T22:10:03.389310733Z" level=error msg="response completed with error" '
    'environment=development err.code="manifest unknown" '
    'err.detail="unknown tag=sha256-9d6819323dbe2303d1cb0330bfb9045187c92e2d991a8610da9a0770c09e9d22" '
    'err.message="manifest unknown" http.request.method=GET '
    'http.request.uri=/v2/office-service/manifests/sha256-9d6819323dbe'
)

_BLOB_PUSH_PROBE = (
    'time="2026-09-20T22:06:43.658080066Z" level=error msg="response completed with error" '
    'environment=development err.code="blob unknown" '
    'err.detail="sha256:0c2e849b45b9915263f2a6df1792b2c1a269c894221c534fd803f09d172b316c" '
    'err.message="blob unknown to registry" http.request.method=HEAD '
    'http.request.uri="/v2/pulsarcd-test/blobs/sha256:0c2e849b45b99152"'
)

_MISSING_REAL_TAG = (
    'time="2026-09-20T22:10:03.389310733Z" level=error msg="response completed with error" '
    'err.code="manifest unknown" err.detail="unknown tag=1.0.1193" '
    'err.message="manifest unknown" http.request.method=GET '
    'http.request.uri=/v2/office-service/manifests/1.0.1193'
)

_BLOB_GET_MISSING = (
    'time="2026-09-20T22:06:43.658080066Z" level=error msg="response completed with error" '
    'err.code="blob unknown" '
    'err.detail="sha256:0c2e849b45b9915263f2a6df1792b2c1a269c894221c534fd803f09d172b316c" '
    'err.message="blob unknown to registry" http.request.method=GET '
    'http.request.uri="/v2/pulsarcd-test/blobs/sha256:0c2e849b45b99152"'
)


class TestIsBenign:
    def test_referrers_fallback_tag_is_benign(self):
        """Docker asks for sha256-<digest> only to look for attestations."""
        assert _make_detector()._is_benign(_REFERRERS_FALLBACK)

    def test_head_blob_probe_is_benign(self):
        """A push HEADs every layer to learn which ones it must upload."""
        assert _make_detector()._is_benign(_BLOB_PUSH_PROBE)

    def test_missing_real_tag_is_kept(self):
        """A deploy pulling a tag that does not exist is a real failure."""
        assert not _make_detector()._is_benign(_MISSING_REAL_TAG)

    def test_get_on_missing_blob_is_kept(self):
        """A GET for an absent layer means a broken image, not a probe."""
        assert not _make_detector()._is_benign(_BLOB_GET_MISSING)

    def test_unrelated_error_is_kept(self):
        assert not _make_detector()._is_benign("Connection refused by database")


def _registry_log(ts, method="HEAD", status=404, **overrides):
    """Distribution log fixture with independent request IDs and escaped UA."""
    fields = {
        "service": "registry", "instance.id": "registry-instance",
        "http.request.id": f"request-{method}-{ts.isoformat()}",
        "http.request.host": "registry.methodinfo.fr",
        "http.request.remoteaddr": "192.168.1.254",
        "http.request.useragent": 'docker/29.2.0 UpstreamClient(Docker-Client \\(linux\\)) "quoted"',
        "http.request.uri": "/v2/intra-muros-chat/manifests/6e25625",
        "http.request.method": method, "http.response.status": str(status),
    }
    if status == 404:
        fields["err.code"] = "manifest unknown"
    fields.update(overrides)
    return {
        "host": "server-d", "container_id": "registry-container",
        "container_name": "privatenetwork_registry.1.abc123",
        "compose_project": "privatenetwork", "compose_service": "registry",
        "timestamp": ts.isoformat(), "level": "ERROR" if status == 404 else "INFO",
        "http_status": status,
        "message": " ".join(f"{key}={json.dumps(value)}" for key, value in fields.items()),
    }


def _registry_detector(*put_entries):
    opensearch = mock.Mock()
    opensearch.logs_index = "pulsarcd-logs"
    opensearch._client.search = mock.AsyncMock(return_value={
        "hits": {"hits": [{"_source": entry} for entry in put_entries]},
    })
    opensearch._client.scroll = mock.AsyncMock()
    opensearch._client.clear_scroll = mock.AsyncMock()
    return _make_detector(opensearch_client=opensearch)


class TestRegistryPushCorrelation:
    async def test_confirmed_push_suppresses_head_only_and_preserves_raw_log(self):
        ts = datetime(2026, 9, 22, 19, 40, 49, 314255)
        head = _registry_log(ts)
        get = _registry_log(ts, method="GET")
        put = _registry_log(ts + timedelta(milliseconds=15), method="PUT", status=201)
        original = head.copy()
        detector = _registry_detector(put)
        assert await detector._filter_registry_push_probes([head, get], ts + timedelta(minutes=2)) == [get]
        assert head == original

    @pytest.mark.parametrize("field", [
        "service", "instance.id", "http.request.host", "http.request.remoteaddr",
        "http.request.useragent", "http.request.uri",
    ])
    async def test_other_request_context_does_not_hide_missing_tag(self, field):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        put = _registry_log(ts + timedelta(milliseconds=15), method="PUT", status=201,
                            **{field: "different"})
        detector = _registry_detector(put)
        assert await detector._filter_registry_push_probes([head], ts + timedelta(minutes=2)) == [head]

    @pytest.mark.parametrize("field", ["host", "container_id", "container_name"])
    async def test_other_container_does_not_hide_missing_tag(self, field):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        put = _registry_log(ts + timedelta(milliseconds=15), method="PUT", status=201)
        put[field] = "different"
        detector = _registry_detector(put)
        assert await detector._filter_registry_push_probes([head], ts + timedelta(minutes=2)) == [head]

    @pytest.mark.parametrize("offset,status", [(-0.01, 201), (5.01, 201), (0.015, 500), (0.015, 200)])
    async def test_only_later_successful_creation_in_short_window_matches(self, offset, status):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        put = _registry_log(ts + timedelta(seconds=offset), method="PUT", status=status)
        detector = _registry_detector(put)
        assert await detector._filter_registry_push_probes([head], ts + timedelta(minutes=2)) == [head]

    async def test_split_collection_resolves_on_empty_next_scan(self):
        ts = datetime.utcnow() - timedelta(seconds=1)
        head = _registry_log(ts)
        detector = _registry_detector()
        detector._fetch_recent_errors = mock.AsyncMock(side_effect=[[head], []])
        detector._registry_successful_puts = mock.AsyncMock(side_effect=[[], [
            detector._registry_event(_registry_log(ts + timedelta(milliseconds=15), method="PUT", status=201))
        ]])
        await detector._scan()
        assert detector._pending_registry_probes
        await detector._scan()
        assert not detector._pending_registry_probes
        assert detector._patterns == {}
        assert detector._total_errors_found == 0

    async def test_unmatched_head_is_released_once_after_grace(self):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        detector = _registry_detector()
        assert await detector._filter_registry_push_probes([head], ts) == []
        assert await detector._filter_registry_push_probes([head], ts + timedelta(seconds=30)) == []
        assert len(detector._pending_registry_probes) == 1
        assert await detector._filter_registry_push_probes([], ts + timedelta(seconds=60)) == [head]
        assert await detector._filter_registry_push_probes([head], ts + timedelta(seconds=61)) == []
        assert not detector._pending_registry_probes

    @pytest.mark.parametrize("field", ["timestamp", "container_id"])
    async def test_incomplete_context_is_kept_without_query(self, field):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        head.pop(field)
        detector = _registry_detector()
        assert await detector._filter_registry_push_probes([head], ts) == [head]
        detector._opensearch._client.search.assert_not_awaited()

    async def test_unrelated_errors_are_never_delayed(self):
        now = datetime.utcnow()
        error = _err("app", "Database unavailable", now)
        detector = _registry_detector()
        assert await detector._filter_registry_push_probes([error], now) == [error]
        detector._opensearch._client.search.assert_not_awaited()

    async def test_query_failure_releases_recent_and_pending_errors(self):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        detector = _registry_detector()
        assert await detector._filter_registry_push_probes([head], ts) == []
        detector._opensearch._client.search.side_effect = RuntimeError("search unavailable")
        assert await detector._filter_registry_push_probes([], ts + timedelta(seconds=1)) == [head]
        assert not detector._pending_registry_probes

    async def test_success_on_second_query_page_and_scroll_cleanup(self):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        unrelated = _registry_log(ts + timedelta(milliseconds=1), method="PUT", status=201,
                                  **{"http.request.uri": "/v2/other/manifests/6e25625"})
        put = _registry_log(ts + timedelta(milliseconds=15), method="PUT", status=201)
        detector = _registry_detector(unrelated)
        detector._REGISTRY_QUERY_PAGE_SIZE = 1
        detector._opensearch._client.search.return_value["_scroll_id"] = "scroll-one"
        detector._opensearch._client.scroll.side_effect = [
            {"_scroll_id": "scroll-two", "hits": {"hits": [{"_source": put}]}},
            {"_scroll_id": "scroll-two", "hits": {"hits": []}},
        ]
        assert await detector._filter_registry_push_probes([head], ts) == []
        assert not detector._pending_registry_probes
        assert detector._opensearch._client.scroll.await_count == 2
        detector._opensearch._client.clear_scroll.assert_awaited_once_with(scroll_id="scroll-two")

    @pytest.mark.parametrize("response", [
        {"timed_out": True, "hits": {"hits": []}},
        {"_shards": {"failed": 1}, "hits": {"hits": []}},
    ])
    async def test_partial_search_does_not_hide_errors(self, response):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        detector = _registry_detector()
        detector._opensearch._client.search.return_value = response
        assert await detector._filter_registry_push_probes([head], ts) == [head]

    async def test_query_page_limit_fails_open(self):
        ts = datetime(2026, 9, 22, 19, 40, 49)
        head = _registry_log(ts)
        put = _registry_log(ts + timedelta(milliseconds=15), method="PUT", status=201)
        detector = _registry_detector(put)
        detector._REGISTRY_QUERY_PAGE_SIZE = 1
        detector._REGISTRY_QUERY_MAX_PAGES = 1
        detector._opensearch._client.search.return_value["_scroll_id"] = "scroll-one"
        assert await detector._filter_registry_push_probes([head], ts) == [head]
        detector._opensearch._client.clear_scroll.assert_awaited_once()
