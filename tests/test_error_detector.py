"""Unit tests for backend/error_detector.py — no infrastructure needed."""

from datetime import datetime, timedelta
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
