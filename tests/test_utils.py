"""Unit tests for backend/utils.py — pure logic, no infrastructure needed."""

from datetime import datetime

import pytest

from backend.utils import (
    build_log_entry,
    detect_http_status,
    detect_log_level,
    extract_timestamp_and_message,
    parse_docker_timestamp,
    parse_io_string,
    parse_memory_string,
    parse_nvidia_smi_csv,
    parse_rocm_smi_csv,
    parse_size_mb,
    should_filter_log_line,
)
from backend.error_detector import normalize_message, text_fingerprint


# ── parse_size_mb ────────────────────────────────────────────────────────────

class TestParseSizeMb:
    def test_megabytes(self):
        assert parse_size_mb("100MB") == pytest.approx(100.0)

    def test_mib(self):
        assert parse_size_mb("100MiB") == pytest.approx(100.0)

    def test_gigabytes(self):
        assert parse_size_mb("1GB") == pytest.approx(1024.0)

    def test_kilobytes(self):
        assert parse_size_mb("1024KB") == pytest.approx(1.0)

    def test_raw_bytes(self):
        assert parse_size_mb("1048576") == pytest.approx(1.0)

    def test_with_space(self):
        assert parse_size_mb("2 GB") == pytest.approx(2048.0)

    def test_invalid_returns_zero(self):
        assert parse_size_mb("notanumber") == 0.0


class TestParseMemoryString:
    def test_basic(self):
        used, limit = parse_memory_string("100MiB / 1GiB")
        assert used == pytest.approx(100.0)
        assert limit == pytest.approx(1024.0)

    def test_missing_separator(self):
        assert parse_memory_string("100MB") == (0.0, 0.0)


class TestParseIoString:
    def test_basic(self):
        read, write = parse_io_string("10MB / 5MB")
        assert read == 10 * 1024 * 1024
        assert write == 5 * 1024 * 1024


# ── detect_log_level ─────────────────────────────────────────────────────────

class TestDetectLogLevel:
    def test_error_in_brackets(self):
        assert detect_log_level("[ERROR] something went wrong") == "ERROR"

    def test_info_word(self):
        assert detect_log_level("INFO: server started") == "INFO"

    def test_warning_normalized(self):
        assert detect_log_level("WARNING: disk full") == "WARN"

    def test_fatal(self):
        assert detect_log_level("FATAL: out of memory") == "FATAL"

    def test_no_level(self):
        assert detect_log_level("just a plain message") is None

    def test_debug(self):
        assert detect_log_level("[DEBUG] trace info") == "DEBUG"

    def test_critical(self):
        assert detect_log_level("CRITICAL failure") == "CRITICAL"


# ── detect_http_status ───────────────────────────────────────────────────────

class TestDetectHttpStatus:
    def test_nginx_format(self):
        assert detect_http_status('"GET /api HTTP/1.1" 200 1234') == 200

    def test_status_equals(self):
        assert detect_http_status("status=404 method=GET") == 404

    def test_status_code_colon(self):
        assert detect_http_status("status_code: 500") == 500

    def test_in_brackets(self):
        assert detect_http_status("[200] OK") == 200

    def test_5xx(self):
        assert detect_http_status("HTTP/1.1 503") == 503

    def test_no_status(self):
        assert detect_http_status("plain log message") is None

    def test_out_of_range_ignored(self):
        # 99 is below 100, should be ignored
        assert detect_http_status("code=99") is None


# ── parse_docker_timestamp ───────────────────────────────────────────────────

class TestParseDockerTimestamp:
    def test_nanosecond_precision(self):
        ts = parse_docker_timestamp("2024-01-15T10:30:00.123456789Z")
        assert ts.year == 2024
        assert ts.month == 1
        assert ts.day == 15
        assert ts.hour == 10
        assert ts.minute == 30
        assert ts.second == 0

    def test_millisecond_precision(self):
        ts = parse_docker_timestamp("2024-01-15T10:30:00.123Z")
        assert ts.microsecond == 123000

    def test_no_fraction(self):
        ts = parse_docker_timestamp("2024-01-15T10:30:00Z")
        assert ts.second == 0

    def test_invalid_returns_utcnow(self):
        before = datetime.utcnow()
        ts = parse_docker_timestamp("not-a-timestamp")
        after = datetime.utcnow()
        assert before <= ts <= after


# ── extract_timestamp_and_message ────────────────────────────────────────────

class TestExtractTimestampAndMessage:
    def test_docker_format(self):
        line = "2024-01-15T10:30:00.123Z Hello world"
        ts, msg = extract_timestamp_and_message(line)
        assert ts.year == 2024
        assert msg == "Hello world"

    def test_no_timestamp(self):
        line = "plain log line"
        ts, msg = extract_timestamp_and_message(line)
        assert msg == "plain log line"
        assert isinstance(ts, datetime)

    def test_empty_message_after_timestamp(self):
        # Pattern requires whitespace after the timestamp; a trailing space gives empty message
        line = "2024-01-15T10:30:00.123Z "
        ts, msg = extract_timestamp_and_message(line)
        assert msg == ""

    def test_timestamp_only_line_not_matched(self):
        # Without trailing whitespace the pattern doesn't match → whole line is the message
        line = "2024-01-15T10:30:00.123Z"
        ts, msg = extract_timestamp_and_message(line)
        assert msg == line


# ── normalize_message & text_fingerprint ────────────────────────────────────

class TestNormalizeMessage:
    def test_numbers_replaced(self):
        n1 = normalize_message("retry 4/5")
        n2 = normalize_message("retry 5/5")
        assert n1 == n2

    def test_hex_replaced(self):
        n = normalize_message("connection id: abcdef01")
        assert "<HEX>" in n

    def test_ip_replaced(self):
        n = normalize_message("from 192.168.1.1:8080")
        assert "<IP>" in n

    def test_timestamp_replaced(self):
        n = normalize_message("at 2024-01-15T10:30:00Z error occurred")
        assert "<TS>" in n

    def test_fingerprint_same_for_similar(self):
        fp1 = text_fingerprint("timeout after 30 seconds")
        fp2 = text_fingerprint("timeout after 45 seconds")
        assert fp1 == fp2

    def test_fingerprint_different_for_different(self):
        fp1 = text_fingerprint("connection refused")
        fp2 = text_fingerprint("disk full")
        assert fp1 != fp2

    def test_fingerprint_length(self):
        fp = text_fingerprint("any message")
        assert len(fp) == 16


# ── should_filter_log_line ───────────────────────────────────────────────────

class TestShouldFilterLogLine:
    def test_cgroup_noise_filtered(self):
        line = 'failed to parse CPU allowed micro secs: strconv.ParseFloat: parsing "max": invalid'
        assert should_filter_log_line(line) is True

    def test_normal_line_not_filtered(self):
        assert should_filter_log_line("server started on port 8080") is False

    def test_empty_not_filtered(self):
        assert should_filter_log_line("") is False


# ── build_log_entry ──────────────────────────────────────────────────────────

class TestBuildLogEntry:
    def test_returns_log_entry(self):
        entry = build_log_entry(
            "2024-01-15T10:30:00.123Z [ERROR] something failed",
            host="server-a",
            container_id="abc123",
            container_name="myapp",
            compose_project="myproject",
            compose_service="web",
        )
        assert entry is not None
        assert entry.host == "server-a"
        assert entry.container_id == "abc123"
        assert entry.level == "ERROR"
        assert entry.compose_project == "myproject"
        assert entry.stream == "stdout"

    def test_empty_message_returns_none(self):
        # Trailing space after timestamp → empty message after extraction → filtered
        entry = build_log_entry(
            "2024-01-15T10:30:00.123Z ",
            host="server-a", container_id="x", container_name="y",
            compose_project=None, compose_service=None,
        )
        assert entry is None

    def test_noise_filtered(self):
        entry = build_log_entry(
            'failed to parse CPU allowed micro secs: parsing "max"',
            host="server-a", container_id="x", container_name="y",
            compose_project=None, compose_service=None,
        )
        assert entry is None

    def test_stream_parameter(self):
        entry = build_log_entry(
            "2024-01-15T10:30:00.123Z log line",
            host="h", container_id="c", container_name="n",
            compose_project=None, compose_service=None,
            stream="stderr",
        )
        assert entry.stream == "stderr"

    def test_http_status_detected(self):
        entry = build_log_entry(
            '2024-01-15T10:30:00.123Z "GET /api" 404 0',
            host="h", container_id="c", container_name="n",
            compose_project=None, compose_service=None,
        )
        assert entry is not None
        assert entry.http_status == 404


# ── GPU parsing ──────────────────────────────────────────────────────────────

class TestParseRocmSmiCsv:
    def test_valid_csv(self):
        stdout = "device,GPU use (%),VRAM Total Memory (B),VRAM Total Used Memory (B)\ncard0,42,1073741824,536870912\n"
        gpu, used, total = parse_rocm_smi_csv(stdout)
        assert gpu == pytest.approx(42.0)
        assert used == pytest.approx(512.0)
        assert total == pytest.approx(1024.0)

    def test_empty(self):
        assert parse_rocm_smi_csv("") == (None, None, None)

    def test_header_only(self):
        assert parse_rocm_smi_csv("device,GPU use (%)") == (None, None, None)


class TestParseNvidiaSmiCsv:
    def test_single_gpu(self):
        stdout = "45, 1234, 8192\n"
        gpu, used, total = parse_nvidia_smi_csv(stdout)
        assert gpu == pytest.approx(45.0)
        assert used == pytest.approx(1234.0)
        assert total == pytest.approx(8192.0)

    def test_multi_gpu(self):
        stdout = "40, 1000, 8192\n60, 2000, 8192\n"
        gpu, used, total = parse_nvidia_smi_csv(stdout)
        assert gpu == pytest.approx(50.0)   # average
        assert used == pytest.approx(3000.0)  # sum
        assert total == pytest.approx(16384.0)  # sum

    def test_empty(self):
        assert parse_nvidia_smi_csv("") == (None, None, None)
