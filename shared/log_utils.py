"""Shared log parsing utilities for PulsarCD.

Common parsing functions used by both the backend and agent to ensure
consistent behavior and avoid code duplication.
"""

import json
import re
from datetime import datetime
from typing import Dict, Optional, Tuple, Any

import structlog

logger = structlog.get_logger()


# ============== ANSI Escape Code Stripping ==============

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')


def strip_ansi(text: str) -> str:
    """Strip ANSI escape codes from text.

    Many containers (structlog ConsoleRenderer, coloredlogs, etc.) output ANSI
    colour/style codes.  These break regex-based detection of log levels and
    other patterns because escape sequences sit directly adjacent to the level
    word, eliminating the ``\\b`` word boundary.
    """
    return _ANSI_RE.sub('', text)


# ============== Size Parsing ==============

def parse_size_mb(size_str: str) -> float:
    """Convert size string to MB.

    Handles various formats:
    - "100MB", "100MiB", "100 MB"
    - "1.5GB", "1.5GiB"
    - "1024KB", "1024KiB"
    - "1073741824" (bytes as raw number)
    """
    size_str = size_str.strip().upper()

    multipliers = {
        "B": 1 / (1024 * 1024),
        "BYTES": 1 / (1024 * 1024),
        "KB": 1 / 1024,
        "KIB": 1 / 1024,
        "MB": 1,
        "MIB": 1,
        "GB": 1024,
        "GIB": 1024,
        "TB": 1024 * 1024,
        "TIB": 1024 * 1024,
    }

    # Sort by suffix length descending so "MB" is tried before "B", "GIB" before "B", etc.
    for suffix, mult in sorted(multipliers.items(), key=lambda x: len(x[0]), reverse=True):
        if size_str.endswith(suffix):
            try:
                return float(size_str[:-len(suffix)].strip()) * mult
            except ValueError:
                return 0.0

    # Try as raw bytes if no unit found
    try:
        return float(size_str) / (1024 * 1024)
    except ValueError:
        return 0.0


# ============== Log Level Detection ==============

LOG_LEVELS = ["CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE"]


def detect_log_level(message: str) -> Optional[str]:
    """Detect log level from message content.

    Looks for common log level patterns in the message.
    Returns normalized level name (WARNING -> WARN).
    """
    # Strip ANSI escape codes first — they break word-boundary matching
    msg_upper = strip_ansi(message).upper()

    # Check for level in brackets first (e.g., "[ERROR]", "[info]")
    bracket_match = re.search(r'\[(\w+)\]', msg_upper)
    if bracket_match:
        level = bracket_match.group(1)
        if level in LOG_LEVELS:
            return level.replace("WARNING", "WARN")

    # Check for level followed by separator (e.g., "ERROR:", "INFO -")
    for level in LOG_LEVELS:
        if re.search(rf'\b{level}\b', msg_upper):
            return level.replace("WARNING", "WARN")

    return None


# ============== HTTP Status Detection ==============

HTTP_STATUS_PATTERNS = [
    r'HTTP/\d\.\d["\s]+(\d{3})',           # HTTP/1.1" 200 or HTTP/1.1 200
    r'status[_\s]*(?:code)?[=:\s]+(\d{3})', # status=200, status_code=200, status: 200
    r'\[(\d{3})\]',                          # [200]
    r'"\s+(\d{3})\s+\d+',                    # nginx: " 200 1234"
    r'\s(\d{3})\s+[-\d]+\s*$',               # traefik: 200 123 at end
    r'"status":\s*(\d{3})',                  # JSON: "status": 200
]


def detect_http_status(message: str) -> Optional[int]:
    """Detect HTTP status code from log message.

    Looks for common HTTP status patterns in access logs.
    """
    for pattern in HTTP_STATUS_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            try:
                status = int(match.group(1))
                if 100 <= status < 600:
                    return status
            except ValueError:
                continue
    return None


# ============== Timestamp Parsing ==============

DOCKER_TIMESTAMP_PATTERN = re.compile(
    r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.?\d*Z?)\s+'
)


def parse_docker_timestamp(timestamp_str: str) -> datetime:
    """Parse Docker log timestamp.

    Handles various timestamp formats from Docker:
    - 2024-01-15T10:30:00.123456789Z
    - 2024-01-15T10:30:00.123Z
    - 2024-01-15T10:30:00Z
    """
    try:
        ts = timestamp_str.rstrip('Z')
        # Truncate nanoseconds to microseconds
        if '.' in ts:
            base, frac = ts.split('.', 1)
            ts = f"{base}.{frac[:6]}"
        return datetime.fromisoformat(ts)
    except Exception:
        return datetime.utcnow()


def extract_timestamp_and_message(line: str) -> Tuple[datetime, str]:
    """Extract timestamp and message from a Docker log line."""
    match = DOCKER_TIMESTAMP_PATTERN.match(line)

    if match:
        timestamp_str = match.group(1)
        message = line[match.end():]
        timestamp = parse_docker_timestamp(timestamp_str)
    else:
        timestamp = datetime.utcnow()
        message = line

    return timestamp, message


# ============== Log Line Parsing ==============

# Known noise patterns to filter
NOISE_PATTERNS = [
    # Go cgroup v2 parsing warning
    (r'failed to parse CPU allowed micro secs', r'parsing.*"max"'),
]


def should_filter_log_line(line: str) -> bool:
    """Check if log line should be filtered out.

    Filters known noise from external libraries that isn't useful.
    """
    for patterns in NOISE_PATTERNS:
        if all(re.search(p, line, re.IGNORECASE) for p in patterns):
            return True
    return False


def parse_log_message(message: str) -> Tuple[Optional[str], Optional[int], Dict[str, Any]]:
    """Parse log message for level, HTTP status, and structured fields."""
    level = detect_log_level(message)
    http_status = detect_http_status(message)
    parsed_fields: Dict[str, Any] = {}

    # Try to parse JSON
    if message.strip().startswith("{"):
        try:
            parsed_fields = json.loads(message.strip())
            # Extract level from JSON if present
            if "level" in parsed_fields:
                json_level = str(parsed_fields["level"]).upper()
                if json_level in LOG_LEVELS or json_level == "WARN":
                    level = json_level.replace("WARNING", "WARN")
            # Extract status from JSON if present
            if "status" in parsed_fields and isinstance(parsed_fields["status"], int):
                http_status = parsed_fields["status"]
        except json.JSONDecodeError:
            pass

    return level, http_status, parsed_fields
