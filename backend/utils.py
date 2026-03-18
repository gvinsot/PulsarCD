"""Utility functions for PulsarCD backend.

Re-exports common parsing functions from shared.log_utils and shared.gpu_utils
so that existing ``from . import utils`` / ``utils.xxx()`` call sites continue
to work without changes.  Backend-only helpers live here directly.
"""

from typing import Optional, Tuple, Any

# Re-export shared utilities so callers can keep using ``utils.func()``
from shared.log_utils import (  # noqa: F401
    strip_ansi,
    parse_size_mb,
    LOG_LEVELS,
    detect_log_level,
    HTTP_STATUS_PATTERNS,
    detect_http_status,
    DOCKER_TIMESTAMP_PATTERN,
    parse_docker_timestamp,
    extract_timestamp_and_message,
    NOISE_PATTERNS,
    should_filter_log_line,
    parse_log_message,
)
from shared.gpu_utils import (  # noqa: F401
    parse_rocm_smi_csv,
    parse_nvidia_smi_csv,
)


# ============== Backend-only helpers ==============

def parse_memory_string(mem_str: str) -> Tuple[float, float]:
    """Parse memory usage string like '100MiB / 1GiB'.

    Returns:
        Tuple of (used_mb, limit_mb)
    """
    parts = mem_str.split(" / ")
    if len(parts) != 2:
        return 0.0, 0.0
    return parse_size_mb(parts[0]), parse_size_mb(parts[1])


def parse_io_string(io_str: str) -> Tuple[int, int]:
    """Parse I/O string like '100MB / 50MB'.

    Returns:
        Tuple of (read_bytes, write_bytes)
    """
    parts = io_str.split(" / ")
    if len(parts) != 2:
        return 0, 0
    return (
        int(parse_size_mb(parts[0]) * 1024 * 1024),
        int(parse_size_mb(parts[1]) * 1024 * 1024)
    )


def build_log_entry(
    line: str,
    host: str,
    container_id: str,
    container_name: str,
    compose_project: Optional[str],
    compose_service: Optional[str],
    stream: str = "stdout",
) -> Optional[Any]:
    """Parse a raw log line and return a LogEntry, or None if filtered.

    Shared by SSHClient and DockerAPIClient to avoid duplicated parsing logic.
    """
    from .models import LogEntry  # local import to avoid circular dependency

    if should_filter_log_line(line):
        return None

    timestamp, message = extract_timestamp_and_message(line)

    # Strip ANSI escape codes — improves searchability, display quality,
    # and ensures level/status detection works correctly
    message = strip_ansi(message)

    if not message.strip():
        return None

    level, http_status, parsed_fields = parse_log_message(message)

    return LogEntry(
        timestamp=timestamp,
        host=host,
        container_id=container_id,
        container_name=container_name,
        compose_project=compose_project,
        compose_service=compose_service,
        stream=stream,
        message=message,
        level=level,
        http_status=http_status,
        parsed_fields=parsed_fields,
    )
