"""Shared utility functions for LogsCrawler backend.

This module contains common parsing functions used by both SSHClient and DockerAPIClient
to avoid code duplication and ensure consistent behavior.
"""

import json
import re
from datetime import datetime
from typing import Dict, Optional, Tuple, Any

import structlog

logger = structlog.get_logger()


# ============== Size Parsing ==============

def parse_size_mb(size_str: str) -> float:
    """Convert size string to MB.
    
    Handles various formats:
    - "100MB", "100MiB", "100 MB"
    - "1.5GB", "1.5GiB"
    - "1024KB", "1024KiB"
    - "1073741824" (bytes as raw number)
    
    Args:
        size_str: Size string with optional unit
        
    Returns:
        Size in megabytes
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


def parse_memory_string(mem_str: str) -> Tuple[float, float]:
    """Parse memory usage string like '100MiB / 1GiB'.
    
    Args:
        mem_str: Memory string in format "used / limit"
        
    Returns:
        Tuple of (used_mb, limit_mb)
    """
    parts = mem_str.split(" / ")
    if len(parts) != 2:
        return 0.0, 0.0
    return parse_size_mb(parts[0]), parse_size_mb(parts[1])


def parse_io_string(io_str: str) -> Tuple[int, int]:
    """Parse I/O string like '100MB / 50MB'.
    
    Args:
        io_str: I/O string in format "read / write"
        
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


# ============== Log Level Detection ==============

# Log levels in order of severity
LOG_LEVELS = ["CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE"]


def detect_log_level(message: str) -> Optional[str]:
    """Detect log level from message content.
    
    Looks for common log level patterns in the message.
    Returns normalized level name (WARNING -> WARN).
    
    Args:
        message: Log message to analyze
        
    Returns:
        Detected log level or None
    """
    msg_upper = message.upper()
    
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

# Patterns to detect HTTP status codes in logs
HTTP_STATUS_PATTERNS = [
    r'HTTP/\d\.\d["\s]+(\d{3})',           # HTTP/1.1" 200 or HTTP/1.1 200
    r'status[_\s]*(?:code)?[=:\s]+(\d{3})', # status=200, status_code=200, status: 200
    r'\[(\d{3})\]',                          # [200]
    r'"\s+(\d{3})\s+\d+',                    # nginx: " 200 1234"
    r'\s(\d{3})\s+[-\d]+\s*$',               # traefik: 200 123 at end
]


def detect_http_status(message: str) -> Optional[int]:
    """Detect HTTP status code from log message.
    
    Looks for common HTTP status patterns in access logs.
    
    Args:
        message: Log message to analyze
        
    Returns:
        HTTP status code (100-599) or None
    """
    for pattern in HTTP_STATUS_PATTERNS:
        match = re.search(pattern, message)
        if match:
            try:
                status = int(match.group(1))
                if 100 <= status < 600:
                    return status
            except ValueError:
                continue
    return None


# ============== Timestamp Parsing ==============

def parse_docker_timestamp(timestamp_str: str) -> datetime:
    """Parse Docker log timestamp.
    
    Handles various timestamp formats from Docker:
    - 2024-01-15T10:30:00.123456789Z
    - 2024-01-15T10:30:00.123Z
    - 2024-01-15T10:30:00Z
    
    Args:
        timestamp_str: Timestamp string from Docker
        
    Returns:
        Parsed datetime (UTC)
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


# ============== Log Line Parsing ==============

# Docker timestamp pattern
DOCKER_TIMESTAMP_PATTERN = re.compile(
    r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.?\d*Z?)\s+'
)

# Known noise patterns to filter
NOISE_PATTERNS = [
    # Go cgroup v2 parsing warning
    (r'failed to parse CPU allowed micro secs', r'parsing.*"max"'),
]


def should_filter_log_line(line: str) -> bool:
    """Check if log line should be filtered out.
    
    Filters known noise from external libraries that isn't useful.
    
    Args:
        line: Log line to check
        
    Returns:
        True if line should be filtered out
    """
    for patterns in NOISE_PATTERNS:
        if all(re.search(p, line, re.IGNORECASE) for p in patterns):
            return True
    return False


def parse_log_message(message: str) -> Tuple[Optional[str], Optional[int], Dict[str, Any]]:
    """Parse log message for level, HTTP status, and structured fields.
    
    Args:
        message: Log message content
        
    Returns:
        Tuple of (level, http_status, parsed_fields)
    """
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


def extract_timestamp_and_message(line: str) -> Tuple[datetime, str]:
    """Extract timestamp and message from a Docker log line.
    
    Args:
        line: Raw log line with optional timestamp prefix
        
    Returns:
        Tuple of (timestamp, message)
    """
    match = DOCKER_TIMESTAMP_PATTERN.match(line)
    
    if match:
        timestamp_str = match.group(1)
        message = line[match.end():]
        timestamp = parse_docker_timestamp(timestamp_str)
    else:
        timestamp = datetime.utcnow()
        message = line
    
    return timestamp, message


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


# ============== GPU Metrics Parsing ==============

def parse_rocm_smi_csv(stdout: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Parse rocm-smi CSV output for GPU metrics.
    
    Expected format:
        device,GPU use (%),VRAM Total Memory (B),VRAM Total Used Memory (B)
        card0,0,1073741824,81498112
    
    Args:
        stdout: Output from rocm-smi --showuse --showmeminfo vram --csv
        
    Returns:
        Tuple of (gpu_percent, vram_used_mb, vram_total_mb)
    """
    if not stdout.strip():
        return None, None, None
    
    lines = stdout.strip().split("\n")
    
    for line in lines:
        line_lower = line.lower()
        # Skip header line
        if "device" in line_lower or "gpu use" in line_lower or not line.strip():
            continue
        # Data lines start with "card0", "card1", etc.
        if line_lower.startswith("card"):
            parts = [p.strip() for p in line.split(",")]
            logger.debug("rocm-smi CSV parts", parts=parts)
            # parts[0]=device, parts[1]=GPU use (%), parts[2]=VRAM Total (B), parts[3]=VRAM Used (B)
            if len(parts) >= 4:
                try:
                    gpu_use = float(parts[1].replace('%', '').strip())
                    vram_total_bytes = float(parts[2].strip())
                    vram_used_bytes = float(parts[3].strip())
                    mem_total = vram_total_bytes / (1024 * 1024)
                    mem_used = vram_used_bytes / (1024 * 1024)
                    logger.info("AMD GPU metrics collected", 
                               gpu_percent=gpu_use, mem_used_mb=mem_used, mem_total_mb=mem_total)
                    return gpu_use, mem_used, mem_total
                except (ValueError, IndexError) as e:
                    logger.warning("Failed to parse rocm-smi CSV line", line=line, error=str(e))
    
    return None, None, None


def parse_nvidia_smi_csv(stdout: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Parse nvidia-smi CSV output for GPU metrics.

    Expected format (from --format=csv,noheader,nounits):
        Single GPU:  45, 1234, 8192
        Multi GPU:   45, 1234, 8192
                     67, 2048, 8192

    For multi-GPU systems, returns average utilization and summed memory.

    Args:
        stdout: Output from nvidia-smi --query-gpu=... --format=csv,noheader,nounits

    Returns:
        Tuple of (gpu_percent, mem_used_mb, mem_total_mb)
    """
    if not stdout.strip():
        return None, None, None

    gpu_utils = []
    mem_used_total = 0.0
    mem_total_total = 0.0

    for line in stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split(", ")
        if len(parts) >= 3:
            try:
                gpu_utils.append(float(parts[0]))
                mem_used_total += float(parts[1])
                mem_total_total += float(parts[2])
            except ValueError as e:
                logger.warning("Failed to parse nvidia-smi line", line=line[:100], error=str(e))
        else:
            logger.warning("nvidia-smi line has fewer than 3 values", line=line[:100], parts_count=len(parts))

    if gpu_utils:
        avg_util = sum(gpu_utils) / len(gpu_utils)
        logger.debug("NVIDIA GPU metrics parsed", gpu_count=len(gpu_utils), avg_util=round(avg_util, 1),
                    mem_used_mb=round(mem_used_total, 1), mem_total_mb=round(mem_total_total, 1))
        return avg_util, mem_used_total, mem_total_total

    return None, None, None
