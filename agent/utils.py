"""Shared utility functions for PulsarCD Agent.

This module contains common parsing functions used by the agent components
to avoid code duplication and ensure consistent behavior.
"""

import json
import re
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

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
    
    for suffix, mult in multipliers.items():
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

HTTP_STATUS_PATTERNS = [
    r'HTTP/\d\.\d["\s]+(\d{3})',           # HTTP/1.1" 200
    r'status[_\s]*(?:code)?[=:\s]+(\d{3})', # status=200, status_code=200
    r'\[(\d{3})\]',                          # [200]
    r'"\s+(\d{3})\s+\d+',                    # nginx: " 200 1234"
    r'\s(\d{3})\s+[-\d]+\s*$',               # traefik: 200 123 at end
    r'"status":\s*(\d{3})',                  # JSON: "status": 200
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


# ============== Log Line Parsing ==============

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


# ============== GPU Metrics ==============

def run_host_command(cmd: List[str], timeout: int = 5) -> subprocess.CompletedProcess:
    """Execute a command with timeout.
    
    Args:
        cmd: Command and arguments as a list
        timeout: Timeout in seconds
        
    Returns:
        CompletedProcess with stdout, stderr, and returncode
        
    Raises:
        FileNotFoundError: If the command binary is not found
    """
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout
    )


def get_gpu_metrics() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Get GPU metrics using nvidia-smi (NVIDIA) or rocm-smi (AMD).

    Tries NVIDIA GPU first with nvidia-smi, then falls back to AMD with rocm-smi.
    NVIDIA is tried first because rocm-smi can return false positives on NVIDIA
    machines when /sys or /dev/dri are mounted.

    Returns:
        Tuple of (gpu_percent, vram_used_mb, vram_total_mb)
        All values are None if no GPU is detected or metrics cannot be collected.
    """
    # Track if we found any GPU tool and any errors
    gpu_tool_found = False
    nvidia_error = None
    rocm_error = None

    # Try NVIDIA GPU first (nvidia-smi is more reliable and won't false-positive on AMD)
    try:
        result = run_host_command(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"],
            timeout=5
        )
        gpu_tool_found = True
        logger.debug("nvidia-smi executed", returncode=result.returncode,
                   stdout_preview=result.stdout[:200] if result.stdout else "(empty)")

        if result.returncode == 0 and result.stdout.strip():
            gpu_percent, mem_used, mem_total = parse_nvidia_smi_csv(result.stdout)
            if gpu_percent is not None:
                logger.debug("NVIDIA GPU metrics collected", gpu_percent=gpu_percent, mem_used_mb=mem_used, mem_total_mb=mem_total)
                return gpu_percent, mem_used, mem_total
            else:
                nvidia_error = f"Parsing failed - stdout: {result.stdout[:200]}"
                logger.warning("nvidia-smi returned data but parsing failed",
                              stdout=result.stdout[:200],
                              hint="Check if nvidia-smi output format has changed")
        elif result.returncode != 0:
            nvidia_error = f"Command failed with code {result.returncode}"
            logger.warning("nvidia-smi command failed",
                          returncode=result.returncode,
                          stderr=result.stderr[:200] if result.stderr else "no error output")

    except FileNotFoundError:
        logger.debug("nvidia-smi not found in PATH, trying rocm-smi")
    except subprocess.TimeoutExpired:
        nvidia_error = "Command timed out after 5 seconds"
        logger.warning("nvidia-smi command timed out after 5 seconds")
    except Exception as e:
        nvidia_error = f"{type(e).__name__}: {str(e)}"
        logger.warning("nvidia-smi failed with unexpected error", error=str(e), error_type=type(e).__name__)

    # Fallback to AMD GPU (rocm-smi with CSV format)
    try:
        result = run_host_command(
            ["rocm-smi", "--showuse", "--showmeminfo", "vram", "--csv"],
            timeout=5
        )
        gpu_tool_found = True
        logger.debug("rocm-smi executed", returncode=result.returncode,
                   stdout_preview=result.stdout[:200] if result.stdout else "(empty)",
                   stderr_preview=result.stderr[:100] if result.stderr else "(empty)")

        if result.returncode == 0 and result.stdout.strip():
            gpu_percent, mem_used, mem_total = parse_rocm_smi_csv(result.stdout)
            if gpu_percent is not None or mem_used is not None:
                return gpu_percent, mem_used, mem_total
            else:
                rocm_error = f"Parsing failed - stdout: {result.stdout[:300]}"
                logger.warning("rocm-smi returned data but parsing failed",
                              stdout=result.stdout[:500],
                              hint="Check if rocm-smi output format has changed")
        elif result.returncode != 0:
            rocm_error = f"Command failed with code {result.returncode}: {result.stderr[:200] if result.stderr else 'no error'}"
            logger.warning("rocm-smi command failed",
                          returncode=result.returncode,
                          stderr=result.stderr[:200] if result.stderr else "no error output")

    except FileNotFoundError:
        if not gpu_tool_found:
            logger.warning("No GPU monitoring tools found - neither nvidia-smi nor rocm-smi are available in PATH")
        else:
            logger.debug("rocm-smi not found in PATH")
    except subprocess.TimeoutExpired:
        rocm_error = "Command timed out after 5 seconds"
        logger.warning("rocm-smi command timed out after 5 seconds")
    except Exception as e:
        rocm_error = f"{type(e).__name__}: {str(e)}"
        logger.warning("rocm-smi failed with unexpected error", error=str(e), error_type=type(e).__name__)

    # Final warning if no GPU data collected
    if gpu_tool_found and (nvidia_error or rocm_error):
        logger.warning("GPU tools found but failed to collect metrics",
                      nvidia_error=nvidia_error, rocm_error=rocm_error)

    return None, None, None


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
                               gpu_percent=gpu_use, mem_used_mb=round(mem_used, 2), mem_total_mb=round(mem_total, 2))
                    return gpu_use, mem_used, mem_total
                except (ValueError, IndexError) as e:
                    logger.warning("Failed to parse rocm-smi CSV line", line=line, error=str(e))
            else:
                logger.warning("rocm-smi CSV line has fewer than 4 columns", line=line, parts_count=len(parts))
    
    logger.warning("No valid GPU data found in rocm-smi output", lines_count=len(lines))
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


# ============== Disk Metrics ==============

def get_disk_metrics() -> Tuple[float, float, float]:
    """Get disk usage metrics for the root filesystem.
    
    Uses 'df' command to get disk usage. Falls back to shutil.disk_usage if available.
    
    Returns:
        Tuple of (disk_total_gb, disk_used_gb, disk_percent)
        Returns (0, 0, 0) if metrics cannot be collected.
    """
    import shutil
    
    # Try using shutil.disk_usage (works in both containers and host)
    try:
        # Check root filesystem
        usage = shutil.disk_usage("/")
        total_gb = usage.total / (1024 ** 3)
        used_gb = usage.used / (1024 ** 3)
        percent = (usage.used / usage.total) * 100 if usage.total > 0 else 0
        logger.debug("Disk metrics collected via shutil", 
                    total_gb=round(total_gb, 2), 
                    used_gb=round(used_gb, 2), 
                    percent=round(percent, 1))
        return round(total_gb, 2), round(used_gb, 2), round(percent, 1)
    except Exception as e:
        logger.debug("shutil.disk_usage failed, trying df command", error=str(e))
    
    # Fallback to df command
    try:
        result = run_host_command(["df", "-B1", "/"], timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split("\n")
            if len(lines) >= 2:
                # Parse df output: Filesystem 1B-blocks Used Available Use% Mounted
                parts = lines[1].split()
                if len(parts) >= 5:
                    total_bytes = int(parts[1])
                    used_bytes = int(parts[2])
                    total_gb = total_bytes / (1024 ** 3)
                    used_gb = used_bytes / (1024 ** 3)
                    percent = (used_bytes / total_bytes) * 100 if total_bytes > 0 else 0
                    logger.debug("Disk metrics collected via df", 
                                total_gb=round(total_gb, 2), 
                                used_gb=round(used_gb, 2), 
                                percent=round(percent, 1))
                    return round(total_gb, 2), round(used_gb, 2), round(percent, 1)
    except FileNotFoundError:
        logger.debug("df command not found")
    except subprocess.TimeoutExpired:
        logger.warning("df command timed out")
    except Exception as e:
        logger.warning("Failed to get disk metrics via df", error=str(e))
    
    return 0.0, 0.0, 0.0
