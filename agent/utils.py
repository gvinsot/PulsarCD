"""Utility functions for PulsarCD Agent.

Re-exports common parsing functions from shared.log_utils and shared.gpu_utils
so that existing ``from . import utils`` / ``utils.xxx()`` call sites continue
to work without changes.  Agent-only helpers (GPU collection, disk metrics)
live here directly.
"""

import subprocess
from typing import Dict, List, Optional, Tuple, Any

import structlog

logger = structlog.get_logger()

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


# ============== Agent-only helpers ==============

def run_host_command(cmd: List[str], timeout: int = 5) -> subprocess.CompletedProcess:
    """Execute a command with timeout.

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
    gpu_tool_found = False
    nvidia_error = None
    rocm_error = None

    # Try NVIDIA GPU first
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

    # Fallback to AMD GPU
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

    if gpu_tool_found and (nvidia_error or rocm_error):
        logger.warning("GPU tools found but failed to collect metrics",
                      nvidia_error=nvidia_error, rocm_error=rocm_error)

    return None, None, None


def get_gpu_process_metrics() -> List[Dict[str, Any]]:
    """Get per-process GPU metrics using nvidia-smi.

    Returns a list of dicts with keys: pid, gpu_memory_used_mb, gpu_sm_percent.
    Falls back to empty list if nvidia-smi is unavailable or fails.
    """
    processes: Dict[int, Dict[str, Any]] = {}

    # 1) Per-process VRAM via --query-compute-apps
    try:
        result = run_host_command(
            ["nvidia-smi", "--query-compute-apps=pid,used_gpu_memory",
             "--format=csv,noheader,nounits"],
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 2:
                    try:
                        pid = int(parts[0])
                        mem_mb = float(parts[1])
                        processes[pid] = {"pid": pid, "gpu_memory_used_mb": mem_mb, "gpu_sm_percent": None}
                    except ValueError:
                        continue
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []
    except Exception as e:
        logger.debug("nvidia-smi query-compute-apps failed", error=str(e))
        return []

    if not processes:
        return []

    # 2) Per-process GPU SM utilization via pmon
    try:
        result = run_host_command(
            ["nvidia-smi", "pmon", "-c", "1", "-s", "u"],
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if line.startswith("#") or not line:
                    continue
                parts = line.split()
                # Format: gpu pid type sm mem enc dec ...
                if len(parts) >= 4:
                    try:
                        pid = int(parts[1])
                        sm_val = parts[3]
                        if pid in processes and sm_val != "-":
                            processes[pid]["gpu_sm_percent"] = float(sm_val)
                    except ValueError:
                        continue
    except Exception as e:
        logger.debug("nvidia-smi pmon failed, VRAM-only mode", error=str(e))

    return list(processes.values())


def get_disk_metrics() -> Tuple[float, float, float]:
    """Get disk usage metrics for the root filesystem.

    Returns:
        Tuple of (disk_total_gb, disk_used_gb, disk_percent)
        Returns (0, 0, 0) if metrics cannot be collected.
    """
    import shutil

    try:
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

    try:
        result = run_host_command(["df", "-B1", "/"], timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split("\n")
            if len(lines) >= 2:
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
