"""Shared GPU metrics parsing for PulsarCD.

Common GPU output parsers used by both the backend and agent.
"""

from typing import Optional, Tuple

import structlog

logger = structlog.get_logger()


def parse_rocm_smi_csv(stdout: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Parse rocm-smi CSV output for GPU metrics.

    Expected format:
        device,GPU use (%),VRAM Total Memory (B),VRAM Total Used Memory (B)
        card0,0,1073741824,81498112
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
                    logger.debug("AMD GPU metrics collected",
                                 gpu_percent=gpu_use, mem_used_mb=round(mem_used, 2),
                                 mem_total_mb=round(mem_total, 2))
                    return gpu_use, mem_used, mem_total
                except (ValueError, IndexError) as e:
                    logger.warning("Failed to parse rocm-smi CSV line", line=line, error=str(e))
            else:
                logger.warning("rocm-smi CSV line has fewer than 4 columns",
                               line=line, parts_count=len(parts))

    logger.warning("No valid GPU data found in rocm-smi output", lines_count=len(lines))
    return None, None, None


def parse_nvidia_smi_csv(stdout: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Parse nvidia-smi CSV output for GPU metrics.

    Expected format (from --format=csv,noheader,nounits):
        Single GPU:  45, 1234, 8192
        Multi GPU:   45, 1234, 8192
                     67, 2048, 8192

    For multi-GPU systems, returns average utilization and summed memory.
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
            logger.warning("nvidia-smi line has fewer than 3 values",
                           line=line[:100], parts_count=len(parts))

    if gpu_utils:
        avg_util = sum(gpu_utils) / len(gpu_utils)
        logger.debug("NVIDIA GPU metrics parsed", gpu_count=len(gpu_utils),
                     avg_util=round(avg_util, 1), mem_used_mb=round(mem_used_total, 1),
                     mem_total_mb=round(mem_total_total, 1))
        return avg_util, mem_used_total, mem_total_total

    return None, None, None
