"""Host endpoints (/api/hosts/*): list, metrics and host actions.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

import asyncio
from typing import Any, Dict, List

import structlog
from fastapi import APIRouter, HTTPException, Request

from .. import api
from ..models import HostInfo

logger = structlog.get_logger()

router = APIRouter()


# ============== Hosts ==============

@router.get("/api/hosts")
async def list_hosts(request: Request) -> List[Dict[str, Any]]:
    """List all hosts: configured hosts plus discovered Docker Swarm nodes.

    Each swarm node is exposed as a host so the Containers tab can show
    containers per node. Host count = configured + swarm nodes.

    hostname/port/username together form the full SSH connection triple of every
    node in the fleet, which is strictly more than the "hosts" block of
    GET /api/config -- an admin-only route.  Viewers therefore only get the name
    and the swarm flag, which is all the UI needs to target a host by name.
    """
    is_admin = getattr(request.state, "role", "") == "admin"

    def _entry(name: str, config: Any, is_swarm_node: bool) -> Dict[str, Any]:
        entry: Dict[str, Any] = {"name": name, "is_swarm_node": is_swarm_node}
        if is_admin:
            entry["hostname"] = config.hostname
            entry["port"] = config.port
            entry["username"] = config.username
        return entry

    configured_names = {h.name for h in api.settings.hosts}
    result = [_entry(host.name, host, False) for host in api.settings.hosts]
    # Add discovered swarm nodes as hosts (so they appear in host list and Containers tab)
    for name, client in api.collector.clients.items():
        if name not in configured_names:
            result.append(_entry(name, client.config, True))
    return result


@router.get("/api/hosts/metrics")
async def get_hosts_metrics() -> Dict[str, Dict[str, Any]]:
    """Get latest metrics for all hosts including GPU and disk usage.
    
    Returns a dict keyed by host name with metrics:
    - cpu_percent, memory_percent, memory_used_mb, memory_total_mb
    - gpu_percent, gpu_memory_used_mb, gpu_memory_total_mb (if GPU available)
    - disk_total_gb, disk_used_gb, disk_percent
    """
    result = {}
    
    # Get latest host metrics from the log store
    try:
        for host_name in api.collector.clients.keys():
            metrics = await api.logstore.get_latest_host_metrics(host_name)
            if metrics:
                result[host_name] = metrics
    except Exception as e:
        logger.error("Failed to get host metrics", error=str(e))
    
    return result


@router.get("/api/hosts/{host_name}/info", response_model=HostInfo)
async def get_host_info(host_name: str) -> HostInfo:
    """On-demand inventory; admin-only like other infrastructure details."""
    client = api.collector.clients.get(host_name)
    if not client:
        raise HTTPException(status_code=404, detail="Host not found")
    try:
        return await asyncio.wait_for(client.get_host_info(), timeout=20)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Host information request timed out")
    except Exception as exc:
        logger.warning("Failed to get host information", host=host_name, error=str(exc))
        raise HTTPException(status_code=502, detail="Host information is unavailable. Check the host connection.")


@router.post("/api/hosts/{host_name}/action")
async def execute_host_action(host_name: str, request: Dict[str, str]) -> Dict[str, Any]:
    """Execute an action on a host (reboot or shutdown).
    
    Args:
        host_name: Name of the host to perform action on
        request: Dict with 'action' key: 'reboot' or 'shutdown'
    """
    action = request.get("action", "").lower()
    
    if action not in ("reboot", "shutdown"):
        raise HTTPException(status_code=400, detail="Invalid action. Must be 'reboot' or 'shutdown'")
    
    client = api.collector.clients.get(host_name)
    if not client:
        raise HTTPException(status_code=404, detail=f"Host '{host_name}' not found")
    
    try:
        if action == "reboot":
            command = "sudo reboot"
        else:
            command = "sudo shutdown -h now"
        
        # Execute the command
        success, output = await client.run_shell_command(command)
        
        # For reboot/shutdown, command may "fail" because connection drops
        # We consider it a success if it was sent
        return {
            "success": True,
            "message": f"{action.capitalize()} command sent to {host_name}",
            "host": host_name,
            "action": action,
            "output": output
        }
    except Exception as e:
        # Connection dropping during shutdown is expected
        if "disconnect" in str(e).lower() or "closed" in str(e).lower():
            return {
                "success": True,
                "message": f"{action.capitalize()} initiated on {host_name}",
                "host": host_name,
                "action": action
            }
        logger.error("Failed to execute host action", host=host_name, action=action, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to {action}: {str(e)}")
