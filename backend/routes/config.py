"""Configuration overview and connectivity test (/api/config, /api/config/test).

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

from typing import Any, Dict

from fastapi import APIRouter

from .. import api

router = APIRouter()


# ============== Configuration ==============

@router.get("/api/config")
async def get_config() -> Dict[str, Any]:
    """Get current configuration (for debugging).

    Returns the loaded configuration including hosts, VictoriaLogs settings,
    and collector settings. Useful for verifying environment variables
    are being parsed correctly.
    """
    # Get list of configured hosts
    configured_hosts = [
        {
            "name": h.name,
            "hostname": h.hostname,
            "mode": h.mode,
            "docker_url": h.docker_url,
            "swarm_manager": h.swarm_manager,
            "swarm_routing": h.swarm_routing,
            "swarm_autodiscover": h.swarm_autodiscover,
        }
        for h in api.settings.hosts
    ]

    # Get list of active clients (including discovered nodes)
    active_clients = [
        {
            "name": name,
            "mode": client.config.mode,
            "hostname": client.config.hostname,
        }
        for name, client in api.collector.clients.items()
    ]

    return {
        "hosts": {
            "configured": configured_hosts,
            "active_clients": active_clients,
            "discovered_nodes": list(api.collector._discovered_nodes.keys()) if hasattr(api.collector, '_discovered_nodes') else [],
        },
        "victorialogs": {
            "url": api.settings.victorialogs.url,
            "has_auth": bool(api.settings.victorialogs.username),
        },
        "collector": {
            "log_interval_seconds": api.settings.collector.log_interval_seconds,
            "metrics_interval_seconds": api.settings.collector.metrics_interval_seconds,
            "log_lines_per_fetch": api.settings.collector.log_lines_per_fetch,
        },
        "ai": {
            "model": api.settings.pulsar_config.llm.model if api.settings.pulsar_config else api.settings.ai.model,
            "url": api.settings.pulsar_config.llm.url if api.settings.pulsar_config else "",
        },
        "swarm": {
            "manager_host": api.collector._swarm_manager_host if hasattr(api.collector, '_swarm_manager_host') else None,
            "routing_enabled": api.collector._swarm_routing_enabled if hasattr(api.collector, '_swarm_routing_enabled') else False,
            "autodiscover_enabled": api.collector._swarm_autodiscover_enabled if hasattr(api.collector, '_swarm_autodiscover_enabled') else False,
        }
    }


@router.get("/api/config/test")
async def test_config() -> Dict[str, Any]:
    """Test configuration by checking connectivity to all hosts.

    Returns status of each configured host including:
    - Connection status
    - Number of containers found
    - Any errors encountered
    """
    results = []

    for name, client in api.collector.clients.items():
        result = {
            "name": name,
            "mode": client.config.mode,
            "status": "unknown",
            "containers": 0,
            "error": None,
        }

        try:
            containers = await client.get_containers()
            result["status"] = "connected"
            result["containers"] = len(containers)
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        results.append(result)

    return {
        "hosts": results,
        "total_hosts": len(results),
        "connected": sum(1 for r in results if r["status"] == "connected"),
        "errors": sum(1 for r in results if r["status"] == "error"),
    }
