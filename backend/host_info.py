"""Read-only inventory parsers for SSH hosts and Docker daemons.

Never inspect the backend's own hardware for a remote Docker host. Docker and
Swarm intentionally return partial inventory when hardware details are absent.
"""

import ipaddress
import re
from typing import Any, Dict

from .models import HostInfo

# Constant script: no host name or other request input is interpolated. Missing
# optional utilities leave a section empty. The caller bounds the whole probe.
SSH_HOST_INFO_COMMAND = r"""
export LC_ALL=C
printf '__PULSAR_hostname__\n'
hostname || exit 1
printf '__PULSAR_os__\n'
cat /etc/os-release 2>/dev/null
printf '__PULSAR_kernel__\n'
uname -r
printf '__PULSAR_architecture__\n'
uname -m
printf '__PULSAR_ips__\n'
hostname -I 2>/dev/null
printf '__PULSAR_cpu__\n'
lscpu 2>/dev/null
printf '__PULSAR_cores__\n'
lscpu -p=SOCKET,CORE 2>/dev/null | sed '/^#/d; /^$/d' | sort -u
printf '__PULSAR_memory__\n'
awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null
printf '__PULSAR_gpus__\n'
if command -v nvidia-smi >/dev/null 2>&1; then
    timeout 5 nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null
fi
printf '__PULSAR_pci__\n'
lspci 2>/dev/null | grep -Ei 'VGA compatible controller|3D controller|Display controller'
printf '__PULSAR_docker__\n'
timeout 5 docker version --format '{{.Server.Version}}' 2>/dev/null
true
"""


def _positive(value: Any) -> float | None:
    try:
        number = float(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def _addresses(value: str) -> list[str]:
    addresses = []
    for token in value.split():
        try:
            address = ipaddress.ip_address(token)
        except ValueError:
            continue
        if not address.is_loopback and not address.is_unspecified:
            addresses.append(str(address))
    return sorted(set(addresses))


def parse_ssh_host_info(output: str) -> HostInfo:
    sections: Dict[str, str] = {}
    parts = re.split(r"^__PULSAR_(\w+)__\s*$", output, flags=re.MULTILINE)
    for index in range(1, len(parts), 2):
        sections[parts[index]] = parts[index + 1].strip()
    os_fields = dict(line.split("=", 1) for line in sections.get("os", "").splitlines() if "=" in line)
    cpu_fields = dict(line.split(":", 1) for line in sections.get("cpu", "").splitlines() if ":" in line)
    cores = {line for line in sections.get("cores", "").splitlines() if re.fullmatch(r"\d+,\d+", line)}
    memory_kb = _positive(sections.get("memory"))
    threads = _positive(cpu_fields.get("CPU(s)"))
    gpus = sections.get("gpus") or sections.get("pci", "")
    return HostInfo(
        hostname=sections.get("hostname") or None,
        os=os_fields.get("PRETTY_NAME", os_fields.get("NAME", "")).strip('"\'') or None,
        kernel=sections.get("kernel") or None,
        architecture=sections.get("architecture") or None,
        ip_addresses=_addresses(sections.get("ips", "")),
        cpu_model=cpu_fields.get("Model name", "").strip() or None,
        cpu_threads=int(threads) if threads else None,
        cpu_cores=len(cores) or None,
        memory_total_mb=round(memory_kb / 1024, 2) if memory_kb else None,
        gpus=gpus.splitlines(),
        docker_version=sections.get("docker") or None,
    )


def docker_host_info(data: Dict[str, Any], daemon_address: str = "") -> HostInfo:
    memory = _positive(data.get("MemTotal"))
    return HostInfo(
        hostname=data.get("Name"), os=data.get("OperatingSystem") or data.get("OSType"),
        kernel=data.get("KernelVersion"), architecture=data.get("Architecture"),
        ip_addresses=_addresses(data.get("Swarm", {}).get("NodeAddr") or daemon_address),
        cpu_threads=data.get("NCPU") or None,
        memory_total_mb=round(memory / 1024**2, 2) if memory else None,
        docker_version=data.get("ServerVersion"),
    )


def swarm_host_info(data: Dict[str, Any]) -> HostInfo:
    description = data.get("Description", {})
    platform = description.get("Platform", {})
    resources = description.get("Resources", {})
    memory = _positive(resources.get("MemoryBytes"))
    nano_cpus = _positive(resources.get("NanoCPUs"))
    return HostInfo(
        hostname=description.get("Hostname"), os=platform.get("OS"),
        architecture=platform.get("Architecture"),
        ip_addresses=_addresses(data.get("Status", {}).get("Addr", "")),
        cpu_threads=int(nano_cpus / 10**9) if nano_cpus else None,
        memory_total_mb=round(memory / 1024**2, 2) if memory else None,
        docker_version=description.get("Engine", {}).get("EngineVersion"),
    )
