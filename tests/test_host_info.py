"""Inventory never probes a different machine or exposes host details to viewers."""

from unittest.mock import AsyncMock

import pytest

from backend.config import HostConfig
from backend.docker_client import DockerAPIClient
from backend.host_client import SwarmProxyClient
from backend.host_info import docker_host_info, parse_ssh_host_info
from backend.models import HostInfo
from backend.ssh_client import SSHClient


SSH_OUTPUT = '''__PULSAR_hostname__
worker-1
__PULSAR_os__
NAME="Debian GNU/Linux"
PRETTY_NAME="Debian GNU/Linux 13 (trixie)"
__PULSAR_kernel__
6.12.0
__PULSAR_architecture__
x86_64
__PULSAR_ips__
10.0.0.2 2001:db8::2 10.0.0.2 127.0.0.1 invalid
__PULSAR_cpu__
CPU(s):                  8
Model name:              Example CPU
__PULSAR_cores__
0,0
0,1
1,0
1,1
0,0
__PULSAR_memory__
16777216
__PULSAR_gpus__
Example GPU, 8192 MiB
__PULSAR_pci__
00:01.0 VGA compatible controller: Example VGA
__PULSAR_docker__
28.0.0
'''


def test_ssh_inventory_parses_physical_cores_separately_from_threads():
    info = parse_ssh_host_info(SSH_OUTPUT)
    assert info.hostname == 'worker-1'
    assert info.os == 'Debian GNU/Linux 13 (trixie)'
    assert info.ip_addresses == ['10.0.0.2', '2001:db8::2']
    assert info.cpu_model == 'Example CPU'
    assert (info.cpu_cores, info.cpu_threads) == (4, 8)
    assert info.memory_total_mb == 16384
    assert info.gpus == ['Example GPU, 8192 MiB']


def test_optional_utilities_missing_do_not_invent_hardware():
    info = parse_ssh_host_info('__PULSAR_hostname__\nsmall-host\n__PULSAR_memory__\ninvalid')
    assert info.hostname == 'small-host'
    assert info.cpu_cores is None
    assert info.memory_total_mb is None
    assert info.gpus == []


def test_pci_fallback_includes_non_nvidia_gpus():
    info = parse_ssh_host_info('__PULSAR_gpus__\n__PULSAR_pci__\n01:00.0 Display controller: AMD GPU')
    assert info.gpus == ['01:00.0 Display controller: AMD GPU']


def test_standalone_docker_uses_only_literal_endpoint_addresses():
    assert docker_host_info({}, '192.0.2.15').ip_addresses == ['192.0.2.15']
    assert docker_host_info({}, 'docker.example').ip_addresses == []
    assert docker_host_info({}, 'localhost').ip_addresses == []


@pytest.mark.asyncio
async def test_ssh_client_uses_only_fixed_command_on_target(monkeypatch):
    client = SSHClient(HostConfig(name='worker', hostname='worker.example'))
    probe = AsyncMock(return_value=(SSH_OUTPUT, '', 0))
    monkeypatch.setattr(client, 'run_command', probe)
    assert (await client.get_host_info()).hostname == 'worker-1'
    command = probe.call_args.args[0]
    assert 'worker.example' not in command
    assert 'sudo' not in command
    probe.return_value = ('', 'connection failed', 1)
    with pytest.raises(RuntimeError):
        await client.get_host_info()


@pytest.mark.asyncio
async def test_docker_inventory_uses_remote_daemon_not_local_shell(monkeypatch):
    client = DockerAPIClient(HostConfig(name='remote', mode='docker', docker_url='tcp://remote:2375'))
    request = AsyncMock(return_value=({
        'Name': 'remote-host', 'OperatingSystem': 'Ubuntu 24.04',
        'Architecture': 'aarch64', 'NCPU': 16, 'MemTotal': 32 * 1024**3,
        'Swarm': {'NodeAddr': '10.0.0.5'}, 'ServerVersion': '28.0.0',
        'Labels': ['secret=must-not-be-returned'],
    }, 200))
    monkeypatch.setattr(client, '_request', request)
    shell = AsyncMock(side_effect=AssertionError('must not inspect backend host'))
    monkeypatch.setattr(client, 'run_shell_command', shell)
    info = await client.get_host_info()
    request.assert_awaited_once_with('GET', '/info')
    shell.assert_not_called()
    assert info.hostname == 'remote-host'
    assert info.cpu_threads == 16
    assert info.cpu_cores is None  # Docker reports logical CPUs only.
    assert info.cpu_model is None
    assert info.memory_total_mb == 32768
    assert info.ip_addresses == ['10.0.0.5']
    assert 'secret' not in info.model_dump_json()
    request.return_value = ({}, 503)
    with pytest.raises(RuntimeError):
        await client.get_host_info()


@pytest.mark.asyncio
async def test_swarm_proxy_reads_worker_inventory_not_manager(monkeypatch):
    manager = DockerAPIClient(HostConfig(name='manager', mode='docker', docker_url='tcp://manager:2375'))
    request = AsyncMock(return_value=({
        'Description': {'Hostname': 'worker', 'Platform': {'OS': 'linux', 'Architecture': 'arm64'},
                        'Resources': {'NanoCPUs': 4_000_000_000, 'MemoryBytes': 8 * 1024**3}},
        'Status': {'Addr': '10.0.0.7'},
    }, 200))
    monkeypatch.setattr(manager, '_request', request)
    worker = SwarmProxyClient(manager, 'worker-id', 'worker')
    info = await worker.get_host_info()
    request.assert_awaited_once_with('GET', '/nodes/worker-id')
    assert info.hostname == 'worker'
    assert info.cpu_threads == 4
    assert info.memory_total_mb == 8192
    assert info.ip_addresses == ['10.0.0.7']


def test_inventory_api_authorization_and_failures(client, auth_headers, monkeypatch):
    import backend.api as api
    from backend.auth import create_token

    host = AsyncMock()
    host.get_host_info.return_value = HostInfo(hostname='target', cpu_threads=8)
    monkeypatch.setitem(api.collector.clients, 'target', host)
    assert client.get('/api/hosts/target/info').status_code == 401
    viewer = {'Authorization': 'Bearer ' + create_token('sec-tester', api.settings.auth.jwt_secret, 1, role='viewer')}
    assert client.get('/api/hosts/target/info', headers=viewer).status_code == 403
    host.get_host_info.assert_not_called()
    response = client.get('/api/hosts/target/info', headers=auth_headers)
    assert response.status_code == 200
    assert response.json()['cpu_threads'] == 8
    assert client.get('/api/hosts/missing/info', headers=auth_headers).status_code == 404
    host.get_host_info.side_effect = TimeoutError()
    assert client.get('/api/hosts/target/info', headers=auth_headers).status_code == 504
    host.get_host_info.side_effect = RuntimeError('sensitive connection error')
    response = client.get('/api/hosts/target/info', headers=auth_headers)
    assert response.status_code == 502
    assert 'sensitive' not in response.text
