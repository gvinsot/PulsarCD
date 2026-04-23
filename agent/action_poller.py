"""Action poller for the agent - polls backend for pending actions and executes them."""

import asyncio
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import structlog

from .docker_collector import DockerCollector

logger = structlog.get_logger()


class ActionPoller:
    """Polls the backend for pending actions and executes them locally."""

    def __init__(
        self,
        backend_url: str,
        agent_id: str,
        docker_collector: DockerCollector,
        poll_interval: int = 2,
        auth_key: str = "",
    ):
        self.backend_url = backend_url.rstrip("/")
        self.agent_id = agent_id
        self.docker = docker_collector
        self.poll_interval = poll_interval
        self.auth_key = auth_key
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False

    def _auth_headers(self) -> dict:
        """Return auth headers for backend requests."""
        if self.auth_key:
            return {"Authorization": f"Bearer {self.auth_key}"}
        return {}

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._auth_headers())
        return self._session

    async def close(self):
        """Close the session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def poll_actions(self) -> List[Dict[str, Any]]:
        """Poll backend for pending actions."""
        session = await self._get_session()
        url = f"{self.backend_url}/api/agent/actions?agent_id={self.agent_id}"

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("actions", [])
                else:
                    logger.warning("Failed to poll actions", status=response.status)
                    return []
        except asyncio.TimeoutError:
            logger.debug("Action poll timeout")
            return []
        except aiohttp.ClientError as e:
            logger.warning("Failed to poll actions", error=str(e))
            return []
        except Exception as e:
            logger.error("Unexpected error polling actions", error=str(e))
            return []

    async def report_system_error(self, category: str, error_type: str, error: str):
        """Report a system-level error to the backend for dashboard visibility."""
        session = await self._get_session()
        url = f"{self.backend_url}/api/agent/system-error"
        payload = {
            "agent_id": self.agent_id,
            "category": category,
            "error_type": error_type,
            "error": str(error)[:1000],
        }
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status != 200:
                    logger.debug("Failed to report system error", status=response.status)
        except Exception:
            pass

    async def send_result(self, action_id: str, success: bool, output: str):
        """Send action result back to backend."""
        session = await self._get_session()
        url = f"{self.backend_url}/api/agent/result"

        payload = {
            "agent_id": self.agent_id,
            "action_id": action_id,
            "success": success,
            "output": output,
            "completed_at": datetime.utcnow().isoformat(),
        }

        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    logger.warning("Failed to send action result", action_id=action_id, status=response.status)
        except Exception as e:
            logger.error("Failed to send action result", action_id=action_id, error=str(e))

    async def execute_action(self, action: Dict[str, Any]) -> Tuple[bool, str]:
        """Execute an action and return result."""
        action_type = action.get("type")
        payload = action.get("payload", {})

        logger.info("Executing action", action_type=action_type, action_id=action.get("id"))

        try:
            if action_type == "container_action":
                return await self._execute_container_action(payload)
            elif action_type == "exec":
                return await self._execute_exec(payload)
            elif action_type == "get_logs":
                return await self._execute_get_logs(payload)
            elif action_type == "get_env":
                return await self._execute_get_env(payload)
            else:
                return False, f"Unknown action type: {action_type}"

        except Exception as e:
            logger.error("Action execution failed", action_type=action_type, error=str(e))
            return False, str(e)

    async def _execute_container_action(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """Execute container action (start, stop, restart, etc.)."""
        container_id = payload.get("container_id")
        action = payload.get("action")

        if not container_id or not action:
            return False, "Missing container_id or action"

        valid_actions = ["start", "stop", "restart", "pause", "unpause", "remove"]
        if action not in valid_actions:
            return False, f"Invalid action: {action}"

        endpoint = f"/containers/{container_id}/{action}"
        method = "POST"

        if action == "remove":
            endpoint = f"/containers/{container_id}?force=true"
            method = "DELETE"

        data, status = await self.docker._request(method, endpoint)

        if status in [200, 204]:
            return True, f"Container {action} successful"
        else:
            return False, f"Container {action} failed: {data}"

    async def _execute_exec(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """Execute command inside container."""
        container_id = payload.get("container_id")
        command = payload.get("command", [])

        if not container_id or not command:
            return False, "Missing container_id or command"

        # Create exec instance
        exec_config = {
            "AttachStdout": True,
            "AttachStderr": True,
            "Cmd": command if isinstance(command, list) else [command],
        }

        data, status = await self.docker._request(
            "POST",
            f"/containers/{container_id}/exec",
            json=exec_config
        )

        if status != 201 or not data:
            return False, f"Failed to create exec: {data}"

        exec_id = data.get("Id")

        # Start exec
        start_config = {"Detach": False, "Tty": False}

        session = await self.docker._get_session()
        url = f"{self.docker._base_url}/exec/{exec_id}/start"

        try:
            async with session.post(url, json=start_config) as response:
                if response.status != 200:
                    return False, f"Failed to start exec: {response.status}"
                output = await response.read()
                # Parse Docker stream format
                result = self._parse_exec_output(output)
                return True, result
        except Exception as e:
            return False, str(e)

    def _parse_exec_output(self, raw_data: bytes) -> str:
        """Parse Docker exec output stream."""
        output = []
        offset = 0

        while offset < len(raw_data):
            if offset + 8 > len(raw_data):
                # Try plain text for remainder
                output.append(raw_data[offset:].decode('utf-8', errors='replace'))
                break

            header = raw_data[offset:offset + 8]
            size = int.from_bytes(header[4:8], byteorder='big')

            if size == 0 or offset + 8 + size > len(raw_data):
                # Try plain text for remainder
                output.append(raw_data[offset:].decode('utf-8', errors='replace'))
                break

            payload = raw_data[offset + 8:offset + 8 + size]
            output.append(payload.decode('utf-8', errors='replace'))
            offset += 8 + size

        return ''.join(output).strip()

    async def _execute_get_logs(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """Get container logs."""
        container_id = payload.get("container_id")
        tail = payload.get("tail", 100)

        if not container_id:
            return False, "Missing container_id"

        logs = await self.docker.get_container_logs(
            container_id=container_id,
            container_name="",
            tail=tail,
        )

        if logs:
            messages = [log.get("message", "") for log in logs]
            return True, "\n".join(messages)
        else:
            return True, "No logs available"

    async def _execute_get_env(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """Get container environment variables."""
        container_id = payload.get("container_id")

        if not container_id:
            return False, "Missing container_id"

        # Execute printenv inside container
        success, output = await self._execute_exec({
            "container_id": container_id,
            "command": ["printenv"]
        })

        return success, output

    async def run(self):
        """Main polling loop."""
        self._running = True
        logger.debug("Action poller started", agent_id=self.agent_id, interval=self.poll_interval)

        while self._running:
            try:
                # Poll for actions
                actions = await self.poll_actions()

                for action in actions:
                    action_id = action.get("id")
                    success, output = await self.execute_action(action)
                    await self.send_result(action_id, success, output)

            except Exception as e:
                logger.error("Error in action poller loop", error=str(e))

            await asyncio.sleep(self.poll_interval)

    def stop(self):
        """Stop the polling loop."""
        self._running = False
