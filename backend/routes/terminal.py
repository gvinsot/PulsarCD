"""Interactive terminal WebSocket (/api/terminal/ws) on the swarm manager.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

import asyncio
import json
import uuid

import jwt
import structlog
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from ..auth import decode_token
from ..auth_middleware import _token_is_revoked
from ..config import find_swarm_manager
from .. import api

logger = structlog.get_logger()

router = APIRouter()


# ============== Terminal WebSocket ==============

def _find_swarm_manager_config():
    """Find the swarm manager host config the terminal opens a shell on.

    This used to fall back to the first configured host whenever none was
    flagged swarm_manager, which with several hosts opens an admin shell on
    whichever one happens to be listed first. The fallback is kept only where
    it cannot pick the wrong machine: a single-host setup that never flags it.
    """
    return find_swarm_manager(api.settings.hosts, fallback_to_only_host=True)


@router.websocket("/api/terminal/ws")
async def terminal_websocket(
    websocket: WebSocket,
    cols: int = Query(default=80),
    rows: int = Query(default=24),
    token: str = Query(default=""),
):
    """Interactive terminal session on the swarm manager host."""
    # Validate JWT token before accepting the WebSocket
    try:
        if not token:
            await websocket.close(code=4001, reason="Missing token")
            return
        payload = decode_token(token, api.settings.auth.jwt_secret)
        if _token_is_revoked(payload):
            logger.warning("Terminal WebSocket rejected: revoked token",
                           user=payload.get("sub", ""))
            await websocket.close(code=4001, reason="Token revoked")
            return
        if payload.get("role") != "admin":
            await websocket.close(code=4003, reason="Admin access required")
            return
    except jwt.PyJWTError as e:
        logger.warning("Terminal WebSocket auth failed", error=str(e))
        await websocket.close(code=4001, reason="Invalid token")
        return

    await websocket.accept()

    manager_config = _find_swarm_manager_config()
    if not manager_config:
        if api.settings.hosts:
            message = "Error: No swarm manager host configured (set swarm_manager on one host)."
        else:
            message = "Error: No host configured."
        await websocket.send_text(f"\x1b[31m{message}\x1b[0m\r\n")
        await websocket.close()
        return

    session_id = str(uuid.uuid4())[:8]
    logger.info("Terminal session starting", session=session_id, host=manager_config.name, mode=manager_config.mode)

    try:
        if manager_config.mode == "ssh":
            await _handle_ssh_terminal(websocket, manager_config, cols, rows, session_id)
        else:
            await _handle_local_terminal(websocket, cols, rows, session_id)
    except WebSocketDisconnect:
        logger.info("Terminal session disconnected", session=session_id)
    except Exception as e:
        logger.error("Terminal session error", session=session_id, error=str(e))
        try:
            await websocket.send_text(f"\r\n\x1b[31mSession error: {e}\x1b[0m\r\n")
        except Exception:
            pass
    finally:
        logger.info("Terminal session ended", session=session_id)


async def _handle_ssh_terminal(websocket, host_config, cols, rows, session_id):
    """Interactive SSH terminal session via asyncssh PTY."""
    import asyncssh
    from pathlib import Path
    from ..ssh_client import resolve_known_hosts, save_host_key_if_needed

    # An unknown or unverifiable host raises UnknownHostKeyError here. It is left
    # to propagate on purpose: the caller already relays str(e) to the terminal,
    # and that message names the known_hosts file and the ssh-keyscan command to
    # run, which is exactly what the operator needs.
    known_hosts_setting, save_key = resolve_known_hosts(
        host_config.ssh_known_hosts_path,
        host_config.hostname,
        host_config.port,
    )
    options = {
        "host": host_config.hostname,
        "port": host_config.port,
        "username": host_config.username,
        "known_hosts": known_hosts_setting,
    }
    if host_config.ssh_key_path:
        key_path = Path(host_config.ssh_key_path).expanduser()
        options["client_keys"] = [str(key_path)]

    async with asyncssh.connect(**options) as conn:
        save_host_key_if_needed(conn, save_key, host_config.hostname, host_config.port)
        process = await conn.create_process(
            term_type="xterm-256color",
            term_size=(cols, rows),
            encoding=None,
        )

        logger.info("SSH PTY session opened", session=session_id, host=host_config.hostname)

        async def _read_stdout():
            try:
                while True:
                    data = await process.stdout.read(4096)
                    if not data:
                        break
                    await websocket.send_bytes(data)
            except Exception:
                pass

        async def _read_stderr():
            try:
                while True:
                    data = await process.stderr.read(4096)
                    if not data:
                        break
                    await websocket.send_bytes(data)
            except Exception:
                pass

        read_task = asyncio.create_task(_read_stdout())
        stderr_task = asyncio.create_task(_read_stderr())

        try:
            while True:
                message = await websocket.receive()

                if message["type"] == "websocket.disconnect":
                    break

                if "text" in message:
                    text = message["text"]
                    if text.startswith("{"):
                        try:
                            cmd = json.loads(text)
                            if cmd.get("type") == "resize":
                                process.change_terminal_size(cmd.get("cols", 80), cmd.get("rows", 24))
                                continue
                        except json.JSONDecodeError:
                            pass
                    process.stdin.write(text.encode("utf-8"))
                elif "bytes" in message:
                    process.stdin.write(message["bytes"])
        finally:
            read_task.cancel()
            stderr_task.cancel()
            process.close()
            try:
                await process.wait_closed()
            except Exception:
                pass


async def _handle_local_terminal(websocket, cols, rows, session_id):
    """Interactive local terminal session via PTY."""
    import pty
    import os
    import fcntl
    import struct
    import termios
    import signal

    master_fd, slave_fd = pty.openpty()

    winsize = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)

    if api.settings.run_user:
        shell_cmd = ["/bin/su", "-", api.settings.run_user]
    else:
        shell_cmd = ["/bin/bash", "--login"]

    process = await asyncio.create_subprocess_exec(
        *shell_cmd,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        preexec_fn=os.setsid,
        env={
            **os.environ,
            "TERM": "xterm-256color",
            "COLUMNS": str(cols),
            "LINES": str(rows),
        },
    )

    os.close(slave_fd)

    logger.info("Local PTY session opened", session=session_id, pid=process.pid)

    loop = asyncio.get_event_loop()

    async def _read_pty_output():
        try:
            while True:
                data = await loop.run_in_executor(None, os.read, master_fd, 4096)
                if not data:
                    break
                await websocket.send_bytes(data)
        except OSError:
            pass

    read_task = asyncio.create_task(_read_pty_output())

    try:
        while True:
            message = await websocket.receive()

            if message["type"] == "websocket.disconnect":
                break

            if "text" in message:
                text = message["text"]
                if text.startswith("{"):
                    try:
                        cmd = json.loads(text)
                        if cmd.get("type") == "resize":
                            winsize = struct.pack("HHHH", cmd.get("rows", 24), cmd.get("cols", 80), 0, 0)
                            fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)
                            os.kill(process.pid, signal.SIGWINCH)
                            continue
                    except json.JSONDecodeError:
                        pass
                os.write(master_fd, text.encode("utf-8"))
            elif "bytes" in message:
                os.write(master_fd, message["bytes"])
    finally:
        read_task.cancel()
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5)
        except Exception:
            process.kill()
        try:
            os.close(master_fd)
        except Exception:
            pass
