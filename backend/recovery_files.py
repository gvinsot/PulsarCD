"""Small stdlib-only helper executed on the build host over a private stdin.

Input/output are JSON; file bytes are base64 so trailing newlines are preserved.
Never emit file content or raw errors on stderr. No shell evaluates file content.
"""

import base64
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys
import tempfile

MAX_SIZE = 4 * 1024 * 1024


def safe_path(root, resource):
    relative = PurePosixPath(resource)
    if (not resource or relative.is_absolute() or ".." in relative.parts
            or "\\" in resource or ":" in resource):
        raise ValueError("Invalid relative path")
    root = Path(root).expanduser().resolve()
    target = root.joinpath(*relative.parts)
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("Symlink refused")
    if not target.resolve().is_relative_to(root):
        raise ValueError("Path outside root")
    return target


def read_file(path):
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_SIZE:
            raise ValueError("Invalid file")
        content = stream.read(MAX_SIZE + 1)
        if len(content) > MAX_SIZE:
            raise ValueError("File too large")
        return content


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".pulsarcd-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.chmod(temporary, 0o600)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def handle(request):
    root = Path(request["root"]).expanduser().resolve()
    operation = request["operation"]
    if operation == "list":
        if not root.is_dir():
            raise ValueError("Root missing")
        result = []
        def failed(error):
            raise error
        for directory, dirs, files in os.walk(root, onerror=failed, followlinks=False):
            dirs[:] = [name for name in dirs if name not in (
                ".git", "node_modules", ".venv", "venv", "__pycache__")
                and not (Path(directory) / name).is_symlink()]
            for name in files:
                if name == ".env":
                    relative = (Path(directory) / name).relative_to(root).as_posix()
                    safe_path(root, relative)
                    result.append(relative)
        return {"files": sorted(result)}
    path = safe_path(root, request["resource"])
    if operation == "read":
        content = read_file(path)
        return {"content": None if content is None else base64.b64encode(content).decode()}
    if operation == "write":
        content = base64.b64decode(request["content"], validate=True)
        if len(content) > MAX_SIZE:
            raise ValueError("File too large")
        # Refuse to silently overwrite a manual edit made since the backup read.
        previous = read_file(path)
        expected = request["expected"]
        expected = None if expected is None else base64.b64decode(expected, validate=True)
        if previous != expected:
            raise ValueError("File changed concurrently")
        atomic_write(path, content)
        return {"written": True}
    raise ValueError("Unknown operation")


if __name__ == "__main__":
    try:
        print(json.dumps(handle(json.load(sys.stdin))))
    except Exception:
        print('{"error":"Recovery file operation failed (path, permissions or concurrent change)"}')
        sys.exit(1)
