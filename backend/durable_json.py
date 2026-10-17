"""Crash-safe JSON files on the data volume.

A plain ``Path.write_text`` truncates the file before writing it, so a backend
killed mid-write (swarm rolling update, OOM, host reboot) leaves an empty or
half-written file behind, and the next start silently loses the whole state.
Writing to a sibling temporary file and swapping it in with ``os.replace``
makes every save all-or-nothing: readers see either the old or the new file.
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def atomic_write_json(path: Path, payload: Any, indent: int = 2) -> None:
    """Serialize *payload* to *path* atomically.

    The temporary file lives in the same directory as *path* so the final
    ``os.replace`` is a rename on one filesystem (atomic on POSIX and on
    Windows).  Errors propagate: callers decide whether a failed save is fatal.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=indent, ensure_ascii=False)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
