"""In-memory record of a background build, test or deploy action.

The registry of running and recent actions (``_background_actions``) is app
state and lives in backend.api.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional


class BackgroundAction:
    """Tracks a background build or deploy action."""
    
    def __init__(self, action_id: str, action_type: str, repo_name: str):
        self.id = action_id
        self.action_type = action_type  # "build" or "deploy"
        self.repo_name = repo_name
        self._status = "running"  # running, completed, failed, cancelled
        self.output_lines: List[str] = []
        self.result: Optional[Dict[str, Any]] = None
        self.started_at = datetime.utcnow()
        self.cancel_event = asyncio.Event()
        self.task: Optional[asyncio.Task] = None
        self.new_line_event = asyncio.Event()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        self.new_line_event.set()

    def append_output(self, line: str):
        self.output_lines.append(line)
        self.new_line_event.set()
    
    def get_output(self) -> str:
        return "\n".join(self.output_lines)
