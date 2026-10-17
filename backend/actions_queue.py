"""Actions queue for agent communication.

This module manages pending actions that agents poll for and execute.

The queue lives in memory and, once :meth:`ActionsQueue.enable_persistence`
has been called (the API lifespan does it), is mirrored to a JSON file on the
data volume (``<data_dir>/actions_queue.json``) so a backend restart does not
lose queued work:

* pending actions are reloaded and still delivered to their agent, within the
  same TTL measured from their original ``created_at``;
* actions that were ``in_progress`` when the backend stopped are marked
  ``failed`` with a ``reason``: the agent may or may not have run them and the
  HTTP caller that was waiting for them is gone.  A result the agent reports
  later still overwrites that status, so the real outcome wins when it arrives;
* agents' ``last_seen`` is restored, so ``/api/agents`` and the "is an agent
  online for this host" routing keep working through a restart.

Why a JSON file rather than OpenSearch: the queue is tiny and short-lived
(actions expire after a minute and finished ones are dropped after five), it
sits on the agent polling hot path, and it must be usable before OpenSearch is
reachable at startup.  A file on the data volume, like ``pipeline_state.json``,
gives synchronous, read-your-writes persistence with no new dependency.

Single-replica constraint: the file is owned by one backend process.  Running
several backend replicas would give each its own queue (an agent poll served by
replica A never sees an action queued on replica B) and they would overwrite
each other's file on a shared volume.  The stack therefore runs the backend
with ``replicas: 1``.

Action results are deliberately *not* written to disk: ``get_env`` and
``exec`` outputs can carry secrets, and the only consumer of a result is the
in-process ``wait_for_action`` caller, which does not survive a restart anyway.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
from pydantic import BaseModel

from .durable_json import atomic_write_json

logger = structlog.get_logger()

# Bump when the on-disk layout changes incompatibly.
_STORE_VERSION = 1

# Finished (completed/failed/expired) actions are kept this long, then pruned.
DEFAULT_RETENTION_SECONDS = 300

# An agent poll that changes nothing but last_seen is persisted at most this
# often: polls arrive every few seconds per agent, and last_seen only needs to
# be roughly right after a restart (the online window is 30 seconds).
_AGENT_SAVE_INTERVAL_SECONDS = 15.0

# Reasons recorded on actions the backend resolved itself.
REASON_EXPIRED = "Not picked up by the agent before the action timeout"
REASON_TIMED_OUT = "Agent did not report a result before the action timeout"
REASON_RESTART_IN_PROGRESS = (
    "Backend restarted while the agent was executing this action; "
    "its outcome is unknown"
)
REASON_RESTART_EXPIRED = "Expired while the backend was restarting"


class ActionStatus(str, Enum):
    """Action status enum."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


_TERMINAL_STATUSES = (ActionStatus.COMPLETED, ActionStatus.FAILED, ActionStatus.EXPIRED)


class ActionType(str, Enum):
    """Action type enum."""
    CONTAINER_ACTION = "container_action"
    EXEC = "exec"
    GET_LOGS = "get_logs"
    GET_ENV = "get_env"


class Action(BaseModel):
    """Action model."""
    id: str
    agent_id: str
    type: ActionType
    payload: Dict[str, Any]
    status: ActionStatus = ActionStatus.PENDING
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[str] = None
    success: Optional[bool] = None
    # Why the backend (not the agent) resolved the action: expiry or restart.
    reason: Optional[str] = None


class AgentInfo(BaseModel):
    """Agent information tracked via action polling."""
    agent_id: str
    last_seen: datetime
    status: str = "healthy"


class ActionsQueue:
    """Queue for agent actions, optionally mirrored to a JSON file."""

    def __init__(
        self,
        action_timeout_seconds: int = 60,
        retention_seconds: int = DEFAULT_RETENTION_SECONDS,
    ):
        self._actions: Dict[str, Action] = {}
        self._agents: Dict[str, AgentInfo] = {}
        self._lock = asyncio.Lock()
        self._action_timeout = timedelta(seconds=action_timeout_seconds)
        self._retention = timedelta(seconds=retention_seconds)
        self._waiters: Dict[str, asyncio.Event] = {}
        # Persistence (disabled until enable_persistence is called)
        self._path: Optional[Path] = None
        self._last_save_monotonic: float = 0.0

    # ── Persistence ──

    @property
    def persistent(self) -> bool:
        return self._path is not None

    def enable_persistence(self, path: str) -> None:
        """Mirror the queue to *path*, reloading and reconciling what it holds.

        Call once at startup, before agents can poll.  Actions already queued in
        memory (none in practice) are kept and merged with the reloaded ones.
        """
        self._path = Path(path)
        self._load()
        self._reconcile_after_restart(datetime.utcnow())
        self._prune(datetime.utcnow())
        self._save(force=True)

    def _load(self) -> None:
        if not self._path or not self._path.exists():
            logger.info("No actions queue file, starting empty",
                        path=str(self._path))
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            if raw.get("version") != _STORE_VERSION:
                raise ValueError(f"unsupported store version {raw.get('version')!r}")
            actions = [Action.model_validate(a) for a in raw.get("actions", [])]
            agents = [AgentInfo.model_validate(a) for a in raw.get("agents", [])]
        except Exception as e:
            # A corrupt file must not keep the backend from starting: losing the
            # queue is what happened on every restart before it was persisted.
            logger.error("Failed to load actions queue, starting empty",
                         path=str(self._path), error=str(e))
            return
        for action in actions:
            self._actions.setdefault(action.id, action)
        for agent in agents:
            known = self._agents.get(agent.agent_id)
            if not known or known.last_seen < agent.last_seen:
                self._agents[agent.agent_id] = agent
        logger.info("Actions queue loaded", path=str(self._path),
                    actions=len(actions), agents=len(agents))

    def _reconcile_after_restart(self, now: datetime) -> None:
        """Resolve actions the previous process left unfinished."""
        requeued = interrupted = expired = 0
        for action in self._actions.values():
            if action.status == ActionStatus.PENDING:
                if now - action.created_at > self._action_timeout:
                    self._finish(action, ActionStatus.EXPIRED, REASON_RESTART_EXPIRED, now)
                    expired += 1
                else:
                    requeued += 1
            elif action.status == ActionStatus.IN_PROGRESS:
                self._finish(action, ActionStatus.FAILED, REASON_RESTART_IN_PROGRESS, now)
                interrupted += 1
            # Every non-terminal action needs a waiter, and a terminal one keeps
            # wait_for_action returning immediately.
            event = self._waiters.setdefault(action.id, asyncio.Event())
            if action.status in _TERMINAL_STATUSES:
                event.set()
        if requeued or interrupted or expired:
            logger.warning("Actions queue reconciled after restart",
                           requeued=requeued, interrupted=interrupted, expired=expired)

    def _save(self, force: bool = False) -> None:
        """Write the queue to disk (no-op when persistence is disabled).

        *force* is set whenever an action changed; an agent-only change (a
        poll refreshing last_seen) is throttled to _AGENT_SAVE_INTERVAL_SECONDS.
        """
        if not self._path:
            return
        now_mono = time.monotonic()
        if not force and now_mono - self._last_save_monotonic < _AGENT_SAVE_INTERVAL_SECONDS:
            return
        payload = {
            "version": _STORE_VERSION,
            "saved_at": datetime.utcnow().isoformat(),
            "actions": [
                a.model_dump(mode="json", exclude={"result"})
                for a in self._actions.values()
            ],
            "agents": [a.model_dump(mode="json") for a in self._agents.values()],
        }
        try:
            atomic_write_json(self._path, payload)
            self._last_save_monotonic = now_mono
        except Exception as e:
            # Keep serving from memory: a full or read-only volume must not
            # break container actions, it only loses restart durability.
            logger.error("Failed to save actions queue", path=str(self._path), error=str(e))

    # ── Internal helpers (caller holds the lock or runs single-threaded) ──

    @staticmethod
    def _finish(action: Action, status: ActionStatus, reason: str, now: datetime) -> None:
        action.status = status
        action.reason = reason
        action.completed_at = now
        if status != ActionStatus.COMPLETED:
            action.success = False

    def _prune(self, now: datetime) -> int:
        """Drop finished actions older than the retention period."""
        to_remove = [
            action_id for action_id, action in self._actions.items()
            if action.status in _TERMINAL_STATUSES
            and now - (action.completed_at or action.created_at) > self._retention
        ]
        for action_id in to_remove:
            del self._actions[action_id]
            self._waiters.pop(action_id, None)
        return len(to_remove)

    # ── Public API ──

    async def create_action(
        self,
        agent_id: str,
        action_type: ActionType,
        payload: Dict[str, Any],
    ) -> Action:
        """Create a new action for an agent."""
        async with self._lock:
            now = datetime.utcnow()
            action = Action(
                id=str(uuid.uuid4()),
                agent_id=agent_id,
                type=action_type,
                payload=payload,
                status=ActionStatus.PENDING,
                created_at=now,
            )
            self._actions[action.id] = action
            self._waiters[action.id] = asyncio.Event()
            self._prune(now)
            self._save(force=True)

            logger.info(
                "Created action",
                action_id=action.id,
                agent_id=agent_id,
                type=action_type,
            )

            return action

    async def get_pending_actions(self, agent_id: str) -> List[Action]:
        """Get pending actions for an agent and mark them as in_progress."""
        async with self._lock:
            pending = []
            now = datetime.utcnow()
            changed = False

            # Update agent last_seen on every poll (replaces heartbeat)
            self._agents[agent_id] = AgentInfo(
                agent_id=agent_id,
                last_seen=now,
                status="healthy",
            )

            for action in list(self._actions.values()):
                if action.agent_id != agent_id:
                    continue

                # Check for expired actions
                if action.status == ActionStatus.PENDING:
                    changed = True
                    if now - action.created_at > self._action_timeout:
                        self._finish(action, ActionStatus.EXPIRED, REASON_EXPIRED, now)
                        continue

                    # Mark as in_progress and return
                    action.status = ActionStatus.IN_PROGRESS
                    action.started_at = now
                    pending.append(action)

                # Also check in_progress actions for timeout
                elif action.status == ActionStatus.IN_PROGRESS:
                    if action.started_at and now - action.started_at > self._action_timeout:
                        self._finish(action, ActionStatus.EXPIRED, REASON_TIMED_OUT, now)
                        changed = True

            if self._prune(now):
                changed = True
            # Persist before returning: once the agent has the action it must
            # not be handed out again by a restarted backend.
            self._save(force=changed)

            return pending

    async def complete_action(
        self,
        action_id: str,
        success: bool,
        output: str,
        agent_id: Optional[str] = None,
    ) -> Optional[Action]:
        """Mark an action as completed.

        When *agent_id* is given the action must belong to that agent, so an
        agent holding a valid key cannot answer for -- or forge the output of --
        an action queued for one of its peers.

        A result that arrives after the backend resolved the action itself
        (timeout, restart) still replaces that status: it is the real outcome.
        """
        async with self._lock:
            action = self._actions.get(action_id)
            if not action:
                logger.warning("Action not found", action_id=action_id)
                return None

            if agent_id is not None and action.agent_id != agent_id:
                logger.warning(
                    "Action result rejected: it belongs to another agent",
                    action_id=action_id,
                    owner=action.agent_id,
                    reported_by=agent_id,
                )
                return None

            if action.reason:
                logger.info("Late action result supersedes backend resolution",
                            action_id=action_id, previous_status=action.status,
                            previous_reason=action.reason)
            action.status = ActionStatus.COMPLETED if success else ActionStatus.FAILED
            action.completed_at = datetime.utcnow()
            action.success = success
            action.result = output
            action.reason = None
            self._save(force=True)

            # Notify waiters
            if action_id in self._waiters:
                self._waiters[action_id].set()

            logger.info(
                "Action completed",
                action_id=action_id,
                success=success,
            )

            return action

    async def get_action(self, action_id: str) -> Optional[Action]:
        """Get an action by ID."""
        return self._actions.get(action_id)

    async def wait_for_action(
        self,
        action_id: str,
        timeout: float = 30.0,
    ) -> Optional[Action]:
        """Wait for an action to complete."""
        if action_id not in self._waiters:
            return None

        try:
            await asyncio.wait_for(
                self._waiters[action_id].wait(),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            pass

        return self._actions.get(action_id)

    async def get_agents(self) -> List[AgentInfo]:
        """Get all known agents."""
        return list(self._agents.values())

    async def get_agent(self, agent_id: str) -> Optional[AgentInfo]:
        """Get agent info."""
        return self._agents.get(agent_id)

    async def is_agent_online(self, agent_id: str, timeout_seconds: int = 30) -> bool:
        """Check if an agent is online (recent action poll)."""
        agent = self._agents.get(agent_id)
        if not agent:
            return False

        return datetime.utcnow() - agent.last_seen < timedelta(seconds=timeout_seconds)

    async def cleanup_old_actions(self, max_age_seconds: Optional[int] = None):
        """Remove old completed/failed/expired actions.

        Pruning also happens on every create and poll, so calling this is only
        needed to apply a different *max_age_seconds* than the retention.
        """
        async with self._lock:
            previous = self._retention
            if max_age_seconds is not None:
                self._retention = timedelta(seconds=max_age_seconds)
            try:
                removed = self._prune(datetime.utcnow())
            finally:
                self._retention = previous
            if removed:
                self._save(force=True)
                logger.debug("Cleaned up old actions", count=removed)


# Global queue instance (persistence is enabled by the API lifespan)
actions_queue = ActionsQueue()
