"""Persistent pipeline state singleton for PulsarCD.

Tracks each pipeline step's state, desired version, current version,
and last log per repository. State is persisted to a JSON file on the
data volume alongside settings.
"""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()

# Maximum log lines stored per stage (persisted to disk for post-restart access)
_MAX_LOG_LINES = 500

# Sentinel for set_pipeline's optional IDs
_UNSET = object()


class StageState:
    """State for a single pipeline stage (build / test / deploy)."""

    __slots__ = (
        "status", "desired_version", "current_version",
        "last_log", "action_id", "updated_at",
    )

    def __init__(
        self,
        status: str = "idle",
        desired_version: Optional[str] = None,
        current_version: Optional[str] = None,
        last_log: Optional[List[str]] = None,
        action_id: Optional[str] = None,
        updated_at: Optional[str] = None,
    ):
        self.status = status
        self.desired_version = desired_version
        self.current_version = current_version
        self.last_log = last_log or []
        self.action_id = action_id
        self.updated_at = updated_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "desired_version": self.desired_version,
            "current_version": self.current_version,
            "last_log": self.last_log,
            "action_id": self.action_id,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StageState":
        return cls(
            status=data.get("status", "idle"),
            desired_version=data.get("desired_version"),
            current_version=data.get("current_version"),
            last_log=data.get("last_log", []),
            action_id=data.get("action_id"),
            updated_at=data.get("updated_at"),
        )


def _migrate_swiftproof(configs: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Move SwiftProof settings saved under the old test_to_deploy transition.

    The review used to gate deployment; it now belongs to the Test stage, so
    the settings live on build_to_test. Projects configured before the move
    keep their choices instead of silently losing the review.
    """
    legacy = configs.get("test_to_deploy") or {}
    moved = {k: legacy.pop(k) for k in list(legacy) if k.startswith("swiftproof_")}
    if moved:
        target = configs.setdefault("build_to_test", {"mode": "auto_with_success"})
        for key, value in moved.items():
            target.setdefault(key, value)
    return configs


class GateDecision:
    """Record of a single gate evaluation."""

    __slots__ = ("transition", "approved", "reason", "timestamp", "version")

    def __init__(
        self,
        transition: str,
        approved: bool,
        reason: str,
        timestamp: Optional[str] = None,
        version: Optional[str] = None,
    ):
        self.transition = transition
        self.approved = approved
        self.reason = reason
        self.timestamp = timestamp or datetime.now(timezone.utc).isoformat()
        self.version = version

    def to_dict(self) -> dict:
        return {
            "transition": self.transition,
            "approved": self.approved,
            "reason": self.reason,
            "timestamp": self.timestamp,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GateDecision":
        return cls(
            transition=data.get("transition", ""),
            approved=data.get("approved", True),
            reason=data.get("reason", ""),
            timestamp=data.get("timestamp"),
            version=data.get("version"),
        )


# Maximum gate decisions kept per pipeline entry
_MAX_GATE_DECISIONS = 10


class PipelineEntry:
    """Pipeline state for a single repository."""

    # The "qa" stage is optional and only used when the test_to_deploy transition
    # has qa_enabled=true. It always exists in state but stays idle when disabled.
    STAGES = ("build", "test", "qa", "deploy")

    def __init__(self):
        self.stages: Dict[str, StageState] = {
            stage: StageState() for stage in self.STAGES
        }
        # Overall pipeline info
        self.current_stage: Optional[str] = None
        self.overall_status: str = "idle"  # idle / running / success / failed / gate_rejected
        self.skip_build: bool = False
        # Project / stack metadata
        self.project_name: Optional[str] = None
        self.stack_name: Optional[str] = None
        # Gate decisions log (most recent last)
        self.gates: List[GateDecision] = []
        # Timestamp of the last successful deployment
        self.last_deployed_at: Optional[str] = None
        # Per-project transition configs: {"build_to_test": {"mode": "...", ...}, "test_to_deploy": {...}}
        # mode: "auto" (no gate), "auto_with_success" (auto if prev succeeded), "agent" (LLM gate), "manual"
        self.transition_configs: Dict[str, Dict[str, Any]] = {}
        self.swiftproof: Dict[str, Any] = {}
        self.swiftproof_revision: int = 0

    def to_dict(self) -> dict:
        return {
            "stages": {name: s.to_dict() for name, s in self.stages.items()},
            "current_stage": self.current_stage,
            "overall_status": self.overall_status,
            "skip_build": self.skip_build,
            "project_name": self.project_name,
            "stack_name": self.stack_name,
            "gates": [g.to_dict() for g in self.gates],
            "last_deployed_at": self.last_deployed_at,
            "transition_configs": self.transition_configs,
            "swiftproof": self.swiftproof,
            "swiftproof_revision": self.swiftproof_revision,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineEntry":
        entry = cls()
        stages_data = data.get("stages", {})
        for stage_name in cls.STAGES:
            if stage_name in stages_data:
                entry.stages[stage_name] = StageState.from_dict(stages_data[stage_name])
        entry.current_stage = data.get("current_stage")
        entry.overall_status = data.get("overall_status", "idle")
        entry.skip_build = data.get("skip_build", False)
        entry.project_name = data.get("project_name")
        entry.stack_name = data.get("stack_name")
        entry.gates = [
            GateDecision.from_dict(g) for g in data.get("gates", [])
        ]
        entry.last_deployed_at = data.get("last_deployed_at")
        entry.transition_configs = _migrate_swiftproof(data.get("transition_configs", {}))
        entry.swiftproof = data.get("swiftproof", {})
        entry.swiftproof_revision = data.get("swiftproof_revision", 0)
        return entry

    # ── Convenience for backward-compatible API response ──

    def to_legacy_dict(self) -> dict:
        """Return a dict compatible with the old _pipeline_state format."""
        # Find version: use the current stage's desired_version first,
        # then fall back to the most recent stage with a desired_version.
        version = None
        if self.current_stage and self.current_stage in self.stages:
            version = self.stages[self.current_stage].desired_version
        if not version:
            for stage_name in reversed(self.STAGES):
                s = self.stages[stage_name]
                if s.desired_version:
                    version = s.desired_version
                    break

        return {
            "stage": self.current_stage or "idle",
            "status": self.overall_status,
            "version": version,
            "deployed_version": self.stages["deploy"].current_version,
            "build_action_id": self.stages["build"].action_id,
            "test_action_id": self.stages["test"].action_id,
            "qa_action_id": self.stages["qa"].action_id,
            "deploy_action_id": self.stages["deploy"].action_id,
            "skip_build": self.skip_build,
            "project_name": self.project_name,
            "stack_name": self.stack_name,
            "gates": [g.to_dict() for g in self.gates],
            "last_deployed_at": self.last_deployed_at,
            "transition_configs": self.transition_configs,
            "swiftproof": self.swiftproof,
            # Enriched per-stage data. ``last_log`` is deliberately left out:
            # the UI polls this every couple of seconds and only reads statuses
            # and versions, while the log tail would make the payload change on
            # every poll of a running build (forcing a full re-render) and add
            # up to _MAX_LOG_LINES lines per stage to it. Logs are served by
            # the dedicated /actions/{id}/logs endpoints.
            "stages": {
                name: {k: v for k, v in s.to_dict().items() if k != "last_log"}
                for name, s in self.stages.items()
            },
        }


class PipelineStateManager:
    """Singleton managing persistent pipeline state for all repos."""

    _instance: Optional["PipelineStateManager"] = None
    _lock = threading.Lock()

    def __init__(self, data_dir: str = "/data"):
        self._path = Path(data_dir) / "pipeline_state.json"
        self._state: Dict[str, PipelineEntry] = {}
        self._load()

    @classmethod
    def get_instance(cls, data_dir: str = "/data") -> "PipelineStateManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(data_dir)
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """Reset singleton (for tests)."""
        cls._instance = None

    # ── Persistence ──

    def _load(self):
        if not self._path.exists():
            logger.info("No pipeline state file, starting fresh", path=str(self._path))
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            for repo_name, entry_data in raw.items():
                self._state[repo_name] = PipelineEntry.from_dict(entry_data)
            logger.info("Pipeline state loaded", path=str(self._path), repos=len(self._state))
        except Exception as e:
            logger.error("Failed to load pipeline state, starting fresh",
                         path=str(self._path), error=str(e))
            self._state = {}

    def _save(self):
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = {repo: entry.to_dict() for repo, entry in self._state.items()}
            self._path.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as e:
            logger.error("Failed to save pipeline state", path=str(self._path), error=str(e))

    # ── Public API ──

    def get(self, repo_name: str) -> Optional[PipelineEntry]:
        return self._state.get(repo_name)

    def get_or_create(self, repo_name: str) -> PipelineEntry:
        if repo_name not in self._state:
            self._state[repo_name] = PipelineEntry()
        return self._state[repo_name]

    def set_stage(
        self,
        repo_name: str,
        stage: str,
        status: str,
        version: str,
        action_id: Optional[str] = None,
        log_lines: Optional[List[str]] = None,
    ):
        """Update a specific stage and the overall pipeline status.

        Args:
            repo_name: Repository name
            stage: Pipeline stage (build / test / deploy / done)
            status: Stage status (running / success / failed / gate_rejected)
            version: Desired version for this pipeline run
            action_id: Optional action ID for this stage
            log_lines: Optional last log lines for this stage
        """
        entry = self.get_or_create(repo_name)
        now = datetime.now(timezone.utc).isoformat()

        if stage == "done":
            # Pipeline completed successfully — mark deploy as success
            entry.current_stage = "done"
            entry.overall_status = "success"
            entry.last_deployed_at = now
            deploy_s = entry.stages["deploy"]
            deploy_s.status = "success"
            deploy_s.current_version = version
            deploy_s.updated_at = now
            if action_id is not None:
                deploy_s.action_id = action_id
            if log_lines is not None:
                deploy_s.last_log = log_lines[-_MAX_LOG_LINES:]
        elif stage in PipelineEntry.STAGES:
            stage_obj = entry.stages[stage]
            stage_obj.status = status
            stage_obj.desired_version = version
            stage_obj.updated_at = now
            if action_id is not None:
                stage_obj.action_id = action_id
            if log_lines is not None:
                stage_obj.last_log = log_lines[-_MAX_LOG_LINES:]
            if status == "success":
                stage_obj.current_version = version

            entry.current_stage = stage
            entry.overall_status = status

        self._save()

    def set_pipeline(
        self,
        repo_name: str,
        stage: str,
        status: str,
        version: str,
        build_id=_UNSET,
        test_id=_UNSET,
        qa_id=_UNSET,
        deploy_id=_UNSET,
    ):
        """Backward-compatible wrapper matching the old _set_pipeline signature.

        Uses a sentinel to distinguish 'not provided' from 'explicitly set to None'.
        """
        entry = self.get_or_create(repo_name)
        now = datetime.now(timezone.utc).isoformat()

        # Update action IDs only if explicitly provided
        if build_id is not _UNSET:
            entry.stages["build"].action_id = build_id
        if test_id is not _UNSET:
            entry.stages["test"].action_id = test_id
        if qa_id is not _UNSET:
            entry.stages["qa"].action_id = qa_id
        if deploy_id is not _UNSET:
            entry.stages["deploy"].action_id = deploy_id

        # Update versions on relevant stages
        if stage in PipelineEntry.STAGES:
            entry.stages[stage].desired_version = version
            entry.stages[stage].status = status
            entry.stages[stage].updated_at = now
            if status == "success":
                entry.stages[stage].current_version = version
            # When a stage starts running, set desired_version on all stages
            # so the version badge always reflects the current pipeline run
            if status == "running":
                for s in PipelineEntry.STAGES:
                    entry.stages[s].desired_version = version
        elif stage == "done":
            entry.stages["deploy"].current_version = version
            entry.stages["deploy"].status = "success"
            entry.stages["deploy"].updated_at = now
            entry.last_deployed_at = now

        entry.current_stage = stage
        entry.overall_status = status

        self._save()

    def update_log(self, repo_name: str, stage: str, log_lines: List[str]):
        """Append/replace last log for a stage."""
        entry = self.get(repo_name)
        if entry and stage in entry.stages:
            entry.stages[stage].last_log = log_lines[-_MAX_LOG_LINES:]
            self._save()

    def set_skip_build(self, repo_name: str, skip: bool):
        entry = self.get_or_create(repo_name)
        entry.skip_build = skip
        self._save()

    def set_project_info(self, repo_name: str, project_name: str, stack_name: str):
        """Set project and stack name metadata for a pipeline entry."""
        entry = self.get_or_create(repo_name)
        entry.project_name = project_name
        entry.stack_name = stack_name
        self._save()

    def record_gate(self, repo_name: str, transition: str, approved: bool, reason: str, version: Optional[str] = None):
        """Record a gate decision (LLM reasoning) for a pipeline entry."""
        entry = self.get(repo_name)
        if not entry:
            return
        decision = GateDecision(transition=transition, approved=approved, reason=reason, version=version)
        entry.gates.append(decision)
        # Keep only the most recent decisions
        if len(entry.gates) > _MAX_GATE_DECISIONS:
            entry.gates = entry.gates[-_MAX_GATE_DECISIONS:]
        self._save()

    def clear_gates(self, repo_name: str):
        """Clear gate decisions when starting a new pipeline run."""
        entry = self.get(repo_name)
        if entry:
            entry.gates = []
            self._save()

    def update_version(self, repo_name: str, stage: str, version: str):
        """Update the version for a stage (e.g., after extracting from build output)."""
        entry = self.get(repo_name)
        if entry and stage in entry.stages:
            entry.stages[stage].desired_version = version
            self._save()

    def set_transition_config(self, repo_name: str, transition: str, config: Dict[str, Any]):
        """Set per-project transition configuration.

        Args:
            repo_name: Repository name
            transition: "version_to_build", "build_to_test" or "test_to_deploy"
            config: {"mode": "auto"|"auto_with_success"|"agent"|"manual",
                     "swiftproof_enabled", "swiftproof_reviewer",
                     "swiftproof_initial_baseline" (build_to_test only),
                     "qa_enabled": bool (test_to_deploy only),
                     "multi_arch": bool (version_to_build only),
                     "platforms": str (version_to_build only, e.g. "linux/amd64,linux/arm64")}
        """
        valid_transitions = {"version_to_build", "build_to_test", "test_to_deploy"}
        valid_modes = {"auto", "auto_with_success", "agent", "manual"}
        if transition not in valid_transitions:
            return
        mode = config.get("mode", "auto_with_success")
        if mode not in valid_modes:
            return
        entry = self.get_or_create(repo_name)
        new_config: Dict[str, Any] = {"mode": mode}
        # test_to_deploy supports an optional QA-pre-deploy step
        if transition == "test_to_deploy":
            new_config["qa_enabled"] = bool(config.get("qa_enabled", False))
        # build_to_test carries the SwiftProof review, which runs at the end of
        # the Test stage. Settings absent from the payload keep their value so
        # that changing only the mode cannot silently disable the review.
        if transition == "build_to_test":
            previous = entry.transition_configs.get(transition, {})
            for key, default in (("swiftproof_enabled", False), ("swiftproof_reviewer", True), ("swiftproof_initial_baseline", "")):
                new_config[key] = config.get(key, previous.get(key, default))
        # version_to_build supports an optional multi-arch build (uses the
        # heavier docker-container buildx driver, with QEMU emulation when
        # cross-building). Single-arch builds use the host docker engine.
        if transition == "version_to_build":
            new_config["multi_arch"] = bool(config.get("multi_arch", False))
            platforms = config.get("platforms")
            if platforms and isinstance(platforms, str):
                new_config["platforms"] = platforms.strip()
        entry.transition_configs[transition] = new_config
        self._save()

    def get_transition_config(self, repo_name: str, transition: str) -> Dict[str, Any]:
        """Get per-project transition config, or empty dict if not set."""
        entry = self.get(repo_name)
        if entry and transition in entry.transition_configs:
            return entry.transition_configs[transition]
        return {}

    def get_all_legacy(self) -> Dict[str, dict]:
        """Return all pipelines in legacy format for the API."""
        return {repo: entry.to_legacy_dict() for repo, entry in self._state.items()}

    def get_legacy(self, repo_name: str) -> dict:
        """Return a single pipeline in legacy format."""
        entry = self._state.get(repo_name)
        if entry:
            return entry.to_legacy_dict()
        return {}

    def reset(self, repo_name: str):
        """Reset pipeline state for a repo (e.g. after stack removal).

        Clears all stage statuses, versions, logs, and gates so the
        stack appears as undeployed in the UI.
        """
        if repo_name in self._state:
            self._state[repo_name] = PipelineEntry()
            self._save()
            logger.info("Pipeline state reset", repo=repo_name)

    def find_repo_by_stack(self, stack_name: str) -> Optional[str]:
        """Reverse-lookup: find repo_name from a Docker stack name."""
        for repo_name, entry in self._state.items():
            if entry.stack_name and entry.stack_name.lower() == stack_name.lower():
                return repo_name
        return None

    def items(self):
        return self._state.items()

    def __contains__(self, repo_name: str) -> bool:
        return repo_name in self._state
