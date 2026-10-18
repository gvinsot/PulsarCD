"""Per-branch preview environments.

A preview is a repository's stack built from one branch and deployed beside
production and QA, with its own stack name, hosts and Traefik routers (see
shared/preview_naming.py for the names, and the ``--preview`` mode of
scripts/deploy-service.sh for how it is isolated). This module owns:

* :class:`PreviewStore` -- the persistent record of the previews PulsarCD
  deployed (``<data_dir>/previews.json``). Only a recorded preview can be torn
  down through the preview API, so an unrelated stack whose name happens to
  start with ``pv-`` is never removed by it.
* :class:`PreviewManager` -- deploy (build the branch under a branch-derived
  image tag, then deploy it with ``--preview``), list, tear down, and expire
  previews that were not redeployed within the configured TTL.

Every Docker command runs through :class:`StackDeployer`, the same execution
path as regular builds and deploys.

Safety: a preview can never overwrite or remove a production or QA stack.

* Names are derived, never accepted from the client, and checked by
  :func:`shared.preview_naming.assert_preview_stack` before any deploy or
  ``docker stack rm``.
* A deploy is refused when its stack already runs but is not a recorded
  preview of the same repository and branch (e.g. a production repository
  that happens to be called ``pv-...``).
* A teardown only accepts a stack recorded in the store, and re-checks it.
* A running stack is only redeployed or removed while it runs the preview's
  image tag, so a production stack that took over a ``pv-`` name is safe.
* The deploy script refuses any preview stack name not starting with ``pv-``.
"""

import asyncio
import json
import os
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import structlog

from shared.preview_naming import (
    PREVIEW_STACK_PREFIX,
    PreviewNames,
    assert_preview_stack,
    is_preview_stack,
    preview_names,
    repo_stack_name,
)

from .config import PreviewConfig

logger = structlog.get_logger()

PREVIEWS_FILE = "previews.json"


class PreviewError(Exception):
    """A preview request that cannot be honoured.

    ``status_code`` follows HTTP semantics so the API layer can forward it:
    400 invalid input, 404 unknown preview, 409 conflict or limit reached.
    """

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class PreviewStore:
    """JSON-file record of previews, keyed by stack name.

    Writes go through a temporary file and ``os.replace`` so a crash never
    leaves a truncated file behind. A missing or unreadable file reads as empty.
    """

    def __init__(self, path: str):
        self._path = Path(path)

    def _load(self) -> Dict[str, Dict[str, Any]]:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as e:
            logger.warning("Preview store unreadable, starting empty",
                           path=str(self._path), error=str(e))
            return {}
        if not isinstance(data, dict):
            return {}
        return {k: v for k, v in data.items() if isinstance(v, dict) and is_preview_stack(k)}

    def _save(self, records: Dict[str, Dict[str, Any]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(records, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self._path)

    def all(self) -> List[Dict[str, Any]]:
        return list(self._load().values())

    def get(self, stack: str) -> Optional[Dict[str, Any]]:
        return self._load().get(stack)

    def upsert(self, stack: str, **fields: Any) -> Dict[str, Any]:
        records = self._load()
        record = records.get(stack, {"stack": stack})
        record.update(fields)
        records[stack] = record
        self._save(records)
        return record

    def remove(self, stack: str) -> None:
        records = self._load()
        if records.pop(stack, None) is not None:
            self._save(records)


class PreviewManager:
    """Deploy, list, tear down and expire preview environments.

    Args:
        store: Persistent preview records.
        config: Preview settings (TTL, limits).
        deployer_factory: Returns the :class:`StackDeployer` used for every
            build, deploy and Docker command (the regular deploy path).
    """

    def __init__(self, store: PreviewStore, config: PreviewConfig,
                 deployer_factory: Callable[[], Any]):
        self.store = store
        self.config = config
        self._deployer_factory = deployer_factory
        self._busy: set = set()
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Naming and checks
    # ------------------------------------------------------------------

    @staticmethod
    def plan(repo_name: str, branch: str) -> PreviewNames:
        """Names of the preview of ``branch`` for ``repo_name`` (no side effect)."""
        from .github_service import _validate_repo_name
        try:
            _validate_repo_name(repo_name)
            return preview_names(branch, repo_stack_name(repo_name))
        except ValueError as e:
            raise PreviewError(str(e), 400) from e

    async def live_preview_stacks(self) -> Optional[List[str]]:
        """Names of running stacks that look like previews, or None if unknown."""
        deployer = self._deployer_factory()
        ok, output = await deployer._run_command("docker stack ls --format '{{.Name}}'")
        if not ok:
            logger.warning("Could not list stacks for previews", output=(output or "")[:200])
            return None
        return [line.strip() for line in output.splitlines()
                if line.strip().startswith(PREVIEW_STACK_PREFIX)]

    @staticmethod
    def _image_has_tag(image: str, image_tag: str) -> bool:
        """True when ``image`` (``repo[:tag][@digest]``) carries the preview tag.

        Variant images are published as ``<variant>-<tag>`` by build-push.sh.
        """
        ref = image.split("@", 1)[0]
        name = ref.rsplit("/", 1)[-1]
        if ":" not in name:
            return False
        tag = name.rsplit(":", 1)[1]
        return tag == image_tag or tag.endswith(f"-{image_tag}")

    async def check_stack_is_preview(self, stack: str, image_tag: str) -> None:
        """Refuse to touch a running stack that does not run the preview image.

        The stack name alone cannot prove ownership: a production repository
        may literally be called ``pv-<x>-<repo>`` and be deployed over a
        recorded preview after the fact. A running preview always runs at
        least one image tagged with its preview tag; a stack that runs none is
        not (or no longer) the preview and is never redeployed or removed.

        Raises:
            PreviewError: 409 when the stack runs no preview image, 503 when
                its services cannot be listed.
        """
        deployer = self._deployer_factory()
        ok, output = await deployer._run_command(
            "docker service ls --filter "
            f"{shlex.quote('label=com.docker.stack.namespace=' + stack)} "
            "--format '{{.Image}}'"
        )
        if not ok:
            raise PreviewError(
                f"Cannot inspect the services of {stack}; refusing to touch it", 503)
        images = [line.strip() for line in (output or "").splitlines() if line.strip()]
        if images and not any(self._image_has_tag(img, image_tag) for img in images):
            raise PreviewError(
                f"Stack {stack} runs no image tagged {image_tag}: it is not the "
                "preview PulsarCD deployed (a production stack may use this name); "
                "refusing to touch it", 409)

    async def check_can_deploy(self, repo_name: str, names: PreviewNames) -> None:
        """Refuse a deploy that could touch anything but this exact preview.

        Raises:
            PreviewError: 409 when the stack is busy, belongs to another
                repository/branch, runs without being a recorded preview, or
                the preview limit is reached; 503 when running stacks cannot
                be listed (the collision check must not be skipped).
        """
        try:
            assert_preview_stack(names.stack, repo_stack_name(repo_name))
        except ValueError as e:
            raise PreviewError(str(e), 400) from e
        if names.stack in self._busy:
            raise PreviewError(f"Preview {names.stack} is already being deployed or removed", 409)

        record = self.store.get(names.stack)
        if record and (record.get("repo") != repo_name or record.get("branch") != names.branch):
            raise PreviewError(
                f"Stack {names.stack} already belongs to the preview of "
                f"{record.get('repo')}@{record.get('branch')}", 409)
        if not record and len(self.store.all()) >= self.config.max_active:
            raise PreviewError(
                f"Preview limit reached ({self.config.max_active}); tear one down first", 409)

        live = await self.live_preview_stacks()
        if live is None:
            raise PreviewError("Cannot list running stacks; refusing to deploy a preview blindly", 503)
        if names.stack in live and not record:
            raise PreviewError(
                f"Stack {names.stack} is running but is not a preview PulsarCD deployed; "
                "refusing to overwrite it", 409)
        if names.stack in live and record:
            await self.check_stack_is_preview(names.stack, names.image_tag)

    # ------------------------------------------------------------------
    # Deploy
    # ------------------------------------------------------------------

    def begin(self, repo_name: str, ssh_url: str, names: PreviewNames,
              ttl_hours: Optional[int], action_id: str) -> Dict[str, Any]:
        """Reserve the stack and record the preview as building.

        Re-checks the busy set, the owner and the limit without awaiting, so
        two requests that both passed :meth:`check_can_deploy` concurrently
        cannot both reserve the stack or exceed ``max_active``.

        Raises:
            PreviewError: 409 as in :meth:`check_can_deploy`.
        """
        if names.stack in self._busy:
            raise PreviewError(f"Preview {names.stack} is already being deployed or removed", 409)
        existing = self.store.get(names.stack) or {}
        if existing and (existing.get("repo") != repo_name or existing.get("branch") != names.branch):
            raise PreviewError(
                f"Stack {names.stack} already belongs to the preview of "
                f"{existing.get('repo')}@{existing.get('branch')}", 409)
        if not existing and len(self.store.all()) >= self.config.max_active:
            raise PreviewError(
                f"Preview limit reached ({self.config.max_active}); tear one down first", 409)
        self._busy.add(names.stack)
        now = _now().isoformat()
        return self.store.upsert(
            names.stack,
            repo=repo_name,
            ssh_url=ssh_url,
            branch=names.branch,
            slug=names.slug,
            image_tag=names.image_tag,
            host_prefix=names.host_prefix,
            # A redeploy without an explicit TTL keeps the preview's own TTL
            # (e.g. 0 = never), rather than falling back to the default.
            ttl_hours=(ttl_hours if ttl_hours is not None
                       else existing.get("ttl_hours", self.config.ttl_hours)),
            created_at=existing.get("created_at", now),
            updated_at=now,
            last_deployed_at=existing.get("last_deployed_at"),
            status="building",
            action_id=action_id,
            error=None,
        )

    async def run_deploy(self, repo_name: str, ssh_url: str, names: PreviewNames,
                         output_callback=None, cancel_event=None) -> Dict[str, Any]:
        """Build the branch under the preview tag, then deploy the preview stack.

        :meth:`begin` must have been called first; the stack is released when
        this returns, whatever the outcome.
        """
        stack = names.stack
        try:
            deployer = self._deployer_factory()
            build = await deployer.build(
                repo_name, ssh_url, names.image_tag, branch=names.branch,
                output_callback=output_callback, cancel_event=cancel_event,
            )
            if not build.get("success"):
                self.store.upsert(stack, status="failed", error="build failed",
                                  updated_at=_now().isoformat())
                return {**build, "action": "preview-deploy", "stack": stack, "stage": "build"}

            self.store.upsert(stack, status="deploying", updated_at=_now().isoformat())
            # Re-check right before Docker is touched: the build takes minutes.
            assert_preview_stack(stack, repo_stack_name(repo_name))
            result = await deployer.deploy(
                repo_name, ssh_url, names.image_tag, preview_branch=names.branch,
                output_callback=output_callback, cancel_event=cancel_event,
            )
            now = _now().isoformat()
            if result.get("success"):
                self.store.upsert(stack, status="active", error=None,
                                  last_deployed_at=now, updated_at=now)
            else:
                self.store.upsert(stack, status="failed", error="deploy failed", updated_at=now)
            result.update({"stack": stack, "stage": "deploy"})
            logger.info("Preview deploy finished", stack=stack, repo=repo_name,
                        branch=names.branch, success=bool(result.get("success")))
            return result
        except Exception as e:
            self.store.upsert(stack, status="failed", error=f"{type(e).__name__}: {e}",
                              updated_at=_now().isoformat())
            raise
        finally:
            self._busy.discard(stack)

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def expires_at(self, record: Dict[str, Any]) -> Optional[datetime]:
        """When a preview expires, or None if it never does."""
        ttl = record.get("ttl_hours")
        if not ttl:
            return None
        anchor = _parse_time(record.get("last_deployed_at")) or _parse_time(record.get("created_at"))
        return anchor + timedelta(hours=int(ttl)) if anchor else None

    async def list_previews(self) -> Dict[str, Any]:
        """Recorded previews with their live state, plus unmanaged ``pv-`` stacks."""
        live = await self.live_preview_stacks()
        live_set = set(live or [])
        previews = []
        for record in sorted(self.store.all(), key=lambda r: r.get("created_at") or ""):
            expires = self.expires_at(record)
            previews.append({
                **{k: v for k, v in record.items() if k != "ssh_url"},
                "running": (record["stack"] in live_set) if live is not None else None,
                "expires_at": expires.isoformat() if expires else None,
            })
        recorded = {p["stack"] for p in previews}
        return {
            "previews": previews,
            # Running pv- stacks PulsarCD has no record of: shown, never touched.
            "unmanaged": sorted(s for s in live_set if s not in recorded),
            "ttl_hours": self.config.ttl_hours,
            "max_active": self.config.max_active,
        }

    # ------------------------------------------------------------------
    # Teardown and expiry
    # ------------------------------------------------------------------

    async def teardown(self, stack: str, reason: str = "manual") -> Dict[str, Any]:
        """Remove a recorded preview stack with ``docker stack rm``.

        Raises:
            PreviewError: 400 for a non-preview name, 404 for an unrecorded
                preview, 409 while it is being deployed, 502 if Docker fails.
        """
        if not is_preview_stack(stack):
            raise PreviewError(f"{stack!r} is not a preview stack", 400)
        record = self.store.get(stack)
        if not record:
            raise PreviewError(f"No preview recorded for stack {stack}", 404)
        try:
            assert_preview_stack(stack, repo_stack_name(record.get("repo") or ""))
        except ValueError as e:
            raise PreviewError(str(e), 400) from e
        if stack in self._busy:
            raise PreviewError(f"Preview {stack} is busy; retry when its deploy finishes", 409)

        self._busy.add(stack)
        try:
            await self.check_stack_is_preview(stack, record.get("image_tag") or "")
            self.store.upsert(stack, status="removing", updated_at=_now().isoformat())
            deployer = self._deployer_factory()
            ok, output = await deployer._run_command(f"docker stack rm {shlex.quote(stack)}")
            # Removing a stack that is already gone is a success for us.
            if not ok and "nothing found in stack" not in (output or "").lower():
                self.store.upsert(stack, status="failed", error=f"teardown failed: {output[:200]}",
                                  updated_at=_now().isoformat())
                raise PreviewError(f"docker stack rm {stack} failed: {output[:500]}", 502)
            self.store.remove(stack)
            logger.info("Preview removed", stack=stack, repo=record.get("repo"),
                        branch=record.get("branch"), reason=reason)
            return {"success": True, "stack": stack, "reason": reason, "output": output}
        finally:
            self._busy.discard(stack)

    def expired(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Recorded previews whose TTL has elapsed and that are not busy."""
        now = now or _now()
        result = []
        for record in self.store.all():
            expires = self.expires_at(record)
            if expires and expires <= now and record["stack"] not in self._busy:
                result.append(record)
        return result

    async def cleanup_expired(self, now: Optional[datetime] = None) -> List[str]:
        """Tear down every expired preview; returns the removed stack names."""
        removed = []
        for record in self.expired(now):
            try:
                await self.teardown(record["stack"], reason="ttl")
                removed.append(record["stack"])
            except PreviewError as e:
                logger.warning("Preview expiry failed", stack=record["stack"], error=str(e))
        return removed

    async def _loop(self) -> None:
        interval = self.config.cleanup_interval_minutes * 60
        while True:
            await asyncio.sleep(interval)
            try:
                removed = await self.cleanup_expired()
                if removed:
                    logger.info("Expired previews removed", stacks=removed)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Preview cleanup pass failed",
                               error_type=type(e).__name__, error=str(e))

    def start(self) -> None:
        """Start the TTL cleanup loop (each record carries its own TTL)."""
        if self._task or not self.config.enabled:
            return
        self._task = asyncio.create_task(self._loop())
        logger.info("Preview TTL cleanup started", ttl_hours=self.config.ttl_hours,
                    interval_minutes=self.config.cleanup_interval_minutes)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
