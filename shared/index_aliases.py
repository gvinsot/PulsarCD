"""Non-destructive OpenSearch index management: versioned indices behind aliases.

OpenSearch cannot change a field type in place. The previous answer to a
mapping mismatch was to delete the index and recreate it, which silently threw
away every stored log line and metric. This module replaces that with:

* New deployments get a versioned concrete index (``pulsarcd-logs-v1``) behind
  an alias carrying the logical name (``pulsarcd-logs``). Every reader and
  writer keeps using the logical name, so nothing else has to know about
  versions.
* Existing deployments keep their plain (non-alias) index as long as its
  mapping is right. A plain index counts as version 1.
* On a mismatch, a migrator (the backend only) runs in the background:

  1. create ``<name>-v<N+1>`` with the expected mapping, no alias yet;
  2. reindex the current index into it (async OpenSearch task, startup is not
     blocked; reads and writes keep hitting the current index meanwhile);
  3. copy again, unblocked, what arrived during the reindex (catch-up on a
     recent ``timestamp`` window), then block writes on the current index and
     copy what arrived during that catch-up, so the block stays short;
  4. check the new index holds at least as many documents as the old one;
  5. in one atomic ``_aliases`` call, point the alias at the new index and
     remove the old one (``remove_index`` is what lets a plain index named
     ``pulsarcd-logs`` be replaced by an alias of the same name).

  Any failure before step 5 lifts the write block, drops the partial copy and
  leaves the old index -- and its data -- untouched. The error is logged and
  reported through :meth:`IndexManager.health`.
* Writers that may not migrate (the agents) only ever report a mismatch; they
  never delete anything.

Shared by the backend (``backend/opensearch_client.py``) and the agent
(``agent/opensearch_writer.py``).
"""

import asyncio
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger()

# Per-index states reported by IndexManager.status / health().
STATE_OK = "ok"
STATE_MIGRATING = "migrating"
STATE_MISMATCH = "mapping_mismatch"       # detected, migration not allowed here
STATE_MIGRATION_FAILED = "migration_failed"
STATE_ERROR = "error"

# Overall states returned by IndexManager.health().
HEALTH_OK = "ok"
HEALTH_MIGRATING = "migrating"
HEALTH_DEGRADED = "degraded"

# Documents written while the full reindex runs are copied again by a
# catch-up reindex on this window before the switch. Log timestamps come from
# the container, so a margin covers collection delay.
DEFAULT_CATCHUP_MARGIN_SECONDS = 900
DEFAULT_TASK_POLL_SECONDS = 5.0
# How many version numbers past the current one are tried when the next name
# is already taken by an index that is part of the alias.
_MAX_VERSION_PROBES = 20


def _unwrap(resp: Any) -> Any:
    """Return the plain payload of an opensearch-py 2.x/3.x response."""
    body = getattr(resp, "body", resp)
    if body is None or isinstance(body, (dict, list, bool, int, str)):
        return body
    try:
        return dict(body)
    except Exception:
        return body


def _is_not_found(exc: Exception) -> bool:
    if getattr(exc, "status_code", None) == 404:
        return True
    text = str(exc).lower()
    return "index_not_found" in text or "no such index" in text or "notfound" in type(exc).__name__.lower()


def _is_already_exists(exc: Exception) -> bool:
    text = str(exc).lower()
    return "resource_already_exists" in text or "already exists" in text


def mapping_mismatches(expected_mapping: dict, actual_props: Dict[str, Any]) -> List[str]:
    """List top-level fields whose actual type differs from the expected one.

    Fields missing from the actual mapping are not mismatches (they are added
    on first write), and object fields carry no "type" so they are skipped.
    """
    expected_props = expected_mapping.get("mappings", {}).get("properties", {})
    mismatches = []
    for field, expected_def in expected_props.items():
        expected_type = expected_def.get("type")
        actual_type = (actual_props.get(field) or {}).get("type")
        if actual_type and expected_type and actual_type != expected_type:
            mismatches.append(f"{field}: {actual_type} -> {expected_type}")
    return mismatches


def index_version(name: str, concrete: str) -> int:
    """Version of a concrete index behind ``name`` (a plain index is v1)."""
    match = re.fullmatch(re.escape(name) + r"-v(\d+)", concrete)
    return int(match.group(1)) if match else 1


def versioned_name(name: str, version: int) -> str:
    return f"{name}-v{version}"


def _create_body(mapping: dict, alias: Optional[str] = None) -> dict:
    body = {k: v for k, v in mapping.items() if k != "aliases"}
    if alias:
        body["aliases"] = {alias: {"is_write_index": True}}
    return body


class IndexMigrationError(Exception):
    """A migration step failed; the old index was left in place."""


class IndexManager:
    """Create, verify and (optionally) migrate indices behind aliases.

    ``allow_migration`` is True only on the backend: agents run on every host
    and must never race each other into reindexing or deleting anything.
    """

    def __init__(
        self,
        client: Any,
        *,
        allow_migration: bool = False,
        migrate_legacy: bool = False,
        catchup_margin_seconds: int = DEFAULT_CATCHUP_MARGIN_SECONDS,
        task_poll_seconds: float = DEFAULT_TASK_POLL_SECONDS,
    ):
        self._client = client
        self.allow_migration = allow_migration
        self.migrate_legacy = migrate_legacy
        self.catchup_margin_seconds = catchup_margin_seconds
        self.task_poll_seconds = task_poll_seconds
        self.status: Dict[str, Dict[str, Any]] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

    # ------------------------------------------------------------------ state

    def _set_status(self, name: str, state: str, **details: Any) -> None:
        self.status[name] = {"state": state, **details}

    def health(self) -> str:
        """Coarse state of all managed indices, safe to expose publicly."""
        states = {s.get("state") for s in self.status.values()}
        if states <= {STATE_OK}:
            return HEALTH_OK
        if states <= {STATE_OK, STATE_MIGRATING}:
            return HEALTH_MIGRATING
        return HEALTH_DEGRADED

    def migration_task(self, name: str) -> Optional[asyncio.Task]:
        return self._tasks.get(name)

    async def resolve(self, name: str) -> Tuple[str, Optional[str], List[str], Dict[str, Any]]:
        """Describe what ``name`` currently points at.

        Returns ``(kind, write_index, concrete_indices, write_index_props)``
        where kind is "missing", "plain" (a real index called ``name``) or
        "alias".
        """
        try:
            mappings = _unwrap(await self._client.indices.get_mapping(index=name))
        except Exception as e:
            if _is_not_found(e):
                return "missing", None, [], {}
            raise
        if not isinstance(mappings, dict) or not mappings:
            return "missing", None, [], {}

        def props_of(concrete: str) -> Dict[str, Any]:
            return (mappings.get(concrete) or {}).get("mappings", {}).get("properties", {})

        if name in mappings:
            return "plain", name, [name], props_of(name)

        concrete = sorted(mappings)
        write_index = concrete[0] if len(concrete) == 1 else None
        try:
            aliases = _unwrap(await self._client.indices.get_alias(name=name))
            for index_name, info in (aliases or {}).items():
                if ((info or {}).get("aliases", {}).get(name) or {}).get("is_write_index"):
                    write_index = index_name
        except Exception as e:
            logger.warning("Cannot read alias definition", alias=name, error=str(e))
        if write_index is None:
            # Several indices and no write index: pick the newest version.
            write_index = max(concrete, key=lambda c: index_version(name, c))
        return "alias", write_index, concrete, props_of(write_index)

    # ----------------------------------------------------------------- ensure

    async def ensure(self, name: str, mapping: dict) -> None:
        """Make ``name`` usable with ``mapping``; never deletes data.

        Raises only when the index cannot be created at all (same contract as
        the old _ensure_index, so the startup retry loop still applies).
        """
        kind, write_index, _, props = await self.resolve(name)

        if kind == "missing":
            await self._create_initial(name, mapping)
            kind, write_index, _, props = await self.resolve(name)
            if kind == "missing":
                raise IndexMigrationError(f"Index {name} could not be created")

        mismatches = mapping_mismatches(mapping, props)
        needs_legacy_migration = kind == "plain" and self.migrate_legacy

        if not mismatches and not needs_legacy_migration:
            logger.info("Index mapping OK", index=name, concrete=write_index, layout=kind)
            self._set_status(name, STATE_OK, concrete=write_index, layout=kind)
            return

        if not self.allow_migration:
            if mismatches:
                logger.error(
                    "Index mapping mismatch: leaving the index untouched "
                    "(the backend migrates it without data loss)",
                    index=name, concrete=write_index, mismatches=mismatches)
                self._set_status(name, STATE_MISMATCH, concrete=write_index,
                                 layout=kind, mismatches=mismatches)
            else:
                self._set_status(name, STATE_OK, concrete=write_index, layout=kind)
            return

        if name in self._tasks and not self._tasks[name].done():
            logger.info("Index migration already running", index=name)
            return

        reason = "mapping_mismatch" if mismatches else "legacy_layout"
        logger.warning("Scheduling non-destructive index migration",
                       index=name, source=write_index, reason=reason,
                       mismatches=mismatches)
        self._set_status(name, STATE_MIGRATING, concrete=write_index, layout=kind,
                         mismatches=mismatches, reason=reason)
        self._tasks[name] = asyncio.create_task(
            self._run_migration(name, mapping, write_index))

    async def _create_initial(self, name: str, mapping: dict) -> None:
        """Create ``<name>-v1`` with ``name`` as its write alias."""
        concrete = versioned_name(name, 1)
        try:
            await self._client.indices.create(index=concrete, body=_create_body(mapping, alias=name))
            logger.warning("Created index", index=concrete, alias=name)
        except Exception as e:
            # Another writer (backend or agent) won the race: fine.
            if _is_already_exists(e):
                logger.info("Index already created concurrently", index=concrete, alias=name)
                return
            logger.error("Failed to create index", index=concrete, alias=name,
                         error=str(e), error_type=type(e).__name__)
            raise

    # -------------------------------------------------------------- migration

    async def _run_migration(self, name: str, mapping: dict, source: str) -> None:
        try:
            target = await self.migrate(name, mapping, source)
            self._set_status(name, STATE_OK, concrete=target, layout="alias")
        except asyncio.CancelledError:
            self._set_status(name, STATE_MIGRATION_FAILED, concrete=source,
                             error="migration cancelled")
            raise
        except Exception as e:
            logger.error("Index migration failed; the old index and its data were kept",
                         index=name, source=source, error=str(e),
                         error_type=type(e).__name__)
            self._set_status(name, STATE_MIGRATION_FAILED, concrete=source,
                             error=str(e)[:500])

    async def _pick_target(self, name: str, source: str) -> str:
        """Next free versioned name, removing an orphan left by a crash."""
        version = index_version(name, source)
        for offset in range(1, _MAX_VERSION_PROBES + 1):
            candidate = versioned_name(name, version + offset)
            kind, _, _, _ = await self.resolve(candidate)
            if kind == "missing":
                return candidate
            aliases = _unwrap(await self._client.indices.get_alias(index=candidate)) or {}
            if (aliases.get(candidate) or {}).get("aliases"):
                continue  # in use by some alias: never touch it
            await self._cleanup_orphan(candidate, source)
            return candidate
        raise IndexMigrationError(f"No free version name for {name}")

    async def _cleanup_orphan(self, orphan: str, source: str) -> None:
        """Drop a half-built target from an interrupted migration.

        It is not part of any alias, so no reader sees it, and its source still
        holds all the data. A reindex task still feeding it is cancelled first,
        otherwise it would auto-create the index again with a dynamic mapping.
        """
        logger.warning("Removing leftover index from an interrupted migration",
                       index=orphan, source=source)
        try:
            tasks = _unwrap(await self._client.tasks.list(actions="*reindex", detailed=True)) or {}
            for node in (tasks.get("nodes") or {}).values():
                for task_id, task in (node.get("tasks") or {}).items():
                    # Match "[name]" exactly: "-v2" must not hit "-v20".
                    if f"[{orphan}]" in str(task.get("description", "")):
                        await self._client.tasks.cancel(task_id=task_id)
                        logger.warning("Cancelled stale reindex task", task_id=task_id)
        except Exception as e:
            logger.warning("Cannot list reindex tasks", error=str(e))
        await self._set_write_block(source, False)
        await self._client.indices.delete(index=orphan)

    async def _set_write_block(self, index: str, blocked: bool) -> None:
        await self._client.indices.put_settings(
            index=index, body={"index": {"blocks": {"write": True if blocked else None}}})

    async def _count(self, index: str) -> int:
        resp = _unwrap(await self._client.count(index=index)) or {}
        return int(resp.get("count", 0))

    async def _reindex(self, body: dict) -> Dict[str, Any]:
        """Run a reindex as a background OpenSearch task and wait for it."""
        resp = _unwrap(await self._client.reindex(
            body=body, wait_for_completion=False, refresh=True)) or {}
        task_id = resp.get("task")
        if not task_id:
            # Server ran it synchronously (or an older client): result is inline.
            return resp
        last_log = time.monotonic()
        try:
            while True:
                task = _unwrap(await self._client.tasks.get(task_id=task_id)) or {}
                if task.get("completed"):
                    if task.get("error"):
                        raise IndexMigrationError(f"Reindex task failed: {task['error']}")
                    return task.get("response") or {}
                if time.monotonic() - last_log >= 60:
                    status = (task.get("task") or {}).get("status") or {}
                    logger.warning("Reindex in progress", task_id=task_id,
                                   total=status.get("total"), created=status.get("created"))
                    last_log = time.monotonic()
                await asyncio.sleep(self.task_poll_seconds)
        except BaseException:
            # Cancelled (shutdown) or polling failed: stop the server-side
            # task too. Left running, it would keep writing into the target
            # the rollback is about to delete, and OpenSearch would auto-create
            # it again with a dynamic mapping.
            try:
                await self._client.tasks.cancel(task_id=task_id)
            except Exception as e:
                logger.warning("Cannot cancel reindex task", task_id=task_id, error=str(e))
            raise

    @staticmethod
    def _check_reindex(result: Dict[str, Any], step: str) -> None:
        failures = result.get("failures") or []
        if failures:
            raise IndexMigrationError(
                f"{step} reported {len(failures)} failures, first: {str(failures[0])[:300]}")

    async def migrate(self, name: str, mapping: dict, source: str) -> str:
        """Move ``source`` into a new versioned index and swap the alias.

        Returns the new concrete index name. On failure the old index is left
        exactly as it was (write block lifted) and IndexMigrationError or the
        client error is raised.
        """
        target = await self._pick_target(name, source)
        await self._client.indices.create(index=target, body=_create_body(mapping))
        logger.warning("Index migration started", index=name, source=source, target=target)

        started = datetime.now(timezone.utc)
        switched = False
        blocked = False
        try:
            result = await self._reindex({
                "conflicts": "proceed",
                "source": {"index": source},
                "dest": {"index": target},
            })
            self._check_reindex(result, "Reindex")
            logger.warning("Index migration: bulk copy done", index=name, target=target,
                           created=result.get("created"), total=result.get("total"))

            # A range on "timestamp" is only meaningful if the *source* maps
            # it as a date (or an ISO string keyword); otherwise copy
            # everything again, skipping documents already present.
            source_props = await self._props_of(source)
            ts_type = (source_props.get("timestamp") or {}).get("type")
            use_range = ts_type in ("date", "date_nanos", "keyword")

            # First catch-up without any block: copies what arrived during
            # the (possibly long) bulk copy, so the blocked pass below only
            # covers a short window.
            catchup_started = datetime.now(timezone.utc)
            self._check_reindex(
                await self._reindex(self._catchup_body(source, target, started, use_range)),
                "Catch-up reindex")

            # Freeze the source so nothing written from now on can be missed,
            # then copy what arrived during the first catch-up.
            await self._set_write_block(source, True)
            blocked = True
            self._check_reindex(
                await self._reindex(
                    self._catchup_body(source, target, catchup_started, use_range)),
                "Final catch-up reindex")

            await self._client.indices.refresh(index=target)
            source_count = await self._count(source)
            target_count = await self._count(target)
            if target_count < source_count:
                raise IndexMigrationError(
                    f"Copy is incomplete: {target_count} documents in {target}, "
                    f"{source_count} in {source}")

            # Guard against swapping onto an index that is not the one created
            # above (e.g. deleted by a concurrent rollback and auto-created
            # again by a still-running reindex with a dynamic mapping).
            target_mismatches = mapping_mismatches(mapping, await self._props_of(target))
            if target_mismatches:
                raise IndexMigrationError(
                    f"Target {target} does not have the expected mapping: {target_mismatches}")

            # One atomic call: readers and writers go from the old index to
            # the new one with no window where the name resolves to nothing.
            await self._client.indices.update_aliases(body={"actions": [
                {"add": {"index": target, "alias": name, "is_write_index": True}},
                {"remove_index": {"index": source}},
            ]})
            switched = True
            logger.warning("Index migration complete", index=name, source=source,
                           target=target, documents=target_count)
            return target
        finally:
            if not switched:
                await self._rollback(source, target, blocked)

    def _catchup_body(self, source: str, target: str, since: datetime,
                      use_range: bool) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "conflicts": "proceed",
            "source": {"index": source},
            "dest": {"index": target},
        }
        if use_range:
            gte = since - timedelta(seconds=self.catchup_margin_seconds)
            body["source"]["query"] = {"range": {"timestamp": {"gte": gte.isoformat()}}}
        else:
            body["dest"]["op_type"] = "create"
        return body

    async def _props_of(self, index: str) -> Dict[str, Any]:
        mappings = _unwrap(await self._client.indices.get_mapping(index=index)) or {}
        entry = mappings.get(index)
        if entry is None and len(mappings) == 1:
            entry = next(iter(mappings.values()))
        return ((entry or {}).get("mappings") or {}).get("properties") or {}

    async def _rollback(self, source: str, target: str, blocked: bool) -> None:
        if blocked:
            try:
                await self._set_write_block(source, False)
            except Exception as e:
                logger.error("Cannot lift the write block after a failed migration; "
                             "clear index.blocks.write manually",
                             index=source, error=str(e))
        try:
            await self._client.indices.delete(index=target)
        except Exception as e:
            logger.warning("Cannot delete the partial migration target",
                           index=target, error=str(e))

    # ------------------------------------------------------------------ admin

    async def concrete_indices(self, name: str) -> List[str]:
        _, _, concrete, _ = await self.resolve(name)
        return concrete

    async def drop(self, name: str) -> List[str]:
        """Delete every concrete index behind ``name`` (explicit admin action)."""
        task = self._tasks.pop(name, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        concrete = await self.concrete_indices(name)
        for index in concrete:
            await self._client.indices.delete(index=index)
        self.status.pop(name, None)
        return concrete

    async def close(self) -> None:
        """Cancel running migrations; each one rolls back on cancellation."""
        tasks = [t for t in self._tasks.values() if not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
