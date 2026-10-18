"""Tests for non-destructive index management (shared/index_aliases.py).

A small in-memory OpenSearch stand-in models what matters here: concrete
indices with mappings and documents, aliases with a write index, write
blocks, reindex tasks and the atomic _aliases call (including remove_index).
"""

import asyncio
import copy

import pytest

from shared import index_aliases as ia
from shared.index_aliases import IndexManager, mapping_mismatches


class NotFoundError(Exception):
    status_code = 404


class AlreadyExistsError(Exception):
    pass


MAPPING = {
    "mappings": {"properties": {
        "timestamp": {"type": "date"},
        "host": {"type": "keyword"},
        "message": {"type": "text"},
    }},
    "settings": {"number_of_shards": 1},
}

BAD_PROPS = {
    "timestamp": {"type": "date"},
    "host": {"type": "text"},          # auto-mapped: the classic mismatch
    "message": {"type": "text"},
}


class FakeIndices:
    def __init__(self, cluster):
        self.c = cluster

    async def get_mapping(self, index):
        names = self.c.resolve_read(index)
        return {n: {"mappings": {"properties": copy.deepcopy(self.c.indices[n]["props"])}}
                for n in names}

    async def get_alias(self, name=None, index=None):
        out = {}
        for concrete, data in self.c.indices.items():
            if index is not None and concrete != index:
                continue
            aliases = {a: dict(cfg) for a, cfg in self.c.aliases.items_for(concrete)}
            if name is not None:
                aliases = {a: v for a, v in aliases.items() if a == name}
                if not aliases:
                    continue
            out[concrete] = {"aliases": aliases}
        if name is not None and not out:
            raise NotFoundError(f"alias [{name}] missing")
        if index is not None and index not in self.c.indices:
            raise NotFoundError(f"no such index [{index}]")
        return out

    async def create(self, index, body):
        self.c.calls.append(("create", index))
        if index in self.c.indices or index in self.c.aliases.names():
            raise AlreadyExistsError(f"resource_already_exists_exception [{index}]")
        self.c.indices[index] = {
            "props": copy.deepcopy(body.get("mappings", {}).get("properties", {})),
            "docs": {}, "blocked": False,
        }
        for alias, cfg in (body.get("aliases") or {}).items():
            self.c.aliases.add(alias, index, cfg.get("is_write_index", False))

    async def delete(self, index):
        self.c.calls.append(("delete", index))
        if index not in self.c.indices:
            raise NotFoundError(f"no such index [{index}]")
        del self.c.indices[index]
        self.c.aliases.drop_index(index)

    async def put_settings(self, index, body):
        blocked = body["index"]["blocks"]["write"]
        for n in self.c.resolve_read(index):
            self.c.indices[n]["blocked"] = bool(blocked)

    async def refresh(self, index):
        return {}

    async def update_aliases(self, body):
        self.c.calls.append(("update_aliases", body))
        if self.c.fail_alias_swap:
            raise RuntimeError("alias swap rejected")
        for action in body["actions"]:
            (kind, args), = action.items()
            if kind == "remove_index":
                del self.c.indices[args["index"]]
                self.c.aliases.drop_index(args["index"])
        for action in body["actions"]:
            (kind, args), = action.items()
            if kind == "add":
                self.c.aliases.add(args["alias"], args["index"], args.get("is_write_index", False))


class Aliases:
    def __init__(self):
        self.data = {}  # alias -> {index: is_write}

    def names(self):
        return set(self.data)

    def add(self, alias, index, is_write):
        self.data.setdefault(alias, {})[index] = is_write

    def drop_index(self, index):
        for alias in list(self.data):
            self.data[alias].pop(index, None)
            if not self.data[alias]:
                del self.data[alias]

    def items_for(self, index):
        return [(a, {"is_write_index": m[index]}) for a, m in self.data.items() if index in m]


class FakeTasks:
    def __init__(self, cluster):
        self.c = cluster

    async def get(self, task_id):
        task = self.c.tasks[task_id]
        task["polls"] += 1
        if task["polls"] < self.c.task_polls_before_done:
            if self.c.on_task_poll:
                await self.c.on_task_poll()
            return {"completed": False, "task": {"status": {}}}
        return {"completed": True, "response": task["run"]()}

    async def list(self, actions=None, detailed=False):
        return {"nodes": {"n1": {"tasks": {
            tid: {"description": t["description"]} for tid, t in self.c.tasks.items()
            if t["polls"] < self.c.task_polls_before_done}}}}

    async def cancel(self, task_id):
        self.c.cancelled.append(task_id)


class FakeCluster:
    def __init__(self):
        self.indices = {}
        self.aliases = Aliases()
        self.indices_api = FakeIndices(self)
        self.tasks_api = FakeTasks(self)
        self.tasks = {}
        self.calls = []
        self.cancelled = []
        self.task_polls_before_done = 1
        self.on_task_poll = None
        self.reindex_failures = []
        self.drop_on_reindex = 0
        self.fail_alias_swap = False

    # opensearch-py style namespaces
    @property
    def indices_ns(self):
        return self.indices_api

    def resolve_read(self, name):
        if name in self.indices:
            return [name]
        if name in self.aliases.data:
            return list(self.aliases.data[name])
        raise NotFoundError(f"index_not_found_exception [{name}]")

    def resolve_write(self, name):
        if name in self.indices:
            return name
        members = self.aliases.data.get(name)
        if not members:
            # Like OpenSearch auto-create: a plain dynamic index appears.
            self.indices[name] = {"props": {}, "docs": {}, "blocked": False}
            return name
        writes = [i for i, w in members.items() if w] or list(members)
        return writes[0]

    def write(self, name, doc_id, doc):
        target = self.resolve_write(name)
        if self.indices[target]["blocked"]:
            raise RuntimeError("cluster_block_exception")
        self.indices[target]["docs"][doc_id] = doc

    def seed_plain(self, name, props, docs):
        self.indices[name] = {"props": copy.deepcopy(props), "docs": dict(docs), "blocked": False}

    async def count(self, index):
        return {"count": sum(len(self.indices[n]["docs"]) for n in self.resolve_read(index))}

    async def reindex(self, body, wait_for_completion=True, refresh=False):
        src = body["source"]["index"]
        dest = body["dest"]["index"]
        query = body["source"].get("query")
        op_create = body["dest"].get("op_type") == "create"
        tid = f"node:{len(self.tasks) + 1}"
        snapshot = dict(self.indices[src]["docs"])

        def run():
            if self.reindex_failures:
                return {"failures": list(self.reindex_failures), "total": len(snapshot)}
            docs = snapshot
            if query:
                since = query["range"]["timestamp"]["gte"]
                docs = {k: v for k, v in docs.items() if v["timestamp"] >= since[:19]}
            items = list(docs.items())
            if self.drop_on_reindex:
                items = items[self.drop_on_reindex:]
            for k, v in items:
                if op_create and k in self.indices[dest]["docs"]:
                    continue
                self.indices[dest]["docs"][k] = v
            return {"failures": [], "total": len(items), "created": len(items)}

        self.tasks[tid] = {"run": run, "polls": 0,
                           "description": f"reindex from [{src}] to [{dest}]"}
        return {"task": tid}


class FakeClient:
    """Exposes the cluster with the opensearch-py attribute layout."""

    def __init__(self, cluster):
        self.cluster = cluster
        self.indices = cluster.indices_api
        self.tasks = cluster.tasks_api
        self.count = cluster.count
        self.reindex = cluster.reindex


def _docs(n, ts="2026-10-17T10:00:00"):
    return {f"d{i}": {"timestamp": ts, "host": f"h{i}", "message": "m"} for i in range(n)}


@pytest.fixture
def cluster():
    return FakeCluster()


def _manager(cluster, **kw):
    kw.setdefault("task_poll_seconds", 0)
    return IndexManager(FakeClient(cluster), **kw)


async def _wait(manager, name):
    task = manager.migration_task(name)
    assert task is not None, "no migration was scheduled"
    await asyncio.wait_for(task, timeout=5)


# --------------------------------------------------------------------- units

def test_mapping_mismatches_reports_type_changes_only():
    actual = {"host": {"type": "text"}, "timestamp": {"type": "date"},
              "parsed_fields": {"enabled": False}}
    expected = {"mappings": {"properties": {
        "host": {"type": "keyword"}, "timestamp": {"type": "date"},
        "parsed_fields": {"type": "object", "enabled": False},
        "level": {"type": "keyword"},   # missing: not a mismatch
    }}}
    assert mapping_mismatches(expected, actual) == ["host: text -> keyword"]


def test_index_version():
    assert ia.index_version("pulsarcd-logs", "pulsarcd-logs") == 1
    assert ia.index_version("pulsarcd-logs", "pulsarcd-logs-v7") == 7
    assert ia.index_version("pulsarcd-logs", "pulsarcd-logs-v7x") == 1


# ------------------------------------------------------------------- ensure

async def test_missing_index_is_created_versioned_behind_alias(cluster):
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    assert "pulsarcd-logs-v1" in cluster.indices
    assert cluster.aliases.data == {"pulsarcd-logs": {"pulsarcd-logs-v1": True}}
    assert m.status["pulsarcd-logs"]["state"] == ia.STATE_OK
    assert m.health() == ia.HEALTH_OK
    # Writers keep using the logical name.
    cluster.write("pulsarcd-logs", "x", {"timestamp": "2026-10-17T10:00:00"})
    assert "x" in cluster.indices["pulsarcd-logs-v1"]["docs"]


async def test_concurrent_creation_is_not_an_error(cluster):
    await _manager(cluster).ensure("pulsarcd-logs", MAPPING)
    m = _manager(cluster)
    await m.ensure("pulsarcd-logs", MAPPING)
    assert m.status["pulsarcd-logs"]["state"] == ia.STATE_OK


async def test_legacy_plain_index_with_good_mapping_is_left_alone(cluster):
    cluster.seed_plain("pulsarcd-logs", MAPPING["mappings"]["properties"], _docs(3))
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    assert m.migration_task("pulsarcd-logs") is None
    assert not [c for c in cluster.calls if c[0] in ("create", "delete")]
    assert m.status["pulsarcd-logs"] == {
        "state": ia.STATE_OK, "concrete": "pulsarcd-logs", "layout": "plain"}


async def test_agent_never_deletes_on_mismatch(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(5))
    m = _manager(cluster, allow_migration=False)
    await m.ensure("pulsarcd-logs", MAPPING)
    assert not [c for c in cluster.calls if c[0] in ("create", "delete")]
    assert len(cluster.indices["pulsarcd-logs"]["docs"]) == 5
    assert m.status["pulsarcd-logs"]["state"] == ia.STATE_MISMATCH
    assert m.status["pulsarcd-logs"]["mismatches"] == ["host: text -> keyword"]
    assert m.health() == ia.HEALTH_DEGRADED


# ---------------------------------------------------------------- migration

async def test_plain_index_mismatch_is_migrated_without_data_loss(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(10))
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    # ensure() returns before the copy: startup is not blocked.
    assert m.status["pulsarcd-logs"]["state"] == ia.STATE_MIGRATING
    assert m.health() == ia.HEALTH_MIGRATING
    await _wait(m, "pulsarcd-logs")

    assert "pulsarcd-logs" not in cluster.indices          # now only an alias
    assert cluster.aliases.data == {"pulsarcd-logs": {"pulsarcd-logs-v2": True}}
    new = cluster.indices["pulsarcd-logs-v2"]
    assert new["props"]["host"]["type"] == "keyword"
    assert set(new["docs"]) == set(_docs(10))
    assert not new["blocked"]
    assert m.status["pulsarcd-logs"]["state"] == ia.STATE_OK
    assert m.health() == ia.HEALTH_OK
    # The swap is a single atomic call that also removes the old index.
    swap = [c for c in cluster.calls if c[0] == "update_aliases"][0][1]
    assert swap["actions"] == [
        {"add": {"index": "pulsarcd-logs-v2", "alias": "pulsarcd-logs", "is_write_index": True}},
        {"remove_index": {"index": "pulsarcd-logs"}},
    ]
    assert ("delete", "pulsarcd-logs") not in cluster.calls


async def test_aliased_index_mismatch_moves_to_next_version(cluster):
    cluster.seed_plain("pulsarcd-logs-v2", BAD_PROPS, _docs(4))
    cluster.aliases.add("pulsarcd-logs", "pulsarcd-logs-v2", True)
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    assert cluster.aliases.data == {"pulsarcd-logs": {"pulsarcd-logs-v3": True}}
    assert "pulsarcd-logs-v2" not in cluster.indices
    assert len(cluster.indices["pulsarcd-logs-v3"]["docs"]) == 4


async def test_documents_written_during_reindex_are_caught_up(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(3, ts="2020-01-01T00:00:00"))
    cluster.task_polls_before_done = 2
    written = []

    async def writer_during_copy():
        if not written:
            cluster.write("pulsarcd-logs", "late", {
                "timestamp": "2099-01-01T00:00:00", "host": "h", "message": "late"})
            written.append(True)

    cluster.on_task_poll = writer_during_copy
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    docs = cluster.indices["pulsarcd-logs-v2"]["docs"]
    assert "late" in docs and len(docs) == 4


async def test_failed_reindex_keeps_old_index_and_lifts_block(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(6))
    cluster.reindex_failures = [{"cause": {"type": "mapper_parsing_exception"}}]
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")

    old = cluster.indices["pulsarcd-logs"]
    assert len(old["docs"]) == 6 and not old["blocked"]
    assert "pulsarcd-logs-v2" not in cluster.indices        # partial copy dropped
    assert not [c for c in cluster.calls if c[0] == "update_aliases"]
    status = m.status["pulsarcd-logs"]
    assert status["state"] == ia.STATE_MIGRATION_FAILED
    assert "mapper_parsing_exception" in status["error"]
    assert m.health() == ia.HEALTH_DEGRADED


async def test_incomplete_copy_aborts_before_swap(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(5))
    cluster.drop_on_reindex = 2   # reindex silently loses documents
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    assert len(cluster.indices["pulsarcd-logs"]["docs"]) == 5
    assert not cluster.indices["pulsarcd-logs"]["blocked"]
    assert "pulsarcd-logs-v2" not in cluster.indices
    assert "incomplete" in m.status["pulsarcd-logs"]["error"]


async def test_failed_alias_swap_rolls_back(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(2))
    cluster.fail_alias_swap = True
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    assert len(cluster.indices["pulsarcd-logs"]["docs"]) == 2
    assert not cluster.indices["pulsarcd-logs"]["blocked"]
    assert "pulsarcd-logs-v2" not in cluster.indices


async def test_orphan_from_interrupted_migration_is_cleaned_up(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(3))
    cluster.indices["pulsarcd-logs"]["blocked"] = True          # crash under block
    cluster.seed_plain("pulsarcd-logs-v2", MAPPING["mappings"]["properties"], _docs(1))
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    assert ("delete", "pulsarcd-logs-v2") in cluster.calls
    assert cluster.aliases.data == {"pulsarcd-logs": {"pulsarcd-logs-v2": True}}
    assert len(cluster.indices["pulsarcd-logs-v2"]["docs"]) == 3


async def test_aliased_next_version_is_skipped(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(1))
    cluster.seed_plain("pulsarcd-logs-v2", {}, {})
    cluster.aliases.add("something-else", "pulsarcd-logs-v2", True)
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    assert "pulsarcd-logs-v2" in cluster.indices
    assert cluster.aliases.data["pulsarcd-logs"] == {"pulsarcd-logs-v3": True}


async def test_opt_in_legacy_migration_moves_good_plain_index(cluster):
    cluster.seed_plain("pulsarcd-logs", MAPPING["mappings"]["properties"], _docs(3))
    m = _manager(cluster, allow_migration=True, migrate_legacy=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    assert m.status["pulsarcd-logs"]["reason"] == "legacy_layout"
    await _wait(m, "pulsarcd-logs")
    assert cluster.aliases.data == {"pulsarcd-logs": {"pulsarcd-logs-v2": True}}


async def test_close_cancels_and_rolls_back(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(2))
    cluster.task_polls_before_done = 10 ** 9   # reindex never finishes
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await asyncio.sleep(0.01)
    await m.close()
    assert "pulsarcd-logs-v2" not in cluster.indices
    assert len(cluster.indices["pulsarcd-logs"]["docs"]) == 2
    assert m.status["pulsarcd-logs"]["state"] == ia.STATE_MIGRATION_FAILED


async def test_drop_deletes_concrete_indices_behind_alias(cluster):
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    assert await m.drop("pulsarcd-logs") == ["pulsarcd-logs-v1"]
    assert not cluster.indices and "pulsarcd-logs" not in m.status


# ------------------------------------------------------- backend and agent

def _backend_client(cluster, **cfg):
    from backend.config import OpenSearchConfig
    from backend.opensearch_client import OpenSearchClient
    client = OpenSearchClient(OpenSearchConfig(hosts=["http://localhost:9200"], **cfg))
    client._client = FakeClient(cluster)
    client.index_manager._client = client._client
    client.index_manager.task_poll_seconds = 0
    return client


async def test_backend_migrates_instead_of_deleting(cluster):
    cluster.seed_plain("pulsarcd-logs", {"host": {"type": "text"}}, _docs(4))
    client = _backend_client(cluster)
    await client._create_logs_index()
    await _wait(client.index_manager, "pulsarcd-logs")
    assert ("delete", "pulsarcd-logs") not in cluster.calls
    assert len(cluster.indices["pulsarcd-logs-v2"]["docs"]) == 4
    assert client.index_health() == "ok"
    assert client.index_status()["pulsarcd-logs"]["concrete"] == "pulsarcd-logs-v2"


async def test_backend_can_disable_auto_migration(cluster):
    cluster.seed_plain("pulsarcd-logs", {"host": {"type": "text"}}, _docs(4))
    client = _backend_client(cluster, auto_migrate_mappings=False)
    await client._create_logs_index()
    assert client.index_manager.migration_task("pulsarcd-logs") is None
    assert client.index_health() == "degraded"
    assert len(cluster.indices["pulsarcd-logs"]["docs"]) == 4


async def test_backend_recreate_index_handles_alias(cluster):
    client = _backend_client(cluster)
    await client._create_logs_index()
    cluster.write("pulsarcd-logs", "x", {"timestamp": "2026-10-17T10:00:00"})
    result = await client.recreate_index("pulsarcd-logs")
    assert result["status"] == "recreated"
    assert ("delete", "pulsarcd-logs-v1") in cluster.calls
    assert cluster.aliases.data == {"pulsarcd-logs": {"pulsarcd-logs-v1": True}}
    assert cluster.indices["pulsarcd-logs-v1"]["docs"] == {}


async def test_agent_writer_reports_mismatch_without_deleting(cluster):
    from agent.config import OpenSearchConfig as AgentOpenSearchConfig
    from agent.opensearch_writer import OpenSearchWriter
    cluster.seed_plain("pulsarcd-logs", {"host": {"type": "text"}}, _docs(4))
    writer = OpenSearchWriter(AgentOpenSearchConfig(hosts=["http://localhost:9200"]))
    writer._client = FakeClient(cluster)
    writer.index_manager._client = writer._client
    await writer._ensure_index(writer.logs_index, MAPPING)
    assert not [c for c in cluster.calls if c[0] in ("create", "delete")]
    assert writer.index_manager.status["pulsarcd-logs"]["state"] == ia.STATE_MISMATCH


def test_health_exposes_coarse_index_state(client):
    import backend.api as api_module
    api_module.opensearch.index_health.return_value = "migrating"
    try:
        data = client.get("/api/health").json()
    finally:
        api_module.opensearch.index_health.return_value = "ok"
    assert data["indices"] == "migrating"


def test_config_env_overrides(monkeypatch):
    from backend.config import load_config
    monkeypatch.setenv("PULSARCD_OPENSEARCH__AUTO_MIGRATE_MAPPINGS", "false")
    monkeypatch.setenv("PULSARCD_OPENSEARCH__MIGRATE_LEGACY_INDICES", "true")
    monkeypatch.setenv("PULSARCD_OPENSEARCH__MIGRATION_CATCHUP_SECONDS", "60")
    settings = load_config()
    assert settings.opensearch.auto_migrate_mappings is False
    assert settings.opensearch.migrate_legacy_indices is True
    assert settings.opensearch.migration_catchup_seconds == 60


# ------------------------------------------------------------ review fixes

async def test_close_also_cancels_server_side_reindex_task(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(2))
    cluster.task_polls_before_done = 10 ** 9
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await asyncio.sleep(0.01)
    await m.close()
    assert cluster.cancelled, "the OpenSearch reindex task kept running"


async def test_orphan_cleanup_does_not_cancel_unrelated_version_task(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(1))
    cluster.seed_plain("pulsarcd-logs-v2", MAPPING["mappings"]["properties"], {})
    cluster.tasks["other:1"] = {"run": lambda: {}, "polls": 0,
                                "description": "reindex from [x] to [pulsarcd-logs-v20]"}
    m = _manager(cluster, allow_migration=True)
    await m._cleanup_orphan("pulsarcd-logs-v2", "pulsarcd-logs")
    assert "other:1" not in cluster.cancelled


async def test_catchup_without_date_timestamp_copies_everything(cluster):
    props = dict(BAD_PROPS, timestamp={"type": "text"})
    cluster.seed_plain("pulsarcd-logs", props, _docs(3))
    bodies = []
    original = cluster.reindex

    async def spy(body, **kw):
        bodies.append(copy.deepcopy(body))
        return await original(body, **kw)

    cluster.reindex = spy
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    assert len(bodies) == 3   # bulk copy, unblocked catch-up, blocked catch-up
    assert all("query" not in b["source"] for b in bodies[1:])
    assert all(b["dest"].get("op_type") == "create" for b in bodies[1:])
    assert len(cluster.indices["pulsarcd-logs-v2"]["docs"]) == 3


async def test_swap_refused_when_target_mapping_is_wrong(cluster):
    cluster.seed_plain("pulsarcd-logs", BAD_PROPS, _docs(2))
    cluster.task_polls_before_done = 2

    async def clobber_target():
        # Target deleted and auto-created again with a dynamic mapping.
        if "pulsarcd-logs-v2" in cluster.indices:
            cluster.indices["pulsarcd-logs-v2"]["props"] = dict(BAD_PROPS)

    cluster.on_task_poll = clobber_target
    m = _manager(cluster, allow_migration=True)
    await m.ensure("pulsarcd-logs", MAPPING)
    await _wait(m, "pulsarcd-logs")
    assert len(cluster.indices["pulsarcd-logs"]["docs"]) == 2
    assert not cluster.indices["pulsarcd-logs"]["blocked"]
    assert not [c for c in cluster.calls if c[0] == "update_aliases"]
    assert "expected mapping" in m.status["pulsarcd-logs"]["error"]
