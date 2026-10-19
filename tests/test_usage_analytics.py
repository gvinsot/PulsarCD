"""Tests for the usage view: query shape, interpretation and API."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

import backend.api as api_module
from backend import usage_analytics as usage
from backend.config import HostConfig
from backend.docker_client import DockerAPIClient


NOW = datetime(2026, 9, 22, 20, 0, 0, tzinfo=timezone.utc)
START = datetime(2026, 9, 22, 19, 0, 0, tzinfo=timezone.utc)

ROUTER_STACKS = {
    "pulsarcd": "pulsarcd",
    "pulsarcd-ws": "pulsarcd",
    "intra-muros": "intra-muros",
}


def _traffic(requests, ips=1, client_errors=0, server_errors=0, avg=None, size=0):
    """A ranking row as VictoriaLogs returns it: every value a string."""
    return {
        "requests": str(requests),
        "distinct_ips": str(ips),
        "client_errors": str(client_errors),
        "server_errors": str(server_errors),
        "avg_duration": "NaN" if avg is None else str(avg),
        "bytes": str(size),
    }


def _endpoint(host, path, requests, router="pulsarcd@swarm", **kwargs):
    return {
        "request_host": host, "path": path, **_traffic(requests, **kwargs),
        "max_duration": "120", "methods": '["GET"]', "router": router or "",
        "last_seen": "2026-09-22T19:58:00Z",
    }


ROWS = {
    "totals": [{"requests": "300", "unique_ips": "42", "distinct_paths": "17", "distinct_hosts": "2",
                "client_errors": "55", "server_errors": "5", "bytes": "1048576",
                "avg_duration": "12.345", "p50": "2.5", "p95": "40", "p99": "NaN"}],
    "timeline": [{"_time": "2026-09-22T19:00:00Z", "ok": "240", "client_errors": "55",
                  "server_errors": "5", "avg_duration_ms": "12.3456"}],
    "hosts": [
        {"request_host": "logs.methodinfo.fr", "distinct_paths": "2",
         **_traffic(200, ips=44, client_errors=53, avg=22.0, size=900)},
        {"request_host": "intra-muros.fr", "distinct_paths": "1",
         **_traffic(100, ips=20, avg=8.0, size=500)},
    ],
    "endpoints": [
        _endpoint("logs.methodinfo.fr", "/api/logs/search", 150, ips=8, client_errors=3, avg=30.0, size=900),
        _endpoint("intra-muros.fr", "/", 100, router="intra-muros@swarm", ips=20, avg=8.0, size=500),
        _endpoint("logs.methodinfo.fr", "/.env", 50, ips=40, client_errors=50, avg=1.0),
    ],
    "routers": [
        {"router": "pulsarcd@swarm", **_traffic(150, ips=8, client_errors=3, avg=30.0, size=900)},
        {"router": "intra-muros@swarm", **_traffic(100, ips=20, avg=8.0, size=500)},
        {"router": "pulsarcd-ws@swarm", **_traffic(10, ips=2, avg=10.0)},
        {"router": "", **_traffic(40, ips=30, client_errors=40, avg=0.5)},
    ],
    "methods": [{"method": "GET", "n": "280"}, {"method": "POST", "n": "20"}],
    "status_codes": [{"status": "200", "n": "240"}, {"status": "404", "n": "55"}],
}


def _summarize(rows=ROWS, stack=None, minutes=60):
    return usage.summarize(rows, minutes=minutes, include_internal=False, stack=stack,
                           stacks=sorted(set(ROUTER_STACKS.values())),
                           router_stacks=ROUTER_STACKS, start=START, end=NOW, generated_at=NOW)


class TestQuery:
    def test_only_requests_are_counted(self):
        queries = usage.build_usage_queries(60, False, None)
        for query in queries.values():
            assert query.startswith('{kind="access"} event:="request" internal:=false')

    def test_internal_traffic_can_be_included(self):
        queries = usage.build_usage_queries(60, True, None)
        assert not any("internal:=false" in q for q in queries.values())

    def test_no_stack_filter_by_default(self):
        assert not any("router:=" in q or "router:in" in q
                       for q in usage.build_usage_queries(60, False, None).values())

    def test_stack_filter_matches_every_provider_suffix(self):
        queries = usage.build_usage_queries(60, False, ["pulsarcd", "pulsarcd-ws"])
        assert '(router:="pulsarcd@"* OR router:="pulsarcd-ws@"*)' in queries["totals"]

    def test_a_stack_without_a_router_matches_nothing(self):
        assert usage.router_filter([]) == "router:in()"

    def test_endpoints_are_ranked_exactly(self):
        endpoints = usage.build_usage_queries(1440, False, None)["endpoints"]
        assert "stats by (request_host, path)" in endpoints
        assert f"limit {usage.TOP_ENDPOINTS}" in endpoints

    def test_timeline_follows_the_window(self):
        assert "_time:1m" in usage.build_usage_queries(60, False, None)["timeline"]
        assert "_time:1h" in usage.build_usage_queries(10080, False, None)["timeline"]


class TestTotals:
    def test_totals(self):
        totals = _summarize()["totals"]
        assert totals["requests"] == 300
        assert totals["unique_ips"] == 42
        assert totals["endpoints"] == 17
        assert totals["client_errors"] == 55 and totals["server_errors"] == 5
        assert totals["error_rate"] == 20.0
        assert totals["requests_per_minute"] == 5.0
        assert totals["bytes"] == 1048576
        assert totals["avg_duration_ms"] == 12.3

    def test_a_percentile_without_data_is_null(self):
        totals = _summarize()["totals"]
        assert (totals["p50"], totals["p95"]) == (2.5, 40.0)
        assert totals["p99"] is None

    def test_empty_response(self):
        result = _summarize({})
        assert result["totals"]["requests"] == 0
        assert result["endpoints"] == [] and result["by_stack"] == []
        assert result["stacks"] == ["intra-muros", "pulsarcd"]

    def test_timeline_covers_the_window(self):
        timeline = _summarize()["timeline"]
        assert len(timeline) == 60
        assert timeline[0] == {"timestamp": "2026-09-22T19:00:00.000Z", "avg_duration_ms": 12.3,
                               "ok": 240, "client_errors": 55, "server_errors": 5}
        assert timeline[1]["ok"] == 0 and timeline[1]["avg_duration_ms"] is None


class TestEndpoints:
    def test_endpoints_keep_their_ranking(self):
        endpoints = _summarize()["endpoints"]
        assert [(e["host"], e["path"], e["requests"]) for e in endpoints] == [
            ("logs.methodinfo.fr", "/api/logs/search", 150),
            ("intra-muros.fr", "/", 100),
            ("logs.methodinfo.fr", "/.env", 50),
        ]

    def test_an_endpoint_carries_its_stack(self):
        by_path = {e["path"]: e for e in _summarize()["endpoints"]}
        assert by_path["/api/logs/search"]["stack"] == "pulsarcd"
        assert by_path["/api/logs/search"]["router"] == "pulsarcd"
        assert by_path["/"]["stack"] == "intra-muros"

    def test_endpoint_metrics(self):
        endpoint = _summarize()["endpoints"][0]
        assert endpoint["share"] == 50.0
        assert endpoint["distinct_ips"] == 8
        assert endpoint["client_errors"] == 3
        assert endpoint["avg_duration_ms"] == 30.0
        assert endpoint["max_duration_ms"] == 120.0
        assert endpoint["methods"] == ["GET"]
        assert endpoint["last_seen"] == "2026-09-22T19:58:00Z"

    def test_hosts_report_their_endpoint_count(self):
        hosts = _summarize()["hosts"]
        assert [(h["host"], h["requests"], h["distinct_paths"]) for h in hosts] == [
            ("logs.methodinfo.fr", 200, 2), ("intra-muros.fr", 100, 1),
        ]


class TestByStack:
    def test_routers_of_one_stack_are_merged(self):
        rows = {row["stack"]: row for row in _summarize()["by_stack"]}
        assert rows["pulsarcd"]["requests"] == 160  # pulsarcd + pulsarcd-ws
        assert sorted(rows["pulsarcd"]["routers"]) == ["pulsarcd", "pulsarcd-ws"]
        assert rows["pulsarcd"]["client_errors"] == 3
        assert rows["pulsarcd"]["bytes"] == 900

    def test_merged_latency_is_weighted_by_volume(self):
        # 150 requests at 30 ms and 10 at 10 ms, not the plain mean of the two.
        rows = {row["stack"]: row for row in _summarize()["by_stack"]}
        assert rows["pulsarcd"]["avg_duration_ms"] == 28.8

    def test_unclaimed_routers_share_one_row(self):
        rows = {row["stack"]: row for row in _summarize()["by_stack"]}
        assert rows[None]["requests"] == 40
        assert rows[None]["routers"] == []

    def test_rows_add_up_to_the_total(self):
        result = _summarize()
        assert sum(r["requests"] for r in result["by_stack"]) == result["totals"]["requests"]

    def test_rows_are_ranked_by_volume(self):
        rows = _summarize()["by_stack"]
        assert [r["requests"] for r in rows] == sorted(
            (r["requests"] for r in rows), reverse=True)


class TestLogStore:
    def _store(self):
        from backend.config import VictoriaLogsConfig
        from backend.log_store import LogStore
        return LogStore(VictoriaLogsConfig())

    async def test_a_stack_selects_its_routers(self):
        store = self._store()
        run = AsyncMock(return_value=ROWS)
        with patch.object(store, "_run_queries", run):
            result = await store.get_usage_overview(minutes=60, stack="pulsarcd",
                                                    router_stacks=ROUTER_STACKS)
        await store.close()
        totals = run.await_args.args[0]["totals"]
        assert 'router:="pulsarcd@"*' in totals and 'router:="pulsarcd-ws@"*' in totals
        assert result["stack"] == "pulsarcd"
        assert result["stacks"] == ["intra-muros", "pulsarcd"]

    async def test_an_unknown_stack_returns_no_request(self):
        # Rather than silently reporting the whole platform under its name.
        store = self._store()
        run = AsyncMock(return_value={})
        with patch.object(store, "_run_queries", run):
            await store.get_usage_overview(minutes=60, stack="gone", router_stacks=ROUTER_STACKS)
        await store.close()
        assert "router:in()" in run.await_args.args[0]["totals"]

    async def test_unreachable_store_returns_an_empty_overview(self):
        from shared.victorialogs import VictoriaLogsError
        store = self._store()
        with patch.object(store, "_run_queries", AsyncMock(side_effect=VictoriaLogsError("down"))):
            result = await store.get_usage_overview(minutes=60, router_stacks=ROUTER_STACKS)
        await store.close()
        assert result["error"] == "Access data unavailable"
        assert result["endpoints"] == [] and result["stacks"] == ["intra-muros", "pulsarcd"]


class TestAgainstVictoriaLogs:
    """The LogsQL itself, run by a real VictoriaLogs."""

    async def test_overview_of_one_stack(self, vlogs):
        import uuid
        from datetime import timedelta

        from backend.config import VictoriaLogsConfig
        from backend.log_store import LogStore
        from shared.victorialogs import KIND_ACCESS, access_row

        store = LogStore(VictoriaLogsConfig(url=vlogs))
        router = f"r{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc)
        docs = [{"timestamp": now - timedelta(seconds=i), "host": "edge", "event": "request",
                 "client_ip": f"198.51.100.{i % 5 + 1}", "internal": False, "request_host": "app.test",
                 "method": "GET" if i % 2 else "POST", "path": "/api" if i < 30 else "/health",
                 "uri": "/api", "status": 200 if i % 10 else 500, "router": f"{router}@swarm",
                 "duration_ms": float(i), "size": 100} for i in range(40)]
        await store.client.insert([access_row(d) for d in docs], KIND_ACCESS)
        await store.client.flush()
        result = await store.get_usage_overview(minutes=60, stack="mine", router_stacks={router: "mine"})
        await store.close()

        assert "error" not in result
        totals = result["totals"]
        assert totals["requests"] == 40 and totals["unique_ips"] == 5 and totals["server_errors"] == 4
        assert totals["bytes"] == 4000 and totals["p50"] is not None
        assert [(e["path"], e["requests"], e["stack"]) for e in result["endpoints"]] == [
            ("/api", 30, "mine"), ("/health", 10, "mine")]
        assert sorted(result["endpoints"][0]["methods"]) == ["GET", "POST"]
        assert result["by_stack"][0]["stack"] == "mine" and result["by_stack"][0]["requests"] == 40
        assert {m["key"] for m in result["methods"]} == {"GET", "POST"}


class TestRouterStacks:
    """The Swarm labels that say which stack a Traefik router belongs to."""

    SERVICES = [
        {"Spec": {"Name": "pulsarcd_swarm-manager", "Labels": {
            "com.docker.stack.namespace": "pulsarcd",
            "traefik.enable": "true",
            "traefik.http.routers.pulsarcd.rule": "Host(`logs.example.com`)",
            "traefik.http.routers.pulsarcd.tls.certresolver": "letsencrypt",
            "traefik.http.routers.pulsarcd-ws.rule": "Host(`logs.example.com`)",
            "traefik.http.services.pulsarcd.loadbalancer.server.port": "8000",
            "traefik.http.middlewares.pulsarcd-edge.chain.middlewares": "waf@file",
        }}},
        # No stack namespace: a plain container, not part of any stack.
        {"Spec": {"Name": "loose", "Labels": {
            "traefik.http.routers.loose.rule": "Host(`loose.example.com`)"}}},
        {"Spec": {"Name": "intra-muros_web", "Labels": {
            "com.docker.stack.namespace": "intra-muros",
            "traefik.http.routers.intra-muros.rule": "Host(`intra-muros.fr`)"}}},
        {"Spec": {"Name": "postgresqlcluster_pg-primary", "Labels": {
            "com.docker.stack.namespace": "postgresqlcluster"}}},
    ]

    async def _routers(self, data, status=200):
        client = DockerAPIClient(HostConfig(name="server-a", docker_host="tcp://h:2375"))
        with patch.object(client, "_request", AsyncMock(return_value=(data, status))):
            return await client.get_traefik_routers()

    async def test_routers_map_to_their_stack(self):
        assert await self._routers(self.SERVICES) == {
            "pulsarcd": "pulsarcd",
            "pulsarcd-ws": "pulsarcd",
            "intra-muros": "intra-muros",
        }

    async def test_services_and_middlewares_are_not_routers(self):
        routers = await self._routers(self.SERVICES)
        assert "pulsarcd-edge" not in routers

    async def test_an_unreachable_daemon_maps_nothing(self):
        assert await self._routers(None, status=500) == {}

    async def _ssh_routers(self, results):
        from backend.ssh_client import SSHClient
        client = SSHClient(HostConfig(name="server-a", hostname="server-a", mode="ssh"))
        with patch.object(client, "run_command", AsyncMock(side_effect=results)):
            return await client.get_traefik_routers()

    async def test_ssh_hosts_read_the_same_labels(self):
        labels = "\n".join(json.dumps(s["Spec"]["Labels"]) for s in self.SERVICES)
        routers = await self._ssh_routers([("abc\ndef\n", "", 0), (labels, "", 0)])
        assert routers == {"pulsarcd": "pulsarcd", "pulsarcd-ws": "pulsarcd",
                           "intra-muros": "intra-muros"}

    async def test_ssh_skips_inspect_on_an_empty_swarm(self):
        # `docker service inspect` with no argument is an error, not an empty
        # answer, so the second command must not run at all.
        assert await self._ssh_routers([("\n", "", 0)]) == {}

    async def test_ssh_ignores_a_service_without_labels(self):
        routers = await self._ssh_routers([("abc\n", "", 0), ("null\n", "", 0)])
        assert routers == {}


class TestUsageApi:
    def test_requires_authentication(self, client):
        assert client.get("/api/usage/overview").status_code == 401

    def test_overview(self, client, auth_headers):
        payload = usage.empty_overview(360, True, "pulsarcd", ["pulsarcd"], NOW)
        mock = AsyncMock(return_value=payload)
        with patch.object(api_module.logstore, "get_usage_overview", mock), \
                patch.object(api_module, "_get_router_stacks", AsyncMock(return_value=ROUTER_STACKS)):
            resp = client.get(
                "/api/usage/overview?minutes=360&include_internal=true&stack=pulsarcd",
                headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["stack"] == "pulsarcd"
        mock.assert_awaited_once_with(minutes=360, include_internal=True, stack="pulsarcd",
                                      router_stacks=ROUTER_STACKS)

    def test_an_empty_stack_parameter_means_every_stack(self, client, auth_headers):
        mock = AsyncMock(return_value=usage.empty_overview(60, False, None, [], NOW))
        with patch.object(api_module.logstore, "get_usage_overview", mock), \
                patch.object(api_module, "_get_router_stacks", AsyncMock(return_value={})):
            client.get("/api/usage/overview?stack=", headers=auth_headers)
        assert mock.await_args.kwargs["stack"] is None

    @pytest.mark.parametrize("minutes", [1, 100000])
    def test_window_bounds(self, client, auth_headers, minutes):
        resp = client.get(f"/api/usage/overview?minutes={minutes}", headers=auth_headers)
        assert resp.status_code == 422
