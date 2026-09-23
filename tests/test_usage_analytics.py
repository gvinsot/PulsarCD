"""Tests for the usage view: query shape, interpretation and API."""

import json
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

import backend.api as api_module
from backend import usage_analytics as usage
from backend.config import HostConfig
from backend.docker_client import DockerAPIClient


NOW = datetime(2026, 9, 22, 20, 0, 0)
START = datetime(2026, 9, 22, 19, 0, 0)

ROUTER_STACKS = {
    "pulsarcd": "pulsarcd",
    "pulsarcd-ws": "pulsarcd",
    "intra-muros": "intra-muros",
}


def _traffic(doc_count, ips=1, client_errors=0, server_errors=0, avg=None, size=0):
    return {
        "doc_count": doc_count,
        "distinct_ips": {"value": ips},
        "client_errors": {"doc_count": client_errors},
        "server_errors": {"doc_count": server_errors},
        "avg_duration": {"value": avg},
        "bytes": {"value": size},
    }


def _path_bucket(path, doc_count, router="pulsarcd@swarm", **kwargs):
    bucket = _traffic(doc_count, **kwargs)
    bucket.update({
        "key": path,
        "max_duration": {"value": 120.0},
        "methods": {"buckets": [{"key": "GET", "doc_count": doc_count}]},
        "router": {"buckets": [{"key": router, "doc_count": doc_count}] if router else []},
        "last_seen": {"value_as_string": "2026-09-22T19:58:00.000Z"},
    })
    return bucket


def _host_bucket(host, doc_count, paths, **kwargs):
    bucket = _traffic(doc_count, **kwargs)
    bucket.update({
        "key": host,
        "distinct_paths": {"value": len(paths)},
        "paths": {"buckets": paths},
    })
    return bucket


RESPONSE = {
    "hits": {"total": {"value": 300, "relation": "eq"}},
    "aggregations": {
        "unique_ips": {"value": 42},
        "distinct_paths": {"value": 17},
        "distinct_hosts": {"value": 2},
        "statuses": {"buckets": [
            {"key": "ok", "doc_count": 240},
            {"key": "client_errors", "doc_count": 55},
            {"key": "server_errors", "doc_count": 5},
        ]},
        "bytes": {"value": 1048576.0},
        "avg_duration": {"value": 12.345},
        "duration": {"values": {"50.0": 2.5, "95.0": 40.0, "99.0": None}},
        "timeline": {"buckets": [{
            "key_as_string": "2026-09-22T19:00:00.000Z",
            "doc_count": 300,
            "avg_duration": {"value": 12.3456},
            "statuses": {"buckets": [
                {"key": "ok", "doc_count": 240},
                {"key": "client_errors", "doc_count": 55},
                {"key": "server_errors", "doc_count": 5},
            ]},
        }]},
        "hosts": {"buckets": [
            _host_bucket("logs.methodinfo.fr", 200, [
                _path_bucket("/api/logs/search", 150, ips=8, client_errors=3, avg=30.0, size=900),
                _path_bucket("/.env", 50, ips=40, client_errors=50, avg=1.0),
            ], ips=44, client_errors=53, avg=22.0, size=900),
            _host_bucket("intra-muros.fr", 100, [
                _path_bucket("/", 100, router="intra-muros@swarm", ips=20, avg=8.0, size=500),
            ], ips=20, avg=8.0, size=500),
        ]},
        "routers": {"buckets": [
            {"key": "pulsarcd@swarm", **_traffic(150, ips=8, client_errors=3, avg=30.0, size=900)},
            {"key": "intra-muros@swarm", **_traffic(100, ips=20, avg=8.0, size=500)},
            {"key": "pulsarcd-ws@swarm", **_traffic(10, ips=2, avg=10.0)},
            {"key": "", **_traffic(40, ips=30, client_errors=40, avg=0.5)},
        ]},
        "methods": {"buckets": [{"key": "GET", "doc_count": 280}, {"key": "POST", "doc_count": 20}]},
        "status_codes": {"buckets": [{"key": 200, "doc_count": 240}, {"key": 404, "doc_count": 55}]},
    },
}


def _summarize(response=RESPONSE, stack=None, minutes=60):
    return usage.summarize(response, minutes=minutes, include_internal=False, stack=stack,
                           stacks=sorted(set(ROUTER_STACKS.values())),
                           router_stacks=ROUTER_STACKS, generated_at=NOW)


class TestQuery:
    def test_only_requests_are_counted(self):
        body = usage.build_usage_query(START, NOW, 60, False, None)
        filters = body["query"]["bool"]["filter"]
        assert {"term": {"event": "request"}} in filters
        assert {"term": {"internal": False}} in filters
        # Every share is computed against the hit count, so it must be exact.
        assert body["track_total_hits"] is True

    def test_internal_traffic_can_be_included(self):
        body = usage.build_usage_query(START, NOW, 60, True, None)
        assert {"term": {"internal": False}} not in body["query"]["bool"]["filter"]

    def test_no_stack_filter_by_default(self):
        body = usage.build_usage_query(START, NOW, 60, False, None)
        assert all("bool" not in f for f in body["query"]["bool"]["filter"])

    def test_stack_filter_matches_every_provider_suffix(self):
        body = usage.build_usage_query(START, NOW, 60, False, ["pulsarcd", "pulsarcd-ws"])
        assert usage.router_filter(["pulsarcd", "pulsarcd-ws"]) in body["query"]["bool"]["filter"]
        assert {"prefix": {"router": "pulsarcd@"}} in usage.router_filter(["pulsarcd"])["bool"]["should"]

    def test_a_stack_without_a_router_matches_nothing(self):
        matched = usage.router_filter([])
        assert matched["bool"]["should"] == [] and matched["bool"]["minimum_should_match"] == 1

    def test_endpoints_are_nested_under_hosts(self):
        # A multi_terms over (host, path) trips the request circuit breaker on
        # a multi-day window; the nested form is deliberate.
        aggs = usage.build_usage_query(START, NOW, 1440, False, None)["aggs"]
        assert "multi_terms" not in str(aggs)
        assert aggs["hosts"]["aggs"]["paths"]["terms"]["field"] == "path"

    def test_timeline_follows_the_window(self):
        assert usage.build_usage_query(START, NOW, 60, False, None)["aggs"]["timeline"][
            "date_histogram"]["fixed_interval"] == "1m"
        assert usage.build_usage_query(START, NOW, 10080, False, None)["aggs"]["timeline"][
            "date_histogram"]["fixed_interval"] == "1h"


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


class TestEndpoints:
    def test_endpoints_of_every_host_are_ranked_together(self):
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
        assert endpoint["last_seen"] == "2026-09-22T19:58:00.000Z"

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


class TestOpenSearchClient:
    def _client(self):
        from backend.config import OpenSearchConfig
        from backend.opensearch_client import OpenSearchClient
        return OpenSearchClient(OpenSearchConfig(hosts=["http://localhost:9200"]))

    async def test_a_stack_selects_its_routers(self):
        client = self._client()
        search = AsyncMock(return_value=RESPONSE)
        with patch.object(client._client, "search", search):
            result = await client.get_usage_overview(minutes=60, stack="pulsarcd",
                                                     router_stacks=ROUTER_STACKS)
        await client.close()
        assert search.await_args.kwargs["index"] == "pulsarcd-access"
        should = search.await_args.kwargs["body"]["query"]["bool"]["filter"][-1]["bool"]["should"]
        assert sorted(s["prefix"]["router"] for s in should) == ["pulsarcd-ws@", "pulsarcd@"]
        assert result["stack"] == "pulsarcd"
        assert result["stacks"] == ["intra-muros", "pulsarcd"]

    async def test_no_stack_queries_everything(self):
        client = self._client()
        search = AsyncMock(return_value=RESPONSE)
        with patch.object(client._client, "search", search):
            await client.get_usage_overview(minutes=60, router_stacks=ROUTER_STACKS)
        await client.close()
        assert len(search.await_args.kwargs["body"]["query"]["bool"]["filter"]) == 3

    async def test_an_unknown_stack_returns_no_request(self):
        # Rather than silently reporting the whole platform under its name.
        client = self._client()
        search = AsyncMock(return_value=RESPONSE)
        with patch.object(client._client, "search", search):
            await client.get_usage_overview(minutes=60, stack="gone",
                                            router_stacks=ROUTER_STACKS)
        await client.close()
        assert search.await_args.kwargs["body"]["query"]["bool"]["filter"][-1]["bool"]["should"] == []

    async def test_missing_index_returns_an_empty_overview(self):
        client = self._client()
        with patch.object(client._client, "search", AsyncMock(side_effect=Exception("index_not_found"))):
            result = await client.get_usage_overview(minutes=60, router_stacks=ROUTER_STACKS)
        await client.close()
        assert result["error"] == "Access data unavailable"
        assert result["endpoints"] == [] and result["stacks"] == ["intra-muros", "pulsarcd"]


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
        with patch.object(api_module.opensearch, "get_usage_overview", mock), \
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
        with patch.object(api_module.opensearch, "get_usage_overview", mock), \
                patch.object(api_module, "_get_router_stacks", AsyncMock(return_value={})):
            client.get("/api/usage/overview?stack=", headers=auth_headers)
        assert mock.await_args.kwargs["stack"] is None

    @pytest.mark.parametrize("minutes", [1, 100000])
    def test_window_bounds(self, client, auth_headers, minutes):
        resp = client.get(f"/api/usage/overview?minutes={minutes}", headers=auth_headers)
        assert resp.status_code == 422
