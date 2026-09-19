"""Tests for the security view: detection rules, query shape and API."""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

import backend.api as api_module
from backend import security_analytics as sec


def _ip(**overrides):
    ip = {
        "requests": 100, "client_errors": 0, "not_found": 0, "denied": 0,
        "rate_limited": 0, "server_errors": 0, "waf_blocks": 0,
        "distinct_paths": 3, "peak_per_minute": 10,
    }
    ip.update(overrides)
    return ip


def _endpoint(**overrides):
    ep = {"requests": 100, "distinct_ips": 2, "client_errors": 0, "server_errors": 0,
          "top_ips": [{"key": "1.2.3.4", "count": 10}], "peak_per_minute": 10}
    ep.update(overrides)
    return ep


def _codes(flags):
    return {f["code"] for f in flags}


class TestIpFlags:
    def test_normal_client_has_no_flag(self):
        assert sec.ip_flags(_ip()) == []

    def test_flood_and_burst(self):
        assert _codes(sec.ip_flags(_ip(peak_per_minute=sec.FLOOD_PEAK_PER_MINUTE))) == {"flood"}
        assert _codes(sec.ip_flags(_ip(peak_per_minute=sec.BURST_PEAK_PER_MINUTE))) == {"burst"}

    def test_scanner_needs_many_404_on_many_paths(self):
        many_404_same_path = _ip(not_found=500, client_errors=500, requests=5000, distinct_paths=1)
        assert "scanner" not in _codes(sec.ip_flags(many_404_same_path))
        scanner = _ip(not_found=sec.SCANNER_MIN_NOT_FOUND, client_errors=40,
                      distinct_paths=sec.SCANNER_MIN_DISTINCT_PATHS)
        assert "scanner" in _codes(sec.ip_flags(scanner))

    def test_waf_denied_rate_limited(self):
        flags = sec.ip_flags(_ip(waf_blocks=sec.WAF_MIN_BLOCKS, denied=sec.DENIED_MIN,
                                 rate_limited=sec.RATE_LIMITED_MIN))
        assert _codes(flags) == {"waf", "denied", "rate_limited"}

    def test_mostly_errors(self):
        assert "errors" in _codes(sec.ip_flags(_ip(requests=50, client_errors=45)))
        assert "errors" not in _codes(sec.ip_flags(_ip(requests=10, client_errors=10)))


class TestEndpointFlags:
    def test_normal_endpoint_has_no_flag(self):
        assert sec.endpoint_flags(_endpoint()) == []

    def test_hammered(self):
        assert "hammered" in _codes(sec.endpoint_flags(_endpoint(peak_per_minute=400)))

    def test_concentrated_on_one_client(self):
        ep = _endpoint(requests=1000, top_ips=[{"key": "1.2.3.4", "count": 900}])
        assert "concentrated" in _codes(sec.endpoint_flags(ep))

    def test_probed_by_many_clients(self):
        ep = _endpoint(requests=100, distinct_ips=20, client_errors=95)
        assert "probed" in _codes(sec.endpoint_flags(ep))


class TestQueries:
    START = datetime(2026, 9, 19, 17, 0, 0)

    def test_internal_traffic_is_excluded_by_default(self):
        body = sec.build_overview_query(self.START, self.START, 60, include_internal=False)
        assert {"term": {"internal": False}} in body["query"]["bool"]["filter"]
        body = sec.build_overview_query(self.START, self.START, 60, include_internal=True)
        assert {"term": {"internal": False}} not in body["query"]["bool"]["filter"]

    @pytest.mark.parametrize("minutes", [5, 60, 360, 361, 1440, 1441, sec.MAX_WINDOW_MINUTES])
    def test_peak_histograms_stay_small(self, minutes):
        # ~100 candidate IPs x buckets per IP must stay far below max_buckets.
        _, bucket_minutes = sec.rate_interval(minutes)
        assert minutes / bucket_minutes <= 360

    @pytest.mark.parametrize("minutes", [5, 60, 360, 1440, sec.MAX_WINDOW_MINUTES])
    def test_timeline_stays_readable(self, minutes):
        step = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}[sec.timeline_interval(minutes)]
        assert minutes / step <= 200

    def test_ip_stats_query_is_limited_to_candidates(self):
        body = sec.build_ip_stats_query(self.START, 60, False, ["1.2.3.4", "5.6.7.8"])
        assert {"terms": {"client_ip": ["1.2.3.4", "5.6.7.8"]}} in body["query"]["bool"]["filter"]
        assert body["aggs"]["ips"]["terms"]["size"] == 2

    def test_clamp_window(self):
        assert sec.clamp_window(1) == sec.MIN_WINDOW_MINUTES
        assert sec.clamp_window(10**6) == sec.MAX_WINDOW_MINUTES
        assert sec.clamp_window("junk") == sec.DEFAULT_WINDOW_MINUTES


def _terms(*pairs):
    return {"buckets": [{"key": k, "doc_count": n} for k, n in pairs]}


OVERVIEW_RESPONSE = {
    "aggregations": {
        "requests": {
            "doc_count": 1200,
            "unique_ips": {"value": 3},
            "statuses": {"buckets": [
                {"key": "ok", "doc_count": 400},
                {"key": "client_errors", "doc_count": 790},
                {"key": "server_errors", "doc_count": 10},
            ]},
            "denied": {"doc_count": 40},
            "rate_limited": {"doc_count": 5},
            "timeline": {"buckets": [{
                "key_as_string": "2026-09-19T17:00:00.000Z", "doc_count": 1200,
                "statuses": {"buckets": [
                    {"key": "ok", "doc_count": 400},
                    {"key": "client_errors", "doc_count": 790},
                    {"key": "server_errors", "doc_count": 10},
                ]},
            }]},
            "by_requests": _terms(("98.82.0.52", 700), ("8.8.8.8", 400)),
            "by_errors": {"doc_count": 790, "ips": _terms(("98.82.0.52", 700), ("45.148.10.38", 90))},
            "endpoints": {"buckets": [{
                "key": ["sowarm.ai", "/static.zip"], "doc_count": 500,
                "distinct_ips": {"value": 1},
                "client_errors": {"doc_count": 500},
                "server_errors": {"doc_count": 0},
                "top_ips": _terms(("98.82.0.52", 500)),
                "peak": {"value": 497.0},
            }]},
        },
        "waf": {
            "doc_count": 12,
            "ips": _terms(("45.148.10.38", 12)),
            "rules": _terms(("SQL Injection Attempt", 12)),
        },
    },
}


def _ip_bucket(ip, requests, peak, **counts):
    return {
        "key": ip, "doc_count": requests,
        "requests": {
            "doc_count": requests,
            "client_errors": {"doc_count": counts.get("client_errors", 0)},
            "not_found": {"doc_count": counts.get("not_found", 0)},
            "denied": {"doc_count": 0},
            "rate_limited": {"doc_count": 0},
            "server_errors": {"doc_count": 0},
            "distinct_paths": {"value": counts.get("distinct_paths", 1)},
            "distinct_hosts": {"value": 1},
            "top_hosts": _terms(("sowarm.ai", requests)),
            "top_paths": _terms(("/", requests)),
            "user_agents": _terms(("curl/8", requests)),
            "peak": {"value": peak},
        },
        "waf_blocks": {"doc_count": counts.get("waf_blocks", 0)},
        "internal": {"buckets": [{"key": 0, "key_as_string": "false", "doc_count": requests}]},
        "first_seen": {"value_as_string": "2026-09-19T17:01:00.000Z"},
        "last_seen": {"value_as_string": "2026-09-19T17:02:00.000Z"},
    }


IP_STATS_RESPONSE = {"aggregations": {"ips": {"buckets": [
    _ip_bucket("8.8.8.8", 400, 20.0),
    _ip_bucket("98.82.0.52", 700, 497.0, client_errors=700, not_found=700, distinct_paths=300),
    _ip_bucket("45.148.10.38", 90, 30.0, client_errors=90, not_found=5, waf_blocks=12),
]}}}


class TestSummarize:
    NOW = datetime(2026, 9, 19, 18, 0, 0)

    def test_candidates_are_merged_without_duplicates(self):
        assert sec.candidate_ips(OVERVIEW_RESPONSE) == ["98.82.0.52", "8.8.8.8", "45.148.10.38"]

    def test_payload(self):
        result = sec.summarize(OVERVIEW_RESPONSE, IP_STATS_RESPONSE, minutes=60,
                               include_internal=False, generated_at=self.NOW)
        assert result["totals"] == {
            "requests": 1200, "unique_ips": 3, "client_errors": 790, "server_errors": 10,
            "denied": 40, "rate_limited": 5, "waf_blocks": 12,
            "flagged_ips": 2, "flagged_endpoints": 1,
        }
        assert result["timeline"] == [{"timestamp": "2026-09-19T17:00:00.000Z",
                                       "ok": 400, "client_errors": 790, "server_errors": 10}]
        # Flagged clients first, most severe first; the quiet one last.
        assert [ip["ip"] for ip in result["ips"]] == ["98.82.0.52", "45.148.10.38", "8.8.8.8"]
        scanner = result["ips"][0]
        assert _codes(scanner["flags"]) == {"flood", "scanner", "errors"}
        assert scanner["user_agent"] == "curl/8"
        assert scanner["internal"] is False
        endpoint = result["endpoints"][0]
        assert (endpoint["host"], endpoint["path"]) == ("sowarm.ai", "/static.zip")
        assert _codes(endpoint["flags"]) == {"hammered", "concentrated"}
        assert result["waf_rules"] == [{"key": "SQL Injection Attempt", "count": 12}]

    def test_long_windows_report_a_per_minute_peak(self):
        # 24 h window: 5-minute buckets, so a 1500-request bucket is 300/min.
        stats = {"aggregations": {"ips": {"buckets": [_ip_bucket("8.8.8.8", 1500, 1500.0)]}}}
        result = sec.summarize(OVERVIEW_RESPONSE, stats, minutes=1440,
                               include_internal=False, generated_at=self.NOW)
        assert result["ips"][0]["peak_per_minute"] == 300.0

    def test_no_candidates(self):
        result = sec.summarize({"aggregations": {}}, None, minutes=60,
                               include_internal=True, generated_at=self.NOW)
        assert result["ips"] == [] and result["endpoints"] == []
        assert result["totals"]["requests"] == 0
        assert result["include_internal"] is True


class TestOpenSearchClient:
    async def test_second_search_only_with_candidates(self):
        from backend.config import OpenSearchConfig
        from backend.opensearch_client import OpenSearchClient

        client = OpenSearchClient(OpenSearchConfig(hosts=["http://localhost:9200"]))
        search = AsyncMock(side_effect=[OVERVIEW_RESPONSE, IP_STATS_RESPONSE])
        with patch.object(client._client, "search", search):
            result = await client.get_security_overview(minutes=60)
        await client.close()
        assert search.await_count == 2
        assert all(c.kwargs["index"] == "pulsarcd-access" for c in search.await_args_list)
        stats_filters = search.await_args_list[1].kwargs["body"]["query"]["bool"]["filter"]
        assert {"terms": {"client_ip": ["98.82.0.52", "8.8.8.8", "45.148.10.38"]}} in stats_filters
        assert result["totals"]["flagged_ips"] == 2

    async def test_missing_index_returns_an_empty_overview(self):
        from backend.config import OpenSearchConfig
        from backend.opensearch_client import OpenSearchClient

        client = OpenSearchClient(OpenSearchConfig(hosts=["http://localhost:9200"]))
        with patch.object(client._client, "search", AsyncMock(side_effect=Exception("index_not_found"))):
            result = await client.get_security_overview(minutes=60)
        await client.close()
        assert result["error"] == "Access data unavailable"
        assert result["ips"] == []


class TestSecurityApi:
    def test_requires_authentication(self, client):
        assert client.get("/api/security/overview").status_code == 401

    def test_overview(self, client, auth_headers):
        payload = sec.empty_overview(360, True, datetime(2026, 9, 19))
        mock = AsyncMock(return_value=payload)
        with patch.object(api_module.opensearch, "get_security_overview", mock):
            resp = client.get("/api/security/overview?minutes=360&include_internal=true",
                              headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["window_minutes"] == 360
        mock.assert_awaited_once_with(minutes=360, include_internal=True)

    @pytest.mark.parametrize("minutes", [1, 100000])
    def test_window_bounds(self, client, auth_headers, minutes):
        resp = client.get(f"/api/security/overview?minutes={minutes}", headers=auth_headers)
        assert resp.status_code == 422

    def test_ip_events(self, client, auth_headers):
        events = [{"timestamp": "2026-09-19T17:00:00", "event": "request", "status": 404}]
        mock = AsyncMock(return_value=events)
        with patch.object(api_module.opensearch, "get_security_ip_events", mock):
            resp = client.get("/api/security/ips/2a01:0e0a::0001?minutes=60", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == {"ip": "2a01:e0a::1", "internal": False, "events": events}
        mock.assert_awaited_once_with("2a01:e0a::1", minutes=60)

    def test_invalid_ip(self, client, auth_headers):
        resp = client.get("/api/security/ips/not-an-ip", headers=auth_headers)
        assert resp.status_code == 400
