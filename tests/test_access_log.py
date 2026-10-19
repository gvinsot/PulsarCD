"""Tests for shared.access_log and the agent's access-record writes."""

import json
from unittest.mock import AsyncMock

import pytest

from shared.access_log import (
    EVENT_REQUEST, EVENT_WAF_BLOCK, build_access_doc, is_internal_ip, normalize_ip,
)
from shared.victorialogs import KIND_ACCESS, KIND_LOG, access_row
from shared.log_utils import parse_log_message


TRAEFIK_LINE = json.dumps({
    "ClientAddr": "80.94.95.211:52053", "ClientHost": "80.94.95.211", "ClientPort": "52053",
    "DownstreamContentSize": 19, "DownstreamStatus": 404, "Duration": 2506383,
    "OriginStatus": 0, "Overhead": 17423, "RequestAddr": "sowarm.ai",
    "RequestHost": "sowarm.ai", "RequestMethod": "GET",
    "RequestPath": "/.git/config?x=1", "RequestProtocol": "HTTP/1.1",
    "RequestScheme": "https", "RouterName": "sowarm@swarm",
    "entryPointName": "websecure", "level": "info", "msg": "",
    "request_User-Agent": "Mozilla/5.0 (scanner)", "time": "2026-09-19T18:14:08Z",
})

CORAZA_LINE = (
    '2026-08-30T13:09:46Z ERR [client "45.148.10.38"] Coraza: Access denied (phase 2). '
    'Remote File Inclusion Attempt [file "_inline_"] [line "14"] [id "1003"] [rev ""] '
    '[msg "Remote File Inclusion Attempt"] [data ""] [severity "emergency"] [ver ""] '
    '[maturity "0"] [accuracy "0"] [hostname ""] [uri "/?redirect=https%3A%2F%2Fevil.com"] '
    '[unique_id "PybAiefQoqZsCGkKFdv"] entryPointName=websecure middlewareName=waf@file '
    'middlewareType=wasm routerName=ai-friendly@swarm'
)


def _entry(message):
    level, http_status, parsed_fields = parse_log_message(message)
    return {
        "timestamp": "2026-09-19T18:14:08.279129",
        "host": "server-a",
        "container_id": "abc",
        "container_name": "privatenetwork_traefik.1.xyz",
        "compose_project": "privatenetwork",
        "compose_service": "traefik",
        "stream": "stdout",
        "message": message,
        "level": level,
        "http_status": http_status,
        "parsed_fields": parsed_fields,
    }


class TestTraefikRequest:
    def test_fields_are_extracted(self):
        doc = build_access_doc(_entry(TRAEFIK_LINE))
        assert doc["event"] == EVENT_REQUEST
        assert doc["client_ip"] == "80.94.95.211"
        assert doc["internal"] is False
        assert doc["request_host"] == "sowarm.ai"
        assert doc["method"] == "GET"
        assert doc["status"] == 404
        assert doc["router"] == "sowarm@swarm"
        assert doc["entrypoint"] == "websecure"
        assert doc["user_agent"] == "Mozilla/5.0 (scanner)"
        assert doc["duration_ms"] == pytest.approx(2.506, abs=0.001)
        assert doc["size"] == 19
        assert doc["timestamp"] == "2026-09-19T18:14:08.279129"
        assert doc["host"] == "server-a"

    def test_path_drops_query_string_but_uri_keeps_it(self):
        doc = build_access_doc(_entry(TRAEFIK_LINE))
        assert doc["path"] == "/.git/config"
        assert doc["uri"] == "/.git/config?x=1"

    def test_unmatched_host_has_no_router(self):
        fields = json.loads(TRAEFIK_LINE)
        del fields["RouterName"]
        doc = build_access_doc(_entry(json.dumps(fields)))
        assert doc["router"] is None

    def test_missing_host_header_still_counts_as_an_endpoint(self):
        fields = json.loads(TRAEFIK_LINE)
        fields["RequestHost"] = ""
        assert build_access_doc(_entry(json.dumps(fields)))["request_host"] == "-"

    def test_private_client_is_internal(self):
        fields = json.loads(TRAEFIK_LINE)
        fields["ClientHost"] = "192.168.1.254"
        assert build_access_doc(_entry(json.dumps(fields)))["internal"] is True

    def test_unparseable_client_is_skipped(self):
        fields = json.loads(TRAEFIK_LINE)
        fields["ClientHost"] = "-"
        assert build_access_doc(_entry(json.dumps(fields))) is None

    def test_other_json_logs_are_ignored(self):
        line = json.dumps({"level": "info", "msg": "request", "status": 200, "path": "/"})
        assert build_access_doc(_entry(line)) is None

    def test_plain_log_lines_are_ignored(self):
        assert build_access_doc(_entry("2026-09-19T18:00:00Z INF Traefik started")) is None


class TestCorazaBlock:
    def test_fields_are_extracted(self):
        doc = build_access_doc(_entry(CORAZA_LINE))
        assert doc["event"] == EVENT_WAF_BLOCK
        assert doc["client_ip"] == "45.148.10.38"
        assert doc["rule_id"] == "1003"
        assert doc["rule_msg"] == "Remote File Inclusion Attempt"
        assert doc["path"] == "/"
        assert doc["uri"] == "/?redirect=https%3A%2F%2Fevil.com"
        assert doc["router"] == "ai-friendly@swarm"
        # The same request is also in the Traefik access log: the WAF event
        # must not be counted as a request with a status.
        assert "status" not in doc

    def test_line_without_client_is_skipped(self):
        assert build_access_doc(_entry(CORAZA_LINE.replace('[client "45.148.10.38"] ', ""))) is None


class TestAddresses:
    def test_ipv6_is_normalized(self):
        assert normalize_ip(" 2001:0db8::0001 ") == "2001:db8::1"

    def test_invalid_address(self):
        assert normalize_ip("not-an-ip") is None
        assert normalize_ip(None) is None

    @pytest.mark.parametrize("ip,internal", [
        ("10.0.1.208", True), ("172.18.0.1", True), ("127.0.0.1", True),
        ("fe80::1", True), ("82.64.64.144", False), ("2a01:e0a::1", False),
    ])
    def test_internal_ranges(self, ip, internal):
        assert is_internal_ip(ip) is internal


def test_access_record_keeps_every_field():
    for line in (TRAEFIK_LINE, CORAZA_LINE):
        doc = build_access_doc(_entry(line))
        row = access_row(doc)
        assert row["kind"] == KIND_ACCESS
        assert row["_time"] == "2026-09-19T18:14:08.279129Z"
        assert row["_msg"]
        # Empty values are dropped (VictoriaLogs treats them as missing).
        expected = {k: v for k, v in doc.items() if k != "timestamp" and v not in (None, "")}
        expected["internal"] = "true" if doc["internal"] else "false"
        assert {k: row[k] for k in expected} == expected


class TestAgentWriter:
    def _writer(self):
        from agent.config import VictoriaLogsConfig
        from agent.victorialogs_writer import VictoriaLogsWriter

        writer = VictoriaLogsWriter(VictoriaLogsConfig(), host_name="server-a")
        writer.client.insert = AsyncMock(side_effect=lambda rows, kind: len(rows))
        return writer

    async def test_access_records_follow_the_log_lines(self):
        writer = self._writer()
        indexed = await writer.index_logs([
            _entry(TRAEFIK_LINE),
            _entry("plain application log line"),
            _entry(CORAZA_LINE),
        ])
        await writer.close()

        assert indexed == 3  # log lines only
        calls = writer.client.insert.await_args_list
        assert [c.args[1] for c in calls] == [KIND_LOG, KIND_ACCESS]
        assert {row["kind"] for row in calls[0].args[0]} == {KIND_LOG}
        assert [row["event"] for row in calls[1].args[0]] == [EVENT_REQUEST, EVENT_WAF_BLOCK]
        # parsed_fields is stored whole, as a JSON string
        assert json.loads(calls[0].args[0][0]["parsed_fields"])["ClientHost"] == "80.94.95.211"

    async def test_no_access_write_without_traefik_lines(self):
        writer = self._writer()
        await writer.index_logs([_entry("plain application log line")])
        await writer.close()
        assert writer.client.insert.await_count == 1
