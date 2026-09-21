"""Tests for blocking a client at the edge: validation, rendering and API."""

import json
from unittest.mock import patch

import pytest

import backend.api as api_module
from backend import ip_blocklist as bl
from backend.auth import create_token


def _headers(role: str = "admin", username: str = "blocktest") -> dict:
    token = create_token(username, api_module.settings.auth.jwt_secret, 1, role=role)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def store(tmp_path):
    return bl.IpBlocklist(path=str(tmp_path / "blocked_ips.json"))


@pytest.fixture
def empty_blocklist(client):
    """Leave the shared store as it was found, whatever a test put in it."""
    yield api_module.ip_blocklist
    api_module.ip_blocklist._entries.clear()
    api_module.ip_blocklist._save()


class TestNormalizeTarget:
    @pytest.mark.parametrize("value,expected", [
        ("45.9.12.7", "45.9.12.7"),
        ("  45.9.12.7  ", "45.9.12.7"),
        ("2a01:0e0a::0001", "2a01:e0a::1"),
        ("103.4.8.0/24", "103.4.8.0/24"),
        # A host bit set in a range is a typo the operator meant as a range.
        ("103.4.8.17/24", "103.4.8.0/24"),
        ("2a01:e0a::/32", "2a01:e0a::/32"),
    ])
    def test_canonical_forms(self, value, expected):
        assert bl.normalize_target(value) == expected

    @pytest.mark.parametrize("value", ["", None, "not-an-ip", "45.9.12", "45.9.12.7/99",
                                       "ClientIP(`x`)", "45.9.12.7 || 1.1.1.1"])
    def test_rejected(self, value):
        assert bl.normalize_target(value) is None

    def test_a_single_address_does_not_become_a_32(self):
        """It is what the Security view shows and what the operator typed."""
        assert bl.normalize_target("45.9.12.7") == "45.9.12.7"


class TestValidateTarget:
    def test_public_address(self):
        assert bl.validate_target("45.9.12.7") == "45.9.12.7"

    @pytest.mark.parametrize("value", ["192.168.1.30", "10.0.0.1", "127.0.0.1",
                                       "169.254.1.1", "192.168.1.0/24", "fd00::1"])
    def test_internal_addresses_are_refused(self, value):
        with pytest.raises(ValueError, match="internal"):
            bl.validate_target(value)

    @pytest.mark.parametrize("value", ["0.0.0.0/0", "45.0.0.0/4", "::/0", "2a01::/16"])
    def test_ranges_that_are_too_broad_are_refused(self, value):
        with pytest.raises(ValueError, match="limited to"):
            bl.validate_target(value)

    def test_the_narrowest_allowed_range_passes(self):
        assert bl.validate_target("45.0.0.0/8") == "45.0.0.0/8"
        assert bl.validate_target("2a01:e0a::/32") == "2a01:e0a::/32"

    def test_garbage(self):
        with pytest.raises(ValueError, match="not an IP address"):
            bl.validate_target("drop table")


class TestCovers:
    def test_exact_address(self):
        assert bl.covers("45.9.12.7", "45.9.12.7")
        assert not bl.covers("45.9.12.7", "45.9.12.8")

    def test_range(self):
        assert bl.covers("103.4.8.0/24", "103.4.8.17")
        assert not bl.covers("103.4.8.0/24", "103.4.9.1")

    def test_across_families(self):
        assert not bl.covers("103.4.8.0/24", "2a01:e0a::1")
        assert not bl.covers("2a01:e0a::/32", "103.4.8.1")

    def test_nonsense_is_not_covered(self):
        assert not bl.covers("103.4.8.0/24", "not-an-ip")


class TestTraefikConfig:
    def test_empty_blocklist_yields_an_empty_configuration(self):
        """Not a router with an empty rule: Traefik would refuse it and keep
        enforcing the previous one, so no unblock would ever take effect."""
        assert bl.traefik_config([]) == {}

    def test_one_address(self):
        config = bl.traefik_config(["45.9.12.7"])
        router = config["http"]["routers"][bl.ROUTER_NAME]
        assert router["rule"] == "ClientIP(`45.9.12.7`)"
        assert router["service"] == "noop@internal"
        assert router["middlewares"] == [bl.MIDDLEWARE_NAME]
        assert router["entryPoints"] == ["websecure"]
        # Present, so the router also matches TLS requests on websecure.
        assert router["tls"] == {}
        plain = config["http"]["routers"][bl.HTTP_ROUTER_NAME]
        assert plain["entryPoints"] == ["web"]
        assert plain["rule"] == router["rule"]
        assert plain["middlewares"] == router["middlewares"]
        assert "tls" not in plain

    def test_priority_beats_application_routers_and_loses_to_acme(self):
        router = bl.traefik_config(["45.9.12.7"])["http"]["routers"][bl.ROUTER_NAME]
        assert router["priority"] > 10_000        # any realistic rule length
        assert router["priority"] < 2 ** 31 - 1   # the ACME challenge router

    def test_several_targets_are_or_ed(self):
        rule = bl.traefik_config(["45.9.12.7", "103.4.8.0/24"])["http"]["routers"][bl.ROUTER_NAME]["rule"]
        assert rule == "ClientIP(`45.9.12.7`) || ClientIP(`103.4.8.0/24`)"

    def test_the_middleware_allows_nothing_reachable(self):
        middleware = bl.traefik_config(["45.9.12.7"])["http"]["middlewares"][bl.MIDDLEWARE_NAME]
        assert middleware == {"ipAllowList": {"sourceRange": ["255.255.255.255/32"]}}

    def test_the_configuration_is_json_serializable(self):
        """It is served verbatim to Traefik's HTTP provider."""
        json.dumps(bl.traefik_config(["45.9.12.7", "103.4.8.0/24"]))


class TestStore:
    async def test_add_and_list(self, store):
        entry = await store.add("45.9.12.7", reason="scanner", blocked_by="boss@example.com")
        assert entry["ip"] == "45.9.12.7"
        assert entry["reason"] == "scanner"
        assert entry["blocked_by"] == "boss@example.com"
        assert entry["blocked_at"].endswith("Z")
        assert store.targets() == ["45.9.12.7"]

    async def test_add_normalizes(self, store):
        await store.add("  103.4.8.17/24 ")
        assert store.targets() == ["103.4.8.0/24"]

    async def test_add_refuses_a_duplicate(self, store):
        await store.add("45.9.12.7")
        with pytest.raises(ValueError, match="already blocked"):
            await store.add("45.9.12.7")

    async def test_add_refuses_an_internal_address(self, store):
        with pytest.raises(ValueError, match="internal"):
            await store.add("192.168.1.30")

    async def test_add_stops_at_the_ceiling(self, store):
        with patch.object(bl, "MAX_ENTRIES", 2):
            await store.add("45.9.12.7")
            await store.add("45.9.12.8")
            with pytest.raises(ValueError, match="full"):
                await store.add("45.9.12.9")

    async def test_remove(self, store):
        await store.add("45.9.12.7")
        assert (await store.remove("45.9.12.7"))["ip"] == "45.9.12.7"
        assert store.targets() == []

    async def test_remove_an_address_that_is_not_blocked(self, store):
        with pytest.raises(ValueError, match="is not blocked"):
            await store.remove("45.9.12.7")

    async def test_remove_an_address_covered_by_a_range_names_the_range(self, store):
        await store.add("103.4.8.0/24")
        with pytest.raises(ValueError, match="covered by the range '103.4.8.0/24'"):
            await store.remove("103.4.8.17")

    async def test_blocked_entry_matches_through_a_range(self, store):
        await store.add("103.4.8.0/24")
        assert store.blocked_entry("103.4.8.17")["ip"] == "103.4.8.0/24"
        assert store.blocked_entry("103.4.9.1") is None

    async def test_entries_survive_a_restart(self, store, tmp_path):
        await store.add("45.9.12.7", reason="scanner")
        reloaded = bl.IpBlocklist(path=str(tmp_path / "blocked_ips.json"))
        assert reloaded.targets() == ["45.9.12.7"]
        assert reloaded.list_entries()[0]["reason"] == "scanner"

    def test_a_corrupt_file_blocks_nobody_but_is_reported(self, tmp_path):
        path = tmp_path / "blocked_ips.json"
        path.write_text("{not json", encoding="utf-8")
        assert bl.IpBlocklist(path=str(path)).targets() == []

    def test_malformed_entries_are_skipped(self, tmp_path):
        path = tmp_path / "blocked_ips.json"
        path.write_text(json.dumps([
            {"ip": "45.9.12.7"},
            {"ip": "not-an-ip"},
            {"ip": "45.9.12.7"},          # duplicate
            {"nothing": "useful"},
        ]), encoding="utf-8")
        assert bl.IpBlocklist(path=str(path)).targets() == ["45.9.12.7"]

    async def test_list_entries_is_most_recent_first(self, store):
        await store.add("45.9.12.7")
        await store.add("45.9.12.8")
        ips = [e["ip"] for e in store.list_entries()]
        assert ips[0] == "45.9.12.8"


class TestBlocklistApi:
    def test_requires_authentication(self, client):
        assert client.get("/api/security/blocklist").status_code == 401
        assert client.post("/api/security/blocklist", json={"ip": "45.9.12.7"}).status_code == 401

    def test_block_then_unblock(self, client, empty_blocklist):
        resp = client.post("/api/security/blocklist",
                           json={"ip": "45.9.12.7", "reason": "scanner"},
                           headers=_headers())
        assert resp.status_code == 200
        assert resp.json()["ip"] == "45.9.12.7"
        assert resp.json()["blocked_by"] == "blocktest"

        listed = client.get("/api/security/blocklist", headers=_headers()).json()
        assert [e["ip"] for e in listed["entries"]] == ["45.9.12.7"]

        resp = client.request("DELETE", "/api/security/blocklist?ip=45.9.12.7",
                              headers=_headers())
        assert resp.status_code == 200
        assert client.get("/api/security/blocklist",
                          headers=_headers()).json()["entries"] == []

    def test_a_cidr_range_round_trips_through_the_query_parameter(self, client, empty_blocklist):
        """A path parameter could not carry the slash, even percent-encoded."""
        assert client.post("/api/security/blocklist", json={"ip": "103.4.8.0/24"},
                           headers=_headers()).status_code == 200
        resp = client.request("DELETE", "/api/security/blocklist?ip=103.4.8.0%2F24",
                              headers=_headers())
        assert resp.status_code == 200

    def test_internal_addresses_are_refused(self, client, empty_blocklist):
        resp = client.post("/api/security/blocklist", json={"ip": "192.168.1.30"},
                           headers=_headers())
        assert resp.status_code == 400
        assert "internal" in resp.json()["detail"]

    def test_the_callers_own_address_is_refused(self, client, empty_blocklist):
        """Blocking it would take the unblock button away with it."""
        with patch.object(api_module, "_client_address", return_value="45.9.12.7"):
            resp = client.post("/api/security/blocklist", json={"ip": "45.9.12.7"},
                               headers=_headers())
        assert resp.status_code == 400
        assert "lock you out" in resp.json()["detail"]

    def test_a_range_covering_the_callers_own_address_is_refused(self, client, empty_blocklist):
        with patch.object(api_module, "_client_address", return_value="103.4.8.17"):
            resp = client.post("/api/security/blocklist", json={"ip": "103.4.8.0/24"},
                               headers=_headers())
        assert resp.status_code == 400

    def test_an_unrelated_caller_address_does_not_block_the_request(self, client, empty_blocklist):
        with patch.object(api_module, "_client_address", return_value="10.0.0.5"):
            resp = client.post("/api/security/blocklist", json={"ip": "45.9.12.7"},
                               headers=_headers())
        assert resp.status_code == 200

    def test_invalid_body(self, client, empty_blocklist):
        resp = client.post("/api/security/blocklist", json={"ip": 42}, headers=_headers())
        assert resp.status_code == 400

    def test_unblocking_an_address_that_is_not_blocked(self, client, empty_blocklist):
        resp = client.request("DELETE", "/api/security/blocklist?ip=45.9.12.7",
                              headers=_headers())
        assert resp.status_code == 400

    @pytest.mark.parametrize("method,url,kwargs", [
        ("post", "/api/security/blocklist", {"json": {"ip": "45.9.12.7"}}),
        ("delete", "/api/security/blocklist?ip=45.9.12.7", {}),
    ])
    def test_viewers_cannot_block(self, client, method, url, kwargs):
        resp = getattr(client, method)(url, headers=_headers(role="viewer"), **kwargs)
        assert resp.status_code == 403

    def test_viewers_can_read_the_list(self, client, empty_blocklist):
        assert client.get("/api/security/blocklist",
                          headers=_headers(role="viewer")).status_code == 200

    def test_the_overview_marks_the_blocked_clients(self, client, empty_blocklist):
        from datetime import datetime
        from unittest.mock import AsyncMock
        from backend import security_analytics as sec

        payload = sec.empty_overview(60, False, datetime(2026, 9, 19))
        payload["ips"] = [{"ip": "45.9.12.7"}, {"ip": "103.4.8.17"}, {"ip": "8.8.8.8"}]
        client.post("/api/security/blocklist", json={"ip": "45.9.12.7"}, headers=_headers())
        client.post("/api/security/blocklist", json={"ip": "103.4.8.0/24"}, headers=_headers())

        with patch.object(api_module.opensearch, "get_security_overview",
                          AsyncMock(return_value=payload)):
            ips = client.get("/api/security/overview", headers=_headers()).json()["ips"]

        assert ips[0]["blocked"]["ip"] == "45.9.12.7"
        # Covered by the range, which is what the view must offer to release.
        assert ips[1]["blocked"]["ip"] == "103.4.8.0/24"
        assert ips[2]["blocked"] is None


class TestTraefikConfigEndpoint:
    PATH = "/api/security/waf/traefik-config"

    def test_without_an_edge_key_the_endpoint_is_closed(self, client):
        with patch.object(api_module.settings.auth, "edge_key", ""):
            resp = client.get(self.PATH)
        assert resp.status_code == 503

    def test_a_session_token_is_not_an_edge_key(self, client):
        """The route authenticates the proxy, never a signed-in user."""
        with patch.object(api_module.settings.auth, "edge_key", "k" * 32):
            resp = client.get(self.PATH, headers=_headers())
        assert resp.status_code == 401

    def test_wrong_and_missing_keys(self, client):
        with patch.object(api_module.settings.auth, "edge_key", "k" * 32):
            assert client.get(self.PATH).status_code == 401
            assert client.get(self.PATH, headers={"Authorization": "Bearer nope"}
                              ).status_code == 401

    def test_serves_the_blocklist(self, client, empty_blocklist):
        client.post("/api/security/blocklist", json={"ip": "45.9.12.7"}, headers=_headers())
        with patch.object(api_module.settings.auth, "edge_key", "k" * 32):
            resp = client.get(self.PATH, headers={"Authorization": f"Bearer {'k' * 32}"})
        assert resp.status_code == 200
        assert resp.json()["http"]["routers"][bl.ROUTER_NAME]["rule"] == "ClientIP(`45.9.12.7`)"

    def test_an_empty_blocklist_still_answers(self, client, empty_blocklist):
        with patch.object(api_module.settings.auth, "edge_key", "k" * 32):
            resp = client.get(self.PATH, headers={"Authorization": f"Bearer {'k' * 32}"})
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_uninitialized_store_returns_a_valid_empty_configuration(self, client):
        with patch.object(api_module.settings.auth, "edge_key", "k" * 32), \
                patch.object(api_module, "ip_blocklist", None):
            resp = client.get(self.PATH, headers={"Authorization": f"Bearer {'k' * 32}"})
        assert resp.status_code == 200
        assert resp.json() == {}
