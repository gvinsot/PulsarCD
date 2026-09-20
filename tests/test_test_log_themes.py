"""Theme suggestions are bounded, use configured credentials, and preserve IDs."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import aiohttp
import pytest

from backend import test_log_themes as themes
from backend.config_file import LLMConfig


class FakeResponse:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def iter_chunked(self, size):
        for start in range(0, len(self.body), size):
            yield self.body[start:start + size]


class FakeSession:
    def __init__(self, response):
        self.post = MagicMock(return_value=response)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


@pytest.fixture
def provider(monkeypatch):
    config = LLMConfig(url="https://provider.example/v1", model="configured-model",
                       api_key="secret-provider-key", max_output_tokens=2048)

    def create(content=None, *, body=None, status=200):
        monkeypatch.setattr(themes, "_get_llm_config", lambda: config)
        if body is None:
            body = json.dumps({"choices": [{"message": {"content": content}}]}).encode()
        session = FakeSession(FakeResponse(body, status))
        factory = MagicMock(return_value=session)
        monkeypatch.setattr(themes.aiohttp, "ClientSession", factory)
        return session, factory

    return create


async def test_grouping_uses_configured_model_and_only_metadata(provider):
    session, factory = provider('{"groups":[{"label":"Authentication","entry_ids":["test-1"]}]}')
    entry = {"id": "test-1", "name": "reject expired token", "file": "auth/test_tokens.py",
             "framework": "pytest", "type": "unit", "log": "sensitive raw log", "status": "failed"}
    result = await themes.group_test_themes([entry])

    assert result == {"groups": [{"label": "Authentication", "entry_ids": ["test-1"]}],
                      "model": "configured-model"}
    args, kwargs = session.post.call_args
    assert args == ("https://provider.example/v1/chat/completions",)
    assert kwargs["headers"]["Authorization"] == "Bearer secret-provider-key"
    payload = kwargs["json"]
    assert payload["model"] == "configured-model"
    assert payload["max_tokens"] == 2048
    assert payload["stream"] is False
    assert "tools" not in payload and "tool_choice" not in payload
    assert json.loads(payload["messages"][1]["content"]) == [
        {key: entry[key] for key in ("id", "name", "file", "framework", "type")}]
    assert "sensitive raw log" not in json.dumps(payload)
    assert factory.call_args.kwargs["timeout"].total == 60


async def test_untrusted_response_cannot_invent_or_duplicate_test_memberships(provider):
    result = {"groups": [
        {"label": "  Access\ncontrol  ", "entry_ids": ["a", "a", "invented", 5, {}]},
        {"label": "Other", "entry_ids": ["a", "b"]},
        {"label": "access control", "entry_ids": ["c"]},
        {"label": "", "entry_ids": ["d"]},
        {"label": "Unused", "entry_ids": ["invented"]},
        {"label": ["invalid"], "entry_ids": ["d"]},
        {"label": "Malformed", "entry_ids": "d"},
        None,
    ]}
    provider("```json\n" + json.dumps(result) + "\n```")
    grouped = await themes.group_test_themes([{"id": identifier, "name": "test " + identifier}
                                             for identifier in "abcd"])
    assert grouped["groups"] == [
        {"label": "Access control", "entry_ids": ["a", "c"]},
        {"label": "Other", "entry_ids": ["b"]},
    ]


@pytest.mark.parametrize("entries", [
    None, {}, "[]", [None],
    [{"id": "test"}],
    [{"id": "", "name": "test"}],
    [{"id": 123, "name": "test"}],
    [{"id": "test", "name": "   "}],
    [{"id": "test", "name": "name", "file": {}}],
    [{"id": "test", "name": "x" * 2001}],
    [{"id": "test", "name": "same"}] * 2,
    [{"id": str(i), "name": "test"} for i in range(themes.MAX_THEME_ENTRIES + 1)],
    [{"id": str(i), "name": "é" * 2000} for i in range(30)],
])
async def test_invalid_or_oversized_input_never_calls_provider(provider, entries):
    session, factory = provider()
    with pytest.raises(ValueError):
        await themes.group_test_themes(entries)
    factory.assert_not_called()
    session.post.assert_not_called()


async def test_empty_input_needs_no_provider_or_configuration(monkeypatch):
    configure = MagicMock(side_effect=AssertionError("must not load configuration"))
    monkeypatch.setattr(themes, "_get_llm_config", configure)
    assert await themes.group_test_themes([]) == {"groups": []}
    configure.assert_not_called()


@pytest.mark.parametrize("url,expected", [
    ("https://provider.example/", "https://provider.example/v1/chat/completions"),
    ("https://provider.example/v1", "https://provider.example/v1/chat/completions"),
    ("https://provider.example/v1/chat/completions", "https://provider.example/v1/chat/completions"),
    ("https://provider.example/v1beta/openai/chat/completions",
     "https://provider.example/v1beta/openai/chat/completions"),
])
async def test_supported_provider_endpoint_forms(monkeypatch, provider, url, expected):
    session, _ = provider('{"groups":[]}')
    monkeypatch.setattr(themes, "_get_llm_config", lambda: LLMConfig(url=url))
    assert (await themes.group_test_themes([{"id": "a", "name": "test"}]))["groups"] == []
    assert session.post.call_args.args == (expected,)


@pytest.mark.parametrize("content", [None, "not json", "[]", "{}", '{"groups":{}}'])
async def test_invalid_provider_json_is_not_caller_error(provider, content):
    provider(content)
    with pytest.raises(themes.ThemeGroupingError, match="invalid"):
        await themes.group_test_themes([{"id": "a", "name": "test"}])


@pytest.mark.parametrize("body", [b"invalid provider json", b'{"choices":[]}', b"[]",
                                  b'{"choices":[{"message":{}}]}'])
async def test_invalid_completion_envelope_is_provider_error(provider, body):
    provider(body=body)
    with pytest.raises(themes.ThemeGroupingError, match="invalid"):
        await themes.group_test_themes([{"id": "a", "name": "test"}])


async def test_provider_http_error_never_exposes_response_body(provider):
    provider(body=b"sensitive provider error with secret-provider-key", status=429)
    with pytest.raises(themes.ThemeGroupingError, match="HTTP 429") as error:
        await themes.group_test_themes([{"id": "a", "name": "test"}])
    assert "secret-provider-key" not in str(error.value)


@pytest.mark.parametrize("failure", [aiohttp.ClientConnectionError("secret endpoint"),
                                      asyncio.TimeoutError("secret endpoint")])
async def test_network_failures_are_safe_provider_errors(provider, failure):
    session, _ = provider()
    session.post.side_effect = failure
    with pytest.raises(themes.ThemeGroupingError, match="could not be reached") as error:
        await themes.group_test_themes([{"id": "a", "name": "test"}])
    assert "secret endpoint" not in str(error.value)


async def test_oversized_provider_response_is_rejected(provider):
    provider(body=b" " * (themes.MAX_THEME_RESPONSE_BYTES + 1))
    with pytest.raises(themes.ThemeGroupingError, match="too large"):
        await themes.group_test_themes([{"id": "a", "name": "test"}])


@pytest.mark.parametrize("url", ["", "not-a-url", "ftp://provider.example", "http://[invalid"])
async def test_bad_provider_configuration_is_not_caller_error(monkeypatch, provider, url):
    _, factory = provider()
    monkeypatch.setattr(themes, "_get_llm_config", lambda: LLMConfig(url=url))
    with pytest.raises(themes.ThemeGroupingError, match="not configured"):
        await themes.group_test_themes([{"id": "a", "name": "test"}])
    factory.assert_not_called()


async def test_configuration_changes_apply_to_next_request(monkeypatch, provider):
    from backend import config as config_module

    original_get_config = themes._get_llm_config
    session, _ = provider('{"groups":[]}')
    # Use the actual resolver after preparing the network double.
    monkeypatch.setattr(themes, "_get_llm_config", original_get_config)
    configured = SimpleNamespace(llm=LLMConfig(model="first-model"))
    monkeypatch.setattr(config_module, "settings", SimpleNamespace(pulsar_config=configured))
    entries = [{"id": "a", "name": "test"}]
    assert (await themes.group_test_themes(entries))["model"] == "first-model"
    configured.llm = LLMConfig(model="updated-model", url="https://new-provider.example/v1")
    assert (await themes.group_test_themes(entries))["model"] == "updated-model"
    assert session.post.call_args.args == ("https://new-provider.example/v1/chat/completions",)
