"""Authenticated action-specific access to optional LLM theme grouping."""

import json
from unittest.mock import AsyncMock

import pytest

from backend import api
from backend import test_log_themes as themes
from backend.auth import create_token
from backend.pipeline_state import PipelineStateManager


ENTRIES = [{"id": "test-1", "name": "reject expired token", "framework": "pytest"}]
RESULT = {"groups": [{"label": "Authentication", "entry_ids": ["test-1"]}], "model": "configured"}


@pytest.fixture
def test_action(client, monkeypatch):
    action = api.BackgroundAction("theme-test-action", "test", "demo")
    action.append_output("tests/auth.py::test_expired_token PASSED")
    monkeypatch.setitem(api._background_actions, action.id, action)
    return action


def path(action_id="theme-test-action"):
    return f"/api/stacks/actions/{action_id}/logs/themes"


def test_theme_grouping_requires_authentication(client, test_action, monkeypatch):
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)
    assert client.post(path(), json={"entries": ENTRIES}).status_code == 401
    group.assert_not_awaited()


def test_viewer_cannot_trigger_theme_grouping(client, test_action, monkeypatch):
    token = create_token("watcher@example.com", api.settings.auth.jwt_secret, 1,
                         role="viewer", auth_source="google")
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)
    response = client.post(path(), json={"entries": ENTRIES}, headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 403
    group.assert_not_awaited()


def test_admin_gets_theme_suggestions_without_changing_action(client, auth_headers, test_action, monkeypatch):
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)
    response = client.post(path(), json={"entries": ENTRIES}, headers=auth_headers)
    assert response.status_code == 200 and response.json() == RESULT
    group.assert_awaited_once_with(ENTRIES)
    assert test_action.status == "running" and test_action.result is None
    assert test_action.output_lines == ["tests/auth.py::test_expired_token PASSED"]


def test_unknown_action_is_rejected_before_provider_call(client, auth_headers, monkeypatch):
    monkeypatch.setattr(api, "pipeline_state", None)
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)
    response = client.post(path("unknown-theme-action"), json={"entries": ENTRIES}, headers=auth_headers)
    assert response.status_code == 404
    group.assert_not_awaited()


@pytest.mark.parametrize("action_type", ["build", "deploy", "qa"])
def test_non_test_action_is_rejected(client, auth_headers, test_action, monkeypatch, action_type):
    test_action.action_type = action_type
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)
    response = client.post(path(), json={"entries": ENTRIES}, headers=auth_headers)
    assert response.status_code == 400
    group.assert_not_awaited()


@pytest.mark.parametrize("body", [b"not-json", b"null", b"[]", b"{}", b'{"entries":'])
def test_invalid_body_is_400(client, auth_headers, test_action, monkeypatch, body):
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)
    response = client.post(path(), content=body, headers={**auth_headers, "Content-Type": "application/json"})
    assert response.status_code == 400
    group.assert_not_awaited()


@pytest.mark.parametrize("entries", [None, {}, [{"id": "a"}],
                                      [{"id": "a", "name": "same"}] * 2])
def test_invalid_entries_are_400_without_network(client, auth_headers, test_action, monkeypatch, entries):
    configure = AsyncMock(side_effect=AssertionError("invalid input must not reach provider configuration"))
    monkeypatch.setattr(themes, "_get_llm_config", configure)
    response = client.post(path(), json={"entries": entries}, headers=auth_headers)
    assert response.status_code == 400
    configure.assert_not_called()


def test_request_body_is_bounded_before_grouping(client, auth_headers, test_action, monkeypatch):
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)
    body = json.dumps({"entries": ENTRIES, "ignored": "x" * (themes.MAX_THEME_PAYLOAD_BYTES * 7)})
    response = client.post(path(), content=body, headers={**auth_headers, "Content-Type": "application/json"})
    assert response.status_code == 400
    group.assert_not_awaited()


def test_provider_errors_are_sanitized(client, auth_headers, test_action, monkeypatch):
    group = AsyncMock(side_effect=themes.ThemeGroupingError("secret-key in provider response"))
    monkeypatch.setattr(themes, "group_test_themes", group)
    response = client.post(path(), json={"entries": ENTRIES}, headers=auth_headers)
    assert response.status_code == 503
    assert response.json() == {"detail": "Test theme grouping is unavailable. Try again later."}


def test_restored_test_action_can_be_grouped(client, auth_headers, tmp_path, monkeypatch):
    manager = PipelineStateManager(str(tmp_path))
    manager.set_stage("demo", "test", "success", "1.2.3", action_id="restored-theme-action",
                      log_lines=["tests/auth.py::test_expired_token PASSED"])
    restored = PipelineStateManager(str(tmp_path))
    monkeypatch.setattr(api, "pipeline_state", restored)
    group = AsyncMock(return_value=RESULT)
    monkeypatch.setattr(themes, "group_test_themes", group)

    response = client.post(path("restored-theme-action"), json={"entries": ENTRIES}, headers=auth_headers)
    assert response.status_code == 200 and response.json() == RESULT
    group.assert_awaited_once_with(ENTRIES)
    assert restored.get("demo").stages["test"].status == "success"
