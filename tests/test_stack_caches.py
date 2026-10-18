"""The stack endpoints hand out copies of their caches, never the caches.

GET /api/stacks/buildable and GET /api/stacks/deployed-tags keep module-level
caches; the MCP tools and other Python callers call these functions directly,
so a caller mutating the result must not change what the next caller sees.
"""

import asyncio
import types
from unittest.mock import AsyncMock, MagicMock

import pytest

import backend.api as api
import backend.routes.stacks as stacks


class FakeDeployer:
    def __init__(self, *args, **kwargs):
        pass

    async def _ensure_repo_cloned(self, repo_name, ssh_url):
        return True, "Repository updated"

    async def has_build_config(self, repo_name):
        return repo_name == "app"

    async def get_all_deployed_stack_tags_with_qa(self, names):
        return {"app": "v1.0.1"}, {"app": "v1.0.2"}


@pytest.fixture
def github(monkeypatch):
    gh = MagicMock()
    gh.is_configured.return_value = True
    gh.get_starred_repos = AsyncMock(return_value=[
        {"owner": "owner", "name": "app", "ssh_url": "git@github.com:owner/app.git"},
        {"owner": "owner", "name": "lib", "ssh_url": "git@github.com:owner/lib.git"},
    ])
    gh.get_latest_tag = AsyncMock(return_value="v1.0.3")
    monkeypatch.setattr(api, "github_service", gh)
    monkeypatch.setattr(api, "settings", types.SimpleNamespace(github=None))
    monkeypatch.setattr(api, "_buildable_cache", {})
    monkeypatch.setattr(api, "_get_deployer_and_host", lambda: (FakeDeployer(), "manager-1"))
    monkeypatch.setattr(stacks, "StackDeployer", FakeDeployer)
    monkeypatch.setattr(stacks, "_deployed_tags_cache", None)
    monkeypatch.setattr(stacks, "_deployed_tags_cache_time", None)
    return gh


@pytest.mark.parametrize("refresh", [True, False])
def test_buildable_result_is_not_the_cache(github, refresh):
    first = asyncio.run(stacks.get_stacks_buildable(refresh=True))
    assert first == {"app": True, "lib": False}
    first["app"] = False
    first["evil"] = True

    # Both the freshly computed and the cached path return a copy.
    again = asyncio.run(stacks.get_stacks_buildable(refresh=refresh))
    again.clear()

    assert api._buildable_cache == {"app": True, "lib": False}
    assert asyncio.run(stacks.get_stacks_buildable(refresh=False)) == {"app": True, "lib": False}


def test_deployed_tags_result_is_not_the_cache(github):
    expected = {"tags": {"app": "v1.0.1"}, "qa_tags": {"app": "v1.0.2"},
                "latest_built": {"app": "v1.0.3", "lib": "v1.0.3"}}
    first = asyncio.run(stacks.get_stacks_deployed_tags())
    assert first == expected
    first["tags"]["app"] = "tampered"
    first["latest_built"].clear()

    cached = asyncio.run(stacks.get_stacks_deployed_tags())
    assert cached == expected
    assert github.get_starred_repos.await_count == 1  # served from the cache
    cached["qa_tags"]["app"] = "tampered"
    cached.pop("tags")

    assert asyncio.run(stacks.get_stacks_deployed_tags()) == expected
