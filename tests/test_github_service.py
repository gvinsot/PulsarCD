"""Tests for backend/github_service.py.

Covers the GitHub API client (caching, rate limiting, tag/version logic) with
a fake aiohttp session, and StackDeployer's shell command construction with a
recording runner in place of SSH / local execution. Nothing here touches the
network or a shell.
"""

import asyncio
import json
import shlex
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend import github_service as gh
from backend.config import GitHubConfig
from backend.github_service import (
    GitHubService,
    StackDeployer,
    _shell_quote_path,
    _validate_repo_name,
    _validate_ssh_url,
)


# ---------------------------------------------------------------------------
# Fake aiohttp session
# ---------------------------------------------------------------------------

class FakeResponse:
    """Just enough of aiohttp.ClientResponse for GitHubService."""

    def __init__(self, status=200, payload=None, headers=None, text=""):
        self.status = status
        self._payload = payload
        self.headers = headers or {}
        self._text = text

    async def json(self):
        return self._payload

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Routes requests to a handler and records every call made."""

    def __init__(self, handler):
        self._handler = handler
        self.calls = []
        self.closed = False

    def _request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        result = self._handler(method, url, kwargs)
        if isinstance(result, Exception):
            raise result
        return result

    def get(self, url, params=None):
        return self._request("GET", url, params=dict(params or {}))

    def post(self, url, json=None):
        return self._request("POST", url, json=json)

    def urls(self, method="GET"):
        return [u for m, u, _ in self.calls if m == method]


def _service(handler, token="ghp_test", **config):
    svc = GitHubService(GitHubConfig(token=token, **config))
    session = FakeSession(handler)
    svc._get_session = AsyncMock(return_value=session)
    return svc, session


def _branch(name, sha=None):
    return {"name": name, "commit": {"sha": sha or f"sha-{name}"}, "protected": False}


def _tag(name, sha):
    return {"name": name, "commit": {"sha": sha, "url": f"https://api.github.com/commit/{sha}"},
            "zipball_url": f"https://zip/{name}"}


def _commit(sha):
    return {
        "sha": sha,
        "commit": {"message": f"msg {sha}", "author": {"name": "dev", "date": "2026-01-01T00:00:00Z"}},
        "author": {"avatar_url": "https://avatar"},
        "parents": [],
    }


@pytest.fixture(autouse=True)
def _tag_date_cache_file(tmp_path, monkeypatch):
    """Keep the persistent tag-date cache away from /data."""
    path = tmp_path / ".tag_date_cache.json"
    monkeypatch.setattr(gh, "TAG_DATE_CACHE_FILE", path)
    return path


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

class TestValidateRepoName:
    @pytest.mark.parametrize("name", [
        "repo", "My-Repo_1", ".github", "_private", "a.b.c", "x" * 100, "9lives",
    ])
    def test_accepts(self, name):
        assert _validate_repo_name(name) == name

    @pytest.mark.parametrize("name", [
        "", "-rf", "..", ".", "a..b", "a/b", "a b", "a;id", "$(id)", "`id`",
        "repo\n", "repo\nrm -rf /", "x" * 101, "é", "repo\x00",
    ])
    def test_rejects(self, name):
        with pytest.raises(ValueError):
            _validate_repo_name(name)

    @pytest.mark.parametrize("value", [None, 42, b"repo"])
    def test_rejects_non_strings(self, value):
        with pytest.raises(ValueError):
            _validate_repo_name(value)


class TestValidateSshUrl:
    @pytest.mark.parametrize("url", [
        "git@github.com:owner/repo.git",
        "git@gitlab.example.org:my-org/.github.git",
        "ssh://git@github.com/owner/repo.git",
        "ssh://git@git.local:2222/owner/repo.git",
        "https://github.com/owner/repo",
        "https://github.com/owner/repo.git",
    ])
    def test_accepts(self, url):
        assert _validate_ssh_url(url) == url

    @pytest.mark.parametrize("url", [
        "",
        "--upload-pack=touch /tmp/x",
        "ext::sh -c id",
        "file:///etc/passwd",
        "http://github.com/owner/repo.git",          # plain http
        "git@github.com:owner/repo",                  # scp form needs .git
        "git@github.com:owner/../repo.git",
        "git@github.com:owner/repo.git\n",
        "git@github.com:owner/repo.git; id",
        "git@-oProxyCommand=id:owner/repo.git",
        "root@github.com:owner/repo.git",
        "ssh://git@github.com:99999999/owner/repo.git",
        "https://github.com/owner/sub/repo.git",
        "https://github.com/" + "a" * 600 + "/repo.git",
    ])
    def test_rejects(self, url):
        with pytest.raises(ValueError):
            _validate_ssh_url(url)


class TestShellQuotePath:
    def test_tilde_prefix_stays_expandable(self):
        assert _shell_quote_path("~/repos") == "~/repos"

    def test_tilde_with_spaces_quotes_the_rest(self):
        assert _shell_quote_path("~/my repos/x") == "~/'my repos/x'"

    def test_absolute_path_with_metacharacters(self):
        quoted = _shell_quote_path("/srv/a b/$(id)")
        assert quoted == "'/srv/a b/$(id)'"
        assert shlex.split(quoted) == ["/srv/a b/$(id)"]

    def test_tilde_user_form_is_fully_quoted(self):
        # Only "~/" is special-cased; "~root/x" must not expand to another home
        assert _shell_quote_path("~root/x") == "'~root/x'"

    def test_plain_path_is_unchanged(self):
        assert _shell_quote_path("/srv/repos") == "/srv/repos"


class TestRepoToStackName:
    @pytest.mark.parametrize("repo,stack", [
        ("PulsarCD", "pulsarcd"),
        ("my_repo.v2", "my-repo-v2"),
        ("--weird__name--", "weird-name"),
        (".github", "github"),
    ])
    def test_mirrors_deploy_script(self, repo, stack):
        assert StackDeployer._repo_to_stack_name(repo) == stack


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

class TestRateLimit:
    def test_primary_limit_uses_reset_header(self):
        svc = GitHubService(GitHubConfig(token="t"))
        reset = int((datetime.now() + timedelta(minutes=3)).timestamp())
        assert svc._handle_rate_limit({"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(reset)}) is True
        assert svc._rate_limit_reset == datetime.fromtimestamp(reset)
        assert svc._is_rate_limited() is True

    def test_primary_limit_without_reset_backs_off_five_minutes(self):
        svc = GitHubService(GitHubConfig(token="t"))
        before = datetime.now()
        assert svc._handle_rate_limit({"X-RateLimit-Remaining": "0"}) is True
        assert timedelta(minutes=4) < svc._rate_limit_reset - before <= timedelta(minutes=5, seconds=1)

    def test_secondary_limit_retry_after(self):
        svc = GitHubService(GitHubConfig(token="t"))
        before = datetime.now()
        assert svc._handle_rate_limit({"Retry-After": "30"}, status=403) is True
        assert timedelta(seconds=29) < svc._rate_limit_reset - before <= timedelta(seconds=31)

    def test_secondary_limit_bad_retry_after_defaults_to_a_minute(self):
        svc = GitHubService(GitHubConfig(token="t"))
        before = datetime.now()
        assert svc._handle_rate_limit({"Retry-After": "soon"}, status=403) is True
        assert timedelta(seconds=59) < svc._rate_limit_reset - before <= timedelta(seconds=61)

    def test_retry_after_on_success_is_ignored(self):
        svc = GitHubService(GitHubConfig(token="t"))
        assert svc._handle_rate_limit({"Retry-After": "30"}, status=200) is False
        assert svc._is_rate_limited() is False

    def test_remaining_quota_is_not_limited(self):
        svc = GitHubService(GitHubConfig(token="t"))
        assert svc._handle_rate_limit({"X-RateLimit-Remaining": "4999"}) is False

    def test_limit_clears_once_reset_time_passes(self):
        svc = GitHubService(GitHubConfig(token="t"))
        svc._rate_limit_reset = datetime.now() - timedelta(seconds=1)
        assert svc._is_rate_limited() is False
        assert svc._rate_limit_reset is None

    @staticmethod
    def _expire(cache):
        for key, (value, _) in list(cache.items()):
            cache[key] = (value, datetime.now() - timedelta(hours=1))

    async def test_rate_limited_serves_stale_branches(self):
        svc, session = _service(lambda *a: FakeResponse(200, [_branch("main")]))
        fresh = await svc.get_repo_branches("o", "r")
        self._expire(svc._branches_cache)
        svc._rate_limit_reset = datetime.now() + timedelta(minutes=5)
        assert await svc.get_repo_branches("o", "r") == fresh
        assert len(session.calls) == 1

    async def test_rate_limited_serves_stale_commits_and_diff(self):
        diff = {"sha": "abc", "commit": {"message": "m", "author": {"name": "n", "date": "d"}},
                "files": []}

        def handler(method, url, kwargs):
            return FakeResponse(200, diff if url.endswith("/abc") else [_commit("abc")])

        svc, session = _service(handler)
        commits = await svc.get_repo_commits("o", "r", per_page=1)
        commit_diff = await svc.get_commit_diff("o", "r", "abc")
        self._expire(svc._commits_cache)
        self._expire(svc._commit_diff_cache)
        svc._rate_limit_reset = datetime.now() + timedelta(minutes=5)
        assert await svc.get_repo_commits("o", "r", per_page=1) == commits
        assert await svc.get_commit_diff("o", "r", "abc") == commit_diff
        assert len(session.calls) == 2

    async def test_rate_limited_next_version_uses_stale_tags(self):
        """Without the stale tags the next version silently fell back to 1.0.0."""
        svc, session = _service(_tags_handler([_tag("v1.4.2", "s1")]))
        assert await svc.get_next_version("o", "r") == "1.4.3"
        calls = len(session.calls)
        self._expire(svc._tags_cache)
        self._expire(svc._branches_cache)
        svc._rate_limit_reset = datetime.now() + timedelta(minutes=5)
        assert await svc.get_next_version("o", "r") == "1.4.3"
        assert len(session.calls) == calls

    async def test_rate_limited_without_cache_returns_empty(self):
        svc, session = _service(lambda *a: FakeResponse(200, []))
        svc._rate_limit_reset = datetime.now() + timedelta(minutes=5)
        assert await svc.get_repo_branches("o", "r") == []
        assert await svc.get_repo_tags("o", "r") == {"tags": [], "branches": {}}
        commits = await svc.get_repo_commits("o", "r")
        assert commits["commits"] == [] and "Rate limited" in commits["error"]
        assert await svc.validate_commit("o", "r", "abc1234") == (True, "")
        assert session.calls == []

    async def test_limit_hit_during_fetch_returns_stale_repos(self):
        responses = iter([
            FakeResponse(200, [{
                "id": 1, "name": "r", "full_name": "o/r", "description": None,
                "html_url": "h", "ssh_url": "git@github.com:o/r.git", "clone_url": "c",
                "language": "Python", "stargazers_count": 0, "updated_at": "u",
                "owner": {"login": "o"}, "private": False,
            }]),
            FakeResponse(403, None, headers={"X-RateLimit-Remaining": "0"}),
        ])
        svc, _ = _service(lambda *a: next(responses))
        first = await svc.get_starred_repos()
        assert [r["name"] for r in first] == ["r"]
        assert first[0]["description"] == ""
        second = await svc.get_starred_repos(force_refresh=True)
        assert second == first
        assert svc._is_rate_limited()

    async def test_limit_hit_during_commits_fetch(self):
        svc, _ = _service(lambda *a: FakeResponse(403, None, headers={"X-RateLimit-Remaining": "0"}))
        result = await svc.get_repo_commits("o", "r")
        assert result == {"commits": [], "has_more": False, "error": "Rate limited by GitHub API"}


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

class TestCaching:
    async def test_branches_are_cached_and_sorted(self):
        payload = [_branch("feature"), _branch("master"), _branch("alpha"), _branch("main")]
        svc, session = _service(lambda *a: FakeResponse(200, payload))
        first = await svc.get_repo_branches("o", "r")
        second = await svc.get_repo_branches("o", "r")
        assert [b["name"] for b in first] == ["main", "master", "alpha", "feature"]
        assert second is first
        assert len(session.calls) == 1

    async def test_expired_entry_is_refetched(self):
        svc, session = _service(lambda *a: FakeResponse(200, [_branch("main")]))
        await svc.get_repo_branches("o", "r")
        value, _ = svc._branches_cache["o/r"]
        svc._branches_cache["o/r"] = (value, datetime.now() - gh.BRANCHES_CACHE_TTL)
        await svc.get_repo_branches("o", "r")
        assert len(session.calls) == 2

    async def test_cache_keys_are_per_repo(self):
        svc, session = _service(lambda *a: FakeResponse(200, [_branch("main")]))
        await svc.get_repo_branches("o", "a")
        await svc.get_repo_branches("o", "b")
        assert len(session.calls) == 2

    async def test_errors_are_not_cached(self):
        responses = iter([FakeResponse(500, None, text="boom"), FakeResponse(200, [_branch("main")])])
        svc, session = _service(lambda *a: next(responses))
        assert await svc.get_repo_branches("o", "r") == []
        assert [b["name"] for b in await svc.get_repo_branches("o", "r")] == ["main"]
        assert len(session.calls) == 2

    async def test_commits_cache_key_includes_branch_and_page(self):
        svc, session = _service(lambda *a: FakeResponse(200, [_commit("a" * 40)]))
        await svc.get_repo_commits("o", "r", branch="main", per_page=1)
        await svc.get_repo_commits("o", "r", branch="main", per_page=1)
        await svc.get_repo_commits("o", "r", branch="dev", per_page=1)
        await svc.get_repo_commits("o", "r", branch="main", per_page=1, page=2)
        assert len(session.calls) == 3
        assert session.calls[0][2]["params"] == {"per_page": 1, "page": 1, "sha": "main"}

    async def test_commits_has_more_and_shape(self):
        svc, _ = _service(lambda *a: FakeResponse(200, [_commit("a" * 40), _commit("b" * 40)]))
        result = await svc.get_repo_commits("o", "r", per_page=2)
        assert result["has_more"] is True
        assert result["commits"][0]["short_sha"] == "aaaaaaa"
        assert result["commits"][0]["author_avatar"] == "https://avatar"

    async def test_starred_repos_cache(self):
        svc, session = _service(lambda *a: FakeResponse(200, []))
        await svc.get_starred_repos()
        await svc.get_starred_repos()
        assert len(session.calls) == 1
        await svc.get_starred_repos(force_refresh=True)
        assert len(session.calls) == 2

    async def test_invalidate_cache_clears_everything(self):
        svc, session = _service(lambda *a: FakeResponse(200, [_branch("main")]))
        await svc.get_repo_branches("o", "r")
        svc.invalidate_cache()
        await svc.get_repo_branches("o", "r")
        assert len(session.calls) == 2
        assert svc._starred_repos_cache is None

    async def test_no_token_short_circuits(self):
        svc, session = _service(lambda *a: FakeResponse(200, []), token=None)
        assert svc.is_configured() is False
        assert await svc.get_starred_repos() == []
        assert await svc.get_repo_branches("o", "r") == []
        assert (await svc.create_tag("o", "r", "v1", "sha"))["success"] is False
        assert session.calls == []


class TestStarredRepos:
    def _repo(self, i):
        return {
            "id": i, "name": f"r{i}", "full_name": f"o/r{i}", "description": "d",
            "html_url": "h", "ssh_url": f"git@github.com:o/r{i}.git", "clone_url": "c",
            "language": None, "stargazers_count": 1, "updated_at": "u", "pushed_at": "p",
            "owner": {"login": "o"}, "private": True,
        }

    async def test_paginates_until_a_short_page(self):
        pages = {1: [self._repo(i) for i in range(100)], 2: [self._repo(100)]}

        def handler(method, url, kwargs):
            return FakeResponse(200, pages[kwargs["params"]["page"]])

        svc, session = _service(handler)
        repos = await svc.get_starred_repos()
        assert len(repos) == 101
        assert len(session.calls) == 2
        assert session.urls()[0] == "https://api.github.com/user/repos"

    async def test_starred_mode_uses_starred_endpoint(self):
        svc, session = _service(lambda *a: FakeResponse(200, []), repos_mode="Starred")
        await svc.get_starred_repos()
        assert session.urls() == ["https://api.github.com/user/starred"]

    async def test_api_error_returns_partial_list(self):
        svc, _ = _service(lambda *a: FakeResponse(401, None, text="Bad credentials"))
        assert await svc.get_starred_repos() == []

    async def test_network_error_returns_empty(self):
        svc, _ = _service(lambda *a: OSError("connection reset"))
        assert await svc.get_starred_repos() == []


# ---------------------------------------------------------------------------
# Tags and versions
# ---------------------------------------------------------------------------

def _tags_handler(tags, branches=None, commit_dates=None, counter=None):
    branches = branches if branches is not None else [_branch("main")]
    commit_dates = commit_dates or {}

    def handler(method, url, kwargs):
        if url.endswith("/tags"):
            return FakeResponse(200, tags[: kwargs["params"]["per_page"]])
        if url.endswith("/branches"):
            return FakeResponse(200, branches)
        if url.startswith("https://api.github.com/commit/"):
            sha = url.rsplit("/", 1)[1]
            if counter is not None:
                counter.append(sha)
            return FakeResponse(200, {"commit": {"committer": {"date": commit_dates.get(sha, "2026-01-01")}}})
        raise AssertionError(f"unexpected {method} {url}")

    return handler


class TestTagsAndVersions:
    @pytest.mark.parametrize("latest,expected", [
        ("v1.0.5", "1.0.6"),
        ("1.2.9", "1.2.10"),
        ("v10.20.30", "10.20.31"),
    ])
    async def test_next_version_bumps_patch(self, latest, expected):
        svc, _ = _service(_tags_handler([_tag(latest, "s1"), _tag("v0.0.1", "s0")]))
        assert await svc.get_next_version("o", "r") == expected

    async def test_next_version_without_tags(self):
        svc, _ = _service(_tags_handler([]))
        assert await svc.get_next_version("o", "r") == "1.0.0"

    async def test_latest_tag(self):
        svc, _ = _service(_tags_handler([_tag("v3.1.4", "s1")]))
        assert await svc.get_latest_tag("o", "r") == "v3.1.4"
        svc2, _ = _service(_tags_handler([]))
        assert await svc2.get_latest_tag("o", "r") is None

    async def test_next_version_uses_highest_of_unordered_tags(self):
        """GitHub does not list tags in version order: the first one is not the latest."""
        tags = [_tag("v1.2.3", "a"), _tag("v1.10.0", "b"), _tag("1.9.7", "c"), _tag("v2.0.1", "d"),
                _tag("v1.99.99", "e")]
        svc, _ = _service(_tags_handler(tags))
        assert await svc.get_next_version("o", "r") == "2.0.2"
        assert await svc.get_latest_tag("o", "r") == "v2.0.1"

    @pytest.mark.parametrize("names,expected", [
        (["latest", "v1.4.2"], "1.4.3"),
        (["release-3", "v1.2", "1.0.7", "nightly"], "1.0.8"),
        (["v2.0.0-rc1", "v1.9.9", "1.9.10+build"], "1.9.10"),
        (["latest", "v1.2"], "1.0.0"),
    ])
    async def test_next_version_ignores_non_release_tags(self, names, expected):
        svc, _ = _service(_tags_handler([_tag(n, f"s{i}") for i, n in enumerate(names)]))
        assert await svc.get_next_version("o", "r") == expected

    async def test_latest_tag_falls_back_to_first_tag_without_releases(self):
        svc, _ = _service(_tags_handler([_tag("latest", "s0"), _tag("v1.2", "s1")]))
        assert await svc.get_latest_tag("o", "r") == "latest"

    async def test_next_version_reads_every_page_of_tags(self):
        # 250 tags: the highest release only appears on the third page.
        tags = [_tag(f"v1.0.{i}", f"s{i}") for i in range(249)] + [_tag("v3.0.0", "top")]
        pages = []

        def handler(method, url, kwargs):
            assert url.endswith("/tags")
            params = kwargs["params"]
            pages.append(params["page"])
            start = (params["page"] - 1) * params["per_page"]
            return FakeResponse(200, tags[start:start + params["per_page"]])

        svc, session = _service(handler)
        assert await svc.get_next_version("o", "r") == "3.0.1"
        assert pages == [1, 2, 3]
        assert all(kw["params"]["per_page"] == 100 for _, _, kw in session.calls)
        # The listing is cached, and create_tag drops it with the other tag caches
        assert await svc.get_next_version("o", "r") == "3.0.1"
        assert pages == [1, 2, 3]
        svc.invalidate_repo_cache("o", "r")
        assert "o/r/all" not in svc._tags_cache

    async def test_tag_pagination_is_capped(self, monkeypatch):
        monkeypatch.setattr(gh, "VERSION_TAGS_MAX_PAGES", 3)
        pages = []

        def handler(method, url, kwargs):
            pages.append(kwargs["params"]["page"])
            return FakeResponse(200, [_tag(f"v1.{kwargs['params']['page']}.{i}", "s") for i in range(100)])

        svc, _ = _service(handler)
        assert await svc.get_next_version("o", "r") == "1.3.100"
        assert pages == [1, 2, 3]

    async def test_next_version_api_error_serves_stale_listing(self):
        responses = iter([FakeResponse(200, [_tag("v1.4.2", "s")]), FakeResponse(500, None, text="boom")])
        svc, _ = _service(lambda *a: next(responses))
        assert await svc.get_next_version("o", "r") == "1.4.3"
        TestRateLimit._expire(svc._tags_cache)
        assert await svc.get_next_version("o", "r") == "1.4.3"

    async def test_untagged_commits_latest_tag_is_highest_release(self):
        commits = [_commit("c2"), _commit("c1")]

        def handler(method, url, kwargs):
            if url.endswith("/commits"):
                return FakeResponse(200, commits)
            return _tags_handler([_tag("latest", "c1"), _tag("v1.0.9", "c0"), _tag("v1.0.10", "c1")])(
                method, url, kwargs)

        svc, _ = _service(handler)
        result = await svc.get_untagged_commits("o", "r", limit=2)
        assert result["latest_tag"]["name"] == "v1.0.10"

    async def test_untagged_commits_latest_tag_is_the_tag_on_the_branch(self):
        """A higher release on another branch must not label HEAD's build."""
        commits = [_commit("head"), _commit("older")]

        def handler(method, url, kwargs):
            if url.endswith("/commits"):
                return FakeResponse(200, commits)
            return _tags_handler([_tag("v2.0.0", "hotfix"), _tag("v1.4.3", "head")])(method, url, kwargs)

        svc, _ = _service(handler)
        result = await svc.get_untagged_commits("o", "r", limit=2)
        assert result["untagged_commits"] == []
        assert result["latest_tag"]["name"] == "v1.4.3"

    async def test_untagged_commits_without_tagged_commit_uses_highest_release(self):
        commits = [_commit("c2"), _commit("c1")]

        def handler(method, url, kwargs):
            if url.endswith("/commits"):
                return FakeResponse(200, commits)
            return _tags_handler([_tag("v1.0.9", "x"), _tag("v1.0.10", "y")])(method, url, kwargs)

        svc, _ = _service(handler)
        result = await svc.get_untagged_commits("o", "r", limit=2)
        assert len(result["untagged_commits"]) == 2
        assert result["latest_tag"]["name"] == "v1.0.10"

    async def test_tags_result_shape(self):
        svc, _ = _service(_tags_handler(
            [_tag("v1.0.1", "s1"), _tag("v1.0.0", "s0")],
            branches=[_branch("dev"), _branch("main")],
        ))
        result = await svc.get_repo_tags("o", "r", limit=10)
        assert [t["name"] for t in result["tags"]] == ["v1.0.1", "v1.0.0"]
        assert result["branches"] == ["main", "dev"]
        assert result["default_branch"] == "main"
        assert result["tags"][0]["created_at"] == "2026-01-01"

    async def test_tag_dates_are_persisted_and_reused(self, _tag_date_cache_file):
        fetched = []
        handler = _tags_handler([_tag("v1.0.0", "abc")], commit_dates={"abc": "2025-05-05"}, counter=fetched)
        svc, _ = _service(handler)
        await svc.get_repo_tags("o", "r")
        assert fetched == ["abc"]
        assert json.loads(_tag_date_cache_file.read_text()) == {"abc": "2025-05-05"}

        # A new process reads the dates from disk instead of asking GitHub again
        svc2, _ = _service(handler)
        result = await svc2.get_repo_tags("o", "r")
        assert fetched == ["abc"]
        assert result["tags"][0]["created_at"] == "2025-05-05"

    async def test_corrupt_tag_date_cache_is_ignored(self, _tag_date_cache_file):
        _tag_date_cache_file.write_text("{oops")
        svc, _ = _service(_tags_handler([_tag("v1.0.0", "abc")]))
        result = await svc.get_repo_tags("o", "r")
        assert result["tags"][0]["created_at"] == "2026-01-01"

    async def test_untagged_commits_stop_at_first_tag(self):
        commits = [_commit("c3"), _commit("c2"), _commit("c1"), _commit("c0")]

        def handler(method, url, kwargs):
            if url.endswith("/commits"):
                return FakeResponse(200, commits)
            return _tags_handler([_tag("v1.0.1", "c1"), _tag("v1.0.0", "c0")])(method, url, kwargs)

        svc, _ = _service(handler)
        result = await svc.get_untagged_commits("o", "r", limit=4)
        assert [c["sha"] for c in result["untagged_commits"]] == ["c3", "c2"]
        assert result["latest_tag"]["name"] == "v1.0.1"
        assert result["total_commits_checked"] == 4

    async def test_create_tag_success_invalidates_only_that_repo(self):
        def handler(method, url, kwargs):
            if method == "POST":
                assert kwargs["json"] == {"ref": "refs/tags/v1.0.1", "sha": "f" * 40}
                return FakeResponse(201, {})
            return _tags_handler([_tag("v1.0.0", "s")])(method, url, kwargs)

        svc, _ = _service(handler)
        await svc.get_repo_tags("o", "r", limit=1)
        await svc.get_repo_tags("o", "other", limit=1)
        result = await svc.create_tag("o", "r", "v1.0.1", "f" * 40)
        assert result == {"success": True, "tag": "v1.0.1", "sha": "f" * 40}
        assert list(svc._tags_cache) == ["o/other/1"]

    async def test_create_tag_failure_reports_github_error(self):
        svc, _ = _service(lambda *a: FakeResponse(422, None, text="Reference already exists"))
        result = await svc.create_tag("o", "r", "v1.0.0", "sha")
        assert result == {"success": False, "error": "Reference already exists"}

    async def test_create_tag_network_error(self):
        svc, _ = _service(lambda *a: OSError("down"))
        result = await svc.create_tag("o", "r", "v1.0.0", "sha")
        assert result["success"] is False and "down" in result["error"]


# ---------------------------------------------------------------------------
# Branch / commit validation and error hints
# ---------------------------------------------------------------------------

class TestValidation:
    async def test_validate_branch(self):
        svc, _ = _service(lambda *a: FakeResponse(200, [_branch("main"), _branch("dev")]))
        assert await svc.validate_branch("o", "r", "dev") == (True, "")
        ok, msg = await svc.validate_branch("o", "r", "nope")
        assert ok is False and "main, dev" in msg

    async def test_validate_branch_is_best_effort(self):
        svc, _ = _service(lambda *a: FakeResponse(403, None, text="forbidden"))
        assert await svc.validate_branch("o", "r", "anything") == (True, "")

    @pytest.mark.parametrize("status,expected_ok", [(200, True), (404, False), (403, True), (502, True)])
    async def test_validate_commit_statuses(self, status, expected_ok):
        svc, _ = _service(lambda *a: FakeResponse(status, {}))
        ok, msg = await svc.validate_commit("o", "r", "abc1234")
        assert ok is expected_ok
        assert (msg == "") is expected_ok

    async def test_validate_commit_network_error_allows(self):
        svc, _ = _service(lambda *a: OSError("down"))
        assert await svc.validate_commit("o", "r", "abc1234") == (True, "")

    def test_permission_error_hints(self):
        hint = GitHubService._parse_permission_error(
            "Resource not accessible by personal access token", 403, "tags", "Contents: Read")
        assert "Contents: Read" in hint and "Fine-grained" in hint
        assert "Access denied (403)" in GitHubService._parse_permission_error("x", 403, "tags", "p")
        assert "(404)" in GitHubService._parse_permission_error("x", 404, "tags", "p")
        assert "status 500" in GitHubService._parse_permission_error("x", 500, "tags", "p")

    async def test_commit_diff_shape_and_cache(self):
        payload = {
            "sha": "abc", "commit": {"message": "m", "author": {"name": "n", "date": "d"}},
            "stats": {"total": 3},
            "files": [{"filename": "a.py", "status": "modified", "additions": 2,
                       "deletions": 1, "changes": 3}],
        }
        svc, session = _service(lambda *a: FakeResponse(200, payload))
        result = await svc.get_commit_diff("o", "r", "abc")
        await svc.get_commit_diff("o", "r", "abc")
        assert len(session.calls) == 1
        assert result["files"][0]["patch"] == ""
        assert result["stats"] == {"total": 3}

    async def test_commit_diff_error_carries_hint(self):
        svc, _ = _service(lambda *a: FakeResponse(404, None, text="Not Found"))
        result = await svc.get_commit_diff("o", "r", "abc")
        assert result["files"] == [] and "(404)" in result["error"]


class TestSession:
    async def test_session_is_recreated_when_token_changes(self):
        svc = GitHubService(GitHubConfig(token="one"))
        try:
            first = await svc._get_session()
            assert await svc._get_session() is first
            assert first.headers["Authorization"] == "token one"
            svc.config.token = "two"
            second = await svc._get_session()
            assert second is not first
            assert first.closed
            assert second.headers["Authorization"] == "token two"
        finally:
            await svc.close()

    async def test_no_authorization_header_without_token(self):
        svc = GitHubService(GitHubConfig(token=None))
        try:
            session = await svc._get_session()
            assert "Authorization" not in session.headers
        finally:
            await svc.close()


# ---------------------------------------------------------------------------
# StackDeployer: command construction
# ---------------------------------------------------------------------------

class Recorder:
    """Stands in for _run_command; answers by matching command substrings."""

    def __init__(self, answers=None):
        self.commands = []
        self.answers = answers or []  # list of (substring, (success, output))

    async def __call__(self, command, output_callback=None, cancel_event=None):
        self.commands.append(command)
        for needle, answer in self.answers:
            if needle in command:
                if output_callback and answer[1]:
                    for line in answer[1].splitlines():
                        output_callback(line)
                return answer
        return True, ""


@pytest.fixture
def no_snapshot(monkeypatch):
    """build()/deploy() snapshot .env files first; that has its own tests."""
    from backend import recovery
    monkeypatch.setattr(recovery, "snapshot_envs", AsyncMock(return_value=0))


def _deployer(answers=None, cloned=(True, ""), **config):
    config.setdefault("repos_path", "/srv/repos")
    deployer = StackDeployer(GitHubConfig(**config), None)
    recorder = Recorder(answers)
    deployer._run_command = recorder
    deployer._ensure_docker_login = AsyncMock()
    deployer._ensure_repo_cloned = AsyncMock(return_value=cloned)
    return deployer, recorder


SSH = "git@github.com:owner/my-repo.git"


class TestBuildCommand:
    @pytest.mark.parametrize("kwargs,suffix", [
        ({}, ' "" ""'),
        ({"branch": "dev"}, " dev \"\""),
        ({"tag": "v1.2.3", "branch": "dev"}, " v1.2.3 \"\""),
        ({"tag": "v1.2.3", "commit": "abc1234"}, " v1.2.3 abc1234"),
        ({"commit": "abc1234"}, ' "" abc1234'),
        ({"no_cache": True}, ' "" "" --no-cache'),
    ])
    async def test_arguments(self, no_snapshot, kwargs, suffix):
        deployer, rec = _deployer()
        result = await deployer.build("my-repo", SSH, version="1.2.3", **kwargs)
        assert result["success"] is True
        assert rec.commands == [
            "cd /srv/repos/PulsarCD/scripts && bash build-push.sh /srv/repos/my-repo 1.2.3" + suffix
        ]
        assert result["branch"] == (kwargs.get("tag") or kwargs.get("branch"))

    async def test_hostile_ref_is_quoted(self, no_snapshot):
        deployer, rec = _deployer()
        await deployer.build("my-repo", SSH, version="1.0; id", branch="x$(id)")
        argv = shlex.split(rec.commands[0].split("&& ", 1)[1])
        assert argv == ["bash", "build-push.sh", "/srv/repos/my-repo", "1.0; id", "x$(id)", ""]

    async def test_tilde_repos_path_stays_expandable(self, no_snapshot):
        deployer, rec = _deployer(repos_path="~/repos")
        await deployer.build("my-repo", SSH, version="1.0")
        assert rec.commands[0].startswith("cd ~/repos/PulsarCD/scripts && bash build-push.sh ~/repos/my-repo ")

    async def test_streams_clone_message_and_output(self, no_snapshot):
        deployer, _ = _deployer(answers=[("build-push.sh", (True, "step 1\nVersion: 1.0.7"))],
                                cloned=(True, "Repository updated"))
        lines = []
        result = await deployer.build("my-repo", SSH, output_callback=lines.append)
        assert lines == ["Repository updated", "step 1", "Version: 1.0.7"]
        assert result["output"] == "Repository updated\n\nstep 1\nVersion: 1.0.7"
        assert result["completed_at"] and result["duration_seconds"] >= 0

    async def test_clone_failure_stops_before_build(self, no_snapshot):
        deployer, rec = _deployer(cloned=(False, "Failed to clone repository: denied"))
        result = await deployer.build("my-repo", SSH)
        assert result["success"] is False
        assert result["output"] == "Failed to clone repository: denied"
        assert rec.commands == []

    async def test_cancel_before_build(self, no_snapshot):
        deployer, rec = _deployer(cloned=(True, "ok"))
        cancel = asyncio.Event()
        cancel.set()
        result = await deployer.build("my-repo", SSH, cancel_event=cancel)
        assert result["success"] is False
        assert "[Cancelled by user]" in result["output"]
        assert rec.commands == []

    async def test_failed_script(self, no_snapshot):
        deployer, _ = _deployer(answers=[("build-push.sh", (False, "error: push denied"))])
        result = await deployer.build("my-repo", SSH)
        assert result["success"] is False
        assert "push denied" in result["output"]

    async def test_logs_in_to_registry_and_snapshots(self, monkeypatch):
        from backend import recovery
        snapshot = AsyncMock(return_value=0)
        monkeypatch.setattr(recovery, "snapshot_envs", snapshot)
        deployer, _ = _deployer()
        await deployer.build("my-repo", SSH)
        deployer._ensure_docker_login.assert_awaited_once()
        snapshot.assert_awaited_once_with(deployer, "my-repo")


class TestDeployCommand:
    @pytest.mark.parametrize("kwargs,args,version", [
        ({"tag": "v1.2.3"}, "1.2.3 v1.2.3", "1.2.3"),
        ({"tag": "1.2.3"}, "1.2.3 1.2.3", "1.2.3"),
        ({"version": "1.0.4"}, "1.0.4 v1.0.4", "1.0.4"),
        ({"version": "v2.0.0"}, "2.0.0 v2.0.0", "2.0.0"),
    ])
    async def test_version_and_checkout_ref(self, no_snapshot, kwargs, args, version):
        deployer, rec = _deployer()
        result = await deployer.deploy("my-repo", SSH, **kwargs)
        assert rec.commands == [
            f"cd /srv/repos/PulsarCD/scripts && bash deploy-service.sh /srv/repos/my-repo {args}"
        ]
        assert result["version"] == version
        assert result["action"] == "deploy"

    async def test_qa_flag(self, no_snapshot):
        deployer, rec = _deployer()
        result = await deployer.deploy("my-repo", SSH, tag="v1.0.0", qa=True)
        assert "bash deploy-service.sh --qa /srv/repos/my-repo 1.0.0 v1.0.0" in rec.commands[0]
        assert result["action"] == "qa-deploy" and result["qa"] is True

    async def test_rejects_hostile_input_before_snapshot(self, monkeypatch):
        from backend import recovery
        snapshot = AsyncMock()
        monkeypatch.setattr(recovery, "snapshot_envs", snapshot)
        deployer, rec = _deployer()
        result = await deployer.deploy("../etc", SSH)
        assert result["success"] is False
        snapshot.assert_not_awaited()
        assert rec.commands == []


class TestTestCommand:
    @pytest.mark.parametrize("kwargs,suffix", [
        ({}, ""),
        ({"branch": "dev"}, " dev"),
        ({"tag": "v1.0.0", "commit": "abc1234"}, " v1.0.0 abc1234"),
        ({"commit": "abc1234"}, ' "" abc1234'),
    ])
    async def test_arguments(self, kwargs, suffix):
        deployer, rec = _deployer()
        result = await deployer.test("my-repo", SSH, **kwargs)
        assert result["success"] is True
        assert rec.commands == [
            "cd /srv/repos/PulsarCD/scripts && bash test.sh /srv/repos/my-repo" + suffix
        ]

    async def test_does_not_snapshot_envs(self, monkeypatch):
        from backend import recovery
        snapshot = AsyncMock()
        monkeypatch.setattr(recovery, "snapshot_envs", snapshot)
        deployer, _ = _deployer()
        await deployer.test("my-repo", SSH)
        snapshot.assert_not_awaited()


class TestEnsureRepoCloned:
    def _deployer(self, answers):
        deployer = StackDeployer(GitHubConfig(repos_path="/srv/repos"), None)
        rec = Recorder(answers)
        deployer._run_command = rec
        return deployer, rec

    async def test_fresh_clone(self):
        deployer, rec = self._deployer([
            ("test -d /srv/repos/my-repo/.git", (True, "not_git")),
            ("test -d /srv/repos/my-repo ", (True, "dir_missing")),
        ])
        ok, msg = await deployer._ensure_repo_cloned("my-repo", SSH)
        assert ok and msg == "Repository cloned successfully"
        assert rec.commands[-1] == (
            "mkdir -p /srv/repos && cd /srv/repos && git clone -- git@github.com:owner/my-repo.git"
        )

    async def test_existing_repo_is_force_updated(self):
        deployer, rec = self._deployer([
            ("test -d /srv/repos/my-repo/.git", (True, "is_git")),
            ("test -d /srv/repos/my-repo ", (True, "dir_exists")),
            ("rev-parse --short HEAD", (True, "abc1234\n")),
        ])
        ok, msg = await deployer._ensure_repo_cloned("my-repo", SSH)
        assert ok and msg == "Repository updated successfully (commit abc1234)"
        assert "git config --global --add safe.directory /srv/repos/my-repo" in rec.commands
        assert any("git fetch origin && git reset --hard origin/" in c for c in rec.commands)

    async def test_update_failure_is_not_fatal(self):
        deployer, _ = self._deployer([
            ("test -d /srv/repos/my-repo/.git", (True, "is_git")),
            ("test -d /srv/repos/my-repo ", (True, "dir_exists")),
            ("git fetch origin", (False, "network down")),
        ])
        ok, msg = await deployer._ensure_repo_cloned("my-repo", SSH)
        assert ok and "update failed: network down" in msg

    async def test_non_git_directory_is_backed_up_and_configs_restored(self):
        deployer, rec = self._deployer([
            ("test -d /srv/repos/my-repo/.git", (True, "not_git")),
            ("test -d /srv/repos/my-repo ", (True, "dir_exists")),
        ])
        ok, msg = await deployer._ensure_repo_cloned("my-repo", SSH)
        assert ok and "config files restored" in msg
        assert rec.commands[2].startswith("mv /srv/repos/my-repo /srv/repos/my-repo.backup.")
        assert rec.commands[3] == "cd /srv/repos && git clone -- git@github.com:owner/my-repo.git"
        assert "devops/.env.qa" in rec.commands[4]
        assert rec.commands[5].startswith("rm -rf /srv/repos/my-repo.backup.")

    async def test_failed_reclone_restores_the_backup(self):
        deployer, rec = self._deployer([
            ("test -d /srv/repos/my-repo/.git", (True, "not_git")),
            ("test -d /srv/repos/my-repo ", (True, "dir_exists")),
            ("git clone", (False, "Permission denied (publickey)")),
        ])
        ok, msg = await deployer._ensure_repo_cloned("my-repo", SSH)
        assert not ok and "Permission denied" in msg
        assert rec.commands[-1].startswith("mv /srv/repos/my-repo.backup.")
        assert rec.commands[-1].endswith(" /srv/repos/my-repo")
        assert not any(c.startswith("rm -rf") for c in rec.commands)

    async def test_directory_check_failure(self):
        deployer, rec = self._deployer([("test -d /srv/repos/my-repo ", (False, "ssh down"))])
        ok, msg = await deployer._ensure_repo_cloned("my-repo", SSH)
        assert not ok and "ssh down" in msg

    async def test_git_identity_is_configured_and_quoted(self):
        deployer = StackDeployer(GitHubConfig(repos_path="/r", username="Jane O'Neil",
                                              useremail="j@x.io"), None)
        rec = Recorder([("test -d", (True, "dir_exists is_git"))])
        deployer._run_command = rec
        await deployer._ensure_repo_cloned("my-repo", SSH)
        assert rec.commands[0] == "git config --global user.name 'Jane O'\"'\"'Neil'"
        assert rec.commands[1] == "git config --global user.email j@x.io"


class TestDockerLogin:
    async def test_password_is_quoted_and_piped(self):
        deployer = StackDeployer(GitHubConfig(registry_url="reg.local:5000",
                                              registry_username="ci",
                                              registry_password="p@ss w'rd"), None)
        rec = Recorder()
        deployer._run_command = rec
        await deployer._ensure_docker_login()
        cmd = rec.commands[0]
        assert cmd.endswith("--password-stdin")
        assert shlex.split(cmd.split("|")[0]) == ["echo", "p@ss w'rd"]

    async def test_skipped_without_credentials(self):
        deployer = StackDeployer(GitHubConfig(registry_url="reg.local"), None)
        rec = Recorder()
        deployer._run_command = rec
        await deployer._ensure_docker_login()
        assert rec.commands == []


class TestHasBuildConfig:
    @pytest.mark.parametrize("output,expected", [("YES", True), ("NO", False)])
    async def test_reads_grep_answer(self, output, expected):
        deployer = StackDeployer(GitHubConfig(repos_path="/srv/repos"), None)
        rec = Recorder([("grep", (True, output))])
        deployer._run_command = rec
        assert await deployer.has_build_config("my-repo") is expected
        assert "/srv/repos/my-repo/devops/docker-compose.swarm.yml" in rec.commands[0]

    async def test_command_failure_is_not_buildable(self):
        deployer = StackDeployer(GitHubConfig(), None)
        deployer._run_command = Recorder([("grep", (False, "YES"))])
        assert await deployer.has_build_config("my-repo") is False


# ---------------------------------------------------------------------------
# StackDeployer: command runner dispatch
# ---------------------------------------------------------------------------

class TestRunCommand:
    async def test_prefers_ssh(self):
        deployer = StackDeployer(GitHubConfig(), MagicMock())
        ssh = MagicMock()
        ssh.run_shell_command = AsyncMock(return_value=(True, "ssh"))
        deployer._get_ssh_client = AsyncMock(return_value=ssh)
        assert await deployer._run_command("ls") == (True, "ssh")
        ssh.run_shell_command.assert_awaited_once_with("ls")

    async def test_ssh_streaming_when_callback_given(self):
        deployer = StackDeployer(GitHubConfig(), None)
        ssh = MagicMock()
        ssh.run_shell_command_streaming = AsyncMock(return_value=(True, "streamed"))
        deployer._get_ssh_client = AsyncMock(return_value=ssh)
        cb = lambda line: None  # noqa: E731
        assert await deployer._run_command("ls", output_callback=cb) == (True, "streamed")
        ssh.run_shell_command_streaming.assert_awaited_once_with("ls", cb, None)

    async def test_ssh_streaming_respects_prior_cancel(self):
        deployer = StackDeployer(GitHubConfig(), None)
        ssh = MagicMock()
        ssh.run_shell_command_streaming = AsyncMock()
        deployer._get_ssh_client = AsyncMock(return_value=ssh)
        cancel = asyncio.Event()
        cancel.set()
        assert await deployer._run_command("ls", cancel_event=cancel) == (False, "[Cancelled by user]")
        ssh.run_shell_command_streaming.assert_not_awaited()

    async def test_unresolvable_ssh_host_gets_a_clear_message(self):
        deployer = StackDeployer(GitHubConfig(ssh_host="dockerhost"), None)
        ssh = MagicMock()
        ssh.run_shell_command = AsyncMock(side_effect=OSError(-2, "Name or service not known"))
        deployer._get_ssh_client = AsyncMock(return_value=ssh)
        ok, msg = await deployer._run_command("ls")
        assert ok is False
        assert "dockerhost" in msg and "PULSARCD_GITHUB__SSH_HOST" in msg

    async def test_other_errors_become_failures(self):
        deployer = StackDeployer(GitHubConfig(), None)
        ssh = MagicMock()
        ssh.run_shell_command = AsyncMock(side_effect=RuntimeError("broken pipe"))
        deployer._get_ssh_client = AsyncMock(return_value=ssh)
        assert await deployer._run_command("ls") == (False, "broken pipe")

    async def test_cancelled_error_is_reported_not_raised(self):
        deployer = StackDeployer(GitHubConfig(), None)
        ssh = MagicMock()
        ssh.run_shell_command_streaming = AsyncMock(side_effect=asyncio.CancelledError())
        deployer._get_ssh_client = AsyncMock(return_value=ssh)
        lines = []
        assert await deployer._run_command("ls", output_callback=lines.append) == (False, "Command cancelled")
        assert lines == ["Command cancelled"]

    async def test_falls_back_to_host_client(self):
        host = MagicMock()
        host.run_shell_command = AsyncMock(return_value=(True, "host"))
        deployer = StackDeployer(GitHubConfig(), host)
        assert await deployer._run_command("ls") == (True, "host")

    async def test_streaming_without_ssh_runs_locally(self):
        host = MagicMock()
        host.run_shell_command = AsyncMock()
        deployer = StackDeployer(GitHubConfig(), host)
        deployer._run_local_streaming = AsyncMock(return_value=(True, "local"))
        assert await deployer._run_command("ls", output_callback=print) == (True, "local")
        host.run_shell_command.assert_not_awaited()


class TestSharedSshClient:
    async def test_deployers_share_one_client_per_target(self, monkeypatch):
        from backend import ssh_client

        created = []

        class FakeSSHClient:
            def __init__(self, config):
                created.append(config)
                self.close = AsyncMock()

        monkeypatch.setattr(ssh_client, "SSHClient", FakeSSHClient)
        monkeypatch.setattr(gh, "_SHARED_SSH_CLIENTS", {})

        cfg = dict(ssh_host="dockerhost", ssh_user="deploy")
        a = await StackDeployer(GitHubConfig(**cfg), None)._get_ssh_client()
        b = await StackDeployer(GitHubConfig(**cfg), None)._get_ssh_client()
        c = await StackDeployer(GitHubConfig(ssh_known_hosts_path="/kh", **cfg), None)._get_ssh_client()
        assert a is b
        assert c is not a  # a stricter host-key policy never reuses a laxer connection
        assert len(created) == 2

        await gh.close_shared_ssh_clients()
        a.close.assert_awaited_once()
        assert gh._SHARED_SSH_CLIENTS == {}

    async def test_no_ssh_host_means_no_client(self):
        assert await StackDeployer(GitHubConfig(), None)._get_ssh_client() is None


# ---------------------------------------------------------------------------
# Deployed tags
# ---------------------------------------------------------------------------

class TestDeployedTags:
    def _deployer(self, output, success=True, registry="reg.local"):
        deployer = StackDeployer(GitHubConfig(registry_url=registry), None)
        deployer._run_command = AsyncMock(return_value=(success, output))
        return deployer

    async def test_single_stack_prefers_registry_image(self):
        deployer = self._deployer("redis:7\nreg.local/app:1.2.3\n")
        assert await deployer.get_deployed_stack_tag("My_App") == (True, "1.2.3")
        cmd = deployer._run_command.await_args.args[0]
        assert "name=my-app_" in cmd

    async def test_single_stack_falls_back_to_any_tag(self):
        deployer = self._deployer("redis:7\npostgres\n")
        assert await deployer.get_deployed_stack_tag("app") == (True, "7")

    async def test_single_stack_without_services(self):
        assert await self._deployer("").get_deployed_stack_tag("app") == (False, None)
        assert await self._deployer("x", success=False).get_deployed_stack_tag("app") == (False, None)

    async def test_bulk_splits_prod_and_qa(self):
        output = "\n".join([
            "app_web reg.local/app:1.0.0",
            "app_redis redis:7",
            "qa-app_web reg.local/app:1.0.1",
            "app-api_web reg.local/app-api:2.0.0",
            "other_web reg.local/other:9.9.9",
            "garbage",
        ])
        deployer = self._deployer(output)
        prod, qa = await deployer.get_all_deployed_stack_tags_with_qa(["app", "app-api", "missing"])
        assert prod == {"app": "1.0.0", "app-api": "2.0.0", "missing": None}
        assert qa == {"app": "1.0.1", "app-api": None, "missing": None}
        assert await deployer.get_all_deployed_stack_tags(["app"]) == {"app": "1.0.0"}

    async def test_bulk_failure_returns_all_none(self):
        deployer = self._deployer("", success=False)
        prod, qa = await deployer.get_all_deployed_stack_tags_with_qa(["app"])
        assert prod == {"app": None} and qa == {"app": None}
