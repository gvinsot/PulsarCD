"""GitHub integration service for PulsarCD."""

import asyncio
import base64
import hashlib
import json
import os
import re
import shlex
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

import aiohttp
import structlog

from .config import GitHubConfig

logger = structlog.get_logger()


def _shell_quote_path(path: str) -> str:
    """Quote a file-system path for shell use, preserving ``~/`` tilde expansion.

    ``shlex.quote("~/repos")`` produces ``'~/repos'`` which prevents the
    shell from expanding ``~``.  This helper keeps the ``~/`` prefix
    unquoted so the remote (or local) shell can expand it, while still
    quoting the rest of the path to handle spaces and special characters.
    """
    if path.startswith("~/"):
        return "~/" + shlex.quote(path[2:])
    return shlex.quote(path)


# Repository names and clone URLs are interpolated into file-system paths and
# git commands that are executed by a shell on the build/deploy host. Only a
# minimal, well-known charset is accepted so no shell metacharacter, path
# traversal or git option can ever reach that shell.
# Anchored with \A and \Z, never ^/$: without re.MULTILINE, "$" also matches
# just before a trailing newline, so a name ending in "\n" would have passed
# the charset check this pattern exists to enforce.
# A leading "." or "_" is allowed because GitHub allows it and nearly every
# organisation owns a ".github" repository: refusing it made build/deploy/env
# fail on a real repository, and has_build_config() swallowed the error into a
# silent "no build config" skip in the UI. Path traversal stays impossible: a
# segment may not contain "/" (outside the charset) and may not be exactly "."
# or ".." (the leading lookahead), and _validate_repo_name additionally refuses
# any ".." sequence.
_SEGMENT_BODY = r"(?![.]{1,2}(?![A-Za-z0-9._-]))[A-Za-z0-9._][A-Za-z0-9._-]{0,99}"
_REPO_NAME_RE = re.compile(r"\A" + _SEGMENT_BODY + r"\Z")

_HOST_PATTERN = r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,252}[A-Za-z0-9])?"
_PATH_SEGMENT_PATTERN = _SEGMENT_BODY

# git@host:owner/repo.git
_SSH_URL_SCP_RE = re.compile(
    r"\Agit@(?P<host>" + _HOST_PATTERN + r"):"
    r"(?P<owner>" + _PATH_SEGMENT_PATTERN + r")/"
    r"(?P<repo>" + _PATH_SEGMENT_PATTERN + r")[.]git\Z"
)
# ssh://git@host[:port]/owner/repo.git
_SSH_URL_SSH_RE = re.compile(
    r"\Assh://git@(?P<host>" + _HOST_PATTERN + r")(?::(?P<port>[0-9]{1,5}))?/"
    r"(?P<owner>" + _PATH_SEGMENT_PATTERN + r")/"
    r"(?P<repo>" + _PATH_SEGMENT_PATTERN + r")[.]git\Z"
)
# https://host/owner/repo[.git]
_SSH_URL_HTTPS_RE = re.compile(
    r"\Ahttps://(?P<host>" + _HOST_PATTERN + r")/"
    r"(?P<owner>" + _PATH_SEGMENT_PATTERN + r")/"
    r"(?P<repo>" + _PATH_SEGMENT_PATTERN + r")(?:[.]git)?\Z"
)


def _validate_repo_name(name: str) -> str:
    """Validate a repository name before it is used to build a shell path.

    Args:
        name: Repository name coming from an HTTP parameter.

    Returns:
        The validated name, unchanged.

    Raises:
        ValueError: If the name is empty, longer than 100 characters, starts
            with ``-`` (which a command could read as an option), contains
            ``..`` or any character outside ``[A-Za-z0-9._-]``.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Invalid repository name: value is empty")
    if name.startswith("-"):
        raise ValueError(f"Invalid repository name {name!r}: must not start with '-'")
    if ".." in name:
        raise ValueError(f"Invalid repository name {name!r}: must not contain '..'")
    if not _REPO_NAME_RE.match(name):
        raise ValueError(
            f"Invalid repository name {name!r}: only letters, digits, '.', '_' and '-' "
            "are allowed (1-100 characters, starting with a letter or a digit)"
        )
    return name


def _validate_ssh_url(url: str) -> str:
    """Validate a git clone URL against an allowlist of known-safe shapes.

    Accepted forms are ``git@host:owner/repo.git``,
    ``ssh://git@host[:port]/owner/repo.git`` and
    ``https://host/owner/repo[.git]``. Anything else -- exotic transports
    (``ext::``, ``file://``), option-looking values (``--upload-pack=...``) or
    shell metacharacters -- is rejected.

    Args:
        url: Clone URL coming from an HTTP parameter.

    Returns:
        The validated URL, unchanged.

    Raises:
        ValueError: If the URL does not match one of the accepted forms.
    """
    if not isinstance(url, str) or not url:
        raise ValueError("Invalid clone URL: value is empty")
    if len(url) > 512:
        raise ValueError("Invalid clone URL: value is too long")
    if ".." in url:
        raise ValueError(f"Invalid clone URL {url!r}: must not contain '..'")
    for pattern in (_SSH_URL_SCP_RE, _SSH_URL_SSH_RE, _SSH_URL_HTTPS_RE):
        if pattern.match(url):
            return url
    raise ValueError(
        f"Invalid clone URL {url!r}: expected git@host:owner/repo.git, "
        "ssh://git@host[:port]/owner/repo.git or https://host/owner/repo.git"
    )


# Cache TTLs
STARRED_REPOS_CACHE_TTL = timedelta(minutes=1)
BRANCHES_CACHE_TTL = timedelta(minutes=5)
COMMITS_CACHE_TTL = timedelta(seconds=30)
COMMIT_DIFF_CACHE_TTL = timedelta(minutes=10)
TAGS_CACHE_TTL = timedelta(minutes=2)

# File path for persistent tag date cache.
# It lives on the data volume, not next to the code: /app belongs to the image's
# non-root user, so the container (which runs as root with "cap_drop: ALL", hence
# without the permission-bypass capability) cannot write there -- and a file
# written inside the image layer would be lost on every deploy anyway.
TAG_DATE_CACHE_FILE = (
    Path(os.environ.get("PULSARCD_DATA_DIR", "/data")) / ".tag_date_cache.json"
)


class GitHubService:
    """Service for interacting with GitHub API."""

    def __init__(self, config: GitHubConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        # Cache for starred repos
        self._starred_repos_cache: Optional[List[Dict[str, Any]]] = None
        self._starred_repos_cache_time: Optional[datetime] = None
        # Cache for branches: key = "owner/repo"
        self._branches_cache: Dict[str, Tuple[List[Dict[str, Any]], datetime]] = {}
        # Cache for commits: key = "owner/repo/branch/per_page/page"
        self._commits_cache: Dict[str, Tuple[Dict[str, Any], datetime]] = {}
        # Cache for commit diffs: key = "owner/repo/sha"
        self._commit_diff_cache: Dict[str, Tuple[Dict[str, Any], datetime]] = {}
        # Cache for tags: key = "owner/repo/limit"
        self._tags_cache: Dict[str, Tuple[Dict[str, Any], datetime]] = {}
        # Persistent cache for tag dates (SHA -> date string)
        # SHAs are immutable so this cache never expires
        self._tag_date_cache: Optional[Dict[str, str]] = None
        self._tag_date_cache_dirty: bool = False
        # Rate limit state
        self._rate_limit_reset: Optional[datetime] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session. Recreates if token has changed."""
        token = self.config.token or ""
        if self._session is not None and not self._session.closed:
            # Check if the token changed since the session was created
            if getattr(self, "_session_token", None) != token:
                logger.info("GitHub token changed, recreating session")
                await self._session.close()
                self._session = None
        if self._session is None or self._session.closed:
            headers = {
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "PulsarCD",
            }
            if token:
                headers["Authorization"] = f"token {token}"
            connector = aiohttp.TCPConnector(limit=20, limit_per_host=20)
            self._session = aiohttp.ClientSession(headers=headers, connector=connector)
            self._session_token = token
        return self._session

    async def close(self):
        """Close the session."""
        if self._session and not self._session.closed:
            await self._session.close()

    def _is_cache_valid(self) -> bool:
        """Check if the starred repos cache is still valid."""
        if self._starred_repos_cache is None or self._starred_repos_cache_time is None:
            return False
        return datetime.now() - self._starred_repos_cache_time < STARRED_REPOS_CACHE_TTL

    def _get_cached(self, cache: dict, key: str, ttl: timedelta) -> Optional[Any]:
        """Get value from a TTL cache dict. Returns None if missing or expired."""
        entry = cache.get(key)
        if entry is None:
            return None
        value, cached_at = entry
        if datetime.now() - cached_at >= ttl:
            del cache[key]
            return None
        return value

    def _set_cached(self, cache: dict, key: str, value: Any):
        """Store a value in a TTL cache dict."""
        cache[key] = (value, datetime.now())

    def _is_rate_limited(self) -> bool:
        """Check if we are currently rate-limited."""
        if self._rate_limit_reset is None:
            return False
        if datetime.now() >= self._rate_limit_reset:
            self._rate_limit_reset = None
            return False
        return True

    @staticmethod
    def _parse_permission_error(error_text: str, status: int, resource: str, required_permission: str) -> str:
        """Parse GitHub API error and return a user-friendly hint if it's a permission issue."""
        if status == 403 and "Resource not accessible by personal access token" in error_text:
            return (
                f"GitHub token lacks the '{required_permission}' permission required to access {resource}. "
                f"Go to GitHub Settings > Developer settings > Personal access tokens > Fine-grained tokens, "
                f"edit your token and enable '{required_permission}' under Repository permissions. "
                f"Alternatively, use a classic token with the 'repo' scope."
            )
        if status == 403:
            return f"Access denied (403) when fetching {resource}. Check that your GitHub token has the required permissions."
        if status == 404:
            return f"Repository not found or not accessible (404) when fetching {resource}."
        return f"GitHub API returned status {status} when fetching {resource}."

    def _handle_rate_limit(self, response_headers, status: int = 200) -> bool:
        """Check response headers for rate limit. Returns True if rate-limited."""
        remaining = response_headers.get("X-RateLimit-Remaining")
        if remaining is not None and int(remaining) <= 0:
            reset_ts = response_headers.get("X-RateLimit-Reset")
            if reset_ts:
                self._rate_limit_reset = datetime.fromtimestamp(int(reset_ts))
            else:
                self._rate_limit_reset = datetime.now() + timedelta(minutes=5)
            logger.warning("GitHub API rate limit hit, backing off",
                          reset_at=str(self._rate_limit_reset))
            return True
        # Secondary rate limit: 403 with Retry-After header
        retry_after = response_headers.get("Retry-After")
        if status == 403 and retry_after:
            try:
                wait_seconds = int(retry_after)
            except ValueError:
                wait_seconds = 60
            self._rate_limit_reset = datetime.now() + timedelta(seconds=wait_seconds)
            logger.warning("GitHub secondary rate limit hit, backing off",
                          retry_after=retry_after, reset_at=str(self._rate_limit_reset))
            return True
        return False

    def _load_tag_date_cache(self) -> Dict[str, str]:
        """Load tag date cache from file."""
        if self._tag_date_cache is not None:
            return self._tag_date_cache
        
        self._tag_date_cache = {}
        try:
            if TAG_DATE_CACHE_FILE.exists():
                with open(TAG_DATE_CACHE_FILE, "r") as f:
                    self._tag_date_cache = json.load(f)
                logger.debug("Loaded tag date cache", entries=len(self._tag_date_cache))
        except Exception as e:
            logger.warning("Failed to load tag date cache", error=str(e))
            self._tag_date_cache = {}
        return self._tag_date_cache

    def _save_tag_date_cache(self):
        """Save tag date cache to file if modified."""
        if not self._tag_date_cache_dirty or self._tag_date_cache is None:
            return
        try:
            with open(TAG_DATE_CACHE_FILE, "w") as f:
                json.dump(self._tag_date_cache, f)
            self._tag_date_cache_dirty = False
            logger.debug("Saved tag date cache", entries=len(self._tag_date_cache))
        except Exception as e:
            logger.warning("Failed to save tag date cache", error=str(e))

    def invalidate_cache(self):
        """Invalidate all in-memory caches."""
        self._starred_repos_cache = None
        self._starred_repos_cache_time = None
        self._branches_cache.clear()
        self._commits_cache.clear()
        self._commit_diff_cache.clear()
        self._tags_cache.clear()
        logger.info("All GitHub caches invalidated")

    async def get_starred_repos(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Get list of repositories to display in Stacks.

        Behaviour depends on ``config.repos_mode``:
        - ``"all"``     → fetch all repos accessible to the authenticated user
        - ``"starred"`` → fetch only starred repos (legacy behaviour)

        Args:
            force_refresh: If True, bypass cache and fetch fresh data.

        Returns:
            List of repo info dicts with name, full_name, description, url, etc.
        """
        # Check cache first (unless force refresh requested)
        if not force_refresh and self._is_cache_valid():
            logger.debug("Returning cached repos", count=len(self._starred_repos_cache))
            return self._starred_repos_cache

        if not self.config.token:
            logger.warning("GitHub token not configured")
            return []

        # If rate-limited, return stale cache if available
        if self._is_rate_limited():
            if self._starred_repos_cache:
                logger.info("Rate limited, returning stale repos cache")
                return self._starred_repos_cache
            return []

        session = await self._get_session()

        starred_only = (self.config.repos_mode or "all").lower() == "starred"

        if starred_only:
            url = "https://api.github.com/user/starred"
            params = {"per_page": 100, "sort": "updated"}
            scope_hint = "Starring: Read"
        else:
            url = "https://api.github.com/user/repos"
            params = {"per_page": 100, "sort": "updated", "affiliation": "owner,collaborator,organization_member"}
            scope_hint = "Contents: Read"

        repos = []
        page = 1

        # Identify which token is in use without writing any part of it: these
        # lines land in the log store that every viewer account can search, and
        # a prefix leaks real secret characters (a `ghp_` PAT gives up ~6).
        # A truncated SHA-256 tells two configured tokens apart, which is all
        # the debugging needed, and is not reversible.
        token_fp = (hashlib.sha256(self.config.token.encode()).hexdigest()[:8]
                    if self.config.token else "none")
        mode_label = "starred" if starred_only else "all"
        logger.info("Fetching repos", mode=mode_label, token_fp=token_fp, url=url)

        try:
            while True:
                params["page"] = page
                async with session.get(url, params=params) as response:
                    # Log response headers for debugging scopes
                    scopes = response.headers.get("X-OAuth-Scopes", "none")
                    rate_limit = response.headers.get("X-RateLimit-Remaining", "?")
                    logger.info("GitHub API response",
                               status=response.status,
                               page=page,
                               scopes=scopes,
                               rate_limit=rate_limit)

                    self._handle_rate_limit(response.headers, response.status)
                    if response.status == 403 and self._is_rate_limited():
                        logger.warning("GitHub rate limit exceeded during repos fetch")
                        if self._starred_repos_cache:
                            return self._starred_repos_cache
                        break
                    if response.status != 200:
                        error_text = await response.text()
                        error_msg = self._parse_permission_error(error_text, response.status, "repos", scope_hint)
                        logger.error("GitHub API error", status=response.status, error=error_text, hint=error_msg)
                        break

                    data = await response.json()
                    logger.info("GitHub API data received", page=page, count=len(data) if data else 0)
                    if not data:
                        break

                    for repo in data:
                        repos.append({
                            "id": repo["id"],
                            "name": repo["name"],
                            "full_name": repo["full_name"],
                            "description": repo["description"] or "",
                            "html_url": repo["html_url"],
                            "ssh_url": repo["ssh_url"],
                            "clone_url": repo["clone_url"],
                            "language": repo["language"],
                            "stargazers_count": repo["stargazers_count"],
                            "updated_at": repo["updated_at"],
                            "pushed_at": repo.get("pushed_at", ""),
                            "owner": repo["owner"]["login"],
                            "private": repo["private"],
                        })

                    # Check if there are more pages
                    if len(data) < 100:
                        break
                    page += 1

            # Update cache
            self._starred_repos_cache = repos
            self._starred_repos_cache_time = datetime.now()
            logger.info("Fetched and cached repos", mode=mode_label, count=len(repos))
            return repos

        except Exception as e:
            logger.error("Failed to fetch repos", mode=mode_label, error=str(e))
            return []

    def is_configured(self) -> bool:
        """Check if GitHub integration is properly configured."""
        return bool(self.config.token)

    async def get_repo_branches(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Get list of branches for a repository.

        Args:
            owner: Repository owner (user or org)
            repo: Repository name

        Returns:
            List of branch info dicts with name, commit sha, etc.
        """
        cache_key = f"{owner}/{repo}"
        cached = self._get_cached(self._branches_cache, cache_key, BRANCHES_CACHE_TTL)
        if cached is not None:
            logger.debug("Returning cached branches", repo=cache_key, count=len(cached))
            return cached

        if not self.config.token:
            logger.warning("GitHub token not configured")
            return []

        if self._is_rate_limited():
            stale = self._branches_cache.get(cache_key)
            if stale:
                logger.info("Rate limited, returning stale branches cache", repo=cache_key)
                return stale[0]
            return []

        session = await self._get_session()
        url = f"https://api.github.com/repos/{owner}/{repo}/branches"
        params = {"per_page": 100}

        branches = []

        try:
            async with session.get(url, params=params) as response:
                self._handle_rate_limit(response.headers, response.status)
                if response.status == 403 and self._is_rate_limited():
                    logger.warning("GitHub rate limit exceeded during branches fetch", repo=cache_key)
                    stale = self._branches_cache.get(cache_key)
                    if stale:
                        return stale[0]
                    return []
                if response.status != 200:
                    error_text = await response.text()
                    error_msg = self._parse_permission_error(error_text, response.status, "branches", "Contents: Read")
                    logger.error("GitHub API error getting branches", status=response.status, error=error_text, hint=error_msg)
                    return []

                data = await response.json()
                for branch in data:
                    branches.append({
                        "name": branch["name"],
                        "sha": branch["commit"]["sha"],
                        "protected": branch.get("protected", False),
                    })

            # Sort branches: main/master first, then alphabetically
            def branch_sort_key(b):
                name = b["name"].lower()
                if name == "main":
                    return (0, name)
                elif name == "master":
                    return (1, name)
                else:
                    return (2, name)

            branches.sort(key=branch_sort_key)
            self._set_cached(self._branches_cache, cache_key, branches)
            logger.info("Fetched and cached branches", repo=f"{owner}/{repo}", count=len(branches))
            return branches

        except Exception as e:
            logger.error("Failed to fetch branches", repo=f"{owner}/{repo}", error=str(e))
            return []

    async def get_repo_tags(self, owner: str, repo: str, limit: int = 10) -> Dict[str, Any]:
        """Get list of tags for a repository, grouped by the branch they were created from.

        Args:
            owner: Repository owner (user or org)
            repo: Repository name
            limit: Maximum number of tags to return

        Returns:
            Dict with tags grouped by branch and metadata
        """
        cache_key = f"{owner}/{repo}/{limit}"
        cached = self._get_cached(self._tags_cache, cache_key, TAGS_CACHE_TTL)
        if cached is not None:
            logger.debug("Returning cached tags", repo=f"{owner}/{repo}", count=len(cached.get("tags", [])))
            return cached

        if not self.config.token:
            logger.warning("GitHub token not configured")
            return {"tags": [], "branches": {}}

        if self._is_rate_limited():
            stale = self._tags_cache.get(cache_key)
            if stale:
                logger.info("Rate limited, returning stale tags cache", repo=f"{owner}/{repo}")
                return stale[0]
            return {"tags": [], "branches": {}}

        session = await self._get_session()
        date_cache = self._load_tag_date_cache()

        # Get all tags with their commit info
        tags_url = f"https://api.github.com/repos/{owner}/{repo}/tags"
        tags = []

        try:
            async with session.get(tags_url, params={"per_page": min(limit, 100)}) as response:
                self._handle_rate_limit(response.headers, response.status)
                if response.status == 403 and self._is_rate_limited():
                    logger.warning("GitHub rate limit exceeded during tags fetch", repo=f"{owner}/{repo}")
                    stale = self._tags_cache.get(cache_key)
                    if stale:
                        return stale[0]
                    return {"tags": [], "branches": {}}
                if response.status != 200:
                    error_text = await response.text()
                    error_msg = self._parse_permission_error(error_text, response.status, "tags", "Contents: Read")
                    logger.error("GitHub API error getting tags", status=response.status, error=error_text, hint=error_msg)
                    return {"tags": [], "branches": {}}

                data = await response.json()
                
                # Build tag list, using cache for dates when available
                tags_needing_dates = []
                for tag_data in data[:limit]:
                    sha = tag_data["commit"]["sha"]
                    tag_info = {
                        "name": tag_data["name"],
                        "sha": sha,
                        "zipball_url": tag_data.get("zipball_url"),
                        "created_at": date_cache.get(sha),  # May be None if not cached
                    }
                    tags.append(tag_info)
                    if tag_info["created_at"] is None:
                        tags_needing_dates.append((tag_info, tag_data["commit"]["url"]))
                
                # Only fetch dates for tags not in cache (limited concurrency)
                if tags_needing_dates:
                    logger.debug("Fetching dates for uncached tags", count=len(tags_needing_dates))
                    sem = asyncio.Semaphore(5)

                    async def fetch_commit_date(tag_info, commit_url):
                        async with sem:
                            try:
                                async with session.get(commit_url) as commit_response:
                                    if commit_response.status == 200:
                                        commit_data = await commit_response.json()
                                        date = commit_data.get("commit", {}).get("committer", {}).get("date")
                                        if date:
                                            tag_info["created_at"] = date
                                            date_cache[tag_info["sha"]] = date
                                            self._tag_date_cache_dirty = True
                            except Exception as e:
                                logger.debug("Could not fetch commit date for tag", tag=tag_info["name"], error=str(e))

                    await asyncio.gather(*[fetch_commit_date(t, url) for t, url in tags_needing_dates])
                    
                    # Save cache if we fetched new dates
                    self._save_tag_date_cache()
                else:
                    logger.debug("All tag dates served from cache", count=len(tags))

            # Get branches to associate tags with branches
            branches = await self.get_repo_branches(owner, repo)
            branch_shas = {b["sha"]: b["name"] for b in branches}

            # Try to get commit info for each tag to find which branch it belongs to
            # This is a simplified approach - we'll group tags by checking if they match branch tips
            # or by extracting branch info from commit history
            tags_by_branch = {"main": [], "other": []}

            for tag in tags:
                # For now, put all tags in a list - the frontend will display them
                # In a more sophisticated implementation, we could trace the commit history
                tags_by_branch["main"].append(tag)

            result = {
                "tags": tags,
                "branches": [b["name"] for b in branches],
                "default_branch": branches[0]["name"] if branches else "main",
            }
            self._set_cached(self._tags_cache, cache_key, result)
            logger.info("Fetched and cached tags", repo=f"{owner}/{repo}", count=len(tags), cached=len(tags) - len(tags_needing_dates))
            return result

        except Exception as e:
            logger.error("Failed to fetch tags", repo=f"{owner}/{repo}", error=str(e))
            return {"tags": [], "branches": []}

    async def get_latest_tag(self, owner: str, repo: str) -> Optional[str]:
        """Get the most recent tag name for a repo (= latest built version).

        Args:
            owner: Repository owner
            repo: Repository name

        Returns:
            Tag name string or None
        """
        tags_data = await self.get_repo_tags(owner, repo, limit=1)
        tags = tags_data.get("tags", [])
        if tags:
            return tags[0]["name"]
        return None

    async def create_tag(self, owner: str, repo: str, tag_name: str, commit_sha: str) -> Dict[str, Any]:
        """Create a lightweight tag on a commit via GitHub API.

        Args:
            owner: Repository owner
            repo: Repository name
            tag_name: Tag name (e.g., "v1.0.5")
            commit_sha: Full SHA of the commit to tag

        Returns:
            Dict with success status and tag info
        """
        if not self.config.token:
            return {"success": False, "error": "GitHub token not configured"}

        session = await self._get_session()
        url = f"https://api.github.com/repos/{owner}/{repo}/git/refs"
        payload = {
            "ref": f"refs/tags/{tag_name}",
            "sha": commit_sha,
        }

        try:
            async with session.post(url, json=payload) as response:
                self._handle_rate_limit(response.headers, response.status)
                if response.status == 201:
                    data = await response.json()
                    logger.info("Created tag", repo=f"{owner}/{repo}", tag=tag_name, sha=commit_sha[:7])
                    # Invalidate tags cache
                    keys_to_remove = [k for k in self._tags_cache if k.startswith(f"{owner}/{repo}/")]
                    for k in keys_to_remove:
                        del self._tags_cache[k]
                    return {"success": True, "tag": tag_name, "sha": commit_sha}
                else:
                    error_text = await response.text()
                    logger.error("Failed to create tag", repo=f"{owner}/{repo}", tag=tag_name, status=response.status, error=error_text)
                    return {"success": False, "error": error_text}
        except Exception as e:
            logger.error("Exception creating tag", repo=f"{owner}/{repo}", tag=tag_name, error=str(e))
            return {"success": False, "error": str(e)}

    async def get_untagged_commits(self, owner: str, repo: str, limit: int = 10, branch: str = None) -> Dict[str, Any]:
        """Get recent commits that don't have a tag pointing to them.

        Compares the latest commits on the given branch against all known tags
        to find commits that haven't been tagged (= not yet built/deployed).

        Args:
            owner: Repository owner
            repo: Repository name
            limit: Max commits to check
            branch: Branch name (defaults to repo default branch)

        Returns:
            Dict with untagged_commits list and latest_tag info
        """
        # Fetch recent commits and tags in parallel
        commits_task = self.get_repo_commits(owner, repo, branch=branch, per_page=limit)
        tags_task = self.get_repo_tags(owner, repo, limit=50)

        commits_data, tags_data = await asyncio.gather(commits_task, tags_task)

        commits = commits_data.get("commits", [])
        tags = tags_data.get("tags", [])
        tagged_shas = {t["sha"] for t in tags}

        untagged = []
        for commit in commits:
            if commit["sha"] not in tagged_shas:
                untagged.append(commit)
            else:
                # Stop at the first tagged commit - everything before it is "new"
                break

        latest_tag = tags[0] if tags else None

        return {
            "untagged_commits": untagged,
            "latest_tag": latest_tag,
            "total_commits_checked": len(commits),
        }

    async def get_next_version(self, owner: str, repo: str) -> str:
        """Compute the next patch version based on the latest tag.

        E.g., if latest tag is v1.0.5, returns "1.0.6".
        If no tags exist, returns "1.0.0".
        """
        tags_data = await self.get_repo_tags(owner, repo, limit=1)
        tags = tags_data.get("tags", [])
        if not tags:
            return "1.0.0"

        m = re.match(r'^v?(\d+)\.(\d+)\.(\d+)$', tags[0]["name"])
        if m:
            major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return f"{major}.{minor}.{patch + 1}"
        return "1.0.0"

    async def get_repo_commits(self, owner: str, repo: str, branch: str = None, per_page: int = 50, page: int = 1) -> Dict[str, Any]:
        """Get commit history for a repository branch.

        Args:
            owner: Repository owner
            repo: Repository name
            branch: Branch name (defaults to repo default branch)
            per_page: Number of commits per page (max 100)
            page: Page number for pagination

        Returns:
            Dict with commits list and pagination info
        """
        cache_key = f"{owner}/{repo}/{branch or 'default'}/{per_page}/{page}"
        cached = self._get_cached(self._commits_cache, cache_key, COMMITS_CACHE_TTL)
        if cached is not None:
            logger.debug("Returning cached commits", repo=f"{owner}/{repo}", branch=branch)
            return cached

        if not self.config.token:
            logger.warning("GitHub token not configured")
            return {"commits": [], "has_more": False, "error": "GitHub token not configured"}

        if self._is_rate_limited():
            stale = self._commits_cache.get(cache_key)
            if stale:
                logger.info("Rate limited, returning stale commits cache", repo=f"{owner}/{repo}")
                return stale[0]
            return {"commits": [], "has_more": False, "error": "Rate limited by GitHub API"}

        session = await self._get_session()
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        params = {"per_page": min(per_page, 100), "page": page}
        if branch:
            params["sha"] = branch

        try:
            async with session.get(url, params=params) as response:
                self._handle_rate_limit(response.headers, response.status)
                if response.status == 403 and self._is_rate_limited():
                    logger.warning("GitHub rate limit exceeded during commits fetch", repo=f"{owner}/{repo}")
                    return {"commits": [], "has_more": False, "error": "Rate limited by GitHub API"}
                if response.status != 200:
                    error_text = await response.text()
                    error_msg = self._parse_permission_error(error_text, response.status, "commits", "Contents: Read")
                    logger.error("GitHub API error getting commits", status=response.status, error=error_text, hint=error_msg)
                    return {"commits": [], "has_more": False, "error": error_msg}

                data = await response.json()
                commits = []
                for c in data:
                    commits.append({
                        "sha": c["sha"],
                        "short_sha": c["sha"][:7],
                        "message": c["commit"]["message"],
                        "author_name": c["commit"]["author"]["name"],
                        "author_avatar": c["author"]["avatar_url"] if c.get("author") else None,
                        "date": c["commit"]["author"]["date"],
                        "parents": [p["sha"] for p in c.get("parents", [])],
                    })

                has_more = len(data) == per_page
                result = {"commits": commits, "has_more": has_more}
                self._set_cached(self._commits_cache, cache_key, result)
                logger.info("Fetched and cached commits", repo=f"{owner}/{repo}", branch=branch, count=len(commits), page=page)
                return result

        except Exception as e:
            logger.error("Failed to fetch commits", repo=f"{owner}/{repo}", error=str(e))
            return {"commits": [], "has_more": False, "error": f"Failed to fetch commits: {str(e)}"}

    async def get_commit_diff(self, owner: str, repo: str, sha: str) -> Dict[str, Any]:
        """Get the diff (changed files) for a specific commit.

        Args:
            owner: Repository owner
            repo: Repository name
            sha: Full commit SHA

        Returns:
            Dict with commit info and list of changed files with patch data
        """
        cache_key = f"{owner}/{repo}/{sha}"
        cached = self._get_cached(self._commit_diff_cache, cache_key, COMMIT_DIFF_CACHE_TTL)
        if cached is not None:
            logger.debug("Returning cached commit diff", repo=f"{owner}/{repo}", sha=sha[:7])
            return cached

        if not self.config.token:
            logger.warning("GitHub token not configured")
            return {"files": [], "stats": {}}

        if self._is_rate_limited():
            stale = self._commit_diff_cache.get(cache_key)
            if stale:
                logger.info("Rate limited, returning stale commit diff cache", repo=f"{owner}/{repo}")
                return stale[0]
            return {"files": [], "stats": {}}

        session = await self._get_session()
        url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}"

        try:
            async with session.get(url) as response:
                self._handle_rate_limit(response.headers, response.status)
                if response.status == 403 and self._is_rate_limited():
                    logger.warning("GitHub rate limit exceeded during commit diff fetch", repo=f"{owner}/{repo}")
                    return {"files": [], "stats": {}, "error": "Rate limited by GitHub API"}
                if response.status != 200:
                    error_text = await response.text()
                    error_msg = self._parse_permission_error(error_text, response.status, "commit diff", "Contents: Read")
                    logger.error("GitHub API error getting commit diff", status=response.status, error=error_text, hint=error_msg)
                    return {"files": [], "stats": {}, "error": error_msg}

                data = await response.json()
                files = []
                for f in data.get("files", []):
                    files.append({
                        "filename": f["filename"],
                        "status": f["status"],
                        "additions": f["additions"],
                        "deletions": f["deletions"],
                        "changes": f["changes"],
                        "patch": f.get("patch", ""),
                        "previous_filename": f.get("previous_filename"),
                    })

                result = {
                    "sha": data["sha"],
                    "message": data["commit"]["message"],
                    "author_name": data["commit"]["author"]["name"],
                    "date": data["commit"]["author"]["date"],
                    "stats": data.get("stats", {}),
                    "files": files,
                }
                self._set_cached(self._commit_diff_cache, cache_key, result)
                return result

        except Exception as e:
            logger.error("Failed to fetch commit diff", repo=f"{owner}/{repo}", sha=sha, error=str(e))
            return {"files": [], "stats": {}}

    async def validate_branch(self, owner: str, repo: str, branch: str) -> tuple[bool, str]:
        """Validate that a branch exists in the repository.

        Args:
            owner: Repository owner
            repo: Repository name
            branch: Branch name to validate

        Returns:
            Tuple of (is_valid, error_message)
            Returns (True, "") if API is unavailable (best-effort validation)
        """
        branches = await self.get_repo_branches(owner, repo)
        
        # If we couldn't fetch branches (API error, permissions, etc.), 
        # allow the operation to proceed - actual git commands will validate
        if not branches:
            logger.warning("Could not validate branch via API, allowing operation to proceed", 
                          repo=f"{owner}/{repo}", branch=branch)
            return True, ""
        
        branch_names = [b["name"] for b in branches]

        if branch in branch_names:
            return True, ""
        return False, f"Branch '{branch}' not found. Available branches: {', '.join(branch_names[:5])}"

    async def validate_commit(self, owner: str, repo: str, commit_id: str) -> tuple[bool, str]:
        """Validate that a commit exists in the repository.

        Args:
            owner: Repository owner
            repo: Repository name
            commit_id: Commit SHA to validate

        Returns:
            Tuple of (is_valid, error_message)
            Returns (True, "") if API is unavailable (best-effort validation)
        """
        if not self.config.token:
            logger.warning("GitHub token not configured, skipping commit validation")
            return True, ""

        if self._is_rate_limited():
            logger.warning("Rate limited, skipping commit validation", repo=f"{owner}/{repo}", commit=commit_id)
            return True, ""

        session = await self._get_session()
        url = f"https://api.github.com/repos/{owner}/{repo}/commits/{commit_id}"

        try:
            async with session.get(url) as response:
                self._handle_rate_limit(response.headers, response.status)
                if response.status == 200:
                    return True, ""
                elif response.status == 404:
                    return False, f"Commit '{commit_id}' not found in repository"
                elif response.status == 403:
                    # Permission error - allow operation to proceed, git will validate
                    logger.warning("Could not validate commit via API (permission denied), allowing operation to proceed",
                                  repo=f"{owner}/{repo}", commit=commit_id)
                    return True, ""
                else:
                    # Other errors - log but allow to proceed
                    logger.warning("Could not validate commit via API, allowing operation to proceed",
                                  repo=f"{owner}/{repo}", commit=commit_id, status=response.status)
                    return True, ""
        except Exception as e:
            logger.warning("Error validating commit via API, allowing operation to proceed",
                          repo=f"{owner}/{repo}", commit=commit_id, error=str(e))
            return True, ""


# StackDeployer is instantiated per request, but the SSH connection it uses must
# not be: asyncssh keeps the socket open until close() is called, so a fresh
# client per request leaked one ESTABLISHED connection each time and exhausted
# the process fd limit (RLIMIT_NOFILE) after a few hundred polls of the
# dashboard endpoints. Share one SSHClient per SSH target instead; SSHClient
# already reconnects on its own when the connection drops.
_SHARED_SSH_CLIENTS: Dict[Tuple[Any, ...], Any] = {}
_SHARED_SSH_LOCK = asyncio.Lock()


async def close_shared_ssh_clients() -> None:
    """Close the SSH connections shared by every StackDeployer (shutdown only)."""
    async with _SHARED_SSH_LOCK:
        clients = list(_SHARED_SSH_CLIENTS.values())
        _SHARED_SSH_CLIENTS.clear()
    for client in clients:
        try:
            await client.close()
        except Exception:
            pass


class StackDeployer:
    """Service for building and deploying stacks from GitHub repos."""

    def __init__(self, config: GitHubConfig, host_client=None):
        """Initialize the deployer.

        Args:
            config: GitHub configuration
            host_client: Host client for executing commands (fallback if no SSH configured)
        """
        self.config = config
        self.host_client = host_client
        self._ssh_client = None

    async def _get_ssh_client(self):
        """Get or create SSH client for host commands."""
        if self._ssh_client is not None:
            return self._ssh_client
        
        # If SSH host is configured, use SSH
        if self.config.ssh_host:
            from .ssh_client import SSHClient
            from .config import HostConfig

            # The host key policy is part of the identity of the connection:
            # two deployers configured with different policies must not share
            # a client that was opened under the laxer of the two.
            key = (
                self.config.ssh_host,
                self.config.ssh_port,
                self.config.ssh_user,
                self.config.ssh_key_path,
                self.config.ssh_known_hosts_path,
            )
            async with _SHARED_SSH_LOCK:
                client = _SHARED_SSH_CLIENTS.get(key)
                if client is None:
                    ssh_config = HostConfig(
                        name="github-deploy-host",
                        hostname=self.config.ssh_host,
                        port=self.config.ssh_port,
                        username=self.config.ssh_user,
                        ssh_key_path=self.config.ssh_key_path,
                        ssh_known_hosts_path=self.config.ssh_known_hosts_path,
                        mode="ssh"
                    )
                    client = SSHClient(ssh_config)
                    _SHARED_SSH_CLIENTS[key] = client
                    logger.info("Using SSH for stack operations",
                               host=self.config.ssh_host,
                               user=self.config.ssh_user,
                               known_hosts=self.config.ssh_known_hosts_path)
            self._ssh_client = client

        return self._ssh_client

    async def close(self):
        """Drop the reference to the shared SSH client.

        The connection itself is owned by ``_SHARED_SSH_CLIENTS`` and outlives
        this deployer; closing it here would tear it down for every other
        in-flight request. Use :func:`close_shared_ssh_clients` at shutdown.
        """
        self._ssh_client = None

    async def _ensure_git_configured(self) -> None:
        """Ensure git is configured with user name and email."""
        if self.config.username:
            await self._run_command(f"git config --global user.name {shlex.quote(self.config.username)}")
        if self.config.useremail:
            await self._run_command(f"git config --global user.email {shlex.quote(self.config.useremail)}")

    async def _ensure_docker_login(self) -> None:
        """Ensure docker is logged in to the registry."""
        if self.config.registry_url and self.config.registry_username and self.config.registry_password:
            # Use echo to pipe password to avoid it showing in command history
            login_cmd = f"echo {shlex.quote(self.config.registry_password)} | docker login {shlex.quote(self.config.registry_url)} -u {shlex.quote(self.config.registry_username)} --password-stdin"
            success, output = await self._run_command(login_cmd)
            if success:
                logger.info("Docker login successful", registry=self.config.registry_url)
            else:
                logger.warning("Docker login failed", registry=self.config.registry_url, error=output)

    async def _ensure_repo_cloned(self, repo_name: str, ssh_url: str) -> tuple[bool, str]:
        """Ensure the repository is cloned and updated on the host.

        If the repo exists, it will be updated with git fetch + reset to match remote.
        If a directory exists but is not a git repo, it will be backed up, cloned,
        and config files (.env, etc.) will be restored.

        Args:
            repo_name: Name of the repository
            ssh_url: SSH URL for cloning

        Returns:
            Tuple of (success, message)
        """
        # Reject anything that could break out of the shell commands below
        try:
            repo_name = _validate_repo_name(repo_name)
            ssh_url = _validate_ssh_url(ssh_url)
        except ValueError as e:
            logger.warning("Rejected unsafe repository parameters", error=str(e))
            return False, str(e)

        # Ensure git is configured before any git operations
        await self._ensure_git_configured()

        repos_path = self.config.repos_path
        repo_path = f"{repos_path}/{repo_name}"
        backup_path = f"{repo_path}.backup.{int(__import__('time').time())}"

        # Every path is quoted before being handed to the remote shell
        q_repos_path = _shell_quote_path(repos_path)
        q_repo_path = _shell_quote_path(repo_path)
        q_git_dir = _shell_quote_path(f"{repo_path}/.git")
        q_backup_path = _shell_quote_path(backup_path)
        q_ssh_url = shlex.quote(ssh_url)

        # Check if directory exists
        check_dir_cmd = f"test -d {q_repo_path} && echo 'dir_exists' || echo 'dir_missing'"
        success, dir_output = await self._run_command(check_dir_cmd)

        if not success:
            return False, f"Failed to check directory existence: {dir_output}"

        # Check if it's a valid git repo
        check_git_cmd = f"test -d {q_git_dir} && echo 'is_git' || echo 'not_git'"
        success, git_output = await self._run_command(check_git_cmd)

        if "dir_missing" in dir_output:
            # Directory doesn't exist - simple clone
            logger.info("Cloning repository", repo=repo_name, path=repo_path)
            # "--" stops git from parsing the URL as an option
            clone_cmd = f"mkdir -p {q_repos_path} && cd {q_repos_path} && git clone -- {q_ssh_url}"
            success, output = await self._run_command(clone_cmd)

            if not success:
                return False, f"Failed to clone repository: {output}"

            return True, "Repository cloned successfully"

        elif "not_git" in git_output:
            # Directory exists but is not a git repo - backup, clone, restore configs
            logger.info("Directory exists but not a git repo, backing up and cloning",
                       repo=repo_name, backup=backup_path)

            # 1. Rename existing directory to backup
            rename_cmd = f"mv {q_repo_path} {q_backup_path}"
            success, output = await self._run_command(rename_cmd)
            if not success:
                return False, f"Failed to backup existing directory: {output}"

            # 2. Clone the repo
            clone_cmd = f"cd {q_repos_path} && git clone -- {q_ssh_url}"
            success, output = await self._run_command(clone_cmd)
            if not success:
                # Restore backup if clone failed
                await self._run_command(f"mv {q_backup_path} {q_repo_path}")
                return False, f"Failed to clone repository: {output}"

            # 3. Copy config files from backup (devops/.env, .env, etc.)
            q_backup_devops_env = _shell_quote_path(f"{backup_path}/devops/.env")
            q_backup_env = _shell_quote_path(f"{backup_path}/.env")
            q_repo_devops = _shell_quote_path(f"{repo_path}/devops")
            q_repo_devops_env = _shell_quote_path(f"{repo_path}/devops/.env")
            q_repo_env = _shell_quote_path(f"{repo_path}/.env")
            restore_cmd = f"""
                if [ -f {q_backup_devops_env} ]; then
                    mkdir -p {q_repo_devops} && cp {q_backup_devops_env} {q_repo_devops_env}
                fi
                if [ -f {q_backup_env} ]; then
                    cp {q_backup_env} {q_repo_env}
                fi
            """
            await self._run_command(restore_cmd)

            # 4. Remove backup
            await self._run_command(f"rm -rf {q_backup_path}")

            return True, "Repository cloned (config files restored from backup)"

        else:
            # Valid git repository - add to safe.directory and force update
            logger.info("Updating repository", repo=repo_name)

            # Add repo to git safe.directory to avoid ownership issues
            safe_dir_cmd = f"git config --global --add safe.directory {q_repo_path}"
            await self._run_command(safe_dir_cmd)

            # Fetch latest and reset to origin (preserves untracked files like .env)
            update_cmd = f"cd {q_repo_path} && git fetch origin && git reset --hard origin/$(git rev-parse --abbrev-ref HEAD)"
            success, output = await self._run_command(update_cmd)

            if not success:
                # Non-fatal, continue with existing code
                logger.warning("Failed to update repo", repo=repo_name, error=output)
                return True, f"Repository exists (update failed: {output})"

            # Get current commit hash
            commit_success, commit_hash = await self._run_command(f"cd {q_repo_path} && git rev-parse --short HEAD")
            commit_id = commit_hash.strip() if commit_success else "unknown"
            return True, f"Repository updated successfully (commit {commit_id})"

    async def _run_command(self, command: str, output_callback=None, cancel_event=None) -> tuple[bool, str]:
        """Run a shell command on the host.

        Prefers SSH if configured (for running on Docker host from container).
        Falls back to host_client or local execution.

        Args:
            command: Shell command to run
            output_callback: Optional callable(str) called for each line of output
            cancel_event: Optional asyncio.Event, if set the command will be cancelled

        Returns:
            Tuple of (success, output)
        """
        try:
            # First, try to use SSH if configured (for executing on host from container)
            ssh_client = await self._get_ssh_client()
            if ssh_client:
                try:
                    if output_callback or cancel_event:
                        return await self._run_ssh_streaming(ssh_client, command, output_callback, cancel_event)
                    return await ssh_client.run_shell_command(command)
                except OSError as e:
                    # Handle DNS/network resolution errors
                    if e.errno == -2 or "Name or service not known" in str(e):
                        error_msg = f"SSH host '{self.config.ssh_host}' cannot be resolved. Check PULSARCD_GITHUB__SSH_HOST configuration."
                        logger.error("SSH host resolution failed", host=self.config.ssh_host, error=str(e))
                        return False, error_msg
                    raise
            
            # Fallback: use the host client if available
            if self.host_client and hasattr(self.host_client, 'run_shell_command'):
                if output_callback or cancel_event:
                    return await self._run_local_streaming(command, output_callback, cancel_event)
                return await self.host_client.run_shell_command(command)
            
            # Last resort: run locally using asyncio with streaming
            return await self._run_local_streaming(command, output_callback, cancel_event)
        except asyncio.CancelledError:
            msg = "Command cancelled"
            if output_callback:
                output_callback(msg)
            return False, msg
        except Exception as e:
            logger.error("Command execution failed", command=command[:80], error=str(e))
            return False, str(e)

    async def _run_local_streaming(self, command: str, output_callback=None, cancel_event=None) -> tuple[bool, str]:
        """Run a command locally with streaming output support."""
        from .config import wrap_command_for_user
        proc = await asyncio.create_subprocess_shell(
            wrap_command_for_user(command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        
        output_lines = []
        try:
            while True:
                # Check cancellation
                if cancel_event and cancel_event.is_set():
                    proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        proc.kill()
                    return False, "\n".join(output_lines) + "\n[Cancelled by user]"
                
                try:
                    line = await asyncio.wait_for(proc.stdout.readline(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                if not line:
                    break
                
                decoded = line.decode('utf-8', errors='replace').rstrip('\n')
                output_lines.append(decoded)
                if output_callback:
                    output_callback(decoded)
            
            await proc.wait()
            return proc.returncode == 0, "\n".join(output_lines).strip()
        except asyncio.CancelledError:
            proc.terminate()
            raise

    async def _run_ssh_streaming(self, ssh_client, command: str, output_callback=None, cancel_event=None) -> tuple[bool, str]:
        """Run a command via SSH with streaming output support."""
        # SSH client doesn't support streaming easily, so run and capture
        # but still check for cancellation periodically
        if cancel_event and cancel_event.is_set():
            return False, "[Cancelled by user]"
        
        # Run the SSH command in a task so we can cancel it
        async def _do_run():
            return await ssh_client.run_shell_command(command)
        
        task = asyncio.create_task(_do_run())
        
        while not task.done():
            if cancel_event and cancel_event.is_set():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                return False, "[Cancelled by user]"
            await asyncio.sleep(0.5)
        
        success, output = task.result()
        if output_callback:
            for line in output.split('\n'):
                output_callback(line)
        return success, output

    async def has_build_config(self, repo_name: str) -> bool:
        """Check if the repo's docker-compose.swarm.yml contains build: directives."""
        try:
            repo_name = _validate_repo_name(repo_name)
        except ValueError as e:
            logger.warning("Rejected unsafe repository name", error=str(e))
            return False

        repos_path = self.config.repos_path
        compose_path = f"{repos_path}/{repo_name}/devops/docker-compose.swarm.yml"
        success, output = await self._run_command(
            f"grep -qE '^\\s+build:' {_shell_quote_path(compose_path)} 2>/dev/null && echo YES || echo NO"
        )
        return success and "YES" in output

    async def build(self, repo_name: str, ssh_url: str, version: str = "1.0",
                   branch: str = None, tag: str = None, commit: str = None,
                   no_cache: bool = False,
                   output_callback=None, cancel_event=None) -> Dict[str, Any]:
        """Build a stack from a repository.

        Args:
            repo_name: Name of the repository
            ssh_url: SSH URL for cloning if needed
            version: Version tag for the build
            branch: Optional branch name to build from
            tag: Optional git tag to build from (takes priority over branch)
            commit: Optional specific commit hash to build from
            output_callback: Optional callable(str) for streaming output
            cancel_event: Optional asyncio.Event for cancellation

        Returns:
            Dict with success status, output, and timing info
        """
        start_time = datetime.utcnow()
        # Tag takes priority over branch for checkout
        checkout_ref = tag or branch
        result = {
            "action": "build",
            "repo": repo_name,
            "version": version,
            "branch": checkout_ref,
            "commit": commit,
            "success": False,
            "output": "",
            "started_at": start_time.isoformat(),
            "completed_at": None,
            "duration_seconds": 0,
        }

        try:
            # Reject unsafe parameters before anything reaches a shell
            _validate_repo_name(repo_name)
            _validate_ssh_url(ssh_url)

            # Ensure docker is logged in to registry
            await self._ensure_docker_login()

            from .recovery import snapshot_envs
            await snapshot_envs(self, repo_name)
            
            # Ensure repo is cloned
            clone_success, clone_msg = await self._ensure_repo_cloned(repo_name, ssh_url)
            if not clone_success:
                result["output"] = clone_msg
                return result

            # Run build script with optional branch and commit
            repos_path = self.config.repos_path
            scripts_path = f"{repos_path}/PulsarCD/scripts"
            repo_path = f"{repos_path}/{repo_name}"

            # Build command with optional branch/tag/commit parameters
            # Pass absolute repo_path to avoid path computation mismatch
            # Script format: build-push.sh <folder> <version> [branch/tag] [commit] [--no-cache]
            # Architectures are declared in the compose file itself — x-platforms
            # per service, x-build-platforms for the whole file — so there is
            # nothing to pass here. An env var on top could only contradict it.
            build_cmd = f"cd {_shell_quote_path(scripts_path)} && bash build-push.sh {_shell_quote_path(repo_path)} {shlex.quote(version)}"
            if checkout_ref:
                build_cmd += f" {shlex.quote(checkout_ref)}"
                if commit:
                    build_cmd += f" {shlex.quote(commit)}"
                else:
                    build_cmd += f" \"\""
            elif commit:
                build_cmd += f" \"\" {shlex.quote(commit)}"
            else:
                build_cmd += f" \"\" \"\""
            if no_cache:
                build_cmd += " --no-cache"

            if output_callback and clone_msg:
                for line in clone_msg.split('\n'):
                    output_callback(line)
            
            # Check cancellation before main build
            if cancel_event and cancel_event.is_set():
                result["output"] = clone_msg + "\n[Cancelled by user]"
                return result

            logger.info("Running build", repo=repo_name, version=version, branch=branch, commit=commit)
            success, output = await self._run_command(build_cmd, output_callback=output_callback, cancel_event=cancel_event)

            result["success"] = success
            result["output"] = f"{clone_msg}\n\n{output}" if clone_msg else output

        except Exception as e:
            result["output"] = str(e)
            logger.error("Build failed", repo=repo_name, error=str(e))

        end_time = datetime.utcnow()
        result["completed_at"] = end_time.isoformat()
        result["duration_seconds"] = (end_time - start_time).total_seconds()

        return result

    async def deploy(self, repo_name: str, ssh_url: str, version: str = "1.0",
                    tag: str = None, qa: bool = False,
                    output_callback=None, cancel_event=None) -> Dict[str, Any]:
        """Deploy a stack from a repository.

        Args:
            repo_name: Name of the repository
            ssh_url: SSH URL for cloning if needed
            version: Version tag for deployment
            tag: Optional specific tag to deploy (e.g., v1.0.5)
            qa: When True, deploy to the isolated QA environment (stack
                prefixed with 'qa-', domains prefixed with 'qa.').
            output_callback: Optional callable(str) for streaming output
            cancel_event: Optional asyncio.Event for cancellation

        Returns:
            Dict with success status, output, and timing info
        """
        start_time = datetime.utcnow()

        # If tag is provided, extract version from it (e.g., v1.0.5 -> 1.0.5)
        deploy_version = tag if tag else version
        # Strip leading 'v' from version if present
        if deploy_version.startswith('v'):
            deploy_version = deploy_version[1:]

        # Ensure we always have a tag for git checkout so the deploy script
        # checks out the correct commit (not the latest on current branch)
        checkout_ref = tag
        if not checkout_ref and deploy_version:
            checkout_ref = f"v{deploy_version}"

        action_label = "qa-deploy" if qa else "deploy"
        result = {
            "action": action_label,
            "repo": repo_name,
            "version": deploy_version,
            "tag": tag,
            "qa": qa,
            "success": False,
            "output": "",
            "started_at": start_time.isoformat(),
            "completed_at": None,
            "duration_seconds": 0,
        }

        try:
            # Reject unsafe parameters before anything reaches a shell
            _validate_repo_name(repo_name)
            _validate_ssh_url(ssh_url)

            from .recovery import snapshot_envs
            await snapshot_envs(self, repo_name)

            # Ensure repo is cloned
            clone_success, clone_msg = await self._ensure_repo_cloned(repo_name, ssh_url)
            if not clone_success:
                result["output"] = clone_msg
                return result

            # Run deploy script
            repos_path = self.config.repos_path
            scripts_path = f"{repos_path}/PulsarCD/scripts"
            repo_path = f"{repos_path}/{repo_name}"

            # Pass absolute repo_path to avoid path computation mismatch
            # Script format: deploy-service.sh [--qa] <folder> <version> [branch/tag]
            qa_flag = "--qa " if qa else ""
            deploy_cmd = f"cd {_shell_quote_path(scripts_path)} && bash deploy-service.sh {qa_flag}{_shell_quote_path(repo_path)} {shlex.quote(deploy_version)}"
            if checkout_ref:
                deploy_cmd += f" {shlex.quote(checkout_ref)}"

            if output_callback and clone_msg:
                for line in clone_msg.split('\n'):
                    output_callback(line)

            # Check cancellation before main deploy
            if cancel_event and cancel_event.is_set():
                result["output"] = clone_msg + "\n[Cancelled by user]"
                return result

            logger.info("Running deploy", repo=repo_name, version=deploy_version, tag=tag, qa=qa)
            success, output = await self._run_command(deploy_cmd, output_callback=output_callback, cancel_event=cancel_event)

            result["success"] = success
            result["output"] = f"{clone_msg}\n\n{output}" if clone_msg else output

        except Exception as e:
            result["output"] = str(e)
            logger.error("Deploy failed", repo=repo_name, error=str(e), qa=qa)

        end_time = datetime.utcnow()
        result["completed_at"] = end_time.isoformat()
        result["duration_seconds"] = (end_time - start_time).total_seconds()

        return result

    async def test(self, repo_name: str, ssh_url: str,
                   branch: str = None, tag: str = None, commit: str = None,
                   output_callback=None, cancel_event=None) -> Dict[str, Any]:
        """Run tests for a stack by executing the 'test' build target from docker-compose.swarm.yml.

        Args:
            repo_name: Name of the repository
            ssh_url: SSH URL for cloning if needed
            branch: Optional branch name to test from
            tag: Optional git tag to test from (takes priority over branch)
            commit: Optional specific commit hash to test from
            output_callback: Optional callable(str) for streaming output
            cancel_event: Optional asyncio.Event for cancellation

        Returns:
            Dict with success status, output, and timing info
        """
        start_time = datetime.utcnow()
        # Tag takes priority over branch for checkout
        checkout_ref = tag or branch
        result = {
            "action": "test",
            "repo": repo_name,
            "branch": checkout_ref,
            "commit": commit,
            "success": False,
            "output": "",
            "started_at": start_time.isoformat(),
            "completed_at": None,
            "duration_seconds": 0,
        }

        try:
            # Reject unsafe parameters before anything reaches a shell
            _validate_repo_name(repo_name)
            _validate_ssh_url(ssh_url)

            # Ensure repo is cloned
            clone_success, clone_msg = await self._ensure_repo_cloned(repo_name, ssh_url)
            if not clone_success:
                result["output"] = clone_msg
                return result

            # Run test script
            repos_path = self.config.repos_path
            scripts_path = f"{repos_path}/PulsarCD/scripts"
            repo_path = f"{repos_path}/{repo_name}"

            # Script format: test.sh <folder> [branch/tag] [commit]
            test_cmd = f"cd {_shell_quote_path(scripts_path)} && bash test.sh {_shell_quote_path(repo_path)}"
            if checkout_ref:
                test_cmd += f" {shlex.quote(checkout_ref)}"
                if commit:
                    test_cmd += f" {shlex.quote(commit)}"
            elif commit:
                test_cmd += f" \"\" {shlex.quote(commit)}"

            if output_callback and clone_msg:
                for line in clone_msg.split('\n'):
                    output_callback(line)

            # Check cancellation before running tests
            if cancel_event and cancel_event.is_set():
                result["output"] = clone_msg + "\n[Cancelled by user]"
                return result

            logger.info("Running tests", repo=repo_name, branch=branch, commit=commit)
            success, output = await self._run_command(test_cmd, output_callback=output_callback, cancel_event=cancel_event)

            result["success"] = success
            result["output"] = f"{clone_msg}\n\n{output}" if clone_msg else output

        except Exception as e:
            result["output"] = str(e)
            logger.error("Test failed", repo=repo_name, error=str(e))

        end_time = datetime.utcnow()
        result["completed_at"] = end_time.isoformat()
        result["duration_seconds"] = (end_time - start_time).total_seconds()

        return result

    async def get_env_file(self, repo_name: str) -> tuple[bool, str]:
        """Get the content of the .env file for a repository.

        Args:
            repo_name: Name of the repository

        Returns:
            Tuple of (success, content_or_error)
        """
        try:
            repo_name = _validate_repo_name(repo_name)
        except ValueError as e:
            logger.warning("Rejected unsafe repository name", error=str(e))
            return False, str(e)

        from .recovery import read_env
        from .backup_vault import BackupError
        try:
            content = await read_env(self, f"{repo_name}/devops/.env")
            return True, "" if content is None else content.decode("utf-8")
        except (BackupError, UnicodeError):
            return False, "Unable to read the environment file"

    async def save_env_file(self, repo_name: str, content: str, actor: str = "operator") -> tuple[bool, str]:
        """Save the content of the .env file for a repository.

        Args:
            repo_name: Name of the repository
            content: The content to write to the .env file

        Returns:
            Tuple of (success, message)
        """
        try:
            repo_name = _validate_repo_name(repo_name)
        except ValueError as e:
            logger.warning("Rejected unsafe repository name", error=str(e))
            return False, str(e)

        if not isinstance(content, str) or len(content.encode("utf-8")) > 4 * 1024 * 1024:
            return False, "Environment content must be text, at most 4 MiB"
        from .recovery import save_env
        from .backup_vault import BackupError
        try:
            revision = await save_env(self, repo_name, content.encode("utf-8"), actor=actor)
            return True, ("File saved and encrypted backup recorded" if revision
                          else "File saved (encrypted backup is disabled)")
        except BackupError as exc:
            return False, str(exc)

    @staticmethod
    def _repo_to_stack_name(repo_name: str) -> str:
        """Convert a repository name to a Docker stack name.

        Mirrors the logic in deploy-service.sh get_stack_name():
        lowercase, replace non-alphanumeric with hyphens, collapse multiples, strip edges.
        """
        import re
        name = repo_name.lower()
        name = re.sub(r'[^a-z0-9]', '-', name)
        name = re.sub(r'-+', '-', name)
        name = name.strip('-')
        return name

    async def get_deployed_stack_tag(self, repo_name: str) -> tuple[bool, Optional[str]]:
        """Get the deployed image tag for a stack from Docker Swarm.

        Filters to only consider images from our registry (skips third-party
        images like redis, postgres, etc.). Falls back to any image tag if
        no registry image is found but services exist (stack is deployed).

        Args:
            repo_name: Name of the repository

        Returns:
            Tuple of (success, tag_or_none)
        """
        stack_name = self._repo_to_stack_name(repo_name)

        # Get ALL service images in the stack
        service_filter = f"name={stack_name}_"
        cmd = f"docker service ls --filter {shlex.quote(service_filter)} --format '{{{{.Image}}}}'"
        success, output = await self._run_command(cmd)

        if not success:
            logger.warning("docker service ls failed", stack=stack_name, repo=repo_name, output=output[:200] if output else "")
            return False, None

        if not output.strip():
            logger.info("No services found for stack", stack=stack_name, repo=repo_name, cmd=cmd)
            return False, None

        registry = self.config.registry_url or ""
        images = [img.strip() for img in output.strip().split('\n') if img.strip()]
        logger.info("Found services for stack", stack=stack_name, repo=repo_name, image_count=len(images), registry=registry, images=images[:5])
        fallback_tag = None

        for image in images:
            # Only consider our images (from our registry)
            if registry and not image.startswith(registry):
                logger.debug("Skipping non-registry image", stack=stack_name, image=image, registry=registry)
                continue
            if ':' in image:
                tag = image.split(':')[-1]
                logger.info("Found deployed tag", stack=stack_name, tag=tag, image=image)
                return True, tag
            return True, "latest"

        # No registry image found, but services exist — stack is deployed
        # Use any available image tag as fallback
        for image in images:
            if ':' in image:
                fallback_tag = image.split(':')[-1]
                break
        logger.warning(
            "No registry image found for stack, using fallback",
            stack=stack_name,
            registry=registry,
            images=images,
            fallback_tag=fallback_tag,
        )
        return True, fallback_tag or "running"

    async def get_all_deployed_stack_tags(self, repo_names: list[str]) -> dict[str, Optional[str]]:
        """Get deployed image tags for all production stacks (see _get_all_deployed_tags_with_qa)."""
        prod, _qa = await self.get_all_deployed_stack_tags_with_qa(repo_names)
        return prod

    async def get_all_deployed_stack_tags_with_qa(
        self, repo_names: list[str]
    ) -> tuple[dict[str, Optional[str]], dict[str, Optional[str]]]:
        """Get deployed image tags for all stacks (production + QA) in a single Docker command.

        QA stacks are deployed with a ``qa-`` prefix (see deploy-service.sh).
        Runs one 'docker service ls' for all services and matches them to repos,
        avoiding N concurrent SSH calls.

        Args:
            repo_names: List of repository names

        Returns:
            Tuple ``(prod_tags, qa_tags)`` where each is ``{repo_name: tag_or_none}``.
        """
        prod_result: dict[str, Optional[str]] = {name: None for name in repo_names}
        qa_result: dict[str, Optional[str]] = {name: None for name in repo_names}

        # Single command to get all service names and images
        cmd = "docker service ls --format '{{.Name}} {{.Image}}'"
        success, output = await self._run_command(cmd)

        if not success or not output.strip():
            logger.warning("docker service ls failed for bulk query", output=output[:200] if output else "")
            return prod_result, qa_result

        registry = self.config.registry_url or ""

        # Build map: stack_name -> list of images
        # We match services to stacks using known stack names from repos
        known_stacks = {self._repo_to_stack_name(name): name for name in repo_names}

        services: list[tuple[str, str]] = []  # (service_name, image)
        for line in output.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            parts = line.split(' ', 1)
            if len(parts) < 2:
                continue
            services.append((parts[0], parts[1]))

        # Match each service to a known stack by prefix (production or QA)
        stacks_images: dict[str, list[str]] = {}
        qa_stacks_images: dict[str, list[str]] = {}
        for service_name, image in services:
            for stack_name in known_stacks:
                if service_name.startswith(stack_name + '_'):
                    stacks_images.setdefault(stack_name, []).append(image)
                    break
                if service_name.startswith('qa-' + stack_name + '_'):
                    qa_stacks_images.setdefault(stack_name, []).append(image)
                    break

        logger.info(
            "Bulk service discovery",
            total_services=len(services),
            matched=sum(len(v) for v in stacks_images.values()),
            qa_matched=sum(len(v) for v in qa_stacks_images.values()),
            stacks=list(stacks_images.keys()),
            qa_stacks=list(qa_stacks_images.keys()),
        )

        def _pick_tag(images: list[str]) -> Optional[str]:
            tag = None
            for image in images:
                if registry and not image.startswith(registry):
                    continue
                if ':' in image:
                    tag = image.split(':')[-1]
                else:
                    tag = "latest"
                break
            if tag is None:
                for image in images:
                    if ':' in image:
                        tag = image.split(':')[-1]
                        break
            return tag or "running"

        # Match repos to stacks
        for repo_name in repo_names:
            stack_name = self._repo_to_stack_name(repo_name)
            images = stacks_images.get(stack_name, [])
            if images:
                prod_result[repo_name] = _pick_tag(images)
            qa_images = qa_stacks_images.get(stack_name, [])
            if qa_images:
                qa_result[repo_name] = _pick_tag(qa_images)

        return prod_result, qa_result
