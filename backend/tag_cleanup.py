"""Automatic cleanup of old git tags and their Docker registry images.

Periodically scans all starred repositories for tags older than a
configurable threshold (default 30 days).  Tags that are currently
deployed or among the N most recent per repo are preserved.

For each tag marked for deletion:
1. The Docker registry image(s) are deleted via the Registry HTTP API v2.
2. The git tag reference is deleted via the GitHub API.

All operations are logged and respect a dry-run mode (enabled by default
in the config so the operator can review before enabling real deletes).
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import aiohttp
import structlog

from .config_file import TagCleanupConfig

logger = structlog.get_logger()


class TagCleaner:
    """Handles discovery and deletion of expired tags and registry images."""

    def __init__(
        self,
        cleanup_config: TagCleanupConfig,
        github_service,
        registry_url: str = "",
        registry_username: str = "",
        registry_password: str = "",
    ):
        self._config = cleanup_config
        self._github = github_service
        self._registry_url = (registry_url or "").rstrip("/")
        self._registry_user = registry_username
        self._registry_pass = registry_password
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if not self._config.enabled:
            logger.info("Tag cleanup disabled")
            return
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Tag cleanup started",
            interval_h=self._config.interval_hours,
            max_age_days=self._config.max_age_days,
            dry_run=self._config.dry_run,
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        await asyncio.sleep(60)  # initial delay to let other services boot
        while True:
            try:
                await self.run_cleanup()
            except Exception as e:
                logger.error("Tag cleanup cycle failed", error=str(e))
            await asyncio.sleep(self._config.interval_hours * 3600)

    # ------------------------------------------------------------------
    # Public entry point (also used by the manual API endpoint)
    # ------------------------------------------------------------------

    async def run_cleanup(self) -> Dict[str, Any]:
        """Run a full cleanup cycle across all repos.

        Returns a summary dict suitable for JSON serialisation.
        """
        repos = await self._github.get_starred_repos()
        if not repos:
            logger.info("Tag cleanup: no repos found")
            return {"repos_checked": 0, "deleted": []}

        # Fetch currently deployed tags for all repos
        deployed_map = await self._get_deployed_tags(repos)

        summary: List[Dict[str, Any]] = []

        for repo in repos:
            owner, name = repo["owner"], repo["name"]
            try:
                result = await self._cleanup_repo(owner, name, deployed_map.get(name))
                if result:
                    summary.append(result)
            except Exception as e:
                logger.error("Tag cleanup failed for repo", repo=name, error=str(e))

        logger.info("Tag cleanup cycle complete",
                     repos=len(repos), deleted=sum(len(s.get("deleted", [])) for s in summary))
        return {"repos_checked": len(repos), "results": summary}

    async def run_cleanup_repo(self, owner: str, repo: str) -> Dict[str, Any]:
        """Run cleanup for a single repo. Public entry point for per-repo cleanup."""
        deployed_map = await self._get_deployed_tags([{"owner": owner, "name": repo}])
        result = await self._cleanup_repo(owner, repo, deployed_map.get(repo))
        return result or {"repo": repo, "total_tags": 0, "protected": [], "deleted": []}

    # ------------------------------------------------------------------
    # Per-repo logic
    # ------------------------------------------------------------------

    async def _cleanup_repo(
        self, owner: str, repo: str, deployed_tag: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Identify and delete expired tags for a single repo."""
        tags_data = await self._github.get_repo_tags(owner, repo, limit=100)
        tags = tags_data.get("tags", [])
        if not tags:
            return None

        cutoff = datetime.now(timezone.utc) - timedelta(days=self._config.max_age_days)
        protected: Set[str] = set()

        # Protect currently deployed tag (and its major.minor family)
        if deployed_tag:
            protected.add(deployed_tag)
            # If deployed tag is "1.0.5", also protect "v1.0.5"
            if not deployed_tag.startswith("v"):
                protected.add(f"v{deployed_tag}")
            else:
                protected.add(deployed_tag.lstrip("v"))

        # Protect the N most recent tags regardless of age
        for tag in tags[: self._config.keep_latest_n]:
            protected.add(tag["name"])

        to_delete: List[Dict[str, Any]] = []

        for tag in tags:
            name = tag["name"]
            if name in protected:
                continue

            created_at = tag.get("created_at")
            if not created_at:
                continue

            try:
                tag_date = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            if tag_date < cutoff:
                to_delete.append(tag)

        if not to_delete:
            return None

        deleted_names: List[str] = []
        for tag in to_delete:
            tag_name = tag["name"]
            version = tag_name.lstrip("v")

            if self._config.dry_run:
                logger.info("Tag cleanup [DRY-RUN] would delete",
                            repo=repo, tag=tag_name, created_at=tag.get("created_at"))
                deleted_names.append(f"{tag_name} (dry-run)")
                continue

            # 1) Delete Docker registry images for this tag
            await self._delete_registry_images(repo, version, tag.get("sha", ""))

            # 2) Delete git tag via GitHub API
            await self._delete_git_tag(owner, repo, tag_name)
            deleted_names.append(tag_name)

        return {
            "repo": repo,
            "total_tags": len(tags),
            "protected": list(protected),
            "deleted": deleted_names,
        }

    # ------------------------------------------------------------------
    # Deployed tag discovery
    # ------------------------------------------------------------------

    async def _get_deployed_tags(self, repos: List[Dict]) -> Dict[str, Optional[str]]:
        """Get currently deployed tags for all repos via the GitHub service."""
        try:
            return await self._github.get_all_deployed_stack_tags(
                [r["name"] for r in repos]
            )
        except Exception as e:
            logger.warning("Could not fetch deployed tags, proceeding without protection",
                           error=str(e))
            return {}

    # ------------------------------------------------------------------
    # Git tag deletion (GitHub API)
    # ------------------------------------------------------------------

    async def _delete_git_tag(self, owner: str, repo: str, tag_name: str) -> bool:
        """Delete a git tag reference via the GitHub API."""
        session = await self._github._get_session()
        url = f"https://api.github.com/repos/{owner}/{repo}/git/refs/tags/{tag_name}"
        try:
            async with session.delete(url) as resp:
                if resp.status == 204:
                    logger.info("Deleted git tag", repo=repo, tag=tag_name)
                    return True
                body = await resp.text()
                logger.warning("Failed to delete git tag",
                               repo=repo, tag=tag_name, status=resp.status, body=body[:200])
                return False
        except Exception as e:
            logger.error("Exception deleting git tag", repo=repo, tag=tag_name, error=str(e))
            return False

    # ------------------------------------------------------------------
    # Docker Registry V2 image deletion
    # ------------------------------------------------------------------

    async def _delete_registry_images(self, repo: str, version: str, commit_sha: str) -> None:
        """Delete Docker image tags from a V2-compatible registry.

        Each build pushes up to 3 tags per image:
          - {major.minor.patch}  (the full version)
          - {major.minor}        (the minor alias — NOT deleted, as it's shared)
          - {commit-sha-short}   (7-char prefix)

        We delete the full-version tag and the commit-sha tag.
        """
        if not self._registry_url:
            return

        tags_to_delete = [version]
        if commit_sha:
            tags_to_delete.append(commit_sha[:7])

        # The repo name in the registry follows the pattern from build-push.sh:
        #   registry.methodinfo.fr/<project>/<service>:<tag>
        # We need to discover actual repository names in the registry that
        # belong to this project.  Use the _catalog endpoint.
        catalog = await self._registry_list_repos()
        if catalog is None:
            return

        # Match registry repos that start with the project stack name
        from .github_service import GitHubService
        stack_name = GitHubService._repo_to_stack_name(repo)
        matching_repos = [r for r in catalog if r.startswith(f"{stack_name}/") or r == stack_name]

        for reg_repo in matching_repos:
            for tag in tags_to_delete:
                await self._delete_registry_tag(reg_repo, tag)

    async def _registry_list_repos(self) -> Optional[List[str]]:
        """List repositories in the Docker registry via GET /v2/_catalog."""
        url = f"https://{self._registry_url}/v2/_catalog"
        auth = None
        if self._registry_user and self._registry_pass:
            auth = aiohttp.BasicAuth(self._registry_user, self._registry_pass)
        try:
            async with aiohttp.ClientSession(auth=auth) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        logger.warning("Registry catalog request failed", status=resp.status)
                        return None
                    data = await resp.json()
                    return data.get("repositories", [])
        except Exception as e:
            logger.error("Failed to list registry repos", error=str(e))
            return None

    async def _delete_registry_tag(self, repo_name: str, tag: str) -> bool:
        """Delete a single image tag from the Docker registry.

        The V2 API requires two steps:
        1. GET the manifest digest for the tag
        2. DELETE the manifest by digest
        """
        base = f"https://{self._registry_url}/v2/{repo_name}"
        auth = None
        if self._registry_user and self._registry_pass:
            auth = aiohttp.BasicAuth(self._registry_user, self._registry_pass)

        try:
            async with aiohttp.ClientSession(auth=auth) as session:
                # Step 1: Get manifest digest
                headers = {"Accept": "application/vnd.docker.distribution.manifest.v2+json"}
                async with session.get(
                    f"{base}/manifests/{tag}",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 404:
                        return False  # tag doesn't exist in registry
                    if resp.status != 200:
                        logger.warning("Registry manifest GET failed",
                                       repo=repo_name, tag=tag, status=resp.status)
                        return False
                    digest = resp.headers.get("Docker-Content-Digest")
                    if not digest:
                        logger.warning("No digest in manifest response",
                                       repo=repo_name, tag=tag)
                        return False

                # Step 2: Delete by digest
                async with session.delete(
                    f"{base}/manifests/{digest}",
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 202:
                        logger.info("Deleted registry image", repo=repo_name, tag=tag, digest=digest[:20])
                        return True
                    body = await resp.text()
                    logger.warning("Registry manifest DELETE failed",
                                   repo=repo_name, tag=tag, status=resp.status, body=body[:200])
                    return False
        except Exception as e:
            logger.error("Exception deleting registry image",
                         repo=repo_name, tag=tag, error=str(e))
            return False
