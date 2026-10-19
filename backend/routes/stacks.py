"""Stack endpoints backed by GitHub and the deployer.

Starred repositories, branches, tags, activity and diffs, stack .env files,
deployed tags and build detection under /api/stacks/, plus
GET /api/github/check-access.

App state is read from backend.api at call time, as ``api.<name>`` (see
backend/routes/__init__.py).
"""

import asyncio
from typing import Dict

import structlog
from fastapi import APIRouter, HTTPException, Query, Request

from ..github_service import StackDeployer
from .. import api

logger = structlog.get_logger()

router = APIRouter()


@router.get("/api/github/check-access")
async def check_github_access():
    """Diagnostic: test GitHub API access for all starred repos."""
    if not api.github_service or not api.github_service.config.token:
        return {"error": "GitHub token not configured"}

    repos = await api.github_service.get_starred_repos()
    if not repos:
        return {"error": "No starred repos found (or token has no access)"}

    results = []
    session = await api.github_service._get_session()

    async def check_repo(repo):
        owner = repo["owner"]
        name = repo["name"]
        url = f"https://api.github.com/repos/{owner}/{name}/commits?per_page=1"
        try:
            async with session.get(url) as response:
                status = response.status
                if status == 200:
                    return {"repo": f"{owner}/{name}", "status": "ok", "code": 200}
                else:
                    error_text = await response.text()
                    return {"repo": f"{owner}/{name}", "status": "error", "code": status, "message": error_text[:200]}
        except Exception as e:
            return {"repo": f"{owner}/{name}", "status": "error", "message": str(e)}

    tasks = [check_repo(r) for r in repos]
    results = await asyncio.gather(*tasks)

    ok_count = sum(1 for r in results if r["status"] == "ok")
    error_count = len(results) - ok_count

    return {
        "total": len(results),
        "ok": ok_count,
        "errors": error_count,
        "repos": results
    }


# ============== Stacks (GitHub Integration) ==============

@router.get("/api/stacks/status")
async def get_stacks_status():
    """Get GitHub integration status."""
    return {
        "configured": api.github_service.is_configured(),
        "username": api.settings.github.username,
    }


@router.get("/api/stacks/test-permissions/{owner}/{repo}")
async def test_github_permissions(owner: str, repo: str):
    """Test GitHub token permissions for a specific repository.

    Returns detailed diagnostics about what the token can and cannot access.
    """
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    session = await api.github_service._get_session()
    results = {}

    # Test 1: Can we access the repo at all?
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}") as resp:
            results["repo_access"] = {"status": resp.status, "ok": resp.status == 200}
            if resp.status == 200:
                data = await resp.json()
                results["repo_access"]["default_branch"] = data.get("default_branch")
                results["repo_access"]["private"] = data.get("private")
            else:
                results["repo_access"]["error"] = (await resp.text())[:200]
            scopes = resp.headers.get("X-OAuth-Scopes", "none")
            results["token_scopes"] = scopes
            results["rate_limit_remaining"] = resp.headers.get("X-RateLimit-Remaining")
    except Exception as e:
        results["repo_access"] = {"status": 0, "ok": False, "error": str(e)}

    # Test 2: Can we list branches? (requires Contents: Read)
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}/branches", params={"per_page": 1}) as resp:
            results["branches_access"] = {"status": resp.status, "ok": resp.status == 200}
            if resp.status == 200:
                data = await resp.json()
                results["branches_access"]["count"] = len(data)
            else:
                results["branches_access"]["error"] = (await resp.text())[:200]
    except Exception as e:
        results["branches_access"] = {"status": 0, "ok": False, "error": str(e)}

    # Test 3: Can we list commits? (requires Contents: Read)
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}/commits", params={"per_page": 1}) as resp:
            results["commits_access"] = {"status": resp.status, "ok": resp.status == 200}
            if resp.status == 200:
                data = await resp.json()
                results["commits_access"]["count"] = len(data)
            else:
                results["commits_access"]["error"] = (await resp.text())[:200]
    except Exception as e:
        results["commits_access"] = {"status": 0, "ok": False, "error": str(e)}

    # Summary
    all_ok = all(r.get("ok") for r in results.values() if isinstance(r, dict) and "ok" in r)
    if all_ok:
        results["summary"] = "All permissions OK - token can access repo, branches, and commits."
    else:
        issues = []
        if not results.get("repo_access", {}).get("ok"):
            issues.append("Cannot access repository (check token has access to this repo)")
        if not results.get("branches_access", {}).get("ok"):
            issues.append("Cannot list branches (need 'Contents: Read' permission)")
        if not results.get("commits_access", {}).get("ok"):
            issues.append("Cannot list commits (need 'Contents: Read' permission)")
        results["summary"] = "Permission issues found: " + "; ".join(issues)

    return results


@router.get("/api/stacks/repos")
async def get_starred_repos():
    """Get list of starred GitHub repositories."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    repos = await api.github_service.get_starred_repos()
    return {"repos": repos, "count": len(repos)}


@router.get("/api/stacks/{owner}/{repo}/branches")
async def get_repo_branches(owner: str, repo: str):
    """Get list of branches for a repository."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    branches = await api.github_service.get_repo_branches(owner, repo)
    return {"branches": branches, "count": len(branches)}


@router.get("/api/stacks/{owner}/{repo}/tags")
async def get_repo_tags(
    owner: str,
    repo: str,
    limit: int = Query(default=10, ge=1, le=100, description="Maximum number of tags to return"),
):
    """Get list of tags for a repository."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")
    
    tags_data = await api.github_service.get_repo_tags(owner, repo, limit)
    return tags_data


@router.get("/api/stacks/{owner}/{repo}/untagged-commits")
async def get_untagged_commits(
    owner: str,
    repo: str,
    limit: int = Query(default=10, ge=1, le=50, description="Maximum number of commits to check"),
    branch: str = Query(default=None, description="Branch name (defaults to repo default branch)"),
):
    """Get recent commits that don't have a tag (not yet built/deployed)."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    return await api.github_service.get_untagged_commits(owner, repo, limit, branch=branch)


@router.get("/api/stacks/{owner}/{repo}/activity")
async def get_repo_activity(
    owner: str,
    repo: str,
    per_page: int = Query(default=30, ge=1, le=100, description="Commits per branch"),
):
    """Get activity data (commits from all branches, tags) for a repository."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    # Quick access check: verify we can reach the repo at all
    # This catches token issues early and gives clear error messages
    session = await api.github_service._get_session()
    try:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}") as check_resp:
            if check_resp.status == 401:
                return {
                    "branches": [], "tags": [], "commits": [], "branch_tip_map": {},
                    "tag_map": {}, "commit_branches": {}, "default_branch": "main",
                    "error": "GitHub token is invalid or expired. Please update your GitHub token in settings.",
                }
            if check_resp.status == 404:
                return {
                    "branches": [], "tags": [], "commits": [], "branch_tip_map": {},
                    "tag_map": {}, "commit_branches": {}, "default_branch": "main",
                    "error": f"Repository '{owner}/{repo}' not found. The token may not have access to this repository.",
                }
            if check_resp.status == 403:
                error_text = await check_resp.text()
                return {
                    "branches": [], "tags": [], "commits": [], "branch_tip_map": {},
                    "tag_map": {}, "commit_branches": {}, "default_branch": "main",
                    "error": f"Access denied to '{owner}/{repo}'. GitHub says: {error_text[:200]}",
                }
            # Get the default branch from the repo metadata
            repo_data = await check_resp.json()
            repo_default_branch = repo_data.get("default_branch", "main")
    except Exception as e:
        logger.error("Failed to check repo access", repo=f"{owner}/{repo}", error=str(e))
        repo_default_branch = "main"

    # Fetch branches and tags first
    branches, tags_data = await asyncio.gather(
        api.github_service.get_repo_branches(owner, repo),
        api.github_service.get_repo_tags(owner, repo, limit=50),
    )

    branches_were_empty = not branches

    # Fetch commits from all branches in parallel
    # If branches list is empty (permissions issue, rate limit), fall back to default branch
    if branches:
        commit_tasks = [
            api.github_service.get_repo_commits(owner, repo, branch=b["name"], per_page=per_page)
            for b in branches
        ]
    else:
        # Fallback: fetch commits without specifying a branch (uses repo default)
        logger.warning("No branches available, falling back to default branch commits", repo=f"{owner}/{repo}")
        commit_tasks = [
            api.github_service.get_repo_commits(owner, repo, per_page=per_page)
        ]
        # Create a synthetic branch entry for the fallback
        branches = [{"name": tags_data.get("default_branch", repo_default_branch), "sha": "", "protected": False}]

    all_results = await asyncio.gather(*commit_tasks)

    # Collect any permission errors from the results
    errors = [r.get("error") for r in all_results if r.get("error")]

    # Deduplicate commits by SHA across all branches, track which branches contain each commit
    seen = {}
    commit_branches = {}  # sha -> list of branch names that contain this commit
    for branch, result in zip(branches, all_results):
        for c in result["commits"]:
            if c["sha"] not in seen:
                seen[c["sha"]] = c
            commit_branches.setdefault(c["sha"], []).append(branch["name"])

    # Sort by date descending
    commits = sorted(seen.values(), key=lambda c: c["date"], reverse=True)

    # Build maps: SHA -> branch/tag names
    branch_tip_map = {}
    for b in branches:
        branch_tip_map.setdefault(b["sha"], []).append(b["name"])

    tag_map = {}
    for t in tags_data.get("tags", []):
        tag_map.setdefault(t["sha"], []).append(t["name"])

    response = {
        "branches": branches,
        "tags": tags_data.get("tags", []),
        "commits": commits,
        "branch_tip_map": branch_tip_map,
        "tag_map": tag_map,
        "commit_branches": commit_branches,
        "default_branch": tags_data.get("default_branch", repo_default_branch),
    }
    if errors:
        response["error"] = errors[0]  # Surface first permission error to frontend
    elif not commits and branches_were_empty:
        # Both branches and commits failed - likely a permission issue
        response["error"] = (
            "Could not fetch repository data. Your GitHub token likely lacks the 'Contents: Read' permission. "
            "Go to GitHub Settings > Developer settings > Personal access tokens > Fine-grained tokens, "
            "edit your token and enable 'Contents: Read' under Repository permissions. "
            "Note: 'Administration: Read' alone is not sufficient to access branches and commits."
        )
    elif not commits:
        # Branches were found but no commits — something unexpected
        response["error"] = (
            f"No commits found on any branch ({len(branches)} branches checked). "
            "This may indicate a permission issue — ensure your GitHub token has 'Contents: Read' permission."
        )
    return response


@router.get("/api/stacks/{owner}/{repo}/commits/{sha}/diff")
async def get_commit_diff(owner: str, repo: str, sha: str):
    """Get the diff for a specific commit."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    return await api.github_service.get_commit_diff(owner, repo, sha)


# env=prod is devops/.env; env=qa is devops/.env.qa, whose variables override
# devops/.env for the QA deployment only.
_ENV_QUERY = Query("prod", pattern="^(prod|qa)$")


@router.get("/api/stacks/{repo_name}/env")
async def get_stack_env(repo_name: str, env: str = _ENV_QUERY):
    """Get the .env (or .env.qa) file content for a stack."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    deployer = StackDeployer(api.settings.github, None)
    success, content = await deployer.get_env_file(repo_name, env=env)

    if not success:
        raise HTTPException(status_code=500, detail=content)

    return {"content": content, "repo": repo_name, "env": env}


@router.put("/api/stacks/{repo_name}/env")
async def save_stack_env(repo_name: str, request: Request, env: str = _ENV_QUERY):
    """Save the .env (or .env.qa) file content for a stack."""
    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    body = await request.json()
    content = body.get("content", "")

    deployer = StackDeployer(api.settings.github, None)
    success, message = await deployer.save_env_file(
        repo_name, content, actor=getattr(request.state, "user", "operator"), env=env)

    if not success:
        raise HTTPException(status_code=500, detail=message)

    return {"success": True, "message": message, "repo": repo_name, "env": env}


_deployed_tags_cache = None
_deployed_tags_cache_time = None
_DEPLOYED_TAGS_CACHE_TTL = 10  # seconds


def invalidate_deployed_tags_cache():
    """Force the next read to reflect changes to Swarm services."""
    global _deployed_tags_cache, _deployed_tags_cache_time
    _deployed_tags_cache = None
    _deployed_tags_cache_time = None


@router.get("/api/stacks/deployed-tags")
async def get_stacks_deployed_tags():
    """Get deployed image tags and latest built tags for all stacks."""
    global _deployed_tags_cache, _deployed_tags_cache_time

    if not api.github_service.is_configured():
        raise HTTPException(status_code=400, detail="GitHub integration not configured")

    # Return cached result if fresh enough
    now = asyncio.get_event_loop().time()
    if _deployed_tags_cache and _deployed_tags_cache_time and (now - _deployed_tags_cache_time) < _DEPLOYED_TAGS_CACHE_TTL:
        # A copy: the MCP tool calls this directly and must not reach the cache.
        return {key: dict(tags) for key, tags in _deployed_tags_cache.items()}

    repos = await api.github_service.get_starred_repos()
    deployer = StackDeployer(api.settings.github, None)

    # Fetch all deployed tags in a single SSH command (avoids concurrent SSH issues).
    # Returns both production and QA stack tags ("qa-<stack>_*" services).
    all_deployed, all_deployed_qa = await deployer.get_all_deployed_stack_tags_with_qa(
        [repo["name"] for repo in repos]
    )

    # Fetch latest built tags from GitHub in parallel
    async def get_latest(repo):
        return repo["name"], await api.github_service.get_latest_tag(repo["owner"], repo["name"])

    latest_results = await asyncio.gather(*[get_latest(r) for r in repos])

    deployed_tags = {name: tag for name, tag in all_deployed.items() if tag}
    deployed_qa_tags = {name: tag for name, tag in all_deployed_qa.items() if tag}
    latest_built = {name: tag for name, tag in latest_results if tag}

    result = {"tags": deployed_tags, "qa_tags": deployed_qa_tags, "latest_built": latest_built}
    _deployed_tags_cache = result
    _deployed_tags_cache_time = now
    return {key: dict(tags) for key, tags in result.items()}


@router.get("/api/stacks/buildable")
async def get_stacks_buildable(refresh: bool = Query(default=False)) -> Dict[str, bool]:
    """Return which stacks have build: directives in their docker-compose.swarm.yml."""
    if not api.github_service.is_configured():
        return {}
    # Copies: the cache is shared with the pipeline chain, so the caller gets
    # its own dict (the values are bools, a shallow copy is enough).
    if api._buildable_cache and not refresh:
        return dict(api._buildable_cache)
    api._buildable_cache.clear()
    repos = await api.github_service.get_starred_repos()
    deployer, _ = api._get_deployer_and_host()
    for repo in repos:
        name = repo["name"]
        ssh_url = repo.get("ssh_url", "")
        try:
            # Update local clone so newly added build: directives are detected
            if ssh_url:
                await deployer._ensure_repo_cloned(name, ssh_url)
            api._buildable_cache[name] = await deployer.has_build_config(name)
        except Exception as e:
            logger.warning("Failed to check build config, assuming buildable",
                           repo=name, error=str(e))
            api._buildable_cache[name] = True
    return dict(api._buildable_cache)
