"""Avoid expected manifest 404s when a build introduces a new registry tag.

Exit 0: listed, 3: definitely absent, 2: inconclusive. Only exit 3 lets the
caller omit its existing manifest check. No credentials or HTTP errors are
printed; unsupported authentication falls back to Docker's own client.
"""

import base64
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.parse import quote, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class _NoRedirects(HTTPRedirectHandler):
    # Never forward a Docker credential to another origin through a redirect.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _basic_auth(registry):
    config_dir = Path(os.environ.get("DOCKER_CONFIG") or Path.home() / ".docker")
    config_file = config_dir / "config.json"
    if not config_file.exists():
        return None
    config = json.loads(config_file.read_text(encoding="utf-8"))
    helper = config.get("credHelpers", {}).get(registry) or config.get("credsStore")
    if helper:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", helper):
            return None
        result = subprocess.run(
            ["docker-credential-" + helper, "get"], input=registry + "\n",
            text=True, capture_output=True, timeout=5, check=True,
        )
        credentials = json.loads(result.stdout)
        username, password = credentials["Username"], credentials["Secret"]
        # Identity tokens need Docker's bearer authentication implementation.
        if username == "<token>":
            return None
        raw = (username + ":" + password).encode("utf-8")
    else:
        auths = config.get("auths", {})
        entry = (auths.get(registry) or auths.get("https://" + registry)
                 or auths.get("https://" + registry + "/v1/") or {})
        if entry.get("identitytoken") or not entry.get("auth"):
            return None
        raw = base64.b64decode(entry["auth"], validate=True)
        if b":" not in raw:
            return None
    return "Basic " + base64.b64encode(raw).decode("ascii")


def tag_exists(registry_url, repository, tag):
    """Return True/False only for a valid, complete registry response.

    HTTP failures, unsupported bearer authentication, malformed responses and
    incomplete/unsafe pagination return None. An existing tag still requires
    the caller's manifest check: the tag listing can contain stale entries.
    """
    try:
        base = urlsplit(registry_url)
        if (base.scheme not in ("https", "http") or not base.netloc
                or base.username or base.password or base.query or base.fragment
                or base.path not in ("", "/")):
            return None
        endpoint = "/v2/" + quote(repository, safe="/") + "/tags/list"
        url = urlunsplit((base.scheme, base.netloc, endpoint, "n=1000", ""))
        opener = build_opener(_NoRedirects())
        authorization = None
        visited = set()
        deadline = time.monotonic() + 15
        for _ in range(100):  # A broken registry cannot paginate forever.
            if url in visited or time.monotonic() >= deadline:
                return None
            visited.add(url)
            headers = {"Accept": "application/json"}
            if authorization:
                headers["Authorization"] = authorization
            try:
                response = opener.open(Request(url, headers=headers), timeout=5)
            except HTTPError as error:
                challenge = error.headers.get("WWW-Authenticate", "")
                error.close()
                if error.code != 401 or authorization or not challenge.lower().startswith("basic "):
                    return None
                # Retrieve credentials only after this origin requests Basic.
                authorization = _basic_auth(base.netloc)
                if not authorization:
                    return None
                headers["Authorization"] = authorization
                response = opener.open(Request(url, headers=headers), timeout=5)
            with response:
                if response.status != 200:
                    return None
                payload = response.read(4 * 1024 * 1024 + 1)
                if len(payload) > 4 * 1024 * 1024:
                    return None
                data = json.loads(payload)
                if data.get("name") != repository or "tags" not in data:
                    return None
                tags = data["tags"]
                if tags is None:
                    tags = []
                if not isinstance(tags, list) or any(not isinstance(value, str) for value in tags):
                    return None
                if tag in tags:
                    return True
                links = response.headers.get_all("Link", [])
            if not links:
                return False
            # Distribution's pagination contract: one Link with rel="next".
            # Unknown/multiple relations are inconclusive, never "absent".
            if len(links) != 1:
                return None
            match = re.fullmatch(r'\s*<([^<>]+)>\s*;\s*rel\s*=\s*"?next"?\s*', links[0])
            if not match:
                return None
            next_url = urlsplit(urljoin(url, match[1]))
            if (next_url.scheme != base.scheme or next_url.netloc != base.netloc
                    or next_url.path != endpoint or next_url.fragment):
                return None
            url = next_url.geturl()
        return None
    except HTTPError as error:
        error.close()
        return None
    except (OSError, HTTPException, ValueError, TypeError, KeyError, AttributeError, subprocess.SubprocessError):
        return None


def main():
    if len(sys.argv) != 2:
        return 2
    registry, separator, reference = sys.argv[1].partition("/")
    repository, colon, tag = reference.rpartition(":")
    if not separator or not colon or not repository or not tag or "@" in reference:
        return 2
    result = tag_exists("https://" + registry, repository, tag)
    # A Python crash exits 1, so absence needs a distinct, intentional status.
    return 2 if result is None else (0 if result else 3)


if __name__ == "__main__":
    sys.exit(main())
