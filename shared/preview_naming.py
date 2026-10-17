"""Naming rules for per-branch preview environments.

A preview is an isolated copy of a repository's stack, built from one branch
and deployed next to production and QA:

    branch "feature/login"  +  repo stack "pulsarcd"
        -> slug   "feature-login-df7c7a"
        -> stack  "pv-feature-login-df7c7a-pulsarcd"
        -> host   "feature-login-df7c7a.<TRAEFIK_HOST>"
        -> image  "<registry>/<image>:pv-feature-login-df7c7a"

This module is the single source of truth for those names. The backend imports
it, and scripts/deploy-service.sh runs it as a script (stdlib only, no PulsarCD
imports) so the shell and the API can never disagree on a stack name:

    python3 shared/preview_naming.py <branch> <repo-stack-name>
    -> prints "<slug>\\t<stack>\\t<image-tag>" or exits 2 with the reason on stderr

Safety properties enforced here:

* The slug is a valid DNS label (``[a-z0-9]`` at both ends, ``[a-z0-9-]``
  inside, at most MAX_SLUG_LEN characters), so ``<slug>.<domain>`` is a host
  a wildcard record and certificate can cover.
* Sanitizing is collision-safe: whenever the branch name cannot be used
  verbatim (upper case, ``/``, ``_``, truncation...), a short hash of the
  original branch is appended, so ``feature/x`` and ``feature-x`` never share
  a stack.
* Reserved slugs (``qa``, ``www``...) are always hashed, so a preview can never
  claim ``qa.<domain>`` or another well-known host.
* Every preview stack starts with ``pv-`` and differs from the production
  stack (``<repo>``) and the QA stack (``qa-<repo>``); see
  :func:`assert_preview_stack`.
"""

from __future__ import annotations

import hashlib
import re
import sys
from typing import NamedTuple

PREVIEW_STACK_PREFIX = "pv-"
QA_STACK_PREFIX = "qa-"

# Slug length: short enough to stay readable in a hostname, long enough to keep
# a meaningful part of the branch next to the 6-character hash.
MAX_SLUG_LEN = 24
MIN_SLUG_LEN = 8
HASH_LEN = 6

# Docker limits a service name ("<stack>_<service>") to 63 characters. Keeping
# the stack at 48 leaves 14 characters for the service name.
MAX_STACK_LEN = 48

# Slugs that would map a preview onto a host that already means something
# (qa.<domain> is the QA stack, www.<domain> usually production...). They are
# never used verbatim.
RESERVED_SLUGS = frozenset({
    "qa", "www", "prod", "production", "staging", "preview", "pv",
    "api", "admin", "mail", "smtp", "ftp", "ns1", "ns2",
})

# Branch names accepted for a preview: what git allows in practice, minus
# anything a shell, a ref parser or a command line could misread.
_BRANCH_RE = re.compile(r"\A[A-Za-z0-9._/+-]{1,200}\Z")
_SLUG_RE = re.compile(r"\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\Z")
_STACK_RE = re.compile(r"\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\Z")
# Shape of the suffix a hashed slug ends with. A branch already ending like
# this is hashed too, otherwise branch "feature-x-217d2b" would reuse the slug
# of "feature/x" ("feature-x-217d2b") verbatim.
_HASHED_SUFFIX_RE = re.compile(r"-[0-9a-f]{%d}\Z" % HASH_LEN)


class PreviewNames(NamedTuple):
    """Every name derived from one (branch, repository) pair."""

    branch: str
    slug: str
    stack: str
    image_tag: str
    host_prefix: str


def validate_branch(branch: str) -> str:
    """Return ``branch`` unchanged if it is safe to build and deploy.

    Raises:
        ValueError: empty, too long, outside ``[A-Za-z0-9._/+-]``, starting
            with ``-`` or ``/``, ending with ``/`` or ``.lock``, or containing
            ``..`` or ``//``.
    """
    if not isinstance(branch, str) or not branch:
        raise ValueError("Branch name is empty")
    if not _BRANCH_RE.match(branch):
        raise ValueError(
            f"Invalid branch name {branch!r}: only letters, digits, '.', '_', '/', "
            "'+' and '-' are allowed (1-200 characters)"
        )
    if branch.startswith(("-", "/", ".")) or branch.endswith(("/", ".", ".lock")):
        raise ValueError(f"Invalid branch name {branch!r}")
    if ".." in branch or "//" in branch:
        raise ValueError(f"Invalid branch name {branch!r}: must not contain '..' or '//'")
    return branch


def _short_hash(branch: str) -> str:
    return hashlib.sha256(branch.encode("utf-8")).hexdigest()[:HASH_LEN]


def branch_slug(branch: str, max_len: int = MAX_SLUG_LEN) -> str:
    """DNS-safe, collision-safe slug for ``branch``.

    The slug is the lower-cased branch with every run of other characters
    turned into ``-``. When that is not exactly the branch name (or is too
    long, empty, reserved, or itself ends like a hashed slug), ``-<hash>`` of
    the original name is appended after truncation. Verbatim slugs therefore
    never end in ``-<6 hex>`` and hashed ones always do, so two different
    branches never share a slug (short of a hash collision).
    """
    validate_branch(branch)
    if max_len < MIN_SLUG_LEN:
        raise ValueError(f"Slug length budget too small: {max_len} < {MIN_SLUG_LEN}")
    base = re.sub(r"[^a-z0-9]+", "-", branch.lower()).strip("-")
    if (base == branch and len(base) <= max_len and base not in RESERVED_SLUGS
            and not _HASHED_SUFFIX_RE.search(base)):
        return base
    head = base[: max_len - HASH_LEN - 1].strip("-")
    digest = _short_hash(branch)
    # A hash-only slug could be all digits; keep a letter in front so the
    # label never looks like part of an IP address.
    return f"{head}-{digest}" if head else f"b-{digest}"


def repo_stack_name(repo_name: str) -> str:
    """Production stack name of a repository (mirrors get_stack_name in deploy-service.sh)."""
    name = re.sub(r"[^a-z0-9]", "-", repo_name.lower())
    return re.sub(r"-+", "-", name).strip("-")


def preview_names(branch: str, base_stack: str) -> PreviewNames:
    """Compute slug, stack, image tag and host prefix for a preview.

    Args:
        branch: Git branch to preview.
        base_stack: Production stack name of the repository (see
            :func:`repo_stack_name`).

    Raises:
        ValueError: invalid branch or stack name, or a repository name so long
            that no meaningful slug fits in MAX_STACK_LEN.
    """
    if not base_stack or not _STACK_RE.match(base_stack):
        raise ValueError(f"Invalid repository stack name {base_stack!r}")
    budget = MAX_STACK_LEN - len(PREVIEW_STACK_PREFIX) - 1 - len(base_stack)
    if budget < MIN_SLUG_LEN:
        raise ValueError(
            f"Repository stack name {base_stack!r} is too long for a preview "
            f"(stack names are limited to {MAX_STACK_LEN} characters)"
        )
    slug = branch_slug(branch, min(MAX_SLUG_LEN, budget))
    stack = f"{PREVIEW_STACK_PREFIX}{slug}-{base_stack}"
    names = PreviewNames(
        branch=branch,
        slug=slug,
        stack=stack,
        image_tag=f"{PREVIEW_STACK_PREFIX}{slug}",
        host_prefix=f"{slug}.",
    )
    assert_preview_stack(stack, base_stack)
    return names


def is_preview_stack(stack: str) -> bool:
    """True when ``stack`` is shaped like a preview stack name."""
    return (
        isinstance(stack, str)
        and stack.startswith(PREVIEW_STACK_PREFIX)
        and len(stack) <= MAX_STACK_LEN
        and bool(_STACK_RE.match(stack))
    )


def assert_preview_stack(stack: str, base_stack: str | None = None) -> str:
    """Refuse any stack name a preview operation must never touch.

    A preview deploy or teardown passes through here before reaching Docker,
    so a bug or a crafted request can never redeploy or remove the production
    stack (``<repo>``) or the QA stack (``qa-<repo>``).

    Raises:
        ValueError: the name is not a preview stack, or it is the production
            or QA stack of ``base_stack``.
    """
    if not is_preview_stack(stack):
        raise ValueError(f"Refusing non-preview stack {stack!r}: preview stacks start with "
                         f"'{PREVIEW_STACK_PREFIX}'")
    if stack.startswith(QA_STACK_PREFIX):
        raise ValueError(f"Refusing QA stack {stack!r}")
    if base_stack is not None:
        if stack in (base_stack, f"{QA_STACK_PREFIX}{base_stack}"):
            raise ValueError(f"Refusing production/QA stack {stack!r}")
        if not stack.endswith(f"-{base_stack}"):
            raise ValueError(f"Stack {stack!r} does not belong to {base_stack!r}")
    return stack


def _main(argv: list[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write("Usage: preview_naming.py <branch> <repo-stack-name>\n")
        return 2
    try:
        names = preview_names(argv[1], argv[2])
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    sys.stdout.write(f"{names.slug}\t{names.stack}\t{names.image_tag}\n")
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
