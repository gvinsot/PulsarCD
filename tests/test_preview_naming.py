"""Slug and stack naming for per-branch previews (shared/preview_naming.py)."""

import random
import re
import string
import subprocess
import sys
from pathlib import Path

import pytest

from shared.preview_naming import (
    MAX_SLUG_LEN,
    MAX_STACK_LEN,
    RESERVED_SLUGS,
    assert_preview_stack,
    branch_slug,
    is_preview_stack,
    preview_names,
    repo_stack_name,
    validate_branch,
)

NAMING_SCRIPT = Path(__file__).resolve().parents[1] / "shared" / "preview_naming.py"
DNS_LABEL = re.compile(r"\A[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")


class TestBranchSlug:
    def test_simple_branch_is_used_verbatim(self):
        assert branch_slug("feature-login") == "feature-login"
        assert branch_slug("main") == "main"
        assert branch_slug("v2") == "v2"

    def test_sanitized_branch_gets_a_hash(self):
        slug = branch_slug("feature/login")
        assert re.fullmatch(r"feature-login-[0-9a-f]{6}", slug)

    def test_sanitizing_is_collision_safe(self):
        branches = ["feature/x", "feature-x", "feature_x", "Feature-X", "feature.x", "feature+x"]
        slugs = [branch_slug(b) for b in branches]
        assert len(set(slugs)) == len(slugs), slugs

    def test_upper_case_is_hashed_not_merged(self):
        assert branch_slug("Fix") != branch_slug("fix")

    def test_long_branch_is_truncated_with_hash(self):
        a = branch_slug("feature/" + "a" * 80)
        b = branch_slug("feature/" + "a" * 81)
        assert len(a) <= MAX_SLUG_LEN and len(b) <= MAX_SLUG_LEN
        assert a != b

    def test_long_plain_branch_is_hashed(self):
        branch = "a" * (MAX_SLUG_LEN + 1)
        slug = branch_slug(branch)
        assert len(slug) <= MAX_SLUG_LEN
        assert slug != branch[:MAX_SLUG_LEN]

    @pytest.mark.parametrize("reserved", sorted(RESERVED_SLUGS))
    def test_reserved_names_never_used_verbatim(self, reserved):
        slug = branch_slug(reserved)
        assert slug != reserved
        assert slug.startswith(reserved + "-")

    def test_qa_branch_can_never_claim_the_qa_host(self):
        names = preview_names("qa", "shop")
        assert names.host_prefix != "qa."
        assert names.stack != "qa-shop"

    def test_symbol_only_branch_gets_a_hash_slug(self):
        slug = branch_slug("+++")
        assert re.fullmatch(r"b-[0-9a-f]{6}", slug)

    def test_slugs_are_dns_labels(self):
        rng = random.Random(1234)
        alphabet = string.ascii_letters + string.digits + "._/+-"
        seen = {}
        for _ in range(2000):
            branch = "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 120)))
            try:
                validate_branch(branch)
            except ValueError:
                continue
            slug = branch_slug(branch)
            assert DNS_LABEL.match(slug), (branch, slug)
            assert len(slug) <= MAX_SLUG_LEN
            assert slug not in RESERVED_SLUGS
            assert seen.setdefault(slug, branch) == branch, (slug, branch, seen[slug])

    def test_deterministic(self):
        assert branch_slug("feature/login") == branch_slug("feature/login")


class TestBranchValidation:
    @pytest.mark.parametrize("branch", [
        "", "-rf", "/abs", "a..b", "a//b", "trailing/", "x.lock", ".hidden",
        "has space", "semi;colon", "$(id)", "back`tick", "new\nline", "a" * 201,
        "quote'", "tilde~1", "caret^", "colon:x", "at@{1}",
    ])
    def test_unsafe_branches_are_refused(self, branch):
        with pytest.raises(ValueError):
            validate_branch(branch)

    @pytest.mark.parametrize("branch", ["main", "feature/login", "release/1.2", "dep+bump", "a_b"])
    def test_ordinary_branches_are_accepted(self, branch):
        assert validate_branch(branch) == branch


class TestPreviewNames:
    def test_names(self):
        names = preview_names("feature-login", "shop")
        assert names.slug == "feature-login"
        assert names.stack == "pv-feature-login-shop"
        assert names.image_tag == "pv-feature-login"
        assert names.host_prefix == "feature-login."

    def test_repo_stack_name_matches_deploy_script(self):
        assert repo_stack_name("Art Retrainer") == "art-retrainer"
        assert repo_stack_name("My__Repo.v2") == "my-repo-v2"

    def test_stack_length_is_bounded(self):
        base = "a" * 30
        names = preview_names("feature/" + "x" * 100, base)
        assert len(names.stack) <= MAX_STACK_LEN
        assert names.stack.endswith("-" + base)

    def test_too_long_repo_is_refused(self):
        with pytest.raises(ValueError):
            preview_names("main", "r" * (MAX_STACK_LEN - 5))

    def test_invalid_repo_stack_is_refused(self):
        for base in ("", "Bad", "-x", "x;y"):
            with pytest.raises(ValueError):
                preview_names("main", base)

    def test_different_branches_never_share_a_stack(self):
        stacks = {preview_names(b, "shop").stack for b in
                  ("feature/a", "feature-a", "feature_a", "Feature-A")}
        assert len(stacks) == 4


class TestStackSafety:
    def test_prod_and_qa_stacks_are_refused(self):
        for stack in ("shop", "qa-shop"):
            with pytest.raises(ValueError):
                assert_preview_stack(stack, "shop")

    def test_non_preview_names_are_refused(self):
        for stack in ("shop", "qa-pv-x-shop", "privatenetwork", "", "pv-", "PV-x-shop",
                      "pv-x-shop;rm", "pv-" + "x" * 60):
            with pytest.raises(ValueError):
                assert_preview_stack(stack)
            assert not is_preview_stack(stack)

    def test_preview_of_another_repo_is_refused(self):
        with pytest.raises(ValueError):
            assert_preview_stack("pv-main-other", "shop")

    def test_preview_stack_is_accepted(self):
        assert assert_preview_stack("pv-main-shop", "shop") == "pv-main-shop"


class TestCommandLine:
    """deploy-service.sh calls the module as a script: same names as the API."""

    def run(self, *args):
        return subprocess.run([sys.executable, str(NAMING_SCRIPT), *args],
                              capture_output=True, text=True, timeout=30)

    def test_prints_slug_stack_and_tag(self):
        result = self.run("feature/login", "shop")
        assert result.returncode == 0, result.stderr
        names = preview_names("feature/login", "shop")
        assert result.stdout == f"{names.slug}\t{names.stack}\t{names.image_tag}\n"

    def test_invalid_branch_exits_2(self):
        result = self.run("$(id)", "shop")
        assert result.returncode == 2
        assert result.stdout == ""

    def test_usage(self):
        assert self.run("only-one").returncode == 2
