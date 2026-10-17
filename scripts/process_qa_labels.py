#!/usr/bin/env python3
"""Prefix Traefik label names in a docker-compose file for QA-mode deployments.

When the same Docker Swarm cluster runs both a prod and a QA copy of a stack,
the Traefik labels declared in their compose files clash: routers, services
and locally-defined middlewares share the same names, so Traefik picks one
and the other becomes unreachable.

This helper rewrites Traefik labels so that every local Traefik identifier
(router / service / middleware defined in the same compose file) is prefixed
with the QA stack prefix. Provider-qualified references like ``noop@internal``
or ``global@file`` are left untouched.

Top-level ``networks:`` entries also get a ``name:`` field prefixed with
``qa-`` so that QA and prod deployments do not collide on explicitly-named
networks, and so a QA stack's external references resolve to QA-prefixed
sibling networks. Well-known shared networks (Traefik proxy, etc.) are
preserved via a denylist that can be extended through the ``QA_NETWORK_KEEP``
environment variable (comma-separated network names).

It also rewrites literal hostnames inside Traefik ``rule=`` labels. The apex
domain (longest dot-suffix shared by every literal hostname found in any
rule) is replaced with ``qa.<apex>``, so:

    Host(`www.pulsarteam.io`) || Host(`pulsarteam.io`)
        -> Host(`www.qa.pulsarteam.io`) || Host(`qa.pulsarteam.io`)

The apex can be overridden by setting ``QA_HOST_APEX`` in the environment
(value should be the already-QA-prefixed apex, e.g. ``qa.example.com``).
``HostRegexp`` is left untouched because its argument is a regex, not a
literal host.

Usage:
    process_qa_labels.py <input_compose> <output_compose> <prefix> [domain_prefix]

``domain_prefix`` defaults to ``qa.``. Preview deployments pass their own
(``<slug>.``, with ``pv-<slug>-`` as ``prefix``), so the same rewrite isolates a
per-branch preview from both production and QA.

Example with prefix="qa-":
    "traefik.http.routers.foo.rule=..."         -> "traefik.http.routers.qa-foo.rule=..."
    "traefik.http.routers.foo.service=foo"      -> "traefik.http.routers.qa-foo.service=qa-foo"
    "traefik.http.routers.foo.service=x@file"   -> unchanged
"""
from __future__ import annotations

import os
import re
import sys

try:
    import yaml  # type: ignore
except ImportError:
    sys.stderr.write(
        "[qa-labels] PyYAML not available - skipping Traefik label rewrite.\n"
        "          Install with: pip install pyyaml\n"
    )
    if len(sys.argv) in (4, 5) and sys.argv[1] != sys.argv[2]:
        try:
            with open(sys.argv[1], "rb") as src, open(sys.argv[2], "wb") as dst:
                dst.write(src.read())
        except OSError as e:
            sys.stderr.write(f"[qa-labels] Failed to copy compose file: {e}\n")
            sys.exit(1)
    sys.exit(0)


# Matches "traefik.<proto>.<kind>.<name>.<rest>" where <name> is the user-defined
# identifier we want to rewrite. Only http/tcp/udp routers/services/middlewares
# are rewritten; other namespaces (e.g. tls.options, providers) are left alone.
_LABEL_KEY_RE = re.compile(
    r"^(traefik\.(?:http|tcp|udp)\.(?:routers|services|middlewares)\.)"
    r"([A-Za-z0-9][A-Za-z0-9_-]*)"
    r"(\..+)$"
)

# A single Host()/HostSNI()/HostHeader() matcher with a backtick-quoted literal arg.
# HostRegexp is intentionally excluded - its argument is a regex, not a hostname.
_HOST_MATCHER_RE = re.compile(
    r"(HostSNI|HostHeader|Host)\(\s*`([^`]+)`\s*\)"
)


def _normalise_labels(labels):
    """Return labels as a list of (key, value_or_None) pairs and a flag telling
    whether the original form was a dict or a list of strings."""
    if isinstance(labels, dict):
        pairs = []
        for k, v in labels.items():
            key = str(k)
            value = None if v is None else str(v)
            pairs.append((key, value))
        return pairs, True
    if isinstance(labels, list):
        pairs = []
        for entry in labels:
            s = str(entry)
            if "=" in s:
                k, _, v = s.partition("=")
                pairs.append((k.strip(), v))
            else:
                pairs.append((s.strip(), None))
        return pairs, False
    return None, False


def _collect_local_names(pairs) -> set:
    names = set()
    for key, _ in pairs:
        m = _LABEL_KEY_RE.match(key)
        if m:
            names.add(m.group(2))
    return names


def _rewrite_reference_list(values: str, local_names: set, prefix: str) -> str:
    """Rewrite a comma-separated list of references (e.g. middlewares=...).

    Items containing '@' are provider-qualified (e.g. 'global@file') and are
    left untouched. Items matching a locally defined name get the prefix.
    Unknown items are left as-is (likely external references).
    """
    parts = [p.strip() for p in values.split(",")]
    out = []
    for p in parts:
        if not p or "@" in p:
            out.append(p)
        elif p in local_names:
            out.append(prefix + p)
        else:
            out.append(p)
    return ",".join(out)


DEFAULT_NETWORK_KEEP = frozenset({
    "proxy",
    "traefik-public",
    "traefik_proxy",
    "postgresqlcluster_internal",
})


def _qa_network_keep_set():
    """Names of networks whose `name:` field must NOT be QA-prefixed.
    Combines the built-in defaults with anything listed in QA_NETWORK_KEEP."""
    keep = set(DEFAULT_NETWORK_KEEP)
    extra = os.environ.get("QA_NETWORK_KEEP", "").strip()
    if extra:
        keep.update(s.strip() for s in extra.split(",") if s.strip())
    return keep


def _network_effective_name(key, net):
    """Return the network name Docker would use, or None when Swarm should
    auto-derive it (non-external, no explicit `name:`).

    Handles both the modern (``external: true`` + ``name: <x>``) and the
    legacy (``external: { name: <x> }``) forms.
    """
    if not isinstance(net, dict):
        return None
    external = net.get("external")
    if isinstance(external, dict) and "name" in external:
        return str(external["name"])
    if net.get("name") is not None:
        return str(net["name"])
    if external is True:
        return str(key)
    return None


def _set_network_name(net, new_name):
    """Write back the new effective name, preserving the original form."""
    external = net.get("external")
    if isinstance(external, dict) and "name" in external:
        external["name"] = new_name
    else:
        net["name"] = new_name


def _process_networks(data, prefix: str, keep_set):
    networks = data.get("networks")
    if not isinstance(networks, dict):
        return
    for key, net in networks.items():
        if not isinstance(net, dict):
            continue
        effective = _network_effective_name(key, net)
        if effective is None:
            continue  # Swarm auto-prefixes via the (already QA-prefixed) stack name
        if effective in keep_set:
            continue
        if effective.startswith(prefix):
            continue
        new_name = prefix + effective
        sys.stderr.write(f"[qa-labels] Network: {effective} -> {new_name}\n")
        _set_network_name(net, new_name)


def _common_dot_suffix(hosts):
    """Return the apex shared by every host: the shortest hostname that
    every other hostname equals or ends with ('.' + apex). None if no
    such apex exists (hosts have unrelated domains).
    """
    if not hosts:
        return None
    candidate = min(hosts, key=len)
    for h in hosts:
        if h == candidate:
            continue
        if not h.endswith("." + candidate):
            return None
    return candidate


def _is_literal_host(host: str) -> bool:
    """A hostname is literal if it does not embed a shell-style variable
    reference. The deploy script prefixes such variables in the environment
    before docker stack deploy substitutes them, so we must not touch them
    here (otherwise the prefix would be applied twice).
    """
    return "$" not in host


def _rewrite_host(host: str, apex_original: str, apex_qa: str) -> str:
    """Map a literal hostname so the apex part swaps from `apex_original` to
    `apex_qa`. Hostnames that don't share the apex (or aren't literal) are
    returned unchanged.
    """
    if not _is_literal_host(host):
        return host
    if host == apex_original:
        return apex_qa
    suffix = "." + apex_original
    if host.endswith(suffix):
        return host[: -len(apex_original)] + apex_qa
    return host


def _rewrite_rule_value(rule_value: str, apex_original: str, apex_qa: str) -> str:
    def repl(m):
        matcher, host = m.group(1), m.group(2)
        return f"{matcher}(`{_rewrite_host(host, apex_original, apex_qa)}`)"
    return _HOST_MATCHER_RE.sub(repl, rule_value)


def _rewrite_pairs(pairs, prefix: str, apex_original, apex_qa):
    local_names = _collect_local_names(pairs)

    rewritten = []
    for key, value in pairs:
        new_key = key
        m = _LABEL_KEY_RE.match(key)
        if m and m.group(2) in local_names:
            new_key = f"{m.group(1)}{prefix}{m.group(2)}{m.group(3)}"

        new_value = value
        if value is not None:
            if key.endswith(".service"):
                v = value.strip()
                if v and "@" not in v and v in local_names:
                    new_value = prefix + v
            elif key.endswith(".middlewares"):
                new_value = _rewrite_reference_list(value, local_names, prefix)
            elif key.endswith(".rule") and apex_original and apex_qa:
                new_value = _rewrite_rule_value(value, apex_original, apex_qa)

        rewritten.append((new_key, new_value))
    return rewritten


def _emit_labels(pairs, was_dict):
    if was_dict:
        return {k: ("" if v is None else v) for k, v in pairs}
    return [k if v is None else f"{k}={v}" for k, v in pairs]


def _iter_label_pairs(data):
    """Yield every (pairs, was_dict, setter) for both 'labels' and
    'deploy.labels' across all services. The setter, called with the rewritten
    labels collection, stores them back on the containing service/deploy dict.
    """
    if not isinstance(data, dict) or not isinstance(data.get("services"), dict):
        return
    for svc in data["services"].values():
        if not isinstance(svc, dict):
            continue

        labels = svc.get("labels")
        pairs, was_dict = _normalise_labels(labels)
        if pairs is not None:
            def _set_svc(new, _svc=svc, _was_dict=was_dict):
                _svc["labels"] = _emit_labels(new, _was_dict)
            yield pairs, was_dict, _set_svc

        deploy = svc.get("deploy")
        if isinstance(deploy, dict):
            dlabels = deploy.get("labels")
            pairs, was_dict = _normalise_labels(dlabels)
            if pairs is not None:
                def _set_deploy(new, _dep=deploy, _was_dict=was_dict):
                    _dep["labels"] = _emit_labels(new, _was_dict)
                yield pairs, was_dict, _set_deploy


def _detect_apex(data):
    """Scan every rule= label in the compose and return the apex domain
    common to all literal hostnames found, or None if none / no consensus.
    """
    hostnames = []
    for pairs, _, _ in _iter_label_pairs(data):
        for key, value in pairs:
            if value is None or not key.endswith(".rule"):
                continue
            if not _LABEL_KEY_RE.match(key):
                continue
            hostnames.extend(
                m.group(2)
                for m in _HOST_MATCHER_RE.finditer(value)
                if _is_literal_host(m.group(2))
            )
    if not hostnames:
        return None
    return _common_dot_suffix(hostnames)


def process(compose_path: str, output_path: str, prefix: str,
            domain_prefix: str = "qa.") -> None:
    with open(compose_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Apex resolution: explicit override wins; otherwise auto-detect across rules.
    override = os.environ.get("QA_HOST_APEX", "").strip() or None
    if override:
        apex_qa = override
        apex_original = (override[len(domain_prefix):]
                         if override.startswith(domain_prefix) else override)
    else:
        apex_original = _detect_apex(data)
        apex_qa = None
        if apex_original:
            apex_qa = (
                apex_original
                if apex_original.startswith(domain_prefix)
                else f"{domain_prefix}{apex_original}"
            )

    if apex_original and apex_qa:
        sys.stderr.write(
            f"[qa-labels] Rewriting hostnames: {apex_original} -> {apex_qa}\n"
        )

    for pairs, _, setter in _iter_label_pairs(data):
        new_pairs = _rewrite_pairs(pairs, prefix, apex_original, apex_qa)
        setter(new_pairs)

    _process_networks(data, prefix, _qa_network_keep_set())

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        sys.stderr.write(
            "Usage: process_qa_labels.py <input_compose> <output_compose> <prefix>"
            " [domain_prefix]\n"
        )
        sys.exit(2)
    _domain_prefix = sys.argv[4] if len(sys.argv) == 5 else "qa."
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.", _domain_prefix):
        sys.stderr.write(f"Invalid domain prefix: {_domain_prefix!r}\n")
        sys.exit(2)
    process(sys.argv[1], sys.argv[2], sys.argv[3], _domain_prefix)
