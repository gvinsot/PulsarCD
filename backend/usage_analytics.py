"""Traffic statistics over the access records (Traefik requests).

The Security view reads the same records looking for attacks; this one
answers the other question asked of them: which endpoints are actually used,
how much, how fast, and by how many clients -- for the whole platform or for
one stack.

Everything here is pure: LogsQL queries are built and their rows interpreted
without touching VictoriaLogs, so the shape of both can be tested directly.

A request carries the Traefik router that served it ("pulsarcd@swarm"), not
the stack it belongs to.  The stack is resolved from the Swarm service labels
(see DockerAPIClient.get_traefik_routers) and passed in as a router name ->
stack mapping, so nothing here needs to reach Docker.
"""

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from shared.access_log import EVENT_REQUEST
from shared.victorialogs import KIND_ACCESS, exact, integer, iso_ms, num, quote, stream

# Both views read the same records over the same windows, and their
# selectors have to offer the same choices, so the window rules are shared.
from .security_analytics import (  # noqa: F401
    CLIENT_ERRORS, DEFAULT_WINDOW_MINUTES, OK, SERVER_ERRORS, clamp_window, timeline,
    timeline_interval,
)

TOP_HOSTS = 25
TOP_ENDPOINTS = 50
TOP_METHODS = 8
TOP_STATUS_CODES = 12
# One row per Traefik router. A few per deployed stack, so this is a
# ceiling rather than a ranking: the by-stack totals must add up.
TOP_ROUTERS = 500
# Latency percentiles reported for the window as a whole. Per endpoint the
# slowest request is reported instead.
PERCENTILES = [50, 95, 99]
# Methods listed per endpoint.
ENDPOINT_METHODS = 4

# Row key standing for "no router matched": Traefik answered the request
# itself, usually a 404 for a host it does not serve.  A router name never
# contains a "@", let alone is empty, so this collides with none.
NO_ROUTER = ""

_TRAFFIC = (
    "count() as requests, count_uniq(client_ip) as distinct_ips, "
    f"count() if ({CLIENT_ERRORS}) as client_errors, count() if ({SERVER_ERRORS}) as server_errors, "
    "avg(duration_ms) as avg_duration, sum(size) as bytes"
)


def router_filter(router_names: Iterable[str]) -> str:
    """Filter matching the requests served by any of these Traefik routers.

    The records hold a router's qualified name ("pulsarcd@swarm") while the
    Swarm labels give the bare one, and the provider suffix depends on how
    Traefik discovered the service.  Matching on the prefix keeps the filter
    independent of it.  No router name means no match, which is what a stack
    that publishes nothing should return.
    """
    names = list(router_names)
    if not names:
        return "router:in()"
    return "(" + " OR ".join(f"router:={quote(name + '@')}*" for name in names) + ")"


def build_usage_queries(minutes: int, include_internal: bool,
                        router_names: Optional[Iterable[str]] = None) -> Dict[str, str]:
    """Totals, timeline and the endpoint, host and router rankings.

    router_names restricts the window to one stack; None means every request.
    """
    # WAF blocks share the records but never reached an application: they
    # are the Security view's subject, not usage.
    parts = [stream(KIND_ACCESS), exact("event", EVENT_REQUEST)]
    if not include_internal:
        parts.append("internal:=false")
    if router_names is not None:
        parts.append(router_filter(router_names))
    base = " ".join(parts)
    quantiles = ", ".join(f"quantile({p / 100:g}, duration_ms) if (duration_ms:*) as p{p}"
                          for p in PERCENTILES)
    return {
        "totals": (f"{base} | stats count() as requests, count_uniq(client_ip) as unique_ips, "
                   "count_uniq(path) as distinct_paths, count_uniq(request_host) as distinct_hosts, "
                   f"count() if ({CLIENT_ERRORS}) as client_errors, "
                   "count() if (status:range[500, 600)) as server_errors, "
                   f"sum(size) as bytes, avg(duration_ms) as avg_duration, {quantiles}"),
        "timeline": (f"{base} | stats by (_time:{timeline_interval(minutes)}) "
                     f"count() if ({OK}) as ok, count() if ({CLIENT_ERRORS}) as client_errors, "
                     "count() if (status:range[500, 600)) as server_errors, "
                     "avg(duration_ms) as avg_duration_ms"),
        "hosts": (f"{base} | stats by (request_host) {_TRAFFIC}, count_uniq(path) as distinct_paths "
                  f"| sort by (requests desc) limit {TOP_HOSTS}"),
        # An exact ranking of (host, path) pairs: the slowest request answers
        # "is anything here slow?" where a per-endpoint percentile would not
        # be worth its cost. An endpoint is served by one router, which names
        # its stack.
        "endpoints": (f"{base} | stats by (request_host, path) {_TRAFFIC}, "
                      "max(duration_ms) if (duration_ms:*) as max_duration, "
                      f"uniq_values(method) limit {ENDPOINT_METHODS} as methods, "
                      "any(router) as router, max(_time) as last_seen "
                      f"| sort by (requests desc) limit {TOP_ENDPOINTS}"),
        "routers": f"{base} | stats by (router) {_TRAFFIC} | sort by (requests desc) limit {TOP_ROUTERS}",
        "methods": f"{base} method:* | stats by (method) count() as n | sort by (n desc) limit {TOP_METHODS}",
        "status_codes": (f"{base} status:* | stats by (status) count() as n "
                         f"| sort by (n desc) limit {TOP_STATUS_CODES}"),
    }


# --- Interpretation ---------------------------------------------------------

def router_base(router: Optional[str]) -> str:
    """The router name without the provider Traefik discovered it through."""
    return (router or NO_ROUTER).split("@", 1)[0]


def _ms(value: Any) -> Optional[float]:
    """A duration, rounded, or None when the rows hold none."""
    result = num(value)
    return round(result, 1) if result is not None else None


def _share(part: int, total: int) -> float:
    return round(100 * part / total, 2) if total else 0.0


def _traffic(row: Dict[str, Any], total_requests: int) -> Dict[str, Any]:
    """The _TRAFFIC statistics of one ranking row."""
    requests = integer(row.get("requests"))
    return {
        "requests": requests,
        "share": _share(requests, total_requests),
        "distinct_ips": integer(row.get("distinct_ips")),
        "client_errors": integer(row.get("client_errors")),
        "server_errors": integer(row.get("server_errors")),
        "avg_duration_ms": _ms(row.get("avg_duration")),
        "bytes": integer(row.get("bytes")),
    }


def _methods(value: Any) -> List[str]:
    try:
        methods = json.loads(value) if value else []
    except (TypeError, ValueError):
        return []
    return [m for m in methods if isinstance(m, str)]


def _endpoint_entry(row: Dict[str, Any], total_requests: int,
                    router_stacks: Dict[str, str]) -> Dict[str, Any]:
    router = router_base(row.get("router"))
    return {
        "host": row.get("request_host"),
        "path": row.get("path"),
        "router": router or None,
        "stack": router_stacks.get(router),
        "methods": _methods(row.get("methods")),
        "max_duration_ms": _ms(row.get("max_duration")),
        "last_seen": row.get("last_seen"),
        **_traffic(row, total_requests),
    }


def _by_stack(rows: List[Dict[str, Any]], total_requests: int,
              router_stacks: Dict[str, str]) -> List[Dict[str, Any]]:
    """Router rows folded into the stacks that publish them.

    Requests no router matched, and routers no deployed stack claims (the
    edge's own file-provider routers, a stack removed since), share a row with
    no stack name rather than being dropped: the rows have to add up to the
    total shown above them.
    """
    stacks: Dict[Optional[str], Dict[str, Any]] = {}
    for router_row in rows:
        name = router_base(router_row.get("router"))
        stack = router_stacks.get(name)
        entry = stacks.get(stack)
        row = _traffic(router_row, total_requests)
        if entry is None:
            stacks[stack] = {"stack": stack, "routers": [], **row}
            entry = stacks[stack]
        else:
            entry["requests"] += row["requests"]
            entry["client_errors"] += row["client_errors"]
            entry["server_errors"] += row["server_errors"]
            entry["bytes"] += row["bytes"]
            entry["share"] = _share(entry["requests"], total_requests)
            # Distinct clients are counted per router; summing them would
            # count a client once per router it reached. The sum is an upper
            # bound, and the column says so.
            entry["distinct_ips"] += row["distinct_ips"]
            entry["avg_duration_ms"] = _weighted_average(entry, row)
        if name:
            entry["routers"].append(name)

    out = list(stacks.values())
    out.sort(key=lambda s: s["requests"], reverse=True)
    return out


def _weighted_average(entry: Dict[str, Any], row: Dict[str, Any]) -> Optional[float]:
    """Average latency of two merged rows, weighted by their request counts."""
    left, right = entry.get("avg_duration_ms"), row.get("avg_duration_ms")
    if left is None:
        return right
    if right is None:
        return left
    # entry["requests"] already includes row's, so the left weight is the
    # difference.
    left_requests = entry["requests"] - row["requests"]
    total = left_requests + row["requests"]
    if not total:
        return None
    return round((left * left_requests + right * row["requests"]) / total, 1)


def empty_overview(minutes: int, include_internal: bool, stack: Optional[str],
                   stacks: List[str], generated_at: datetime,
                   error: Optional[str] = None) -> Dict[str, Any]:
    result = {
        "window_minutes": minutes,
        "include_internal": include_internal,
        "stack": stack,
        "stacks": stacks,
        "generated_at": iso_ms(generated_at),
        "timeline_interval": timeline_interval(minutes),
        "totals": {
            "requests": 0, "unique_ips": 0, "endpoints": 0, "hosts": 0,
            "client_errors": 0, "server_errors": 0, "bytes": 0,
            "requests_per_minute": 0.0, "error_rate": 0.0,
            "avg_duration_ms": None, "p50": None, "p95": None, "p99": None,
        },
        "timeline": [],
        "endpoints": [],
        "hosts": [],
        "by_stack": [],
        "methods": [],
        "status_codes": [],
    }
    if error:
        result["error"] = error
    return result


def summarize(results: Dict[str, List[Dict[str, Any]]], *, minutes: int, include_internal: bool,
              stack: Optional[str], stacks: List[str], router_stacks: Dict[str, str],
              start: datetime, end: datetime, generated_at: datetime) -> Dict[str, Any]:
    """The usage view's payload, from the rows of build_usage_queries."""
    result = empty_overview(minutes, include_internal, stack, stacks, generated_at)
    totals = (results.get("totals") or [{}])[0]
    requests = integer(totals.get("requests"))

    result["timeline"] = timeline(results.get("timeline", []), start, end, timeline_interval(minutes),
                                  ("ok", "client_errors", "server_errors"), ("avg_duration_ms",))
    result["endpoints"] = [_endpoint_entry(row, requests, router_stacks)
                           for row in results.get("endpoints", [])]
    result["hosts"] = [
        {"host": row.get("request_host"), "distinct_paths": integer(row.get("distinct_paths")),
         **_traffic(row, requests)}
        for row in results.get("hosts", [])
    ]
    result["by_stack"] = _by_stack(results.get("routers", []), requests, router_stacks)
    result["methods"] = [{"key": row.get("method"), "count": integer(row.get("n"))}
                         for row in results.get("methods", [])]
    result["status_codes"] = [{"status": integer(row.get("status")), "count": integer(row.get("n"))}
                              for row in results.get("status_codes", [])]

    client_errors = integer(totals.get("client_errors"))
    server_errors = integer(totals.get("server_errors"))
    result["totals"] = {
        "requests": requests,
        "unique_ips": integer(totals.get("unique_ips")),
        "endpoints": integer(totals.get("distinct_paths")),
        "hosts": integer(totals.get("distinct_hosts")),
        "client_errors": client_errors,
        "server_errors": server_errors,
        "bytes": integer(totals.get("bytes")),
        "requests_per_minute": round(requests / minutes, 2) if minutes else 0.0,
        "error_rate": _share(client_errors + server_errors, requests),
        "avg_duration_ms": _ms(totals.get("avg_duration")),
        **{f"p{p}": _ms(totals.get(f"p{p}")) for p in PERCENTILES},
    }
    return result
