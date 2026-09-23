"""Traffic statistics over the access index (Traefik requests).

The Security view reads the same index looking for attacks; this one answers
the other question asked of it: which endpoints are actually used, how much,
how fast, and by how many clients -- for the whole platform or for one stack.

Everything here is pure: query bodies are built and responses interpreted
without touching OpenSearch, so the shape of both can be tested directly.

A request carries the Traefik router that served it ("pulsarcd@swarm"), not
the stack it belongs to.  The stack is resolved from the Swarm service labels
(see DockerAPIClient.get_traefik_routers) and passed in as a router name ->
stack mapping, so nothing here needs to reach Docker.
"""

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from shared.access_log import EVENT_REQUEST

# Both views read the same index over the same windows, and their selectors
# have to offer the same choices, so the window rules are shared.
from .security_analytics import DEFAULT_WINDOW_MINUTES, clamp_window, timeline_interval  # noqa: F401

# Endpoints are collected as the busiest paths of the busiest hosts, then
# flattened and ranked together. A `multi_terms` over (host, path) would rank
# them exactly, but it builds a composite key for every document and trips the
# request circuit breaker of a modest OpenSearch node on a multi-day window;
# two nested `terms` cost a fraction of that. The host ceiling is what makes it
# approximate: a path served by a host outside the busiest TOP_HOSTS is not
# listed, which on an edge serving a few dozen domains never happens.
TOP_HOSTS = 25
TOP_PATHS_PER_HOST = 15
TOP_ENDPOINTS = 50
TOP_METHODS = 8
TOP_STATUS_CODES = 12
# One bucket per Traefik router. A few per deployed stack, so this is a
# ceiling rather than a ranking: the by-stack totals must add up.
TOP_ROUTERS = 500
# Latency percentiles reported for the window as a whole. Per endpoint the
# slowest request is reported instead, see the `max_duration` aggregation.
PERCENTILES = [50, 95, 99]

_STATUS_RANGES = [
    {"key": "ok", "from": 100, "to": 400},
    {"key": "client_errors", "from": 400, "to": 500},
    {"key": "server_errors", "from": 500, "to": 600},
]
_CLIENT_ERRORS = {"range": {"status": {"gte": 400, "lt": 500}}}
_SERVER_ERRORS = {"range": {"status": {"gte": 500}}}

# Bucket key standing for "no router matched": Traefik answered the request
# itself, usually a 404 for a host it does not serve.  A router name never
# contains a "@", let alone is empty, so this collides with none.
NO_ROUTER = ""


def _traffic_aggs() -> Dict[str, Any]:
    """Sub-aggregations every ranking (endpoint, host, router) reports."""
    return {
        "distinct_ips": {"cardinality": {"field": "client_ip"}},
        "client_errors": {"filter": _CLIENT_ERRORS},
        "server_errors": {"filter": _SERVER_ERRORS},
        "avg_duration": {"avg": {"field": "duration_ms"}},
        "bytes": {"sum": {"field": "size"}},
    }


def router_filter(router_names: Iterable[str]) -> Dict[str, Any]:
    """Query matching the requests served by any of these Traefik routers.

    The index holds a router's qualified name ("pulsarcd@swarm") while the
    Swarm labels give the bare one, and the provider suffix depends on how
    Traefik discovered the service.  Matching on the prefix keeps the filter
    independent of it.  No router name means no match, which is what a stack
    that publishes nothing should return.
    """
    return {
        "bool": {
            "should": [{"prefix": {"router": f"{name}@"}} for name in router_names],
            "minimum_should_match": 1,
        }
    }


def build_usage_query(start: datetime, end: datetime, minutes: int, include_internal: bool,
                      router_names: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """Totals, timeline and the endpoint, host and router rankings.

    router_names restricts the window to one stack; None means every request.
    """
    filters: List[Dict[str, Any]] = [
        {"range": {"timestamp": {"gte": start.isoformat()}}},
        # WAF blocks share the index but never reached an application: they
        # are the Security view's subject, not usage.
        {"term": {"event": EVENT_REQUEST}},
    ]
    if not include_internal:
        filters.append({"term": {"internal": False}})
    if router_names is not None:
        filters.append(router_filter(router_names))

    return {
        "size": 0,
        # Without this the hit count stops at 10 000 and every share computed
        # from it would be wrong on any busy window.
        "track_total_hits": True,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            "unique_ips": {"cardinality": {"field": "client_ip"}},
            "distinct_paths": {"cardinality": {"field": "path"}},
            "distinct_hosts": {"cardinality": {"field": "request_host"}},
            "statuses": {"range": {"field": "status", "ranges": _STATUS_RANGES}},
            "bytes": {"sum": {"field": "size"}},
            "avg_duration": {"avg": {"field": "duration_ms"}},
            "duration": {"percentiles": {"field": "duration_ms", "percents": PERCENTILES}},
            "timeline": {
                "date_histogram": {
                    "field": "timestamp",
                    "fixed_interval": timeline_interval(minutes),
                    "min_doc_count": 0,
                    "extended_bounds": {"min": start.isoformat(), "max": end.isoformat()},
                },
                "aggs": {
                    "statuses": {"range": {"field": "status", "ranges": _STATUS_RANGES}},
                    "avg_duration": {"avg": {"field": "duration_ms"}},
                },
            },
            # One aggregation, two tables: the outer buckets rank the hosts
            # and the inner ones their endpoints.
            "hosts": {
                "terms": {"field": "request_host", "size": TOP_HOSTS},
                "aggs": {
                    **_traffic_aggs(),
                    "distinct_paths": {"cardinality": {"field": "path"}},
                    "paths": {
                        "terms": {"field": "path", "size": TOP_PATHS_PER_HOST},
                        "aggs": {
                            **_traffic_aggs(),
                            # Deliberately no per-endpoint percentile: a
                            # t-digest per bucket is what tips the request
                            # circuit breaker. The slowest request answers the
                            # same question ("is anything here slow?") for a
                            # max aggregation's cost.
                            "max_duration": {"max": {"field": "duration_ms"}},
                            "methods": {"terms": {"field": "method", "size": 4}},
                            # Which stack the endpoint belongs to: an endpoint
                            # is served by one router, so its busiest one
                            # names it.
                            "router": {"terms": {"field": "router", "size": 1,
                                                 "missing": NO_ROUTER}},
                            "last_seen": {"max": {"field": "timestamp"}},
                        },
                    },
                },
            },
            "routers": {
                "terms": {"field": "router", "size": TOP_ROUTERS, "missing": NO_ROUTER},
                "aggs": _traffic_aggs(),
            },
            "methods": {"terms": {"field": "method", "size": TOP_METHODS}},
            "status_codes": {"terms": {"field": "status", "size": TOP_STATUS_CODES}},
        },
    }


# --- Interpretation ---------------------------------------------------------

def router_base(router: Optional[str]) -> str:
    """The router name without the provider Traefik discovered it through."""
    return (router or NO_ROUTER).split("@", 1)[0]


def _count(agg: Optional[Dict[str, Any]]) -> int:
    return (agg or {}).get("doc_count", 0)


def _value(agg: Optional[Dict[str, Any]]) -> float:
    return (agg or {}).get("value") or 0


def _ms(agg: Optional[Dict[str, Any]]) -> Optional[float]:
    """An average duration, rounded, or None when the bucket holds none."""
    value = (agg or {}).get("value")
    return round(value, 1) if value is not None else None


def _percentiles(agg: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """{"p50": .., "p95": .., "p99": ..}; a percentile with no data is None."""
    values = (agg or {}).get("values") or {}
    result = {}
    for percent in PERCENTILES:
        value = values.get(f"{percent}.0", values.get(str(percent)))
        # An empty bucket answers NaN, which json.dumps would write as a
        # literal no browser accepts.
        result[f"p{percent}"] = round(value, 1) if isinstance(value, (int, float)) and value == value else None
    return result


def _share(part: int, total: int) -> float:
    return round(100 * part / total, 2) if total else 0.0


def _traffic(bucket: Dict[str, Any], total_requests: int) -> Dict[str, Any]:
    """The metrics _traffic_aggs collected, for one ranking row."""
    requests = bucket["doc_count"]
    return {
        "requests": requests,
        "share": _share(requests, total_requests),
        "distinct_ips": bucket.get("distinct_ips", {}).get("value", 0),
        "client_errors": _count(bucket.get("client_errors")),
        "server_errors": _count(bucket.get("server_errors")),
        "avg_duration_ms": _ms(bucket.get("avg_duration")),
        "bytes": int(_value(bucket.get("bytes"))),
    }


def _endpoint_entry(host: str, bucket: Dict[str, Any], total_requests: int,
                    router_stacks: Dict[str, str]) -> Dict[str, Any]:
    routers = bucket.get("router", {}).get("buckets", [])
    router = router_base(routers[0]["key"]) if routers else NO_ROUTER
    return {
        "host": host,
        "path": bucket["key"],
        "router": router or None,
        "stack": router_stacks.get(router),
        "methods": [b["key"] for b in bucket.get("methods", {}).get("buckets", [])],
        "max_duration_ms": _ms(bucket.get("max_duration")),
        "last_seen": bucket.get("last_seen", {}).get("value_as_string"),
        **_traffic(bucket, total_requests),
    }


def _endpoints(host_buckets: List[Dict[str, Any]], total_requests: int,
               router_stacks: Dict[str, str]) -> List[Dict[str, Any]]:
    """Every host's endpoints, ranked against each other rather than per host."""
    endpoints = [
        _endpoint_entry(host["key"], path, total_requests, router_stacks)
        for host in host_buckets
        for path in host.get("paths", {}).get("buckets", [])
    ]
    endpoints.sort(key=lambda e: e["requests"], reverse=True)
    return endpoints[:TOP_ENDPOINTS]


def _by_stack(buckets: List[Dict[str, Any]], total_requests: int,
              router_stacks: Dict[str, str]) -> List[Dict[str, Any]]:
    """Router buckets folded into the stacks that publish them.

    Requests no router matched, and routers no deployed stack claims (the
    edge's own file-provider routers, a stack removed since), share a row with
    no stack name rather than being dropped: the rows have to add up to the
    total shown above them.
    """
    stacks: Dict[Optional[str], Dict[str, Any]] = {}
    for bucket in buckets:
        stack = router_stacks.get(router_base(bucket["key"]))
        entry = stacks.get(stack)
        row = _traffic(bucket, total_requests)
        if entry is None:
            stacks[stack] = {"stack": stack, "routers": [], **row}
            entry = stacks[stack]
        else:
            entry["requests"] += row["requests"]
            entry["client_errors"] += row["client_errors"]
            entry["server_errors"] += row["server_errors"]
            entry["bytes"] += row["bytes"]
            entry["share"] = _share(entry["requests"], total_requests)
            # Cardinality is per router bucket; summing it would count a
            # client once per router it reached. The sum is an upper bound,
            # and the column says so.
            entry["distinct_ips"] += row["distinct_ips"]
            entry["avg_duration_ms"] = _weighted_average(entry, row)
        name = router_base(bucket["key"])
        if name:
            entry["routers"].append(name)

    rows = list(stacks.values())
    rows.sort(key=lambda s: s["requests"], reverse=True)
    return rows


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
        "generated_at": generated_at.isoformat() + "Z",
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


def summarize(response: Dict[str, Any], *, minutes: int, include_internal: bool,
              stack: Optional[str], stacks: List[str], router_stacks: Dict[str, str],
              generated_at: datetime) -> Dict[str, Any]:
    """The usage view's payload, from the single search response."""
    result = empty_overview(minutes, include_internal, stack, stacks, generated_at)
    aggs = response.get("aggregations", {})
    total = response.get("hits", {}).get("total", {})
    requests = total.get("value", 0) if isinstance(total, dict) else (total or 0)

    result["timeline"] = [
        {
            "timestamp": b["key_as_string"],
            "avg_duration_ms": _ms(b.get("avg_duration")),
            **{s["key"]: s["doc_count"] for s in b.get("statuses", {}).get("buckets", [])},
        }
        for b in aggs.get("timeline", {}).get("buckets", [])
    ]

    host_buckets = aggs.get("hosts", {}).get("buckets", [])
    result["endpoints"] = _endpoints(host_buckets, requests, router_stacks)
    result["hosts"] = [
        {
            "host": b["key"],
            "distinct_paths": b.get("distinct_paths", {}).get("value", 0),
            **_traffic(b, requests),
        }
        for b in host_buckets
    ]
    result["by_stack"] = _by_stack(
        aggs.get("routers", {}).get("buckets", []), requests, router_stacks)
    result["methods"] = [
        {"key": b["key"], "count": b["doc_count"]}
        for b in aggs.get("methods", {}).get("buckets", [])
    ]
    result["status_codes"] = [
        {"status": b["key"], "count": b["doc_count"]}
        for b in aggs.get("status_codes", {}).get("buckets", [])
    ]

    statuses = {b["key"]: b["doc_count"]
                for b in aggs.get("statuses", {}).get("buckets", [])}
    client_errors = statuses.get("client_errors", 0)
    server_errors = statuses.get("server_errors", 0)
    result["totals"] = {
        "requests": requests,
        "unique_ips": aggs.get("unique_ips", {}).get("value", 0),
        "endpoints": aggs.get("distinct_paths", {}).get("value", 0),
        "hosts": aggs.get("distinct_hosts", {}).get("value", 0),
        "client_errors": client_errors,
        "server_errors": server_errors,
        "bytes": int(_value(aggs.get("bytes"))),
        "requests_per_minute": round(requests / minutes, 2) if minutes else 0.0,
        "error_rate": _share(client_errors + server_errors, requests),
        "avg_duration_ms": _ms(aggs.get("avg_duration")),
        **_percentiles(aggs.get("duration")),
    }
    return result
