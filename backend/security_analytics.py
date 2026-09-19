"""Attack detection over the access index (Traefik requests and WAF blocks).

Everything here is pure: query bodies are built and responses interpreted
without touching OpenSearch, so the detection rules can be tested directly.
OpenSearchClient.get_security_overview runs two searches: the first ranks
candidate client IPs and hot endpoints, the second computes detailed
statistics for the candidates only.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from shared.access_log import EVENT_REQUEST, EVENT_WAF_BLOCK

MIN_WINDOW_MINUTES = 5
MAX_WINDOW_MINUTES = 7 * 24 * 60
DEFAULT_WINDOW_MINUTES = 60

# Candidates taken from each ranking (by requests, by 4xx, by WAF blocks).
TOP_N = 25
MAX_IP_EVENTS = 200

# --- Detection thresholds -------------------------------------------------
# A client's peak is its busiest minute in the window. Over long windows the
# busiest 5-minute or 1-hour bucket is used instead, divided back to a
# per-minute rate (see rate_interval), so short bursts are smoothed there.
FLOOD_PEAK_PER_MINUTE = 300          # 5 req/s sustained for a whole minute
BURST_PEAK_PER_MINUTE = 120          # 2 req/s
SCANNER_MIN_NOT_FOUND = 30           # probing for files that do not exist...
SCANNER_MIN_DISTINCT_PATHS = 20      # ...across many different paths
DENIED_MIN = 30                      # 401/403 answers: credentials or allowlists
RATE_LIMITED_MIN = 10                # 429 answers from the rate-limit middleware
WAF_MIN_BLOCKS = 3
MOSTLY_ERRORS_MIN_REQUESTS = 20
MOSTLY_ERRORS_RATIO = 0.8
# Endpoints
CONCENTRATED_MIN_REQUESTS = 200      # one client sends most of the traffic
CONCENTRATED_TOP_SHARE = 0.8
PROBED_MIN_REQUESTS = 50             # many clients, almost only errors:
PROBED_MIN_IPS = 5                   # a distributed scan of one path
PROBED_ERROR_RATIO = 0.9

_SEVERITY_SCORE = {"high": 3, "medium": 1}

_STATUS_RANGES = [
    {"key": "ok", "from": 100, "to": 400},
    {"key": "client_errors", "from": 400, "to": 500},
    {"key": "server_errors", "from": 500, "to": 600},
]
_CLIENT_ERRORS = {"range": {"status": {"gte": 400, "lt": 500}}}
_SERVER_ERRORS = {"range": {"status": {"gte": 500}}}
_DENIED = {"terms": {"status": [401, 403]}}
_RATE_LIMITED = {"term": {"status": 429}}
_IS_REQUEST = {"term": {"event": EVENT_REQUEST}}
_IS_WAF_BLOCK = {"term": {"event": EVENT_WAF_BLOCK}}


def clamp_window(minutes: Any) -> int:
    try:
        value = int(minutes)
    except (TypeError, ValueError):
        return DEFAULT_WINDOW_MINUTES
    return max(MIN_WINDOW_MINUTES, min(value, MAX_WINDOW_MINUTES))


def timeline_interval(minutes: int) -> str:
    """date_histogram interval of the request timeline (at most ~170 points)."""
    if minutes <= 60:
        return "1m"
    if minutes <= 360:
        return "5m"
    if minutes <= 1440:
        return "15m"
    return "1h"


def rate_interval(minutes: int) -> Tuple[str, int]:
    """Bucket used to find peak rates, and its length in minutes.

    One-minute buckets up to 6 h. Beyond that the per-client histograms would
    exceed search.max_buckets (65535 for a whole response) with ~100 clients.
    """
    if minutes <= 360:
        return "1m", 1
    if minutes <= 1440:
        return "5m", 5
    return "1h", 60


def _window_filters(start: datetime, include_internal: bool) -> List[Dict[str, Any]]:
    filters: List[Dict[str, Any]] = [{"range": {"timestamp": {"gte": start.isoformat()}}}]
    if not include_internal:
        filters.append({"term": {"internal": False}})
    return filters


def _peak_aggs(minutes: int) -> Dict[str, Any]:
    interval, _ = rate_interval(minutes)
    return {
        "per_interval": {"date_histogram": {"field": "timestamp", "fixed_interval": interval}},
        "peak": {"max_bucket": {"buckets_path": "per_interval>_count"}},
    }


def build_overview_query(start: datetime, end: datetime, minutes: int,
                         include_internal: bool) -> Dict[str, Any]:
    """Totals, timeline, candidate IPs and hot endpoints."""
    return {
        "size": 0,
        "query": {"bool": {"filter": _window_filters(start, include_internal)}},
        "aggs": {
            "requests": {
                "filter": _IS_REQUEST,
                "aggs": {
                    "unique_ips": {"cardinality": {"field": "client_ip"}},
                    "statuses": {"range": {"field": "status", "ranges": _STATUS_RANGES}},
                    "denied": {"filter": _DENIED},
                    "rate_limited": {"filter": _RATE_LIMITED},
                    "timeline": {
                        "date_histogram": {
                            "field": "timestamp",
                            "fixed_interval": timeline_interval(minutes),
                            "min_doc_count": 0,
                            "extended_bounds": {"min": start.isoformat(), "max": end.isoformat()},
                        },
                        "aggs": {"statuses": {"range": {"field": "status", "ranges": _STATUS_RANGES}}},
                    },
                    # A flood ranks by volume; a scanner with a few hundred
                    # 404s would not, next to busy legitimate clients.
                    "by_requests": {"terms": {"field": "client_ip", "size": TOP_N}},
                    "by_errors": {
                        "filter": _CLIENT_ERRORS,
                        "aggs": {"ips": {"terms": {"field": "client_ip", "size": TOP_N}}},
                    },
                    "endpoints": {
                        "multi_terms": {
                            "terms": [{"field": "request_host"}, {"field": "path"}],
                            "size": TOP_N,
                        },
                        "aggs": {
                            "distinct_ips": {"cardinality": {"field": "client_ip"}},
                            "client_errors": {"filter": _CLIENT_ERRORS},
                            "server_errors": {"filter": _SERVER_ERRORS},
                            "top_ips": {"terms": {"field": "client_ip", "size": 3}},
                            **_peak_aggs(minutes),
                        },
                    },
                },
            },
            "waf": {
                "filter": _IS_WAF_BLOCK,
                "aggs": {
                    "ips": {"terms": {"field": "client_ip", "size": TOP_N}},
                    "rules": {"terms": {"field": "rule_msg", "size": 10}},
                },
            },
        },
    }


def candidate_ips(overview: Dict[str, Any]) -> List[str]:
    """IPs worth a detailed look: top by requests, by 4xx and by WAF blocks."""
    aggs = overview.get("aggregations", {})
    requests = aggs.get("requests", {})
    rankings = [
        requests.get("by_requests", {}),
        requests.get("by_errors", {}).get("ips", {}),
        aggs.get("waf", {}).get("ips", {}),
    ]
    ips: List[str] = []
    for ranking in rankings:
        for bucket in ranking.get("buckets", []):
            if bucket["key"] not in ips:
                ips.append(bucket["key"])
    return ips


def build_ip_stats_query(start: datetime, minutes: int, include_internal: bool,
                         ips: List[str]) -> Dict[str, Any]:
    """Per-IP statistics for the candidates returned by candidate_ips."""
    filters = _window_filters(start, include_internal) + [{"terms": {"client_ip": ips}}]
    return {
        "size": 0,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            "ips": {
                "terms": {"field": "client_ip", "size": len(ips)},
                "aggs": {
                    "requests": {
                        "filter": _IS_REQUEST,
                        "aggs": {
                            "client_errors": {"filter": _CLIENT_ERRORS},
                            "not_found": {"filter": {"term": {"status": 404}}},
                            "denied": {"filter": _DENIED},
                            "rate_limited": {"filter": _RATE_LIMITED},
                            "server_errors": {"filter": _SERVER_ERRORS},
                            "distinct_paths": {"cardinality": {"field": "path"}},
                            "distinct_hosts": {"cardinality": {"field": "request_host"}},
                            "top_hosts": {"terms": {"field": "request_host", "size": 3}},
                            "top_paths": {"terms": {"field": "path", "size": 5}},
                            "user_agents": {"terms": {"field": "user_agent", "size": 1}},
                            **_peak_aggs(minutes),
                        },
                    },
                    "waf_blocks": {"filter": _IS_WAF_BLOCK},
                    "internal": {"terms": {"field": "internal", "size": 1}},
                    "first_seen": {"min": {"field": "timestamp"}},
                    "last_seen": {"max": {"field": "timestamp"}},
                },
            }
        },
    }


def build_ip_events_query(ip: str, start: datetime, size: int = MAX_IP_EVENTS) -> Dict[str, Any]:
    """Latest requests and WAF blocks of one client."""
    return {
        "size": size,
        "sort": [{"timestamp": {"order": "desc"}}],
        "query": {"bool": {"filter": [
            {"range": {"timestamp": {"gte": start.isoformat()}}},
            {"term": {"client_ip": ip}},
        ]}},
        "_source": ["timestamp", "event", "request_host", "method", "uri", "status",
                    "router", "user_agent", "rule_msg", "duration_ms"],
    }


# --- Interpretation ---------------------------------------------------------

def _count(agg: Optional[Dict[str, Any]]) -> int:
    return (agg or {}).get("doc_count", 0)


def _peak_per_minute(bucket: Dict[str, Any], minutes: int) -> float:
    _, bucket_minutes = rate_interval(minutes)
    value = (bucket.get("peak") or {}).get("value") or 0
    return round(value / bucket_minutes, 1)


def _top(agg: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{"key": b["key"], "count": b["doc_count"]} for b in (agg or {}).get("buckets", [])]


def _flag(code: str, severity: str, label: str, detail: str) -> Dict[str, str]:
    return {"code": code, "severity": severity, "label": label, "detail": detail}


def ip_flags(ip: Dict[str, Any]) -> List[Dict[str, str]]:
    """Detection rules for one client IP."""
    flags = []
    peak = ip["peak_per_minute"]
    if peak >= FLOOD_PEAK_PER_MINUTE:
        flags.append(_flag("flood", "high", "Flood", f"Peak of {peak:g} requests/min"))
    elif peak >= BURST_PEAK_PER_MINUTE:
        flags.append(_flag("burst", "medium", "Burst", f"Peak of {peak:g} requests/min"))
    if ip["not_found"] >= SCANNER_MIN_NOT_FOUND and ip["distinct_paths"] >= SCANNER_MIN_DISTINCT_PATHS:
        flags.append(_flag("scanner", "high", "Scanner",
                           f"{ip['not_found']} × 404 over {ip['distinct_paths']} distinct paths"))
    if ip["waf_blocks"] >= WAF_MIN_BLOCKS:
        flags.append(_flag("waf", "high", "WAF", f"{ip['waf_blocks']} requests blocked by the WAF"))
    if ip["denied"] >= DENIED_MIN:
        flags.append(_flag("denied", "medium", "Denied", f"{ip['denied']} × 401/403"))
    if ip["rate_limited"] >= RATE_LIMITED_MIN:
        flags.append(_flag("rate_limited", "medium", "Rate limited", f"{ip['rate_limited']} × 429"))
    if (ip["requests"] >= MOSTLY_ERRORS_MIN_REQUESTS
            and ip["client_errors"] / ip["requests"] >= MOSTLY_ERRORS_RATIO):
        share = round(100 * ip["client_errors"] / ip["requests"])
        flags.append(_flag("errors", "medium", "Mostly 4xx", f"{share}% of its requests got a 4xx"))
    return flags


def endpoint_flags(ep: Dict[str, Any]) -> List[Dict[str, str]]:
    """Detection rules for one endpoint (host + path)."""
    flags = []
    peak = ep["peak_per_minute"]
    if peak >= FLOOD_PEAK_PER_MINUTE:
        flags.append(_flag("hammered", "high", "Hammered", f"Peak of {peak:g} requests/min"))
    elif peak >= BURST_PEAK_PER_MINUTE:
        flags.append(_flag("busy", "medium", "Busy", f"Peak of {peak:g} requests/min"))
    top = ep["top_ips"][0] if ep["top_ips"] else None
    if top and ep["requests"] >= CONCENTRATED_MIN_REQUESTS:
        share = top["count"] / ep["requests"]
        if share >= CONCENTRATED_TOP_SHARE:
            flags.append(_flag("concentrated", "medium", "One client",
                               f"{round(100 * share)}% of the requests come from {top['key']}"))
    if (ep["requests"] >= PROBED_MIN_REQUESTS and ep["distinct_ips"] >= PROBED_MIN_IPS
            and ep["client_errors"] / ep["requests"] >= PROBED_ERROR_RATIO):
        flags.append(_flag("probed", "medium", "Probed",
                           f"{ep['distinct_ips']} clients, {round(100 * ep['client_errors'] / ep['requests'])}% 4xx"))
    return flags


def _score(flags: List[Dict[str, str]]) -> int:
    return sum(_SEVERITY_SCORE.get(f["severity"], 0) for f in flags)


def _ip_entry(bucket: Dict[str, Any], minutes: int) -> Dict[str, Any]:
    requests = bucket.get("requests", {})
    internal_buckets = bucket.get("internal", {}).get("buckets", [])
    user_agents = _top(requests.get("user_agents"))
    ip = {
        "ip": bucket["key"],
        "internal": bool(internal_buckets and internal_buckets[0]["key"]),
        "requests": _count(requests),
        "client_errors": _count(requests.get("client_errors")),
        "not_found": _count(requests.get("not_found")),
        "denied": _count(requests.get("denied")),
        "rate_limited": _count(requests.get("rate_limited")),
        "server_errors": _count(requests.get("server_errors")),
        "waf_blocks": _count(bucket.get("waf_blocks")),
        "distinct_paths": requests.get("distinct_paths", {}).get("value", 0),
        "distinct_hosts": requests.get("distinct_hosts", {}).get("value", 0),
        "top_hosts": _top(requests.get("top_hosts")),
        "top_paths": _top(requests.get("top_paths")),
        "user_agent": user_agents[0]["key"] if user_agents else None,
        "peak_per_minute": _peak_per_minute(requests, minutes),
        "first_seen": bucket.get("first_seen", {}).get("value_as_string"),
        "last_seen": bucket.get("last_seen", {}).get("value_as_string"),
    }
    ip["flags"] = ip_flags(ip)
    ip["score"] = _score(ip["flags"])
    return ip


def _endpoint_entry(bucket: Dict[str, Any], minutes: int) -> Dict[str, Any]:
    host, path = bucket["key"]
    ep = {
        "host": host,
        "path": path,
        "requests": bucket["doc_count"],
        "distinct_ips": bucket.get("distinct_ips", {}).get("value", 0),
        "client_errors": _count(bucket.get("client_errors")),
        "server_errors": _count(bucket.get("server_errors")),
        "top_ips": _top(bucket.get("top_ips")),
        "peak_per_minute": _peak_per_minute(bucket, minutes),
    }
    ep["flags"] = endpoint_flags(ep)
    ep["score"] = _score(ep["flags"])
    return ep


def empty_overview(minutes: int, include_internal: bool, generated_at: datetime,
                   error: Optional[str] = None) -> Dict[str, Any]:
    result = {
        "window_minutes": minutes,
        "include_internal": include_internal,
        "generated_at": generated_at.isoformat() + "Z",
        "timeline_interval": timeline_interval(minutes),
        "totals": {
            "requests": 0, "unique_ips": 0, "client_errors": 0, "server_errors": 0,
            "denied": 0, "rate_limited": 0, "waf_blocks": 0, "flagged_ips": 0,
            "flagged_endpoints": 0,
        },
        "timeline": [],
        "ips": [],
        "endpoints": [],
        "waf_rules": [],
    }
    if error:
        result["error"] = error
    return result


def summarize(overview: Dict[str, Any], ip_stats: Optional[Dict[str, Any]], *,
              minutes: int, include_internal: bool, generated_at: datetime) -> Dict[str, Any]:
    """The security view's payload, from the two search responses."""
    result = empty_overview(minutes, include_internal, generated_at)
    aggs = overview.get("aggregations", {})
    requests = aggs.get("requests", {})
    waf = aggs.get("waf", {})
    statuses = {b["key"]: b["doc_count"] for b in requests.get("statuses", {}).get("buckets", [])}

    result["timeline"] = [
        {
            "timestamp": b["key_as_string"],
            **{s["key"]: s["doc_count"] for s in b.get("statuses", {}).get("buckets", [])},
        }
        for b in requests.get("timeline", {}).get("buckets", [])
    ]

    ip_buckets = (ip_stats or {}).get("aggregations", {}).get("ips", {}).get("buckets", [])
    ips = [_ip_entry(b, minutes) for b in ip_buckets]
    ips.sort(key=lambda ip: (ip["score"], ip["requests"]), reverse=True)
    result["ips"] = ips

    endpoints = [_endpoint_entry(b, minutes)
                 for b in requests.get("endpoints", {}).get("buckets", [])]
    endpoints.sort(key=lambda ep: (ep["score"], ep["requests"]), reverse=True)
    result["endpoints"] = endpoints

    result["waf_rules"] = _top(waf.get("rules"))
    result["totals"] = {
        "requests": _count(requests),
        "unique_ips": requests.get("unique_ips", {}).get("value", 0),
        "client_errors": statuses.get("client_errors", 0),
        "server_errors": statuses.get("server_errors", 0),
        "denied": _count(requests.get("denied")),
        "rate_limited": _count(requests.get("rate_limited")),
        "waf_blocks": _count(waf),
        "flagged_ips": sum(1 for ip in ips if ip["flags"]),
        "flagged_endpoints": sum(1 for ep in endpoints if ep["flags"]),
    }
    return result
