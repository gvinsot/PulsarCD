"""Attack detection over the access records (Traefik requests and WAF blocks).

Everything here is pure: LogsQL queries are built and their rows interpreted
without touching VictoriaLogs, so the detection rules can be tested directly.
LogStore.get_security_overview runs two rounds of queries: the overview ranks
candidate client IPs and hot endpoints, the detail round computes statistics
for those candidates only.
"""

import math
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from shared.access_log import EVENT_REQUEST, EVENT_WAF_BLOCK
from shared.victorialogs import (
    KIND_ACCESS, any_of, bucket_times, exact, field, integer, iso_ms, num, parse_time, quote,
    stream, text_filter,
)

from .ip_blocklist import ROUTER_NAME as EDGE_ROUTER_NAME

MIN_WINDOW_MINUTES = 5
MAX_WINDOW_MINUTES = 7 * 24 * 60
DEFAULT_WINDOW_MINUTES = 60

# Candidates taken from each ranking (by requests, by 4xx, by WAF blocks).
TOP_N = 25
MAX_IP_EVENTS = 200
# Rows of the request explorer, newest first.
MAX_REQUESTS = 500
# (host, uri, client) groups read to rank each attack category's targets and
# clients. The category totals are exact; only these top lists are sampled.
ATTACK_SAMPLES = 1000
ATTACK_TOP = 5

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

OK = "status:range[100, 400)"
CLIENT_ERRORS = "status:range[400, 500)"
SERVER_ERRORS = "status:>=500"
DENIED = "status:in(401, 403)"
RATE_LIMITED = "status:=429"
NOT_FOUND = "status:=404"
IS_REQUEST = exact("event", EVENT_REQUEST)
IS_WAF_BLOCK = exact("event", EVENT_WAF_BLOCK)

ANSWERED = "status:range[200, 300)"
# Requests the edge refused because the client is on the blocklist (both the
# HTTPS and the HTTP router of ip_blocklist share this prefix).
REFUSED_AT_EDGE = f"router:{quote(EDGE_ROUTER_NAME)}*"

_STATUS_COUNTS = (f"count() if ({OK}) as ok, count() if ({CLIENT_ERRORS}) as client_errors, "
                  f"count() if ({SERVER_ERRORS}) as server_errors")

# --- Attack categories --------------------------------------------------------
# What a request was after, read from its path, or from its whole URI for the
# payloads a query string carries. The same patterns (the common subset of RE2
# and Python's re) drive the exact LogsQL counts and the Python classification
# of the sampled requests, so both put a request in the same category. Ordered
# by severity: a request matching several categories counts for the first.
ATTACK_CATEGORIES: Tuple[Dict[str, str], ...] = (
    {"code": "injection", "label": "Injection", "field": "uri",
     "description": "SQL, script, template or command injection payloads",
     "pattern": (r"(?i)union(\s|\+|%20)+(all(\s|\+|%20)+)?select|<script|%3cscript|javascript:"
                 r"|\$\{jndi|%24%7bjndi|\$\{\{|information_schema|xp_cmdshell|sleep\([0-9]"
                 r"|benchmark\(|/bin/(ba)?sh|cmd\.exe|(\s|\+|%20|;|%3b|\||%7c)(wget|curl)(\s|\+|%20)")},
    {"code": "traversal", "label": "Path traversal", "field": "uri",
     "description": "Climbing out of the web root to read system files",
     "pattern": (r"(?i)\.\./|\.\.\\|\.\.%2f|\.\.%5c|%2e%2e|%252e%252e|/etc/(passwd|shadow|hosts)"
                 r"|win\.ini|boot\.ini|/proc/self/")},
    {"code": "secrets", "label": "Secrets & source", "field": "path",
     "description": "Environment files, credentials, keys and version-control folders",
     "pattern": (r"(?i)(^|/)\.(env|git|svn|hg|bzr|aws|ssh|docker|kube|npmrc|pypirc|netrc|htpasswd"
                 r"|htaccess|ds_store|vscode|idea)([./~;_-]|$)|\.env([./~;]|$)"
                 r"|(^|/)(auth|credentials|secrets?|appsettings|parameters)\.(json|ya?ml|ini|xml|toml)"
                 r"|(^|/)(id_rsa|id_ed25519|id_ecdsa)([./~;]|$)")},
    {"code": "exploits", "label": "Known exploits", "field": "path",
     "description": "Routers, VPN appliances, frameworks and tooling with public exploits",
     "pattern": (r"(?i)(^|/)(cgi-bin|boaform|hnap1|gponform|vendor/phpunit|_ignition|actuator"
                 r"|server-status|solr|jenkins|manager/html|telescope|_profiler|owa|ecp|autodiscover"
                 r"|remote/login|global-protect|dana-na|containers/json|api/v1/pods|setup\.cgi)"
                 r"([/;._-]|$)")},
    {"code": "wordpress", "label": "WordPress", "field": "path",
     "description": "WordPress admin, login and plugin paths: no WordPress runs here",
     "pattern": (r"(?i)(^|/)((wp-admin|wp-content|wp-includes|wp-json)([/;]|$)"
                 r"|(wp-login|wp-config|wp-cron|wp-signup|wp-activate|wp-blog-header|wp-load"
                 r"|wp-mail|wp-comments-post|wp-trackback|xmlrpc)\.php)")},
    {"code": "php", "label": "PHP & admin panels", "field": "path",
     "description": "PHP scripts and database admin tools",
     "pattern": (r"(?i)\.php[0-9]?([/;.]|$)|(^|/)(phpmyadmin|pma|myadmin|mysqladmin|adminer"
                 r"|phpinfo|pgadmin)([/._-]|$)")},
    {"code": "backups", "label": "Backups & dumps", "field": "path",
     "description": "Archives, database dumps and editor leftovers",
     "pattern": (r"(?i)\.(bak|backup|old|orig|save|swp|sql|sqlite3?|db|dump|tar|tgz|gz|bz2|xz|zip"
                 r"|rar|7z)$")},
)
_ATTACKS_BY_CODE = {cat["code"]: cat for cat in ATTACK_CATEGORIES}
_ATTACK_RES = [(cat["code"], cat["field"], re.compile(cat["pattern"])) for cat in ATTACK_CATEGORIES]

# Status choices of the request explorer.
REQUEST_STATUSES = {
    "2xx": ANSWERED,
    "3xx": "status:range[300, 400)",
    "4xx": CLIENT_ERRORS,
    "404": NOT_FOUND,
    "denied": DENIED,
    "429": RATE_LIMITED,
    "5xx": SERVER_ERRORS,
    "edge": REFUSED_AT_EDGE,
}


def _attack_match(category: Dict[str, str]) -> str:
    return f"{field(category['field'])}:~{quote(category['pattern'])}"


ANY_ATTACK = "(" + " OR ".join(_attack_match(cat) for cat in ATTACK_CATEGORIES) + ")"


def attack_filter(code: str) -> str:
    """Requests of one category: its pattern, and none of the categories above it."""
    filters = []
    for cat in ATTACK_CATEGORIES:
        if cat["code"] == code:
            return "(" + " ".join([_attack_match(cat)] + filters) + ")"
        filters.append(f"NOT {_attack_match(cat)}")
    raise ValueError(f"Unknown attack category: {code}")


def classify_request(uri: Any) -> Optional[str]:
    """Attack category of a request URI, as attack_filter would count it."""
    uri = str(uri or "")
    path = uri.split("?", 1)[0]
    for code, name, pattern in _ATTACK_RES:
        if pattern.search(uri if name == "uri" else path):
            return code
    return None


def clamp_window(minutes: Any) -> int:
    try:
        value = int(minutes)
    except (TypeError, ValueError):
        return DEFAULT_WINDOW_MINUTES
    return max(MIN_WINDOW_MINUTES, min(value, MAX_WINDOW_MINUTES))


def resolve_window(minutes: Any, start: Any, end: Any, now: datetime) -> Tuple[datetime, datetime, int]:
    """``(start, end, minutes)`` of the requested range.

    Without ``start``/``end`` it is the last ``minutes``. With both, it is that
    explicit range -- a bucket picked on the timeline, possibly one minute
    long -- ending at ``now`` at the latest. Raises ValueError otherwise.
    """
    if start is None and end is None:
        minutes = clamp_window(minutes)
        return now - timedelta(minutes=minutes), now, minutes
    begin, finish = parse_time(start), parse_time(end)
    if begin is None or finish is None:
        raise ValueError("start and end must both be ISO 8601 timestamps")
    finish = min(finish, now)
    if finish <= begin:
        raise ValueError("The range must end after it starts, and start in the past")
    if finish - begin > timedelta(minutes=MAX_WINDOW_MINUTES):
        raise ValueError("The range cannot exceed 7 days")
    return begin, finish, max(1, math.ceil((finish - begin).total_seconds() / 60))


def timeline_interval(minutes: int) -> str:
    """Bucket of the request timeline (at most ~170 points)."""
    if minutes <= 60:
        return "1m"
    if minutes <= 360:
        return "5m"
    if minutes <= 1440:
        return "15m"
    return "1h"


def rate_interval(minutes: int) -> Tuple[str, int]:
    """Bucket used to find peak rates, and its length in minutes.

    One-minute buckets up to 6 h; coarser beyond, so a week-long window does
    not count every client minute by minute.
    """
    if minutes <= 360:
        return "1m", 1
    if minutes <= 1440:
        return "5m", 5
    return "1h", 60


def base_filter(include_internal: bool) -> str:
    """Access records of the window (the time range is a query argument)."""
    parts = [stream(KIND_ACCESS)]
    if not include_internal:
        parts.append("internal:=false")
    return " ".join(parts)


def endpoint_filter(endpoints: List[Tuple[str, str]]) -> str:
    """Records of any of these (request_host, path) endpoints."""
    if not endpoints:
        return "path:in()"
    return "(" + " OR ".join(
        f"({exact('request_host', host)} {exact('path', path)})" for host, path in endpoints) + ")"


def build_overview_queries(minutes: int, include_internal: bool) -> Dict[str, str]:
    """Totals, timeline, candidate IPs and hot endpoints."""
    base = base_filter(include_internal)
    requests = f"{base} {IS_REQUEST}"
    waf = f"{base} {IS_WAF_BLOCK}"
    return {
        "totals": (f"{requests} | stats count() as requests, count_uniq(client_ip) as unique_ips, "
                   f"{_STATUS_COUNTS}, count() if ({DENIED}) as denied, "
                   f"count() if ({RATE_LIMITED}) as rate_limited"),
        "timeline": f"{requests} | stats by (_time:{timeline_interval(minutes)}) {_STATUS_COUNTS}",
        # A flood ranks by volume; a scanner with a few hundred 404s would
        # not, next to busy legitimate clients.
        "by_requests": (f"{requests} client_ip:* | stats by (client_ip) count() as n "
                        f"| sort by (n desc) limit {TOP_N}"),
        "by_errors": (f"{requests} {CLIENT_ERRORS} client_ip:* | stats by (client_ip) count() as n "
                      f"| sort by (n desc) limit {TOP_N}"),
        "endpoints": (f"{requests} | stats by (request_host, path) count() as requests, "
                      f"count_uniq(client_ip) as distinct_ips, "
                      f"count() if ({CLIENT_ERRORS}) as client_errors, "
                      f"count() if ({SERVER_ERRORS}) as server_errors "
                      f"| sort by (requests desc) limit {TOP_N}"),
        "waf_total": f"{waf} | stats count() as n",
        "waf_ips": (f"{waf} client_ip:* | stats by (client_ip) count() as n "
                    f"| sort by (n desc) limit {TOP_N}"),
        "waf_rules": f"{waf} rule_msg:* | stats by (rule_msg) count() as n | sort by (n desc) limit 10",
        "attack_samples": (f"{requests} {ANY_ATTACK} | stats by (request_host, uri, client_ip) "
                           f"count() as n, count() if ({ANSWERED}) as answered "
                           f"| sort by (n desc) limit {ATTACK_SAMPLES}"),
        # One query per category: a single one would repeat every exclusive
        # filter for each of its statistics.
        **{f"attack_{code}": (f"{requests} {attack_filter(code)} | stats count() as requests, "
                              f"count_uniq(client_ip) as clients, count() if ({ANSWERED}) as answered, "
                              "max(_time) as last_seen")
           for code in _ATTACKS_BY_CODE},
    }


def candidate_ips(overview: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    """IPs worth a detailed look: top by requests, by 4xx and by WAF blocks."""
    ips: List[str] = []
    for ranking in ("by_requests", "by_errors", "waf_ips"):
        for row in overview.get(ranking, []):
            ip = row.get("client_ip")
            if ip and ip not in ips:
                ips.append(ip)
    return ips


def top_endpoints(overview: Dict[str, List[Dict[str, Any]]]) -> List[Tuple[str, str]]:
    return [(row.get("request_host", ""), row.get("path", "")) for row in overview.get("endpoints", [])]


def build_detail_queries(minutes: int, include_internal: bool,
                         overview: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    """Per-IP and per-endpoint statistics for the overview's candidates."""
    base = base_filter(include_internal)
    rate, _ = rate_interval(minutes)
    queries: Dict[str, str] = {}
    ips = candidate_ips(overview)
    if ips:
        scope = f"{base} {any_of('client_ip', ips)}"
        requests = f"{scope} {IS_REQUEST}"
        queries.update({
            "ip_stats": (
                f"{scope} | stats by (client_ip) "
                f"count() if ({IS_REQUEST}) as requests, "
                f"count() if ({IS_REQUEST} {CLIENT_ERRORS}) as client_errors, "
                f"count() if ({IS_REQUEST} {NOT_FOUND}) as not_found, "
                f"count() if ({IS_REQUEST} {DENIED}) as denied, "
                f"count() if ({IS_REQUEST} {RATE_LIMITED}) as rate_limited, "
                f"count() if ({IS_REQUEST} {SERVER_ERRORS}) as server_errors, "
                f"count() if ({IS_WAF_BLOCK}) as waf_blocks, "
                f"count_uniq(path) if ({IS_REQUEST}) as distinct_paths, "
                f"count_uniq(request_host) if ({IS_REQUEST}) as distinct_hosts, "
                f"any(internal) as internal, min(_time) as first_seen, max(_time) as last_seen"),
            "ip_hosts": (f"{requests} request_host:* | stats by (client_ip, request_host) count() as n "
                         "| sort by (n desc) partition by (client_ip) limit 3"),
            "ip_paths": (f"{requests} path:* | stats by (client_ip, path) count() as n "
                         "| sort by (n desc) partition by (client_ip) limit 5"),
            "ip_agents": (f"{requests} user_agent:* | stats by (client_ip, user_agent) count() as n "
                          "| sort by (n desc) partition by (client_ip) limit 1"),
            "ip_peaks": (f"{requests} | stats by (client_ip, _time:{rate}) count() as c "
                         "| stats by (client_ip) max(c) as peak"),
        })
    endpoints = top_endpoints(overview)
    if endpoints:
        scope = f"{base} {IS_REQUEST} {endpoint_filter(endpoints)}"
        queries.update({
            "endpoint_ips": (f"{scope} client_ip:* | stats by (request_host, path, client_ip) count() as n "
                             "| sort by (n desc) partition by (request_host, path) limit 3"),
            "endpoint_peaks": (f"{scope} | stats by (request_host, path, _time:{rate}) count() as c "
                               "| stats by (request_host, path) max(c) as peak"),
        })
    return queries


_EVENT_FIELDS = ("_time, event, client_ip, request_host, method, uri, status, router, user_agent, "
                 "rule_msg, duration_ms")


def build_ip_events_query(ip: str) -> str:
    """Latest requests and WAF blocks of one client (newest first via limit=)."""
    return f"{stream(KIND_ACCESS)} {exact('client_ip', ip)} | fields {_EVENT_FIELDS}"


def build_requests_query(include_internal: bool, *, ip: Optional[str] = None,
                         host: Optional[str] = None, path: Optional[str] = None,
                         uri: Optional[str] = None, rule: Optional[str] = None,
                         text: Optional[str] = None, status: Optional[str] = None,
                         category: Optional[str] = None) -> str:
    """Requests and WAF blocks matching the request explorer's filters.

    Every value is quoted, and ``text`` goes through the log search's own
    parser: nothing typed can change the query. ``status`` is a key of
    REQUEST_STATUSES, or ``waf`` for the WAF blocks; ``category`` a code of
    ATTACK_CATEGORIES. Raises ValueError for an unknown one. A client asked
    for by address is shown even when it is internal.
    """
    parts = [base_filter(include_internal or bool(ip))]
    if status == "waf":
        parts.append(IS_WAF_BLOCK)
    elif status:
        if status not in REQUEST_STATUSES:
            raise ValueError(f"Unknown status filter: {status}")
        parts += [IS_REQUEST, REQUEST_STATUSES[status]]
    if category:
        if category not in _ATTACKS_BY_CODE:
            raise ValueError(f"Unknown attack category: {category}")
        if not status:
            parts.append(IS_REQUEST)
        parts.append(attack_filter(category))
    for name, value in (("client_ip", ip), ("request_host", host), ("path", path), ("uri", uri),
                        ("rule_msg", rule)):
        if value:
            parts.append(exact(name, value))
    search = text_filter(text, "uri", default_op="AND")
    if search:
        parts.append(f"({search})")
    return f"{' '.join(parts)} | fields {_EVENT_FIELDS}"


def event_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    status = num(row.get("status"))
    return {
        "timestamp": row.get("_time"),
        "event": row.get("event"),
        "client_ip": row.get("client_ip"),
        "request_host": row.get("request_host"),
        "method": row.get("method"),
        "uri": row.get("uri"),
        "status": int(status) if status is not None else None,
        "router": row.get("router"),
        "user_agent": row.get("user_agent"),
        "rule_msg": row.get("rule_msg"),
        "duration_ms": num(row.get("duration_ms")),
    }


# --- Interpretation ---------------------------------------------------------

def _peak_per_minute(peak: Any, minutes: int) -> float:
    _, bucket_minutes = rate_interval(minutes)
    return round((num(peak) or 0) / bucket_minutes, 1)


def _top(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    return [{"key": row.get(key), "count": integer(row.get("n"))} for row in rows]


def _grouped(rows: List[Dict[str, Any]], *keys: str) -> Dict[Any, List[Dict[str, Any]]]:
    out: Dict[Any, List[Dict[str, Any]]] = {}
    for row in rows:
        group = tuple(row.get(k) for k in keys)
        out.setdefault(group[0] if len(group) == 1 else group, []).append(row)
    for group_rows in out.values():
        group_rows.sort(key=lambda r: integer(r.get("n")), reverse=True)
    return out


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


def _ip_entry(row: Dict[str, Any], detail: Dict[str, Dict[Any, Any]], minutes: int) -> Dict[str, Any]:
    ip_address = row.get("client_ip")
    agents = _top(detail["agents"].get(ip_address, []), "user_agent")
    ip = {
        "ip": ip_address,
        "internal": row.get("internal") == "true",
        "requests": integer(row.get("requests")),
        "client_errors": integer(row.get("client_errors")),
        "not_found": integer(row.get("not_found")),
        "denied": integer(row.get("denied")),
        "rate_limited": integer(row.get("rate_limited")),
        "server_errors": integer(row.get("server_errors")),
        "waf_blocks": integer(row.get("waf_blocks")),
        "distinct_paths": integer(row.get("distinct_paths")),
        "distinct_hosts": integer(row.get("distinct_hosts")),
        "top_hosts": _top(detail["hosts"].get(ip_address, []), "request_host"),
        "top_paths": _top(detail["paths"].get(ip_address, []), "path"),
        "user_agent": agents[0]["key"] if agents else None,
        "peak_per_minute": _peak_per_minute(detail["peaks"].get(ip_address), minutes),
        "first_seen": row.get("first_seen"),
        "last_seen": row.get("last_seen"),
    }
    ip["flags"] = ip_flags(ip)
    ip["score"] = _score(ip["flags"])
    return ip


def _endpoint_entry(row: Dict[str, Any], detail: Dict[str, Dict[Any, Any]], minutes: int) -> Dict[str, Any]:
    key = (row.get("request_host"), row.get("path"))
    ep = {
        "host": key[0],
        "path": key[1],
        "requests": integer(row.get("requests")),
        "distinct_ips": integer(row.get("distinct_ips")),
        "client_errors": integer(row.get("client_errors")),
        "server_errors": integer(row.get("server_errors")),
        "top_ips": _top(detail["endpoint_ips"].get(key, []), "client_ip"),
        "peak_per_minute": _peak_per_minute(detail["endpoint_peaks"].get(key), minutes),
    }
    ep["flags"] = endpoint_flags(ep)
    ep["score"] = _score(ep["flags"])
    return ep


def _ranked(counts: Dict[Any, Dict[str, Any]]) -> List[Tuple[Any, Dict[str, Any]]]:
    return sorted(counts.items(), key=lambda item: item[1]["count"], reverse=True)[:ATTACK_TOP]


def attacks(overview: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """The attack categories seen in the window, most severe first.

    Totals come from each category's own query, exact; the top targets and
    clients from the sampled (host, uri, client) groups. A target is the path,
    or the whole URI for a category read from it (its payload is the point),
    with the hosts it was asked of, busiest first.
    """
    samples: Dict[str, Dict[str, Dict[Any, Dict[str, Any]]]] = {
        code: {"targets": {}, "clients": {}} for code in _ATTACKS_BY_CODE}
    for row in overview.get("attack_samples", []):
        uri = str(row.get("uri") or "")
        code = classify_request(uri)
        if code is None:
            continue
        n, answered = integer(row.get("n")), integer(row.get("answered"))
        target = uri if _ATTACKS_BY_CODE[code]["field"] == "uri" else uri.split("?", 1)[0]
        # One entry per target, whichever host it was asked of: a scanner
        # sends the same probe to every host it finds.
        entry = samples[code]["targets"].setdefault(target, {"count": 0, "answered": 0, "hosts": {}})
        entry["count"] += n
        entry["answered"] += answered
        host = row.get("request_host") or "-"
        entry["hosts"][host] = entry["hosts"].get(host, 0) + n
        client = samples[code]["clients"].setdefault(row.get("client_ip"), {"count": 0})
        client["count"] += n

    result = []
    for cat in ATTACK_CATEGORIES:
        code = cat["code"]
        totals = (overview.get(f"attack_{code}") or [{}])[0]
        requests = integer(totals.get("requests"))
        if not requests:
            continue
        result.append({
            "code": code,
            "label": cat["label"],
            "description": cat["description"],
            "field": cat["field"],
            "requests": requests,
            "clients": integer(totals.get("clients")),
            "answered": integer(totals.get("answered")),
            "last_seen": totals.get("last_seen"),
            "top_targets": [{"target": target, "count": counts["count"], "answered": counts["answered"],
                             "hosts": sorted(counts["hosts"], key=counts["hosts"].get, reverse=True)}
                            for target, counts in _ranked(samples[code]["targets"])],
            "top_ips": [{"key": ip, "count": counts["count"]}
                        for ip, counts in _ranked(samples[code]["clients"])],
        })
    return result


def empty_overview(minutes: int, include_internal: bool, generated_at: datetime,
                   error: Optional[str] = None) -> Dict[str, Any]:
    result = {
        "window_minutes": minutes,
        "include_internal": include_internal,
        "generated_at": iso_ms(generated_at),
        "timeline_interval": timeline_interval(minutes),
        "totals": {
            "requests": 0, "unique_ips": 0, "client_errors": 0, "server_errors": 0,
            "denied": 0, "rate_limited": 0, "waf_blocks": 0, "flagged_ips": 0,
            "flagged_endpoints": 0, "attack_requests": 0, "attack_answered": 0,
        },
        "timeline": [],
        "ips": [],
        "endpoints": [],
        "waf_rules": [],
        "attacks": [],
        "attack_categories": [{"code": cat["code"], "label": cat["label"]} for cat in ATTACK_CATEGORIES],
    }
    if error:
        result["error"] = error
    return result


def timeline(rows: List[Dict[str, Any]], start: datetime, end: datetime, interval: str,
             fields: Tuple[str, ...], averages: Tuple[str, ...] = ()) -> List[Dict[str, Any]]:
    """Every bucket of the window, the empty ones as zeros (null averages)."""
    by_time = {parse_time(row.get("_time")): row for row in rows}
    out = []
    for bucket in bucket_times(start, end, interval):
        row = by_time.get(bucket, {})
        point: Dict[str, Any] = {"timestamp": iso_ms(bucket)}
        for name in averages:
            value = num(row.get(name))
            point[name] = round(value, 1) if value is not None else None
        for name in fields:
            point[name] = integer(row.get(name))
        out.append(point)
    return out


def summarize(overview: Dict[str, List[Dict[str, Any]]], details: Dict[str, List[Dict[str, Any]]], *,
              minutes: int, include_internal: bool, start: datetime, end: datetime,
              generated_at: datetime) -> Dict[str, Any]:
    """The security view's payload, from the overview and detail rows."""
    result = empty_overview(minutes, include_internal, generated_at)
    totals = (overview.get("totals") or [{}])[0]
    waf_total = (overview.get("waf_total") or [{}])[0]

    result["timeline"] = timeline(overview.get("timeline", []), start, end, timeline_interval(minutes),
                                  ("ok", "client_errors", "server_errors"))

    detail = {
        "hosts": _grouped(details.get("ip_hosts", []), "client_ip"),
        "paths": _grouped(details.get("ip_paths", []), "client_ip"),
        "agents": _grouped(details.get("ip_agents", []), "client_ip"),
        "peaks": {row.get("client_ip"): row.get("peak") for row in details.get("ip_peaks", [])},
        "endpoint_ips": _grouped(details.get("endpoint_ips", []), "request_host", "path"),
        "endpoint_peaks": {(row.get("request_host"), row.get("path")): row.get("peak")
                           for row in details.get("endpoint_peaks", [])},
    }
    ips = [_ip_entry(row, detail, minutes) for row in details.get("ip_stats", [])]
    ips.sort(key=lambda ip: (ip["score"], ip["requests"]), reverse=True)
    result["ips"] = ips

    endpoints = [_endpoint_entry(row, detail, minutes) for row in overview.get("endpoints", [])]
    endpoints.sort(key=lambda ep: (ep["score"], ep["requests"]), reverse=True)
    result["endpoints"] = endpoints

    result["waf_rules"] = _top(overview.get("waf_rules", []), "rule_msg")
    result["attacks"] = attacks(overview)
    result["totals"] = {
        "requests": integer(totals.get("requests")),
        "unique_ips": integer(totals.get("unique_ips")),
        "client_errors": integer(totals.get("client_errors")),
        "server_errors": integer(totals.get("server_errors")),
        "denied": integer(totals.get("denied")),
        "rate_limited": integer(totals.get("rate_limited")),
        "waf_blocks": integer(waf_total.get("n")),
        "flagged_ips": sum(1 for ip in ips if ip["flags"]),
        "flagged_endpoints": sum(1 for ep in endpoints if ep["flags"]),
        "attack_requests": sum(cat["requests"] for cat in result["attacks"]),
        "attack_answered": sum(cat["answered"] for cat in result["attacks"]),
    }
    return result
