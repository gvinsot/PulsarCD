"""Traefik access-log and Coraza WAF parsing for the security view.

Traefik writes one JSON object per request (accessLog.format: json) and the
Coraza WAF plugin writes one text line per blocked request. The logs index
keeps the JSON in a disabled object, so it cannot answer "which IP sent the
most requests" or "which endpoint is being hammered". Each such line is
therefore also turned into a flat document for the access index, where the
client IP, host and path are fields that can be aggregated.

Shared by the agent (which writes the documents) and the backend (which
creates the index and queries it), so both use the same mapping.
"""

import ipaddress
import re
from typing import Any, Dict, Optional

MAX_PATH_LENGTH = 256
MAX_URI_LENGTH = 1024
MAX_HOST_LENGTH = 255
MAX_USER_AGENT_LENGTH = 256
MAX_RULE_MSG_LENGTH = 256

EVENT_REQUEST = "request"
EVENT_WAF_BLOCK = "waf_block"

ACCESS_INDEX_MAPPING = {
    "mappings": {
        # Unknown fields are kept in _source but never mapped: a dynamically
        # mapped field would later fail _verify_mapping, which recreates the
        # index (and drops its data) on any type mismatch.
        "dynamic": False,
        "properties": {
            "timestamp": {"type": "date"},
            "host": {"type": "keyword"},
            "event": {"type": "keyword"},
            "client_ip": {"type": "ip"},
            "internal": {"type": "boolean"},
            "request_host": {"type": "keyword"},
            "method": {"type": "keyword"},
            "path": {"type": "keyword"},
            # Full request URI with its query string, for the per-IP detail
            # only: stored, never searched or aggregated.
            "uri": {"type": "keyword", "index": False, "doc_values": False},
            "status": {"type": "integer"},
            "router": {"type": "keyword"},
            "entrypoint": {"type": "keyword"},
            "duration_ms": {"type": "float"},
            "size": {"type": "long"},
            "user_agent": {"type": "keyword"},
            "rule_id": {"type": "keyword"},
            "rule_msg": {"type": "keyword"},
        },
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.refresh_interval": "5s",
    },
}

# Keys every Traefik JSON access-log line carries; together they are specific
# enough that no other container's JSON logs are mistaken for one.
_TRAEFIK_KEYS = ("ClientHost", "DownstreamStatus", "RequestPath")

# Traefik writes the headers kept by accessLog.fields.headers as request_<Name>.
_USER_AGENT_KEY = "request_User-Agent"

_CORAZA_MARKER = "Coraza: Access denied"
_CORAZA_CLIENT_RE = re.compile(r'\[client "([^"]+)"\]')
_CORAZA_TAG_RE = re.compile(r'\[(id|msg|uri) "((?:[^"\\]|\\.)*)"\]')
_ROUTER_NAME_RE = re.compile(r'\brouterName=(\S+)')


def normalize_ip(value: Any) -> Optional[str]:
    """Canonical form of an IP address, or None if value is not one."""
    try:
        return str(ipaddress.ip_address(str(value).strip()))
    except ValueError:
        return None


def is_internal_ip(ip: str) -> bool:
    """Whether an address belongs to a private, loopback or link-local range.

    Internal traffic (CI pushing to a registry, nodes pulling images, clients
    coming back through the router's NAT loopback) dominates the request
    counts, so the security view hides it by default.
    """
    addr = ipaddress.ip_address(ip)
    return addr.is_private or addr.is_loopback or addr.is_link_local


def _text(value: Any, max_length: int) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    return text[:max_length]


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _path(uri: str) -> str:
    """The request path without its query string, as the endpoint key."""
    return uri.split("?", 1)[0][:MAX_PATH_LENGTH] or "/"


def _base_doc(entry: Dict[str, Any], event: str, ip: str) -> Dict[str, Any]:
    return {
        "timestamp": entry.get("timestamp"),
        "host": entry.get("host"),
        "event": event,
        "client_ip": ip,
        "internal": is_internal_ip(ip),
    }


def _from_traefik(entry: Dict[str, Any], fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ip = normalize_ip(fields.get("ClientHost"))
    if ip is None:
        return None
    uri = str(fields.get("RequestPath") or "")
    duration_ns = _int(fields.get("Duration"))
    doc = _base_doc(entry, EVENT_REQUEST, ip)
    doc.update({
        # A request without a Host header is still an endpoint worth counting.
        "request_host": _text(fields.get("RequestHost"), MAX_HOST_LENGTH) or "-",
        "method": _text(fields.get("RequestMethod"), 16),
        "path": _path(uri),
        "uri": uri[:MAX_URI_LENGTH],
        "status": _int(fields.get("DownstreamStatus")),
        # Absent when no router matched the host: the request got a 404
        # from Traefik itself.
        "router": _text(fields.get("RouterName"), MAX_HOST_LENGTH),
        "entrypoint": _text(fields.get("entryPointName"), 64),
        "duration_ms": round(duration_ns / 1e6, 3) if duration_ns is not None else None,
        "size": _int(fields.get("DownstreamContentSize")),
        "user_agent": _text(fields.get(_USER_AGENT_KEY), MAX_USER_AGENT_LENGTH),
    })
    return doc


def _from_coraza(entry: Dict[str, Any], message: str) -> Optional[Dict[str, Any]]:
    client = _CORAZA_CLIENT_RE.search(message)
    ip = normalize_ip(client.group(1)) if client else None
    if ip is None:
        return None
    tags = dict(_CORAZA_TAG_RE.findall(message))
    uri = tags.get("uri", "")
    router = _ROUTER_NAME_RE.search(message)
    doc = _base_doc(entry, EVENT_WAF_BLOCK, ip)
    doc.update({
        "path": _path(uri),
        "uri": uri[:MAX_URI_LENGTH],
        "router": router.group(1)[:MAX_HOST_LENGTH] if router else None,
        "rule_id": _text(tags.get("id"), 32),
        "rule_msg": _text(tags.get("msg"), MAX_RULE_MSG_LENGTH),
    })
    return doc


def build_access_doc(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Access-index document for a collected log entry.

    Returns None unless the entry is a Traefik access-log line or a Coraza
    denial, or when its client address cannot be parsed.
    """
    fields = entry.get("parsed_fields") or {}
    if isinstance(fields, dict) and all(key in fields for key in _TRAEFIK_KEYS):
        return _from_traefik(entry, fields)
    message = entry.get("message") or ""
    if _CORAZA_MARKER in message:
        return _from_coraza(entry, message)
    return None
