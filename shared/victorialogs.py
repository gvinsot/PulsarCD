"""VictoriaLogs client and LogsQL helpers shared by the backend and the agents.

Everything PulsarCD stores -- container logs, container and host metrics,
Traefik access records, pipeline runs and their logs -- lives in one
VictoriaLogs instance. Each record carries a ``kind`` stream field, so every
query starts with a ``{kind="..."}`` stream filter and only reads its own data.

VictoriaLogs is append-only and keeps every value as a string: numbers come
back as ``"12.5"``, missing averages as ``"NaN"``. Use :func:`num` and
:func:`integer` to read them.

Query text built from user input must go through :func:`quote` (values) and
:func:`field` (names); :func:`text_filter` turns a free-text search into a
bounded LogsQL expression.
"""

import json
import math
import re
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp

# --- Record kinds and their stream fields -----------------------------------
# Stream fields identify where a record comes from and never change for that
# source: VictoriaLogs groups records by them, so a {kind="log", host="x"}
# filter only reads that host's logs. Per-record values (ids, statuses, IPs)
# must never be stream fields.
KIND_LOG = "log"
KIND_CONTAINER_METRIC = "container_metric"
KIND_HOST_METRIC = "host_metric"
KIND_ACCESS = "access"
KIND_PIPELINE_RUN = "pipeline_run"
KIND_PIPELINE_RUN_LOG = "pipeline_run_log"
KIND_SELFTEST = "selftest"   # connectivity probes, never mixed with real data

STREAM_FIELDS: Dict[str, Tuple[str, ...]] = {
    KIND_LOG: ("kind", "host", "container_name"),
    KIND_CONTAINER_METRIC: ("kind", "host", "container_name"),
    KIND_HOST_METRIC: ("kind", "host"),
    KIND_ACCESS: ("kind", "host"),
    KIND_PIPELINE_RUN: ("kind", "repo"),
    KIND_PIPELINE_RUN_LOG: ("kind", "repo"),
    KIND_SELFTEST: ("kind", "host"),
}

DEFAULT_TIMEOUT = 30.0
MAX_TEXT_QUERY_LENGTH = 2048
MAX_BUCKETS = 2000

_INTERVAL_RE = re.compile(r"^(\d{1,4})(ms|s|m|h|d)$")
_INTERVAL_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}

_FIELD_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


class VictoriaLogsError(Exception):
    """A VictoriaLogs request failed (unreachable, bad query, timeout)."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


# --- LogsQL building blocks -------------------------------------------------

def quote(value: Any) -> str:
    """A LogsQL string literal for ``value``.

    JSON string escaping is valid inside a double-quoted LogsQL string (it is
    decoded as a Go string literal), so no input can break out of it.
    """
    return json.dumps(str(value), ensure_ascii=False)


def field(name: str) -> str:
    """A field name, quoted unless it is a plain identifier."""
    return name if _FIELD_NAME_RE.match(name) else quote(name)


def stream(kind: str, **labels: Any) -> str:
    """``{kind="..."[, label="..."]}`` stream filter."""
    parts = [f"kind={quote(kind)}"]
    for key, value in labels.items():
        if value is not None:
            parts.append(f"{field(key)}={quote(value)}")
    return "{" + ", ".join(parts) + "}"


def exact(name: str, value: Any) -> str:
    return f"{field(name)}:={quote(value)}"


def any_of(name: str, values: Iterable[Any]) -> str:
    """``name:in(...)``; an empty list matches nothing."""
    return f"{field(name)}:in({', '.join(quote(v) for v in values)})"


def rfc3339(dt: datetime) -> str:
    """UTC RFC 3339 timestamp; naive datetimes are taken as UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def iso_ms(dt: datetime) -> str:
    """UTC timestamp with milliseconds, the format the UI charts expect."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def interval_seconds(interval: str) -> Optional[float]:
    """Seconds in a "<number><unit>" interval (ms, s, m, h, d), or None."""
    match = _INTERVAL_RE.match(interval.strip()) if isinstance(interval, str) else None
    return int(match.group(1)) * _INTERVAL_UNIT_SECONDS[match.group(2)] if match else None


def bucket_times(start: datetime, end: datetime, interval: str) -> List[datetime]:
    """Bucket starts of ``stats by (_time:interval)`` between start and end.

    VictoriaLogs aligns buckets on the interval since the Unix epoch, in UTC.
    """
    step = interval_seconds(interval) or 3600.0
    current = int(start.timestamp() // step) * step
    out: List[datetime] = []
    while current < end.timestamp() and len(out) <= MAX_BUCKETS:
        out.append(datetime.fromtimestamp(current, tz=timezone.utc))
        current += step
    return out


def parse_time(value: Any) -> Optional[datetime]:
    """Aware UTC datetime from a VictoriaLogs ``_time`` string (or None)."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Python < 3.11 rejects more than 6 fractional digits; VictoriaLogs
    # returns nanoseconds.
    match = re.match(r"^(.*\.\d{6})\d+(.*)$", text)
    if match:
        text = match.group(1) + match.group(2)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def num(value: Any) -> Optional[float]:
    """A float from a returned value, None for missing / NaN / non-numeric."""
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(result) or math.isinf(result) else result


def integer(value: Any, default: int = 0) -> int:
    result = num(value)
    return int(result) if result is not None else default


# --- Free-text search ---------------------------------------------------------

_TOKEN_RE = re.compile(r'"((?:[^"\\]|\\.)*)"(\*?)|(\()|(\))|([^\s()"]+)')
_OPERATORS = {"AND": "AND", "OR": "OR", "NOT": "NOT", "&&": "AND", "||": "OR", "|": "OR", "+": None}


def _term(name: str, text: str, prefix: bool) -> Optional[str]:
    if not text:
        return None
    return f"{name}:i({quote(text)}{'*' if prefix else ''})"


def _tokens(text: str, name: str) -> List[str]:
    """Search text as a flat list of terms, operators and parentheses."""
    out: List[str] = []
    for match in _TOKEN_RE.finditer(text):
        phrase, phrase_star, lparen, rparen, word = match.groups()
        if phrase is not None:
            literal = re.sub(r'\\(.)', r'\1', phrase)
            term = _term(name, literal, bool(phrase_star))
            if term:
                out.append(term)
        elif lparen:
            out.append("(")
        elif rparen:
            out.append(")")
        elif word in _OPERATORS:
            if _OPERATORS[word]:
                out.append(_OPERATORS[word])
        else:
            negate = False
            while word[:1] in ("-", "+", "!"):
                negate = negate or word[0] in ("-", "!")
                word = word[1:]
            prefix = word.endswith("*")
            word = word.rstrip("*")
            term = _term(name, word, prefix)
            if term:
                if negate:
                    out.append("NOT")
                out.append(term)
    return out


def text_filter(text: Optional[str], name: str = "_msg", default_op: str = "OR") -> Optional[str]:
    """Translate a user search into a case-insensitive LogsQL filter on ``name``.

    Supports words, "quoted phrases", a trailing ``*`` (prefix), AND / OR / NOT
    (also ``-word``) and parentheses -- the syntax the log search always
    offered. Every term is passed as a quoted literal, so field:value,
    regexps and pipes typed by the user are searched as text and can never
    change the query. Returns None for an empty search.
    """
    if not text:
        return None
    text = str(text).strip()[:MAX_TEXT_QUERY_LENGTH]
    tokens = _tokens(text, field(name))
    # Unbalanced parentheses: drop them all rather than reject the search.
    depth = 0
    balanced = True
    for token in tokens:
        depth += token == "("
        depth -= token == ")"
        if depth < 0:
            balanced = False
            break
    if not balanced or depth:
        tokens = [t for t in tokens if t not in ("(", ")")]

    out: List[str] = []
    for token in tokens:
        operand_start = token not in ("AND", "OR", ")")
        prev = out[-1] if out else None
        if token in ("AND", "OR"):
            if prev is None or prev in ("AND", "OR", "NOT", "("):
                continue  # dangling binary operator
            out.append(token)
            continue
        if token == ")":
            while out and out[-1] in ("AND", "OR", "NOT"):
                out.pop()
            if out and out[-1] == "(":
                out.pop()  # empty group
                continue
            out.append(token)
            continue
        if operand_start and prev is not None and prev not in ("AND", "OR", "NOT", "("):
            # "error -timeout" means error without timeout, whatever the
            # default operator: an OR there would match almost everything.
            out.append("AND" if token == "NOT" else default_op)
        out.append(token)
    while out and out[-1] in ("AND", "OR", "NOT", "("):
        out.pop()
    expression = " ".join(out).strip()
    return f"({expression})" if expression else None


# --- Record builders ----------------------------------------------------------

def _clean(row: Dict[str, Any]) -> Dict[str, Any]:
    """Drop empty values (VictoriaLogs treats them as missing anyway)."""
    out = {}
    for key, value in row.items():
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            value = "true" if value else "false"
        elif isinstance(value, datetime):
            value = rfc3339(value)
        out[key] = value
    return out


def _time_of(entry: Dict[str, Any]) -> str:
    stamp = entry.get("timestamp")
    if isinstance(stamp, datetime):
        return rfc3339(stamp)
    parsed = parse_time(stamp)
    return rfc3339(parsed) if parsed else rfc3339(datetime.now(timezone.utc))


def log_row(entry: Dict[str, Any]) -> Dict[str, Any]:
    """A container log line. parsed_fields is kept whole, as a JSON string."""
    parsed_fields = entry.get("parsed_fields")
    return _clean({
        "_time": _time_of(entry),
        "_msg": entry.get("message") or "",
        "kind": KIND_LOG,
        "host": entry.get("host"),
        "container_id": entry.get("container_id"),
        "container_name": entry.get("container_name"),
        "compose_project": entry.get("compose_project"),
        "compose_service": entry.get("compose_service"),
        "stream": entry.get("stream"),
        "level": entry.get("level"),
        "http_status": entry.get("http_status"),
        "parsed_fields": json.dumps(parsed_fields, default=str) if parsed_fields else None,
    })


def metric_row(kind: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """A container or host metrics sample; every metric is a field."""
    row = {key: value for key, value in metrics.items() if key != "timestamp"}
    row.update({
        "_time": _time_of(metrics),
        # A record needs a message; one per source keeps them compressible.
        "_msg": metrics.get("container_name") or metrics.get("host") or kind,
        "kind": kind,
    })
    return _clean(row)


def access_row(doc: Dict[str, Any]) -> Dict[str, Any]:
    """A Traefik request or WAF block (shared.access_log.build_access_doc)."""
    row = {key: value for key, value in doc.items() if key != "timestamp"}
    summary = " ".join(str(part) for part in (
        doc.get("method") or doc.get("event"), doc.get("request_host") or "",
        doc.get("path") or "", doc.get("status") or "") if part)
    row.update({"_time": _time_of(doc), "_msg": summary or doc.get("event"), "kind": KIND_ACCESS})
    return _clean(row)


# --- HTTP client --------------------------------------------------------------

def _encode_params(params: Dict[str, Any]) -> List[Tuple[str, str]]:
    out = []
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            value = rfc3339(value)
        elif isinstance(value, (dict, list)):
            value = json.dumps(value)
        out.append((key, str(value)))
    return out


class VictoriaLogsClient:
    """Minimal async client for the VictoriaLogs HTTP API."""

    def __init__(self, url: str, *, timeout: float = DEFAULT_TIMEOUT,
                 username: Optional[str] = None, password: Optional[str] = None):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self._auth = aiohttp.BasicAuth(username, password or "") if username else None
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(auth=self._auth)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _post(self, path: str, *, data: Any = None, params: Optional[Dict[str, Any]] = None,
                    headers: Optional[Dict[str, str]] = None, timeout: Optional[float] = None):
        session = await self._get_session()
        try:
            return await session.post(
                f"{self.url}{path}", data=data, params=_encode_params(params or {}),
                headers=headers, timeout=aiohttp.ClientTimeout(total=timeout or self.timeout))
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise VictoriaLogsError(f"VictoriaLogs unreachable: {type(exc).__name__}: {exc}") from exc

    @staticmethod
    async def _check(response: aiohttp.ClientResponse) -> None:
        if response.status != 200:
            body = (await response.text())[:500]
            response.release()
            raise VictoriaLogsError(f"VictoriaLogs returned {response.status}: {body}",
                                    status=response.status)

    async def health(self) -> bool:
        session = await self._get_session()
        try:
            async with session.get(f"{self.url}/health",
                                   timeout=aiohttp.ClientTimeout(total=5)) as response:
                return response.status == 200
        except (aiohttp.ClientError, TimeoutError):
            return False

    async def insert(self, rows: Sequence[Dict[str, Any]], kind: str) -> int:
        """Append ``rows`` of one kind. Returns the number of rows sent.

        The content type matters: VictoriaLogs answers 200 to a JSON-lines
        body sent as form data but stores none of it.
        """
        if not rows:
            return 0
        body = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows) + "\n"
        response = await self._post(
            "/insert/jsonline", data=body.encode("utf-8"),
            params={"_stream_fields": ",".join(STREAM_FIELDS[kind])},
            headers={"Content-Type": "application/stream+json"})
        await self._check(response)
        response.release()
        return len(rows)

    async def iter_query(self, query: str, *, start: Optional[datetime] = None,
                         end: Optional[datetime] = None, limit: Optional[int] = None,
                         timeout: Optional[float] = None,
                         extra_stream_filters: Optional[Dict[str, Any]] = None,
                         ) -> AsyncIterator[Dict[str, Any]]:
        """Stream the rows of a LogsQL query, one dict per row.

        ``limit`` (query arg) returns the most recent rows; ``end`` is
        exclusive. Closing the iterator early stops the query server-side.
        """
        seconds = timeout or self.timeout
        response = await self._post("/select/logsql/query", data={"query": query}, params={
            "start": start, "end": end, "limit": limit, "timeout": f"{seconds:g}s",
            "extra_stream_filters": extra_stream_filters,
        }, timeout=seconds + 5)
        try:
            await self._check(response)
            # Split lines ourselves: StreamReader's line iteration refuses
            # lines longer than its buffer, and a run-log chunk can be.
            pending = b""
            async for chunk in response.content.iter_chunked(1 << 16):
                pending += chunk
                *lines, pending = pending.split(b"\n")
                for line in lines:
                    if line.strip():
                        yield json.loads(line)
            if pending.strip():
                yield json.loads(pending)
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise VictoriaLogsError(f"VictoriaLogs query interrupted: {exc}") from exc
        finally:
            response.release()

    async def query(self, query: str, **kwargs: Any) -> List[Dict[str, Any]]:
        """All the rows of a LogsQL query (see iter_query for the arguments)."""
        return [row async for row in self.iter_query(query, **kwargs)]

    async def probe(self, host: str) -> Dict[str, Any]:
        """Write a self-test record, make it searchable and read it back."""
        marker = f"probe-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
        result: Dict[str, Any] = {"marker": marker}
        try:
            await self.insert([{"_time": rfc3339(datetime.now(timezone.utc)), "_msg": marker,
                                "kind": KIND_SELFTEST, "host": host}], KIND_SELFTEST)
            result["write"] = "ok"
            await self.flush()
            rows = await self.query(f"{stream(KIND_SELFTEST, host=host)} {exact('_msg', marker)} "
                                    "| stats count() as n")
            result["read"] = "ok" if rows and integer(rows[0].get("n")) == 1 else "missing"
        except VictoriaLogsError as exc:
            result["error"] = str(exc)[:300]
        result["ok"] = result.get("read") == "ok"
        return result

    async def flush(self) -> None:
        """Make the rows inserted so far searchable now (tests, self-test)."""
        response = await self._post("/internal/force_flush")
        await self._check(response)
        response.release()
