"""Deterministic deployment gate and temporary bridge to PulsarCD's configured LLM."""
import asyncio
import base64
from bisect import bisect_left
from contextlib import AsyncExitStack
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import sys
from urllib.parse import urlsplit
import zipfile

import aiohttp
from aiohttp import web

from .config_file import load_config_file
from .pipeline_state import PipelineStateManager
from .swiftproof_worker import save

_locks = {}


def deployment_lock(repo):
    return _locks.setdefault(repo, asyncio.Lock())


def state():
    return PipelineStateManager.get_instance()


def config(repo):
    # SwiftProof belongs to the Test stage: it runs once the automated suite
    # passes, and its verdict is part of that stage's outcome. Deploying to QA
    # or production is only a deployment.
    return state().get_transition_config(repo, "build_to_test")


def enabled(repo):
    return config(repo).get("swiftproof_enabled", False) is True


def storage():
    return state()._path.parent / "swiftproof"


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def public_result(result):
    return {k: result[k] for k in ("id", "status", "code", "reason", "head", "base", "release", "approval", "tool_version") if k in result}


def remember(repo, result):
    state().get_or_create(repo).swiftproof = public_result(result)
    state()._save()


class ProviderBridge:
    """One job, one upstream, one model; no infrastructure/MCP tools or redirects."""
    def __init__(self, llm):
        self.llm = llm
        self.token = secrets.token_urlsafe(32)
        self.calls = 0

    async def __aenter__(self):
        url = self.llm.url.rstrip("/")
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
            raise ValueError("Invalid PulsarCD LLM endpoint")
        self.url = url if url.endswith("/chat/completions") else url + ("/chat/completions" if url.endswith("/v1") else "/v1/chat/completions")
        self.session = aiohttp.ClientSession(trust_env=False, timeout=aiohttp.ClientTimeout(total=600))
        app = web.Application(client_max_size=131072)
        app.router.add_post("/v1/chat/completions", self.complete)
        self.runner = web.AppRunner(app, access_log=None)
        try:
            await self.runner.setup()
            site = web.TCPSite(self.runner, "127.0.0.1", 0)
            await site.start()
            self.port = self.runner.addresses[0][1]
            return self
        except BaseException:
            await self.runner.cleanup()
            await self.session.close()
            raise

    async def __aexit__(self, *exc):
        await self.runner.cleanup()
        await self.session.close()

    async def complete(self, request):
        if not hmac.compare_digest(request.headers.get("Authorization", ""), "Bearer " + self.token):
            raise web.HTTPUnauthorized()
        if self.calls >= 20:
            raise web.HTTPTooManyRequests()
        self.calls += 1
        try:
            body = await request.json()
            if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
                raise web.HTTPBadRequest()
            # Forward only the Chat Completions fields used by SwiftProof.
            body = {k: body[k] for k in ("messages", "tools", "parallel_tool_calls") if k in body}
            body.update(model=self.llm.model, max_completion_tokens=min(self.llm.max_output_tokens, 4096), stream=False)
            headers = {"Authorization": "Bearer " + self.llm.api_key} if self.llm.api_key else {}
            async with self.session.post(self.url, json=body, headers=headers, allow_redirects=False) as response:
                if response.status != 200:
                    return web.json_response({"error": "PulsarCD provider request failed"}, status=502)
                data = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    data.extend(chunk)
                    if len(data) > 1024 * 1024:
                        raise web.HTTPBadGateway()
                return web.Response(body=data, content_type="application/json")
        except web.HTTPException:
            raise
        except Exception:
            return web.json_response({"error": "PulsarCD provider unavailable"}, status=502)


async def worker(deployer, payload, llm=None, cancel_event=None):
    """Use encrypted SSH stdin, never shell arguments, for temporary job tokens."""
    source = Path(__file__).with_name("swiftproof_worker.py").read_text(encoding="utf-8")
    ssh = await deployer._get_ssh_client()
    async with AsyncExitStack() as stack:
        connection = await ssh.connect() if ssh else None
        if llm:
            bridge = await stack.enter_async_context(ProviderBridge(llm))
            port = bridge.port
            if connection:
                listener = await connection.forward_remote_port("127.0.0.1", 0, "127.0.0.1", port)
                if listener is None:
                    raise ValueError("SSH reverse forwarding is required to reach the PulsarCD LLM")
                stack.callback(listener.close)
                port = listener.get_port()
            payload = dict(payload, provider={"endpoint": f"http://127.0.0.1:{port}/v1", "model": llm.model, "token": bridge.token})
        data = json.dumps(payload)

        async def exchange():
            if connection:
                async with connection.create_process("python3 -c " + shlex.quote(source), encoding="utf-8") as process:
                    try:
                        stdout, _ = await process.communicate(data)
                        return stdout
                    except BaseException:
                        process.terminate()
                        raise
            if deployer.host_client:
                raise ValueError("SwiftProof requires SSH configuration for a remote deployment host")
            process = await asyncio.create_subprocess_exec(sys.executable, "-c", source,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                stdout, _ = await process.communicate(data.encode())
                return stdout.decode()
            except BaseException:
                process.kill()
                await process.wait()
                raise

        task = asyncio.create_task(exchange())
        cancellation = asyncio.create_task(cancel_event.wait()) if cancel_event else None
        try:
            done, _ = await asyncio.wait([task] + ([cancellation] if cancellation else []), timeout=1900, return_when=asyncio.FIRST_COMPLETED)
            if task not in done:
                raise ValueError("SwiftProof cancelled or timed out")
            output = await task
            if len(output) > 12 * 1024 * 1024:
                raise ValueError("SwiftProof worker response exceeded its budget")
            response = json.loads(output)
            if not response.get("ok"):
                raise ValueError(response.get("error", "SwiftProof worker failed"))
            return response["result"]
        finally:
            for pending in (task, cancellation):
                if pending and not pending.done():
                    pending.cancel()
            await asyncio.gather(*[p for p in (task, cancellation) if p], return_exceptions=True)


def request_for(deployer, repo, release):
    release = release.removeprefix("v")
    return {"repo": repo, "release": release, "repos_path": deployer.config.repos_path,
            "stack": deployer._repo_to_stack_name(repo),
            "binary": os.environ.get("PULSARCD_SWIFTPROOF_BINARY", "/usr/local/bin/swiftproof"),
            "initial_baseline": config(repo).get("swiftproof_initial_baseline", "")}


def report_file(review_id, name):
    if not re.fullmatch(r"[0-9a-f]{64}", review_id) or name not in ("report.zip", "result.json"):
        raise ValueError("Invalid report path")
    return storage() / review_id / name


def read_result(review_id):
    result = json.loads(report_file(review_id, "result.json").read_text(encoding="utf-8"))
    if hashlib.sha256(report_file(review_id, "report.zip").read_bytes()).hexdigest() != result["archive_sha256"]:
        raise ValueError("Stored report integrity check failed")
    return result


def failure_detail(report):
    """Name what broke, using the recorded report rather than raw command stderr.

    A code 3 or 4 means the run never reached a verdict; the report already
    holds the reason, so the administrator should not have to open the archive
    to read it.
    """
    for check in report.get("checks", []):
        if check.get("status") != "ERROR":
            continue
        detail = str(check.get("kind") or check.get("id") or "check") + " failed"
        if type(check.get("exit_code")) is int:
            detail += " (exit " + str(check["exit_code"]) + ")"
        output = " ".join(str(check.get("output", "")).split())[:300]
        return detail + (": " + output if output else "")
    for note in report.get("unverified", []):
        if isinstance(note, str) and note.strip():
            return " ".join(note.split())[:300]
    return ""


def report_findings(report):
    """Index recorded v1 observations for navigation without re-evaluating risk.

    SwiftProof's report, including its evidence and verdict, stays authoritative.
    Signals and review targets have no hypothesis status; keep that field empty
    rather than presenting them as reproduced defects.
    """
    def objects(key):
        value = report.get(key)
        return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []

    def strings(value):
        return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []

    def string(value):
        return value if isinstance(value, str) else ""

    def line(value):
        return value if type(value) is int and value > 0 else 0

    evidence = {item["id"]: item for item in objects("evidence") if isinstance(item.get("id"), str)}
    findings = []

    def add(kind, source, title, status="", details=None):
        start = line(source.get("line", source.get("start_line")))
        finding = {
            "id": f"{kind}-{len(findings) + 1}", "source_id": string(source.get("id")),
            "kind": kind, "title": title, "severity": string(source.get("severity")),
            "status": status, "path": string(source.get("path")), "line": start,
            "end_line": max(start, line(source.get("end_line"))),
            "side": "old" if source.get("side") == "old" else "new",
            "evidence": details or [],
        }
        findings.append(finding)

    seen_hypotheses = set()
    for hypothesis in objects("hypotheses") + objects("reproduced_issues"):
        source_id = string(hypothesis.get("id"))
        if source_id and source_id in seen_hypotheses:
            continue
        if source_id:
            seen_hypotheses.add(source_id)
        details = [evidence[key] for key in strings(hypothesis.get("evidence_ids")) if key in evidence]
        rationale = string(hypothesis.get("rationale"))
        if rationale:
            details = [{"description": rationale}] + details
        add("hypothesis", hypothesis, string(hypothesis.get("title")),
            string(hypothesis.get("status")), details)

    for signal in objects("linter"):
        detail = string(signal.get("evidence"))
        add("signal", signal, string(signal.get("summary")), details=[{"description": detail}] if detail else [])

    locations = {}
    for finding in findings:
        key = (finding["path"], finding["side"], finding["title"])
        locations.setdefault(key, []).append(finding["line"])
    for coordinates in locations.values():
        coordinates.sort()

    for target in objects("review_targets"):
        reasons = strings(target.get("reasons"))
        start, end = line(target.get("start_line")), line(target.get("end_line"))
        side = "old" if target.get("side") == "old" else "new"
        def represented(reason):
            coordinates = locations.get((string(target.get("path")), side, reason), [])
            offset = bisect_left(coordinates, start)
            return offset < len(coordinates) and (not start or coordinates[offset] <= max(start, end))
        # Review targets aggregate signals/hypotheses already indexed above.
        # Retain standalone targets, including historical reports with no signals.
        if reasons and all(represented(reason) for reason in reasons):
            continue
        add("review_target", target, "; ".join(reasons) or string(target.get("path")))

    for area in strings(report.get("unverified")):
        if area:
            add("unverified", {}, area, "UNVERIFIED")

    rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return sorted(findings, key=lambda finding: (
        finding["status"] != "REPRODUCED",
        finding["status"] in ("DISMISSED", "NOT_REPRODUCED"),
        rank.get(finding["severity"].lower(), 4),
    ))


def structured_report(review_id, result):
    """Read a verified archive without extracting or executing its artifacts."""
    with zipfile.ZipFile(report_file(review_id, "report.zip")) as bundle:
        if sum(info.file_size for info in bundle.infolist()) > 8 * 1024 * 1024:
            raise ValueError("Expanded report exceeded its budget")
        report = json.loads(bundle.read("confidence-report.json"))
        if not isinstance(report, dict):
            raise ValueError("Invalid report format")
        markdown = bundle.read("CONFIDENCE_REPORT.md").decode("utf-8")
    return {"result": public_result(result), "report": report, "markdown": markdown,
            "findings": report_findings(report)}


async def review(deployer, repo, release, cancel_event=None):
    if not enabled(repo):
        return {"status": "disabled", "reason": "SwiftProof is disabled for this project"}
    remember(repo, {"status": "running", "reason": "Reviewing deployment commits and image provenance"})
    try:
        request = request_for(deployer, repo, release)
        identity = await worker(deployer, dict(request, action="inspect"), cancel_event=cancel_event)
        llm = load_config_file(str(state()._path.parent)).llm if config(repo).get("swiftproof_reviewer", True) else None
        if llm and (not llm.url or not llm.model):
            llm = None
        provider_id = {"url": llm.url, "model": llm.model, "max_output_tokens": llm.max_output_tokens} if llm else None
        review_id = fingerprint({"identity": identity, "provider": provider_id,
                                 "revision": state().get_or_create(repo).swiftproof_revision})
        result = None
        if report_file(review_id, "result.json").parent.exists():
            try:
                result = read_result(review_id)
                if result.get("identity") != identity or result.get("id") != review_id:
                    raise ValueError("Cached report identity mismatch")
            except (OSError, ValueError, KeyError):
                # Lost/corrupt evidence must be regenerated under a new ID;
                # an approval of the previous archive must never carry over.
                result = None
                entry = state().get_or_create(repo)
                entry.swiftproof_revision += 1
                state()._save()
                review_id = fingerprint({"identity": identity, "provider": provider_id,
                                         "revision": entry.swiftproof_revision})
        if result is None:
            reply = await worker(deployer, dict(request, action="review", identity=identity), llm, cancel_event)
            if reply.get("identity") != identity or type(reply.get("code")) is not int or reply["code"] not in range(5):
                raise ValueError("Invalid SwiftProof worker result")
            archive = base64.b64decode(reply["archive"], validate=True)
            if len(archive) > 8 * 1024 * 1024:
                raise ValueError("Report archive exceeded its budget")
            with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
                if sum(info.file_size for info in bundle.infolist()) > 8 * 1024 * 1024:
                    raise ValueError("Expanded report exceeded its budget")
                report = json.loads(bundle.read("confidence-report.json"))
                if (report["change"]["head_commit"] != identity["head"] or report["change"]["base_commit"] != identity["base"]
                        or report["exit_code"] != reply["code"]):
                    raise ValueError("Archived report comparison mismatch")
            code = reply["code"]
            status, reason = {
                0: ("passed", "SwiftProof completed without a blocking finding"),
                1: ("blocked", "SwiftProof reproduced a high or critical issue"),
                2: ("needs_review", "Human review required: inspect the evidence before approving"),
                3: ("error", "SwiftProof configuration or comparison failed"),
                4: ("error", "SwiftProof execution failed"),
            }[code]
            if code in (3, 4):
                detail = failure_detail(report)
                reason += " — " + detail if detail else ""
            result = dict(id=review_id, status=status, reason=reason, code=code, identity=identity,
                          head=identity["head"], base=identity["base"], release=identity["release"],
                          archive_sha256=hashlib.sha256(archive).hexdigest(), tool_version=reply["tool_version"])
            path = report_file(review_id, "report.zip")
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.write_bytes(archive)
            save(report_file(review_id, "result.json"), result)
        remember(repo, result)
        return result
    except asyncio.CancelledError:
        remember(repo, {"status": "error", "code": 4, "reason": "SwiftProof review cancelled"})
        raise
    except Exception as error:
        # Worker errors are deliberately sanitized; do not surface transport
        # exceptions which can contain connection strings or provider details.
        reason = str(error) if isinstance(error, ValueError) else "SwiftProof unavailable: " + type(error).__name__
        result = {"status": "error", "code": 4, "reason": reason}
        remember(repo, result)
        return result


def approve(repo, review_id, user, reason):
    current = state().get_or_create(repo).swiftproof
    result = read_result(review_id)
    if not enabled(repo) or current.get("id") != review_id or result["identity"]["repo"] != repo or result["status"] != "needs_review" or result["code"] != 2:
        raise ValueError("Only the current code-2 report can be approved")
    if not user or not isinstance(reason, str) or not reason.strip() or len(reason) > 2000:
        raise ValueError("A named administrator and a reason are required")
    from datetime import datetime, timezone
    result.update(status="approved", approval={"user": user, "reason": reason.strip(), "at": datetime.now(timezone.utc).isoformat()})
    save(report_file(review_id, "result.json"), result)
    remember(repo, result)
    return public_result(result)

