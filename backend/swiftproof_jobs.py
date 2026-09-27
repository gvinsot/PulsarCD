"""On-demand SwiftProof plans and proofs, outside the CI/CD pipeline.

A job runs the SwiftProof binary on the deployment host through the same
worker and LLM bridge as the Test-stage review, but it never builds, tags or
deploys anything and never changes the pipeline gate (``PipelineEntry.swiftproof``).

- A **plan** asks the configured LLM how it would implement an intent,
  read-only, and lets SwiftProof assess it with fixed rules (``PLAN.json``).
- A **proof** compares any commit the host checkout knows (default: the
  default branch tip) with what production runs, and keeps the evidence
  archive. Given a plan, it also reports the scope drift against it.

Jobs are asynchronous because a proof may take up to 30 minutes: callers get
an id and poll. Results are persisted under ``<data_dir>/swiftproof/jobs/``.
"""
import asyncio
import base64
from datetime import datetime, timezone
import hashlib
import io
import json
import re
import uuid
import zipfile

from . import swiftproof
from .config_file import load_config_file
from .swiftproof_worker import save

MARKDOWN_LIMIT = 60000
PLAN_LIMIT = 1024 * 1024
_ID = re.compile(r"[0-9a-f]{32}")
_tasks = {}
_cancels = {}
_locks = {}

PROOF_STATUS = {
    0: ("passed", "SwiftProof completed without a blocking finding"),
    1: ("blocked", "SwiftProof reproduced a high or critical issue"),
    2: ("needs_review", "Human review required: inspect the evidence"),
    3: ("error", "SwiftProof configuration or comparison failed"),
    4: ("error", "SwiftProof execution failed"),
}
PLAN_STATUS = {
    0: ("ok", "No plan category is flagged"),
    2: ("flagged", "A plan category is flagged or something is unverified: discuss it before the work starts"),
    3: ("error", "SwiftProof plan configuration failed"),
    4: ("error", "SwiftProof plan execution failed"),
}


def now():
    return datetime.now(timezone.utc).isoformat()


def directory(job_id):
    if not isinstance(job_id, str) or not _ID.fullmatch(job_id):
        raise ValueError("Invalid job id")
    return swiftproof.storage() / "jobs" / job_id


def _write(job):
    save(directory(job["id"]) / "job.json", job)


def _read(job_id):
    path = directory(job_id) / "job.json"
    if not path.is_file():
        raise ValueError("Unknown SwiftProof job " + job_id)
    job = json.loads(path.read_text(encoding="utf-8"))
    if job["status"] in ("queued", "running") and job_id not in _tasks:
        # The process that ran it is gone (PulsarCD restarted).
        job.update(status="interrupted", reason="PulsarCD restarted while the job ran", finished_at=now())
        _write(job)
    return job


def _archive(job_id):
    job = _read(job_id)
    data = (directory(job_id) / "archive.zip").read_bytes()
    if hashlib.sha256(data).hexdigest() != job.get("archive_sha256"):
        raise ValueError("Stored archive integrity check failed")
    return zipfile.ZipFile(io.BytesIO(data))


def _text(bundle, name, limit=MARKDOWN_LIMIT):
    try:
        text = bundle.read(name).decode("utf-8", errors="replace")
    except KeyError:
        return ""
    return text if len(text) <= limit else text[:limit] + "\n\n[truncated: download the archive for the rest]"


def _llm(enabled):
    if not enabled:
        return None
    llm = load_config_file(str(swiftproof.state()._path.parent)).llm
    return llm if llm and llm.url and llm.model else None


async def start(deployer, repo, kind, intent=None, head=None, base=None, plan_id=None, reviewer=None):
    """Validate, record and schedule a job; return its public record."""
    if kind not in ("plan", "prove"):
        raise ValueError("Unknown job kind")
    params = {"head": head or "", "base": base or ""}
    plan = None
    if kind == "plan":
        if not isinstance(intent, str) or not intent.strip():
            raise ValueError("A plan needs an intent describing the change")
        if len(intent.encode()) > 65536:
            raise ValueError("The intent exceeds 64 KiB")
        params["intent_sha256"] = hashlib.sha256(intent.encode()).hexdigest()
        llm = _llm(True)
        if not llm:
            raise ValueError("A plan needs the LLM configured in PulsarCD (URL and model)")
    else:
        if reviewer is None:
            reviewer = swiftproof.config(repo).get("swiftproof_reviewer", True) is not False
        llm = _llm(reviewer)
        params["reviewer"] = bool(llm)
        if plan_id:
            source = _read(plan_id)
            if source["kind"] != "plan" or source["repo"] != repo or source["status"] not in ("ok", "flagged"):
                raise ValueError("plan_id must be a finished plan of " + repo)
            with _archive(plan_id) as bundle:
                plan = bundle.read("PLAN.json").decode("utf-8")
            if len(plan.encode()) > PLAN_LIMIT:
                raise ValueError("The plan exceeds 1 MiB and cannot be checked remotely")
            params["plan_id"] = plan_id
    job = {"id": uuid.uuid4().hex, "kind": kind, "repo": repo, "status": "queued",
           "reason": "Waiting for the previous SwiftProof job of this project", "params": params,
           "created_at": now()}
    _write(job)
    request = {k: v for k, v in swiftproof.request_for(deployer, repo, "").items() if k != "release"}
    request.update(action=kind, head=head or "", base=base or "")
    if kind == "plan":
        request["intent"] = intent
    if plan:
        request["plan"] = plan
    cancel = asyncio.Event()
    _cancels[job["id"]] = cancel
    _tasks[job["id"]] = asyncio.create_task(_run(deployer, job, request, llm, cancel))
    return public(job)


async def _run(deployer, job, request, llm, cancel):
    try:
        async with _locks.setdefault(job["repo"], asyncio.Lock()):
            if cancel.is_set():
                job.update(status="cancelled", reason="Cancelled before it started", finished_at=now())
                return
            job.update(status="running", reason="SwiftProof is running on the deployment host", started_at=now())
            _write(job)
            reply = await swiftproof.worker(deployer, request, llm, cancel)
            code = reply.get("code")
            table = PLAN_STATUS if job["kind"] == "plan" else PROOF_STATUS
            if type(code) is not int or code not in table:
                raise ValueError("Invalid SwiftProof worker result")
            archive = base64.b64decode(reply["archive"], validate=True)
            if len(archive) > 8 * 1024 * 1024:
                raise ValueError("Archive exceeded its budget")
            with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
                if sum(info.file_size for info in bundle.infolist()) > 8 * 1024 * 1024:
                    raise ValueError("Expanded archive exceeded its budget")
                if job["kind"] == "prove":
                    report = json.loads(bundle.read("confidence-report.json"))
                    change = report.get("change", {})
                    if (change.get("head_commit") != reply["identity"]["head"]
                            or change.get("base_commit") != reply["identity"]["base"] or report.get("exit_code") != code):
                        raise ValueError("Archived report comparison mismatch")
                    detail = swiftproof.failure_detail(report) if code in (3, 4) else ""
                elif code in (0, 2):
                    json.loads(bundle.read("PLAN.json"))
                    detail = ""
                else:
                    # plan writes no report: its last console line says why it stopped.
                    lines = [line for line in _text(bundle, "pulsarcd-run.log", 100000).splitlines() if line.strip()]
                    detail = " ".join(lines[-1].split())[:300] if lines else ""
            status, reason = table[code]
            path = directory(job["id"]) / "archive.zip"
            path.write_bytes(archive)
            job.update(status=status, reason=reason + (" — " + detail if detail else ""), code=code,
                       identity=reply["identity"], warnings=reply.get("warnings", []),
                       tool_version=reply.get("tool_version", ""),
                       archive_sha256=hashlib.sha256(archive).hexdigest(), finished_at=now())
    except asyncio.CancelledError:
        job.update(status="cancelled", reason="Cancelled", finished_at=now())
    except Exception as error:
        # Same sanitizing rule as the pipeline review: never surface transport
        # exceptions, which can contain connection strings or provider details.
        reason = str(error) if isinstance(error, ValueError) else "SwiftProof unavailable: " + type(error).__name__
        job.update(status="cancelled" if cancel.is_set() else "error",
                   reason="Cancelled" if cancel.is_set() else reason, finished_at=now())
    finally:
        _write(job)
        _tasks.pop(job["id"], None)
        _cancels.pop(job["id"], None)


def public(job):
    return {k: job[k] for k in ("id", "kind", "repo", "status", "reason", "code", "params", "identity", "warnings",
                                "tool_version", "created_at", "started_at", "finished_at") if k in job}


def cancel(job_id):
    job = _read(job_id)
    if job_id not in _cancels:
        raise ValueError("Job " + job_id + " is not running (" + job["status"] + ")")
    _cancels[job_id].set()
    return public(job)


def listing(repo=None, limit=20):
    root = swiftproof.storage() / "jobs"
    if not root.is_dir():
        return []
    jobs = []
    for path in root.iterdir():
        if _ID.fullmatch(path.name) and (path / "job.json").is_file():
            try:
                job = _read(path.name)
            except (OSError, ValueError, KeyError):
                continue
            if repo is None or job["repo"].lower() == repo.lower():
                jobs.append(public(job))
    jobs.sort(key=lambda job: job["created_at"], reverse=True)
    return jobs[:max(1, min(limit, 100))]


def result(job_id, include_markdown=True):
    """Return a job with its outcome: plan assessment, or proof verdict and findings."""
    job = _read(job_id)
    data = public(job)
    if "archive_sha256" not in job:
        return data
    with _archive(job_id) as bundle:
        if job["kind"] == "plan":
            if job.get("code") in (0, 2):
                plan = json.loads(bundle.read("PLAN.json"))
                data["plan"] = {k: plan.get(k) for k in ("base_ref", "base_commit", "model", "intent_sha256",
                                                          "proposal", "assessment", "contract", "unverified")}
            if include_markdown:
                data["markdown"] = _text(bundle, "PLAN.md")
        else:
            report = json.loads(bundle.read("confidence-report.json"))
            data["findings"] = swiftproof.report_findings(report)
            if isinstance(report.get("plan_drift"), dict):
                data["plan_drift"] = report["plan_drift"]
            if include_markdown:
                data["markdown"] = _text(bundle, "CONFIDENCE_REPORT.md")
        if job.get("status") == "error" and include_markdown:
            data["log_tail"] = _text(bundle, "pulsarcd-run.log", 8000)[-8000:]
    return data


def archive_bytes(job_id):
    _archive(job_id).close()
    return (directory(job_id) / "archive.zip").read_bytes()
