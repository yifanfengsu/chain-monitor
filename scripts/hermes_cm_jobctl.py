#!/usr/bin/env python3
"""Background job controller for Hermes chain-monitor Telegram operations."""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import secrets
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
_configured_jobs_root = Path(os.environ.get("HERMES_JOBCTL_JOBS_ROOT", "reports/hermes/jobs"))
JOBS_ROOT = _configured_jobs_root if _configured_jobs_root.is_absolute() else REPO_ROOT / _configured_jobs_root
JOB_LOCK = JOBS_ROOT / ".jobctl.lock"
DEFAULT_AUDIT_LOG = "reports/hermes/ops_audit.ndjson"
DEFAULT_OPS_LOCK = "reports/hermes/.hermes_ops.lock"
SCHEMA = "chain_monitor_hermes_job_v1"
AUDIT_SCHEMA = "chain_monitor_hermes_job_audit_v1"
JOB_ID_RE = re.compile(r"^cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]{8,16}$")
FINAL_STATUSES = {"succeeded", "failed", "cancelled"}
ACTIVE_STATUSES = {"pending", "running"}
JOB_KINDS = {"daily-flow", "space-check", "archive-compress-check", "weekly-review"}
KIND_LABELS = {
    "daily-flow": "标准日报流程",
    "space-check": "空间检查",
    "archive-compress-check": "归档压缩预检",
    "weekly-review": "周复盘",
}


class JobctlError(Exception):
    def __init__(self, reason: str, message: str, exit_code: int = 2) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.exit_code = exit_code


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def beijing_today() -> str:
    override = os.environ.get("HERMES_TEST_BEIJING_TODAY", "").strip()
    if override:
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", override):
            raise JobctlError("invalid_test_beijing_today", "❌ 已拒绝：HERMES_TEST_BEIJING_TODAY 必须是 YYYY-MM-DD。")
        try:
            parsed = dt.date.fromisoformat(override)
        except ValueError as exc:
            raise JobctlError("invalid_test_beijing_today", "❌ 已拒绝：HERMES_TEST_BEIJING_TODAY 必须是真实日期。") from exc
        if parsed.isoformat() != override:
            raise JobctlError("invalid_test_beijing_today", "❌ 已拒绝：HERMES_TEST_BEIJING_TODAY 必须是真实日期。")
        return override
    try:
        from zoneinfo import ZoneInfo

        tzinfo = ZoneInfo("Asia/Shanghai")
    except Exception:  # noqa: BLE001
        tzinfo = dt.timezone(dt.timedelta(hours=8))
    return dt.datetime.now(tzinfo).date().isoformat()


def current_beijing_date_override_allowed() -> bool:
    if os.environ.get("HERMES_ALLOW_CURRENT_BEIJING_DATE") != "1":
        return False
    if os.environ.get("HERMES_OPS_PLATFORM", "").lower() == "telegram":
        return False
    return os.environ.get("HERMES_OPS_ROUTER_OK") != "1"


def current_beijing_date_refusal_message(report_date: str) -> str:
    return "\n".join(
        [
            f"❌ 已拒绝：{report_date} 仍是当前北京时间逻辑日，数据可能还在写入。",
            "请在北京时间次日 00:05 后重试：",
            f"标准日报流程{report_date}",
        ]
    )


def parse_utc(value: str) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def duration_text(start_value: str, end_value: str = "") -> str:
    start = parse_utc(start_value)
    if start is None:
        return ""
    end = parse_utc(end_value) or dt.datetime.now(dt.timezone.utc)
    seconds = max(0, int((end - start).total_seconds()))
    minutes, sec = divmod(seconds, 60)
    hours, minute = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minute}m{sec}s"
    if minutes:
        return f"{minutes}m{sec}s"
    return f"{sec}s"


def safe_rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return f"[PRIVATE_PATH]/{path.name}"


def audit_path() -> Path:
    configured = os.environ.get("HERMES_OPS_AUDIT_LOG", DEFAULT_AUDIT_LOG)
    path = Path(configured)
    return path if path.is_absolute() else REPO_ROOT / path


def default_ops_lock_path() -> str:
    candidate = Path("/run/lock/chain-monitor-hermes-ops.lock")
    if candidate.parent.is_dir() and os.access(candidate.parent, os.W_OK):
        return str(candidate)
    return DEFAULT_OPS_LOCK


def sanitize_request_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    if cleaned:
        return cleaned
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"cmjobctl_{stamp}_{secrets.token_hex(6)}"


def sha256_label(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def redact(value: str) -> str:
    redacted = re.sub(r"0x[0-9A-Fa-f]{64}", "0xTX_REDACTED", value)
    redacted = re.sub(r"0x[0-9A-Fa-f]{40}", "0xADDR_REDACTED", redacted)
    redacted = re.sub(r"[0-9]{6,12}:[A-Za-z0-9_-]{25,}", "[REDACTED_TELEGRAM_TOKEN]", redacted)
    redacted = re.sub(
        r"https?://\S*(?:alchemy|infura|quicknode|ankr|blast|drpc|getblock|chainstack|nodereal)\S*",
        "[REDACTED_RPC_URL]",
        redacted,
        flags=re.IGNORECASE,
    )
    redacted = re.sub(
        r"(^|[^A-Za-z0-9_])((?:export\s+)?[A-Za-z0-9_]*(?:TELEGRAM_BOT_TOKEN|RPC_URL|API_KEY|CHAT_ID|PASSWORD|SECRET|TOKEN)[A-Za-z0-9_]*=)[^\s;,\"}\]]+",
        r"\1\2[REDACTED_SECRET]",
        redacted,
        flags=re.IGNORECASE,
    )
    return re.sub(r"/(?:root|home|run-project)(?:/[^\s<>\"'`;,)]*)*", "[PRIVATE_PATH]", redacted)


def validate_date(value: str, label: str = "date") -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value or ""):
        raise JobctlError("invalid_date", f"❌ 已拒绝：{label} 必须是 YYYY-MM-DD。")
    try:
        parsed = dt.date.fromisoformat(value)
    except ValueError as exc:
        raise JobctlError("invalid_date", f"❌ 已拒绝：无效日期 invalid_date：{value}") from exc
    if parsed.isoformat() != value:
        raise JobctlError("invalid_date", f"❌ 已拒绝：无效日期 invalid_date：{value}")
    return value


def validate_job_id(job_id: str) -> str:
    if not JOB_ID_RE.fullmatch(job_id or ""):
        raise JobctlError("invalid_job_id", "❌ 已拒绝：job_id 格式无效。")
    return job_id


def validate_kind(kind: str) -> str:
    if kind not in JOB_KINDS:
        raise JobctlError("invalid_job_kind", "❌ 已拒绝：不支持的后台任务类型。")
    return kind


def validate_kind_args(kind: str, args: argparse.Namespace) -> tuple[str, str, str, list[str]]:
    validate_kind(kind)
    if kind == "daily-flow":
        report_date = validate_date(args.date or "")
        return report_date, "", "", ["--date", report_date]
    if kind == "space-check":
        if args.date or args.start or args.end:
            raise JobctlError("invalid_arguments", "❌ 已拒绝：space-check 不接受日期参数。")
        return "", "", "", []
    if kind == "archive-compress-check":
        report_date = validate_date(args.date or "")
        return report_date, "", "", ["--date", report_date]
    if kind == "weekly-review":
        start = validate_date(args.start or "", "start")
        end = validate_date(args.end or "", "end")
        start_date = dt.date.fromisoformat(start)
        end_date = dt.date.fromisoformat(end)
        if start_date > end_date:
            raise JobctlError("invalid_date_range", "❌ 已拒绝：周复盘 START 必须小于等于 END。")
        if (end_date - start_date).days > 13:
            raise JobctlError("date_range_too_large", "❌ 已拒绝：周复盘范围最多 14 天。")
        return "", start, end, ["--start", start, "--end", end]
    raise JobctlError("invalid_job_kind", "❌ 已拒绝：不支持的后台任务类型。")


def job_dir(job_id: str) -> Path:
    validate_job_id(job_id)
    return JOBS_ROOT / job_id


def meta_path(job_id: str) -> Path:
    return job_dir(job_id) / "meta.json"


def status_path(job_id: str) -> Path:
    return job_dir(job_id) / "status.json"


def result_path(job_id: str) -> Path:
    return job_dir(job_id) / "result.md"


def result_file_for_meta(meta: dict[str, Any]) -> Path:
    return result_path(str(meta.get("job_id", "")))


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise JobctlError("job_not_found", "❌ 未找到任务。") from exc
    except json.JSONDecodeError as exc:
        raise JobctlError("metadata_corrupt", "❌ 任务元数据损坏，请 SSH 检查。") from exc
    if not isinstance(payload, dict):
        raise JobctlError("metadata_corrupt", "❌ 任务元数据损坏，请 SSH 检查。")
    return payload


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def write_status_json(meta: dict[str, Any]) -> None:
    payload = {
        "schema": SCHEMA,
        "job_id": meta.get("job_id", ""),
        "kind": meta.get("kind", ""),
        "status": meta.get("status", ""),
        "created_at_utc": meta.get("created_at_utc", ""),
        "started_at_utc": meta.get("started_at_utc", ""),
        "finished_at_utc": meta.get("finished_at_utc", ""),
        "pid": meta.get("pid"),
        "exit_code": meta.get("exit_code"),
        "failed_step": meta.get("failed_step", ""),
        "failed_substep": meta.get("failed_substep", ""),
        "failed_command": meta.get("failed_command", ""),
        "refused_reason": meta.get("refused_reason", ""),
        "timeout_hit": meta.get("timeout_hit", ""),
        "timeout_limit_sec": meta.get("timeout_limit_sec", ""),
        "result_path": meta.get("result_path", ""),
        "updated_at_utc": utc_now(),
    }
    write_json_atomic(status_path(str(meta["job_id"])), payload)


def update_meta(
    job_id: str,
    *,
    status: str | None = None,
    pid: int | None = None,
    exit_code: int | None = None,
    failed_step: str | None = None,
    failed_substep: str | None = None,
    failed_command: str | None = None,
    refused_reason: str | None = None,
    timeout_hit: str | None = None,
    timeout_limit_sec: int | None = None,
    started: bool = False,
    finished: bool = False,
) -> dict[str, Any]:
    meta = read_json(meta_path(job_id))
    now = utc_now()
    if status is not None:
        meta["status"] = status
    if pid is not None:
        meta["pid"] = pid
    if exit_code is not None:
        meta["exit_code"] = exit_code
    if failed_step is not None:
        meta["failed_step"] = redact(failed_step)
    if failed_substep is not None:
        meta["failed_substep"] = redact(failed_substep)
    if failed_command is not None:
        meta["failed_command"] = redact(failed_command)
    if refused_reason is not None:
        meta["refused_reason"] = redact(refused_reason)
    if timeout_hit is not None:
        meta["timeout_hit"] = timeout_hit
    if timeout_limit_sec is not None:
        meta["timeout_limit_sec"] = timeout_limit_sec
    if started and not meta.get("started_at_utc"):
        meta["started_at_utc"] = now
    if finished and not meta.get("finished_at_utc"):
        meta["finished_at_utc"] = now
    write_json_atomic(meta_path(job_id), meta)
    write_status_json(meta)
    if meta.get("status") in FINAL_STATUSES:
        (job_dir(job_id) / "done.flag").write_text(str(meta.get("status", "")) + "\n", encoding="utf-8")
    return meta


@contextmanager
def job_lock() -> Iterator[None]:
    JOBS_ROOT.mkdir(parents=True, exist_ok=True)
    with JOB_LOCK.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def process_alive(pid: Any) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def refresh_meta(job_id: str) -> dict[str, Any]:
    meta = read_json(meta_path(job_id))
    status = str(meta.get("status") or "")
    pid = meta.get("pid")
    if status in ACTIVE_STATUSES and isinstance(pid, int) and not process_alive(pid):
        final_status = "cancelled" if (job_dir(job_id) / "cancel.request").exists() else "failed"
        meta = update_meta(job_id, status=final_status, finished=True)
    return meta


def all_job_metas() -> list[dict[str, Any]]:
    if not JOBS_ROOT.exists():
        return []
    metas: list[dict[str, Any]] = []
    for path in JOBS_ROOT.glob("cmjob_*/meta.json"):
        job_id = path.parent.name
        if not JOB_ID_RE.fullmatch(job_id):
            continue
        try:
            metas.append(refresh_meta(job_id))
        except JobctlError:
            continue
    return sorted(metas, key=lambda item: str(item.get("created_at_utc") or ""), reverse=True)


def same_signature(
    meta: dict[str, Any],
    kind: str,
    date: str,
    start: str,
    end: str,
    statuses: set[str],
) -> bool:
    return (
        meta.get("kind") == kind
        and meta.get("date", "") == date
        and meta.get("start", "") == start
        and meta.get("end", "") == end
        and meta.get("status") in statuses
    )


def generate_job_id() -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"cmjob_{stamp}_{secrets.token_hex(6)}"


def safe_argv_for(kind: str, job_id: str, extra: list[str]) -> list[str]:
    return ["./scripts/hermes_cm_ops.sh", "__run-job", "--job-id", job_id, "--kind", kind, *extra]


def write_audit(
    *,
    event: str,
    request_id: str,
    job_id: str = "",
    kind: str = "",
    allowed: bool,
    refused_reason: str = "",
    status: str = "",
    exit_code: int | None = None,
    date: str = "",
    start: str = "",
    end: str = "",
    original_text_hash: str = "",
) -> None:
    payload = {
        "schema": AUDIT_SCHEMA,
        "ts_utc": utc_now(),
        "request_id": request_id,
        "event": event,
        "job_id": job_id,
        "kind": kind,
        "allowed": allowed,
        "refused_reason": refused_reason,
        "status": status,
        "exit_code": exit_code,
        "date": date,
        "start": start,
        "end": end,
        "original_text_hash": original_text_hash,
    }
    path = audit_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def submit_message(meta: dict[str, Any]) -> str:
    job_id = str(meta["job_id"])
    label = KIND_LABELS.get(str(meta.get("kind")), str(meta.get("kind")))
    return "\n".join(
        [
            f"✅ 已提交后台任务：{label}",
            f"job_id: {job_id}",
            f"查询进度：任务状态{job_id}",
            f"查看结果：查看结果{job_id}",
            f"查看日志：查看日志{job_id}",
            f"失败诊断：诊断任务{job_id}",
            "说明：该任务会在 VPS 后台继续执行，Telegram 不需要等待。",
        ]
    )


def existing_succeeded_message(meta: dict[str, Any]) -> str:
    job_id = str(meta.get("job_id", ""))
    return "\n".join(
        [
            "已有成功任务",
            "created_new_job=false",
            "status=refused_existing_succeeded",
            f"查看结果{job_id}",
            f"如需重跑：重新标准日报流程{meta.get('date', '')} 我确认重跑",
        ]
    )


def submit_job(args: argparse.Namespace, request_id: str, original_hash: str) -> int:
    kind = validate_kind(args.kind)
    date, start, end, extra = validate_kind_args(kind, args)
    force_rerun = bool(getattr(args, "force_rerun", False))
    if force_rerun and kind != "daily-flow":
        raise JobctlError("invalid_arguments", "❌ 已拒绝：--force-rerun 仅支持 daily-flow。")
    if kind == "daily-flow" and date == beijing_today() and not current_beijing_date_override_allowed():
        write_audit(
            event="job_submit",
            request_id=request_id,
            kind=kind,
            allowed=False,
            refused_reason="current_beijing_date_protected",
            date=date,
            start=start,
            end=end,
            original_text_hash=original_hash,
        )
        print(current_beijing_date_refusal_message(date))
        return 2
    with job_lock():
        for meta in all_job_metas():
            if same_signature(meta, kind, date, start, end, ACTIVE_STATUSES):
                write_audit(
                    event="job_submit",
                    request_id=request_id,
                    job_id=str(meta.get("job_id", "")),
                    kind=kind,
                    allowed=True,
                    status=str(meta.get("status", "")),
                    date=date,
                    start=start,
                    end=end,
                    original_text_hash=original_hash,
                )
                print("已有相同任务正在执行。")
                print(f"job_id: {meta.get('job_id')}")
                print(f"查询：任务状态{meta.get('job_id')}")
                return 0
        if kind == "daily-flow" and not force_rerun:
            for meta in all_job_metas():
                if same_signature(meta, kind, date, start, end, {"succeeded"}):
                    write_audit(
                        event="job_submit",
                        request_id=request_id,
                        job_id=str(meta.get("job_id", "")),
                        kind=kind,
                        allowed=False,
                        refused_reason="refused_existing_succeeded",
                        status=str(meta.get("status", "")),
                        date=date,
                        start=start,
                        end=end,
                        original_text_hash=original_hash,
                    )
                    print(existing_succeeded_message(meta))
                    return 0

        job_id = generate_job_id()
        directory = job_dir(job_id)
        directory.mkdir(parents=True, exist_ok=False)
        for name in ("stdout.log", "stderr.log", "result.md"):
            (directory / name).touch(mode=0o600, exist_ok=True)
        argv = safe_argv_for(kind, job_id, extra)
        result_rel = safe_rel(result_path(job_id))
        meta = {
            "schema": SCHEMA,
            "job_id": job_id,
            "kind": kind,
            "status": "pending",
            "created_at_utc": utc_now(),
            "started_at_utc": "",
            "finished_at_utc": "",
            "date": date,
            "start": start,
            "end": end,
            "request_id": request_id,
            "argv": argv,
            "force_rerun": force_rerun,
            "pid": None,
            "exit_code": None,
            "result_path": result_rel,
        }
        write_json_atomic(meta_path(job_id), meta)
        write_status_json(meta)

        if args.test_no_spawn or os.environ.get("HERMES_JOBCTL_TEST_MODE") == "1":
            write_audit(
                event="job_submit",
                request_id=request_id,
                job_id=job_id,
                kind=kind,
                allowed=True,
                status="pending",
                date=date,
                start=start,
                end=end,
                original_text_hash=original_hash,
            )
            print(submit_message(meta))
            return 0

        env = os.environ.copy()
        env["HERMES_OPS_ROUTER_OK"] = "1"
        env["HERMES_OPS_JOB_RUNNER_OK"] = "1"
        env["HERMES_OPS_REQUEST_ID"] = request_id
        env["HERMES_OPS_AUDIT_LOG"] = os.environ.get("HERMES_OPS_AUDIT_LOG", DEFAULT_AUDIT_LOG)
        env["HERMES_OPS_LOCK_PATH"] = os.environ.get("HERMES_OPS_LOCK_PATH", default_ops_lock_path())
        env["HERMES_OPS_LOCK_TIMEOUT_SEC"] = os.environ.get("HERMES_OPS_LOCK_TIMEOUT_SEC", "3600")
        env["HERMES_OPS_PLATFORM"] = os.environ.get("HERMES_OPS_PLATFORM", "telegram")
        with (directory / "stdout.log").open("ab") as stdout, (directory / "stderr.log").open("ab") as stderr:
            try:
                proc = subprocess.Popen(
                    argv,
                    cwd=str(REPO_ROOT),
                    env=env,
                    stdout=stdout,
                    stderr=stderr,
                    shell=False,
                    start_new_session=True,
                )
            except OSError as exc:
                meta = update_meta(job_id, status="failed", exit_code=127, finished=True)
                write_audit(
                    event="job_submit",
                    request_id=request_id,
                    job_id=job_id,
                    kind=kind,
                    allowed=False,
                    refused_reason="spawn_failed",
                    status=str(meta.get("status", "")),
                    exit_code=127,
                    date=date,
                    start=start,
                    end=end,
                    original_text_hash=original_hash,
                )
                raise JobctlError("spawn_failed", f"❌ 后台任务提交失败：{exc.__class__.__name__}", 1) from exc
        meta = update_meta(job_id, status="running", pid=proc.pid, started=True)
        write_audit(
            event="job_submit",
            request_id=request_id,
            job_id=job_id,
            kind=kind,
            allowed=True,
            status=str(meta.get("status", "")),
            date=date,
            start=start,
            end=end,
            original_text_hash=original_hash,
        )
        print(submit_message(meta))
        return 0


def load_existing(job_id: str) -> dict[str, Any]:
    validate_job_id(job_id)
    if not meta_path(job_id).exists():
        raise JobctlError("job_not_found", "❌ 未找到任务。")
    return refresh_meta(job_id)


def action_status(args: argparse.Namespace, request_id: str, original_hash: str) -> int:
    with job_lock():
        meta = load_existing(args.job_id)
        write_audit(
            event="job_status",
            request_id=request_id,
            job_id=str(meta.get("job_id", "")),
            kind=str(meta.get("kind", "")),
            allowed=True,
            status=str(meta.get("status", "")),
            exit_code=meta.get("exit_code"),
            date=str(meta.get("date", "")),
            start=str(meta.get("start", "")),
            end=str(meta.get("end", "")),
            original_text_hash=original_hash,
        )
    started = str(meta.get("started_at_utc") or meta.get("created_at_utc") or "")
    finished = str(meta.get("finished_at_utc") or "")
    print("后台任务状态")
    print(f"job_id: {meta.get('job_id')}")
    print(f"kind: {meta.get('kind')}")
    print(f"status: {meta.get('status')}")
    print(f"created_at: {meta.get('created_at_utc')}")
    print(f"started_at: {meta.get('started_at_utc')}")
    print(f"finished_at: {meta.get('finished_at_utc')}")
    print(f"duration: {duration_text(started, finished)}")
    print(f"exit_code: {meta.get('exit_code')}")
    if meta.get("failed_substep") or meta.get("failed_command"):
        print(f"failed_step: {meta.get('failed_step', '')}")
        print(f"failed_substep: {meta.get('failed_substep', '')}")
        print(f"failed_command: {meta.get('failed_command', '')}")
        print(f"refused_reason: {meta.get('refused_reason', '')}")
        print(f"timeout_hit: {meta.get('timeout_hit', '')}")
        print(f"timeout_limit_sec: {meta.get('timeout_limit_sec', '')}")
    print(f"result command: 查看结果{meta.get('job_id')}")
    print(f"log command: 查看日志{meta.get('job_id')}")
    print(f"diagnose command: 诊断任务{meta.get('job_id')}")
    return 0


def short_target(meta: dict[str, Any]) -> str:
    if meta.get("date"):
        return str(meta["date"])
    if meta.get("start") or meta.get("end"):
        return f"{meta.get('start', '')}..{meta.get('end', '')}"
    return "-"


def short_result(meta: dict[str, Any]) -> str:
    path = result_file_for_meta(meta)
    if not path.exists() or meta.get("status") not in FINAL_STATUSES:
        return ""
    for line in redact(path.read_text(encoding="utf-8", errors="replace")).splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return line[:80]
    return ""


def action_list(args: argparse.Namespace, request_id: str, original_hash: str) -> int:
    limit = min(max(args.limit, 1), 30)
    with job_lock():
        metas = all_job_metas()[:limit]
        write_audit(
            event="job_status",
            request_id=request_id,
            allowed=True,
            refused_reason="",
            status="list",
            original_text_hash=original_hash,
        )
    print("job_id | kind | status | date/range | created_at | short result")
    for meta in metas:
        print(
            f"{meta.get('job_id')} | {meta.get('kind')} | {meta.get('status')} | "
            f"{short_target(meta)} | {meta.get('created_at_utc')} | {short_result(meta)}"
        )
    return 0


def action_result(args: argparse.Namespace, request_id: str, original_hash: str) -> int:
    with job_lock():
        meta = load_existing(args.job_id)
        write_audit(
            event="job_result",
            request_id=request_id,
            job_id=str(meta.get("job_id", "")),
            kind=str(meta.get("kind", "")),
            allowed=True,
            status=str(meta.get("status", "")),
            exit_code=meta.get("exit_code"),
            date=str(meta.get("date", "")),
            start=str(meta.get("start", "")),
            end=str(meta.get("end", "")),
            original_text_hash=original_hash,
        )
    if meta.get("status") not in FINAL_STATUSES:
        print(f"任务尚未完成，请先查询状态：任务状态{meta.get('job_id')}")
        return 0
    path = result_file_for_meta(meta)
    if not path.exists():
        print("数据不足：result.md 不存在，请 SSH 检查任务目录。")
        return 1
    text = redact(path.read_text(encoding="utf-8", errors="replace"))
    if len(text) > 3500:
        text = text[:3500] + f"\n\n[已截断，完整文件在 VPS: {safe_rel(path)}]"
    print(text)
    return 0


def tail_lines(path: Path, count: int) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-count:])


def action_log(args: argparse.Namespace, request_id: str, original_hash: str) -> int:
    tail = min(max(args.tail, 1), 120)
    with job_lock():
        meta = load_existing(args.job_id)
        write_audit(
            event="job_log",
            request_id=request_id,
            job_id=str(meta.get("job_id", "")),
            kind=str(meta.get("kind", "")),
            allowed=True,
            status=str(meta.get("status", "")),
            exit_code=meta.get("exit_code"),
            date=str(meta.get("date", "")),
            start=str(meta.get("start", "")),
            end=str(meta.get("end", "")),
            original_text_hash=original_hash,
        )
    directory = job_dir(str(meta["job_id"]))
    stdout_text = redact(tail_lines(directory / "stdout.log", tail))
    stderr_text = redact(tail_lines(directory / "stderr.log", tail))
    print(f"日志尾部 job_id: {meta.get('job_id')} tail={tail}")
    print("## stdout.log")
    print(stdout_text or "(empty)")
    print("## stderr.log")
    print(stderr_text or "(empty)")
    return 0


def extract_field(text: str, key: str) -> str:
    pattern = re.compile(rf"^{re.escape(key)}\s*[=:：]\s*(.*)$", re.MULTILINE)
    match = pattern.search(text)
    return match.group(1).strip() if match else ""


def extract_block(text: str, label: str) -> str:
    lines = text.splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if line.strip() in {f"{label}:", f"{label}："}:
            start = index + 1
            break
    if start is None:
        return ""
    stop_labels = {
        "stdout_tail:",
        "stdout_tail：",
        "stderr_tail:",
        "stderr_tail：",
        "建议：",
        "建议:",
        "## runner_output",
        "request_id=",
    }
    collected: list[str] = []
    for line in lines[start:]:
        stripped = line.strip()
        if stripped in stop_labels and collected:
            break
        if stripped.startswith("failed_") and collected:
            break
        collected.append(line)
    value = "\n".join(collected).strip()
    return value


def audit_refused_reason_for(meta: dict[str, Any]) -> str:
    request_id = str(meta.get("request_id") or "")
    job_id = str(meta.get("job_id") or "")
    date = str(meta.get("date") or "")
    path = audit_path()
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    for line in reversed(lines[-5000:]):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        reason = str(payload.get("refused_reason") or "")
        if not reason:
            continue
        if job_id and str(payload.get("job_id") or "") == job_id:
            return reason
        if request_id and str(payload.get("request_id") or "") == request_id:
            return reason
        if date and str(payload.get("date") or "") == date and str(payload.get("command") or "") in {"daily-flow", "__run-job"}:
            return reason
    return ""


def status_refused_reason_for(meta: dict[str, Any]) -> str:
    job_id = str(meta.get("job_id") or "")
    if not job_id:
        return ""
    try:
        payload = read_json(status_path(job_id))
    except JobctlError:
        return ""
    return str(payload.get("refused_reason") or "")


def argv_uses_run_job(argv: Any) -> bool:
    return isinstance(argv, list) and len(argv) >= 2 and argv[1] == "__run-job"


def action_diagnose(args: argparse.Namespace, request_id: str, original_hash: str) -> int:
    with job_lock():
        meta = load_existing(args.job_id)
        write_audit(
            event="job_diagnose",
            request_id=request_id,
            job_id=str(meta.get("job_id", "")),
            kind=str(meta.get("kind", "")),
            allowed=True,
            status=str(meta.get("status", "")),
            exit_code=meta.get("exit_code"),
            date=str(meta.get("date", "")),
            start=str(meta.get("start", "")),
            end=str(meta.get("end", "")),
            original_text_hash=original_hash,
        )
    path = result_file_for_meta(meta)
    text = redact(path.read_text(encoding="utf-8", errors="replace")) if path.exists() else ""
    failed_step = str(meta.get("failed_step") or extract_field(text, "failed_step"))
    failed_substep = str(meta.get("failed_substep") or extract_field(text, "failed_substep"))
    failed_command = str(meta.get("failed_command") or extract_field(text, "failed_command"))
    refused_reason = str(
        meta.get("refused_reason")
        or status_refused_reason_for(meta)
        or extract_field(text, "refused_reason")
        or audit_refused_reason_for(meta)
    )
    timeout_hit = str(meta.get("timeout_hit") or extract_field(text, "timeout_hit") or "")
    timeout_limit_sec = str(meta.get("timeout_limit_sec") or extract_field(text, "timeout_limit_sec") or "")
    exit_code = meta.get("exit_code")
    detail_exit_code = extract_field(text, "exit_code")
    if detail_exit_code:
        exit_code = detail_exit_code
    stdout_tail = extract_block(text, "stdout_tail")
    stderr_tail = extract_block(text, "stderr_tail")
    job_id = str(meta.get("job_id", ""))
    kind = str(meta.get("kind", ""))
    date = str(meta.get("date", ""))
    directory = job_dir(job_id)
    if not stdout_tail:
        stdout_tail = redact(tail_lines(directory / "stdout.log", 80))
    if not stderr_tail:
        stderr_tail = redact(tail_lines(directory / "stderr.log", 80))
    argv = meta.get("argv", [])
    argv_text = redact(json.dumps(argv, ensure_ascii=False))
    exit_code_text = str(exit_code if exit_code is not None else "")
    stdout_empty = not stdout_tail.strip()
    stderr_empty = not stderr_tail.strip()

    print("任务诊断")
    print(f"job_id={job_id}")
    print(f"kind={kind}")
    print(f"status={meta.get('status')}")
    print(f"failed_step={failed_step}")
    print(f"failed_substep={failed_substep}")
    print(f"failed_command={failed_command}")
    print(f"refused_reason={refused_reason}")
    print(f"exit_code={exit_code}")
    print(f"timeout_hit={timeout_hit or 'false'}")
    print(f"timeout_limit_sec={timeout_limit_sec}")
    print(f"result_path={safe_rel(path)}")
    print(f"meta_argv={argv_text}")
    print()
    print("stdout_tail:")
    print(stdout_tail or "(empty)")
    print()
    print("stderr_tail:")
    print(stderr_tail or "(empty)")
    print()
    print("下一步建议:")
    if refused_reason == "current_beijing_date_protected":
        print(f"等北京时间次日 00:05 后重新运行标准日报流程{date}。")
        print(f"/chain-monitor-report-analyst 标准日报流程{date}")
    elif refused_reason:
        print(f"任务被 preflight/refusal 拒绝：{refused_reason}")
        print("请先处理拒绝原因，再重新提交。")
    elif exit_code_text == "2" and stdout_empty and stderr_empty:
        print("这通常是 preflight/refusal。请检查 refused_reason、meta argv 和 audit。")
        print("请重点确认：是否提交了当前北京时间逻辑日。")
        print("请重点确认：是否本地 jobctl 仍是旧版。")
        print("请重点确认：meta.json argv 是否为 __run-job。")
        if argv_uses_run_job(argv):
            print("meta.json argv 已使用 __run-job。")
        else:
            print("meta.json argv 不是 __run-job，可能本地 jobctl 仍是旧版，请重新运行 install/pull。")
    elif kind == "daily-flow" and failed_substep:
        print("请先修复上述失败子步骤，再重新运行：")
        print(f"/chain-monitor-report-analyst 标准日报流程{date}")
    elif meta.get("status") in ACTIVE_STATUSES:
        print(f"任务仍在执行，先查看日志：查看日志{job_id}")
    else:
        print(f"请先查看日志尾部：查看日志{job_id}")
    return 0


def action_cancel(args: argparse.Namespace, request_id: str, original_hash: str) -> int:
    if not args.confirm:
        raise JobctlError("missing_cancel_confirm", "取消任务需要确认短语：取消任务JOB_ID 我确认取消")
    with job_lock():
        meta = load_existing(args.job_id)
        if meta.get("status") not in ACTIVE_STATUSES:
            write_audit(
                event="job_cancel",
                request_id=request_id,
                job_id=str(meta.get("job_id", "")),
                kind=str(meta.get("kind", "")),
                allowed=False,
                refused_reason="job_not_active",
                status=str(meta.get("status", "")),
                exit_code=meta.get("exit_code"),
                date=str(meta.get("date", "")),
                start=str(meta.get("start", "")),
                end=str(meta.get("end", "")),
                original_text_hash=original_hash,
            )
            raise JobctlError("job_not_active", "❌ 已拒绝：只能取消 pending/running 任务。")
        directory = job_dir(str(meta["job_id"]))
        (directory / "cancel.request").write_text(utc_now() + "\n", encoding="utf-8")
        pid = meta.get("pid")
        if not isinstance(pid, int) or pid <= 0:
            meta = update_meta(str(meta["job_id"]), status="cancelled", exit_code=-15, finished=True)
            write_audit(
                event="job_cancel",
                request_id=request_id,
                job_id=str(meta.get("job_id", "")),
                kind=str(meta.get("kind", "")),
                allowed=True,
                status=str(meta.get("status", "")),
                exit_code=meta.get("exit_code"),
                date=str(meta.get("date", "")),
                start=str(meta.get("start", "")),
                end=str(meta.get("end", "")),
                original_text_hash=original_hash,
            )
            print(f"已取消 pending 任务：{meta.get('job_id')}")
            return 0
        if not process_alive(pid):
            meta = update_meta(str(meta["job_id"]), status="cancelled", exit_code=-15, finished=True)
            write_audit(
                event="job_cancel",
                request_id=request_id,
                job_id=str(meta.get("job_id", "")),
                kind=str(meta.get("kind", "")),
                allowed=True,
                status=str(meta.get("status", "")),
                exit_code=meta.get("exit_code"),
                date=str(meta.get("date", "")),
                start=str(meta.get("start", "")),
                end=str(meta.get("end", "")),
                original_text_hash=original_hash,
            )
            print(f"任务进程已结束，状态已标记 cancelled：{meta.get('job_id')}")
            return 0
        try:
            if os.getpgid(pid) != pid:
                raise JobctlError("unsafe_process_group", "❌ 已拒绝：任务进程组校验失败，请 SSH 检查。")
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            meta = update_meta(str(meta["job_id"]), status="cancelled", exit_code=-15, finished=True)
            write_audit(
                event="job_cancel",
                request_id=request_id,
                job_id=str(meta.get("job_id", "")),
                kind=str(meta.get("kind", "")),
                allowed=True,
                status=str(meta.get("status", "")),
                exit_code=meta.get("exit_code"),
                date=str(meta.get("date", "")),
                start=str(meta.get("start", "")),
                end=str(meta.get("end", "")),
                original_text_hash=original_hash,
            )
            print(f"任务进程已结束，状态已标记 cancelled：{meta.get('job_id')}")
            return 0
        time.sleep(2)
        if process_alive(pid):
            write_audit(
                event="job_cancel",
                request_id=request_id,
                job_id=str(meta.get("job_id", "")),
                kind=str(meta.get("kind", "")),
                allowed=True,
                status=str(meta.get("status", "")),
                exit_code=meta.get("exit_code"),
                date=str(meta.get("date", "")),
                start=str(meta.get("start", "")),
                end=str(meta.get("end", "")),
                original_text_hash=original_hash,
            )
            print(f"已发送取消请求，但进程仍在退出中：{meta.get('job_id')}")
            print("请稍后查询：任务状态" + str(meta.get("job_id")))
            print("如长时间 running，请 SSH 检查；不会使用 kill -9。")
            return 0
        meta = update_meta(str(meta["job_id"]), status="cancelled", exit_code=-15, finished=True)
        write_audit(
            event="job_cancel",
            request_id=request_id,
            job_id=str(meta.get("job_id", "")),
            kind=str(meta.get("kind", "")),
            allowed=True,
            status=str(meta.get("status", "")),
            exit_code=meta.get("exit_code"),
            date=str(meta.get("date", "")),
            start=str(meta.get("start", "")),
            end=str(meta.get("end", "")),
            original_text_hash=original_hash,
        )
        print(f"已取消任务：{meta.get('job_id')}")
        return 0


def action_update(args: argparse.Namespace) -> int:
    validate_job_id(args.job_id)
    if args.status not in {"pending", "running", "succeeded", "failed", "cancelled"}:
        raise JobctlError("invalid_status", "invalid status")
    with job_lock():
        update_meta(
            args.job_id,
            status=args.status,
            pid=args.pid,
            exit_code=args.exit_code,
            failed_step=args.failed_step,
            failed_substep=args.failed_substep,
            failed_command=args.failed_command,
            refused_reason=args.refused_reason,
            timeout_hit=args.timeout_hit,
            timeout_limit_sec=args.timeout_limit_sec,
            started=args.status == "running",
            finished=args.status in FINAL_STATUSES,
        )
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hermes chain-monitor background job controller.")
    sub = parser.add_subparsers(dest="action", required=True)

    submit = sub.add_parser("submit")
    submit.add_argument("--kind", required=True, choices=sorted(JOB_KINDS))
    submit.add_argument("--date")
    submit.add_argument("--start")
    submit.add_argument("--end")
    submit.add_argument("--force-rerun", action="store_true")
    submit.add_argument("--test-no-spawn", action="store_true")

    status = sub.add_parser("status")
    status.add_argument("--job-id", required=True)

    listing = sub.add_parser("list")
    listing.add_argument("--limit", type=int, default=10)

    result = sub.add_parser("result")
    result.add_argument("--job-id", required=True)

    log = sub.add_parser("log")
    log.add_argument("--job-id", required=True)
    log.add_argument("--tail", type=int, default=80)

    diagnose = sub.add_parser("diagnose")
    diagnose.add_argument("--job-id", required=True)

    cancel = sub.add_parser("cancel")
    cancel.add_argument("--job-id", required=True)
    cancel.add_argument("--confirm", action="store_true")

    update = sub.add_parser("__update")
    update.add_argument("--job-id", required=True)
    update.add_argument("--status", required=True)
    update.add_argument("--pid", type=int)
    update.add_argument("--exit-code", type=int)
    update.add_argument("--failed-step")
    update.add_argument("--failed-substep")
    update.add_argument("--failed-command")
    update.add_argument("--refused-reason")
    update.add_argument("--timeout-hit", choices=("true", "false"))
    update.add_argument("--timeout-limit-sec", type=int)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    request_id = sanitize_request_id(os.environ.get("HERMES_OPS_REQUEST_ID", ""))
    original_hash = os.environ.get("HERMES_OPS_ORIGINAL_COMMAND_HASH", "")
    if original_hash and not original_hash.startswith("sha256:"):
        original_hash = sha256_label(original_hash)
    try:
        if args.action == "submit":
            return submit_job(args, request_id, original_hash)
        if args.action == "status":
            return action_status(args, request_id, original_hash)
        if args.action == "list":
            return action_list(args, request_id, original_hash)
        if args.action == "result":
            return action_result(args, request_id, original_hash)
        if args.action == "log":
            return action_log(args, request_id, original_hash)
        if args.action == "diagnose":
            return action_diagnose(args, request_id, original_hash)
        if args.action == "cancel":
            return action_cancel(args, request_id, original_hash)
        if args.action == "__update":
            return action_update(args)
        raise JobctlError("unknown_action", "unknown action")
    except JobctlError as exc:
        event_map = {
            "submit": "job_submit",
            "status": "job_status",
            "list": "job_status",
            "result": "job_result",
            "log": "job_log",
            "diagnose": "job_diagnose",
            "cancel": "job_cancel",
        }
        if args.action in event_map:
            try:
                write_audit(
                    event=event_map[args.action],
                    request_id=request_id,
                    job_id=getattr(args, "job_id", "") or "",
                    kind=getattr(args, "kind", "") or "",
                    allowed=False,
                    refused_reason=exc.reason,
                    original_text_hash=original_hash,
                )
            except OSError:
                pass
        print(exc.message, file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
