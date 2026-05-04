#!/usr/bin/env python3
"""Deterministic Chinese Telegram command router for Hermes chain-monitor ops."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
WRAPPER_ARGV0 = "./scripts/hermes_cm_ops.sh"
DEFAULT_AUDIT_LOG = "reports/hermes/ops_audit.ndjson"
MAX_INPUT_CHARS = 300

RELATIVE_DATE_TERMS = (
    "今天",
    "昨天",
    "前天",
    "今日",
    "昨日",
    "明天",
    "today",
    "yesterday",
    "tomorrow",
)

RELATIVE_DATE_MESSAGE = (
    "⚠️ 请使用绝对日期 YYYY-MM-DD。\n"
    "示例：分析报告2026-05-01\n"
    "我不会根据“今天/昨天/前天”自动执行。"
)

MODE_MESSAGE = "⚠️ 请指定“快速”或“深度”。\n示例：生成摘要2026-05-01 快速"
UNSUPPORTED_MESSAGE = "❌ 已拒绝：不支持的中文命令。请发送“命令提示”查看固定中文菜单。"
INVALID_DATE_MESSAGE = "❌ 已拒绝：无效日期 invalid_date，请使用真实日期 YYYY-MM-DD。"
INVALID_RANGE_MESSAGE = "❌ 已拒绝：周复盘日期范围无效，请使用 START到END，例如：周复盘2026-04-27到2026-05-03。"
INVALID_JOB_ID_MESSAGE = "❌ 已拒绝：job_id 格式无效。"
CANCEL_CONFIRM_MESSAGE = "取消任务需要确认短语：取消任务JOB_ID 我确认取消"
RERUN_CONFIRM_MESSAGE = "重跑标准日报流程需要确认短语：重新标准日报流程YYYY-MM-DD 我确认重跑"
JOB_ID_PATTERN = r"cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]+"


class RouterError(Exception):
    def __init__(self, reason: str, message: str, *, date: str = "", mode: str = "") -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.date = date
        self.mode = mode


def utc_timestamp() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def generate_request_id() -> str:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"cmrouter_{stamp}_{secrets.token_hex(6)}"


def sanitize_request_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    return cleaned or generate_request_id()


def sha256_label(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def audit_path() -> Path:
    configured = os.environ.get("HERMES_OPS_AUDIT_LOG", DEFAULT_AUDIT_LOG)
    path = Path(configured)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def write_audit(
    *,
    request_id: str,
    platform: str,
    original_text_hash: str,
    command_intent: str,
    allowed: bool,
    refused_reason: str,
    date: str,
    mode: str,
    auto_build: bool,
    exit_code: int,
) -> None:
    event = {
        "schema": "chain_monitor_hermes_cn_router_audit_v1",
        "ts_utc": utc_timestamp(),
        "request_id": request_id,
        "event": "router_finish",
        "platform": platform,
        "original_text_hash": original_text_hash,
        "command_intent": command_intent,
        "allowed": allowed,
        "refused_reason": refused_reason,
        "date": date,
        "mode": mode,
        "auto_build": auto_build,
        "exit_code": exit_code,
    }
    path = audit_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")


def normalize_text(raw_text: str) -> str:
    text = raw_text.replace("\u3000", " ").strip()
    prefix = "/chain-monitor-report-analyst"
    if text.startswith(prefix):
        text = text[len(prefix) :]
        if text.startswith("@"):
            parts = text.split(maxsplit=1)
            text = parts[1] if len(parts) == 2 else ""
        text = text.strip()
    return text


def validate_date(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise RouterError("invalid_date", INVALID_DATE_MESSAGE, date=value)
    try:
        parsed = datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise RouterError("invalid_date", INVALID_DATE_MESSAGE, date=value) from exc
    if parsed.isoformat() != value:
        raise RouterError("invalid_date", INVALID_DATE_MESSAGE, date=value)
    return value


def validate_date_range(start: str, end: str) -> tuple[str, str]:
    start_value = validate_date(start)
    end_value = validate_date(end)
    start_date = datetime.date.fromisoformat(start_value)
    end_date = datetime.date.fromisoformat(end_value)
    if start_date > end_date:
        raise RouterError("invalid_date_range", INVALID_RANGE_MESSAGE, date=f"{start_value}..{end_value}")
    if (end_date - start_date).days > 13:
        raise RouterError(
            "date_range_too_large",
            "❌ 已拒绝：周复盘范围最多 14 天，请缩小 START 到 END。",
            date=f"{start_value}..{end_value}",
        )
    return start_value, end_value


def validate_job_id(value: str) -> str:
    if not re.fullmatch(JOB_ID_PATTERN, value or ""):
        raise RouterError("invalid_job_id", INVALID_JOB_ID_MESSAGE)
    return value


def has_relative_date(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in RELATIVE_DATE_TERMS if term.isascii()) or any(
        term in text for term in RELATIVE_DATE_TERMS if not term.isascii()
    )


def wrapper_argv(action: str, *args: str) -> list[str]:
    return [WRAPPER_ARGV0, action, *args]


def parse_mode(value: str) -> str:
    if value == "快速":
        return "fast"
    if value == "深度":
        return "deep"
    raise RouterError("missing_mode", MODE_MESSAGE, mode=value)


def parse_command(text: str) -> dict[str, Any]:
    if not text:
        raise RouterError("unsupported_grammar", UNSUPPORTED_MESSAGE)
    if len(text) > MAX_INPUT_CHARS:
        raise RouterError("input_too_long", "❌ 已拒绝：输入过长，请使用固定中文命令。")
    if has_relative_date(text):
        raise RouterError("relative_date_forbidden", RELATIVE_DATE_MESSAGE)

    if text in {"命令提示", "功能列表", "菜单", "帮助", "命令", "怎么用", "使用说明"}:
        return {
            "action": "command-menu",
            "command_intent": "command-menu",
            "argv": wrapper_argv("command-menu"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    if text in {"系统体检", "体检", "健康检查", "状态", "系统状态"}:
        return {
            "action": "system-health",
            "command_intent": "system-health",
            "argv": wrapper_argv("system-health"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    if text in {"监听器体检", "监听器检查", "监听状态", "监听器状态", "最近数据", "零活动排查"} or re.fullmatch(
        r"零活动排查\s*[0-9]{4}-[0-9]{2}-[0-9]{2}",
        text,
    ):
        return {
            "action": "listener-health",
            "command_intent": "listener-health",
            "argv": wrapper_argv("listener-health"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(
        r"(?:标准日报流程|跑标准日报流程|每日标准流程|日常报告流程)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        text,
    )
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "submit-daily-flow",
            "command_intent": "submit-daily-flow",
            "argv": wrapper_argv("submit-daily-flow", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(
        r"(?:重新标准日报流程|重跑标准日报流程)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})(?:\s*(.*))?",
        text,
    )
    if match:
        report_date = validate_date(match.group(1))
        confirmation = (match.group(2) or "").strip()
        if confirmation != "我确认重跑":
            raise RouterError("missing_rerun_confirm", RERUN_CONFIRM_MESSAGE, date=report_date)
        return {
            "action": "submit-daily-flow",
            "command_intent": "submit-daily-flow",
            "argv": wrapper_argv("submit-daily-flow", "--date", report_date, "--force-rerun"),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:生成日报|生成报告|生成每日报告)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "report",
            "command_intent": "report",
            "argv": wrapper_argv("report", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:分析报告|快速分析报告|日报分析|报告分析)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "analyze",
            "command_intent": "analyze",
            "argv": wrapper_argv("analyze", "--date", report_date, "--mode", "fast"),
            "date": report_date,
            "mode": "fast",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:检查回放|回放检查|回放摘要|检查replay|Replay检查)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "replay-check",
            "command_intent": "replay-check",
            "argv": wrapper_argv("replay-check", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:数据质量|报告是否有效|检查数据质量|异常摘要)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "data-quality",
            "command_intent": "data-quality",
            "argv": wrapper_argv("data-quality", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:Profile复盘|profile复盘|画像复盘|后验画像)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "profile-review",
            "command_intent": "profile-review",
            "argv": wrapper_argv("profile-review", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:Blocker复盘|blocker复盘|阻断复盘|风控阻断复盘)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "blocker-review",
            "command_intent": "blocker-review",
            "argv": wrapper_argv("blocker-review", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(
        r"(?:Shadow复盘|shadow复盘|影子复盘|Shadow Funnel复盘|影子漏斗)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        text,
    )
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "shadow-review",
            "command_intent": "shadow-review",
            "argv": wrapper_argv("shadow-review", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    if text in {"空间检查", "磁盘检查", "VPS空间检查", "数据库空间检查", "DB空间检查"}:
        return {
            "action": "submit-space-check",
            "command_intent": "submit-space-check",
            "argv": wrapper_argv("submit-space-check"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    if text in {"空间快检", "快速空间检查", "快速磁盘检查"}:
        return {
            "action": "space-fast",
            "command_intent": "space-fast",
            "argv": wrapper_argv("space-fast"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    if text in {"数据库体积诊断", "DB体积诊断", "数据库为什么大"}:
        return {
            "action": "db-size-diagnose",
            "command_intent": "db-size-diagnose",
            "argv": wrapper_argv("db-size-diagnose"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    if text in {"数据库瘦身预检", "DB瘦身预检", "数据库清理预检"}:
        return {
            "action": "db-slim-dry-run",
            "command_intent": "db-slim-dry-run",
            "argv": wrapper_argv("db-slim-dry-run"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:归档压缩预检|压缩预检|archive压缩预检|Archive压缩预检)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "submit-archive-compress-check",
            "command_intent": "submit-archive-compress-check",
            "argv": wrapper_argv("submit-archive-compress-check", "--date", report_date),
            "date": report_date,
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(
        r"(?:周复盘|周度复盘|每周复盘)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*到\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        text,
    )
    if match:
        start, end = validate_date_range(match.group(1), match.group(2))
        return {
            "action": "submit-weekly-review",
            "command_intent": "submit-weekly-review",
            "argv": wrapper_argv("submit-weekly-review", "--start", start, "--end", end),
            "date": f"{start}..{end}",
            "mode": "",
            "auto_build": False,
        }

    if text.startswith(("周复盘", "周度复盘", "每周复盘")):
        raise RouterError("invalid_date_range", INVALID_RANGE_MESSAGE)

    match = re.fullmatch(r"(?:任务状态|查询任务|任务进度)\s*(" + JOB_ID_PATTERN + r")", text)
    if match:
        job_id = validate_job_id(match.group(1))
        return {
            "action": "job-status",
            "command_intent": "job-status",
            "argv": wrapper_argv("job-status", "--job-id", job_id),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:查看结果|任务结果|读取结果)\s*(" + JOB_ID_PATTERN + r")", text)
    if match:
        job_id = validate_job_id(match.group(1))
        return {
            "action": "job-result",
            "command_intent": "job-result",
            "argv": wrapper_argv("job-result", "--job-id", job_id),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:查看日志|任务日志)\s*(" + JOB_ID_PATTERN + r")", text)
    if match:
        job_id = validate_job_id(match.group(1))
        return {
            "action": "job-log",
            "command_intent": "job-log",
            "argv": wrapper_argv("job-log", "--job-id", job_id),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"(?:诊断任务|任务诊断)\s*(" + JOB_ID_PATTERN + r")", text)
    if match:
        job_id = validate_job_id(match.group(1))
        return {
            "action": "job-diagnose",
            "command_intent": "job-diagnose",
            "argv": wrapper_argv("job-diagnose", "--job-id", job_id),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    if text in {"最近任务", "任务列表", "查看任务"}:
        return {
            "action": "job-list",
            "command_intent": "job-list",
            "argv": wrapper_argv("job-list"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    match = re.fullmatch(r"取消任务\s*(" + JOB_ID_PATTERN + r")(?:\s*(.*))?", text)
    if match:
        job_id = validate_job_id(match.group(1))
        confirmation = (match.group(2) or "").strip()
        if confirmation != "我确认取消":
            raise RouterError("missing_cancel_confirm", CANCEL_CONFIRM_MESSAGE)
        return {
            "action": "job-cancel",
            "command_intent": "job-cancel",
            "argv": wrapper_argv("job-cancel", "--job-id", job_id, "--confirm"),
            "date": "",
            "mode": "",
            "auto_build": False,
        }

    if re.match(r"^(?:任务状态|查询任务|任务进度|查看结果|任务结果|读取结果|查看日志|任务日志|诊断任务|任务诊断|取消任务)", text):
        if text.startswith("取消任务"):
            raise RouterError("missing_cancel_confirm", CANCEL_CONFIRM_MESSAGE)
        raise RouterError("invalid_job_id", INVALID_JOB_ID_MESSAGE)

    match = re.fullmatch(r"(?:深度分析报告|深度复盘|深度分析)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
    if match:
        report_date = validate_date(match.group(1))
        return {
            "action": "analyze",
            "command_intent": "analyze",
            "argv": wrapper_argv("analyze", "--date", report_date, "--mode", "deep"),
            "date": report_date,
            "mode": "deep",
            "auto_build": False,
        }

    match = re.fullmatch(
        r"(?:生成摘要|生成输入包|生成分析输入包)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})(?:\s*(\S+))?",
        text,
    )
    if match:
        report_date = validate_date(match.group(1))
        mode_word = match.group(2)
        if not mode_word:
            raise RouterError("missing_mode", MODE_MESSAGE, date=report_date)
        mode = parse_mode(mode_word)
        return {
            "action": "digest",
            "command_intent": "digest",
            "argv": wrapper_argv("digest", "--date", report_date, "--mode", mode),
            "date": report_date,
            "mode": mode,
            "auto_build": False,
        }

    match = re.fullmatch(
        r"(?:构建并分析报告|自动构建并分析|自动生成并分析)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})(?:\s*(\S+))?",
        text,
    )
    if match:
        report_date = validate_date(match.group(1))
        mode_word = match.group(2)
        if not mode_word:
            raise RouterError("missing_mode", MODE_MESSAGE, date=report_date)
        mode = parse_mode(mode_word)
        return {
            "action": "analyze",
            "command_intent": "analyze",
            "argv": wrapper_argv("analyze", "--date", report_date, "--mode", mode, "--auto-build"),
            "date": report_date,
            "mode": mode,
            "auto_build": True,
        }

    match = re.fullmatch(r"每日收尾\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s+我确认压缩", text)
    if match:
        close_date = validate_date(match.group(1))
        return {
            "action": "close",
            "command_intent": "close",
            "argv": wrapper_argv("close", "--date", close_date, "--confirm-compress"),
            "date": close_date,
            "mode": "",
            "auto_build": False,
        }

    raise RouterError("unsupported_grammar", UNSUPPORTED_MESSAGE)


def redact_output(value: str) -> str:
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


def limit_output(value: str) -> str:
    lines = value.splitlines()
    limited = "\n".join(lines[:120])
    if len(lines) > 120:
        limited += "\n[output truncated]"
    if len(limited) > 12000:
        limited = limited[:12000] + "\n[output truncated]"
    return limited


def execute_wrapper(argv: list[str], platform: str, original_hash: str, request_id: str) -> int:
    env = os.environ.copy()
    env["HERMES_OPS_ROUTER_OK"] = "1"
    env["HERMES_OPS_PLATFORM"] = platform
    env["HERMES_OPS_ORIGINAL_COMMAND_HASH"] = original_hash
    env["HERMES_OPS_REQUEST_ID"] = request_id
    completed = subprocess.run(
        argv,
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        check=False,
    )
    stdout = limit_output(redact_output(completed.stdout))
    stderr = limit_output(redact_output(completed.stderr))
    if stdout:
        print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)
    return completed.returncode


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Route fixed Chinese Hermes chain-monitor commands.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--text", help="Chinese command text")
    input_group.add_argument("--stdin", action="store_true", help="Read Chinese command from stdin")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--execute", action="store_true", help="Execute the validated wrapper command")
    mode_group.add_argument("--dry-run", action="store_true", help="Print the validated argv JSON without executing")
    parser.add_argument("--platform", choices=("telegram", "cli", "hermes"), default="hermes")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    raw_text = args.text if args.text is not None else sys.stdin.read(MAX_INPUT_CHARS + 1)
    request_id = sanitize_request_id(os.environ.get("HERMES_OPS_REQUEST_ID", "")) if os.environ.get("HERMES_OPS_REQUEST_ID") else generate_request_id()
    original_hash = sha256_label(raw_text)
    if len(raw_text) > MAX_INPUT_CHARS:
        exit_code = 2
        write_audit(
            request_id=request_id,
            platform=args.platform,
            original_text_hash=original_hash,
            command_intent="unknown",
            allowed=False,
            refused_reason="input_too_long",
            date="",
            mode="",
            auto_build=False,
            exit_code=exit_code,
        )
        print("❌ 已拒绝：输入过长，请使用固定中文命令。")
        return exit_code
    text = normalize_text(raw_text)

    try:
        command = parse_command(text)
    except RouterError as exc:
        exit_code = 2
        write_audit(
            request_id=request_id,
            platform=args.platform,
            original_text_hash=original_hash,
            command_intent="unknown",
            allowed=False,
            refused_reason=exc.reason,
            date=exc.date,
            mode=exc.mode,
            auto_build=False,
            exit_code=exit_code,
        )
        print(exc.message)
        return exit_code

    if args.execute:
        exit_code = execute_wrapper(command["argv"], args.platform, original_hash, request_id)
    else:
        exit_code = 0
        response: dict[str, Any] = {"allowed": True, "action": command["action"], "argv": command["argv"]}
        if command["action"] == "submit-daily-flow":
            response["warning"] = "注意：如果该日期仍是当前北京时间逻辑日，submit 阶段会拒绝。"
        print(json.dumps(response, ensure_ascii=False))

    write_audit(
        request_id=request_id,
        platform=args.platform,
        original_text_hash=original_hash,
        command_intent=command["command_intent"],
        allowed=exit_code == 0,
        refused_reason="" if exit_code == 0 else "wrapper_failed",
        date=command["date"],
        mode=command["mode"],
        auto_build=command["auto_build"],
        exit_code=exit_code,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
