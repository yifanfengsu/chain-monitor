#!/usr/bin/env python3
"""Read-only canonical daily report schema checks for Hermes ops."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hermes_lp_diagnose as lpdiag  # noqa: E402


REQUIRED_FIELDS = (
    "lp_signal_summary",
    "lp_stage_summary",
    "clmm_summary",
    "lp_suppression_summary",
    "lp_suppression_sample_replay_summary",
    "candidate_frontier_summary",
    "candidate_coverage_summary",
    "outcome_diagnosis_summary",
)


def validate_date(value: str) -> str:
    if len(value) != 10:
        raise ValueError("date must be YYYY-MM-DD")
    parsed = dt.date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("date must be YYYY-MM-DD")
    return value


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def read_report(logical_date: str, daily_dir: Path) -> tuple[dict[str, Any], str, list[str]]:
    path = daily_dir / f"daily_report_{logical_date}.json"
    if not path.exists():
        return {}, "missing", [f"daily_report_missing:{path.as_posix()}"]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {}, "unreadable", [f"daily_report_unreadable:{exc.__class__.__name__}"]
    if not isinstance(payload, dict):
        return {}, "unreadable", ["daily_report_schema_non_dict"]
    return payload, "present", []


def sqlite_lp_counts(logical_date: str, db_path: Path) -> tuple[dict[str, int], list[str]]:
    # lpdiag.open_ro_sqlite uses URI ?mode=ro through sqlite_store.
    warnings: list[str] = []
    start_ts, end_ts = lpdiag.logical_window(logical_date)
    counts = {"signals": 0, "raw_events": 0, "parsed_events": 0, "delivery_audit": 0}
    if not db_path.exists():
        warnings.append("sqlite_missing:data/chain_monitor.sqlite")
        return counts, warnings
    try:
        conn = lpdiag.open_ro_sqlite(db_path)
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_open_failed:{exc.__class__.__name__}")
        return counts, warnings
    try:
        signals = lpdiag.signals_stats(conn, start_ts, end_ts, warnings)
        raw_events = lpdiag.event_stats(
            conn,
            "raw_events",
            ("captured_at", "created_at", "updated_at"),
            "raw_json",
            ("raw_kind", "listener_scan_path", "pool_address"),
            ("pool_address",),
            start_ts,
            end_ts,
            warnings,
        )
        parsed_events = lpdiag.event_stats(
            conn,
            "parsed_events",
            ("parsed_at", "created_at", "updated_at"),
            "parsed_json",
            ("parsed_kind", "role_group", "lp_alert_stage_candidate", "pool_address"),
            ("lp_alert_stage_candidate", "pool_address"),
            start_ts,
            end_ts,
            warnings,
        )
        delivery = lpdiag.delivery_stats(conn, start_ts, end_ts, warnings)
        counts = {
            "signals": lpdiag.safe_int(signals.get("lp_like_any")),
            "raw_events": lpdiag.safe_int(raw_events.get("lp_like_any")),
            "parsed_events": lpdiag.safe_int(parsed_events.get("lp_like_any")),
            "delivery_audit": lpdiag.safe_int(delivery.get("lp_like_total")),
        }
    finally:
        conn.close()
    return counts, warnings


def lp_missing_reason(payload: dict[str, Any], counts: dict[str, int], missing_fields: list[str]) -> str:
    if not missing_fields:
        return str(payload.get("lp_missing_reason") or "none")
    if "lp_signal_summary" in missing_fields and counts.get("signals", 0) > 0:
        return "report_mapping_missing"
    if counts.get("raw_events", 0) + counts.get("parsed_events", 0) > 0 and counts.get("signals", 0) == 0:
        return "lp_analyzer_or_gate_missing"
    if all(int(value or 0) == 0 for value in counts.values()):
        return "no_lp_samples_or_coverage_gap"
    reason = str(payload.get("lp_missing_reason") or "").strip()
    return reason or "unknown"


def run(logical_date: str, db_path: Path, daily_dir: Path) -> int:
    payload, status, warnings = read_report(logical_date, daily_dir)
    counts, sqlite_warnings = sqlite_lp_counts(logical_date, db_path)
    warnings.extend(sqlite_warnings)
    field_status = {field: ("present" if field in payload else "missing") for field in REQUIRED_FIELDS}
    missing = [field for field, state in field_status.items() if state == "missing"]
    reason = lp_missing_reason(payload, counts, missing)
    candidate_frontier = as_dict(payload.get("candidate_frontier_summary"))
    candidate_coverage = as_dict(payload.get("candidate_coverage_summary"))
    outcome_diagnosis = as_dict(payload.get("outcome_diagnosis_summary"))
    if missing:
        schema_check = reason if reason != "unknown" else "missing_fields"
    elif not candidate_frontier:
        schema_check = "candidate_frontier_missing"
    elif not candidate_coverage:
        schema_check = "candidate_coverage_missing"
    elif not outcome_diagnosis:
        schema_check = "outcome_diagnosis_missing"
    else:
        schema_check = "ok"

    print(f"Chain Monitor 日报结构检查｜{logical_date}")
    print(f"daily_report_status={status}")
    print("required_fields=" + "；".join(f"{field}={state}" for field, state in field_status.items()))
    print(
        "sqlite_lp_like_counts="
        f"signals={counts.get('signals', 0)} "
        f"raw_events={counts.get('raw_events', 0)} "
        f"parsed_events={counts.get('parsed_events', 0)} "
        f"delivery_audit={counts.get('delivery_audit', 0)}"
    )
    print(f"lp_missing_reason={reason}")
    print(f"candidate_frontier_status={'present' if candidate_frontier else 'missing'}")
    print(f"candidate_coverage_status={'present' if candidate_coverage else 'missing'}")
    print(f"outcome_diagnosis_status={'present' if outcome_diagnosis else 'missing'}")
    print(f"schema_check={schema_check}")
    if warnings:
        print("limitations=" + "；".join(dict.fromkeys(warnings)))
    print("说明=只读结构检查，不修改 DB、archive 或 gate。")
    return 0 if status == "present" else 1


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check canonical daily report schema.")
    parser.add_argument("--date", required=True, help="Beijing logical date YYYY-MM-DD")
    parser.add_argument("--db-path", default=os.environ.get("HERMES_SCHEMA_CHECK_DB_PATH", "data/chain_monitor.sqlite"))
    parser.add_argument("--daily-dir", default=os.environ.get("HERMES_SCHEMA_CHECK_DAILY_DIR", "reports/daily"))
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        logical_date = validate_date(args.date)
    except ValueError as exc:
        print(f"error: invalid_date:{exc}", file=sys.stderr)
        return 2
    return run(logical_date, Path(args.db_path), Path(args.daily_dir))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
