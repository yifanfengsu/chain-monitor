#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import shutil
import subprocess
import sys
from collections import Counter
from datetime import UTC as PY_UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

THIS_DIR = Path(__file__).resolve().parent
ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
REPORTS_DIR = ROOT / "reports"
DAILY_REPORT_DIR = REPORTS_DIR / "daily"
LEGACY_REPORTS_DIR = REPORTS_DIR / "legacy"

if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
if str(LEGACY_REPORTS_DIR) not in sys.path:
    sys.path.insert(0, str(LEGACY_REPORTS_DIR))

import report_data_loader  # noqa: E402
from legacy.generate_overnight_run_analysis_latest import (  # noqa: E402
    BJ_TZ,
    UTC,
    compute_archive_integrity,
    compute_asset_market_states,
    compute_candidate_tradeable_summary,
    compute_final_trading_output_summary,
    compute_majors,
    compute_market_context,
    compute_no_trade_lock_summary,
    compute_outcome_price_sources,
    compute_prealert_lifecycle_summary,
    compute_prealerts,
    compute_quality_and_fastlane,
    compute_telegram_suppression,
    compute_trade_actions,
    compute_trade_opportunities,
    fast_archive_ts_from_line,
    fmt_ts,
    join_lp_rows,
    load_quality_cache,
    load_signals,
    load_trade_opportunity_cache,
    load_runtime_config,
    normalize_opportunity_summary,
    rate,
    stream_case_followups,
    stream_cases,
    stream_delivery_audit,
    to_int,
)

DAILY_REPORT_MAX_SEGMENT_GAP_SEC = 1800
TIMEZONE_NAME = "Asia/Shanghai"
WINDOW_NAMES = ("30s", "60s", "300s")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate canonical daily report")
    parser.add_argument("--date", help="Logical Beijing date YYYY-MM-DD")
    parser.add_argument("--start-date", help="Start logical Beijing date YYYY-MM-DD for range rebuild")
    parser.add_argument("--end-date", help="End logical Beijing date YYYY-MM-DD for range rebuild")
    return parser


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("DATE must be YYYY-MM-DD") from exc


def _date_range(start_value: str, end_value: str) -> list[str]:
    start = _parse_date(start_value)
    end = _parse_date(end_value)
    if end < start:
        raise argparse.ArgumentTypeError("END_DATE must be greater than or equal to START_DATE")
    values: list[str] = []
    current = start
    while current <= end:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _fmt_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), PY_UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_beijing(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), BJ_TZ).strftime("%Y-%m-%d %H:%M:%S UTC+8")


def build_logical_day_window(logical_date: str) -> dict[str, Any]:
    day = _parse_date(logical_date)
    start_bj = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=BJ_TZ)
    end_bj = start_bj + timedelta(days=1) - timedelta(seconds=1)
    start_ts = int(start_bj.astimezone(PY_UTC).timestamp())
    end_ts = int(end_bj.astimezone(PY_UTC).timestamp())
    return {
        "logical_date": logical_date,
        "timezone": TIMEZONE_NAME,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "logical_window_start_utc": _fmt_utc(start_ts),
        "logical_window_end_utc": _fmt_utc(end_ts),
        "logical_window_start_beijing": _fmt_beijing(start_ts),
        "logical_window_end_beijing": _fmt_beijing(end_ts),
        "wall_clock_duration_hours": round((end_ts - start_ts + 1) / 3600.0, 2),
    }


def _row_ts(row: dict[str, Any]) -> int | None:
    for key in (
        "archive_ts",
        "archive_written_at",
        "captured_at",
        "parsed_at",
        "created_at",
        "updated_at",
        "notifier_sent_at",
    ):
        value = to_int(row.get(key))
        if value is not None:
            return value
    return None


def _collect_timestamp_entries(
    datasets: dict[str, list[dict[str, Any]]],
    *,
    start_ts: int,
    end_ts: int,
) -> list[tuple[int, str]]:
    entries: list[tuple[int, str]] = []
    for source_name, rows in datasets.items():
        for row in rows:
            ts = _row_ts(row)
            if ts is None or ts < start_ts or ts > end_ts:
                continue
            entries.append((int(ts), source_name))
    return sorted(entries, key=lambda item: item[0])


def build_segment_summary(
    timestamp_entries: list[tuple[int, str]],
    *,
    logical_window_start_ts: int,
    logical_window_end_ts: int,
    max_gap_sec: int = DAILY_REPORT_MAX_SEGMENT_GAP_SEC,
) -> dict[str, Any]:
    entries = sorted(
        [
            (int(ts), str(source))
            for ts, source in timestamp_entries
            if logical_window_start_ts <= int(ts) <= logical_window_end_ts
        ],
        key=lambda item: item[0],
    )
    wall_clock_duration_hours = round((int(logical_window_end_ts) - int(logical_window_start_ts) + 1) / 3600.0, 2)
    if not entries:
        return {
            "segment_count": 0,
            "segments": [],
            "selected_segments": [],
            "active_duration_hours": 0.0,
            "wall_clock_duration_hours": wall_clock_duration_hours,
            "max_gap_hours": None,
            "gap_warnings": ["no_timestamped_rows_in_logical_day_window"],
        }

    segments: list[dict[str, Any]] = []
    current_start = entries[0][0]
    current_end = entries[0][0]
    current_counts: Counter[str] = Counter()
    prev_ts = entries[0][0]
    gaps: list[tuple[int, int, int]] = []

    for ts, source in entries:
        gap = int(ts) - int(prev_ts)
        if gap > max_gap_sec and current_counts:
            gaps.append((gap, prev_ts, ts))
            segments.append(
                _segment_payload(
                    current_start,
                    current_end,
                    current_counts,
                    index=len(segments) + 1,
                )
            )
            current_start = int(ts)
            current_counts = Counter()
        current_end = int(ts)
        current_counts[str(source)] += 1
        prev_ts = int(ts)

    if current_counts:
        segments.append(
            _segment_payload(
                current_start,
                current_end,
                current_counts,
                index=len(segments) + 1,
            )
        )

    active_duration_hours = round(sum(float(segment["duration_sec"]) for segment in segments) / 3600.0, 2)
    max_gap_sec_value = max((gap for gap, _, _ in gaps), default=0)
    gap_warnings = [
        f"gap_gt_{max_gap_sec}s:{_fmt_utc(start)}->{_fmt_utc(end)}"
        for gap, start, end in gaps[:10]
    ]
    if len(segments) > 1:
        gap_warnings.append("multiple_segments_do_not_treat_wall_clock_as_continuous_runtime")

    return {
        "segment_count": len(segments),
        "segments": segments,
        "selected_segments": [segment["segment_id"] for segment in segments],
        "active_duration_hours": active_duration_hours,
        "wall_clock_duration_hours": wall_clock_duration_hours,
        "max_gap_hours": round(max_gap_sec_value / 3600.0, 2) if max_gap_sec_value else 0.0,
        "gap_warnings": gap_warnings,
    }


def _segment_payload(start_ts: int, end_ts: int, source_counts: Counter[str], *, index: int) -> dict[str, Any]:
    duration_sec = max(0, int(end_ts) - int(start_ts))
    return {
        "segment_id": f"segment_{index}",
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "start_utc": _fmt_utc(start_ts),
        "end_utc": _fmt_utc(end_ts),
        "start_beijing": _fmt_beijing(start_ts),
        "end_beijing": _fmt_beijing(end_ts),
        "duration_sec": duration_sec,
        "duration_hours": round(duration_sec / 3600.0, 2),
        "row_count": int(sum(source_counts.values())),
        "source_counts": dict(sorted(source_counts.items())),
    }


def _safe_loader(name: str, func: Callable[..., report_data_loader.LoadResult], window: dict[str, Any]) -> report_data_loader.LoadResult:
    try:
        return func(window=window, compare_archive=False)
    except Exception as exc:
        return report_data_loader.LoadResult(
            rows=[],
            source="unavailable",
            row_count=0,
            warnings=[f"{name}_load_failed:{exc}"],
            fallback_used=False,
            mismatch_info={"loader": name, "mismatch": None, "match_rate": None},
        )


def _light_sqlite_dataset(loader_key: str, *, window: dict[str, Any]) -> report_data_loader.LoadResult | None:
    config = report_data_loader.DB_TABLES[loader_key]
    table = str(config.get("table") or "")
    time_column = str(config.get("time_column") or "")
    if not table or not time_column:
        return None
    conn = report_data_loader._connect()
    if conn is None:
        return None
    try:
        if not report_data_loader._db_table_exists(conn, table):
            return report_data_loader.LoadResult(
                rows=[],
                source="unavailable",
                row_count=0,
                warnings=[f"db_table_missing:{table}"],
                fallback_used=False,
                mismatch_info={"loader": loader_key, "sqlite_rows": 0, "mismatch": None},
            )
        start_ts, end_ts = int(window["start_ts"]), int(window["end_ts"])
        row = conn.execute(
            f"SELECT COUNT(*) AS count, MIN({time_column}) AS start_ts, MAX({time_column}) AS end_ts "
            f"FROM {table} WHERE {time_column} >= ? AND {time_column} <= ?",
            (start_ts, end_ts),
        ).fetchone()
        count = int(row["count"] or 0) if row else 0
        rows: list[dict[str, Any]] = []
        if loader_key in {"raw_events", "parsed_events", "signals", "delivery_audit", "case_followups"} and count > 0:
            for ts_row in conn.execute(
                f"SELECT {time_column} AS ts FROM {table} WHERE {time_column} >= ? AND {time_column} <= ? ORDER BY {time_column}",
                (start_ts, end_ts),
            ):
                ts = to_int(ts_row["ts"])
                if ts is not None:
                    rows.append({"archive_ts": ts})
        return report_data_loader.LoadResult(
            rows=rows,
            source="sqlite" if count > 0 else "unavailable",
            row_count=count,
            warnings=[],
            fallback_used=False,
            mismatch_info={
                "loader": loader_key,
                "sqlite_rows": count,
                "match_rate": None,
                "mismatch": None,
                "start_ts": to_int(row["start_ts"]) if row else None,
                "end_ts": to_int(row["end_ts"]) if row else None,
            },
        )
    finally:
        conn.close()


def _light_archive_dataset(loader_key: str, *, window: dict[str, Any]) -> report_data_loader.LoadResult:
    config = report_data_loader.DB_TABLES[loader_key]
    archive_category = str(config.get("archive") or "")
    cache_name = str(config.get("cache") or "")
    if not archive_category:
        return report_data_loader.LoadResult(
            rows=[],
            source="cache" if cache_name else "unavailable",
            row_count=0,
            warnings=[] if cache_name else [f"no_archive_source:{loader_key}"],
            fallback_used=bool(cache_name),
            mismatch_info={"loader": loader_key, "mismatch": None},
        )
    start_ts, end_ts = int(window["start_ts"]), int(window["end_ts"])
    archive_dates = report_data_loader._archive_date_keys_for_window(window)
    rows: list[dict[str, Any]] = []
    count = 0
    warnings: list[str] = []
    for path in report_data_loader.archive_paths(archive_category):
        if archive_dates and report_data_loader.archive_date_key(path) not in archive_dates:
            continue
        try:
            with report_data_loader.open_archive_text(path) as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    ts = fast_archive_ts_from_line(line)
                    if ts is None or ts < start_ts or ts > end_ts:
                        continue
                    count += 1
                    if loader_key in {"raw_events", "parsed_events", "signals", "delivery_audit", "case_followups"}:
                        rows.append({"archive_ts": ts})
        except OSError as exc:
            warnings.append(f"archive_read_failed:{archive_category}:{path.name}:{exc}")
    return report_data_loader.LoadResult(
        rows=rows,
        source="archive" if count > 0 else "unavailable",
        row_count=count,
        warnings=warnings,
        fallback_used=True,
        mismatch_info={"loader": loader_key, "archive_rows": count, "mismatch": None},
    )


def _light_dataset(loader_key: str, *, window: dict[str, Any]) -> report_data_loader.LoadResult:
    try:
        sqlite_result = _light_sqlite_dataset(loader_key, window=window)
        if sqlite_result is not None and sqlite_result.row_count > 0:
            return sqlite_result
        archive_result = _light_archive_dataset(loader_key, window=window)
        if archive_result.row_count > 0:
            if sqlite_result is not None:
                archive_result.warnings.extend(sqlite_result.warnings)
            return archive_result
        return sqlite_result or archive_result
    except Exception as exc:
        return report_data_loader.LoadResult(
            rows=[],
            source="unavailable",
            row_count=0,
            warnings=[f"{loader_key}_light_load_failed:{exc}"],
            fallback_used=False,
            mismatch_info={"loader": loader_key, "mismatch": None},
        )


def _latest_available_logical_date() -> str:
    latest_ts: int | None = None
    for loader_key in ("signals", "raw_events", "parsed_events", "delivery_audit"):
        config = report_data_loader.DB_TABLES.get(loader_key, {})
        table = str(config.get("table") or "")
        time_column = str(config.get("time_column") or "")
        if table and time_column:
            conn = report_data_loader._connect()
            if conn is not None:
                try:
                    if report_data_loader._db_table_exists(conn, table):
                        row = conn.execute(f"SELECT MAX({time_column}) AS latest_ts FROM {table}").fetchone()
                        ts = to_int(row["latest_ts"]) if row else None
                        if ts is not None and (latest_ts is None or ts > latest_ts):
                            latest_ts = int(ts)
                finally:
                    conn.close()
        archive_category = str(config.get("archive") or "")
        if not archive_category:
            continue
        for path in report_data_loader.archive_paths(archive_category):
            try:
                with report_data_loader.open_archive_text(path) as handle:
                    for raw_line in handle:
                        ts = fast_archive_ts_from_line(raw_line)
                        if ts is not None and (latest_ts is None or ts > latest_ts):
                            latest_ts = int(ts)
            except OSError:
                continue
    if latest_ts is None:
        return datetime.now(BJ_TZ).date().isoformat()
    return datetime.fromtimestamp(latest_ts, BJ_TZ).date().isoformat()


def _load_daily_datasets(window: dict[str, Any]) -> dict[str, report_data_loader.LoadResult]:
    loader_keys = (
        "raw_events",
        "parsed_events",
        "signals",
        "delivery_audit",
        "case_followups",
        "asset_cases",
        "asset_market_states",
        "trade_opportunities",
        "outcomes",
        "telegram_deliveries",
        "market_context_attempts",
    )
    return {name: _light_dataset(name, window=window) for name in loader_keys}


def _source_summary(
    loader_results: dict[str, report_data_loader.LoadResult],
    sqlite_source: dict[str, Any],
) -> dict[str, Any]:
    components = {name: result.source for name, result in sorted(loader_results.items())}
    useful_sources = {source for source in components.values() if source not in {"", "unavailable"}}
    data_source = "mixed" if len(useful_sources) > 1 else next(iter(useful_sources), "unavailable")
    source_warnings = sorted(
        set(
            warning
            for result in loader_results.values()
            for warning in result.warnings
            if warning
        )
        | set(str(item) for item in list(sqlite_source.get("warnings") or []) if item)
    )
    mismatch_warnings = sorted(
        set(
            warning
            for warning in source_warnings
            if "mismatch" in warning
        )
        | set(str(item) for item in list(sqlite_source.get("mismatch_warnings") or []) if item)
    )
    return {
        "report_data_source": data_source,
        "data_source": data_source,
        "source_components": components,
        "row_counts": {name: result.row_count for name, result in sorted(loader_results.items())},
        "archive_fallback_used": any(result.fallback_used or result.source in {"archive", "cache"} for result in loader_results.values()),
        "source_warnings": source_warnings,
        "db_archive_mismatch_warnings": mismatch_warnings,
        "mismatch_warnings": mismatch_warnings,
        "db_archive_mirror_match_rate": sqlite_source.get("db_archive_mirror_match_rate"),
        "sqlite_health": sqlite_source.get("sqlite_health", {}),
        "loader_results": {
            name: {
                "source": result.source,
                "row_count": result.row_count,
                "warnings": result.warnings,
                "fallback_used": result.fallback_used,
                "mismatch_info": result.mismatch_info,
                "compressed_archive_rows": result.compressed_archive_rows,
            }
            for name, result in sorted(loader_results.items())
        },
    }


def _archive_health(sqlite_source: dict[str, Any]) -> dict[str, Any]:
    return {
        "archive_base_dir": sqlite_source.get("archive_base_dir"),
        "archive_base_dir_exists": sqlite_source.get("archive_base_dir_exists"),
        "archive_dirs_by_category": sqlite_source.get("archive_dirs_by_category", {}),
        "archive_rows_by_category": sqlite_source.get("archive_rows_by_category", {}),
        "compressed_archive_rows": sqlite_source.get("compressed_archive_rows", {}),
        "compressed_archive_detected": sqlite_source.get("compressed_archive_detected"),
        "archive_fallback_used": bool(sqlite_source.get("archive_fallback_used")),
    }


def _empty_cli_major() -> dict[str, Any]:
    expected = [
        f"{asset}/{quote}"
        for asset in ("ETH", "BTC", "SOL")
        for quote in ("USDT", "USDC")
    ]
    return {
        "available": False,
        "reason": "date_scoped_daily_report_uses_window_rows",
        "expected_major_pairs": expected,
        "covered_major_pairs": [],
        "missing_expected_pairs": [],
        "configured_but_disabled_major_pools": [],
        "malformed_major_pool_entries": [],
    }


def _build_run_overview(
    *,
    window: dict[str, Any],
    loader_results: dict[str, report_data_loader.LoadResult],
    window_signal_rows: list[dict[str, Any]],
    lp_rows: list[dict[str, Any]],
    followup_summary: dict[str, Any],
) -> dict[str, Any]:
    asset_case_ids = {
        str(row.get("asset_case_id") or "")
        for row in lp_rows
        if str(row.get("asset_case_id") or "").strip()
    }
    delivered = sum(1 for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    return {
        "analysis_window_start": window["logical_window_start_utc"],
        "analysis_window_end": window["logical_window_end_utc"],
        "analysis_window_start_utc": window["logical_window_start_utc"],
        "analysis_window_end_utc": window["logical_window_end_utc"],
        "analysis_window_start_beijing": window["logical_window_start_beijing"],
        "analysis_window_end_beijing": window["logical_window_end_beijing"],
        "duration_hours": window["wall_clock_duration_hours"],
        "wall_clock_duration_hours": window["wall_clock_duration_hours"],
        "total_raw_events": loader_results["raw_events"].row_count,
        "total_parsed_events": loader_results["parsed_events"].row_count,
        "total_signal_rows": len(window_signal_rows),
        "lp_signal_rows": len(lp_rows),
        "delivered_lp_signals": int(delivered),
        "suppressed_lp_signals": max(len(lp_rows) - int(delivered), 0),
        "asset_case_count": len(asset_case_ids),
        "case_followup_count": int(followup_summary.get("followup_rows") or loader_results["case_followups"].row_count or 0),
    }


def _stage_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter(str(row.get("lp_alert_stage") or "") for row in lp_rows if str(row.get("lp_alert_stage") or "").strip())
    total = len(lp_rows)
    return {
        "lp_signal_rows": total,
        "stage_distribution": dict(sorted(counter.items())),
        "stage_distribution_pct": {key: rate(value, total) for key, value in sorted(counter.items())},
        "prealert_count": counter.get("prealert", 0),
        "confirm_count": counter.get("confirm", 0),
        "climax_count": counter.get("climax", 0),
        "exhaustion_risk_count": counter.get("exhaustion_risk", 0),
    }


def _normalize_daily_summaries(
    *,
    telegram: dict[str, Any],
    prealerts: dict[str, Any],
    prealert_lifecycle: dict[str, Any],
    asset_market_states: dict[str, Any],
    no_trade_lock: dict[str, Any],
    outcome: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    telegram = dict(telegram)
    before_after = dict(telegram.get("messages_before_after_suppression_estimate") or {})
    raw_lp = int(before_after.get("raw_lp_signals") or 0)
    sent = int(before_after.get("sent_telegram_messages") or 0)
    suppressed = int(telegram.get("total_suppressed") or max(raw_lp - sent, 0))
    telegram.setdefault("telegram_should_send_count", sent)
    telegram.setdefault("telegram_suppressed_count", suppressed)
    telegram.setdefault("telegram_suppression_ratio", rate(suppressed, raw_lp))
    telegram.setdefault("telegram_suppression_reasons", telegram.get("suppression_reasons", {}))
    telegram.setdefault("messages_before_suppression_estimate", raw_lp)
    telegram.setdefault("messages_after_suppression_actual", sent)
    telegram.setdefault("high_value_suppressed_count", 0)

    lifecycle = dict(prealert_lifecycle)
    lifecycle.setdefault("prealert_candidate_count", prealerts.get("prealert_candidate_count", 0))
    lifecycle.setdefault("prealert_gate_passed_count", prealerts.get("prealert_gate_passed_count", 0))
    lifecycle.setdefault("prealert_active_count", lifecycle.get("active_count", 0))
    lifecycle.setdefault("prealert_delivered_count", lifecycle.get("delivered_count", 0))
    lifecycle.setdefault("prealert_upgraded_to_confirm_count", lifecycle.get("upgraded_count", 0))
    lifecycle.setdefault("prealert_expired_count", lifecycle.get("expired_count", 0))

    states = dict(asset_market_states)
    states.setdefault("state_transition_count", states.get("state_change_count", 0))
    states.setdefault("final_state_by_asset", states.get("current_final_state_per_asset", {}))

    locks = dict(no_trade_lock)
    locks.setdefault("lock_suppressed_count", locks.get("suppressed_count", 0))
    locks.setdefault("lock_released_count", locks.get("release_count", 0))

    outcome = dict(outcome)
    outcome.setdefault("outcome_price_source_distribution", outcome.get("source_distribution", {}))
    outcome.setdefault("outcome_failure_reason_distribution", outcome.get("failure_reasons", {}))
    return telegram, lifecycle, states, locks, outcome


def _maturity_summary(opportunities: dict[str, Any]) -> dict[str, Any]:
    maturity = str(opportunities.get("verified_maturity") or "unknown")
    return {
        "verified_maturity": maturity,
        "verified_should_not_be_traded_reason": opportunities.get("verified_should_not_be_traded_reason") or "",
        "maturity_reasons": opportunities.get("maturity_reasons") or [],
        "candidate_is_trade_signal": False,
        "verified_is_mature_trade_signal": maturity == "mature",
    }


def _classify_daily_state(opportunities: dict[str, Any], maturity: dict[str, Any]) -> str:
    verified = int(opportunities.get("opportunity_verified_count") or 0)
    candidate = int(opportunities.get("opportunity_candidate_count") or 0)
    if verified > 0 and maturity.get("verified_maturity") == "mature":
        return "opportunity"
    if candidate > 0 or verified > 0:
        return "filtering"
    return "research"


def _build_limitations(
    *,
    segment_summary: dict[str, Any],
    data_source_summary: dict[str, Any],
    opportunities: dict[str, Any],
    lp_rows: list[dict[str, Any]],
) -> list[str]:
    limitations: list[str] = []
    if not lp_rows:
        limitations.append("insufficient_lp_rows_in_logical_day_window")
    if int(segment_summary.get("segment_count") or 0) > 1:
        limitations.append("multiple_active_segments; report does not claim continuous runtime")
    limitations.extend(str(item) for item in data_source_summary.get("source_warnings") or [])
    limitations.extend(str(item) for item in data_source_summary.get("db_archive_mismatch_warnings") or [])
    if str(opportunities.get("verified_maturity") or "") == "immature":
        limitations.append("verified_maturity=immature; VERIFIED must not be treated as mature trade signal")
    if int(opportunities.get("opportunity_candidate_count") or 0) > 0:
        limitations.append("CANDIDATE is not a trade signal")
    return sorted(set(item for item in limitations if item))


def _build_key_findings(summary: dict[str, Any]) -> list[str]:
    run = summary["run_overview"]
    opp = summary["trade_opportunity_summary"]
    source = summary["data_source_summary"]
    segment = summary["segment_summary"]
    return [
        f"logical_date={summary['logical_date']} source={source.get('data_source')} lp_rows={run.get('lp_signal_rows')}",
        f"segments={segment.get('segment_count')} active_hours={segment.get('active_duration_hours')} wall_clock_hours={segment.get('wall_clock_duration_hours')}",
        f"opportunities: candidate={opp.get('opportunity_candidate_count', 0)} verified={opp.get('opportunity_verified_count', 0)} blocked={opp.get('opportunity_blocked_count', 0)}",
        f"verified_maturity={summary['maturity_summary'].get('verified_maturity', 'unknown')}; CANDIDATE is not a trade signal",
    ]


def _build_key_risks(summary: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    if summary["segment_summary"].get("gap_warnings"):
        risks.extend(summary["segment_summary"]["gap_warnings"][:3])
    if summary["data_source_summary"].get("db_archive_mismatch_warnings"):
        risks.extend(summary["data_source_summary"]["db_archive_mismatch_warnings"][:3])
    if summary["maturity_summary"].get("verified_maturity") == "immature":
        risks.append("VERIFIED maturity is immature and must remain research/filter evidence only")
    return risks[:8]


def _build_next_actions(summary: dict[str, Any]) -> list[str]:
    actions = [
        "继续用 canonical daily report 做日级复盘，不再混用旧三件套主报告。",
        "优先补 outcome 30s/60s/300s 完整性和 blocker 后验样本。",
    ]
    if summary["major_coverage_summary"].get("missing_major_pairs"):
        actions.append("继续补 majors 覆盖，优先 BTC/SOL 主池。")
    if summary["data_source_summary"].get("db_archive_mismatch_warnings"):
        actions.append("先处理 SQLite/archive mismatch，再解释指标变化。")
    return actions


def build_daily_summary(logical_date: str) -> dict[str, Any]:
    window = build_logical_day_window(logical_date)
    loader_window = {"start_ts": window["start_ts"], "end_ts": window["end_ts"]}
    loader_results = _load_daily_datasets(loader_window)
    timestamp_entries = _collect_timestamp_entries(
        {name: result.rows for name, result in loader_results.items()},
        start_ts=window["start_ts"],
        end_ts=window["end_ts"],
    )
    segment_summary = build_segment_summary(
        timestamp_entries,
        logical_window_start_ts=window["start_ts"],
        logical_window_end_ts=window["end_ts"],
    )

    sqlite_source = report_data_loader.report_source_summary(window=loader_window, fast=True)
    data_source_summary = _source_summary(loader_results, sqlite_source)
    archive_health = _archive_health(sqlite_source)

    runtime_config = load_runtime_config()
    signal_rows, _signal_inventory = load_signals(window=loader_window, archive_dates=report_data_loader._archive_date_keys_for_window(loader_window))
    quality_rows, quality_by_signal, _quality_inventory = load_quality_cache()
    trade_opportunity_cache, _trade_opportunity_inventory = load_trade_opportunity_cache()
    window_signal_rows, _all_lp_rows, lp_rows = join_lp_rows(
        signal_rows,
        quality_by_signal,
        int(window["start_ts"]),
        int(window["end_ts"]),
    )
    _ = quality_rows

    signal_ids = {str(row.get("signal_id") or "") for row in lp_rows if row.get("signal_id")}
    delivered_signal_ids = {
        str(row.get("signal_id") or "")
        for row in lp_rows
        if row.get("signal_id") and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    }
    archive_dates = report_data_loader._archive_date_keys_for_window(loader_window)
    _delivery_inventory, delivery_summary = stream_delivery_audit(
        int(window["start_ts"]),
        int(window["end_ts"]),
        signal_ids,
        delivered_signal_ids,
        archive_dates=archive_dates,
    )
    _cases_inventory, cases_summary = stream_cases(
        int(window["start_ts"]),
        int(window["end_ts"]),
        signal_ids,
        delivered_signal_ids,
        archive_dates=archive_dates,
    )
    _followup_inventory, followup_summary = stream_case_followups(
        int(window["start_ts"]),
        int(window["end_ts"]),
        delivered_signal_ids,
        archive_dates=archive_dates,
    )

    run_overview = _build_run_overview(
        window=window,
        loader_results=loader_results,
        window_signal_rows=window_signal_rows,
        lp_rows=lp_rows,
        followup_summary=followup_summary,
    )
    lp_stage_summary = _stage_summary(lp_rows)
    trade_action_summary = compute_trade_actions(lp_rows)
    asset_market_state_summary = compute_asset_market_states(lp_rows)
    no_trade_lock_summary = compute_no_trade_lock_summary(lp_rows)
    prealert_rows_summary = compute_prealerts(lp_rows, set(_empty_cli_major()["expected_major_pairs"]), {})
    prealert_lifecycle_summary = compute_prealert_lifecycle_summary(lp_rows)
    final_trading_output_summary = compute_final_trading_output_summary(lp_rows)
    trade_opportunity_summary = normalize_opportunity_summary(compute_trade_opportunities(trade_opportunity_cache, lp_rows))
    outcome_summary = compute_outcome_price_sources(lp_rows)
    telegram_suppression_summary = compute_telegram_suppression(lp_rows)
    market_context_health = {
        "window": compute_market_context(lp_rows),
        "quality_reports_cli": {"available": False, "reason": "date_scoped_daily_report_uses_window_rows"},
    }
    major_coverage_summary = compute_majors(lp_rows, _empty_cli_major(), runtime_config)
    archive_integrity_summary = compute_archive_integrity(lp_rows, delivery_summary, cases_summary, followup_summary)
    candidate_verified_summary = compute_candidate_tradeable_summary(lp_rows)
    quality_and_fastlane = compute_quality_and_fastlane(lp_rows, {})

    (
        telegram_suppression_summary,
        prealert_lifecycle_summary,
        asset_market_state_summary,
        no_trade_lock_summary,
        outcome_summary,
    ) = _normalize_daily_summaries(
        telegram=telegram_suppression_summary,
        prealerts=prealert_rows_summary,
        prealert_lifecycle=prealert_lifecycle_summary,
        asset_market_states=asset_market_state_summary,
        no_trade_lock=no_trade_lock_summary,
        outcome=outcome_summary,
    )

    blocker_summary = {
        "hard_blocker_distribution": trade_opportunity_summary.get("hard_blocker_distribution")
        or trade_opportunity_summary.get("opportunity_hard_blocker_distribution")
        or {},
        "verification_blocker_distribution": trade_opportunity_summary.get("verification_blocker_distribution", {}),
        "top_blockers": trade_opportunity_summary.get("top_blockers", []),
        "blocker_saved_rate": trade_opportunity_summary.get("blocker_saved_rate"),
        "blocker_false_block_rate": trade_opportunity_summary.get("blocker_false_block_rate"),
    }
    maturity_summary = _maturity_summary(trade_opportunity_summary)

    analysis_window = {
        **window,
        "start_utc": window["logical_window_start_utc"],
        "end_utc": window["logical_window_end_utc"],
        "start_bj": window["logical_window_start_beijing"],
        "end_bj": window["logical_window_end_beijing"],
        "duration_hours": window["wall_clock_duration_hours"],
        "selection_reason": "canonical Beijing logical day; no cross-gap longest-window selection",
    }

    summary: dict[str, Any] = {
        "report_type": "daily_canonical",
        "logical_date": logical_date,
        "timezone": TIMEZONE_NAME,
        "logical_window_start_utc": window["logical_window_start_utc"],
        "logical_window_end_utc": window["logical_window_end_utc"],
        "logical_window_start_beijing": window["logical_window_start_beijing"],
        "logical_window_end_beijing": window["logical_window_end_beijing"],
        "analysis_window": analysis_window,
        "segment_summary": segment_summary,
        "segment_count": segment_summary["segment_count"],
        "segments": segment_summary["segments"],
        "active_duration_hours": segment_summary["active_duration_hours"],
        "wall_clock_duration_hours": segment_summary["wall_clock_duration_hours"],
        "max_gap_hours": segment_summary["max_gap_hours"],
        "gap_warnings": segment_summary["gap_warnings"],
        "data_sources": {
            name: {
                "source": result.source,
                "row_count": result.row_count,
                "fallback_used": result.fallback_used,
                "warnings": result.warnings,
            }
            for name, result in sorted(loader_results.items())
        },
        "data_source_summary": data_source_summary,
        "data_source": data_source_summary["data_source"],
        "archive_fallback_used": bool(data_source_summary["archive_fallback_used"]),
        "source_warnings": data_source_summary["source_warnings"],
        "db_archive_mismatch_warnings": data_source_summary["db_archive_mismatch_warnings"],
        "sqlite_health": sqlite_source.get("sqlite_health", {}),
        "archive_health": archive_health,
        "run_overview": run_overview,
        "lp_stage_summary": lp_stage_summary,
        "trade_action_summary": trade_action_summary,
        "asset_market_state_summary": asset_market_state_summary,
        "no_trade_lock_summary": no_trade_lock_summary,
        "trade_opportunity_summary": trade_opportunity_summary,
        "candidate_verified_summary": candidate_verified_summary,
        "candidate_tradeable_summary": candidate_verified_summary,
        "blocker_summary": blocker_summary,
        "outcome_summary": outcome_summary,
        "outcome_source_summary": outcome_summary,
        "outcome_price_source_summary": outcome_summary,
        "telegram_suppression_summary": telegram_suppression_summary,
        "prealert_lifecycle_summary": prealert_lifecycle_summary,
        "prealert_summary": prealert_rows_summary,
        "major_coverage_summary": major_coverage_summary,
        "majors_coverage_summary": major_coverage_summary,
        "market_context_health": market_context_health,
        "archive_integrity_summary": archive_integrity_summary,
        "final_trading_output_summary": final_trading_output_summary,
        "quality_and_fastlane_summary": quality_and_fastlane,
        "maturity_summary": maturity_summary,
    }
    summary["limitations"] = _build_limitations(
        segment_summary=segment_summary,
        data_source_summary=data_source_summary,
        opportunities=trade_opportunity_summary,
        lp_rows=lp_rows,
    )
    summary["key_findings"] = _build_key_findings(summary)
    summary["key_risks"] = _build_key_risks(summary)
    summary["next_actions"] = _build_next_actions(summary)
    summary["daily_state_classification"] = _classify_daily_state(trade_opportunity_summary, maturity_summary)
    return summary


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return "n/a"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def build_markdown(summary: dict[str, Any]) -> str:
    segment = summary["segment_summary"]
    run = summary["run_overview"]
    maturity = summary["maturity_summary"]
    lines: list[str] = []
    lines.append(f"# Canonical Daily Report - {summary['logical_date']}")
    lines.append("")
    lines.append("## 1. 执行摘要")
    lines.append(f"- 逻辑日期: `{summary['logical_date']}` timezone=`{summary['timezone']}`")
    lines.append(f"- 系统状态: `{summary['daily_state_classification']}`")
    lines.append(f"- LP rows=`{run.get('lp_signal_rows')}` candidate=`{summary['trade_opportunity_summary'].get('opportunity_candidate_count', 0)}` verified=`{summary['trade_opportunity_summary'].get('opportunity_verified_count', 0)}` blocked=`{summary['trade_opportunity_summary'].get('opportunity_blocked_count', 0)}`")
    lines.append("- CANDIDATE 不是交易信号；canonical daily report 不是交易建议。")
    if maturity.get("verified_maturity") == "immature":
        lines.append("- VERIFIED maturity=immature，不能当成熟交易信号。")
    lines.append("")
    lines.append("## 2. 数据源与完整性")
    lines.append(f"- data_source=`{summary['data_source']}` archive_fallback_used=`{str(summary['archive_fallback_used']).lower()}`")
    lines.append(f"- source_components=`{_json_text(summary['data_source_summary'].get('source_components'))}`")
    lines.append(f"- raw={run.get('total_raw_events')} parsed={run.get('total_parsed_events')} signals={run.get('total_signal_rows')} lp={run.get('lp_signal_rows')} delivered={run.get('delivered_lp_signals')}")
    lines.append("")
    lines.append("## 3. 日期窗口与运行段")
    lines.append(f"- UTC: `{summary['logical_window_start_utc']} -> {summary['logical_window_end_utc']}`")
    lines.append(f"- Beijing: `{summary['logical_window_start_beijing']} -> {summary['logical_window_end_beijing']}`")
    lines.append(f"- wall_clock_duration_hours=`{segment['wall_clock_duration_hours']}` active_duration_hours=`{segment['active_duration_hours']}` segment_count=`{segment['segment_count']}` max_gap_hours=`{segment['max_gap_hours']}`")
    if int(segment["segment_count"]) > 1:
        lines.append("- 本日存在多个 active segments；报告不会声称连续运行 X 小时。")
    for item in segment["segments"][:8]:
        lines.append(f"- {item['segment_id']}: `{item['start_utc']} -> {item['end_utc']}` active=`{item['duration_hours']}h` rows=`{item['row_count']}` sources=`{_json_text(item.get('source_counts'))}`")
    lines.append("")
    lines.append("## 4. SQLite / archive 健康度")
    lines.append(f"- sqlite_initialized=`{summary['sqlite_health'].get('initialized', 'unknown')}` db_archive_mirror_match_rate=`{summary['data_source_summary'].get('db_archive_mirror_match_rate')}`")
    lines.append(f"- archive_rows=`{_json_text(summary['archive_health'].get('archive_rows_by_category'))}`")
    lines.append("")
    lines.append("## 5. Market context 健康度")
    lines.append(f"- window=`{_json_text(summary['market_context_health'].get('window'))}`")
    lines.append("")
    lines.append("## 6. LP / pool stage 总览")
    lines.append(f"- stage_distribution=`{_json_text(summary['lp_stage_summary'].get('stage_distribution'))}`")
    lines.append("")
    lines.append("## 7. Trade action 总览")
    lines.append(f"- trade_action_distribution=`{_json_text(summary['trade_action_summary'].get('trade_action_distribution'))}`")
    lines.append("")
    lines.append("## 8. Asset market state / NO_TRADE_LOCK")
    lines.append(f"- asset_state_distribution=`{_json_text(summary['asset_market_state_summary'].get('state_distribution'))}`")
    lines.append(f"- no_trade_lock=`{_json_text(summary['no_trade_lock_summary'])}`")
    lines.append("")
    lines.append("## 9. Trade opportunity 总览")
    lines.append(f"- opportunity_summary=`{_json_text({key: summary['trade_opportunity_summary'].get(key) for key in ('opportunity_none_count','opportunity_candidate_count','opportunity_verified_count','opportunity_blocked_count','opportunity_invalidated_count','opportunity_candidate_to_verified_rate','verified_maturity')})}`")
    lines.append("- BLOCKED / NO_TRADE_LOCK 的价值要结合 blocker/outcome 后验解释；没有机会不等于系统差。")
    lines.append("")
    lines.append("## 10. Candidate / Verified / Blocked 分析")
    lines.append(f"- candidate_verified_summary=`{_json_text(summary['candidate_verified_summary'])}`")
    lines.append(f"- blocker_summary=`{_json_text(summary['blocker_summary'])}`")
    lines.append("")
    lines.append("## 11. Outcome 30s/60s/300s 完整性")
    lines.append(f"- window_status=`{_json_text(summary['outcome_summary'].get('window_status'))}`")
    lines.append(f"- completed_rates=`{_json_text({name: summary['outcome_summary'].get(f'outcome_{name}_completed_rate') for name in WINDOW_NAMES})}`")
    lines.append("")
    lines.append("## 12. Telegram 降噪与 suppression")
    lines.append(f"- telegram=`{_json_text(summary['telegram_suppression_summary'])}`")
    lines.append("")
    lines.append("## 13. Prealert lifecycle")
    lines.append(f"- prealert=`{_json_text(summary['prealert_lifecycle_summary'])}`")
    lines.append("")
    lines.append("## 14. Major coverage")
    lines.append(f"- covered=`{_json_text(summary['major_coverage_summary'].get('covered_major_pairs'))}` missing=`{_json_text(summary['major_coverage_summary'].get('missing_major_pairs'))}`")
    lines.append("")
    lines.append("## 15. 风险与 limitations")
    for item in summary.get("limitations") or ["n/a"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 16. 今日结论：系统是研究/过滤/机会哪一类状态？")
    lines.append(f"- `{summary['daily_state_classification']}`")
    lines.append("")
    lines.append("## 17. 下一步建议")
    for item in summary.get("next_actions") or ["n/a"]:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def _add_metric(
    rows: list[dict[str, Any]],
    metric_group: str,
    metric_name: str,
    value: Any,
    *,
    sample_size: Any = "",
    asset: str = "",
    pair: str = "",
    window: str = "",
    notes: str = "",
) -> None:
    rows.append(
        {
            "metric_group": metric_group,
            "metric_name": metric_name,
            "value": _json_text(value),
            "sample_size": sample_size,
            "asset": asset,
            "pair": pair,
            "window": window,
            "notes": notes,
        }
    )


def build_csv_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    window_label = f"{summary['logical_window_start_utc']} -> {summary['logical_window_end_utc']}"
    for key in ("segment_count", "active_duration_hours", "wall_clock_duration_hours", "max_gap_hours"):
        _add_metric(rows, "segment", key, summary.get(key), window=window_label)
    for key, value in summary["run_overview"].items():
        _add_metric(rows, "run_overview", key, value, sample_size=summary["run_overview"].get("lp_signal_rows"), window=window_label)
    for group_name, payload in (
        ("lp_stage", summary["lp_stage_summary"]),
        ("trade_action", summary["trade_action_summary"]),
        ("asset_market_state", summary["asset_market_state_summary"]),
        ("no_trade_lock", summary["no_trade_lock_summary"]),
        ("trade_opportunity", summary["trade_opportunity_summary"]),
        ("candidate_verified", summary["candidate_verified_summary"]),
        ("blocker", summary["blocker_summary"]),
        ("outcome", summary["outcome_summary"]),
        ("telegram", summary["telegram_suppression_summary"]),
        ("prealert", summary["prealert_lifecycle_summary"]),
        ("major_coverage", summary["major_coverage_summary"]),
        ("market_context", summary["market_context_health"].get("window", {})),
        ("data_source", summary["data_source_summary"]),
    ):
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                _add_metric(rows, group_name, key, value, sample_size=summary["run_overview"].get("lp_signal_rows"), window=window_label)
            elif value not in (None, "", [], {}):
                _add_metric(rows, group_name, key, value, sample_size=summary["run_overview"].get("lp_signal_rows"), window=window_label)
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = io.StringIO()
    fieldnames = ["metric_group", "metric_name", "value", "sample_size", "asset", "pair", "window", "notes"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    path.write_text(output.getvalue(), encoding="utf-8")


def write_daily_artifacts(summary: dict[str, Any], *, output_dir: Path = DAILY_REPORT_DIR) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    logical_date = str(summary["logical_date"])
    markdown = build_markdown(summary)
    csv_rows = build_csv_rows(summary)
    dated_md = output_dir / f"daily_report_{logical_date}.md"
    dated_csv = output_dir / f"daily_report_{logical_date}.csv"
    dated_json = output_dir / f"daily_report_{logical_date}.json"
    latest_md = output_dir / "daily_report_latest.md"
    latest_csv = output_dir / "daily_report_latest.csv"
    latest_json = output_dir / "daily_report_latest.json"

    dated_md.write_text(markdown, encoding="utf-8")
    _write_csv(dated_csv, csv_rows)
    dated_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    shutil.copyfile(dated_md, latest_md)
    shutil.copyfile(dated_csv, latest_csv)
    shutil.copyfile(dated_json, latest_json)
    return {
        "dated_markdown": str(dated_md),
        "dated_csv": str(dated_csv),
        "dated_json": str(dated_json),
        "latest_markdown": str(latest_md),
        "latest_csv": str(latest_csv),
        "latest_json": str(latest_json),
    }


def generate_daily_report(logical_date: str | None = None, *, output_dir: Path = DAILY_REPORT_DIR) -> tuple[dict[str, Any], dict[str, str]]:
    selected_date = logical_date or _latest_available_logical_date()
    summary = build_daily_summary(selected_date)
    written = write_daily_artifacts(summary, output_dir=output_dir)
    return summary, written


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if bool(args.start_date) != bool(args.end_date):
        raise SystemExit("--start-date and --end-date must be provided together")
    outputs: list[dict[str, Any]] = []
    if args.start_date and args.end_date:
        for logical_date in _date_range(args.start_date, args.end_date):
            result = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--date", logical_date],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                sys.stderr.write(f"daily_report_range_failed:{logical_date}:exit={result.returncode}\n")
                if result.stdout:
                    sys.stdout.write(result.stdout)
                if result.stderr:
                    sys.stderr.write(result.stderr)
                return result.returncode
            try:
                payload = json.loads(result.stdout.strip().splitlines()[-1])
                reports = payload.get("reports") if isinstance(payload, dict) else []
                if isinstance(reports, list):
                    outputs.extend(item for item in reports if isinstance(item, dict))
            except (IndexError, json.JSONDecodeError):
                outputs.append({"logical_date": logical_date, "status": "ok"})
        print(json.dumps({"status": "ok", "reports": outputs}, ensure_ascii=False))
        return 0

    dates = [args.date or _latest_available_logical_date()]
    for logical_date in dates:
        summary, written = generate_daily_report(logical_date)
        outputs.append(
            {
                "logical_date": summary["logical_date"],
                "data_source": summary["data_source"],
                "segment_count": summary["segment_count"],
                "outputs": written,
            }
        )
    print(json.dumps({"status": "ok", "reports": outputs}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
