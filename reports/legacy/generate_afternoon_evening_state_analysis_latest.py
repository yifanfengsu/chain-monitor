#!/usr/bin/env python3
from __future__ import annotations

# Deprecated legacy report generator.
# Daily workflow uses reports/generate_daily_report_latest.py.
# This script is kept only for manual/debug compatibility.

import argparse
import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from generate_overnight_run_analysis_latest import (  # noqa: E402
    APP_DIR,
    ARCHIVE_DIR,
    BJ_TZ,
    DATA_DIR,
    REPORTS_DIR,
    ROOT,
    SERVER_TZ,
    UTC,
    FileInventory,
    adverse_move,
    archive_dates_for_window,
    canonical_asset,
    compute_archive_integrity,
    compute_final_trading_output_summary,
    compute_market_context,
    compute_majors,
    compute_trade_opportunities,
    compute_trade_actions,
    first_value,
    fmt_ts,
    inventory_category,
    inventory_ndjson,
    join_lp_rows,
    load_asset_case_cache,
    load_trade_opportunity_cache,
    load_quality_cache,
    load_signals,
    median,
    normalize_opportunity_summary,
    rate,
    run_cli,
    sqlite_report_source_summary,
    stream_case_followups,
    stream_cases,
    stream_delivery_audit,
    to_float,
    to_int,
    _sqlite_connect,
    _sqlite_table_exists,
)

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
if str(REPORTS_DIR) not in sys.path:
    sys.path.insert(0, str(REPORTS_DIR))

from report_data_loader import (  # noqa: E402
    archive_date_key,
    archive_paths as report_archive_paths,
    open_archive_text as report_open_archive_text,
)
from report_output_utils import write_dated_report_copies

MARKDOWN_PATH = REPORTS_DIR / "afternoon_evening_state_analysis_latest.md"
CSV_PATH = REPORTS_DIR / "afternoon_evening_state_metrics_latest.csv"
JSON_PATH = REPORTS_DIR / "afternoon_evening_state_summary_latest.json"

REQUIRED_CATEGORIES = ("raw_events", "parsed_events", "signals", "delivery_audit", "cases")
OPTIONAL_CATEGORIES = ("case_followups",)
ALL_ARCHIVE_CATEGORIES = REQUIRED_CATEGORIES + OPTIONAL_CATEGORIES

SAFE_CONFIG_KEYS = [
    "DEFAULT_USER_TIER",
    "MARKET_CONTEXT_ADAPTER_MODE",
    "MARKET_CONTEXT_PRIMARY_VENUE",
    "MARKET_CONTEXT_SECONDARY_VENUE",
    "OKX_PUBLIC_BASE_URL",
    "KRAKEN_FUTURES_BASE_URL",
    "ARCHIVE_ENABLE_RAW_EVENTS",
    "ARCHIVE_ENABLE_PARSED_EVENTS",
    "ARCHIVE_ENABLE_SIGNALS",
    "ARCHIVE_ENABLE_CASES",
    "ARCHIVE_ENABLE_CASE_FOLLOWUPS",
    "ARCHIVE_ENABLE_DELIVERY_AUDIT",
    "LP_ASSET_CASE_PERSIST_ENABLE",
    "LP_QUALITY_STATS_ENABLE",
    "LP_MAJOR_ASSETS",
    "LP_MAJOR_QUOTES",
    "TRADE_ACTION_ENABLE",
    "ASSET_MARKET_STATE_ENABLE",
    "NO_TRADE_LOCK_ENABLE",
    "NO_TRADE_LOCK_WINDOW_SEC",
    "NO_TRADE_LOCK_TTL_SEC",
    "TELEGRAM_SEND_ONLY_STATE_CHANGES",
    "TELEGRAM_SUPPRESS_REPEAT_STATE_SEC",
    "CHASE_ENABLE_AFTER_MIN_SAMPLES",
    "CHASE_MIN_FOLLOWTHROUGH_60S_RATE",
    "CHASE_MAX_ADVERSE_60S_RATE",
    "CHASE_REQUIRE_OUTCOME_COMPLETION_RATE",
    "OPPORTUNITY_ENABLE",
    "OPPORTUNITY_REQUIRE_LIVE_CONTEXT",
    "OPPORTUNITY_REQUIRE_BROADER_CONFIRM",
    "OPPORTUNITY_REQUIRE_OUTCOME_HISTORY",
    "OPPORTUNITY_MIN_CANDIDATE_SCORE",
    "OPPORTUNITY_MIN_VERIFIED_SCORE",
    "OPPORTUNITY_MIN_HISTORY_SAMPLES",
    "OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE",
    "OPPORTUNITY_MAX_60S_ADVERSE_RATE",
    "OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE",
    "OPPORTUNITY_CALIBRATION_ENABLE",
    "OPPORTUNITY_CALIBRATION_MIN_SAMPLES",
    "OPPORTUNITY_CALIBRATION_STRONG_SAMPLES",
    "OPPORTUNITY_CALIBRATION_MAX_POSITIVE_ADJUSTMENT",
    "OPPORTUNITY_CALIBRATION_MAX_NEGATIVE_ADJUSTMENT",
    "OPPORTUNITY_CALIBRATION_COMPLETION_MIN",
    "OPPORTUNITY_CALIBRATION_ADVERSE_MAX",
    "OPPORTUNITY_CALIBRATION_FOLLOWTHROUGH_MIN",
    "OPPORTUNITY_NON_LP_EVIDENCE_ENABLE",
    "OPPORTUNITY_NON_LP_EVIDENCE_WINDOW_SEC",
    "OPPORTUNITY_NON_LP_SCORE_WEIGHT",
    "OPPORTUNITY_NON_LP_STRONG_BLOCKER_ENABLE",
    "OPPORTUNITY_NON_LP_TENTATIVE_WEIGHT",
    "OPPORTUNITY_NON_LP_OBSERVE_WEIGHT",
    "OPPORTUNITY_MAX_PER_ASSET_PER_HOUR",
    "OPPORTUNITY_COOLDOWN_SEC",
    "OUTCOME_SCHEDULER_ENABLE",
    "OUTCOME_TICK_INTERVAL_SEC",
    "OUTCOME_WINDOW_GRACE_SEC",
    "OUTCOME_CATCHUP_MAX_SEC",
    "OUTCOME_SETTLE_BATCH_SIZE",
    "OUTCOME_USE_MARKET_CONTEXT_PRICE",
    "OUTCOME_MARKET_CONTEXT_REFRESH_ON_DUE",
    "OUTCOME_PREFER_OKX_MARK",
    "OUTCOME_ALLOW_CATCHUP_WITH_LATEST_MARK",
    "OUTCOME_EXPIRE_AFTER_SEC",
    "SQLITE_ENABLE",
    "SQLITE_DB_PATH",
    "SQLITE_REPORT_READ_PREFER_DB",
    "SQLITE_REPORT_FALLBACK_TO_ARCHIVE",
    "REPORT_ARCHIVE_READ_GZIP",
    "REPORT_DB_ARCHIVE_COMPARE",
    "REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH",
]

LONG_ACTION_KEYS = {
    "LONG_BIAS_OBSERVE",
    "LONG_CANDIDATE",
    "LONG_CHASE_ALLOWED",
    "DO_NOT_CHASE_LONG",
    "TRADEABLE_LONG",
    "REVERSAL_WATCH_LONG",
}
SHORT_ACTION_KEYS = {
    "SHORT_BIAS_OBSERVE",
    "SHORT_CANDIDATE",
    "SHORT_CHASE_ALLOWED",
    "DO_NOT_CHASE_SHORT",
    "TRADEABLE_SHORT",
    "REVERSAL_WATCH_SHORT",
}
LONG_STATE_KEYS = {
    "OBSERVE_LONG",
    "DO_NOT_CHASE_LONG",
    "LONG_CANDIDATE",
    "TRADEABLE_LONG",
}
SHORT_STATE_KEYS = {
    "OBSERVE_SHORT",
    "DO_NOT_CHASE_SHORT",
    "SHORT_CANDIDATE",
    "TRADEABLE_SHORT",
}
NEUTRAL_STATE_KEYS = {
    "NO_TRADE_CHOP",
    "NO_TRADE_LOCK",
    "WAIT_CONFIRMATION",
    "DATA_GAP_NO_TRADE",
}
WINDOW_NAMES = ("30s", "60s", "300s")


def mean(values: list[Any], digits: int = 4) -> float | None:
    cleaned = [float(v) for v in values if to_float(v) is not None]
    if not cleaned:
        return None
    return round(float(statistics.mean(cleaned)), digits)


def pct(numerator: int, denominator: int) -> float | None:
    if not denominator:
        return None
    return round(numerator / denominator * 100.0, 2)


def canonical_pair_label(pair_label: str | None) -> str:
    raw = str(pair_label or "").strip().upper().replace(" ", "")
    if "/" not in raw:
        return raw
    base, quote = raw.split("/", 1)
    return f"{canonical_asset(base)}/{quote.replace('.E', '')}"


def window_status_value(row: dict[str, Any], window_name: str, key: str) -> Any:
    windows = row.get("outcome_windows") if isinstance(row.get("outcome_windows"), dict) else {}
    item = dict(windows.get(window_name) or {})
    if key in item and item.get(key) not in (None, "", [], {}):
        return item.get(key)
    if key == "status":
        if to_float(row.get(f"move_after_alert_{window_name}")) is not None:
            return "completed"
        return "pending"
    if key == "price_source":
        return row.get("outcome_price_source") or ""
    if key == "failure_reason":
        return row.get("outcome_failure_reason") or ""
    return None


def normalize_outcome_failure_reason(reason: str | None) -> str:
    normalized = str(reason or "").strip()
    if normalized in {
        "market_price_unavailable",
        "pending_not_processed",
        "catchup_window_exceeded",
        "symbol_unavailable",
        "market_context_error",
        "sqlite_write_failed",
        "window_elapsed_without_price_update",
    }:
        return normalized
    if not normalized:
        return "unknown"
    if normalized in {"price_unavailable", "live_market_price_unavailable", "market_context_and_pool_price_unavailable"}:
        return "market_price_unavailable"
    if normalized in {"no_symbol", "market_symbol_unavailable", "symbol_mismatch"}:
        return "symbol_unavailable"
    if normalized.startswith("market_context"):
        return "market_context_error"
    return normalized


def env_whitelist() -> dict[str, str]:
    path = ROOT / ".env"
    payload: dict[str, str] = {}
    if not path.exists():
        return payload
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in SAFE_CONFIG_KEYS:
            continue
        payload[key] = value.strip().strip("'").strip('"')
    return payload


def load_runtime_config() -> dict[str, dict[str, Any]]:
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
    import config  # type: ignore

    env_values = env_whitelist()
    payload: dict[str, dict[str, Any]] = {}
    for key in SAFE_CONFIG_KEYS:
        value = getattr(config, key, None)
        if isinstance(value, (list, tuple, set)):
            value = list(value)
        payload[key] = {
            "runtime_value": value,
            "env_present": key in env_values,
            "env_value": env_values.get(key),
        }
    return payload


def archive_files(category: str) -> list[Path]:
    return report_archive_paths(category)


def archive_file_for_date(category: str, date: str) -> Path | None:
    for path in archive_files(category):
        if archive_date_key(path) == date:
            return path
    return None


def fast_archive_ts_from_line(line: str) -> int | None:
    marker = '"archive_ts":'
    idx = line.find(marker)
    if idx == -1:
        return None
    pos = idx + len(marker)
    length = len(line)
    while pos < length and line[pos] in {" ", "\t"}:
        pos += 1
    end = pos
    while end < length and line[end].isdigit():
        end += 1
    if end == pos:
        return None
    try:
        return int(line[pos:end])
    except ValueError:
        return None


def latest_archive_date() -> str:
    dates: set[str] = set()
    for category in ALL_ARCHIVE_CATEGORIES:
        for path in archive_files(category):
            dates.add(archive_date_key(path))
    if not dates:
        raise RuntimeError("no archive files found")
    return sorted(dates)[-1]


def ndjson_stats(path: Path, *, start_ts: int | None = None, end_ts: int | None = None) -> dict[str, Any]:
    count = 0
    first_ts: int | None = None
    last_ts: int | None = None
    with report_open_archive_text(path) as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            ts = fast_archive_ts_from_line(line)
            if ts is None:
                continue
            if start_ts is not None and ts < start_ts:
                continue
            if end_ts is not None and ts > end_ts:
                continue
            count += 1
            if first_ts is None:
                first_ts = ts
            last_ts = ts
    return {"count": count, "start_ts": first_ts, "end_ts": last_ts}


def load_union_entries(paths_by_category: dict[str, Path], *, start_ts: int, end_ts: int | None = None) -> list[tuple[int, str]]:
    entries: list[tuple[int, str]] = []
    for category, path in paths_by_category.items():
        with report_open_archive_text(path) as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                ts = fast_archive_ts_from_line(line)
                if ts is None or ts < start_ts:
                    continue
                if end_ts is not None and ts > end_ts:
                    continue
                entries.append((ts, category))
    entries.sort(key=lambda item: item[0])
    return entries


def sqlite_heartbeat_stats(start_ts: int, end_ts: int | None = None) -> dict[str, Any] | None:
    conn = _sqlite_connect()
    if conn is None:
        return None
    table_map = {
        "raw_events": ("raw_events", "captured_at"),
        "parsed_events": ("parsed_events", "parsed_at"),
    }
    try:
        source_after_cutoff: dict[str, dict[str, Any]] = {}
        entries: list[tuple[int, str]] = []
        for category, (table, time_column) in table_map.items():
            if not _sqlite_table_exists(conn, table):
                return None
            clauses = [f"{time_column} >= ?"]
            params: list[Any] = [int(start_ts)]
            if end_ts is not None:
                clauses.append(f"{time_column} <= ?")
                params.append(int(end_ts))
            where = " AND ".join(clauses)
            stat_row = conn.execute(
                f"SELECT COUNT(*) AS count, MIN({time_column}) AS start_ts, MAX({time_column}) AS end_ts "
                f"FROM {table} WHERE {where}",
                tuple(params),
            ).fetchone()
            source_after_cutoff[category] = {
                "count": int(stat_row["count"] or 0) if stat_row else 0,
                "start_ts": to_int(stat_row["start_ts"]) if stat_row else None,
                "end_ts": to_int(stat_row["end_ts"]) if stat_row else None,
            }
            for row in conn.execute(
                f"SELECT {time_column} AS ts FROM {table} WHERE {where} ORDER BY {time_column}",
                tuple(params),
            ):
                ts = to_int(row["ts"])
                if ts is not None:
                    entries.append((ts, category))
        entries.sort(key=lambda item: item[0])
        return {
            "entries": entries,
            "source_after_cutoff": source_after_cutoff,
        }
    finally:
        conn.close()


def fast_inventory_ndjson(path: Path, notes: str) -> FileInventory:
    if not path.exists():
        return FileInventory(str(path.relative_to(ROOT)), False, 0, None, None, notes)
    count = 0
    start_ts: int | None = None
    end_ts: int | None = None
    with report_open_archive_text(path) as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            ts = fast_archive_ts_from_line(line)
            count += 1
            if start_ts is None:
                start_ts = ts
            end_ts = ts
    return FileInventory(str(path.relative_to(ROOT)), True, count, start_ts, end_ts, notes)


def build_union_segments(entries: list[tuple[int, str]], *, gap_threshold_sec: int = 300) -> list[dict[str, Any]]:
    if not entries:
        return []
    segments: list[dict[str, Any]] = []
    current: dict[str, Any] = {
        "start_ts": entries[0][0],
        "end_ts": entries[0][0],
        "total_rows": 0,
        "category_counts": Counter(),
        "category_first_ts": {},
        "category_last_ts": {},
    }
    prev_ts = entries[0][0]
    for ts, category in entries:
        if ts - prev_ts >= gap_threshold_sec and int(current["total_rows"]) > 0:
            current["duration_sec"] = int(current["end_ts"]) - int(current["start_ts"])
            current["duration_hours"] = round(int(current["duration_sec"]) / 3600.0, 2)
            segments.append(current)
            current = {
                "start_ts": ts,
                "end_ts": ts,
                "total_rows": 0,
                "category_counts": Counter(),
                "category_first_ts": {},
                "category_last_ts": {},
            }
        current["end_ts"] = ts
        current["total_rows"] = int(current["total_rows"]) + 1
        current["category_counts"][category] += 1
        current["category_first_ts"].setdefault(category, ts)
        current["category_last_ts"][category] = ts
        prev_ts = ts
    current["duration_sec"] = int(current["end_ts"]) - int(current["start_ts"])
    current["duration_hours"] = round(int(current["duration_sec"]) / 3600.0, 2)
    segments.append(current)
    return segments


def gap_summary(entries: list[tuple[int, str]], thresholds: tuple[int, ...] = (300, 600, 900, 1800)) -> dict[str, Any]:
    timestamps = [ts for ts, _ in entries]
    gap_payload: dict[str, Any] = {}
    for threshold in thresholds:
        gaps = [(b - a, a, b) for a, b in zip(timestamps, timestamps[1:]) if b - a >= threshold]
        gap_payload[f"gap_ge_{threshold}_count"] = len(gaps)
        gap_payload[f"gap_ge_{threshold}_examples"] = [
            {
                "gap_min": round(gap / 60.0, 2),
                "start_utc": fmt_ts(start, UTC),
                "end_utc": fmt_ts(end, UTC),
                "start_beijing": fmt_ts(start, BJ_TZ),
                "end_beijing": fmt_ts(end, BJ_TZ),
            }
            for gap, start, end in gaps[:10]
        ]
    return gap_payload


def choose_analysis_window(latest_date: str, *, prefer_sqlite: bool = False) -> dict[str, Any]:
    cutoff_bj = datetime.strptime(latest_date, "%Y-%m-%d").replace(tzinfo=BJ_TZ, hour=13, minute=0, second=0)
    cutoff_ts = int(cutoff_bj.timestamp())
    paths_by_category = {
        category: path
        for category in ALL_ARCHIVE_CATEGORIES
        if (path := archive_file_for_date(category, latest_date)) is not None
    }

    heartbeat_categories = ("raw_events", "parsed_events")
    sqlite_heartbeat = sqlite_heartbeat_stats(cutoff_ts) if prefer_sqlite else None
    if sqlite_heartbeat is not None:
        source_after_cutoff = sqlite_heartbeat["source_after_cutoff"]
        heartbeat_entries = sqlite_heartbeat["entries"]
    else:
        missing_required = [category for category in REQUIRED_CATEGORIES if category not in paths_by_category]
        if missing_required:
            raise RuntimeError(f"required latest-day archives missing: {missing_required}")
        source_after_cutoff = {
            category: ndjson_stats(paths_by_category[category], start_ts=cutoff_ts)
            for category in heartbeat_categories
        }
        heartbeat_entries = load_union_entries({category: paths_by_category[category] for category in heartbeat_categories}, start_ts=cutoff_ts)
    segments = build_union_segments(heartbeat_entries, gap_threshold_sec=300)

    candidates: list[dict[str, Any]] = []
    for segment in segments:
        coverage_categories = set(segment["category_counts"].keys())
        required_firsts = []
        required_lasts = []
        for category in heartbeat_categories:
            first_ts = to_int(source_after_cutoff[category]["start_ts"])
            last_ts = to_int(source_after_cutoff[category]["end_ts"])
            if first_ts is None or last_ts is None:
                required_firsts = []
                break
            if last_ts < int(segment["start_ts"]) or first_ts > int(segment["end_ts"]):
                required_firsts = []
                break
            required_firsts.append(max(first_ts, int(segment["start_ts"])))
            required_lasts.append(min(last_ts, int(segment["end_ts"])))
            coverage_categories.add(category)
        if not required_firsts or not required_lasts:
            continue
        overlap_start = max(required_firsts)
        overlap_end = min(required_lasts)
        if overlap_end <= overlap_start:
            continue
        candidates.append(
            {
                "segment_start_ts": int(segment["start_ts"]),
                "segment_end_ts": int(segment["end_ts"]),
                "segment_duration_sec": int(segment["duration_sec"]),
                "segment_duration_hours": float(segment["duration_hours"]),
                "coverage_categories": sorted(coverage_categories),
                "overlap_start_ts": overlap_start,
                "overlap_end_ts": overlap_end,
                "overlap_duration_sec": overlap_end - overlap_start,
                "overlap_duration_hours": round((overlap_end - overlap_start) / 3600.0, 2),
                "category_counts": dict(segment["category_counts"]),
            }
        )

    if not candidates:
        raise RuntimeError("no contiguous afternoon-evening segment covers all required sources")

    chosen = sorted(
        candidates,
        key=lambda item: (
            -int(item["overlap_duration_sec"]),
            -int(item["overlap_end_ts"]),
            -int(sum(item["category_counts"].values())),
        ),
    )[0]

    if sqlite_heartbeat is not None:
        window_heartbeat = sqlite_heartbeat_stats(
            int(chosen["overlap_start_ts"]),
            int(chosen["overlap_end_ts"]),
        )
        window_entries = list((window_heartbeat or {}).get("entries") or [])
    else:
        window_entries = load_union_entries(
            {category: paths_by_category[category] for category in heartbeat_categories},
            start_ts=int(chosen["overlap_start_ts"]),
            end_ts=int(chosen["overlap_end_ts"]),
        )
    partial_lead_counts = {}
    for category in heartbeat_categories:
        if sqlite_heartbeat is not None:
            partial_payload = sqlite_heartbeat_stats(cutoff_ts, int(chosen["overlap_start_ts"]) - 1)
            partial = (partial_payload or {}).get("source_after_cutoff", {}).get(category, {"count": 0})
        else:
            path = paths_by_category[category]
            partial = ndjson_stats(path, start_ts=cutoff_ts, end_ts=int(chosen["overlap_start_ts"]) - 1)
        partial_lead_counts[category] = partial["count"]

    return {
        "latest_archive_date": latest_date,
        "cutoff_ts": cutoff_ts,
        "cutoff_utc": fmt_ts(cutoff_ts, UTC),
        "cutoff_beijing": fmt_ts(cutoff_ts, BJ_TZ),
        "start_ts": int(chosen["overlap_start_ts"]),
        "end_ts": int(chosen["overlap_end_ts"]),
        "start_utc": fmt_ts(int(chosen["overlap_start_ts"]), UTC),
        "end_utc": fmt_ts(int(chosen["overlap_end_ts"]), UTC),
        "start_server_local": fmt_ts(int(chosen["overlap_start_ts"]), SERVER_TZ),
        "end_server_local": fmt_ts(int(chosen["overlap_end_ts"]), SERVER_TZ),
        "start_beijing": fmt_ts(int(chosen["overlap_start_ts"]), BJ_TZ),
        "end_beijing": fmt_ts(int(chosen["overlap_end_ts"]), BJ_TZ),
        "duration_hours": round((int(chosen["overlap_end_ts"]) - int(chosen["overlap_start_ts"])) / 3600.0, 2),
        "selection_reason": (
            "selected the longest continuous segment after Beijing 13:00 using raw_events and parsed_events as the runtime heartbeat; "
            "signals/delivery_audit/cases are analyzed inside the chosen heartbeat window instead of defining it, "
            "continuity itself is anchored on raw/parsed heartbeats rather than LP-signal gaps, "
            "because the service can be healthy even when LP opportunities are sparse."
        ),
        "segments_after_cutoff": candidates,
        "source_after_cutoff": source_after_cutoff,
        "partial_lead_counts_before_window": partial_lead_counts,
        "required_gap_summary": gap_summary(window_entries),
        "required_union_row_count": len(window_entries),
        "paths_by_category": {key: str(path.relative_to(ROOT)) for key, path in paths_by_category.items()},
        "heartbeat_source": "sqlite" if sqlite_heartbeat is not None else "archive",
    }


def build_data_source_inventory(latest_date: str, window: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    inventories: list[dict[str, Any]] = []
    missing: list[str] = []

    for category in ALL_ARCHIVE_CATEGORIES:
        paths = archive_files(category)
        if not paths:
            missing.append(str((ARCHIVE_DIR / category / "*.ndjson(.gz)").relative_to(ROOT)))
        for path in paths:
            info = fast_inventory_ndjson(path, f"{category} archive")
            window_count = ""
            after_cutoff_count = ""
            if archive_date_key(path) == latest_date:
                window_stats = ndjson_stats(path, start_ts=int(window["start_ts"]), end_ts=int(window["end_ts"]))
                after_cutoff = ndjson_stats(path, start_ts=int(window["cutoff_ts"]))
                window_count = window_stats["count"]
                after_cutoff_count = after_cutoff["count"]
            inventories.append(
                {
                    "path": info.path,
                    "exists": info.exists,
                    "record_count": info.record_count,
                    "start_utc": fmt_ts(info.start_ts, UTC),
                    "end_utc": fmt_ts(info.end_ts, UTC),
                    "start_beijing": fmt_ts(info.start_ts, BJ_TZ),
                    "end_beijing": fmt_ts(info.end_ts, BJ_TZ),
                    "window_record_count": window_count,
                    "after_cutoff_count": after_cutoff_count,
                    "notes": info.notes,
                }
            )

    _, _, quality_inventory = load_quality_cache()
    asset_case_payload, asset_case_inventory = load_asset_case_cache()
    asset_state_records, asset_state_inventory = load_asset_market_state_cache()
    trade_opportunity_payload, trade_opportunity_inventory = load_trade_opportunity_cache()
    cache_inventories = [quality_inventory, asset_case_inventory, asset_state_inventory, trade_opportunity_inventory]

    for info in cache_inventories:
        if not info.exists:
            missing.append(info.path)
        inventories.append(
            {
                "path": info.path,
                "exists": info.exists,
                "record_count": info.record_count,
                "start_utc": fmt_ts(info.start_ts, UTC),
                "end_utc": fmt_ts(info.end_ts, UTC),
                "start_beijing": fmt_ts(info.start_ts, BJ_TZ),
                "end_beijing": fmt_ts(info.end_ts, BJ_TZ),
                "window_record_count": "",
                "after_cutoff_count": "",
                "notes": info.notes,
            }
        )

    if asset_case_payload == {}:
        missing.append(str((DATA_DIR / "asset_cases.cache.json").relative_to(ROOT)))
    if not asset_state_records:
        missing.append(str((DATA_DIR / "asset_market_states.cache.json").relative_to(ROOT)))
    if trade_opportunity_payload == {}:
        missing.append(str((DATA_DIR / "trade_opportunities.cache.json").relative_to(ROOT)))
    return inventories, sorted(set(missing))


def inventory_item_dict(info: FileInventory, *, window_record_count: Any = "", after_cutoff_count: Any = "") -> dict[str, Any]:
    return {
        "path": info.path,
        "exists": info.exists,
        "record_count": info.record_count,
        "start_utc": fmt_ts(info.start_ts, UTC),
        "end_utc": fmt_ts(info.end_ts, UTC),
        "start_beijing": fmt_ts(info.start_ts, BJ_TZ),
        "end_beijing": fmt_ts(info.end_ts, BJ_TZ),
        "window_record_count": window_record_count,
        "after_cutoff_count": after_cutoff_count,
        "notes": info.notes,
    }


def build_date_scoped_data_source_inventory(
    window: dict[str, Any],
    signal_inventory: list[FileInventory],
    delivery_inventory: list[FileInventory],
    cases_inventory: list[FileInventory],
    followup_inventory: list[FileInventory],
    quality_inventory: FileInventory,
    asset_case_inventory: FileInventory,
    asset_state_inventory: FileInventory,
    trade_opportunity_inventory: FileInventory,
) -> tuple[list[dict[str, Any]], list[str]]:
    selected_archive_dates = archive_dates_for_window(window)
    archive_inventories = [
        *inventory_category("raw_events", "raw events archive", window=window, archive_dates=selected_archive_dates),
        *inventory_category("parsed_events", "parsed events archive", window=window, archive_dates=selected_archive_dates),
        *signal_inventory,
        *delivery_inventory,
        *cases_inventory,
        *followup_inventory,
    ]
    cache_inventories = [
        quality_inventory,
        asset_case_inventory,
        asset_state_inventory,
        trade_opportunity_inventory,
    ]
    inventories = [inventory_item_dict(info, window_record_count=info.record_count) for info in archive_inventories]
    inventories.extend(inventory_item_dict(info) for info in cache_inventories)
    missing = [info.path for info in archive_inventories + cache_inventories if not info.exists]
    return inventories, sorted(set(missing))


def load_asset_market_state_cache() -> tuple[list[dict[str, Any]], FileInventory]:
    path = DATA_DIR / "asset_market_states.cache.json"
    if not path.exists():
        return [], FileInventory(str(path.relative_to(ROOT)), False, 0, None, None, "asset market state cache")
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = list(payload.get("records") or [])
    times: list[int] = []
    generated_at = to_int(payload.get("generated_at"))
    if generated_at is not None:
        times.append(generated_at)
    for row in records:
        for key in (
            "asset_market_state_started_at",
            "asset_market_state_updated_at",
            "first_seen_at",
            "last_telegram_sent_at",
            "no_trade_lock_started_at",
            "no_trade_lock_until",
            "prealert_expires_at",
        ):
            ts = to_int(row.get(key))
            if ts is not None:
                times.append(ts)
    inventory = FileInventory(
        str(path.relative_to(ROOT)),
        True,
        len(records),
        min(times) if times else None,
        max(times) if times else None,
        "asset market state cache",
    )
    return records, inventory


def source_window_counts(window: dict[str, Any], latest_date: str, *, prefer_sqlite: bool = False) -> dict[str, int]:
    if prefer_sqlite:
        conn = _sqlite_connect()
        if conn is not None:
            table_map = {
                "raw_events": ("raw_events", "captured_at"),
                "parsed_events": ("parsed_events", "parsed_at"),
                "signals": ("signals", "archive_written_at"),
                "delivery_audit": ("delivery_audit", "archive_written_at"),
                "case_followups": ("case_followups", "archive_written_at"),
            }
            try:
                payload = {category: 0 for category in ALL_ARCHIVE_CATEGORIES}
                for category, (table, time_column) in table_map.items():
                    if not _sqlite_table_exists(conn, table):
                        continue
                    row = conn.execute(
                        f"SELECT COUNT(*) AS count FROM {table} WHERE {time_column} >= ? AND {time_column} <= ?",
                        (int(window["start_ts"]), int(window["end_ts"])),
                    ).fetchone()
                    payload[category] = int(row["count"] or 0) if row else 0
                if _sqlite_table_exists(conn, "signals"):
                    row = conn.execute(
                        "SELECT COUNT(*) AS count FROM signals "
                        "WHERE archive_written_at >= ? AND archive_written_at <= ? "
                        "AND asset_case_id IS NOT NULL AND TRIM(asset_case_id) != ''",
                        (int(window["start_ts"]), int(window["end_ts"])),
                    ).fetchone()
                    payload["cases"] = int(row["count"] or 0) if row else 0
                return payload
            finally:
                conn.close()
    payload: dict[str, int] = {}
    for category in ALL_ARCHIVE_CATEGORIES:
        path = archive_file_for_date(category, latest_date)
        if path is None:
            payload[category] = 0
            continue
        payload[category] = int(ndjson_stats(path, start_ts=int(window["start_ts"]), end_ts=int(window["end_ts"]))["count"])
    return payload


def high_value_row(row: dict[str, Any]) -> bool:
    if bool(row.get("asset_market_state_changed")):
        return True
    return str(row.get("telegram_update_kind") or "") in {"state_change", "risk_blocker", "candidate", "opportunity"}


def row_direction(row: dict[str, Any]) -> str:
    state_key = str(row.get("asset_market_state_key") or "")
    if state_key in LONG_STATE_KEYS:
        return "long"
    if state_key in SHORT_STATE_KEYS:
        return "short"
    trade_action_key = str(row.get("trade_action_key") or "")
    if trade_action_key in LONG_ACTION_KEYS:
        return "long"
    if trade_action_key in SHORT_ACTION_KEYS:
        return "short"
    bucket = str(row.get("direction_bucket") or "")
    if bucket == "buy_pressure":
        return "long"
    if bucket == "sell_pressure":
        return "short"
    return ""


def direction_adverse(row: dict[str, Any], window_name: str) -> bool | None:
    move = to_float(row.get(f"move_after_alert_{window_name}"))
    if move is None:
        return None
    direction = row_direction(row)
    if direction == "short":
        return move > 0
    if direction == "long":
        return move < 0
    return None


def is_strong_lp_row(row: dict[str, Any]) -> bool:
    if str(row.get("lp_alert_stage") or "") in {"confirm", "climax", "exhaustion_risk"}:
        return True
    if str(row.get("asset_market_state_key") or "") in {
        "LONG_CANDIDATE",
        "SHORT_CANDIDATE",
        "TRADEABLE_LONG",
        "TRADEABLE_SHORT",
    }:
        return True
    if str(row.get("lp_confirm_scope") or "") in {"local_confirm", "broader_confirm"}:
        return True
    return (to_float(row.get("trade_action_confidence")) or 0.0) >= 0.75


def compute_run_overview(
    window: dict[str, Any],
    latest_date: str,
    window_signal_rows: list[dict[str, Any]],
    lp_rows: list[dict[str, Any]],
    asset_case_count: int,
    case_followup_count: int,
    *,
    prefer_sqlite: bool = False,
) -> dict[str, Any]:
    counts = source_window_counts(window, latest_date, prefer_sqlite=prefer_sqlite)
    delivered_lp = sum(1 for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    suppressed_lp = sum(
        1
        for row in lp_rows
        if str(row.get("telegram_suppression_reason") or "").strip()
        or str(row.get("telegram_update_kind") or "") == "suppressed"
        or row.get("telegram_should_send") is False
    )
    return {
        "analysis_window_start_utc": window["start_utc"],
        "analysis_window_start_beijing": window["start_beijing"],
        "analysis_window_end_utc": window["end_utc"],
        "analysis_window_end_beijing": window["end_beijing"],
        "duration_hours": window["duration_hours"],
        "total_raw_events": counts["raw_events"],
        "total_parsed_events": counts["parsed_events"],
        "total_signal_rows": len(window_signal_rows),
        "lp_signal_rows": len(lp_rows),
        "delivered_lp_signals": delivered_lp,
        "suppressed_lp_signals": suppressed_lp,
        "asset_case_count": asset_case_count,
        "case_followup_count": case_followup_count,
    }


def compute_stage_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter(str(row.get("lp_alert_stage") or "") for row in lp_rows)
    total = len(lp_rows)
    return {
        "prealert_count": counter.get("prealert", 0),
        "confirm_count": counter.get("confirm", 0),
        "climax_count": counter.get("climax", 0),
        "exhaustion_risk_count": counter.get("exhaustion_risk", 0),
        "stage_distribution_pct": {
            key: pct(counter.get(key, 0), total) for key in ("prealert", "confirm", "climax", "exhaustion_risk")
        },
    }


def compute_asset_market_state_detail(
    lp_rows: list[dict[str, Any]],
    window_end_ts: int,
    cache_records: list[dict[str, Any]],
) -> dict[str, Any]:
    state_rows = [row for row in lp_rows if str(row.get("asset_market_state_key") or "").strip()]
    change_rows = [row for row in state_rows if row.get("asset_market_state_changed")]
    counter = Counter(str(row.get("asset_market_state_key") or "") for row in state_rows)
    transitions = Counter()
    state_paths: dict[str, list[dict[str, Any]]] = defaultdict(list)
    duration_summary: dict[str, dict[str, Any]] = {}
    final_state_by_asset: dict[str, dict[str, Any]] = {}

    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        asset = canonical_asset(row.get("asset_symbol"))
        if asset:
            by_asset[asset].append(row)

    for asset, rows in by_asset.items():
        rows.sort(key=lambda item: (int(item["archive_ts"]), str(item.get("signal_id") or "")))
        changed = [row for row in rows if row.get("asset_market_state_changed")]
        for row in changed:
            prev = str(row.get("previous_asset_market_state_key") or "none")
            curr = str(row.get("asset_market_state_key") or "")
            transitions[f"{prev}->{curr}"] += 1
            state_paths[asset].append(
                {
                    "ts": int(row["archive_ts"]),
                    "utc": fmt_ts(int(row["archive_ts"]), UTC),
                    "beijing": fmt_ts(int(row["archive_ts"]), BJ_TZ),
                    "state_key": curr,
                    "previous_state_key": prev,
                    "reason": str(row.get("asset_market_state_reason") or ""),
                    "trade_action_key": str(row.get("trade_action_key") or ""),
                    "lp_alert_stage": str(row.get("lp_alert_stage") or ""),
                    "telegram_update_kind": str(row.get("telegram_update_kind") or ""),
                }
            )

        state_durations = Counter()
        for index, row in enumerate(changed):
            current_key = str(row.get("asset_market_state_key") or "")
            start_ts = int(row["archive_ts"])
            end_ts = window_end_ts
            if index + 1 < len(changed):
                end_ts = int(changed[index + 1]["archive_ts"])
            if end_ts >= start_ts and current_key:
                state_durations[current_key] += end_ts - start_ts

        last_row = rows[-1]
        final_state_by_asset[asset] = {
            "state_key": str(last_row.get("asset_market_state_key") or ""),
            "state_label": str(last_row.get("asset_market_state_label") or ""),
            "updated_at": int(last_row.get("archive_ts") or 0),
            "updated_at_utc": fmt_ts(int(last_row.get("archive_ts") or 0), UTC),
            "updated_at_beijing": fmt_ts(int(last_row.get("archive_ts") or 0), BJ_TZ),
            "telegram_update_kind": str(last_row.get("telegram_update_kind") or ""),
        }
        duration_summary[asset] = {
            "by_state_sec": dict(sorted(state_durations.items())),
            "observed_transition_count": len(changed),
        }

    cache_final_state_by_asset = {}
    for row in cache_records:
        asset = canonical_asset(row.get("asset_symbol"))
        if not asset:
            continue
        cache_final_state_by_asset[asset] = {
            "state_key": str(row.get("asset_market_state_key") or ""),
            "state_label": str(row.get("asset_market_state_label") or ""),
            "updated_at": to_int(row.get("asset_market_state_updated_at")),
            "updated_at_utc": fmt_ts(to_int(row.get("asset_market_state_updated_at")), UTC),
            "updated_at_beijing": fmt_ts(to_int(row.get("asset_market_state_updated_at")), BJ_TZ),
            "suppressed_signal_count_in_state": to_int(row.get("suppressed_signal_count_in_state")),
            "transition_count": to_int(row.get("transition_count")),
        }

    return {
        "state_distribution": dict(sorted(counter.items())),
        "state_transition_count": sum(transitions.values()),
        "state_changed_count": len(change_rows),
        "state_change_message_count": sum(1 for row in state_rows if str(row.get("telegram_update_kind") or "") == "state_change"),
        "repeated_state_suppressed_count": sum(
            1 for row in state_rows if str(row.get("telegram_suppression_reason") or "") == "same_asset_state_repeat"
        ),
        "final_state_by_asset": dict(sorted(final_state_by_asset.items())),
        "cache_final_state_by_asset": dict(sorted(cache_final_state_by_asset.items())),
        "asset_state_duration_summary": dict(sorted(duration_summary.items())),
        "state_transition_paths": {
            asset: [item["state_key"] for item in path]
            for asset, path in sorted(state_paths.items())
        },
        "eth_state_path": state_paths.get("ETH", []),
    }


def compute_no_trade_lock_detail(lp_rows: list[dict[str, Any]], runtime_config: dict[str, dict[str, Any]]) -> dict[str, Any]:
    entered_rows = [
        row for row in lp_rows
        if str(row.get("asset_market_state_key") or "") == "NO_TRADE_LOCK"
        and bool(row.get("asset_market_state_changed"))
    ]
    suppressed_rows = [
        row for row in lp_rows if str(row.get("telegram_suppression_reason") or "").startswith("no_trade_lock")
    ]
    release_rows = [
        row
        for row in lp_rows
        if bool(row.get("asset_market_state_changed"))
        and str(row.get("previous_asset_market_state_key") or "") == "NO_TRADE_LOCK"
        and str(row.get("asset_market_state_key") or "") != "NO_TRADE_LOCK"
    ]

    durations = []
    for row in entered_rows:
        started = to_int(row.get("no_trade_lock_started_at"))
        until = to_int(row.get("no_trade_lock_until"))
        if started is not None and until is not None and until >= started:
            durations.append(until - started)

    release_reason_counter = Counter(
        str(row.get("no_trade_lock_released_by") or "unknown") for row in release_rows
    )
    suppressed_stage_counter = Counter(str(row.get("lp_alert_stage") or "") for row in suppressed_rows)

    missed_lock_examples: list[dict[str, Any]] = []
    window_sec = int(runtime_config["NO_TRADE_LOCK_WINDOW_SEC"]["runtime_value"] or 120)
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(lp_rows, key=lambda item: (int(item["archive_ts"]), str(item.get("signal_id") or ""))):
        asset = canonical_asset(row.get("asset_symbol"))
        if asset:
            by_asset[asset].append(row)

    for asset, rows in by_asset.items():
        for index, row in enumerate(rows):
            direction = row_direction(row)
            if direction not in {"long", "short"} or not is_strong_lp_row(row):
                continue
            ts = int(row["archive_ts"])
            paired = None
            triggered = False
            for candidate in rows[index + 1:]:
                if int(candidate["archive_ts"]) - ts > window_sec:
                    break
                if str(candidate.get("asset_market_state_key") or "") == "NO_TRADE_LOCK":
                    triggered = True
                    break
                candidate_direction = row_direction(candidate)
                if candidate_direction and candidate_direction != direction and is_strong_lp_row(candidate):
                    paired = candidate
            if paired is not None and not triggered:
                missed_lock_examples.append(
                    {
                        "asset": asset,
                        "first_signal_id": str(row.get("signal_id") or ""),
                        "second_signal_id": str(paired.get("signal_id") or ""),
                        "first_time_beijing": fmt_ts(ts, BJ_TZ),
                        "second_time_beijing": fmt_ts(int(paired["archive_ts"]), BJ_TZ),
                        "first_trade_action": str(row.get("trade_action_key") or ""),
                        "second_trade_action": str(paired.get("trade_action_key") or ""),
                        "first_state": str(row.get("asset_market_state_key") or ""),
                        "second_state": str(paired.get("asset_market_state_key") or ""),
                    }
                )
                if len(missed_lock_examples) >= 5:
                    break
        if len(missed_lock_examples) >= 5:
            break

    return {
        "lock_entered_count": len(entered_rows),
        "lock_suppressed_count": len(suppressed_rows),
        "lock_released_count": len(release_rows),
        "avg_lock_duration_sec": mean(durations, 2),
        "median_lock_duration_sec": median(durations, 2),
        "lock_release_reasons": dict(sorted(release_reason_counter.items())),
        "lock_prevented_message_count": len(suppressed_rows),
        "lock_suppressed_stage_distribution": dict(sorted(suppressed_stage_counter.items())),
        "missed_lock_examples": missed_lock_examples,
    }


def compute_telegram_detail(lp_rows: list[dict[str, Any]], previous_report: dict[str, Any]) -> dict[str, Any]:
    sent_rows = [row for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at")]
    suppression_rows = [
        row
        for row in lp_rows
        if str(row.get("telegram_suppression_reason") or "").strip()
        or str(row.get("telegram_update_kind") or "") == "suppressed"
        or row.get("telegram_should_send") is False
    ]
    should_send_rows = [row for row in lp_rows if row.get("telegram_should_send") is True]
    reason_counter = Counter(str(row.get("telegram_suppression_reason") or "") for row in suppression_rows if str(row.get("telegram_suppression_reason") or "").strip())
    update_kinds = Counter(str(row.get("telegram_update_kind") or "") for row in lp_rows if str(row.get("telegram_update_kind") or "").strip())

    high_value_suppressed = [row for row in suppression_rows if high_value_row(row)]
    low_value_suppressed = [row for row in suppression_rows if not high_value_row(row)]
    should_send_but_not_sent = [
        row
        for row in lp_rows
        if row.get("telegram_should_send") is True and not (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    ]

    previous_before_after = (
        previous_report.get("telegram_suppression_summary", {}).get("messages_before_after_suppression_estimate", {})
    )
    previous_window = previous_report.get("analysis_window", {})
    previous_hours = to_float(previous_window.get("duration_hours"))
    previous_raw = to_int(previous_before_after.get("raw_lp_signals"))
    previous_sent = to_int(previous_before_after.get("sent_telegram_messages"))

    sent_per_hour = None
    prev_sent_per_hour = None
    if lp_rows:
        current_hours = (int(lp_rows[-1]["archive_ts"]) - int(lp_rows[0]["archive_ts"])) / 3600.0 if len(lp_rows) > 1 else None
        if current_hours:
            sent_per_hour = round(len(sent_rows) / current_hours, 2)
    if previous_hours and previous_sent is not None and previous_hours > 0:
        prev_sent_per_hour = round(previous_sent / previous_hours, 2)

    return {
        "telegram_should_send_count": len(should_send_rows),
        "telegram_suppressed_count": len(suppression_rows),
        "telegram_suppression_ratio": rate(max(len(lp_rows) - len(sent_rows), 0), len(lp_rows)),
        "telegram_suppression_reasons": dict(sorted(reason_counter.items())),
        "telegram_update_kind_distribution": dict(sorted(update_kinds.items())),
        "messages_before_suppression_estimate": len(lp_rows),
        "messages_after_suppression_actual": len(sent_rows),
        "high_value_suppressed_count": len(high_value_suppressed),
        "low_value_suppressed_count": len(low_value_suppressed),
        "should_send_but_not_sent_count": len(should_send_but_not_sent),
        "previous_report_raw_lp_signals": previous_raw,
        "previous_report_sent_telegram_messages": previous_sent,
        "previous_report_duration_hours": previous_hours,
        "current_sent_messages_per_hour": sent_per_hour,
        "previous_sent_messages_per_hour": prev_sent_per_hour,
    }


def compute_prealert_lifecycle_detail(
    lp_rows: list[dict[str, Any]],
    asset_case_payload: dict[str, Any],
) -> dict[str, Any]:
    def episode_key(row: dict[str, Any]) -> str:
        asset_case_id = str(row.get("asset_case_id") or "")
        first_seen_at = to_int(row.get("first_seen_at"))
        first_seen_stage = str(row.get("first_seen_stage") or "")
        signal_id = str(row.get("signal_id") or "")
        if asset_case_id and first_seen_at is not None:
            return f"{asset_case_id}:{first_seen_at}:{first_seen_stage}"
        if asset_case_id:
            return asset_case_id
        if signal_id:
            return signal_id
        return f"row:{int(row['archive_ts'])}"

    candidate_rows = [row for row in lp_rows if row.get("lp_prealert_candidate")]
    gate_passed_rows = [row for row in lp_rows if row.get("lp_prealert_gate_passed")]
    prealert_rows = [row for row in lp_rows if str(row.get("lp_alert_stage") or "") == "prealert"]

    candidate_keys = {episode_key(row) for row in candidate_rows}
    gate_passed_keys = {episode_key(row) for row in gate_passed_rows}
    prealert_keys = {episode_key(row) for row in prealert_rows}
    lifecycle_counter = Counter()
    first_seen_counter = Counter()
    lifecycle_keys: dict[str, set[str]] = defaultdict(set)
    for row in lp_rows:
        key = episode_key(row)
        lifecycle_state = str(row.get("prealert_lifecycle_state") or "")
        first_seen_stage = str(row.get("first_seen_stage") or "")
        if lifecycle_state:
            lifecycle_counter[lifecycle_state] += 1
            lifecycle_keys[lifecycle_state].add(key)
        if first_seen_stage:
            first_seen_counter[first_seen_stage] += 1

    prealert_to_confirm_values = [
        to_int(row.get("prealert_to_confirm_sec"))
        for row in lp_rows
        if to_int(row.get("prealert_to_confirm_sec")) is not None
    ]
    asset_case_values = [
        to_int(row.get("asset_case_prealert_to_confirm_sec"))
        for row in lp_rows
        if to_int(row.get("asset_case_prealert_to_confirm_sec")) is not None
    ]
    prealert_to_confirm_values.extend(asset_case_values)

    case_rows = list(asset_case_payload.get("cases") or [])
    cache_case_prealert_values = [
        to_int(case.get("prealert_to_confirm_sec"))
        for case in case_rows
        if to_int(case.get("prealert_to_confirm_sec")) is not None
    ]
    prealert_to_confirm_values.extend(cache_case_prealert_values)

    delivered_rows = {
        episode_key(row)
        for row in prealert_rows
        if row.get("sent_to_telegram") or row.get("notifier_sent_at") or str(row.get("prealert_lifecycle_state") or "") == "delivered"
    }
    merged_rows = {
        episode_key(row)
        for row in lp_rows
        if str(row.get("prealert_lifecycle_state") or "") == "merged"
        or bool(row.get("lp_prealert_asset_case_preserved"))
    }
    upgraded_rows = {
        episode_key(row)
        for row in lp_rows
        if str(row.get("prealert_lifecycle_state") or "") == "upgraded_to_confirm"
        or to_int(row.get("prealert_to_confirm_sec")) is not None
        or to_int(row.get("asset_case_prealert_to_confirm_sec")) is not None
    }
    overwritten_count = len({episode_key(row) for row in lp_rows if row.get("lp_prealert_stage_overwritten")})
    visible_to_user_count = len({episode_key(row) for row in lp_rows if row.get("prealert_visible_to_user")})
    first_seen_stage_prealert_count = len(
        {episode_key(row) for row in lp_rows if str(row.get("first_seen_stage") or "") == "prealert"}
    )

    return {
        "prealert_candidate_count": len(candidate_keys),
        "prealert_gate_passed_count": len(gate_passed_keys),
        "prealert_active_count": len(lifecycle_keys.get("active", set())),
        "prealert_delivered_count": len(delivered_rows or lifecycle_keys.get("delivered", set())),
        "prealert_merged_count": len(merged_rows or lifecycle_keys.get("merged", set())),
        "prealert_upgraded_to_confirm_count": len(upgraded_rows or lifecycle_keys.get("upgraded_to_confirm", set())),
        "prealert_expired_count": len(lifecycle_keys.get("expired", set())),
        "prealert_suppressed_by_lock_count": len(lifecycle_keys.get("suppressed_by_lock", set())),
        "median_prealert_to_confirm_sec": median(prealert_to_confirm_values, 2),
        "first_seen_stage_distribution": dict(sorted(first_seen_counter.items())),
        "first_seen_stage_prealert_count": first_seen_stage_prealert_count,
        "prealert_visible_to_user_count": visible_to_user_count,
        "prealert_stage_overwritten_count": overwritten_count,
        "prealert_count": len(prealert_keys),
    }


def summarize_followthrough(rows: list[dict[str, Any]], window_name: str) -> dict[str, Any]:
    resolved = []
    followthrough = 0
    adverse = 0
    completed = 0
    for row in rows:
        status = str(window_status_value(row, window_name, "status") or "")
        if status == "completed":
            completed += 1
        move = to_float(row.get(f"direction_adjusted_move_after_{window_name}"))
        adv = row.get(f"adverse_by_direction_{window_name}")
        if move is None:
            continue
        resolved.append(row)
        if move > 0:
            followthrough += 1
        if adv is True:
            adverse += 1
    return {
        "count": len(rows),
        "completed_count": completed,
        "completed_rate": rate(completed, len(rows)),
        "resolved_count": len(resolved),
        "followthrough_count": followthrough,
        "followthrough_rate": rate(followthrough, len(resolved)),
        "adverse_count": adverse,
        "adverse_rate": rate(adverse, len(resolved)),
    }


def compute_candidate_tradeable_detail(
    lp_rows: list[dict[str, Any]],
    runtime_config: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidate_rows = [
        row for row in lp_rows if str(row.get("asset_market_state_key") or "") in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}
    ]
    tradeable_rows = [
        row for row in lp_rows if str(row.get("asset_market_state_key") or "") in {"TRADEABLE_LONG", "TRADEABLE_SHORT"}
    ]
    candidate_counter = Counter(str(row.get("asset_market_state_key") or "") for row in candidate_rows)
    tradeable_counter = Counter(str(row.get("asset_market_state_key") or "") for row in tradeable_rows)

    long_candidate_rows = [row for row in candidate_rows if str(row.get("asset_market_state_key") or "") == "LONG_CANDIDATE"]
    short_candidate_rows = [row for row in candidate_rows if str(row.get("asset_market_state_key") or "") == "SHORT_CANDIDATE"]
    tradeable_long_rows = [row for row in tradeable_rows if str(row.get("asset_market_state_key") or "") == "TRADEABLE_LONG"]
    tradeable_short_rows = [row for row in tradeable_rows if str(row.get("asset_market_state_key") or "") == "TRADEABLE_SHORT"]

    candidate_60 = summarize_followthrough(candidate_rows, "60s")
    tradeable_60 = summarize_followthrough(tradeable_rows, "60s")

    min_samples = int(runtime_config["CHASE_ENABLE_AFTER_MIN_SAMPLES"]["runtime_value"] or 20)
    min_followthrough = float(runtime_config["CHASE_MIN_FOLLOWTHROUGH_60S_RATE"]["runtime_value"] or 0.58)
    max_adverse = float(runtime_config["CHASE_MAX_ADVERSE_60S_RATE"]["runtime_value"] or 0.35)
    min_completion = float(runtime_config["CHASE_REQUIRE_OUTCOME_COMPLETION_RATE"]["runtime_value"] or 0.7)

    primary_blockers: list[str] = []
    if len(candidate_rows) < min_samples:
        primary_blockers.append(f"candidate_sample_count_below_min_samples({len(candidate_rows)}<{min_samples})")
    if candidate_60["completed_rate"] is not None and float(candidate_60["completed_rate"]) < min_completion:
        primary_blockers.append(
            f"candidate_outcome_completion_rate_below_threshold({candidate_60['completed_rate']}<{min_completion})"
        )
    if candidate_60["followthrough_rate"] is not None and float(candidate_60["followthrough_rate"]) < min_followthrough:
        primary_blockers.append(
            f"candidate_followthrough_60s_rate_below_threshold({candidate_60['followthrough_rate']}<{min_followthrough})"
        )
    if candidate_60["adverse_rate"] is not None and float(candidate_60["adverse_rate"]) > max_adverse:
        primary_blockers.append(
            f"candidate_adverse_60s_rate_above_threshold({candidate_60['adverse_rate']}>{max_adverse})"
        )
    if candidate_rows and candidate_60["resolved_count"] == 0:
        primary_blockers.append("candidate_rows_have_no_resolved_direction_adjusted_60s_outcomes")

    legacy_chase_rows = [
        row
        for row in lp_rows
        if str(row.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
    ]
    candidate_to_tradeable_count = 0
    by_asset = defaultdict(list)
    for row in sorted(lp_rows, key=lambda item: (int(item["archive_ts"]), str(item.get("signal_id") or ""))):
        asset = canonical_asset(row.get("asset_symbol"))
        if asset:
            by_asset[asset].append(row)
    for rows in by_asset.values():
        saw_candidate = False
        for row in rows:
            state = str(row.get("asset_market_state_key") or "")
            if state in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
                saw_candidate = True
            if saw_candidate and state in {"TRADEABLE_LONG", "TRADEABLE_SHORT"}:
                candidate_to_tradeable_count += 1
                break

    return {
        "long_candidate_count": len(long_candidate_rows),
        "short_candidate_count": len(short_candidate_rows),
        "tradeable_long_count": len(tradeable_long_rows),
        "tradeable_short_count": len(tradeable_short_rows),
        "candidate_to_tradeable_count": candidate_to_tradeable_count,
        "candidate_outcome_completed_rate": candidate_60["completed_rate"],
        "candidate_adverse_60s_rate": candidate_60["adverse_rate"],
        "candidate_followthrough_60s_rate": candidate_60["followthrough_rate"],
        "candidate_distribution": dict(sorted(candidate_counter.items())),
        "tradeable_distribution": dict(sorted(tradeable_counter.items())),
        "candidate_outcome_60s": candidate_60,
        "tradeable_outcome_60s": tradeable_60,
        "primary_tradeable_blockers": primary_blockers,
        "legacy_chase_action_rows": len(legacy_chase_rows),
        "legacy_chase_action_delivered_rows": sum(
            1 for row in legacy_chase_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at")
        ),
    }


def compute_outcome_detail(lp_rows: list[dict[str, Any]], previous_report: dict[str, Any]) -> dict[str, Any]:
    status_by_window = {window_name: Counter() for window_name in WINDOW_NAMES}
    source_by_window = {window_name: Counter() for window_name in WINDOW_NAMES}
    failure_counter = Counter()
    catchup_completed_count = 0
    catchup_expired_count = 0
    settled_by_counter = Counter()

    for row in lp_rows:
        for window_name in WINDOW_NAMES:
            status = str(window_status_value(row, window_name, "status") or "")
            if status:
                status_by_window[window_name][status] += 1
            source = str(window_status_value(row, window_name, "price_source") or "")
            if source:
                source_by_window[window_name][source] += 1
            failure_reason = str(window_status_value(row, window_name, "failure_reason") or "")
            if failure_reason:
                failure_counter[normalize_outcome_failure_reason(failure_reason)] += 1
            window_payload = dict((row.get("outcome_windows") or {}).get(window_name) or {})
            settled_by = str(window_payload.get("settled_by") or "")
            if settled_by:
                settled_by_counter[settled_by] += 1
            is_catchup = bool(window_payload.get("catchup")) or settled_by == "outcome_scheduler_catchup"
            if is_catchup and status == "completed":
                catchup_completed_count += 1
            if is_catchup and status == "expired":
                catchup_expired_count += 1

    overall_source_distribution = Counter()
    for counter in source_by_window.values():
        overall_source_distribution.update(counter)

    previous_outcome = previous_report.get("outcome_price_source_summary", {})
    previous_source_distribution = previous_outcome.get("source_distribution", {}) if isinstance(previous_outcome, dict) else {}
    previous_total_sources = sum(int(value) for value in previous_source_distribution.values()) if previous_source_distribution else 0
    previous_pool_quote_proxy_share = None
    if previous_total_sources:
        previous_pool_quote_proxy_share = round(
            int(previous_source_distribution.get("pool_quote_proxy", 0)) / previous_total_sources,
            4,
        )
    current_total_sources = sum(overall_source_distribution.values())
    current_pool_quote_proxy_share = None
    if current_total_sources:
        current_pool_quote_proxy_share = round(
            int(overall_source_distribution.get("pool_quote_proxy", 0)) / current_total_sources,
            4,
        )

    outcome_counts: dict[str, Any] = {}
    for window_name in WINDOW_NAMES:
        for status_name in ("pending", "completed", "unavailable", "expired"):
            outcome_counts[f"outcome_{window_name}_{status_name}_count"] = int(status_by_window[window_name].get(status_name, 0))

    return {
        **outcome_counts,
        "outcome_30s_completed_rate": rate(status_by_window["30s"].get("completed", 0), len(lp_rows)),
        "outcome_60s_completed_rate": rate(status_by_window["60s"].get("completed", 0), len(lp_rows)),
        "outcome_300s_completed_rate": rate(status_by_window["300s"].get("completed", 0), len(lp_rows)),
        "outcome_price_source_distribution": dict(sorted(overall_source_distribution.items())),
        "outcome_price_source_by_window": {key: dict(sorted(value.items())) for key, value in source_by_window.items()},
        "outcome_failure_reason_distribution": dict(sorted(failure_counter.items())),
        "catchup_completed_count": int(catchup_completed_count),
        "catchup_expired_count": int(catchup_expired_count),
        "scheduler_health_summary": {
            "pending_count": sum(int(counter.get("pending", 0)) for counter in status_by_window.values()),
            "completed_count": sum(int(counter.get("completed", 0)) for counter in status_by_window.values()),
            "unavailable_count": sum(int(counter.get("unavailable", 0)) for counter in status_by_window.values()),
            "expired_count": sum(int(counter.get("expired", 0)) for counter in status_by_window.values()),
            "catchup_completed_count": int(catchup_completed_count),
            "catchup_expired_count": int(catchup_expired_count),
            "settled_by_distribution": dict(sorted(settled_by_counter.items())),
        },
        "window_status_distribution": {key: dict(sorted(value.items())) for key, value in status_by_window.items()},
        "expired_rate_by_window": {
            key: rate(counter.get("expired", 0), len(lp_rows))
            for key, counter in status_by_window.items()
        },
        "previous_report_source_distribution": previous_source_distribution,
        "previous_pool_quote_proxy_share": previous_pool_quote_proxy_share,
        "current_pool_quote_proxy_share": current_pool_quote_proxy_share,
        "pool_quote_proxy_share_delta_vs_previous": (
            None
            if previous_pool_quote_proxy_share is None or current_pool_quote_proxy_share is None
            else round(current_pool_quote_proxy_share - previous_pool_quote_proxy_share, 4)
        ),
    }


def compute_trade_action_detail(lp_rows: list[dict[str, Any]], candidate_tradeable: dict[str, Any]) -> dict[str, Any]:
    base = compute_trade_actions(lp_rows)
    distribution = dict(sorted(base.get("trade_action_distribution", {}).items()))
    delivered_distribution = Counter(
        str(row.get("trade_action_key") or "")
        for row in lp_rows
        if str(row.get("trade_action_key") or "").strip()
        and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    )

    do_not_chase_rows = [
        row
        for row in lp_rows
        if str(row.get("trade_action_key") or "") in {"DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT"}
    ]
    do_not_chase_bad = 0
    do_not_chase_resolved = 0
    for row in do_not_chase_rows:
        direction = row_direction(row)
        move = to_float(row.get("move_after_alert_300s"))
        if move is None or direction not in {"long", "short"}:
            continue
        do_not_chase_resolved += 1
        if direction == "long" and move > 0:
            do_not_chase_bad += 1
        if direction == "short" and move < 0:
            do_not_chase_bad += 1

    no_trade_rows = [
        row
        for row in lp_rows
        if str(row.get("trade_action_key") or "") in {"NO_TRADE", "WAIT_CONFIRMATION", "CONFLICT_NO_TRADE", "DATA_GAP_NO_TRADE"}
    ]
    noisy_after_no_trade = 0
    noisy_resolved = 0
    for row in no_trade_rows:
        move_300 = to_float(row.get("move_after_alert_300s"))
        if move_300 is None:
            continue
        noisy_resolved += 1
        if abs(move_300) < 0.003:
            noisy_after_no_trade += 1

    base.update(
        {
            "trade_action_distribution": distribution,
            "delivered_distribution": dict(sorted(delivered_distribution.items())),
            "do_not_chase_would_have_avoided_bad_chase_rate": rate(
                max(do_not_chase_resolved - do_not_chase_bad, 0),
                do_not_chase_resolved,
            ),
            "no_trade_high_noise_rate_300s": rate(noisy_after_no_trade, noisy_resolved),
            "long_bias_observe_count": distribution.get("LONG_BIAS_OBSERVE", 0),
            "short_bias_observe_count": distribution.get("SHORT_BIAS_OBSERVE", 0),
            "long_candidate_count": candidate_tradeable["long_candidate_count"],
            "short_candidate_count": candidate_tradeable["short_candidate_count"],
            "tradeable_count": candidate_tradeable["tradeable_long_count"] + candidate_tradeable["tradeable_short_count"],
            "legacy_chase_action_rows": candidate_tradeable["legacy_chase_action_rows"],
        }
    )
    return base


def classify_counterexample(row: dict[str, Any], lp_rows: list[dict[str, Any]], runtime_config: dict[str, dict[str, Any]]) -> list[str]:
    reasons: list[str] = []
    if str(row.get("lp_confirm_quality") or "") in {"late_confirm", "chase_risk"}:
        reasons.append("late_confirm / chase_risk")
    if str(row.get("lp_confirm_scope") or "") == "local_confirm" or str(row.get("lp_broader_alignment") or "") != "confirmed":
        reasons.append("local_confirm_not_broader_confirm")
    if str(row.get("market_context_source") or "") == "unavailable":
        reasons.append("market_context_unavailable")
    if (to_int(row.get("asset_case_supporting_pair_count")) or 0) <= 1 or not bool(row.get("asset_case_multi_pool")):
        reasons.append("single_pool_or_low_multi_pool_resonance")
    if str(row.get("lp_absorption_context") or "") in {"local_sell_pressure_absorption", "local_buy_pressure_absorption"}:
        reasons.append(str(row.get("lp_absorption_context")))
    if (
        row.get("adverse_by_direction_30s") is True
        or row.get("adverse_by_direction_60s") is True
        or row.get("adverse_by_direction_300s") is True
    ):
        reasons.append("direction_aware_adverse_outcome")

    window_sec = int(runtime_config["NO_TRADE_LOCK_WINDOW_SEC"]["runtime_value"] or 120)
    asset = canonical_asset(row.get("asset_symbol"))
    ts = int(row["archive_ts"])
    direction = row_direction(row)
    if asset and direction:
        nearby_opposite = False
        lock_triggered = False
        for candidate in lp_rows:
            if canonical_asset(candidate.get("asset_symbol")) != asset:
                continue
            delta = abs(int(candidate["archive_ts"]) - ts)
            if delta == 0 or delta > window_sec:
                continue
            if row_direction(candidate) not in {"long", "short"}:
                continue
            if row_direction(candidate) != direction and is_strong_lp_row(candidate):
                nearby_opposite = True
            if str(candidate.get("asset_market_state_key") or "") == "NO_TRADE_LOCK":
                lock_triggered = True
        if nearby_opposite and not lock_triggered:
            reasons.append("NO_TRADE_LOCK_not_triggered")

    if not reasons:
        reasons.append("possible_code_misclassification")
    return reasons


def compute_directional_adverse_summary(
    lp_rows: list[dict[str, Any]],
    runtime_config: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    bearish_rows = [row for row in lp_rows if row_direction(row) == "short"]
    bullish_rows = [row for row in lp_rows if row_direction(row) == "long"]

    def summarize(rows: list[dict[str, Any]], window_name: str) -> dict[str, Any]:
        resolved = [row for row in rows if to_float(row.get(f"move_after_alert_{window_name}")) is not None]
        adverse = [row for row in resolved if direction_adverse(row, window_name) is True]
        return {
            "resolved_count": len(resolved),
            "adverse_count": len(adverse),
            "adverse_rate": rate(len(adverse), len(resolved)),
        }

    adverse_examples = []
    reason_counter = Counter()
    resolved_for_examples = [
        row
        for row in lp_rows
        if to_float(row.get("move_after_alert_300s")) is not None and direction_adverse(row, "300s") is True
    ]
    resolved_for_examples.sort(key=lambda row: abs(to_float(row.get("move_after_alert_300s")) or 0.0), reverse=True)
    for row in resolved_for_examples[:5]:
        reasons = classify_counterexample(row, lp_rows, runtime_config)
        reason_counter.update(reasons)
        adverse_examples.append(
            {
                "signal_id": str(row.get("signal_id") or ""),
                "time_beijing": fmt_ts(int(row["archive_ts"]), BJ_TZ),
                "pair": canonical_pair_label(row.get("pair_label")),
                "stage": str(row.get("lp_alert_stage") or ""),
                "trade_action": str(row.get("trade_action_key") or ""),
                "asset_market_state": str(row.get("asset_market_state_key") or ""),
                "move_after_30s": to_float(row.get("move_after_alert_30s")),
                "move_after_60s": to_float(row.get("move_after_alert_60s")),
                "move_after_300s": to_float(row.get("move_after_alert_300s")),
                "reasons": reasons,
            }
        )

    return {
        "buy_pressure_adverse_30s_rate": summarize(bullish_rows, "30s")["adverse_rate"],
        "buy_pressure_adverse_60s_rate": summarize(bullish_rows, "60s")["adverse_rate"],
        "buy_pressure_adverse_300s_rate": summarize(bullish_rows, "300s")["adverse_rate"],
        "sell_pressure_adverse_30s_rate": summarize(bearish_rows, "30s")["adverse_rate"],
        "sell_pressure_adverse_60s_rate": summarize(bearish_rows, "60s")["adverse_rate"],
        "sell_pressure_adverse_300s_rate": summarize(bearish_rows, "300s")["adverse_rate"],
        "bearish_group_30s": summarize(bearish_rows, "30s"),
        "bearish_group_60s": summarize(bearish_rows, "60s"),
        "bearish_group_300s": summarize(bearish_rows, "300s"),
        "bullish_group_30s": summarize(bullish_rows, "30s"),
        "bullish_group_60s": summarize(bullish_rows, "60s"),
        "bullish_group_300s": summarize(bullish_rows, "300s"),
        "counterexample_reason_distribution": dict(sorted(reason_counter.items())),
        "top_counterexamples": adverse_examples,
    }


def compute_archive_detail(
    lp_rows: list[dict[str, Any]],
    delivery_summary: dict[str, Any],
    cases_summary: dict[str, Any],
    followup_summary: dict[str, Any],
    latest_date: str,
    window: dict[str, Any],
    *,
    prefer_sqlite: bool = False,
) -> dict[str, Any]:
    base = compute_archive_integrity(lp_rows, delivery_summary, cases_summary, followup_summary)
    raw_path = archive_file_for_date("raw_events", latest_date)
    parsed_path = archive_file_for_date("parsed_events", latest_date)
    signal_path = archive_file_for_date("signals", latest_date)

    parsed_event_ids: set[str] = set()
    raw_exists = raw_path is not None
    parsed_exists = parsed_path is not None
    if prefer_sqlite:
        conn = _sqlite_connect()
        if conn is not None:
            try:
                if _sqlite_table_exists(conn, "raw_events"):
                    raw_exists = bool(
                        conn.execute(
                            "SELECT 1 FROM raw_events WHERE captured_at >= ? AND captured_at <= ? LIMIT 1",
                            (int(window["start_ts"]), int(window["end_ts"])),
                        ).fetchone()
                    )
                if _sqlite_table_exists(conn, "parsed_events"):
                    parsed_rows = conn.execute(
                        "SELECT event_id FROM parsed_events WHERE parsed_at >= ? AND parsed_at <= ?",
                        (int(window["start_ts"]), int(window["end_ts"])),
                    ).fetchall()
                    parsed_event_ids = {
                        str(row["event_id"] or "")
                        for row in parsed_rows
                        if str(row["event_id"] or "").strip()
                    }
                    parsed_exists = bool(parsed_event_ids)
            finally:
                conn.close()
    elif parsed_path is not None:
        with report_open_archive_text(parsed_path) as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                ts = to_int(payload.get("archive_ts"))
                if ts is None or ts < int(window["start_ts"]) or ts > int(window["end_ts"]):
                    continue
                data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
                event_id = str(data.get("event_id") or "")
                if event_id:
                    parsed_event_ids.add(event_id)

    signal_event_ids = {
        str(row.get("event_id") or "") for row in lp_rows if str(row.get("event_id") or "").strip()
    }
    suppressed_ids = {
        str(row.get("signal_id") or "")
        for row in lp_rows
        if str(row.get("telegram_suppression_reason") or "").strip()
        or str(row.get("telegram_update_kind") or "") == "suppressed"
    }
    matched_signal_ids = set(delivery_summary.get("matched_signal_ids") or set())

    base.update(
        {
            "raw_archive_exists": raw_exists,
            "parsed_archive_exists": parsed_exists,
            "signals_archive_exists": signal_path is not None,
            "signal_event_in_parsed_rate": rate(len(signal_event_ids & parsed_event_ids), len(signal_event_ids)),
            "suppressed_signal_archive_count": len(suppressed_ids),
            "suppressed_signal_delivery_match_rate": rate(len(suppressed_ids & matched_signal_ids), len(suppressed_ids)),
        }
    )
    return base


def compute_noise_assessment(
    lp_rows: list[dict[str, Any]],
    telegram: dict[str, Any],
    trade_actions: dict[str, Any],
    state_summary: dict[str, Any],
) -> dict[str, Any]:
    sent_rows = [row for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at")]
    directed_sent = [
        row for row in sent_rows if row_direction(row) in {"long", "short"}
    ]
    conflict_pairs = 0
    for prev, curr in zip(directed_sent, directed_sent[1:]):
        if canonical_asset(prev.get("asset_symbol")) != canonical_asset(curr.get("asset_symbol")):
            continue
        if row_direction(prev) == row_direction(curr):
            continue
        if int(curr["archive_ts"]) - int(prev["archive_ts"]) <= 300:
            conflict_pairs += 1

    return {
        "delivered_lp_ratio": rate(len(sent_rows), len(lp_rows)),
        "suppressed_ratio": telegram["telegram_suppression_ratio"],
        "high_value_suppressed_count": telegram["high_value_suppressed_count"],
        "direction_conflict_pairs_within_5m": conflict_pairs,
        "state_change_message_count": state_summary["state_change_message_count"],
        "risk_blocker_message_count": trade_actions.get("delivered_distribution", {}).get("CONFLICT_NO_TRADE", 0),
    }


def compute_scorecard(
    run_overview: dict[str, Any],
    telegram: dict[str, Any],
    states: dict[str, Any],
    no_trade_lock: dict[str, Any],
    prealerts: dict[str, Any],
    candidate_tradeable: dict[str, Any],
    outcomes: dict[str, Any],
    market_context: dict[str, Any],
    trade_actions: dict[str, Any],
    majors: dict[str, Any],
    noise: dict[str, Any],
) -> dict[str, float]:
    readiness = 8.0
    if majors.get("current_sample_still_eth_only"):
        readiness -= 1.8
    if int(run_overview["lp_signal_rows"]) < 80:
        readiness -= 1.2
    if int(run_overview["total_raw_events"]) == 0 or int(run_overview["total_parsed_events"]) == 0:
        readiness -= 2.0
    readiness = max(0.0, round(readiness, 1))

    telegram_score = 5.5
    suppression_ratio = to_float(telegram["telegram_suppression_ratio"]) or 0.0
    telegram_score += min(3.0, suppression_ratio * 3.5)
    if int(telegram["high_value_suppressed_count"]) > 0:
        telegram_score -= min(2.0, int(telegram["high_value_suppressed_count"]) * 0.25)
    telegram_score = max(0.0, round(min(10.0, telegram_score), 1))

    state_score = 4.5
    if states["state_changed_count"] > 0:
        state_score += 2.0
    if len(states["state_distribution"]) >= 5:
        state_score += 1.5
    if states["repeated_state_suppressed_count"] > 0:
        state_score += 0.8
    state_score = max(0.0, round(min(10.0, state_score), 1))

    lock_score = 3.5
    if no_trade_lock["lock_entered_count"] > 0:
        lock_score += 2.0
    if no_trade_lock["lock_suppressed_count"] > 0:
        lock_score += 2.0
    if no_trade_lock["lock_released_count"] > 0:
        lock_score += 1.0
    if no_trade_lock["missed_lock_examples"]:
        lock_score -= 1.0
    lock_score = max(0.0, round(min(10.0, lock_score), 1))

    prealert_score = 2.5
    if prealerts["prealert_candidate_count"] > 0:
        prealert_score += 1.0
    if prealerts["first_seen_stage_prealert_count"] > 0:
        prealert_score += 2.0
    if prealerts["median_prealert_to_confirm_sec"] is not None:
        prealert_score += 1.0
    if prealerts["prealert_count"] <= 1:
        prealert_score -= 0.5
    prealert_score = max(0.0, round(min(10.0, prealert_score), 1))

    candidate_score = 4.5
    if candidate_tradeable["long_candidate_count"] + candidate_tradeable["short_candidate_count"] > 0:
        candidate_score += 1.0
    if candidate_tradeable["tradeable_long_count"] + candidate_tradeable["tradeable_short_count"] == 0:
        candidate_score -= 0.7
    if candidate_tradeable["legacy_chase_action_rows"] > 0:
        candidate_score -= 0.8
    if candidate_tradeable["candidate_outcome_completed_rate"] is not None:
        candidate_score += 0.8
    candidate_score = max(0.0, round(min(10.0, candidate_score), 1))

    outcome_score = 3.0
    for key in ("outcome_30s_completed_rate", "outcome_60s_completed_rate", "outcome_300s_completed_rate"):
        value = to_float(outcomes.get(key))
        if value is not None:
            outcome_score += value * 2.0
    if outcomes["current_pool_quote_proxy_share"] is not None and float(outcomes["current_pool_quote_proxy_share"]) < 0.25:
        outcome_score += 1.0
    outcome_score = max(0.0, round(min(10.0, outcome_score), 1))

    market_score = 3.5
    live_public_rate = to_float(market_context.get("live_public_rate"))
    if live_public_rate is not None:
        market_score += min(5.0, live_public_rate * 5.0)
    if int(market_context.get("okx_failure") or 0) == 0 and int(market_context.get("okx_success") or 0) > 0:
        market_score += 1.0
    market_score = max(0.0, round(min(10.0, market_score), 1))

    trade_action_score = 5.0
    if trade_actions.get("do_not_chase_would_have_avoided_bad_chase_rate") is not None:
        trade_action_score += 2.0 * float(trade_actions["do_not_chase_would_have_avoided_bad_chase_rate"])
    if trade_actions.get("legacy_chase_action_rows", 0) > 0:
        trade_action_score -= 0.8
    if trade_actions.get("conflict_no_trade_count", 0) > 0:
        trade_action_score += 0.6
    trade_action_score = max(0.0, round(min(10.0, trade_action_score), 1))

    noise_score = 5.0
    if noise["suppressed_ratio"] is not None:
        noise_score += min(2.5, float(noise["suppressed_ratio"]) * 3.0)
    if int(noise["high_value_suppressed_count"]) > 0:
        noise_score -= min(2.0, int(noise["high_value_suppressed_count"]) * 0.2)
    if int(noise["direction_conflict_pairs_within_5m"]) > 0:
        noise_score -= min(1.5, int(noise["direction_conflict_pairs_within_5m"]) * 0.2)
    noise_score = max(0.0, round(min(10.0, noise_score), 1))

    overall = round(
        statistics.mean(
            [
                readiness,
                telegram_score,
                state_score,
                lock_score,
                prealert_score,
                candidate_score,
                outcome_score,
                market_score,
                trade_action_score,
                noise_score,
            ]
        ),
        1,
    )

    return {
        "research_sampling_readiness": readiness,
        "telegram_denoise_effect": telegram_score,
        "asset_market_state_usability": state_score,
        "no_trade_lock_protection_value": lock_score,
        "prealert_lifecycle_effectiveness": prealert_score,
        "candidate_tradeable_credibility": candidate_score,
        "outcome_completeness": outcome_score,
        "live_market_context_readiness": market_score,
        "trade_action_assistive_value": trade_action_score,
        "noise_control": noise_score,
        "overall_self_use_score": overall,
    }


def baseline_previous_report() -> dict[str, Any]:
    path = REPORTS_DIR / "overnight_trade_action_summary_latest.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def append_metric(
    rows: list[dict[str, Any]],
    metric_group: str,
    metric_name: str,
    value: Any,
    *,
    asset: str = "",
    pair: str = "",
    stage: str = "",
    trade_action: str = "",
    asset_market_state: str = "",
    sample_size: Any = "",
    window: str = "",
    notes: str = "",
) -> None:
    rows.append(
        {
            "metric_group": metric_group,
            "metric_name": metric_name,
            "asset": asset,
            "pair": pair,
            "stage": stage,
            "trade_action": trade_action,
            "asset_market_state": asset_market_state,
            "value": value,
            "sample_size": sample_size,
            "window": window,
            "notes": notes,
        }
    )


def build_csv_rows(
    window: dict[str, Any],
    run_overview: dict[str, Any],
    stages: dict[str, Any],
    states: dict[str, Any],
    no_trade_lock: dict[str, Any],
    telegram: dict[str, Any],
    prealerts: dict[str, Any],
    candidate_tradeable: dict[str, Any],
    opportunities: dict[str, Any],
    outcomes: dict[str, Any],
    market_context: dict[str, Any],
    trade_actions: dict[str, Any],
    adverse_direction: dict[str, Any],
    archive_integrity: dict[str, Any],
    majors: dict[str, Any],
    scorecard: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    window_label = f"{window['start_utc']} -> {window['end_utc']}"

    for key, value in run_overview.items():
        append_metric(rows, "run_overview", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for stage, value in stages["stage_distribution_pct"].items():
        append_metric(rows, "lp_stage", "stage_distribution_pct", value, stage=stage, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in stages.items():
        if key == "stage_distribution_pct":
            continue
        append_metric(rows, "lp_stage", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for state_key, value in states["state_distribution"].items():
        append_metric(rows, "asset_market_state", "state_distribution", value, asset_market_state=state_key, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for asset, payload in states["final_state_by_asset"].items():
        append_metric(rows, "asset_market_state", "final_state_by_asset", payload["state_key"], asset=asset, asset_market_state=payload["state_key"], sample_size=1, window=window_label)
    for key, value in no_trade_lock.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "no_trade_lock", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in telegram.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "telegram", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for reason, value in telegram["telegram_suppression_reasons"].items():
        append_metric(rows, "telegram", "telegram_suppression_reasons", value, notes=reason, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in prealerts.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "prealert", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in candidate_tradeable.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "candidate_tradeable", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for state_key, value in candidate_tradeable["candidate_distribution"].items():
        append_metric(rows, "candidate_tradeable", "candidate_distribution", value, asset_market_state=state_key, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for state_key, value in candidate_tradeable["tradeable_distribution"].items():
        append_metric(rows, "candidate_tradeable", "tradeable_distribution", value, asset_market_state=state_key, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in opportunities.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "trade_opportunity", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for window_name, distribution in outcomes["window_status_distribution"].items():
        for status_name, value in distribution.items():
            append_metric(rows, "outcome", f"{window_name}_status", value, window=window_name, sample_size=run_overview["lp_signal_rows"], notes=status_name)
    for source_name, value in outcomes["outcome_price_source_distribution"].items():
        append_metric(rows, "outcome", "outcome_price_source_distribution", value, stage=source_name, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for reason, value in outcomes["outcome_failure_reason_distribution"].items():
        append_metric(rows, "outcome", "outcome_failure_reason_distribution", value, stage=reason, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key in (
        "outcome_30s_pending_count",
        "outcome_30s_completed_count",
        "outcome_30s_unavailable_count",
        "outcome_30s_expired_count",
        "outcome_30s_completed_rate",
        "outcome_60s_completed_rate",
        "outcome_300s_completed_rate",
        "catchup_completed_count",
        "catchup_expired_count",
    ):
        append_metric(rows, "outcome", key, outcomes.get(key), sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in market_context.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "market_context", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for action, value in trade_actions["trade_action_distribution"].items():
        append_metric(rows, "trade_action", "trade_action_distribution", value, trade_action=action, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in trade_actions.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "trade_action", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in adverse_direction.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "adverse_direction", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in archive_integrity.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(rows, "archive", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)

    append_metric(rows, "majors", "covered_major_pairs", "|".join(majors["covered_major_pairs"]), sample_size=len(majors["covered_major_pairs"]), window=window_label)
    append_metric(rows, "majors", "missing_major_pairs", "|".join(majors["missing_major_pairs"]), sample_size=len(majors["missing_major_pairs"]), window=window_label)
    append_metric(rows, "majors", "eth_signal_count", majors["eth_signal_count"], asset="ETH", sample_size=run_overview["lp_signal_rows"], window=window_label)
    append_metric(rows, "majors", "btc_signal_count", majors["btc_signal_count"], asset="BTC", sample_size=run_overview["lp_signal_rows"], window=window_label)
    append_metric(rows, "majors", "sol_signal_count", majors["sol_signal_count"], asset="SOL", sample_size=run_overview["lp_signal_rows"], window=window_label)

    for key, value in scorecard.items():
        append_metric(rows, "score", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "metric_group",
                "metric_name",
                "asset",
                "pair",
                "stage",
                "trade_action",
                "asset_market_state",
                "value",
                "sample_size",
                "window",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def build_top_findings(
    run_overview: dict[str, Any],
    telegram: dict[str, Any],
    states: dict[str, Any],
    no_trade_lock: dict[str, Any],
    prealerts: dict[str, Any],
    candidate_tradeable: dict[str, Any],
    outcomes: dict[str, Any],
    market_context: dict[str, Any],
    trade_actions: dict[str, Any],
    majors: dict[str, Any],
    archive_integrity: dict[str, Any],
) -> list[str]:
    findings = [
        f"selected a fully covered Beijing afternoon-evening window from {run_overview['analysis_window_start_beijing']} to {run_overview['analysis_window_end_beijing']} ({run_overview['duration_hours']}h); raw/parsed/signals/delivery/cases all overlap in this interval.",
        f"LP sample size in the selected window is {run_overview['lp_signal_rows']} rows with {run_overview['delivered_lp_signals']} delivered LP Telegram messages.",
        f"Telegram denoise is materially active: delivered/raw ratio is {rate(run_overview['delivered_lp_signals'], run_overview['lp_signal_rows'])}, suppression ratio is {telegram['telegram_suppression_ratio']}, and suppression reasons are dominated by {telegram['telegram_suppression_reasons']}.",
        f"asset-level market state is live in real data: {states['state_changed_count']} state changes, distribution {states['state_distribution']}, final states {states['final_state_by_asset']}.",
        f"NO_TRADE_LOCK is real rather than theoretical: entered={no_trade_lock['lock_entered_count']} suppressed={no_trade_lock['lock_suppressed_count']} released={no_trade_lock['lock_released_count']}.",
        f"prealert lifecycle evidence is limited in this window: prealert stage rows={prealerts['prealert_count']}, first_seen_stage=prealert count={prealerts['first_seen_stage_prealert_count']}, median_prealert_to_confirm_sec={prealerts['median_prealert_to_confirm_sec']}.",
        f"candidate/tradeable ladder is only partially validated: candidate rows={candidate_tradeable['long_candidate_count'] + candidate_tradeable['short_candidate_count']}, tradeable rows={candidate_tradeable['tradeable_long_count'] + candidate_tradeable['tradeable_short_count']}, blockers={candidate_tradeable['primary_tradeable_blockers']}.",
        f"outcome price sourcing shifted away from prior pool_quote_proxy-heavy behavior: current sources={outcomes['outcome_price_source_distribution']}, previous pool_quote_proxy share={outcomes['previous_pool_quote_proxy_share']}, current share={outcomes['current_pool_quote_proxy_share']}.",
        f"live market context is stable in the selected window: live_public_rate={market_context['live_public_rate']}, unavailable_rate={market_context['unavailable_rate']}, okx_attempts={market_context['okx_attempts']}, kraken_attempts={market_context['kraken_attempts']}.",
        f"majors coverage is still ETH-only in the selected window: covered={majors['covered_major_pairs']} missing={majors['missing_major_pairs']}.",
        f"archive integrity is strong on the signal->delivery->followup path: signal_delivery_audit_match_rate={archive_integrity['signal_delivery_audit_match_rate']} signal_case_followup_match_rate={archive_integrity['signal_case_followup_match_rate']}.",
        f"trade_action remains useful as an assistive hint, but legacy chase labels still appear {trade_actions['legacy_chase_action_rows']} times even though asset_market_state only reached candidate states.",
    ]
    return findings[:12]


def build_recommendations(
    telegram: dict[str, Any],
    candidate_tradeable: dict[str, Any],
    outcomes: dict[str, Any],
    majors: dict[str, Any],
    prealerts: dict[str, Any],
) -> list[str]:
    recommendations = []
    if candidate_tradeable["legacy_chase_action_rows"] > 0:
        recommendations.append("fully eliminate legacy LONG_CHASE_ALLOWED / SHORT_CHASE_ALLOWED wording from user-facing and archived action summaries when the state machine only wants candidate status.")
    if prealerts["first_seen_stage_prealert_count"] == 0 or prealerts["prealert_count"] <= 1:
        recommendations.append("collect more Beijing-day prealert samples and verify first_seen_stage/prealert_to_confirm_sec survive all later upgrades.")
    if outcomes["outcome_60s_completed_rate"] is not None and float(outcomes["outcome_60s_completed_rate"]) < 0.7:
        recommendations.append("keep improving 30s/60s outcome completion until short-window reversal checks are reliably usable.")
    if majors.get("current_sample_still_eth_only"):
        recommendations.append("prioritize enabling and validating BTC/SOL major pools before expanding any long-tail coverage.")
    if telegram["high_value_suppressed_count"] > 0:
        recommendations.append("audit suppressed high-value rows and confirm they were delivery-policy drops rather than unintended Telegram suppression.")
    return recommendations[:6]


def build_limitations(
    data_sources_missing: list[str],
    prealerts: dict[str, Any],
    candidate_tradeable: dict[str, Any],
    outcomes: dict[str, Any],
    majors: dict[str, Any],
) -> list[str]:
    limitations = []
    if data_sources_missing:
        limitations.append(f"missing data sources: {data_sources_missing}")
    if prealerts["prealert_count"] <= 1:
        limitations.append("selected window contains too few prealert-stage rows to fully validate the new prealert lifecycle on its own.")
    if candidate_tradeable["tradeable_long_count"] + candidate_tradeable["tradeable_short_count"] == 0:
        limitations.append("selected window contains no TRADEABLE_* states, so candidate-to-tradeable promotion is only validated negatively (blocked), not positively (triggered).")
    if outcomes["outcome_30s_completed_rate"] is not None and float(outcomes["outcome_30s_completed_rate"]) < 0.7:
        limitations.append("30s outcome completion is still incomplete enough that very short-term reversal conclusions must stay conservative.")
    if majors.get("current_sample_still_eth_only"):
        limitations.append("BTC/SOL have no real selected-window samples, so market-state, lock, and outcome conclusions are ETH-centric.")
    return limitations


def build_markdown(
    data_sources: list[dict[str, Any]],
    window: dict[str, Any],
    runtime_config: dict[str, dict[str, Any]],
    run_overview: dict[str, Any],
    stages: dict[str, Any],
    states: dict[str, Any],
    telegram: dict[str, Any],
    no_trade_lock: dict[str, Any],
    prealerts: dict[str, Any],
    candidate_tradeable: dict[str, Any],
    final_outputs: dict[str, Any],
    opportunities: dict[str, Any],
    outcomes: dict[str, Any],
    market_context: dict[str, Any],
    trade_actions: dict[str, Any],
    adverse_direction: dict[str, Any],
    archive_integrity: dict[str, Any],
    majors: dict[str, Any],
    noise: dict[str, Any],
    scorecard: dict[str, Any],
    cli_market_context: dict[str, Any],
    cli_major: dict[str, Any],
    cli_summary: dict[str, Any],
    top_findings: list[str],
    top_recommendations: list[str],
    limitations: list[str],
) -> str:
    opportunities = normalize_opportunity_summary(opportunities)
    lines: list[str] = []
    lines.append("# Afternoon / Evening State Analysis")
    lines.append("")
    lines.append("## 1. 执行摘要")
    lines.append("")
    for item in top_findings[:8]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 2. 数据源与完整性说明")
    lines.append("")
    for item in data_sources:
        lines.append(
            f"- `{item['path']}` | records=`{item['record_count']}` | range_utc=`{item['start_utc']} -> {item['end_utc']}` | "
            f"range_bj=`{item['start_beijing']} -> {item['end_beijing']}` | window_records=`{item['window_record_count']}` | note=`{item['notes']}`"
        )
    lines.append("")
    lines.append("## 3. 北京时间下午到晚上分析窗口")
    lines.append("")
    lines.append(f"- `cutoff_beijing={window['cutoff_beijing']}`")
    lines.append(f"- `analysis_window_utc={window['start_utc']} -> {window['end_utc']}`")
    lines.append(f"- `analysis_window_server={window['start_server_local']} -> {window['end_server_local']}`")
    lines.append(f"- `analysis_window_beijing={window['start_beijing']} -> {window['end_beijing']}`")
    lines.append(f"- `duration_hours={window['duration_hours']}`")
    lines.append(f"- selection_reason: {window['selection_reason']}")
    lines.append(f"- partial_lead_counts_before_window={window['partial_lead_counts_before_window']}")
    lines.append(f"- gap_summary={window['required_gap_summary']}")
    lines.append("")
    lines.append("## 4. 非敏感运行配置摘要")
    lines.append("")
    for key in SAFE_CONFIG_KEYS:
        lines.append(f"- `{key}` = `{runtime_config[key]['runtime_value']}`")
    lines.append("")
    lines.append("## 5. asset-level market state 总览")
    lines.append("")
    lines.append(f"- `state_distribution={states['state_distribution']}`")
    lines.append(f"- `state_transition_count={states['state_transition_count']}` `state_changed_count={states['state_changed_count']}`")
    lines.append(f"- `repeated_state_suppressed_count={states['repeated_state_suppressed_count']}`")
    lines.append(f"- `final_state_by_asset={states['final_state_by_asset']}`")
    lines.append(f"- `eth_state_path={states['eth_state_path']}`")
    lines.append("")
    lines.append("## 6. Telegram 降噪与 suppression 分析")
    lines.append("")
    lines.append(f"- `messages_before_suppression_estimate={telegram['messages_before_suppression_estimate']}` `messages_after_suppression_actual={telegram['messages_after_suppression_actual']}`")
    lines.append(f"- `telegram_should_send_count={telegram['telegram_should_send_count']}` `telegram_suppressed_count={telegram['telegram_suppressed_count']}`")
    lines.append(f"- `telegram_suppression_ratio={telegram['telegram_suppression_ratio']}`")
    lines.append(f"- `telegram_suppression_reasons={telegram['telegram_suppression_reasons']}`")
    lines.append(f"- `telegram_update_kind_distribution={telegram['telegram_update_kind_distribution']}`")
    lines.append(f"- `high_value_suppressed_count={telegram['high_value_suppressed_count']}` `should_send_but_not_sent_count={telegram['should_send_but_not_sent_count']}`")
    lines.append("")
    lines.append("## 7. NO_TRADE_LOCK 分析")
    lines.append("")
    lines.append(f"- `lock_entered_count={no_trade_lock['lock_entered_count']}` `lock_suppressed_count={no_trade_lock['lock_suppressed_count']}` `lock_released_count={no_trade_lock['lock_released_count']}`")
    lines.append(f"- `avg_lock_duration_sec={no_trade_lock['avg_lock_duration_sec']}` `median_lock_duration_sec={no_trade_lock['median_lock_duration_sec']}`")
    lines.append(f"- `lock_release_reasons={no_trade_lock['lock_release_reasons']}`")
    lines.append(f"- `lock_suppressed_stage_distribution={no_trade_lock['lock_suppressed_stage_distribution']}`")
    lines.append(f"- `missed_lock_examples={no_trade_lock['missed_lock_examples']}`")
    lines.append("")
    lines.append("## 8. prealert 生命周期分析")
    lines.append("")
    for key in [
        "prealert_count",
        "prealert_candidate_count",
        "prealert_gate_passed_count",
        "prealert_active_count",
        "prealert_delivered_count",
        "prealert_merged_count",
        "prealert_upgraded_to_confirm_count",
        "prealert_expired_count",
        "prealert_suppressed_by_lock_count",
        "median_prealert_to_confirm_sec",
        "first_seen_stage_prealert_count",
        "prealert_stage_overwritten_count",
    ]:
        lines.append(f"- `{key}={prealerts[key]}`")
    lines.append(f"- `first_seen_stage_distribution={prealerts['first_seen_stage_distribution']}`")
    lines.append("")
    lines.append("## 9. candidate vs tradeable 分析")
    lines.append("")
    for key in [
        "long_candidate_count",
        "short_candidate_count",
        "tradeable_long_count",
        "tradeable_short_count",
        "candidate_to_tradeable_count",
        "candidate_outcome_completed_rate",
        "candidate_followthrough_60s_rate",
        "candidate_adverse_60s_rate",
        "legacy_chase_action_rows",
    ]:
        lines.append(f"- `{key}={candidate_tradeable[key]}`")
    lines.append(f"- `primary_tradeable_blockers={candidate_tradeable['primary_tradeable_blockers']}`")
    lines.append("")
    lines.append("## 9A. trade_opportunity 分析")
    lines.append("")
    lines.append(f"- `opportunity_summary={opportunities.get('opportunity_summary') or dict()}`")
    lines.append(f"- `verified_maturity={opportunities.get('verified_maturity', 'unknown')}` `verified_should_not_be_traded_reason={opportunities.get('verified_should_not_be_traded_reason', '')}` `maturity_reasons={opportunities.get('maturity_reasons') or []}`")
    lines.append(f"- `opportunity_score_median={opportunities.get('opportunity_score_median')}` `opportunity_score_p90={opportunities.get('opportunity_score_p90')}`")
    lines.append(f"- `raw_score_vs_calibrated_score={opportunities.get('raw_score_vs_calibrated_score') or dict()}`")
    lines.append(f"- `calibration_adjustment_distribution={opportunities.get('calibration_adjustment_distribution') or dict()}`")
    lines.append(f"- `candidate_outcome_60s={opportunities.get('candidate_outcome_60s') or dict()}`")
    lines.append(f"- `verified_outcome_60s={opportunities.get('verified_outcome_60s') or dict()}`")
    lines.append(f"- `opportunity_profile_count={opportunities.get('opportunity_profile_count')}`")
    lines.append(f"- `top_profiles_by_sample={opportunities.get('top_profiles_by_sample') or []}`")
    lines.append(f"- `top_profiles_by_followthrough={opportunities.get('top_profiles_by_followthrough') or []}`")
    lines.append(f"- `top_profiles_by_adverse={opportunities.get('top_profiles_by_adverse') or []}`")
    lines.append(f"- `profiles_ready_for_verified={opportunities.get('profiles_ready_for_verified') or []}`")
    lines.append(f"- `estimated_samples_needed_for_verified_by_profile={opportunities.get('estimated_samples_needed_for_verified_by_profile') or dict()}`")
    lines.append(f"- `final_trading_output_distribution={final_outputs['final_trading_output_distribution']}`")
    lines.append(f"- `legacy_chase_downgraded_count={final_outputs['legacy_chase_downgraded_count']}` `legacy_chase_leaked_count={final_outputs['legacy_chase_leaked_count']}` `messages_blocked_by_opportunity_gate={final_outputs['messages_blocked_by_opportunity_gate']}`")
    lines.append(f"- `all_opportunity_labels_verified={final_outputs['all_opportunity_labels_verified']}` `all_candidate_labels_are_candidate={final_outputs['all_candidate_labels_are_candidate']}` `blocked_covers_legacy_chase_risk={final_outputs['blocked_covers_legacy_chase_risk']}`")
    lines.append(f"- `blocker_effectiveness={opportunities.get('blocker_effectiveness') or dict()}`")
    lines.append(f"- `hard_blocker_distribution={opportunities.get('hard_blocker_distribution') or dict()}`")
    lines.append(f"- `verification_blocker_distribution={opportunities.get('verification_blocker_distribution') or dict()}`")
    lines.append(f"- `non_lp_evidence_summary={opportunities.get('non_lp_evidence_summary') or dict()}`")
    lines.append(f"- `opportunity_non_lp_support_count={opportunities.get('opportunity_non_lp_support_count')}` `opportunity_non_lp_risk_count={opportunities.get('opportunity_non_lp_risk_count')}`")
    lines.append(f"- `top_non_lp_supporting_evidence={opportunities.get('top_non_lp_supporting_evidence') or []}`")
    lines.append(f"- `top_non_lp_blockers={opportunities.get('top_non_lp_blockers') or []}`")
    lines.append(f"- `opportunities_upgraded_by_non_lp={opportunities.get('opportunities_upgraded_by_non_lp')}` `opportunities_blocked_by_non_lp={opportunities.get('opportunities_blocked_by_non_lp')}`")
    lines.append(f"- `opportunities_upgraded_by_calibration={opportunities.get('opportunities_upgraded_by_calibration')}` `opportunities_downgraded_by_calibration={opportunities.get('opportunities_downgraded_by_calibration')}`")
    lines.append(f"- `candidates_blocked_by_calibration={opportunities.get('candidates_blocked_by_calibration')}` `verified_allowed_by_calibration={opportunities.get('verified_allowed_by_calibration')}`")
    lines.append(f"- `top_positive_adjustments={opportunities.get('top_positive_adjustments') or []}`")
    lines.append(f"- `top_negative_adjustments={opportunities.get('top_negative_adjustments') or []}`")
    lines.append(f"- `calibration_reason_distribution={opportunities.get('calibration_reason_distribution') or dict()}`")
    lines.append(f"- `calibration_source_distribution={opportunities.get('calibration_source_distribution') or dict()}`")
    lines.append(f"- `calibration_confidence_distribution={opportunities.get('calibration_confidence_distribution') or dict()}`")
    lines.append(f"- `non_lp_conflict_cases={opportunities.get('non_lp_conflict_cases') or []}`")
    lines.append(f"- `opportunity_budget_suppressed_count={opportunities.get('opportunity_budget_suppressed_count')}` `opportunity_cooldown_suppressed_count={opportunities.get('opportunity_cooldown_suppressed_count')}`")
    lines.append(f"- `why_no_opportunities={opportunities.get('why_no_opportunities') or []}`")
    lines.append(f"- `top_blockers={opportunities.get('top_blockers') or dict()}`")
    lines.append(f"- `next_threshold_suggestions={opportunities.get('next_threshold_suggestions') or []}`")
    lines.append("")
    lines.append("## 10. outcome price source 与 30s/60s/300s 分析")
    lines.append("")
    lines.append(f"- `window_status_distribution={outcomes['window_status_distribution']}`")
    lines.append(f"- `outcome_price_source_distribution={outcomes['outcome_price_source_distribution']}`")
    lines.append(f"- `outcome_failure_reason_distribution={outcomes['outcome_failure_reason_distribution']}`")
    lines.append(f"- `scheduler_health_summary={outcomes['scheduler_health_summary']}`")
    lines.append(f"- `expired_rate_by_window={outcomes['expired_rate_by_window']}`")
    lines.append(f"- previous baseline `source_distribution={outcomes['previous_report_source_distribution']}`")
    lines.append(f"- `pool_quote_proxy_share_delta_vs_previous={outcomes['pool_quote_proxy_share_delta_vs_previous']}`")
    lines.append("")
    lines.append("## 11. OKX/Kraken live market context 分析")
    lines.append("")
    lines.append(f"- window `live_public_count={market_context['live_public_count']}` `unavailable_count={market_context['unavailable_count']}`")
    lines.append(f"- window `okx_attempts={market_context['okx_attempts']}` `okx_success={market_context['okx_success']}` `okx_failure={market_context['okx_failure']}`")
    lines.append(f"- window `kraken_attempts={market_context['kraken_attempts']}` `kraken_success={market_context['kraken_success']}` `kraken_failure={market_context['kraken_failure']}`")
    lines.append(f"- window `requested_to_resolved_distribution={market_context['requested_to_resolved_distribution']}`")
    lines.append(f"- CLI full `live_public_hit_rate={cli_market_context.get('live_public_hit_rate')}` `unavailable_rate={cli_market_context.get('unavailable_rate')}`")
    lines.append("")
    lines.append("## 12. trade_action 与交易辅助价值分析")
    lines.append("")
    lines.append(f"- `trade_action_distribution={trade_actions['trade_action_distribution']}`")
    lines.append(f"- `delivered_distribution={trade_actions['delivered_distribution']}`")
    lines.append(f"- `do_not_chase_would_have_avoided_bad_chase_rate={trade_actions['do_not_chase_would_have_avoided_bad_chase_rate']}`")
    lines.append(f"- `no_trade_high_noise_rate_300s={trade_actions['no_trade_high_noise_rate_300s']}`")
    lines.append(f"- `generic_confirm_success_rate_300s={trade_actions['generic_confirm_success_rate_300s']}` `generic_confirm_adverse_rate_300s={trade_actions['generic_confirm_adverse_rate_300s']}`")
    lines.append("")
    lines.append("## 13. 卖压后涨 / 买压后跌 反例专项")
    lines.append("")
    lines.append(f"- `sell_pressure_adverse_30s_rate={adverse_direction['sell_pressure_adverse_30s_rate']}`")
    lines.append(f"- `sell_pressure_adverse_60s_rate={adverse_direction['sell_pressure_adverse_60s_rate']}`")
    lines.append(f"- `sell_pressure_adverse_300s_rate={adverse_direction['sell_pressure_adverse_300s_rate']}`")
    lines.append(f"- `buy_pressure_adverse_30s_rate={adverse_direction['buy_pressure_adverse_30s_rate']}`")
    lines.append(f"- `buy_pressure_adverse_60s_rate={adverse_direction['buy_pressure_adverse_60s_rate']}`")
    lines.append(f"- `buy_pressure_adverse_300s_rate={adverse_direction['buy_pressure_adverse_300s_rate']}`")
    lines.append(f"- `counterexample_reason_distribution={adverse_direction['counterexample_reason_distribution']}`")
    lines.append(f"- `top_counterexamples={adverse_direction['top_counterexamples']}`")
    lines.append("")
    lines.append("## 14. raw/parsed/signals archive 完整性")
    lines.append("")
    for key in [
        "raw_archive_exists",
        "parsed_archive_exists",
        "signals_archive_exists",
        "signal_delivery_audit_match_rate",
        "signal_case_followup_match_rate",
        "signal_case_attached_match_rate",
        "delivered_signal_notifier_sent_at_rate",
        "signal_event_in_parsed_rate",
        "suppressed_signal_delivery_match_rate",
    ]:
        lines.append(f"- `{key}={archive_integrity[key]}`")
    lines.append("")
    lines.append("## 15. majors 覆盖与样本代表性")
    lines.append("")
    lines.append(f"- `covered_major_pairs={majors['covered_major_pairs']}`")
    lines.append(f"- `missing_major_pairs={majors['missing_major_pairs']}`")
    lines.append(f"- `asset_distribution={majors['asset_distribution']}`")
    lines.append(f"- `pair_distribution={majors['pair_distribution']}`")
    lines.append(f"- CLI `recommended_next_round_pairs={cli_major.get('recommended_next_round_pairs')}`")
    lines.append("")
    lines.append("## 16. 噪音与误判风险评估")
    lines.append("")
    for key, value in noise.items():
        lines.append(f"- `{key}={value}`")
    lines.append("")
    lines.append("## 17. 最终评分")
    lines.append("")
    for key, value in scorecard.items():
        lines.append(f"- `{key}={value}/10`")
    lines.append("")
    lines.append("## 18. 下一轮建议")
    lines.append("")
    for item in top_recommendations:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 19. 限制与不确定性")
    lines.append("")
    for item in limitations:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 附录: quality_reports CLI 参考")
    lines.append("")
    lines.append(f"- `market-context-health.live_public_hit_rate={cli_market_context.get('live_public_hit_rate')}`")
    lines.append(f"- `major-pool-coverage.covered_major_pairs={cli_major.get('covered_major_pairs')}`")
    lines.append(f"- `summary.overall={cli_summary.get('overall')}`")
    return "\n".join(lines) + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate afternoon/evening state analysis latest report")
    parser.add_argument("--date", help="Use archive date YYYY-MM-DD as the logical report date")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    runtime_config = load_runtime_config()
    sqlite_source = sqlite_report_source_summary(fast=bool(args.date))
    latest_date = args.date or latest_archive_date()
    window = choose_analysis_window(latest_date, prefer_sqlite=bool(args.date))
    selected_logical_date = str(window["end_utc"])[:10]
    if args.date and selected_logical_date != args.date:
        raise RuntimeError(
            f"requested logical date {args.date} resolved to afternoon/evening window ending on {selected_logical_date}"
        )
    selected_archive_dates = archive_dates_for_window(window)

    signal_rows, signal_inventory = load_signals(window=window, archive_dates=selected_archive_dates)
    quality_rows, quality_by_signal, quality_inventory = load_quality_cache()
    asset_case_payload, asset_case_inventory = load_asset_case_cache()
    asset_state_records, asset_state_inventory = load_asset_market_state_cache()
    trade_opportunity_cache, trade_opportunity_inventory = load_trade_opportunity_cache()

    window_signal_rows, _, lp_rows = join_lp_rows(
        signal_rows,
        quality_by_signal,
        int(window["start_ts"]),
        int(window["end_ts"]),
    )

    signal_ids = {str(row.get("signal_id") or "") for row in lp_rows if row.get("signal_id")}
    delivered_signal_ids = {
        str(row.get("signal_id") or "")
        for row in lp_rows
        if row.get("signal_id") and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    }
    delivery_inventory, delivery_summary = stream_delivery_audit(
        int(window["start_ts"]),
        int(window["end_ts"]),
        signal_ids,
        delivered_signal_ids,
        archive_dates=selected_archive_dates,
    )
    cases_inventory, cases_summary = stream_cases(
        int(window["start_ts"]),
        int(window["end_ts"]),
        signal_ids,
        delivered_signal_ids,
        archive_dates=selected_archive_dates,
    )
    followup_inventory, followup_summary = stream_case_followups(
        int(window["start_ts"]),
        int(window["end_ts"]),
        delivered_signal_ids,
        archive_dates=selected_archive_dates,
    )

    _ = delivery_inventory, cases_inventory, followup_inventory

    if args.date:
        cli_market_context = {
            "available": False,
            "reason": "skipped_for_date_scoped_report",
            "detail": "window market_context metrics are computed from date-scoped signal rows",
        }
        cli_major = {
            "available": False,
            "reason": "skipped_for_date_scoped_report",
            "expected_major_pairs": [
                f"{asset}/{quote}"
                for asset in ("ETH", "BTC", "SOL")
                for quote in ("USDT", "USDC")
            ],
            "covered_major_pairs": [],
            "missing_expected_pairs": [],
        }
        cli_summary = {
            "available": False,
            "reason": "skipped_for_date_scoped_report",
            "overall": {},
        }
        cli_summary_csv = ""
    else:
        cli_market_context = run_cli(["--market-context-health"], expect_json=True)
        cli_major = run_cli(["--major-pool-coverage"], expect_json=True)
        cli_summary = run_cli(["--summary"], expect_json=True)
        cli_summary_csv = run_cli(["--summary", "--format", "csv"], expect_json=False)

    previous_report = baseline_previous_report()
    if args.date:
        data_sources, missing_sources = build_date_scoped_data_source_inventory(
            window,
            signal_inventory,
            delivery_inventory,
            cases_inventory,
            followup_inventory,
            quality_inventory,
            asset_case_inventory,
            asset_state_inventory,
            trade_opportunity_inventory,
        )
    else:
        data_sources, missing_sources = build_data_source_inventory(latest_date, window)
    data_sources.append(
        {
            "path": "data/chain_monitor.sqlite",
            "exists": bool(sqlite_source.get("sqlite_rows_by_table")),
            "record_count": sum(int(v) for v in (sqlite_source.get("sqlite_rows_by_table") or {}).values() if isinstance(v, int) and v > 0),
            "start_ts": None,
            "end_ts": None,
            "start_utc": "",
            "end_utc": "",
            "start_beijing": "",
            "end_beijing": "",
            "window_record_count": "",
            "after_cutoff_count": "",
            "notes": (
                f"sqlite mirror/query layer report_data_source={sqlite_source.get('report_data_source') or sqlite_source.get('data_source')} "
                f"sqlite_rows_by_table={sqlite_source.get('sqlite_rows_by_table', {})} "
                f"archive_rows_by_category={sqlite_source.get('archive_rows_by_category', {})} "
                f"db_archive_mirror_match_rate={sqlite_source.get('db_archive_mirror_match_rate')} "
                f"archive_fallback_used={bool(sqlite_source.get('archive_fallback_used'))} "
                f"mismatch_warnings={sqlite_source.get('mismatch_warnings', [])}"
            ),
        }
    )

    asset_case_ids = {
        str(row.get("asset_case_id") or "")
        for row in lp_rows
        if str(row.get("asset_case_id") or "").strip()
    }
    asset_case_ids.update(set(cases_summary.get("case_ids") or set()))

    run_overview = compute_run_overview(
        window,
        latest_date,
        window_signal_rows,
        lp_rows,
        len(asset_case_ids),
        int(followup_summary.get("followup_rows") or 0),
        prefer_sqlite=bool(args.date),
    )
    stage_summary = compute_stage_summary(lp_rows)
    asset_market_states = compute_asset_market_state_detail(lp_rows, int(window["end_ts"]), asset_state_records)
    no_trade_lock = compute_no_trade_lock_detail(lp_rows, runtime_config)
    telegram = compute_telegram_detail(lp_rows, previous_report)
    prealerts = compute_prealert_lifecycle_detail(lp_rows, asset_case_payload)
    candidate_tradeable = compute_candidate_tradeable_detail(lp_rows, runtime_config)
    opportunities = normalize_opportunity_summary(compute_trade_opportunities(trade_opportunity_cache, lp_rows))
    final_outputs = compute_final_trading_output_summary(lp_rows)
    outcome_detail = compute_outcome_detail(lp_rows, previous_report)
    market_context = compute_market_context(lp_rows)
    trade_actions = compute_trade_action_detail(lp_rows, candidate_tradeable)
    adverse_direction = compute_directional_adverse_summary(lp_rows, runtime_config)
    archive_integrity = compute_archive_detail(
        lp_rows,
        delivery_summary,
        cases_summary,
        followup_summary,
        latest_date,
        window,
        prefer_sqlite=bool(args.date),
    )
    majors = compute_majors(lp_rows, cli_major, runtime_config)
    noise = compute_noise_assessment(lp_rows, telegram, trade_actions, asset_market_states)
    scorecard = compute_scorecard(
        run_overview,
        telegram,
        asset_market_states,
        no_trade_lock,
        prealerts,
        candidate_tradeable,
        outcome_detail,
        market_context,
        trade_actions,
        majors,
        noise,
    )

    top_findings = build_top_findings(
        run_overview,
        telegram,
        asset_market_states,
        no_trade_lock,
        prealerts,
        candidate_tradeable,
        outcome_detail,
        market_context,
        trade_actions,
        majors,
        archive_integrity,
    )
    top_findings.extend(
        [
            (
                f"统一出口审计：final_output_distribution={final_outputs['final_trading_output_distribution']} "
                f"verified={final_outputs['delivered_verified_count']} "
                f"candidate={final_outputs['delivered_candidate_count']} "
                f"blocked={final_outputs['delivered_blocked_count']}."
            ),
            (
                f"legacy chase 审计：downgraded={final_outputs['legacy_chase_downgraded_count']} "
                f"leaked={final_outputs['legacy_chase_leaked_count']} "
                f"gate_failures={final_outputs['opportunity_gate_failures']} "
                f"blocked_by_gate={final_outputs['messages_blocked_by_opportunity_gate']}."
            ),
        ]
    )
    top_recommendations = build_recommendations(
        telegram,
        candidate_tradeable,
        outcome_detail,
        majors,
        prealerts,
    )
    limitations = build_limitations(
        missing_sources,
        prealerts,
        candidate_tradeable,
        outcome_detail,
        majors,
    )

    summary_payload = {
        "analysis_window": window,
        "data_sources": data_sources,
        "report_data_source": sqlite_source.get("report_data_source", sqlite_source.get("data_source", "archive")),
        "data_source": sqlite_source.get("data_source", "archive"),
        "data_source_summary": sqlite_source.get("data_source_summary", sqlite_source),
        "sqlite_health": sqlite_source.get("sqlite_health", {}),
        "sqlite_rows_by_table": sqlite_source.get("sqlite_rows_by_table", {}),
        "archive_rows_by_category": sqlite_source.get("archive_rows_by_category", {}),
        "compressed_archive_rows": sqlite_source.get("compressed_archive_rows", {}),
        "archive_fallback_used": bool(sqlite_source.get("archive_fallback_used")),
        "db_archive_mirror_match_rate": sqlite_source.get("db_archive_mirror_match_rate"),
        "db_archive_mirror_detail": sqlite_source.get("db_archive_mirror_detail", {}),
        "db_archive_mismatch_warnings": [
            category
            for category, item in (sqlite_source.get("db_archive_mirror_detail") or {}).items()
            if item.get("mismatch")
        ] + list(sqlite_source.get("mismatch_warnings") or []),
        "mismatch_warnings": sqlite_source.get("mismatch_warnings", []),
        "runtime_config_summary": runtime_config,
        "run_overview": run_overview,
        "lp_stage_summary": stage_summary,
        "asset_market_state_summary": asset_market_states,
        "no_trade_lock_summary": no_trade_lock,
        "telegram_suppression_summary": telegram,
        "prealert_lifecycle_summary": prealerts,
        "candidate_tradeable_summary": candidate_tradeable,
        "final_trading_output_summary": final_outputs,
        "trade_opportunity_summary": opportunities,
        "outcome_source_summary": outcome_detail,
        "market_context_health": {
            "window": market_context,
            "quality_reports_cli": cli_market_context,
        },
        "trade_action_summary": trade_actions,
        "adverse_direction_summary": adverse_direction,
        "archive_integrity_summary": archive_integrity,
        "majors_coverage_summary": majors,
        "noise_assessment": noise,
        "scorecard": scorecard,
        "top_findings": top_findings,
        "top_recommendations": top_recommendations,
        "limitations": limitations,
        "quality_reports_cli_summary": cli_summary,
        "quality_reports_cli_summary_csv": cli_summary_csv,
    }

    csv_rows = build_csv_rows(
        window,
        run_overview,
        stage_summary,
        asset_market_states,
        no_trade_lock,
        telegram,
        prealerts,
        candidate_tradeable,
        opportunities,
        outcome_detail,
        market_context,
        trade_actions,
        adverse_direction,
        archive_integrity,
        majors,
        scorecard,
    )
    window_label = f"{fmt_ts(window['start_ts'])} -> {fmt_ts(window['end_ts'])}"
    for key, value in final_outputs.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        append_metric(csv_rows, "final_trading_output", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in final_outputs.get("final_trading_output_distribution", {}).items():
        append_metric(csv_rows, "final_trading_output", "final_trading_output_distribution", value, asset_market_state=key, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in final_outputs.get("opportunity_gate_failures", {}).items():
        append_metric(csv_rows, "final_trading_output", "opportunity_gate_failures", value, asset_market_state=key, sample_size=run_overview["lp_signal_rows"], window=window_label)
    markdown = build_markdown(
        data_sources,
        window,
        runtime_config,
        run_overview,
        stage_summary,
        asset_market_states,
        telegram,
        no_trade_lock,
        prealerts,
        candidate_tradeable,
        final_outputs,
        opportunities,
        outcome_detail,
        market_context,
        trade_actions,
        adverse_direction,
        archive_integrity,
        majors,
        noise,
        scorecard,
        cli_market_context,
        cli_major,
        cli_summary,
        top_findings,
        top_recommendations,
        limitations,
    )

    MARKDOWN_PATH.write_text(markdown, encoding="utf-8")
    write_csv(CSV_PATH, csv_rows)
    JSON_PATH.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_dated_report_copies(
        {
            "markdown": MARKDOWN_PATH,
            "csv": CSV_PATH,
            "json": JSON_PATH,
        },
        tz=BJ_TZ,
        report_date=args.date or selected_logical_date,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
