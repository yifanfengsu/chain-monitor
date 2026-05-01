#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
REPORTS_DIR = ROOT / "reports"
DAILY_DIR = REPORTS_DIR / "daily"
BJ_TZ = timezone(timedelta(hours=8))

for import_path in (ROOT, APP_DIR):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

import config as app_config  # noqa: E402
from app import report_data_loader  # noqa: E402


LP_STAGES = {"prealert", "confirm", "climax", "exhaustion_risk"}
EXPECTED_MAJOR_PAIRS = (
    "ETH/USDT",
    "ETH/USDC",
    "BTC/USDT",
    "BTC/USDC",
    "SOL/USDT",
    "SOL/USDC",
)
GAP_THRESHOLD_SEC = 3600
ARCHIVE_TS_RE = re.compile(r'"archive_ts"\s*:\s*(\d+)')
TRADE_ACTION_FIELDS = (
    "trade_action",
    "trade_action_key",
    "final_trading_output_label",
    "action_label",
    "intent_type",
)
PREALERT_COUNT_FIELDS = (
    "prealert_candidate",
    "lp_prealert_candidate",
    "prealert_gate_passed",
    "lp_prealert_gate_passed",
    "prealert_active",
    "prealert_delivered",
    "prealert_upgraded_to_confirm",
    "prealert_expired",
    "prealert_lifecycle_state",
    "prealert_to_confirm_sec",
    "asset_case_prealert_to_confirm_sec",
)


def _to_int(value: Any) -> int | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _to_float(value: Any) -> float | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "sent", "completed", "success"}


def _from_json(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 4)


def _percentile(values: list[float], ratio: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int((len(ordered) - 1) * ratio)
    return round(float(ordered[index]), 4)


def _fmt_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_bj(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), BJ_TZ).strftime("%Y-%m-%d %H:%M:%S UTC+8")


def _container_values(row: dict[str, Any]) -> list[dict[str, Any]]:
    containers = [row]
    for key in (
        "event",
        "metadata",
        "data",
        "signal",
        "opportunity",
        "prealert_diagnostics",
        "quality",
        "quality_snapshot",
        "history_snapshot",
        "trade_opportunity_history_snapshot",
        "opportunity_json",
        "state_json",
        "audit_json",
        "message_json",
        "stats_json",
        "lifecycle_json",
    ):
        value = row.get(key)
        if isinstance(value, str) and value[:1] in {"{", "["}:
            value = _from_json(value, {})
        if isinstance(value, dict):
            containers.append(value)
            nested = value.get("metadata")
            if isinstance(nested, dict):
                containers.append(nested)
    return containers


def _first(row: dict[str, Any], *keys: str) -> Any:
    for container in _container_values(row):
        for key in keys:
            value = container.get(key)
            if value not in (None, "", [], {}, ()):
                return value
    return None


def _field_present(row: dict[str, Any], *keys: str) -> bool:
    for container in _container_values(row):
        if any(key in container for key in keys):
            return True
    return False


def _first_with_key(row: dict[str, Any], keys: tuple[str, ...]) -> tuple[str | None, Any]:
    for container in _container_values(row):
        for key in keys:
            if key in container:
                value = container.get(key)
                if value not in (None, "", [], {}, ()):
                    return key, value
    return None, None


def _row_ts(row: dict[str, Any]) -> int | None:
    return _to_int(
        _first(
            row,
            "archive_ts",
            "archive_written_at",
            "created_at",
            "updated_at",
            "timestamp",
            "parsed_at",
            "captured_at",
        )
    )


def _canonical_asset(value: Any) -> str:
    raw = str(value or "").strip().upper().replace(".E", "")
    return {"WETH": "ETH", "WBTC": "BTC", "CBBTC": "BTC", "WSOL": "SOL"}.get(raw, raw)


def _canonical_pair(value: Any, asset: Any = None) -> str:
    raw = str(value or "").strip().upper().replace(" ", "")
    if "/" in raw:
        base, quote = raw.split("/", 1)
        return f"{_canonical_asset(base)}/{quote.replace('.E', '')}"
    asset_value = _canonical_asset(asset)
    return asset_value if asset_value else raw


def _logical_window(logical_date: str) -> dict[str, Any]:
    parsed = date.fromisoformat(logical_date)
    start_bj = datetime(parsed.year, parsed.month, parsed.day, tzinfo=BJ_TZ)
    start_ts = int(start_bj.timestamp())
    end_ts = start_ts + 24 * 3600 - 1
    return {
        "logical_date": logical_date,
        "timezone": "Asia/Shanghai",
        "start_ts": start_ts,
        "end_ts": end_ts,
        "logical_window_start_utc": _fmt_utc(start_ts),
        "logical_window_end_utc": _fmt_utc(end_ts),
        "logical_window_start_beijing": _fmt_bj(start_ts),
        "logical_window_end_beijing": _fmt_bj(end_ts),
        "wall_clock_duration_hours": 24.0,
        "selection_reason": "canonical Beijing natural day; no cross-gap longest-window selection",
    }


def _db_path() -> Path:
    raw = Path(str(getattr(app_config, "SQLITE_DB_PATH", "data/chain_monitor.sqlite")))
    if raw.is_absolute():
        return raw
    return ROOT / raw


def _sqlite_latest_timestamp() -> int | None:
    path = _db_path()
    if not path.exists():
        return None
    max_ts: int | None = None
    try:
        conn = sqlite3.connect(str(path))
    except sqlite3.Error:
        return None
    try:
        for cfg in report_data_loader.DB_TABLES.values():
            table = str(cfg.get("table") or "")
            column = str(cfg.get("time_column") or "")
            if not table or not column:
                continue
            try:
                exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not exists:
                    continue
                row = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()
            except sqlite3.Error:
                continue
            value = _to_int(row[0] if row else None)
            if value is not None and (max_ts is None or value > max_ts):
                max_ts = value
    finally:
        conn.close()
    return max_ts


def _archive_file_date(path: Path) -> str | None:
    name = path.name
    if name.endswith(".ndjson.gz"):
        name = name[: -len(".ndjson.gz")]
    elif name.endswith(".ndjson"):
        name = name[: -len(".ndjson")]
    else:
        name = path.stem
    try:
        return date.fromisoformat(name).isoformat()
    except ValueError:
        return None


def _archive_latest_logical_date() -> str | None:
    dates: set[str] = set()
    for category in ("signals", "parsed_events", "raw_events", "delivery_audit", "case_followups"):
        for path in report_data_loader.archive_paths(category):
            file_date = _archive_file_date(path)
            if file_date:
                dates.add(file_date)
    return sorted(dates)[-1] if dates else None


def _existing_daily_latest_logical_date() -> str | None:
    dates: set[str] = set()
    for path in DAILY_DIR.glob("daily_report_*.json"):
        if path.name == "daily_report_latest.json":
            continue
        value = path.stem.removeprefix("daily_report_")
        try:
            dates.add(date.fromisoformat(value).isoformat())
        except ValueError:
            continue
    return sorted(dates)[-1] if dates else None


def latest_available_logical_date() -> str:
    archive_date = _archive_latest_logical_date()
    if archive_date:
        return archive_date
    existing_daily_date = _existing_daily_latest_logical_date()
    if existing_daily_date:
        return existing_daily_date
    latest_ts = _sqlite_latest_timestamp()
    if latest_ts is None:
        raise RuntimeError("No SQLite or archive timestamps available for automatic logical date selection")
    return datetime.fromtimestamp(latest_ts, BJ_TZ).date().isoformat()


def _load_result_dict(result: report_data_loader.LoadResult) -> dict[str, Any]:
    return {
        "source": result.source,
        "row_count": result.row_count,
        "warnings": list(result.warnings),
        "fallback_used": result.fallback_used,
        "compressed_archive_rows": result.compressed_archive_rows,
        "mismatch_info": result.mismatch_info,
    }


def _archive_base_dir() -> Path:
    raw = Path(str(getattr(app_config, "ARCHIVE_BASE_DIR", APP_DIR / "data" / "archive")))
    if raw.is_absolute():
        return raw
    return ROOT / raw


def _window_archive_dates(window: dict[str, Any]) -> list[str]:
    start_day = datetime.fromtimestamp(int(window["start_ts"]), UTC).date()
    end_day = datetime.fromtimestamp(int(window["end_ts"]), UTC).date()
    values: list[str] = []
    current = start_day
    while current <= end_day:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _line_archive_ts(line: str) -> int | None:
    match = ARCHIVE_TS_RE.search(line[:80])
    return _to_int(match.group(1)) if match else None


def _read_archive_category(
    category: str,
    window: dict[str, Any],
    *,
    parse_payload: bool,
) -> report_data_loader.LoadResult:
    root = _archive_base_dir() / category
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    paths: list[Path] = []
    for date_key in _window_archive_dates(window):
        plain = root / f"{date_key}.ndjson"
        gz = root / f"{date_key}.ndjson.gz"
        if plain.exists():
            paths.append(plain)
        elif gz.exists():
            paths.append(gz)
    if not paths:
        return report_data_loader.LoadResult([], "unavailable", 0, [f"archive_missing:{category}"], False, {}, 0)
    for path in paths:
        try:
            with report_data_loader.open_archive_text(path) as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    if not parse_payload:
                        ts = _line_archive_ts(line)
                        if ts is not None and int(window["start_ts"]) <= ts <= int(window["end_ts"]):
                            rows.append({"archive_ts": ts})
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        warnings.append(f"invalid_json:{path.name}")
                        continue
                    if not isinstance(payload, dict):
                        continue
                    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                    if not isinstance(data, dict):
                        continue
                    row = dict(data)
                    ts = _to_int(payload.get("archive_ts")) or _row_ts(row)
                    if ts is None:
                        continue
                    if int(window["start_ts"]) <= ts <= int(window["end_ts"]):
                        row.setdefault("archive_ts", ts)
                        row.setdefault("archive_written_at", ts)
                        rows.append(row)
        except OSError as exc:
            warnings.append(f"archive_read_failed:{path.name}:{exc}")
    compressed_rows = sum(1 for path in paths if path.name.endswith(".gz"))
    return report_data_loader.LoadResult(rows, "archive", len(rows), warnings, False, {}, compressed_rows)


def _empty_result(source: str = "unavailable", warning: str | None = None) -> report_data_loader.LoadResult:
    return report_data_loader.LoadResult([], source, 0, [warning] if warning else [], False, {}, 0)


def _sqlite_count_only(loader_key: str, window: dict[str, Any]) -> report_data_loader.LoadResult | None:
    cfg = report_data_loader.DB_TABLES[loader_key]
    table = str(cfg.get("table") or "")
    column = str(cfg.get("time_column") or "")
    if not table or not column:
        return None
    path = _db_path()
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(str(path))
    except sqlite3.Error:
        return None
    try:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if not exists:
            return None
        row = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} >= ? AND {column} <= ?",
            (window["start_ts"], window["end_ts"]),
        ).fetchone()
        count = int(row[0] if row else 0)
        return report_data_loader.LoadResult(
            rows=[],
            source="sqlite",
            row_count=count,
            warnings=[],
            fallback_used=False,
            mismatch_info={"loader": loader_key, "sqlite_rows": count, "match_rate": None, "mismatch": None},
        )
    except sqlite3.Error as exc:
        return report_data_loader.LoadResult(
            rows=[],
            source="unavailable",
            row_count=0,
            warnings=[f"db_count_failed:{loader_key}:{exc}"],
            fallback_used=False,
            mismatch_info={"loader": loader_key, "sqlite_rows": 0, "match_rate": None, "mismatch": None},
        )
    finally:
        conn.close()


def _load_window(window: dict[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, report_data_loader.LoadResult]]:
    bounds = {"start_ts": window["start_ts"], "end_ts": window["end_ts"]}
    loaders: dict[str, Callable[..., report_data_loader.LoadResult]] = {
        "raw_events": report_data_loader.load_raw_events,
        "parsed_events": report_data_loader.load_parsed_events,
        "signals": report_data_loader.load_signals,
        "delivery_audit": report_data_loader.load_delivery_audit,
        "case_followups": report_data_loader.load_case_followups,
        "asset_cases": report_data_loader.load_asset_cases,
        "asset_market_states": report_data_loader.load_asset_market_states,
        "trade_opportunities": report_data_loader.load_trade_opportunities,
        "outcomes": report_data_loader.load_outcomes,
        "quality_stats": report_data_loader.load_quality_stats,
        "telegram_deliveries": report_data_loader.load_telegram_deliveries,
        "market_context_attempts": report_data_loader.load_market_context_attempts,
        "prealert_lifecycle": report_data_loader.load_prealert_lifecycle,
    }
    count_only = {"raw_events", "parsed_events", "case_followups", "asset_cases", "telegram_deliveries"}
    results: dict[str, report_data_loader.LoadResult] = {}
    for key, load in loaders.items():
        try:
            result = _sqlite_count_only(key, bounds) if key in count_only else None
            if result is None:
                result = load(window=bounds, compare_archive=key in {"signals", "delivery_audit", "case_followups"})
        except Exception as exc:  # pragma: no cover - defensive report degradation
            result = _empty_result(warning=f"load_failed:{key}:{exc}")
        results[key] = result
    rows = {key: result.rows for key, result in results.items()}
    return rows, results


def _data_source_summary(results: dict[str, report_data_loader.LoadResult]) -> dict[str, Any]:
    source_components = {key: result.source for key, result in results.items()}
    row_counts = {key: result.row_count for key, result in results.items()}
    warnings = sorted({warning for result in results.values() for warning in result.warnings})
    fallback_used = any(result.fallback_used for result in results.values())
    active_sources = {source for source in source_components.values() if source and source != "unavailable"}
    source = "mixed" if len(active_sources) > 1 else next(iter(active_sources), "unavailable")
    mirror_detail = {
        key: dict(result.mismatch_info or {})
        for key, result in results.items()
        if result.mismatch_info
    }
    mismatch_items = {
        key: item
        for key, item in mirror_detail.items()
        if item.get("mismatch")
    }
    match_rates = [
        float(item.get("match_rate"))
        for item in mirror_detail.values()
        if item.get("match_rate") is not None
    ]
    return {
        "report_data_source": source,
        "data_source": source,
        "source_components": source_components,
        "row_counts": row_counts,
        "loader_results": {key: _load_result_dict(result) for key, result in results.items()},
        "archive_fallback_used": fallback_used,
        "db_archive_mirror_match_rate": round(sum(match_rates) / len(match_rates), 4) if match_rates else None,
        "db_archive_mirror_detail": mirror_detail,
        "db_archive_mismatch_categories": sorted(mismatch_items),
        "db_archive_mismatch_rows": sum(
            abs(int(item.get("sqlite_rows") or 0) - int(item.get("archive_rows") or item.get("cache_rows") or 0))
            for item in mismatch_items.values()
        ),
        "mismatch_warnings": warnings,
        "source_warnings": warnings,
        "sqlite_health": report_data_loader.sqlite_health(fast=True),
    }


def _is_lp_row(row: dict[str, Any]) -> bool:
    stage = str(_first(row, "lp_alert_stage", "lp_stage", "stage") or "").strip().lower()
    if stage in LP_STAGES:
        return True
    return bool(
        _first(row, "asset_market_state_key", "trade_opportunity_status", "trade_opportunity_id")
    )


def _build_segments(signal_rows: list[dict[str, Any]], lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = sorted(ts for ts in (_row_ts(row) for row in signal_rows) if ts is not None)
    if not timestamps:
        return {
            "segment_count": 0,
            "gap_threshold_sec": GAP_THRESHOLD_SEC,
            "largest_gap_sec": None,
            "active_duration_hours": 0.0,
            "wall_clock_duration_hours": 24.0,
            "segments": [],
            "gap_warnings": ["no_signal_rows_in_logical_day"],
        }
    segments: list[dict[str, Any]] = []
    start = prev = timestamps[0]
    largest_gap = 0
    for ts in timestamps[1:]:
        gap = ts - prev
        if gap > largest_gap:
            largest_gap = gap
        if gap > GAP_THRESHOLD_SEC:
            segments.append({"start_ts": start, "end_ts": prev})
            start = ts
        prev = ts
    segments.append({"start_ts": start, "end_ts": prev})
    lp_ts = [_row_ts(row) for row in lp_rows]
    formatted = []
    active_sec = 0
    for item in segments:
        seg_start = int(item["start_ts"])
        seg_end = int(item["end_ts"])
        duration = max(seg_end - seg_start, 0)
        active_sec += duration
        formatted.append(
            {
                "start_ts": seg_start,
                "end_ts": seg_end,
                "start_utc": _fmt_utc(seg_start),
                "end_utc": _fmt_utc(seg_end),
                "start_beijing": _fmt_bj(seg_start),
                "end_beijing": _fmt_bj(seg_end),
                "duration_hours": round(duration / 3600.0, 2),
                "signal_rows": sum(1 for ts in timestamps if seg_start <= ts <= seg_end),
                "lp_signal_rows": sum(1 for ts in lp_ts if ts is not None and seg_start <= ts <= seg_end),
            }
        )
    gap_warnings = []
    if largest_gap > GAP_THRESHOLD_SEC:
        gap_warnings.append(f"large_gap_detected:{largest_gap}s; active_duration_excludes_gap")
    return {
        "segment_count": len(formatted),
        "gap_threshold_sec": GAP_THRESHOLD_SEC,
        "largest_gap_sec": largest_gap,
        "active_duration_hours": round(active_sec / 3600.0, 2),
        "wall_clock_duration_hours": 24.0,
        "segments": formatted,
        "gap_warnings": gap_warnings,
    }


def _telegram_summary(lp_rows: list[dict[str, Any]], delivery_rows: list[dict[str, Any]]) -> dict[str, Any]:
    sent_lp = [
        row
        for row in lp_rows
        if _is_true(_first(row, "sent_to_telegram", "telegram_sent", "notifier_sent_at", "delivered_notification"))
    ]
    should_send = [row for row in lp_rows if _is_true(_first(row, "telegram_should_send"))]
    reason_counter = Counter(
        str(_first(row, "telegram_suppression_reason", "suppression_reason") or "")
        for row in lp_rows
        if str(_first(row, "telegram_suppression_reason", "suppression_reason") or "").strip()
    )
    delivery_sent = sum(1 for row in delivery_rows if _is_true(_first(row, "sent")))
    delivery_suppressed = sum(
        1
        for row in delivery_rows
        if _is_true(_first(row, "suppressed")) or str(_first(row, "suppression_reason") or "").strip()
    )
    sent_count = len(sent_lp) if lp_rows else delivery_sent
    suppressed = max(len(lp_rows) - len(sent_lp), 0) if lp_rows else delivery_suppressed
    total = len(lp_rows) if lp_rows else sent_count + suppressed
    return {
        "telegram_should_send_count": len(should_send),
        "telegram_suppressed_count": suppressed,
        "telegram_suppression_ratio": _rate(suppressed, total),
        "telegram_suppression_reasons": dict(sorted(reason_counter.items())),
        "messages_before_suppression_estimate": total,
        "messages_after_suppression_actual": sent_count,
        "high_value_suppressed_count": 0,
        "delivered_lp_signals": sent_count,
        "suppressed_lp_signals": suppressed,
    }


def _status(row: dict[str, Any]) -> str:
    return str(
        _first(
            row,
            "trade_opportunity_status_at_creation",
            "status_at_creation",
            "trade_opportunity_status",
            "status",
        )
        or "NONE"
    ).upper()


def _outcome_for_rows(rows: list[dict[str, Any]], prefix: str = "opportunity") -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for window in ("30s", "60s", "300s"):
        status_key = f"{prefix}_outcome_{window}"
        follow_key = f"{prefix}_followthrough_{window}"
        adverse_key = f"{prefix}_adverse_{window}"
        completed = [row for row in rows if str(_first(row, status_key) or "").lower() == "completed"]
        followthrough = sum(1 for row in completed if _is_true(_first(row, follow_key)))
        adverse = sum(1 for row in completed if _is_true(_first(row, adverse_key)))
        payload[window] = {
            "count": len(rows),
            "resolved_count": len(completed),
            "followthrough_count": followthrough,
            "followthrough_rate": _rate(followthrough, len(completed)),
            "adverse_count": adverse,
            "adverse_rate": _rate(adverse, len(completed)),
            "expired_count": sum(1 for row in rows if str(_first(row, status_key) or "").lower() == "expired"),
            "unavailable_count": sum(1 for row in rows if str(_first(row, status_key) or "").lower() in {"", "pending", "missing"}),
        }
    return payload


def _string_list(value: Any) -> list[str]:
    if value in (None, "", [], {}, ()):
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    decoded = _from_json(value)
    if isinstance(decoded, list):
        return [str(item).strip() for item in decoded if str(item).strip()]
    raw = str(value).strip()
    if not raw:
        return []
    separator = ";" if ";" in raw else ","
    return [item.strip() for item in raw.split(separator) if item.strip()]


def _maturity_payload(maturity: Any, reasons: list[str], reason: Any, source: str) -> dict[str, Any]:
    maturity_text = str(maturity or "unknown").strip().lower() or "unknown"
    clean_reasons = sorted({str(item).strip() for item in reasons if str(item).strip()})
    reason_text = str(reason or "").strip()
    if maturity_text == "mature":
        reason_text = reason_text if reason_text not in {"n/a", "none"} else ""
    elif not clean_reasons:
        clean_reasons = ["insufficient_data"] if maturity_text == "unknown" else [f"verified_maturity={maturity_text}"]
    if maturity_text != "mature" and not reason_text:
        reason_text = ";".join(clean_reasons) if clean_reasons else "insufficient_data"
    return {
        "verified_maturity": maturity_text,
        "maturity_reasons": clean_reasons,
        "verified_should_not_be_traded_reason": reason_text,
        "verified_maturity_source": source,
    }


def _unknown_maturity(reason: str, source: str) -> dict[str, Any]:
    return _maturity_payload("unknown", [reason], reason, source)


def _maturity_from_mapping(mapping: dict[str, Any], source: str) -> dict[str, Any] | None:
    maturity = _first(mapping, "verified_maturity", "maturity")
    if maturity in (None, "", [], {}, ()):
        return None
    reasons = _string_list(_first(mapping, "maturity_reasons", "verified_maturity_reasons"))
    reason = _first(
        mapping,
        "verified_should_not_be_traded_reason",
        "should_not_be_traded_reason",
        "not_tradeable_reason",
    )
    return _maturity_payload(maturity, reasons, reason, source)


def _maturity_from_rows(rows: list[dict[str, Any]], source: str) -> dict[str, Any] | None:
    for row in rows:
        payload = _maturity_from_mapping(row, source)
        if payload:
            return payload
    return None


def _derive_verified_maturity(
    *,
    verified_count: int,
    outcome_completion_rate: float | None,
    max_profile_sample_count: int | None,
    source: str,
) -> dict[str, Any]:
    if verified_count <= 0:
        return _unknown_maturity("no_verified_opportunity", source)
    if outcome_completion_rate is None and max_profile_sample_count is None:
        return _unknown_maturity("insufficient_data", source)
    if outcome_completion_rate is None:
        return _unknown_maturity("insufficient_data", source)
    if max_profile_sample_count is None:
        return _unknown_maturity("insufficient_verified_samples", source)

    reasons: list[str] = []
    min_completion = float(getattr(app_config, "OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE", 0.70))
    min_samples = int(getattr(app_config, "OPPORTUNITY_MIN_HISTORY_SAMPLES", 20))
    if float(outcome_completion_rate) < min_completion:
        reasons.append(f"outcome_completion_rate_below_{min_completion:.2f}")
    if int(max_profile_sample_count) < min_samples:
        reasons.append(f"profile_sample_count_below_{min_samples}")
    if reasons:
        reasons.append("immature_verified_warning")
        return _maturity_payload("immature", reasons, ";".join(reasons), source)
    return _maturity_payload("mature", [], "", source)


def _read_opportunity_db_summary() -> dict[str, Any]:
    path = _db_path()
    if not path.exists():
        return {"available": False, "reason": "sqlite_db_missing"}
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        return {"available": False, "reason": f"sqlite_open_failed:{exc}"}
    try:
        table_names = {
            str(row["name"])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "trade_opportunities" not in table_names:
            return {"available": False, "reason": "trade_opportunities_table_missing"}
        status_counts = {
            str(row["status"] or "NONE"): int(row["count"])
            for row in conn.execute(
                "SELECT status, COUNT(*) AS count FROM trade_opportunities GROUP BY status"
            ).fetchall()
        }
        total_outcomes: int | None = None
        completed_outcomes: int | None = None
        if "opportunity_outcomes" in table_names:
            total_outcomes = int(conn.execute("SELECT COUNT(*) FROM opportunity_outcomes").fetchone()[0])
            completed_outcomes = int(
                conn.execute("SELECT COUNT(*) FROM opportunity_outcomes WHERE status='completed'").fetchone()[0]
            )
        max_profile_sample_count: int | None = None
        if "quality_stats" in table_names:
            profile_rows = conn.execute(
                "SELECT sample_count, stats_json FROM quality_stats WHERE scope_type='opportunity_profile' AND stage='all'"
            ).fetchall()
            samples: list[int] = []
            for row in profile_rows:
                value = _to_int(row["sample_count"])
                stats = _from_json(row["stats_json"], {})
                if value is None and isinstance(stats, dict):
                    value = _to_int(stats.get("sample_count"))
                if value is not None:
                    samples.append(value)
            max_profile_sample_count = max(samples) if samples else None
        outcome_completion_rate = (
            round(float(completed_outcomes) / float(total_outcomes), 4)
            if total_outcomes
            else None
        )
        maturity = _derive_verified_maturity(
            verified_count=int(status_counts.get("VERIFIED", 0)),
            outcome_completion_rate=outcome_completion_rate,
            max_profile_sample_count=max_profile_sample_count,
            source="opportunity_db_summary",
        )
        return {
            "available": True,
            "status_counts": status_counts,
            "verified_count": int(status_counts.get("VERIFIED", 0)),
            "candidate_count": int(status_counts.get("CANDIDATE", 0)),
            "outcome_completion_rate": outcome_completion_rate,
            "max_profile_sample_count": max_profile_sample_count,
            **maturity,
        }
    except sqlite3.Error as exc:
        return {"available": False, "reason": f"opportunity_db_summary_failed:{exc}"}
    finally:
        conn.close()


def _maturity_from_quality_rows(
    quality_rows: list[dict[str, Any]],
    *,
    verified_count: int,
    verified_outcome_completion_rate: float | None,
    verified_outcome_data_available: bool,
) -> dict[str, Any] | None:
    explicit = _maturity_from_rows(quality_rows, "quality_stats")
    if explicit:
        return explicit
    profile_rows = [
        row
        for row in quality_rows
        if str(_first(row, "scope_type") or "").strip() == "opportunity_profile"
        and str(_first(row, "stage") or "all").strip() == "all"
    ]
    if not profile_rows:
        return None
    samples = [_to_int(_first(row, "sample_count")) for row in profile_rows]
    completion_values = [
        _to_float(_first(row, "completion_60s_rate", "outcome_completion_rate"))
        for row in profile_rows
    ]
    max_profile_sample_count = max((value for value in samples if value is not None), default=None)
    completion_rate = max((value for value in completion_values if value is not None), default=None)
    if completion_rate is None and verified_outcome_data_available:
        completion_rate = verified_outcome_completion_rate
    return _derive_verified_maturity(
        verified_count=verified_count,
        outcome_completion_rate=completion_rate,
        max_profile_sample_count=max_profile_sample_count,
        source="quality_stats",
    )


def _resolve_verified_maturity(
    rows: list[dict[str, Any]],
    quality_rows: list[dict[str, Any]],
    db_summary: dict[str, Any],
    summary: dict[str, Any],
    *,
    verified_outcome_data_available: bool,
) -> dict[str, Any]:
    explicit = _maturity_from_rows(rows, "trade_opportunity_rows")
    if explicit:
        return explicit
    verified_count = int(summary.get("opportunity_verified_count") or 0)
    if not rows:
        return _unknown_maturity("insufficient_data", "missing")
    if verified_count <= 0:
        return _unknown_maturity("no_verified_opportunity", "trade_summary")
    db_maturity = _maturity_from_mapping(db_summary, "opportunity_db_summary") if db_summary.get("available") else None
    if db_maturity:
        return db_maturity
    quality_maturity = _maturity_from_quality_rows(
        quality_rows,
        verified_count=verified_count,
        verified_outcome_completion_rate=summary.get("verified_outcome_completion_rate"),
        verified_outcome_data_available=verified_outcome_data_available,
    )
    if quality_maturity:
        return quality_maturity
    return _unknown_maturity("insufficient_data", "trade_summary")


def _trade_opportunity_summary(
    rows: list[dict[str, Any]],
    *,
    quality_rows: list[dict[str, Any]] | None = None,
    opportunity_db_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    by_status = Counter(_status(row) for row in rows)
    candidate_rows = [row for row in rows if _status(row) == "CANDIDATE"]
    verified_rows = [row for row in rows if _status(row) == "VERIFIED"]
    blocked_rows = [row for row in rows if _status(row) == "BLOCKED"]
    scores = [
        value
        for value in (_to_float(_first(row, "trade_opportunity_score", "score", "calibrated_score")) for row in rows)
        if value is not None
    ]
    hard_blockers = Counter(
        str(_first(row, "primary_hard_blocker", "trade_opportunity_primary_blocker", "primary_blocker") or "")
        for row in blocked_rows
        if str(_first(row, "primary_hard_blocker", "trade_opportunity_primary_blocker", "primary_blocker") or "").strip()
    )
    verification_blockers = Counter(
        str(_first(row, "primary_verification_blocker") or "")
        for row in rows
        if str(_first(row, "primary_verification_blocker") or "").strip()
    )
    candidate_outcomes = _outcome_for_rows(candidate_rows)
    verified_outcomes = _outcome_for_rows(verified_rows)
    blocker_saved_values = [_first(row, "blocker_saved_trade") for row in blocked_rows if _first(row, "blocker_saved_trade") is not None]
    false_block_values = [_first(row, "blocker_false_block_possible") for row in blocked_rows if _first(row, "blocker_false_block_possible") is not None]
    maturity_reasons = []
    if not verified_rows:
        maturity_reasons.append("no_verified_rows")
    if candidate_outcomes["60s"]["resolved_count"] == 0:
        maturity_reasons.append("candidate_outcome_history_incomplete")
    verified_outcome_data_available = any(
        _field_present(
            row,
            "opportunity_outcome_60s",
            "opportunity_followthrough_60s",
            "opportunity_adverse_60s",
        )
        for row in verified_rows
    )
    summary = {
        "window_record_count": len(rows),
        "creation_status_distribution": dict(sorted(by_status.items())),
        "opportunity_none_count": by_status.get("NONE", 0),
        "opportunity_candidate_count": len(candidate_rows),
        "opportunity_verified_count": len(verified_rows),
        "opportunity_blocked_count": len(blocked_rows),
        "opportunity_expired_count": by_status.get("EXPIRED", 0),
        "opportunity_invalidated_count": by_status.get("INVALIDATED", 0),
        "opportunity_candidate_to_verified_rate": _rate(len(verified_rows), len(candidate_rows)),
        "opportunity_score_median": round(float(median(scores)), 4) if scores else None,
        "opportunity_score_p90": _percentile(scores, 0.9),
        "opportunity_hard_blocker_distribution": dict(sorted(hard_blockers.items())),
        "hard_blocker_distribution": dict(sorted(hard_blockers.items())),
        "verification_blocker_distribution": dict(sorted(verification_blockers.items())),
        "candidate_outcome_30s": candidate_outcomes["30s"],
        "candidate_outcome_60s": candidate_outcomes["60s"],
        "candidate_outcome_300s": candidate_outcomes["300s"],
        "verified_outcome_30s": verified_outcomes["30s"],
        "verified_outcome_60s": verified_outcomes["60s"],
        "verified_outcome_300s": verified_outcomes["300s"],
        "opportunity_candidate_followthrough_60s_rate": candidate_outcomes["60s"]["followthrough_rate"],
        "opportunity_candidate_adverse_60s_rate": candidate_outcomes["60s"]["adverse_rate"],
        "opportunity_verified_followthrough_60s_rate": verified_outcomes["60s"]["followthrough_rate"],
        "opportunity_verified_adverse_60s_rate": verified_outcomes["60s"]["adverse_rate"],
        "candidate_outcome_completion_rate": _rate(candidate_outcomes["60s"]["resolved_count"], len(candidate_rows)),
        "verified_outcome_completion_rate": _rate(verified_outcomes["60s"]["resolved_count"], len(verified_rows)),
        "blocker_saved_rate": _rate(sum(1 for value in blocker_saved_values if _is_true(value)), len(blocker_saved_values)),
        "blocker_false_block_rate": _rate(sum(1 for value in false_block_values if _is_true(value)), len(false_block_values)),
        "top_blockers": dict(hard_blockers.most_common(10)),
        "why_no_opportunities": maturity_reasons or [],
    }
    summary.update(
        _resolve_verified_maturity(
            rows,
            quality_rows or [],
            opportunity_db_summary or {"available": False, "reason": "not_loaded"},
            summary,
            verified_outcome_data_available=verified_outcome_data_available,
        )
    )
    return summary


def _asset_market_state_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    state_counter = Counter(
        str(_first(row, "current_state", "asset_market_state_key") or "")
        for row in rows
        if str(_first(row, "current_state", "asset_market_state_key") or "").strip()
    )
    final: dict[str, Any] = {}
    transitions = 0
    for row in sorted(rows, key=lambda item: _row_ts(item) or 0):
        asset = _canonical_asset(_first(row, "asset", "asset_symbol"))
        current = str(_first(row, "current_state", "asset_market_state_key") or "")
        if not asset or not current:
            continue
        if _is_true(_first(row, "state_changed", "asset_market_state_changed")):
            transitions += 1
        final[asset] = {
            "state_key": current,
            "state_label": _first(row, "asset_market_state_label") or current,
            "updated_at": _row_ts(row),
        }
    return {
        "state_distribution": dict(sorted(state_counter.items())),
        "state_transition_count": transitions,
        "state_change_count": transitions,
        "final_state_by_asset": final,
        "current_final_state_per_asset": final,
    }


def _no_trade_lock_summary(asset_state_summary: dict[str, Any]) -> dict[str, Any]:
    states = asset_state_summary.get("state_distribution") or {}
    lock_count = int(states.get("NO_TRADE_LOCK") or 0)
    return {
        "lock_entered_count": lock_count,
        "lock_suppressed_count": lock_count,
        "lock_released_count": 0,
    }


def _outcome_source_summary(outcome_rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_counter = Counter(str(_first(row, "outcome_price_source", "price_source") or "") for row in outcome_rows)
    source_counter.pop("", None)
    failure_counter = Counter(str(_first(row, "failure_reason") or "") for row in outcome_rows)
    failure_counter.pop("", None)
    payload: dict[str, Any] = {
        "outcome_price_source_distribution": dict(sorted(source_counter.items())),
        "outcome_failure_reason_distribution": dict(sorted(failure_counter.items())),
        "catchup_completed_count": sum(1 for row in outcome_rows if _is_true(_first(row, "catchup")) and str(_first(row, "status") or "").lower() == "completed"),
        "catchup_expired_count": sum(1 for row in outcome_rows if _is_true(_first(row, "catchup")) and str(_first(row, "status") or "").lower() == "expired"),
        "scheduler_health_summary": {},
    }
    for seconds in (30, 60, 300):
        rows = [row for row in outcome_rows if _to_int(_first(row, "window_sec")) == seconds]
        completed = sum(1 for row in rows if str(_first(row, "status") or "").lower() == "completed")
        payload[f"outcome_{seconds}s_completed_rate"] = _rate(completed, len(rows))
        payload[f"outcome_{seconds}s_completed_count"] = completed
    return payload


def _market_context_health(attempt_rows: list[dict[str, Any]], lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    venue_attempts = Counter()
    venue_success = Counter()
    failures = Counter()
    resolved = Counter()
    attempt_count = 0
    success_count = 0
    last_success_ts: int | None = None
    for row in attempt_rows:
        attempt_count += 1
        venue = str(_first(row, "venue") or "")
        success = _is_true(_first(row, "success"))
        if success:
            success_count += 1
            row_ts = _row_ts(row)
            if row_ts is not None and (last_success_ts is None or row_ts > last_success_ts):
                last_success_ts = row_ts
        if venue:
            venue_attempts[venue] += 1
            if success:
                venue_success[venue] += 1
        failure = str(_first(row, "failure_reason") or "")
        if failure:
            failures[failure] += 1
        symbol = str(_first(row, "resolved_symbol") or "")
        if symbol:
            resolved[symbol] += 1
    source_counter = Counter(str(_first(row, "market_context_source") or "") for row in lp_rows)
    total_lp = len(lp_rows)
    window = {
        "live_public_rate": _rate(source_counter.get("live_public", 0), total_lp),
        "unavailable_rate": _rate(source_counter.get("unavailable", 0), total_lp),
        "market_context_attempt_count": attempt_count,
        "market_context_success_count": success_count,
        "market_context_failure_count": max(attempt_count - success_count, 0),
        "market_context_attempt_success_rate": _rate(success_count, attempt_count),
        "market_context_success_rate": _rate(success_count, attempt_count),
        "market_context_success_rate_reason": "no_attempts" if attempt_count == 0 else "",
        "market_context_primary_venue": str(getattr(app_config, "MARKET_CONTEXT_PRIMARY_VENUE", "")),
        "market_context_secondary_venue": str(getattr(app_config, "MARKET_CONTEXT_SECONDARY_VENUE", "")),
        "market_context_fixture_mode_detected": sum(
            1 for row in attempt_rows
            if "fixture" in str(_first(row, "endpoint", "failure_reason") or "").lower()
        ),
        "market_context_live_mode_detected": sum(
            1 for row in attempt_rows
            if any(item in str(_first(row, "endpoint", "venue") or "").lower() for item in ("okx", "kraken"))
        ),
        "market_context_unavailable_count": source_counter.get("unavailable", 0),
        "market_context_last_success_ts": last_success_ts,
        "okx_attempts": venue_attempts.get("okx_perp", 0) + venue_attempts.get("okx", 0),
        "okx_success": venue_success.get("okx_perp", 0) + venue_success.get("okx", 0),
        "kraken_attempts": venue_attempts.get("kraken_futures", 0) + venue_attempts.get("kraken", 0),
        "kraken_success": venue_success.get("kraken_futures", 0) + venue_success.get("kraken", 0),
        "resolved_symbol_distribution": dict(resolved.most_common(20)),
        "top_failure_reasons": dict(failures.most_common(10)),
    }
    return {"window": window}


def _major_coverage_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    pair_counter = Counter()
    asset_counter = Counter()
    for row in lp_rows:
        asset = _canonical_asset(_first(row, "asset", "asset_symbol"))
        pair = _canonical_pair(_first(row, "pair", "pair_label"), asset)
        if asset:
            asset_counter[asset] += 1
        if pair and "/" in pair:
            pair_counter[pair] += 1
    covered = sorted(pair for pair in EXPECTED_MAJOR_PAIRS if pair_counter.get(pair, 0) > 0)
    missing = sorted(pair for pair in EXPECTED_MAJOR_PAIRS if pair not in covered)
    return {
        "configured_major_assets": ["ETH", "WETH", "BTC", "WBTC", "CBBTC", "SOL", "WSOL"],
        "configured_major_quotes": ["USDT", "USDC", "USDC.E"],
        "asset_distribution": dict(sorted(asset_counter.items())),
        "pair_distribution": dict(sorted(pair_counter.items())),
        "covered_major_pairs": covered,
        "missing_major_pairs": missing,
        "eth_signal_count": asset_counter.get("ETH", 0),
        "btc_signal_count": asset_counter.get("BTC", 0),
        "sol_signal_count": asset_counter.get("SOL", 0),
        "current_sample_still_eth_only": bool(asset_counter) and set(asset_counter).issubset({"ETH"}),
        "major_cli_summary": {
            "available": False,
            "reason": "date_scoped_daily_report_uses_window_rows",
            "expected_major_pairs": list(EXPECTED_MAJOR_PAIRS),
            "covered_major_pairs": covered,
            "missing_expected_pairs": missing,
        },
        "btc_coverage_status": "has_recent_signals" if asset_counter.get("BTC", 0) > 0 else "no_recent_btc_signals",
        "btc_recent_signal_count": asset_counter.get("BTC", 0),
        "btc_recent_outcomes_count": 0,
        "btc_usdc_configured": "BTC/USDC" in EXPECTED_MAJOR_PAIRS,
        "btc_usdt_configured": "BTC/USDT" in EXPECTED_MAJOR_PAIRS,
        "sol_unsupported_reason": "native_solana_requires_solana_listener; current active LP scan is Ethereum-only",
    }


def _read_trade_replay_summary(logical_date: str) -> dict[str, Any]:
    path = _db_path()
    if not path.exists():
        return {"trade_replay_available": False, "reason": "sqlite_db_missing", "logical_date": logical_date, "replay_count": 0}
    try:
        import quality_reports

        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except Exception as exc:
        return {"trade_replay_available": False, "reason": f"sqlite_open_failed:{exc}", "logical_date": logical_date, "replay_count": 0}
    try:
        tables = {str(row["name"]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "trade_replay_examples" not in tables or "trade_replay_profile_stats" not in tables:
            return {"trade_replay_available": False, "reason": "trade_replay_tables_missing", "logical_date": logical_date, "replay_count": 0}
        return quality_reports._trade_replay_summary_payload(conn, logical_date)
    except Exception as exc:
        return {"trade_replay_available": False, "reason": f"trade_replay_summary_failed:{exc}", "logical_date": logical_date, "replay_count": 0}
    finally:
        conn.close()


def _shadow_opportunity_summary(opportunity_rows: list[dict[str, Any]], replay_summary: dict[str, Any]) -> dict[str, Any]:
    shadow_candidate = 0
    shadow_verified = 0
    for row in opportunity_rows:
        shadow_status = str(_first(row, "trade_opportunity_shadow_status", "shadow_status") or "NONE")
        if shadow_status == "SHADOW_CANDIDATE":
            shadow_candidate += 1
        elif shadow_status == "SHADOW_VERIFIED":
            shadow_verified += 1
    positive_profiles = [
        item for item in replay_summary.get("top_positive_profiles", [])
        if "SHADOW_" in str(item.get("profile_key") or "")
    ]
    negative_profiles = [
        item for item in replay_summary.get("top_negative_profiles", [])
        if "SHADOW_" in str(item.get("profile_key") or "")
    ]
    return {
        "shadow_candidate_count": shadow_candidate,
        "shadow_verified_count": shadow_verified,
        "shadow_replay_count": int(replay_summary.get("shadow_replay_count") or 0),
        "shadow_positive_profile_count": len(positive_profiles),
        "shadow_negative_profile_count": len(negative_profiles),
    }


def _runtime_health_summary(logical_date: str) -> dict[str, Any]:
    try:
        import runtime_health

        return runtime_health.build_runtime_health_report(date_str=logical_date)
    except Exception as exc:
        return {"data_quality_status": "unknown", "data_gap_warnings": [f"runtime_health_unavailable:{exc}"]}


def _data_quality_summary(
    *,
    runtime: dict[str, Any],
    data_source: dict[str, Any],
    market_context: dict[str, Any],
    run_overview: dict[str, Any],
    opportunity_count: int,
) -> dict[str, Any]:
    warnings = list(runtime.get("data_gap_warnings") or [])
    mismatch_warnings = list(data_source.get("mismatch_warnings") or data_source.get("source_warnings") or [])
    mismatch_rows = 0
    mismatch_categories: list[str] = []
    for key, item in (data_source.get("db_archive_mirror_detail") or {}).items():
        if not isinstance(item, dict) or not item.get("mismatch"):
            continue
        mismatch_categories.append(str(key))
        mismatch_rows += abs(int(item.get("sqlite_rows") or 0) - int(item.get("archive_rows") or 0))
    if mismatch_warnings and not mismatch_categories:
        mismatch_categories = [str(item).split(":")[1] if ":" in str(item) else str(item) for item in mismatch_warnings]
        mismatch_rows = len(mismatch_warnings)
    status = str(runtime.get("data_quality_status") or "valid")
    if mismatch_rows > 0 and status == "valid":
        status = "degraded"
    if int(runtime.get("raw_events_count") or run_overview.get("total_raw_events") or 0) == 0 and opportunity_count > 0:
        warnings.append("stale_or_cross_day_data_warning")
        status = "degraded" if status == "valid" else status
    if int(runtime.get("zero_activity_day") or 0):
        status = "invalid_or_no_activity"
    return {
        "active_hours": runtime.get("active_hours"),
        "raw_events_count": runtime.get("raw_events_count", run_overview.get("total_raw_events")),
        "parsed_events_count": runtime.get("parsed_events_count", run_overview.get("total_parsed_events")),
        "signals_count": runtime.get("signals_count", run_overview.get("total_signal_rows")),
        "max_raw_event_gap_sec": runtime.get("max_raw_event_gap_sec"),
        "max_signal_gap_sec": runtime.get("max_signal_gap_sec"),
        "max_gap": max(
            float(runtime.get("max_raw_event_gap_sec") or 0.0),
            float(runtime.get("max_signal_gap_sec") or 0.0),
        ),
        "zero_activity_day": bool(runtime.get("zero_activity_day")),
        "data_gap_warnings": sorted(set(str(item) for item in warnings if item)),
        "market_context_success_rate": (market_context.get("window") or {}).get("market_context_success_rate"),
        "db_archive_mismatch_status": "mismatch" if mismatch_rows > 0 else "match_or_unchecked",
        "db_archive_mirror_match_rate": data_source.get("db_archive_mirror_match_rate"),
        "db_archive_mismatch_categories": mismatch_categories,
        "db_archive_mismatch_rows": mismatch_rows,
        "data_quality_status": status,
    }


def _candidate_verified_summary(trade_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_count": trade_summary.get("opportunity_candidate_count", 0),
        "verified_count": trade_summary.get("opportunity_verified_count", 0),
        "candidate_distribution": {"CANDIDATE": trade_summary.get("opportunity_candidate_count", 0)},
        "verified_distribution": {"VERIFIED": trade_summary.get("opportunity_verified_count", 0)},
        "candidate_outcome_60s": trade_summary.get("candidate_outcome_60s", {}),
        "verified_outcome_60s": trade_summary.get("verified_outcome_60s", {}),
        "candidate_outcome_completed_rate": trade_summary.get("candidate_outcome_completion_rate"),
        "verified_maturity": trade_summary.get("verified_maturity"),
        "verified_should_not_be_traded_reason": trade_summary.get("verified_should_not_be_traded_reason"),
        "maturity_reasons": trade_summary.get("maturity_reasons", []),
    }


def _top_distribution(distribution: dict[str, Any], limit: int = 5) -> dict[str, int]:
    counter = Counter({str(key): int(value) for key, value in (distribution or {}).items()})
    return dict(counter.most_common(limit))


def _trade_action_summary(signal_rows: list[dict[str, Any]], delivery_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counter: Counter[str] = Counter()
    source_fields: Counter[str] = Counter()
    fields_found = False
    for row in [*signal_rows, *delivery_rows]:
        if any(_field_present(row, field) for field in TRADE_ACTION_FIELDS):
            fields_found = True
        key, value = _first_with_key(row, TRADE_ACTION_FIELDS)
        if key is None or value in (None, "", [], {}, ()):
            continue
        label = str(value).strip()
        if not label:
            continue
        counter[label] += 1
        source_fields[key] += 1
    if not fields_found:
        return {
            "available": False,
            "trade_action_distribution": {},
            "trade_action_distribution_top": {},
            "reason": "no_trade_action_fields_found",
            "source_fields": {},
        }
    if not counter:
        return {
            "available": False,
            "trade_action_distribution": {},
            "trade_action_distribution_top": {},
            "reason": "no_trade_action_values_found",
            "source_fields": dict(sorted(source_fields.items())),
        }
    distribution = dict(sorted(counter.items()))
    return {
        "available": True,
        "trade_action_distribution": distribution,
        "trade_action_distribution_top": _top_distribution(distribution),
        "source_fields": dict(sorted(source_fields.items())),
        "row_count": sum(counter.values()),
    }


def _is_prealert_stage(row: dict[str, Any]) -> bool:
    return str(_first(row, "first_seen_stage", "lp_alert_stage", "stage") or "").strip().lower() == "prealert"


def _prealert_state(row: dict[str, Any]) -> str:
    return str(_first(row, "prealert_lifecycle_state", "state", "lifecycle_state") or "").strip().lower()


def _prealert_missing_summary() -> dict[str, Any]:
    return {
        "available": False,
        "source": "missing",
        "reason": "prealert_lifecycle_missing",
        "prealert_candidate_count": 0,
        "prealert_gate_passed_count": 0,
        "prealert_active_count": 0,
        "prealert_delivered_count": 0,
        "prealert_upgraded_to_confirm_count": 0,
        "prealert_expired_count": 0,
        "median_prealert_to_confirm_sec": None,
    }


def _prealert_lifecycle_from_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    durations = [
        _to_float(_first(row, "prealert_to_confirm_sec", "asset_case_prealert_to_confirm_sec"))
        for row in rows
    ]
    durations = [value for value in durations if value is not None]
    distribution = Counter(_prealert_state(row) for row in rows if _prealert_state(row))
    return {
        "available": True,
        "source": "prealert_lifecycle",
        "row_count": len(rows),
        "prealert_candidate_count": sum(1 for row in rows if _is_true(_first(row, "candidate", "prealert_candidate", "lp_prealert_candidate"))),
        "prealert_gate_passed_count": sum(1 for row in rows if _is_true(_first(row, "gate_passed", "prealert_gate_passed", "lp_prealert_gate_passed"))),
        "prealert_active_count": sum(1 for row in rows if _is_true(_first(row, "active", "prealert_active")) or _prealert_state(row) == "active"),
        "prealert_delivered_count": sum(1 for row in rows if _is_true(_first(row, "delivered", "prealert_delivered")) or _prealert_state(row) == "delivered"),
        "prealert_upgraded_to_confirm_count": sum(1 for row in rows if _is_true(_first(row, "upgraded_to_confirm", "prealert_upgraded_to_confirm")) or _prealert_state(row) == "upgraded_to_confirm"),
        "prealert_expired_count": sum(1 for row in rows if _is_true(_first(row, "expired", "prealert_expired")) or _prealert_state(row) == "expired"),
        "median_prealert_to_confirm_sec": round(float(median(durations)), 1) if durations else None,
        "lifecycle_distribution": dict(sorted(distribution.items())),
    }


def _prealert_lifecycle_from_signals(signal_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows_with_fields = [
        row
        for row in signal_rows
        if _is_prealert_stage(row) or any(_field_present(row, field) for field in PREALERT_COUNT_FIELDS)
    ]
    if not rows_with_fields:
        return _prealert_missing_summary()
    durations = [
        _to_float(_first(row, "prealert_to_confirm_sec", "asset_case_prealert_to_confirm_sec"))
        for row in rows_with_fields
    ]
    durations = [value for value in durations if value is not None]
    distribution = Counter(_prealert_state(row) for row in rows_with_fields if _prealert_state(row))
    return {
        "available": True,
        "source": "signal_rows",
        "row_count": len(rows_with_fields),
        "prealert_candidate_count": sum(
            1
            for row in rows_with_fields
            if _is_true(_first(row, "prealert_candidate", "lp_prealert_candidate")) or _is_prealert_stage(row)
        ),
        "prealert_gate_passed_count": sum(
            1 for row in rows_with_fields if _is_true(_first(row, "prealert_gate_passed", "lp_prealert_gate_passed"))
        ),
        "prealert_active_count": sum(
            1 for row in rows_with_fields if _is_true(_first(row, "prealert_active")) or _prealert_state(row) == "active"
        ),
        "prealert_delivered_count": sum(
            1
            for row in rows_with_fields
            if _is_true(_first(row, "prealert_delivered", "prealert_visible_to_user"))
            or (_is_prealert_stage(row) and _is_true(_first(row, "sent_to_telegram", "telegram_sent", "notifier_sent_at", "delivered_notification")))
            or _prealert_state(row) == "delivered"
        ),
        "prealert_upgraded_to_confirm_count": sum(
            1
            for row in rows_with_fields
            if _is_true(_first(row, "prealert_upgraded_to_confirm"))
            or _prealert_state(row) == "upgraded_to_confirm"
            or _to_float(_first(row, "prealert_to_confirm_sec", "asset_case_prealert_to_confirm_sec")) is not None
        ),
        "prealert_expired_count": sum(
            1 for row in rows_with_fields if _is_true(_first(row, "prealert_expired")) or _prealert_state(row) == "expired"
        ),
        "median_prealert_to_confirm_sec": round(float(median(durations)), 1) if durations else None,
        "lifecycle_distribution": dict(sorted(distribution.items())),
    }


def _prealert_lifecycle_summary(prealert_rows: list[dict[str, Any]], signal_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if prealert_rows:
        return _prealert_lifecycle_from_table(prealert_rows)
    return _prealert_lifecycle_from_signals(signal_rows)


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Canonical Daily Report",
        "",
        f"- logical_date: `{payload['logical_date']}`",
        f"- logical_window_utc: `{payload['logical_window_start_utc']} -> {payload['logical_window_end_utc']}`",
        f"- active_duration_hours: `{payload['active_duration_hours']}`",
        f"- wall_clock_duration_hours: `{payload['wall_clock_duration_hours']}`",
        f"- data_source: `{payload['data_source_summary'].get('report_data_source')}`",
        f"- report_conclusion: `{payload.get('report_conclusion')}`",
        "",
        "## Summary",
        "",
    ]
    for item in payload.get("key_findings", []):
        lines.append(f"- {item}")
    trade_action = payload.get("trade_action_summary") or {}
    prealert = payload.get("prealert_lifecycle_summary") or {}
    trade_action_top = trade_action.get("trade_action_distribution_top") or _top_distribution(trade_action.get("trade_action_distribution") or {})
    lines.extend(
        [
            "",
            "## Metric Snapshot",
            "",
            f"- verified_maturity: `{payload.get('trade_opportunity_summary', {}).get('verified_maturity', 'unknown')}`",
            f"- trade_action_distribution_top: `{json.dumps(trade_action_top, ensure_ascii=False, sort_keys=True)}`",
            f"- prealert_lifecycle_available: `{bool(prealert.get('available'))}` source=`{prealert.get('source', 'missing')}`",
        ]
    )
    replay = payload.get("trade_replay_summary") or {}
    shadow = payload.get("shadow_opportunity_summary") or {}
    data_quality = payload.get("data_quality_summary") or {}
    lines.extend(
        [
            "",
            "## 交易后验 / Trade Replay",
            "",
            f"- trade_replay_available: `{bool(replay.get('trade_replay_available'))}`",
            f"- replay_count: `{replay.get('replay_count', 0)}`",
            f"- valid_replay_count: `{replay.get('valid_replay_count', replay.get('valid_count', 0))}`",
            f"- win_rate: `{replay.get('win_rate')}`",
            f"- avg_net_pnl_bps: `{replay.get('avg_net_pnl_bps')}`",
            f"- clean_followthrough_rate: `{replay.get('clean_followthrough_rate')}`",
            f"- bad_entry_rate: `{replay.get('bad_entry_rate')}`",
            f"- absorption_reversal_rate: `{replay.get('absorption_reversal_rate')}`",
            f"- chop_rate: `{replay.get('chop_rate')}`",
            f"- data_invalid_rate: `{replay.get('data_invalid_rate')}`",
            f"- suppressed_profitable_rate: `{replay.get('suppressed_profitable_rate')}`",
            f"- blocked_saved_rate_estimate: `{replay.get('blocked_saved_rate_estimate')}`",
            f"- top_positive_profiles: `{json.dumps(replay.get('top_positive_profiles', [])[:3], ensure_ascii=False, sort_keys=True)}`",
            f"- top_negative_profiles: `{json.dumps(replay.get('top_negative_profiles', [])[:3], ensure_ascii=False, sort_keys=True)}`",
            "",
            "## Shadow Opportunity 学习样本",
            "",
            f"- shadow_candidate_count: `{shadow.get('shadow_candidate_count', 0)}`",
            f"- shadow_verified_count: `{shadow.get('shadow_verified_count', 0)}`",
            f"- shadow_replay_count: `{shadow.get('shadow_replay_count', 0)}`",
            f"- shadow_positive_profile_count: `{shadow.get('shadow_positive_profile_count', 0)}`",
            f"- shadow_negative_profile_count: `{shadow.get('shadow_negative_profile_count', 0)}`",
            "",
            "## 数据质量",
            "",
            f"- active_hours: `{data_quality.get('active_hours')}`",
            f"- max_gap: `{data_quality.get('max_gap')}`",
            f"- zero_activity_day: `{data_quality.get('zero_activity_day')}`",
            f"- market_context_success_rate: `{data_quality.get('market_context_success_rate')}`",
            f"- db_archive_mismatch_status: `{data_quality.get('db_archive_mismatch_status')}`",
            f"- data_quality_status: `{data_quality.get('data_quality_status')}`",
        ]
    )
    lines.extend(["", "## Limitations", ""])
    for item in payload.get("limitations", []) or ["none"]:
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def _csv_text(payload: dict[str, Any]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["metric_group", "metric_name", "value"])
    writer.writeheader()
    rows = [
        ("window", "logical_date", payload.get("logical_date")),
        ("window", "active_duration_hours", payload.get("active_duration_hours")),
        ("window", "wall_clock_duration_hours", payload.get("wall_clock_duration_hours")),
        ("run", "raw_events_count", payload.get("run_overview", {}).get("total_raw_events")),
        ("run", "parsed_events_count", payload.get("run_overview", {}).get("total_parsed_events")),
        ("run", "signals_count", payload.get("run_overview", {}).get("total_signal_rows")),
        ("run", "lp_signal_rows", payload.get("run_overview", {}).get("lp_signal_rows")),
        ("opportunity", "opportunity_candidate_count", payload.get("trade_opportunity_summary", {}).get("opportunity_candidate_count")),
        ("opportunity", "opportunity_verified_count", payload.get("trade_opportunity_summary", {}).get("opportunity_verified_count")),
        ("opportunity", "verified_maturity", payload.get("trade_opportunity_summary", {}).get("verified_maturity")),
        ("trade_action", "trade_action_distribution_top", payload.get("trade_action_summary", {}).get("trade_action_distribution_top")),
        ("prealert", "prealert_candidate_count", payload.get("prealert_lifecycle_summary", {}).get("prealert_candidate_count")),
        ("prealert", "prealert_delivered_count", payload.get("prealert_lifecycle_summary", {}).get("prealert_delivered_count")),
        ("prealert", "median_prealert_to_confirm_sec", payload.get("prealert_lifecycle_summary", {}).get("median_prealert_to_confirm_sec")),
        ("telegram", "telegram_suppression_ratio", payload.get("telegram_suppression_summary", {}).get("telegram_suppression_ratio")),
        ("market_context", "okx_attempts", payload.get("market_context_health", {}).get("window", {}).get("okx_attempts")),
        ("market_context", "market_context_attempt_success_rate", payload.get("market_context_health", {}).get("window", {}).get("market_context_attempt_success_rate")),
        ("majors", "missing_major_pairs_count", len(payload.get("major_coverage_summary", {}).get("missing_major_pairs") or [])),
        ("majors", "btc_recent_signal_count", payload.get("major_coverage_summary", {}).get("btc_recent_signal_count")),
        ("trade_replay", "replay_count", payload.get("trade_replay_summary", {}).get("replay_count")),
        ("trade_replay", "valid_replay_count", payload.get("trade_replay_summary", {}).get("valid_replay_count")),
        ("trade_replay", "avg_net_pnl_bps", payload.get("trade_replay_summary", {}).get("avg_net_pnl_bps")),
        ("trade_replay", "clean_followthrough_rate", payload.get("trade_replay_summary", {}).get("clean_followthrough_rate")),
        ("trade_replay", "bad_entry_rate", payload.get("trade_replay_summary", {}).get("bad_entry_rate")),
        ("trade_replay", "absorption_reversal_rate", payload.get("trade_replay_summary", {}).get("absorption_reversal_rate")),
        ("shadow", "shadow_candidate_count", payload.get("shadow_opportunity_summary", {}).get("shadow_candidate_count")),
        ("shadow", "shadow_verified_count", payload.get("shadow_opportunity_summary", {}).get("shadow_verified_count")),
        ("data_quality", "data_quality_status", payload.get("data_quality_summary", {}).get("data_quality_status")),
    ]
    for group, name, value in rows:
        writer.writerow({"metric_group": group, "metric_name": name, "value": json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value})
    return output.getvalue()


def build_daily_report(logical_date: str) -> dict[str, Any]:
    window = _logical_window(logical_date)
    rows, results = _load_window(window)
    signal_rows = rows.get("signals", [])
    lp_rows = [row for row in signal_rows if _is_lp_row(row)]
    segment_summary = _build_segments(signal_rows, lp_rows)
    delivery_rows = rows.get("delivery_audit", [])
    telegram = _telegram_summary(lp_rows, rows.get("telegram_deliveries", []) or delivery_rows)
    opportunity_rows = rows.get("trade_opportunities", []) or [
        row
        for row in signal_rows
        if _first(row, "trade_opportunity_status", "trade_opportunity_status_at_creation", "trade_opportunity_id")
    ]
    opportunity_db_summary = _read_opportunity_db_summary()
    trade_summary = _trade_opportunity_summary(
        opportunity_rows,
        quality_rows=rows.get("quality_stats", []),
        opportunity_db_summary=opportunity_db_summary,
    )
    asset_state = _asset_market_state_summary(rows.get("asset_market_states", []) or signal_rows)
    no_trade_lock = _no_trade_lock_summary(asset_state)
    outcome = _outcome_source_summary(rows.get("outcomes", []) or signal_rows)
    market_context = _market_context_health(rows.get("market_context_attempts", []), lp_rows)
    majors = _major_coverage_summary(lp_rows)
    data_source = _data_source_summary(results)
    replay_summary = _read_trade_replay_summary(logical_date)
    shadow_summary = _shadow_opportunity_summary(opportunity_rows, replay_summary)
    runtime_health_summary = _runtime_health_summary(logical_date)
    trade_actions = _trade_action_summary(signal_rows, delivery_rows)
    prealert_lifecycle = _prealert_lifecycle_summary(rows.get("prealert_lifecycle", []), signal_rows)
    verified_maturity = str(trade_summary.get("verified_maturity") or "unknown")
    verified_reason = str(trade_summary.get("verified_should_not_be_traded_reason") or "")
    limitations = ["CANDIDATE is not a trade signal"]
    if verified_maturity == "immature":
        limitations.append("verified_maturity=immature; VERIFIED must not be treated as mature trade signal")
    elif verified_maturity == "unknown":
        limitations.append(f"verified_maturity_unknown:{verified_reason or 'insufficient_data'}")
    if not trade_actions.get("available"):
        limitations.append(f"trade_action_summary_unavailable:{trade_actions.get('reason', 'unknown')}")
    if not prealert_lifecycle.get("available"):
        limitations.append("prealert_lifecycle_missing")
    limitations.extend(segment_summary.get("gap_warnings") or [])
    limitations.extend(data_source.get("source_warnings") or [])
    result_count = lambda key: results.get(key, _empty_result()).row_count
    run_overview = {
        "total_raw_events": result_count("raw_events"),
        "total_parsed_events": result_count("parsed_events"),
        "total_signal_rows": len(signal_rows),
        "lp_signal_rows": len(lp_rows),
        "delivered_lp_signals": telegram["delivered_lp_signals"],
        "suppressed_lp_signals": telegram["suppressed_lp_signals"],
        "asset_case_count": result_count("asset_cases"),
        "case_followup_count": result_count("case_followups"),
    }
    data_quality = _data_quality_summary(
        runtime=runtime_health_summary,
        data_source=data_source,
        market_context=market_context,
        run_overview=run_overview,
        opportunity_count=len(opportunity_rows),
    )
    blocker_summary = {
        "hard_blocker_distribution": trade_summary.get("hard_blocker_distribution", {}),
        "verification_blocker_distribution": trade_summary.get("verification_blocker_distribution", {}),
        "top_blockers": trade_summary.get("top_blockers", {}),
        "blocker_saved_rate": trade_summary.get("blocker_saved_rate"),
        "blocker_false_block_rate": trade_summary.get("blocker_false_block_rate"),
    }
    key_findings = [
        f"logical_date={logical_date} source={data_source.get('report_data_source')} lp_rows={len(lp_rows)}",
        f"segments={segment_summary['segment_count']} active_hours={segment_summary['active_duration_hours']} wall_clock_hours=24.0",
        (
            "opportunities: "
            f"candidate={trade_summary['opportunity_candidate_count']} "
            f"verified={trade_summary['opportunity_verified_count']} "
            f"blocked={trade_summary['opportunity_blocked_count']}"
        ),
        f"verified_maturity={verified_maturity}; CANDIDATE is not a trade signal",
        (
            "trade_replay: "
            f"available={bool(replay_summary.get('trade_replay_available'))} "
            f"replay={int(replay_summary.get('replay_count') or 0)} "
            f"valid={int(replay_summary.get('valid_replay_count') or replay_summary.get('valid_count') or 0)}"
        ),
        f"data_quality={data_quality.get('data_quality_status')} zero_activity_day={bool(data_quality.get('zero_activity_day'))}",
        f"trade_action_distribution_top={json.dumps(trade_actions.get('trade_action_distribution_top') or {}, ensure_ascii=False, sort_keys=True)}",
        f"prealert_lifecycle_available={bool(prealert_lifecycle.get('available'))} source={prealert_lifecycle.get('source', 'missing')}",
    ]
    key_risks = ["CANDIDATE is not a trade signal"]
    if verified_maturity == "immature":
        key_risks.append("VERIFIED maturity is immature and must remain research/filter evidence only")
    elif verified_maturity == "unknown":
        key_risks.append("VERIFIED maturity is unknown because supporting maturity data is insufficient")
    if not replay_summary.get("trade_replay_available"):
        limitations.append("trade_replay_missing")
    if data_quality.get("data_quality_status") in {"degraded", "invalid_or_no_activity"}:
        limitations.append(f"data_quality={data_quality.get('data_quality_status')}")
    if data_quality.get("data_quality_status") == "invalid_or_no_activity":
        report_conclusion = "data_invalid"
    elif data_quality.get("data_quality_status") == "degraded":
        report_conclusion = "data_invalid"
    elif replay_summary.get("trade_replay_available") and replay_summary.get("top_positive_profiles"):
        report_conclusion = "replay_positive_profiles_found"
    elif shadow_summary.get("shadow_candidate_count") or shadow_summary.get("shadow_verified_count"):
        report_conclusion = "learning_samples_accumulating"
    elif trade_summary.get("opportunity_blocked_count") and not trade_summary.get("opportunity_candidate_count") and not trade_summary.get("opportunity_verified_count"):
        report_conclusion = "risk_filtering_only"
    elif verified_maturity == "mature" and replay_summary.get("trade_replay_available"):
        report_conclusion = "production_trade_ready"
    else:
        report_conclusion = "learning_samples_accumulating"
    payload = {
        "report_type": "daily_canonical",
        "logical_date": logical_date,
        "timezone": "Asia/Shanghai",
        "analysis_window": {
            **window,
            "duration_hours": 24.0,
            "start_utc": window["logical_window_start_utc"],
            "end_utc": window["logical_window_end_utc"],
            "start_bj": window["logical_window_start_beijing"],
            "end_bj": window["logical_window_end_beijing"],
        },
        "logical_window_start_utc": window["logical_window_start_utc"],
        "logical_window_end_utc": window["logical_window_end_utc"],
        "logical_window_start_beijing": window["logical_window_start_beijing"],
        "logical_window_end_beijing": window["logical_window_end_beijing"],
        "segment_summary": segment_summary,
        "active_duration_hours": segment_summary["active_duration_hours"],
        "wall_clock_duration_hours": 24.0,
        "run_overview": run_overview,
        "data_source": data_source.get("report_data_source"),
        "data_source_summary": data_source,
        "data_quality_summary": data_quality,
        "runtime_health_summary": runtime_health_summary,
        "market_context_health": market_context,
        "trade_opportunity_summary": trade_summary,
        "trade_replay_summary": replay_summary,
        "profile_posterior_summary": {
            "top_positive_profiles": replay_summary.get("top_positive_profiles", []),
            "top_negative_profiles": replay_summary.get("top_negative_profiles", []),
            "recommended_profile_actions": replay_summary.get("recommended_profile_actions", []),
        },
        "shadow_opportunity_summary": shadow_summary,
        "candidate_verified_summary": _candidate_verified_summary(trade_summary),
        "candidate_tradeable_summary": _candidate_verified_summary(trade_summary),
        "blocker_summary": blocker_summary,
        "outcome_summary": outcome,
        "outcome_source_summary": outcome,
        "telegram_suppression_summary": telegram,
        "asset_market_state_summary": asset_state,
        "no_trade_lock_summary": no_trade_lock,
        "trade_action_summary": trade_actions,
        "prealert_lifecycle_summary": prealert_lifecycle,
        "major_coverage_summary": majors,
        "majors_coverage_summary": majors,
        "archive_health": {
            "archive_base_dir": str(getattr(app_config, "ARCHIVE_BASE_DIR", APP_DIR / "data" / "archive")),
            "archive_fallback_used": data_source.get("archive_fallback_used"),
        },
        "report_conclusion": report_conclusion,
        "key_findings": key_findings,
        "key_risks": key_risks,
        "limitations": sorted(set(str(item) for item in limitations if item)),
    }
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_daily_artifacts(payload: dict[str, Any], *, output_dir: Path = DAILY_DIR) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    logical_date = str(payload["logical_date"])
    latest_md = output_dir / "daily_report_latest.md"
    latest_csv = output_dir / "daily_report_latest.csv"
    latest_json = output_dir / "daily_report_latest.json"
    dated_md = output_dir / f"daily_report_{logical_date}.md"
    dated_csv = output_dir / f"daily_report_{logical_date}.csv"
    dated_json = output_dir / f"daily_report_{logical_date}.json"
    markdown = _markdown(payload)
    csv_text = _csv_text(payload)
    for path in (latest_md, dated_md):
        _write_text(path, markdown)
    for path in (latest_csv, dated_csv):
        _write_text(path, csv_text)
    for path in (latest_json, dated_json):
        _write_json(path, payload)
    return {
        "latest_markdown": str(latest_md),
        "latest_csv": str(latest_csv),
        "latest_json": str(latest_json),
        "dated_markdown": str(dated_md),
        "dated_csv": str(dated_csv),
        "dated_json": str(dated_json),
    }


def generate_daily_report(logical_date: str | None = None, *, output_dir: Path = DAILY_DIR) -> tuple[dict[str, Any], dict[str, str]]:
    selected_date = logical_date or latest_available_logical_date()
    payload = build_daily_report(selected_date)
    written = write_daily_artifacts(payload, output_dir=output_dir)
    return payload, written


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("END_DATE must be greater than or equal to START_DATE")
    values: list[str] = []
    current = start
    while current <= end:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate canonical Beijing-logical-day daily report")
    parser.add_argument("--date", help="Generate report for the specified Beijing logical date YYYY-MM-DD")
    parser.add_argument("--start-date", help="Generate reports for an inclusive Beijing logical date range")
    parser.add_argument("--end-date", help="Generate reports for an inclusive Beijing logical date range")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.date and (args.start_date or args.end_date):
        raise SystemExit("--date cannot be combined with --start-date/--end-date")
    if bool(args.start_date) != bool(args.end_date):
        raise SystemExit("--start-date and --end-date must be provided together")
    if args.start_date and args.end_date:
        reports = []
        for logical_date in _date_range(args.start_date, args.end_date):
            payload, written = generate_daily_report(logical_date)
            reports.append({"logical_date": payload["logical_date"], "outputs": written})
        print(json.dumps({"status": "ok", "reports": reports}, ensure_ascii=False))
        return 0
    payload, written = generate_daily_report(args.date)
    print(
        json.dumps(
            {
                "status": "ok",
                "logical_date": payload["logical_date"],
                "outputs": written,
                "active_duration_hours": payload["active_duration_hours"],
                "wall_clock_duration_hours": payload["wall_clock_duration_hours"],
                "limitations": payload["limitations"][:10],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
