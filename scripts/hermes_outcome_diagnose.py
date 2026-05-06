#!/usr/bin/env python3
"""Read-only outcome loop diagnosis for Hermes Telegram control."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


BJ_TZ = dt.timezone(dt.timedelta(hours=8))
DEFAULT_DB_PATH = "data/chain_monitor.sqlite"
DEFAULT_DAILY_DIR = "reports/daily"
PRICE_FAILURE_TERMS = ("price", "snapshot", "unavailable", "missing", "no_price", "no market")
TEXT_LIMIT = 64


def parse_date(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value or ""):
        raise ValueError("date must be YYYY-MM-DD")
    parsed = dt.date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("date must be YYYY-MM-DD")
    return value


def logical_window(logical_date: str) -> tuple[int, int]:
    parsed = dt.date.fromisoformat(logical_date)
    start_bj = dt.datetime(parsed.year, parsed.month, parsed.day, tzinfo=BJ_TZ)
    start_ts = int(start_bj.timestamp())
    return start_ts, start_ts + 24 * 3600 - 1


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def ident(column: str, alias: str = "") -> str:
    escaped = column.replace('"', '""')
    if alias:
        return f'{alias}."{escaped}"'
    return f'"{escaped}"'


def epoch_expr(column: str, alias: str = "") -> str:
    ref = ident(column, alias)
    return f"(CASE WHEN CAST({ref} AS REAL) > 10000000000 THEN CAST({ref} AS REAL) / 1000.0 ELSE CAST({ref} AS REAL) END)"


def time_expr(cols: set[str], candidates: tuple[str, ...], alias: str = "") -> str:
    present = [column for column in candidates if column in cols]
    if not present:
        return ""
    if len(present) == 1:
        return epoch_expr(present[0], alias)
    return "COALESCE(" + ", ".join(epoch_expr(column, alias) for column in present) + ")"


def select_value(cols: set[str], column: str, alias_name: str, table_alias: str) -> str:
    if column in cols:
        return f"{ident(column, table_alias)} AS {ident(alias_name)}"
    return f"'' AS {ident(alias_name)}"


def safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def redact_text(value: Any, *, limit: int = TEXT_LIMIT) -> str:
    text = str(value or "").strip()
    text = re.sub(r"0x[0-9A-Fa-f]{64}", "0xTX_REDACTED", text)
    text = re.sub(r"0x[0-9A-Fa-f]{40}", "0xADDR_REDACTED", text)
    text = re.sub(r"/(?:root|home|run-project)(?:/[^\s<>\"'`;,)]*)*", "[PRIVATE_PATH]", text)
    if len(text) > limit:
        text = text[: max(limit - 3, 1)] + "..."
    return text or "missing"


def normalize_status(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"", "NULL", "NONE", "NA", "N/A"}:
        return "NONE"
    return redact_text(text, limit=40)


def normalize_small(value: Any) -> str:
    return redact_text(str(value or "").strip().lower() or "missing", limit=40)


def format_rate(matched: int, total: int) -> str:
    if total <= 0:
        return "missing (0/0)"
    return f"{(matched / total) * 100:.2f}% ({matched}/{total})"


def format_number(value: Any) -> str:
    if value is None:
        return "missing"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


def format_counter(counter: Counter[str], *, limit: int = 12) -> str:
    if not counter:
        return "missing"
    return "；".join(f"{redact_text(key)}={value}" for key, value in counter.most_common(limit))


def chunked(values: Iterable[str], size: int = 500) -> Iterable[list[str]]:
    batch: list[str] = []
    for value in values:
        if not value:
            continue
        batch.append(str(value))
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_daily_signals(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> list[dict[str, Any]]:
    table = "signals"
    cols = columns(conn, table)
    if not cols:
        warnings.append("sqlite_missing_table:signals")
        return []
    expr = time_expr(cols, ("timestamp", "archive_written_at", "created_at", "updated_at", "notifier_sent_at"), "s")
    if not expr:
        warnings.append("sqlite_missing_time_column:signals")
        return []
    wanted = (
        "signal_id",
        "trade_opportunity_id",
        "asset",
        "pair",
        "trade_opportunity_status",
        "canonical_semantic_key",
        "trade_action_key",
        "asset_market_state_key",
        "lp_alert_stage",
        "delivery_decision",
        "signal_json",
    )
    select_list = ", ".join(select_value(cols, column, column, "s") for column in wanted)
    rows = conn.execute(f"SELECT {select_list} FROM signals s WHERE {expr} >= ? AND {expr} <= ?", (start_ts, end_ts)).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        payload = safe_json(row["signal_json"])
        result.append(
            {
                "signal_id": first_text(row["signal_id"]),
                "trade_opportunity_id": first_text(row["trade_opportunity_id"], payload.get("trade_opportunity_id")),
                "asset": first_text(row["asset"], payload.get("asset"), payload.get("asset_symbol")),
                "pair": first_text(row["pair"], payload.get("pair"), payload.get("pair_label")),
                "status": normalize_status(first_text(row["trade_opportunity_status"], payload.get("trade_opportunity_status"), payload.get("status"))),
                "signal_type": first_text(
                    row["canonical_semantic_key"],
                    row["trade_action_key"],
                    payload.get("canonical_semantic_key"),
                    payload.get("signal_type"),
                    payload.get("event_type"),
                    payload.get("intent"),
                ),
                "stage": first_text(
                    row["lp_alert_stage"],
                    payload.get("lp_alert_stage"),
                    payload.get("stage"),
                    row["asset_market_state_key"],
                    row["delivery_decision"],
                ),
            }
        )
    return result


def fetch_daily_opportunities(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> list[dict[str, Any]]:
    table = "trade_opportunities"
    cols = columns(conn, table)
    if not cols:
        warnings.append("sqlite_missing_table:trade_opportunities")
        return []
    expr = time_expr(cols, ("created_at", "updated_at"), "t")
    if not expr:
        warnings.append("sqlite_missing_time_column:trade_opportunities")
        return []
    wanted = (
        "trade_opportunity_id",
        "signal_id",
        "asset",
        "pair",
        "status",
        "side",
        "maturity",
        "opportunity_profile_strategy",
        "opportunity_profile_key",
        "label",
        "opportunity_json",
    )
    select_list = ", ".join(select_value(cols, column, column, "t") for column in wanted)
    rows = conn.execute(
        f"SELECT {select_list} FROM trade_opportunities t WHERE {expr} >= ? AND {expr} <= ?",
        (start_ts, end_ts),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        payload = safe_json(row["opportunity_json"])
        profile_key = first_text(row["opportunity_profile_key"], payload.get("opportunity_profile_key"))
        result.append(
            {
                "trade_opportunity_id": first_text(row["trade_opportunity_id"], payload.get("trade_opportunity_id")),
                "signal_id": first_text(row["signal_id"], payload.get("signal_id")),
                "asset": first_text(row["asset"], payload.get("asset"), payload.get("asset_symbol")),
                "pair": first_text(row["pair"], payload.get("pair"), payload.get("pair_label")),
                "status": normalize_status(first_text(row["status"], payload.get("trade_opportunity_status"), payload.get("status"))),
                "signal_type": first_text(
                    row["opportunity_profile_strategy"],
                    payload.get("opportunity_profile_strategy"),
                    payload.get("canonical_semantic_key"),
                    payload.get("trade_action_key"),
                    profile_key.split("|")[2] if "|" in profile_key and len(profile_key.split("|")) > 2 else "",
                ),
                "stage": first_text(row["maturity"], payload.get("maturity"), payload.get("stage"), row["label"]),
            }
        )
    return result


def table_select_columns(cols: set[str], wanted: tuple[str, ...], alias: str) -> str:
    return ", ".join(select_value(cols, column, column, alias) for column in wanted)


def fetch_rows_by_ids(
    conn: sqlite3.Connection,
    table: str,
    id_column: str,
    ids: set[str],
    wanted: tuple[str, ...],
    warnings: list[str],
) -> list[dict[str, Any]]:
    cols = columns(conn, table)
    if not cols:
        warnings.append(f"sqlite_missing_table:{table}")
        return []
    if id_column not in cols:
        warnings.append(f"sqlite_missing_column:{table}.{id_column}")
        return []
    rows: list[dict[str, Any]] = []
    select_list = table_select_columns(cols, wanted, "x")
    for batch in chunked(ids):
        placeholders = ",".join("?" for _ in batch)
        sql = f"SELECT {select_list} FROM {table} x WHERE TRIM(CAST({ident(id_column, 'x')} AS TEXT)) IN ({placeholders})"
        rows.extend(dict(row) for row in conn.execute(sql, batch).fetchall())
    return rows


def fetch_rows_by_window(
    conn: sqlite3.Connection,
    table: str,
    time_candidates: tuple[str, ...],
    wanted: tuple[str, ...],
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> list[dict[str, Any]]:
    cols = columns(conn, table)
    if not cols:
        warnings.append(f"sqlite_missing_table:{table}")
        return []
    expr = time_expr(cols, time_candidates, "x")
    if not expr:
        warnings.append(f"sqlite_missing_time_column:{table}")
        return []
    select_list = table_select_columns(cols, wanted, "x")
    sql = f"SELECT {select_list} FROM {table} x WHERE {expr} >= ? AND {expr} <= ?"
    return [dict(row) for row in conn.execute(sql, (start_ts, end_ts)).fetchall()]


def unique_rows(rows: Iterable[dict[str, Any]], key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        parts = [str(row.get(field) or "") for field in key_fields]
        key = "|".join(parts)
        if not key.strip("|"):
            key = json.dumps(row, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def fetch_related_outcomes(
    conn: sqlite3.Connection,
    signal_ids: set[str],
    opportunity_ids: set[str],
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> list[dict[str, Any]]:
    wanted = (
        "outcome_id",
        "signal_id",
        "trade_opportunity_id",
        "asset",
        "pair",
        "window_sec",
        "due_at",
        "status",
        "failure_reason",
        "completed_at",
        "created_at",
        "updated_at",
        "start_price",
        "end_price",
        "outcome_price_source",
        "price_source",
    )
    rows: list[dict[str, Any]] = []
    rows.extend(fetch_rows_by_ids(conn, "outcomes", "signal_id", signal_ids, wanted, warnings))
    rows.extend(fetch_rows_by_ids(conn, "outcomes", "trade_opportunity_id", opportunity_ids, wanted, warnings))
    rows.extend(fetch_rows_by_window(conn, "outcomes", ("created_at", "completed_at", "due_at", "updated_at"), wanted, start_ts, end_ts, warnings))
    return unique_rows(rows, ("outcome_id", "signal_id", "trade_opportunity_id", "window_sec"))


def fetch_related_opportunity_outcomes(
    conn: sqlite3.Connection,
    opportunity_ids: set[str],
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> list[dict[str, Any]]:
    wanted = (
        "trade_opportunity_id",
        "window_sec",
        "opportunity_profile_key",
        "due_at",
        "start_price",
        "end_price",
        "outcome_price_source",
        "price_source",
        "status",
        "failure_reason",
        "completed_at",
        "created_at",
        "updated_at",
    )
    rows: list[dict[str, Any]] = []
    rows.extend(fetch_rows_by_ids(conn, "opportunity_outcomes", "trade_opportunity_id", opportunity_ids, wanted, warnings))
    rows.extend(fetch_rows_by_window(conn, "opportunity_outcomes", ("created_at", "completed_at", "due_at", "updated_at"), wanted, start_ts, end_ts, warnings))
    return unique_rows(rows, ("trade_opportunity_id", "window_sec"))


def fetch_replay_examples(conn: sqlite3.Connection, logical_date: str, warnings: list[str]) -> list[dict[str, Any]]:
    table = "trade_replay_examples"
    cols = columns(conn, table)
    if not cols:
        warnings.append("sqlite_missing_table:trade_replay_examples")
        return []
    if "logical_date" not in cols:
        warnings.append("sqlite_missing_column:trade_replay_examples.logical_date")
        return []
    wanted = (
        "replay_id",
        "logical_date",
        "replay_scope",
        "signal_id",
        "trade_opportunity_id",
        "opportunity_status",
        "asset",
        "pair",
        "signal_stage",
        "lp_stage",
        "profile_key",
        "data_valid",
        "invalid_reason",
        "price_source",
        "created_at",
    )
    select_list = table_select_columns(cols, wanted, "r")
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {select_list} FROM trade_replay_examples r WHERE {ident('logical_date', 'r')} = ?",
            (logical_date,),
        ).fetchall()
    ]


def profile_stats(conn: sqlite3.Connection, logical_date: str, warnings: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "source": "missing",
        "profile_count": 0,
        "max_sample_count": None,
        "confidence_distribution": Counter(),
    }
    cols = columns(conn, "trade_replay_profile_daily_stats")
    if cols and "logical_date" in cols:
        sample_col = "valid_sample_count" if "valid_sample_count" in cols else "sample_count" if "sample_count" in cols else ""
        confidence_col = "confidence_level" if "confidence_level" in cols else ""
        if sample_col:
            select_conf = ident(confidence_col) if confidence_col else "''"
            rows = conn.execute(
                f"SELECT {ident(sample_col)} AS sample_count, {select_conf} AS confidence_level "
                'FROM trade_replay_profile_daily_stats WHERE "logical_date" = ?',
                (logical_date,),
            ).fetchall()
            samples = [safe_int(row["sample_count"]) for row in rows]
            result["source"] = "trade_replay_profile_daily_stats"
            result["profile_count"] = len(rows)
            result["max_sample_count"] = max(samples) if samples else None
            result["confidence_distribution"] = Counter(normalize_small(row["confidence_level"]) for row in rows)
            return result
    cols = columns(conn, "quality_stats")
    if cols and {"scope_type", "stage"}.issubset(cols):
        sample_col = "sample_count" if "sample_count" in cols else ""
        if sample_col:
            rows = conn.execute(
                f"SELECT {ident(sample_col)} AS sample_count FROM quality_stats "
                "WHERE scope_type='opportunity_profile' AND stage='all'"
            ).fetchall()
            samples = [safe_int(row["sample_count"]) for row in rows]
            result["source"] = "quality_stats"
            result["profile_count"] = len(rows)
            result["max_sample_count"] = max(samples) if samples else None
            return result
    warnings.append("sqlite_missing_profile_stats")
    return result


def read_daily_report(logical_date: str, daily_dir: Path) -> tuple[dict[str, Any], str, list[str]]:
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


def dict_value(payload: dict[str, Any], *path: str) -> Any:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def report_field_status(payload: dict[str, Any]) -> str:
    fields = (
        "run_overview",
        "data_source_summary",
        "trade_opportunity_summary",
        "outcome_summary",
        "outcome_source_summary",
        "trade_replay_summary",
        "trade_replay_profile_summary",
    )
    return "；".join(f"{field}={'present' if isinstance(payload.get(field), dict) else 'missing'}" for field in fields)


def report_mapping_diagnostics(
    payload: dict[str, Any],
    report_status: str,
    *,
    signals_total: int,
    opportunities_total: int,
    outcome_rows: int,
    opportunity_outcome_rows: int,
    replay_rows: int,
) -> list[str]:
    diagnostics: list[str] = []
    if report_status != "present":
        diagnostics.append("daily_report_missing_or_unreadable")
        return diagnostics
    report_signals = safe_int(dict_value(payload, "run_overview", "total_signal_rows"))
    report_opps = safe_int(dict_value(payload, "trade_opportunity_summary", "window_record_count"))
    report_replay = safe_int(dict_value(payload, "trade_replay_summary", "replay_count"))
    outcome_60s_rate = dict_value(payload, "outcome_source_summary", "outcome_60s_completed_rate")
    if report_signals and signals_total and report_signals != signals_total:
        diagnostics.append(f"signals_count_mismatch:report={report_signals}:sqlite={signals_total}")
    if report_opps and opportunities_total and report_opps != opportunities_total:
        diagnostics.append(f"opportunities_count_mismatch:report={report_opps}:sqlite={opportunities_total}")
    if report_replay and replay_rows and report_replay != replay_rows:
        diagnostics.append(f"replay_count_mismatch:report={report_replay}:sqlite={replay_rows}")
    if (outcome_rows or opportunity_outcome_rows) and outcome_60s_rate is None:
        diagnostics.append("daily_report_missing_outcome_60s_completed_rate")
    return diagnostics


def completion_counter(rows: Iterable[dict[str, Any]]) -> tuple[int, Counter[str]]:
    statuses: Counter[str] = Counter()
    completed = 0
    for row in rows:
        status = normalize_small(row.get("status"))
        statuses[status] += 1
        if status == "completed":
            completed += 1
    return completed, statuses


def due_counters(rows: Iterable[dict[str, Any]], now_ts: float) -> tuple[int, int, int]:
    future_pending = 0
    past_pending = 0
    missing_due_pending = 0
    for row in rows:
        status = normalize_small(row.get("status"))
        if status not in {"pending", "scheduled", "registered", "missing"}:
            continue
        due_at = safe_float(row.get("due_at"))
        if due_at is None:
            missing_due_pending += 1
        elif due_at > now_ts:
            future_pending += 1
        else:
            past_pending += 1
    return future_pending, past_pending, missing_due_pending


def price_failure_count(rows: Iterable[dict[str, Any]]) -> tuple[int, Counter[str]]:
    count = 0
    reasons: Counter[str] = Counter()
    for row in rows:
        reason = str(row.get("failure_reason") or row.get("invalid_reason") or "").strip()
        price_source = first_text(row.get("outcome_price_source"), row.get("price_source"))
        start_price = safe_float(row.get("start_price"))
        end_price = safe_float(row.get("end_price"))
        status = normalize_small(row.get("status"))
        reason_text = reason.lower()
        is_price_failure = any(term in reason_text for term in PRICE_FAILURE_TERMS)
        if status in {"failed", "expired", "error"} and (start_price is None or end_price is None):
            is_price_failure = True
        if status == "completed" and (start_price is None or end_price is None or not price_source):
            is_price_failure = True
        if is_price_failure:
            count += 1
            reasons[redact_text(reason or "price_snapshot_missing", limit=50)] += 1
    return count, reasons


def group_missing(rows: Iterable[dict[str, Any]], matched_ids: set[str], id_field: str) -> dict[str, Counter[str]]:
    counters = {
        "asset": Counter(),
        "status": Counter(),
        "signal_type": Counter(),
        "stage": Counter(),
    }
    for row in rows:
        row_id = str(row.get(id_field) or "").strip()
        if row_id and row_id in matched_ids:
            continue
        counters["asset"][redact_text(row.get("asset"), limit=32)] += 1
        counters["status"][normalize_status(row.get("status"))] += 1
        counters["signal_type"][redact_text(row.get("signal_type"), limit=50)] += 1
        counters["stage"][redact_text(row.get("stage"), limit=50)] += 1
    return counters


def open_ro_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def run(logical_date: str, db_path: Path, daily_dir: Path) -> int:
    start_ts, end_ts = logical_window(logical_date)
    payload, report_status, report_warnings = read_daily_report(logical_date, daily_dir)
    warnings = list(report_warnings)

    signals: list[dict[str, Any]] = []
    opportunities: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    opportunity_outcomes: list[dict[str, Any]] = []
    replay_examples: list[dict[str, Any]] = []
    profile = {"source": "missing", "profile_count": 0, "max_sample_count": None, "confidence_distribution": Counter()}

    if not db_path.exists():
        warnings.append(f"sqlite_missing:{db_path.as_posix()}")
    else:
        try:
            conn = open_ro_db(db_path)
        except sqlite3.Error as exc:
            warnings.append(f"sqlite_open_failed:{exc.__class__.__name__}")
        else:
            try:
                signals = fetch_daily_signals(conn, start_ts, end_ts, warnings)
                opportunities = fetch_daily_opportunities(conn, start_ts, end_ts, warnings)
                signal_ids = {row["signal_id"] for row in signals if row.get("signal_id")}
                opportunity_ids = {row["trade_opportunity_id"] for row in opportunities if row.get("trade_opportunity_id")}
                outcomes = fetch_related_outcomes(conn, signal_ids, opportunity_ids, start_ts, end_ts, warnings)
                opportunity_outcomes = fetch_related_opportunity_outcomes(conn, opportunity_ids, start_ts, end_ts, warnings)
                replay_examples = fetch_replay_examples(conn, logical_date, warnings)
                profile = profile_stats(conn, logical_date, warnings)
            except sqlite3.Error as exc:
                warnings.append(f"sqlite_query_failed:{exc.__class__.__name__}")
            finally:
                conn.close()

    signals_total = len(signals)
    opportunities_total = len(opportunities)
    signal_ids = {row["signal_id"] for row in signals if row.get("signal_id")}
    opportunity_ids = {row["trade_opportunity_id"] for row in opportunities if row.get("trade_opportunity_id")}
    matched_signal_ids = {str(row.get("signal_id") or "").strip() for row in outcomes if str(row.get("signal_id") or "").strip() in signal_ids}
    matched_opportunity_outcome_ids = {
        str(row.get("trade_opportunity_id") or "").strip()
        for row in opportunity_outcomes
        if str(row.get("trade_opportunity_id") or "").strip() in opportunity_ids
    }
    replay_opportunity_ids = {
        str(row.get("trade_opportunity_id") or "").strip()
        for row in replay_examples
        if str(row.get("trade_opportunity_id") or "").strip() in opportunity_ids
    }

    outcome_completed, outcome_statuses = completion_counter(outcomes)
    opportunity_outcome_completed, opportunity_outcome_statuses = completion_counter(opportunity_outcomes)
    now_ts = safe_float(os.environ.get("HERMES_OUTCOME_DIAGNOSE_NOW_TS")) or time.time()
    outcome_future_pending, outcome_past_pending, outcome_missing_due = due_counters(outcomes, now_ts)
    opp_future_pending, opp_past_pending, opp_missing_due = due_counters(opportunity_outcomes, now_ts)
    outcome_price_failures, outcome_price_reasons = price_failure_count(outcomes)
    opp_price_failures, opp_price_reasons = price_failure_count(opportunity_outcomes)
    replay_price_failures, replay_price_reasons = price_failure_count(replay_examples)
    unlinked_outcomes = sum(1 for row in outcomes if not first_text(row.get("signal_id"), row.get("trade_opportunity_id")))
    unlinked_opp_outcomes = sum(1 for row in opportunity_outcomes if not first_text(row.get("trade_opportunity_id")))
    replay_without_opp = sum(1 for row in replay_examples if not first_text(row.get("trade_opportunity_id")))
    signal_missing = group_missing(signals, matched_signal_ids, "signal_id")
    opportunity_missing = group_missing(opportunities, matched_opportunity_outcome_ids, "trade_opportunity_id")
    mapping_diagnostics = report_mapping_diagnostics(
        payload,
        report_status,
        signals_total=signals_total,
        opportunities_total=opportunities_total,
        outcome_rows=len(outcomes),
        opportunity_outcome_rows=len(opportunity_outcomes),
        replay_rows=len(replay_examples),
    )

    report_rates = {
        "outcome_60s_completed_rate": dict_value(payload, "outcome_source_summary", "outcome_60s_completed_rate"),
        "candidate_outcome_completion_rate": dict_value(payload, "trade_opportunity_summary", "candidate_outcome_completion_rate"),
        "verified_outcome_completion_rate": dict_value(payload, "trade_opportunity_summary", "verified_outcome_completion_rate"),
        "candidate_tradeable_completed_rate": dict_value(payload, "candidate_tradeable_summary", "candidate_outcome_completed_rate"),
        "replay_count": dict_value(payload, "trade_replay_summary", "replay_count"),
    }
    replay_scope_counts = Counter(normalize_small(row.get("replay_scope")) for row in replay_examples)
    opportunity_status_counter = Counter(row.get("status") or "NONE" for row in opportunities)

    print(f"Chain Monitor Outcome闭环诊断｜{logical_date}")
    print("policy=只读 SQLite/daily_report；未修改 DB；未执行外部生成命令。")
    print(f"daily_report_status={report_status}")
    if payload:
        print(f"daily_report字段存在性={report_field_status(payload)}")
    print(f"signals_total={signals_total}")
    print(f"trade_opportunities_total={opportunities_total}")
    print(f"outcomes_total={len(outcomes)} completed={outcome_completed} completed_rate={format_rate(outcome_completed, len(outcomes))}")
    print(
        "opportunity_outcomes_total="
        f"{len(opportunity_outcomes)} completed={opportunity_outcome_completed} "
        f"completed_rate={format_rate(opportunity_outcome_completed, len(opportunity_outcomes))}"
    )
    print(f"trade_replay_examples_total={len(replay_examples)} replay_scope_distribution={format_counter(replay_scope_counts)}")
    print(f"signals_to_outcomes_match_rate={format_rate(len(matched_signal_ids), signals_total)}")
    print(f"opportunities_to_opportunity_outcomes_match_rate={format_rate(len(matched_opportunity_outcome_ids), opportunities_total)}")
    print(f"opportunities_to_replay_examples_match_rate={format_rate(len(replay_opportunity_ids), opportunities_total)}")
    print(f"trade_opportunity_status_distribution={format_counter(opportunity_status_counter)}")
    print(f"outcomes_status_distribution={format_counter(outcome_statuses)}")
    print(f"opportunity_outcomes_status_distribution={format_counter(opportunity_outcome_statuses)}")
    print(
        "daily_report_rates="
        + "；".join(f"{key}={format_number(value)}" for key, value in report_rates.items())
    )
    print(
        "profile_sample_summary="
        f"source={profile.get('source')} profile_count={profile.get('profile_count')} "
        f"max_sample_count={format_number(profile.get('max_sample_count'))} "
        f"confidence_distribution={format_counter(profile.get('confidence_distribution') or Counter())}"
    )

    print("signals_outcome_missing_by_asset=" + format_counter(signal_missing["asset"]))
    print("signals_outcome_missing_by_status=" + format_counter(signal_missing["status"]))
    print("signals_outcome_missing_by_signal_type=" + format_counter(signal_missing["signal_type"]))
    print("signals_outcome_missing_by_stage=" + format_counter(signal_missing["stage"]))
    print("opportunities_outcome_missing_by_asset=" + format_counter(opportunity_missing["asset"]))
    print("opportunities_outcome_missing_by_status=" + format_counter(opportunity_missing["status"]))
    print("opportunities_outcome_missing_by_signal_type=" + format_counter(opportunity_missing["signal_type"]))
    print("opportunities_outcome_missing_by_stage=" + format_counter(opportunity_missing["stage"]))

    time_window_possible = outcome_future_pending + opp_future_pending
    worker_stall_possible = (signals_total > 0 and len(outcomes) == 0) or (
        opportunities_total > 0 and len(opportunity_outcomes) == 0
    ) or (outcome_past_pending + opp_past_pending > 0)
    id_link_possible = unlinked_outcomes + unlinked_opp_outcomes + replay_without_opp
    price_possible = outcome_price_failures + opp_price_failures + replay_price_failures
    mapping_possible = bool(mapping_diagnostics)

    print("outcome缺失原因推断:")
    print(
        "- 时间窗口未到="
        f"{'可能' if time_window_possible else '不明显'} "
        f"pending_due_in_future={time_window_possible} pending_missing_due={outcome_missing_due + opp_missing_due}"
    )
    print(
        "- price snapshot 缺失="
        f"{'可能' if price_possible else '不明显'} "
        f"affected_rows={price_possible} reasons={format_counter(outcome_price_reasons + opp_price_reasons + replay_price_reasons)}"
    )
    print(
        "- signal_id / opportunity_id 未关联="
        f"{'可能' if id_link_possible or (len(outcomes) > 0 and not matched_signal_ids and signals_total > 0) else '不明显'} "
        f"unlinked_outcomes={unlinked_outcomes} unlinked_opportunity_outcomes={unlinked_opp_outcomes} replay_without_opportunity_id={replay_without_opp}"
    )
    print(
        "- outcome worker 未运行="
        f"{'可能' if worker_stall_possible else '不明显'} "
        f"past_due_pending={outcome_past_pending + opp_past_pending} outcomes_rows={len(outcomes)} opportunity_outcomes_rows={len(opportunity_outcomes)}"
    )
    print(
        "- report 字段映射错误="
        f"{'可能' if mapping_possible else '不明显'} "
        f"diagnostics={';'.join(mapping_diagnostics) if mapping_diagnostics else 'none'}"
    )

    recommendations: list[str] = []
    if worker_stall_possible:
        recommendations.append("优先检查 outcome scheduler / catchup 是否持续结算到 SQLite。")
    if price_possible:
        recommendations.append("核对 market context 与 price snapshot 覆盖，重点看 failure_reason 聚合。")
    if id_link_possible:
        recommendations.append("检查 signal_id 与 trade_opportunity_id 在 signals、outcomes、replay 的传递。")
    elif len(matched_signal_ids) < signals_total:
        recommendations.append("复核未匹配 signals 是否属于 observe/suppressed 类非 outcome 注册口径。")
    if mapping_possible:
        recommendations.append("对齐 daily_report outcome 字段映射，避免 SQLite 有数据但日报显示缺失。")
    if not recommendations:
        recommendations.append("闭环链路未见明显断点，继续观察后续逻辑日样本累积。")
    print("下一步建议:")
    for idx, item in enumerate(recommendations[:5], start=1):
        print(f"{idx}. {item}")

    if warnings:
        print("limitations=" + "；".join(dict.fromkeys(warnings)))
    print("说明=只读脱敏诊断，不含执行指令。")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose outcome/replay/profile loop coverage for a logical date.")
    parser.add_argument("--date", required=True, help="Beijing logical date YYYY-MM-DD")
    parser.add_argument("--db-path", default=os.environ.get("HERMES_OUTCOME_DIAGNOSE_DB_PATH", DEFAULT_DB_PATH))
    parser.add_argument("--daily-dir", default=os.environ.get("HERMES_OUTCOME_DIAGNOSE_DAILY_DIR", DEFAULT_DAILY_DIR))
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        logical_date = parse_date(args.date)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    daily_dir = Path(args.daily_dir)
    if not daily_dir.is_absolute():
        daily_dir = Path.cwd() / daily_dir
    return run(logical_date, db_path, daily_dir)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
