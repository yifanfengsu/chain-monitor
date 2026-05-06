#!/usr/bin/env python3
"""Read-only LP signal diagnostics for Hermes chain-monitor Telegram ops."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any


LP_TERMS = ("lp", "pool", "liquidity", "clmm")
REPORT_FIELDS = (
    "lp_signal_summary",
    "lp_signals",
    "lp_stage_summary",
    "clmm_summary",
    "lp_suppression_summary",
    "lp_suppression_replay_summary",
    "major_coverage_summary",
)
LP_DETAIL_REPORT_FIELDS = (
    "lp_signal_summary",
    "lp_signals",
    "lp_stage_summary",
    "clmm_summary",
    "lp_suppression_summary",
)
MAJOR_PAIRS = (
    "ETH/USDT",
    "ETH/USDC",
    "BTC/USDT",
    "BTC/USDC",
    "SOL/USDT",
    "SOL/USDC",
)
BJ_TZ = dt.timezone(dt.timedelta(hours=8))


def validate_date(value: str) -> str:
    if len(value) != 10:
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


def safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def colref(column: str, alias: str = "") -> str:
    return f"{alias}.{column}" if alias else column


def time_condition(
    cols: set[str],
    candidates: tuple[str, ...],
    start_ts: int,
    end_ts: int,
    *,
    alias: str = "",
) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []
    for column in candidates:
        if column not in cols:
            continue
        ref = colref(column, alias)
        parts.append(f"(({ref} >= ? AND {ref} <= ?) OR ({ref} >= ? AND {ref} <= ?))")
        params.extend([start_ts, end_ts, start_ts * 1000, end_ts * 1000])
    if not parts:
        return "", []
    return "(" + " OR ".join(parts) + ")", params


def text_like_predicate(
    cols: set[str],
    candidates: tuple[str, ...],
    terms: tuple[str, ...] = LP_TERMS,
    *,
    alias: str = "",
) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []
    for column in candidates:
        if column not in cols:
            continue
        ref = colref(column, alias)
        for term in terms:
            parts.append(f"LOWER(COALESCE(CAST({ref} AS TEXT), '')) LIKE ?")
            params.append(f"%{term}%")
    if not parts:
        return "", []
    return "(" + " OR ".join(parts) + ")", params


def nonempty_predicate(cols: set[str], candidates: tuple[str, ...], *, alias: str = "") -> str:
    parts = []
    for column in candidates:
        if column in cols:
            ref = colref(column, alias)
            parts.append(f"TRIM(COALESCE(CAST({ref} AS TEXT), '')) != ''")
    return "(" + " OR ".join(parts) + ")" if parts else ""


def count_query(conn: sqlite3.Connection, sql: str, params: list[Any], warnings: list[str], label: str) -> int:
    try:
        row = conn.execute(sql, params).fetchone()
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_query_failed:{label}:{exc.__class__.__name__}")
        return 0
    return safe_int(row[0] if row else 0)


def count_table(
    conn: sqlite3.Connection,
    table: str,
    time_candidates: tuple[str, ...],
    start_ts: int,
    end_ts: int,
    warnings: list[str],
    *,
    predicate: str = "",
    predicate_params: list[Any] | None = None,
) -> int:
    cols = columns(conn, table)
    if not cols:
        warnings.append(f"sqlite_missing_table:{table}")
        return 0
    time_sql, time_params = time_condition(cols, time_candidates, start_ts, end_ts)
    if not time_sql:
        warnings.append(f"sqlite_missing_time_column:{table}")
        return 0
    sql = f"SELECT COUNT(*) FROM {table} WHERE {time_sql}"
    params = list(time_params)
    if predicate:
        sql += f" AND ({predicate})"
        params.extend(predicate_params or [])
    return count_query(conn, sql, params, warnings, table)


def term_counts(
    conn: sqlite3.Connection,
    table: str,
    time_candidates: tuple[str, ...],
    text_column: str,
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> dict[str, int]:
    cols = columns(conn, table)
    if not cols:
        warnings.append(f"sqlite_missing_table:{table}")
        return {term: 0 for term in LP_TERMS}
    if text_column not in cols:
        warnings.append(f"sqlite_missing_column:{table}.{text_column}")
        return {term: 0 for term in LP_TERMS}
    result: dict[str, int] = {}
    for term in LP_TERMS:
        pred, params = text_like_predicate(cols, (text_column,), (term,))
        result[term] = count_table(
            conn,
            table,
            time_candidates,
            start_ts,
            end_ts,
            warnings,
            predicate=pred,
            predicate_params=params,
        )
    return result


def signals_stats(conn: sqlite3.Connection, start_ts: int, end_ts: int, warnings: list[str]) -> dict[str, Any]:
    table = "signals"
    cols = columns(conn, table)
    json_counts = term_counts(
        conn,
        table,
        ("timestamp", "archive_written_at", "created_at", "updated_at", "notifier_sent_at"),
        "signal_json",
        start_ts,
        end_ts,
        warnings,
    )
    indexed_pred = nonempty_predicate(cols, ("lp_alert_stage", "pool_address"))
    indexed = count_table(
        conn,
        table,
        ("timestamp", "archive_written_at", "created_at", "updated_at", "notifier_sent_at"),
        start_ts,
        end_ts,
        warnings,
        predicate=indexed_pred,
    ) if indexed_pred else 0
    text_pred, text_params = text_like_predicate(
        cols,
        (
            "signal_json",
            "lp_alert_stage",
            "canonical_semantic_key",
            "trade_action_key",
            "asset_market_state_key",
            "pool_address",
            "scan_path",
            "notifier_template",
            "delivery_decision",
        ),
    )
    any_parts = [part for part in (text_pred, indexed_pred) if part]
    any_count = count_table(
        conn,
        table,
        ("timestamp", "archive_written_at", "created_at", "updated_at", "notifier_sent_at"),
        start_ts,
        end_ts,
        warnings,
        predicate=" OR ".join(any_parts),
        predicate_params=text_params,
    ) if any_parts else 0
    total = count_table(
        conn,
        table,
        ("timestamp", "archive_written_at", "created_at", "updated_at", "notifier_sent_at"),
        start_ts,
        end_ts,
        warnings,
    )
    return {
        "total": total,
        "signal_json_counts": json_counts,
        "indexed_lp_rows": indexed,
        "lp_like_any": any_count,
    }


def event_stats(
    conn: sqlite3.Connection,
    table: str,
    time_candidates: tuple[str, ...],
    json_column: str,
    text_columns: tuple[str, ...],
    indexed_columns: tuple[str, ...],
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> dict[str, Any]:
    cols = columns(conn, table)
    json_counts = term_counts(conn, table, time_candidates, json_column, start_ts, end_ts, warnings)
    indexed_pred = nonempty_predicate(cols, indexed_columns)
    indexed_count = count_table(
        conn,
        table,
        time_candidates,
        start_ts,
        end_ts,
        warnings,
        predicate=indexed_pred,
    ) if indexed_pred else 0
    text_pred, text_params = text_like_predicate(cols, (json_column, *text_columns))
    any_parts = [part for part in (text_pred, indexed_pred) if part]
    any_count = count_table(
        conn,
        table,
        time_candidates,
        start_ts,
        end_ts,
        warnings,
        predicate=" OR ".join(any_parts),
        predicate_params=text_params,
    ) if any_parts else 0
    total = count_table(conn, table, time_candidates, start_ts, end_ts, warnings)
    return {
        "total": total,
        "json_counts": json_counts,
        "indexed_lp_rows": indexed_count,
        "lp_like_any": any_count,
    }


def delivery_stats(conn: sqlite3.Connection, start_ts: int, end_ts: int, warnings: list[str]) -> dict[str, Any]:
    audit_cols = columns(conn, "delivery_audit")
    if not audit_cols:
        warnings.append("sqlite_missing_table:delivery_audit")
        return {"lp_like_total": 0, "pushed": 0, "suppressed": 0, "stage_distribution": {}}
    time_sql, time_params = time_condition(
        audit_cols,
        ("notifier_sent_at", "timestamp", "archive_written_at", "created_at", "updated_at"),
        start_ts,
        end_ts,
        alias="da",
    )
    if not time_sql:
        warnings.append("sqlite_missing_time_column:delivery_audit")
        return {"lp_like_total": 0, "pushed": 0, "suppressed": 0, "stage_distribution": {}}

    signal_cols = columns(conn, "signals")
    audit_text, audit_params = text_like_predicate(
        audit_cols,
        (
            "audit_json",
            "stage",
            "gate_reason",
            "reason",
            "suppression_reason",
            "notifier_template",
            "delivery_decision",
            "blocked_reason",
        ),
        alias="da",
    )
    signal_text, signal_params = ("", [])
    signal_indexed = ""
    join_sql = ""
    if signal_cols and "signal_id" in audit_cols and "signal_id" in signal_cols:
        join_sql = " LEFT JOIN signals s ON da.signal_id = s.signal_id"
        signal_text, signal_params = text_like_predicate(
            signal_cols,
            (
                "signal_json",
                "lp_alert_stage",
                "canonical_semantic_key",
                "trade_action_key",
                "asset_market_state_key",
                "pool_address",
                "scan_path",
            ),
            alias="s",
        )
        signal_indexed = nonempty_predicate(signal_cols, ("lp_alert_stage", "pool_address"), alias="s")
    lp_parts = [part for part in (audit_text, signal_text, signal_indexed) if part]
    if not lp_parts:
        warnings.append("sqlite_missing_lp_text_columns:delivery_audit")
        return {"lp_like_total": 0, "pushed": 0, "suppressed": 0, "stage_distribution": {}}
    lp_pred = "(" + " OR ".join(lp_parts) + ")"
    lp_params = audit_params + signal_params

    base = f"FROM delivery_audit da{join_sql} WHERE {time_sql} AND {lp_pred}"
    base_params = time_params + lp_params
    total = count_query(conn, f"SELECT COUNT(*) {base}", base_params, warnings, "delivery_audit_lp")

    sent_parts = []
    if "sent_to_telegram" in audit_cols:
        sent_parts.append("COALESCE(da.sent_to_telegram, 0) = 1")
    if "delivered" in audit_cols:
        sent_parts.append("COALESCE(da.delivered, 0) = 1")
    pushed = count_query(
        conn,
        f"SELECT COUNT(*) {base} AND ({' OR '.join(sent_parts)})" if sent_parts else f"SELECT COUNT(*) {base} AND 0",
        base_params,
        warnings,
        "delivery_audit_lp_pushed",
    )

    suppressed_parts = []
    if "suppressed" in audit_cols:
        suppressed_parts.append("COALESCE(da.suppressed, 0) = 1")
    if "sent_to_telegram" in audit_cols:
        suppressed_parts.append("COALESCE(da.sent_to_telegram, 0) = 0")
    suppress_text, suppress_params = text_like_predicate(
        audit_cols,
        ("suppression_reason", "reason", "gate_reason", "delivery_decision", "blocked_reason"),
        ("suppress", "blocked", "gate", "no_trade", "skip"),
        alias="da",
    )
    if suppress_text:
        suppressed_parts.append(suppress_text)
    suppressed = count_query(
        conn,
        f"SELECT COUNT(*) {base} AND ({' OR '.join(suppressed_parts)})" if suppressed_parts else f"SELECT COUNT(*) {base} AND 0",
        base_params + suppress_params,
        warnings,
        "delivery_audit_lp_suppressed",
    )

    stage_expr = "COALESCE(da.stage, 'missing')" if "stage" in audit_cols else "'missing'"
    decision_expr = "COALESCE(da.delivery_decision, 'missing')" if "delivery_decision" in audit_cols else "'missing'"
    distribution: Counter[str] = Counter()
    try:
        rows = conn.execute(
            f"SELECT {stage_expr} AS stage, {decision_expr} AS decision, COUNT(*) AS count {base} "
            f"GROUP BY {stage_expr}, {decision_expr}",
            base_params,
        ).fetchall()
        for row in rows:
            stage = str(row["stage"] or "missing").strip()[:48] or "missing"
            decision = str(row["decision"] or "missing").strip()[:48] or "missing"
            distribution[f"{stage}/{decision}"] += safe_int(row["count"])
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_query_failed:delivery_audit_lp_distribution:{exc.__class__.__name__}")

    return {
        "lp_like_total": total,
        "pushed": pushed,
        "suppressed": suppressed,
        "stage_distribution": dict(distribution.most_common(12)),
    }


def pair_coverage(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    coverage = as_dict(payload.get("major_coverage_summary")) or as_dict(payload.get("majors_coverage_summary"))
    covered = {str(item).upper() for item in coverage.get("covered_major_pairs") or []}
    missing = {str(item).upper() for item in coverage.get("missing_major_pairs") or []}
    pair_dist = as_dict(coverage.get("pair_distribution"))
    parts: list[str] = []
    for pair in MAJOR_PAIRS:
        count = safe_int(pair_dist.get(pair, 0))
        if pair in covered or count > 0:
            status = f"covered(count={count})"
        elif pair in missing:
            status = "missing"
        elif coverage:
            status = "unknown"
        else:
            status = "unknown:no_major_coverage_summary"
        parts.append(f"{pair}:{status}")
    return "；".join(parts), coverage


def format_json_counts(label: str, counts: dict[str, int]) -> str:
    return "；".join(f"{label} LIKE '%{term}%'={safe_int(counts.get(term))}" for term in LP_TERMS)


def format_distribution(value: dict[str, Any]) -> str:
    if not value:
        return "missing"
    return "；".join(f"{key}={safe_int(count)}" for key, count in value.items())


def format_report_counter_rows(rows: Any, key_name: str, count_name: str = "rows") -> str:
    if not isinstance(rows, list) or not rows:
        return "missing"
    parts: list[str] = []
    for item in rows[:8]:
        if not isinstance(item, dict):
            continue
        key = str(item.get(key_name) or item.get("reason") or item.get("pair") or item.get("intent") or "unknown")
        count = safe_int(item.get(count_name, item.get("count", item.get("rows", 0))))
        parts.append(f"{key}={count}")
    return "；".join(parts) if parts else "missing"


def format_suppression_replay(rows: Any) -> str:
    if not isinstance(rows, list) or not rows:
        return "missing"
    parts: list[str] = []
    for item in rows[:8]:
        if not isinstance(item, dict):
            continue
        parts.append(
            f"{item.get('reason', 'unknown')}:count={safe_int(item.get('count'))},"
            f"replay={safe_int(item.get('replay_count'))},"
            f"avg={item.get('avg_net_pnl_bps', 'missing')},"
            f"action={item.get('recommended_action', 'missing')}"
        )
    return "；".join(parts) if parts else "missing"


def run(logical_date: str, db_path: Path, daily_dir: Path) -> int:
    start_ts, end_ts = logical_window(logical_date)
    payload, report_status, warnings = read_daily_report(logical_date, daily_dir)
    run_overview = as_dict(payload.get("run_overview"))
    quality = as_dict(payload.get("data_quality_summary"))
    report_fields = {field: ("present" if field in payload else "missing") for field in REPORT_FIELDS}
    lp_signal_summary = as_dict(payload.get("lp_signal_summary"))
    lp_stage_summary = as_dict(payload.get("lp_stage_summary"))
    clmm_summary = as_dict(payload.get("clmm_summary"))
    lp_report_suppression = as_dict(payload.get("lp_suppression_summary"))
    lp_suppression_replay = as_dict(payload.get("lp_suppression_replay_summary"))
    lp_signal_rows = payload.get("lp_signal_rows", run_overview.get("lp_signal_rows"))
    delivered_lp = payload.get("delivered_lp_signals", run_overview.get("delivered_lp_signals"))
    suppressed_lp = payload.get("suppressed_lp_signals", run_overview.get("suppressed_lp_signals"))
    coverage_line, coverage = pair_coverage(payload)

    signals = {"total": 0, "signal_json_counts": {term: 0 for term in LP_TERMS}, "indexed_lp_rows": 0, "lp_like_any": 0}
    raw_events = {"total": 0, "json_counts": {term: 0 for term in LP_TERMS}, "indexed_lp_rows": 0, "lp_like_any": 0}
    parsed_events = {"total": 0, "json_counts": {term: 0 for term in LP_TERMS}, "indexed_lp_rows": 0, "lp_like_any": 0}
    delivery = {"lp_like_total": 0, "pushed": 0, "suppressed": 0, "stage_distribution": {}}

    if not db_path.exists():
        warnings.append("sqlite_missing:data/chain_monitor.sqlite")
    else:
        try:
            conn = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            warnings.append(f"sqlite_open_failed:{exc.__class__.__name__}")
        else:
            try:
                signals = signals_stats(conn, start_ts, end_ts, warnings)
                raw_events = event_stats(
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
                parsed_events = event_stats(
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
                delivery = delivery_stats(conn, start_ts, end_ts, warnings)
            finally:
                conn.close()

    signal_any = safe_int(signals.get("lp_like_any"))
    raw_parsed_any = safe_int(raw_events.get("lp_like_any")) + safe_int(parsed_events.get("lp_like_any"))
    delivery_total = safe_int(delivery.get("lp_like_total"))
    detail_missing = [field for field in LP_DETAIL_REPORT_FIELDS if report_fields[field] == "missing"]

    report_missing_lp_counts = lp_signal_rows in (None, "", "missing", 0, "0")
    mapping_reasons: list[str] = []
    if len(detail_missing) == len(LP_DETAIL_REPORT_FIELDS):
        mapping_reasons.append("daily_report 缺少 LP 明细字段")
    elif detail_missing:
        mapping_reasons.append("daily_report LP 明细字段部分缺失:" + ",".join(detail_missing))
    if report_missing_lp_counts:
        mapping_reasons.append("daily_report lp_signal_rows 为 0 或缺失")
    mapping_problem = signal_any > 0 and bool(mapping_reasons)

    print(f"Chain Monitor LP诊断｜{logical_date}")
    print(f"date={logical_date}")
    print(f"daily_report_status={report_status}")
    print("daily_report字段存在性=" + "；".join(f"{field}={status}" for field, status in report_fields.items()))
    print(
        "daily_report_LP计数="
        f"run_overview.lp_signal_rows={lp_signal_rows if lp_signal_rows is not None else 'missing'} "
        f"delivered_lp_signals={delivered_lp if delivered_lp is not None else 'missing'} "
        f"suppressed_lp_signals={suppressed_lp if suppressed_lp is not None else 'missing'}"
    )
    if lp_signal_summary:
        print(
            "lp_signal_summary="
            f"available={lp_signal_summary.get('available', 'missing')} "
            f"lp_signal_rows={lp_signal_summary.get('lp_signal_rows', 'missing')} "
            f"delivered={lp_signal_summary.get('delivered_count', 'missing')} "
            f"suppressed={lp_signal_summary.get('suppressed_count', 'missing')} "
            f"suppression_rate={lp_signal_summary.get('suppression_rate', 'missing')} "
            f"sqlite_lp_like_signals={lp_signal_summary.get('lp_like_signals_sqlite', 'missing')} "
            f"top_pairs={format_report_counter_rows(lp_signal_summary.get('top_pairs'), 'pair')} "
            f"top_suppression_reasons={format_report_counter_rows(lp_signal_summary.get('top_suppression_reasons'), 'reason', 'count')}"
        )
    if lp_stage_summary:
        print(
            "lp_stage_summary="
            f"available={lp_stage_summary.get('available', 'missing')} "
            f"by_stage={json.dumps(lp_stage_summary.get('by_stage', {}), ensure_ascii=False, sort_keys=True)} "
            f"unknown_rate={lp_stage_summary.get('unknown_rate', 'missing')}"
        )
    if clmm_summary:
        print(
            "clmm_summary="
            f"available={clmm_summary.get('available', 'missing')} "
            f"clmm_like_rows={clmm_summary.get('clmm_like_rows', 'missing')} "
            f"position_events={clmm_summary.get('position_events', 'missing')} "
            f"increase_liquidity={clmm_summary.get('increase_liquidity', 'missing')} "
            f"decrease_liquidity={clmm_summary.get('decrease_liquidity', 'missing')} "
            f"collect={clmm_summary.get('collect', 'missing')}"
        )
    if lp_report_suppression:
        print(
            "lp_suppression_summary="
            f"available={lp_report_suppression.get('available', 'missing')} "
            f"total={lp_report_suppression.get('total', 'missing')} "
            f"delivered={lp_report_suppression.get('delivered', 'missing')} "
            f"suppressed={lp_report_suppression.get('suppressed', 'missing')} "
            f"suppression_rate={lp_report_suppression.get('suppression_rate', 'missing')} "
            f"by_reason={format_distribution(as_dict(lp_report_suppression.get('by_reason')))}"
        )
    if lp_suppression_replay:
        print(
            "lp_suppression_replay_summary="
            f"available={lp_suppression_replay.get('available', 'missing')} "
            f"overall_suppressed_avg_net_pnl_bps={lp_suppression_replay.get('overall_suppressed_avg_net_pnl_bps', 'missing')} "
            f"diagnosis={lp_suppression_replay.get('diagnosis', 'missing')} "
            f"by_reason={format_suppression_replay(lp_suppression_replay.get('by_reason'))}"
        )
    print(
        "data_quality="
        f"status={quality.get('data_quality_status', 'missing')} "
        f"zero_activity_day={quality.get('zero_activity_day', 'missing')}"
    )
    print(
        "SQLite LP-like计数 signals: "
        f"total={safe_int(signals.get('total'))}；"
        f"{format_json_counts('signal_json', as_dict(signals.get('signal_json_counts')))}；"
        f"indexed_lp_rows={safe_int(signals.get('indexed_lp_rows'))}；"
        f"lp_like_any_signals={signal_any}"
    )
    print(
        "SQLite LP-like计数 raw_events: "
        f"total={safe_int(raw_events.get('total'))}；"
        f"{format_json_counts('raw_json', as_dict(raw_events.get('json_counts')))}；"
        f"indexed_pool_rows={safe_int(raw_events.get('indexed_lp_rows'))}；"
        f"lp_like_any_raw_events={safe_int(raw_events.get('lp_like_any'))}"
    )
    print(
        "SQLite LP-like计数 parsed_events: "
        f"total={safe_int(parsed_events.get('total'))}；"
        f"{format_json_counts('parsed_json', as_dict(parsed_events.get('json_counts')))}；"
        f"indexed_lp_rows={safe_int(parsed_events.get('indexed_lp_rows'))}；"
        f"lp_like_any_parsed_events={safe_int(parsed_events.get('lp_like_any'))}"
    )
    print(
        "delivery_audit LP相关推送抑制="
        f"lp_like_total={delivery_total} "
        f"pushed={safe_int(delivery.get('pushed'))} "
        f"suppressed={safe_int(delivery.get('suppressed'))} "
        f"stage_decision_distribution={format_distribution(as_dict(delivery.get('stage_distribution')))}"
    )
    print("major coverage ETH/BTC/SOL x USDT/USDC=" + coverage_line)
    if coverage:
        print(
            "major_coverage_summary="
            f"covered_major_pairs={len(coverage.get('covered_major_pairs') or [])} "
            f"missing_major_pairs={len(coverage.get('missing_major_pairs') or [])} "
            f"current_sample_still_eth_only={coverage.get('current_sample_still_eth_only', 'missing')}"
        )
    else:
        print("major_coverage_summary=missing")

    if mapping_problem:
        print("report_mapping判断=可能存在 report mapping 问题：SQLite signals 有 LP-like 样本；" + "；".join(mapping_reasons) + "。")
    elif signal_any > 0 and lp_signal_summary:
        print("report_mapping判断=LP 数据存在，daily_report LP mapping 正常。")
    elif signal_any > 0:
        print("report_mapping判断=SQLite signals 有 LP-like 样本；daily_report LP 映射未发现明显断点。")
    else:
        print("report_mapping判断=不适用：SQLite signals 未发现 LP-like 样本。")

    if raw_parsed_any > 0 and signal_any == 0:
        print("analyzer_gate判断=raw/parsed 有 LP-like 样本但 signals 为 0，优先排查 LP analyzer / gate。")
    elif signal_any > 0 and delivery_total == 0:
        print("analyzer_gate判断=signals 有 LP-like 样本但 delivery_audit 无 LP 记录，排查 delivery audit mapping 或推送 gate。")
    elif safe_int(delivery.get("suppressed")) > 0:
        print("analyzer_gate判断=delivery_audit 显示 LP 相关抑制，需结合 gate_reason/suppression_reason 聚合继续排查。")
    else:
        print("analyzer_gate判断=未发现 raw/parsed -> signals 或 delivery gate 的明显断点。")

    if signal_any == 0 and raw_parsed_any == 0 and delivery_total == 0:
        print("样本判断=全部为 0：当天无可用 LP 样本或扫描覆盖不足。")
    else:
        print("样本判断=存在 LP-like 聚合样本；按上方 report/analyzer/gate 判断定位。")
    if lp_suppression_replay:
        diagnosis = str(lp_suppression_replay.get("diagnosis") or "missing")
        if diagnosis == "possible_over_suppression":
            print("后验判断=存在可能过度抑制的 reason；只建议复核阈值，不自动放宽 gate。")
        elif diagnosis == "suppression_seems_correct":
            print("后验判断=suppression 后验未显示明显误杀；不建议直接放松 gate。")
        else:
            print("后验判断=suppression replay 样本不足；继续积累，不建议直接放松 gate。")
    if warnings:
        print("limitations=" + "；".join(dict.fromkeys(warnings)))
    print("说明=只读脱敏诊断，不含执行指令。")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose date-scoped LP signal coverage.")
    parser.add_argument("--date", required=True, help="Beijing logical date YYYY-MM-DD")
    parser.add_argument("--db-path", default=os.environ.get("HERMES_LP_DIAGNOSE_DB_PATH", "data/chain_monitor.sqlite"))
    parser.add_argument("--daily-dir", default=os.environ.get("HERMES_LP_DIAGNOSE_DAILY_DIR", "reports/daily"))
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
