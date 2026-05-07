from __future__ import annotations

import datetime as dt
import json
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any


BJ_TZ = dt.timezone(dt.timedelta(hours=8))
REQUIRED_OPPORTUNITY_OUTCOME_COLUMNS = {
    "evaluated_at": "REAL",
    "outcome_ts": "REAL",
    "net_pnl_bps": "REAL",
    "source": "TEXT",
    "catchup_reason": "TEXT",
}


def logical_window(logical_date: str) -> tuple[int, int]:
    parsed = dt.date.fromisoformat(logical_date)
    start = dt.datetime(parsed.year, parsed.month, parsed.day, tzinfo=BJ_TZ)
    start_ts = int(start.timestamp())
    return start_ts, start_ts + 24 * 3600 - 1


def coerce_epoch(value: Any) -> float | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed > 10_000_000_000:
        return parsed / 1000.0
    return parsed


def coerce_float(value: Any) -> float | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_int(value: Any) -> int | None:
    parsed = coerce_float(value)
    return None if parsed is None else int(parsed)


def truthy(value: Any, *, default: bool = True) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "ok", "valid", "completed"}


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def ensure_opportunity_outcome_columns(conn: sqlite3.Connection) -> None:
    existing = table_columns(conn, "opportunity_outcomes")
    for column, column_type in REQUIRED_OPPORTUNITY_OUTCOME_COLUMNS.items():
        if column not in existing:
            conn.execute(f'ALTER TABLE opportunity_outcomes ADD COLUMN "{column}" {column_type}')
            existing.add(column)


def time_filter_sql(alias: str, columns: set[str], candidates: tuple[str, ...]) -> str:
    exprs: list[str] = []
    for column in candidates:
        if column not in columns:
            continue
        ref = f'{alias}."{column}"'
        exprs.append(f"(CASE WHEN CAST({ref} AS REAL) > 10000000000 THEN CAST({ref} AS REAL) / 1000.0 ELSE CAST({ref} AS REAL) END)")
    if not exprs:
        return ""
    if len(exprs) == 1:
        return exprs[0]
    return "COALESCE(" + ", ".join(exprs) + ")"


def _row_dict(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return dict(row)


def fetch_pending_rows(conn: sqlite3.Connection, logical_date: str) -> list[dict[str, Any]]:
    start_ts, end_ts = logical_window(logical_date)
    if not table_exists(conn, "opportunity_outcomes"):
        return []
    oo_cols = table_columns(conn, "opportunity_outcomes")
    opp_cols = table_columns(conn, "trade_opportunities")
    oo_expr = time_filter_sql("oo", oo_cols, ("created_at", "updated_at", "due_at", "completed_at"))
    conditions: list[str] = []
    params: list[Any] = []
    if oo_expr:
        conditions.append(f"({oo_expr} >= ? AND {oo_expr} <= ?)")
        params.extend([start_ts, end_ts])
    if opp_cols:
        opp_expr = time_filter_sql("t", opp_cols, ("created_at", "updated_at"))
        if opp_expr:
            conditions.append(f"({opp_expr} >= ? AND {opp_expr} <= ?)")
            params.extend([start_ts, end_ts])
    date_filter = " OR ".join(conditions) if conditions else "1=1"
    join = ""
    select_extra = "'' AS joined_signal_id, NULL AS opportunity_created_at"
    if table_exists(conn, "trade_opportunities"):
        join = 'LEFT JOIN trade_opportunities t ON t.trade_opportunity_id = oo.trade_opportunity_id'
        signal_expr = 't.signal_id' if "signal_id" in opp_cols else "''"
        created_expr = 't.created_at' if "created_at" in opp_cols else "NULL"
        select_extra = f"{signal_expr} AS joined_signal_id, {created_expr} AS opportunity_created_at"
    rows = conn.execute(
        f"""
        SELECT oo.*, {select_extra}
        FROM opportunity_outcomes oo
        {join}
        WHERE LOWER(COALESCE(oo.status, '')) = 'pending'
          AND ({date_filter})
        ORDER BY COALESCE(oo.due_at, oo.created_at), oo.trade_opportunity_id, oo.window_sec
        """,
        tuple(params),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _fetch_replay_rows(conn: sqlite3.Connection, logical_date: str) -> list[dict[str, Any]]:
    if not table_exists(conn, "trade_replay_examples"):
        return []
    cols = table_columns(conn, "trade_replay_examples")
    if "logical_date" not in cols or "trade_opportunity_id" not in cols:
        return []
    where = 'logical_date = ?'
    params: list[Any] = [logical_date]
    if "replay_scope" in cols:
        where += " AND replay_scope = 'full'"
    return [_row_dict(row) for row in conn.execute(f"SELECT * FROM trade_replay_examples WHERE {where}", tuple(params)).fetchall()]


def _fetch_outcome_rows(conn: sqlite3.Connection, pending_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not pending_rows or not table_exists(conn, "outcomes"):
        return []
    cols = table_columns(conn, "outcomes")
    clauses: list[str] = []
    params: list[Any] = []
    opportunity_ids = sorted({str(row.get("trade_opportunity_id") or "").strip() for row in pending_rows if str(row.get("trade_opportunity_id") or "").strip()})
    signal_ids = sorted({str(row.get("joined_signal_id") or "").strip() for row in pending_rows if str(row.get("joined_signal_id") or "").strip()})
    if opportunity_ids and "trade_opportunity_id" in cols:
        placeholders = ",".join("?" for _ in opportunity_ids)
        clauses.append(f"trade_opportunity_id IN ({placeholders})")
        params.extend(opportunity_ids)
    if signal_ids and "signal_id" in cols:
        placeholders = ",".join("?" for _ in signal_ids)
        clauses.append(f"signal_id IN ({placeholders})")
        params.extend(signal_ids)
    if not clauses:
        return []
    return [_row_dict(row) for row in conn.execute(f"SELECT * FROM outcomes WHERE {' OR '.join(clauses)}", tuple(params)).fetchall()]


def _best_replay_by_opportunity(replay_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in replay_rows:
        opportunity_id = str(row.get("trade_opportunity_id") or "").strip()
        if not opportunity_id or coerce_float(row.get("net_pnl_bps")) is None:
            continue
        if not truthy(row.get("data_valid", 1), default=True):
            continue
        result.setdefault(opportunity_id, row)
    return result


def _outcomes_index(outcome_rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int], dict[str, Any]]]:
    by_opportunity: dict[tuple[str, int], dict[str, Any]] = {}
    by_signal: dict[tuple[str, int], dict[str, Any]] = {}
    for row in outcome_rows:
        status = str(row.get("status") or "").strip().lower()
        if status != "completed":
            continue
        window = coerce_int(row.get("window_sec"))
        if window is None:
            continue
        opportunity_id = str(row.get("trade_opportunity_id") or "").strip()
        signal_id = str(row.get("signal_id") or "").strip()
        if opportunity_id:
            by_opportunity.setdefault((opportunity_id, int(window)), row)
        if signal_id:
            by_signal.setdefault((signal_id, int(window)), row)
    return by_opportunity, by_signal


def _label_from_net(value: float | None, fallback: Any = None) -> str:
    label = str(fallback or "").strip()
    if label:
        return label
    if value is None:
        return "neutral"
    if value > 0:
        return "success"
    if value < 0:
        return "adverse"
    return "neutral"


def _completion_from_replay(row: dict[str, Any], now_ts: int) -> dict[str, Any]:
    net = coerce_float(row.get("net_pnl_bps"))
    outcome_ts = coerce_epoch(row.get("exit_ts") or row.get("completed_at") or row.get("created_at")) or float(now_ts)
    entry = coerce_float(row.get("entry_price"))
    exit_price = coerce_float(row.get("exit_price"))
    adjusted = None if net is None else round(net / 10_000.0, 6)
    return {
        "source": "catchup_from_replay",
        "status": "completed",
        "completed_at": int(outcome_ts),
        "evaluated_at": int(now_ts),
        "outcome_ts": int(outcome_ts),
        "net_pnl_bps": net,
        "start_price": entry,
        "end_price": exit_price,
        "direction_adjusted_move": adjusted,
        "raw_move": adjusted,
        "followthrough": 1 if net is not None and net > 0 else 0,
        "adverse": 1 if net is not None and net < 0 else 0,
        "reversal": 1 if net is not None and net < 0 else 0,
        "result_label": _label_from_net(net, row.get("label")),
        "price_source": str(row.get("price_source") or "trade_replay_examples"),
        "outcome_price_source": str(row.get("price_source") or "trade_replay_examples"),
        "end_price_source": str(row.get("price_source") or "trade_replay_examples"),
        "failure_reason": "",
    }


def _completion_from_outcome(row: dict[str, Any], now_ts: int) -> dict[str, Any]:
    adjusted = coerce_float(row.get("direction_adjusted_move"))
    raw_move = coerce_float(row.get("raw_move"))
    net = coerce_float(row.get("net_pnl_bps"))
    if net is None and adjusted is not None:
        net = round(adjusted * 10_000.0, 2)
    outcome_ts = coerce_epoch(row.get("completed_at") or row.get("updated_at") or row.get("due_at")) or float(now_ts)
    return {
        "source": "catchup_from_outcomes",
        "status": "completed",
        "completed_at": int(outcome_ts),
        "evaluated_at": int(now_ts),
        "outcome_ts": int(outcome_ts),
        "net_pnl_bps": net,
        "start_price": coerce_float(row.get("start_price")),
        "end_price": coerce_float(row.get("end_price")),
        "direction_adjusted_move": adjusted,
        "raw_move": raw_move,
        "followthrough": 1 if truthy(row.get("followthrough"), default=False) else 0,
        "adverse": 1 if truthy(row.get("adverse"), default=False) else 0,
        "reversal": 1 if truthy(row.get("reversal"), default=False) else 0,
        "result_label": _label_from_net(net, row.get("result_label")),
        "price_source": str(row.get("price_source") or row.get("outcome_price_source") or "outcomes"),
        "outcome_price_source": str(row.get("outcome_price_source") or row.get("price_source") or "outcomes"),
        "end_price_source": str(row.get("end_price_source") or row.get("outcome_price_source") or row.get("price_source") or "outcomes"),
        "failure_reason": "",
    }


def _set_clause_payload(columns: set[str], payload: dict[str, Any], now_ts: int) -> dict[str, Any]:
    values = dict(payload)
    values["updated_at"] = int(now_ts)
    values["settled_by"] = "opportunity_outcome_catchup"
    values["catchup"] = 1
    values["catchup_reason"] = ""
    return {key: value for key, value in values.items() if key in columns}


def _update_row(
    conn: sqlite3.Connection,
    columns: set[str],
    row: dict[str, Any],
    payload: dict[str, Any],
    now_ts: int,
) -> None:
    values = _set_clause_payload(columns, payload, now_ts)
    assignments = ", ".join(f'"{key}" = ?' for key in values)
    params = list(values.values())
    params.extend([str(row.get("trade_opportunity_id") or ""), int(coerce_int(row.get("window_sec")) or 0)])
    conn.execute(
        f'UPDATE opportunity_outcomes SET {assignments} WHERE trade_opportunity_id = ? AND window_sec = ?',
        tuple(params),
    )


def _record_unresolved(
    conn: sqlite3.Connection,
    columns: set[str],
    row: dict[str, Any],
    reason: str,
    now_ts: int,
) -> None:
    payload = {
        "failure_reason": reason,
        "catchup_reason": reason,
        "updated_at": int(now_ts),
    }
    values = {key: value for key, value in payload.items() if key in columns}
    if not values:
        return
    assignments = ", ".join(f'"{key}" = ?' for key in values)
    params = list(values.values())
    params.extend([str(row.get("trade_opportunity_id") or ""), int(coerce_int(row.get("window_sec")) or 0)])
    conn.execute(
        f'UPDATE opportunity_outcomes SET {assignments} WHERE trade_opportunity_id = ? AND window_sec = ?',
        tuple(params),
    )


def _find_completion(
    row: dict[str, Any],
    replay_by_opportunity: dict[str, dict[str, Any]],
    outcomes_by_opportunity: dict[tuple[str, int], dict[str, Any]],
    outcomes_by_signal: dict[tuple[str, int], dict[str, Any]],
    now_ts: int,
) -> tuple[dict[str, Any] | None, str]:
    opportunity_id = str(row.get("trade_opportunity_id") or "").strip()
    signal_id = str(row.get("joined_signal_id") or row.get("signal_id") or "").strip()
    window = coerce_int(row.get("window_sec"))
    if not opportunity_id:
        return None, "missing_link_id"
    if window is None:
        return None, "missing_link_id"
    replay_row = replay_by_opportunity.get(opportunity_id)
    if replay_row:
        return _completion_from_replay(replay_row, now_ts), ""
    outcome_row = outcomes_by_opportunity.get((opportunity_id, int(window)))
    if outcome_row:
        return _completion_from_outcome(outcome_row, now_ts), ""
    if signal_id:
        outcome_row = outcomes_by_signal.get((signal_id, int(window)))
        if outcome_row:
            return _completion_from_outcome(outcome_row, now_ts), ""
    return None, "missing_price_snapshot"


def opportunity_outcome_catchup(
    db_path: str | Path,
    logical_date: str,
    *,
    dry_run: bool = True,
    now_ts: int | float | None = None,
) -> dict[str, Any]:
    path = Path(db_path)
    now_value = int(now_ts or time.time())
    result: dict[str, Any] = {
        "logical_date": logical_date,
        "dry_run": bool(dry_run),
        "db_path": str(path),
        "pending_rows": 0,
        "past_due_pending_count": 0,
        "future_pending_count": 0,
        "due_time_parse_error_count": 0,
        "would_update_rows": 0,
        "updated_count": 0,
        "still_pending_count": 0,
        "completed_from_replay": 0,
        "completed_from_outcomes": 0,
        "unresolved_count": 0,
        "unresolved_reason_distribution": {},
        "horizon_distribution": {},
        "source_distribution": {},
        "updates": [],
    }
    if not path.exists():
        result["error"] = "sqlite_missing"
        return result

    try:
        import sqlite_store  # type: ignore
    except ImportError:
        from app import sqlite_store  # type: ignore

    conn = sqlite_store.open_sqlite_connection(path, readonly=dry_run, row_factory=True)
    try:
        if not table_exists(conn, "opportunity_outcomes"):
            result["error"] = "missing_table:opportunity_outcomes"
            return result
        if not dry_run:
            if not sqlite_store.transaction_with_retry(
                conn,
                lambda: ensure_opportunity_outcome_columns(conn),
                op_name="outcome_catchup:ensure_schema",
                table_name="opportunity_outcomes",
            ):
                result["error"] = "ensure_schema_failed"
                return result
        pending_rows = fetch_pending_rows(conn, logical_date)
        result["pending_rows"] = len(pending_rows)
        horizon_counter = Counter(str(coerce_int(row.get("window_sec")) or "missing") for row in pending_rows)
        result["horizon_distribution"] = dict(sorted(horizon_counter.items()))
        replay_by_opportunity = _best_replay_by_opportunity(_fetch_replay_rows(conn, logical_date))
        outcomes_by_opportunity, outcomes_by_signal = _outcomes_index(_fetch_outcome_rows(conn, pending_rows))
        columns = table_columns(conn, "opportunity_outcomes") | set(REQUIRED_OPPORTUNITY_OUTCOME_COLUMNS)
        reason_counter: Counter[str] = Counter()
        source_counter: Counter[str] = Counter()
        still_pending = 0
        updates: list[dict[str, Any]] = []
        write_actions: list[tuple[str, dict[str, Any], dict[str, Any] | str]] = []
        for row in pending_rows:
            due_at = coerce_epoch(row.get("due_at"))
            window = coerce_int(row.get("window_sec"))
            if due_at is None:
                reason_counter["due_time_parse_error"] += 1
                result["due_time_parse_error_count"] += 1
                still_pending += 1
                if not dry_run:
                    write_actions.append(("unresolved", row, "due_time_parse_error"))
                continue
            if due_at > now_value:
                reason_counter["due_time_not_reached"] += 1
                result["future_pending_count"] += 1
                still_pending += 1
                continue
            result["past_due_pending_count"] += 1
            completion, reason = _find_completion(
                row,
                replay_by_opportunity,
                outcomes_by_opportunity,
                outcomes_by_signal,
                now_value,
            )
            if completion is None:
                reason_counter[reason or "missing_price_snapshot"] += 1
                still_pending += 1
                if not dry_run:
                    write_actions.append(("unresolved", row, reason or "missing_price_snapshot"))
                continue
            source = str(completion.get("source") or "catchup_unknown")
            source_counter[source] += 1
            result["would_update_rows"] += 1
            if source == "catchup_from_replay":
                result["completed_from_replay"] += 1
            elif source == "catchup_from_outcomes":
                result["completed_from_outcomes"] += 1
            updates.append(
                {
                    "trade_opportunity_id": str(row.get("trade_opportunity_id") or "")[:80],
                    "window_sec": window,
                    "source": source,
                    "net_pnl_bps": completion.get("net_pnl_bps"),
                }
            )
            if not dry_run:
                write_actions.append(("update", row, completion))
                result["updated_count"] += 1
        result["still_pending_count"] = still_pending
        result["unresolved_count"] = still_pending
        result["unresolved_reason_distribution"] = dict(sorted(reason_counter.items()))
        result["source_distribution"] = dict(sorted(source_counter.items()))
        result["updates"] = updates[:20]
        if not dry_run:
            batch_size = sqlite_store.sqlite_write_batch_size()
            for index in range(0, len(write_actions), batch_size):
                batch = write_actions[index : index + batch_size]

                def apply_batch(actions: list[tuple[str, dict[str, Any], dict[str, Any] | str]] = batch) -> None:
                    for action, action_row, payload in actions:
                        if action == "update" and isinstance(payload, dict):
                            _update_row(conn, columns, action_row, payload, now_value)
                        elif action == "unresolved":
                            _record_unresolved(conn, columns, action_row, str(payload), now_value)

                if not sqlite_store.transaction_with_retry(
                    conn,
                    apply_batch,
                    op_name="outcome_catchup:update_batch",
                    table_name="opportunity_outcomes",
                ):
                    result["error"] = "sqlite_update_failed"
                    break
                sqlite_store._pause_between_batches()
            after = fetch_pending_rows(conn, logical_date)
            result["still_pending_count"] = len(after)
        return result
    except Exception:
        if not dry_run and conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def json_line(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
