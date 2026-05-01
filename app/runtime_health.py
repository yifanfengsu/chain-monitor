"""Runtime health and data gap detection for chain-monitor.

CLI:
  python -m app.runtime_health --summary
  python -m app.runtime_health --date YYYY-MM-DD

Outputs:
  - active_hours
  - last_raw_event_ts, max_raw_event_gap_sec
  - last_signal_ts, max_signal_gap_sec
  - data_quality_status (valid / degraded / invalid_or_no_activity)
  - zero_activity_day flag
  - data_gap_warnings list

Queries SQLite signals and raw_events tables for timestamps.
Gap > 15 min: warning
Gap > 60 min: data_quality=degraded
active_hours=0 or raw_events=0: data_quality=invalid_or_no_activity
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone

from sqlite_store import get_connection, init_sqlite_store

_GAP_WARNING_SEC = 900  # 15 min
_GAP_DEGRADED_SEC = 3600  # 60 min
_BJ_TZ = timezone(timedelta(hours=8))


def _date_bounds(date_str: str | None) -> tuple[int | None, int | None]:
    if not date_str:
        return None, None
    parsed = datetime.strptime(date_str, "%Y-%m-%d")
    start = parsed.replace(tzinfo=_BJ_TZ)
    start_ts = int(start.timestamp())
    return start_ts, start_ts + 24 * 3600 - 1


def _fetch_ts(conn, table: str, column: str, start_ts: int | None, end_ts: int | None) -> list[float]:
    clauses: list[str] = []
    params: list[object] = []
    if start_ts is not None:
        clauses.append(f"{column} >= ?")
        params.append(start_ts)
    if end_ts is not None:
        clauses.append(f"{column} <= ?")
        params.append(end_ts)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    try:
        rows = conn.execute(f"SELECT {column} FROM {table} {where} ORDER BY {column} ASC", tuple(params)).fetchall()
    except Exception:
        return []
    return [float(r[0]) for r in rows if r[0] is not None]


def _query_timestamps(date_str: str | None = None) -> dict:
    conn = get_connection()
    if conn is None:
        conn = init_sqlite_store()
    if conn is None:
        return {"error": "no_sqlite_connection"}

    start_ts, end_ts = _date_bounds(date_str)
    raw_timestamps = _fetch_ts(conn, "raw_events", "captured_at", start_ts, end_ts)
    parsed_timestamps = _fetch_ts(conn, "parsed_events", "parsed_at", start_ts, end_ts)
    signal_timestamps = _fetch_ts(conn, "signals", "timestamp", start_ts, end_ts)

    clauses = ["success=1"]
    params: list[object] = []
    if start_ts is not None:
        clauses.append("created_at >= ?")
        params.append(start_ts)
    if end_ts is not None:
        clauses.append("created_at <= ?")
        params.append(end_ts)
    mc_rows = conn.execute(
        f"SELECT created_at FROM market_context_attempts WHERE {' AND '.join(clauses)} ORDER BY created_at ASC",
        tuple(params),
    ).fetchall()
    mc_timestamps = [float(r[0]) for r in mc_rows if r[0] is not None]

    raw_clauses: list[str] = []
    raw_params: list[object] = []
    if start_ts is not None:
        raw_clauses.append("captured_at >= ?")
        raw_params.append(start_ts)
    if end_ts is not None:
        raw_clauses.append("captured_at <= ?")
        raw_params.append(end_ts)
    raw_where = f"WHERE {' AND '.join(raw_clauses)}" if raw_clauses else ""
    block_rows = conn.execute(f"SELECT MAX(block_number) FROM raw_events {raw_where}", tuple(raw_params)).fetchone()

    opp_clauses: list[str] = []
    opp_params: list[object] = []
    if start_ts is not None:
        opp_clauses.append("created_at >= ?")
        opp_params.append(start_ts)
    if end_ts is not None:
        opp_clauses.append("created_at <= ?")
        opp_params.append(end_ts)
    opp_where = f"WHERE {' AND '.join(opp_clauses)}" if opp_clauses else ""
    opp_rows = conn.execute(f"SELECT COUNT(*) FROM trade_opportunities {opp_where}", tuple(opp_params)).fetchone()

    return {
        "raw_timestamps": raw_timestamps,
        "parsed_timestamps": parsed_timestamps,
        "signal_timestamps": signal_timestamps,
        "mc_timestamps": mc_timestamps,
        "last_block_seen": int(block_rows[0]) if block_rows and block_rows[0] is not None else None,
        "trade_opportunity_count": int(opp_rows[0]) if opp_rows else 0,
    }


def _compute_gaps(timestamps: list[float]) -> tuple[float | None, list[str]]:
    if not timestamps:
        return None, ["no_events"]
    gaps = []
    warnings: list[str] = []
    for i in range(1, len(timestamps)):
        gap = timestamps[i] - timestamps[i - 1]
        gaps.append(gap)
        if gap > _GAP_WARNING_SEC:
            warnings.append(f"gap_{int(gap)}s_at_ts_{timestamps[i]:.0f}")
    max_gap = max(gaps) if gaps else 0.0
    return max_gap, warnings


def build_runtime_health_report(date_str: str | None = None) -> dict:
    data = _query_timestamps(date_str)
    if "error" in data:
        return data

    raw_timestamps = data["raw_timestamps"]
    parsed_timestamps = data["parsed_timestamps"]
    signal_timestamps = data["signal_timestamps"]
    mc_timestamps = data["mc_timestamps"]
    last_block_seen = data["last_block_seen"]

    raw_event_count = len(raw_timestamps)
    parsed_event_count = len(parsed_timestamps)
    signal_count = len(signal_timestamps)

    max_raw_gap, raw_warnings = _compute_gaps(raw_timestamps)
    max_signal_gap, signal_warnings = _compute_gaps(signal_timestamps)

    all_warnings = list(raw_warnings) + list(signal_warnings)

    # active hours
    active_hours = 0.0
    if raw_timestamps:
        span = raw_timestamps[-1] - raw_timestamps[0]
        active_hours = round(span / 3600.0, 2)

    last_raw_event_ts = raw_timestamps[-1] if raw_timestamps else None
    last_parsed_event_ts = parsed_timestamps[-1] if parsed_timestamps else None
    last_signal_ts = signal_timestamps[-1] if signal_timestamps else None
    last_market_context_success_ts = mc_timestamps[-1] if mc_timestamps else None

    # data quality status
    zero_activity = (raw_event_count == 0 and signal_count == 0)
    if raw_event_count == 0 and int(data.get("trade_opportunity_count") or 0) > 0:
        all_warnings.append("stale_or_cross_day_data_warning")
    if zero_activity or raw_event_count == 0:
        data_quality_status = "invalid_or_no_activity"
    elif max_raw_gap and max_raw_gap > _GAP_DEGRADED_SEC:
        data_quality_status = "degraded"
    elif max_signal_gap and max_signal_gap > _GAP_DEGRADED_SEC:
        data_quality_status = "degraded"
    else:
        data_quality_status = "valid"

    return {
        "active_hours": active_hours,
        "last_raw_event_ts": last_raw_event_ts,
        "last_parsed_event_ts": last_parsed_event_ts,
        "last_signal_ts": last_signal_ts,
        "last_market_context_success_ts": last_market_context_success_ts,
        "last_block_seen": last_block_seen,
        "raw_events_count": raw_event_count,
        "parsed_events_count": parsed_event_count,
        "signals_count": signal_count,
        "max_raw_event_gap_sec": max_raw_gap,
        "max_signal_gap_sec": max_signal_gap,
        "data_gap_warnings": all_warnings,
        "zero_activity_day": 1 if zero_activity else 0,
        "data_quality_status": data_quality_status,
        "process_heartbeat_ts": int(time.time()),
        "trade_opportunities_count": int(data.get("trade_opportunity_count") or 0),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Runtime health and data gap detection")
    parser.add_argument("--summary", action="store_true", help="Full runtime health summary")
    parser.add_argument("--date", type=str, default=None, help="Filter to date YYYY-MM-DD")
    args = parser.parse_args(argv)

    init_sqlite_store()
    payload = build_runtime_health_report(date_str=args.date)
    if args.summary and not args.date:
        try:
            import sqlite_store

            sqlite_store.write_runtime_heartbeat(payload)
        except Exception:
            pass
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
