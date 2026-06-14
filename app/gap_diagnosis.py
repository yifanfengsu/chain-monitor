from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "chain_monitor.sqlite"
DEFAULT_ARCHIVE_BASE_DIR = PROJECT_ROOT / "app" / "data" / "archive"
BJ_TZ = timezone(timedelta(hours=8))
GAP_THRESHOLD_SEC = 3600


@dataclass(frozen=True)
class LayerConfig:
    table: str
    time_candidates: tuple[str, ...]
    sparse: bool = False
    high_frequency: bool = False
    collection_layer: bool = False


SQLITE_LAYERS: dict[str, LayerConfig] = {
    "raw_events": LayerConfig(
        "raw_events",
        ("captured_at", "ingest_ts", "timestamp", "ts", "archive_ts"),
        high_frequency=True,
        collection_layer=True,
    ),
    "parsed_events": LayerConfig(
        "parsed_events",
        ("parsed_at", "timestamp", "ts", "archive_ts"),
        high_frequency=True,
        collection_layer=True,
    ),
    "signals": LayerConfig(
        "signals",
        ("archive_written_at", "timestamp", "notifier_sent_at", "created_at", "updated_at"),
    ),
    "delivery_audit": LayerConfig(
        "delivery_audit",
        ("archive_written_at", "timestamp", "notifier_sent_at", "created_at", "updated_at"),
        high_frequency=True,
        collection_layer=True,
    ),
    "trade_opportunities": LayerConfig(
        "trade_opportunities",
        ("created_at", "updated_at", "expires_at"),
        sparse=True,
    ),
    "opportunity_outcomes": LayerConfig(
        "opportunity_outcomes",
        ("created_at", "updated_at", "completed_at", "evaluated_at", "outcome_ts", "due_at"),
        sparse=True,
    ),
    "runtime_heartbeats": LayerConfig(
        "runtime_heartbeats",
        ("check_ts", "process_heartbeat_ts"),
        high_frequency=True,
        collection_layer=True,
    ),
}

ARCHIVE_CATEGORIES: dict[str, tuple[str, ...]] = {
    "raw_events": ("archive_ts", "captured_at", "ingest_ts", "timestamp", "ts"),
    "parsed_events": ("archive_ts", "parsed_at", "timestamp", "ts"),
    "signals": ("archive_ts", "archive_written_at", "timestamp", "ts", "created_at"),
    "delivery_audit": ("archive_ts", "archive_written_at", "timestamp", "notifier_sent_at", "created_at"),
}


def _window(logical_date: str) -> tuple[int, int]:
    parsed = date.fromisoformat(logical_date)
    start_bj = datetime(parsed.year, parsed.month, parsed.day, tzinfo=BJ_TZ)
    start_ts = int(start_bj.timestamp())
    return start_ts, start_ts + 24 * 3600 - 1


def _bj_text(ts: int | float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _to_epoch(value: Any) -> int | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    if numeric > 10_000_000_000:
        numeric = numeric / 1000.0
    if numeric <= 0:
        return None
    return int(numeric)


def _hourly_counts(timestamps: list[int]) -> dict[str, int]:
    counts = {f"{hour:02d}": 0 for hour in range(24)}
    for ts in timestamps:
        hour = datetime.fromtimestamp(int(ts), BJ_TZ).hour
        counts[f"{hour:02d}"] += 1
    return counts


def _gap_summary_from_timestamps(
    *,
    layer: str,
    timestamps: list[int],
    time_field_used: str,
    status_when_empty: str,
    sparse: bool = False,
    threshold_sec: int = GAP_THRESHOLD_SEC,
) -> dict[str, Any]:
    ordered = sorted(int(ts) for ts in timestamps if ts is not None)
    gaps: list[dict[str, Any]] = []
    max_gap = 0
    max_from: int | None = None
    max_to: int | None = None
    previous: int | None = None
    for ts in ordered:
        if previous is not None:
            gap = int(ts - previous)
            if gap > max_gap:
                max_gap = gap
                max_from = previous
                max_to = ts
            if gap > threshold_sec:
                gaps.append(
                    {
                        "gap_sec": gap,
                        "from_bj": _bj_text(previous),
                        "to_bj": _bj_text(ts),
                    }
                )
        previous = ts

    if not ordered:
        status = status_when_empty
    elif gaps and layer == "signals":
        status = "signal_generation_gap"
    elif gaps and sparse:
        status = "sparse_large_gap_not_collection"
    elif gaps:
        status = "large_gap"
    elif sparse:
        status = "sparse_ok"
    else:
        status = "ok"

    return {
        "layer": layer,
        "rows": len(ordered),
        "first_bj": _bj_text(ordered[0]) if ordered else None,
        "last_bj": _bj_text(ordered[-1]) if ordered else None,
        "max_gap_sec": int(max_gap) if ordered else None,
        "max_gap_from_bj": _bj_text(max_from),
        "max_gap_to_bj": _bj_text(max_to),
        "gaps_over_threshold": gaps,
        "gaps_over_threshold_count": len(gaps),
        "hourly_counts": _hourly_counts(ordered),
        "time_field_used": time_field_used,
        "status": status,
        "sparse_layer": bool(sparse),
    }


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({_quote(table)})").fetchall()}


def _epoch_expr(column: str) -> str:
    ref = _quote(column)
    return f"(CASE WHEN CAST({ref} AS REAL) > 10000000000 THEN CAST({ref} AS REAL) / 1000.0 ELSE CAST({ref} AS REAL) END)"


def _logical_date_count(conn: sqlite3.Connection, table: str, logical_date: str, cols: set[str]) -> int | None:
    if "logical_date" not in cols:
        return None
    try:
        row = conn.execute(
            f"SELECT COUNT(*) FROM {_quote(table)} WHERE CAST({_quote('logical_date')} AS TEXT) = ?",
            (logical_date,),
        ).fetchone()
    except sqlite3.Error:
        return None
    return int(row[0] if row else 0)


def _timestamps_for_field(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    start_ts: int,
    end_ts: int,
) -> list[int]:
    expr = _epoch_expr(field)
    try:
        rows = conn.execute(
            f"SELECT {expr} AS ts FROM {_quote(table)} WHERE {expr} >= ? AND {expr} <= ? ORDER BY {expr} ASC",
            (start_ts, end_ts),
        ).fetchall()
    except sqlite3.Error:
        return []
    timestamps: list[int] = []
    for row in rows:
        ts = _to_epoch(row["ts"] if isinstance(row, sqlite3.Row) else row[0])
        if ts is not None:
            timestamps.append(ts)
    return timestamps


def _sqlite_layer_summary(
    conn: sqlite3.Connection,
    *,
    logical_date: str,
    layer: str,
    config: LayerConfig,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    if not _table_exists(conn, config.table):
        return _gap_summary_from_timestamps(
            layer=layer,
            timestamps=[],
            time_field_used="unavailable",
            status_when_empty="missing_table",
            sparse=config.sparse,
        )

    cols = _columns(conn, config.table)
    logical_rows = _logical_date_count(conn, config.table, logical_date, cols)
    present_candidates = [candidate for candidate in config.time_candidates if candidate in cols]
    if not present_candidates:
        summary = _gap_summary_from_timestamps(
            layer=layer,
            timestamps=[],
            time_field_used="unavailable",
            status_when_empty="time_field_unavailable",
            sparse=config.sparse,
        )
        if logical_rows is not None:
            summary["rows"] = logical_rows
        return summary

    best_field = present_candidates[0]
    best_timestamps: list[int] = []
    for field in present_candidates:
        timestamps = _timestamps_for_field(conn, config.table, field, start_ts, end_ts)
        if timestamps:
            best_field = field
            best_timestamps = timestamps
            break

    if not best_timestamps and logical_rows and logical_rows > 0:
        summary = _gap_summary_from_timestamps(
            layer=layer,
            timestamps=[],
            time_field_used=best_field,
            status_when_empty="time_field_unavailable",
            sparse=config.sparse,
        )
        summary["rows"] = logical_rows
        return summary

    return _gap_summary_from_timestamps(
        layer=layer,
        timestamps=best_timestamps,
        time_field_used=best_field,
        status_when_empty="no_rows_for_date",
        sparse=config.sparse,
    )


def _open_sqlite(db_path: Path) -> sqlite3.Connection | None:
    if not db_path.exists():
        return None
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _archive_date_keys(start_ts: int, end_ts: int) -> list[str]:
    start_day = datetime.fromtimestamp(start_ts, timezone.utc).date()
    end_day = datetime.fromtimestamp(end_ts, timezone.utc).date()
    values: list[str] = []
    current = start_day
    while current <= end_day:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _open_text(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _archive_payload_time(payload: dict[str, Any], candidates: tuple[str, ...]) -> tuple[int | None, str]:
    containers: list[dict[str, Any]] = [payload]
    data = payload.get("data")
    if isinstance(data, dict):
        containers.append(data)
        for nested_key in ("metadata", "event", "signal"):
            nested = data.get(nested_key)
            if isinstance(nested, dict):
                containers.append(nested)
    for field in candidates:
        for container in containers:
            if field not in container:
                continue
            ts = _to_epoch(container.get(field))
            if ts is not None:
                return ts, field
    return None, "unavailable"


def _archive_layer_summary(
    *,
    archive_base_dir: Path,
    category: str,
    logical_date: str,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    root = archive_base_dir / category
    paths: list[Path] = []
    for date_key in _archive_date_keys(start_ts, end_ts):
        plain = root / f"{date_key}.ndjson"
        gz = root / f"{date_key}.ndjson.gz"
        if plain.exists():
            paths.append(plain)
        if gz.exists() and plain not in paths:
            paths.append(gz)
    if not paths:
        return _gap_summary_from_timestamps(
            layer=f"archive_{category}",
            timestamps=[],
            time_field_used="unavailable",
            status_when_empty="missing_archive",
        ) | {
            "archive_category": category,
            "archive_mtime_used": False,
            "archive_mtime_status": "ignored",
        }

    timestamps: list[int] = []
    field_counts: dict[str, int] = {}
    parseable_rows = 0
    for path in paths:
        try:
            with _open_text(path) as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    ts, field = _archive_payload_time(payload, ARCHIVE_CATEGORIES[category])
                    if ts is None:
                        continue
                    parseable_rows += 1
                    field_counts[field] = field_counts.get(field, 0) + 1
                    if start_ts <= ts <= end_ts:
                        timestamps.append(ts)
        except OSError:
            continue

    time_field = max(field_counts.items(), key=lambda item: item[1])[0] if field_counts else "unavailable"
    status_when_empty = "time_field_unavailable" if parseable_rows == 0 else "no_rows_for_date"
    return _gap_summary_from_timestamps(
        layer=f"archive_{category}",
        timestamps=timestamps,
        time_field_used=time_field,
        status_when_empty=status_when_empty,
    ) | {
        "archive_category": category,
        "archive_mtime_used": False,
        "archive_mtime_status": "ignored",
        "archive_files": [path.name for path in paths],
        "archive_compressed_files": [path.name for path in paths if path.name.endswith(".gz")],
        "parseable_rows": parseable_rows,
        "logical_date": logical_date,
    }


def _is_large_gap(layer: dict[str, Any]) -> bool:
    return int(layer.get("gaps_over_threshold_count") or 0) > 0


def _is_continuous(layer: dict[str, Any]) -> bool:
    return int(layer.get("rows") or 0) > 0 and not _is_large_gap(layer) and str(layer.get("status")) in {
        "ok",
        "sparse_ok",
    }


def _is_unavailable(layer: dict[str, Any]) -> bool:
    return str(layer.get("status") or "") in {"missing_table", "missing_archive", "time_field_unavailable"}


def diagnose_layers(layers: dict[str, dict[str, Any]]) -> dict[str, Any]:
    raw = layers.get("raw_events", {})
    parsed = layers.get("parsed_events", {})
    signals = layers.get("signals", {})
    audit = layers.get("delivery_audit", {})
    opportunities = layers.get("trade_opportunities", {})
    heartbeats = layers.get("runtime_heartbeats", {})

    warnings: list[str] = []
    evidence: list[str] = []
    time_field_unavailable_layers = [
        name
        for name in ("raw_events", "parsed_events")
        if str((layers.get(name) or {}).get("status") or "") == "time_field_unavailable"
    ]
    if time_field_unavailable_layers:
        warnings.append("report_time_field_unavailable:" + ",".join(time_field_unavailable_layers))

    sqlite_mirror_gap_layers: list[str] = []
    for name in ("raw_events", "parsed_events", "signals", "delivery_audit"):
        archive_layer = layers.get(f"archive_{name}", {})
        sqlite_layer = layers.get(name, {})
        if _is_continuous(archive_layer) and (_is_large_gap(sqlite_layer) or str(sqlite_layer.get("status")) == "no_rows_for_date"):
            sqlite_mirror_gap_layers.append(name)
    if sqlite_mirror_gap_layers:
        warnings.append("sqlite_mirror_gap:" + ",".join(sqlite_mirror_gap_layers))

    raw_or_parsed_large = _is_large_gap(raw) or _is_large_gap(parsed)
    raw_or_parsed_unavailable = _is_unavailable(raw) or _is_unavailable(parsed)
    heartbeat_large = _is_large_gap(heartbeats)
    heartbeat_continuous = _is_continuous(heartbeats)
    heartbeat_known = not _is_unavailable(heartbeats) and str(heartbeats.get("status") or "") != "no_rows_for_date"

    if sqlite_mirror_gap_layers:
        diagnosis = "sqlite_mirror_gap"
        final_status = "collection_degraded"
        collection_degraded = True
        evidence.append("archive_internal_timestamps_continuous_but_sqlite_gap")
    elif heartbeat_large and _is_large_gap(audit) and raw_or_parsed_large:
        diagnosis = "listener_runtime_gap"
        final_status = "collection_degraded"
        collection_degraded = True
        evidence.append("runtime_heartbeats_delivery_audit_raw_or_parsed_all_gap")
    elif _is_large_gap(audit):
        diagnosis = "listener_or_audit_gap"
        final_status = "collection_degraded"
        collection_degraded = True
        evidence.append("delivery_audit_gap_affects_collection_diagnosis")
        if heartbeat_continuous:
            warnings.append("heartbeat_continuous_with_delivery_audit_gap:check_rpc_or_archive_ingest")
    elif heartbeat_continuous and raw_or_parsed_large:
        diagnosis = "rpc_or_scanner_gap"
        final_status = "collection_degraded"
        collection_degraded = True
        evidence.append("heartbeat_continuous_but_raw_or_parsed_gap")
    elif _is_continuous(audit) and _is_large_gap(signals):
        diagnosis = "signal_generation_gap"
        final_status = "warning_signal_sparse"
        collection_degraded = False
        evidence.append("delivery_audit_continuous_but_signals_gap")
    elif _is_continuous(signals) and _is_large_gap(opportunities):
        diagnosis = "opportunity_generation_sparse"
        final_status = "warning_opportunity_sparse"
        collection_degraded = False
        evidence.append("signals_continuous_but_trade_opportunities_sparse_gap")
    elif time_field_unavailable_layers and not (_is_large_gap(signals) or _is_large_gap(audit)):
        diagnosis = "report_time_field_unavailable"
        final_status = "warning_time_field_unavailable"
        collection_degraded = False
        evidence.append("raw_or_parsed_missing_reliable_time_field")
    elif raw_or_parsed_large:
        diagnosis = "parser_gap" if _is_large_gap(parsed) and not _is_large_gap(raw) else "rpc_or_scanner_gap"
        final_status = "collection_degraded"
        collection_degraded = True
        evidence.append("raw_or_parsed_collection_layer_gap")
    elif raw_or_parsed_unavailable and _is_continuous(audit):
        diagnosis = "report_time_field_unavailable"
        final_status = "warning_time_field_unavailable"
        collection_degraded = False
        evidence.append("audit_continuous_raw_or_parsed_time_unavailable")
    elif heartbeat_known and heartbeat_large:
        diagnosis = "listener_runtime_gap"
        final_status = "collection_degraded"
        collection_degraded = True
        evidence.append("runtime_heartbeats_gap")
    else:
        diagnosis = "ok"
        final_status = "ok"
        collection_degraded = False
        evidence.append("no_collection_gap_detected")

    if diagnosis == "signal_generation_gap":
        next_check = "验证信号量骤降是链上活动减少、gate/cooldown 抑制，还是扫描覆盖收缩。"
    elif diagnosis == "opportunity_generation_sparse":
        next_check = "机会层是稀疏层；先核对 signals 到 opportunity 的 gate/blocker 分布，不按 listener 停摆处理。"
    elif diagnosis == "report_time_field_unavailable":
        next_check = "补齐 raw/parsed 可用于逻辑日 gap 诊断的事件时间字段，不能仅按 created_at/updated_at 下结论。"
    elif collection_degraded:
        next_check = "优先核对 listener/runtime、RPC scanner、archive ingest 和 SQLite mirror 的连续性。"
    else:
        next_check = "保持观察；如 signals 继续骤降，复核 gate/cooldown 和扫描覆盖。"

    return {
        "root_diagnosis": diagnosis,
        "diagnosis": diagnosis,
        "final_status": final_status,
        "collection_degraded": collection_degraded,
        "degraded_reason": diagnosis if diagnosis != "ok" else "",
        "warnings": sorted(set(warnings)),
        "evidence": evidence,
        "next_check": next_check,
        "time_field_unavailable_layers": time_field_unavailable_layers,
        "sqlite_mirror_gap_layers": sqlite_mirror_gap_layers,
    }


def _resolve_path(value: str | Path | None, default: Path) -> Path:
    raw = Path(value) if value else default
    if raw.is_absolute():
        return raw
    return PROJECT_ROOT / raw


def build_gap_diagnosis(
    logical_date: str,
    *,
    db_path: str | Path | None = None,
    archive_base_dir: str | Path | None = None,
    threshold_sec: int = GAP_THRESHOLD_SEC,
) -> dict[str, Any]:
    start_ts, end_ts = _window(logical_date)
    resolved_db = _resolve_path(db_path, DEFAULT_DB_PATH)
    resolved_archive = _resolve_path(archive_base_dir, DEFAULT_ARCHIVE_BASE_DIR)

    layers: dict[str, dict[str, Any]] = {}
    conn: sqlite3.Connection | None = None
    try:
        conn = _open_sqlite(resolved_db)
    except sqlite3.Error:
        conn = None
    if conn is None:
        for name, config in SQLITE_LAYERS.items():
            layers[name] = _gap_summary_from_timestamps(
                layer=name,
                timestamps=[],
                time_field_used="unavailable",
                status_when_empty="sqlite_unavailable",
                sparse=config.sparse,
                threshold_sec=threshold_sec,
            )
    else:
        try:
            for name, config in SQLITE_LAYERS.items():
                layers[name] = _sqlite_layer_summary(
                    conn,
                    logical_date=logical_date,
                    layer=name,
                    config=config,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
        finally:
            conn.close()

    for category in ARCHIVE_CATEGORIES:
        layers[f"archive_{category}"] = _archive_layer_summary(
            archive_base_dir=resolved_archive,
            category=category,
            logical_date=logical_date,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    diagnosis = diagnose_layers(layers)
    table_time_fields_used = {
        name: str(layer.get("time_field_used") or "unavailable")
        for name, layer in layers.items()
    }
    layer_max_gaps = {
        name: layer.get("max_gap_sec")
        for name, layer in layers.items()
    }
    large_gap_layers = [
        name
        for name, layer in layers.items()
        if int(layer.get("gaps_over_threshold_count") or 0) > 0
    ]
    payload = {
        "schema": "chain_monitor_gap_diagnosis_v1",
        "logical_date": logical_date,
        "timezone": "Asia/Shanghai",
        "gap_threshold_sec": threshold_sec,
        "window": {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_bj": _bj_text(start_ts),
            "end_bj": _bj_text(end_ts),
        },
        "layers": layers,
        "gap_diagnosis_summary": {
            **diagnosis,
            "large_gap_layers": large_gap_layers,
            "table_time_fields_used": table_time_fields_used,
            "layer_max_gap_sec": layer_max_gaps,
        },
        "archive_mtime_used": False,
        "archive_mtime_status": "ignored_for_gap_diagnosis",
        "db_path": str(resolved_db),
        "archive_base_dir": str(resolved_archive),
    }
    return payload


def _json_line(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def render_text(payload: dict[str, Any]) -> str:
    summary = payload.get("gap_diagnosis_summary") if isinstance(payload.get("gap_diagnosis_summary"), dict) else {}
    layers = payload.get("layers") if isinstance(payload.get("layers"), dict) else {}
    lines = [
        f"监听间隔诊断 {payload.get('logical_date')}",
        f"root_diagnosis={summary.get('root_diagnosis', 'missing')}",
        f"final_status={summary.get('final_status', 'missing')}",
        f"collection_degraded={str(bool(summary.get('collection_degraded'))).lower()}",
        f"degraded_reason={summary.get('degraded_reason') or 'none'}",
        f"archive_mtime_used={str(bool(payload.get('archive_mtime_used'))).lower()}",
        f"archive_mtime_status={payload.get('archive_mtime_status', 'missing')}",
        "table_time_fields_used=" + _json_line(summary.get("table_time_fields_used") or {}),
        "warnings=" + _json_line(summary.get("warnings") or []),
        "evidence=" + _json_line(summary.get("evidence") or []),
        "",
        "各层 gap summary:",
    ]
    preferred_order = [
        "raw_events",
        "parsed_events",
        "signals",
        "delivery_audit",
        "trade_opportunities",
        "opportunity_outcomes",
        "runtime_heartbeats",
        "archive_raw_events",
        "archive_parsed_events",
        "archive_signals",
        "archive_delivery_audit",
    ]
    for name in preferred_order:
        layer = layers.get(name)
        if not isinstance(layer, dict):
            continue
        lines.append(
            "layer={name} rows={rows} status={status} time_field_used={field} "
            "first_bj={first} last_bj={last} max_gap_sec={gap} "
            "max_gap_from_bj={gap_from} max_gap_to_bj={gap_to} "
            "gaps_over_threshold={gap_count}".format(
                name=name,
                rows=layer.get("rows", 0),
                status=layer.get("status", "missing"),
                field=layer.get("time_field_used", "missing"),
                first=layer.get("first_bj") or "missing",
                last=layer.get("last_bj") or "missing",
                gap=layer.get("max_gap_sec"),
                gap_from=layer.get("max_gap_from_bj") or "missing",
                gap_to=layer.get("max_gap_to_bj") or "missing",
                gap_count=layer.get("gaps_over_threshold_count", 0),
            )
        )
        lines.append(f"hourly_counts {name}=" + _json_line(layer.get("hourly_counts") or {}))
    lines.extend(
        [
            "",
            f"next_check={summary.get('next_check') or 'missing'}",
            "说明=signals gap 不等于 listener_runtime_gap；trade_opportunities/opportunity_outcomes 是稀疏层，不单独判定监听停摆。",
            "注意=不提供交易建议。",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Layered listener/data gap diagnosis")
    parser.add_argument("--date", required=True, help="Beijing logical date YYYY-MM-DD")
    parser.add_argument("--db-path", default=None, help="SQLite DB path")
    parser.add_argument("--archive-base-dir", default=None, help="Archive base dir")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    try:
        date.fromisoformat(args.date)
    except ValueError:
        parser.error("--date must be YYYY-MM-DD")

    payload = build_gap_diagnosis(
        args.date,
        db_path=args.db_path,
        archive_base_dir=args.archive_base_dir,
    )
    if args.format == "json":
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
