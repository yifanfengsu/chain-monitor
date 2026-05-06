#!/usr/bin/env python3
"""Safe opportunity_outcomes catchup from persisted replay/outcomes."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
for path in (ROOT, APP_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from opportunity_outcome_catchup import json_line, opportunity_outcome_catchup  # noqa: E402


def parse_date(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value or ""):
        raise ValueError("date must be YYYY-MM-DD")
    parsed = dt.date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("date must be YYYY-MM-DD")
    return value


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Catch up past-due opportunity_outcomes from replay/outcomes.")
    parser.add_argument("--date", required=True, help="Beijing logical date YYYY-MM-DD")
    parser.add_argument("--db-path", default=os.environ.get("HERMES_OUTCOME_CATCHUP_DB_PATH", "data/chain_monitor.sqlite"))
    parser.add_argument("--now-ts", type=float, default=os.environ.get("HERMES_OUTCOME_CATCHUP_NOW_TS"))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Only report would_update_rows; do not write SQLite")
    mode.add_argument("--execute", action="store_true", help="Update completed past-due opportunity_outcomes")
    parser.add_argument("--confirm", action="store_true", help="Required with --execute unless CONFIRM=YES")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        logical_date = parse_date(args.date)
    except ValueError as exc:
        print(f"error: invalid_date:{exc}", file=sys.stderr)
        return 2
    if args.execute and not (args.confirm or os.environ.get("CONFIRM") == "YES"):
        print("error: outcome-catchup execute requires --confirm or CONFIRM=YES", file=sys.stderr)
        return 2
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = ROOT / db_path
    result = opportunity_outcome_catchup(
        db_path,
        logical_date,
        dry_run=bool(args.dry_run),
        now_ts=args.now_ts,
    )
    mode = "dry-run" if args.dry_run else "execute"
    print(f"Chain Monitor outcome-catchup｜{logical_date}")
    print(f"mode={mode}")
    print("policy=只补 opportunity_outcomes；不创建 opportunity；不改 signals；不改 gate；不推 Telegram。")
    print(f"pending_rows={result.get('pending_rows', 0)}")
    print(f"past_due_pending_count={result.get('past_due_pending_count', 0)}")
    print(f"future_pending_count={result.get('future_pending_count', 0)}")
    print(f"would_update_rows={result.get('would_update_rows', 0)}")
    print(f"updated_count={result.get('updated_count', 0)}")
    print(f"still_pending_count={result.get('still_pending_count', 0)}")
    print(f"completed_from_replay={result.get('completed_from_replay', 0)}")
    print(f"completed_from_outcomes={result.get('completed_from_outcomes', 0)}")
    print("unresolved_reason_distribution=" + json_line(result.get("unresolved_reason_distribution", {})))
    print("source_distribution=" + json_line(result.get("source_distribution", {})))
    public_summary = {key: value for key, value in result.items() if key not in {"db_path", "updates"}}
    print("summary_json=" + json_line(public_summary))
    if result.get("error"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
