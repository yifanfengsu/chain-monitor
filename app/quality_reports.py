from __future__ import annotations

import argparse
import csv
import io
import json
import sys

from quality_manager import QualityManager
from state_manager import StateManager


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="chain-monitor LP quality/outcome summary")
    parser.add_argument("--summary", action="store_true", help="输出全量 JSON summary")
    parser.add_argument("--top-bad-prealerts", action="store_true", help="输出低 prealert precision 维度")
    parser.add_argument("--fastlane-roi", action="store_true", help="输出低 fastlane ROI 维度")
    parser.add_argument("--days", type=int, default=None, help="仅统计最近 N 天")
    parser.add_argument("--limit", type=int, default=None, help="仅统计最近 N 条 outcome")
    parser.add_argument("--top-n", type=int, default=None, help="top/bottom 行数")
    parser.add_argument("--format", choices=("json", "csv"), default="json", help="输出格式")
    return parser


def _render_csv(rows: list[dict]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "dimension",
            "key",
            "label",
            "sample_size",
            "quality_score",
            "prealert_precision_score",
            "confirm_conversion_score",
            "climax_reversal_score",
            "market_context_alignment_score",
            "fastlane_roi_score",
            "quality_hint",
            "tuning_hint",
            "actionable",
        ],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    manager = QualityManager(state_manager=StateManager())
    report = manager.build_report(days=args.days, limit=args.limit, top_n=args.top_n or 10)

    if args.top_bad_prealerts:
        payload = report.get("top_bad_prealerts") or []
    elif args.fastlane_roi:
        payload = report.get("fastlane_roi_laggards") or []
    else:
        payload = report if args.summary or (not args.top_bad_prealerts and not args.fastlane_roi) else report

    if args.format == "csv":
        rows = manager.export_rows(days=args.days, limit=args.limit)
        sys.stdout.write(_render_csv(rows))
        return 0

    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
