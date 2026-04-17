from __future__ import annotations

import argparse
import csv
from collections import defaultdict
import io
import json
from pathlib import Path
import sys

from config import ARCHIVE_BASE_DIR, LP_MAJOR_ASSETS, LP_MAJOR_QUOTES, PROJECT_ROOT
from lp_product_helpers import canonical_asset_symbol, normalize_symbol
from lp_registry import ACTIVE_LP_POOLS
from quality_manager import QualityManager
from state_manager import StateManager


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="chain-monitor LP quality/outcome summary")
    parser.add_argument("--summary", action="store_true", help="输出全量 JSON summary")
    parser.add_argument("--top-bad-prealerts", action="store_true", help="输出低 prealert precision 维度")
    parser.add_argument("--fastlane-roi", action="store_true", help="输出低 fastlane ROI 维度")
    parser.add_argument("--market-context-health", action="store_true", help="输出 live market context 健康度")
    parser.add_argument("--major-pool-coverage", action="store_true", help="输出 majors pool 覆盖情况")
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


def _iter_archive_rows(category: str, *, base_dir: str | Path = ARCHIVE_BASE_DIR) -> list[dict]:
    root = Path(base_dir) / category
    rows: list[dict] = []
    if not root.exists():
        return rows
    for path in sorted(root.glob("*.ndjson")):
        try:
            with path.open(encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
                    if data:
                        rows.append(data)
        except OSError:
            continue
    return rows


def _signal_row_value(row: dict, key: str):
    def _present(value) -> bool:
        return value not in (None, "", [], {})

    if key in row and _present(row.get(key)):
        return row.get(key)
    signal = row.get("signal") if isinstance(row.get("signal"), dict) else {}
    event = row.get("event") if isinstance(row.get("event"), dict) else {}
    signal_context = signal.get("context") if isinstance(signal.get("context"), dict) else {}
    signal_metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    event_metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    for container in (signal_context, signal_metadata, event_metadata, signal, event):
        if key in container and _present(container.get(key)):
            return container.get(key)
    return None


def _canonical_major_pair_label(pair_label: str | None) -> str:
    raw = str(pair_label or "").strip().upper().replace(" ", "")
    if not raw or "/" not in raw:
        return raw
    left, right = raw.split("/", 1)
    return f"{canonical_asset_symbol(left)}/{normalize_symbol(right).replace('.E', '')}"


def build_market_context_health_report(*, base_dir: str | Path = ARCHIVE_BASE_DIR) -> dict:
    rows = _iter_archive_rows("signals", base_dir=base_dir)
    venue_stats = defaultdict(lambda: defaultdict(int))
    endpoint_stats = defaultdict(lambda: defaultdict(int))
    resolved_symbol_stats = defaultdict(lambda: defaultdict(int))
    attempt_symbol_stats = defaultdict(lambda: defaultdict(int))
    failure_reason_counts = defaultdict(int)
    unavailable_reason_breakdown = defaultdict(int)
    warnings: list[str] = []
    total_attempts = 0

    unavailable_count = sum(1 for row in rows if str(_signal_row_value(row, "market_context_source") or "") == "unavailable")
    live_public_count = sum(1 for row in rows if str(_signal_row_value(row, "market_context_source") or "") == "live_public")

    for row in rows:
        venue = str(_signal_row_value(row, "market_context_venue") or "")
        source = str(_signal_row_value(row, "market_context_source") or "")
        resolved_symbol = str(
            _signal_row_value(row, "market_context_resolved_symbol")
            or _signal_row_value(row, "market_context_requested_symbol")
            or ""
        )
        final_failure_reason = str(_signal_row_value(row, "market_context_failure_reason") or "")
        if venue:
            venue_stats[venue]["signal_total"] += 1
            if source == "live_public":
                venue_stats[venue]["signal_success"] += 1
            elif source == "unavailable":
                venue_stats[venue]["signal_unavailable"] += 1
        if resolved_symbol:
            resolved_symbol_stats[resolved_symbol]["signal_total"] += 1
            if source == "live_public":
                resolved_symbol_stats[resolved_symbol]["signal_success"] += 1
            elif source == "unavailable":
                resolved_symbol_stats[resolved_symbol]["signal_failure"] += 1
        if source == "unavailable" and final_failure_reason:
            unavailable_reason_breakdown[final_failure_reason] += 1

        for attempt in list(_signal_row_value(row, "market_context_attempts") or []):
            total_attempts += 1
            attempt_venue = str(attempt.get("venue") or venue or "")
            endpoint = str(attempt.get("endpoint") or "")
            attempt_symbol = str(attempt.get("symbol") or resolved_symbol or "")
            status = str(attempt.get("status") or "")
            failure_reason = str(attempt.get("failure_reason") or "")
            http_status = attempt.get("http_status")

            if attempt_venue:
                venue_stats[attempt_venue]["attempt_total"] += 1
                venue_stats[attempt_venue][f"attempt_{status or 'unknown'}"] += 1
                if status in {"success", "cache_hit"}:
                    venue_stats[attempt_venue]["attempt_success"] += 1
                if status == "failure":
                    venue_stats[attempt_venue]["attempt_failure"] += 1
                if failure_reason.startswith("timeout"):
                    venue_stats[attempt_venue]["timeout_count"] += 1
                if "malformed" in failure_reason:
                    venue_stats[attempt_venue]["malformed_payload_count"] += 1
                if "symbol_mismatch" in failure_reason:
                    venue_stats[attempt_venue]["symbol_mismatch_count"] += 1
                if "no_symbol" in failure_reason:
                    venue_stats[attempt_venue]["no_symbol_count"] += 1
                if http_status is not None:
                    venue_stats[attempt_venue][f"http_{int(http_status)}"] += 1
            if endpoint and endpoint != "cache":
                endpoint_stats[endpoint]["attempt_total"] += 1
                endpoint_stats[endpoint][f"attempt_{status or 'unknown'}"] += 1
                if status in {"success", "cache_hit"}:
                    endpoint_stats[endpoint]["attempt_success"] += 1
                if status == "failure":
                    endpoint_stats[endpoint]["attempt_failure"] += 1
            if attempt_symbol:
                attempt_symbol_stats[attempt_symbol]["attempt_total"] += 1
                attempt_symbol_stats[attempt_symbol][f"attempt_{status or 'unknown'}"] += 1
                if status in {"success", "cache_hit"}:
                    attempt_symbol_stats[attempt_symbol]["attempt_success"] += 1
                if status == "failure":
                    attempt_symbol_stats[attempt_symbol]["attempt_failure"] += 1
                if failure_reason:
                    attempt_symbol_stats[attempt_symbol]["failure_total"] += 1
            if failure_reason:
                failure_reason_counts[failure_reason] += 1

    if not rows:
        warnings.append("archive/signals 为空或缺失，无法评估 market context hit rate")

    def _sort_table(source: dict, *, key_name: str) -> list[dict]:
        return [
            {
                key_name: key,
                **dict(value),
            }
            for key, value in sorted(
                source.items(),
                key=lambda item: (
                    -int(item[1].get("signal_total") or item[1].get("attempt_total") or 0),
                    item[0],
                ),
            )
        ]
    
    def _attach_rates(table: list[dict]) -> list[dict]:
        enriched = []
        for row in table:
            item = dict(row)
            signal_total = int(item.get("signal_total") or 0)
            attempt_total = int(item.get("attempt_total") or 0)
            signal_success = int(item.get("signal_success") or 0)
            attempt_success = int(item.get("attempt_success") or 0)
            attempt_failure = int(item.get("attempt_failure") or 0)
            item["signal_hit_rate"] = round(signal_success / signal_total, 4) if signal_total else 0.0
            item["attempt_hit_rate"] = round(attempt_success / attempt_total, 4) if attempt_total else 0.0
            item["attempt_failure_rate"] = round(attempt_failure / attempt_total, 4) if attempt_total else 0.0
            enriched.append(item)
        return enriched

    total = len(rows)
    per_venue = _attach_rates(_sort_table(venue_stats, key_name="venue"))
    per_endpoint = _attach_rates(_sort_table(endpoint_stats, key_name="endpoint"))
    per_resolved_symbol = _attach_rates(_sort_table(resolved_symbol_stats, key_name="symbol"))
    per_attempt_symbol = _attach_rates(_sort_table(attempt_symbol_stats, key_name="symbol"))
    return {
        "archive_base_dir": str(Path(base_dir)),
        "signal_rows": total,
        "total_attempts": total_attempts,
        "live_public_count": live_public_count,
        "unavailable_count": unavailable_count,
        "live_public_hit_rate": round((live_public_count / total), 4) if total else 0.0,
        "unavailable_rate": round((unavailable_count / total), 4) if total else 0.0,
        "per_venue": per_venue,
        "per_endpoint": per_endpoint,
        "per_resolved_symbol": per_resolved_symbol,
        "per_attempt_symbol": per_attempt_symbol,
        "per_symbol": per_resolved_symbol,
        "venue_hit_rate": [
            {"venue": row["venue"], "hit_rate": row["signal_hit_rate"]}
            for row in per_venue
        ],
        "endpoint_hit_rate": [
            {"endpoint": row["endpoint"], "hit_rate": row["attempt_hit_rate"]}
            for row in per_endpoint
        ],
        "symbol_hit_rate": [
            {"symbol": row["symbol"], "hit_rate": row["signal_hit_rate"]}
            for row in per_resolved_symbol
        ],
        "unavailable_reason_breakdown": dict(sorted(unavailable_reason_breakdown.items(), key=lambda item: (-item[1], item[0]))),
        "failure_reason_counts": dict(sorted(failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "top_failure_reasons": [
            {"reason": reason, "count": count}
            for reason, count in sorted(failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
        "warnings": warnings,
    }


def build_major_pool_coverage_report(
    manager: QualityManager | None = None,
    *,
    pool_book_path: Path | None = None,
) -> dict:
    manager = manager or QualityManager(state_manager=StateManager())
    records = list(manager._combined_records(limit=manager.history_limit))
    pool_book_path = pool_book_path or (PROJECT_ROOT / "data" / "lp_pools.json")
    pool_book_exists = pool_book_path.exists()
    warnings: list[str] = []
    suggestions: list[str] = []

    configured_assets = []
    for item in LP_MAJOR_ASSETS:
        normalized = canonical_asset_symbol(item)
        if normalized and normalized not in configured_assets:
            configured_assets.append(normalized)
    configured_quotes = []
    for item in LP_MAJOR_QUOTES:
        normalized = str(item or "").strip().upper().replace(".E", "")
        if normalized and normalized not in configured_quotes:
            configured_quotes.append(normalized)

    expected_pairs = []
    for asset in configured_assets:
        if asset not in {"ETH", "BTC", "SOL"}:
            continue
        for quote in configured_quotes:
            if quote not in {"USDT", "USDC"}:
                continue
            expected_pairs.append(f"{asset}/{quote}")

    sample_count_by_pair = defaultdict(int)
    sample_count_by_asset = defaultdict(int)
    for record in records:
        pair_label = _canonical_major_pair_label(record.get("pair_label") or "")
        asset_symbol = canonical_asset_symbol(record.get("asset_symbol") or "")
        if asset_symbol in {"ETH", "BTC", "SOL"}:
            sample_count_by_asset[asset_symbol] += 1
        if pair_label:
            sample_count_by_pair[pair_label] += 1

    active_major_pools = []
    covered_pairs = set()
    for meta in ACTIVE_LP_POOLS.values():
        if not bool(meta.get("is_major_pool")):
            continue
        pair_label = str(meta.get("pair_label") or "")
        canonical_pair_label = _canonical_major_pair_label(pair_label)
        covered_pairs.add(canonical_pair_label)
        active_major_pools.append(
            {
                "pair_label": pair_label,
                "canonical_pair_label": canonical_pair_label,
                "pool_address": str(meta.get("pool_address") or meta.get("address") or ""),
                "priority": int(meta.get("priority") or 3),
                "major_priority_score": float(meta.get("major_priority_score") or 1.0),
                "major_match_mode": str(meta.get("major_match_mode") or ""),
                "trend_pool_match_mode": str(meta.get("trend_pool_match_mode") or ""),
                "is_primary_trend_pool": bool(meta.get("is_primary_trend_pool")),
                "sample_size": int(sample_count_by_pair.get(canonical_pair_label) or 0),
            }
        )

    missing_pairs = [pair for pair in expected_pairs if pair not in covered_pairs]
    covered_expected_pairs = [pair for pair in expected_pairs if pair in covered_pairs]
    missing_assets = [
        asset
        for asset in configured_assets
        if asset in {"ETH", "BTC", "SOL"}
        and not any(pair.startswith(f"{asset}/") and pair in covered_pairs for pair in expected_pairs)
    ]
    under_sampled_assets = [
        {
            "asset_symbol": asset,
            "sample_size": int(sample_count_by_asset.get(asset) or 0),
        }
        for asset in configured_assets
        if asset in {"ETH", "BTC", "SOL"} and int(sample_count_by_asset.get(asset) or 0) < 2
    ]
    under_sampled_pairs = [
        {
            "pair_label": pair,
            "sample_size": int(sample_count_by_pair.get(pair) or 0),
        }
        for pair in expected_pairs
        if int(sample_count_by_pair.get(pair) or 0) < manager.actionable_min_samples
    ]
    major_pair_quality = []
    quality_converging_pairs = []
    for pair in expected_pairs:
        pair_records = [record for record in records if _canonical_major_pair_label(record.get("pair_label") or "") == pair]
        scores = manager._scope_scores(pair_records, lambda _: True)
        payload = {
            "pair_label": pair,
            "covered": pair in covered_pairs,
            "sample_size": int(scores.get("sample_size") or 0),
            "quality_score": round(float(scores.get("quality_score") or 0.0), 4),
            "climax_reversal_score": round(float(scores.get("climax_reversal_score") or 0.0), 4),
            "market_context_alignment_score": round(float(scores.get("market_context_alignment_score") or 0.0), 4),
            "actionable": int(scores.get("sample_size") or 0) >= manager.actionable_min_samples,
        }
        major_pair_quality.append(payload)
        if payload["covered"] and payload["actionable"]:
            quality_converging_pairs.append(payload)

    if not pool_book_exists:
        warnings.append("data/lp_pools.json 不存在；当前只能根据公开仓库默认配置评估 coverage")
        suggestions.append("新增本地私有 pool book 时，优先补 ETH/BTC/SOL 对 USDT/USDC 主池，不扩 long-tail")
    if missing_pairs:
        warnings.append("majors 主池覆盖不完整，下一轮应优先补 majors 而不是 long-tail")
        suggestions.append(f"优先补齐：{', '.join(missing_pairs[:6])}")
    if missing_assets:
        warnings.append(f"缺少 major 资产覆盖：{', '.join(missing_assets)}")
    if not active_major_pools:
        warnings.append("当前没有 active major pools 命中 LP major metadata")
    if under_sampled_assets:
        suggestions.append("当前 majors outcome 样本仍偏少，建议先扩 BTC/SOL/更多 ETH 主池")
    if under_sampled_pairs:
        suggestions.append(
            "样本稀少的 majors pairs："
            + ", ".join(item["pair_label"] for item in under_sampled_pairs[:6])
        )

    return {
        "pool_book_path": str(pool_book_path),
        "pool_book_exists": bool(pool_book_exists),
        "configured_major_assets": configured_assets,
        "configured_major_quotes": configured_quotes,
        "expected_major_pairs": expected_pairs,
        "covered_expected_pairs": covered_expected_pairs,
        "active_major_pools": sorted(active_major_pools, key=lambda item: (-float(item["major_priority_score"]), item["pair_label"])),
        "missing_expected_pairs": missing_pairs,
        "missing_major_assets": missing_assets,
        "under_sampled_major_assets": under_sampled_assets,
        "under_sampled_major_pairs": under_sampled_pairs,
        "major_pair_quality": major_pair_quality,
        "quality_converging_major_pairs": quality_converging_pairs,
        "warnings": warnings,
        "suggestions": suggestions,
    }


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    manager = QualityManager(state_manager=StateManager())

    if args.market_context_health:
        payload = build_market_context_health_report()
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0

    if args.major_pool_coverage:
        payload = build_major_pool_coverage_report(manager)
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0

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
