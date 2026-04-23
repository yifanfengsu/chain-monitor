#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import io
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
DAILY_COMPARE_DIR = REPORTS_DIR / "daily_compare"
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class ReportSpec:
    key: str
    latest_filename: str
    generator_script: str


@dataclass
class SummaryRecord:
    report_type: str
    path: Path
    logical_date: str | None
    data: dict[str, Any]
    source_kind: str
    filename_date: str | None = None
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CandidatePath:
    report_type: str
    path: tuple[str, ...]


@dataclass
class ExtractedValue:
    value: Any
    source: str
    source_path: str
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MetricSpec:
    metric_group: str
    metric_name: str
    direction: str = "count_only"
    candidates: tuple[CandidatePath, ...] = ()
    extractor: Callable[[dict[str, SummaryRecord]], ExtractedValue | None] | None = None


REPORT_SPECS: dict[str, ReportSpec] = {
    "afternoon_evening_state": ReportSpec(
        key="afternoon_evening_state",
        latest_filename="afternoon_evening_state_summary_latest.json",
        generator_script="reports/generate_afternoon_evening_state_analysis_latest.py",
    ),
    "overnight_trade_action": ReportSpec(
        key="overnight_trade_action",
        latest_filename="overnight_trade_action_summary_latest.json",
        generator_script="reports/generate_overnight_trade_action_analysis_latest.py",
    ),
    "overnight_run": ReportSpec(
        key="overnight_run",
        latest_filename="overnight_run_summary_latest.json",
        generator_script="reports/generate_overnight_run_analysis_latest.py",
    ),
}

REPORT_ORDER = (
    "afternoon_evening_state",
    "overnight_trade_action",
    "overnight_run",
)

PRIMARY_DIRECTIONAL_METRICS = {
    "blocker_saved_rate": 5,
    "blocker_false_block_rate": 5,
    "candidate_60s_followthrough_rate": 4,
    "candidate_60s_adverse_rate": 4,
    "candidate_outcome_completion_rate": 4,
    "verified_60s_followthrough_rate": 4,
    "verified_60s_adverse_rate": 4,
    "verified_outcome_completion_rate": 4,
    "outcome_30s_completed_rate": 5,
    "outcome_60s_completed_rate": 5,
    "outcome_300s_completed_rate": 5,
    "market_context_attempt_success_rate": 5,
    "live_public_rate": 4,
    "unavailable_rate": 4,
    "high_value_suppressed_count": 3,
    "legacy_chase_leaked_count": 4,
    "missing_major_pairs_count": 3,
    "current_sample_still_eth_only": 2,
    "mismatch_warning_count": 3,
    "db_archive_mirror_match_rate": 3,
}


def _path(report_type: str, *parts: str) -> CandidatePath:
    return CandidatePath(report_type=report_type, path=tuple(parts))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate daily compare report from structured summary JSON files")
    parser.add_argument("--date", help="Treat DATE as today_date and compare with the closest previous available date")
    parser.add_argument("--strict", action="store_true", help="Fail when today/previous compare inputs are incomplete")
    return parser


def _present(value: Any) -> bool:
    return value is not None and not (isinstance(value, str) and value == "")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_bool(value: Any) -> bool:
    return isinstance(value, bool)


def _json_dumps(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _short_value(value: Any, limit: int = 120) -> str:
    text = _json_dumps(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _format_table_value(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return _short_value(value, limit=80)


def _pct_change(today_value: Any, previous_value: Any) -> float | None:
    if not _is_number(today_value) or not _is_number(previous_value):
        return None
    if float(previous_value) == 0.0:
        return None
    return round((float(today_value) - float(previous_value)) / float(previous_value) * 100.0, 2)


def _abs_change(today_value: Any, previous_value: Any) -> float | None:
    if not _is_number(today_value) or not _is_number(previous_value):
        return None
    return round(float(today_value) - float(previous_value), 6)


def _parse_date(text: str | None) -> str | None:
    raw = str(text or "")
    match = DATE_RE.search(raw)
    return match.group(1) if match else None


def _get_in(payload: Any, path: tuple[str, ...]) -> Any:
    current = payload
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return None
        current = current.get(part)
    return current


def _extract_date_from_payload(data: dict[str, Any], path: Path) -> str | None:
    candidate_paths = (
        ("analysis_window", "end_utc"),
        ("analysis_window", "cutoff_utc"),
        ("analysis_window", "end_beijing"),
        ("run_overview", "analysis_window_end_utc"),
        ("run_overview", "analysis_window_end_beijing"),
    )
    for candidate_path in candidate_paths:
        date_value = _parse_date(_get_in(data, candidate_path))
        if date_value:
            return date_value
    return _parse_date(path.name)


def _compact_data_source_summary(bundle: dict[str, SummaryRecord]) -> ExtractedValue | None:
    for report_type in REPORT_ORDER:
        record = bundle.get(report_type)
        if record is None:
            continue
        summary = record.data.get("data_source_summary")
        if not isinstance(summary, dict):
            continue
        compact = {
            "report_data_source": summary.get("report_data_source"),
            "data_source": summary.get("data_source"),
            "source_components": summary.get("source_components"),
            "archive_fallback_used": summary.get("archive_fallback_used"),
            "db_archive_mirror_match_rate": summary.get("db_archive_mirror_match_rate"),
            "mismatch_warnings": summary.get("mismatch_warnings"),
        }
        return ExtractedValue(
            value=compact,
            source=report_type,
            source_path="data_source_summary",
        )
    return None


def _extract_value(bundle: dict[str, SummaryRecord], candidates: tuple[CandidatePath, ...]) -> ExtractedValue | None:
    for candidate in candidates:
        record = bundle.get(candidate.report_type)
        if record is None:
            continue
        value = _get_in(record.data, candidate.path)
        if _present(value):
            return ExtractedValue(
                value=value,
                source=candidate.report_type,
                source_path=".".join(candidate.path),
            )
    return None


def _extract_ratio_from_paths(
    bundle: dict[str, SummaryRecord],
    *,
    report_type: str,
    numerator_path: tuple[str, ...],
    denominator_path: tuple[str, ...],
    source_path: str,
) -> ExtractedValue | None:
    record = bundle.get(report_type)
    if record is None:
        return None
    numerator = _get_in(record.data, numerator_path)
    denominator = _get_in(record.data, denominator_path)
    if not _is_number(numerator) or not _is_number(denominator) or float(denominator) == 0.0:
        return None
    return ExtractedValue(
        value=round(float(numerator) / float(denominator), 4),
        source=report_type,
        source_path=source_path,
    )


def _extract_len(
    bundle: dict[str, SummaryRecord],
    candidates: tuple[CandidatePath, ...],
    *,
    source_suffix: str = ".len",
) -> ExtractedValue | None:
    extracted = _extract_value(bundle, candidates)
    if extracted is None:
        return None
    if isinstance(extracted.value, (list, tuple, dict, set)):
        size = len(extracted.value)
    else:
        return None
    return ExtractedValue(
        value=size,
        source=extracted.source,
        source_path=f"{extracted.source_path}{source_suffix}",
        notes=list(extracted.notes),
    )


def _extract_market_context_attempt_success_rate(bundle: dict[str, SummaryRecord]) -> ExtractedValue | None:
    extracted = _extract_value(bundle, (_path("afternoon_evening_state", "market_context_health", "window"), _path("overnight_trade_action", "market_context_health", "window"), _path("overnight_run", "market_context_health", "window")))
    if extracted is None or not isinstance(extracted.value, dict):
        return None
    window = extracted.value
    attempts = 0
    successes = 0
    for prefix in ("okx", "kraken"):
        attempt_value = window.get(f"{prefix}_attempts")
        success_value = window.get(f"{prefix}_success")
        if _is_number(attempt_value):
            attempts += int(attempt_value)
        if _is_number(success_value):
            successes += int(success_value)
    if attempts <= 0:
        return None
    return ExtractedValue(
        value=round(successes / attempts, 4),
        source=extracted.source,
        source_path=f"{extracted.source_path}.derived_attempt_success_rate",
    )


def _extract_candidate_completion_rate(bundle: dict[str, SummaryRecord]) -> ExtractedValue | None:
    direct = _extract_value(bundle, (_path("afternoon_evening_state", "candidate_tradeable_summary", "candidate_outcome_completed_rate"),))
    if direct is not None:
        return direct
    return _extract_ratio_from_paths(
        bundle,
        report_type="afternoon_evening_state",
        numerator_path=("trade_opportunity_summary", "candidate_outcome_60s", "resolved_count"),
        denominator_path=("trade_opportunity_summary", "candidate_outcome_60s", "count"),
        source_path="trade_opportunity_summary.candidate_outcome_60s.resolved_count/count",
    )


def _extract_verified_completion_rate(bundle: dict[str, SummaryRecord]) -> ExtractedValue | None:
    return _extract_ratio_from_paths(
        bundle,
        report_type="afternoon_evening_state",
        numerator_path=("trade_opportunity_summary", "verified_outcome_60s", "resolved_count"),
        denominator_path=("trade_opportunity_summary", "verified_outcome_60s", "count"),
        source_path="trade_opportunity_summary.verified_outcome_60s.resolved_count/count",
    ) or _extract_ratio_from_paths(
        bundle,
        report_type="overnight_trade_action",
        numerator_path=("trade_opportunity_summary", "verified_outcome_60s", "resolved_count"),
        denominator_path=("trade_opportunity_summary", "verified_outcome_60s", "count"),
        source_path="trade_opportunity_summary.verified_outcome_60s.resolved_count/count",
    )


def _extract_legacy_chase_leaked_count(bundle: dict[str, SummaryRecord]) -> ExtractedValue | None:
    return _extract_value(
        bundle,
        (_path("overnight_trade_action", "final_trading_output_summary", "legacy_chase_leaked_count"),),
    )


METRIC_SPECS: tuple[MetricSpec, ...] = (
    MetricSpec("run_data_integrity", "raw_events_count", candidates=(_path("afternoon_evening_state", "run_overview", "total_raw_events"),)),
    MetricSpec("run_data_integrity", "parsed_events_count", candidates=(_path("afternoon_evening_state", "run_overview", "total_parsed_events"),)),
    MetricSpec("run_data_integrity", "signals_count", candidates=(_path("afternoon_evening_state", "run_overview", "total_signal_rows"),)),
    MetricSpec("run_data_integrity", "lp_signal_rows", candidates=(_path("afternoon_evening_state", "run_overview", "lp_signal_rows"),)),
    MetricSpec("run_data_integrity", "delivered_lp_signals", candidates=(_path("afternoon_evening_state", "run_overview", "delivered_lp_signals"),)),
    MetricSpec("run_data_integrity", "suppressed_lp_signals", candidates=(_path("afternoon_evening_state", "run_overview", "suppressed_lp_signals"),)),
    MetricSpec("run_data_integrity", "asset_case_count", candidates=(_path("afternoon_evening_state", "run_overview", "asset_case_count"),)),
    MetricSpec("run_data_integrity", "case_followup_count", candidates=(_path("afternoon_evening_state", "run_overview", "case_followup_count"),)),
    MetricSpec("run_data_integrity", "data_source_summary", direction="object", extractor=_compact_data_source_summary),
    MetricSpec("run_data_integrity", "db_archive_mirror_match_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "data_source_summary", "db_archive_mirror_match_rate"), _path("overnight_trade_action", "data_source_summary", "db_archive_mirror_match_rate"), _path("overnight_run", "data_source_summary", "db_archive_mirror_match_rate"))),
    MetricSpec("run_data_integrity", "missing_tables", direction="object", candidates=(_path("afternoon_evening_state", "data_source_summary", "sqlite_health", "missing_tables"), _path("overnight_trade_action", "data_source_summary", "sqlite_health", "missing_tables"), _path("overnight_run", "data_source_summary", "sqlite_health", "missing_tables"))),
    MetricSpec("run_data_integrity", "missing_tables_count", direction="lower_better", extractor=lambda bundle: _extract_len(bundle, (_path("afternoon_evening_state", "data_source_summary", "sqlite_health", "missing_tables"), _path("overnight_trade_action", "data_source_summary", "sqlite_health", "missing_tables"), _path("overnight_run", "data_source_summary", "sqlite_health", "missing_tables")))),
    MetricSpec("run_data_integrity", "mismatch_warnings", direction="object", candidates=(_path("afternoon_evening_state", "data_source_summary", "mismatch_warnings"), _path("overnight_trade_action", "data_source_summary", "mismatch_warnings"), _path("overnight_run", "data_source_summary", "mismatch_warnings"))),
    MetricSpec("run_data_integrity", "mismatch_warning_count", direction="lower_better", extractor=lambda bundle: _extract_len(bundle, (_path("afternoon_evening_state", "data_source_summary", "mismatch_warnings"), _path("overnight_trade_action", "data_source_summary", "mismatch_warnings"), _path("overnight_run", "data_source_summary", "mismatch_warnings")))),
    MetricSpec("telegram_denoise", "telegram_should_send_count", candidates=(_path("afternoon_evening_state", "telegram_suppression_summary", "telegram_should_send_count"),)),
    MetricSpec("telegram_denoise", "telegram_suppressed_count", candidates=(_path("afternoon_evening_state", "telegram_suppression_summary", "telegram_suppressed_count"), _path("overnight_trade_action", "telegram_suppression_summary", "total_suppressed"), _path("overnight_run", "telegram_suppression_summary", "total_suppressed"))),
    MetricSpec("telegram_denoise", "telegram_suppression_ratio", candidates=(_path("afternoon_evening_state", "telegram_suppression_summary", "telegram_suppression_ratio"),)),
    MetricSpec("telegram_denoise", "telegram_suppression_reasons", direction="object", candidates=(_path("afternoon_evening_state", "telegram_suppression_summary", "telegram_suppression_reasons"), _path("overnight_trade_action", "telegram_suppression_summary", "suppression_reasons"), _path("overnight_run", "telegram_suppression_summary", "suppression_reasons"))),
    MetricSpec("telegram_denoise", "messages_before_suppression_estimate", candidates=(_path("afternoon_evening_state", "telegram_suppression_summary", "messages_before_suppression_estimate"), _path("overnight_trade_action", "telegram_suppression_summary", "messages_before_after_suppression_estimate", "raw_lp_signals"), _path("overnight_run", "telegram_suppression_summary", "messages_before_after_suppression_estimate", "raw_lp_signals"))),
    MetricSpec("telegram_denoise", "messages_after_suppression_actual", candidates=(_path("afternoon_evening_state", "telegram_suppression_summary", "messages_after_suppression_actual"), _path("overnight_trade_action", "telegram_suppression_summary", "messages_before_after_suppression_estimate", "sent_telegram_messages"), _path("overnight_run", "telegram_suppression_summary", "messages_before_after_suppression_estimate", "sent_telegram_messages"))),
    MetricSpec("telegram_denoise", "high_value_suppressed_count", direction="lower_better", candidates=(_path("afternoon_evening_state", "telegram_suppression_summary", "high_value_suppressed_count"),)),
    MetricSpec("telegram_denoise", "legacy_chase_leaked_count", direction="lower_better", extractor=_extract_legacy_chase_leaked_count),
    MetricSpec("asset_market_state", "state_distribution", direction="object", candidates=(_path("afternoon_evening_state", "asset_market_state_summary", "state_distribution"), _path("overnight_trade_action", "asset_market_state_summary", "state_distribution"), _path("overnight_run", "asset_market_state_summary", "state_distribution"))),
    MetricSpec("asset_market_state", "state_transition_count", candidates=(_path("afternoon_evening_state", "asset_market_state_summary", "state_transition_count"), _path("overnight_trade_action", "asset_market_state_summary", "state_change_count"), _path("overnight_run", "asset_market_state_summary", "state_change_count"))),
    MetricSpec("asset_market_state", "final_state_by_asset", direction="object", candidates=(_path("afternoon_evening_state", "asset_market_state_summary", "final_state_by_asset"), _path("overnight_trade_action", "asset_market_state_summary", "current_final_state_per_asset"), _path("overnight_run", "asset_market_state_summary", "current_final_state_per_asset"))),
    MetricSpec("asset_market_state", "no_trade_lock_entered_count", candidates=(_path("afternoon_evening_state", "no_trade_lock_summary", "lock_entered_count"), _path("overnight_trade_action", "no_trade_lock_summary", "lock_entered_count"), _path("overnight_run", "no_trade_lock_summary", "lock_entered_count"))),
    MetricSpec("asset_market_state", "no_trade_lock_suppressed_count", candidates=(_path("afternoon_evening_state", "no_trade_lock_summary", "lock_suppressed_count"), _path("overnight_trade_action", "no_trade_lock_summary", "suppressed_count"), _path("overnight_run", "no_trade_lock_summary", "suppressed_count"))),
    MetricSpec("asset_market_state", "no_trade_lock_released_count", candidates=(_path("afternoon_evening_state", "no_trade_lock_summary", "lock_released_count"), _path("overnight_trade_action", "no_trade_lock_summary", "release_count"), _path("overnight_run", "no_trade_lock_summary", "release_count"))),
    MetricSpec("trade_action_opportunity", "trade_action_distribution", direction="object", candidates=(_path("afternoon_evening_state", "trade_action_summary", "trade_action_distribution"), _path("overnight_trade_action", "trade_action_summary", "trade_action_distribution"))),
    MetricSpec("trade_action_opportunity", "opportunity_none_count", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_none_count"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_none_count"))),
    MetricSpec("trade_action_opportunity", "opportunity_candidate_count", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_candidate_count"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_candidate_count"))),
    MetricSpec("trade_action_opportunity", "opportunity_verified_count", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_verified_count"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_verified_count"))),
    MetricSpec("trade_action_opportunity", "opportunity_blocked_count", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_blocked_count"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_blocked_count"))),
    MetricSpec("trade_action_opportunity", "opportunity_invalidated_count", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_invalidated_count"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_invalidated_count"))),
    MetricSpec("trade_action_opportunity", "candidate_to_verified_rate", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_candidate_to_verified_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_candidate_to_verified_rate"))),
    MetricSpec("trade_action_opportunity", "hard_blocker_distribution", direction="object", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_hard_blocker_distribution"), _path("overnight_trade_action", "trade_opportunity_summary", "hard_blocker_distribution"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_hard_blocker_distribution"))),
    MetricSpec("trade_action_opportunity", "verification_blocker_distribution", direction="object", candidates=(_path("overnight_trade_action", "trade_opportunity_summary", "verification_blocker_distribution"),)),
    MetricSpec("trade_action_opportunity", "opportunity_score_median", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_score_median"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_score_median"))),
    MetricSpec("trade_action_opportunity", "opportunity_score_p90", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "opportunity_score_p90"), _path("overnight_trade_action", "trade_opportunity_summary", "opportunity_score_p90"))),
    MetricSpec("trade_action_opportunity", "why_no_verified", direction="object", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "why_no_verified"), _path("overnight_trade_action", "trade_opportunity_summary", "why_no_verified"))),
    MetricSpec("trade_action_opportunity", "why_no_opportunities", direction="object", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "why_no_opportunities"), _path("overnight_trade_action", "trade_opportunity_summary", "why_no_opportunities"))),
    MetricSpec("candidate_verified_posterior", "candidate_30s_followthrough_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "candidate_outcome_30s", "followthrough_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "candidate_outcome_30s", "followthrough_rate"))),
    MetricSpec("candidate_verified_posterior", "candidate_60s_followthrough_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "candidate_outcome_60s", "followthrough_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "candidate_outcome_60s", "followthrough_rate"))),
    MetricSpec("candidate_verified_posterior", "candidate_300s_followthrough_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "candidate_outcome_300s", "followthrough_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "candidate_outcome_300s", "followthrough_rate"))),
    MetricSpec("candidate_verified_posterior", "candidate_30s_adverse_rate", direction="lower_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "candidate_outcome_30s", "adverse_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "candidate_outcome_30s", "adverse_rate"))),
    MetricSpec("candidate_verified_posterior", "candidate_60s_adverse_rate", direction="lower_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "candidate_outcome_60s", "adverse_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "candidate_outcome_60s", "adverse_rate"))),
    MetricSpec("candidate_verified_posterior", "candidate_300s_adverse_rate", direction="lower_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "candidate_outcome_300s", "adverse_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "candidate_outcome_300s", "adverse_rate"))),
    MetricSpec("candidate_verified_posterior", "candidate_outcome_completion_rate", direction="higher_better", extractor=_extract_candidate_completion_rate),
    MetricSpec("candidate_verified_posterior", "verified_30s_followthrough_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "verified_outcome_30s", "followthrough_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "verified_outcome_30s", "followthrough_rate"))),
    MetricSpec("candidate_verified_posterior", "verified_60s_followthrough_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "verified_outcome_60s", "followthrough_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "verified_outcome_60s", "followthrough_rate"))),
    MetricSpec("candidate_verified_posterior", "verified_300s_followthrough_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "verified_outcome_300s", "followthrough_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "verified_outcome_300s", "followthrough_rate"))),
    MetricSpec("candidate_verified_posterior", "verified_30s_adverse_rate", direction="lower_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "verified_outcome_30s", "adverse_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "verified_outcome_30s", "adverse_rate"))),
    MetricSpec("candidate_verified_posterior", "verified_60s_adverse_rate", direction="lower_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "verified_outcome_60s", "adverse_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "verified_outcome_60s", "adverse_rate"))),
    MetricSpec("candidate_verified_posterior", "verified_300s_adverse_rate", direction="lower_better", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "verified_outcome_300s", "adverse_rate"), _path("overnight_trade_action", "trade_opportunity_summary", "verified_outcome_300s", "adverse_rate"))),
    MetricSpec("candidate_verified_posterior", "verified_outcome_completion_rate", direction="higher_better", extractor=_extract_verified_completion_rate),
    MetricSpec("blocker_risk_filter", "blocker_saved_rate", direction="higher_better", candidates=(_path("overnight_trade_action", "trade_opportunity_summary", "blocker_saved_rate"),)),
    MetricSpec("blocker_risk_filter", "blocker_false_block_rate", direction="lower_better", candidates=(_path("overnight_trade_action", "trade_opportunity_summary", "blocker_false_block_rate"),)),
    MetricSpec("blocker_risk_filter", "blocker_distribution", direction="object", candidates=(_path("afternoon_evening_state", "trade_opportunity_summary", "top_blockers"), _path("overnight_trade_action", "trade_opportunity_summary", "top_blockers"))),
    MetricSpec("blocker_risk_filter", "top_effective_blockers", direction="object", candidates=(_path("overnight_trade_action", "trade_opportunity_summary", "blocker_effectiveness", "top_effective_blockers"),)),
    MetricSpec("blocker_risk_filter", "top_overblocking_blockers", direction="object", candidates=(_path("overnight_trade_action", "trade_opportunity_summary", "blocker_effectiveness", "top_overblocking_blockers"),)),
    MetricSpec("prealert", "prealert_candidate_count", candidates=(_path("afternoon_evening_state", "prealert_lifecycle_summary", "prealert_candidate_count"), _path("overnight_trade_action", "prealert_funnel_summary", "prealert_candidate_count"), _path("overnight_run", "prealert_summary", "prealert_candidate_count"))),
    MetricSpec("prealert", "prealert_gate_passed_count", candidates=(_path("afternoon_evening_state", "prealert_lifecycle_summary", "prealert_gate_passed_count"), _path("overnight_trade_action", "prealert_funnel_summary", "prealert_gate_passed_count"), _path("overnight_run", "prealert_summary", "prealert_gate_passed_count"))),
    MetricSpec("prealert", "prealert_active_count", candidates=(_path("afternoon_evening_state", "prealert_lifecycle_summary", "prealert_active_count"), _path("overnight_trade_action", "prealert_lifecycle_summary", "active_count"), _path("overnight_run", "prealert_lifecycle_summary", "active_count"))),
    MetricSpec("prealert", "prealert_delivered_count", candidates=(_path("afternoon_evening_state", "prealert_lifecycle_summary", "prealert_delivered_count"), _path("overnight_trade_action", "prealert_lifecycle_summary", "delivered_count"), _path("overnight_run", "prealert_lifecycle_summary", "delivered_count"))),
    MetricSpec("prealert", "prealert_upgraded_to_confirm_count", candidates=(_path("afternoon_evening_state", "prealert_lifecycle_summary", "prealert_upgraded_to_confirm_count"), _path("overnight_trade_action", "prealert_lifecycle_summary", "upgraded_count"), _path("overnight_run", "prealert_lifecycle_summary", "upgraded_count"))),
    MetricSpec("prealert", "prealert_expired_count", candidates=(_path("afternoon_evening_state", "prealert_lifecycle_summary", "prealert_expired_count"), _path("overnight_trade_action", "prealert_lifecycle_summary", "expired_count"), _path("overnight_run", "prealert_lifecycle_summary", "expired_count"))),
    MetricSpec("prealert", "median_prealert_to_confirm_sec", candidates=(_path("afternoon_evening_state", "prealert_lifecycle_summary", "median_prealert_to_confirm_sec"),)),
    MetricSpec("outcome_integrity", "outcome_30s_completed_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "outcome_30s_completed_rate"), _path("overnight_trade_action", "outcome_price_source_summary", "outcome_30s_completed_rate"))),
    MetricSpec("outcome_integrity", "outcome_60s_completed_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "outcome_60s_completed_rate"), _path("overnight_trade_action", "outcome_price_source_summary", "outcome_60s_completed_rate"))),
    MetricSpec("outcome_integrity", "outcome_300s_completed_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "outcome_300s_completed_rate"), _path("overnight_trade_action", "outcome_price_source_summary", "outcome_300s_completed_rate"))),
    MetricSpec("outcome_integrity", "outcome_price_source_distribution", direction="object", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "outcome_price_source_distribution"), _path("overnight_trade_action", "outcome_price_source_summary", "outcome_price_source_distribution"))),
    MetricSpec("outcome_integrity", "expired_rate_by_window", direction="object", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "expired_rate_by_window"),)),
    MetricSpec("outcome_integrity", "outcome_failure_reason_distribution", direction="object", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "outcome_failure_reason_distribution"), _path("overnight_trade_action", "outcome_price_source_summary", "outcome_failure_reason_distribution"))),
    MetricSpec("outcome_integrity", "catchup_completed_count", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "catchup_completed_count"), _path("overnight_trade_action", "outcome_price_source_summary", "catchup_completed_count"))),
    MetricSpec("outcome_integrity", "catchup_expired_count", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "catchup_expired_count"), _path("overnight_trade_action", "outcome_price_source_summary", "catchup_expired_count"))),
    MetricSpec("outcome_integrity", "scheduler_health_summary", direction="object", candidates=(_path("afternoon_evening_state", "outcome_source_summary", "scheduler_health_summary"), _path("overnight_trade_action", "outcome_price_source_summary", "scheduler_health_summary"))),
    MetricSpec("market_context", "live_public_rate", direction="higher_better", candidates=(_path("afternoon_evening_state", "market_context_health", "window", "live_public_rate"), _path("overnight_trade_action", "market_context_health", "window", "live_public_rate"), _path("overnight_run", "market_context_health", "window", "live_public_rate"))),
    MetricSpec("market_context", "unavailable_rate", direction="lower_better", candidates=(_path("afternoon_evening_state", "market_context_health", "window", "unavailable_rate"), _path("overnight_trade_action", "market_context_health", "window", "unavailable_rate"), _path("overnight_run", "market_context_health", "window", "unavailable_rate"))),
    MetricSpec("market_context", "okx_success", candidates=(_path("afternoon_evening_state", "market_context_health", "window", "okx_success"), _path("overnight_trade_action", "market_context_health", "window", "okx_success"), _path("overnight_run", "market_context_health", "window", "okx_success"))),
    MetricSpec("market_context", "kraken_success", candidates=(_path("afternoon_evening_state", "market_context_health", "window", "kraken_success"), _path("overnight_trade_action", "market_context_health", "window", "kraken_success"), _path("overnight_run", "market_context_health", "window", "kraken_success"))),
    MetricSpec("market_context", "market_context_attempt_success_rate", direction="higher_better", extractor=_extract_market_context_attempt_success_rate),
    MetricSpec("market_context", "resolved_symbol_distribution", direction="object", candidates=(_path("afternoon_evening_state", "market_context_health", "window", "resolved_symbol_distribution"), _path("overnight_trade_action", "market_context_health", "window", "resolved_symbol_distribution"), _path("overnight_run", "market_context_health", "window", "resolved_symbol_distribution"))),
    MetricSpec("market_context", "top_failure_reasons", direction="object", candidates=(_path("afternoon_evening_state", "market_context_health", "window", "top_failure_reasons"), _path("overnight_trade_action", "market_context_health", "window", "top_failure_reasons"), _path("overnight_run", "market_context_health", "window", "top_failure_reasons"))),
    MetricSpec("majors_coverage", "covered_major_pairs", direction="object", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "covered_major_pairs"), _path("overnight_trade_action", "majors_coverage_summary", "covered_major_pairs"), _path("overnight_run", "majors_coverage_summary", "covered_major_pairs"))),
    MetricSpec("majors_coverage", "configured_but_disabled_major_pools", direction="object", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "major_cli_summary", "configured_but_disabled_major_pools"), _path("overnight_trade_action", "majors_coverage_summary", "major_cli_summary", "configured_but_disabled_major_pools"), _path("overnight_run", "majors_coverage_summary", "major_cli_summary", "configured_but_disabled_major_pools"))),
    MetricSpec("majors_coverage", "configured_but_unsupported_chain", direction="object", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "major_cli_summary", "configured_but_unsupported_chain"), _path("overnight_trade_action", "majors_coverage_summary", "major_cli_summary", "configured_but_unsupported_chain"), _path("overnight_run", "majors_coverage_summary", "major_cli_summary", "configured_but_unsupported_chain"))),
    MetricSpec("majors_coverage", "configured_but_validation_failed", direction="object", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "major_cli_summary", "configured_but_validation_failed"), _path("overnight_trade_action", "majors_coverage_summary", "major_cli_summary", "configured_but_validation_failed"), _path("overnight_run", "majors_coverage_summary", "major_cli_summary", "configured_but_validation_failed"))),
    MetricSpec("majors_coverage", "missing_major_pairs", direction="object", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "missing_major_pairs"), _path("overnight_trade_action", "majors_coverage_summary", "missing_major_pairs"), _path("overnight_run", "majors_coverage_summary", "missing_major_pairs"))),
    MetricSpec("majors_coverage", "missing_major_pairs_count", direction="lower_better", extractor=lambda bundle: _extract_len(bundle, (_path("afternoon_evening_state", "majors_coverage_summary", "missing_major_pairs"), _path("overnight_trade_action", "majors_coverage_summary", "missing_major_pairs"), _path("overnight_run", "majors_coverage_summary", "missing_major_pairs")))),
    MetricSpec("majors_coverage", "eth_signal_count", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "eth_signal_count"), _path("overnight_trade_action", "majors_coverage_summary", "eth_signal_count"), _path("overnight_run", "majors_coverage_summary", "eth_signal_count"))),
    MetricSpec("majors_coverage", "btc_signal_count", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "btc_signal_count"), _path("overnight_trade_action", "majors_coverage_summary", "btc_signal_count"), _path("overnight_run", "majors_coverage_summary", "btc_signal_count"))),
    MetricSpec("majors_coverage", "sol_signal_count", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "sol_signal_count"), _path("overnight_trade_action", "majors_coverage_summary", "sol_signal_count"), _path("overnight_run", "majors_coverage_summary", "sol_signal_count"))),
    MetricSpec("majors_coverage", "current_sample_still_eth_only", direction="lower_better", candidates=(_path("afternoon_evening_state", "majors_coverage_summary", "current_sample_still_eth_only"), _path("overnight_trade_action", "majors_coverage_summary", "current_sample_still_eth_only"), _path("overnight_run", "majors_coverage_summary", "current_sample_still_eth_only"))),
)


def discover_summary_inventory(reports_dir: Path) -> dict[str, list[SummaryRecord]]:
    inventory: dict[str, list[SummaryRecord]] = {key: [] for key in REPORT_SPECS}
    for report_type, spec in REPORT_SPECS.items():
        latest_path = reports_dir / spec.latest_filename
        candidate_paths: list[Path] = []
        if latest_path.exists():
            candidate_paths.append(latest_path)
        candidate_paths.extend(sorted(reports_dir.glob(f"{latest_path.stem}_*.json")))
        seen: set[Path] = set()
        for path in candidate_paths:
            resolved = path.resolve()
            if resolved in seen or not path.is_file():
                continue
            seen.add(resolved)
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(data, dict):
                continue
            filename_date = _parse_date(path.name)
            logical_date = _extract_date_from_payload(data, path)
            source_kind = "latest_summary_json" if path.name == spec.latest_filename else "dated_summary_json"
            warnings: list[str] = []
            if filename_date and logical_date and filename_date != logical_date:
                warnings.append(f"filename_date_mismatch:{filename_date}!={logical_date}")
            inventory[report_type].append(
                SummaryRecord(
                    report_type=report_type,
                    path=path,
                    logical_date=logical_date,
                    data=data,
                    source_kind=source_kind,
                    filename_date=filename_date,
                    warnings=warnings,
                )
            )
    return inventory


def _all_available_dates(inventory: dict[str, list[SummaryRecord]]) -> list[str]:
    dates = {
        record.logical_date
        for records in inventory.values()
        for record in records
        if record.logical_date
    }
    return sorted(dates)


def select_compare_dates(
    inventory: dict[str, list[SummaryRecord]],
    *,
    requested_date: str | None = None,
) -> dict[str, Any]:
    available_dates = _all_available_dates(inventory)
    if requested_date:
        today_date = requested_date
        previous_candidates = [value for value in available_dates if value < requested_date]
        previous_date = previous_candidates[-1] if previous_candidates else None
    else:
        today_date = available_dates[-1] if available_dates else None
        previous_date = available_dates[-2] if len(available_dates) >= 2 else None
    if today_date and previous_date:
        compare_basis = "exact_previous_day" if datetime.fromisoformat(today_date) - datetime.fromisoformat(previous_date) == timedelta(days=1) else "previous_available_day"
    elif today_date:
        compare_basis = "not_available"
    else:
        compare_basis = "no_data"
    return {
        "today_date": today_date,
        "previous_date": previous_date,
        "compare_basis": compare_basis,
        "available_dates": available_dates,
    }


def _pick_best_record(records: list[SummaryRecord], logical_date: str) -> SummaryRecord | None:
    candidates = [record for record in records if record.logical_date == logical_date]
    if not candidates:
        return None

    def _priority(record: SummaryRecord) -> tuple[int, float, str]:
        filename_matches = record.filename_date == logical_date
        if record.source_kind == "dated_summary_json" and filename_matches:
            rank = 0
        elif record.source_kind == "latest_summary_json":
            rank = 1
        elif record.source_kind == "dated_summary_json":
            rank = 2
        else:
            rank = 3
        try:
            mtime_key = -record.path.stat().st_mtime
        except OSError:
            mtime_key = 0.0
        return (rank, mtime_key, record.path.name)

    return sorted(candidates, key=_priority)[0]


def load_date_bundle(
    inventory: dict[str, list[SummaryRecord]],
    *,
    logical_date: str | None,
) -> tuple[dict[str, SummaryRecord], list[str]]:
    bundle: dict[str, SummaryRecord] = {}
    limitations: list[str] = []
    if not logical_date:
        limitations.append("date_selection_failed:no_logical_date")
        return bundle, limitations
    for report_type in REPORT_ORDER:
        record = _pick_best_record(inventory.get(report_type, []), logical_date)
        if record is None:
            limitations.append(f"missing_summary:{report_type}:{logical_date}")
            continue
        bundle[report_type] = record
        limitations.extend(record.warnings)
    return bundle, limitations


def _refresh_latest_reports(
    *,
    project_root: Path,
    reports_dir: Path,
    report_types: tuple[str, ...] = REPORT_ORDER,
) -> list[str]:
    warnings: list[str] = []
    for report_type in report_types:
        spec = REPORT_SPECS[report_type]
        latest_path = reports_dir / spec.latest_filename
        if latest_path.exists():
            continue
        script_path = project_root / spec.generator_script
        if not script_path.exists():
            warnings.append(f"generator_missing:{report_type}")
            continue
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            warnings.append(f"generator_failed:{report_type}:{result.returncode}:{stderr[:180]}")
    return warnings


def _compare_metric(spec: MetricSpec, today_bundle: dict[str, SummaryRecord], previous_bundle: dict[str, SummaryRecord]) -> dict[str, Any]:
    today_extracted = spec.extractor(today_bundle) if spec.extractor else _extract_value(today_bundle, spec.candidates)
    previous_extracted = spec.extractor(previous_bundle) if spec.extractor else _extract_value(previous_bundle, spec.candidates)
    today_value = today_extracted.value if today_extracted else None
    previous_value = previous_extracted.value if previous_extracted else None
    notes: list[str] = []
    if today_extracted:
        notes.extend(today_extracted.notes)
    if previous_extracted:
        notes.extend(previous_extracted.notes)
    if today_extracted and previous_extracted and today_extracted.source_path != previous_extracted.source_path:
        notes.append(f"source_path_changed:{previous_extracted.source_path}->{today_extracted.source_path}")
    if today_extracted and previous_extracted and today_extracted.source != previous_extracted.source:
        notes.append(f"source_report_changed:{previous_extracted.source}->{today_extracted.source}")

    classification = "unchanged"
    interpretation = ""
    abs_change = None
    pct_change = None

    if today_extracted is None and previous_extracted is None:
        classification = "insufficient"
        interpretation = "today/previous 字段都缺失，无法判断"
    elif today_extracted is None:
        classification = "insufficient"
        interpretation = "today 字段缺失，无法判断"
    elif previous_extracted is None:
        classification = "insufficient"
        interpretation = "previous 字段缺失，无法判断"
    else:
        if _is_number(today_value) and _is_number(previous_value):
            abs_change = _abs_change(today_value, previous_value)
            pct_change = _pct_change(today_value, previous_value)
            if abs_change == 0:
                classification = "unchanged"
                interpretation = "与 previous 持平"
            elif spec.direction == "higher_better":
                classification = "improvement" if float(today_value) > float(previous_value) else "regression"
                interpretation = "改善：指标上升" if classification == "improvement" else "退步：指标下降"
            elif spec.direction == "lower_better":
                classification = "improvement" if float(today_value) < float(previous_value) else "regression"
                interpretation = "改善：指标下降" if classification == "improvement" else "退步：指标上升"
            else:
                classification = "changed"
                interpretation = "数量变化；仅表示规模/活动变化，不直接代表质量改善"
        elif _is_bool(today_value) and _is_bool(previous_value):
            if today_value == previous_value:
                classification = "unchanged"
                interpretation = "布尔状态未变化"
            elif spec.direction == "lower_better":
                classification = "improvement" if not today_value and previous_value else "regression"
                interpretation = "改善：限制状态消失" if classification == "improvement" else "退步：限制状态出现"
            elif spec.direction == "higher_better":
                classification = "improvement" if today_value and not previous_value else "regression"
                interpretation = "改善：可用状态出现" if classification == "improvement" else "退步：可用状态消失"
            else:
                classification = "changed"
                interpretation = "状态变化；需结合上下文解读"
        else:
            if type(today_value) is not type(previous_value):
                classification = "schema_drift"
                interpretation = "schema drift / 字段类型不一致，无法严格比较"
            elif _json_dumps(today_value) == _json_dumps(previous_value):
                classification = "unchanged"
                interpretation = "结构化内容未变化"
            else:
                classification = "changed"
                interpretation = "结构化内容变化；需人工查看差异"

    return {
        "metric_group": spec.metric_group,
        "metric_name": spec.metric_name,
        "today_value": today_value,
        "previous_value": previous_value,
        "abs_change": abs_change,
        "pct_change": pct_change,
        "classification": classification,
        "direction": spec.direction,
        "interpretation": interpretation,
        "data_source": {
            "today": {
                "report_type": today_extracted.source if today_extracted else None,
                "path": today_extracted.source_path if today_extracted else None,
            },
            "previous": {
                "report_type": previous_extracted.source if previous_extracted else None,
                "path": previous_extracted.source_path if previous_extracted else None,
            },
        },
        "notes": sorted(set(note for note in notes if note)),
    }


def build_core_metric_rows(today_bundle: dict[str, SummaryRecord], previous_bundle: dict[str, SummaryRecord]) -> list[dict[str, Any]]:
    return [_compare_metric(spec, today_bundle, previous_bundle) for spec in METRIC_SPECS]


def _index_metric(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["metric_name"]: row for row in rows}


def _flag_rows(rows: list[dict[str, Any]], classification: str) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("classification") == classification]


def _score_row(row: dict[str, Any]) -> int:
    return PRIMARY_DIRECTIONAL_METRICS.get(str(row.get("metric_name") or ""), 1)


def _top_flag(rows: list[dict[str, Any]], classification: str) -> dict[str, Any] | None:
    candidates = _flag_rows(rows, classification)
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda row: (-_score_row(row), str(row.get("metric_name") or "")),
    )[0]


def _metric_delta_text(row: dict[str, Any]) -> str:
    metric_name = str(row.get("metric_name") or "")
    previous_text = _format_table_value(row.get("previous_value"))
    today_text = _format_table_value(row.get("today_value"))
    pct_change = row.get("pct_change")
    suffix = f" ({pct_change:+.2f}%)" if _is_number(pct_change) else ""
    return f"{metric_name}: {previous_text} -> {today_text}{suffix}"


def _metric_value(rows_by_name: dict[str, dict[str, Any]], metric_name: str) -> dict[str, Any] | None:
    return rows_by_name.get(metric_name)


def _metric_json_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _summarize_reason_object(value: Any) -> str:
    if isinstance(value, dict) and value:
        top_items = sorted(value.items(), key=lambda item: (-int(item[1]) if _is_number(item[1]) else 0, str(item[0])))[:3]
        return ", ".join(f"{key}={_format_table_value(item_value)}" for key, item_value in top_items)
    if isinstance(value, list) and value:
        return ", ".join(str(item) for item in value[:3])
    return "无明确结构化原因"


def build_question_answers(rows: list[dict[str, Any]], *, today_date: str, previous_date: str | None, compare_basis: str) -> dict[str, str]:
    rows_by_name = _index_metric(rows)
    improvements = _flag_rows(rows, "improvement")
    regressions = _flag_rows(rows, "regression")
    weighted_improvement = sum(_score_row(row) for row in improvements)
    weighted_regression = sum(_score_row(row) for row in regressions)
    comparable_directional_rows = [
        row
        for row in rows
        if row.get("metric_name") in PRIMARY_DIRECTIONAL_METRICS
        and row.get("classification") in {"improvement", "regression", "unchanged"}
    ]
    changed_directional_rows = [
        row
        for row in comparable_directional_rows
        if row.get("classification") in {"improvement", "regression"}
    ]

    if not previous_date:
        insufficient = f"无法判断；缺少 {today_date} 之前的 previous_date 数据。"
        return {str(index): insufficient for index in range(1, 14)}

    if len(changed_directional_rows) < 2:
        comparable_names = ", ".join(row["metric_name"] for row in comparable_directional_rows[:6]) or "无"
        overall = f"证据不足，无法把整体明确判定为进步/持平/退步；当前可直接判定的方向性指标太少，仅有 {comparable_names}。"
    elif not improvements and not regressions:
        overall = "持平；可判定的方向性指标没有出现明确变化。"
    elif weighted_improvement >= weighted_regression + 4:
        evidence = "; ".join(_metric_delta_text(row) for row in sorted(improvements, key=lambda item: (-_score_row(item), item["metric_name"]))[:3])
        overall = f"有进步；主要依据是 {evidence}。"
    elif weighted_regression >= weighted_improvement + 4:
        evidence = "; ".join(_metric_delta_text(row) for row in sorted(regressions, key=lambda item: (-_score_row(item), item["metric_name"]))[:3])
        overall = f"有退步；主要依据是 {evidence}。"
    else:
        evidence = "; ".join(_metric_delta_text(row) for row in sorted(improvements + regressions, key=lambda item: (-_score_row(item), item["metric_name"]))[:4])
        overall = f"持平偏混合；方向性证据互相抵消，主要变化包括 {evidence}。"

    telegram_after = _metric_value(rows_by_name, "messages_after_suppression_actual")
    high_value = _metric_value(rows_by_name, "high_value_suppressed_count")
    suppression_ratio = _metric_value(rows_by_name, "telegram_suppression_ratio")
    after_comparable = telegram_after and telegram_after.get("classification") in {"changed", "unchanged"}
    high_value_comparable = high_value and high_value.get("classification") in {"improvement", "regression", "unchanged"}
    if after_comparable and high_value_comparable:
        after_today = telegram_after["today_value"]
        after_previous = telegram_after["previous_value"]
        high_today = high_value.get("today_value")
        high_previous = high_value.get("previous_value")
        ratio_text = _metric_delta_text(suppression_ratio) if suppression_ratio else ""
        if _is_number(after_today) and _is_number(after_previous) and after_today < after_previous and (not _is_number(high_today) or not _is_number(high_previous) or high_today <= high_previous):
            telegram_answer = f"Telegram 噪音变少了；实际发出消息 {after_previous} -> {after_today}，高价值误抑制 {high_previous} -> {high_today}。{ratio_text}".strip()
        elif _is_number(after_today) and _is_number(after_previous) and after_today > after_previous:
            telegram_answer = f"Telegram 噪音变多了；实际发出消息 {after_previous} -> {after_today}。{ratio_text}".strip()
        else:
            telegram_answer = f"Telegram 噪音变化证据混合；{_metric_delta_text(telegram_after)}，高价值误抑制 {high_previous} -> {high_today}。"
    elif after_comparable:
        telegram_answer = f"实际发出消息 {_format_table_value(telegram_after.get('previous_value'))} -> {_format_table_value(telegram_after.get('today_value'))}，但缺少高价值误抑制或 suppression previous 数据，不足以严格判断 Telegram 噪音是否改善。"
    else:
        telegram_answer = "证据不足；缺少 messages_after_suppression_actual 或 high_value_suppressed_count。"

    candidate_count = _metric_value(rows_by_name, "opportunity_candidate_count")
    candidate_ft = _metric_value(rows_by_name, "candidate_60s_followthrough_rate")
    candidate_adverse = _metric_value(rows_by_name, "candidate_60s_adverse_rate")
    candidate_completion = _metric_value(rows_by_name, "candidate_outcome_completion_rate")
    count_comparable = candidate_count and candidate_count.get("classification") in {"changed", "unchanged"}
    quality_candidates = [
        row
        for row in (candidate_ft, candidate_adverse, candidate_completion)
        if row and row.get("classification") in {"improvement", "regression", "unchanged"}
    ]
    if count_comparable and quality_candidates:
        count_text = _metric_delta_text(candidate_count)
        quality_bits: list[str] = []
        if candidate_ft and candidate_ft.get("classification") == "improvement":
            quality_bits.append(_metric_delta_text(candidate_ft))
        elif candidate_ft and candidate_ft.get("classification") == "regression":
            quality_bits.append(_metric_delta_text(candidate_ft))
        if candidate_adverse and candidate_adverse.get("classification") == "improvement":
            quality_bits.append(_metric_delta_text(candidate_adverse))
        elif candidate_adverse and candidate_adverse.get("classification") == "regression":
            quality_bits.append(_metric_delta_text(candidate_adverse))
        if candidate_completion and candidate_completion.get("classification") == "improvement":
            quality_bits.append(_metric_delta_text(candidate_completion))
        elif candidate_completion and candidate_completion.get("classification") == "regression":
            quality_bits.append(_metric_delta_text(candidate_completion))
        if not quality_bits:
            candidate_answer = f"Candidate 数量变化为 {count_text}；质量指标没有明显方向性变化。"
        else:
            candidate_answer = f"Candidate 数量变化为 {count_text}；质量相关证据为 {'; '.join(quality_bits)}。"
    elif count_comparable:
        candidate_answer = f"Candidate 数量变化为 {_metric_delta_text(candidate_count)}；但 followthrough/adverse/completion 的 previous 数据不足，无法严格判断质量提升或下降。"
    else:
        candidate_answer = "Candidate 数量和质量的 day-over-day 证据都不足。"

    verified_count = _metric_value(rows_by_name, "opportunity_verified_count")
    why_no_opportunities = _metric_value(rows_by_name, "why_no_opportunities")
    verification_blockers = _metric_value(rows_by_name, "verification_blocker_distribution")
    hard_blockers = _metric_value(rows_by_name, "hard_blocker_distribution")
    if verified_count and _is_number(verified_count.get("today_value")) and int(verified_count["today_value"]) > 0:
        verified_answer = f"今天出现了 verified；{_metric_delta_text(verified_count)}。"
    else:
        reason_value = None
        if why_no_opportunities and _present(why_no_opportunities.get("today_value")):
            reason_value = why_no_opportunities.get("today_value")
        elif verification_blockers and _present(verification_blockers.get("today_value")):
            reason_value = verification_blockers.get("today_value")
        elif hard_blockers and _present(hard_blockers.get("today_value")):
            reason_value = hard_blockers.get("today_value")
        verified_answer = f"今天没有 verified；主要结构化原因是 {_summarize_reason_object(reason_value)}。"

    blocker_saved = _metric_value(rows_by_name, "blocker_saved_rate")
    blocker_false = _metric_value(rows_by_name, "blocker_false_block_rate")
    blocker_saved_comparable = blocker_saved and blocker_saved.get("classification") in {"improvement", "regression", "unchanged"}
    blocker_false_comparable = blocker_false and blocker_false.get("classification") in {"improvement", "regression", "unchanged"}
    if blocker_saved_comparable and blocker_false_comparable:
        if blocker_saved.get("classification") == "improvement" and blocker_false.get("classification") == "improvement":
            blocker_answer = f"Blocker 更有效；{_metric_delta_text(blocker_saved)}，同时 {_metric_delta_text(blocker_false)}。"
        elif blocker_false.get("classification") == "regression":
            blocker_answer = f"Blocker 有过保守风险；{_metric_delta_text(blocker_false)}。"
        else:
            blocker_answer = f"Blocker 效果基本持平；{_metric_delta_text(blocker_saved)}，{_metric_delta_text(blocker_false)}。"
    elif blocker_saved_comparable or blocker_false_comparable:
        blocker_answer = "Blocker 有部分结构化证据，但 today/previous 不完整，不足以严格判断更有效还是更保守。"
    else:
        blocker_answer = "证据不足；缺少 blocker_saved_rate 或 blocker_false_block_rate。"

    outcome_rows = [rows_by_name.get(name) for name in ("outcome_30s_completed_rate", "outcome_60s_completed_rate", "outcome_300s_completed_rate")]
    comparable_outcomes = [row for row in outcome_rows if row and row.get("classification") in {"improvement", "regression", "unchanged"}]
    if comparable_outcomes:
        outcome_answer = "；".join(_metric_delta_text(row) for row in comparable_outcomes)
        if any(row.get("classification") == "improvement" for row in comparable_outcomes) and not any(row.get("classification") == "regression" for row in comparable_outcomes):
            outcome_answer = f"30s/60s/300s outcome 完整性改善；{outcome_answer}。"
        elif any(row.get("classification") == "regression" for row in comparable_outcomes) and not any(row.get("classification") == "improvement" for row in comparable_outcomes):
            outcome_answer = f"30s/60s/300s outcome 完整性退步；{outcome_answer}。"
        else:
            outcome_answer = f"30s/60s/300s outcome 完整性变化混合；{outcome_answer}。"
    else:
        outcome_answer = "证据不足；缺少 outcome completed rate。"

    market_context = _metric_value(rows_by_name, "market_context_attempt_success_rate")
    if market_context:
        if market_context.get("classification") == "improvement":
            market_context_answer = f"Market context 命中率改善；{_metric_delta_text(market_context)}。"
        elif market_context.get("classification") == "regression":
            market_context_answer = f"Market context 命中率退步；{_metric_delta_text(market_context)}。"
        else:
            market_context_answer = f"Market context 命中率基本持平；{_metric_delta_text(market_context)}。"
    else:
        market_context_answer = "证据不足；缺少 market_context_attempt_success_rate。"

    covered_pairs = _metric_value(rows_by_name, "covered_major_pairs")
    btc_count = _metric_value(rows_by_name, "btc_signal_count")
    sol_count = _metric_value(rows_by_name, "sol_signal_count")
    coverage_bits: list[str] = []
    if covered_pairs:
        coverage_bits.append(_metric_delta_text(covered_pairs))
    if btc_count:
        coverage_bits.append(_metric_delta_text(btc_count))
    if sol_count:
        coverage_bits.append(_metric_delta_text(sol_count))
    majors_answer = "；".join(coverage_bits) if coverage_bits else "证据不足；缺少 majors coverage 字段。"

    eth_only = _metric_value(rows_by_name, "current_sample_still_eth_only")
    if eth_only:
        if eth_only.get("classification") in {"regression", "improvement", "unchanged"}:
            if bool(eth_only.get("today_value")):
                eth_only_answer = f"今天仍然主要只在 ETH 上学习；current_sample_still_eth_only={eth_only.get('today_value')}。"
            else:
                eth_only_answer = f"今天不再是纯 ETH 学习样本；current_sample_still_eth_only={eth_only.get('today_value')}。"
        else:
            eth_only_answer = f"今天的样本状态是 current_sample_still_eth_only={eth_only.get('today_value')}，但 previous 对照不足。"
    else:
        eth_only_answer = "证据不足；缺少 current_sample_still_eth_only。"

    top_improvement = _top_flag(rows, "improvement")
    top_regression = _top_flag(rows, "regression")
    improvement_answer = _metric_delta_text(top_improvement) if top_improvement else "证据不足；没有足够强的方向性改善指标。"
    regression_answer = _metric_delta_text(top_regression) if top_regression else "证据不足；没有足够强的方向性退步指标。"

    if top_regression:
        next_action = f"明天最优先修 {top_regression['metric_name']} 对应链路；当前最明显退步是 {_metric_delta_text(top_regression)}。"
    elif verified_count and _is_number(verified_count.get("today_value")) and int(verified_count["today_value"]) == 0 and why_no_opportunities and _present(why_no_opportunities.get("today_value")):
        next_action = f"明天最优先处理 verified 缺失的主因：{_summarize_reason_object(why_no_opportunities.get('today_value'))}。"
    else:
        next_action = f"明天先补齐 {compare_basis} 之外仍缺失的结构化字段，并继续观察 {_metric_delta_text(top_improvement) if top_improvement else '关键质量指标'}。"

    return {
        "1": overall,
        "2": f"判断依据基于 compare_basis={compare_basis} 的结构化 JSON 指标，不使用 markdown 正文字符串比较。",
        "3": telegram_answer,
        "4": candidate_answer,
        "5": verified_answer,
        "6": blocker_answer,
        "7": outcome_answer,
        "8": market_context_answer,
        "9": majors_answer,
        "10": eth_only_answer,
        "11": improvement_answer,
        "12": regression_answer,
        "13": next_action,
    }


def _bundle_source_files(bundle: dict[str, SummaryRecord]) -> dict[str, dict[str, Any]]:
    return {
        report_type: {
            "path": str(record.path.relative_to(ROOT) if record.path.is_absolute() and record.path.is_relative_to(ROOT) else record.path),
            "source_kind": record.source_kind,
            "logical_date": record.logical_date,
            "warnings": list(record.warnings),
        }
        for report_type, record in bundle.items()
    }


def _aggregate_data_source_summary(bundle: dict[str, SummaryRecord]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for report_type, record in bundle.items():
        summary = record.data.get("data_source_summary")
        if not isinstance(summary, dict):
            payload[report_type] = {"available": False}
            continue
        payload[report_type] = {
            "available": True,
            "path": str(record.path.relative_to(ROOT) if record.path.is_absolute() and record.path.is_relative_to(ROOT) else record.path),
            "source_kind": record.source_kind,
            "report_data_source": summary.get("report_data_source"),
            "data_source": summary.get("data_source"),
            "source_components": summary.get("source_components"),
            "archive_fallback_used": summary.get("archive_fallback_used"),
            "db_archive_mirror_match_rate": summary.get("db_archive_mirror_match_rate"),
            "mismatch_warnings": summary.get("mismatch_warnings"),
        }
    return payload


def _build_limitations(
    rows: list[dict[str, Any]],
    *,
    today_bundle: dict[str, SummaryRecord],
    previous_bundle: dict[str, SummaryRecord],
    base_limitations: list[str],
) -> list[str]:
    limitations = list(base_limitations)
    missing_metrics = [row["metric_name"] for row in rows if row.get("classification") == "insufficient"]
    schema_drift_metrics = [row["metric_name"] for row in rows if row.get("classification") == "schema_drift"]
    if missing_metrics:
        limitations.append(f"missing_metrics={','.join(sorted(missing_metrics)[:20])}")
    if schema_drift_metrics:
        limitations.append(f"schema_drift_metrics={','.join(sorted(schema_drift_metrics)[:20])}")
    for bundle_name, bundle in (("today", today_bundle), ("previous", previous_bundle)):
        for report_type, record in bundle.items():
            source_limitations = record.data.get("limitations")
            if isinstance(source_limitations, list):
                for item in source_limitations[:5]:
                    limitations.append(f"{bundle_name}:{report_type}:{item}")
    return sorted(set(item for item in limitations if item))


def _build_key_findings(rows: list[dict[str, Any]], answers: dict[str, str]) -> list[str]:
    findings = [answers["1"], answers["3"], answers["4"], answers["5"], answers["8"], answers["9"]]
    return [item for item in findings if item][:8]


def _build_key_risks(rows: list[dict[str, Any]], answers: dict[str, str]) -> list[str]:
    risks: list[str] = []
    top_regression = _top_flag(rows, "regression")
    if top_regression:
        risks.append(f"Top regression: {_metric_delta_text(top_regression)}")
    for key in ("5", "6", "7", "10", "12"):
        answer = answers.get(key)
        if answer:
            risks.append(answer)
    return risks[:8]


def _build_next_actions(rows: list[dict[str, Any]], answers: dict[str, str]) -> list[str]:
    actions = [answers.get("13", "")]
    top_regression = _top_flag(rows, "regression")
    if top_regression and top_regression["metric_name"] != "missing_major_pairs_count":
        actions.append(f"修复 {top_regression['metric_name']} 对应的采集/筛选/后验链路。")
    missing_pairs = _top_flag([row for row in rows if row.get("metric_name") == "missing_major_pairs_count"], "regression")
    if missing_pairs:
        actions.append("继续补 majors 覆盖，优先看 BTC/SOL 仍缺哪些 pair。")
    return [item for item in actions if item][:6]


def _status_csv(compare_available: bool, today_date: str | None, previous_date: str | None, compare_basis: str, note: str) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "metric_group",
            "metric_name",
            "today_value",
            "previous_value",
            "abs_change",
            "pct_change",
            "interpretation",
            "data_source",
            "notes",
        ],
    )
    writer.writeheader()
    writer.writerow(
        {
            "metric_group": "status",
            "metric_name": "compare_available",
            "today_value": str(compare_available).lower(),
            "previous_value": previous_date or "",
            "abs_change": "",
            "pct_change": "",
            "interpretation": f"today_date={today_date or 'n/a'} compare_basis={compare_basis}",
            "data_source": "daily_compare",
            "notes": note,
        }
    )
    return output.getvalue()


def _rows_to_csv(rows: list[dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "metric_group",
            "metric_name",
            "today_value",
            "previous_value",
            "abs_change",
            "pct_change",
            "interpretation",
            "data_source",
            "notes",
        ],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "metric_group": row["metric_group"],
                "metric_name": row["metric_name"],
                "today_value": _json_dumps(row.get("today_value")),
                "previous_value": _json_dumps(row.get("previous_value")),
                "abs_change": row.get("abs_change", ""),
                "pct_change": row.get("pct_change", ""),
                "interpretation": row.get("interpretation", ""),
                "data_source": _json_dumps(row.get("data_source")),
                "notes": " | ".join(row.get("notes") or []),
            }
        )
    return output.getvalue()


def _core_markdown_table(rows: list[dict[str, Any]]) -> str:
    preferred_metrics = [
        "raw_events_count",
        "parsed_events_count",
        "lp_signal_rows",
        "delivered_lp_signals",
        "suppressed_lp_signals",
        "telegram_suppression_ratio",
        "opportunity_candidate_count",
        "opportunity_verified_count",
        "blocker_saved_rate",
        "blocker_false_block_rate",
        "outcome_60s_completed_rate",
        "market_context_attempt_success_rate",
        "missing_major_pairs_count",
    ]
    rows_by_name = _index_metric(rows)
    selected = [rows_by_name[name] for name in preferred_metrics if name in rows_by_name]
    lines = [
        "| metric | today | previous | abs_change | pct_change | interpretation |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in selected:
        abs_text = "" if row.get("abs_change") is None else _format_table_value(row.get("abs_change"))
        pct_text = "" if row.get("pct_change") is None else f"{row.get('pct_change'):+.2f}%"
        lines.append(
            f"| {row['metric_name']} | {_format_table_value(row.get('today_value'))} | {_format_table_value(row.get('previous_value'))} | {abs_text} | {pct_text} | {row.get('interpretation') or ''} |"
        )
    return "\n".join(lines)


def _source_section(source_files: dict[str, dict[str, Any]], label: str) -> list[str]:
    lines = [f"- {label}:"]
    for report_type in REPORT_ORDER:
        item = source_files.get(report_type)
        if not item:
            lines.append(f"  - {report_type}: missing")
            continue
        lines.append(
            f"  - {report_type}: {item['path']} ({item['source_kind']}, logical_date={item['logical_date']})"
        )
    return lines


def build_markdown_report(payload: dict[str, Any]) -> str:
    compare_available = bool(payload.get("compare_available"))
    today_date = payload.get("today_date")
    previous_date = payload.get("previous_date")
    compare_basis = payload.get("compare_basis")
    source_files = payload.get("source_files") or {}
    answers = payload.get("question_answers") or {}
    limitations = payload.get("limitations") or []
    lines: list[str] = []
    lines.append("# Daily Compare Report")
    lines.append("")
    lines.append("## 执行摘要")
    lines.append(f"- today_date: `{today_date}`")
    lines.append(f"- previous_date: `{previous_date}`")
    lines.append(f"- compare_basis: `{compare_basis}`")
    if compare_available:
        lines.append(f"- 整体判断：{answers.get('1', '无法判断')}")
        lines.append(f"- 判断依据：{answers.get('2', '证据不足')}")
    else:
        lines.append("- compare not available：缺少可比较的 today/previous 结构化 summary JSON。")
        lines.append(f"- 结论：{answers.get('1', '无法判断')}")
    lines.append("")
    lines.append("## 数据来源与 Compare Basis")
    lines.extend(_source_section(source_files.get("today", {}), "today"))
    lines.extend(_source_section(source_files.get("previous", {}), "previous"))
    lines.append("")
    lines.append("## 核心指标对比表")
    if compare_available:
        lines.append(_core_markdown_table(payload.get("core_metrics_compare") or []))
    else:
        lines.append("- 缺少 previous_date，可比表未生成。")
    lines.append("")
    lines.append("## 噪音与消息质量对比")
    lines.append(f"- {answers.get('3', '证据不足')}")
    lines.append("")
    lines.append("## 候选/机会/阻止对比")
    lines.append(f"- {answers.get('4', '证据不足')}")
    lines.append(f"- {answers.get('5', '证据不足')}")
    lines.append(f"- {answers.get('6', '证据不足')}")
    lines.append("")
    lines.append("## Outcome 完整性对比")
    lines.append(f"- {answers.get('7', '证据不足')}")
    lines.append("")
    lines.append("## Market Context 与 Majors 覆盖对比")
    lines.append(f"- {answers.get('8', '证据不足')}")
    lines.append(f"- {answers.get('9', '证据不足')}")
    lines.append(f"- {answers.get('10', '证据不足')}")
    lines.append("")
    lines.append("## 今天相对昨天的进步")
    lines.append(f"- {answers.get('11', '证据不足')}")
    lines.append("")
    lines.append("## 今天相对昨天的退步")
    lines.append(f"- {answers.get('12', '证据不足')}")
    lines.append("")
    lines.append("## 明天最该做什么")
    lines.append(f"- {answers.get('13', '证据不足')}")
    lines.append("")
    lines.append("## 限制与不确定性")
    if limitations:
        for item in limitations[:20]:
            lines.append(f"- {item}")
    else:
        lines.append("- 无额外限制。")
    return "\n".join(lines).strip() + "\n"


def build_compare_payload(
    *,
    today_date: str,
    previous_date: str | None,
    compare_basis: str,
    today_bundle: dict[str, SummaryRecord],
    previous_bundle: dict[str, SummaryRecord],
    base_limitations: list[str],
) -> dict[str, Any]:
    if not previous_date:
        answers = {str(index): f"无法判断；{today_date} 之前缺少 previous_date 结构化 summary JSON。" for index in range(1, 14)}
        payload = {
            "compare_available": False,
            "compare_window": f"{today_date} vs unavailable",
            "today_date": today_date,
            "previous_date": None,
            "compare_basis": compare_basis,
            "source_files": {
                "today": _bundle_source_files(today_bundle),
                "previous": {},
            },
            "data_source_summary": {
                "selection_rule": "dated summary JSON -> matching latest summary JSON -> refreshed latest summary JSON",
                "today": _aggregate_data_source_summary(today_bundle),
                "previous": {},
            },
            "core_metrics_compare": [],
            "improvement_flags": [],
            "regression_flags": [],
            "unchanged_flags": [],
            "key_findings": ["compare not available: no previous available date"],
            "key_risks": ["无法进行 day-over-day 比较，不应编造 yesterday 值"],
            "next_actions": ["至少保留两个逻辑日期的主 summary JSON 后再运行 compare"],
            "limitations": sorted(set(base_limitations + ["missing_previous_date"])),
            "question_answers": answers,
        }
        payload["markdown"] = build_markdown_report(payload)
        payload["csv"] = _status_csv(False, today_date, None, compare_basis, "missing_previous_date")
        return payload

    rows = build_core_metric_rows(today_bundle, previous_bundle)
    answers = build_question_answers(rows, today_date=today_date, previous_date=previous_date, compare_basis=compare_basis)
    limitations = _build_limitations(
        rows,
        today_bundle=today_bundle,
        previous_bundle=previous_bundle,
        base_limitations=base_limitations,
    )
    payload = {
        "compare_available": True,
        "compare_window": f"{today_date} vs {previous_date}",
        "today_date": today_date,
        "previous_date": previous_date,
        "compare_basis": compare_basis,
        "source_files": {
            "today": _bundle_source_files(today_bundle),
            "previous": _bundle_source_files(previous_bundle),
        },
        "data_source_summary": {
            "selection_rule": "dated summary JSON -> matching latest summary JSON -> refreshed latest summary JSON",
            "today": _aggregate_data_source_summary(today_bundle),
            "previous": _aggregate_data_source_summary(previous_bundle),
        },
        "core_metrics_compare": rows,
        "improvement_flags": _flag_rows(rows, "improvement"),
        "regression_flags": _flag_rows(rows, "regression"),
        "unchanged_flags": _flag_rows(rows, "unchanged"),
        "key_findings": _build_key_findings(rows, answers),
        "key_risks": _build_key_risks(rows, answers),
        "next_actions": _build_next_actions(rows, answers),
        "limitations": limitations,
        "question_answers": answers,
    }
    payload["markdown"] = build_markdown_report(payload)
    payload["csv"] = _rows_to_csv(rows)
    return payload


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_compare_artifacts(payload: dict[str, Any], *, output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    today_date = str(payload.get("today_date") or "unknown")
    previous_date = payload.get("previous_date")
    written: dict[str, str] = {}

    latest_md = output_dir / "daily_compare_latest.md"
    latest_csv = output_dir / "daily_compare_latest.csv"
    latest_json = output_dir / "daily_compare_latest.json"

    _write_text(latest_md, payload["markdown"])
    _write_text(latest_csv, payload["csv"])
    json_payload = {key: value for key, value in payload.items() if key not in {"markdown", "csv"}}
    _write_json(latest_json, json_payload)
    written["latest_markdown"] = str(latest_md)
    written["latest_csv"] = str(latest_csv)
    written["latest_json"] = str(latest_json)

    if previous_date and bool(payload.get("compare_available")):
        base_name = f"daily_compare_{today_date}_vs_{previous_date}"
        dated_md = output_dir / f"{base_name}.md"
        dated_csv = output_dir / f"{base_name}.csv"
        dated_json = output_dir / f"{base_name}.json"
        _write_text(dated_md, payload["markdown"])
        _write_text(dated_csv, payload["csv"])
        _write_json(dated_json, json_payload)
        written["dated_markdown"] = str(dated_md)
        written["dated_csv"] = str(dated_csv)
        written["dated_json"] = str(dated_json)

    return written


def generate_daily_compare(
    *,
    requested_date: str | None = None,
    strict: bool = False,
    reports_dir: Path = REPORTS_DIR,
    output_dir: Path = DAILY_COMPARE_DIR,
    project_root: Path = ROOT,
    allow_generate: bool = True,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    inventory = discover_summary_inventory(reports_dir)
    selection = select_compare_dates(inventory, requested_date=requested_date)
    generation_warnings: list[str] = []
    if allow_generate and (not selection["available_dates"] or (requested_date and requested_date not in selection["available_dates"])):
        generation_warnings = _refresh_latest_reports(project_root=project_root, reports_dir=reports_dir)
        inventory = discover_summary_inventory(reports_dir)
        selection = select_compare_dates(inventory, requested_date=requested_date)

    today_date = selection.get("today_date")
    previous_date = selection.get("previous_date")
    compare_basis = selection.get("compare_basis")
    if not today_date:
        payload = {
            "compare_available": False,
            "compare_window": "unavailable",
            "today_date": None,
            "previous_date": None,
            "compare_basis": "no_data",
            "source_files": {"today": {}, "previous": {}},
            "data_source_summary": {"selection_rule": "dated summary JSON -> matching latest summary JSON -> refreshed latest summary JSON"},
            "core_metrics_compare": [],
            "improvement_flags": [],
            "regression_flags": [],
            "unchanged_flags": [],
            "key_findings": ["compare not available: no structured summary JSON found"],
            "key_risks": ["reports/ 下没有可用的主 summary JSON"],
            "next_actions": ["先生成主报告 summary JSON，再运行 daily compare"],
            "limitations": sorted(set(generation_warnings + ["no_available_dates"])),
            "question_answers": {str(index): "无法判断；reports/ 下没有可用的主 summary JSON。" for index in range(1, 14)},
        }
        payload["markdown"] = build_markdown_report(payload)
        payload["csv"] = _status_csv(False, None, None, "no_data", "no_available_dates")
        if strict:
            return 2, payload, {}
        written = write_compare_artifacts(payload, output_dir=output_dir)
        return 0, payload, written

    today_bundle, today_limitations = load_date_bundle(inventory, logical_date=today_date)
    previous_bundle, previous_limitations = load_date_bundle(inventory, logical_date=previous_date) if previous_date else ({}, [])
    base_limitations = generation_warnings + today_limitations + previous_limitations

    missing_today_reports = [report_type for report_type in REPORT_ORDER if report_type not in today_bundle]
    missing_previous_reports = [report_type for report_type in REPORT_ORDER if previous_date and report_type not in previous_bundle]
    if strict and (missing_today_reports or missing_previous_reports or not previous_date):
        payload = {
            "compare_available": False,
            "compare_window": f"{today_date} vs {previous_date or 'unavailable'}",
            "today_date": today_date,
            "previous_date": previous_date,
            "compare_basis": compare_basis,
            "source_files": {
                "today": _bundle_source_files(today_bundle),
                "previous": _bundle_source_files(previous_bundle),
            },
            "data_source_summary": {
                "selection_rule": "dated summary JSON -> matching latest summary JSON -> refreshed latest summary JSON",
                "today": _aggregate_data_source_summary(today_bundle),
                "previous": _aggregate_data_source_summary(previous_bundle),
            },
            "core_metrics_compare": [],
            "improvement_flags": [],
            "regression_flags": [],
            "unchanged_flags": [],
            "key_findings": ["strict compare failed: selected dates do not have complete primary summary coverage"],
            "key_risks": ["strict mode refuses to fabricate or partially infer missing today/previous data"],
            "next_actions": ["先补齐 today/previous 的三份主 summary JSON，再运行 daily-compare-strict"],
            "limitations": sorted(set(base_limitations + [f"missing_today_reports={','.join(missing_today_reports)}", f"missing_previous_reports={','.join(missing_previous_reports)}"])),
            "question_answers": {str(index): "无法判断；strict 模式下 today/previous 的主 summary JSON 不完整。" for index in range(1, 14)},
        }
        payload["markdown"] = build_markdown_report(payload)
        payload["csv"] = _status_csv(False, today_date, previous_date, compare_basis, "strict_incomplete_primary_summary")
        return 2, payload, {}

    payload = build_compare_payload(
        today_date=today_date,
        previous_date=previous_date,
        compare_basis=compare_basis,
        today_bundle=today_bundle,
        previous_bundle=previous_bundle,
        base_limitations=base_limitations,
    )
    if strict and not payload.get("compare_available"):
        return 2, payload, {}
    written = write_compare_artifacts(payload, output_dir=output_dir)
    return 0, payload, written


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    exit_code, payload, written = generate_daily_compare(requested_date=args.date, strict=args.strict)
    output = {
        "status": "ok" if exit_code == 0 else "error",
        "compare_available": payload.get("compare_available"),
        "today_date": payload.get("today_date"),
        "previous_date": payload.get("previous_date"),
        "compare_basis": payload.get("compare_basis"),
        "outputs": written,
        "limitations": payload.get("limitations", [])[:10],
    }
    print(json.dumps(output, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
