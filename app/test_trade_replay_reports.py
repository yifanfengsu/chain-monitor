from __future__ import annotations

import unittest
from pathlib import Path

import reports.generate_daily_report_latest as daily_report
from reports.generate_daily_compare_report import DAILY_REPORT_KEY, SummaryRecord, _build_replay_compare


class TradeReplayReportDiagnosticsTests(unittest.TestCase):
    def test_daily_report_normalizes_replay_diagnostics(self) -> None:
        payload = daily_report._normalize_trade_replay_summary(
            {
                "trade_replay_available": False,
                "replay_count": 0,
                "input_source_counts": {
                    "signals": 2,
                    "trade_opportunities": 0,
                    "delivery_audit": 1,
                    "telegram_deliveries": 0,
                    "shadow_opportunities": 1,
                    "suppressed": 1,
                    "blocked": 1,
                },
                "eligibility_summary": {
                    "eligible_count": 0,
                    "ineligible_count": 2,
                    "ineligible_reasons": {"direction_ambiguous": 2},
                },
                "warnings": ["trade_replay_missing:all_rows_missing_direction"],
                "query_errors": ["query_failed"],
                "price_errors": ["price_failed"],
                "schema_errors": ["schema_failed"],
            },
            "2026-04-30",
        )

        self.assertFalse(payload["trade_replay_available"])
        self.assertEqual(2, payload["input_source_counts"]["signals"])
        self.assertEqual(2, payload["eligibility_summary"]["ineligible_reasons"]["direction_ambiguous"])
        self.assertIn("trade_replay_missing:all_rows_missing_direction", payload["warnings"])
        self.assertEqual(["query_failed"], payload["query_errors"])
        self.assertEqual(["price_failed"], payload["price_errors"])
        self.assertEqual(["schema_failed"], payload["schema_errors"])

    def test_daily_compare_keeps_replay_missing_diagnostics(self) -> None:
        def record(logical_date: str, available: bool, signals: int, reason: str) -> SummaryRecord:
            return SummaryRecord(
                report_type=DAILY_REPORT_KEY,
                path=Path(f"daily_report_{logical_date}.json"),
                logical_date=logical_date,
                source_kind="json",
                data={
                    "trade_replay_summary": {
                        "trade_replay_available": available,
                        "replay_count": 1 if available else 0,
                        "valid_replay_count": 1 if available else 0,
                        "input_source_counts": {"signals": signals, "trade_opportunities": 0},
                        "eligibility_summary": {
                            "eligible_count": 1 if available else 0,
                            "ineligible_count": 0 if available else signals,
                            "ineligible_reasons": {} if available else {reason: signals},
                        },
                        "warnings": [] if available else [f"trade_replay_missing:{reason}"],
                    },
                    "shadow_opportunity_summary": {},
                    "data_quality_summary": {"data_quality_status": "valid"},
                },
            )

        compare = _build_replay_compare(
            {DAILY_REPORT_KEY: record("2026-04-30", False, 2, "direction_ambiguous")},
            {DAILY_REPORT_KEY: record("2026-04-29", True, 1, "none")},
        )

        self.assertEqual("insufficient", compare["status"])
        self.assertEqual(2, compare["input_source_counts"]["today"]["signals"])
        self.assertEqual(2, compare["eligibility_summary"]["today"]["ineligible_reasons"]["direction_ambiguous"])
        self.assertIn("trade_replay_missing:direction_ambiguous", compare["warnings"])


if __name__ == "__main__":
    unittest.main()
