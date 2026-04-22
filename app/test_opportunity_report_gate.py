import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from reports.generate_overnight_run_analysis_latest import compute_final_trading_output_summary
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


class OpportunityReportGateTests(unittest.TestCase):
    def _row(self, context: dict, *, delivered: bool) -> dict:
        row = dict(context)
        row["sent_to_telegram"] = bool(delivered)
        row["notifier_sent_at"] = int(context.get("trade_opportunity_created_at") or context.get("archive_ts") or 0) if delivered else None
        row["notifier_line1"] = f"{row.get('final_trading_output_label') or ''}｜{row.get('asset_symbol') or ''}"
        return row

    def test_report_counts_downgrades_and_detects_no_legacy_chase_leak(self) -> None:
        candidate_manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        candidate_event = make_event(ts=1_710_100_300)
        candidate_signal = make_signal(candidate_event)
        candidate_manager.apply_lp_signal(candidate_event, candidate_signal)

        blocked_manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        blocked_event = make_event(intent_type="pool_sell_pressure", ts=1_710_100_310)
        blocked_signal = make_signal(
            blocked_event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
            trade_action_key="SHORT_CHASE_ALLOWED",
            trade_action_label="可顺势追空",
        )
        blocked_manager.apply_lp_signal(blocked_event, blocked_signal)

        none_manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        none_event = make_event(ts=1_710_100_320)
        none_signal = make_signal(
            none_event,
            lp_confirm_scope="local_confirm",
            lp_broader_alignment="confirmed",
            lp_absorption_context="broader_buy_pressure_confirmed",
            asset_market_state_key="WAIT_CONFIRMATION",
            asset_market_state_label="等待确认",
        )
        none_manager.apply_lp_signal(none_event, none_signal)

        verified_records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        verified_manager = TradeOpportunityManager(state_manager=StubStateManager(records=verified_records), persistence_enabled=False)
        verified_event = make_event(ts=1_710_100_330)
        verified_signal = make_signal(verified_event)
        verified_manager.apply_lp_signal(verified_event, verified_signal)

        summary = compute_final_trading_output_summary(
            [
                self._row(candidate_signal.context, delivered=True),
                self._row(blocked_signal.context, delivered=True),
                self._row(none_signal.context, delivered=False),
                self._row(verified_signal.context, delivered=True),
            ]
        )

        self.assertEqual(0, summary["legacy_chase_leaked_count"])
        self.assertEqual(0, summary["delivered_legacy_chase_count"])
        self.assertEqual(3, summary["legacy_chase_downgraded_count"])
        self.assertEqual(1, summary["messages_blocked_by_opportunity_gate"])
        self.assertTrue(summary["all_opportunity_labels_verified"])
        self.assertTrue(summary["all_candidate_labels_are_candidate"])
        self.assertTrue(summary["blocked_covers_legacy_chase_risk"])

    def test_report_detects_historical_legacy_chase_leak_without_unified_audit_fields(self) -> None:
        summary = compute_final_trading_output_summary(
            [
                {
                    "asset_symbol": "ETH",
                    "trade_action_key": "LONG_CHASE_ALLOWED",
                    "trade_action_label": "可顺势追多",
                    "sent_to_telegram": True,
                    "notifier_sent_at": 1_710_100_500,
                    "notifier_line1": "可顺势追多｜ETH｜旧版直推",
                }
            ]
        )

        self.assertEqual(0, summary["final_trading_output_audited_row_count"])
        self.assertEqual(1, summary["final_trading_output_unaudited_row_count"])
        self.assertEqual(1, summary["legacy_chase_leaked_count"])
        self.assertEqual(1, summary["delivered_legacy_chase_count"])
        self.assertEqual(1, summary["trade_action_chase_without_opportunity_count"])


if __name__ == "__main__":
    unittest.main()
