import unittest
from unittest.mock import patch

from analyzer import BehaviorAnalyzer
from models import Event, Signal
from pipeline import SignalPipeline
from price_service import PriceService
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


class LpLatencyOutcomeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state_manager = StateManager()
        self.pipeline = SignalPipeline(
            price_service=PriceService(),
            state_manager=self.state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=SignalQualityGate(state_manager=self.state_manager),
        )

    def _event(
        self,
        *,
        ts: int,
        quote_amount: float,
        usd_value: float,
        intent_type: str = "pool_buy_pressure",
        confirmation_score: float = 0.80,
        first_chain_seen_at: int | None = None,
        parsed_at: int | None = None,
    ) -> Event:
        return Event(
            tx_hash=f"0xlp{ts}{quote_amount}",
            address="0x1111111111111111111111111111111111111111",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type != "pool_sell_pressure" else "卖出",
            usd_value=usd_value,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.90,
            confirmation_score=confirmation_score,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "first_chain_seen_at": first_chain_seen_at or ts,
                "parsed_at": parsed_at or ts,
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "pool_label": "ETH/USDC",
                        "base_amount": 1.0,
                        "quote_amount": quote_amount,
                    }
                },
            },
        )

    def _signal(self, event: Event, *, stage: str, sweep_phase: str = "") -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.90,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.88,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            signal_id=f"sig:{stage}:{event.ts}",
            metadata={},
        )
        signal.context.update({
            "pair_label": "ETH/USDC",
            "lp_alert_stage": stage,
            "lp_sweep_phase": sweep_phase,
            "outcome_tracking": {
                "schema_version": "lp_alert_outcome_v1",
                "windows": {"30s": {}, "60s": {}, "300s": {}},
            },
        })
        signal.metadata.update({
            "pair_label": "ETH/USDC",
            "lp_alert_stage": stage,
            "lp_sweep_phase": sweep_phase,
        })
        return signal

    def test_prealert_to_confirm_outcome_chain_is_recorded(self) -> None:
        history = self._event(ts=950, quote_amount=1_000.0, usd_value=1_000.0)
        prealert = self._event(ts=1_000, quote_amount=1_020.0, usd_value=2_000.0, confirmation_score=0.34)
        confirm = self._event(ts=1_040, quote_amount=1_050.0, usd_value=120_000.0, confirmation_score=0.88)

        self.state_manager.apply_event(history)
        self.state_manager.apply_event(prealert)
        self.pipeline._annotate_lp_market_timing(prealert)
        prealert_signal = self._signal(prealert, stage="prealert")
        prealert_record = self.pipeline._record_lp_outcome_runtime(prealert, prealert_signal)

        self.state_manager.apply_event(confirm)
        self.pipeline._annotate_lp_market_timing(confirm)
        confirm_signal = self._signal(confirm, stage="confirm")
        self.pipeline._record_lp_outcome_runtime(confirm, confirm_signal)

        updated_prealert = self.state_manager.get_lp_outcome_record(str(prealert_record.get("record_id") or ""))

        self.assertTrue(updated_prealert.get("confirm_after_prealert"))
        self.assertFalse(updated_prealert.get("false_prealert"))
        self.assertEqual(40, updated_prealert.get("time_to_confirm"))
        self.assertGreater(float(updated_prealert.get("move_before_alert") or 0.0), 0.0)
        self.assertGreater(float(updated_prealert.get("move_after_alert_30s") or 0.0), 0.0)
        self.assertTrue(updated_prealert.get("followthrough_positive"))
        self.assertEqual("completed", ((updated_prealert.get("outcome_windows") or {}).get("30s") or {}).get("status"))
        self.assertEqual("pending", ((updated_prealert.get("outcome_windows") or {}).get("60s") or {}).get("status"))

    def test_climax_to_reversal_chain_is_recorded(self) -> None:
        history = self._event(ts=1_900, quote_amount=1_000.0, usd_value=1_000.0)
        climax = self._event(ts=2_000, quote_amount=1_030.0, usd_value=120_000.0, confirmation_score=0.90)
        reversal = self._event(
            ts=2_045,
            quote_amount=1_000.0,
            usd_value=15_000.0,
            intent_type="pool_sell_pressure",
            confirmation_score=0.72,
        )

        self.state_manager.apply_event(history)
        self.state_manager.apply_event(climax)
        climax_signal = self._signal(climax, stage="climax", sweep_phase="sweep_confirmed")
        climax_record = self.pipeline._record_lp_outcome_runtime(climax, climax_signal)

        self.state_manager.apply_event(reversal)
        reversal_signal = self._signal(reversal, stage="exhaustion_risk", sweep_phase="sweep_exhaustion_risk")
        self.pipeline._record_lp_outcome_runtime(reversal, reversal_signal)

        updated_climax = self.state_manager.get_lp_outcome_record(str(climax_record.get("record_id") or ""))

        self.assertTrue(updated_climax.get("reversal_after_climax"))
        self.assertLess(float(updated_climax.get("move_after_alert") or 0.0), 0.0)
        self.assertTrue(updated_climax.get("followthrough_negative"))
        self.assertTrue(updated_climax.get("adverse_by_direction_30s"))

    def test_move_fields_and_latency_are_exposed(self) -> None:
        history = self._event(ts=2_950, quote_amount=1_000.0, usd_value=1_000.0)
        event = self._event(
            ts=3_000,
            quote_amount=1_030.0,
            usd_value=120_000.0,
            confirmation_score=0.88,
            first_chain_seen_at=2_988,
            parsed_at=2_996,
        )
        signal = self._signal(event, stage="confirm")

        self.state_manager.apply_event(history)
        self.state_manager.apply_event(event)
        timing = self.pipeline._annotate_lp_market_timing(event)
        payload = self.pipeline._record_lp_outcome_runtime(event, signal)

        with patch("pipeline.time.time", return_value=3_005):
            self.pipeline.finalize_notification_delivery(event, signal, delivered=True)

        self.assertEqual(8_000, timing.get("lp_detect_latency_ms"))
        self.assertGreater(float(payload.get("move_before_alert_30s") or 0.0), 0.02)
        self.assertIn("move_before_alert", signal.context.get("outcome_tracking") or {})
        self.assertIn("move_after_alert", signal.context.get("outcome_tracking") or {})
        self.assertEqual(17_000, signal.context.get("lp_end_to_end_latency_ms"))
        self.assertEqual(3_005, (signal.context.get("lp_outcome_record") or {}).get("notifier_sent_at"))
        self.assertIn("30s", (signal.context.get("outcome_tracking") or {}).get("windows") or {})

    def test_sell_pressure_price_rise_is_marked_adverse(self) -> None:
        history = self._event(ts=5_950, quote_amount=1_000.0, usd_value=1_000.0)
        alert = self._event(
            ts=6_000,
            quote_amount=1_020.0,
            usd_value=80_000.0,
            intent_type="pool_sell_pressure",
            confirmation_score=0.86,
        )
        followup = self._event(
            ts=6_040,
            quote_amount=1_060.0,
            usd_value=25_000.0,
            intent_type="pool_buy_pressure",
            confirmation_score=0.52,
        )

        self.state_manager.apply_event(history)
        self.state_manager.apply_event(alert)
        alert_signal = self._signal(alert, stage="confirm")
        record = self.pipeline._record_lp_outcome_runtime(alert, alert_signal)

        self.state_manager.apply_event(followup)
        followup_signal = self._signal(followup, stage="prealert")
        self.pipeline._record_lp_outcome_runtime(followup, followup_signal)

        updated = self.state_manager.get_lp_outcome_record(str(record.get("record_id") or ""))

        self.assertGreater(float(updated.get("raw_move_after_30s") or 0.0), 0.0)
        self.assertLess(float(updated.get("direction_adjusted_move_after_30s") or 0.0), 0.0)
        self.assertTrue(updated.get("adverse_by_direction_30s"))

    def test_outcome_windows_can_be_restored_after_sync(self) -> None:
        history = self._event(ts=4_950, quote_amount=1_000.0, usd_value=1_000.0)
        event = self._event(ts=5_000, quote_amount=1_010.0, usd_value=4_000.0, confirmation_score=0.34)
        signal = self._signal(event, stage="prealert")

        self.state_manager.apply_event(history)
        self.state_manager.apply_event(event)
        self.pipeline._record_lp_outcome_runtime(event, signal)

        records = self.state_manager.get_recent_lp_outcome_records(limit=10)
        restored = StateManager()
        restored.restore_lp_outcome_records(records)
        reloaded = restored.get_recent_lp_outcome_records(limit=10)[0]

        self.assertEqual("pending", ((reloaded.get("outcome_windows") or {}).get("30s") or {}).get("status"))
        self.assertEqual("pending", ((reloaded.get("outcome_windows") or {}).get("60s") or {}).get("status"))
        self.assertEqual("pending", ((reloaded.get("outcome_windows") or {}).get("300s") or {}).get("status"))


if __name__ == "__main__":
    unittest.main()
