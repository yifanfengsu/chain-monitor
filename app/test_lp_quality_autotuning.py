import unittest

from analyzer import BehaviorAnalyzer
from models import Event, Signal
from pipeline import SignalPipeline
from price_service import PriceService
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


class LpQualityAutotuningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state_manager = StateManager()
        self.quality_manager = QualityManager(state_manager=self.state_manager)
        self.engine = StrategyEngine()
        self.pipeline = SignalPipeline(
            price_service=PriceService(),
            state_manager=self.state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=self.engine,
            quality_gate=SignalQualityGate(state_manager=self.state_manager),
            quality_manager=self.quality_manager,
        )

    def _seed_record(
        self,
        *,
        record_id: str,
        stage: str,
        pool: str = "0xpool",
        pair: str = "ETH/USDC",
        asset: str = "ETH",
        direction: str = "buy_pressure",
        confirm_after_prealert=None,
        false_prealert=None,
        reversal_after_climax=None,
        move_after_60s=None,
        move_after_300s=None,
        market_source: str = "unavailable",
        timing: str = "",
        fastlane: bool = False,
    ) -> None:
        record = {
            "schema_version": "lp_alert_outcome_v1",
            "record_id": record_id,
            "signal_id": record_id,
            "event_id": record_id,
            "pair_label": pair,
            "pool_address": pool,
            "asset_symbol": asset,
            "intent_type": "pool_buy_pressure" if direction == "buy_pressure" else "pool_sell_pressure",
            "lp_alert_stage": stage,
            "lp_sweep_phase": "",
            "direction_bucket": direction,
            "created_at": 1_710_000_000,
            "anchor_price_proxy": 1.0,
            "followthrough_positive": move_after_60s is not None and float(move_after_60s) > 0 if direction == "buy_pressure" else move_after_60s is not None and float(move_after_60s) < 0,
            "followthrough_negative": move_after_60s is not None and float(move_after_60s) < 0 if direction == "buy_pressure" else move_after_60s is not None and float(move_after_60s) > 0,
            "reversal_after_climax": reversal_after_climax,
            "confirm_after_prealert": confirm_after_prealert,
            "false_prealert": false_prealert,
            "time_to_confirm": 30 if confirm_after_prealert else None,
            "move_before_alert": 0.0,
            "move_before_alert_30s": 0.0,
            "move_before_alert_60s": 0.0,
            "move_after_alert": move_after_60s or move_after_300s or 0.0,
            "move_after_alert_60s": move_after_60s,
            "move_after_alert_300s": move_after_300s,
            "asset_case_id": "asset_case:ETH:test",
            "asset_case_key": "ethereum|ETH|buy_pressure|onchain_lp",
            "asset_case_stage": stage,
            "asset_case_quality_score": 0.0,
            "market_context_source": market_source,
            "alert_relative_timing": timing,
            "market_move_before_alert_30s": None,
            "market_move_before_alert_60s": None,
            "market_move_after_alert_60s": None,
            "market_move_after_alert_300s": None,
            "lp_promoted_fastlane": fastlane,
            "notifier_sent_at": None,
            "delivered_notification": False,
        }
        self.state_manager._lp_outcome_records[record_id] = record
        self.state_manager._lp_outcome_history.append(record)

    def _event(self, *, stage: str = "prealert", pool: str = "0xpool") -> Event:
        return Event(
            tx_hash=f"0xquality{stage}",
            address=pool,
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=25_000.0,
            kind="swap",
            ts=1_710_000_300,
            intent_type="pool_buy_pressure",
            intent_stage="preliminary" if stage == "prealert" else "confirmed",
            intent_confidence=0.82,
            confirmation_score=0.38 if stage == "prealert" else 0.82,
            pricing_status="exact",
            pricing_confidence=0.94,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "user_tier": "trader",
                "token_symbol": "ETH",
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "pool_label": "ETH/USDC",
                        "base_token_symbol": "ETH",
                        "quote_token_symbol": "USDC",
                    }
                },
            },
        )

    def _signal(self, event: Event, *, stage: str) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.84,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.82,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
        )
        signal.context.update(
            {
                "user_tier": "trader",
                "pair_label": "ETH/USDC",
                "asset_case_id": "asset_case:ETH:test",
                "asset_case_label": "ETH",
                "asset_case_supporting_pair_count": 2,
                "lp_alert_stage": stage,
                "lp_stage_badge": {
                    "prealert": "预警",
                    "confirm": "确认",
                    "climax": "高潮",
                    "exhaustion_risk": "风险",
                }[stage],
                "lp_state_label": {
                    "prealert": "买盘建立中",
                    "confirm": "持续买压",
                    "climax": "买方清扫",
                    "exhaustion_risk": "买方清扫（回吐风险高）",
                }[stage],
                "lp_market_read": "先手观察｜需看 60s 是否续单",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "续单消失 / 快速反向池流",
                "lp_followup_required": True,
                "lp_scan_path": "secondary",
                "lp_detect_latency_ms": 600,
                "lp_exhaustion_confidence": 0.56 if stage == "climax" else 0.22,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def test_low_prealert_precision_makes_future_prealerts_harder_for_trader(self) -> None:
        self._seed_record(record_id="pre1", stage="prealert", confirm_after_prealert=False, false_prealert=True, move_after_300s=-0.01)
        self._seed_record(record_id="pre2", stage="prealert", confirm_after_prealert=False, false_prealert=True, move_after_300s=-0.02)
        self._seed_record(record_id="pre3", stage="prealert", confirm_after_prealert=True, false_prealert=False, move_after_300s=0.005)

        event = self._event(stage="prealert")
        signal = self._signal(event, stage="prealert")
        payload = self.quality_manager.annotate_lp_signal(event, signal)
        delivery_class, delivery_reason = self.engine._apply_delivery(
            event,
            signal,
            "observe",
            "lp_directional_prealert_observe",
        )

        self.assertLess(payload["prealert_precision_score"], 0.55)
        self.assertEqual("drop", delivery_class)
        self.assertIn("prealert_precision_filtered", delivery_reason)

    def test_high_climax_reversal_biases_current_stage_toward_exhaustion_risk(self) -> None:
        self._seed_record(record_id="cl1", stage="climax", reversal_after_climax=True, move_after_60s=-0.02)
        self._seed_record(record_id="cl2", stage="climax", reversal_after_climax=True, move_after_60s=-0.01)
        self._seed_record(record_id="cl3", stage="climax", reversal_after_climax=True, move_after_60s=-0.015)

        event = self._event(stage="climax")
        signal = self._signal(event, stage="climax")
        payload = self.quality_manager.annotate_lp_signal(event, signal)

        self.assertTrue(payload["exhaustion_bias_preferred"])
        self.assertEqual("exhaustion_risk", signal.context["lp_alert_stage"])

    def test_low_fastlane_roi_makes_promotion_harder(self) -> None:
        self._seed_record(record_id="fast1", stage="prealert", move_after_60s=-0.01, move_after_300s=-0.02, fastlane=True)
        self._seed_record(record_id="fast2", stage="confirm", move_after_60s=-0.008, move_after_300s=-0.01, fastlane=True)
        self._seed_record(record_id="fast3", stage="prealert", move_after_60s=-0.005, move_after_300s=-0.01, fastlane=True)

        event = self._event(stage="prealert")
        signal = self._signal(event, stage="prealert")
        self.quality_manager.annotate_lp_signal(event, signal)

        payload = self.pipeline._promote_lp_fastlane_if_needed(
            event,
            signal,
            {"lp_structure_score": 0.62, "lp_multi_pool_resonance": 1},
        )

        self.assertEqual({}, payload)

    def test_empty_history_falls_back_to_neutral_scores_without_crashing(self) -> None:
        event = self._event(stage="confirm")
        signal = self._signal(event, stage="confirm")

        payload = self.quality_manager.annotate_lp_signal(event, signal)

        self.assertGreater(payload["pool_quality_score"], 0.45)
        self.assertGreater(payload["asset_case_quality_score"], 0.45)
        self.assertIn(payload["quality_score_brief"], {"历史样本有限", "历史一般", "历史结构较稳", "历史传导较强", "历史命中一般"})


if __name__ == "__main__":
    unittest.main()
