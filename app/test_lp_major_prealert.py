import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from asset_case_manager import AssetCaseManager
from models import Event, Signal
from notifier import format_signal_message
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from user_tiers import apply_user_tier_context
from user_tiers import evaluate_user_tier_lp_delivery


class LpMajorPrealertTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state_manager = StateManager()
        self.gate = SignalQualityGate(state_manager=self.state_manager)
        self.asset_case_manager = AssetCaseManager(window_sec=90)

    def _event(
        self,
        *,
        token: str,
        pair_label: str,
        usd_value: float = 2_200.0,
        same_pool_continuity: int = 1,
        multi_pool_resonance: int = 0,
        pool_volume_surge_ratio: float = 1.12,
        reserve_skew: float = 0.09,
        action_intensity: float = 0.20,
        confirmation_score: float = 0.30,
        ts: int = 1_710_000_000,
    ) -> Event:
        base_symbol, quote_symbol = pair_label.split("/", 1)
        return Event(
            tx_hash=f"0xmajorprealert{token}{ts}",
            address=f"0x{abs(hash((pair_label, ts))) % (10 ** 8):08x}",
            token=token,
            amount=1.0,
            side="买入",
            usd_value=usd_value,
            kind="swap",
            ts=ts,
            intent_type="pool_buy_pressure",
            intent_stage="preliminary",
            intent_confidence=0.56,
            confirmation_score=confirmation_score,
            pricing_status="exact",
            pricing_confidence=0.90,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": token,
                "raw": {
                    "lp_context": {
                        "pair_label": pair_label,
                        "pool_label": pair_label,
                        "base_token_symbol": base_symbol,
                        "quote_token_symbol": quote_symbol,
                    }
                },
                "lp_analysis": {
                    "reserve_skew": reserve_skew,
                    "action_intensity": action_intensity,
                    "same_pool_continuity": same_pool_continuity,
                    "multi_pool_resonance": multi_pool_resonance,
                    "pool_volume_surge_ratio": pool_volume_surge_ratio,
                },
            },
        )

    def _signal(
        self,
        event: Event,
        *,
        tier: str,
        prealert_precision: float = 0.72,
        asset_quality: float = 0.64,
        supporting_pairs: int = 1,
    ) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.80,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.80,
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
                "user_tier": tier,
                "message_template": "brief",
                "pair_label": event.metadata["raw"]["lp_context"]["pair_label"],
                "lp_event": True,
                "message_variant": "lp_directional",
                "asset_case_label": event.token,
                "asset_case_id": f"asset_case:{event.token}:prealert",
                "asset_case_supporting_pair_count": supporting_pairs,
                "lp_alert_stage": "prealert",
                "lp_stage_badge": "预警",
                "lp_state_label": "买盘建立中",
                "lp_market_read": "先手观察｜结构建立中，先看 30-90s 是否续单 / 跨池共振",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "续单消失 / 快速反向池流",
                "lp_followup_required": True,
                "pool_quality_score": asset_quality,
                "pair_quality_score": asset_quality,
                "asset_case_quality_score": asset_quality,
                "prealert_precision_score": prealert_precision,
                "lp_major_pool": event.token in {"ETH", "BTC", "SOL"},
                "lp_major_priority_score": 1.25 if event.token in {"ETH", "BTC", "SOL"} else 1.0,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def _gate_decision(self, event: Event):
        return self.gate.evaluate(
            event,
            {"strategy_role": "lp_pool", "pair_label": event.metadata["raw"]["lp_context"]["pair_label"]},
            {"behavior_type": "pool_buy_pressure", "confidence": 0.80, "reason": "unit_test"},
            {"score": 1.0, "alpha_score": 1.0},
            {"score": 1.0},
            {"avg_usd": 100.0, "max_usd": 500.0, "windows": {"24h": {"avg_usd": 100.0, "max_usd": 500.0}}},
            {
                "liquidity_proxy_usd": 600_000.0,
                "volume_24h_proxy_usd": 6_000_000.0,
                "buy_cluster_5m": 1,
                "sell_cluster_5m": 0,
                "token_net_flow_usd_5m": 8_000.0,
                "token_net_flow_usd_1h": 12_000.0,
                "resonance_5m": {"resonance_score": 0.28, "same_side_unique_addresses": 1},
            },
        )

    def test_major_pool_single_pool_event_can_trigger_prealert_earlier(self) -> None:
        event = self._event(token="SOL", pair_label="SOL/USDC")

        decision = self._gate_decision(event)

        self.assertTrue(decision.passed)
        self.assertTrue(decision.metrics.get("lp_prealert_candidate"))
        self.assertTrue(decision.metrics.get("lp_prealert_applied"))
        self.assertEqual("major_pool", decision.metrics.get("lp_pool_priority_class"))
        self.assertIn(decision.metrics.get("lp_prealert_reason"), {"lp_prealert_major_direction_building", "lp_prealert_reserve_skew_emerging"})

    def test_non_major_same_shape_is_blocked_by_guard(self) -> None:
        event = self._event(token="PEPE", pair_label="PEPE/USDC")

        decision = self._gate_decision(event)

        self.assertFalse(decision.metrics.get("lp_prealert_applied"))
        self.assertEqual("standard_pool", decision.metrics.get("lp_pool_priority_class"))
        self.assertNotEqual("gate_prealert_observe_pass", decision.metrics.get("lp_stage_decision"))

    def test_major_prealert_aggregates_at_asset_case_layer(self) -> None:
        event1 = self._event(token="ETH", pair_label="ETH/USDT", ts=1_710_000_000)
        signal1 = self._signal(event1, tier="trader")
        payload1 = self.asset_case_manager.merge_lp_signal(
            event1,
            signal1,
            gate_metrics={"lp_multi_pool_resonance": 1, "lp_same_pool_continuity": 1, "lp_pool_volume_surge_ratio": 1.2},
        )

        event2 = self._event(token="ETH", pair_label="ETH/USDC", ts=1_710_000_040)
        signal2 = self._signal(event2, tier="trader")
        payload2 = self.asset_case_manager.merge_lp_signal(
            event2,
            signal2,
            gate_metrics={"lp_multi_pool_resonance": 2, "lp_same_pool_continuity": 1, "lp_pool_volume_surge_ratio": 1.3},
        )
        apply_user_tier_context(event2, signal2, {"user_tier": "trader"})
        message = format_signal_message(signal2, event2)

        self.assertEqual(payload1["asset_case_id"], payload2["asset_case_id"])
        self.assertEqual("prealert", payload2["asset_case_stage"])
        self.assertEqual(2, payload2["asset_case_supporting_pair_count"])
        self.assertTrue(message.splitlines()[0].startswith("预警｜ETH｜"))
        self.assertIn("ETH/USDT", message)
        self.assertIn("ETH/USDC", message)

    def test_trader_only_allows_high_quality_major_prealert_while_retail_stays_blocked(self) -> None:
        event = self._event(token="ETH", pair_label="ETH/USDC")
        trader_signal = self._signal(event, tier="trader", prealert_precision=0.76, asset_quality=0.66, supporting_pairs=1)
        retail_signal = self._signal(event, tier="retail", prealert_precision=0.76, asset_quality=0.66, supporting_pairs=1)
        research_signal = self._signal(event, tier="research", prealert_precision=0.40, asset_quality=0.40, supporting_pairs=1)

        trader_allowed, trader_reason = evaluate_user_tier_lp_delivery(event, trader_signal, "observe")
        retail_allowed, retail_reason = evaluate_user_tier_lp_delivery(event, retail_signal, "observe")
        research_allowed, research_reason = evaluate_user_tier_lp_delivery(event, research_signal, "observe")

        self.assertTrue(trader_allowed)
        self.assertEqual("user_tier_trader_allowed", trader_reason)
        self.assertFalse(retail_allowed)
        self.assertIn("stage_filtered_prealert", retail_reason)
        self.assertTrue(research_allowed)
        self.assertEqual("user_tier_research_allowed", research_reason)

    def test_trader_filters_low_quality_single_pool_major_prealert(self) -> None:
        event = self._event(token="BTC", pair_label="BTC/USDC")
        signal = self._signal(event, tier="trader", prealert_precision=0.54, asset_quality=0.50, supporting_pairs=1)

        allowed, reason = evaluate_user_tier_lp_delivery(event, signal, "observe")

        self.assertFalse(allowed)
        self.assertIn("single_pool_prealert_filtered", reason)


if __name__ == "__main__":
    unittest.main()
