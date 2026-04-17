import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from delivery_policy import can_emit_delivery_notification
from models import Event, Signal
from notifier import format_signal_message
from strategy_engine import StrategyEngine


class LpUserTierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = StrategyEngine()

    def _event(self, *, tier: str, intent_type: str = "pool_buy_pressure", stage: str = "prealert") -> Event:
        return Event(
            tx_hash=f"0xtier{tier}{stage}",
            address="0x1111111111111111111111111111111111111111",
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=20_000.0,
            kind="swap",
            ts=1_710_000_000,
            intent_type=intent_type,
            intent_stage="confirmed" if stage != "prealert" else "preliminary",
            intent_confidence=0.80,
            confirmation_score=0.72 if stage != "prealert" else 0.36,
            pricing_status="exact",
            pricing_confidence=0.94,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "user_tier": tier,
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

    def _signal(
        self,
        event: Event,
        *,
        tier: str,
        stage: str,
        supporting_pairs: int = 1,
        asset_quality: float = 0.72,
        prealert_precision: float = 0.72,
    ) -> Signal:
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
                "user_tier": tier,
                "pair_label": "ETH/USDC",
                "lp_event": True,
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
                "lp_market_read": "先手观察｜需看 60s 是否续单" if stage == "prealert" else "更像趋势确认，不是首发先手",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "续单消失 / 快速反向池流",
                "lp_followup_required": True,
                "lp_scan_path": "secondary",
                "lp_detect_latency_ms": 1200,
                "asset_case_id": "asset_case:test" if supporting_pairs >= 1 else "",
                "asset_case_label": "ETH",
                "asset_case_supporting_pair_count": supporting_pairs,
                "asset_case_quality_score": asset_quality,
                "pool_quality_score": asset_quality,
                "pair_quality_score": asset_quality,
                "prealert_precision_score": prealert_precision,
                "quality_score_brief": "历史传导较强",
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def test_retail_default_does_not_receive_single_pool_weak_prealert(self) -> None:
        event = self._event(tier="retail", stage="prealert")
        signal = self._signal(
            event,
            tier="retail",
            stage="prealert",
            supporting_pairs=1,
            asset_quality=0.44,
            prealert_precision=0.42,
        )

        delivery_class, delivery_reason = self.engine._apply_delivery(
            event,
            signal,
            "observe",
            "lp_directional_prealert_observe",
        )

        self.assertEqual("drop", delivery_class)
        self.assertIn("user_tier_retail", delivery_reason)

    def test_trader_can_receive_high_quality_prealert(self) -> None:
        event = self._event(tier="trader", stage="prealert")
        signal = self._signal(
            event,
            tier="trader",
            stage="prealert",
            supporting_pairs=2,
            asset_quality=0.76,
            prealert_precision=0.74,
        )

        delivery_class, delivery_reason = self.engine._apply_delivery(
            event,
            signal,
            "observe",
            "lp_directional_prealert_observe",
        )

        self.assertEqual("observe", delivery_class)
        self.assertEqual("lp_directional_prealert_observe", delivery_reason)
        self.assertTrue(can_emit_delivery_notification(event, signal))

    def test_research_keeps_full_stage_first_message(self) -> None:
        event = self._event(tier="research", stage="prealert")
        signal = self._signal(
            event,
            tier="research",
            stage="prealert",
            supporting_pairs=1,
            asset_quality=0.40,
            prealert_precision=0.40,
        )
        signal.context["message_template"] = "brief"

        delivery_class, _ = self.engine._apply_delivery(
            event,
            signal,
            "observe",
            "lp_directional_prealert_observe",
        )
        message = format_signal_message(signal, event)

        self.assertEqual("observe", delivery_class)
        self.assertTrue(message.splitlines()[0].startswith("预警｜ETH/USDC｜"))
        self.assertIn("链路：secondary｜延迟 1200ms", message)

    def test_retail_still_receives_confirm_and_exhaustion_risk(self) -> None:
        for stage in ("confirm", "exhaustion_risk"):
            with self.subTest(stage=stage):
                event = self._event(tier="retail", stage=stage)
                signal = self._signal(
                    event,
                    tier="retail",
                    stage=stage,
                    supporting_pairs=2,
                    asset_quality=0.72,
                    prealert_precision=0.70,
                )

                delivery_class, _ = self.engine._apply_delivery(
                    event,
                    signal,
                    "observe",
                    "lp_directional_pressure_observe",
                )

                self.assertEqual("observe", delivery_class)
                self.assertTrue(can_emit_delivery_notification(event, signal))

    def test_non_lp_paths_are_not_accidentally_blocked(self) -> None:
        event = Event(
            tx_hash="0xclmmtier",
            address="0x2222222222222222222222222222222222222222",
            token="ETH",
            amount=1.0,
            side="增加流动性",
            usd_value=80_000.0,
            kind="lp_add_liquidity",
            ts=1_710_000_000,
            intent_type="clmm_range_shift",
            intent_stage="confirmed",
            intent_confidence=0.82,
            confirmation_score=0.78,
            pricing_status="exact",
            pricing_confidence=0.92,
            usd_value_available=True,
            strategy_role="liquidity_provider",
            metadata={"user_tier": "retail"},
        )
        signal = Signal(
            type="lp_position_intent",
            confidence=0.82,
            priority=1,
            tier="Tier 1",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.84,
            semantic="lp_position",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="primary",
            delivery_reason="clmm_primary",
        )

        delivery_class, delivery_reason = self.engine._apply_delivery(
            event,
            signal,
            "primary",
            "clmm_primary",
        )

        self.assertEqual("primary", delivery_class)
        self.assertEqual("clmm_primary", delivery_reason)


if __name__ == "__main__":
    unittest.main()
