import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from asset_case_manager import AssetCaseManager
from models import Event, Signal
from notifier import format_signal_message
from user_tiers import apply_user_tier_context


class LpAssetCaseAggregationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = AssetCaseManager(window_sec=90)

    def _event(
        self,
        *,
        pair_label: str,
        address: str,
        stage: str,
        ts: int,
        intent_type: str = "pool_buy_pressure",
        liquidation_stage: str = "none",
    ) -> Event:
        return Event(
            tx_hash=f"0xasset{pair_label}{ts}",
            address=address,
            token="ETH",
            amount=1.0,
            side="买入" if intent_type != "pool_sell_pressure" else "卖出",
            usd_value=80_000.0 if stage != "prealert" else 6_000.0,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="confirmed" if stage != "prealert" else "preliminary",
            intent_confidence=0.82,
            confirmation_score=0.82 if stage != "prealert" else 0.38,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "liquidation_stage": liquidation_stage,
                "raw": {
                    "lp_context": {
                        "pair_label": pair_label,
                        "pool_label": pair_label,
                        "base_token_symbol": pair_label.split("/")[0],
                        "quote_token_symbol": pair_label.split("/")[1],
                    }
                },
            },
        )

    def _signal(self, event: Event, *, stage: str, tier: str = "trader") -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.86,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.84,
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
                "pair_label": event.metadata["raw"]["lp_context"]["pair_label"],
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
                "lp_detect_latency_ms": 900,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def _gate_metrics(self, *, multi_pool: int = 0, surge: float = 1.5) -> dict:
        return {
            "lp_multi_pool_resonance": multi_pool,
            "lp_same_pool_continuity": 1,
            "lp_pool_volume_surge_ratio": surge,
        }

    def test_two_same_asset_pools_merge_into_one_asset_case(self) -> None:
        event1 = self._event(pair_label="ETH/USDT", address="0xpool1", stage="prealert", ts=1_000)
        signal1 = self._signal(event1, stage="prealert")
        payload1 = self.manager.merge_lp_signal(event1, signal1, gate_metrics=self._gate_metrics(multi_pool=1))

        event2 = self._event(pair_label="ETH/USDC", address="0xpool2", stage="prealert", ts=1_030)
        signal2 = self._signal(event2, stage="prealert")
        payload2 = self.manager.merge_lp_signal(event2, signal2, gate_metrics=self._gate_metrics(multi_pool=2))

        self.assertEqual(payload1["asset_case_id"], payload2["asset_case_id"])
        self.assertEqual(2, payload2["asset_case_supporting_pair_count"])
        self.assertTrue(payload2["asset_case_aggregated"])

    def test_aggregated_message_uses_asset_label_instead_of_pair(self) -> None:
        event1 = self._event(pair_label="ETH/USDT", address="0xpool1", stage="prealert", ts=1_000)
        signal1 = self._signal(event1, stage="prealert", tier="trader")
        self.manager.merge_lp_signal(event1, signal1, gate_metrics=self._gate_metrics(multi_pool=1))

        event2 = self._event(pair_label="ETH/USDC", address="0xpool2", stage="prealert", ts=1_020)
        signal2 = self._signal(event2, stage="prealert", tier="trader")
        self.manager.merge_lp_signal(event2, signal2, gate_metrics=self._gate_metrics(multi_pool=2))
        apply_user_tier_context(event2, signal2, {"user_tier": "trader"})

        message = format_signal_message(signal2, event2)

        self.assertTrue(message.splitlines()[0].startswith("预警｜ETH｜"))
        self.assertIn("ETH/USDT", message)
        self.assertIn("ETH/USDC", message)

    def test_confirm_upgrades_existing_prealert_case(self) -> None:
        event1 = self._event(pair_label="ETH/USDT", address="0xpool1", stage="prealert", ts=1_000)
        signal1 = self._signal(event1, stage="prealert")
        payload1 = self.manager.merge_lp_signal(event1, signal1, gate_metrics=self._gate_metrics(multi_pool=1))

        event2 = self._event(pair_label="ETH/USDC", address="0xpool2", stage="confirm", ts=1_040)
        signal2 = self._signal(event2, stage="confirm")
        payload2 = self.manager.merge_lp_signal(event2, signal2, gate_metrics=self._gate_metrics(multi_pool=2, surge=2.2))

        self.assertEqual(payload1["asset_case_id"], payload2["asset_case_id"])
        self.assertEqual("confirm", payload2["asset_case_stage"])

    def test_climax_and_exhaustion_attach_to_existing_case(self) -> None:
        climax_event = self._event(pair_label="ETH/USDT", address="0xpool1", stage="climax", ts=2_000)
        climax_signal = self._signal(climax_event, stage="climax")
        climax_payload = self.manager.merge_lp_signal(
            climax_event,
            climax_signal,
            gate_metrics=self._gate_metrics(multi_pool=2, surge=2.8),
        )

        risk_event = self._event(pair_label="ETH/USDC", address="0xpool2", stage="exhaustion_risk", ts=2_040)
        risk_signal = self._signal(risk_event, stage="exhaustion_risk")
        risk_payload = self.manager.merge_lp_signal(
            risk_event,
            risk_signal,
            gate_metrics=self._gate_metrics(multi_pool=2, surge=3.1),
        )

        self.assertEqual(climax_payload["asset_case_id"], risk_payload["asset_case_id"])
        self.assertEqual("exhaustion_risk", risk_payload["asset_case_stage"])

    def test_liquidation_override_is_not_merged(self) -> None:
        event = self._event(
            pair_label="ETH/USDC",
            address="0xpool1",
            stage="confirm",
            ts=3_000,
            liquidation_stage="execution",
        )
        signal = self._signal(event, stage="confirm")

        payload = self.manager.merge_lp_signal(event, signal, gate_metrics=self._gate_metrics(multi_pool=2))

        self.assertEqual({}, payload)
        self.assertFalse(event.metadata.get("asset_case_id"))

    def test_pool_level_fallback_still_works_for_single_pool_research_view(self) -> None:
        event = self._event(pair_label="ETH/USDC", address="0xpool1", stage="prealert", ts=4_000)
        signal = self._signal(event, stage="prealert", tier="research")
        self.manager.merge_lp_signal(event, signal, gate_metrics=self._gate_metrics(multi_pool=0))
        apply_user_tier_context(event, signal, {"user_tier": "research"})

        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("预警｜ETH/USDC｜"))


if __name__ == "__main__":
    unittest.main()
