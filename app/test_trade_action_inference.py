import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from trade_action import apply_trade_action


class TradeActionInferenceTests(unittest.TestCase):
    def _event(
        self,
        *,
        intent_type: str = "pool_buy_pressure",
        ts: int = 1_710_000_000,
        token: str = "ETH",
    ) -> Event:
        return Event(
            tx_hash=f"0xtradeaction{intent_type}{ts}",
            address="0xtradeactionpool",
            token=token,
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=45_000.0,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.86,
            confirmation_score=0.82,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": token,
                "raw": {
                    "lp_context": {
                        "pair_label": f"{token}/USDC",
                        "pool_label": f"{token}/USDC",
                        "base_token_symbol": token,
                        "quote_token_symbol": "USDC",
                    }
                },
            },
        )

    def _signal(self, event: Event, **overrides) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.88,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="trade_action_test",
            quality_score=0.86,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
        )
        context = {
            "user_tier": "trader",
            "message_variant": "lp_directional",
            "message_template": "brief",
            "lp_event": True,
            "pair_label": f"{event.token}/USDC",
            "asset_case_label": event.token,
            "asset_symbol": event.token,
            "asset_case_id": f"asset_case:{event.token}:trade_action",
            "asset_case_direction": "buy_pressure" if event.intent_type == "pool_buy_pressure" else "sell_pressure",
            "asset_case_supporting_pair_count": 2,
            "lp_alert_stage": "confirm",
            "lp_confirm_scope": "broader_confirm",
            "lp_confirm_quality": "clean_confirm",
            "lp_absorption_context": "broader_buy_pressure_confirmed" if event.intent_type == "pool_buy_pressure" else "broader_sell_pressure_confirmed",
            "lp_broader_alignment": "confirmed",
            "lp_sweep_phase": "",
            "lp_sweep_continuation_score": 0.0,
            "lp_chase_risk_score": 0.10,
            "market_context_source": "live_public",
            "alert_relative_timing": "confirming",
            "market_move_before_alert_30s": 0.002 if event.intent_type == "pool_buy_pressure" else -0.002,
            "market_move_before_alert_60s": 0.003 if event.intent_type == "pool_buy_pressure" else -0.003,
            "market_move_after_alert_60s": 0.004 if event.intent_type == "pool_buy_pressure" else -0.004,
            "basis_bps": 4.0 if event.intent_type == "pool_buy_pressure" else -4.0,
            "mark_index_spread_bps": 2.0 if event.intent_type == "pool_buy_pressure" else -2.0,
            "pool_quality_score": 0.68,
            "pair_quality_score": 0.70,
            "asset_case_quality_score": 0.74,
            "quality_scope_pool_size": 6,
            "quality_scope_pair_size": 6,
            "quality_scope_asset_size": 8,
            "quality_actionable_sample_size": 3,
            "quality_score_brief": "历史传导较强",
            "lp_followup_check": "90s：是否继续跨池放大",
            "lp_invalidation": "连续性中断 / 反向压力出现",
        }
        context.update(overrides)
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_local_buy_pressure_without_broader_confirm_never_allows_long_chase(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            lp_broader_alignment="weak_or_missing",
        )

        payload = apply_trade_action(event, signal)

        self.assertIn(payload["trade_action_key"], {"LONG_BIAS_OBSERVE", "WAIT_CONFIRMATION"})
        self.assertNotEqual("LONG_CHASE_ALLOWED", payload["trade_action_key"])

    def test_local_sell_pressure_without_broader_confirm_never_allows_short_chase(self) -> None:
        event = self._event(intent_type="pool_sell_pressure")
        signal = self._signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            lp_broader_alignment="weak_or_missing",
        )

        payload = apply_trade_action(event, signal)

        self.assertIn(payload["trade_action_key"], {"SHORT_BIAS_OBSERVE", "WAIT_CONFIRMATION"})
        self.assertNotEqual("SHORT_CHASE_ALLOWED", payload["trade_action_key"])

    def test_buy_side_exhaustion_downgrades_to_do_not_chase_long(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(
            event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
        )

        payload = apply_trade_action(event, signal)

        self.assertEqual("DO_NOT_CHASE_LONG", payload["trade_action_key"])

    def test_sell_side_exhaustion_downgrades_to_do_not_chase_short(self) -> None:
        event = self._event(intent_type="pool_sell_pressure")
        signal = self._signal(
            event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
        )

        payload = apply_trade_action(event, signal)

        self.assertEqual("DO_NOT_CHASE_SHORT", payload["trade_action_key"])

    def test_sweep_building_only_stays_wait_or_bias(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(
            event,
            lp_alert_stage="prealert",
            lp_sweep_phase="sweep_building",
            lp_confirm_scope="local_confirm",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            lp_broader_alignment="weak_or_missing",
        )

        payload = apply_trade_action(event, signal)

        self.assertIn(payload["trade_action_key"], {"LONG_BIAS_OBSERVE", "WAIT_CONFIRMATION"})
        self.assertNotEqual("LONG_CHASE_ALLOWED", payload["trade_action_key"])

    def test_broader_buy_confirm_with_live_clean_quality_allows_long_chase(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(event)

        payload = apply_trade_action(event, signal)

        self.assertEqual("LONG_CHASE_ALLOWED", payload["trade_action_key"])

    def test_broader_sell_confirm_with_live_clean_quality_allows_short_chase(self) -> None:
        event = self._event(intent_type="pool_sell_pressure")
        signal = self._signal(event)

        payload = apply_trade_action(event, signal)

        self.assertEqual("SHORT_CHASE_ALLOWED", payload["trade_action_key"])

    def test_market_context_unavailable_forces_data_gap_no_trade(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(
            event,
            market_context_source="unavailable",
            market_context_failure_reason="okx_timeout",
        )

        payload = apply_trade_action(event, signal)

        self.assertEqual("DATA_GAP_NO_TRADE", payload["trade_action_key"])

    def test_late_or_chase_quality_can_not_be_rendered_as_chase_allowed(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(
            event,
            lp_confirm_quality="chase_risk",
            lp_chase_risk_score=0.82,
            alert_relative_timing="late",
        )

        payload = apply_trade_action(event, signal)

        self.assertNotEqual("LONG_CHASE_ALLOWED", payload["trade_action_key"])
        self.assertIn(payload["trade_action_key"], {"DO_NOT_CHASE_LONG", "LONG_BIAS_OBSERVE", "WAIT_CONFIRMATION"})


if __name__ == "__main__":
    unittest.main()
