import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message
from trade_action import apply_trade_action


class TradeActionNotifierTests(unittest.TestCase):
    def _event(self, *, intent_type: str = "pool_buy_pressure", ts: int = 1_710_000_000) -> Event:
        return Event(
            tx_hash=f"0xnotifier{intent_type}{ts}",
            address="0xnotifierpool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=48_000.0,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.86,
            confirmation_score=0.84,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
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

    def _signal(self, event: Event, *, tier: str = "trader", **overrides) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.90,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="trade_action_notifier_test",
            quality_score=0.88,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
        )
        is_buy = event.intent_type == "pool_buy_pressure"
        context = {
            "user_tier": tier,
            "message_variant": "lp_directional",
            "lp_event": True,
            "pair_label": "ETH/USDC",
            "asset_case_label": "ETH",
            "asset_symbol": "ETH",
            "asset_case_id": "asset_case:ETH:notifier",
            "asset_case_direction": "buy_pressure" if is_buy else "sell_pressure",
            "asset_case_supporting_pair_count": 2,
            "lp_alert_stage": "confirm",
            "lp_confirm_scope": "broader_confirm",
            "lp_confirm_quality": "clean_confirm",
            "lp_absorption_context": "broader_buy_pressure_confirmed" if is_buy else "broader_sell_pressure_confirmed",
            "lp_broader_alignment": "confirmed",
            "lp_sweep_phase": "",
            "market_context_source": "live_public",
            "alert_relative_timing": "confirming",
            "market_move_before_alert_30s": 0.002 if is_buy else -0.002,
            "market_move_before_alert_60s": 0.003 if is_buy else -0.003,
            "market_move_after_alert_60s": 0.004 if is_buy else -0.004,
            "basis_bps": 4.0 if is_buy else -4.0,
            "mark_index_spread_bps": 1.2 if is_buy else -1.2,
            "pool_quality_score": 0.68,
            "pair_quality_score": 0.70,
            "asset_case_quality_score": 0.76,
            "quality_scope_pool_size": 8,
            "quality_scope_pair_size": 8,
            "quality_scope_asset_size": 8,
            "quality_actionable_sample_size": 3,
            "quality_score_brief": "历史传导较强",
            "lp_detect_latency_ms": 650,
            "lp_followup_check": "90s：是否继续跨池放大",
            "lp_invalidation": "连续性中断 / 反向压力出现",
            "message_template": "",
        }
        context.update(overrides)
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_first_line_downgrades_legacy_chase_to_wait_confirmation(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(event)
        apply_trade_action(event, signal)

        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("等待确认｜"))
        self.assertNotIn("可顺势追多", message)

    def test_long_chase_message_explains_why_long_is_allowed(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(event)
        apply_trade_action(event, signal)

        message = format_signal_message(signal, event)

        self.assertIn("为什么：", message)
        self.assertIn("链上买压扩散", message)

    def test_short_chase_message_explains_why_short_is_allowed(self) -> None:
        event = self._event(intent_type="pool_sell_pressure")
        signal = self._signal(event)
        apply_trade_action(event, signal)

        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("等待确认｜"))
        self.assertNotIn("可顺势追空", message)
        self.assertIn("为什么：", message)

    def test_do_not_chase_message_contains_reason_and_trigger(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(
            event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
        )
        apply_trade_action(event, signal)

        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("不追多｜"))
        self.assertIn("为什么：", message)
        self.assertIn("触发：", message)

    def test_wait_confirmation_message_contains_trigger_line(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(
            event,
            lp_alert_stage="prealert",
            lp_confirm_scope="local_confirm",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            lp_broader_alignment="weak_or_missing",
        )
        apply_trade_action(event, signal)

        message = format_signal_message(signal, event)

        self.assertIn(signal.context["trade_action_key"], {"LONG_BIAS_OBSERVE", "WAIT_CONFIRMATION"})
        self.assertIn("触发：", message)

    def test_debug_line_only_shows_for_research(self) -> None:
        trader_event = self._event(intent_type="pool_buy_pressure")
        trader_signal = self._signal(trader_event, tier="trader")
        apply_trade_action(trader_event, trader_signal)
        trader_message = format_signal_message(trader_signal, trader_event)

        research_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_100)
        research_signal = self._signal(research_event, tier="research")
        apply_trade_action(research_event, research_signal)
        research_message = format_signal_message(research_signal, research_event)

        self.assertNotIn("调试：stage=", trader_message)
        self.assertIn("调试：stage=", research_message)


if __name__ == "__main__":
    unittest.main()
