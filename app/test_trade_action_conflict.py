import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message
from trade_action import apply_trade_action, build_trade_action_signal_summary


class TradeActionConflictTests(unittest.TestCase):
    def _event(self, *, intent_type: str, ts: int) -> Event:
        return Event(
            tx_hash=f"0xconflict{intent_type}{ts}",
            address="0xconflictpoll",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=55_000.0,
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

    def _signal(self, event: Event, **overrides) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.90,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="trade_action_conflict_test",
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
            "user_tier": "trader",
            "message_variant": "lp_directional",
            "message_template": "brief",
            "lp_event": True,
            "pair_label": "ETH/USDC",
            "asset_case_label": "ETH",
            "asset_symbol": "ETH",
            "asset_case_id": "asset_case:ETH:conflict",
            "asset_case_direction": "buy_pressure" if is_buy else "sell_pressure",
            "asset_case_supporting_pair_count": 2,
            "lp_alert_stage": "confirm",
            "lp_confirm_scope": "broader_confirm",
            "lp_confirm_quality": "clean_confirm",
            "lp_absorption_context": "broader_buy_pressure_confirmed" if is_buy else "broader_sell_pressure_confirmed",
            "lp_broader_alignment": "confirmed",
            "lp_sweep_phase": "sweep_confirmed",
            "lp_sweep_continuation_score": 0.84,
            "market_context_source": "live_public",
            "alert_relative_timing": "confirming",
            "market_move_before_alert_30s": 0.002 if is_buy else -0.002,
            "market_move_before_alert_60s": 0.003 if is_buy else -0.003,
            "market_move_after_alert_60s": 0.004 if is_buy else -0.004,
            "basis_bps": 3.0 if is_buy else -3.0,
            "mark_index_spread_bps": 1.5 if is_buy else -1.5,
            "pool_quality_score": 0.70,
            "pair_quality_score": 0.72,
            "asset_case_quality_score": 0.78,
            "quality_scope_pool_size": 8,
            "quality_scope_pair_size": 8,
            "quality_scope_asset_size": 10,
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

    def test_opposite_direction_signals_within_window_turn_into_conflict_no_trade(self) -> None:
        buy_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        buy_signal = self._signal(buy_event)
        apply_trade_action(buy_event, buy_signal)
        recent = [build_trade_action_signal_summary(buy_event, buy_signal)]

        sell_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_090)
        sell_signal = self._signal(sell_event)
        payload = apply_trade_action(sell_event, sell_signal, recent_signals=recent)

        self.assertEqual("CONFLICT_NO_TRADE", payload["trade_action_key"])

    def test_conflict_message_first_line_starts_with_no_trade(self) -> None:
        buy_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        buy_signal = self._signal(buy_event)
        apply_trade_action(buy_event, buy_signal)
        recent = [build_trade_action_signal_summary(buy_event, buy_signal)]

        sell_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_060)
        sell_signal = self._signal(sell_event)
        apply_trade_action(sell_event, sell_signal, recent_signals=recent)

        message = format_signal_message(sell_signal, sell_event)
        self.assertTrue(message.splitlines()[0].startswith("不交易｜"))

    def test_strong_broader_live_side_can_override_weak_opposite_signal(self) -> None:
        weak_buy_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        weak_buy_signal = self._signal(
            weak_buy_event,
            lp_alert_stage="prealert",
            lp_confirm_scope="local_confirm",
            lp_confirm_quality="unconfirmed_confirm",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            lp_broader_alignment="weak_or_missing",
            lp_sweep_phase="sweep_building",
            lp_sweep_continuation_score=0.30,
            market_context_source="live_public",
            quality_scope_pool_size=1,
            quality_scope_pair_size=1,
            quality_scope_asset_size=1,
        )
        apply_trade_action(weak_buy_event, weak_buy_signal)
        recent = [build_trade_action_signal_summary(weak_buy_event, weak_buy_signal)]

        strong_sell_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_050)
        strong_sell_signal = self._signal(strong_sell_event)
        payload = apply_trade_action(strong_sell_event, strong_sell_signal, recent_signals=recent)

        self.assertEqual("SHORT_CHASE_ALLOWED", payload["trade_action_key"])
        self.assertNotEqual("CONFLICT_NO_TRADE", payload["trade_action_key"])


if __name__ == "__main__":
    unittest.main()
