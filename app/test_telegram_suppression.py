import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from asset_market_state import AssetMarketStateManager
from models import Event, Signal


class _StubStateManager:
    def __init__(self, records=None) -> None:
        self._records = list(records or [])

    def get_recent_lp_outcome_records(self, limit: int = 500):
        return list(self._records)[-limit:]


class TelegramSuppressionTests(unittest.TestCase):
    def _event(self, *, intent_type: str = "pool_buy_pressure", ts: int = 1_710_000_000) -> Event:
        return Event(
            tx_hash=f"0xtele{intent_type}{ts}",
            address="0xtelegrampool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=55_000.0,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.88,
            confirmation_score=0.84,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={"token_symbol": "ETH", "raw": {"lp_context": {"pair_label": "ETH/USDC"}}},
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
            reason="telegram_suppression_test",
            quality_score=0.88,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
            signal_id=f"sig:{event.intent_type}:{event.ts}",
        )
        is_buy = event.intent_type == "pool_buy_pressure"
        context = {
            "user_tier": "trader",
            "message_variant": "lp_directional",
            "lp_event": True,
            "pair_label": "ETH/USDC",
            "asset_case_label": "ETH",
            "asset_symbol": "ETH",
            "asset_case_id": "asset_case:ETH:telegram",
            "asset_case_direction": "buy_pressure" if is_buy else "sell_pressure",
            "asset_case_supporting_pair_count": 2,
            "lp_alert_stage": "confirm",
            "lp_confirm_scope": "broader_confirm",
            "lp_confirm_quality": "clean_confirm",
            "lp_absorption_context": "broader_buy_pressure_confirmed" if is_buy else "broader_sell_pressure_confirmed",
            "lp_broader_alignment": "confirmed",
            "lp_sweep_phase": "",
            "market_context_source": "live_public",
            "pair_quality_score": 0.74,
            "asset_case_quality_score": 0.80,
            "trade_action_key": "LONG_CHASE_ALLOWED" if is_buy else "SHORT_CHASE_ALLOWED",
            "trade_action_reason": "方向扩散",
            "trade_action_required_confirmation": "继续看 broader confirm",
            "trade_action_invalidated_by": "反向强信号",
            "trade_action_evidence_pack": "跨池2｜OKX 同向｜clean confirm",
            "trade_action_confidence": 0.90,
            "trade_action_debug": {"strength_score": 0.88},
        }
        context.update(overrides)
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_repeat_same_state_is_suppressed(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        first_event = self._event(ts=1_710_000_000)
        first_signal = self._signal(first_event)
        manager.apply_lp_signal(first_event, first_signal)

        second_event = self._event(ts=1_710_000_020)
        second_signal = self._signal(second_event)
        payload = manager.apply_lp_signal(second_event, second_signal)

        self.assertFalse(payload["telegram_should_send"])
        self.assertEqual("same_asset_state_repeat", payload["telegram_suppression_reason"])

    def test_risk_blocker_state_is_sent(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        event = self._event(ts=1_710_000_000)
        signal = self._signal(
            event,
            trade_action_key="DO_NOT_CHASE_LONG",
            trade_action_reason="高潮后回吐风险",
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
            trade_action_debug={"strength_score": 0.84},
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertTrue(payload["telegram_should_send"])
        self.assertEqual("risk_blocker", payload["telegram_update_kind"])

    def test_candidate_state_is_sent(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        event = self._event(ts=1_710_000_000)
        signal = self._signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertTrue(payload["telegram_should_send"])
        self.assertEqual("candidate", payload["telegram_update_kind"])

    def test_suppression_reason_is_available_for_delivery_audit_payload(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        first_event = self._event(ts=1_710_000_000)
        first_signal = self._signal(first_event)
        manager.apply_lp_signal(first_event, first_signal)

        repeat_event = self._event(ts=1_710_000_020)
        repeat_signal = self._signal(repeat_event)
        payload = manager.apply_lp_signal(repeat_event, repeat_signal)

        self.assertEqual("same_asset_state_repeat", payload["telegram_suppression_reason"])
        self.assertFalse(repeat_event.metadata["telegram_should_send"])
        self.assertEqual("suppressed", repeat_signal.context["telegram_update_kind"])


if __name__ == "__main__":
    unittest.main()
