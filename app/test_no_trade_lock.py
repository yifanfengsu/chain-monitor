import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from asset_market_state import AssetMarketStateManager
from models import Event, Signal


class _StubStateManager:
    def get_recent_lp_outcome_records(self, limit: int = 500):
        return []


class NoTradeLockTests(unittest.TestCase):
    def _event(self, *, intent_type: str, ts: int) -> Event:
        return Event(
            tx_hash=f"0xlock{intent_type}{ts}",
            address="0xlockpool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=65_000.0,
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
            metadata={
                "token_symbol": "ETH",
                "raw": {"lp_context": {"pair_label": "ETH/USDC", "pool_label": "ETH/USDC"}},
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
            reason="no_trade_lock_test",
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
            "asset_case_id": "asset_case:ETH:lock",
            "asset_case_direction": "buy_pressure" if is_buy else "sell_pressure",
            "asset_case_supporting_pair_count": 2,
            "lp_alert_stage": "confirm",
            "lp_confirm_scope": "broader_confirm",
            "lp_confirm_quality": "clean_confirm",
            "lp_absorption_context": "broader_buy_pressure_confirmed" if is_buy else "broader_sell_pressure_confirmed",
            "lp_broader_alignment": "confirmed",
            "lp_sweep_phase": "sweep_confirmed",
            "market_context_source": "live_public",
            "pair_quality_score": 0.74,
            "asset_case_quality_score": 0.80,
            "trade_action_key": "LONG_CHASE_ALLOWED" if is_buy else "SHORT_CHASE_ALLOWED",
            "trade_action_reason": "方向扩散",
            "trade_action_required_confirmation": "继续看 broader confirm",
            "trade_action_invalidated_by": "反向强信号",
            "trade_action_evidence_pack": "跨池2｜clean confirm",
            "trade_action_confidence": 0.88,
            "trade_action_debug": {"strength_score": 0.84},
        }
        context.update(overrides)
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_opposite_strong_signals_enter_no_trade_lock(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        buy_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        buy_signal = self._signal(buy_event)
        manager.apply_lp_signal(buy_event, buy_signal)

        sell_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_090)
        sell_signal = self._signal(sell_event)
        payload = manager.apply_lp_signal(sell_event, sell_signal)

        self.assertEqual("NO_TRADE_LOCK", payload["asset_market_state_key"])
        self.assertTrue(payload["no_trade_lock_active"])
        self.assertTrue(payload["telegram_should_send"])

    def test_lock_suppresses_followup_local_signals(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        first_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        first_signal = self._signal(first_event)
        manager.apply_lp_signal(first_event, first_signal)
        lock_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_090)
        lock_signal = self._signal(lock_event)
        manager.apply_lp_signal(lock_event, lock_signal)

        local_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_120)
        local_signal = self._signal(
            local_event,
            trade_action_key="LONG_BIAS_OBSERVE",
            lp_alert_stage="prealert",
            lp_confirm_scope="local_confirm",
            lp_sweep_phase="sweep_building",
            trade_action_debug={"strength_score": 0.42},
        )
        payload = manager.apply_lp_signal(local_event, local_signal)

        self.assertEqual("NO_TRADE_LOCK", payload["asset_market_state_key"])
        self.assertFalse(payload["telegram_should_send"])

    def test_clean_broader_strong_side_releases_lock(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        buy_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        manager.apply_lp_signal(buy_event, self._signal(buy_event))
        sell_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_090)
        manager.apply_lp_signal(sell_event, self._signal(sell_event))

        release_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_220)
        release_signal = self._signal(release_event)
        payload = manager.apply_lp_signal(release_event, release_signal)

        self.assertIn(payload["asset_market_state_key"], {"SHORT_CANDIDATE", "TRADEABLE_SHORT"})
        self.assertFalse(payload["no_trade_lock_active"])
        self.assertEqual("broader_clean_dominance", payload["no_trade_lock_released_by"])

    def test_lock_expiry_allows_reassessment(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        buy_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        manager.apply_lp_signal(buy_event, self._signal(buy_event))
        sell_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_090)
        manager.apply_lp_signal(sell_event, self._signal(sell_event))

        later_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_500)
        later_signal = self._signal(
            later_event,
            trade_action_key="LONG_BIAS_OBSERVE",
            lp_alert_stage="prealert",
            lp_confirm_scope="local_confirm",
            lp_sweep_phase="sweep_building",
            trade_action_debug={"strength_score": 0.44},
        )
        payload = manager.apply_lp_signal(later_event, later_signal)

        self.assertIn(payload["asset_market_state_key"], {"WAIT_CONFIRMATION", "OBSERVE_LONG"})
        self.assertFalse(payload["no_trade_lock_active"])

    def test_suppressed_signal_payload_preserves_reason_for_archive(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        first_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_000)
        first_signal = self._signal(first_event)
        manager.apply_lp_signal(first_event, first_signal)

        second_event = self._event(intent_type="pool_buy_pressure", ts=1_710_000_020)
        second_signal = self._signal(second_event)
        payload = manager.apply_lp_signal(second_event, second_signal)

        self.assertEqual("same_asset_state_repeat", payload["telegram_suppression_reason"])
        self.assertEqual("same_asset_state_repeat", second_event.metadata["telegram_suppression_reason"])
        self.assertEqual("suppressed", second_signal.context["telegram_update_kind"])


if __name__ == "__main__":
    unittest.main()
