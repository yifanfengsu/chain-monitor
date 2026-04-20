import os
import tempfile
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


class AssetMarketStateTests(unittest.TestCase):
    def _event(self, *, intent_type: str = "pool_buy_pressure", ts: int = 1_710_000_000) -> Event:
        return Event(
            tx_hash=f"0xassetstate{intent_type}{ts}",
            address="0xassetstatepool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=55_000.0,
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
            confidence=0.88,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="asset_market_state_test",
            quality_score=0.86,
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
            "asset_case_id": "asset_case:ETH:market_state",
            "asset_case_direction": "buy_pressure" if is_buy else "sell_pressure",
            "asset_case_supporting_pair_count": 2,
            "lp_alert_stage": "confirm",
            "lp_confirm_scope": "broader_confirm",
            "lp_confirm_quality": "clean_confirm",
            "lp_absorption_context": "broader_buy_pressure_confirmed" if is_buy else "broader_sell_pressure_confirmed",
            "lp_broader_alignment": "confirmed",
            "lp_sweep_phase": "",
            "lp_sweep_continuation_score": 0.84,
            "market_context_source": "live_public",
            "alert_relative_timing": "confirming",
            "pool_quality_score": 0.70,
            "pair_quality_score": 0.72,
            "asset_case_quality_score": 0.78,
            "quality_scope_asset_size": 10,
            "quality_actionable_sample_size": 3,
            "trade_action_key": "LONG_CHASE_ALLOWED" if is_buy else "SHORT_CHASE_ALLOWED",
            "trade_action_label": "可顺势追多" if is_buy else "可顺势追空",
            "trade_action_reason": "链上方向扩散",
            "trade_action_required_confirmation": "继续看 broader confirm",
            "trade_action_invalidated_by": "反向强信号",
            "trade_action_evidence_pack": "跨池2｜OKX 同向｜clean confirm",
            "trade_action_confidence": 0.88,
            "trade_action_debug": {"strength_score": 0.86},
            "lp_prealert_candidate": False,
            "lp_prealert_gate_passed": False,
            "prealert_precision_score": 0.72,
        }
        context.update(overrides)
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_local_buy_pressure_maps_to_wait_or_observe_long(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        event = self._event()
        signal = self._signal(
            event,
            trade_action_key="LONG_BIAS_OBSERVE",
            lp_alert_stage="prealert",
            lp_confirm_scope="local_confirm",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            trade_action_debug={"strength_score": 0.42},
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertIn(payload["asset_market_state_key"], {"WAIT_CONFIRMATION", "OBSERVE_LONG"})

    def test_broader_confirm_defaults_to_long_candidate_without_post_validation(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        event = self._event()
        signal = self._signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("LONG_CANDIDATE", payload["asset_market_state_key"])
        self.assertNotEqual("TRADEABLE_LONG", payload["asset_market_state_key"])

    def test_same_state_repeat_is_suppressed(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        event1 = self._event(ts=1_710_000_000)
        signal1 = self._signal(event1)
        manager.apply_lp_signal(event1, signal1)

        event2 = self._event(ts=1_710_000_030)
        signal2 = self._signal(event2)
        payload2 = manager.apply_lp_signal(event2, signal2)

        self.assertFalse(payload2["asset_market_state_changed"])
        self.assertFalse(payload2["telegram_should_send"])
        self.assertEqual("same_asset_state_repeat", payload2["telegram_suppression_reason"])

    def test_state_transition_triggers_telegram_send(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        event1 = self._event(ts=1_710_000_000)
        signal1 = self._signal(
            event1,
            trade_action_key="LONG_BIAS_OBSERVE",
            lp_alert_stage="prealert",
            lp_confirm_scope="local_confirm",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            trade_action_debug={"strength_score": 0.40},
        )
        manager.apply_lp_signal(event1, signal1)

        event2 = self._event(ts=1_710_000_090)
        signal2 = self._signal(event2)
        payload2 = manager.apply_lp_signal(event2, signal2)

        self.assertTrue(payload2["asset_market_state_changed"])
        self.assertTrue(payload2["telegram_should_send"])
        self.assertEqual("candidate", payload2["telegram_update_kind"])

    def test_persisted_state_recovers_without_repeat_spam(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = f"{temp_dir}/asset_market_state_cache.json"
            manager1 = AssetMarketStateManager(
                state_manager=_StubStateManager(),
                cache_path=cache_path,
                persistence_enabled=True,
                recover_on_start=False,
            )
            event1 = self._event(ts=1_710_000_000)
            signal1 = self._signal(event1)
            payload1 = manager1.apply_lp_signal(event1, signal1)
            self.assertTrue(payload1["telegram_should_send"])
            manager1.flush(force=True)

            manager2 = AssetMarketStateManager(
                state_manager=_StubStateManager(),
                cache_path=cache_path,
                persistence_enabled=True,
                recover_on_start=True,
            )
            event2 = self._event(ts=1_710_000_030)
            signal2 = self._signal(event2)
            payload2 = manager2.apply_lp_signal(event2, signal2)

            self.assertFalse(payload2["asset_market_state_changed"])
            self.assertFalse(payload2["telegram_should_send"])


if __name__ == "__main__":
    unittest.main()
