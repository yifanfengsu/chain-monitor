import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from asset_market_state import AssetMarketStateManager
from models import Event, Signal


class _StubStateManager:
    def get_recent_lp_outcome_records(self, limit: int = 500):
        return []


class PrealertLifecycleTests(unittest.TestCase):
    def _event(self, *, intent_type: str = "pool_buy_pressure", ts: int = 1_710_000_000) -> Event:
        return Event(
            tx_hash=f"0xprealert{intent_type}{ts}",
            address="0xprealertpool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=35_000.0,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="preliminary",
            intent_confidence=0.72,
            confirmation_score=0.42,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={"token_symbol": "ETH", "raw": {"lp_context": {"pair_label": "ETH/USDC"}}},
        )

    def _signal(self, event: Event, **overrides) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.78,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="prealert_lifecycle_test",
            quality_score=0.80,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
            signal_id=f"sig:{event.ts}",
        )
        is_buy = event.intent_type == "pool_buy_pressure"
        context = {
            "user_tier": "research",
            "message_variant": "lp_directional",
            "lp_event": True,
            "pair_label": "ETH/USDC",
            "asset_case_label": "ETH",
            "asset_symbol": "ETH",
            "asset_case_id": "asset_case:ETH:prealert",
            "asset_case_direction": "buy_pressure" if is_buy else "sell_pressure",
            "asset_case_supporting_pair_count": 1,
            "lp_alert_stage": "prealert",
            "lp_confirm_scope": "local_confirm",
            "lp_confirm_quality": "unconfirmed_confirm",
            "lp_absorption_context": "pool_only_unconfirmed_pressure",
            "lp_broader_alignment": "weak_or_missing",
            "lp_sweep_phase": "sweep_building",
            "market_context_source": "live_public",
            "pair_quality_score": 0.64,
            "asset_case_quality_score": 0.68,
            "trade_action_key": "LONG_BIAS_OBSERVE" if is_buy else "SHORT_BIAS_OBSERVE",
            "trade_action_reason": "早期方向结构",
            "trade_action_required_confirmation": "等待 broader confirm",
            "trade_action_invalidated_by": "反向强信号",
            "trade_action_evidence_pack": "单池首发｜方向建立中",
            "trade_action_confidence": 0.64,
            "trade_action_debug": {"strength_score": 0.40},
            "lp_prealert_candidate": True,
            "lp_prealert_gate_passed": True,
            "lp_prealert_delivery_allowed": True,
            "prealert_precision_score": 0.74,
        }
        context.update(overrides)
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_prealert_gate_passed_sets_first_seen_stage_and_active_lifecycle(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        event = self._event(ts=1_710_000_000)
        signal = self._signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("prealert", payload["first_seen_stage"])
        self.assertEqual("active", payload["prealert_lifecycle_state"])

    def test_confirm_records_prealert_to_confirm_seconds(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        prealert_event = self._event(ts=1_710_000_000)
        prealert_signal = self._signal(prealert_event)
        manager.apply_lp_signal(prealert_event, prealert_signal)

        confirm_event = self._event(ts=1_710_000_045)
        confirm_signal = self._signal(
            confirm_event,
            lp_alert_stage="confirm",
            lp_confirm_scope="broader_confirm",
            lp_confirm_quality="clean_confirm",
            lp_absorption_context="broader_buy_pressure_confirmed",
            lp_sweep_phase="",
            trade_action_key="LONG_CHASE_ALLOWED",
            trade_action_debug={"strength_score": 0.86},
        )
        payload = manager.apply_lp_signal(confirm_event, confirm_signal)

        self.assertEqual(45, payload["prealert_to_confirm_sec"])
        self.assertIn(payload["prealert_lifecycle_state"], {"upgraded_to_confirm", "merged"})

    def test_prealert_can_expire_without_upgrade(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        prealert_event = self._event(ts=1_710_000_000)
        prealert_signal = self._signal(prealert_event)
        manager.apply_lp_signal(prealert_event, prealert_signal)

        later_event = self._event(ts=1_710_000_090)
        later_signal = self._signal(
            later_event,
            trade_action_key="WAIT_CONFIRMATION",
            lp_prealert_candidate=False,
            lp_prealert_gate_passed=False,
            trade_action_debug={"strength_score": 0.30},
        )
        payload = manager.apply_lp_signal(later_event, later_signal)

        self.assertEqual("expired", payload["prealert_lifecycle_state"])

    def test_prealert_suppressed_by_lock_still_records_lifecycle(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager())
        first_buy_event = self._event(ts=1_710_000_000)
        first_buy_signal = self._signal(
            first_buy_event,
            lp_alert_stage="confirm",
            lp_confirm_scope="broader_confirm",
            lp_confirm_quality="clean_confirm",
            lp_absorption_context="broader_buy_pressure_confirmed",
            lp_sweep_phase="sweep_confirmed",
            trade_action_key="LONG_CHASE_ALLOWED",
            trade_action_debug={"strength_score": 0.84},
            lp_prealert_candidate=False,
            lp_prealert_gate_passed=False,
        )
        manager.apply_lp_signal(first_buy_event, first_buy_signal)

        opposite_event = self._event(intent_type="pool_sell_pressure", ts=1_710_000_060)
        opposite_signal = self._signal(
            opposite_event,
            lp_alert_stage="confirm",
            lp_confirm_scope="broader_confirm",
            lp_confirm_quality="clean_confirm",
            lp_absorption_context="broader_sell_pressure_confirmed",
            lp_sweep_phase="sweep_confirmed",
            trade_action_key="SHORT_CHASE_ALLOWED",
            trade_action_debug={"strength_score": 0.84},
            lp_prealert_candidate=False,
            lp_prealert_gate_passed=False,
        )
        manager.apply_lp_signal(opposite_event, opposite_signal)

        locked_prealert_event = self._event(ts=1_710_000_090)
        locked_prealert_signal = self._signal(locked_prealert_event)
        payload = manager.apply_lp_signal(locked_prealert_event, locked_prealert_signal)

        self.assertEqual("suppressed_by_lock", payload["prealert_lifecycle_state"])
        self.assertFalse(payload["telegram_should_send"])


if __name__ == "__main__":
    unittest.main()
