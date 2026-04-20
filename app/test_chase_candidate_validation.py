import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from asset_market_state import AssetMarketStateManager
from models import Event, Signal


class _StubStateManager:
    def __init__(self, records) -> None:
        self._records = list(records)

    def get_recent_lp_outcome_records(self, limit: int = 500):
        return list(self._records)[-limit:]


def _outcome_row(*, direction_bucket: str, move_60s: float | None, adverse: bool | None, status: str = "completed") -> dict:
    return {
        "direction_bucket": direction_bucket,
        "lp_alert_stage": "confirm",
        "direction_adjusted_move_after_60s": move_60s,
        "adverse_by_direction_60s": adverse,
        "outcome_price_source": "okx_mark",
        "outcome_windows": {
            "60s": {
                "status": status,
                "direction_adjusted_move_after": move_60s,
                "adverse_by_direction": adverse,
                "price_source": "okx_mark",
            }
        },
    }


class ChaseCandidateValidationTests(unittest.TestCase):
    def _event(self, *, intent_type: str = "pool_buy_pressure", ts: int = 1_710_000_000) -> Event:
        return Event(
            tx_hash=f"0xchase{intent_type}{ts}",
            address="0xchasepool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=75_000.0,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.90,
            confirmation_score=0.88,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={"token_symbol": "ETH", "raw": {"lp_context": {"pair_label": "ETH/USDC"}}},
        )

    def _signal(self, event: Event) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.92,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="candidate_validation_test",
            quality_score=0.90,
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
            "asset_case_id": "asset_case:ETH:candidate",
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
            "trade_action_evidence_pack": "跨池2｜OKX 同向｜clean confirm",
            "trade_action_confidence": 0.90,
            "trade_action_debug": {"strength_score": 0.88},
        }
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_insufficient_samples_stays_long_candidate(self) -> None:
        manager = AssetMarketStateManager(state_manager=_StubStateManager([]))
        event = self._event()
        signal = self._signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("LONG_CANDIDATE", payload["asset_market_state_key"])

    def test_good_rolling_success_unlocks_tradeable_long(self) -> None:
        records = [_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = AssetMarketStateManager(state_manager=_StubStateManager(records))
        event = self._event()
        signal = self._signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("TRADEABLE_LONG", payload["asset_market_state_key"])

    def test_high_adverse_or_low_completion_blocks_tradeable(self) -> None:
        records = []
        for _ in range(12):
            records.append(_outcome_row(direction_bucket="buy_pressure", move_60s=-0.003, adverse=True))
        for _ in range(8):
            records.append(_outcome_row(direction_bucket="buy_pressure", move_60s=None, adverse=None, status="expired"))
        manager = AssetMarketStateManager(state_manager=_StubStateManager(records))
        event = self._event()
        signal = self._signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertIn(payload["asset_market_state_key"], {"OBSERVE_LONG", "LONG_CANDIDATE"})
        self.assertNotEqual("TRADEABLE_LONG", payload["asset_market_state_key"])

    def test_short_side_mirror_can_unlock_tradeable_short(self) -> None:
        records = [_outcome_row(direction_bucket="sell_pressure", move_60s=0.006, adverse=False) for _ in range(20)]
        manager = AssetMarketStateManager(state_manager=_StubStateManager(records))
        event = self._event(intent_type="pool_sell_pressure")
        signal = self._signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("TRADEABLE_SHORT", payload["asset_market_state_key"])


if __name__ == "__main__":
    unittest.main()
