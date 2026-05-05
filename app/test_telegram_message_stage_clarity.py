import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message


BANNED_TERMS = ("买入", "卖出", "入场", "做多", "做空", "开仓", "止损", "止盈", "杠杆", "仓位")


class TelegramMessageStageClarityTests(unittest.TestCase):
    def _event(self, *, ts: int = 1_710_200_000) -> Event:
        return Event(
            tx_hash=f"0xstageclarity{ts}",
            address="0xstageclaritypool",
            token="ETH",
            amount=1.0,
            side="流入",
            usd_value=75_000.0,
            kind="swap",
            ts=ts,
            intent_type="pool_buy_pressure",
            intent_stage="confirmed",
            intent_confidence=0.90,
            confirmation_score=0.88,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={"token_symbol": "ETH", "raw": {"lp_context": {"pair_label": "ETH/USDC"}}},
        )

    def _signal(self, event: Event, **context_overrides) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.90,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="stage_clarity_test",
            quality_score=0.88,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
            signal_id=f"sig:stage:{event.ts}",
        )
        context = {
            "message_variant": "lp_directional",
            "message_template": "long",
            "lp_event": True,
            "pair_label": "ETH/USDC",
            "asset_case_label": "ETH",
            "asset_symbol": "ETH",
            "trade_opportunity_side": "LONG",
            "trade_opportunity_score": 0.74,
            "trade_opportunity_confidence": "medium",
            "trade_opportunity_time_horizon": "60s",
            "trade_opportunity_reason": "结构进入观察，但需要 replay/profile 继续证明。",
            "trade_opportunity_evidence": ["broader_confirm", "live_context"],
            "trade_opportunity_required_confirmation": "等待 profile 样本和 followthrough 继续确认。",
            "trade_opportunity_invalidated_by": "反向强信号",
            "opportunity_profile_key": "ETH|LONG|confirm|normal",
            "trade_opportunity_replay_eligible": True,
        }
        context.update(context_overrides)
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def assertNoBannedTerms(self, message: str) -> None:
        for term in BANNED_TERMS:
            self.assertNotIn(term, message)

    def test_observe_message_is_explicitly_not_tradeable(self) -> None:
        event = self._event()
        signal = self._signal(event, trade_opportunity_status="")

        message = format_signal_message(signal, event)

        self.assertIn("观察级", message)
        self.assertIn("不可交易", message)
        self.assertNoBannedTerms(message)

    def test_candidate_message_is_explicitly_not_tradeable(self) -> None:
        event = self._event(ts=1_710_200_010)
        signal = self._signal(
            event,
            trade_opportunity_status="CANDIDATE",
            trade_opportunity_blockers=["profile_sample_count_insufficient"],
            trade_opportunity_verification_blockers=["profile_sample_count_insufficient"],
        )

        message = format_signal_message(signal, event)

        self.assertIn("候选级", message)
        self.assertIn("仍不可交易", message)
        self.assertIn("replay 支撑：样本不足", message)
        self.assertNoBannedTerms(message)

    def test_blocked_message_contains_blocker_and_no_action_text(self) -> None:
        event = self._event(ts=1_710_200_020)
        signal = self._signal(
            event,
            trade_opportunity_status="BLOCKED",
            trade_opportunity_primary_blocker="no_trade_lock",
            trade_opportunity_blockers=["no_trade_lock"],
        )

        message = format_signal_message(signal, event)

        self.assertIn("已阻断", message)
        self.assertIn("no_trade_lock", message)
        self.assertIn("不建议行动", message)
        self.assertNoBannedTerms(message)

    def test_verified_immature_message_is_explicitly_not_tradeable(self) -> None:
        event = self._event(ts=1_710_200_030)
        signal = self._signal(
            event,
            trade_opportunity_status="VERIFIED",
            trade_opportunity_maturity="immature",
            trade_opportunity_blockers=["profile_sample_count_insufficient"],
        )

        message = format_signal_message(signal, event)

        self.assertIn("验证未成熟", message)
        self.assertIn("不可交易", message)
        self.assertNoBannedTerms(message)


if __name__ == "__main__":
    unittest.main()
