import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from analyzer import BehaviorAnalyzer
from market_context_adapter import FixtureMarketContextAdapter, UnavailableMarketContextAdapter
from models import Event, Signal
from notifier import format_signal_message
from pipeline import SignalPipeline
from price_service import PriceService
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer
from user_tiers import apply_user_tier_context


FIXTURE_CONTEXT = {
    "binance_perp:ETH": {
        "perp_last_price": 3512.0,
        "perp_mark_price": 3511.5,
        "perp_index_price": 3510.9,
        "spot_reference_price": 3511.2,
        "funding_direction": "positive",
        "funding_estimate": 0.00012,
        "basis_bps": 8.4,
        "market_move_before_alert_30s": 0.0010,
        "market_move_before_alert_60s": 0.0018,
        "market_move_after_alert_60s": 0.0062,
        "market_move_after_alert_300s": 0.0110,
    },
    "binance_perp:PEPE": {
        "perp_last_price": 0.0000102,
        "perp_mark_price": 0.0000102,
        "perp_index_price": 0.0000101,
        "spot_reference_price": 0.0000101,
        "market_move_before_alert_30s": 0.0200,
        "market_move_before_alert_60s": 0.0300,
        "market_move_after_alert_60s": 0.0020,
        "market_move_after_alert_300s": -0.0040,
    },
}


class MarketContextAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture_adapter = FixtureMarketContextAdapter(FIXTURE_CONTEXT)

    def _event(self, *, token: str = "ETH") -> Event:
        return Event(
            tx_hash=f"0xmarket{token}",
            address="0x1111111111111111111111111111111111111111",
            token=token,
            amount=1.0,
            side="买入",
            usd_value=25_000.0,
            kind="swap",
            ts=1_710_000_000,
            intent_type="pool_buy_pressure",
            intent_stage="preliminary",
            intent_confidence=0.70,
            confirmation_score=0.38,
            pricing_status="exact",
            pricing_confidence=0.92,
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

    def _signal(self, event: Event) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.82,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.80,
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
                "user_tier": "trader",
                "pair_label": f"{event.token}/USDC",
                "lp_event": True,
                "asset_case_label": event.token,
                "asset_case_id": "asset_case:test",
                "asset_case_supporting_pair_count": 2,
                "lp_alert_stage": "prealert",
                "lp_stage_badge": "预警",
                "lp_state_label": "买盘建立中",
                "lp_market_read": "先手观察｜需看 60s 是否续单",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "续单消失 / 快速反向池流",
                "lp_followup_required": True,
                "lp_scan_path": "secondary",
                "lp_detect_latency_ms": 800,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def _pipeline(self, adapter) -> SignalPipeline:
        state_manager = StateManager()
        return SignalPipeline(
            price_service=PriceService(),
            state_manager=state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=SignalQualityGate(state_manager=state_manager),
            market_context_adapter=adapter,
            quality_manager=QualityManager(state_manager=state_manager),
        )

    def test_fixture_adapter_returns_perp_and_spot_context(self) -> None:
        payload = self.fixture_adapter.get_market_context("ETH", 1_710_000_000)

        self.assertEqual("fixture", payload["market_context_source"])
        self.assertEqual(3512.0, payload["perp_last_price"])
        self.assertEqual(3511.2, payload["spot_reference_price"])
        self.assertEqual("leading", payload["alert_relative_timing"])

    def test_relative_timing_classification_supports_leading_confirming_and_late(self) -> None:
        confirming = self.fixture_adapter.classify_alert_relative_timing(
            {
                "market_move_before_alert_30s": 0.007,
                "market_move_before_alert_60s": 0.010,
                "market_move_after_alert_60s": 0.004,
            }
        )
        late = self.fixture_adapter.get_market_context("PEPE", 1_710_000_000)["alert_relative_timing"]

        self.assertEqual("confirming", confirming)
        self.assertEqual("late", late)

    def test_unavailable_fallback_is_graceful(self) -> None:
        adapter = UnavailableMarketContextAdapter()
        payload = adapter.get_market_context("ETH", 1_710_000_000)

        self.assertEqual("unavailable", payload["market_context_source"])
        self.assertEqual("", payload["alert_relative_timing"])

    def test_message_can_render_perp_timing_without_network(self) -> None:
        pipeline = self._pipeline(self.fixture_adapter)
        event = self._event(token="ETH")
        signal = self._signal(event)

        pipeline._annotate_market_context(event, signal)
        apply_user_tier_context(event, signal, {"user_tier": "trader"})
        message = format_signal_message(signal, event)

        self.assertIn("合约视角：领先", message)


if __name__ == "__main__":
    unittest.main()
