import time
import unittest
from unittest.mock import patch

from analyzer import BehaviorAnalyzer
import listener
from models import Event, Signal
from pipeline import SignalPipeline
from price_service import PriceService
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


class LpPrealertFastlaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state_manager = StateManager()
        self.quality_gate = SignalQualityGate(state_manager=self.state_manager)
        self.pipeline = SignalPipeline(
            price_service=PriceService(),
            state_manager=self.state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=self.quality_gate,
        )

    def _event(
        self,
        *,
        address: str = "0x1111111111111111111111111111111111111111",
        usd_value: float = 2_000.0,
        same_pool_continuity: int = 1,
        multi_pool_resonance: int = 0,
        pool_volume_surge_ratio: float = 1.35,
        reserve_skew: float = 0.12,
        action_intensity: float = 0.28,
        confirmation_score: float = 0.33,
        ts: int = 1_710_000_000,
    ) -> Event:
        return Event(
            tx_hash=f"0xprealert{ts}",
            address=address,
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=usd_value,
            kind="swap",
            ts=ts,
            intent_type="pool_buy_pressure",
            intent_stage="preliminary",
            intent_confidence=0.58,
            confirmation_score=confirmation_score,
            pricing_status="exact",
            pricing_confidence=0.90,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "pool_label": "ETH/USDC",
                        "base_amount": 1.0,
                        "quote_amount": usd_value,
                    }
                },
                "lp_analysis": {
                    "reserve_skew": reserve_skew,
                    "action_intensity": action_intensity,
                    "same_pool_continuity": same_pool_continuity,
                    "multi_pool_resonance": multi_pool_resonance,
                    "pool_volume_surge_ratio": pool_volume_surge_ratio,
                },
            },
        )

    def _signal(self, event: Event, *, stage: str = "prealert") -> Signal:
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
            signal_id=f"sig:{event.ts}:{stage}",
            metadata={"pair_label": "ETH/USDC"},
        )
        signal.context.update({
            "pair_label": "ETH/USDC",
            "lp_alert_stage": stage,
        })
        signal.metadata["lp_alert_stage"] = stage
        return signal

    def _token_snapshot(self) -> dict:
        return {
            "liquidity_proxy_usd": 80_000.0,
            "volume_24h_proxy_usd": 800_000.0,
            "buy_cluster_5m": 2,
            "sell_cluster_5m": 0,
            "token_net_flow_usd_5m": 12_000.0,
            "token_net_flow_usd_1h": 30_000.0,
            "resonance_5m": {"resonance_score": 0.40, "same_side_unique_addresses": 2},
            "resonance_15m": {"resonance_score": 0.36},
            "resonance_1h": {"resonance_score": 0.32},
        }

    def _address_snapshot(self) -> dict:
        return {
            "avg_usd": 100.0,
            "max_usd": 500.0,
            "windows": {"24h": {"avg_usd": 100.0, "max_usd": 500.0}},
        }

    def test_single_pool_first_directional_event_can_trigger_prealert(self) -> None:
        event = self._event()
        decision = self.quality_gate.evaluate(
            event,
            {"strategy_role": "lp_pool", "priority": 3},
            {"behavior_type": "pool_buy_pressure", "confidence": 0.82, "reason": "unit_test"},
            {"score": 1.0, "alpha_score": 1.0},
            {"score": 1.0},
            self._address_snapshot(),
            self._token_snapshot(),
        )

        self.assertTrue(decision.passed)
        self.assertTrue(decision.metrics.get("lp_prealert_candidate"))
        self.assertTrue(decision.metrics.get("lp_prealert_applied"))
        self.assertEqual(1, decision.metrics.get("lp_same_pool_continuity"))
        self.assertEqual(0, decision.metrics.get("lp_multi_pool_resonance"))
        self.assertEqual("gate_prealert_observe_pass", decision.metrics.get("lp_stage_decision"))

    def test_confirm_strength_lp_does_not_route_back_to_prealert(self) -> None:
        engine = StrategyEngine()
        event = self._event(
            same_pool_continuity=2,
            multi_pool_resonance=2,
            pool_volume_surge_ratio=2.4,
            reserve_skew=0.22,
            action_intensity=0.56,
            confirmation_score=0.84,
            usd_value=80_000.0,
        )

        allowed = engine._allow_lp_prealert_observe(
            event=event,
            lp_trend_primary_pool=True,
            lp_prealert_applied=True,
            pricing_confidence=event.pricing_confidence,
            confirmation_score=event.confirmation_score,
            same_pool_continuity=2,
            multi_pool_resonance=2,
            lp_volume_surge_ratio=2.4,
        )

        self.assertFalse(allowed)

    def test_prealert_pool_is_promoted_into_fastlane(self) -> None:
        event = self._event()
        decision = self.quality_gate.evaluate(
            event,
            {"strategy_role": "lp_pool", "priority": 3},
            {"behavior_type": "pool_buy_pressure", "confidence": 0.82, "reason": "unit_test"},
            {"score": 1.0, "alpha_score": 1.0},
            {"score": 1.0},
            self._address_snapshot(),
            self._token_snapshot(),
        )
        signal = self._signal(event, stage="prealert")

        payload = self.pipeline._promote_lp_fastlane_if_needed(event, signal, decision.metrics)

        self.assertEqual(str(event.address).lower(), payload.get("pool_address"))
        self.assertEqual("prealert", payload.get("last_signal_stage"))
        self.assertIn(str(event.address).lower(), self.state_manager.get_active_lp_fastlane_pool_addresses(now_ts=event.ts))
        self.assertEqual(str(payload.get("promote_reason") or ""), signal.metadata.get("lp_promote_reason"))

    def test_promoted_pool_scan_path_is_distinct_from_secondary_path(self) -> None:
        promoted_pool = "0x1111111111111111111111111111111111111111"
        secondary_pool = "0x2222222222222222222222222222222222222222"
        self.state_manager.register_lp_fastlane_pool(
            promoted_pool,
            now_ts=1_710_000_000,
            ttl_sec=120,
            reason="lp_prealert_fastlane",
            alert_stage="prealert",
            signal_id="sig:test",
            priority_score=0.64,
        )

        logs = [
            {
                "topics": [
                    listener.ERC20_TRANSFER_EVENT_SIG,
                    listener._topic_for_address(promoted_pool),
                    listener._topic_for_address("0x9999999999999999999999999999999999999999"),
                ],
                "scan_group": "lp_promoted_main",
            },
            {
                "topics": [
                    listener.ERC20_TRANSFER_EVENT_SIG,
                    listener._topic_for_address(secondary_pool),
                    listener._topic_for_address("0x8888888888888888888888888888888888888888"),
                ],
                "scan_group": "runtime_adjacent_secondary",
            },
        ]

        with patch.object(listener, "ACTIVE_LP_POOL_ADDRESSES", {promoted_pool, secondary_pool}), patch.object(listener, "WATCH_ADDRESSES", set()):
            promoted = listener._lp_promoted_scan_addresses(now_ts=1_710_000_000)
            paths = listener._lp_scan_paths_by_pool_from_logs(logs, [promoted_pool, secondary_pool])

        self.assertEqual([promoted_pool], promoted)
        self.assertEqual("promoted_main", paths.get(promoted_pool))
        self.assertEqual("secondary", paths.get(secondary_pool))

    def test_primary_trend_scan_uses_its_own_interval(self) -> None:
        with patch.object(listener, "LOW_CU_MODE", False), patch.object(listener, "LP_SECONDARY_SCAN_ENABLE", True), patch.object(listener, "LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN", False), patch.object(listener, "LP_PRIMARY_TREND_SCAN_INTERVAL_SEC", 10.0), patch.object(listener, "LP_SECONDARY_SCAN_INTERVAL_SEC", 60.0), patch.object(listener, "_lp_primary_trend_scan_addresses", return_value=["0xpool"]), patch.object(listener.time, "time", return_value=100.0):
            self.assertEqual(10.0, listener._effective_lp_primary_trend_interval_sec())
            should_run, reason = listener._should_run_lp_primary_trend_scan(last_scan_ts=70.0)

        self.assertTrue(should_run)
        self.assertEqual("lp_primary_trend_scheduled", reason)


if __name__ == "__main__":
    unittest.main()
