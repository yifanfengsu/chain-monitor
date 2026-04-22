from __future__ import annotations

import unittest

from lp_registry import normalize_lp_pool_entries
from models import Event, Signal
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from user_tiers import evaluate_user_tier_lp_delivery


class MajorPoolEnabledRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state_manager = StateManager()
        self.gate = SignalQualityGate(state_manager=self.state_manager)

    def _event_from_meta(self, meta: dict, *, token: str) -> Event:
        return Event(
            tx_hash=f"0x{token.lower()}majorroute",
            address=str(meta.get("pool_address") or meta.get("address") or ""),
            token=token,
            amount=1.0,
            side="买入",
            usd_value=28_000.0,
            kind="swap",
            ts=1_710_000_000,
            intent_type="pool_buy_pressure",
            intent_stage="preliminary",
            intent_confidence=0.60,
            confirmation_score=0.34,
            pricing_status="exact",
            pricing_confidence=0.92,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": token,
                "raw": {
                    "lp_context": {
                        "pair_label": meta.get("pair_label"),
                        "pool_label": meta.get("pair_label"),
                        "base_token_symbol": meta.get("base_token_symbol"),
                        "quote_token_symbol": meta.get("quote_token_symbol"),
                        "dex": meta.get("dex"),
                        "protocol": meta.get("protocol"),
                    }
                },
                "lp_analysis": {
                    "reserve_skew": 0.18,
                    "action_intensity": 0.48,
                    "same_pool_continuity": 1,
                    "multi_pool_resonance": 0,
                    "pool_volume_surge_ratio": 1.35,
                },
            },
        )

    def _signal(self, event: Event, *, tier: str, major_pool: bool, major_priority_score: float) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.80,
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
                "user_tier": tier,
                "lp_alert_stage": "prealert",
                "asset_case_label": event.token,
                "asset_case_supporting_pair_count": 1,
                "pool_quality_score": 0.66,
                "pair_quality_score": 0.66,
                "asset_case_quality_score": 0.66,
                "prealert_precision_score": 0.76,
                "fastlane_roi_score": 0.58,
                "lp_major_pool": major_pool,
                "lp_major_priority_score": major_priority_score,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def test_enabled_major_pool_affects_gate_and_user_tier_routing(self) -> None:
        pools = normalize_lp_pool_entries(
            [
                {
                    "pool_address": "0x1111111111111111111111111111111111111111",
                    "chain": "ethereum",
                    "pair_label": "BTC/USDT",
                    "base_symbol": "BTC",
                    "quote_symbol": "USDT",
                    "canonical_asset": "BTC",
                    "quote_canonical": "USDT",
                    "dex": "UnitTest",
                    "protocol": "unit_test",
                    "pool_type": "spot_lp",
                    "enabled": True,
                    "priority": 1,
                    "major_pool": True,
                    "major_match_mode": "major_family_match",
                    "validation_required": False,
                    "source_note": "unit_test",
                    "notes": "btc/usdt enabled major pool",
                }
            ]
        )
        meta = pools["0x1111111111111111111111111111111111111111"]
        event = self._event_from_meta(meta, token="BTC")

        decision = self.gate.evaluate(
            event,
            {"strategy_role": "lp_pool", "pair_label": str(meta.get("pair_label") or "")},
            {"behavior_type": "pool_buy_pressure", "confidence": 0.80},
            {"score": 0.78, "alpha_score": 0.78},
            {"score": 0.75},
            {"avg_usd": 5_000.0, "max_usd": 35_000.0},
            {"liquidity_proxy_usd": 2_500_000.0, "volume_24h_proxy_usd": 18_000_000.0},
        )
        signal = self._signal(
            event,
            tier="trader",
            major_pool=bool(decision.metrics["lp_major_pool"]),
            major_priority_score=float(decision.metrics["lp_major_priority_score"]),
        )
        allowed, reason = evaluate_user_tier_lp_delivery(event, signal, "observe")

        self.assertTrue(decision.metrics["lp_major_pool"])
        self.assertGreater(decision.metrics["lp_major_priority_score"], 1.0)
        self.assertEqual("major_pool", decision.metrics["lp_pool_priority_class"])
        self.assertTrue(allowed)
        self.assertEqual("user_tier_trader_allowed", reason)

    def test_non_major_pool_is_not_promoted_as_major(self) -> None:
        pools = normalize_lp_pool_entries(
            [
                {
                    "pool_address": "0x2222222222222222222222222222222222222222",
                    "chain": "ethereum",
                    "pair_label": "PEPE/USDC",
                    "base_symbol": "PEPE",
                    "quote_symbol": "USDC",
                    "canonical_asset": "PEPE",
                    "quote_canonical": "USDC",
                    "dex": "UnitTest",
                    "protocol": "unit_test",
                    "pool_type": "spot_lp",
                    "enabled": True,
                    "priority": 1,
                    "major_pool": False,
                    "major_match_mode": "non_major_pool",
                    "validation_required": False,
                    "source_note": "unit_test",
                    "notes": "non major pool",
                }
            ]
        )
        meta = pools["0x2222222222222222222222222222222222222222"]
        event = self._event_from_meta(meta, token="PEPE")

        decision = self.gate.evaluate(
            event,
            {"strategy_role": "lp_pool", "pair_label": str(meta.get("pair_label") or "")},
            {"behavior_type": "pool_buy_pressure", "confidence": 0.80},
            {"score": 0.78, "alpha_score": 0.78},
            {"score": 0.75},
            {"avg_usd": 5_000.0, "max_usd": 35_000.0},
            {"liquidity_proxy_usd": 2_500_000.0, "volume_24h_proxy_usd": 18_000_000.0},
        )

        self.assertFalse(decision.metrics["lp_major_pool"])
        self.assertEqual(1.0, decision.metrics["lp_major_priority_score"])
        self.assertEqual("standard_pool", decision.metrics["lp_pool_priority_class"])


if __name__ == "__main__":
    unittest.main()
