from pathlib import Path
import tempfile
import unittest

from models import Event
from quality_reports import build_major_pool_coverage_report
from quality_manager import QualityManager
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from lp_registry import classify_trend_pool_meta


class MajorPoolCoverageTests(unittest.TestCase):
    def test_major_pool_coverage_report_warns_when_pool_book_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = QualityManager(state_manager=StateManager())
            report = build_major_pool_coverage_report(
                manager,
                pool_book_path=Path(temp_dir) / "missing_lp_pools.json",
            )

        self.assertFalse(report["pool_book_exists"])
        self.assertTrue(report["warnings"])
        self.assertIn("lp_pools.json", report["warnings"][0])

    def test_major_pool_coverage_flags_missing_btc_and_sol_assets(self) -> None:
        report = build_major_pool_coverage_report(QualityManager(state_manager=StateManager()))

        self.assertIn("BTC", report["missing_major_assets"])
        self.assertIn("SOL", report["missing_major_assets"])
        self.assertIn("ETH/USDC", report["covered_expected_pairs"])
        self.assertTrue(report["covered_major_pools"])
        self.assertIn("BTC/USDT", report["recommended_next_round_pairs"])
        self.assertIn("SOL/USDC", report["recommended_next_round_pairs"])

    def test_sol_usdc_classifies_as_major_primary_pool(self) -> None:
        meta = classify_trend_pool_meta(
            {
                "pair_label": "SOL/USDC",
                "base_token_contract": "",
                "base_token_symbol": "WSOL",
                "quote_token_contract": "",
                "quote_token_symbol": "USDC",
                "token0_contract": "",
                "token0_symbol": "WSOL",
                "token1_contract": "",
                "token1_symbol": "USDC",
            }
        )

        self.assertTrue(meta["is_major_pool"])
        self.assertTrue(meta["is_primary_trend_pool"])
        self.assertGreater(meta["major_priority_score"], 1.0)

    def test_signal_quality_gate_marks_major_pool_priority_class(self) -> None:
        state_manager = StateManager()
        gate = SignalQualityGate(state_manager=state_manager)
        event = Event(
            tx_hash="0xsolmajor",
            address="0xsolpool",
            token="SOL",
            amount=1.0,
            side="买入",
            usd_value=35_000.0,
            kind="swap",
            ts=1_710_000_000,
            intent_type="pool_buy_pressure",
            intent_stage="confirmed",
            intent_confidence=0.84,
            confirmation_score=0.80,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "raw": {
                    "lp_context": {
                        "pair_label": "SOL/USDC",
                        "base_token_symbol": "SOL",
                        "quote_token_symbol": "USDC",
                    }
                },
                "lp_analysis": {
                    "reserve_skew": 0.20,
                    "action_intensity": 0.52,
                    "same_pool_continuity": 2,
                    "multi_pool_resonance": 2,
                    "pool_volume_surge_ratio": 2.1,
                },
            },
        )

        decision = gate.evaluate(
            event,
            {"strategy_role": "lp_pool", "pair_label": "SOL/USDC"},
            {"behavior_type": "pool_buy_pressure", "confidence": 0.84},
            {"alpha_score": 0.75, "structure_score": 0.75},
            {"score": 0.72},
            {"avg_usd": 10_000.0, "max_usd": 40_000.0},
            {"liquidity_proxy_usd": 2_000_000.0, "volume_24h_proxy_usd": 20_000_000.0},
        )

        self.assertEqual("major_pool", decision.metrics["lp_pool_priority_class"])
        self.assertTrue(decision.metrics["lp_major_pool"])


if __name__ == "__main__":
    unittest.main()
