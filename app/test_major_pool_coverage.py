from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from quality_reports import build_major_pool_coverage_report


class StubQualityManager:
    def __init__(self, records: list[dict] | None = None) -> None:
        self._records = list(records or [])
        self.history_limit = 500
        self.actionable_min_samples = 2

    def _combined_records(self, limit=None):
        del limit
        return list(self._records)

    def _scope_scores(self, records, predicate):
        filtered = [row for row in records if predicate(row)]
        sample_size = len(filtered)
        return {
            "sample_size": sample_size,
            "quality_score": 0.7 if sample_size else 0.0,
            "climax_reversal_score": 0.6 if sample_size else 0.0,
            "market_context_alignment_score": 0.65 if sample_size else 0.0,
        }


class MajorPoolCoverageTests(unittest.TestCase):
    def test_major_pool_coverage_report_warns_when_pool_book_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = build_major_pool_coverage_report(
                StubQualityManager(),
                pool_book_path=Path(temp_dir) / "missing_lp_pools.json",
            )

        self.assertFalse(report["pool_book_exists"])
        self.assertTrue(report["warnings"])
        self.assertIn("lp_pools.json", report["warnings"][0])
        self.assertIn("BTC/USDT", report["missing_major_pairs"])

    def test_major_pool_coverage_flags_current_missing_btc_and_sol_assets(self) -> None:
        report = build_major_pool_coverage_report(StubQualityManager())

        self.assertIn("BTC", report["missing_major_assets"])
        self.assertIn("SOL", report["missing_major_assets"])
        self.assertIn("ETH/USDC", report["covered_expected_pairs"])
        self.assertIn("BTC/USDT", [item["pair_label"] for item in report["configured_but_disabled_major_pools"]])
        self.assertIn("SOL/USDT", report["recommended_next_round_pairs"])

    def test_enabled_major_pool_without_recent_signal_is_reported(self) -> None:
        payload = [
            {
                "pool_address": "0x1111111111111111111111111111111111111111",
                "chain": "ethereum",
                "pair_label": "BTC/USDT",
                "base_symbol": "BTC",
                "quote_symbol": "USDT",
                "canonical_asset": "BTC",
                "dex": "UnitTest",
                "protocol": "unit_test",
                "pool_type": "spot_lp",
                "enabled": True,
                "priority": 1,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "notes": "unit test BTC/USDT",
            },
            {
                "pool_address": "0x2222222222222222222222222222222222222222",
                "chain": "ethereum",
                "pair_label": "SOL/USDC",
                "base_symbol": "WSOL",
                "quote_symbol": "USDC",
                "canonical_asset": "SOL",
                "dex": "UnitTest",
                "protocol": "unit_test",
                "pool_type": "spot_lp",
                "enabled": True,
                "priority": 2,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "notes": "unit test SOL/USDC",
            },
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lp_pools.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            report = build_major_pool_coverage_report(
                StubQualityManager(),
                pool_book_path=path,
            )

        self.assertIn("BTC/USDT", report["covered_major_pairs"])
        self.assertIn("SOL/USDC", report["covered_major_pairs"])
        self.assertIn("BTC/USDT", report["no_recent_signal_major_pairs"])
        self.assertIn("SOL/USDC", report["no_recent_signal_major_pairs"])
        self.assertTrue(report["enabled_major_pools"])
        self.assertTrue(any(item["reason"] == "enabled_no_recent_signal" for item in report["next_round_priority"]))

    def test_major_pool_coverage_marks_recent_active_pairs(self) -> None:
        payload = [
            {
                "pool_address": "0x3333333333333333333333333333333333333333",
                "chain": "ethereum",
                "pair_label": "BTC/USDT",
                "base_symbol": "WBTC",
                "quote_symbol": "USDT",
                "canonical_asset": "BTC",
                "dex": "UnitTest",
                "protocol": "unit_test",
                "pool_type": "spot_lp",
                "enabled": True,
                "priority": 1,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "notes": "active BTC/USDT",
            }
        ]
        manager = StubQualityManager(records=[{"pair_label": "WBTC/USDT", "asset_symbol": "BTC"}])
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lp_pools.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            report = build_major_pool_coverage_report(manager, pool_book_path=path)

        self.assertIn("BTC/USDT", report["active_recent_major_pairs"])
        self.assertNotIn("BTC/USDT", report["no_recent_signal_major_pairs"])

    def test_placeholder_enabled_major_pool_is_reported_as_malformed(self) -> None:
        payload = [
            {
                "pool_address": "0xPLACEHOLDER_SOL_USDT",
                "chain": "replace_me_chain",
                "pair_label": "SOL/USDT",
                "base_symbol": "SOL",
                "quote_symbol": "USDT",
                "canonical_asset": "SOL",
                "dex": "REPLACE_ME",
                "protocol": "replace_me",
                "pool_type": "replace_me_pool_type",
                "enabled": True,
                "priority": 1,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "placeholder": True,
                "notes": "placeholder should stay disabled",
            }
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lp_pools.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            report = build_major_pool_coverage_report(
                StubQualityManager(),
                pool_book_path=path,
            )

        self.assertTrue(report["placeholder_major_pool_entries"])
        self.assertTrue(report["malformed_major_pool_entries"])
        self.assertIn("placeholder_enabled", report["malformed_major_pool_entries"][0]["reasons"])
        self.assertTrue(report["recommended_local_config_actions"])

    def test_coverage_report_outputs_recommended_actions_for_disabled_pairs(self) -> None:
        payload = [
            {
                "pool_address": "0x4444444444444444444444444444444444444444",
                "chain": "ethereum",
                "pair_label": "CBBTC/USDC.E",
                "base_symbol": "CBBTC",
                "quote_symbol": "USDC.E",
                "canonical_asset": "BTC",
                "dex": "UnitTest",
                "protocol": "unit_test",
                "pool_type": "spot_lp",
                "enabled": False,
                "priority": 1,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "notes": "configured but disabled BTC/USDC",
            }
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lp_pools.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            report = build_major_pool_coverage_report(
                StubQualityManager(),
                pool_book_path=path,
            )

        self.assertIn("BTC/USDC", [item["pair_label"] for item in report["configured_but_disabled_major_pools"]])
        self.assertTrue(
            any("disabled" in action or "补齐" in action for action in report["recommended_local_config_actions"])
        )


if __name__ == "__main__":
    unittest.main()
