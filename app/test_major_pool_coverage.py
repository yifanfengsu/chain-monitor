from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from quality_reports import build_major_pool_coverage_report
from lp_registry import UNISWAP_V3_ETHEREUM_FACTORY


USDC_CONTRACT = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
WBTC_CONTRACT = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"


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


def _major_entry(
    *,
    pool_address: str,
    pair_label: str,
    base_symbol: str,
    quote_symbol: str,
    canonical_asset: str,
    enabled: bool,
    chain: str = "ethereum",
    protocol: str = "uniswap_v3",
    pool_type: str = "clmm",
    fee_tier: int = 3000,
    priority: int = 1,
    base_contract: str = "",
    quote_contract: str = "",
) -> dict:
    return {
        "pool_address": pool_address,
        "chain": chain,
        "pair_label": pair_label,
        "base_symbol": base_symbol,
        "quote_symbol": quote_symbol,
        "base_contract": base_contract,
        "quote_contract": quote_contract,
        "canonical_asset": canonical_asset,
        "quote_canonical": "USDC" if "USDC" in quote_symbol.upper() else quote_symbol.replace(".E", ""),
        "dex": "UnitTest" if chain == "ethereum" else ("Orca" if chain == "solana" else "Uniswap"),
        "protocol": protocol,
        "pool_type": pool_type,
        "enabled": enabled,
        "priority": priority,
        "fee_tier": fee_tier,
        "major_pool": True,
        "major_match_mode": "major_family_match",
        "validation_required": protocol == "uniswap_v3",
        "source_note": "unit_test",
        "notes": f"unit test {pair_label}",
    }


def _validator(item, resolved):
    pair_label = str(resolved.get("pair_label") or "")
    if pair_label == "WBTC/USDT":
        return {
            "status": "passed",
            "token0": USDT_CONTRACT,
            "token1": WBTC_CONTRACT,
            "fee": int(resolved.get("fee_tier") or 0),
            "factory": UNISWAP_V3_ETHEREUM_FACTORY,
        }
    if pair_label == "WBTC/USDC":
        return {
            "status": "passed",
            "token0": USDC_CONTRACT,
            "token1": WBTC_CONTRACT,
            "fee": int(resolved.get("fee_tier") or 0),
            "factory": UNISWAP_V3_ETHEREUM_FACTORY,
        }
    return {
        "status": "failed",
        "reasons": ["token_pair_mismatch"],
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
        self.assertIn("SOL/USDT", report["recommended_next_round_pairs"])

    def test_enabled_major_pool_without_recent_signal_is_reported(self) -> None:
        payload = [
            _major_entry(
                pool_address="0x1111111111111111111111111111111111111111",
                pair_label="WBTC/USDT",
                base_symbol="WBTC",
                quote_symbol="USDT",
                canonical_asset="BTC",
                enabled=True,
                base_contract=WBTC_CONTRACT,
                quote_contract=USDT_CONTRACT,
            ),
            _major_entry(
                pool_address="0x2222222222222222222222222222222222222222",
                pair_label="SOL/USDC",
                base_symbol="WSOL",
                quote_symbol="USDC",
                canonical_asset="SOL",
                enabled=True,
                protocol="unit_test",
                pool_type="spot_lp",
                fee_tier=0,
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lp_pools.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            report = build_major_pool_coverage_report(
                StubQualityManager(),
                pool_book_path=path,
                onchain_validator=_validator,
            )

        self.assertIn("BTC/USDT", report["covered_major_pairs"])
        self.assertIn("SOL/USDC", report["covered_major_pairs"])
        self.assertIn("BTC/USDT", report["no_recent_signal_major_pairs"])
        self.assertIn("SOL/USDC", report["no_recent_signal_major_pairs"])
        self.assertTrue(report["enabled_major_pools"])
        self.assertTrue(any(item["reason"] == "enabled_no_recent_signal" for item in report["next_round_priority"]))

    def test_major_pool_coverage_marks_recent_active_pairs(self) -> None:
        payload = [
            _major_entry(
                pool_address="0x3333333333333333333333333333333333333333",
                pair_label="WBTC/USDT",
                base_symbol="WBTC",
                quote_symbol="USDT",
                canonical_asset="BTC",
                enabled=True,
                base_contract=WBTC_CONTRACT,
                quote_contract=USDT_CONTRACT,
            )
        ]
        manager = StubQualityManager(records=[{"pair_label": "WBTC/USDT", "asset_symbol": "BTC"}])
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lp_pools.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            report = build_major_pool_coverage_report(manager, pool_book_path=path, onchain_validator=_validator)

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
                "quote_canonical": "USDT",
                "dex": "REPLACE_ME",
                "protocol": "replace_me",
                "pool_type": "replace_me_pool_type",
                "enabled": True,
                "priority": 1,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "placeholder": True,
                "validation_required": False,
                "source_note": "unit_test",
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
            _major_entry(
                pool_address="0x4444444444444444444444444444444444444444",
                pair_label="CBBTC/USDC.E",
                base_symbol="CBBTC",
                quote_symbol="USDC.E",
                canonical_asset="BTC",
                enabled=False,
                base_contract="0x1111111111111111111111111111111111111111",
                quote_contract=USDC_CONTRACT,
            )
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

    def test_coverage_report_separates_unsupported_and_validation_failed(self) -> None:
        payload = [
            _major_entry(
                pool_address="0x99ac8ca7087fa4a2a1fb6357269965a2014abc35",
                pair_label="WBTC/USDC",
                base_symbol="WBTC",
                quote_symbol="USDC",
                canonical_asset="BTC",
                enabled=True,
                base_contract=WBTC_CONTRACT,
                quote_contract=USDC_CONTRACT,
            ),
            _major_entry(
                pool_address="0x9db9e0e53058c89e5b94e29621a205198648425b",
                pair_label="WBTC/USDT",
                base_symbol="WBTC",
                quote_symbol="USDT",
                canonical_asset="BTC",
                enabled=True,
                base_contract=WBTC_CONTRACT,
                quote_contract=USDT_CONTRACT,
            ),
            _major_entry(
                pool_address="Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE",
                pair_label="SOL/USDC",
                base_symbol="SOL",
                quote_symbol="USDC",
                canonical_asset="SOL",
                enabled=False,
                chain="solana",
                protocol="orca_whirlpool",
                fee_tier=0,
            ),
            _major_entry(
                pool_address="0x1cca8388e671e83010843a1a0535b04f2d7e946b",
                pair_label="SOL/USDC",
                base_symbol="SOL",
                quote_symbol="USDC",
                canonical_asset="SOL",
                enabled=False,
                chain="base",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lp_pools.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            report = build_major_pool_coverage_report(
                StubQualityManager(),
                pool_book_path=path,
                onchain_validator=lambda item, resolved: (
                    _validator(item, resolved)
                    if str(resolved.get("pair_label") or "") == "WBTC/USDC"
                    else {"status": "failed", "reasons": ["fee_tier_mismatch"]}
                ),
            )

        self.assertIn("BTC/USDC", report["covered_major_pairs"])
        self.assertTrue(report["configured_but_unsupported_chain"])
        self.assertTrue(report["configured_but_validation_failed"])
        self.assertNotIn("BTC/USDC", report["missing_major_pairs"])
        self.assertNotIn("BTC/USDT", report["missing_major_pairs"])
        self.assertNotIn("SOL/USDC", report["missing_major_pairs"])


if __name__ == "__main__":
    unittest.main()
