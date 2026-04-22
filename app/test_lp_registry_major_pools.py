from __future__ import annotations

import unittest

from lp_registry import classify_trend_pool_meta, normalize_lp_pool_entries


class LpRegistryMajorPoolsTests(unittest.TestCase):
    def test_enabled_major_alias_pools_are_canonicalized(self) -> None:
        pools = normalize_lp_pool_entries(
            [
                {
                    "pool_address": "0x1111111111111111111111111111111111111111",
                    "chain": "ethereum",
                    "pair_label": "WBTC/USDC.E",
                    "base_symbol": "WBTC",
                    "quote_symbol": "USDC.E",
                    "canonical_asset": "BTC",
                    "dex": "UnitTest",
                    "protocol": "unit_test",
                    "pool_type": "spot_lp",
                    "enabled": True,
                    "priority": 1,
                    "major_pool": True,
                    "major_match_mode": "major_family_match",
                    "notes": "wbtc/usdce major pool",
                },
                {
                    "pool_address": "0x2222222222222222222222222222222222222222",
                    "chain": "ethereum",
                    "pair_label": "WSOL/USDT",
                    "base_symbol": "WSOL",
                    "quote_symbol": "USDT",
                    "canonical_asset": "SOL",
                    "dex": "UnitTest",
                    "protocol": "unit_test",
                    "pool_type": "spot_lp",
                    "enabled": True,
                    "priority": 2,
                    "major_pool": True,
                    "major_match_mode": "major_family_match",
                    "notes": "wsol/usdt major pool",
                },
            ]
        )

        btc_meta = pools["0x1111111111111111111111111111111111111111"]
        sol_meta = pools["0x2222222222222222222222222222222222222222"]

        self.assertTrue(btc_meta["is_major_pool"])
        self.assertEqual("BTC", btc_meta["canonical_asset"])
        self.assertEqual("BTC/USDC", btc_meta["canonical_pair_label"])
        self.assertGreater(btc_meta["major_priority_score"], 1.0)
        self.assertTrue(btc_meta["is_primary_trend_pool"])

        self.assertTrue(sol_meta["is_major_pool"])
        self.assertEqual("SOL", sol_meta["canonical_asset"])
        self.assertEqual("SOL/USDT", sol_meta["canonical_pair_label"])
        self.assertEqual("family_match", sol_meta["trend_pool_match_mode"])

    def test_non_major_pool_is_not_misclassified(self) -> None:
        pools = normalize_lp_pool_entries(
            [
                {
                    "pool_address": "0x3333333333333333333333333333333333333333",
                    "chain": "ethereum",
                    "pair_label": "PEPE/USDC",
                    "base_symbol": "PEPE",
                    "quote_symbol": "USDC",
                    "canonical_asset": "PEPE",
                    "dex": "UnitTest",
                    "protocol": "unit_test",
                    "pool_type": "spot_lp",
                    "enabled": True,
                    "priority": 1,
                    "major_pool": False,
                    "major_match_mode": "non_major_pool",
                    "notes": "non-major pool",
                }
            ]
        )

        meta = pools["0x3333333333333333333333333333333333333333"]
        self.assertFalse(meta["is_major_pool"])
        self.assertEqual(1.0, meta["major_priority_score"])
        self.assertEqual("", meta["major_base_symbol"])
        self.assertEqual("non_trend_pool", meta["trend_pool_match_mode"])

    def test_usdce_and_cbbtc_classify_as_major_family_match(self) -> None:
        meta = classify_trend_pool_meta(
            {
                "pair_label": "CBBTC/USDC.E",
                "base_token_contract": "",
                "base_token_symbol": "CBBTC",
                "quote_token_contract": "",
                "quote_token_symbol": "USDC.E",
                "token0_contract": "",
                "token0_symbol": "CBBTC",
                "token1_contract": "",
                "token1_symbol": "USDC.E",
            }
        )

        self.assertTrue(meta["is_major_pool"])
        self.assertEqual("BTC", meta["major_base_symbol"])
        self.assertEqual("USDC", meta["major_quote_symbol"])
        self.assertTrue(meta["is_primary_trend_pool"])


if __name__ == "__main__":
    unittest.main()
