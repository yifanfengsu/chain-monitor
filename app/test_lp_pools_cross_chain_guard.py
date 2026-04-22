from __future__ import annotations

import unittest

from lp_registry import normalize_lp_pool_entries, validate_lp_pool_book_entries


class LpPoolsCrossChainGuardTests(unittest.TestCase):
    def test_solana_pool_is_marked_unsupported_and_not_normalized(self) -> None:
        entries = [
            {
                "pool_address": "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE",
                "chain": "solana",
                "pair_label": "SOL/USDC",
                "base_symbol": "SOL",
                "quote_symbol": "USDC",
                "canonical_asset": "SOL",
                "quote_canonical": "USDC",
                "dex": "Orca",
                "protocol": "orca_whirlpool",
                "pool_type": "clmm",
                "enabled": True,
                "priority": 1,
                "fee_tier": 0,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "validation_required": False,
                "source_note": "unit_test",
                "notes": "solana candidate",
            }
        ]

        payload = validate_lp_pool_book_entries(entries)

        self.assertEqual({}, normalize_lp_pool_entries(entries))
        self.assertEqual([], payload["malformed_major_pool_entries"])
        self.assertTrue(payload["configured_but_unsupported_chain"])
        self.assertEqual("unsupported_chain", payload["configured_but_unsupported_chain"][0]["reason"])

    def test_base_pool_is_marked_unsupported_and_not_normalized(self) -> None:
        entries = [
            {
                "pool_address": "0x1cca8388e671e83010843a1a0535b04f2d7e946b",
                "chain": "base",
                "pair_label": "SOL/USDC",
                "base_symbol": "SOL",
                "quote_symbol": "USDC",
                "canonical_asset": "SOL",
                "quote_canonical": "USDC",
                "dex": "Uniswap",
                "protocol": "uniswap_v3",
                "pool_type": "clmm",
                "enabled": True,
                "priority": 1,
                "fee_tier": 3000,
                "major_pool": True,
                "major_match_mode": "major_family_match",
                "validation_required": True,
                "source_note": "unit_test",
                "notes": "base candidate",
            }
        ]

        payload = validate_lp_pool_book_entries(entries)

        self.assertEqual({}, normalize_lp_pool_entries(entries))
        self.assertEqual([], payload["malformed_major_pool_entries"])
        self.assertTrue(payload["configured_but_unsupported_chain"])
        self.assertEqual("base", payload["configured_but_unsupported_chain"][0]["chain"])


if __name__ == "__main__":
    unittest.main()
