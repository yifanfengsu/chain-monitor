from __future__ import annotations

import unittest

from lp_registry import validate_lp_pool_book_entries


def _major_entry(
    *,
    pool_address: str,
    pair_label: str = "BTC/USDT",
    base_symbol: str = "BTC",
    quote_symbol: str = "USDT",
    canonical_asset: str = "BTC",
    enabled: bool = True,
    priority: int = 1,
    dex: str = "UnitTest",
    protocol: str = "unit_test",
    pool_type: str = "spot_lp",
    chain: str = "ethereum",
    major_pool: bool = True,
    major_match_mode: str = "major_family_match",
    notes: str = "unit test major pool",
    placeholder: bool = False,
) -> dict:
    return {
        "pool_address": pool_address,
        "chain": chain,
        "pair_label": pair_label,
        "base_symbol": base_symbol,
        "quote_symbol": quote_symbol,
        "canonical_asset": canonical_asset,
        "dex": dex,
        "protocol": protocol,
        "pool_type": pool_type,
        "enabled": enabled,
        "priority": priority,
        "major_pool": major_pool,
        "major_match_mode": major_match_mode,
        "notes": notes,
        "placeholder": placeholder,
    }


class LpPoolsSchemaValidatorTests(unittest.TestCase):
    def test_placeholder_enabled_is_rejected(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(
                    pool_address="0xPLACEHOLDER_BTC_USDT",
                    placeholder=True,
                )
            ]
        )

        self.assertTrue(payload["placeholder_major_pool_entries"])
        self.assertTrue(payload["malformed_major_pool_entries"])
        self.assertIn("placeholder_enabled", payload["malformed_major_pool_entries"][0]["reasons"])

    def test_malformed_address_is_rejected(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(pool_address="0x1234")
            ]
        )

        self.assertTrue(payload["malformed_major_pool_entries"])
        self.assertIn("invalid_pool_address", payload["malformed_major_pool_entries"][0]["reasons"])

    def test_duplicate_pool_address_and_priority_conflict_warn(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(pool_address="0x1111111111111111111111111111111111111111", pair_label="BTC/USDT", priority=1),
                _major_entry(pool_address="0x1111111111111111111111111111111111111111", pair_label="BTC/USDT", priority=1),
            ]
        )

        warning_types = {item["warning_type"] for item in payload["duplicate_pool_warnings"]}
        self.assertIn("duplicate_pool_address", warning_types)
        self.assertIn("major_pair_priority_conflict", warning_types)
        self.assertTrue(payload["recommended_local_config_actions"])

    def test_same_major_pair_with_split_priority_is_allowed(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(pool_address="0x2222222222222222222222222222222222222222", pair_label="BTC/USDT", priority=1),
                _major_entry(pool_address="0x3333333333333333333333333333333333333333", pair_label="WBTC/USDT", base_symbol="WBTC", priority=2),
            ]
        )

        self.assertEqual([], [item for item in payload["duplicate_pool_warnings"] if item["warning_type"] == "major_pair_priority_conflict"])
        self.assertIn("BTC/USDT", payload["covered_major_pairs"])

    def test_aliases_canonicalize_to_configured_disabled_major_pair(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(
                    pool_address="0x4444444444444444444444444444444444444444",
                    pair_label="CBBTC/USDC.E",
                    base_symbol="CBBTC",
                    quote_symbol="USDC.E",
                    canonical_asset="BTC",
                    enabled=False,
                )
            ]
        )

        self.assertEqual([], payload["malformed_major_pool_entries"])
        self.assertIn("BTC/USDC", [item["pair_label"] for item in payload["configured_but_disabled_major_pools"]])


if __name__ == "__main__":
    unittest.main()
