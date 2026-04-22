from __future__ import annotations

import unittest

from lp_registry import UNISWAP_V3_ETHEREUM_FACTORY, validate_lp_pool_book_entries


USDC_CONTRACT = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
WBTC_CONTRACT = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
CBBTC_CONTRACT = "0x1111111111111111111111111111111111111111"


def _validator_payload(*, token0: str, token1: str, fee: int) -> dict:
    return {
        "status": "passed",
        "token0": token0,
        "token1": token1,
        "fee": fee,
        "factory": UNISWAP_V3_ETHEREUM_FACTORY,
    }


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
    protocol: str = "uniswap_v3",
    pool_type: str = "clmm",
    chain: str = "ethereum",
    major_pool: bool = True,
    major_match_mode: str = "major_family_match",
    notes: str = "unit test major pool",
    placeholder: bool = False,
    base_contract: str = "",
    quote_contract: str = "",
    fee_tier: int | None = 3000,
    source_note: str = "unit_test",
    validation_required: bool = True,
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
        "dex": dex,
        "protocol": protocol,
        "pool_type": pool_type,
        "enabled": enabled,
        "priority": priority,
        "fee_tier": fee_tier,
        "major_pool": major_pool,
        "major_match_mode": major_match_mode,
        "validation_required": validation_required,
        "source_note": source_note,
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

    def test_ethereum_wbtc_usdc_pool_is_covered_after_validation(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(
                    pool_address="0x99ac8ca7087fa4a2a1fb6357269965a2014abc35",
                    pair_label="WBTC/USDC",
                    base_symbol="WBTC",
                    quote_symbol="USDC",
                    canonical_asset="BTC",
                    base_contract=WBTC_CONTRACT,
                    quote_contract=USDC_CONTRACT,
                    fee_tier=3000,
                )
            ],
            onchain_validator=lambda item, resolved: _validator_payload(
                token0=USDC_CONTRACT,
                token1=WBTC_CONTRACT,
                fee=int(resolved.get("fee_tier") or 0),
            ),
        )

        self.assertEqual([], payload["configured_but_validation_failed"])
        self.assertIn("BTC/USDC", payload["covered_major_pairs"])

    def test_ethereum_wbtc_usdt_pool_is_covered_after_validation(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(
                    pool_address="0x9db9e0e53058c89e5b94e29621a205198648425b",
                    pair_label="WBTC/USDT",
                    base_symbol="WBTC",
                    quote_symbol="USDT",
                    canonical_asset="BTC",
                    base_contract=WBTC_CONTRACT,
                    quote_contract=USDT_CONTRACT,
                    fee_tier=3000,
                )
            ],
            onchain_validator=lambda item, resolved: _validator_payload(
                token0=USDT_CONTRACT,
                token1=WBTC_CONTRACT,
                fee=int(resolved.get("fee_tier") or 0),
            ),
        )

        self.assertEqual([], payload["configured_but_validation_failed"])
        self.assertIn("BTC/USDT", payload["covered_major_pairs"])

    def test_duplicate_pool_address_and_priority_conflict_warn(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(
                    pool_address="0x1111111111111111111111111111111111111111",
                    pair_label="BTC/USDT",
                    priority=1,
                    protocol="unit_test",
                    pool_type="spot_lp",
                    validation_required=False,
                ),
                _major_entry(
                    pool_address="0x1111111111111111111111111111111111111111",
                    pair_label="BTC/USDT",
                    priority=1,
                    protocol="unit_test",
                    pool_type="spot_lp",
                    validation_required=False,
                ),
            ],
        )

        warning_types = {item["warning_type"] for item in payload["duplicate_pool_warnings"]}
        self.assertIn("duplicate_pool_address", warning_types)
        self.assertIn("major_pair_priority_conflict", warning_types)
        self.assertTrue(payload["recommended_local_config_actions"])

    def test_same_major_pair_with_split_priority_is_allowed(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(
                    pool_address="0x2222222222222222222222222222222222222222",
                    pair_label="BTC/USDT",
                    priority=1,
                    protocol="unit_test",
                    pool_type="spot_lp",
                    validation_required=False,
                ),
                _major_entry(
                    pool_address="0x3333333333333333333333333333333333333333",
                    pair_label="WBTC/USDT",
                    base_symbol="WBTC",
                    priority=2,
                    protocol="unit_test",
                    pool_type="spot_lp",
                    validation_required=False,
                ),
            ],
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
                    base_contract=CBBTC_CONTRACT,
                    quote_contract=USDC_CONTRACT,
                )
            ]
        )

        self.assertEqual([], payload["malformed_major_pool_entries"])
        self.assertIn("BTC/USDC", [item["pair_label"] for item in payload["configured_but_disabled_major_pools"]])

    def test_optional_cbbtc_pool_keeps_distinct_token_contract(self) -> None:
        payload = validate_lp_pool_book_entries(
            [
                _major_entry(
                    pool_address="0x4548280ac92507c9092a511c7396cbea78fa9e49",
                    pair_label="cbBTC/USDC",
                    base_symbol="cbBTC",
                    quote_symbol="USDC",
                    canonical_asset="BTC",
                    enabled=False,
                    base_contract=CBBTC_CONTRACT,
                    quote_contract=USDC_CONTRACT,
                )
            ]
        )

        disabled = payload["configured_but_disabled_major_pools"][0]
        self.assertEqual("BTC/USDC", disabled["pair_label"])
        self.assertNotEqual(CBBTC_CONTRACT.lower(), WBTC_CONTRACT.lower())


if __name__ == "__main__":
    unittest.main()
