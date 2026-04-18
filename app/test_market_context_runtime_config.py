import unittest

from market_context_adapter import (
    MarketContextConfigError,
    build_market_context_runtime_self_check,
    format_market_context_runtime_self_check,
    validate_market_context_runtime_self_check,
)


class MarketContextRuntimeConfigTests(unittest.TestCase):
    def test_live_mode_blocks_legacy_binance_bybit_profile(self) -> None:
        report = build_market_context_runtime_self_check(
            mode="live",
            primary_venue="binance_perp",
            secondary_venue="bybit_perp",
        )

        self.assertFalse(report["startup_ready"])
        self.assertIn("live_mode_legacy_primary_venue:binance_perp", report["issues"])
        self.assertIn("live_mode_legacy_secondary_venue:bybit_perp", report["issues"])
        self.assertEqual(
            ["binance_perp", "bybit_perp"],
            report["effective_path"][:2],
        )

    def test_live_mode_accepts_okx_kraken_profile(self) -> None:
        report = build_market_context_runtime_self_check(
            mode="live",
            primary_venue="okx_perp",
            secondary_venue="kraken_futures",
        )

        self.assertTrue(report["startup_ready"])
        self.assertEqual([], report["issues"])
        self.assertTrue(report["recommended_profile_match"])

    def test_validate_runtime_self_check_raises_for_blocked_profile(self) -> None:
        report = build_market_context_runtime_self_check(
            mode="live",
            primary_venue="binance_perp",
            secondary_venue="bybit_perp",
        )

        with self.assertRaises(MarketContextConfigError):
            validate_market_context_runtime_self_check(report)

    def test_format_runtime_self_check_includes_expected_profile(self) -> None:
        report = build_market_context_runtime_self_check(
            mode="live",
            primary_venue="kraken_futures",
            secondary_venue="okx_perp",
        )

        rendered = format_market_context_runtime_self_check(report)

        self.assertIn("startup_check=ready", rendered)
        self.assertIn("expected_us_vps_profile=okx_perp -> kraken_futures", rendered)


if __name__ == "__main__":
    unittest.main()
