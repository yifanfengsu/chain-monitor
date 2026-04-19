import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from archive_store import ArchiveStore
from quality_reports import build_market_context_health_report


class QualityReportRatesTests(unittest.TestCase):
    def test_market_context_rates_are_bounded_by_one(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            archive = ArchiveStore(base_dir=Path(temp_dir), category_enabled={"signals": True})
            archive.write_signal(
                {
                    "signal_archive_key": "sig-rate-1",
                    "signal_id": "sig-rate-1",
                    "market_context_source": "live_public",
                    "market_context_venue": "okx_perp",
                    "market_context_requested_symbol": "ETHUSDC",
                    "market_context_resolved_symbol": "ETH-USDT-SWAP",
                    "market_context_attempts": [
                        {
                            "venue": "okx_perp",
                            "symbol": "ETH-USDT-SWAP",
                            "requested_symbol": "ETHUSDC",
                            "stage": "cache",
                            "endpoint": "cache",
                            "status": "cache_hit",
                            "failure_reason": "",
                            "failure_stage": "",
                            "http_status": None,
                            "latency_ms": 1,
                        },
                        {
                            "venue": "okx_perp",
                            "symbol": "ETH-USDT-SWAP",
                            "requested_symbol": "ETHUSDC",
                            "stage": "mark_price",
                            "endpoint": "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT-SWAP",
                            "status": "success",
                            "failure_reason": "",
                            "failure_stage": "",
                            "http_status": 200,
                            "latency_ms": 70,
                        },
                    ],
                },
                archive_ts=1_710_000_000,
            )
            report = build_market_context_health_report(base_dir=Path(temp_dir))

        self.assertLessEqual(report["context_request_hit_rate"], 1.0)
        self.assertLessEqual(report["cache_hit_rate"], 1.0)
        for row in report["per_venue"]:
            self.assertLessEqual(float(row["venue_attempt_success_rate"]), 1.0)
            self.assertLessEqual(float(row["attempt_hit_rate"]), 1.0)
        for row in report["per_endpoint"]:
            self.assertLessEqual(float(row["endpoint_attempt_success_rate"]), 1.0)
            self.assertLessEqual(float(row["attempt_hit_rate"]), 1.0)


if __name__ == "__main__":
    unittest.main()
