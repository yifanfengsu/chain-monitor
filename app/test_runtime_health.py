from __future__ import annotations

import unittest
from unittest import mock

import runtime_health


class RuntimeHealthTests(unittest.TestCase):
    def test_zero_activity_day_is_detected(self) -> None:
        with mock.patch.object(
            runtime_health,
            "_query_timestamps",
            return_value={
                "raw_timestamps": [],
                "parsed_timestamps": [],
                "signal_timestamps": [],
                "mc_timestamps": [],
                "last_block_seen": None,
                "trade_opportunity_count": 0,
            },
        ):
            payload = runtime_health.build_runtime_health_report(date_str="2026-04-30")

        self.assertEqual(0, payload["raw_events_count"])
        self.assertEqual(0, payload["signals_count"])
        self.assertEqual(1, payload["zero_activity_day"])
        self.assertEqual("invalid_or_no_activity", payload["data_quality_status"])


if __name__ == "__main__":
    unittest.main()
