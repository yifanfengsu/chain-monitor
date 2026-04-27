import unittest

from reports.generate_daily_report_latest import build_logical_day_window, build_segment_summary


class DailyReportWindowSelectionTests(unittest.TestCase):
    def test_date_uses_beijing_logical_day_window(self) -> None:
        window = build_logical_day_window("2026-04-24")

        self.assertEqual("2026-04-24", window["logical_date"])
        self.assertEqual("Asia/Shanghai", window["timezone"])
        self.assertEqual("2026-04-23 16:00:00 UTC", window["logical_window_start_utc"])
        self.assertEqual("2026-04-24 15:59:59 UTC", window["logical_window_end_utc"])
        self.assertEqual("2026-04-24 00:00:00 UTC+8", window["logical_window_start_beijing"])
        self.assertEqual("2026-04-24 23:59:59 UTC+8", window["logical_window_end_beijing"])

    def test_large_gap_splits_segments_without_fake_39h_window(self) -> None:
        window = build_logical_day_window("2026-04-24")
        entries = [
            (window["start_ts"] + 60, "signals"),
            (window["start_ts"] + 120, "raw_events"),
            (window["start_ts"] + 7200, "signals"),
            (window["start_ts"] + 7260, "parsed_events"),
        ]

        segment_summary = build_segment_summary(
            entries,
            logical_window_start_ts=window["start_ts"],
            logical_window_end_ts=window["end_ts"],
            max_gap_sec=1800,
        )

        self.assertEqual(2, segment_summary["segment_count"])
        self.assertEqual(["segment_1", "segment_2"], segment_summary["selected_segments"])
        self.assertLess(segment_summary["active_duration_hours"], 1.0)
        self.assertEqual(24.0, segment_summary["wall_clock_duration_hours"])
        self.assertGreater(segment_summary["max_gap_hours"], 1.0)
        self.assertIn("multiple_segments_do_not_treat_wall_clock_as_continuous_runtime", segment_summary["gap_warnings"])


if __name__ == "__main__":
    unittest.main()
