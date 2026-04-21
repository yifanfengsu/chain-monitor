import sys
import tempfile
import unittest
from pathlib import Path

import sqlite_store

ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = ROOT / "reports"
if str(REPORTS_DIR) not in sys.path:
    sys.path.insert(0, str(REPORTS_DIR))

from generate_afternoon_evening_state_analysis_latest import compute_outcome_detail  # noqa: E402
from generate_overnight_run_analysis_latest import merge_sqlite_outcomes_into_rows  # noqa: E402


class OutcomeSchedulerReportTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_outcome(self, signal_id: str, window_sec: int, status: str, *, failure_reason: str = "", catchup: bool = False) -> None:
        sqlite_store.upsert_outcome_window(
            {
                "outcome_id": f"outcome_{signal_id}_{window_sec}",
                "signal_id": signal_id,
                "asset": "ETH",
                "pair": "ETH/USDC",
                "direction": "long",
                "window_sec": window_sec,
                "due_at": 1_000 + window_sec,
                "created_at": 1_000,
                "start_price": 100.0,
                "start_price_source": "okx_mark",
                "end_price": 101.0 if status == "completed" else None,
                "outcome_price_source": "okx_mark" if status == "completed" else "unavailable",
                "price_source": "okx_mark" if status == "completed" else "unavailable",
                "raw_move": 0.01 if status == "completed" else None,
                "direction_adjusted_move": 0.01 if status == "completed" else None,
                "followthrough": status == "completed",
                "adverse": False if status == "completed" else None,
                "status": status,
                "failure_reason": failure_reason,
                "completed_at": 1_000 + window_sec + 1 if status != "pending" else None,
                "settled_by": "outcome_scheduler_catchup" if catchup else "outcome_scheduler",
                "catchup": catchup,
            }
        )

    def test_reports_read_completed_unavailable_expired_and_pending(self) -> None:
        self._write_outcome("sig-completed", 30, "completed", catchup=True)
        self._write_outcome("sig-unavailable", 30, "unavailable", failure_reason="market_price_unavailable")
        self._write_outcome("sig-expired", 30, "expired", failure_reason="catchup_window_exceeded", catchup=True)
        self._write_outcome("sig-pending", 30, "pending")
        for signal_id in ("sig-completed", "sig-unavailable", "sig-expired", "sig-pending"):
            self._write_outcome(signal_id, 60, "completed")
            self._write_outcome(signal_id, 300, "completed")

        lp_rows = merge_sqlite_outcomes_into_rows(
            [
                {"signal_id": "sig-completed", "outcome_windows": {}},
                {"signal_id": "sig-unavailable", "outcome_windows": {}},
                {"signal_id": "sig-expired", "outcome_windows": {}},
                {"signal_id": "sig-pending", "outcome_windows": {}},
            ]
        )
        summary = compute_outcome_detail(lp_rows, previous_report={})

        self.assertEqual(1, summary["outcome_30s_completed_count"])
        self.assertEqual(1, summary["outcome_30s_unavailable_count"])
        self.assertEqual(1, summary["outcome_30s_expired_count"])
        self.assertEqual(1, summary["outcome_30s_pending_count"])
        self.assertEqual(0.25, summary["outcome_30s_completed_rate"])
        self.assertEqual(1.0, summary["outcome_60s_completed_rate"])
        self.assertEqual(1.0, summary["outcome_300s_completed_rate"])
        self.assertEqual(1, summary["catchup_completed_count"])
        self.assertEqual(1, summary["catchup_expired_count"])
        self.assertEqual(1, summary["outcome_failure_reason_distribution"]["market_price_unavailable"])
        self.assertEqual(1, summary["outcome_failure_reason_distribution"]["catchup_window_exceeded"])
        self.assertIn("scheduler_health_summary", summary)


if __name__ == "__main__":
    unittest.main()
