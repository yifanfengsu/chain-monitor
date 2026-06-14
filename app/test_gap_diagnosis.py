from __future__ import annotations

import gzip
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app import gap_diagnosis


BJ_TZ = timezone(timedelta(hours=8))


def bj_ts(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return int(parsed.replace(tzinfo=BJ_TZ).timestamp())


class GapDiagnosisTests(unittest.TestCase):
    def _db(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        temp = tempfile.TemporaryDirectory()
        return temp, Path(temp.name) / "chain_monitor.sqlite"

    def test_delivery_audit_continuous_signals_gap_is_signal_generation_gap(self) -> None:
        temp, db_path = self._db()
        self.addCleanup(temp.cleanup)
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE signals (signal_id TEXT PRIMARY KEY, timestamp REAL);
                CREATE TABLE delivery_audit (audit_id TEXT PRIMARY KEY, archive_written_at REAL);
                """
            )
            signal_times = [
                "2026-05-10 00:11:05",
                "2026-05-10 01:00:00",
                "2026-05-10 02:00:00",
                "2026-05-10 03:00:00",
                "2026-05-10 04:00:00",
                "2026-05-10 05:00:00",
                "2026-05-10 05:39:38",
                "2026-05-10 07:07:49",
                "2026-05-10 08:00:00",
                "2026-05-10 09:00:00",
                "2026-05-10 10:00:00",
                "2026-05-10 11:00:00",
                "2026-05-10 12:00:00",
                "2026-05-10 13:00:00",
                "2026-05-10 14:00:00",
                "2026-05-10 15:00:00",
                "2026-05-10 16:00:00",
                "2026-05-10 17:00:00",
                "2026-05-10 18:00:00",
                "2026-05-10 19:00:00",
                "2026-05-10 20:00:00",
                "2026-05-10 21:00:00",
                "2026-05-10 22:00:00",
                "2026-05-10 23:00:00",
                "2026-05-10 23:46:49",
            ]
            for idx, value in enumerate(signal_times):
                conn.execute("INSERT INTO signals VALUES (?, ?)", (f"sig-{idx}", bj_ts(value)))
            start = bj_ts("2026-05-10 00:00:03")
            end = bj_ts("2026-05-10 23:59:55")
            ts = start
            idx = 0
            while ts <= end:
                conn.execute("INSERT INTO delivery_audit VALUES (?, ?)", (f"audit-{idx}", ts))
                ts += 49
                idx += 1
            conn.commit()
        finally:
            conn.close()

        payload = gap_diagnosis.build_gap_diagnosis(
            "2026-05-10",
            db_path=db_path,
            archive_base_dir=Path(temp.name) / "archive",
        )
        summary = payload["gap_diagnosis_summary"]

        self.assertEqual("signal_generation_gap", summary["root_diagnosis"])
        self.assertNotEqual("listener_runtime_gap", summary["root_diagnosis"])
        self.assertFalse(summary["collection_degraded"])
        self.assertEqual(5291, payload["layers"]["signals"]["max_gap_sec"])
        self.assertEqual(49, payload["layers"]["delivery_audit"]["max_gap_sec"])

    def test_delivery_audit_gap_is_listener_or_audit_gap(self) -> None:
        temp, db_path = self._db()
        self.addCleanup(temp.cleanup)
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE signals (signal_id TEXT PRIMARY KEY, timestamp REAL);
                CREATE TABLE delivery_audit (audit_id TEXT PRIMARY KEY, archive_written_at REAL);
                """
            )
            for hour in range(24):
                ts = bj_ts(f"2026-05-10 {hour:02d}:00:00")
                conn.execute("INSERT INTO signals VALUES (?, ?)", (f"sig-{hour}", ts))
            conn.execute("INSERT INTO delivery_audit VALUES (?, ?)", ("audit-1", bj_ts("2026-05-10 00:00:00")))
            conn.execute("INSERT INTO delivery_audit VALUES (?, ?)", ("audit-2", bj_ts("2026-05-10 04:00:00")))
            conn.commit()
        finally:
            conn.close()

        payload = gap_diagnosis.build_gap_diagnosis(
            "2026-05-10",
            db_path=db_path,
            archive_base_dir=Path(temp.name) / "archive",
        )

        self.assertEqual("listener_or_audit_gap", payload["gap_diagnosis_summary"]["root_diagnosis"])
        self.assertTrue(payload["gap_diagnosis_summary"]["collection_degraded"])

    def test_raw_parsed_without_reliable_time_field_are_not_no_rows(self) -> None:
        temp, db_path = self._db()
        self.addCleanup(temp.cleanup)
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE raw_events (event_id TEXT PRIMARY KEY, logical_date TEXT, created_at REAL);
                CREATE TABLE parsed_events (event_id TEXT PRIMARY KEY, logical_date TEXT, created_at REAL);
                """
            )
            conn.execute("INSERT INTO raw_events VALUES (?, ?, ?)", ("raw-1", "2026-05-10", bj_ts("2026-05-10 12:00:00")))
            conn.execute("INSERT INTO parsed_events VALUES (?, ?, ?)", ("parsed-1", "2026-05-10", bj_ts("2026-05-10 12:00:00")))
            conn.commit()
        finally:
            conn.close()

        payload = gap_diagnosis.build_gap_diagnosis(
            "2026-05-10",
            db_path=db_path,
            archive_base_dir=Path(temp.name) / "archive",
        )

        self.assertEqual("time_field_unavailable", payload["layers"]["raw_events"]["status"])
        self.assertEqual("time_field_unavailable", payload["layers"]["parsed_events"]["status"])
        self.assertNotEqual("no_rows_for_date", payload["layers"]["raw_events"]["status"])
        self.assertIn("report_time_field_unavailable:raw_events,parsed_events", payload["gap_diagnosis_summary"]["warnings"])

    def test_trade_opportunities_sparse_gap_does_not_degrade_collection(self) -> None:
        temp, db_path = self._db()
        self.addCleanup(temp.cleanup)
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE signals (signal_id TEXT PRIMARY KEY, timestamp REAL);
                CREATE TABLE trade_opportunities (trade_opportunity_id TEXT PRIMARY KEY, created_at REAL);
                """
            )
            for hour in range(24):
                ts = bj_ts(f"2026-05-10 {hour:02d}:00:00")
                conn.execute("INSERT INTO signals VALUES (?, ?)", (f"sig-{hour}", ts))
            conn.execute("INSERT INTO trade_opportunities VALUES (?, ?)", ("opp-1", bj_ts("2026-05-10 00:30:00")))
            conn.execute("INSERT INTO trade_opportunities VALUES (?, ?)", ("opp-2", bj_ts("2026-05-10 05:30:00")))
            conn.commit()
        finally:
            conn.close()

        payload = gap_diagnosis.build_gap_diagnosis(
            "2026-05-10",
            db_path=db_path,
            archive_base_dir=Path(temp.name) / "archive",
        )

        self.assertEqual("opportunity_generation_sparse", payload["gap_diagnosis_summary"]["root_diagnosis"])
        self.assertFalse(payload["gap_diagnosis_summary"]["collection_degraded"])

    def test_archive_gz_mtime_is_not_used_as_day_continuity_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            signals_dir = archive_root / "signals"
            signals_dir.mkdir(parents=True)
            path = signals_dir / "2026-05-10.ndjson.gz"
            rows = [
                {"archive_ts": bj_ts("2026-05-10 00:00:03"), "data": {"signal_id": "sig-1"}},
                {"archive_ts": bj_ts("2026-05-10 00:00:52"), "data": {"signal_id": "sig-2"}},
            ]
            with gzip.open(path, "wt", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")
            late_mtime = bj_ts("2026-05-12 12:00:00")
            os.utime(path, (late_mtime, late_mtime))

            payload = gap_diagnosis.build_gap_diagnosis(
                "2026-05-10",
                db_path=Path(temp) / "missing.sqlite",
                archive_base_dir=archive_root,
            )

        archive_signals = payload["layers"]["archive_signals"]
        self.assertFalse(payload["archive_mtime_used"])
        self.assertEqual("ignored_for_gap_diagnosis", payload["archive_mtime_status"])
        self.assertEqual("2026-05-10 00:00:52", archive_signals["last_bj"])
        self.assertEqual(49, archive_signals["max_gap_sec"])


if __name__ == "__main__":
    unittest.main()
