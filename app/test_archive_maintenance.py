import gzip
import json
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import archive_maintenance
import report_data_loader


class ArchiveMaintenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.date = "2026-04-20"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_archive(self, category: str = "signals", date: str | None = None, payload: dict | None = None) -> Path:
        date = date or self.date
        path = self.archive_dir / category / f"{date}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        row = payload or {
            "archive_ts": 1_710_000_000,
            "data": {"signal_id": "sig-compress", "archive_written_at": 1_710_000_000},
        }
        path.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
        return path

    def test_dry_run_does_not_compress(self) -> None:
        path = self._write_archive()

        payload = archive_maintenance.compress_date(self.date, base_dir=self.archive_dir)

        self.assertEqual("dry-run", payload["mode"])
        self.assertTrue(path.exists())
        self.assertFalse(path.with_suffix(path.suffix + ".gz").exists())
        self.assertIn("signals", payload["skipped"])

    def test_execute_compresses_date_and_gzip_is_readable(self) -> None:
        path = self._write_archive()
        gz_path = self.archive_dir / "signals" / f"{self.date}.ndjson.gz"

        payload = archive_maintenance.compress_date(self.date, execute=True, base_dir=self.archive_dir)

        self.assertFalse(path.exists())
        self.assertTrue(gz_path.exists())
        self.assertIn("signals", payload["compressed"])
        with gzip.open(gz_path, "rt", encoding="utf-8") as handle:
            row = json.loads(handle.readline())
        self.assertEqual("sig-compress", row["data"]["signal_id"])

    def test_existing_gzip_is_not_recompressed(self) -> None:
        path = self._write_archive()
        gz_path = self.archive_dir / "signals" / f"{self.date}.ndjson.gz"
        with gzip.open(gz_path, "wt", encoding="utf-8") as handle:
            handle.write("{}\n")
        original_size = gz_path.stat().st_size

        payload = archive_maintenance.compress_date(self.date, execute=True, base_dir=self.archive_dir)

        self.assertTrue(path.exists())
        self.assertEqual(original_size, gz_path.stat().st_size)
        self.assertIn("signals", payload["already_compressed"])

    def test_missing_file_does_not_crash(self) -> None:
        payload = archive_maintenance.compress_date(self.date, execute=True, base_dir=self.archive_dir)

        self.assertIn("signals", payload["missing"])
        self.assertFalse(payload["refused"])

    def test_today_date_refuses_by_default(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._write_archive(date=today)

        payload = archive_maintenance.compress_date(today, execute=True, base_dir=self.archive_dir, allow_today=False)

        self.assertTrue(payload["refused"])
        self.assertEqual("refusing_to_compress_active_archive_date", payload["reason"])

    def test_does_not_touch_non_archive_files(self) -> None:
        outside = self.root / "data" / "chain_monitor.sqlite"
        outside.parent.mkdir(parents=True, exist_ok=True)
        outside.write_text("not a database", encoding="utf-8")
        self._write_archive()

        archive_maintenance.compress_date(self.date, execute=True, base_dir=self.archive_dir)

        self.assertTrue(outside.exists())
        self.assertEqual("not a database", outside.read_text(encoding="utf-8"))

    def test_reports_loader_can_read_compressed_archive(self) -> None:
        self._write_archive()
        archive_maintenance.compress_date(self.date, execute=True, base_dir=self.archive_dir)

        rows = report_data_loader.iter_archive_payloads("signals", base_dir=self.archive_dir)

        self.assertEqual(1, len(rows))
        self.assertEqual("sig-compress", rows[0]["signal_id"])

    def test_make_help_lists_new_targets(self) -> None:
        result = subprocess.run(
            ["make", "help"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode)
        self.assertIn("daily-close", result.stdout)
        self.assertIn("archive-compress-date", result.stdout)
        self.assertIn("sqlite-checkpoint", result.stdout)


if __name__ == "__main__":
    unittest.main()
