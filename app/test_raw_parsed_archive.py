import os
import json
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from analyzer import BehaviorAnalyzer
from archive_store import ArchiveStore
from pipeline import SignalPipeline
from price_service import PriceService
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


class RawParsedArchiveTests(unittest.TestCase):
    def _pipeline(self, archive_dir: Path) -> SignalPipeline:
        state_manager = StateManager()
        return SignalPipeline(
            price_service=PriceService(),
            state_manager=state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=SignalQualityGate(state_manager=state_manager),
            quality_manager=QualityManager(state_manager=state_manager),
            archive_store=ArchiveStore(
                base_dir=archive_dir,
                category_enabled={"raw_events": True, "parsed_events": True},
            ),
        )

    def _read_first(self, archive_dir: Path, category: str) -> dict:
        paths = sorted((archive_dir / category).glob("*.ndjson"))
        self.assertTrue(paths)
        with paths[0].open(encoding="utf-8") as handle:
            return json.loads(next(handle))["data"]

    def test_raw_and_parsed_archives_include_debug_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            archive_dir = Path(temp_dir)
            pipeline = self._pipeline(archive_dir)
            archive_status = {}
            raw_item = {
                "tx_hash": "0xrawarchive",
                "block_number": 123,
                "address": "0xpoolarchive",
                "kind": "swap_log",
                "captured_at": 1_710_000_111,
                "listener_scan_path": "lp_primary_trend",
            }
            parsed = {
                "tx_hash": "0xrawarchive",
                "parsed_kind": "swap",
                "role_group": "lp_pool",
                "parse_status": "parsed",
                "parsed_at": 1_710_000_112,
            }

            pipeline._archive_raw_event(raw_item, archive_status, 1_710_000_111)
            pipeline._archive_parsed_event(parsed, archive_status, 1_710_000_112)

            raw_row = self._read_first(archive_dir, "raw_events")
            parsed_row = self._read_first(archive_dir, "parsed_events")

        self.assertEqual("0xrawarchive", raw_row["tx_hash"])
        self.assertEqual("swap_log", raw_row["raw_kind"])
        self.assertEqual("lp_primary_trend", raw_row["listener_scan_path"])
        self.assertEqual("swap", parsed_row["parsed_kind"])
        self.assertEqual("lp_pool", parsed_row["role_group"])
        self.assertEqual("parsed", parsed_row["parse_status"])


if __name__ == "__main__":
    unittest.main()
