import json
from pathlib import Path
import tempfile
import unittest

from analyzer import BehaviorAnalyzer
from archive_store import ArchiveStore
from models import Event, Signal
from pipeline import SignalPipeline
from price_service import PriceService
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


class SignalArchiveIntegrityTests(unittest.TestCase):
    def _pipeline(self, archive_dir: Path, state_manager: StateManager) -> SignalPipeline:
        return SignalPipeline(
            price_service=PriceService(),
            state_manager=state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=SignalQualityGate(state_manager=state_manager),
            quality_manager=QualityManager(state_manager=state_manager),
            archive_store=ArchiveStore(base_dir=archive_dir),
        )

    def _event(self) -> Event:
        return Event(
            tx_hash="0xsignalarchive",
            address="0xpoolarchive",
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=42_000.0,
            kind="swap",
            ts=1_710_000_300,
            intent_type="pool_buy_pressure",
            intent_stage="confirmed",
            intent_confidence=0.88,
            confirmation_score=0.84,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            event_id="evt-archive-1",
            archive_ts=1_710_000_300,
            delivery_class="primary",
            delivery_reason="unit_test",
            metadata={
                "token_symbol": "ETH",
                "asset_case_id": "asset_case:archive",
                "asset_case_key": "ethereum|ETH|buy_pressure|onchain_lp",
                "lp_alert_stage": "confirm",
                "market_context_source": "live_public",
                "market_context_venue": "binance_perp",
                "alert_relative_timing": "confirming",
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "base_token_symbol": "ETH",
                        "quote_token_symbol": "USDC",
                    }
                },
            },
        )

    def _signal(self, event: Event) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.9,
            priority=1,
            tier="Tier 1",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="archive_test",
            quality_score=0.88,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            signal_id="sig-archive-1",
            event_id=event.event_id,
            archive_ts=event.archive_ts,
            delivery_class="primary",
            delivery_reason="unit_test",
        )
        signal.context.update(
            {
                "user_tier": "research",
                "pair_label": "ETH/USDC",
                "asset_case_label": "ETH",
                "asset_case_id": "asset_case:archive",
                "asset_case_key": "ethereum|ETH|buy_pressure|onchain_lp",
                "lp_alert_stage": "confirm",
                "lp_stage_badge": "确认",
                "lp_state_label": "更广泛买压确认",
                "lp_market_read": "更像 clean confirm：已有 broader perp/spot 同向确认，但仍不是首发先手",
                "market_context_source": "live_public",
                "market_context_venue": "binance_perp",
                "market_context_requested_symbol": "ETHUSDC",
                "market_context_resolved_symbol": "ETHUSDT",
                "market_context_failure_reason": "",
                "alert_relative_timing": "confirming",
                "message_variant": "lp_directional",
                "message_template": "brief",
                "operational_intent_key": "lp_pool_buy_pressure",
                "lp_confirm_quality": "clean_confirm",
                "lp_confirm_reason": "多池/更广泛盘口同向，且预走不大",
                "lp_confirm_alignment_score": 0.82,
                "lp_chase_risk_score": 0.12,
                "lp_confirm_timing_bucket": "clean_window",
                "lp_absorption_context": "broader_buy_pressure_confirmed",
                "lp_absorption_confidence": 0.81,
                "lp_broader_alignment": "confirmed",
                "lp_local_vs_broad_reason": "多池同向，且 broader perp/spot 同向",
                "lp_sweep_phase": "",
                "lp_sweep_display_stage": "confirm",
                "lp_outcome_record": {"record_id": "outcome-archive-1"},
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def _read_rows(self, archive_dir: Path, category: str) -> list[dict]:
        rows = []
        for path in sorted((archive_dir / category).glob("*.ndjson")):
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    rows.append(json.loads(line)["data"])
        return rows

    def test_signal_archive_writes_flat_record_and_reconciles_with_delivery_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            archive_dir = Path(temp_dir) / "nested" / "archive"
            state_manager = StateManager()
            state_manager._lp_outcome_records["outcome-archive-1"] = {"record_id": "outcome-archive-1"}
            pipeline = self._pipeline(archive_dir, state_manager)
            event = self._event()
            signal = self._signal(event)

            pipeline.finalize_notification_delivery(
                event,
                signal,
                delivered=True,
                archive_status={},
            )
            pipeline.finalize_notification_delivery(
                event,
                signal,
                delivered=True,
                archive_status={},
            )

            signal_rows = self._read_rows(archive_dir, "signals")
            audit_rows = self._read_rows(archive_dir, "delivery_audit")

        self.assertTrue(signal_rows)
        self.assertEqual(1, len(signal_rows))
        signal_row = signal_rows[0]
        self.assertEqual("sig-archive-1", signal_row["signal_id"])
        self.assertEqual("evt-archive-1", signal_row["event_id"])
        self.assertEqual("asset_case:archive", signal_row["asset_case_id"])
        self.assertEqual("notifier_delivered", signal_row["delivery_decision"])
        self.assertTrue(signal_row["sent_to_telegram"])
        self.assertIsNotNone(signal_row["notifier_sent_at"])
        self.assertEqual("confirm", signal_row["lp_alert_stage"])
        self.assertEqual("ETHUSDC", signal_row["market_context_requested_symbol"])
        self.assertEqual("ETHUSDT", signal_row["market_context_resolved_symbol"])
        self.assertEqual("clean_confirm", signal_row["lp_confirm_quality"])
        self.assertEqual("broader_buy_pressure_confirmed", signal_row["lp_absorption_context"])
        self.assertEqual("clean_window", signal_row["lp_confirm_timing_bucket"])

        matching_audits = [row for row in audit_rows if row.get("signal_id") == "sig-archive-1"]
        self.assertTrue(matching_audits)
        self.assertEqual("0xsignalarchive", matching_audits[-1]["tx_hash"])
        self.assertEqual("asset_case:archive", matching_audits[-1]["asset_case_id"])
        self.assertEqual("confirm", matching_audits[-1]["lp_alert_stage"])


if __name__ == "__main__":
    unittest.main()
