from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

from analyzer import BehaviorAnalyzer
from models import Event, Signal
from pipeline import SignalPipeline
from price_service import PriceService
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer
from user_tiers import evaluate_user_tier_lp_delivery
from quality_reports import main as quality_reports_main


class QualityReportsTests(unittest.TestCase):
    def _seed_record(
        self,
        state_manager: StateManager,
        *,
        record_id: str,
        stage: str,
        pool: str = "0xpool",
        pair: str = "ETH/USDC",
        asset: str = "ETH",
        direction: str = "buy_pressure",
        confirm_after_prealert=None,
        false_prealert=None,
        reversal_after_climax=None,
        move_after_60s=None,
        move_after_300s=None,
        market_source: str = "unavailable",
        timing: str = "",
        fastlane: bool = False,
        created_at: int = 1_710_000_000,
    ) -> None:
        record = {
            "schema_version": "lp_alert_outcome_v1",
            "record_id": record_id,
            "signal_id": record_id,
            "event_id": record_id,
            "pair_label": pair,
            "pool_address": pool,
            "asset_symbol": asset,
            "intent_type": "pool_buy_pressure" if direction == "buy_pressure" else "pool_sell_pressure",
            "lp_alert_stage": stage,
            "lp_sweep_phase": "",
            "direction_bucket": direction,
            "created_at": created_at,
            "anchor_price_proxy": 1.0,
            "followthrough_positive": move_after_60s is not None and float(move_after_60s) > 0 if direction == "buy_pressure" else move_after_60s is not None and float(move_after_60s) < 0,
            "followthrough_negative": move_after_60s is not None and float(move_after_60s) < 0 if direction == "buy_pressure" else move_after_60s is not None and float(move_after_60s) > 0,
            "reversal_after_climax": reversal_after_climax,
            "confirm_after_prealert": confirm_after_prealert,
            "false_prealert": false_prealert,
            "time_to_confirm": 30 if confirm_after_prealert else None,
            "move_before_alert": 0.0,
            "move_before_alert_30s": 0.0,
            "move_before_alert_60s": 0.0,
            "move_after_alert": move_after_60s or move_after_300s or 0.0,
            "move_after_alert_60s": move_after_60s,
            "move_after_alert_300s": move_after_300s,
            "asset_case_id": "asset_case:ETH:test",
            "asset_case_key": "ethereum|ETH|buy_pressure|onchain_lp",
            "asset_case_stage": stage,
            "asset_case_quality_score": 0.0,
            "market_context_source": market_source,
            "alert_relative_timing": timing,
            "market_move_before_alert_30s": None,
            "market_move_before_alert_60s": None,
            "market_move_after_alert_60s": None,
            "market_move_after_alert_300s": None,
            "lp_promoted_fastlane": fastlane,
            "notifier_sent_at": None,
            "delivered_notification": False,
        }
        state_manager._lp_outcome_records[record_id] = record
        state_manager._lp_outcome_history.append(record)

    def _event(self, *, stage: str = "prealert", pool: str = "0xpool") -> Event:
        return Event(
            tx_hash=f"0xquality{stage}",
            address=pool,
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=25_000.0,
            kind="swap",
            ts=1_710_000_300,
            intent_type="pool_buy_pressure",
            intent_stage="preliminary" if stage == "prealert" else "confirmed",
            intent_confidence=0.82,
            confirmation_score=0.38 if stage == "prealert" else 0.82,
            pricing_status="exact",
            pricing_confidence=0.94,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "user_tier": "research",
                "token_symbol": "ETH",
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "pool_label": "ETH/USDC",
                        "base_token_symbol": "ETH",
                        "quote_token_symbol": "USDC",
                    }
                },
            },
        )

    def _signal(self, event: Event, *, stage: str, user_tier: str = "research") -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.84,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.82,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
        )
        signal.context.update(
            {
                "user_tier": user_tier,
                "pair_label": "ETH/USDC",
                "asset_case_id": "asset_case:ETH:test",
                "asset_case_label": "ETH",
                "asset_case_supporting_pair_count": 2,
                "lp_alert_stage": stage,
                "lp_stage_badge": {
                    "prealert": "预警",
                    "confirm": "确认",
                    "climax": "高潮",
                    "exhaustion_risk": "风险",
                }[stage],
                "lp_state_label": {
                    "prealert": "买盘建立中",
                    "confirm": "持续买压",
                    "climax": "买方清扫",
                    "exhaustion_risk": "买方清扫（回吐风险高）",
                }[stage],
                "lp_market_read": "先手观察｜需看 60s 是否续单",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "续单消失 / 快速反向池流",
                "lp_followup_required": True,
                "lp_scan_path": "secondary",
                "lp_detect_latency_ms": 600,
                "lp_exhaustion_confidence": 0.56 if stage == "climax" else 0.22,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def test_market_context_config_check_cli_outputs_self_check_payload(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = quality_reports_main(["--market-context-config-check"])

        payload = json.loads(output.getvalue())
        self.assertEqual(0, exit_code)
        self.assertIn("startup_ready", payload)
        self.assertIn("mode", payload)
        self.assertIn("issues", payload)

    def _manager(self, state_manager: StateManager, stats_path: str, actionable_min_samples: int = 2) -> QualityManager:
        return QualityManager(
            state_manager=state_manager,
            stats_path=stats_path,
            stats_flush_interval_sec=0.0,
            actionable_min_samples=actionable_min_samples,
        )

    def _pipeline(self, state_manager: StateManager, quality_manager: QualityManager) -> SignalPipeline:
        return SignalPipeline(
            price_service=PriceService(),
            state_manager=state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=SignalQualityGate(state_manager=state_manager),
            quality_manager=quality_manager,
        )

    def test_quality_stats_persist_and_report_generate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stats_path = str(Path(temp_dir) / "lp_quality_stats.cache.json")
            state_manager = StateManager()
            self._seed_record(state_manager, record_id="pre1", stage="prealert", confirm_after_prealert=False, false_prealert=True, move_after_300s=-0.01)
            self._seed_record(state_manager, record_id="pre2", stage="prealert", confirm_after_prealert=True, false_prealert=False, move_after_300s=0.01)
            self._seed_record(state_manager, record_id="cl1", stage="climax", reversal_after_climax=True, move_after_60s=-0.02)
            manager = self._manager(state_manager, stats_path)

            manager.sync_from_state_manager(force=True)
            reloaded = self._manager(StateManager(), stats_path)
            report = reloaded.build_report(top_n=3)

            self.assertTrue(Path(stats_path).exists())
            self.assertEqual(3, report["overall"]["sample_size"])
            self.assertEqual(2, report["overall"]["prealert_count"])
            self.assertEqual(1, report["overall"]["climax_reversal_count"])
            self.assertTrue(report["top_bad_prealerts"])

    def test_samples_insufficient_guard_keeps_conclusions_soft(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stats_path = str(Path(temp_dir) / "lp_quality_stats.cache.json")
            state_manager = StateManager()
            self._seed_record(state_manager, record_id="pre1", stage="prealert", confirm_after_prealert=True, false_prealert=False, move_after_300s=0.01)
            manager = self._manager(state_manager, stats_path, actionable_min_samples=4)

            manager.sync_from_state_manager(force=True)
            report = manager.build_report(top_n=3)
            asset_row = report["dimensions"]["asset"][0]

            self.assertFalse(asset_row["actionable"])
            self.assertEqual("样本不足，仅观察", asset_row["tuning_hint"])
            self.assertEqual("历史样本有限", asset_row["quality_hint"])

    def test_low_prealert_precision_affects_trader_route_and_ops_hint(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stats_path = str(Path(temp_dir) / "lp_quality_stats.cache.json")
            state_manager = StateManager()
            self._seed_record(state_manager, record_id="pre1", stage="prealert", confirm_after_prealert=False, false_prealert=True, move_after_300s=-0.01)
            self._seed_record(state_manager, record_id="pre2", stage="prealert", confirm_after_prealert=False, false_prealert=True, move_after_300s=-0.02)
            self._seed_record(state_manager, record_id="pre3", stage="prealert", confirm_after_prealert=True, false_prealert=False, move_after_300s=0.005)
            manager = self._manager(state_manager, stats_path)

            event = self._event(stage="prealert")
            signal = self._signal(event, stage="prealert", user_tier="trader")
            payload = manager.annotate_lp_signal(event, signal)
            allowed, reason = evaluate_user_tier_lp_delivery(event, signal, "observe")
            report = manager.build_report(top_n=3)

            self.assertLess(payload["prealert_precision_score"], 0.55)
            self.assertFalse(allowed)
            self.assertIn("prealert_precision_filtered", reason)
            self.assertIn("retail/trader", report["top_bad_prealerts"][0]["tuning_hint"])

    def test_high_climax_reversal_affects_exhaustion_bias(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stats_path = str(Path(temp_dir) / "lp_quality_stats.cache.json")
            state_manager = StateManager()
            self._seed_record(state_manager, record_id="cl1", stage="climax", reversal_after_climax=True, move_after_60s=-0.02)
            self._seed_record(state_manager, record_id="cl2", stage="climax", reversal_after_climax=True, move_after_60s=-0.01)
            self._seed_record(state_manager, record_id="cl3", stage="climax", reversal_after_climax=True, move_after_60s=-0.015)
            manager = self._manager(state_manager, stats_path)

            event = self._event(stage="climax")
            signal = self._signal(event, stage="climax")
            payload = manager.annotate_lp_signal(event, signal)

            self.assertTrue(payload["exhaustion_bias_preferred"])
            self.assertEqual("exhaustion_risk", signal.context["lp_alert_stage"])

    def test_low_fastlane_roi_affects_promotion_priority(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stats_path = str(Path(temp_dir) / "lp_quality_stats.cache.json")
            state_manager = StateManager()
            self._seed_record(state_manager, record_id="fast1", stage="prealert", move_after_60s=-0.01, move_after_300s=-0.02, fastlane=True)
            self._seed_record(state_manager, record_id="fast2", stage="confirm", move_after_60s=-0.008, move_after_300s=-0.01, fastlane=True)
            self._seed_record(state_manager, record_id="fast3", stage="prealert", move_after_60s=-0.005, move_after_300s=-0.01, fastlane=True)
            manager = self._manager(state_manager, stats_path)
            pipeline = self._pipeline(state_manager, manager)

            event = self._event(stage="prealert")
            signal = self._signal(event, stage="prealert")
            manager.annotate_lp_signal(event, signal)
            payload = pipeline._promote_lp_fastlane_if_needed(
                event,
                signal,
                {"lp_structure_score": 0.62, "lp_multi_pool_resonance": 1},
            )

            self.assertEqual({}, payload)

    def test_empty_report_and_export_do_not_crash(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stats_path = str(Path(temp_dir) / "lp_quality_stats.cache.json")
            manager = self._manager(StateManager(), stats_path)

            report = manager.build_report(top_n=3)
            rows = manager.export_rows()

            self.assertEqual(0, report["overall"]["sample_size"])
            self.assertEqual([], rows)


if __name__ == "__main__":
    unittest.main()
