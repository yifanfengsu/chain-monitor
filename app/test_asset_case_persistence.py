import os
from pathlib import Path
import tempfile
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from asset_case_manager import AssetCaseManager
from models import Event, Signal


class AssetCasePersistenceTests(unittest.TestCase):
    def _event(
        self,
        *,
        pair_label: str,
        address: str,
        stage: str,
        ts: int,
        intent_type: str = "pool_buy_pressure",
        liquidation_stage: str = "none",
    ) -> Event:
        return Event(
            tx_hash=f"0xasset{pair_label}{ts}",
            address=address,
            token="ETH",
            amount=1.0,
            side="买入" if intent_type != "pool_sell_pressure" else "卖出",
            usd_value=80_000.0 if stage != "prealert" else 6_000.0,
            kind="swap",
            ts=ts,
            intent_type=intent_type,
            intent_stage="confirmed" if stage != "prealert" else "preliminary",
            intent_confidence=0.82,
            confirmation_score=0.82 if stage != "prealert" else 0.38,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "liquidation_stage": liquidation_stage,
                "raw": {
                    "lp_context": {
                        "pair_label": pair_label,
                        "pool_label": pair_label,
                        "base_token_symbol": pair_label.split("/")[0],
                        "quote_token_symbol": pair_label.split("/")[1],
                    }
                },
            },
        )

    def _signal(self, event: Event, *, stage: str) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.86,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.84,
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
                "user_tier": "research",
                "pair_label": event.metadata["raw"]["lp_context"]["pair_label"],
                "lp_event": True,
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
                "lp_market_read": "先手观察｜需看 60s 是否续单" if stage == "prealert" else "更像趋势确认，不是首发先手",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "续单消失 / 快速反向池流",
                "lp_followup_required": True,
                "lp_scan_path": "secondary",
                "lp_detect_latency_ms": 900,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def _gate_metrics(self, *, multi_pool: int = 0, surge: float = 1.5) -> dict:
        return {
            "lp_multi_pool_resonance": multi_pool,
            "lp_same_pool_continuity": 1,
            "lp_pool_volume_surge_ratio": surge,
        }

    def test_create_flush_and_reload_case(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = str(Path(temp_dir) / "asset_cases.cache.json")
            manager = AssetCaseManager(cache_path=cache_path, flush_interval_sec=0.0)
            event = self._event(pair_label="ETH/USDC", address="0xpool1", stage="prealert", ts=1_710_000_000)
            signal = self._signal(event, stage="prealert")

            payload = manager.merge_lp_signal(event, signal, gate_metrics=self._gate_metrics(multi_pool=1))
            manager.flush(force=True)

            restored = AssetCaseManager(cache_path=cache_path, flush_interval_sec=0.0)
            restored_snapshot = restored.snapshot()

            self.assertEqual(1, len(restored_snapshot))
            self.assertEqual(payload["asset_case_id"], restored_snapshot[0]["asset_case_id"])
            self.assertTrue(restored_snapshot[0]["restored_from_cache"])

    def test_reloaded_prealert_case_can_upgrade_to_confirm(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = str(Path(temp_dir) / "asset_cases.cache.json")
            manager = AssetCaseManager(cache_path=cache_path, flush_interval_sec=0.0)
            prealert_event = self._event(pair_label="ETH/USDT", address="0xpool1", stage="prealert", ts=1_710_000_000)
            prealert_signal = self._signal(prealert_event, stage="prealert")
            prealert_payload = manager.merge_lp_signal(prealert_event, prealert_signal, gate_metrics=self._gate_metrics(multi_pool=1))
            manager.flush(force=True)

            restored = AssetCaseManager(cache_path=cache_path, flush_interval_sec=0.0)
            confirm_event = self._event(pair_label="ETH/USDC", address="0xpool2", stage="confirm", ts=1_710_000_040)
            confirm_signal = self._signal(confirm_event, stage="confirm")
            confirm_payload = restored.merge_lp_signal(confirm_event, confirm_signal, gate_metrics=self._gate_metrics(multi_pool=2, surge=2.1))

            self.assertEqual(prealert_payload["asset_case_id"], confirm_payload["asset_case_id"])
            self.assertEqual("confirm", confirm_payload["asset_case_stage"])
            self.assertTrue(confirm_payload["asset_case_recovered"])

    def test_expired_cases_are_pruned(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = str(Path(temp_dir) / "asset_cases.cache.json")
            manager = AssetCaseManager(cache_path=cache_path, flush_interval_sec=0.0, window_sec=60)
            event = self._event(pair_label="ETH/USDC", address="0xpool1", stage="prealert", ts=1_710_000_000)
            signal = self._signal(event, stage="prealert")
            manager.merge_lp_signal(event, signal, gate_metrics=self._gate_metrics())
            manager.flush(force=True)

            removed = manager.prune_expired(now_ts=1_710_000_200)

            self.assertEqual(1, len(removed))
            self.assertEqual([], manager.snapshot())

    def test_malformed_cache_gracefully_falls_back_to_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "asset_cases.cache.json"
            cache_path.write_text("{not-json", encoding="utf-8")

            manager = AssetCaseManager(cache_path=str(cache_path), flush_interval_sec=0.0)

            self.assertEqual([], manager.snapshot())
            self.assertEqual("empty_or_missing", manager._last_load_status)

    def test_version_mismatch_uses_safe_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "asset_cases.cache.json"
            cache_path.write_text(
                """
{
  "schema_version": "asset_case_cache_v0",
  "cases": [
    {"asset_case_id": "asset_case:ETH:legacy", "asset_case_key": "legacy"}
  ]
}
""".strip(),
                encoding="utf-8",
            )

            manager = AssetCaseManager(cache_path=str(cache_path), flush_interval_sec=0.0)

            self.assertEqual([], manager.snapshot())
            self.assertTrue(str(manager._last_load_status).startswith("schema_mismatch"))


if __name__ == "__main__":
    unittest.main()
