import json
import os
import unittest
from pathlib import Path

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from clmm_decoder_normalizer import build_parser_input_from_decoder_snapshot
from clmm_parser import infer_position_intent, parse_clmm_candidate, update_position_state
from clmm_position_state import reset_position_runtime_state
from models import Event
from notifier import format_signal_message
from processor import _parse_clmm_position_candidate
from signal_interpreter import SignalInterpreter
from strategy_engine import StrategyEngine


FIXTURE_DIR = Path(__file__).resolve().parent / "test_fixtures" / "clmm_decoder"


class ClmmDecoderReplayTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        reset_position_runtime_state()
        self.interpreter = SignalInterpreter()
        self.engine = StrategyEngine(
            min_confidence=0.0,
            min_address_score=0.0,
            min_token_score=0.0,
            min_behavior_confidence=0.0,
            require_non_normal_behavior=False,
        )

    def _load_fixture(self, filename: str) -> dict:
        with (FIXTURE_DIR / filename).open(encoding="utf-8") as fp:
            return json.load(fp)

    def _assert_subset(self, actual: dict, expected: dict, *, label: str) -> None:
        for key, value in (expected or {}).items():
            if key == "decoded_logs_count":
                self.assertEqual(value, len(actual.get("decoded_logs") or []), f"{label}:{key}")
            elif isinstance(value, float):
                self.assertAlmostEqual(value, float(actual.get(key) or 0.0), places=6, msg=f"{label}:{key}")
            else:
                self.assertEqual(value, actual.get(key), f"{label}:{key}")

    def _event_from_partial(self, parsed: dict, watch_address: str) -> Event:
        return Event(
            tx_hash=str(parsed.get("tx_hash") or ""),
            address=str(watch_address or "").lower(),
            token=str(parsed.get("token_contract") or "").lower() or None,
            amount=float(parsed.get("value") or 0.0),
            side=str(parsed.get("side") or ""),
            usd_value=float(parsed.get("value") or 0.0),
            kind=str(parsed.get("kind") or "clmm_observe"),
            ts=int(parsed.get("ingest_ts") or parsed.get("timestamp") or 0),
            intent_type=str(parsed.get("intent_type") or ""),
            intent_confidence=float(parsed.get("intent_confidence") or 0.0),
            intent_stage="weak",
            confirmation_score=float(parsed.get("confirmation_score") or 0.0),
            pricing_status="unknown",
            pricing_confidence=0.0,
            strategy_role="liquidity_provider",
            metadata={
                "pair_label": parsed.get("pair_label"),
                "monitor_type": parsed.get("monitor_type"),
                "clmm_position_event": bool(parsed.get("clmm_position_event")),
                "clmm_partial_candidate": bool(parsed.get("clmm_partial_candidate")),
                "clmm_partial_support": bool(parsed.get("clmm_partial_support")),
                "clmm_partial_reason": str(parsed.get("clmm_partial_reason") or ""),
                "clmm_manager_protocol": str(parsed.get("clmm_manager_protocol") or ""),
                "clmm_manager_address": str(parsed.get("clmm_manager_address") or ""),
                "clmm_candidate_status": str(parsed.get("clmm_candidate_status") or ""),
                "clmm_context": dict(parsed.get("clmm_context") or {}),
                "raw": dict(parsed),
            },
        )

    def test_decoder_aligned_v3_replay_fixtures(self) -> None:
        for filename in [
            "uniswap_v3_open_decoder_snapshot.json",
            "uniswap_v3_increase_decoder_snapshot.json",
            "uniswap_v3_decrease_decoder_snapshot.json",
            "uniswap_v3_collect_decoder_snapshot.json",
            "uniswap_v3_close_decoder_snapshot.json",
            "uniswap_v3_range_shift_pair_decoder_snapshot.json",
            "uniswap_v3_jit_like_decoder_snapshot.json",
        ]:
            with self.subTest(fixture=filename):
                fixture = self._load_fixture(filename)
                self.assertEqual("decoded_logs_snapshot", fixture.get("source_kind"))
                self.assertTrue(fixture.get("original_shape_version"))
                self.assertTrue(fixture.get("source_tx_hash"))
                self.assertTrue(fixture.get("source_block_number"))
                self.assertTrue(fixture.get("source_transactions") or fixture.get("source_tx_hash"))

                for step in fixture.get("steps") or []:
                    item = build_parser_input_from_decoder_snapshot(fixture, step)
                    self._assert_subset(item, step.get("expected_normalized_candidate") or {}, label=f"{filename}:{step.get('label')}:normalized")

                    candidate = parse_clmm_candidate(
                        item,
                        chain=str(fixture.get("chain") or "ethereum"),
                        watch_address=str(fixture.get("watch_address") or "").lower(),
                        watch_meta={},
                        transfers=[],
                        flows={},
                    )
                    self.assertIsNotNone(candidate, step.get("label"))
                    self.assertEqual(step.get("expected_parser_status"), candidate.get("parse_status"), step.get("label"))
                    if candidate.get("parse_status") != "parsed":
                        continue

                    state = update_position_state(candidate)
                    intent = infer_position_intent(candidate)
                    self.assertEqual(step.get("expected_intent"), intent.get("intent_type"), step.get("label"))
                    self._assert_subset(state, step.get("expected_state") or {}, label=f"{filename}:{step.get('label')}:state")

    def test_decoder_aligned_v4_partial_candidate_fixture(self) -> None:
        fixture = self._load_fixture("uniswap_v4_partial_candidate_decoder_snapshot.json")
        self.assertEqual("decoded_logs_snapshot", fixture.get("source_kind"))
        self.assertTrue(fixture.get("original_shape_version"))
        self.assertTrue(fixture.get("source_tx_hash"))
        self.assertTrue(fixture.get("source_block_number"))
        step = (fixture.get("steps") or [])[0]
        item = build_parser_input_from_decoder_snapshot(fixture, step)
        self._assert_subset(item, step.get("expected_normalized_candidate") or {}, label="v4:normalized")

        candidate = parse_clmm_candidate(
            item,
            chain=str(fixture.get("chain") or "ethereum"),
            watch_address=str(fixture.get("watch_address") or "").lower(),
            watch_meta={},
            transfers=[],
            flows={},
        )
        self.assertIsNotNone(candidate)
        self.assertEqual(step.get("expected_parser_status"), candidate.get("parse_status"))
        self.assertTrue(candidate.get("partial_support"))

        parsed = _parse_clmm_position_candidate(
            item,
            str(fixture.get("watch_address") or "").lower(),
            [],
            {},
        )
        self.assertIsNotNone(parsed)
        self._assert_subset(parsed, step.get("expected_runtime_parsed") or {}, label="v4:runtime")
        self.assertEqual(step.get("expected_intent"), parsed.get("intent_type"))

        watch_address = str(fixture.get("watch_address") or "").lower()
        event = self._event_from_partial(parsed, watch_address)
        signal = self.engine.decide(
            event,
            {"strategy_role": "liquidity_provider", "label": "Replay LP"},
            {"behavior_type": "clmm_partial_support", "confidence": 1.0, "reason": "decoder_fixture_replay"},
            {"score": 1.0, "grade": "A"},
            {"score": 1.0, "grade": "A"},
            gate_metrics={
                "cooldown_key": "clmm_partial:decoder",
                "quality_score": 0.1,
                "adjusted_quality_score": 0.1,
                "resonance_score": 0.0,
                "dynamic_min_usd": 0.0,
            },
        )
        self.assertIsNotNone(signal)
        delivery_class, delivery_reason = self.engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "liquidity_provider"},
            gate_metrics={},
        )
        signal.delivery_class = delivery_class
        signal.delivery_reason = delivery_reason
        self.interpreter.interpret(
            event,
            signal,
            {"behavior_type": "clmm_partial_support", "confidence": 1.0, "reason": "decoder_fixture_replay"},
            {
                "address": watch_address,
                "label": "Replay LP",
                "strategy_role": "liquidity_provider",
                "semantic_role": "lp_wallet",
                "wallet_function": "unknown",
            },
            {
                "watch_address": watch_address,
                "watch_address_label": "Replay LP",
                "watch_meta": {"strategy_role": "liquidity_provider"},
                "counterparty": str(parsed.get("clmm_manager_address") or "").lower(),
                "counterparty_label": "Uniswap v4 Position Manager",
                "counterparty_meta": {
                    "address": str(parsed.get("clmm_manager_address") or "").lower(),
                    "label": "Uniswap v4 Position Manager",
                    "strategy_role": "protocol_contract",
                    "semantic_role": "protocol_contract",
                    "wallet_function": "position_manager",
                },
            },
            {"recent": []},
            {},
            gate_metrics={"resonance_score": 0.0},
        )
        message = format_signal_message(signal, event)

        self.assertEqual(("observe", "clmm_partial_support_observe"), (delivery_class, delivery_reason))
        self.assertEqual("debug", signal.context.get("message_template"))
        self.assertIn("V4 PositionManager 命中（部分支持）", message)
        self.assertNotEqual("clmm_position_open", signal.context.get("operational_intent_key"))


if __name__ == "__main__":
    unittest.main()
