import json
import os
import unittest
from pathlib import Path

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from clmm_parser import infer_position_intent, parse_clmm_candidate, update_position_state
from clmm_position_state import reset_position_runtime_state
from models import Event
from notifier import format_signal_message
from processor import _parse_clmm_position_candidate
from signal_interpreter import SignalInterpreter
from strategy_engine import StrategyEngine


FIXTURE_DIR = Path(__file__).resolve().parent / "test_fixtures" / "clmm"


class ClmmReplayFixtureTests(unittest.TestCase):
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

    def _manager_meta(self, fixture: dict) -> dict:
        protocol = str(fixture.get("protocol") or "")
        return {
            "protocol": protocol,
            "chain": str(fixture.get("chain") or "ethereum"),
            "manager_address": str(fixture.get("manager_address") or "").lower(),
            "abi_profile": str(fixture.get("manager_abi_profile") or f"{protocol}_position_manager"),
            "enabled": True,
            "note": f"golden-fixture:{fixture.get('fixture_name')}",
        }

    def _build_item(self, fixture: dict, step: dict) -> dict:
        item = dict(step.get("item") or {})
        item.setdefault("chain", str(fixture.get("chain") or "ethereum"))
        item.setdefault("watch_address", str(fixture.get("watch_address") or "").lower())
        item["position_manager_meta"] = self._manager_meta(fixture)
        clmm_context = dict(item.get("clmm_context") or {})
        if fixture.get("pair_label") and not clmm_context.get("pair_label"):
            clmm_context["pair_label"] = fixture.get("pair_label")
        item["clmm_context"] = clmm_context
        return item

    def _assert_subset(self, actual: dict, expected: dict, *, label: str) -> None:
        for key, value in (expected or {}).items():
            self.assertEqual(value, actual.get(key), f"{label}:{key}")

    def _replay_v3_fixture(self, filename: str) -> tuple[dict, dict, dict, dict]:
        fixture = self._load_fixture(filename)
        last_candidate: dict = {}
        last_intent: dict = {}
        last_state: dict = {}

        for step in fixture.get("steps") or []:
            label = str(step.get("label") or filename)
            item = self._build_item(fixture, step)
            candidate = parse_clmm_candidate(
                item,
                chain=str(fixture.get("chain") or "ethereum"),
                watch_address=str(fixture.get("watch_address") or "").lower(),
                watch_meta={},
                transfers=[],
                flows={},
            )
            self.assertIsNotNone(candidate, label)
            expected = step.get("expected") or {}
            self.assertEqual(expected.get("parse_status"), candidate.get("parse_status"), label)

            if candidate.get("parse_status") != "parsed":
                last_candidate = candidate
                continue

            state = update_position_state(candidate)
            intent = infer_position_intent(candidate)

            self._assert_subset(candidate, expected.get("candidate") or {}, label=f"{label}:candidate")
            self._assert_subset(intent, expected.get("intent") or {}, label=f"{label}:intent")
            self._assert_subset(state, expected.get("state") or {}, label=f"{label}:state")

            last_candidate = candidate
            last_intent = intent
            last_state = state

        return fixture, last_candidate, last_intent, last_state

    def _event_from_parsed(self, parsed: dict, watch_meta: dict) -> Event:
        raw = dict(parsed)
        return Event(
            tx_hash=str(parsed.get("tx_hash") or ""),
            address=str(parsed.get("watch_address") or "").lower(),
            token=str(parsed.get("token_contract") or "").lower() or None,
            amount=float(parsed.get("value") or 0.0),
            side=str(parsed.get("side") or parsed.get("direction") or ""),
            usd_value=float(parsed.get("value") or 0.0),
            kind=str(parsed.get("kind") or "clmm_observe"),
            ts=int(parsed.get("timestamp") or parsed.get("ingest_ts") or 0),
            intent_type=str(parsed.get("intent_type") or ""),
            intent_confidence=float(parsed.get("intent_confidence") or 0.0),
            intent_stage="weak",
            confirmation_score=float(parsed.get("confirmation_score") or 0.0),
            pricing_status="unknown",
            pricing_confidence=0.0,
            strategy_role=str(watch_meta.get("strategy_role") or "liquidity_provider"),
            metadata={
                "token_symbol": parsed.get("token_symbol"),
                "quote_symbol": parsed.get("quote_symbol"),
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
                "raw": raw,
            },
        )

    def _interpret(self, event: Event, signal, *, watch_meta: dict, counterparty_label: str) -> dict:
        counterparty = str(event.metadata.get("clmm_manager_address") or "").lower()
        self.interpreter.interpret(
            event=event,
            signal=signal,
            behavior={
                "behavior_type": str(signal.behavior_type or "clmm_partial_support"),
                "confidence": 1.0,
                "reason": "golden_fixture_replay",
            },
            watch_meta=watch_meta,
            watch_context={
                "watch_address": event.address,
                "watch_address_label": watch_meta.get("label", ""),
                "watch_meta": watch_meta,
                "counterparty": counterparty,
                "counterparty_label": counterparty_label,
                "counterparty_meta": {
                    "address": counterparty,
                    "label": counterparty_label,
                    "strategy_role": "protocol_contract",
                    "semantic_role": "protocol_contract",
                    "wallet_function": "position_manager",
                },
            },
            address_snapshot={"recent": []},
            token_snapshot={},
            gate_metrics={"resonance_score": 0.0},
        )
        return signal.context

    def test_uniswap_v3_golden_replay_fixtures(self) -> None:
        for filename in [
            "uniswap_v3_open.json",
            "uniswap_v3_increase.json",
            "uniswap_v3_decrease.json",
            "uniswap_v3_collect.json",
            "uniswap_v3_close.json",
            "uniswap_v3_range_shift_pair.json",
            "uniswap_v3_jit_like.json",
        ]:
            with self.subTest(fixture=filename):
                fixture, candidate, intent, state = self._replay_v3_fixture(filename)
                self.assertTrue(fixture.get("source_tx_hash") or fixture.get("source_transactions"))
                self.assertEqual("uniswap_v3", fixture.get("protocol"))
                self.assertEqual(str(fixture.get("manager_address") or "").lower(), candidate.get("position_manager"))
                self.assertEqual(str(fixture.get("pair_label") or ""), candidate.get("pair_label"))
                self.assertTrue(intent.get("intent_type"))
                self.assertTrue(state.get("position_key"))

    def test_uniswap_v4_partial_candidate_fixture_replay(self) -> None:
        fixture = self._load_fixture("uniswap_v4_partial_candidate.json")
        step = (fixture.get("steps") or [])[0]
        item = self._build_item(fixture, step)
        parsed = _parse_clmm_position_candidate(
            item,
            str(fixture.get("watch_address") or "").lower(),
            [],
            {},
        )

        self.assertIsNotNone(parsed)
        self._assert_subset(parsed, (step.get("expected") or {}).get("parsed") or {}, label="partial:parsed")

        watch_meta = {
            "address": str(fixture.get("watch_address") or "").lower(),
            "label": "Replay LP",
            "strategy_role": "liquidity_provider",
            "semantic_role": "lp_wallet",
            "wallet_function": "unknown",
        }
        event = self._event_from_parsed(parsed, watch_meta)
        signal = self.engine.decide(
            event,
            watch_meta,
            {"behavior_type": "clmm_partial_support", "confidence": 1.0, "reason": "golden_fixture_replay"},
            {"score": 1.0, "grade": "A"},
            {"score": 1.0, "grade": "A"},
            gate_metrics={
                "cooldown_key": "clmm_partial:golden",
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
            watch_meta,
            gate_metrics={},
        )
        signal.delivery_class = delivery_class
        signal.delivery_reason = delivery_reason
        context = self._interpret(
            event,
            signal,
            watch_meta=watch_meta,
            counterparty_label="Uniswap v4 Position Manager",
        )
        message = format_signal_message(signal, event)

        self.assertEqual(("observe", "clmm_partial_support_observe"), (delivery_class, delivery_reason))
        self.assertEqual("debug", context.get("message_template"))
        self.assertIn("V4 PositionManager 命中（部分支持）", message)
        self.assertIn("decode primitives", message)


if __name__ == "__main__":
    unittest.main()
