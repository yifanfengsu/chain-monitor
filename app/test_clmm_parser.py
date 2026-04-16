import unittest

from clmm_parser import infer_position_intent, parse_clmm_candidate, update_position_state
from clmm_position_state import get_position_state, reset_position_runtime_state
from processor import _parse_clmm_position_candidate


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
OWNER = "0x1111111111111111111111111111111111111111"
MANAGER = "0x2222222222222222222222222222222222222222"
POOL = "0x3333333333333333333333333333333333333333"


class ClmmParserTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_position_runtime_state()

    def _decoded(self, *, address: str, event_name: str, args: dict, log_index: int) -> dict:
        return {
            "address": address,
            "event_name": event_name,
            "args": args,
            "log_index": log_index,
        }

    def _item(
        self,
        *,
        token_id: int,
        ingest_ts: int,
        method_name: str,
        decoded_logs: list[dict],
        amount0: float = 5.0,
        amount1: float = 10_000.0,
        tick_lower: int = -600,
        tick_upper: int = 600,
        token0_symbol: str = "ETH",
        token1_symbol: str = "USDC",
        protocol: str = "uniswap_v3",
        burst_meta: dict | None = None,
    ) -> dict:
        return {
            "tx_hash": f"0xclmm{token_id}{ingest_ts}",
            "watch_address": OWNER,
            "chain": "ethereum",
            "ingest_ts": ingest_ts,
            "method_name": method_name,
            "position_manager_meta": {
                "protocol": protocol,
                "chain": "ethereum",
                "manager_address": MANAGER,
                "abi_profile": f"{protocol}_position_manager",
                "enabled": True,
                "note": "unit-test",
            },
            "decoded_logs": decoded_logs,
            "clmm_context": {
                "pool": POOL,
                "pair_label": f"{token0_symbol}/{token1_symbol}",
                "token0": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "token1": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "token0_symbol": token0_symbol,
                "token1_symbol": token1_symbol,
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "burst_meta": burst_meta or {},
            },
            "token_id": str(token_id),
            "token0_contract": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "token1_contract": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "token0_symbol": token0_symbol,
            "token1_symbol": token1_symbol,
            "amount0": amount0,
            "amount1": amount1,
        }

    def _parse_and_infer(self, item: dict) -> tuple[dict, dict, dict]:
        candidate = parse_clmm_candidate(
            item,
            chain="ethereum",
            watch_address=OWNER,
            watch_meta={},
            transfers=[],
            flows={},
        )
        self.assertIsNotNone(candidate)
        self.assertEqual("parsed", candidate.get("parse_status"))
        state = update_position_state(candidate)
        intent = infer_position_intent(candidate)
        return candidate, intent, state

    def _mint_logs(
        self,
        *,
        token_id: int,
        liquidity: float = 100.0,
        amount0: float = 5.0,
        amount1: float = 10_000.0,
        tick_lower: int = -600,
        tick_upper: int = 600,
    ) -> list[dict]:
        return [
            self._decoded(
                address=MANAGER,
                event_name="Transfer",
                args={"from": ZERO_ADDRESS, "to": OWNER, "tokenId": token_id},
                log_index=0,
            ),
            self._decoded(
                address=MANAGER,
                event_name="IncreaseLiquidity",
                args={"tokenId": token_id, "liquidity": liquidity, "amount0": amount0, "amount1": amount1},
                log_index=1,
            ),
            self._decoded(
                address=POOL,
                event_name="Mint",
                args={
                    "owner": OWNER,
                    "tickLower": tick_lower,
                    "tickUpper": tick_upper,
                    "amount": liquidity,
                    "amount0": amount0,
                    "amount1": amount1,
                },
                log_index=2,
            ),
        ]

    def _increase_logs(
        self,
        *,
        token_id: int,
        liquidity: float = 40.0,
        amount0: float = 2.0,
        amount1: float = 4_000.0,
        tick_lower: int = -600,
        tick_upper: int = 600,
    ) -> list[dict]:
        return [
            self._decoded(
                address=MANAGER,
                event_name="IncreaseLiquidity",
                args={"tokenId": token_id, "liquidity": liquidity, "amount0": amount0, "amount1": amount1},
                log_index=0,
            ),
            self._decoded(
                address=POOL,
                event_name="Mint",
                args={
                    "owner": OWNER,
                    "tickLower": tick_lower,
                    "tickUpper": tick_upper,
                    "amount": liquidity,
                    "amount0": amount0,
                    "amount1": amount1,
                },
                log_index=1,
            ),
        ]

    def _decrease_logs(
        self,
        *,
        token_id: int,
        liquidity: float = 30.0,
        amount0: float = 1.0,
        amount1: float = 2_000.0,
        tick_lower: int = -600,
        tick_upper: int = 600,
        burn: bool = False,
    ) -> list[dict]:
        logs = [
            self._decoded(
                address=MANAGER,
                event_name="DecreaseLiquidity",
                args={"tokenId": token_id, "liquidity": liquidity, "amount0": amount0, "amount1": amount1},
                log_index=0,
            ),
            self._decoded(
                address=POOL,
                event_name="Burn",
                args={
                    "owner": OWNER,
                    "tickLower": tick_lower,
                    "tickUpper": tick_upper,
                    "amount": liquidity,
                    "amount0": amount0,
                    "amount1": amount1,
                },
                log_index=1,
            ),
        ]
        if burn:
            logs.append(
                self._decoded(
                    address=MANAGER,
                    event_name="Transfer",
                    args={"from": OWNER, "to": ZERO_ADDRESS, "tokenId": token_id},
                    log_index=2,
                )
            )
        return logs

    def test_v3_mint_open_maps_to_position_open(self) -> None:
        item = self._item(token_id=12345, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=12345))
        candidate, intent, state = self._parse_and_infer(item)

        self.assertEqual("clmm_position_open", candidate.get("base_action"))
        self.assertEqual("clmm_position_open", intent.get("intent_type"))
        self.assertEqual("12345", state.get("token_id"))
        self.assertEqual(100.0, state.get("current_liquidity"))

    def test_increase_liquidity_maps_to_add_range_liquidity(self) -> None:
        self._parse_and_infer(self._item(token_id=1, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=1)))
        item = self._item(token_id=1, ingest_ts=1_710_000_060, method_name="increaseLiquidity", decoded_logs=self._increase_logs(token_id=1))
        _, intent, state = self._parse_and_infer(item)

        self.assertEqual("clmm_position_add_range_liquidity", intent.get("intent_type"))
        self.assertEqual(140.0, state.get("current_liquidity"))

    def test_decrease_liquidity_maps_to_remove_range_liquidity(self) -> None:
        self._parse_and_infer(self._item(token_id=2, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=2)))
        item = self._item(token_id=2, ingest_ts=1_710_000_090, method_name="decreaseLiquidity", decoded_logs=self._decrease_logs(token_id=2))
        _, intent, state = self._parse_and_infer(item)

        self.assertEqual("clmm_position_remove_range_liquidity", intent.get("intent_type"))
        self.assertEqual(70.0, state.get("current_liquidity"))

    def test_collect_only_maps_to_passive_fee_harvest(self) -> None:
        self._parse_and_infer(self._item(token_id=3, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=3)))
        item = self._item(
            token_id=3,
            ingest_ts=1_710_000_120,
            method_name="collect",
            decoded_logs=[
                self._decoded(
                    address=MANAGER,
                    event_name="Collect",
                    args={"tokenId": 3, "recipient": OWNER, "amount0": 0.3, "amount1": 120.0},
                    log_index=0,
                ),
            ],
        )
        _, intent, state = self._parse_and_infer(item)

        self.assertEqual("clmm_passive_fee_harvest", intent.get("intent_type"))
        self.assertEqual(1, state.get("collected_fees_count"))
        self.assertEqual(100.0, state.get("current_liquidity"))

    def test_burn_close_maps_to_position_close(self) -> None:
        self._parse_and_infer(self._item(token_id=4, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=4)))
        item = self._item(token_id=4, ingest_ts=1_710_000_150, method_name="burn", decoded_logs=self._decrease_logs(token_id=4, liquidity=100.0, amount0=5.0, amount1=10_000.0, burn=True))
        _, intent, state = self._parse_and_infer(item)

        self.assertEqual("clmm_position_close", intent.get("intent_type"))
        self.assertEqual(0.0, state.get("current_liquidity"))

    def test_range_shift_detected_on_remove_then_new_range(self) -> None:
        self._parse_and_infer(self._item(token_id=10, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=10, tick_lower=-600, tick_upper=600)))
        self._parse_and_infer(self._item(token_id=10, ingest_ts=1_710_000_180, method_name="decreaseLiquidity", decoded_logs=self._decrease_logs(token_id=10, liquidity=100.0, amount0=5.0, amount1=10_000.0)))
        item = self._item(
            token_id=11,
            ingest_ts=1_710_000_210,
            method_name="mint",
            decoded_logs=self._mint_logs(token_id=11, amount0=8.0, amount1=18_000.0, tick_lower=-120, tick_upper=120),
            amount0=8.0,
            amount1=18_000.0,
            tick_lower=-120,
            tick_upper=120,
        )
        _, intent, _ = self._parse_and_infer(item)

        self.assertEqual("clmm_range_shift", intent.get("intent_type"))

    def test_inventory_recenter_when_range_shift_keeps_similar_exposure(self) -> None:
        self._parse_and_infer(self._item(token_id=20, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=20, tick_lower=-600, tick_upper=600)))
        self._parse_and_infer(self._item(token_id=20, ingest_ts=1_710_000_180, method_name="decreaseLiquidity", decoded_logs=self._decrease_logs(token_id=20, liquidity=100.0, amount0=5.0, amount1=10_000.0)))
        item = self._item(
            token_id=21,
            ingest_ts=1_710_000_210,
            method_name="mint",
            decoded_logs=self._mint_logs(token_id=21, amount0=5.1, amount1=10_100.0, tick_lower=-180, tick_upper=180),
            amount0=5.1,
            amount1=10_100.0,
            tick_lower=-180,
            tick_upper=180,
        )
        _, intent, _ = self._parse_and_infer(item)

        self.assertEqual("clmm_inventory_recenter", intent.get("intent_type"))

    def test_short_lived_position_around_burst_swap_maps_to_jit_likely(self) -> None:
        self._parse_and_infer(self._item(token_id=30, ingest_ts=1_710_000_000, method_name="mint", decoded_logs=self._mint_logs(token_id=30)))
        item = self._item(
            token_id=30,
            ingest_ts=1_710_000_090,
            method_name="decreaseLiquidity",
            decoded_logs=self._decrease_logs(token_id=30, liquidity=100.0, amount0=5.0, amount1=10_000.0, burn=True),
            burst_meta={"swap_burst_count": 4, "impact_window_hit": True},
        )
        _, intent, _ = self._parse_and_infer(item)

        self.assertEqual("clmm_jit_liquidity_likely", intent.get("intent_type"))

    def test_v4_manager_path_returns_partial_support_reason(self) -> None:
        item = self._item(
            token_id=999,
            ingest_ts=1_710_000_500,
            method_name="modifyLiquidities",
            decoded_logs=[],
            protocol="uniswap_v4",
        )
        candidate = parse_clmm_candidate(item, chain="ethereum", watch_address=OWNER, watch_meta={}, transfers=[], flows={})

        self.assertIsNotNone(candidate)
        self.assertEqual("candidate_only", candidate.get("parse_status"))
        self.assertIn("partial_support", candidate)
        self.assertIn("uniswap_v4", candidate.get("reason", ""))

    def test_v4_manager_path_surfaces_partial_support_event_in_processor(self) -> None:
        item = self._item(
            token_id=999,
            ingest_ts=1_710_000_500,
            method_name="modifyLiquidities",
            decoded_logs=[],
            protocol="uniswap_v4",
            amount0=0.75,
            amount1=1_250.0,
        )
        parsed = _parse_clmm_position_candidate(item, OWNER, [], {})

        self.assertIsNotNone(parsed)
        self.assertEqual("clmm_partial_support", parsed.get("monitor_type"))
        self.assertFalse(parsed.get("clmm_position_event"))
        self.assertTrue(parsed.get("clmm_partial_candidate"))
        self.assertTrue(parsed.get("clmm_partial_support"))
        self.assertEqual("candidate_only", parsed.get("clmm_candidate_status"))
        self.assertEqual("clmm_partial_support_observation", parsed.get("intent_type"))
        self.assertGreater(float(parsed.get("value") or 0.0), 0.0)


if __name__ == "__main__":
    unittest.main()
