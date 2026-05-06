from __future__ import annotations

import unittest
from unittest import mock

import listener
import lp_drop_metadata
from constants import ERC20_TRANSFER_EVENT_SIG


def _topic(address: str) -> str:
    return f"0x{'0' * 24}{address.lower()[2:]}"


class ListenerPrefilterDropMetadataTests(unittest.TestCase):
    def test_lp_prefilter_drop_audit_contains_replay_metadata(self) -> None:
        pool = "0x1111111111111111111111111111111111111111"
        counterparty = "0x2222222222222222222222222222222222222222"
        watch = "0x3333333333333333333333333333333333333333"
        base_token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        quote_token = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        candidate = {
            "tx_hash": "0x" + "12" * 32,
            "ingest_ts": 1777996800,
            "touched_lp_pools": [pool],
            "pool_transfer_count_by_pool": {pool: 2},
            "participant_addresses": [pool, counterparty, watch],
            "logs": [
                {
                    "address": base_token,
                    "topics": [ERC20_TRANSFER_EVENT_SIG, _topic(pool), _topic(counterparty)],
                    "data": "0x64",
                },
                {
                    "address": quote_token,
                    "topics": [ERC20_TRANSFER_EVENT_SIG, _topic(counterparty), _topic(pool)],
                    "data": "0x3e8",
                },
            ],
        }
        decision = {"is_noise": True, "reason": "adjacent_watch_listener_high_confidence_lp_noise"}
        pool_meta = {
            "pool_address": pool,
            "pair_label": "WETH/USDC",
            "base_token_contract": base_token,
            "base_token_symbol": "WETH",
            "quote_token_contract": quote_token,
            "quote_token_symbol": "USDC",
        }

        with mock.patch.object(lp_drop_metadata, "get_lp_pool_meta", return_value=pool_meta):
            record = listener._listener_lp_adjacent_skip_audit_record(
                candidate=candidate,
                watch_address=watch,
                decision=decision,
            )

        self.assertEqual(1, record["drop_metadata_version"])
        self.assertEqual("listener_prefilter/drop", record["drop_reason"])
        self.assertEqual("drop", record["delivery_class"])
        self.assertEqual("listener_prefilter", record["stage"])
        self.assertEqual(candidate["tx_hash"], record["tx_hash"])
        self.assertTrue(record["event_id"].startswith("evt_"))
        self.assertEqual(pool, record["pool_address"])
        self.assertEqual("ETH/USDC", record["pair"])
        self.assertEqual("ETH", record["asset"])
        self.assertEqual("ETH", record["base"])
        self.assertEqual("USDC", record["quote"])
        self.assertEqual(1777996800, record["event_ts"])
        self.assertEqual("long", record["side"])
        self.assertEqual("pool_buy_pressure", record["intent_type"])


if __name__ == "__main__":
    unittest.main()
