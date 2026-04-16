import unittest

import filter
from address_intelligence import AddressIntelligenceManager
from processor import _attach_context_fields


class AddressRegistryMixin:
    def setUp(self) -> None:
        self._orig_meta = dict(filter.ADDRESS_META)
        self._orig_labels = dict(filter.ADDRESS_LABELS)
        self._orig_all_watch = set(filter.ALL_WATCH_ADDRESSES)
        self._orig_watch = set(filter.WATCH_ADDRESSES)

    def tearDown(self) -> None:
        filter.ADDRESS_META.clear()
        filter.ADDRESS_META.update(self._orig_meta)
        filter.ADDRESS_LABELS.clear()
        filter.ADDRESS_LABELS.update(self._orig_labels)
        filter.ALL_WATCH_ADDRESSES.clear()
        filter.ALL_WATCH_ADDRESSES.update(self._orig_all_watch)
        filter.WATCH_ADDRESSES.clear()
        filter.WATCH_ADDRESSES.update(self._orig_watch)

    def _register_meta(
        self,
        address: str,
        *,
        label: str,
        role: str = "exchange",
        strategy_role: str = "exchange_hot_wallet",
        semantic_role: str = "exchange_hot_wallet",
        wallet_function: str = "exchange_hot",
        entity_id: str = "",
        entity_label: str = "",
        entity_type: str = "exchange",
        entity_source: str = "address_book",
        entity_attribution_strength: str = "confirmed_entity",
        is_watch: bool = True,
    ) -> None:
        addr = address.lower()
        filter.ADDRESS_META[addr] = {
            "address": addr,
            "label": label,
            "category": "exchange_whale" if entity_type == "exchange" else "unknown",
            "priority": 2,
            "is_active": True,
            "note": "",
            "source": "unit_test",
            "role": role,
            "strategy_role": strategy_role,
            "semantic_role": semantic_role,
            "role_source": "explicit",
            "entity_id": entity_id,
            "entity_label": entity_label,
            "entity_type": entity_type,
            "wallet_function": wallet_function,
            "cluster_tags": [],
            "ownership_confidence": 1.0 if entity_id else 0.0,
            "entity_confidence": 1.0 if entity_id else 0.0,
            "entity_source": entity_source,
            "entity_attribution_strength": entity_attribution_strength,
            "entity_why": ["unit_test_seed"],
            "wallet_function_confidence": 1.0,
            "wallet_function_source": "address_book",
        }
        filter.ADDRESS_LABELS[addr] = label
        if is_watch:
            filter.ALL_WATCH_ADDRESSES.add(addr)
            filter.WATCH_ADDRESSES.add(addr)

    def _parse_transfer(self, *, from_addr: str, to_addr: str, watch_address: str, counterparty: str) -> dict:
        parsed = {
            "kind": "token_transfer",
            "watch_address": watch_address.lower(),
            "counterparty": counterparty.lower(),
            "from": from_addr.lower(),
            "to": to_addr.lower(),
            "token_contract": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "token_symbol": "USDT",
            "quote_token_contract": "",
            "quote_symbol": "",
            "value": 1.0,
            "tx_hash": "0xtest",
        }
        return _attach_context_fields(
            parsed,
            watch_address=watch_address.lower(),
            counterparty=counterparty.lower(),
            stable_tokens_touched=["0xdac17f958d2ee523a2206206994597c13d831ec7"],
            volatile_tokens_touched=[],
        )


class ExchangeEntityAttributionTests(AddressRegistryMixin, unittest.TestCase):
    def test_same_entity_deposit_to_hot_is_user_deposit_consolidation(self) -> None:
        deposit = "0x1000000000000000000000000000000000000001"
        hot = "0x1000000000000000000000000000000000000002"
        self._register_meta(
            deposit,
            label="Binance Deposit",
            strategy_role="exchange_deposit_wallet",
            semantic_role="exchange_deposit_address",
            wallet_function="exchange_deposit",
            entity_id="binance",
            entity_label="Binance",
        )
        self._register_meta(
            hot,
            label="Binance Hot",
            strategy_role="exchange_hot_wallet",
            semantic_role="exchange_hot_wallet",
            wallet_function="exchange_hot",
            entity_id="binance",
            entity_label="Binance",
        )

        parsed = self._parse_transfer(from_addr=deposit, to_addr=hot, watch_address=hot, counterparty=deposit)

        self.assertEqual("confirmed", parsed.get("exchange_internality"))
        self.assertEqual("exchange_user_deposit_consolidation", parsed.get("exchange_transfer_purpose"))
        self.assertEqual("exchange_user_deposit_consolidation", parsed.get("exchange_transfer_purpose_family"))
        self.assertEqual("confirmed", parsed.get("exchange_transfer_purpose_strength"))
        self.assertTrue(parsed.get("possible_internal_transfer"))

    def test_same_entity_hot_to_cold_is_overflow_to_cold(self) -> None:
        hot = "0x1000000000000000000000000000000000000011"
        cold = "0x1000000000000000000000000000000000000012"
        self._register_meta(hot, label="Binance Hot", wallet_function="exchange_hot", entity_id="binance", entity_label="Binance")
        self._register_meta(
            cold,
            label="Binance Cold",
            strategy_role="exchange_hot_wallet",
            semantic_role="exchange_hot_wallet",
            wallet_function="exchange_cold",
            entity_id="binance",
            entity_label="Binance",
        )

        parsed = self._parse_transfer(from_addr=hot, to_addr=cold, watch_address=hot, counterparty=cold)

        self.assertEqual("exchange_hot_wallet_overflow_to_cold", parsed.get("exchange_transfer_purpose"))
        self.assertEqual("confirmed", parsed.get("exchange_internality"))
        self.assertEqual("confirmed", parsed.get("exchange_transfer_purpose_strength"))

    def test_same_entity_cold_to_hot_is_cold_wallet_topup(self) -> None:
        cold = "0x1000000000000000000000000000000000000021"
        hot = "0x1000000000000000000000000000000000000022"
        self._register_meta(cold, label="Binance Cold", wallet_function="exchange_cold", entity_id="binance", entity_label="Binance")
        self._register_meta(hot, label="Binance Hot", wallet_function="exchange_hot", entity_id="binance", entity_label="Binance")

        parsed = self._parse_transfer(from_addr=cold, to_addr=hot, watch_address=hot, counterparty=cold)

        self.assertEqual("exchange_hot_wallet_cold_wallet_topup", parsed.get("exchange_transfer_purpose"))
        self.assertEqual("confirmed", parsed.get("exchange_internality"))
        self.assertEqual("confirmed", parsed.get("exchange_transfer_purpose_strength"))

    def test_hot_to_external_is_not_internal(self) -> None:
        hot = "0x1000000000000000000000000000000000000031"
        external = "0x1000000000000000000000000000000000000032"
        self._register_meta(hot, label="Binance Hot", wallet_function="exchange_hot", entity_id="binance", entity_label="Binance")

        parsed = self._parse_transfer(from_addr=hot, to_addr=external, watch_address=hot, counterparty=external)

        self.assertEqual("exchange_hot_wallet_withdrawal_outflow", parsed.get("exchange_transfer_purpose"))
        self.assertEqual("no", parsed.get("exchange_internality"))
        self.assertEqual("no", parsed.get("exchange_transfer_purpose_strength"))
        self.assertEqual("none", parsed.get("exchange_external_counterparty_risk_class"))
        self.assertFalse(parsed.get("exchange_distribution_risk_target_eligible"))
        self.assertFalse(parsed.get("possible_internal_transfer"))

    def test_external_to_exchange_deposit_defaults_to_neutral_inflow_observation_context(self) -> None:
        external = "0x1000000000000000000000000000000000000038"
        deposit = "0x1000000000000000000000000000000000000039"
        self._register_meta(
            deposit,
            label="Binance Deposit",
            strategy_role="exchange_deposit_wallet",
            semantic_role="exchange_deposit_address",
            wallet_function="exchange_deposit",
            entity_id="binance",
            entity_label="Binance",
        )

        parsed = self._parse_transfer(from_addr=external, to_addr=deposit, watch_address=deposit, counterparty=external)

        self.assertEqual("exchange_external_inflow", parsed.get("exchange_transfer_purpose_family"))
        self.assertEqual("observe", parsed.get("exchange_inflow_context_strength"))
        self.assertFalse(parsed.get("exchange_followup_ready"))
        self.assertEqual("no", parsed.get("exchange_transfer_purpose_strength"))

    def test_hot_to_external_entity_label_alone_is_not_distribution_risk_target(self) -> None:
        hot = "0x100000000000000000000000000000000000003a"
        labeled = "0x100000000000000000000000000000000000003b"
        self._register_meta(hot, label="Binance Hot", wallet_function="exchange_hot", entity_id="binance", entity_label="Binance")
        self._register_meta(
            labeled,
            label="Some Labeled External",
            role="unknown",
            strategy_role="unknown",
            semantic_role="unknown",
            wallet_function="unknown",
            entity_id="",
            entity_label="External Desk",
            entity_type="unknown",
            entity_attribution_strength="unknown",
            is_watch=False,
        )

        parsed = self._parse_transfer(from_addr=hot, to_addr=labeled, watch_address=hot, counterparty=labeled)

        self.assertEqual("none", parsed.get("exchange_external_counterparty_risk_class"))
        self.assertFalse(parsed.get("exchange_distribution_risk_target_eligible"))

    def test_hot_to_smart_money_target_marks_distribution_risk_target_eligible(self) -> None:
        hot = "0x100000000000000000000000000000000000003c"
        smart_money = "0x100000000000000000000000000000000000003d"
        self._register_meta(hot, label="Binance Hot", wallet_function="exchange_hot", entity_id="binance", entity_label="Binance")
        self._register_meta(
            smart_money,
            label="Smart Money",
            role="smart_money",
            strategy_role="smart_money_wallet",
            semantic_role="trader_wallet",
            wallet_function="unknown",
            entity_type="unknown",
            entity_attribution_strength="confirmed_entity",
            is_watch=False,
        )

        parsed = self._parse_transfer(from_addr=hot, to_addr=smart_money, watch_address=hot, counterparty=smart_money)

        self.assertEqual("smart_money", parsed.get("exchange_external_counterparty_risk_class"))
        self.assertTrue(parsed.get("exchange_distribution_risk_target_eligible"))

    def test_hot_to_bridge_like_target_marks_distribution_risk_target_eligible(self) -> None:
        hot = "0x100000000000000000000000000000000000003e"
        bridge = "0x100000000000000000000000000000000000003f"
        self._register_meta(hot, label="Binance Hot", wallet_function="exchange_hot", entity_id="binance", entity_label="Binance")
        self._register_meta(
            bridge,
            label="Across Bridge",
            role="bridge",
            strategy_role="unknown",
            semantic_role="bridge_contract",
            wallet_function="bridge",
            entity_type="unknown",
            entity_attribution_strength="unknown",
            is_watch=False,
        )

        parsed = self._parse_transfer(from_addr=hot, to_addr=bridge, watch_address=hot, counterparty=bridge)

        self.assertEqual("bridge", parsed.get("exchange_external_counterparty_risk_class"))
        self.assertTrue(parsed.get("exchange_distribution_risk_target_eligible"))

    def test_different_exchange_entities_do_not_become_same_entity_internal(self) -> None:
        binance_hot = "0x1000000000000000000000000000000000000041"
        bybit_hot = "0x1000000000000000000000000000000000000042"
        self._register_meta(binance_hot, label="Binance Hot", wallet_function="exchange_hot", entity_id="binance", entity_label="Binance")
        self._register_meta(bybit_hot, label="Bybit Hot", wallet_function="exchange_hot", entity_id="bybit", entity_label="Bybit")

        parsed = self._parse_transfer(from_addr=binance_hot, to_addr=bybit_hot, watch_address=binance_hot, counterparty=bybit_hot)

        self.assertEqual("no", parsed.get("exchange_internality"))
        self.assertEqual("exchange_unknown_flow", parsed.get("exchange_transfer_purpose_family"))
        self.assertEqual("no", parsed.get("exchange_transfer_purpose_strength"))
        self.assertFalse(parsed.get("possible_internal_transfer"))
        self.assertNotEqual("exchange_internal_rebalance", parsed.get("exchange_transfer_purpose"))

    def test_likely_same_entity_deposit_to_hot_keeps_family_but_drops_strength(self) -> None:
        deposit = "0x1000000000000000000000000000000000000061"
        hot = "0x1000000000000000000000000000000000000062"
        self._register_meta(
            deposit,
            label="Binance Deposit Candidate",
            strategy_role="exchange_deposit_wallet",
            semantic_role="exchange_deposit_address",
            wallet_function="exchange_deposit",
            entity_id="",
            entity_label="Binance",
            entity_attribution_strength="likely_entity",
        )
        self._register_meta(
            hot,
            label="Binance Hot Candidate",
            strategy_role="exchange_hot_wallet",
            semantic_role="exchange_hot_wallet",
            wallet_function="exchange_hot",
            entity_id="",
            entity_label="Binance",
            entity_attribution_strength="likely_entity",
        )

        parsed = self._parse_transfer(from_addr=deposit, to_addr=hot, watch_address=hot, counterparty=deposit)

        self.assertEqual("likely", parsed.get("exchange_internality"))
        self.assertEqual("exchange_user_deposit_consolidation", parsed.get("exchange_transfer_purpose_family"))
        self.assertEqual("likely", parsed.get("exchange_transfer_purpose_strength"))
        self.assertFalse(parsed.get("possible_internal_transfer"))

    def test_likely_same_entity_hot_to_cold_stays_likely_only(self) -> None:
        hot = "0x1000000000000000000000000000000000000071"
        cold = "0x1000000000000000000000000000000000000072"
        self._register_meta(
            hot,
            label="Binance Hot Candidate",
            wallet_function="exchange_hot",
            entity_id="",
            entity_label="Binance",
            entity_attribution_strength="likely_entity",
        )
        self._register_meta(
            cold,
            label="Binance Cold Candidate",
            strategy_role="exchange_hot_wallet",
            semantic_role="exchange_hot_wallet",
            wallet_function="exchange_cold",
            entity_id="",
            entity_label="Binance",
            entity_attribution_strength="likely_entity",
        )

        parsed = self._parse_transfer(from_addr=hot, to_addr=cold, watch_address=hot, counterparty=cold)

        self.assertEqual("likely", parsed.get("exchange_internality"))
        self.assertEqual("exchange_hot_wallet_overflow_to_cold", parsed.get("exchange_transfer_purpose_family"))
        self.assertEqual("likely", parsed.get("exchange_transfer_purpose_strength"))
        self.assertFalse(parsed.get("possible_internal_transfer"))

    def test_missing_entity_id_only_yields_likely_or_adjacent(self) -> None:
        known_exchange = "0x1000000000000000000000000000000000000051"
        candidate = "0x1000000000000000000000000000000000000052"
        adjacent_only = "0x1000000000000000000000000000000000000053"
        self._register_meta(
            known_exchange,
            label="Binance Hot",
            wallet_function="exchange_hot",
            entity_id="binance",
            entity_label="Binance",
        )

        manager = AddressIntelligenceManager(watch_addresses=set())
        intel = manager._ensure_intel(candidate, 1)
        intel.top_counterparties[known_exchange] = 4
        intel.same_block_with_watch_count = 3
        intel.same_token_resonance_count = 2
        intel.exchange_interactions = 4
        intel.suspected_role = "exchange_adjacent"
        intel.role_confidence = 0.72

        likely = manager.infer_entity_context(candidate)
        self.assertEqual("likely_entity", likely.get("entity_attribution_strength"))
        self.assertEqual("inferred_likely", likely.get("entity_source"))
        self.assertEqual("", likely.get("entity_id"))
        self.assertEqual("Binance", likely.get("entity_label"))

        adjacent_intel = manager._ensure_intel(adjacent_only, 1)
        adjacent_intel.exchange_interactions = 5
        adjacent_intel.same_block_with_watch_count = 2
        adjacent_intel.suspected_role = "exchange_adjacent"
        adjacent_intel.role_confidence = 0.61

        adjacent = manager.infer_entity_context(adjacent_only)
        self.assertEqual("adjacent_only", adjacent.get("entity_attribution_strength"))
        self.assertEqual("adjacent_only", adjacent.get("entity_source"))
        self.assertEqual("", adjacent.get("entity_id"))


if __name__ == "__main__":
    unittest.main()
