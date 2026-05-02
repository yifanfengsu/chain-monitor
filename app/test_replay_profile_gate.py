from __future__ import annotations

import unittest

from replay_profile_gate import (
    evaluate_replay_profile_gate,
    is_replay_profile_negative_blocker,
    is_sampled_negative_profile,
    local_absorption_quality_low,
    profile_key_unknown_dimensions,
    profile_unknown_diagnostics,
    repair_profile_key,
    replay_profile_summary,
)
from trade_action import infer_trade_action


NEGATIVE_PROFILE_KEY = "ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low"


class ReplayProfileGateTests(unittest.TestCase):
    def test_high_confidence_negative_profile_blocks(self) -> None:
        stat = {
            "profile_key": NEGATIVE_PROFILE_KEY,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        decision = evaluate_replay_profile_gate(stat)

        self.assertTrue(is_replay_profile_negative_blocker(stat))
        self.assertEqual("block_profile", decision["action"])
        self.assertEqual("replay_profile_negative", decision["blocker"])

    def test_replay_recommended_negative_profile_blocks_even_with_some_clean_followthrough(self) -> None:
        stat = {
            "profile_key": "ETH|SHORT|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low",
            "valid_sample_count": 18,
            "avg_net_pnl_bps": -18.49,
            "win_rate": 0.1111,
            "clean_followthrough_rate": 0.1111,
            "chop_rate": 0.8889,
            "recommended_action": "block_profile",
        }

        decision = evaluate_replay_profile_gate(stat)

        self.assertTrue(is_replay_profile_negative_blocker(stat))
        self.assertEqual("block_profile", decision["action"])
        self.assertEqual("replay_profile_negative", decision["blocker"])

    def test_low_sample_positive_is_research_only(self) -> None:
        decision = evaluate_replay_profile_gate(
            {
                "profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|basis_normal|quality_high",
                "valid_sample_count": 1,
                "avg_net_pnl_bps": 70.38,
                "win_rate": 1.0,
                "clean_followthrough_rate": 1.0,
                "chop_rate": 0.0,
            }
        )

        self.assertEqual("needs_more_samples", decision["action"])
        self.assertEqual("", decision["blocker"])
        self.assertEqual("positive_profile_needs_more_samples", decision["research_hint"])

    def test_local_absorption_quality_low_risk_downgrade(self) -> None:
        payload = infer_trade_action(
            {
                "direction": "long",
                "lp_alert_stage": "confirm",
                "lp_confirm_scope": "local_confirm",
                "lp_confirm_quality": "unconfirmed_confirm",
                "lp_absorption_context": "local_buy_pressure_absorption",
                "market_context_source": "live_public",
                "alert_relative_timing": "leading",
                "asset_case_quality_score": 0.42,
                "pair_quality_score": 0.44,
                "pool_quality_score": 0.46,
                "major_asset": True,
                "market_direction_aligned": True,
                "lp_conflict_resolution": "no_conflict",
            }
        )

        self.assertTrue(local_absorption_quality_low({"lp_absorption_context": "local_buy_pressure_absorption", "asset_case_quality_score": 0.42}))
        self.assertEqual("DO_NOT_CHASE_LONG", payload["trade_action_key"])
        self.assertIn("replay_no_edge_context", payload["trade_action_blockers"])
        self.assertEqual("heuristic_quality_low", payload["lp_market_read_evidence"])
        self.assertNotIn("历史 replay", payload["trade_action_reason"])

    def test_local_absorption_quality_high_not_flat_do_not_chase(self) -> None:
        payload = infer_trade_action(
            {
                "direction": "long",
                "lp_alert_stage": "confirm",
                "lp_confirm_scope": "local_confirm",
                "lp_confirm_quality": "unconfirmed_confirm",
                "lp_absorption_context": "local_buy_pressure_absorption",
                "market_context_source": "live_public",
                "alert_relative_timing": "leading",
                "asset_case_quality_score": 0.90,
                "pair_quality_score": 0.90,
                "pool_quality_score": 0.90,
                "major_asset": True,
                "market_direction_aligned": True,
                "lp_conflict_resolution": "no_conflict",
            }
        )

        self.assertNotEqual("DO_NOT_CHASE_LONG", payload["trade_action_key"])
        self.assertIn(payload["trade_action_key"], {"LONG_BIAS_OBSERVE", "WAIT_CONFIRMATION"})

    def test_profile_key_unknown_diagnostics_and_backfill(self) -> None:
        repaired = repair_profile_key(
            "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
            {
                "lp_alert_stage": "confirm",
                "lp_confirm_scope": "local_confirm",
            },
        )

        self.assertEqual(NEGATIVE_PROFILE_KEY, repaired)
        self.assertNotIn("lp_stage", profile_key_unknown_dimensions(repaired))

        diagnostics = profile_unknown_diagnostics(
            [
                {
                    "profile_key": "ETH|SHORT|unknown|sweep_confirmed|unknown|local_absorption|major|basis_normal|quality_low",
                    "valid_sample_count": 18,
                    "avg_net_pnl_bps": -18.49,
                }
            ]
        )

        self.assertGreater(diagnostics["profile_unknown_field_rate"], 0.0)
        self.assertEqual(1, diagnostics["unknown_by_dimension"]["lp_stage"])
        self.assertEqual(1, diagnostics["unknown_by_dimension"]["market_timing"])
        self.assertIn("missing_lp_stage", diagnostics["unknown_missing_sources"])
        self.assertNotIn("missing_lp_alert_stage", diagnostics["unknown_missing_sources"])
        self.assertNotIn("missing_trade_action_stage", diagnostics["unknown_missing_sources"])
        self.assertNotIn("missing_sweep_phase", diagnostics["unknown_missing_sources"])
        self.assertEqual(
            "asset|side|lp_stage|sweep_phase|market_timing|absorption_context|asset_class|basis_bucket|quality_bucket",
            diagnostics["example_profile_key_format"],
        )
        self.assertIn("lp_stage", diagnostics["dimension_names"])
        self.assertEqual(1.0, diagnostics["unknown_rate_by_dimension"]["lp_stage"])

    def test_unknown_diagnostics_reports_all_lp_stage_sources_when_no_stage_or_sweep_exists(self) -> None:
        diagnostics = profile_unknown_diagnostics(
            [
                {
                    "profile_key": "ETH|LONG|unknown|unknown|leading|local_absorption|major|basis_normal|quality_low",
                    "valid_sample_count": 2,
                    "avg_net_pnl_bps": -1.0,
                }
            ]
        )

        missing = diagnostics["unknown_missing_sources"]
        self.assertEqual(1, missing["missing_lp_stage"])
        self.assertEqual(1, missing["missing_lp_alert_stage"])
        self.assertEqual(1, missing["missing_trade_action_stage"])
        self.assertEqual(1, missing["missing_sweep_phase"])

    def test_profile_key_lp_stage_backfills_from_real_lp_alert_stage(self) -> None:
        repaired = repair_profile_key(
            "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
            {
                "lp_alert_stage": "local_confirm",
            },
        )

        self.assertEqual(NEGATIVE_PROFILE_KEY, repaired)
        self.assertNotIn("unknown", repaired.split("|"))

    def test_profile_key_lp_stage_backfills_from_real_lp_stage(self) -> None:
        repaired = repair_profile_key(
            "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
            {
                "lp_stage": "prealert",
            },
        )

        self.assertEqual(
            "ETH|LONG|prealert|confirm|leading|local_absorption|major|basis_normal|quality_low",
            repaired,
        )
        self.assertNotIn("lp_stage", profile_key_unknown_dimensions(repaired))

    def test_profile_key_lp_stage_backfills_from_signal_json_lp_stage_context(self) -> None:
        repaired = repair_profile_key(
            "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
            {
                "signal_json": {
                    "lp_stage_context": {
                        "lp_alert_stage": "prealert",
                    }
                },
            },
        )

        self.assertEqual(
            "ETH|LONG|prealert|confirm|leading|local_absorption|major|basis_normal|quality_low",
            repaired,
        )

    def test_unknown_diagnostics_reports_missing_lp_stage_with_sweep_phase_present(self) -> None:
        diagnostics = profile_unknown_diagnostics(
            [
                {
                    "profile_key": "ETH|LONG|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_low",
                    "valid_sample_count": 16,
                    "avg_net_pnl_bps": -21.16,
                    "sweep_phase": "exhaustion_risk",
                }
            ]
        )

        missing = diagnostics["top_unknown_profiles"][0]["missing_sources"]
        self.assertEqual(["missing_lp_stage"], missing)
        self.assertEqual(1, diagnostics["unknown_missing_sources"]["missing_lp_stage"])

    def test_profile_key_repair_does_not_fill_lp_stage_from_only_sweep_phase(self) -> None:
        repaired = repair_profile_key(
            "ETH|LONG|unknown|unknown|leading|local_absorption|major|basis_normal|quality_low",
            {
                "sweep_phase": "exhaustion_risk",
            },
        )

        self.assertEqual(
            "ETH|LONG|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_low",
            repaired,
        )
        self.assertIn("lp_stage", profile_key_unknown_dimensions(repaired))

    def test_profile_key_repair_does_not_fill_lp_stage_from_empty_fields(self) -> None:
        repaired = repair_profile_key(
            "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
            {
                "lp_alert_stage": "",
                "lp_alert_stage_candidate": "unknown",
                "lp_stage": "",
                "trade_action_stage": "",
            },
        )

        self.assertIn("|unknown|confirm|", repaired)

    def test_replay_profile_summary_layers_negative_and_low_sample_positive(self) -> None:
        summary = replay_profile_summary(
            [
                {
                    "profile_key": NEGATIVE_PROFILE_KEY,
                    "valid_sample_count": 48,
                    "avg_net_pnl_bps": -21.67,
                    "win_rate": 0.0,
                    "clean_followthrough_rate": 0.0,
                    "chop_rate": 0.9583,
                },
                {
                    "profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|basis_normal|quality_high",
                    "valid_sample_count": 1,
                    "avg_net_pnl_bps": 70.38,
                    "win_rate": 1.0,
                    "clean_followthrough_rate": 1.0,
                    "chop_rate": 0.0,
                },
            ]
        )

        self.assertEqual(1, summary["replay_profile_blocker_count"])
        self.assertEqual(NEGATIVE_PROFILE_KEY, summary["sampled_negative_profiles"][0]["profile_key"])
        self.assertEqual(NEGATIVE_PROFILE_KEY, summary["blocker_grade_negative_profiles"][0]["profile_key"])
        self.assertEqual(NEGATIVE_PROFILE_KEY, summary["high_confidence_negative_profiles"][0]["profile_key"])
        self.assertEqual(70.38, summary["low_sample_positive_profiles"][0]["avg_net_pnl_bps"])
        self.assertEqual([], summary["high_confidence_positive_profiles"])

        low_sample = summary["low_sample_positive_profiles"][0]
        self.assertEqual("needs_more_samples", evaluate_replay_profile_gate(low_sample)["action"])

    def test_sampled_negative_is_not_always_blocker_grade(self) -> None:
        sampled_only = {
            "profile_key": "ETH|LONG|broader_confirm|confirm|leading|broader_absorption|major|basis_normal|quality_medium",
            "valid_sample_count": 20,
            "avg_net_pnl_bps": -0.5,
            "clean_followthrough_rate": 0.40,
            "chop_rate": 0.20,
        }
        blocker_grade = {
            "profile_key": NEGATIVE_PROFILE_KEY,
            "valid_sample_count": 20,
            "avg_net_pnl_bps": -21.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.95,
        }

        summary = replay_profile_summary([sampled_only, blocker_grade])

        self.assertTrue(is_sampled_negative_profile(sampled_only))
        self.assertFalse(is_replay_profile_negative_blocker(sampled_only))
        self.assertTrue(is_replay_profile_negative_blocker(blocker_grade))
        self.assertIn(
            "ETH|LONG|broader_confirm|confirm|leading|broader_absorption|major|basis_normal|quality_medium",
            [item["profile_key"] for item in summary["sampled_negative_profiles"]],
        )
        self.assertIn(NEGATIVE_PROFILE_KEY, [item["profile_key"] for item in summary["sampled_negative_profiles"]])
        self.assertEqual([NEGATIVE_PROFILE_KEY], [item["profile_key"] for item in summary["blocker_grade_negative_profiles"]])


if __name__ == "__main__":
    unittest.main()
