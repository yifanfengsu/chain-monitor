from __future__ import annotations

import unittest

from opportunity_status_explanation import derive_opportunity_status


class OpportunityDerivedStatusTests(unittest.TestCase):
    def test_none_with_primary_hard_blocker_is_blocked_like(self) -> None:
        payload = derive_opportunity_status({"status": "NONE", "primary_hard_blocker": "data_gap"})

        self.assertEqual("NONE", payload["raw_status"])
        self.assertEqual("BLOCKED_LIKE", payload["derived_status"])
        self.assertTrue(payload["has_hard_blocker"])

    def test_none_with_replay_profile_negative_is_replay_negative(self) -> None:
        payload = derive_opportunity_status({"status": "NONE", "blockers_json": '["replay_profile_negative"]'})

        self.assertEqual("REPLAY_NEGATIVE", payload["derived_status"])
        self.assertEqual("replay_profile_negative", payload["derived_reason"])

    def test_none_with_low_quality_is_low_quality(self) -> None:
        payload = derive_opportunity_status({"status": "NONE", "primary_blocker": "low_quality"})

        self.assertEqual("LOW_QUALITY", payload["derived_status"])

    def test_none_with_do_not_chase_is_do_not_chase(self) -> None:
        payload = derive_opportunity_status({"status": "NONE", "primary_blocker": "do_not_chase_long"})

        self.assertEqual("DO_NOT_CHASE", payload["derived_status"])

    def test_none_with_local_absorption_is_local_absorption(self) -> None:
        payload = derive_opportunity_status({"status": "NONE", "blockers_json": '["local_absorption"]'})

        self.assertEqual("LOCAL_ABSORPTION", payload["derived_status"])

    def test_none_with_gate_failure_is_gate_failed(self) -> None:
        payload = derive_opportunity_status(
            {
                "status": "NONE",
                "opportunity_gate_required": 1,
                "opportunity_gate_passed": 0,
                "opportunity_gate_failure_reason": "opportunity_gate_rejected",
            }
        )

        self.assertEqual("GATE_FAILED", payload["derived_status"])
        self.assertTrue(payload["has_gate_failure"])

    def test_none_with_would_have_been_candidate_is_near_candidate(self) -> None:
        payload = derive_opportunity_status({"status": "NONE", "would_have_been_candidate": 1})

        self.assertEqual("NEAR_CANDIDATE", payload["derived_status"])
        self.assertTrue(payload["is_near_candidate"])

    def test_none_without_explanation_is_true_none(self) -> None:
        payload = derive_opportunity_status({"status": "NONE"})

        self.assertEqual("TRUE_NONE", payload["derived_status"])
        self.assertFalse(payload["has_blocker"])
        self.assertFalse(payload["has_gate_failure"])

    def test_candidate_and_verified_are_not_overwritten(self) -> None:
        candidate = derive_opportunity_status({"status": "CANDIDATE", "primary_hard_blocker": "data_gap"})
        verified = derive_opportunity_status({"status": "VERIFIED", "primary_blocker": "low_quality"})

        self.assertEqual("CANDIDATE", candidate["derived_status"])
        self.assertEqual("VERIFIED", verified["derived_status"])


if __name__ == "__main__":
    unittest.main()
