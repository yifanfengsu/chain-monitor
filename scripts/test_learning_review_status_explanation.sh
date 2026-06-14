#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TEST_DATE="2099-12-26"
REPORT="${REPO_ROOT}/reports/daily/daily_report_${TEST_DATE}.json"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/learning_review_status.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
  rm -f "$REPORT"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

require_out() {
  local pattern="$1"
  grep -Fq -- "$pattern" "$TMP_DIR/learning.out" || fail "learning-review output missing: ${pattern}"
}

forbid_out() {
  local pattern="$1"
  if grep -Fq -- "$pattern" "$TMP_DIR/learning.out"; then
    fail "learning-review output contains forbidden text: ${pattern}"
  fi
}

mkdir -p "${REPO_ROOT}/reports/daily"
cat >"$REPORT" <<'JSON'
{
  "logical_date": "2099-12-26",
  "data_quality_summary": {
    "data_quality_status": "valid",
    "zero_activity_day": false
  },
  "run_overview": {
    "lp_signal_rows": 108
  },
  "data_source_summary": {
    "row_counts": {
      "delivery_audit": 108,
      "telegram_deliveries": 0
    }
  },
  "trade_replay_summary": {
    "replay_source": "persisted",
    "replay_scope": "full",
    "replay_count": 105,
    "valid_replay_count": 105,
    "avg_net_pnl_bps": -12.5,
    "suppressed_avg_net_pnl_bps": -10.2,
    "replay_coverage_rate_candidate": 0
  },
  "trade_replay_profile_summary": {
    "blocker_grade_negative_profiles": [
      {
        "profile_key": "ETH|LONG|fixture",
        "valid_sample_count": 48,
        "avg_net_pnl_bps": -21.67
      }
    ],
    "high_confidence_positive_profiles": []
  },
  "trade_opportunity_summary": {
    "opportunity_candidate_count": 0,
    "opportunity_verified_count": 0,
    "candidate_outcome_completion_rate": 0.0,
    "opportunity_status_explained_summary": {
      "total": 108,
      "raw_status_distribution": {
        "NONE": 108
      },
      "derived_status_distribution": {
        "NONE": 28,
        "BLOCKED_LIKE": 80,
        "CANDIDATE": 0,
        "VERIFIED": 0
      },
      "none_count": 108,
      "true_none_count": 28,
      "none_with_blockers_count": 80,
      "none_with_hard_blockers_count": 80,
      "none_with_primary_blocker_count": 80,
      "blocked_like_none_count": 80,
      "top_none_reasons": {
        "score_below_shadow_candidate": 28
      },
      "top_blocker_like_reasons": {
        "blocked_by_replay_profile_negative": 50,
        "blocked_by_low_quality": 30
      },
      "top_gate_failure_reasons": {},
      "status_assignment_warning": "raw_status_all_none_but_blocked_like_present",
      "status_assignment_diagnosis": "raw_none_contains_explainable_opportunities"
    }
  },
  "opportunity_status_explained_summary": {
    "total": 108,
    "raw_status_distribution": {
      "NONE": 108
    },
    "derived_status_distribution": {
      "NONE": 28,
      "BLOCKED_LIKE": 80,
      "CANDIDATE": 0,
      "VERIFIED": 0
    },
    "none_count": 108,
    "true_none_count": 28,
    "none_with_blockers_count": 80,
    "none_with_hard_blockers_count": 80,
    "none_with_primary_blocker_count": 80,
    "blocked_like_none_count": 80,
    "top_none_reasons": {
      "score_below_shadow_candidate": 28
    },
    "top_blocker_like_reasons": {
      "blocked_by_replay_profile_negative": 50,
      "blocked_by_low_quality": 30
    },
    "top_gate_failure_reasons": {},
    "status_assignment_warning": "raw_status_all_none_but_blocked_like_present",
    "status_assignment_diagnosis": "raw_none_contains_explainable_opportunities"
  },
  "shadow_funnel_summary": {
    "shadow_candidate_count": 0,
    "shadow_verified_count": 0,
    "shadow_reason_distribution": {
      "score_below_shadow_candidate": 28
    }
  },
  "telegram_suppression_summary": {
    "messages_before_suppression_estimate": 0,
    "messages_after_suppression_actual": 0,
    "telegram_suppression_ratio": 0,
    "high_value_suppressed_count": 0
  },
  "candidate_frontier_summary": {
    "near_candidate_count": 0,
    "near_candidate_replay_count": 0,
    "near_candidate_avg_net_pnl_bps": null,
    "diagnosis": "gate_closed_because_quality_low"
  },
  "candidate_coverage_summary": {
    "available": true,
    "signals_total": 555,
    "trade_opportunities_total": 108,
    "none_count": 108,
    "blocked_count": 0,
    "candidate_count": 0,
    "verified_count": 0,
    "replay_examples_total": 105,
    "replay_examples_with_opportunity": 105,
    "replay_examples_status_candidate": 0,
    "near_candidate_count": 0,
    "near_candidate_avg_net_pnl_bps": null,
    "diagnosis": "gate_closed_because_quality_low"
  },
  "outcome_diagnosis_summary": {
    "available": true,
    "signals_total": 555,
    "trade_opportunities_total": 108,
    "outcomes_total": 324,
    "outcomes_completed": 324,
    "opportunity_outcomes_total": 324,
    "opportunity_outcomes_completed": 324,
    "opportunity_outcomes_pending": 0,
    "opportunity_outcomes_past_due_pending": 0,
    "signals_to_outcomes_match_rate": 0.1946,
    "opportunities_to_opportunity_outcomes_match_rate": 1.0,
    "opportunities_to_replay_examples_match_rate": 0.9722,
    "diagnosis": "ok",
    "recommended_next_check": "continue monitoring"
  },
  "lp_signal_summary": {
    "available": true,
    "lp_signal_rows": 108,
    "lp_like_signals_sqlite": 108
  },
  "lp_stage_summary": {
    "available": true
  },
  "clmm_summary": {
    "available": true
  },
  "lp_suppression_summary": {
    "available": true,
    "suppression_rate": 0.4
  },
  "lp_suppression_replay_summary": {
    "available": true,
    "diagnosis": "suppression_seems_correct"
  },
  "lp_suppression_sample_replay_summary": {
    "available": true,
    "diagnosis": "early_suppression_seems_correct",
    "sample_limit_per_reason": 100,
    "valid_replay_count": 20,
    "by_reason": []
  }
}
JSON

cd "$REPO_ROOT"
HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" \
HERMES_OPS_ROUTER_OK=1 \
  "$OPS" learning-review --date "$TEST_DATE" >"$TMP_DIR/learning.out"

require_out "CANDIDATE=0=是"
require_out "blocked_like_none_count=80"
require_out "gate_failed_none_count=0"
require_out "near_candidate=0"
require_out "没有 CANDIDATE"
require_out "true_none=28"
require_out "gate 保持合理"
require_out "不建议放宽 gate"
forbid_out "当日无候选机会"

echo "OK: learning-review status explanation test passed"
