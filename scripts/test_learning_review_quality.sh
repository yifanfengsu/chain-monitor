#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TEST_DATE="2099-12-27"
REPORT="${REPO_ROOT}/reports/daily/daily_report_${TEST_DATE}.json"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/learning_review_quality.XXXXXX")"

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
  "logical_date": "2099-12-27",
  "data_quality_summary": {
    "data_quality_status": "valid",
    "zero_activity_day": false
  },
  "run_overview": {
    "lp_signal_rows": 97
  },
  "data_source_summary": {
    "row_counts": {
      "delivery_audit": 6496,
      "telegram_deliveries": 47
    }
  },
  "trade_replay_summary": {
    "replay_source": "persisted",
    "replay_scope": "full",
    "replay_count": 96,
    "valid_replay_count": 96,
    "avg_net_pnl_bps": -21.24,
    "suppressed_avg_net_pnl_bps": -19.95,
    "replay_coverage_rate_candidate": 0.0
  },
  "trade_replay_profile_summary": {
    "blocker_grade_negative_profiles": [
      {
        "profile_key": "ETH|LONG|confirm",
        "valid_sample_count": 48,
        "avg_net_pnl_bps": -21.67
      }
    ],
    "high_confidence_positive_profiles": []
  },
  "trade_opportunity_summary": {
    "opportunity_candidate_count": 0,
    "candidate_outcome_completion_rate": 0.0
  },
  "shadow_funnel_summary": {
    "shadow_candidate_count": 0,
    "shadow_verified_count": 0,
    "shadow_reason_distribution": {
      "score_below_shadow_candidate": 43
    }
  },
  "telegram_suppression_summary": {
    "messages_before_suppression_estimate": 97,
    "messages_after_suppression_actual": 47,
    "telegram_suppression_ratio": 0.515,
    "high_value_suppressed_count": 0
  },
  "candidate_frontier_summary": {
    "available": true,
    "opportunities_total": 97,
    "none_count": 95,
    "blocked_count": 2,
    "candidate_count": 0,
    "verified_count": 0,
    "near_candidate_count": 0,
    "near_candidate_replay_count": 0,
    "near_candidate_avg_net_pnl_bps": null,
    "top_near_candidate_blockers": {},
    "top_missing_requirements": {},
    "diagnosis": "gate_closed_because_quality_low"
  },
  "candidate_coverage_summary": {
    "available": true,
    "signals_total": 648,
    "trade_opportunities_total": 97,
    "none_count": 95,
    "blocked_count": 2,
    "candidate_count": 0,
    "verified_count": 0,
    "replay_examples_total": 96,
    "replay_examples_with_opportunity": 96,
    "replay_examples_status_candidate": 0,
    "near_candidate_count": 0,
    "near_candidate_avg_net_pnl_bps": null,
    "diagnosis": "gate_closed_because_quality_low"
  },
  "outcome_diagnosis_summary": {
    "available": true,
    "signals_total": 648,
    "trade_opportunities_total": 97,
    "outcomes_total": 291,
    "outcomes_completed": 291,
    "opportunity_outcomes_total": 291,
    "opportunity_outcomes_completed": 0,
    "opportunity_outcomes_pending": 291,
    "opportunity_outcomes_past_due_pending": 291,
    "signals_to_outcomes_match_rate": 0.1497,
    "opportunities_to_opportunity_outcomes_match_rate": 1.0,
    "opportunities_to_replay_examples_match_rate": 0.9897,
    "diagnosis": "opportunity_outcomes_pending_not_settled",
    "recommended_next_check": "outcome-catchup dry-run"
  },
  "lp_signal_summary": {
    "available": true,
    "lp_signal_rows": 97,
    "delivered_count": 47,
    "suppressed_count": 50,
    "suppression_rate": 0.515,
    "lp_like_signals_sqlite": 1261
  },
  "lp_stage_summary": {
    "available": true,
    "by_stage": {
      "confirm": 55,
      "exhaustion_risk": 42,
      "prealert": 0,
      "unknown": 0
    },
    "unknown_rate": 0.0
  },
  "clmm_summary": {
    "available": true,
    "clmm_like_rows": 1261
  },
  "lp_suppression_summary": {
    "available": true,
    "total": 6496,
    "delivered": 75,
    "suppressed": 6421,
    "suppression_rate": 0.988,
    "by_reason": {
      "gate/lp_noise_filtered": 2866
    }
  },
  "lp_suppression_replay_summary": {
    "available": true,
    "overall_suppressed_avg_net_pnl_bps": -19.95,
    "diagnosis": "suppression_seems_correct",
    "by_reason": []
  },
  "major_coverage_summary": {
    "missing_major_pairs": ["BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC"]
  }
}
JSON

HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" \
HERMES_OPS_ROUTER_OK=1 \
  "$OPS" learning-review --date "$TEST_DATE" >"$TMP_DIR/learning.out"

require_out "今日结论"
require_out "明天只建议改一个点"
require_out "CANDIDATE=0=是"
require_out "LP mapping 状态"
require_out "opportunity_outcomes 未结算"
require_out "P0排查项=opportunity_outcomes 未结算"
require_out "明天只建议改一个点：修 outcome catchup / opportunity_outcomes 结算"
require_out "为什么系统没有提升"
require_out "系统今天学到了什么"

for forbidden in "买入" "卖出" "开仓" "止损" "止盈" "杠杆" "仓位"; do
  forbid_out "$forbidden"
done

echo "OK: learning-review quality test passed"
