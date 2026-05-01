#!/usr/bin/env bash
set -Eeuo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

SCRIPT="scripts/hermes_cm_ops.sh"

[[ -f "$SCRIPT" ]] || die "missing ${SCRIPT}"

bash -n "$SCRIPT"

required_patterns=(
  "report"
  "close"
  "health"
  "digest"
  "analyze"
  "--auto-build"
  "source_date_verified"
  "hermes_digest_input_"
  "--confirm-compress"
  "--allow-today"
  "timeout"
  "mktemp"
  "trap"
  "reports/hermes"
)

for pattern in "${required_patterns[@]}"; do
  if ! grep -Fq -- "$pattern" "$SCRIPT"; then
    die "missing required pattern in ${SCRIPT}: ${pattern}"
  fi
done

for forbidden in \
  "make run" \
  "make run-research"
do
  if grep -Fq -- "$forbidden" "$SCRIPT"; then
    die "forbidden command reference in ${SCRIPT}: ${forbidden}"
  fi
done

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck "$SCRIPT" "$0"
fi

echo "OK: hermes_cm_ops validation passed"
