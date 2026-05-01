#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKILL_NAME="chain-monitor-report-analyst"
SKILL="${REPO_ROOT}/hermes_skills/${SKILL_NAME}/SKILL.md"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_text() {
  local pattern="$1"
  local label="$2"
  if ! grep -Fq "${pattern}" "${SKILL}"; then
    fail "missing ${label}: ${pattern}"
  fi
}

[[ -f "${SKILL}" ]] || fail "skill file not found: ${SKILL}"

first_line="$(sed -n '1p' "${SKILL}")"
[[ "${first_line}" == "---" ]] || fail "YAML frontmatter must start on line 1"

require_text "name: chain-monitor-report-analyst" "frontmatter name"
require_text "description:" "frontmatter description"
require_text "version:" "frontmatter version"
require_text "platforms: [linux]" "frontmatter platforms"

for section in \
  "## Mission" \
  "## Source Priority" \
  "## Approved Commands" \
  "## Telegram Output Format" \
  "## Security Rules" \
  "## Final Checklist"
do
  require_text "${section}" "required section"
done

for term in \
  "Do not provide direct trading" \
  ".env" \
  "Telegram token" \
  "RPC" \
  "raw rows" \
  "archive" \
  "CANDIDATE" \
  "VERIFIED"
do
  require_text "${term}" "security term"
done

for term in \
  "## Date-specific Analysis" \
  "分析 YYYY-MM-DD 的报告" \
  "hermes_cm_ops.sh analyze"
do
  require_text "${term}" "date-specific analysis term"
done

code_fence_count="$(grep -c '^```' "${SKILL}" || true)"
if (( code_fence_count % 2 != 0 )); then
  fail "Markdown code block fence count is odd: ${code_fence_count}"
fi

echo "OK: Hermes skill validation passed"
