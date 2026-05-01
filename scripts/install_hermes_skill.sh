#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKILL_NAME="chain-monitor-report-analyst"
SRC="${REPO_ROOT}/hermes_skills/${SKILL_NAME}/SKILL.md"
DEST_ROOT="${HERMES_SKILLS_DIR:-${HOME}/.hermes/skills}"
DEST_DIR="${DEST_ROOT}/${SKILL_NAME}"
DEST="${DEST_DIR}/SKILL.md"

if [[ ! -f "${SRC}" ]]; then
  echo "ERROR: source skill not found: ${SRC}" >&2
  exit 1
fi

mkdir -p "${DEST_DIR}"
cp "${SRC}" "${DEST}"

echo "Installed Hermes skill:"
echo "  ${DEST}"
echo
echo "Suggested checks:"
echo "  test -f \"${DEST}\" && sed -n '1,40p' \"${DEST}\""
echo "  hermes skills list | grep ${SKILL_NAME} || true"
echo
echo "Suggested usage:"
echo "  cd \"${REPO_ROOT}\""
echo "  hermes chat --skills ${SKILL_NAME} -q \"使用 chain-monitor-report-analyst 技能，检查 reports 目录并生成一份简短日报分析。不要修改代码，不要给交易建议。\""
