#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKILL_NAME="chain-monitor-report-analyst"
SKILL="${REPO_ROOT}/hermes_skills/${SKILL_NAME}/SKILL.md"
TELEGRAM_REF="${REPO_ROOT}/hermes_skills/${SKILL_NAME}/references/telegram_control.md"
CATALOG="${REPO_ROOT}/docs/hermes_telegram_command_catalog.md"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_text() {
  local pattern="$1"
  local label="$2"
  if ! grep -Fq -- "$pattern" "$SKILL"; then
    fail "missing ${label}: ${pattern}"
  fi
}

require_file_text() {
  local path="$1"
  local pattern="$2"
  local label="$3"
  if ! grep -Fq -- "$pattern" "$path"; then
    fail "missing ${label} in ${path#"$REPO_ROOT"/}: ${pattern}"
  fi
}

for path in "$SKILL" "$TELEGRAM_REF" "$CATALOG"; do
  [[ -f "$path" ]] || fail "required file not found: ${path#"$REPO_ROOT"/}"
done

first_line="$(sed -n '1p' "$SKILL")"
[[ "$first_line" == "---" ]] || fail "YAML frontmatter must start on line 1"

for pattern in \
  "name: chain-monitor-report-analyst" \
  "description:" \
  "version:" \
  "platforms: [linux]" \
  "## Mission" \
  "## When to use / 何时使用" \
  "## Source Priority" \
  "## 中文 Telegram Command Router - Mandatory" \
  "## Long Running Commands Must Be Submitted As Jobs" \
  "## 中文 Telegram 手动控制菜单" \
  "## Approved Commands" \
  "## Hermes Gateway / Telegram Control Rules" \
  "## Telegram 中文命令示例" \
  "## Data Integrity Check" \
  "## Learning Review" \
  "## Candidate Coverage Diagnosis" \
  "## LP Signal Diagnosis" \
  "## Not approved / Forbidden from Telegram" \
  "## 每日中文 Telegram 工作流" \
  "## Output Style" \
  "## Security Rules" \
  "## Final Checklist"
do
  require_text "$pattern" "required section/content"
done

for term in \
  "Do not provide direct trading" \
  "position size, leverage, take-profit, or stop-loss" \
  ".env" \
  "Telegram token" \
  "RPC" \
  "API key" \
  "private address" \
  "raw rows" \
  "archive" \
  "CANDIDATE" \
  "VERIFIED" \
  "不提供直接交易建议" \
  "不给交易建议" \
  "不得输出地址簿" \
  "不得输出 token/RPC/API key"
do
  require_text "$term" "security term"
done

for term in \
  "references/telegram_control.md" \
  "中文命令必须先被规范化到固定意图" \
  "chain-monitor-cn-router" \
  "~/.hermes/bin" \
  "/reset" \
  "install_hermes_skill.sh" \
  "hermes_cm_cn_router.py" \
  "router-only" \
  "不得先运行 date" \
  "不运行 date" \
  "不得运行 TZ=Asia/Shanghai date" \
  "不把 昨天 转日期" \
  "不得解析昨天" \
  "不得因为报告缺失就自动生成日报" \
  "不得因为报告缺失自动生成日报" \
  "不得自动生成日报" \
  "不得直接调用 make" \
  "不得直接调用 hermes_cm_ops.sh 的 dated operations" \
  "Telegram 中文请求不得直接调用 hermes_cm_ops.sh report/analyze/digest/close" \
  "不得从中文 Telegram 请求直接调用 raw shell" \
  "不得同步执行" \
  "job_id" \
  "任务状态" \
  "查看结果" \
  "诊断任务" \
  "输出默认脱敏" \
  "数据库瘦身预检" \
  "db-slim-dry-run"
do
  require_text "$term" "Chinese Telegram router term"
done

for example in \
  "命令提示" \
  "系统体检" \
  "监听器体检" \
  "锁状态" \
  "标准日报流程2026-05-01" \
  "任务状态cmjob_" \
  "查看结果cmjob_" \
  "诊断任务cmjob_" \
  "分析报告2026-05-01" \
  "检查回放2026-05-01" \
  "数据质量2026-05-01" \
  "数据完整性检查2026-05-01" \
  "Profile复盘2026-05-01" \
  "Blocker复盘2026-05-01" \
  "Shadow复盘2026-05-01" \
  "学习复盘2026-05-04" \
  "CANDIDATE覆盖诊断2026-05-04" \
  "Outcome闭环诊断2026-05-04" \
  "LP诊断2026-05-04" \
  "空间检查" \
  "数据库瘦身预检" \
  "归档压缩预检2026-05-01" \
  "周复盘2026-04-27到2026-05-03" \
  "生成日报YYYY-MM-DD" \
  "深度分析报告YYYY-MM-DD" \
  "生成摘要YYYY-MM-DD 快速" \
  "生成摘要YYYY-MM-DD 深度" \
  "/chain-monitor-report-analyst 命令提示" \
  "/chain-monitor-report-analyst 标准日报流程2026-05-01"
do
  require_text "$example" "Chinese command example"
done

require_file_text "$TELEGRAM_REF" "# Telegram 中文控制参考" "Telegram control reference title"
require_file_text "$TELEGRAM_REF" "所有中文 Telegram 命令必须先经过" "Telegram mandatory router reference"
require_file_text "$TELEGRAM_REF" "中文输入 -> router intent -> cm_ops wrapper argv" "Telegram command mapping reference"
require_file_text "$CATALOG" "# Chain Monitor Telegram 中文命令目录" "command catalog title"

APPROVED_BLOCK="$(awk '
  /^## Approved Commands$/ {
    in_section = 1
    next
  }
  in_section && /^## / {
    in_section = 0
  }
  in_section {
    print
  }
' "$SKILL")"

[[ -n "$APPROVED_BLOCK" ]] || fail "Approved Commands section is empty"

for command in \
  "~/.hermes/bin/chain-monitor-cn-router --text \"<中文命令>\" --execute --platform telegram" \
  "~/.hermes/bin/chain-monitor-cn-router --stdin --execute --platform telegram" \
  "~/.hermes/bin/chain-monitor-cn-router --text \"<中文命令>\" --dry-run --platform telegram" \
  "./scripts/hermes_cm_cn_router.py --text \"<中文命令>\" --execute --platform telegram" \
  "./scripts/hermes_cm_cn_router.py --stdin --execute --platform telegram" \
  "./scripts/hermes_cm_cn_router.py --text \"<中文命令>\" --dry-run --platform telegram" \
  "./scripts/hermes_cm_ops.sh command-menu" \
  "./scripts/hermes_cm_ops.sh lock-status" \
  "./scripts/hermes_cm_ops.sh system-health" \
  "./scripts/hermes_cm_ops.sh listener-health" \
  "./scripts/hermes_cm_ops.sh submit-daily-flow --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh data-integrity --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh learning-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh candidate-coverage --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh daily-report-schema-check --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh outcome-diagnose --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh lp-diagnose --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh submit-space-check" \
  "./scripts/hermes_cm_ops.sh space-fast" \
  "./scripts/hermes_cm_ops.sh db-size-diagnose" \
  "./scripts/hermes_cm_ops.sh db-slim-dry-run" \
  "./scripts/hermes_cm_ops.sh submit-archive-compress-check --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh submit-weekly-review --start YYYY-MM-DD --end YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh job-status --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-result --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-log --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-diagnose --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-list" \
  "./scripts/hermes_cm_ops.sh job-cancel --job-id JOB_ID --confirm"
do
  grep -Fq -- "$command" <<<"$APPROVED_BLOCK" || fail "Approved Commands missing: ${command}"
done

APPROVED_TELEGRAM_BLOCK="$(awk '
  /Telegram 中文请求允许：/ {
    in_section = 1
    next
  }
  in_section && /Local repo fallback only/ {
    in_section = 0
  }
  in_section {
    print
  }
' "$SKILL")"

[[ -n "$APPROVED_TELEGRAM_BLOCK" ]] || fail "approved Telegram launcher block is empty"

for forbidden in \
  "make run" \
  "make run-research" \
  "make db-compact-execute" \
  "operational payload export execute" \
  "make db-vacuum" \
  "make db-prune-execute" \
  "delete DB" \
  "修改地址簿" \
  "make archive-compress-date" \
  "make daily-close COMPRESS=YES" \
  "systemctl" \
  "pkill" \
  "kill -9" \
  "rm -rf" \
  "bash -c" \
  "sh -c" \
  "python -c" \
  "curl | sh" \
  "wget | sh"
do
  if grep -Fq -- "$forbidden" <<<"$APPROVED_TELEGRAM_BLOCK"; then
    fail "forbidden command in approved Telegram launcher block: ${forbidden}"
  fi
done

for term in \
  "Hermes digest / Telegram output must be redacted by default." \
  "Do not output unredacted EVM addresses" \
  "Do not output unredacted transaction hashes" \
  "Telegram 中文 control must call only \`~/.hermes/bin/chain-monitor-cn-router\`" \
  "local repo fallback may call \`./scripts/hermes_cm_cn_router.py\`" \
  "Hermes gateway" \
  "TELEGRAM_ALLOWED_USERS" \
  "GATEWAY_ALLOWED_USERS" \
  "GATEWAY_ALLOW_ALL_USERS" \
  "approvals.mode" \
  "/yolo" \
  "telegram.require_mention" \
  "ops_audit.ndjson" \
  "redaction / 脱敏" \
  "router-only / 只能调用 launcher/router" \
  "每日收尾是 high-risk" \
  "每日收尾不属于每日默认流程" \
  "我确认压缩" \
  "每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法" \
  "不要建议用户使用 \`--allow-today\`"
do
  require_text "$term" "required safety/control term"
done

code_fence_count="$(grep -c '^```' "$SKILL" || true)"
if (( code_fence_count % 2 != 0 )); then
  fail "Markdown code block fence count is odd: ${code_fence_count}"
fi

echo "OK: Hermes skill validation passed"
