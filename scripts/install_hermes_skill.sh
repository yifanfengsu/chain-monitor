#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKILL_NAME="chain-monitor-report-analyst"
SRC_DIR="${REPO_ROOT}/hermes_skills/${SKILL_NAME}"
DEST_ROOT_RAW="${HERMES_SKILLS_DIR:-${HOME}/.hermes/skills}"
DEFAULT_SKILLS_ROOT="${HOME}/.hermes/skills"
BIN_DIR="${HOME}/.hermes/bin"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_text() {
  local path="$1"
  local pattern="$2"

  if ! grep -Fq -- "$pattern" "$path"; then
    fail "安装后验证失败：${path} 缺少 ${pattern}"
  fi
}

assert_safe_dir_value() {
  local value="$1"
  local label="$2"

  [[ -n "$value" ]] || fail "${label} 不能为空"
  [[ "$value" == /* ]] || fail "${label} 必须是绝对路径：${value}"
  case "$value" in
    "/"|"${HOME}"|"${HOME}/.hermes")
      fail "${label} 指向不安全目录：${value}"
      ;;
  esac
}

assert_safe_target() {
  local target="$1"
  local dest_root="$2"

  [[ -n "$target" ]] || fail "target 不能为空"
  case "$target" in
    "/"|"${HOME}"|"${HOME}/.hermes"|"${HOME}/.hermes/skills"|"$dest_root")
      fail "target 指向不安全目录：${target}"
      ;;
  esac

  if [[ "$target" != "${DEFAULT_SKILLS_ROOT}/"* && "$target" != "${dest_root}/"* ]]; then
    fail "target 不在预期 skills 目录下：${target}"
  fi
}

run_validation() {
  bash "${REPO_ROOT}/scripts/validate_hermes_skill.sh"
  bash "${REPO_ROOT}/scripts/validate_hermes_telegram_control.sh"
  if [[ -f "${REPO_ROOT}/scripts/validate_hermes_cm_ops.sh" ]]; then
    bash "${REPO_ROOT}/scripts/validate_hermes_cm_ops.sh"
  fi
}

copy_skill_dir() {
  local dest_root="$1"
  local dest_dir="$2"

  mkdir -p "$dest_root"

  if command -v rsync >/dev/null 2>&1; then
    mkdir -p "$dest_dir"
    rsync -a --delete "${SRC_DIR}/" "${dest_dir}/"
  else
    assert_safe_target "$dest_dir" "$dest_root"
    rm -rf "$dest_dir"
    mkdir -p "$(dirname "$dest_dir")"
    cp -a "$SRC_DIR" "$dest_dir"
  fi
}

write_launchers() {
  local router_launcher="${BIN_DIR}/chain-monitor-cn-router"
  local ops_launcher="${BIN_DIR}/chain-monitor-ops"

  mkdir -p "$BIN_DIR"

  cat >"$router_launcher" <<LAUNCHER
#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_ROOT="${REPO_ROOT}"
cd "\$REPO_ROOT"
exec "\$REPO_ROOT/scripts/hermes_cm_cn_router.py" "\$@"
LAUNCHER

  cat >"$ops_launcher" <<LAUNCHER
#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_ROOT="${REPO_ROOT}"
cd "\$REPO_ROOT"
exec "\$REPO_ROOT/scripts/hermes_cm_ops.sh" "\$@"
LAUNCHER

  chmod 700 "$router_launcher" "$ops_launcher"
}

verify_launcher() {
  local router_launcher="${BIN_DIR}/chain-monitor-cn-router"
  local tmp_dir
  local rc=0

  [[ -x "$router_launcher" ]] || fail "launcher 不存在或不可执行：~/.hermes/bin/chain-monitor-cn-router"

  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hermes_install_verify.XXXXXX")"
  trap 'rm -rf "$tmp_dir"' RETURN

  set +e
  "$router_launcher" --text "分析昨天的报告" --dry-run --platform telegram >"$tmp_dir/relative.out" 2>"$tmp_dir/relative.err"
  rc=$?
  set -e
  if [[ "$rc" -eq 0 ]]; then
    fail "相对日期 dry-run 应拒绝，但实际成功"
  fi
  grep -Fq "绝对日期" "$tmp_dir/relative.out" "$tmp_dir/relative.err" || fail "相对日期拒绝缺少：绝对日期"
  grep -Fq "YYYY-MM-DD" "$tmp_dir/relative.out" "$tmp_dir/relative.err" || fail "相对日期拒绝缺少：YYYY-MM-DD"

  "$router_launcher" --text "分析报告2026-05-01" --dry-run --platform telegram >"$tmp_dir/absolute.out" 2>"$tmp_dir/absolute.err"
  grep -Fq "analyze" "$tmp_dir/absolute.out" || fail "绝对日期 dry-run 缺少 analyze"
  grep -Fq -- "--date" "$tmp_dir/absolute.out" || fail "绝对日期 dry-run 缺少 --date"
  grep -Fq "2026-05-01" "$tmp_dir/absolute.out" || fail "绝对日期 dry-run 缺少 2026-05-01"
  grep -Fq -- "--mode" "$tmp_dir/absolute.out" || fail "绝对日期 dry-run 缺少 --mode"
  grep -Fq "fast" "$tmp_dir/absolute.out" || fail "绝对日期 dry-run 缺少 fast"

  "$router_launcher" --text "命令提示" --dry-run --platform telegram >"$tmp_dir/menu.out" 2>"$tmp_dir/menu.err"
  grep -Fq "command-menu" "$tmp_dir/menu.out" || fail "命令提示 dry-run 缺少 command-menu"

  "$router_launcher" --text "锁状态" --dry-run --platform telegram >"$tmp_dir/lock_status.out" 2>"$tmp_dir/lock_status.err"
  grep -Fq "lock-status" "$tmp_dir/lock_status.out" || fail "锁状态 dry-run 缺少 lock-status"

  "$router_launcher" --text "Outcome闭环诊断2026-05-04" --dry-run --platform telegram >"$tmp_dir/outcome.out" 2>"$tmp_dir/outcome.err"
  grep -Fq "outcome-diagnose" "$tmp_dir/outcome.out" || fail "Outcome闭环诊断 dry-run 缺少 outcome-diagnose"
}

[[ -d "$SRC_DIR" ]] || fail "source skill directory not found: ${SRC_DIR}"
[[ -f "${SRC_DIR}/SKILL.md" ]] || fail "source SKILL.md not found"
[[ -f "${SRC_DIR}/references/telegram_control.md" ]] || fail "source references/telegram_control.md not found"

run_validation

assert_safe_dir_value "$DEST_ROOT_RAW" "HERMES skills 目录"
mkdir -p "$DEST_ROOT_RAW"
DEST_ROOT="$(cd "$DEST_ROOT_RAW" && pwd -P)"
DEST_DIR="${DEST_ROOT}/${SKILL_NAME}"
assert_safe_target "$DEST_DIR" "$DEST_ROOT"

copy_skill_dir "$DEST_ROOT" "$DEST_DIR"

[[ -f "${DEST_DIR}/SKILL.md" ]] || fail "installed SKILL.md missing"
[[ -f "${DEST_DIR}/references/telegram_control.md" ]] || fail "installed references/telegram_control.md missing"
require_text "${DEST_DIR}/SKILL.md" "hermes_cm_cn_router.py"
require_text "${DEST_DIR}/references/telegram_control.md" "所有中文 Telegram 命令必须先经过"
require_text "${DEST_DIR}/SKILL.md" "不得先运行 date"
require_text "${DEST_DIR}/SKILL.md" "不得把 今天/昨天/前天 转换为具体日期"

write_launchers
verify_launcher

cat <<'MESSAGE'
Hermes skill 已完整安装。
references 已安装。
launcher 已创建：~/.hermes/bin/chain-monitor-cn-router

请在 Telegram 中发送 /reset，确保新 skill 生效。

推荐测试：
  /chain-monitor-report-analyst 命令提示
  预期：返回完整中文命令菜单。

  /chain-monitor-report-analyst 锁状态
  预期：只读返回 Hermes lock 状态，不删除 lock 文件。

  /chain-monitor-report-analyst 标准日报流程昨天
  预期：拒绝。

  /chain-monitor-report-analyst 标准日报流程2026-05-01
  预期：返回 job_id，不在 Telegram 中同步等待。

长任务现在会返回 job_id，不会在 Telegram 中同步等待。
请继续测试：
  /chain-monitor-report-analyst 任务状态cmjob_...
  /chain-monitor-report-analyst 查看结果cmjob_...
  /chain-monitor-report-analyst 诊断任务cmjob_...

不会输出 token、RPC、完整 user id 或完整 chat id。
MESSAGE
