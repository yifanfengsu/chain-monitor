---
name: chain-monitor-report-analyst
description: Analyze chain-monitor canonical daily reports, daily compare reports, SQLite aggregate diagnostics, Telegram signal quality, LP/CLMM quality, and opportunity quality; produce Chinese Telegram-ready daily summaries with engineering recommendations.
version: 1.0.0
platforms: [linux]
metadata:
  hermes:
    tags: [crypto, onchain, monitoring, telegram, sqlite, reports, trading-research]
    category: analytics
    requires_toolsets: [terminal]
---

# chain-monitor-report-analyst

## Mission

Use this Skill to analyze `chain-monitor` reports in `/run-project/chain-monitor` or the current repository workdir. The output is a Chinese Telegram-ready daily report plus engineering improvement recommendations for operations, reporting quality, data integrity, Telegram signal quality, LP/CLMM quality, and opportunity quality.

This project is a research and monitoring system. `CANDIDATE` is not a trade signal, and `VERIFIED` is still not an order instruction.

## When to use / 何时使用

Use this Skill when the user requests chain-monitor report analysis, daily report generation, digest generation, health checks, or Chinese Telegram control through `/chain-monitor-report-analyst` in Telegram or CLI.

支持中文用户命令，例如：

```text
命令提示
系统体检
监听器体检
标准日报流程2026-05-01
分析报告2026-05-01
检查回放2026-05-01
数据质量2026-05-01
周复盘2026-04-27到2026-05-03
```

Telegram 控制只做系统运维分析、日报生成、digest 生成、报告复盘。不执行交易，不给交易指令，不提供直接交易建议。

## Scope

This Skill is limited to:

- Report analysis from canonical daily reports, daily compare reports, CSV summaries, and aggregate diagnostics.
- Runtime health checks using approved read-only or report-generation commands.
- Data completeness and integrity analysis.
- Telegram signal quality analysis, including delivered, suppressed, and sparse-output behavior.
- LP, CLMM, `trade_action`, `asset_market_state`, and opportunity quality analysis.
- Outcome, maturity, followthrough, adverse movement, blocker, and regression checks when data exists.
- Engineering recommendations and tomorrow's monitoring checklist.

This Skill does not perform trading execution, account actions, order placement, position sizing, leverage selection, or direct trading decisions.

## Hard Boundaries

- Do not provide direct trading advice.
- Do not generate buy direction, sell direction, entry, exit, position size, leverage, take-profit, or stop-loss recommendations.
- Do not modify `.env`, private runtime config, private address lists, API keys, Telegram token, RPC URL, service credentials, SSH keys, SQLite files, archive files, or runtime caches.
- Do not output private address lists.
- Do not output SQLite raw rows / raw payload.
- Do not output full archive payloads.
- Do not delete, reset, rewrite, compact, vacuum, prune, compress, or otherwise mutate archive/cache/db state.
- Do not run the listener.
- Do not automatically restart production services.
- Do not use `--yolo`.
- Do not convert this project into an auto-trading system.
- Do not weaken hard blockers such as `NO_TRADE`, `NO_TRADE_LOCK`, `DO_NOT_CHASE_LONG`, or `DO_NOT_CHASE_SHORT`.
- Do not reinterpret `BLOCKED` as an actionable inverse trade.

## Source Priority

Use sources in this order:

1. `reports/daily/daily_report_latest.json`
2. `reports/daily/daily_report_latest.md`
3. `reports/daily_compare/daily_compare_latest.json`
4. `reports/daily_compare/daily_compare_latest.md`
5. `reports/quality_rows.csv`
6. output from `make report-source-fast`
7. output from `make db-summary`
8. output from `make opportunity-db`
9. output from `make coverage`

Rules:

- Numbers must come from JSON/CSV first whenever available.
- Markdown is for human explanation, limitations, and narrative context.
- Legacy reports under `reports/legacy/` are only supplementary debug context and must not become the primary daily source.
- If canonical reports are missing, state exactly what is missing. Do not invent missing data.
- Do not fabricate previous-day reports, outcomes, prices, market context, watch addresses, LP pools, CLMM managers, or opportunity history.

## 中文 Telegram Command Router - Mandatory

For Telegram or `/chain-monitor-report-analyst` Chinese control requests, the first and only executable step is the stable launcher for the deterministic router:

```bash
~/.hermes/bin/chain-monitor-cn-router --text "<原始中文命令>" --execute --platform telegram
```

如果 launcher 不存在，reply to the user first:

```bash
./scripts/install_hermes_skill.sh
```

Then ask the user to send `/reset` in Telegram.

See `references/telegram_control.md` for the router grammar reference.

Rules:

- Telegram / Hermes gateway 场景必须优先调用 `~/.hermes/bin/chain-monitor-cn-router --text "<原始中文命令>" --execute --platform telegram`。
- launcher 不存在时，不要自己 `cd` 到仓库后手动运行 wrapper；回复用户先运行 `./scripts/install_hermes_skill.sh`，然后在 Telegram 发送 `/reset`。
- 中文 Telegram 请求只允许调用 launcher/router，不允许直接调用 `./scripts/hermes_cm_ops.sh`。
- 中文命令必须先被规范化到固定意图，由 router 执行固定 grammar。
- 不得直接 cd 到仓库后手动运行 cm_ops。
- 不得先运行 date。
- 不运行 date。
- 不得运行 date -u。
- 不得运行 TZ=Asia/Shanghai date。
- 不得把 今天/昨天/前天 转换为具体日期。
- 不把 昨天 转日期。
- 不得根据 `today` / `yesterday` / `tomorrow` 推断日期。
- 不得把 `read_file reports/...` 当作相对日期处理的一部分。
- 不得直接调用 ./scripts/hermes_cm_ops.sh report/analyze/digest/close。
- Telegram 中文请求不得直接调用 hermes_cm_ops.sh report/analyze/digest/close。
- 不得直接调用 hermes_cm_ops.sh 的 dated operations。
- 不得因为报告缺失就自动生成日报。
- 不得因为报告缺失自动生成日报。
- 不得从中文 Telegram 请求直接调用 make。
- 不得从中文 Telegram 请求直接调用 raw shell。
- 不得从 Telegram 文本构造任意 shell、`bash -c`、`sh -c`、`python -c`、`systemctl`、`kill`、`rm` 或未批准 `make` 命令。
- 只有中文 router 返回允许时，系统操作才可以执行。
- 如果 router 拒绝，直接把拒绝信息返回给用户。
- wrapper 输出里包含 `request_id` 时，回复必须展示 `request_id`。
- 如果用户问为什么地址/交易 hash 被隐藏，说明输出默认脱敏。
- 不提供直接交易建议，不把 `CANDIDATE` 或 `VERIFIED` 描述成买卖指令。

## Long Running Commands Must Be Submitted As Jobs

Telegram 中的长命令不得同步执行。

以下命令必须提交后台 job：

- 标准日报流程YYYY-MM-DD
- 空间检查
- 归档压缩预检YYYY-MM-DD
- 周复盘START到END

用户会收到 job_id。后续用“任务状态JOB_ID / 查看结果JOB_ID / 查看日志JOB_ID / 诊断任务JOB_ID / 最近任务”查询。

Rules:

- 不要因为用户催促就重新提交相同任务；应查询 existing job 或让用户使用“任务状态JOB_ID”。
- 不得绕过 job submit 直接执行 daily-flow/space-check/weekly-review。
- 不得用 date 解析相对日期。
- 不得自动生成日报。
- 输出必须中文、简短，展示 job_id 和后续查询命令。
- 长任务不得同步执行，不得让 Telegram 等到流程结束。

示例：

用户：

```text
/chain-monitor-report-analyst 标准日报流程2026-05-01
```

正确回复：

```text
✅ 已提交后台任务：标准日报流程
job_id：cmjob_...
查询进度：任务状态cmjob_...
查看结果：查看结果cmjob_...
查看日志：查看日志cmjob_...
失败诊断：诊断任务cmjob_...
说明：该任务会在 VPS 后台继续执行，Telegram 不需要等待。
```

用户：

```text
/chain-monitor-report-analyst 任务状态cmjob_...
```

用户：

```text
/chain-monitor-report-analyst 查看结果cmjob_...
/chain-monitor-report-analyst 诊断任务cmjob_...
```

## 中文 Telegram 手动控制菜单

本菜单覆盖第 5 步 12 个功能，并额外支持“命令提示”。

用户可以输入：

```text
/chain-monitor-report-analyst 命令提示
```

获取完整中文命令菜单。

Telegram 中文命令必须先调用：

```bash
~/.hermes/bin/chain-monitor-cn-router --text "<原始中文命令>" --execute --platform telegram
```

Rules:

- 不得手动把中文命令翻译成 shell。
- 不得直接调用 make。
- 不得直接调用 hermes_cm_ops.sh 的 dated operations。
- 不得把“昨天/今天/前天”转换成日期。
- 不得运行 date -u 或 TZ=Asia/Shanghai date 来解析用户相对日期。
- 报告缺失时，不得自动生成日报；必须提示用户运行：标准日报流程YYYY-MM-DD。
- 所有输出中文、简洁、适合 Telegram。
- 必须展示 request_id，如果 wrapper 输出中有。
- 不输出原始地址、tx hash、RPC URL、Telegram token、私有路径。
- 不得输出地址簿。
- 不得输出 token/RPC/API key。
- 不给交易建议。
- 不得解析昨天。

Telegram 示例：

```text
/chain-monitor-report-analyst 命令提示
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 查看结果cmjob_...
/chain-monitor-report-analyst 分析报告2026-05-01
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
/chain-monitor-report-analyst Profile复盘2026-05-01
/chain-monitor-report-analyst Blocker复盘2026-05-01
/chain-monitor-report-analyst Shadow复盘2026-05-01
/chain-monitor-report-analyst 空间检查
/chain-monitor-report-analyst 空间快检
/chain-monitor-report-analyst 归档压缩预检2026-05-01
/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03
```

拒绝示例：

用户：

```text
/chain-monitor-report-analyst 分析昨天的报告
```

必须拒绝：

```text
⚠️ 请使用绝对日期 YYYY-MM-DD。
示例：分析报告2026-05-01
我不会根据“今天/昨天/前天”自动执行。
```

禁止：

- date -u
- TZ=Asia/Shanghai date
- 自动生成日报
- analyze

## Approved Commands

Telegram 中文请求允许：

```bash
~/.hermes/bin/chain-monitor-cn-router --text "<中文命令>" --execute --platform telegram
~/.hermes/bin/chain-monitor-cn-router --stdin --execute --platform telegram
~/.hermes/bin/chain-monitor-cn-router --text "<中文命令>" --dry-run --platform telegram
```

Local repo fallback only, not the preferred Telegram path:

```bash
./scripts/hermes_cm_cn_router.py --text "<中文命令>" --execute --platform telegram
./scripts/hermes_cm_cn_router.py --stdin --execute --platform telegram
./scripts/hermes_cm_cn_router.py --text "<中文命令>" --dry-run --platform telegram
```

Wrapper internal commands document only:

```bash
./scripts/hermes_cm_ops.sh command-menu
./scripts/hermes_cm_ops.sh system-health
./scripts/hermes_cm_ops.sh listener-health
./scripts/hermes_cm_ops.sh submit-daily-flow --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh submit-space-check
./scripts/hermes_cm_ops.sh space-fast
./scripts/hermes_cm_ops.sh submit-archive-compress-check --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh submit-weekly-review --start YYYY-MM-DD --end YYYY-MM-DD
./scripts/hermes_cm_ops.sh job-status --job-id JOB_ID
./scripts/hermes_cm_ops.sh job-result --job-id JOB_ID
./scripts/hermes_cm_ops.sh job-log --job-id JOB_ID
./scripts/hermes_cm_ops.sh job-diagnose --job-id JOB_ID
./scripts/hermes_cm_ops.sh job-list
./scripts/hermes_cm_ops.sh job-cancel --job-id JOB_ID --confirm
./scripts/hermes_cm_ops.sh report --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD --mode fast
./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD --mode deep
./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast
./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode deep
./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast --auto-build
./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode deep --auto-build
./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress
```

这些 cm_ops 命令只能作为 router 内部调用 / local CLI allowed，但不得从 Telegram 中文请求直接调用。`help` 和 `health` 可以由 router 内部调用：

```bash
./scripts/hermes_cm_ops.sh help
./scripts/hermes_cm_ops.sh health
```

Local/internal long command names retained for job runner only:

```bash
./scripts/hermes_cm_ops.sh daily-flow --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh space-check
./scripts/hermes_cm_ops.sh archive-compress-check --date YYYY-MM-DD
./scripts/hermes_cm_ops.sh weekly-review --start YYYY-MM-DD --end YYYY-MM-DD
```

这些旧长命令不得作为 Telegram 中文 grammar 的直接目标。

保留以下安全聚合 make 命令仅作为 wrapper 内部或本地开发参考，不作为 Telegram 直接映射：

```bash
make env-check
make report-source-fast
make db-integrity DB_INTEGRITY_FAST=YES
make db-summary
make opportunity-db
make coverage
make report-daily
make report-daily-date DATE=YYYY-MM-DD
make daily-compare DATE=YYYY-MM-DD
make daily-compare-rebuild DATE=YYYY-MM-DD
```

Rules:

- Use `daily-compare-rebuild` only when canonical report inputs are missing or stale.
- If `daily-compare-rebuild` is used, explicitly state that rebuild was performed.
- DB summaries may be reported only as aggregate counts and health status.
- Never print DB raw rows, DB raw payloads, full archive payloads, private addresses, tokens, keys, or RPC URLs.
- Telegram 中文请求必须先通过 router；direct raw shell commands are not approved for Telegram control.
- Every wrapper invocation appends an audit event to `reports/hermes/ops_audit.ndjson`.

## Hermes Gateway / Telegram Control Rules

- Telegram control must go through the official Hermes gateway and this Skill's slash command, not a separate Telegram inbound bot.
- Each Hermes Skill automatically becomes a slash command. For this Skill, use `/chain-monitor-report-analyst`.
- User authorization must rely on `TELEGRAM_ALLOWED_USERS` or `GATEWAY_ALLOWED_USERS`.
- Do not set `GATEWAY_ALLOW_ALL_USERS=true`.
- Do not use `/yolo`.
- Do not request or require `approvals.mode: off`; use manual or smart approvals.
- In groups, prefer `telegram.require_mention: true`.
- Telegram 中文控制必须先调用 `~/.hermes/bin/chain-monitor-cn-router`; repo fallback only 可调用 `./scripts/hermes_cm_cn_router.py`。
- router-only / 只能调用 launcher/router, do not execute raw shell commands from Telegram intents.
- Do not call unapproved `make` commands.
- All wrapper calls write `reports/hermes/ops_audit.ndjson`.
- All digest and Telegram output must use redaction / 脱敏 by default.
- Do not output Telegram token, RPC URL, API key, private address books, raw SQLite rows, or full archive payloads.

## Telegram 中文命令示例

```text
/chain-monitor-report-analyst 命令提示
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 查看结果cmjob_...
/chain-monitor-report-analyst 诊断任务cmjob_...
/chain-monitor-report-analyst 分析报告2026-05-01
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
/chain-monitor-report-analyst Profile复盘2026-05-01
/chain-monitor-report-analyst Blocker复盘2026-05-01
/chain-monitor-report-analyst Shadow复盘2026-05-01
/chain-monitor-report-analyst 空间检查
/chain-monitor-report-analyst 空间快检
/chain-monitor-report-analyst 归档压缩预检2026-05-01
/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03
/chain-monitor-report-analyst 生成日报YYYY-MM-DD
/chain-monitor-report-analyst 深度分析报告YYYY-MM-DD
/chain-monitor-report-analyst 生成摘要YYYY-MM-DD 快速
/chain-monitor-report-analyst 生成摘要YYYY-MM-DD 深度
/chain-monitor-report-analyst 构建并分析报告YYYY-MM-DD 快速
```

High-risk 示例：

```text
/chain-monitor-report-analyst 每日收尾YYYY-MM-DD 我确认压缩
```

每日收尾是 high-risk。每日收尾不属于每日默认流程。每日收尾必须包含“我确认压缩”。

标准日报流程失败时，不要建议用户运行“每日收尾YYYY-MM-DD 我确认压缩”。daily-close 默认只是 compression dry-run，不真正压缩 archive；只有 `COMPRESS=YES CONFIRM=YES` 才会执行实际 archive gzip。“每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法。先让用户执行：

```text
/chain-monitor-report-analyst 诊断任务JOB_ID
/chain-monitor-report-analyst 查看日志JOB_ID
```

不要因为 daily-close failure 就压缩 archive。

Hard failure examples:

用户：

```text
/chain-monitor-report-analyst 分析昨天的报告
```

必须回复：

```text
⚠️ 请使用绝对日期 YYYY-MM-DD。
示例：分析报告2026-05-01
我不会根据“今天/昨天/前天”自动执行。
```

禁止执行：

```text
date -u
TZ=Asia/Shanghai date
./scripts/hermes_cm_ops.sh report ...
./scripts/hermes_cm_ops.sh analyze ...
read_file reports/...
```

用户：

```text
/chain-monitor-report-analyst 生成今天的日报
```

必须拒绝，禁止生成日报。

成功样例：

用户：

```text
/chain-monitor-report-analyst 分析报告2026-05-01
```

允许调用：

```bash
~/.hermes/bin/chain-monitor-cn-router --text "分析报告2026-05-01" --execute --platform telegram
```

router 再调用：

```bash
./scripts/hermes_cm_ops.sh analyze --date 2026-05-01 --mode fast
```

## Date-specific Analysis

用户说：

```text
分析报告YYYY-MM-DD
快速分析报告YYYY-MM-DD
日报分析YYYY-MM-DD
报告分析YYYY-MM-DD
```

Hermes should run:

```bash
~/.hermes/bin/chain-monitor-cn-router --text "分析报告YYYY-MM-DD" --execute --platform telegram
```

用户说：

```text
深度分析报告YYYY-MM-DD
深度复盘YYYY-MM-DD
深度分析YYYY-MM-DD
```

Hermes should run:

```bash
~/.hermes/bin/chain-monitor-cn-router --text "深度分析报告YYYY-MM-DD" --execute --platform telegram
```

用户说：

```text
构建并分析报告YYYY-MM-DD 快速
自动构建并分析YYYY-MM-DD 深度
```

Hermes may add `--auto-build` only for those explicit auto-build forms:

```bash
~/.hermes/bin/chain-monitor-cn-router --text "构建并分析报告YYYY-MM-DD 快速" --execute --platform telegram
~/.hermes/bin/chain-monitor-cn-router --text "构建并分析报告YYYY-MM-DD 深度" --execute --platform telegram
```

The router internally maps those requests to `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast|deep [--auto-build]`.

Then Hermes must read:

```text
reports/hermes/hermes_digest_input_YYYY-MM-DD.md
```

Finally, generate the Chinese summary using the Telegram Output Format in this Skill.

Rules:

- If the router refuses, stop immediately and do not read report files.
- Digest inputs and Telegram summaries must remain redacted by default.
- Do not output unredacted EVM addresses, transaction hashes, RPC URLs, Telegram tokens, or absolute private paths.
- Do not read only `reports/hermes/hermes_digest_latest.md` unless its digest header confirms `report_date=YYYY-MM-DD`.
- If the digest header shows `source_date_verified=false`, state that the data date was not fully verified.
- If the digest header shows `source_fallback_to_latest=true`, warn that this analysis may not be strictly date-specific.
- Do not provide trading advice.
- Do not treat `CANDIDATE` or `VERIFIED` as an order instruction.
- Requests such as “今天的分析报告”“昨天的报告”“分析一下” must not execute; ask for an absolute `YYYY-MM-DD`.

## Not approved / Forbidden from Telegram

Do not call these commands or actions directly from Telegram. Use only the approved wrapper command when a supported operation exists.

```text
make run
make run-research
make health
make db-report
make db-compact-execute
make db-vacuum
make db-prune-execute
make archive-compress-date
make daily-close COMPRESS=YES
make daily-close direct/raw
systemctl stop/restart/disable/mask
pkill / kill / kill -9
rm -rf
bash -c / sh -c / python -c
curl | sh / wget | sh
editing .env
editing data/addresses.json
editing data/lp_pools.json
输出真实地址簿
dumping raw SQLite rows
dumping raw archive events
outputting private addresses or raw address book
买入/卖出/仓位/杠杆/止损/止盈建议
```

Explain that direct/raw maintenance commands are outside this Skill's Telegram control boundary. For daily close, only the launcher/router may trigger the validated wrapper argv `./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress`, and only with explicit date and confirmation.

## 每日中文 Telegram 工作流

推荐每日流程必须使用中文命令：

1. `/chain-monitor-report-analyst 系统体检`
2. `/chain-monitor-report-analyst 标准日报流程YYYY-MM-DD`
3. `/chain-monitor-report-analyst 任务状态JOB_ID`
4. 失败时先用 `/chain-monitor-report-analyst 诊断任务JOB_ID` 和 `/chain-monitor-report-analyst 查看日志JOB_ID`
5. `/chain-monitor-report-analyst 分析报告YYYY-MM-DD`
6. 可选：`/chain-monitor-report-analyst 生成摘要YYYY-MM-DD 快速`
7. 可选深度复盘：`/chain-monitor-report-analyst 深度分析报告YYYY-MM-DD`

“每日收尾”不属于默认每日流程。

“每日收尾”必须使用：

```text
/chain-monitor-report-analyst 每日收尾YYYY-MM-DD 我确认压缩
```

不要建议用户使用 `--allow-today`。

## Daily Analysis Procedure

1. For Telegram, use `~/.hermes/bin/chain-monitor-cn-router`; do not depend on the current working directory.
2. Read `AGENTS.md` before analysis and follow its repository rules.
3. Confirm the report date. Telegram control requires absolute `YYYY-MM-DD`; do not infer relative dates.
4. Check whether canonical latest reports exist:
   - `reports/daily/daily_report_latest.json`
   - `reports/daily/daily_report_latest.md`
   - `reports/daily_compare/daily_compare_latest.json`
   - `reports/daily_compare/daily_compare_latest.md`
5. If canonical reports are missing during a Telegram request, do not automatically generate them unless the router-accepted command was the explicit “构建并分析报告YYYY-MM-DD 快速/深度” form. Use `daily-compare-rebuild` only when canonical inputs are missing or stale and the approved workflow allows it, then state that rebuild was performed.
6. Read JSON/CSV sources first and extract all numbers from structured data.
7. Read Markdown sources only for explanations, limitations, and user-facing narrative.
8. When running `make report-source-fast`, `make db-summary`, `make opportunity-db`, or `make coverage` locally, extract only aggregate diagnostics.
9. Generate a Chinese Telegram summary under 2500 Chinese characters.
10. If a longer analysis is needed, write it to `reports/hermes/hermes_analysis_YYYY-MM-DD.md`.
11. Do not modify code, production config, private config, archive files, cache files, DB files, or raw data.
12. Do not delete data.
13. Do not run the listener.

## Metrics To Extract

Extract these metrics when available:

- Report date.
- Source completeness.
- Canonical report availability.
- SQLite health / integrity.
- Archive/cache fallback status.
- Signal counts.
- Telegram delivered / suppressed / sparse output quality.
- LP rows / delivered LP / LP stage / LP direction.
- CLMM coverage / CLMM event quality.
- `trade_action` distribution.
- `asset_market_state` changes.
- Opportunity counts: `CANDIDATE` / `VERIFIED` / `BLOCKED` / `NONE`.
- Blocker reasons.
- Verified maturity.
- Outcome completion.
- Followthrough / adverse if available.
- Market context source / hit rate / unavailable reason.
- Major asset coverage: ETH / BTC / SOL where available.
- Missing data and stale data.
- Regression compared with previous report if daily compare exists.

## Output Style

- 中文。
- Telegram-friendly。
- 简短。
- 展示 `request_id`。
- 可以展示使用的 wrapper 命令，但不要展示 raw shell 或超出 approved wrapper 的命令。
- 可以展示安全结果路径。
- 不输出原始地址、tx hash、RPC URL、Telegram token、私有路径。
- 不给交易建议。

## Telegram Output Format

The Telegram output must be Chinese and no longer than 2500 Chinese characters. Use this exact structure:

```text
Chain Monitor 日报｜YYYY-MM-DD

总评：
...

关键数字：
- ...
- ...

系统健康：
...

数据完整性：
...

Telegram 信号质量：
...

LP / CLMM / opportunity 质量：
...

今日异常：
...

最重要的 3 个改进动作：
1. ...
2. ...
3. ...

明天检查项：
- ...

注意：这不是交易建议。
```

Do not include direct buy/sell advice, position sizing, leverage, take-profit, stop-loss, or order instructions.

## Detailed Report Output

If the analysis is too long for Telegram, write the full analysis to:

```text
reports/hermes/hermes_analysis_YYYY-MM-DD.md
```

The detailed report may include structured sections for source inventory, extracted metrics, limitations, regressions, blocker analysis, and engineering recommendations. It must still avoid secrets, raw private data, raw DB rows, and full archive payloads.

## Missing Data Behavior

- If data is missing, write `数据不足`.
- List the missing files or missing fields.
- Do not fabricate yesterday's data.
- Do not fabricate market context.
- Do not infer the full research flow from sparse Telegram output.
- Do not use legacy reports as the primary source when canonical reports are missing.
- Explain whether approved report-generation commands were run and whether outputs are still incomplete.

## Date Rules

- Daily reports use Beijing logical dates.
- Telegram 控制必须使用绝对 `YYYY-MM-DD`。
- 不支持相对日期自动执行；“今天”“昨天”“前天”“今日”“昨日”“today”“yesterday”必须要求用户改写。
- Non-Telegram scheduled report analysis may follow repository scheduling rules, but Telegram control must never infer the date.
- If the VPS timezone is UTC, explicitly compute the report date rather than relying on local `date`.
- Do not run `daily-close` or compression for the current UTC date through Telegram; wrapper protection should refuse current UTC dates by default.
- If the requested Telegram date is ambiguous, ask for clarification and do not execute.

## Security Rules

- Hermes digest / Telegram output must be redacted by default.
- Do not output unredacted EVM addresses.
- Do not output unredacted transaction hashes.
- Do not output unredacted RPC URLs.
- Do not output unredacted Telegram tokens.
- Do not output absolute private paths such as `/root/...`, `/home/...`, or `/run-project/...`.
- Telegram 中文 control must call only `~/.hermes/bin/chain-monitor-cn-router`; local repo fallback may call `./scripts/hermes_cm_cn_router.py`; do not execute raw shell commands from Telegram.
- Telegram 中文请求不得直接调用 hermes_cm_ops.sh report/analyze/digest/close.
- Do not output raw private address list.
- Do not output `.env`.
- Do not output token / key / RPC URL.
- Do not output Telegram token.
- Do not output SQLite raw rows / raw payload.
- Do not output full archive payload.
- For DB, output only aggregate health/counts.
- Use `make env-check` only for non-sensitive configuration summaries.
- Treat `data/`, `app/data/archive/`, SQLite WAL/SHM files, caches, and generated historical reports as valuable private or operational state.

## Final Checklist

- Am I in repo root?
- Did I read `AGENTS.md`?
- Is this read-only analysis?
- Am I about to expose secrets?
- Am I about to touch archive/cache/db?
- Am I using JSON/CSV for numbers?
- Are canonical reports available?
- Is date clear?
- Is Telegram output under 2500 Chinese characters?
- Did I avoid trading advice?
