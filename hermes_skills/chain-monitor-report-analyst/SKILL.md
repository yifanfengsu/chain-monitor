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

## Approved Commands

The following commands may be run automatically from the repository root when needed for daily analysis:

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

## Telegram Command Mapping

Telegram users may use these safe intents. Hermes must execute the safe wrapper from the repository root instead of running raw maintenance commands directly.

Generate a report for one day:

User says:

```text
生成 YYYY-MM-DD 的报告
```

Hermes should run:

```bash
./scripts/hermes_cm_ops.sh report --date YYYY-MM-DD
```

Daily close:

User says:

```text
每日收尾 YYYY-MM-DD，我确认压缩
```

Hermes should run:

```bash
./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress
```

If the user does not provide an explicit date, or does not include the explicit confirmation phrase `我确认压缩`, Hermes must refuse and ask the user to provide the missing date or confirmation. For state-changing commands such as `daily-close`, Hermes must require both an explicit date and explicit confirmation words.

Confirm system health:

User says:

```text
确认系统健康
```

Hermes should run:

```bash
./scripts/hermes_cm_ops.sh health
```

## Date-specific Analysis

User says:

```text
分析 YYYY-MM-DD 的报告
生成 YYYY-MM-DD 的分析报告
YYYY-MM-DD 日报分析
```

Hermes should run:

```bash
./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast
```

If the user explicitly says the report may be generated when missing, for example `如果报告缺失就自动生成` or `允许自动补建`, Hermes may run:

```bash
./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast --auto-build
```

Then Hermes must read:

```text
reports/hermes/hermes_digest_input_YYYY-MM-DD.md
```

Finally, generate the Chinese summary using the Telegram Output Format in this Skill.

Rules:

- Do not read only `reports/hermes/hermes_digest_latest.md` unless its digest header confirms `report_date=YYYY-MM-DD`.
- If the digest header shows `source_date_verified=false`, state that the data date was not fully verified.
- If the digest header shows `source_fallback_to_latest=true`, warn that this analysis may not be strictly date-specific.
- Do not provide trading advice.
- Do not treat `CANDIDATE` or `VERIFIED` as an order instruction.

Generate today's analysis report:

User says:

```text
今天的分析报告
```

Hermes should first resolve `YYYY-MM-DD` to the previous completed Beijing logical date unless the user explicitly specified another date. Then run:

```bash
./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast
```

Then read:

```text
reports/hermes/hermes_digest_input_YYYY-MM-DD.md
```

Finally, generate the Chinese summary using the Telegram Output Format in this Skill.

Rules:

- `今日` defaults to the previous completed Beijing logical date unless the user explicitly specifies a date.
- Do not read only `reports/hermes/hermes_digest_latest.md` unless its digest header confirms the same `YYYY-MM-DD`.
- `daily-close` and other state-changing commands require an explicit date plus explicit confirmation words.
- Do not output `CANDIDATE` or `VERIFIED` as a trading instruction.
- Do not provide buy/sell, position sizing, leverage, take-profit, or stop-loss advice.

## Commands Requiring Explicit User Approval

Run these commands only after explicit user approval:

```bash
make daily-close DATE=YYYY-MM-DD
make daily-close-strict DATE=YYYY-MM-DD
make db-migrate-date DATE=YYYY-MM-DD
make sqlite-checkpoint
```

Explain the requested date, why the action is needed, and whether a safer read-only alternative exists.

## Forbidden / Dangerous Actions

These commands are dangerous for Hermes automation. They are allowed only if the user confirms the exact command in the same conversation:

```bash
make run
make run-research
make db-compact-execute CONFIRM=YES
make db-vacuum CONFIRM=YES
make db-prune-execute CONFIRM=YES
make archive-compress-date DATE=YYYY-MM-DD CONFIRM=YES
make daily-close DATE=YYYY-MM-DD COMPRESS=YES CONFIRM=YES
```

Before any exact-command exception, verify that it does not duplicate a production listener, does not touch the current UTC date unsafely, and does not expose secrets. Prefer dry-run or read-only diagnostics.

## Daily Analysis Procedure

1. Confirm the current directory is the repository root or the user-specified `chain-monitor` workdir.
2. Read `AGENTS.md` before analysis and follow its repository rules.
3. Confirm the report date. By default, use the previous completed Beijing logical date.
4. Check whether canonical latest reports exist:
   - `reports/daily/daily_report_latest.json`
   - `reports/daily/daily_report_latest.md`
   - `reports/daily_compare/daily_compare_latest.json`
   - `reports/daily_compare/daily_compare_latest.md`
5. If canonical reports are missing, run approved report-generation commands first. Use `daily-compare-rebuild` only when canonical inputs are missing or stale, then state that rebuild was performed.
6. Read JSON/CSV sources first and extract all numbers from structured data.
7. Read Markdown sources only for explanations, limitations, and user-facing narrative.
8. When running `make report-source-fast`, `make db-summary`, `make opportunity-db`, or `make coverage`, extract only aggregate diagnostics.
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
- Scheduled morning analysis defaults to the previous completed Beijing logical date.
- If the VPS timezone is UTC, explicitly compute the report date rather than relying on local `date`.
- Do not run `daily-close` or compression for the current UTC date unless the user explicitly requests it.
- If the requested date is ambiguous, ask for clarification or state the chosen previous completed Beijing logical date.

## Security Rules

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
