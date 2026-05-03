# Chain Monitor Telegram 中文命令目录

## 控制路径

```text
Telegram -> Hermes gateway -> /chain-monitor-report-analyst -> ~/.hermes/bin/chain-monitor-cn-router -> scripts/hermes_cm_cn_router.py -> scripts/hermes_cm_ops.sh
```

## 日期规则

- 所有执行型日期必须是 YYYY-MM-DD。
- DATE 是北京时间逻辑日。
- 不支持 今天、昨天、前天、今日、昨日、today、yesterday 自动执行。
- 如果用户使用相对日期，必须拒绝并要求改用 YYYY-MM-DD。
- 周复盘日期范围格式为 START到END，例如：周复盘2026-04-27到2026-05-03

## 每日推荐手动流程

```text
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 查看结果cmjob_...
/chain-monitor-report-analyst 诊断任务cmjob_...
/chain-monitor-report-analyst 分析报告2026-05-01
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
```

## 长任务说明

以下命令不会同步等待完成，而是提交后台任务并返回 job_id：

- 标准日报流程YYYY-MM-DD
- 空间检查
- 归档压缩预检YYYY-MM-DD
- 周复盘START到END

返回后请使用：

- 任务状态JOB_ID
- 查看结果JOB_ID
- 查看日志JOB_ID
- 诊断任务JOB_ID
- 最近任务

## 中文功能表

| 中文命令 | 功能 | 底层 wrapper | 风险等级 | 说明 |
| --- | --- | --- | --- | --- |
| 命令提示 | 返回完整中文命令菜单 | `./scripts/hermes_cm_ops.sh command-menu` | low | 不执行 make，不读敏感数据。 |
| 系统体检 | 检查 SQLite / report source / market health / coverage | `./scripts/hermes_cm_ops.sh system-health` | low | wrapper 内部可限时执行只读健康检查，包括 db-report、report-source-fast、market health、coverage，但 Telegram 不得直接调用 make。 |
| 监听器体检 | 检查监听器是否可能停摆、最近 raw/parsed/signal/archive 时间、zero activity 风险 | `./scripts/hermes_cm_ops.sh listener-health` | low | 只读，不重启 listener。 |
| 标准日报流程YYYY-MM-DD | 提交后台任务跑展开后的 daily-close 子步骤 + full replay + report + compare + checkpoint | `./scripts/hermes_cm_ops.sh submit-daily-flow --date YYYY-MM-DD` | medium | async job。daily-close 默认只做 archive compression dry-run；不执行 archive gzip，不 compact，不 vacuum，不 prune。 |
| 分析报告YYYY-MM-DD | Hermes 分析 canonical daily report | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast` | low-medium | 不自动生成日报；报告缺失时应提示先运行“标准日报流程YYYY-MM-DD”或“生成日报YYYY-MM-DD”，不得自动构建。 |
| 检查回放YYYY-MM-DD | 检查 trade replay 是否为 persisted full replay | `./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD` | low | 返回 replay_source、replay_scope、persisted_rows_found、replay_count、valid_replay_count、avg_net_pnl_bps、suppressed_replay_count 等摘要。 |
| 数据质量YYYY-MM-DD | 判断这天是否适合做策略质量分析 | `./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD` | low | 返回 data_quality_status、zero_activity_day、active_hours、signal_count、raw_event_count、LP 行数、market context、coverage、DB/archive mismatch。 |
| Profile复盘YYYY-MM-DD | 查看 trade_replay_profile_daily_stats 的 profile 后验 | `./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD` | low | 返回样本最多 profile、avg_net_pnl_bps、clean_followthrough_rate、bad_entry_rate、absorption_reversal_rate、chop_rate、recommended_action。 |
| Blocker复盘YYYY-MM-DD | 查看 replay_profile_negative / no_trade_lock / low_quality 等 blocker 分布 | `./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD` | low | 用于判断风控是否正确阻止负收益 profile。 |
| Shadow复盘YYYY-MM-DD | 查看 shadow_funnel_summary | `./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD` | low | 返回 shadow_candidate_count、shadow_verified_count、near_candidate_but_blocked、score_below_shadow_candidate 等。 |
| 空间检查 | 提交后台任务查看 SQLite / WAL / archive / reports 占用 | `./scripts/hermes_cm_ops.sh submit-space-check` | low-medium | async job。只读，不删除，不 vacuum，不 prune。 |
| 空间快检 | 快速同步查看 SQLite/WAL/SHM 文件大小 | `./scripts/hermes_cm_ops.sh space-fast` | low | sync quick。不递归扫描 archive/reports。 |
| 归档压缩预检YYYY-MM-DD | 提交后台 archive compression dry-run | `./scripts/hermes_cm_ops.sh submit-archive-compress-check --date YYYY-MM-DD` | low-medium | async job。只 dry-run，不真正压缩。 |
| 周复盘START到END | 提交后台周度数据质量、profile、suppression、shadow、空间摘要 | `./scripts/hermes_cm_ops.sh submit-weekly-review --start START --end END` | medium | async job。只读聚合，不改参数，不放宽 verified，不给交易建议。 |
| 任务状态JOB_ID | 查看后台任务状态 | `./scripts/hermes_cm_ops.sh job-status --job-id JOB_ID` | low | 返回 status、duration、exit_code、结果/日志查询命令。 |
| 查看结果JOB_ID | 查看任务完成后的 result.md 摘要 | `./scripts/hermes_cm_ops.sh job-result --job-id JOB_ID` | low | 最多返回安全摘要，长内容截断。 |
| 查看日志JOB_ID | 查看任务 stdout/stderr 日志尾部 | `./scripts/hermes_cm_ops.sh job-log --job-id JOB_ID` | low | tail 摘要，默认脱敏。 |
| 诊断任务JOB_ID | 查看失败任务的 failed_substep、failed_command、timeout_hit 和下一步建议 | `./scripts/hermes_cm_ops.sh job-diagnose --job-id JOB_ID` | low | 只读，不执行 make。标准日报流程失败时先用它定位子步骤。 |
| 最近任务 | 列出最近 10 个任务 | `./scripts/hermes_cm_ops.sh job-list` | low | 展示 job_id、kind、status、date/range、created_at。 |
| 取消任务JOB_ID 我确认取消 | 取消 pending/running 后台任务 | `./scripts/hermes_cm_ops.sh job-cancel --job-id JOB_ID --confirm` | medium | 必须包含确认短语，不使用 kill -9。 |

## 禁止命令区

- 不支持“分析昨天的报告”
- 不支持“生成今天的日报”
- 不支持“重启监听器”
- 不支持“帮我运行 make run”
- 不支持“修改地址簿”
- 不支持“降低 VERIFIED 阈值”
- 不支持“db vacuum”
- 不支持“prune”
- 不支持“compact execute”
- 不支持“删除 archive/cache/db”
- 不提供买入/卖出、仓位、杠杆、止盈止损。

## 标准日报流程失败诊断

- `make daily-close DATE=YYYY-MM-DD` 在标准流程语义中是前置收尾，不是最后一步。
- daily-close 默认只是 compression dry-run；只有 `COMPRESS=YES CONFIRM=YES` 才会执行实际 archive gzip。
- 标准日报流程已经展开 daily-close 子步骤：migrate archive、DB integrity、DB report、archive mirror check、初次 report/compare、archive compress dry-run、sqlite checkpoint，然后才 full replay、二次 report/compare、最终 checkpoint。
- 失败时先运行 `诊断任务JOB_ID` 和 `查看日志JOB_ID`，查看 `failed_substep`、`failed_command`、`exit_code`、`timeout_hit`。
- “每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法。
- 不要因为 daily-close failure 就压缩 archive；只有明确执行 archive compression execute 场景才可以提到“我确认压缩”。

## 命令提示输出模板

```text
📋 Chain Monitor 中文命令菜单

【每日检查】
- 系统体检：检查 DB / report source / market / coverage
- 监听器体检：检查监听器是否停摆、最近数据时间
- 空间检查：提交后台任务查看 DB / archive / reports 占用
- 空间快检：快速同步查看 SQLite / WAL 文件大小

【日报流程】
- 标准日报流程YYYY-MM-DD：提交后台任务，展开 daily-close 子步骤 + full replay + report + compare + checkpoint
- 分析报告YYYY-MM-DD：分析已存在日报
- 检查回放YYYY-MM-DD：确认 replay_source=persisted、scope=full
- 数据质量YYYY-MM-DD：判断该日是否有效

【后验复盘】
- Profile复盘YYYY-MM-DD
- Blocker复盘YYYY-MM-DD
- Shadow复盘YYYY-MM-DD

【维护预检】
- 归档压缩预检YYYY-MM-DD：提交后台 dry-run 任务，不压缩

【周复盘】
- 周复盘START到END：提交后台任务，例如：周复盘2026-04-27到2026-05-03

【后台任务】
- 任务状态JOB_ID
- 查看结果JOB_ID
- 查看日志JOB_ID
- 诊断任务JOB_ID
- 最近任务
- 取消任务JOB_ID 我确认取消

规则：
- 日期必须用 YYYY-MM-DD
- 不支持 今天/昨天/前天 自动执行
- 输出默认脱敏
- 不提供交易建议
```
