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
/chain-monitor-report-analyst 分析报告2026-05-01
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
```

## 中文功能表

| 中文命令 | 功能 | 底层 wrapper | 风险等级 | 说明 |
| --- | --- | --- | --- | --- |
| 命令提示 | 返回完整中文命令菜单 | `./scripts/hermes_cm_ops.sh command-menu` | low | 不执行 make，不读敏感数据。 |
| 系统体检 | 检查 SQLite / report source / market health / coverage | `./scripts/hermes_cm_ops.sh system-health` | low | wrapper 内部可限时执行只读健康检查，包括 db-report、report-source-fast、market health、coverage，但 Telegram 不得直接调用 make。 |
| 监听器体检 | 检查监听器是否可能停摆、最近 raw/parsed/signal/archive 时间、zero activity 风险 | `./scripts/hermes_cm_ops.sh listener-health` | low | 只读，不重启 listener。 |
| 标准日报流程YYYY-MM-DD | 手动跑 daily-close + trade-replay-full + report-daily-date + daily-compare + sqlite-checkpoint | `./scripts/hermes_cm_ops.sh daily-flow --date YYYY-MM-DD` | medium | 不压缩 archive，不 compact，不 vacuum，不 prune。默认拒绝当前北京时间逻辑日，避免对仍在写入的数据做收尾。 |
| 分析报告YYYY-MM-DD | Hermes 分析 canonical daily report | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast` | low-medium | 不自动生成日报；报告缺失时应提示先运行“标准日报流程YYYY-MM-DD”或“生成日报YYYY-MM-DD”，不得自动构建。 |
| 检查回放YYYY-MM-DD | 检查 trade replay 是否为 persisted full replay | `./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD` | low | 返回 replay_source、replay_scope、persisted_rows_found、replay_count、valid_replay_count、avg_net_pnl_bps、suppressed_replay_count 等摘要。 |
| 数据质量YYYY-MM-DD | 判断这天是否适合做策略质量分析 | `./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD` | low | 返回 data_quality_status、zero_activity_day、active_hours、signal_count、raw_event_count、LP 行数、market context、coverage、DB/archive mismatch。 |
| Profile复盘YYYY-MM-DD | 查看 trade_replay_profile_daily_stats 的 profile 后验 | `./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD` | low | 返回样本最多 profile、avg_net_pnl_bps、clean_followthrough_rate、bad_entry_rate、absorption_reversal_rate、chop_rate、recommended_action。 |
| Blocker复盘YYYY-MM-DD | 查看 replay_profile_negative / no_trade_lock / low_quality 等 blocker 分布 | `./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD` | low | 用于判断风控是否正确阻止负收益 profile。 |
| Shadow复盘YYYY-MM-DD | 查看 shadow_funnel_summary | `./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD` | low | 返回 shadow_candidate_count、shadow_verified_count、near_candidate_but_blocked、score_below_shadow_candidate 等。 |
| 空间检查 | 查看 SQLite / WAL / archive / reports 占用 | `./scripts/hermes_cm_ops.sh space-check` | low | 只读，不删除，不 vacuum，不 prune。 |
| 归档压缩预检YYYY-MM-DD | archive compression dry-run | `./scripts/hermes_cm_ops.sh archive-compress-check --date YYYY-MM-DD` | low-medium | 只 dry-run，不真正压缩。 |
| 周复盘START到END | 周度数据质量、profile、suppression、shadow、空间摘要 | `./scripts/hermes_cm_ops.sh weekly-review --start START --end END` | medium | 只读聚合，不改参数，不放宽 verified，不给交易建议。 |

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

## 命令提示输出模板

```text
📋 Chain Monitor 中文命令菜单

【每日检查】
- 系统体检：检查 DB / report source / market / coverage
- 监听器体检：检查监听器是否停摆、最近数据时间
- 空间检查：查看 DB / archive / reports 占用

【日报流程】
- 标准日报流程YYYY-MM-DD：daily-close + full replay + report + compare + checkpoint
- 分析报告YYYY-MM-DD：分析已存在日报
- 检查回放YYYY-MM-DD：确认 replay_source=persisted、scope=full
- 数据质量YYYY-MM-DD：判断该日是否有效

【后验复盘】
- Profile复盘YYYY-MM-DD
- Blocker复盘YYYY-MM-DD
- Shadow复盘YYYY-MM-DD

【维护预检】
- 归档压缩预检YYYY-MM-DD：只 dry-run，不压缩

【周复盘】
- 周复盘START到END，例如：周复盘2026-04-27到2026-05-03

规则：
- 日期必须用 YYYY-MM-DD
- 不支持 今天/昨天/前天 自动执行
- 输出默认脱敏
- 不提供交易建议
```
