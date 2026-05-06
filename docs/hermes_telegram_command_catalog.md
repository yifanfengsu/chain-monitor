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
- 标准日报流程只能跑已经结束的北京时间逻辑日，不支持当前北京时间日期。
- 例如北京时间 2026-05-03 还没结束时，不能跑 `标准日报流程2026-05-03`。
- 请在次日 00:05 后执行 `标准日报流程YYYY-MM-DD`。
- 周复盘日期范围格式为 START到END，例如：周复盘2026-04-27到2026-05-03

## 每日推荐手动流程

```text
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 锁状态
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 查看结果cmjob_...
/chain-monitor-report-analyst 诊断任务cmjob_...
/chain-monitor-report-analyst 分析报告2026-05-01
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
/chain-monitor-report-analyst 学习复盘2026-05-01
/chain-monitor-report-analyst 学习总结2026-05-01
/chain-monitor-report-analyst CANDIDATE覆盖诊断2026-05-04
/chain-monitor-report-analyst LP诊断2026-05-04
/chain-monitor-report-analyst 生成日报2026-05-01
/chain-monitor-report-analyst 深度分析报告2026-05-01
/chain-monitor-report-analyst 生成摘要2026-05-01 快速
/chain-monitor-report-analyst 数据库瘦身预检
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
- 锁状态

## 中文功能表

| 中文命令 | 功能 | 底层 wrapper | 风险等级 | 说明 |
| --- | --- | --- | --- | --- |
| 命令提示 | 返回完整中文命令菜单 | `./scripts/hermes_cm_ops.sh command-menu` | low | 不执行 make，不读敏感数据。 |
| 锁状态 / 锁检查 / Hermes锁状态 | 只读检查 Hermes lock 是否被占用 | `./scripts/hermes_cm_ops.sh lock-status` | low | 不删除 lock 文件；lock 未被持有时提示后续命令可重试，lock 被持有时提示查看最近任务/任务状态。 |
| 系统体检 | 检查 SQLite / report source / market health / coverage | `./scripts/hermes_cm_ops.sh system-health` | low | wrapper 内部可限时执行只读健康检查，包括 db-report、report-source-fast、market health、coverage，但 Telegram 不得直接调用 make。 |
| 监听器体检 | 检查监听器是否可能停摆、最近 raw/parsed/signal/archive 时间、zero activity 风险、Telegram outbound 健康 | `./scripts/hermes_cm_ops.sh listener-health` | low | 只读，不重启 listener，不发送 Telegram warning；输出 sent_ok / sent_failed、最近成功/失败时间和 pool timeout / NetworkError / TimedOut 聚合 warning。 |
| 标准日报流程YYYY-MM-DD | 提交后台任务跑展开后的 daily-close 子步骤 + full replay + report + compare + checkpoint | `./scripts/hermes_cm_ops.sh submit-daily-flow --date YYYY-MM-DD` | medium | async job。只能跑已经结束的北京时间逻辑日；当前北京时间日期会在 submit 阶段拒绝，不创建 job。daily-close 默认只做 archive compression dry-run；不执行 archive gzip，不 compact，不 vacuum，不 prune。 |
| 生成日报YYYY-MM-DD | 生成 canonical 日报 | `./scripts/hermes_cm_ops.sh report --date YYYY-MM-DD` | low-medium | sync。不执行 vacuum/prune/compact execute。 |
| 分析报告YYYY-MM-DD | Hermes 分析 canonical daily report | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast` | low-medium | 不自动生成日报；报告缺失时应提示先运行“标准日报流程YYYY-MM-DD”或“生成日报YYYY-MM-DD”，不得自动构建。 |
| 深度分析报告YYYY-MM-DD | Hermes 深度分析 canonical daily report | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode deep` | medium | 不自动生成日报；只生成分析输入，后续由 Hermes 产出中文复盘。 |
| 生成摘要YYYY-MM-DD 快速 | 生成快速分析输入包 | `./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD --mode fast` | low | sync。输出脱敏 digest 路径。 |
| 生成摘要YYYY-MM-DD 深度 | 生成深度分析输入包 | `./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD --mode deep` | medium | sync。输出脱敏 digest 路径。 |
| 检查回放YYYY-MM-DD | 检查 trade replay 是否为 persisted full replay | `./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD` | low | 返回 replay_source、replay_scope、persisted_rows_found、replay_count、valid_replay_count、avg_net_pnl_bps、suppressed_replay_count 等摘要。 |
| 数据质量YYYY-MM-DD | 判断这天是否适合做策略质量分析 | `./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD` | low | 返回 data_quality_status、zero_activity_day、active_hours、signal_count、raw_event_count、LP 行数、market context、coverage、DB/archive mismatch。 |
| Profile复盘YYYY-MM-DD | 查看 trade_replay_profile_daily_stats 的 profile 后验 | `./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD` | low | 返回样本最多 profile、avg_net_pnl_bps、clean_followthrough_rate、bad_entry_rate、absorption_reversal_rate、chop_rate、recommended_action。 |
| Blocker复盘YYYY-MM-DD | 查看 replay_profile_negative / no_trade_lock / low_quality 等 blocker 分布 | `./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD` | low | 用于判断风控是否正确阻止负收益 profile。 |
| Shadow复盘YYYY-MM-DD | 查看 shadow_funnel_summary | `./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD` | low | 返回 shadow_candidate_count、shadow_verified_count、near_candidate_but_blocked、score_below_shadow_candidate 等。 |
| 学习复盘YYYY-MM-DD / 每日学习YYYY-MM-DD / 学习总结YYYY-MM-DD / 今日学习复盘YYYY-MM-DD | 汇总 data-quality、replay、profile、blocker、shadow、delivery audit 和 Telegram delivery 摘要 | `./scripts/hermes_cm_ops.sh learning-review --date YYYY-MM-DD` | low | 输出中文学习结论：数据是否有效、replay_source/replay_scope、avg/suppressed PnL、CANDIDATE 覆盖率、主要负收益 profile、主要 blocker、shadow_candidate/shadow_verified、LP signal rows 是否缺失、Telegram 噪音判断、今日结论和明天唯一改动点；不输出执行指令。 |
| CANDIDATE覆盖诊断YYYY-MM-DD / 候选覆盖诊断YYYY-MM-DD / 候选覆盖YYYY-MM-DD | 排查当日 signals -> trade_opportunities -> replay 的 CANDIDATE 覆盖连接 | `./scripts/hermes_cm_ops.sh candidate-coverage --date YYYY-MM-DD` | low | 只读 SQLite 和 daily_report；返回 signals 总数、opportunity 总数、状态分布、replay examples 关联数量、replay status=CANDIDATE 数量、delivery_audit 实时推送 stage/status 分布，并提示是 gate 严格还是 replay 关联层可能漏接。 |
| 日报结构检查YYYY-MM-DD / 日报schema检查YYYY-MM-DD / 报告结构检查YYYY-MM-DD | 检查 canonical daily report 新学习字段是否完整 | `./scripts/hermes_cm_ops.sh daily-report-schema-check --date YYYY-MM-DD` | low | 只读 daily_report 和 SQLite 聚合；检查 LP / CLMM / candidate frontier 字段，输出 report_mapping_missing / lp_analyzer_or_gate_missing / no_lp_samples_or_coverage_gap。 |
| Outcome闭环诊断YYYY-MM-DD / 后验闭环诊断YYYY-MM-DD / 结果闭环诊断YYYY-MM-DD | 排查当日 signals/opportunities -> outcomes/replay/profile 闭环不足 | `./scripts/hermes_cm_ops.sh outcome-diagnose --date YYYY-MM-DD` | low | 只读 SQLite 和 daily_report；返回 signals、trade_opportunities、outcomes、opportunity_outcomes、trade_replay_examples 总数及匹配率，按 asset/status/signal_type/stage 汇总 outcome 缺失，并推断时间窗口、price snapshot、ID 关联、worker、report mapping 问题。 |
| LP诊断YYYY-MM-DD / LP信号诊断YYYY-MM-DD / 池子诊断YYYY-MM-DD / CLMM诊断YYYY-MM-DD | 排查 daily report 中 LP signal rows 缺失的来源 | `./scripts/hermes_cm_ops.sh lp-diagnose --date YYYY-MM-DD` | low | 只读 daily_report 和 SQLite 聚合；输出 LP 相关字段存在性、signals/raw/parsed LP-like 计数、delivery_audit 推送/抑制数量、ETH/BTC/SOL x USDT/USDC major coverage，并判断 report mapping、LP analyzer/gate 或样本覆盖不足。 |
| 空间检查 | 提交后台任务查看 SQLite / WAL / archive / reports 占用 | `./scripts/hermes_cm_ops.sh submit-space-check` | low-medium | async job。只读，不删除，不 vacuum，不 prune。 |
| 空间快检 | 快速同步查看 SQLite/WAL/SHM 文件大小 | `./scripts/hermes_cm_ops.sh space-fast` | low | sync quick。不递归扫描 archive/reports。 |
| 数据库瘦身预检 / DB瘦身预检 / 数据库清理预检 | operational payload export dry-run 汇总 | `./scripts/hermes_cm_ops.sh db-slim-dry-run` | low-medium | sync dry-run。只显示 telegram_deliveries / delivery_audit candidate_rows 和 estimated_savings；不执行 export execute、VACUUM、compact、prune，不清 JSON payload，不写 archive。 |
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
- 不支持“学习复盘昨天”
- 不支持“候选覆盖昨天”
- 不支持“LP诊断昨天”
- 不支持“生成今天的日报”
- 不支持“重启监听器”
- 不支持“帮我运行 make run”
- 不支持“修改地址簿”
- 不支持“降低 VERIFIED 阈值”
- 不支持“db vacuum”
- 不支持“prune”
- 不支持“compact execute”
- 不支持“operational payload export execute”
- 不支持“删除 archive/cache/db”
- 不支持“delete DB”
- 不支持“修改地址簿”
- 不支持手动删除 `/run/lock/chain-monitor-hermes-*`
- 不支持用 `fuser` / `lsof` 后自行清理 lock
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
- 监听器体检：检查监听器是否停摆、最近数据时间、Telegram outbound 健康
- 空间检查：提交后台任务查看 DB / archive / reports 占用
- 空间快检：快速同步查看 SQLite / WAL 文件大小
- 数据库体积诊断：解释 DB / WAL / archive / reports 哪个大
- 数据库瘦身预检：只运行 operational payload export dry-run，显示候选行和预计节省

【日报流程】
- 标准日报流程YYYY-MM-DD：提交后台任务，展开 daily-close 子步骤 + full replay + report + compare + checkpoint
- 生成日报YYYY-MM-DD：生成 canonical 日报
- 分析报告YYYY-MM-DD：分析已存在日报
- 深度分析报告YYYY-MM-DD：深度分析已存在日报
- 生成摘要YYYY-MM-DD 快速：生成快速分析输入包
- 生成摘要YYYY-MM-DD 深度：生成深度分析输入包
- 检查回放YYYY-MM-DD：确认 replay_source=persisted、scope=full
- 数据质量YYYY-MM-DD：判断该日是否有效

【后验复盘】
- Profile复盘YYYY-MM-DD
- Blocker复盘YYYY-MM-DD
- Shadow复盘YYYY-MM-DD
- 学习复盘YYYY-MM-DD：整合数据质量、回放、profile、blocker、shadow、Telegram 去噪，输出中文学习结论
- CANDIDATE覆盖诊断YYYY-MM-DD：排查 replay 中 CANDIDATE 覆盖率为 0 的原因
- Outcome闭环诊断YYYY-MM-DD：排查 outcome/replay/profile 闭环不足的原因
- LP诊断YYYY-MM-DD：排查 daily_report 中 LP signal rows 缺失的 report/analyzer/gate 链路

【维护预检】
- 归档压缩预检YYYY-MM-DD：提交后台 dry-run 任务，不压缩

【周复盘】
- 周复盘START到END：提交后台任务，例如：周复盘2026-04-27到2026-05-03

【后台任务 / 诊断】
- 任务状态JOB_ID
- 查看结果JOB_ID
- 查看日志JOB_ID
- 诊断任务JOB_ID
- 最近任务
- 锁状态：只读检查 Hermes lock 是否被占用，不删除 lock 文件
- 取消任务JOB_ID 我确认取消

规则：
- 日期必须用 YYYY-MM-DD
- 不支持 今天/昨天/前天 自动执行
- 标准日报流程只能跑已经结束的北京时间逻辑日
- 不支持当前北京时间日期；例如北京时间 2026-05-03 还没结束时，不能跑 标准日报流程2026-05-03
- 请在次日 00:05 后执行标准日报流程YYYY-MM-DD
- 长任务会返回 job_id
- 标准日报流程、空间检查、归档压缩预检、周复盘是后台任务
- 数据库瘦身预检只能 dry-run
- 输出默认脱敏
- 不提供交易建议
- 不开放 vacuum / prune / compact execute / export execute / delete DB / 修改地址簿
```
