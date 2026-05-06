# Telegram 中文控制补充参考

本文件补充仓库根目录下的 Telegram 控制索引；完整 Skill 参考见 `hermes_skills/chain-monitor-report-analyst/references/telegram_control.md`。

新增学习复盘、覆盖诊断、Outcome 闭环诊断和 LP 诊断命令：

| 中文输入 | router intent | cm_ops wrapper argv | 说明 |
| --- | --- | --- | --- |
| 学习复盘YYYY-MM-DD / 每日学习YYYY-MM-DD / 学习总结YYYY-MM-DD / 今日学习复盘YYYY-MM-DD | learning-review | `./scripts/hermes_cm_ops.sh learning-review --date YYYY-MM-DD` | 汇总 data-quality、replay-check、profile-review、blocker-review、shadow-review、delivery audit 和 Telegram delivery 摘要，输出中文学习结论。 |
| CANDIDATE覆盖诊断YYYY-MM-DD / 候选覆盖诊断YYYY-MM-DD / 候选覆盖YYYY-MM-DD | candidate-coverage | `./scripts/hermes_cm_ops.sh candidate-coverage --date YYYY-MM-DD` | 只读 SQLite 和 daily_report，输出 signals/opportunity/replay/delivery_audit 聚合，判断 CANDIDATE 为 0 是 gate 结果还是 replay 关联层可能漏接。 |
| 日报结构检查YYYY-MM-DD / 日报schema检查YYYY-MM-DD / 报告结构检查YYYY-MM-DD | daily-report-schema-check | `./scripts/hermes_cm_ops.sh daily-report-schema-check --date YYYY-MM-DD` | 只读 daily_report 和 SQLite 聚合，检查 LP / CLMM / candidate frontier 字段是否完整，并输出 `report_mapping_missing` 等结构原因。 |
| Outcome闭环诊断YYYY-MM-DD / 后验闭环诊断YYYY-MM-DD / 结果闭环诊断YYYY-MM-DD | outcome-diagnose | `./scripts/hermes_cm_ops.sh outcome-diagnose --date YYYY-MM-DD` | 只读 SQLite 和 daily_report，输出 signals/opportunities/outcomes/opportunity_outcomes/replay 匹配率，按 asset/status/signal_type/stage 汇总 outcome 缺失并推断闭环不足原因。 |
| LP诊断YYYY-MM-DD / LP信号诊断YYYY-MM-DD / 池子诊断YYYY-MM-DD / CLMM诊断YYYY-MM-DD | lp-diagnose | `./scripts/hermes_cm_ops.sh lp-diagnose --date YYYY-MM-DD` | 只读 SQLite 和 daily_report，输出 LP 字段存在性、signals/raw/parsed LP-like 计数、delivery_audit 推送/抑制数量和 major coverage，判断是 report mapping、LP analyzer/gate 还是样本覆盖不足。 |

规则：

- 只接受绝对日期 `YYYY-MM-DD`。
- `学习复盘昨天`、`每日学习昨天`、`学习总结昨天`、`今日学习复盘昨天` 必须拒绝，refused_reason=`relative_date_forbidden`。
- `候选覆盖昨天`、`CANDIDATE覆盖诊断昨天` 必须拒绝，refused_reason=`relative_date_forbidden`。
- `日报结构检查昨天`、`日报schema检查昨天`、`报告结构检查昨天` 必须拒绝，refused_reason=`relative_date_forbidden`。
- `Outcome闭环诊断昨天`、`后验闭环诊断昨天`、`结果闭环诊断昨天` 必须拒绝，refused_reason=`relative_date_forbidden`。
- `LP诊断昨天`、`LP信号诊断昨天`、`池子诊断昨天`、`CLMM诊断昨天` 必须拒绝，refused_reason=`relative_date_forbidden`。
- candidate-coverage wrapper 不直接执行 `make`；只读取 SQLite 和 canonical daily report 的聚合字段。
- daily-report-schema-check wrapper 不直接执行 `make`；只读取 SQLite 和 canonical daily report 的聚合字段。
- outcome-diagnose wrapper 不直接执行 `make`；只读取 SQLite 和 canonical daily report 的聚合字段。
- lp-diagnose wrapper 不直接执行 `make`；只读取 SQLite 和 canonical daily report 的聚合字段。
- 输出不包含执行指令。
