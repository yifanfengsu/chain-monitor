# Telegram 中文控制参考

所有中文 Telegram 命令必须先经过 `~/.hermes/bin/chain-monitor-cn-router`。

本文件是 router grammar 参考，不是让模型手动执行命令。`references` 里的 mapping 是 router 的 grammar，不是让模型手动映射执行；不是让模型手动把中文翻译成 shell。

“命令提示”用于返回所有功能菜单。

相对日期必须拒绝。不得先运行 `date -u`，不得运行 `TZ=Asia/Shanghai date`，不得把 今天/昨天/前天/今日/昨日/today/yesterday/tomorrow 转换成具体日期。

标准日报流程只能跑已经结束的北京时间逻辑日，不支持当前北京时间日期。例如北京时间 2026-05-03 还没结束时，不能跑 `标准日报流程2026-05-03`；请在次日 00:05 后执行同一命令。

控制路径：

```text
Telegram -> Hermes gateway -> /chain-monitor-report-analyst -> ~/.hermes/bin/chain-monitor-cn-router -> scripts/hermes_cm_cn_router.py -> validated ./scripts/hermes_cm_ops.sh argv
```

只有 router 允许后，router 才能设置 `HERMES_OPS_ROUTER_OK=1` 并调用 wrapper。模型不得直接把中文输入映射到 `./scripts/hermes_cm_ops.sh`，不得从 Telegram 文本直接运行 raw shell，不得直接调用 make。

Hermes lock 硬规则：

- 不得手动删除 `/run/lock/chain-monitor-hermes-*`。
- 不得对 Hermes lock 文件执行 `rm -f`。
- 不得用 `fuser` / `lsof` 判断后自行清理 lock。
- 遇到 `lock_busy` 只能调用：`锁状态`、`最近任务`、`任务状态JOB_ID`。
- lock 清理只能由 wrapper 内部安全逻辑处理；当前不提供 Telegram 清理锁命令。

## 中文输入 -> router intent -> cm_ops wrapper argv

| 中文输入 | router intent | cm_ops wrapper argv | 风险等级 | 执行模式 |
| --- | --- | --- | --- | --- |
| 命令提示 / 功能列表 / 菜单 / 帮助 / 命令 / 怎么用 / 使用说明 | command-menu | `./scripts/hermes_cm_ops.sh command-menu` | low | sync quick |
| 锁状态 / 锁检查 / Hermes锁状态 | lock-status | `./scripts/hermes_cm_ops.sh lock-status` | low | sync quick；只读检查 Hermes lock 是否被占用，不删除 lock 文件 |
| 系统体检 / 体检 / 健康检查 / 系统状态 / 状态 | system-health | `./scripts/hermes_cm_ops.sh system-health` | low | sync |
| 监听器体检 / 监听器检查 / 监听状态 / 监听器状态 / 最近数据 / 零活动排查 | listener-health | `./scripts/hermes_cm_ops.sh listener-health` | low | sync |
| 标准日报流程YYYY-MM-DD / 跑标准日报流程YYYY-MM-DD / 每日标准流程YYYY-MM-DD / 日常报告流程YYYY-MM-DD | submit-daily-flow | `./scripts/hermes_cm_ops.sh submit-daily-flow --date YYYY-MM-DD` | medium | async job；当前北京时间日期会拒绝且不创建 job |
| 分析报告YYYY-MM-DD / 快速分析报告YYYY-MM-DD / 日报分析YYYY-MM-DD / 报告分析YYYY-MM-DD | analyze fast | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast` | low-medium | sync |
| 检查回放YYYY-MM-DD / 回放检查YYYY-MM-DD / 回放摘要YYYY-MM-DD / 检查replayYYYY-MM-DD / Replay检查YYYY-MM-DD | replay-check | `./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD` | low | sync quick |
| 数据质量YYYY-MM-DD / 报告是否有效YYYY-MM-DD / 检查数据质量YYYY-MM-DD / 异常摘要YYYY-MM-DD | data-quality | `./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD` | low | sync quick |
| Profile复盘YYYY-MM-DD / profile复盘YYYY-MM-DD / 画像复盘YYYY-MM-DD / 后验画像YYYY-MM-DD | profile-review | `./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD` | low | sync quick |
| Blocker复盘YYYY-MM-DD / blocker复盘YYYY-MM-DD / 阻断复盘YYYY-MM-DD / 风控阻断复盘YYYY-MM-DD | blocker-review | `./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD` | low | sync quick |
| Shadow复盘YYYY-MM-DD / shadow复盘YYYY-MM-DD / 影子复盘YYYY-MM-DD / Shadow Funnel复盘YYYY-MM-DD / 影子漏斗YYYY-MM-DD | shadow-review | `./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD` | low | sync quick |
| 学习复盘YYYY-MM-DD / 每日学习YYYY-MM-DD / 学习总结YYYY-MM-DD / 今日学习复盘YYYY-MM-DD | learning-review | `./scripts/hermes_cm_ops.sh learning-review --date YYYY-MM-DD` | low | sync quick；整合 data-quality、replay、profile、blocker、shadow、delivery audit 和 Telegram delivery 摘要 |
| CANDIDATE覆盖诊断YYYY-MM-DD / 候选覆盖诊断YYYY-MM-DD / 候选覆盖YYYY-MM-DD | candidate-coverage | `./scripts/hermes_cm_ops.sh candidate-coverage --date YYYY-MM-DD` | low | sync quick；只读 SQLite 和 daily_report，排查 CANDIDATE 覆盖率为 0 是 gate 结果还是 replay 关联层漏接 |
| 日报结构检查YYYY-MM-DD / 日报schema检查YYYY-MM-DD / 报告结构检查YYYY-MM-DD | daily-report-schema-check | `./scripts/hermes_cm_ops.sh daily-report-schema-check --date YYYY-MM-DD` | low | sync quick；只读 daily_report 和 SQLite 聚合，检查 LP / CLMM / candidate frontier 字段完整性 |
| Outcome闭环诊断YYYY-MM-DD / 后验闭环诊断YYYY-MM-DD / 结果闭环诊断YYYY-MM-DD | outcome-diagnose | `./scripts/hermes_cm_ops.sh outcome-diagnose --date YYYY-MM-DD` | low | sync quick；只读 SQLite 和 daily_report，排查 signals/opportunities -> outcomes/replay/profile 闭环不足 |
| Outcome补全预检YYYY-MM-DD / 后验补全预检YYYY-MM-DD | outcome-catchup | `./scripts/hermes_cm_ops.sh outcome-catchup --date YYYY-MM-DD --dry-run` | low | sync dry-run；Telegram 只允许预检，不开放 execute |
| LP抑制抽样预检YYYY-MM-DD / LP抽样预检YYYY-MM-DD / LP suppression抽样预检YYYY-MM-DD | lp-suppression-sample-replay | `./scripts/hermes_cm_ops.sh lp-suppression-sample-replay --date YYYY-MM-DD --dry-run` | low | sync dry-run；Telegram 只允许预检，不开放 execute |
| LP诊断YYYY-MM-DD / LP信号诊断YYYY-MM-DD / 池子诊断YYYY-MM-DD / CLMM诊断YYYY-MM-DD | lp-diagnose | `./scripts/hermes_cm_ops.sh lp-diagnose --date YYYY-MM-DD` | low | sync quick；只读 SQLite 和 daily_report，排查 LP signal rows 缺失是 report mapping、解析/gate 还是扫描覆盖不足 |
| 空间检查 / 磁盘检查 / VPS空间检查 / 数据库空间检查 / DB空间检查 | submit-space-check | `./scripts/hermes_cm_ops.sh submit-space-check` | low-medium | async job |
| 空间快检 / 快速空间检查 / 快速磁盘检查 | space-fast | `./scripts/hermes_cm_ops.sh space-fast` | low | sync quick |
| 数据库体积诊断 / DB体积诊断 / 数据库为什么大 | db-size-diagnose | `./scripts/hermes_cm_ops.sh db-size-diagnose` | low | sync quick |
| 数据库瘦身预检 / DB瘦身预检 / 数据库清理预检 | db-slim-dry-run | `./scripts/hermes_cm_ops.sh db-slim-dry-run` | low-medium | sync dry-run |
| 归档压缩预检YYYY-MM-DD / 压缩预检YYYY-MM-DD / archive压缩预检YYYY-MM-DD / Archive压缩预检YYYY-MM-DD | submit-archive-compress-check | `./scripts/hermes_cm_ops.sh submit-archive-compress-check --date YYYY-MM-DD` | low-medium | async job |
| 周复盘START到END / 周复盘 START 到 END / 周度复盘START到END / 每周复盘START到END | submit-weekly-review | `./scripts/hermes_cm_ops.sh submit-weekly-review --start YYYY-MM-DD --end YYYY-MM-DD` | medium | async job |
| 任务状态JOB_ID / 查询任务JOB_ID / 任务进度JOB_ID | job-status | `./scripts/hermes_cm_ops.sh job-status --job-id JOB_ID` | low | sync quick |
| 查看结果JOB_ID / 任务结果JOB_ID / 读取结果JOB_ID | job-result | `./scripts/hermes_cm_ops.sh job-result --job-id JOB_ID` | low | sync quick |
| 查看日志JOB_ID / 任务日志JOB_ID | job-log | `./scripts/hermes_cm_ops.sh job-log --job-id JOB_ID` | low | sync quick |
| 诊断任务JOB_ID / 任务诊断JOB_ID | job-diagnose | `./scripts/hermes_cm_ops.sh job-diagnose --job-id JOB_ID` | low | sync quick |
| 最近任务 / 任务列表 / 查看任务 | job-list | `./scripts/hermes_cm_ops.sh job-list` | low | sync quick |
| 取消任务JOB_ID 我确认取消 | job-cancel | `./scripts/hermes_cm_ops.sh job-cancel --job-id JOB_ID --confirm` | medium | sync quick |

## 保留 grammar

| 中文输入 | router intent | cm_ops wrapper argv | 风险等级 |
| --- | --- | --- | --- |
| 生成日报YYYY-MM-DD / 生成报告YYYY-MM-DD / 生成每日报告YYYY-MM-DD | report | `./scripts/hermes_cm_ops.sh report --date YYYY-MM-DD` | low-medium |
| 深度分析报告YYYY-MM-DD / 深度复盘YYYY-MM-DD / 深度分析YYYY-MM-DD | analyze deep | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode deep` | medium |
| 生成摘要YYYY-MM-DD 快速 / 生成输入包YYYY-MM-DD 快速 / 生成分析输入包YYYY-MM-DD 快速 | digest fast | `./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD --mode fast` | low |
| 生成摘要YYYY-MM-DD 深度 / 生成输入包YYYY-MM-DD 深度 / 生成分析输入包YYYY-MM-DD 深度 | digest deep | `./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD --mode deep` | medium |
| 构建并分析报告YYYY-MM-DD 快速 / 自动构建并分析YYYY-MM-DD 快速 / 自动生成并分析YYYY-MM-DD 快速 | analyze fast auto-build | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast --auto-build` | medium |
| 构建并分析报告YYYY-MM-DD 深度 / 自动构建并分析YYYY-MM-DD 深度 / 自动生成并分析YYYY-MM-DD 深度 | analyze deep auto-build | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode deep --auto-build` | medium-high |
| 每日收尾YYYY-MM-DD 我确认压缩 | close | `./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress` | high |

“每日收尾”如果绑定压缩，不在新菜单中列为每日默认流程。新增“标准日报流程”才是推荐手动流程。

兼容说明：`daily-flow`、`space-check`、`archive-compress-check`、`weekly-review` 仍是 wrapper 内部/local CLI 逻辑名称，但 Telegram grammar 必须映射到 `submit-*` 后台任务，不得同步执行。

## Grammar Details

- 所有可执行命令必须使用绝对日期 `YYYY-MM-DD`。
- 日期必须用 Python `datetime.date.fromisoformat` 校验真实日期。
- 标准日报流程只能跑已经结束的北京时间逻辑日，不支持当前北京时间日期；submit 阶段会返回 `current_beijing_date_protected`。
- 支持日期与中文命令之间无空格，例如 `分析报告2026-05-01`。
- 支持日期与中文命令之间有空格，例如 `分析报告 2026-05-01`。
- 周复盘使用 `START到END`，例如 `周复盘2026-04-27到2026-05-03`。
- 周复盘 START / END 必须是 `YYYY-MM-DD`，START <= END，范围最多 14 天。
- 学习复盘只接受绝对日期，例如 `学习复盘2026-05-04` 或 `学习总结2026-05-04`；`学习复盘昨天` 和 `学习总结昨天` 必须拒绝。
- CANDIDATE 覆盖诊断只接受绝对日期，例如 `CANDIDATE覆盖诊断2026-05-04`；`候选覆盖昨天` 必须拒绝。
- Outcome 闭环诊断只接受绝对日期，例如 `Outcome闭环诊断2026-05-04`；`后验闭环诊断昨天` 必须拒绝。
- Outcome 补全预检只接受绝对日期，例如 `Outcome补全预检2026-05-04`；只映射 dry-run。
- LP 抑制抽样预检只接受绝对日期，例如 `LP抑制抽样预检2026-05-04`；只映射 dry-run。
- LP 诊断只接受绝对日期，例如 `LP诊断2026-05-04`；`LP诊断昨天` 必须拒绝。
- job_id 必须匹配 `cmjob_YYYYMMDDTHHMMSSZ_<hex>`。
- 取消任务必须包含“我确认取消”。
- `生成摘要2026-05-01` 没有 快速/深度，必须拒绝并要求指定模式。
- `数据库瘦身预检` 只能映射到 `db-slim-dry-run`，只做 operational payload export dry-run。
- 普通“分析报告YYYY-MM-DD”不得自动生成日报，不得自动 auto-build。
- 只有“构建并分析报告YYYY-MM-DD 快速/深度”才允许 auto-build。
- close 只能由“每日收尾YYYY-MM-DD 我确认压缩”触发。
- “每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法。
- `lock_busy` 只能引导用户使用 `锁状态`、`最近任务`、`任务状态JOB_ID`，不得提示删除 lock 文件。

## Forbidden Relative Date Examples

这些相对日期必须拒绝，refused_reason=`relative_date_forbidden`，不允许 date 命令，不允许 report/analyze/digest/close/daily-flow，不允许读取 reports：

- 分析昨天的报告
- 学习复盘昨天
- 候选覆盖昨天
- Outcome闭环诊断昨天
- LP诊断昨天
- 生成今天的日报
- 标准日报流程昨天
- 周复盘上周
- 把今天 close 掉
- 分析前天数据
- 生成今日摘要
- analyze yesterday

拒绝回复：

```text
⚠️ 请使用绝对日期 YYYY-MM-DD。
示例：分析报告2026-05-01
我不会根据“今天/昨天/前天”自动执行。
```

## 中文拒绝示例

Skill 必须拒绝或要求改写：

- 帮我运行 make run
- 重启监听器
- systemctl restart chain-monitor
- 修改地址簿
- 修改 LP 池
- 修改 .env
- 降低 VERIFIED 阈值
- db vacuum
- prune
- compact execute
- operational payload export execute
- 删除 archive/cache/db
- delete DB
- 修改地址簿
- 把地址簿发给我
- 把真实监控地址发出来
- 给我买入卖出建议
- 这个币能不能买
- 给我仓位、杠杆、止损、止盈
- 执行 bash -c ...

## 中文 Telegram 回复模板

成功：

```text
✅ 已完成：{action}
request_id：{request_id}
结果文件：{safe_path}
输出已脱敏。
```

后台任务提交成功：

```text
✅ 已提交后台任务：标准日报流程
job_id：cmjob_...
查询进度：任务状态cmjob_...
查看结果：查看结果cmjob_...
查看日志：查看日志cmjob_...
失败诊断：诊断任务cmjob_...
说明：该任务会在 VPS 后台继续执行，Telegram 不需要等待。
```

拒绝：

```text
❌ 已拒绝：{reason}
请发送“命令提示”查看固定中文菜单。
```

需要绝对日期：

```text
⚠️ 请使用绝对日期 YYYY-MM-DD。
示例：分析报告2026-05-01
我不会根据“今天/昨天/前天”自动执行。
```

需要模式：

```text
⚠️ 请指定“快速”或“深度”。
示例：生成摘要2026-05-01 快速
```

## 命令提示菜单

```text
📋 Chain Monitor 中文命令菜单

【每日检查】
- 系统体检：检查 DB / report source / market / coverage
- 监听器体检：检查监听器是否停摆、最近数据时间
- 空间检查：提交后台任务查看 DB / archive / reports 占用
- 空间快检：快速同步查看 SQLite / WAL / SHM 文件大小
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
- 学习复盘YYYY-MM-DD
- CANDIDATE覆盖诊断YYYY-MM-DD
- LP诊断YYYY-MM-DD

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

## 端到端测试顺序

1. Telegram 中发送 `/reset`。
2. 发送 `/chain-monitor-report-analyst 命令提示`。
3. 发送 `/chain-monitor-report-analyst 分析昨天的报告`。
   预期：拒绝，不运行 date，不执行 report/analyze/digest/close，不读取 reports。
4. 发送 `/chain-monitor-report-analyst 标准日报流程2026-05-01`。
   预期：router 映射到 submit-daily-flow，立即返回 job_id。
5. 发送 `/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03`。
   预期：router 映射到 submit-weekly-review，立即返回 job_id。
6. 发送 `/chain-monitor-report-analyst 学习复盘2026-05-04`。
   预期：router 映射到 learning-review，同步返回中文学习结论，不包含执行指令。
7. 发送 `/chain-monitor-report-analyst CANDIDATE覆盖诊断2026-05-04`。
   预期：router 映射到 candidate-coverage，同步返回 status 分布和 replay/opportunity 连接情况。
8. 发送 `/chain-monitor-report-analyst LP诊断2026-05-04`。
   预期：router 映射到 lp-diagnose，同步返回 LP 字段存在性、SQLite LP-like 计数、report mapping 判断。
9. 发送 `/chain-monitor-report-analyst 学习复盘昨天`。
   预期：拒绝 relative_date_forbidden，不读取 reports。
10. 发送 `/chain-monitor-report-analyst 任务状态cmjob_...`。
   预期：router 映射到 job-status。
11. 如果任务 failed，发送 `/chain-monitor-report-analyst 诊断任务cmjob_...`。
   预期：router 映射到 job-diagnose，只读返回 failed_substep 和 failed_command。

## 标准日报流程失败诊断

- 标准日报流程的 daily-close 阶段必须展开子步骤，不得只把 `make daily-close DATE=YYYY-MM-DD` 当成黑盒。
- daily-close 默认只是 compression dry-run，不真正压缩 archive。
- 只有 `COMPRESS=YES CONFIRM=YES` 才执行实际 archive gzip。
- 标准日报流程 failed 时，先使用 `诊断任务JOB_ID` 和 `查看日志JOB_ID`。
- 不要因为 daily-close failure 就压缩 archive。
- “每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法。

## 安全处理要点

- 所有中文 Telegram 请求必须先匹配 router 固定 grammar，再由 router 映射到 wrapper 命令。
- 不匹配 grammar 的请求不得执行。
- Wrapper 命令列只能由 router 生成。
- Telegram 中文请求不得直接调用 `make`、`raw shell`、`systemctl`、`bash -c`、`sh -c`、`python -c`、`rm`、`kill`、`vacuum`、`prune`、`compact execute`、`export execute`、`delete DB`。
- 不开放 listener restart / stop / kill。
- 不开放修改地址簿、LP 池、阈值、`.env`、token、RPC。
- 输出必须中文、简短、Telegram-friendly，并且不给交易建议。
