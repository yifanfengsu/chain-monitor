# Telegram 中文控制参考

所有中文 Telegram 命令必须先经过 `~/.hermes/bin/chain-monitor-cn-router`。

本文件是 router grammar 参考，不是让模型手动执行命令。`references` 里的 mapping 是 router 的 grammar，不是让模型手动映射执行；不是让模型手动把中文翻译成 shell。

“命令提示”用于返回所有功能菜单。

相对日期必须拒绝。不得先运行 `date -u`，不得运行 `TZ=Asia/Shanghai date`，不得把 今天/昨天/前天/今日/昨日/today/yesterday/tomorrow 转换成具体日期。

控制路径：

```text
Telegram -> Hermes gateway -> /chain-monitor-report-analyst -> ~/.hermes/bin/chain-monitor-cn-router -> scripts/hermes_cm_cn_router.py -> validated ./scripts/hermes_cm_ops.sh argv
```

只有 router 允许后，router 才能设置 `HERMES_OPS_ROUTER_OK=1` 并调用 wrapper。模型不得直接把中文输入映射到 `./scripts/hermes_cm_ops.sh`，不得从 Telegram 文本直接运行 raw shell，不得直接调用 make。

## 中文输入 -> router intent -> cm_ops wrapper argv

| 中文输入 | router intent | cm_ops wrapper argv | 风险等级 |
| --- | --- | --- | --- |
| 命令提示 / 功能列表 / 菜单 / 帮助 / 命令 / 怎么用 / 使用说明 | command-menu | `./scripts/hermes_cm_ops.sh command-menu` | low |
| 系统体检 / 体检 / 健康检查 / 系统状态 / 状态 | system-health | `./scripts/hermes_cm_ops.sh system-health` | low |
| 监听器体检 / 监听器检查 / 监听状态 / 监听器状态 / 最近数据 / 零活动排查 | listener-health | `./scripts/hermes_cm_ops.sh listener-health` | low |
| 标准日报流程YYYY-MM-DD / 跑标准日报流程YYYY-MM-DD / 每日标准流程YYYY-MM-DD / 日常报告流程YYYY-MM-DD | daily-flow | `./scripts/hermes_cm_ops.sh daily-flow --date YYYY-MM-DD` | medium |
| 分析报告YYYY-MM-DD / 快速分析报告YYYY-MM-DD / 日报分析YYYY-MM-DD / 报告分析YYYY-MM-DD | analyze fast | `./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast` | low-medium |
| 检查回放YYYY-MM-DD / 回放检查YYYY-MM-DD / 回放摘要YYYY-MM-DD / 检查replayYYYY-MM-DD / Replay检查YYYY-MM-DD | replay-check | `./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD` | low |
| 数据质量YYYY-MM-DD / 报告是否有效YYYY-MM-DD / 检查数据质量YYYY-MM-DD / 异常摘要YYYY-MM-DD | data-quality | `./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD` | low |
| Profile复盘YYYY-MM-DD / profile复盘YYYY-MM-DD / 画像复盘YYYY-MM-DD / 后验画像YYYY-MM-DD | profile-review | `./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD` | low |
| Blocker复盘YYYY-MM-DD / blocker复盘YYYY-MM-DD / 阻断复盘YYYY-MM-DD / 风控阻断复盘YYYY-MM-DD | blocker-review | `./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD` | low |
| Shadow复盘YYYY-MM-DD / shadow复盘YYYY-MM-DD / 影子复盘YYYY-MM-DD / Shadow Funnel复盘YYYY-MM-DD / 影子漏斗YYYY-MM-DD | shadow-review | `./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD` | low |
| 空间检查 / 磁盘检查 / VPS空间检查 / 数据库空间检查 / DB空间检查 | space-check | `./scripts/hermes_cm_ops.sh space-check` | low |
| 归档压缩预检YYYY-MM-DD / 压缩预检YYYY-MM-DD / archive压缩预检YYYY-MM-DD / Archive压缩预检YYYY-MM-DD | archive-compress-check | `./scripts/hermes_cm_ops.sh archive-compress-check --date YYYY-MM-DD` | low-medium |
| 周复盘START到END / 周复盘 START 到 END / 周度复盘START到END / 每周复盘START到END | weekly-review | `./scripts/hermes_cm_ops.sh weekly-review --start YYYY-MM-DD --end YYYY-MM-DD` | medium |

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

## Grammar Details

- 所有可执行命令必须使用绝对日期 `YYYY-MM-DD`。
- 日期必须用 Python `datetime.date.fromisoformat` 校验真实日期。
- 支持日期与中文命令之间无空格，例如 `分析报告2026-05-01`。
- 支持日期与中文命令之间有空格，例如 `分析报告 2026-05-01`。
- 周复盘使用 `START到END`，例如 `周复盘2026-04-27到2026-05-03`。
- 周复盘 START / END 必须是 `YYYY-MM-DD`，START <= END，范围最多 14 天。
- `生成摘要2026-05-01` 没有 快速/深度，必须拒绝并要求指定模式。
- 普通“分析报告YYYY-MM-DD”不得自动生成日报，不得自动 auto-build。
- 只有“构建并分析报告YYYY-MM-DD 快速/深度”才允许 auto-build。
- close 只能由“每日收尾YYYY-MM-DD 我确认压缩”触发。

## Forbidden Relative Date Examples

这些相对日期必须拒绝，refused_reason=`relative_date_forbidden`，不允许 date 命令，不允许 report/analyze/digest/close/daily-flow，不允许读取 reports：

- 分析昨天的报告
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
- 删除 archive/cache/db
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

## 端到端测试顺序

1. Telegram 中发送 `/reset`。
2. 发送 `/chain-monitor-report-analyst 命令提示`。
3. 发送 `/chain-monitor-report-analyst 分析昨天的报告`。
   预期：拒绝，不运行 date，不执行 report/analyze/digest/close，不读取 reports。
4. 发送 `/chain-monitor-report-analyst 标准日报流程2026-05-01`。
   预期：router 映射到 daily-flow，并由 wrapper 执行固定流程。
5. 发送 `/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03`。
   预期：router 映射到 weekly-review。

## 安全处理要点

- 所有中文 Telegram 请求必须先匹配 router 固定 grammar，再由 router 映射到 wrapper 命令。
- 不匹配 grammar 的请求不得执行。
- Wrapper 命令列只能由 router 生成。
- Telegram 中文请求不得直接调用 `make`、`raw shell`、`systemctl`、`bash -c`、`sh -c`、`python -c`、`rm`、`kill`、`vacuum`、`prune`、`compact execute`。
- 不开放 listener restart / stop / kill。
- 不开放修改地址簿、LP 池、阈值、`.env`、token、RPC。
- 输出必须中文、简短、Telegram-friendly，并且不给交易建议。
