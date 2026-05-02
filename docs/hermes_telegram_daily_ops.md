# Chain Monitor 的 Hermes Telegram 中文日常操作手册

## 说明

推荐控制路径：

```text
Telegram -> Hermes gateway -> /chain-monitor-report-analyst -> ~/.hermes/bin/chain-monitor-cn-router -> scripts/hermes_cm_cn_router.py -> scripts/hermes_cm_ops.sh
```

不要为 inbound control 额外运行新的 Telegram bot。项目的 `notifier.py` 保持 outbound-only，用于系统主动推送链上告警。

详细部署验收见 `docs/hermes_telegram_e2e_deployment.md`。安装或更新 skill 后，必须在 Telegram 中发送 `/reset`，否则 gateway 会话可能继续使用旧 skill。中文命令实际入口是 `~/.hermes/bin/chain-monitor-cn-router`。

## Telegram Setup Checklist

- `TELEGRAM_BOT_TOKEN` 已在 `~/.hermes/.env` 设置。
- `TELEGRAM_ALLOWED_USERS` 或 `GATEWAY_ALLOWED_USERS` 已设置。
- 不要设置 `GATEWAY_ALLOW_ALL_USERS=true`。
- `approvals.mode` 应为 `manual` 或 `smart`，不应为 `off`。
- 不使用 `/yolo`。
- 不设置 `HERMES_YOLO_MODE`。
- 群组中建议 `telegram.require_mention: true`。
- skill 已安装到 `~/.hermes/skills/chain-monitor-report-analyst`，或通过 `skills.external_dirs` 暴露。
- `~/.hermes/bin/chain-monitor-cn-router` 已存在并可执行。
- gateway 正在运行。
- Telegram 中已经发送 `/reset`。

## 每日中文命令

直接复制到 Telegram 使用：

```text
/chain-monitor-report-analyst 命令提示
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 分析报告2026-05-01
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
/chain-monitor-report-analyst Profile复盘2026-05-01
/chain-monitor-report-analyst Blocker复盘2026-05-01
/chain-monitor-report-analyst Shadow复盘2026-05-01
/chain-monitor-report-analyst 空间检查
/chain-monitor-report-analyst 归档压缩预检2026-05-01
/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03
/chain-monitor-report-analyst 生成日报2026-05-01
/chain-monitor-report-analyst 深度分析报告2026-05-01
/chain-monitor-report-analyst 生成摘要2026-05-01 快速
/chain-monitor-report-analyst 生成摘要2026-05-01 深度
/chain-monitor-report-analyst 构建并分析报告2026-05-01 快速
```

日期必须是绝对 `YYYY-MM-DD`。不要发送“今天”“昨天”“前天”这类相对日期。

## 手动控制菜单

发送：

```text
/chain-monitor-report-analyst 命令提示
```

预期返回完整中文菜单。

12 个手动功能：

- 系统体检：检查 SQLite / report source / market / coverage。
- 监听器体检：检查 listener 进程、最近 raw/parsed/signal/archive 时间和 zero activity 风险。
- 标准日报流程YYYY-MM-DD：daily-close + trade-replay-full + report-daily-date + daily-compare + sqlite-checkpoint。
- 分析报告YYYY-MM-DD：分析已存在 canonical daily report，不自动生成日报。
- 检查回放YYYY-MM-DD：确认 replay_source=persisted、replay_scope=full。
- 数据质量YYYY-MM-DD：判断该日是否适合策略质量分析。
- Profile复盘YYYY-MM-DD：查看 profile 后验。
- Blocker复盘YYYY-MM-DD：查看 blocker 分布。
- Shadow复盘YYYY-MM-DD：查看 shadow funnel。
- 空间检查：查看 SQLite / WAL / archive / reports 占用。
- 归档压缩预检YYYY-MM-DD：只 dry-run，不压缩。
- 周复盘START到END：最多 14 天的只读聚合复盘。

每日建议流程：

```text
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 分析报告2026-05-01
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
```

不支持相对日期：今天、昨天、前天、今日、昨日、today、yesterday 都会被拒绝。

Telegram 当前不开放：

- 不开放重启/停止/kill listener。
- 重启 / 停止 / kill listener。
- 删除 archive/cache/db。
- vacuum / prune / compact execute。
- archive-compress-date 真正压缩。
- 修改地址簿、LP 池、阈值、`.env`、token、RPC。
- 买入/卖出、仓位、杠杆、止盈止损建议。

检查 audit log：

```bash
tail -n 20 reports/hermes/ops_audit.ndjson
```

## 为什么不用相对日期

Telegram 控制里的执行型命令必须使用 `YYYY-MM-DD`，不允许“今天”“昨天”“前天”“today”“yesterday”。

原因：

- 防止 Telegram 控制在时区、跨日、缓存/归档边界上误执行。
- 防止模型把相对日期自由解释成错误日期。
- 防止报告缺失时被自动生成或自动分析。

错误示例：

```text
/chain-monitor-report-analyst 分析昨天的报告
```

预期：拒绝，不执行。

如果相对日期没有被拒绝，说明新 skill 未生效或 router 被绕过。请重新运行 `scripts/install_hermes_skill.sh`，然后在 Telegram 中发送 `/reset`。

正确示例：

```text
/chain-monitor-report-analyst 分析报告2026-05-02
```

## 高风险中文命令

```text
/chain-monitor-report-analyst 每日收尾2026-05-01 我确认压缩
```

- 每日收尾是 high-risk。
- 每日收尾不属于每日默认流程。
- 必须使用绝对日期。
- 必须包含精确确认短语“我确认压缩”。
- 当前 UTC 日期应由 wrapper 默认拒绝。
- skill 不应建议 `--allow-today`。

## 被拒绝示例

- 分析昨天的报告
- 生成今天的日报
- 把今天的 close 掉
- 帮我运行 make run
- 重启一下系统
- 修改 .env
- 把真实监控地址发出来
- 给我买入卖出建议

## Verification

在仓库根目录运行：

```bash
bash scripts/validate_hermes_skill.sh
bash scripts/validate_hermes_cm_ops.sh
bash scripts/validate_hermes_telegram_control.sh
bash scripts/hermes_telegram_e2e_smoke.sh
bash scripts/hermes_gateway_preflight.sh
tail -n 5 reports/hermes/ops_audit.ndjson
```

`ops_audit.ndjson` 只应记录脱敏后的聚合审计信息，不应出现 token、RPC URL、完整用户 ID、完整 chat ID、真实地址或交易 hash。

每次 Telegram 测试后检查：

```bash
tail -n 20 reports/hermes/ops_audit.ndjson
```

## Troubleshooting

- Telegram 命令没有反应：检查 gateway status 和 allowed users。
- skill command not found：检查 skill 安装路径或 `skills.external_dirs`。
- 相对日期被拒绝：改成 `YYYY-MM-DD`。
- 如果修改 skill 后 Telegram 仍按旧规则执行，请运行 install 脚本并在 Telegram 中 `/reset`。
- Hermes 文档说明已安装 skill 在新会话中生效；当前会话可能需要 `/reset`。
- lock busy：等待当前操作完成。
- 输出隐藏地址：这是 redaction 默认行为。
- 出现危险命令审批：不要批准 raw shell；只使用 wrapper。
