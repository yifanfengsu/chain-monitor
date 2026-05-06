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

## Outbound 发送层健康检查

`app/notifier.py` 是系统主动推送链上告警的 outbound 发送层。日常体检除了确认 listener、SQLite、market context 正常，也要关注 Telegram outbound 健康：

- `get_notifier_health()` 中的 `pool_timeout_count`、`retry_after_count`、`network_error_count`、`consecutive_failures`。
- 日志中的 `notifier send failed: type=... attempt=... consecutive_failures=...`。
- 如果出现 `Pool timeout: All connections in the connection pool are occupied` 且链上监听、解析、SQLite 写入仍正常，短期优先重启 listener 恢复 Telegram client 连接池；长期必须使用显式 `HTTPXRequest` 和应用层 send semaphore，避免 worker burst 打爆连接池。

推荐 outbound env：

```bash
TELEGRAM_CONNECTION_POOL_SIZE=16
TELEGRAM_POOL_TIMEOUT=10
TELEGRAM_SEND_CONCURRENCY=3
```

## 每日中文命令

直接复制到 Telegram 使用：

```text
/chain-monitor-report-analyst 命令提示
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
/chain-monitor-report-analyst Profile复盘2026-05-01
/chain-monitor-report-analyst Blocker复盘2026-05-01
/chain-monitor-report-analyst Shadow复盘2026-05-01
/chain-monitor-report-analyst 空间检查
/chain-monitor-report-analyst 空间快检
/chain-monitor-report-analyst 归档压缩预检2026-05-01
/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03
/chain-monitor-report-analyst 生成日报2026-05-01
/chain-monitor-report-analyst 深度分析报告2026-05-01
/chain-monitor-report-analyst 生成摘要2026-05-01 快速
/chain-monitor-report-analyst 生成摘要2026-05-01 深度
/chain-monitor-report-analyst 构建并分析报告2026-05-01 快速
```

日期必须是绝对 `YYYY-MM-DD`。不要发送“今天”“昨天”“前天”这类相对日期。

标准日报流程只能跑已经结束的北京时间逻辑日，不支持当前北京时间日期。例如北京时间 2026-05-03 还没结束时，不能跑 `/chain-monitor-report-analyst 标准日报流程2026-05-03`；请在次日 00:05 后执行同一命令。

## 手动控制菜单

发送：

```text
/chain-monitor-report-analyst 命令提示
```

预期返回完整中文菜单。

手动功能：

- 系统体检：检查 SQLite / report source / market / coverage。
- 监听器体检：检查 listener 进程、最近 raw/parsed/signal/archive 时间和 zero activity 风险。
- 锁状态：只读检查 Hermes lock 是否被占用，不删除 lock 文件。
- 标准日报流程YYYY-MM-DD：展开 daily-close 子步骤，然后 trade-replay-full、report-daily-date、daily-compare、sqlite-checkpoint。只能跑已经结束的北京时间逻辑日，当前北京时间日期会被 submit 阶段拒绝且不创建 job。daily-close 默认只是 compression dry-run。
- 分析报告YYYY-MM-DD：分析已存在 canonical daily report，不自动生成日报。
- 检查回放YYYY-MM-DD：确认 replay_source=persisted、replay_scope=full。
- 数据质量YYYY-MM-DD：判断该日是否适合策略质量分析。
- Profile复盘YYYY-MM-DD：查看 profile 后验。
- Blocker复盘YYYY-MM-DD：查看 blocker 分布。
- Shadow复盘YYYY-MM-DD：查看 shadow funnel。
- Outcome闭环诊断YYYY-MM-DD：只读排查 outcome/replay/profile 闭环不足。
- Outcome补全预检YYYY-MM-DD：只做 outcome-catchup dry-run，不从 Telegram 执行写入。
- LP抑制抽样预检YYYY-MM-DD：只做 LP early suppression sample replay dry-run，不从 Telegram 执行写入。
- 空间检查：提交后台任务查看 SQLite / WAL / archive / reports 占用。
- 空间快检：快速同步查看 SQLite / WAL / SHM 文件大小，不递归扫描 archive/reports。
- 归档压缩预检YYYY-MM-DD：提交后台 dry-run 任务，只 dry-run，不压缩。
- 周复盘START到END：提交后台任务，最多 14 天的只读聚合复盘。

## 后台任务模式

标准日报流程、空间检查、归档压缩预检、周复盘会返回 job_id，Telegram 不会等待任务完成。

后续使用：

```text
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 查看结果cmjob_...
/chain-monitor-report-analyst 查看日志cmjob_...
/chain-monitor-report-analyst 诊断任务cmjob_...
/chain-monitor-report-analyst 最近任务
/chain-monitor-report-analyst 锁状态
```

如果任务长时间 running，先看“查看日志JOB_ID”，再 SSH 排查。若任务 failed，先看“诊断任务JOB_ID”和“查看日志JOB_ID”，确认 failed_substep / failed_command。不要重复提交同一日期/同一范围的相同任务；job controller 会尽量返回 existing job。

示例：

```text
/chain-monitor-report-analyst 标准日报流程2026-05-01
```

返回 job_id 后：

```text
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 查看结果cmjob_...
/chain-monitor-report-analyst 诊断任务cmjob_...
```

每日建议流程：

```text
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 查看结果cmjob_...
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

## 标准日报流程失败诊断

标准日报流程不再把 `make daily-close DATE=YYYY-MM-DD` 当成黑盒。daily-close 的内部子步骤会逐步记录状态，普通失败会显示 `failed_substep`、`failed_command`、`exit_code`、`timeout_hit`、`timeout_limit_sec`、`stdout_tail`、`stderr_tail` 和 `result_path`。

daily-close 默认只是 compression dry-run，不真正压缩 archive。只有 `COMPRESS=YES CONFIRM=YES` 才会执行实际 archive gzip。

失败排查顺序：

```text
/chain-monitor-report-analyst 诊断任务JOB_ID
/chain-monitor-report-analyst 查看日志JOB_ID
```

“每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法。不要因为 daily-close failure 就压缩 archive；只有明确需要 archive compression execute 时，才允许使用包含“我确认压缩”的高风险命令。

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
- 每日收尾不是普通标准日报流程失败的修复方法。
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
- lock busy：使用“锁状态 / 最近任务 / 任务状态JOB_ID”诊断；不要手动删除 `/run/lock/chain-monitor-hermes-*`。
- 输出隐藏地址：这是 redaction 默认行为。
- 出现危险命令审批：不要批准 raw shell；只使用 wrapper。
