# Hermes Telegram 端到端部署验收

## 控制路径

```text
Telegram -> Hermes gateway -> /chain-monitor-report-analyst -> ~/.hermes/bin/chain-monitor-cn-router -> scripts/hermes_cm_cn_router.py -> scripts/hermes_cm_ops.sh
```

## 为什么需要 launcher

- Hermes gateway 会话的当前工作目录不一定是 chain-monitor 仓库。
- Skill references 必须完整安装。
- launcher 固定 repo root，避免模型自己 cd /run-project/chain-monitor。
- launcher 只进入中文 router，不允许绕过。

## 安装步骤

```bash
cd /run-project/chain-monitor
bash scripts/validate_hermes_skill.sh
bash scripts/validate_hermes_telegram_control.sh
bash scripts/install_hermes_skill.sh
```

安装完成后，在 Telegram 中发送：

```text
/reset
```

## Hermes 配置检查

- `TELEGRAM_BOT_TOKEN` 已设置，但不输出。
- `TELEGRAM_ALLOWED_USERS` 或 `GATEWAY_ALLOWED_USERS` 已设置。
- 不设置 `GATEWAY_ALLOW_ALL_USERS=true`。
- `approvals.mode` 不应为 `off`。
- 不使用 `/yolo`。
- 不设置 `HERMES_YOLO_MODE`。
- 群组建议 `telegram.require_mention: true`。
- `display.tool_progress_command` 建议为 `false`，避免 Telegram 显示过多 terminal 命令细节。

## Gateway 检查

User service:

```bash
hermes gateway status
hermes gateway start
journalctl --user -u hermes-gateway -f
```

System service:

```bash
sudo hermes gateway status --system
journalctl -u hermes-gateway -f
```

不要在日志里转发 token、RPC、完整 user id、完整 chat id、私有地址或 tx hash。

## Telegram 必测

```text
/reset
```

```text
/chain-monitor-report-analyst 命令提示
```

预期：显示完整中文命令菜单。

```text
/chain-monitor-report-analyst 分析昨天的报告
```

预期：拒绝，不运行 date，不生成日报，不 analyze。

```text
/chain-monitor-report-analyst 生成今天的日报
```

预期：拒绝。

```text
/chain-monitor-report-analyst 分析报告2026-05-01
```

预期：执行 fast analyze，回复包含 request_id。

```text
/chain-monitor-report-analyst 系统体检
```

预期：执行 system-health，写 audit log。

## 第 5 步验收：Telegram 必测

```text
/reset
/chain-monitor-report-analyst 命令提示
/chain-monitor-report-analyst 系统体检
/chain-monitor-report-analyst 监听器体检
/chain-monitor-report-analyst 标准日报流程2026-05-01
/chain-monitor-report-analyst 任务状态cmjob_...
/chain-monitor-report-analyst 诊断任务cmjob_...
/chain-monitor-report-analyst 查看日志cmjob_...
/chain-monitor-report-analyst 检查回放2026-05-01
/chain-monitor-report-analyst 数据质量2026-05-01
/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03
```

标准日报流程只能跑已经结束的北京时间逻辑日，不支持当前北京时间日期。例如北京时间 2026-05-03 还没结束时，不能跑 `/chain-monitor-report-analyst 标准日报流程2026-05-03`；请在次日 00:05 后执行。

失败标准：

- “命令提示”没有返回完整菜单。
- “分析昨天的报告”没有拒绝。
- Telegram 中出现 raw make 执行建议。
- Telegram 中出现 date -u / TZ=Asia/Shanghai date 来解析昨天。
- 任何命令绕过 router 直接跑 cm_ops dated operation。

## 第 5.1 步验收：后台任务模式

Telegram 必测：

```text
/chain-monitor-report-analyst 标准日报流程2026-05-01
```

预期：立即返回 job_id，不等待流程完成。

```text
/chain-monitor-report-analyst 任务状态cmjob_...
```

预期：返回 running/succeeded/failed。

```text
/chain-monitor-report-analyst 查看结果cmjob_...
```

预期：完成后返回 result.md 摘要。

```text
/chain-monitor-report-analyst 诊断任务cmjob_...
```

预期：只读返回 failed_substep / failed_command / timeout_hit / stdout_tail / stderr_tail / 下一步建议，不执行 make。

```text
/chain-monitor-report-analyst 空间检查
```

预期：返回 job_id。

```text
/chain-monitor-report-analyst 空间快检
```

预期：同步快速返回，不递归 du。

失败标准：

- Telegram 卡住等待 daily-flow 完成。
- Telegram 超时。
- “标准日报流程”直接同步执行 make。
- 没有 job_id。
- job result 没有落盘。
- failed job 不能显示 failed_substep / failed_command。
- 重复提交同日期 daily-flow 创建多个 running job。

## daily-flow 失败诊断验收

- 标准日报流程不应只调用 `make daily-close DATE=YYYY-MM-DD`；daily-close 子步骤必须展开记录。
- daily-close 默认只是 compression dry-run，只有 `COMPRESS=YES CONFIRM=YES` 才执行实际 archive gzip。
- 失败时先用 `/chain-monitor-report-analyst 诊断任务JOB_ID` 和 `/chain-monitor-report-analyst 查看日志JOB_ID`。
- “每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法。
- 不要因为 daily-close failure 就压缩 archive。

## 服务器验证

```bash
tail -n 20 reports/hermes/ops_audit.ndjson
```

必须看到：

- cn_router audit
- cm_ops audit
- request_id
- allowed/refused_reason
- relative_date_forbidden for “分析昨天的报告”
- analyze for absolute date command

只检查聚合审计，不输出 token、RPC、完整 user id、完整 chat id、私有地址或 tx hash。

## 明确失败标准

如果 Telegram 中再次出现：

- `date -u`
- `TZ='Asia/Shanghai' date`
- `TZ=Asia/Shanghai date`
- “昨天是 YYYY-MM-DD，开始跑分析”
- “报告缺失，先生成日报”
- raw make 执行建议
- 绕过 router 直接跑 cm_ops dated operation

那么部署失败，说明新 skill 没生效或 router 被绕过。

## 修复方法

- 重新运行 `scripts/install_hermes_skill.sh`。
- Telegram 发送 `/reset`。
- 检查 `~/.hermes/skills/chain-monitor-report-analyst/references/telegram_control.md` 是否存在。
- 检查 `~/.hermes/bin/chain-monitor-cn-router` 是否存在并可执行。
- 检查 `SKILL.md` 是否包含 “不得先运行 date”。
- 检查 gateway 是否重启或会话是否 reset。

## 不在本步骤创建 Hermes cron

- 本步骤只固化人工 Telegram 控制。
- 定时日报/自动化在下一步单独做。
- 自动化需要确定“日报目标日期”的 deterministic 规则，不能由模型把“昨天”转日期。
