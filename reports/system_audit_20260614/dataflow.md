# chain-monitor 数据流全图（审查梳理）

```
                    ┌─────────────────────────────────────────────────────────┐
                    │  以太坊 RPC (Alchemy/生产; 本审查未调用)                  │
                    └─────────────────────────────────────────────────────────┘
                                          │ eth_blockNumber / eth_getBlock / eth_getLogs
                                          ▼
  [producer]  app/listener.py:1721
    · 轮询 last_block+1..latest（PRODUCER_POLL_INTERVAL_SEC=1.0）
    · LOW_CU_MODE: 取 block header → bloom 预过滤 → 按 scan_groups 做定向 getLogs(Transfer)
    · 块内去重 (tx_hash, log_index)；构建 eth/token_flow candidate
    · ❗I1: 任一块取数失败→break→重试同块，无上限（可永久卡死）
    · ❗M1: 无 reorg 处理
                                          │ _enqueue → tx_queue (asyncio.Queue, max 5000)
                                          │ (背压: 满则 spill 到 /tmp, replay_spill_worker 回灌)
                                          ▼
  [worker ×5]  app/main.py:391  → handle_tx → pipeline.process
    · ❗M3: 5 协程共享无锁 state_manager
                                          ▼
  [pipeline]  app/pipeline.py  (主处理管道)
    1. 解析 raw→parsed event；intent 分类:
         · 交易所流  pipeline.py:8876-9000 (deposit/withdraw/buy_prep; size_bucket 1500/10000)
         · LP 动作   lp_analyzer.py:109-135 (base_net/quote_net→swap/add/removal/rebalance)
         · 行为      analyzer.py (behavior_type; ❗M2 与 lp_analyzer 的 pool_buy/sell 重叠)
         · 清算      liquidation_detector.py
    2. 富化: price_service / address_intelligence / market_context_adapter
                                          ▼
  [signal_quality_gate]  app/signal_quality_gate.py
    · 金额/质量/冷却/稳定币非swap 硬过滤 (QUALITY_GATE_*; S2 魔法数字)
    · 多数事件在此 drop（样本: 1758 条 delivery_audit 全是 drop）
                                          ▼
  [strategy_engine → asset_market_state → trade_opportunity]
    · CANDIDATE / VERIFIED / BLOCKED（OPPORTUNITY_* 阈值；硬阻断 NO_TRADE_LOCK 等）
    · shadow opportunity（研究用，不发 TG）
                                          ▼
            ┌───────────────────────────────┬───────────────────────────────┐
            ▼                               ▼                               ▼
  [archive_store]                    [notifier]                    [outcome_scheduler]
  app/archive_store.py               send_signal → Telegram        app/outcome_scheduler.py
   · NDJSON (app/data/archive)        · 仅 state change/candidate    · ❗F1: 只 register_from_lp_record
   · + SQLite mirror (data/*.sqlite)  · 中文、稀疏                       (pipeline.py:450) → 仅 LP 信号
   · ❗I3: 写异常静默吞掉                                              · 30/60/300s 结算用 market_context
   · raw/parsed 受 tier 门控(S3)                                       · ❗F1: 默认 adapter=unavailable
                                                                          → start/end price=None
                                                                          → 全部 market_price_unavailable
                                                                      · 结果: outcome 表 0 行
                                          │
                                          ▼
  [reports]  reports/generate_daily_report_latest.py 等
    · DB-first 读，archive/cache fallback
    · ❗I3: REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH 默认 False

闭环缺口（核心）: signal → (应有) outcome → quality/calibration → 调阈值。
  F1 使 outcome 永空 → 闭环断开 → 系统从不知道自己准不准。
```

## 模块清单（app/，非 test，56 个）— 关键节点
| 模块 | 职责 |
|---|---|
| main.py | 装配 + 协程编排（producer / 5 worker / outcome_scheduler / spill）|
| listener.py | 区块轮询、bloom 预过滤、定向 getLogs、候选构建、入队 |
| pipeline.py (9785 行) | 解析→intent→富化→门控→策略→归档/通知/outcome 注册 |
| lp_analyzer.py | LP 动作分类（swap/add/removal/rebalance）|
| analyzer.py | 行为分类（whale_action / inventory / pool_buy_sell）|
| signal_quality_gate.py (2562 行) | 信号质量门控 |
| strategy_engine / asset_market_state / trade_opportunity | 状态机 + 机会层 |
| outcome_scheduler.py | 30/60/300s 结果结算（F1 受限）|
| archive_store.py | NDJSON + SQLite 双写（I3 静默吞错）|
| sqlite_store.py (6218 行) | SQLite 镜像/迁移/压缩/完整性 |
| market_context_adapter.py | live(OKX/Kraken) / fixture / unavailable（默认 unavailable）|
| state_manager.py (2666 行) | 共享运行时状态（M3 无锁）|

## 配置覆盖面
config.py 共 ~355 个 `_get_*_env` 项，全部可被 .env 覆盖（本审查未读 .env，仅用默认值）。
关键默认值：PRODUCER_POLL_INTERVAL_SEC=1.0、LOW_CU_MODE=True、MARKET_CONTEXT_ADAPTER_MODE=unavailable、
OUTCOME_WINDOWS=(30,60,300)、SQLITE_ENABLE=True、SQLITE_ARCHIVE_MIRROR_ENABLE=True、
REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH=False、DEFAULT_USER_TIER=research。
```
