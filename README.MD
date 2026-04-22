raw_events：系统“看到了什么”
parsed_events：系统“理解成了什么”
signals：系统“最终发出的 signal / delivery 决策 / market context 诊断”
cases：系统“把这件事当成哪条持续故事在跟踪”

私有 data 文件：
- 默认读取 `data/addresses.json` 和 `data/lp_pools.json`
- 可选读取 `data/entities.json` 和 `data/address_aliases.json`
- 可选读取 `data/clmm_managers.json`，用于本地注入已知 CLMM PositionManager（如 `protocol/chain/manager_address/abi_profile/enabled/note`）
- 公开仓库缺少这些文件时，会分别以空地址簿 `{}` 和空池子簿 `[]` 启动，基础 import / unittest 仍可运行
- `entities.json` 用来本地注入实体归属（`entity_id/entity_label/entity_type/cluster_tags`）
- `address_aliases.json` 用来本地覆盖地址别名、钱包职能和实体补丁（如 `wallet_function/entity_source/entity_confidence`）
- `clmm_managers.json` 用来本地启用 Uniswap v3 / v4 PositionManager registry；缺失时 CLMM parser 会优雅降级为不识别 manager
- 需要本地注入时，直接把对应 JSON 文件放到 `data/` 目录即可；公开仓库提供 `data/entities.example.json`、`data/address_aliases.example.json` 和 `data/clmm_managers.example.json` 作为最小 schema 参考
- `data/lp_pools.json` 推荐本地 schema：
  - `pool_address`
  - `chain`
  - `pair_label`
  - `base_symbol`
  - `quote_symbol`
  - `canonical_asset`
  - `dex`
  - `protocol`
  - `pool_type`
  - `enabled`
  - `priority`
  - `major_pool`
  - `major_match_mode`
  - `notes`
  - 可选：`placeholder`
- 可直接参考 `data/lp_pools.example.json`
- validator 会 canonicalize `BTC/WBTC/CBBTC -> BTC`、`SOL/WSOL -> SOL`、`USDC/USDC.E -> USDC`
- placeholder 地址必须保持 `enabled=false`；coverage validator 会把 `placeholder_enabled` 当成 malformed entry
- malformed address 不会被静默忽略；duplicate pool address 和同 pair 同 priority 会给 warning

CLMM replay fixture 分两层：
- `app/test_fixtures/clmm/` 是 normalized replay，直接对齐 parser 当前稳定输入接口
- `app/test_fixtures/clmm_decoder/` 是 decoder-aligned replay，保留更贴近上游 `decoded_logs_snapshot` 的形状，再经 `clmm_decoder_normalizer` 归一化后进入 parser/state/intent 链路

本次产品化 patch 新增了 4 个应用层 seam：
- user tiers：`retail / trader / research`，通过 `app/user_tiers.py` + `DEFAULT_USER_TIER` 控制 LP stage 准入、模板细节、debug 行展示与 asset-case 优先级；当前测试/默认运行保持 `DEFAULT_USER_TIER=research`
- asset case：`app/asset_case_manager.py` 维护短窗 token/asset 级聚合，只聚合 LP pool 事件；`asset_case_id / asset_case_key / asset_case_stage / supporting_pairs` 都挂在 event/signal metadata 里，并默认持久化到 `data/asset_cases.cache.json` 以支持跨进程/重启恢复
- market context：`app/market_context_adapter.py` 提供 `fixture / unavailable / live` 三层接口；`live` 只接公共市场数据 API，没有外部市场数据时统一回落到 `market_context_source=unavailable`
  - 当前 US VPS 场景默认主路径已切到 `OKX public + Kraken Futures public`
  - `Binance / Bybit` 只保留作 fallback / diagnostics，不再作为主成功路径
- signal archive：`app/data/archive/signals/YYYY-MM-DD.ndjson` 现在会稳定落盘 `signal_id / asset_case_id / delivery_decision / notifier_sent_at / market_context diagnostics`，用于和 `delivery_audit` / outcome 做 1:1 对账
- outcome / quality：`app/quality_manager.py` 基于 `state_manager` 里的 `lp_alert_outcome_v1` 滚动记录计算 `pool_quality_score / pair_quality_score / asset_case_quality_score / prealert_precision_score / climax_reversal_score / fastlane_roi_score`，并把 rolling outcome / ops summary 持久化到 `data/lp_quality_stats.cache.json`
- trade action layer：`app/trade_action.py` 把已有 LP stage / sweep / broader-confirm / live market context / quality / asset-case 聚合成 trader-facing `trade_action_*` 字段；Telegram 第一行优先显示 `可顺势追多 / 可顺势追空 / 等待确认 / 不追多 / 不追空 / 不交易 / 数据缺口，不交易`
- asset market state layer：`app/asset_market_state.py` 在 `trade_action` 之上新增 asset-level 交易状态机；默认持久化当前资产状态、prealert lifecycle、`NO_TRADE_LOCK` 和 Telegram suppression，使 Telegram 只在状态变化 / 风险阻止 / 候选状态时发声，而 archive/report 继续保留全量研究流
- trade opportunity layer：`app/trade_opportunity.py` 在 `trade_action + asset_market_state` 之上新增最终机会裁决层；它不重写原状态机，只负责把“研究信号”收敛成极少数 `CANDIDATE / VERIFIED / BLOCKED`

quality/outcome 本地结构说明：
- 原始 outcome 仍由 `StateManager.record_lp_outcome_alert()` 写入 `lp_alert_outcome_v1`
- 新增字段会把 `asset_symbol / asset_case_id / alert_relative_timing / market_context_source / lp_promoted_fastlane / asset_market_state_key / outcome_price_source` 一起写进 outcome record
- quality manager 不做黑盒 ML，而是直接从 rolling outcome history 上算可解释 heuristic，并把结果回写到 signal context / metadata 供 routing、promotion、Telegram 模板使用
- trade action / asset market state 都只是规则化风险提示，不是自动下单，也不是投资建议，更不会接私有账户 API
- 默认用户视角已切到 asset-level state machine：Telegram 只推状态变化 / 风险阻止 / 交易候选；如果要看 full stream，请看 archive/report，不要把 Telegram 当成研究全量流
- `LONG_CANDIDATE / SHORT_CANDIDATE` 只表示候选，不表示“现在就可追”
- `TRADEABLE_LONG / TRADEABLE_SHORT` 只有 rolling outcome 样本量、followthrough、adverse、completion rate 同时达标时才会出现
- `NO_TRADE_LOCK` 用来处理短时双向扫流动性 / 买卖压冲突，目的是降低冲突期刷屏和误交易
- `LONG_CHASE_ALLOWED / SHORT_CHASE_ALLOWED` 仍保留在 `trade_action` 层做底层语义兼容，但默认 user-facing 状态机会先把它们降级为 `LONG_CANDIDATE / SHORT_CANDIDATE`，直到后验证明达标
- `trade_opportunity` 是交易辅助提示，不是投资建议；即使 `VERIFIED`，也不是自动下单，更不会给仓位大小或止损止盈价格
- `trade_opportunity` 默认只允许三类 Telegram 主消息：
  - 多头/空头候选
  - 多头/空头机会
  - blocker / no-trade / verified opportunity state change
- 普通 `OBSERVE_LONG / OBSERVE_SHORT / WAIT_CONFIRMATION` 默认不推 Telegram；它们仍会完整写入 archive/report，供研究和阈值调参
- 现在大量出现“不交易 / 不追 / blocker”是正常的，因为机会层是严格筛选器，不是放大发送器
- 只有同时满足 `多源确认 + 方向一致 + live market context + 质量过线 + 后验样本达标 + 风险过滤通过 + 无冲突/无锁定 + 明确失效条件` 时，才可能出现 `VERIFIED`
- 如果历史后验样本不足，最多只会出现 `CANDIDATE`，并明确标注“候选，不可盲追”
- 新增持久化缓存：`data/trade_opportunities.cache.json`，重启后保留 rolling stats、budget、cooldown、candidate/verified/blocker 历史
- 新增 reports 会回答：
  - 为什么没有机会
  - verified 是否真的优于普通信号
  - blocker 是否减少了错误交易诱导
  - 还差多少样本才可能开放 verified
- 如果用户用 `trade_action` 做交易，仍应自行结合仓位、止损和整体风险管理

live market context 最小 env：
- `MARKET_CONTEXT_ADAPTER_MODE=live|fixture|unavailable`
- `MARKET_CONTEXT_PRIMARY_VENUE=okx_perp`
- `MARKET_CONTEXT_SECONDARY_VENUE=kraken_futures`
- `MARKET_CONTEXT_TIMEOUT_SEC=2.5`
- `MARKET_CONTEXT_CACHE_TTL_SEC=2.0`
- `MARKET_CONTEXT_FAILURE_CACHE_TTL_SEC=1.0`
- `OKX_PUBLIC_BASE_URL=https://www.okx.com`
- `KRAKEN_FUTURES_BASE_URL=https://futures.kraken.com`
- `BINANCE_FAPI_BASE_URL=https://fapi.binance.com`
- `BINANCE_SPOT_BASE_URL=https://api.binance.com`
- `BYBIT_V5_BASE_URL=https://api.bybit.com`
- live 启动时会做配置自检；若 `primary/secondary` 仍指向 `binance_perp/bybit_perp`，进程会 fail-closed 直接拒绝启动

live market context 诊断补充：
- 每条 LP signal 会带 `market_context_attempted / requested_symbol / resolved_symbol / failure_reason / failure_stage / endpoint / latency_ms`
- 现在还会带 `attempted_venues / fallback_chain`
- ETH/BTC/SOL 主池默认优先尝试更广泛的 majors perp reference：
  - OKX：优先 `ETH/BTC/SOL-USDT-SWAP`，再回退同资产的 `USDC/USD` swap
  - Kraken Futures：优先 `PF_*USD`，再回退 `PI_*USD`

asset-case / quality cache：
- asset-case cache 默认路径：`data/asset_cases.cache.json`
- quality/outcome cache 默认路径：`data/lp_quality_stats.cache.json`
- archive base dir 默认路径：`app/data/archive`
- 示例 schema：`data/asset_cases.cache.example.json`、`data/lp_quality_stats.cache.example.json`

quality report / 导出入口：
- `python -m app.quality_reports --summary`
- `python -m app.quality_reports --top-bad-prealerts`
- `python -m app.quality_reports --fastlane-roi`
- `python -m app.quality_reports --market-context-health`
- `python -m app.quality_reports --market-context-config-check`
- `python -m app.quality_reports --major-pool-coverage`
- `python -m app.quality_reports --db-summary`
- `python -m app.quality_reports --db-integrity`
- `python -m app.quality_reports --opportunity-db-summary`
- `python -m app.quality_reports --report-source-summary`
- `python -m app.quality_reports --format csv`

SQLite mirror / query layer：
- SQLite 是分析库，不是 full archive 的第二份完整副本；主监听、report fallback 和历史研究仍保留 JSON / NDJSON archive / cache
- 长期 full 保留的核心表是 `signals / signal_features / outcomes / trade_opportunities / opportunity_outcomes / quality_stats / asset_market_states / no_trade_locks / asset_cases / prealert_lifecycle / market_context_snapshots`
- `raw_events / parsed_events` 默认按 `index_only` 导入；full raw/parsed payload 长期保留在 `.ndjson / .ndjson.gz` archive
- `delivery_audit / telegram_deliveries / case_followups / market_context_attempts` 默认按 `slim` 或诊断型模式保留，不把 full payload 长期复制进 SQLite
- 默认 DB 路径：`data/chain_monitor.sqlite`
- reports 默认 `SQLITE_REPORT_READ_PREFER_DB=1`，会优先从 SQLite 读取；如果 DB 文件不存在、缺表、空表或读取失败，会在 `SQLITE_REPORT_FALLBACK_TO_ARCHIVE=1` 时回退到 archive/cache
- report fallback 同时支持 `*.ndjson` 和 `*.ndjson.gz`；同一天两种文件都存在时优先未压缩 `*.ndjson`，避免重复计数
- 查看当前 report source、DB/archive 行数、压缩 archive 检测和 mismatch warning：
  - `./venv/bin/python -m app.quality_reports --report-source-summary`
- 如果需要临时强制 archive-first，可设置：
  - `SQLITE_REPORT_READ_PREFER_DB=0`
  - `SQLITE_REPORT_FALLBACK_TO_ARCHIVE=1`
- 运行数据库属于本地私有数据，不要提交到 git；仓库已忽略 `data/*.sqlite`、`data/*.sqlite-wal`、`data/*.sqlite-shm`、`data/*.db`
- 初始化：
  - `./venv/bin/python -m app.sqlite_store --init`
- 检查：
  - `./venv/bin/python -m app.sqlite_store --summary`
  - `./venv/bin/python -m app.sqlite_store --integrity-check`
  - `./venv/bin/python -m app.quality_reports --db-summary`
  - `./venv/bin/python -m app.quality_reports --db-integrity`
  - `./venv/bin/python -m app.quality_reports --db-size-breakdown`
  - `./venv/bin/python -m app.quality_reports --db-value-audit`
  - `./venv/bin/python -m app.quality_reports --db-retention-recommendation`
- 迁移已有 archive：
  - `./venv/bin/python -m app.sqlite_store --migrate-archive --all`
  - `./venv/bin/python -m app.sqlite_store --migrate-archive --date YYYY-MM-DD`
- 写入模式默认建议：
  - `SQLITE_RAW_EVENTS_MODE=index_only`
  - `SQLITE_PARSED_EVENTS_MODE=index_only`
  - `SQLITE_DELIVERY_AUDIT_MODE=slim`
  - `SQLITE_TELEGRAM_DELIVERY_MODE=slim`
  - `SQLITE_MARKET_CONTEXT_ATTEMPT_MODE=slim`
  - `SQLITE_CASE_FOLLOWUP_MODE=slim`
  - `signals / outcomes / trade_opportunities / asset_market_states / quality_stats` 保持 `full`
- 迁移是可重复执行的；去重基于 `event_id / signal_id / signal_archive_key / trade_opportunity_id` 等主键，不会要求一次性迁完全部历史
- 即使启用 SQLite，raw_events / parsed_events / signals / delivery_audit / case_followups 仍继续写 NDJSON archive；asset cases / asset market states / quality stats / trade opportunities 仍保留 JSON cache
- 不建议删除旧 archive/cache：它们仍是兼容层、恢复路径和 report fallback 的来源；SQLite 初始化或写入失败时主流程会 warning 并继续走原 archive
- compact / retention 建议流程：
  - 先压缩 archive：保留 `.ndjson.gz`
  - 再做 `compact` 清理 DB 里的重复 full payload
  - 如需真正释放磁盘，再单独做 `VACUUM`
- compact / vacuum 命令：
  - `./venv/bin/python -m app.sqlite_store --compact --dry-run`
  - `./venv/bin/python -m app.sqlite_store --compact --execute`
  - `./venv/bin/python -m app.sqlite_store --compact-table raw_events --dry-run`
  - `CONFIRM=YES ./venv/bin/python -m app.sqlite_store --vacuum`
- 安全约束：
  - 没有 `archive_path / payload_hash` 或 archive 文件不存在时，compact 不会清 full payload
  - `VACUUM` 是单独命令，不会在 compact 后自动执行
- 日终收尾：
  - `make daily-close DATE=YYYY-MM-DD`：迁移指定日期 archive 到 SQLite、检查 DB、生成报告、压缩 dry-run、执行 WAL checkpoint
  - `make daily-close DATE=YYYY-MM-DD COMPRESS=YES`：完整日终流程，并在 dry-run 后 gzip 指定日期 archive
  - `make archive-compress-dry-run DATE=YYYY-MM-DD`：预览指定日期会压缩哪些 archive
  - `make archive-compress-date DATE=YYYY-MM-DD CONFIRM=YES`：明确确认后压缩指定日期 archive
  - `make sqlite-checkpoint`：执行 `PRAGMA wal_checkpoint(TRUNCATE)`
- 不要压缩当天 UTC 正在写入的日期；压缩前应先 migrate archive 到 SQLite。压缩保留数据为 `.ndjson.gz`，prune 才是删除数据，两者不是一回事；当前阶段建议只压缩，不删除
- SQLite 当前适合单人自用、本地查询和轻量 report；多进程/多用户/长期高并发以后再评估 Postgres
- retention/prune 是可选能力，默认不自动删除：
  - `./venv/bin/python -m app.sqlite_store --prune --dry-run`
  - `./venv/bin/python -m app.sqlite_store --prune --execute`

majors-first pool book 指引：
- `LP_MAJOR_ASSETS / LP_MAJOR_QUOTES / LP_MAJOR_PRIORITY_SCORE` 控制 majors 覆盖与优先级
- 本地扩 `data/lp_pools.json` 时优先补 `ETH / BTC / SOL` 对 `USDT / USDC` 主池
- 下一轮研究采样明确优先补 `BTC/USDT`、`BTC/USDC`、`SOL/USDT`、`SOL/USDC`，以及更多已验证的 `ETH` 主池
- `python -m app.quality_reports --major-pool-coverage` 现在会额外输出：
  - `covered_major_pairs`
  - `missing_major_pairs`
  - `configured_but_disabled_major_pools`
  - `placeholder_major_pool_entries`
  - `malformed_major_pool_entries`
  - `enabled_major_pools`
  - `active_recent_major_pairs`
  - `no_recent_signal_major_pairs`
  - `recommended_local_config_actions`
- 本地补 BTC/SOL 主池的最小流程：
  - 以 `data/lp_pools.example.json` 为模板，把 `BTC/USDT`、`BTC/USDC`、`SOL/USDT`、`SOL/USDC` 复制到私有 `data/lp_pools.json`
  - 填真实 `pool_address`；不要伪造地址，也不要把 placeholder 当 enabled pool
  - 核对 `chain / dex / protocol / pool_type / base_symbol / quote_symbol / canonical_asset`
  - 只有确认 metadata 正确后才把 `enabled=false` 改成 `enabled=true`
  - 重启当前 research/listener 进程，让 `lp_registry` 重新加载本地 `data/lp_pools.json`
  - 运行 `./venv/bin/python -m app.quality_reports --major-pool-coverage`
  - 看到 `enabled_major_pools` 中出现目标 pair，且 `placeholder_major_pool_entries / malformed_major_pool_entries` 没有对应条目，才算配置通过
  - 跑一轮 research 默认流程后，再看 `active_recent_major_pairs` 是否出现目标 pair；如果仍在 `no_recent_signal_major_pairs`，先排查 listener 是否扫到该池
- 如何确认 listener 扫到了对应池：
  - `major-pool-coverage` 中目标 pair 先进入 `enabled_major_pools`
  - 跑过一轮 research/live 采样后，目标 pair 应从 `no_recent_signal_major_pairs` 迁移到 `active_recent_major_pairs`
  - 如果长期没有迁移，优先检查 `pool_address`、`chain`、`dex`、`pool_type` 和本地 listener 扫描范围
- `data/lp_pools.json` 属于本地私有配置，不要提交到 GitHub；公开仓库只保留 example
- 先不扩 long-tail 的原因：
  - 当前 outcome / quality / market context 样本仍主要集中在 majors
  - 先把 majors 的 `prealert -> confirm`、confirm scope、reversal / exhaustion 统计做稳更有价值
- 公开仓库不内置 unverifiable 私有 pool 地址；缺少本地 pool book 时 coverage report 会给 warning，但不会伪造数据

当前 patch 明确不做多频道真实分发：
- 默认只跑 `research` 视图，研究期消息保留最完整字段
- 如果后续需要 `retail / trader / research` 多频道，可由用户自行多进程启动并传不同 env / `CHAT_ID`
- 多频道编排、私有账户 API key、下单/账户读取都不在本次 patch 范围

## 常用 Make 命令

- `make preflight`：启动前全套检查，包含安全配置、编译、SQLite、market context、coverage、archive 和 git 状态。
- `make run`：启动主程序。
- `make run-research`：强制 `research` + OKX/Kraken live context 启动。
- `make health`：查看 market context 健康度。
- `make coverage`：查看 major pool 覆盖。
- `make db-init / db-summary / db-integrity / db-report`：初始化、摘要、完整性和 SQLite report 检查。
- `make db-size / db-value-audit / db-retention`：查看 DB 体积、数据价值和 retention 建议。
- `make db-compact-dry-run / db-compact-execute CONFIRM=YES / db-vacuum CONFIRM=YES`：预览 compact、执行 compact、执行 VACUUM。
- `make db-maintenance`：串行执行 DB 体积、价值、retention 和 compact dry-run。
- `make db-migrate-all / db-migrate-date DATE=YYYY-MM-DD`：把 NDJSON archive mirror 到 SQLite。
- `make daily-close DATE=YYYY-MM-DD`：执行日终迁移、DB 检查、报告生成、压缩 dry-run 和 checkpoint。
- `make daily-close DATE=YYYY-MM-DD COMPRESS=YES`：日终流程并压缩指定日期 archive。
- `make archive-compress-dry-run DATE=YYYY-MM-DD`：预览压缩。
- `make archive-compress-date DATE=YYYY-MM-DD CONFIRM=YES`：明确确认后压缩指定日期 archive。
- `make sqlite-checkpoint`：执行 SQLite WAL checkpoint。
- `make report-overnight / report-state / report-all`：生成 overnight trade action、afternoon/evening state 或全量常用报告。
- `make test-all`：运行分组后的 SQLite、opportunity、state、action、LP 和 core 测试。
- `make archive-check`：列出 archive 最新目录，确认 raw/parsed/signals/audit/case 是否落盘。

提醒：
- 不要在启动前删除 archive/cache/db。
- 不要把 `.sqlite / .sqlite-wal / .sqlite-shm` 提交到 Git。
- 不要压缩当天正在写的 UTC 日期；如确实需要，必须显式 `ALLOW_TODAY=YES`。
- SQLite 是分析库，不替代 archive。
- compression 只把 `.ndjson` 替换为 `.ndjson.gz`，不等同于 prune；`make db-prune-execute` 才会删除过期 DB 数据。
- `make db-prune-execute` 必须显式加 `CONFIRM=YES`。
- 真实 DB 文件和运行数据不应提交 git。
