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
- `address_aliases.json` 只能增强已存在于 `addresses.json` 的地址；如果 `data/addresses.json` 缺失，系统会 fail-closed 为 `ADDRESS_BOOK={}`，不会由 alias-only 数据创建监控地址
- `clmm_managers.json` 用来本地启用 Uniswap v3 / v4 PositionManager registry；缺失时 CLMM parser 会优雅降级为不识别 manager
- 需要本地注入时，直接把对应 JSON 文件放到 `data/` 目录即可；公开仓库提供 `data/entities.example.json`、`data/address_aliases.example.json` 和 `data/clmm_managers.example.json` 作为最小 schema 参考
- `data/lp_pools.json` 推荐本地 schema：
  - `pool_address`
  - `chain`
  - `pair_label`
  - `base_symbol`
  - `quote_symbol`
  - `canonical_asset`
  - `quote_canonical`
  - `dex`
  - `protocol`
  - `pool_type`
  - `fee_tier`
  - `enabled`
  - `priority`
  - `major_pool`
  - `validation_required`
  - `source_note`
  - `major_match_mode`
  - `notes`
  - 可选：`placeholder`
- `data/lp_pools.json` 既支持顶层数组，也支持按 section 分组的对象；`data/lp_pools.example.json` 现在就是 sectioned example
- validator 会 canonicalize `BTC/WBTC/CBBTC -> BTC`、`SOL/WSOL -> SOL`、`USDC/USDC.E -> USDC`
- placeholder 地址必须保持 `enabled=false`；coverage validator 会把 `placeholder_enabled` 当成 malformed entry
- malformed address 不会被静默忽略；duplicate pool address、unsupported chain、validator failure 和同 pair 同 priority 都会给 warning

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
- `VERIFIED` 仍要看 `verified_maturity`：当 maturity=`immature` 时只能用于研究复盘，不应当作成熟交易信号；`CANDIDATE` 从来不是交易信号
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
- `python -m app.quality_reports --report-source-summary --fast`
- `python -m app.quality_reports --report-source-fast`
- `python -m app.quality_reports --format csv`

SQLite mirror / query layer：
- SQLite 是 mirror / query / analysis layer，不替代 NDJSON archive；主监听、report fallback 和历史研究仍保留 JSON / NDJSON archive / cache
- 长期 full 保留的核心表是 `signals / signal_features / outcomes / trade_opportunities / opportunity_outcomes / quality_stats / asset_market_states / no_trade_locks / asset_cases / prealert_lifecycle / market_context_snapshots`
- `raw_events / parsed_events` 默认按 `index_only` 导入；full raw/parsed payload 长期保留在 `.ndjson / .ndjson.gz` archive
- `delivery_audit / telegram_deliveries / case_followups / market_context_attempts` 默认按 `slim` 或诊断型模式保留，不把 full payload 长期复制进 SQLite
- 默认 DB 路径：`data/chain_monitor.sqlite`
- reports 默认 `SQLITE_REPORT_READ_PREFER_DB=1`，会优先从 SQLite 读取；如果 DB 文件不存在、缺表、空表或读取失败，会在 `SQLITE_REPORT_FALLBACK_TO_ARCHIVE=1` 时回退到 archive/cache
- report fallback 同时支持 `*.ndjson` 和 `*.ndjson.gz`；同一天两种文件都存在时优先未压缩 `*.ndjson`，避免重复计数
- 查看当前 report source、DB/archive 行数、压缩 archive 检测和 mismatch warning：
  - `./venv/bin/python -m app.quality_reports --report-source-summary`
- 快速健康检查用 fast mode，不统计 archive 行数，也不扫描 `.ndjson.gz`：
  - `./venv/bin/python -m app.quality_reports --report-source-summary --fast`
  - `./venv/bin/python -m app.quality_reports --report-source-fast`
- heavy `--report-source-summary` 会扫描 archive / DB source mismatch；在大 archive 或大 SQLite 场景下可能较慢，只在完整检查时运行
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
  - `SQLITE_FULL_JSON_RETENTION_DAYS=3`
  - `signals / outcomes / trade_opportunities / asset_market_states / quality_stats` 保持 `full`
- `raw_events / parsed_events / delivery_audit / telegram_deliveries` 新写入会补 `archive_path / archive_date / payload_hash / payload_mode`，用于后续安全 compact；`market_context_attempts` 至少保留 `payload_hash / payload_mode`
- 迁移是可重复执行的；去重基于 `event_id / signal_id / signal_archive_key / trade_opportunity_id` 等主键，不会要求一次性迁完全部历史
- 即使启用 SQLite，raw_events / parsed_events / signals / delivery_audit / case_followups 仍继续写 NDJSON archive；asset cases / asset market states / quality stats / trade opportunities 仍保留 JSON cache
- 不要再跑前删除 archive/cache/db；旧数据是 `opportunity / verified / quality / outcome / blocker` 后验分析的基础，也是恢复路径和 report fallback 的来源；SQLite 初始化或写入失败时主流程会 warning 并继续走原 archive
- compact / retention 建议流程：
  - 先做 legacy payload metadata backfill，给旧 full payload rows 回填 `archive_path / archive_date / payload_hash / payload_mode`
  - 再做 `compact` 清理 DB 里的重复 full payload
  - 如需真正释放磁盘，再单独做 `VACUUM`
- 如果 `compact` 输出里 `skipped_no_payload_hash` 或 `skipped_no_archive_path` 很高，先跑 backfill，不要绕过 hash/archive 校验直接清 payload
- backfill / compact / vacuum 推荐顺序：
  - `make db-backfill-payload-dry-run`
  - `make db-backfill-payload-execute CONFIRM=YES`
  - `make db-compact-dry-run`
  - `make db-compact-execute CONFIRM=YES`
  - `make db-vacuum CONFIRM=YES`
- backfill / compact / vacuum 命令：
  - `./venv/bin/python -m app.sqlite_store --backfill-payload-metadata --dry-run`
  - `./venv/bin/python -m app.sqlite_store --backfill-payload-metadata --execute`
  - `./venv/bin/python -m app.sqlite_store --backfill-payload-metadata --table raw_events --dry-run`
  - `./venv/bin/python -m app.sqlite_store --backfill-payload-metadata --table parsed_events --execute`
  - `./venv/bin/python -m app.sqlite_store --backfill-payload-metadata --date YYYY-MM-DD --dry-run`
  - `./venv/bin/python -m app.sqlite_store --compact --dry-run`
  - `./venv/bin/python -m app.sqlite_store --compact --execute`
  - `./venv/bin/python -m app.sqlite_store --compact-table raw_events --dry-run`
  - `CONFIRM=YES ./venv/bin/python -m app.sqlite_store --vacuum`
- 安全约束：
  - backfill 只写 metadata，不清 payload，也不会触发 `VACUUM`
  - 只有 `compact` 才会清理 DB 里的 full payload
  - `compact` 清 payload 后，SQLite 文件不会立刻变小；真正回收磁盘要单独执行 `VACUUM`
  - 没有 `archive_path / payload_hash`、archive 文件不存在、或 archive/hash 校验不一致时，compact 不会清 full payload
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

## 每日 canonical report

- 日常只看一份 canonical daily report，不再把 `afternoon_evening_state`、`overnight_trade_action`、`overnight_run` 三套主报告当成日常入口。
- 旧三件套已退役；如果脚本仍保留，只在 `reports/legacy/` 下作为 legacy/debug 兼容入口，日常报告不依赖它们。
- canonical daily report 默认按北京时间 UTC+8 / `Asia/Shanghai` 自然日生成。比如 `--date 2026-04-24` 的逻辑窗口是北京时间 `2026-04-24 00:00:00 -> 2026-04-24 23:59:59`，对应 UTC `2026-04-23 16:00:00 -> 2026-04-24 15:59:59`。
- 日报不会跨大 gap 伪装连续运行。窗口内超过阈值的 gap 会拆成多个 segment，并同时输出 `wall_clock_duration_hours` 和 `active_duration_hours`。
- `wall_clock_duration_hours` 是完整逻辑日窗口长度；`active_duration_hours` 是实际有数据的 active segments 时长合计。`segment_count > 1` 时，报告不会声称“连续运行 X 小时”。
- 生成命令：
  - `make report-daily`
  - `make report-daily-date DATE=YYYY-MM-DD`
  - `make report-daily-range START_DATE=YYYY-MM-DD END_DATE=YYYY-MM-DD`
- 输出路径：
  - `reports/daily/daily_report_YYYY-MM-DD.md`
  - `reports/daily/daily_report_YYYY-MM-DD.csv`
  - `reports/daily/daily_report_YYYY-MM-DD.json`
  - `reports/daily/daily_report_latest.md/csv/json`
- 清理旧生成报告：
  - `make report-clean-dry-run`
  - `make report-clean-generated CONFIRM=YES`
- 清理命令只处理 generated reports，不删除 `archive/cache/db`，也不删除 `reports/*.py`、templates、examples、README 或 `.gitkeep`。如果历史报告已被 git 跟踪，需要另行用 `git rm --cached` 清索引。
- 重建历史每日报告：
  - `make report-daily-range START_DATE=YYYY-MM-DD END_DATE=YYYY-MM-DD`
- daily compare 现在默认只比较 canonical daily reports；旧三件套只有显式启用 legacy fallback 时才会作为兼容输入。
- canonical daily report 不是交易建议。`CANDIDATE` 不是交易信号；`VERIFIED` 在 `maturity=immature` 时不能当成熟交易信号。
- 删除或清理旧 report generator / generated reports 不影响 archive/cache/db；不要删除 `app/data/archive/`、`data/chain_monitor.sqlite` 或 `data/*.cache.json`。

## 每日对比报告

- 作用：
  - `make daily-compare` 会优先比较两份 canonical daily report，重点看系统质量、Telegram 降噪、候选/机会质量、outcome 完整性、market context 命中率和 majors 覆盖是进步、持平还是退步
  - `make daily-compare DATE=YYYY-MM-DD` 会把 `DATE` 视作 today，并自动寻找小于该日期的最近一个可用日期作为 previous
  - compare 只基于真实结构化 summary JSON，不会编造 yesterday；如果前一个日期不存在，会明确写 `compare not available`
- 命令：
  - `make daily-compare`
  - `make daily-compare DATE=YYYY-MM-DD`
  - `make daily-compare-strict`
  - `make daily-compare-strict DATE=YYYY-MM-DD`
  - `make daily-compare-rebuild`
  - `make daily-compare-rebuild DATE=YYYY-MM-DD`
- 模式区别：
  - `daily-compare`：宽松模式；能出就出，缺字段或缺某类 summary 时写进 `limitations`
  - `daily-compare-strict`：严格模式；要求 today/previous 的 canonical daily reports 已存在，缺失就 fail-closed，并在 JSON / Markdown 里写出 `strict_failure_reason`
  - `daily-compare-rebuild`：显式优先补 today/previous 的 canonical daily reports，再按宽松模式输出 compare；适合日常补齐依赖后再看报告，并会输出 `rebuild_summary` / `rebuild_warnings`
- 输入文件：
  - 第一优先读取 `reports/daily/daily_report_YYYY-MM-DD.json`
  - 其次允许读取 `reports/daily/daily_report_latest.json`，但 summary 内 `logical_date` 必须匹配
  - 默认不会 fallback 到旧三件套 summary；canonical daily report 缺失时会写入 `limitations`，不会编造
  - 只有显式运行 `./venv/bin/python reports/generate_daily_compare_report.py --legacy-fallback` 时，才允许使用旧三件套 summary：`overnight_trade_action`、`afternoon_evening_state`、`overnight_run`
  - 使用旧三件套 fallback 时，JSON 会写 `source_mode=legacy_three_report_bundle`、`legacy_fallback_used=true`，limitations 会写 `legacy_source_used`
  - `daily-compare-rebuild` 会优先尝试按逻辑日期重建 canonical daily report；重建仍依赖真实 SQLite / archive / cache 数据，不会伪造历史
  - compare 不会用 Markdown 正文做字符串 diff；如果字段不存在，会在报告里写 `字段缺失` 或 `schema drift / 字段不一致`
- `compare_basis`：
  - `exact_previous_day`：today 与精确前一天比较
  - `previous_available_day`：精确前一天缺 summary，改用更早的最近可用日期
  - `not_available`：today 有数据，但没有 previous 可比日期
  - `no_data`：`reports/` 下没有可用的主 summary JSON
- 输出目录：
  - `reports/daily_compare/`
- 输出文件：
  - `reports/daily_compare/daily_compare_YYYY-MM-DD_vs_YYYY-MM-DD.md`
  - `reports/daily_compare/daily_compare_YYYY-MM-DD_vs_YYYY-MM-DD.csv`
  - `reports/daily_compare/daily_compare_YYYY-MM-DD_vs_YYYY-MM-DD.json`
  - `reports/daily_compare/daily_compare_latest.md`
  - `reports/daily_compare/daily_compare_latest.csv`
  - `reports/daily_compare/daily_compare_latest.json`
- 输出内容：
  - Markdown：执行摘要、数据来源、compare basis、`重建尝试与严格模式结果`、问题回答和关键限制
  - CSV：逐指标的 `today_value / previous_value / abs_change / pct_change / interpretation`
  - JSON：结构化 compare summary，包含 `today_date`、`previous_date`、`compare_basis`、`source_files`、`strict_failure_reason`、`rebuild_summary`、`rebuild_warnings`、`data_source_summary`、`improvement_flags`、`regression_flags`、`limitations`
- `source_files`：
  - compare JSON 顶层会显式列出 compare 实际使用的 `today` 和 `previous` source；canonical 模式下是 `daily_report` 路径
  - legacy fallback 模式下才会列出三份旧 summary JSON：`afternoon_evening_state`、`overnight_trade_action`、`overnight_run`
  - 每个 source entry 都会带 `path / source_kind / logical_date / warnings`
  - 如果缺少某类 summary，不会编造，会在 `source_files` 和 Markdown “数据来源”章节里明确标成 missing
- `strict_failure_reason`：
  - 只有 strict 模式最终失败时才会非空，用来解释为什么 fail-closed，例如 `no_previous_available`、`incomplete_today_bundle`、`rebuild_failed`
  - strict 模式失败不会只给 exit code 2；JSON 和 Markdown 都会写明原因
- `rebuild_summary` / `rebuild_warnings`：
  - `rebuild_summary` 是给机器读的结构化结果，会写明是否启用了 strict / rebuild、选择了哪个 today / previous，以及 today/previous 各 report type 是否尝试补建、成功或失败
  - `rebuild_warnings` 是给人看的字符串摘要；如果补建失败、输出缺失、logical date mismatch，都会列在这里
  - rebuild 只会尝试补 today/previous 需要的 dated summaries，不会编造 yesterday；如果 `compare_basis=previous_available_day`，说明并不是精确昨天
- 如何理解结果：
  - Candidate 变多不一定更好；要同时看 `followthrough / adverse / completion`
  - Blocked 变多不一定是退步；可能是市场更差，或者系统更会保护你
  - Telegram 消息变少不一定代表漏信号；要同时看 suppression reason 和 `high_value_suppressed_count`
  - Verified 为 `0` 不一定是坏事；如果 blocker saved rate 提升且 false block rate 没恶化，可能只是系统更保守
  - `compare_basis=exact_previous_day` 说明是精确日对日；`compare_basis=previous_available_day` 说明中间有缺口，不是自然日连续对比
- 重要提醒：
  - compare 报告不是交易建议，不是自动下单建议
  - 如果 yesterday / previous 数据缺失，报告不会编造
  - 如果 `source_files` 缺失、字段缺失或发生 `schema drift`，报告会在 `limitations` 里明确写出
  - compare 命令只负责比较，不会压缩 archive，也不会删除 archive/cache/db

majors-first pool book 指引：
- `LP_MAJOR_ASSETS / LP_MAJOR_QUOTES / LP_MAJOR_PRIORITY_SCORE` 控制 majors 覆盖与优先级
- 当前第一优先级是补 Ethereum BTC majors：
  - `WBTC/USDC` Uniswap v3 `0.3%`
  - `WBTC/USDC` Uniswap v3 `0.05%`
  - `WBTC/USDT` Uniswap v3 `0.3%`
  - 可选 `USDC/cbBTC`
- `data/lp_pools.example.json` 已按 section 提供公开候选：
  - `ethereum_btc_major_pools`
  - `optional_ethereum_cbbtc_pools`
  - `solana_sol_major_candidates`
  - `base_sol_evm_candidates`
- 当前系统运行态默认只把 `chain=ethereum` 的 pool 视作 active scan 候选；Solana 原生池和 Base 池只做 coverage / warning，不会误算进 Ethereum majors
- `python -m app.quality_reports --major-pool-coverage` 现在会额外输出：
  - `covered_major_pairs`
  - `missing_major_pairs`
  - `configured_but_disabled_major_pools`
  - `configured_but_unsupported_chain`
  - `configured_but_validation_failed`
  - `placeholder_major_pool_entries`
  - `malformed_major_pool_entries`
  - `enabled_major_pools`
  - `active_recent_major_pairs`
  - `no_recent_signal_major_pairs`
  - `recommended_local_config_actions`
- 本地启用流程：
  - 复制 `data/lp_pools.example.json` 中需要的 entry 到私有 `data/lp_pools.json`；可以复制整段 section，也可以只拷单个条目
  - 先启用 Ethereum BTC pools；`enabled=true` 前必须确认 `chain / dex / protocol / pool_type / base_symbol / quote_symbol / canonical_asset / fee_tier`
  - BTC 如果出现在 `configured_but_disabled_major_pools`，表示本地已配置但还没有启用；确认真实 pool 元数据后再把 `enabled=true`
  - 运行 `./venv/bin/python -m app.quality_reports --major-pool-coverage`
  - 确认 validator 通过：`token0 / token1 / fee / factory` 没有落到 `configured_but_validation_failed`
  - 确认没有进入 `configured_but_unsupported_chain / malformed_major_pool_entries / placeholder_major_pool_entries`
  - 重启当前 research/listener 进程，让 `lp_registry` 重新加载本地 `data/lp_pools.json`
  - 跑一轮 research 默认流程后，再看目标 pair 是否从 `enabled_major_pools` 迁移到 `active_recent_major_pairs`
- Solana 原生池不是 Ethereum 池：
  - Orca / Raydium 的 `SOL/USDC` 需要 Solana listener/parser
  - 不要把 Solana base58 pool address 放进 Ethereum scanner
  - 没有 Solana listener 时，SOL 应保持 missing/unsupported，不要用占位数据绕过 majors guard
- Base `SOL/USDC` 是 EVM，但不是 Ethereum mainnet：
  - 需要 Base RPC/listener
  - 当前 Ethereum-only 运行态下只会显示 `configured_but_unsupported_chain`
  - Base 池需要明确的 Base chain routing，不能当作 Ethereum 池
- 如何确认 listener 扫到了对应池：
  - `major-pool-coverage` 中目标 pair 先进入 `enabled_major_pools`
  - 跑过一轮 research/live 采样后，目标 pair 应从 `no_recent_signal_major_pairs` 迁移到 `active_recent_major_pairs`
  - 如果长期没有迁移，优先检查 `pool_address`、`chain`、`dex`、`pool_type` 和本地 listener 扫描范围
- `data/lp_pools.json` 属于本地私有配置，不要提交到 GitHub；公开仓库只保留 example
- 先不扩 long-tail 的原因：
  - 当前 outcome / quality / market context 样本仍主要集中在 majors
  - 先把 majors 的 `prealert -> confirm`、confirm scope、reversal / exhaustion 统计做稳更有价值
- 公开仓库不内置 unverifiable 私有 pool 地址；缺少本地 pool book 时 coverage report 会给 warning，但不会伪造数据

Large module refactor roadmap：
- 本轮不拆业务逻辑、不改变 import 和行为，只记录后续低风险拆分方向
- `pipeline.py` 后续可按职责拆出 `pipeline_archiving.py`、`pipeline_market_context.py`、`pipeline_opportunity_delivery.py`
- `sqlite_store.py` 后续可拆出 `sqlite_compaction.py`、`sqlite_migration.py`
- `trade_opportunity.py` 后续可拆出 `opportunity_scoring.py`、`opportunity_reports.py`
- 拆分前应先补 characterization tests，确保 LP / trade_action / asset_market_state / trade_opportunity 语义不变

当前 patch 明确不做多频道真实分发：
- 默认只跑 `research` 视图，研究期消息保留最完整字段
- 如果后续需要 `retail / trader / research` 多频道，可由用户自行多进程启动并传不同 env / `CHAT_ID`
- 多频道编排、私有账户 API key、下单/账户读取都不在本次 patch 范围

## 常用 Make 命令

SQLite 角色先说明清楚：
- SQLite 是 mirror / query / analysis layer，不替代 `app/data/archive/**/*.ndjson`、`*.ndjson.gz` 和本地 JSON cache。
- reports 默认优先读 SQLite，但 DB 不存在、缺表、空表或读取失败时，仍可回退到 archive / cache。
- 不要再跑前删除 `archive/cache/db`；旧数据是 `opportunity / verified / quality / outcome / blocker` 后验分析的基础。

### 1. 启动前检查

- `make smoke-fast`
- `make smoke`
- `make smoke-full`
- `make preflight`
- `make preflight-full`
- `make env-check`
- `make health`
- `make coverage`
- `make db-integrity`
- `make db-report`
- `make report-source`
- `make report-source-fast`

`make env-check` 只打印非敏感配置：`DEFAULT_USER_TIER`、market context venue、archive 开关、SQLite 开关、`SQLITE_REPORT_READ_PREFER_DB`、`OPPORTUNITY_ENABLE`、`ASSET_MARKET_STATE_ENABLE`、`NO_TRADE_LOCK_ENABLE`。不会打印 Telegram token、RPC URL、API key、secret 或私钥。

`make smoke` 是 `make smoke-fast` 的别名，只跑 compile、fast DB integrity、market-context health 和 majors coverage，适合日常快速检查。fast DB integrity 会跳过 archive mirror 行数统计和 full `PRAGMA integrity_check`；`make smoke-full` 和 `make preflight-full` 会额外运行 `db-report`、heavy `report-source`、`opportunity-db` 和 `archive-check`，需要完整 DB/archive 检查时再跑。

### 2. 运行

- `make run`
- `make run-research`

`make run-research` 会强制使用 `DEFAULT_USER_TIER=research`、`MARKET_CONTEXT_ADAPTER_MODE=live`、`MARKET_CONTEXT_PRIMARY_VENUE=okx_perp`、`MARKET_CONTEXT_SECONDARY_VENUE=kraken_futures`。

### 3. 报告

- `make report-daily`
- `make report-daily-date DATE=YYYY-MM-DD`
- `make report-daily-range START_DATE=YYYY-MM-DD END_DATE=YYYY-MM-DD`
- `make report-all`
- `make report-clean-dry-run`
- `make report-clean-generated CONFIRM=YES`
- legacy/debug only:
  - `make report-legacy-all`
  - `make report-overnight`
  - `make report-state`
  - `make report-run`

`make report-all` 现在只跑 canonical daily report 和 daily compare，不再默认生成旧三件套。`make report-state` / `make report-overnight` / `make report-run` 是 legacy/debug 入口；`make report-run` 会在 `reports/legacy/generate_overnight_run_analysis_latest.py` 存在时执行，脚本不存在时只输出 `report-run script not found`。如果旧脚本已移除，`make report-legacy-all` 会友好提示 `Legacy report generators removed.`。

报告生成物默认不进 git：`reports/daily/*.md/csv/json`、`reports/*latest*.md/csv/json`、`reports/daily_compare/*.md/csv/json`、`reports/generated/` 和 `reports/archive/` 已被忽略。需要分享某一天的报告时，用 `git add -f reports/...` 精确添加；不要用 `git add .` 提交海量生成报告。

### 4. SQLite 基础维护

- `make db-init`
- `make db-summary`
- `make db-integrity`
- `make db-report`
- `make db-migrate-all`
- `make db-migrate-date DATE=YYYY-MM-DD`
- `make sqlite-checkpoint`

`make db-integrity DB_INTEGRITY_FAST=YES` 会跳过 archive mirror counts 和 full `PRAGMA integrity_check`，只做 SQLite schema/table count 快速检查。`make db-report` 会串行执行 `--db-summary`、`--db-integrity`、`--opportunity-db-summary`，适合做 DB-first report 检查。`make report-source-fast` 只看 DB 是否存在、SQLite table row counts、report read preference 和 archive 目录是否存在；`make report-source` 会做完整 DB/archive source 检查，并可能扫描大 archive / `.ndjson.gz` 行数。

### 5. SQLite 体积治理

- `make db-size`
- `make db-value-audit`
- `make db-retention`
- `make db-maintenance`
- `make db-backfill-payload-dry-run`
- `make db-backfill-payload-execute CONFIRM=YES`
- `make db-backfill-table-dry-run TABLE=parsed_events`
- `make db-backfill-table-execute TABLE=parsed_events CONFIRM=YES`
- `make db-compact-dry-run`
- `make db-compact-execute CONFIRM=YES`
- `make db-compact-table-dry-run TABLE=raw_events`
- `make db-compact-table-execute TABLE=raw_events CONFIRM=YES`
- `make db-vacuum CONFIRM=YES`

体积治理的区别和顺序：
- backfill 是给旧数据补 `archive_path / archive_date / payload_hash / payload_mode`，只写 metadata，不清 payload。
- compact 是清理 SQLite 里已经有 archive 备份的 full payload；它会让表变瘦，但 SQLite 文件不会立刻变小。
- vacuum 才会真正重写 SQLite 文件并回收磁盘空间。
- 如果 `db-compact-dry-run` 里 `skipped_no_payload_hash` 很高，先跑 backfill，不要跳过 hash/archive 校验。
- 如果 `make db-vacuum` 输出 `confirm_required`，说明没有带 `CONFIRM=YES`。
- 如果 DB 仍然很大，先看 `make db-size` 和 `make db-value-audit`，不要直接删 DB。

推荐顺序：
- `make db-backfill-payload-dry-run`
- `make db-backfill-payload-execute CONFIRM=YES`
- `make db-compact-dry-run`
- `make db-compact-execute CONFIRM=YES`
- `make db-vacuum CONFIRM=YES`
- `make db-integrity`
- `make db-summary`

`compression`、`compact`、`vacuum`、`prune` 不是一回事：
- compression 是把 archive 的 `.ndjson` 压成 `.ndjson.gz`，不删除记录。
- compact 是清理 SQLite 里可由 archive 重建的 full payload。
- vacuum 是缩小 SQLite 文件本身。
- prune 是按 retention 删除行；它是单独维护动作，不在标准 daily-close 流程里。需要时先跑 `make db-prune-dry-run`，真正执行再用 `make db-prune-execute CONFIRM=YES`。

### 6. Archive / 日终

- `make archive-status DATE=YYYY-MM-DD`
- `make archive-compress-dry-run DATE=YYYY-MM-DD`
- `make archive-compress-date DATE=YYYY-MM-DD CONFIRM=YES`
- `make daily-close DATE=YYYY-MM-DD`
- `make daily-close DATE=YYYY-MM-DD COMPRESS=YES`
- `make daily-close DATE=YYYY-MM-DD COMPRESS=YES CONFIRM=YES`
- `make daily-close-strict DATE=YYYY-MM-DD COMPRESS=YES`
- `make daily-close-strict DATE=YYYY-MM-DD COMPRESS=YES CONFIRM=YES`

`daily-close` 会按顺序执行：`archive -> SQLite migrate`、`db-integrity`、`db-summary/db-report`、mirror-check、`report-daily-date DATE=$(DATE)`、`daily-compare DATE=$(DATE)`、archive compression dry-run、SQLite checkpoint。它不再默认生成 `afternoon_evening_state`、`overnight_trade_action` 或 `overnight_run`。

Archive / 日终的安全约束：
- compression 是 gzip archive，不是删除数据。
- `COMPRESS=YES` 只表示进入压缩分支；这个 Makefile 里真正执行 gzip 仍需要显式 `CONFIRM=YES`。
- 不要压缩当天仍在写入的 archive date；如确实需要，必须显式 `ALLOW_TODAY=YES`。
- `daily-close` 和 `daily-close-strict` 都不会自动做 prune，也不会删除 DB / cache / archive。
- `daily-close-strict` 会用 `mirror-check --strict`；如果 DB / archive mismatch，会直接失败。

### 7. 测试

- `make test-sqlite`
- `make test-sqlite-compact`
- `make test-reports`
- `make test-opportunity`
- `make test-state`
- `make test-action`
- `make test-lp`
- `make test-clmm`
- `make test-core`
- `make test-all`

这些 target 会自动过滤当前分支不存在的测试模块：存在就运行，不存在就打印 `skip missing test module: ...`，不会因为分支上暂缺某个测试文件而让整个 target 直接失败。

### 8. 安全提醒

- 不要跑前删除 `archive/cache/db`。旧数据是 `opportunity / verified / quality / outcome / blocker` 后验分析、report fallback 和 mirror 对账的基础。
- DB 文件不会提交 git；仓库已忽略 `data/*.sqlite`、`data/*.sqlite-wal`、`data/*.sqlite-shm`、`data/*.db`。
- `CONFIRM=YES` 是危险命令确认，当前用于 `db-backfill-*-execute`、`db-compact-*-execute`、`db-vacuum`、`archive-compress-date`、`db-prune-execute`，以及 `daily-close COMPRESS=YES` 的实际压缩执行。
- `CANDIDATE` 不是交易信号，只表示候选，不代表“现在就该追”。
- `VERIFIED` 也不是自动下单，不会触发下单、不会给你自动仓位、也不会替代止损止盈和风险管理。
- `NO_TRADE` / `NO_TRADE_LOCK` / `DO_NOT_CHASE_LONG` / `DO_NOT_CHASE_SHORT` 必须按硬阻断对待，不是可忽略的小提示。
- 系统是个人交易辅助和研究工具，不是自动交易系统。
