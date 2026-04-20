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
  - `dex`
  - `protocol`
  - `pool_type`
  - `enabled`
  - `priority`
  - 可选：`placeholder` / `note`
- 可直接参考 `data/lp_pools.example.json`
- placeholder 地址必须保持 `enabled=false`；coverage validator 会把 `placeholder_enabled` 当成 malformed entry

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
- `python -m app.quality_reports --format csv`

majors-first pool book 指引：
- `LP_MAJOR_ASSETS / LP_MAJOR_QUOTES / LP_MAJOR_PRIORITY_SCORE` 控制 majors 覆盖与优先级
- 本地扩 `data/lp_pools.json` 时优先补 `ETH / BTC / SOL` 对 `USDT / USDC` 主池
- 下一轮研究采样明确优先补 `BTC/USDT`、`BTC/USDC`、`SOL/USDT`、`SOL/USDC`，以及更多已验证的 `ETH` 主池
- `python -m app.quality_reports --major-pool-coverage` 现在会额外输出：
  - `covered_major_pairs`
  - `missing_major_pairs`
  - `configured_but_disabled_major_pools`
  - `malformed_major_pool_entries`
  - `recommended_local_config_actions`
- 先不扩 long-tail 的原因：
  - 当前 outcome / quality / market context 样本仍主要集中在 majors
  - 先把 majors 的 `prealert -> confirm`、confirm scope、reversal / exhaustion 统计做稳更有价值
- 公开仓库不内置 unverifiable 私有 pool 地址；缺少本地 pool book 时 coverage report 会给 warning，但不会伪造数据

当前 patch 明确不做多频道真实分发：
- 默认只跑 `research` 视图，研究期消息保留最完整字段
- 如果后续需要 `retail / trader / research` 多频道，可由用户自行多进程启动并传不同 env / `CHAT_ID`
- 多频道编排、私有账户 API key、下单/账户读取都不在本次 patch 范围
