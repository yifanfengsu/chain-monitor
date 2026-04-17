raw_events：系统“看到了什么”
parsed_events：系统“理解成了什么”
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

CLMM replay fixture 分两层：
- `app/test_fixtures/clmm/` 是 normalized replay，直接对齐 parser 当前稳定输入接口
- `app/test_fixtures/clmm_decoder/` 是 decoder-aligned replay，保留更贴近上游 `decoded_logs_snapshot` 的形状，再经 `clmm_decoder_normalizer` 归一化后进入 parser/state/intent 链路

本次产品化 patch 新增了 4 个应用层 seam：
- user tiers：`retail / trader / research`，通过 `app/user_tiers.py` + `DEFAULT_USER_TIER` 控制 LP stage 准入、模板细节、debug 行展示与 asset-case 优先级；当前测试/默认运行保持 `DEFAULT_USER_TIER=research`
- asset case：`app/asset_case_manager.py` 维护短窗 token/asset 级聚合，只聚合 LP pool 事件；`asset_case_id / asset_case_key / asset_case_stage / supporting_pairs` 都挂在 event/signal metadata 里，并默认持久化到 `data/asset_cases.cache.json` 以支持跨进程/重启恢复
- market context：`app/market_context_adapter.py` 提供 `fixture / unavailable / live` 三层接口；`live` 只接公共市场数据 API（Binance USDⓈ-M / Bybit public），没有外部市场数据时统一回落到 `market_context_source=unavailable`
- outcome / quality：`app/quality_manager.py` 基于 `state_manager` 里的 `lp_alert_outcome_v1` 滚动记录计算 `pool_quality_score / pair_quality_score / asset_case_quality_score / prealert_precision_score / climax_reversal_score / fastlane_roi_score`，并把 rolling outcome / ops summary 持久化到 `data/lp_quality_stats.cache.json`

quality/outcome 本地结构说明：
- 原始 outcome 仍由 `StateManager.record_lp_outcome_alert()` 写入 `lp_alert_outcome_v1`
- 新增字段会把 `asset_symbol / asset_case_id / alert_relative_timing / market_context_source / lp_promoted_fastlane` 一起写进 outcome record
- quality manager 不做黑盒 ML，而是直接从 rolling outcome history 上算可解释 heuristic，并把结果回写到 signal context / metadata 供 routing、promotion、Telegram 模板使用

live market context 最小 env：
- `MARKET_CONTEXT_ADAPTER_MODE=live|fixture|unavailable`
- `MARKET_CONTEXT_PRIMARY_VENUE=binance_perp`
- `MARKET_CONTEXT_SECONDARY_VENUE=bybit_perp`
- `MARKET_CONTEXT_TIMEOUT_SEC=2.5`
- `MARKET_CONTEXT_CACHE_TTL_SEC=2.0`
- `BINANCE_FAPI_BASE_URL=https://fapi.binance.com`
- `BINANCE_SPOT_BASE_URL=https://api.binance.com`
- `BYBIT_V5_BASE_URL=https://api.bybit.com`

asset-case / quality cache：
- asset-case cache 默认路径：`data/asset_cases.cache.json`
- quality/outcome cache 默认路径：`data/lp_quality_stats.cache.json`
- 示例 schema：`data/asset_cases.cache.example.json`、`data/lp_quality_stats.cache.example.json`

quality report / 导出入口：
- `python -m app.quality_reports --summary`
- `python -m app.quality_reports --top-bad-prealerts`
- `python -m app.quality_reports --fastlane-roi`
- `python -m app.quality_reports --format csv`

当前 patch 明确不做多频道真实分发：
- 默认只跑 `research` 视图，研究期消息保留最完整字段
- 如果后续需要 `retail / trader / research` 多频道，可由用户自行多进程启动并传不同 env / `CHAT_ID`
- 多频道编排、私有账户 API key、下单/账户读取都不在本次 patch 范围
