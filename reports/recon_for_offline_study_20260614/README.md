# 链上监控系统勘察报告 — 信号定义提取 + 历史数据可得性评估

> **定位声明**
> 本任务勘察一个已停止的链上监控系统（`~/whale`，独立仓库，与 cta_strategy 无关）。
> 该系统曾被认为发现有预测力的信号（LP 大额流出 + 链上资金转入交易所 → 价格下跌），
> 但死于"实时轮询 + Telegram 人工通知"的执行延迟（信号到达时行情已在尾部）。
> 本勘察为"用历史数据离线检验该信号是否构成可立项 edge"做准备——**只摸清信号定义与
> 数据可得性，不做研究本身，不运行实时系统、不烧 CU、不读凭证**。
>
> 本报告**纯勘察**：未运行实时监控、未读 `.env`、未调用付费 RPC（连 2b 允许的单合约单日
> getLogs 小样本试探也主动跳过以零烧 CU）、未做任何回测/统计/立项。

生成时间（UTC）：2026-06-14T12:31:55Z ｜ 仓库：`chain-monitor` ｜ 链：以太坊（仅）

---

## 第 0 部分：仓库已经回答了什么 / 还缺什么

| 主题 | 仓库是否已回答 | 出处 |
|---|---|---|
| 系统架构、模块职责 | ✅ 完整 | `AGENTS.md`（权威），`README.MD`（操作向） |
| 信号精确定义与阈值 | ✅ 代码可还原 | `app/config.py`（阈值默认值）、`app/pipeline.py`、`app/lp_analyzer.py`、`app/analyzer.py`、`app/signal_quality_gate.py`、`app/liquidation_detector.py` |
| 事件→价格关联机制 | ✅ 机制存在 / ❌ 无数据 | `app/outcome_scheduler.py`（30/60/300s）、`app/trade_replay.py` |
| 后验/盈亏分析 | ⚠️ 仅"基础设施已建"，**无真实样本** | `SIGNAL_TO_TRADE_POSTERIOR_UPGRADE.md`（见 Q6） |
| 历史落盘数据 | ⚠️ 仅 ~1 天、本地、未入版本库 | `app/data/archive/*.ndjson`（gitignore），`data/chain_monitor.sqlite`（空） |
| 监控范围（地址/池/CEX） | ✅ | `data/addresses.json`(19) `data/lp_pools.json`(4) `data/entities.json`(6) |
| 价格源 | ✅ | `app/market_data_clients.py`（OKX perp 主 / Kraken futures 次） |

**仓库已明确、可直接引用**：信号定义、阈值、outcome 窗口、价格源、监控范围。
**仓库缺失、必须新做**：任何**经过验证的** event→price 统计；足量历史样本；
跨多日的落盘数据。`SIGNAL_TO_TRADE_POSTERIOR_UPGRADE.md` 标题暗示的"后验"
**是代码实现 + 单元测试，不是实证结论**（详见 Q6）。

> 重要约束（来自 `AGENTS.md`）：本系统**不是**自动交易系统，`CANDIDATE`/`VERIFIED`
> **不是**下单指令。本勘察沿用该边界——只评估信号是否值得离线研究，不产出交易指令。

---

## 第 1 部分：信号定义提取（精确还原，见 `signal_definitions.json`）

**(1a) 监控对象范围**
- **链**：仅以太坊（active LP 扫描 Ethereum-only；Solana/Base 仅出现在覆盖诊断，非运行时扫描目标）。
- **代币**：**动态**——由 19 个被监控地址 + 4 个 LP 池触碰的 token 决定，无固定白名单。样本期 top token：USDT、WETH、USDC、PEPE 等。
- **DEX/池**：4 个 Uniswap V2 池——`ETH/USDT`、`ETH/USDC`、`WBTC/USDT`、`WBTC/USDC`。
- **被监控地址**：19 个（14 active）；分布 smart_money 11、exchange_hot_wallet 4、market_maker 3、celebrity 3、issuer 1。
- **CEX 实体（用于分类）**：binance、okx、bybit、jump_trading、wintermute、uniswap。

**(1b) 每个信号的触发条件（精确到阈值）**——金额分桶：`small <$1500 / medium $1500–$10000 / large ≥$10000`（`pipeline.py:8790`）。

- **大额转入交易所 `exchange_deposit_candidate`（策略档核心信号①）**
  `pipeline.py:8876-8907`：非 swap 事件 + 被监控地址是交易所热钱包 + `direction==流入` + `size_bucket==large(≥$10k)` + 对手方为外部非交易所地址 → 触发（conf 0.56）。medium 桶需 stablecoin 或对手未知。镜像：非交易所地址把 token 打向交易所相关对手方（`pipeline.py:8983`）也判 deposit。**语义=资产进入 CEX 热钱包→潜在抛压→看跌先验。**
- **LP 大额流出 `liquidity_removal`（策略档核心信号②）**
  `lp_analyzer.py:129-133, 250-257`：监控池内 `base_net<0 AND quote_net<0`（**两侧储备同时流出** = Burn/移除流动性，区别于 swap 的一进一出）→ conf 0.76。预警阈值：`MIN_ACTION_INTENSITY=0.26`、`MIN_VOLUME_SURGE_RATIO=1.25`、`LP_PREALERT_MIN_USD=1500`。
- **流动性清扫 `lp_sweep`**（`config.py:315-322`）：同池同向 burst — `reserve_skew≥0.18`、`action_intensity≥0.42`、`volume_surge≥1.8`、`same_pool_continuity≥2`、`burst_event_count≥3` 且 `burst_window≤45s`；分 continuation(≥0.56) / exhaustion(≥0.60) 两态，`sweep_exhaustion_risk` 是**硬阻断**。
- **同池连续买/卖压 `pool_buy/sell_pressure`**：swap + 连续同向 ≥ max(min_consecutive-1, 2)（`analyzer.py:126-139`），conf 0.78。
- **交易所外流 `exchange_withdraw_candidate`** / **入场前准备 `possible_buy_preparation`**：见 `pipeline.py:8909-9000`。
- **清算 `liquidation`**（`liquidation_detector.py`，`config.py:768-777`）：`MIN_USD=35k`/`PRIMARY=125k`，risk score≥0.66 / execution≥0.86。
- **其他**：`liquidity_addition`、`pool_rebalance`、`lp_burst_fastlane`、`smart_money swap_execution`、CLMM 仓位事件类（6 种）、`market_making_inventory_move`、邻接/下游预警、`pure_transfer`（噪声基线）。完整 16 类见 JSON。

**(1c) 轮询机制（仅理解语义与延迟，不复用）**
- 区块轮询 `PRODUCER_POLL_INTERVAL_SEC = 1.0s`；LP 扫描分级：主趋势 12s、次级 18s(low-CU)、邻接次级 15s、扩展 60s（默认关）。
- 用 Alchemy `getLogs`（bloom 预过滤 + 方向性扫描，`LOW_CU_MODE=True` 省 CU）。
- 链路：`listener`(getLogs) → `processor/pipeline`(解析+intent) → `signal_quality_gate` → `trade_opportunity` → `notifier`(Telegram)。outcome 在 `outcome_scheduler` 以 5s tick 在 +30/60/300s 结算。
- **延迟来源**见 Q4。

**(1d) 代码里是否已有"事件→价格"关联？**
- **是**：`outcome_scheduler.py` 定义 `OUTCOME_WINDOWS=(30,60,300)` 秒，对每条信号在 +30/60/300s 用 **OKX perp mark** 结算价格，派生 followthrough_rate / adverse_rate / completion_rate；`trade_replay.py` 进一步模拟入场(+5s)/止损(30bps)/止盈(50bps)/最长持有(60s)、扣费(6bps)+滑点(5bps)、打标签。
- **但**：该机制**从未产出真实样本**（见 Q2a / Q6）——所有 outcome 计数为 0。

---

## 第 2 部分：历史数据可得性（见 `data_availability.json`）

**(2a) 自身落盘**
- **SQLite (`data/chain_monitor.sqlite`, 508KB)**：19 张表，**仅 `schema_meta` 1 行**，其余全空 → mirror 从未填充/被重置，**不可用**。
- **NDJSON archive (`app/data/archive/`，gitignore，本地唯一副本)**：
  - `delivery_audit.ndjson`：**1758 行 / 6.5MB**，覆盖 **2026-04-05 18:14 → 04-06 14:24 BJ（~20 小时）**。字段极丰富（`archive_ts`、`tx_hash`、`token`、`usd_value`、`side`、`intent_type`、`is_exchange_related`、`lp_action` 等），可对齐价格。**但全部是 `delivery_class=drop`（被门控丢弃的事件日志，不是已推送信号）。**
  - `signals/*.ndjson`：**仅 3 条已触发信号**（4-04: 2、4-05: 1）；带 tx_hash/ts/token/usd_value/intent_type/confidence，**不含价格/outcome 字段**。
  - `raw_events`/`parsed_events`/`cases`：0（未启用保留）。
- **Telegram 推送历史导出**：未发现导出文件；样本窗口内 delivery_audit 全是 drop（实际什么都没推）。
- **结论**：自身落盘 = **~1 天、本地、未入版本库、无价格 outcome**。作为现成原料**红灯**。

**(2b) 批量回溯途径（只评估，未拉取；连允许的单合约单日 getLogs 试探也主动跳过以零烧 CU）**
| 途径 | 可得性 | 成本 | 工程量 | 质量 |
|---|---|---|---|---|
| **归档节点 getLogs（推荐）** | 高（Alchemy 已配） | Alchemy CU | 中（复用 app/ 分类器重放历史 log；池仅 4 个，scope 小） | 高/canonical，定义零漂移 |
| Dune | 高 | 低 | 中（SQL+重写分类，有定义漂移风险） | 良 |
| The Graph (UniV2 子图) | 中 | 低 | 中 | LP 好、交易所地址流弱 |
| Allium/Nansen | 高（付费） | 高 | 低-中 | 高，自带 CEX 标签 |

**(2c) 价格对齐**
- `delivery_audit`/`signals` 带 `archive_ts`（unix）+ event `ts`；按 `floor(ts/60)` 对齐分钟 K。系统本身用 OKX perp mark 在 30/60/300s（亚分钟）结算，精细研究建议用 OKX 1m/trades。
- 监控的 major（LP 池为 ETH/WBTC → **ETH、BTC**）任何价格库都覆盖；长尾 token（PEPE 等）需补价。（cta_strategy 价格库未查，跨仓库越界——ETH/BTC 假定安全。）

---

## 第 3 部分：标的映射

**(3a) 可交易永续**
- **LP 信号标的 = ETH、BTC**（池固定 ETH/USDT、ETH/USDC、WBTC/USDT、WBTC/USDC）→ OKX/Binance 永续均有、流动性极高 → **可货币化候选（最干净）**。
- **交易所充值信号标的 = 动态**，需逐 token 核对：WETH→ETH 永续 ✓、PEPE→PEPE 永续 ✓；**USDT/USDC 主导的流（1318/1758 是 stablecoin flow）本身无方向永续**——被搬运的稳定币不可直接做方向，只能当作"对某资产的抛压/买盘"的间接证据。

**(3b) 时间尺度**
- 频率（样本期 ~20h，**多为门控前/已丢弃**）：deposit 候选 233、withdraw 候选 78、LP 买+卖压 33、liquidity_addition 1、liquidity_removal 0。门控后真正 actionable 远低于此。
- **单日样本量远低于系统自身要求的 `OPPORTUNITY_MIN_HISTORY_SAMPLES=20`。**
- 反应窗口：系统在 30s/60s/300s 衡量 → 假设的 edge 是**分钟级**，与"执行延迟杀死信号"的叙事一致。

---

## 第 4 部分：勘察结论

### Q1 — 信号清单
16 类信号，**全部"代码明确"**（无"仅文档提及未实现"项）。核心两个（策略档点名）：
`exchange_deposit_candidate`（转入交易所→抛压）与 `liquidity_removal`（LP 两侧同时流出）。
精确定义+阈值见 `signal_definitions.json`。所有数值阈值为 `config.py` 的**可被 .env 覆盖的默认值**（未读 .env，运行时实值可能不同——此处标注为不确定项）。

### Q2 — 历史数据可得性判定：🟡 **黄灯（偏红）**
- 信号定义提取本身 = 🟢 绿灯（完全可还原）。
- 自身落盘作为原料 = 🔴 红灯（~1 天、本地、无价格 outcome、SQLite 空）。
- 经一次中等工程的**归档节点 getLogs 回溯**后 = 🟡 黄灯可启动。
- 综合：**离线研究不能"立即"用现成数据启动，但有清晰的中等成本路径。**

### Q3 — 可交易标的清单
- 最干净：**ETH、BTC**（LP 信号，永续齐备、流动性极高）。
- 次级：交易所充值信号涉及的有永续的长尾 token（如 PEPE）；稳定币流仅作间接证据。
- 事件频率：见 3b（单日不足，需多日回溯凑样本）。

### Q4 — 延迟解剖
原系统延迟（实时路径）：
1. 区块轮询间隔 ~1s（+ 出块 ~12s 的天然滞后） →
2. getLogs + 解析 + 分级扫描（LP 主趋势 12s / 次级 18s 周期，**最坏几十秒才轮到某池**） →
3. 多层门控 + 序列确认窗口（`INTENT_CONFIRMATION_WINDOW_SEC=900s`、共振 300s） →
4. Telegram 推送 + **人工读消息→手动下单**（分钟~数分钟，不可控）。
**人工环节是主延迟。** 去掉人工 + 缩短确认窗口、改事件驱动（WebSocket/mempool 替代轮询）后，理论可压到**出块后数秒级**。鉴于信号反应窗口本就 30/60/300s，"自动化能否救活"取决于**信号在出块后前 30–60s 是否仍有剩余 edge** —— 这正是离线研究要回答的第一问。（初判：值得一验，但 edge 可能很薄。）

### Q5 — 给前置研究的原料清单（只给原料，不设计研究）
- **建议先验信号**：`liquidity_removal`（定义最干净、池固定、标的=ETH/BTC）；其次 `exchange_deposit_candidate`（样本最多 233/天，但标的需逐 token 映射、稳定币流需特殊处理）。
- **标的**：ETH、BTC（起步）；扩展到有永续的长尾 token。
- **历史数据源**：归档节点 `getLogs`（4 个 UniV2 池的 Swap/Mint/Burn + 触碰交易所热钱包的 ERC20 Transfer），**复用 `app/lp_analyzer.py` + `app/pipeline.py` 分类器重放**以保证定义零漂移。
- **价格**：OKX perp 1m（与系统 outcome 源一致）。
- **可覆盖时长**：受限于回溯预算，建议先回溯 **数周~数月**（远超现有 1 天）以达到 ≥20 样本/profile。

### Q6 — 已有工作复用判定：⚠️ **需用严格方法重做（基础设施可复用，结论不可信）**
- `SIGNAL_TO_TRADE_POSTERIOR_UPGRADE.md` 实施的是 **Shadow Opportunity Layer + Trade Replay Engine**——即"如何计算后验"的**代码与单元测试（23 个，全过）**，**不是**对真实数据跑出的实证结论。
- **未经严格验证**：所有测试基于合成 fixture；daily report 显示 outcome 30/60/300s 完成数 = 0、candidate=0、verified=0、SQLite outcomes 表 0 行 → **后验从未在真实样本上运行过**。
- **可直接复用**：`trade_replay.py` 的 PnL/MFE/MAE/标签逻辑、`outcome_scheduler.py` 的 30/60/300s 窗口、`lp_analyzer.py`/`pipeline.py` 的信号分类器——作为离线研究的**计算工具**。
- **必须重做**：任何"该信号有预测力"的判断——目前仓库**没有任何数据支撑**该结论，策略档所述 edge 只能视为**操作者实时观察的主观假设**，待离线证伪/证实。

---

**前置研究可启动性**：🟡 **受限**——信号定义完备、计算工具齐备、标的清晰，但**缺历史数据 + 缺已验证 edge**；需先做一次中等成本的归档节点回溯（复用现有分类器）才能产出足量带价格 outcome 的样本。

链上监控勘察完成于 2026-06-14T12:31:55Z，信号数:16 / 历史数据:黄灯(偏红) / 可交易标的:2(核心ETH/BTC)+长尾 / 已有后验分析:需重做 / 前置研究可启动:受限 / 已 push：已确认
