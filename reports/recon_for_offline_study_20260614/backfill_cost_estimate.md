# 链上历史数据回溯成本估算

> **定位声明**
> 本任务为"是否值得花钱回溯历史数据做离线信号研究"的决策提供成本数字。当年系统死于
> 7×24 实时监控的**持续** CU 消耗；本任务估算的是**一次性、有限范围**的历史回溯成本
> （4 个 UniV2 池 + 4 个交易所热钱包地址，数周–数月），两者性质不同。
> **纯估算：未实际拉取任何历史数据、未发起批量 getLogs、未烧任何 CU、未读 `.env`。**
> 唯一网络动作 = 查 Alchemy / OKX 公开计费文档（无需 key）。

生成时间（UTC）：2026-06-14T13:21:50Z ｜ 仓库：`chain-monitor` ｜ 链：以太坊（仅）

---

## TL;DR

**因为回溯只查约 8 个特定地址（4 池 + 4 交易所钱包）的 log，而非全链扫描，
用 2,000 块/次的 `eth_getLogs` 批量回溯，即使回溯 12 个月，总消耗也仅
~0.4M CU（保守上限 ~11.4M CU），完全落在 Alchemy 免费档 30M CU/月之内 → 实际花费 ≈ $0。**

成本根本不是决策约束；真正的约束是**稀有信号（LP `liquidity_removal`）的样本量**，
它要求把回溯时长拉长（建议 12 个月），而拉长时长几乎不增加成本。

---

## 第 1 部分：回溯的调用结构（读代码得出，未执行）

### (1a) 离线回溯需要拉的原始数据

| 维度 | 数据 | 取数方式 |
|---|---|---|
| LP 信号 | 4 个 UniV2 池的 `Swap / Mint / Burn / Sync` 事件 log | `eth_getLogs` 按 `address=[4 池地址]` 过滤（一次调用可含多地址） |
| 交易所充值信号 | 4 个 exchange_hot_wallet 的 ERC20 `Transfer` | `eth_getLogs` 按 `topics=[Transfer_sig, [4 地址], null]`（from 侧）+ `[Transfer_sig, null, [4 地址]]`（to 侧） |
| 价格对齐 | 各事件区块的时间戳 | 见下方"时间戳"——可用稀疏锚点近似（近免费）或逐区块 `eth_getBlockByNumber` |

- **代码依据**：`app/listener.py` 实时路径用 `topics=[ERC20_TRANSFER_EVENT_SIG, topic_chunk, None]`（from 侧）与 `[..., None, topic_chunk]`（to 侧）扫描 Transfer（`listener.py:614-621`），并通过"from/to 是否等于池地址"推断池活动（`listener.py:454`）。回溯沿用同一过滤形状，但把区块范围从"单块"放大为批量。
- **是否还需别的**：UniV2 `Swap` 事件已自带 `amount0In/out, amount1In/out`，配合 `Sync`（储备快照）即可重建 `base_net/quote_net`，**无需**额外储备查询。区块时间戳是唯一附加项。
- **可复用现有分类器**：用 `app/lp_analyzer.py`（action 分类）+ `app/pipeline.py`（exchange intent / size_bucket）**离线重放**历史 log → 信号定义零漂移（与勘察报告 Q5 一致）。

### (1b) getLogs 调用粒度与 CU 单价

- **实时路径粒度（参照，不复用）**：`fromBlock == toBlock`（单块/次，`listener.py:620-621`），`PRODUCER_POLL_INTERVAL_SEC=1.0`，bloom 预过滤跳过无关块（`LISTENER_BLOCK_BLOOM_PREFILTER_ENABLE=True`），方向性扫描只查必要的 from/to 侧（`LISTENER_GET_LOGS_DIRECTIONAL_SCAN_ENABLE=True`），`LISTENER_TOPIC_CHUNK_SIZE=50`（一次可 OR 50 个地址 topic）。这些优化是为**实时省 CU**；回溯不受 1 块/次约束。
- **回溯应采用的粒度**：Alchemy 以太坊 `eth_getLogs` 允许"**2,000 块范围、不限返回条数**"或"任意范围但 ≤10,000 条"。回溯选 **2,000 块/次**（无条数上限、可预测调用数），因过滤后仅 8 地址、单次返回量远低于 150MB 响应上限。
- **Alchemy 公开计费**（官方文档，无需 key）：
  - `eth_getLogs` = **60 CU/次**（部分三方资料称 75，作上限参考）
  - `eth_getBlockByNumber` = **20 CU/次**
  - `eth_blockNumber` = 10 CU/次
  - 免费档 = **30,000,000 CU/月**；PAYG = **$0.45 / 1M CU**（前 300M），其后 $0.40 / 1M CU。

> CU 单价来源：Alchemy Docs — Compute Unit Costs / Pricing Plans / eth_getLogs limits（见文末 Sources）。

---

## 第 2 部分：量级估算（算术）

**区块基数**：以太坊 ~12s/块 → **7,200 块/天**、**216,000 块/月**（30 天）。
**分片**：2,000 块/次 → **108 次/月/查询流**。

**每个 2,000 块分片的 getLogs 调用（base 模型）**：
1. LP 池（address 过滤，含 4 池）= 1 次
2. 交易所 Transfer from 侧 = 1 次
3. 交易所 Transfer to 侧 = 1 次
→ **3 次 getLogs / 分片**

**时间戳**：getLogs 不返回时间戳。
- *base*：稀疏锚点（每小时取 1 块 `eth_getBlockByNumber` 做线性插值）≈ 720 块/月 × 20 CU ≈ **近免费**；分钟级 K 线对齐足够（60s/300s 窗口尤其安全）。
- *保守上限*：对每个含事件的唯一区块逐个 `eth_getBlockByNumber`（按 ~1,500 唯一事件块/天估）。

### (2c) 多档时长成本表

| 回溯时长 | 区块数 | getLogs 调用次数 (base) | 总 CU (base) | 对应美元 | 总 CU (保守上限) | 美元 (上限) | 预期事件样本量（粗推） |
|---|---|---|---|---|---|---|---|
| 1 个月 | 216,000 | 324 | ~33.8K | **$0** | ~0.95M | **$0** | deposit≈7,000；LP 方向≈1,000；**LP 移除≈0–60** |
| 3 个月 | 648,000 | 972 | ~101.5K | **$0** | ~2.86M | **$0** | deposit≈21,000；LP 方向≈3,000；**LP 移除≈0–180** |
| 6 个月 | 1,296,000 | 1,944 | ~203K | **$0** | ~5.71M | **$0** | deposit≈42,000；LP 方向≈6,000；**LP 移除≈数十–数百** |
| 12 个月 | 2,592,000 | 3,888 | ~406K | **$0** | ~11.4M | **$0** | deposit≈84,000；LP 方向≈12,000；**LP 移除≈数十–数百** |

> 全部档位（含保守上限）均 ≤ 免费档 30M CU/月 → **若在单个计费月内完成，实际花费 = $0。**
> 样本量列由勘察报告 3b 的单日**门控前**频率粗推（deposit ~233/天、LP 买+卖压 ~33/天、
> `liquidity_removal` 单日样本里为 0），**门控后 actionable 量会显著更低**（按 ~5–15% 通过率）。
> "每 profile ≥20 样本"对照：deposit 几天即够；LP 方向 1 个月够；**`liquidity_removal`（最干净的 LP 信号）最稀，是样本瓶颈，需 6–12 个月。**

**反模式对照（不要这样做）**：若沿用实时路径"1 块/次"回溯 12 个月 = 466.6M CU ≈ **$190**——
比 2,000 块批量贵 ~1,000 倍。批量分片是省钱关键。

### (2d) 价格数据成本

- **OKX 公开 K 线**（`/api/v5/market/candles`、`history-candles`）：公共行情，**无需 key、免费**；1m 粒度，ETH/BTC 历史完整覆盖。仅有速率限制（每 IP 限频），不计费。
- **长尾 token 补价**：若 token 在 OKX/Binance 有现货或永续，公共 K 线同样免费（Binance `klines` 公共端点也免费）；个别无上所的 token 才需替代源。
- **价格数据成本 ≈ $0**，只是工程量（拉取 + 对齐），与 CU 无关。

---

## 第 3 部分：成本–样本量权衡结论

### Q1 — 单位成本（确定数）
`eth_getLogs` = **60 CU/次**（上限参考 75）；按 PAYG $0.45/1M CU 折算 ≈ **$0.000027/次**。
辅助：`eth_getBlockByNumber` = 20 CU（≈ $0.000009/次）。免费档 30M CU/月先行抵扣。

### Q2 — 多档时长成本表
见 2c。base 模型 1–12 个月 CU 从 ~34K 到 ~406K；保守上限 ≤ 11.4M；**所有档位 $0（落在免费档内）。**

### Q3 — 最小可行档
- 受**最稀信号**约束。若研究核心是 **`exchange_deposit_candidate`**（量大）：**~1 个月**即可得数千样本、足以切训练/测试 → **1 个月 / $0**。
- 若研究核心是 **LP `liquidity_removal`**（最干净但最稀）：单月可能凑不满 20 个干净样本 → 最小可行 **~6 个月 / $0**。

### Q4 — 推荐档
**12 个月 / ≈ $0**。理由：成本对时长几乎不敏感（12 个月仍在免费档内），而更长窗口能
（a）让稀有的 LP 移除信号攒够样本，（b）支持训练/测试双样本 + 跨市场状态（牛/熊/震荡）稳健性检验。
既然钱不是约束，就用数据量最大化的档位。

### Q5 — 成本性质说明（与当年 7×24 对比）
- **本次回溯 = 一次性、有界**：拉到的历史事件可**永久落盘复用**（NDJSON/parquet），重复研究零额外 CU。12 个月全量 ~0.4M CU（占免费档 ~1.4%）。
- **当年实时监控 = 持续、无界**：1s 轮询 + 每块 getLogs + 逐块取时间戳 + outcome 结算，量级约**数千万 CU/月且永不停**（粗估 ~40M+ CU/月，长期超免费档），成本随在线时长线性累积。
- 结论：**回溯的 CU 经济性与实时监控完全不同**——前者是可忽略的一次性支出，后者才是当年的持续负担。回溯不会重蹈"烧 CU"覆辙。

### Q6 — 不确定性（给区间，非单点）
| 误差源 | 影响 | 处理 |
|---|---|---|
| getLogs CU 单价（60 vs 75） | ±25% CU | 绝对值仍 $0，不影响结论 |
| 时间戳取法（稀疏锚点 vs 逐块） | base 34K → 上限 11.4M CU（12mo） | 即使上限仍 ≤ 免费档 |
| 繁忙交易所钱包（如 Binance）单分片 log 量 | 极端时需把 2,000 块再切小 → 调用数 ×2–3 | CU 仍可忽略 |
| 事件密度随行情波动 | 样本量列 ±50% | 已用区间表述 |
| 免费档时序（与其他用量/跨月共享） | 可能少量进入 PAYG | 即便如此 12mo 上限 ≈ $0–$5 |
| 10k-log 上限（若误用宽范围而非 2K） | 繁忙地址需更多调用 | 用 2K 块范围规避 |
| RPC 重试（`RPC_RETRY_MAX_ATTEMPTS=5`）/ 重组 | 失败重发放大调用数 | 小幅，量级不变 |

**综合区间**：12 个月回溯总成本 **$0（base，免费档内）～ 约 $0–$5（最坏：保守上限且与其他用量共享计费月、单价取 75）**。

---

## Sources（公开文档，未用 key）
- [Alchemy Docs — Compute Unit Costs](https://www.alchemy.com/docs/reference/compute-unit-costs)（eth_getLogs=60, eth_getBlockByNumber=20, eth_blockNumber=10）
- [Alchemy Docs — Pricing Plans](https://www.alchemy.com/docs/reference/pricing-plans)（免费档 30M CU/月；PAYG $0.45/M 前 300M，其后 $0.40/M）
- [Alchemy Docs — eth_getLogs limits](https://www.alchemy.com/docs/reference/eth-getlogs)（以太坊：2K 块范围无条数上限，或任意范围 ≤10K 条；响应 ≤150MB）
- [Alchemy Pricing](https://www.alchemy.com/pricing)
- OKX 公开行情 `/api/v5/market/candles` 与 `history-candles`（公共端点免费，无需 key）

---

回溯成本估算完成于 2026-06-14T13:21:50Z，getLogs 单位成本:60 CU/次(≈$0.000027) / 最小可行档:1–6 个月+$0 / 推荐档:12 个月+≈$0 / 一次性非持续 / 已 push：待提交
