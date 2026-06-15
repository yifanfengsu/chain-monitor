# 聪明钱地址名单来源与可靠性勘察 — 前视偏差风险判定

> **定位声明**
> 本勘察为"聪明钱追踪信号能否用现有地址名单做离线研究"提供前提判断。核心问题不是
> "这些地址够不够多"，而是"它们凭什么被认定为聪明钱"——若是基于历史盈利事后挑选
> （前视偏差），则跟随它们的任何回测都会测出虚假 edge（挑中赢家的幸存者效应），与
> 配对研究"全样本挑协整对"、横截面"幸存者偏差"是同一类陷阱。本勘察判定每个地址的
> 认定属：① 身份锚定（身份先验，可用）/ ② 盈利后验（事后挑赢家，有毒）/ ③ 来源
> 不明（需查清）。
>
> **纯勘察**：只读 `data/*.json` + 代码/文档/git 元数据；未修改任何名单或代码；未运行
> 实时监控；未读 `.env`；未调用付费 RPC（**零 Alchemy CU**）。身份核对仅用 Etherscan
> **公开网页**做 **2 次**轻量标签查询（网页访问，非链上 RPC，不烧 CU）。

生成时间（UTC）：2026-06-15T10:31:36Z ｜ 仓库：`~/whale`（chain-monitor）｜ 源文件：`data/addresses.json`

---

## 第 0 部分：勘察方法与三个结构性发现（先看，决定了证据天花板）

**证据来源**：`data/addresses.json`（19 地址）、`data/address_aliases.json`、`data/entities.json`、`.gitignore`、`AGENTS.md`/`README.MD` grep、`app/` 中 smart_money 使用点、Etherscan 公开标签 ×2。

三个发现限定了"能拿到多硬的证据"：

1. **`data/addresses.json` 未入版本库**（`.gitignore`：`data/*` 仅放行 `*.example.json` 与 `backfill_12m/manifest.json`）。
   → **没有 git 历史**，无法从 commit 还原"某地址何时、为何加入"。`git log -- data/addresses.json` = 空。
   **provenance 只能依赖文件内 `note` 字段。**
2. **`source` 字段是自指链接**：每个地址的 `source` = `https://etherscan.io/address/<该地址自己>`。
   它是"查看此地址"的链接，**不是来源出处**，对"凭什么是聪明钱"提供**零信息**。
3. **无任何选址方法论文档**：`AGENTS.md` / `README.MD` grep `lookonchain|spoton|聪明钱|挑选|胜率|pnl|win_rate|盈利` = **0 命中**。schema 里**没有** `pnl_rank/win_rate/profit/return` 字段。
   **唯一的盈利证据是 `note` 里的散文**（`卖高买低精准获利`、`卖高买低`）。

> 含义：这套名单**没有可复现的选址规则**，认定依据散落在人工 `note` 里。这本身就把"研究
> 可信度"压低——但下面会看到，`note` 的措辞已足够把多数地址判为有毒/不明。

---

## 第 1 部分：名单提取

### (1a) 11 个 `category=smart_money` 地址（全字段见 `address_provenance.json`）

| # | 地址(尾) | label | strategy_role | active | prio | `note` 关键词 |
|---|---|---|---|---|---|---|
| 1 | …d621 | Jump Trading | market_maker_wallet | ✅ | 1 | Jump **做市商** |
| 2 | …c080 | Wintermute | market_maker_wallet | ✅ | 1 | **高频做市**与库存调节 |
| 3 | …8862 | Cumberland | market_maker_wallet | ✅ | 1 | 机构级**做市**与大额调仓 |
| 4 | …4877 | ETH **Smart Trader** Whale | alpha_wallet | ✅ | 1 | Lookonchain追踪｜**卖高买低精准获利，Alpha 特征最强** |
| 5 | …6029 | Large ETH Aave Whale | smart_money_wallet | ✅ | 1 | Lookonchain追踪｜Aave 借贷与**卖高买低** |
| 6 | …6d22 | Bitget ETH Whale | smart_money_wallet | ✅ | 2 | Lookonchain监测｜扩大 ETH **大额行为覆盖** |
| 7 | …4d1e | Old Top Whale | smart_money_wallet | ✅ | 2 | Lookonchain监测｜独立**旧鲸鱼**主题 |
| 8 | …318f | ETH Withdrawal Whale | smart_money_wallet | ❌ | 2 | Lookonchain监测｜Binance **提现主题**重复 |
| 9 | …d1c4 | ETH Large Withdrawal Whale | smart_money_wallet | ❌ | 2 | Lookonchain监测｜**高价值**但主题同质化 |
| 10 | …eec8 | 0xe5c **Giant Whale** | smart_money_wallet | ❌ | 1 | SpotOnChain追踪｜0xe5c **簇中主地址** |
| 11 | …a6b0 | **7 Siblings** Whale | smart_money_wallet | ❌ | 2 | SpotOnChain追踪｜保留主地址 |

**结构**：3 个机构做市商 + 8 个 Lookonchain/SpotOnChain "鲸鱼"。7 active / 4 inactive。
所有 `source` 字段均为自指 Etherscan 链接（见第 0 部分发现 2）。

**其余 8 个非 smart_money 地址**（便于理解整个标注体系）：exchange_whale 4（Bitfinex 2 / Binance 14 / Bybit / OKX）、celebrity_whale 3（Justin Sun / Jeffrey Wilcke ×2）、issuer_whale 1（Tether Treasury）。
这 8 个**按角色天然身份锚定**（公开 CEX 热钱包 / USDT 金库 / 知名个人；`address_aliases.json` 中 CEX 均 `entity_attribution_strength=confirmed_entity, confidence=1.0`），但它们是**观察对象**（看 CEX 流入 / 增发 / 名人动向），**不是"可跟随的盈利聪明钱"**，不参与聪明钱 alpha 假设。

### (1b) 名单在 pipeline 中的使用（smart_money 标签触发什么）

`app/filter.py:1327 strategy_role_group()` 把 `strategy_role` 映射为组：`market_maker_wallet`→`market_maker`，`alpha_wallet`/`smart_money_wallet`→`smart_money`。该组：
- 在 `app/notifier.py:447-497` 把这些地址的 **swap/执行**路由为 `smart_money_primary` / `smart_money_observe`（及 `market_maker_*`）Telegram 通知变体；
- 在 `app/followup_tracker.py` 触发 **`smart_money_case`** 后续跟踪与"已确认"判定。

**语义**：系统把名单地址的链上成交**当作天生值得跟随的信号**推给操作者。**这正是前视偏差致命的地方**——若地址因"历史赚钱"被选入，系统就是在让人"跟随一个被事后挑出的赢家"。

---

## 第 2 部分：逐地址认定来源判定（核心）

判据：① 需有**独立于交易结果**的真实世界身份先验；② 有"基于盈利/胜率/精准获利"的选入痕迹；③ 无身份、仅分析服务昵称（来源不明/被结果筛选）。

### ① 身份锚定（3 个，全是做市商）
- **Jump Trading / Wintermute / Cumberland**。依据：`note` 明写"做市商/做市"；均为公开机构做市商；**Etherscan 公开标签实测**（2026-06-15）`0xf584…d621` = **"Jump Trading" / "Fund"**（真实实体标签，身份先验于任何盈利观察）。
- **关键限定（不可省）**：这 3 个是**做市商，不是方向性 alpha**。做市流是库存驱动/提供流动性/常为 taker 的对手方且偏均值回复。**"跟它预测价格方向"是另一个、更弱的假设**，不等于"跟随盈利聪明交易者"。身份干净 ≠ 方向性有预测力。

### ② 盈利后验（2 个，有毒，且都是 active priority-1）
- **ETH Smart Trader Whale `0xce27…4877`**：`note` 明写 **"卖高买低精准获利，Alpha 特征最强，优先启用"** = 因赚钱而选。**Etherscan 实测**该地址公开标签**仅 "Beacon Depositor"**（通用活动标签），**无任何实体身份**——"Smart Trader"是 Lookonchain 因其盈利而起的昵称。**教科书级幸存者/前视偏差**：匿名地址、最高优先级、因历史精准获利被选。
- **Large ETH Aave Whale `0x54d2…6029`**：`note` "Aave 借贷与**卖高买低**，偏策略型大户" = 基于盈利行为模式选入，无真实身份。
- 这 2 个是名单里**最毒**的：恰恰被标为最高优先级/最强 alpha，而它们正是被事后挑出的赢家。任何"跟随"回测会测出**虚假 edge**。

### ③ 来源不明 / 被分析服务结果筛选（6 个）
- **Bitget ETH Whale / Old Top Whale / ETH Withdrawal Whale / ETH Large Withdrawal Whale / 0xe5c Giant Whale / 7 Siblings Whale**。
- 共同点：**无真实世界身份**，只有 Lookonchain/SpotOnChain 起的**昵称**（"Giant Whale"、"7 Siblings"…）；`note` 给的理由是**体量/覆盖/主题**（"扩大大额行为覆盖""旧鲸鱼主题""高价值"），**不是显性盈利**。
- 但："体量/主题"至多是**弱行为锚**，**不是身份锚**；且其**来源渠道**（Lookonchain/SpotOnChain 这类专门曝光"聪明钱/巨鲸异动"的分析媒体）本身就是**按结果/话题度筛选**地址——一个地址出现在它们的信息流里，正是因为它做了"值得报道"（往往是盈利）的事。→ **潜在盈利后验倾向，不能当干净锚定。**

### (2d) 行为时效性
- `note` 字段**无 `added_time`**，标注日期未知（文件 mtime 4/4，但无逐地址时间）。
- inactive 4 个（#8/#9/#10/#11）当前已停用。active 7 个里：#7 "**旧**鲸鱼"明确暗示年代久→漂移风险高；②类 2 个的盈利观察是**无日期的过去**，标注后是否仍"卖高买低"无从校验。
- 即便 ① 的 3 个做市商，做市行为会随做市策略/库存调整漂移；本勘察未独立复核近期链上活跃度（避免烧 CU）。

---

## 第 3 部分：名单整体质量

### (3a) 类别分布
| 类 | 数量 | 地址 |
|---|---|---|
| ① 身份锚定 | **3** | Jump / Wintermute / Cumberland（**全是做市商**）|
| ② 盈利后验 | **2** | ETH Smart Trader Whale / Large ETH Aave Whale（**active, prio1**）|
| ③ 来源不明 | **6** | Bitget / Old Top / 两个 Withdrawal / 0xe5c Giant / 7 Siblings |

**②+③ = 8/11 = 73%。"可跟随的方向性聪明钱"中身份锚定干净者 = 0**（3 个干净的全是做市商，非方向 alpha）。

### (3b) 前视偏差总体风险：🔴 **红灯**
- 按本任务判据"多数盈利后验/来源不明 → 有毒"：8/11 多数 ②/③ → **红灯**。
- 更致命的是**质量与毒性正相关**：被标为"Alpha 特征最强/优先启用/priority-1"的恰是 ②（因赚钱被选）。**名单里"看起来最像 alpha"的地址，正是前视偏差最重的地址**——跟随回测会系统性高估 edge。
- 唯一干净子集（3 做市商）回答的是**另一个假设**（做市流跟随），且样本仅 3。

### (3c) 样本充裕度（次要，前视清白后才有意义——此处已不清白）
做市商（Jump/Wintermute/Cumberland）链上交易频次极高（万级 tx），单地址事件研究样本充足；Lookonchain 鲸鱼频次不一。**但前视偏差红灯下，样本多寡无意义**：样本越多只会把虚假 edge 估得越"显著"。

### (3d) 是否有这些地址的历史交易可用于离线研究
**否（仓库内基本没有）**：
- `data/chain_monitor.sqlite`（508KB）19 表**仅 schema_meta 1 行**，其余全空（沿用上次勘察结论）。
- 本地 NDJSON archive 仅 **~1 天**、多为门控丢弃事件，smart-money 执行样本≈0（`smart_money_non_execution_weak` drop 仅 1、`market_making_inventory_move` 仅 2）。
- `data/backfill_12m/` 是**只针对 4 个 UniV2 池的 Burn 事件**回填（15253 条），**完全不覆盖这 11 个聪明钱地址的成交**。
→ 这些地址的历史交易需**重新回溯**（逐地址 ERC20 Transfer + DEX swap 日志）。

---

## 第 4 部分：勘察结论

### Q1 — 名单清单 + 逐地址认定（依据见 `address_provenance.json`）
11 个 smart_money：**①×3（Jump/Wintermute/Cumberland，做市商，身份实测/公开）** ｜ **②×2（ETH Smart Trader Whale、Large ETH Aave Whale，note 明写"卖高买低精准获利/卖高买低"，前者 Etherscan 实测无身份仅"Beacon Depositor"）** ｜ **③×6（Lookonchain/SpotOnChain 鲸鱼，无身份、体量/主题选入、来源渠道结果筛选）**。

### Q2 — 前视偏差判定（核心）：🔴 **红灯**
**现有名单不能用于无偏的方向性聪明钱离线研究。** 8/11（73%）为盈利后验/来源不明；且"最强 alpha/最高优先级"的地址恰是因盈利被事后挑中（②）。跟随它们的回测会测出**幸存者效应导致的虚假 edge**。唯一干净的 3 个是**做市商**——身份锚定但属**另一假设**（做市流跟随，且方向性预测力存疑），且样本仅 3，**不构成可用的"黄灯干净子集"**来回答原始假设。

### Q3 — 数量与样本
- 前视**清白**的方向性聪明钱地址：**0 个**。
- 身份锚定地址：3 个，但均为做市商（非方向 alpha），且对"聪明钱 alpha"假设而言**有效可用 = 0**。
- 样本充裕度：做市商样本充足、但因假设不匹配而无意义；其余地址前视红灯，样本越多越有害。

### Q4 — 历史数据可得性
仓库内**无**这些地址的历史成交（SQLite 空、archive ~1 天且 smart-money 执行≈0、backfill_12m 仅 LP Burn 不含这些地址）。需**重新回溯**：
- 路1/黄灯子集若硬做 → 逐地址 Transfer+swap 日志，工程量**中**（地址数少，可复用上次 LP 回填用过的 public no-key RPC `eth.drpc.org`，零 Alchemy CU 路径已验证可行）。
- 路2（滚动定义）→ 需**全市场地址宇宙 + 逐时点业绩排名**，工程量**大**（远超 LP 回填）。

### Q5 — 路线判定：**暂缓**（严谨续路唯有路2，需大工程）
- **路1（现有名单身份锚定）：排除。** 对"方向性聪明钱 alpha"假设，现有名单红灯；干净子集只剩 3 个做市商，属另一假设且样本不足，不能直接开题。
- **路2（滚动/时点定义聪明钱）：唯一严谨路径，但是大工程。** 需用全市场地址历史、按**每个决策时点的截至当时**业绩（无前瞻泄漏）滚动定义"聪明钱"，再做样本外跟随检验。能根除幸存者偏差，但需全市场地址数据 + 时点对齐的业绩计算，远超本系统现有数据与上次 LP 回填规模。
- **建议默认：暂缓。** 叠加上次勘察结论（整套离线研究本就缺历史数据、缺已验证 edge、需中等回溯），在未决定投入路2大工程前，聪明钱方向**不具备无偏开题条件**。若操作者愿投路2工程，则按路2推进；否则暂缓优先于"用脏名单硬上"。

### Q6 — 观察节
1. **"source"字段是伪来源**：自指 Etherscan 链接给人"有出处"的错觉，实则零信息。审查任何名单先看 source 是否指向**选入依据**而非地址自身。
2. **质量标签与毒性正相关是最危险的信号**：本名单把"Alpha 特征最强/优先启用"给了因赚钱被选的匿名地址——越是"看起来最该跟"的，越毒。这与配对研究"协整最强的对"、横截面"历史收益最高的票"被事后挑中是同构陷阱。
3. **分析服务（Lookonchain/SpotOnChain）= 隐性前视来源**：即便 `note` 写的是"体量/覆盖"，只要地址来自"专曝聪明钱异动"的媒体，就已被**结果/话题度**预筛过，不能当干净锚。
4. **身份干净 ≠ 可跟随**：做市商身份最实锤，却最不该当方向 alpha 跟。"前视清白"和"信号有预测力"是两个独立闸门，名单同时卡在两端。
5. **无 git 历史 + 无方法论文档**：名单不可审计、不可复现。若要走任何路线，**第一步应是把选址规则文档化并入版本库**，否则下一次"它凭什么是聪明钱"还得重勘。

---

聪明钱地址勘察完成于 2026-06-15T10:31:36Z，地址数:11 / 来源分布:[身份锚定3/盈利后验2/不明6] / 前视偏差:红灯 / 可用现有名单:否(干净子集仅3做市商,属另一假设且样本不足) / 路线:暂缓(严谨续路唯有路2大工程) / 已 push:[已确认]
