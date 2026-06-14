# chain-monitor（~/whale）逻辑与工程审查报告

> **定位声明**
> 本审查对象是"系统作为软件工程实现的正确性与设计质量"，不是"信号有无 edge"（后者是
> 策略研究；liquidity_removal 已单独证否，见 `reports/lp_removal_offline_study_20260614/`）。
> 一个信号可以无 edge 但代码正确，也可以有潜力但被实现 bug 毁掉——本审查只回答前者。
> "信号设计缺陷"（如把噪声当信号）属设计层，与"该信号经实证无 edge"是不同维度，分别记录。

> **审查纪律**
> 如实报告，不辩护不夸大。每条发现标注：① 严重度（致命/重要/次要/建议）；② 证据（文件:行号）；
> ③ 影响；④ 确定性（确认 bug / 确认设计缺陷 / 需运行确认的疑点）。代码未明确处标"需运行确认"。

> **范围与方法**：纯静态审查为主——只读代码/文档/配置，**未运行实时监控、未读 .env、未烧 CU**。
> 唯一动态动作：用已落盘历史数据（app/data/archive）在**临时 SQLite 副本**上做最小重放，验证写路径。
> 不重新验证信号 edge（策略层，已单独研究）。不触碰 cta_strategy。

生成于 2026-06-14T15:15:59Z ｜ 审查文件：app/ 56 个非测试模块 + config/Makefile/AGENTS.md

详见：`findings.json`（结构化发现）、`dataflow.md`（数据流全图+模块清单+配置覆盖面）。

---

## 第 1 部分：系统全貌（先证明读懂）

**数据流**（详图见 dataflow.md）：
`RPC → producer(轮询+bloom+定向getLogs) → tx_queue → 5×worker → pipeline(解析→intent分类→富化→
signal_quality_gate→strategy/state/opportunity) → {archive_store(NDJSON+SQLite双写) | notifier(Telegram) |
outcome_scheduler(30/60/300s结算)} → reports(DB-first)`。

**核心闭环（设计意图）**：signal → outcome（应验否）→ quality/calibration → 回调阈值。
**实测**：该闭环断开（见 F1）。

**模块/配置**：56 个非测试模块；config.py ~355 个可被 .env 覆盖的项（本审查用默认值，未读 .env）。
关键默认：`PRODUCER_POLL_INTERVAL_SEC=1.0`、`LOW_CU_MODE=True`、`MARKET_CONTEXT_ADAPTER_MODE=unavailable`、
`OUTCOME_WINDOWS=(30,60,300)`、`SQLITE_ENABLE=True`、`REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH=False`。

---

## 第 2–4 部分：发现（按严重度，证据见 findings.json）

### 🔴 致命（1）
**F1 — 系统无法验证自身预测准确率**（确认设计缺陷）
`app/pipeline.py:450`、`config.py:884`、`outcome_scheduler.py:202-204,280-285`
outcome 链路有两处结构断点：① **只对 LP 记录注册 outcome**（`register_from_lp_record` 是 pipeline 中
唯一注册点）→ 交易所流/清算/聪明钱等信号永不被跟踪；② outcome 起止价取自 market_context，而其
**默认 'unavailable'** → 默认 `make run` 下所有 outcome 立即 `market_price_unavailable`。
**结果**：outcome 表 0 行、daily report `outcome_*_completed_count` 全 0，系统从不记录自己说对没说对，
calibration/shadow/replay 全部缺真实样本。**这是一个"监控+验证"系统失去验证维度的根本缺陷。**

### 🟠 重要（3）
- **I1 — 单个持续失败的区块让 producer 永久卡死**（确认 bug）`listener.py:1782/1794/1810-1811/2054-2056`
  失败即 break 重试同块、无上限。本环境实测公共 RPC 408 频发；生产 RPC 抖动时监控可静默停摆、越落越远。
- **I2 — outcome 只覆盖 LP 信号，主力信号族（交易所流，样本日 233+ 条）无任何 ground truth**（确认设计缺陷）`pipeline.py:450`
- **I3 — SQLite/归档写异常被静默吞掉，NDJSON 与 SQLite 可无声分叉**（确认 bug）`archive_store.py:159-161,183-185`
  replay 证实写路径本身正确（真实 payload 成功写入临时库）→ 本地库全空非写 bug，而是该库从未被某次运行
  填充（库被重 init/运行期未用此库；`data/*.sqlite` 本就 gitignore 且易失）。但静默吞错使真分叉无人知晓；
  叠加 `REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH=False`（config.py:972）→ DB-first 报告可能基于残缺 DB 下结论。

### 🟡 次要（3）
- **M1 — 无 reorg 处理**（确认设计缺陷）`listener.py:1745,2054-2056`：重组期少量事件可能漏处理/基于孤块。
- **M2 — pool_buy/sell_pressure 在 analyzer.py 与 lp_analyzer.py 用不同判据重复判定，可能互相矛盾**（确认设计缺陷）`analyzer.py:126-139` vs `lp_analyzer.py:226-241`。
- **M3 — 5 并发 worker 共享无锁 state_manager，跨 await 读改写可能竞态**（需运行确认）`main.py:391-394`，state_manager 无任何 Lock。

### 🟢 建议（3，多为设计层）
- **S1 — 信号不区分"信息性参与者"与"机械噪声"**（确认设计缺陷）`lp_analyzer.py:250-257`、`pipeline.py:8876-8907`
  liquidity_removal=任意 Burn（研究证实 ~99% 是 dust+自动化再平衡）；exchange_deposit=任意大额入所（不分散户例行 vs 知情出货）。门控只有金额下限，无机器人/再平衡识别。**输入即噪声，是源头问题。**
- **S2 — 约 355 个阈值为无依据魔法数字**（确认设计缺陷）`config.py`（355 处 `_get_*_env`；校准相关注释仅 16 行且指 calibration 配置项本身）。
- **S3 — 原始/解析事件归档按 tier 门控，非 research 运行无法离线重放**（需运行确认）`config.py:120,930-931`；实际 archive raw/parsed 为空。

### ✅ 已核实"非问题"（避免夸大）
- whale **内部时区一致**：archive_ts 用 UTC unix，BJ 仅展示用，outcome 用 UTC+OKX(UTC)。**whale 自身无 UTC+8 坑**（该坑只在外部 cta_strategy 库，本审查不涉及）。
- **SQLite 写路径正确**（replay 验证）。
- **区块游标在瞬时失败下不丢块**（processed_until 仅随成功块推进；但持续失败会卡死=I1）。
- liquidity_removal / exchange_deposit 的**触发语义与代码一致、自洽**。

---

## 第 5 部分：审查结论（Q1–Q6）

**Q1 发现清单**：致命 1（F1）/ 重要 3（I1,I2,I3）/ 次要 3（M1,M2,M3）/ 建议 3（S1,S2,S3）。逐条文件:行号+确定性见 `findings.json`。

**Q2 致命与重要的修复方向**（只给方向）：
- F1/I2：把 outcome 注册从 LP-only 扩到**全部信号**（在 `finalize_notification_delivery` 统一注册）；
  outcome 价格源**独立于研判用 market_context**，默认即用只读价（OKX 公共 1m 或离线对齐），unavailable 时显式告警而非静默产 0。
- I1：单块最大重试+退避，超限记 gap 并跳过/切备用 RPC；持久化 last_processed_block 支持续传与缺口审计。
- I3：mirror 失败计数并暴露到健康指标；周期性 DB/NDJSON 行数对账；生产将 mismatch 设为失败或降级 data_quality。

**Q3 根本性设计评价**：
- **最大优点**：分层清晰、归档/镜像/报告/门控/机会层职责分明，且**保留了大量可观测元数据**（delivery_audit
  单条 ~90 字段），加上已内建的 outcome 30/60/300s 窗口与 replay 引擎——**为"严格离线验证"打好了骨架**。
- **最大缺陷**：**它名为"监控+验证"系统，却结构性地无法验证自己**（F1）——验证只接 LP、价格默认不可用、
  outcome 恒空。再加上信号输入未滤噪（S1），等于"用拍脑袋阈值（S2）发未滤噪的信号，且从不复盘对错"。

**Q4 与策略层的边界（值不值得修）**：
- **工程能修、且修了有意义**：F1/I2（可验证性）、I1（liveness）、I3（数据一致性）、M1/M3（健壮性）。
  这些修好后，系统能**度量任意信号的真实表现**——这本身有价值，**与单个信号是否有 edge 无关**。
- **修了也救不回特定信号的 edge**：liquidity_removal 已实证无 edge（死因 a），即便把 F1 修到完美、
  outcome 全程记录，它也只会**更快地确认它没用**。S1（滤噪）属设计层：能提升"信号候选质量"，但不保证
  任何具体信号有 edge——它决定的是"输入是不是噪声"，不决定"过滤后的信号有没有钱"。
- 结论：**先修可验证性（F1/I1/I3），再用它去筛信号**；不要在已证否的 liquidity_removal 上投入工程。

**Q5 改造成"严格验证任意链上信号 edge 的研究工具"所需核心改动**（很可能是它的最高价值用途）：
1. **outcome 注册全信号化 + 价格源独立可用**（修 F1/I2）——任意信号都能拿到 t0/+Δ 价格窗口与应验标签；
2. **可靠回溯/重放层**：本研究已示范（公共 RPC 批量 getLogs + 复用分类器 + 只读价库对齐），把它沉淀为
   一等公民（保留 raw/parsed，修 S3），让"定义信号→回溯→对齐→统计"成为标准流程；
3. **预注册式评估框架**：把本研究的 P1–P5（基线对照、衰减曲线、抢跑诊断、规模分层、时间切分、成本账）
   做成可复用模块，对任意新信号一键产出"立项/死因"判定；
4. **健壮采集**（修 I1/M1）+ **数据一致性对账**（修 I3）作为底座。
→ 即：**从"实时通知器"转型为"链上信号离线证伪流水线"**。它当前最缺的恰是验证维度，而骨架（多维元数据
+ outcome 窗口 + replay 引擎）已具备，转型成本相对可控，价值远高于继续做一个无法自证的通知器。

**Q6 观察节**
- 系统把大量精力花在"发信号前的多层门控/抑制/预警分级"（config 355 阈值、gate 2562 行），却在
  "发完之后它对不对"上结构性留白——**精雕输入、放空输出验证**，是本系统最反直觉的失衡。
- F1 与策略研究互为印证：正因为系统从不记录 outcome，操作者当年只能凭 Telegram 主观印象判断信号"有效"，
  这正是 liquidity_removal 被主观高估的温床。**修可验证性 = 给未来所有信号判断装上客观标尺。**
- 方法论限制：本审查未读 .env、未实跑，I1 的"卡死"在生产 RPC 行为下成立但严重程度依赖实际 RPC 稳定性；
  M3 竞态、S3 tier 行为标注为"需运行确认"。

---

链上监控系统审查完成于 2026-06-14T15:15:59Z，发现:[致命1/重要3/次要3/建议3] / 可验证性缺陷:[确认——outcome 仅接 LP 信号 + 价格默认 unavailable → outcome 表恒空，系统无法自证准确率(F1)] / 最高价值用途:[改造为"链上信号离线证伪流水线"(Q5)] / 已 push:已确认
