# Bubble Buster 架构评审与演进路线报告 (Architecture Review & Evolution Roadmap)

- **初版日期**：2026-09-04
- **融合更新**：2026-09-05（结合 [Codex 复核意见](file:///Users/zhangshuai/PycharmProjects/bubble_buster/docs/ARCHITECTURE_REVIEW_CODEX.md) 完成全面校准）
- **最新实战校准**：2026-09-25（结合 [2026-09-22 线上审计报告](file:///Users/zhangshuai/PycharmProjects/bubble_buster/docs/audits/2026-09-22/REPORT.md) 与提交 `a276cc6` 落地成果）
- **评审角色**：系统架构师联合评审 (Gemini & Codex)
- **评审范围**：核心交易系统 (`core/`)、交易所适配层 (`infra/`)、看板服务 (`dashboard_*.py`)、数据存储 (`schema.sql` / `core/state_store.py`) 及系统运维调度

---

## 目录

- [执行摘要：实战数据校准与阶段跃迁](#执行摘要实战数据校准与阶段跃迁)
- [一、系统定位与业务全貌](#一系统定位与业务全貌)
- [二、架构设计亮点与实战成果](#二架构设计亮点与实战成果)
- [三、深层架构痛点与真实风险边界](#三深层架构痛点与真实风险边界)
- [四、关键架构模式纠偏（特别附录）](#四关键架构模式纠偏特别附录)
- [五、最新校准的演进路线图 (P1 为主轴)](#五最新校准的演进路线图-p1-为主轴)
- [六、P1.1 改造方案详案：PositionManager 纯函数化](#六p11-改造方案详案positionmanager-纯函数化)
- [七、目标架构全景图](#七目标架构全景图)
- [八、总结与工程准则](#八总结与工程准则)

---

## 执行摘要：实战数据校准与阶段跃迁

在 2026-09-22 的生产环境专项审计及 2026-09-25 的提交 `a276cc6` 中，系统完成了对最致命的底层并发漏洞的修复与线上验证。

本次校准基于真实生产测量数据（150万行快照库、多账户运行实况）确立了三个核心结论：
1. **原 P0 防御风险已实质性闭环**：
   - 订单终态被迟到 REST 倒退、新账户状态被旧 REST 快照覆盖、调度器主循环被现金流 I/O 卡死等竞态已彻底修复；
   - 随之落地的 [`tests/test_audit_probe_regression.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_audit_probe_regression.py) 构筑了严密的防线（全工程通过用例增至 **353 passed**）。
2. **推翻“SQLite 锁竞争瓶颈”假设，避免过度设计**：
   - 线上 404 MiB 数据库实测表明，WAL 模式下锁争用并未造成线上瓶颈（无锁超时、无死锁）；
   - 真正的性能热点是“遍历 150 万历史行提取最新权益”的慢 SQL，该问题已在 `a276cc6` 中通过索引定位与窗口优化降至毫秒级；因此**数据库写队列或迁移 PostgreSQL 无限期延后**。
3. **路线图主轴正式推进到「P1 核心领域解耦」**：
   - 最紧迫的隐患从“底层崩溃与锁并发”转移到**“业务核心代码巨石化与风控难以隔离测试”**；
   - 即刻启动 **P1.1：`PositionManager` 纯风控决策评估与统一退出执行器分离**。

---

## 一、系统定位与业务全貌

### 1.1 业务属性
Bubble Buster 是一套针对**币安 USDT 永续合约 (Binance USDT-M Futures)** 的多账户量化交易系统。
核心策略模型为：
- **定时 Top-N 涨幅做空**：固定时间（如 07:40 / 08:00）筛选全市场日内涨幅最大的币种，等待 1 小时阴线（或先阳后阴结构确认）分批做空；
- **分层风控矩阵**：固定止盈（TP）、动态移动止盈、早盘保护（07:55 按前高止损）、中午保护（12:00 按区间最高价止损）、小时级盈利回撤止盈（如 18% 回撤保护）、日内定时浮亏砍仓（11:55）、组合级止盈止损（-3.5% 止损 / +9% 减仓或清仓）；
- **动态再平衡与资金分配**：依据账户净值、杠杆与资金利用率动态调整新旧仓位权重；
- **多账户隔离机制**：支持独立全功能交易账户（`acc01`~`acc04`）、砍仓专用账户（`loss_cut_only`）、只读监控账户（`readonly`）。

### 1.2 运行时拓扑
系统采用单进程服务（`main.py service`）内置调度循环，替代外部 crontab；同时通过 FastAPI 与 SQLite 本地缓存提供实时监控看板。

```mermaid
flowchart TD
  subgraph Scheduler["内置服务调度器 (StrategyRuntimeService)"]
    Cron["时间窗口触发器 (Entry / Loss-Cut / Protection)"]
    Loop["巡检循环 (Manage / Balance / Reconcile)"]
    AsyncTasks["Single-Flight 异步任务 (现金流同步 / 只读快照)"]
  end

  subgraph TradingCore["交易与风控核心 (Core)"]
    Strategy["Top10ShortStrategy\n(选币/等待阴线/加仓/再平衡)"]
    PM["PositionManager\n(TP/SL巡检/多重保护止损/平仓)"]
    Snapshot["AccountSnapshotProvider\n(网络I/O不持锁 / 增量状态防倒退)"]
  end

  subgraph InfraLayer["基础设施与适配层 (Infra)"]
    RateLimit["RateLimitCoordinator\n(单进程内跨账户共享限流)"]
    Client["BinanceFuturesClient\n(签名/重试/规格归一化/算法单)"]
    UserStream["BinanceUserStreamState\n(微秒级时间戳 + 终态保护)"]
    Notifier["ServerChanNotifier\n(就绪检测与告警)"]
  end

  subgraph Storage["数据持久化 (WAL模式)"]
    DB[(SQLite: state.db\n17张表 Runs/Positions/Events/Fills/Snapshots等)]
    FileLock["runtime.lock\n(fcntl.flock 进程锁)"]
  end

  subgraph Dashboard["监控控制台"]
    Web["dashboard_fastapi.py + dashboard_server.py\n(HTTP 请求只读 SQLite; 索引定向扫描)"]
  end

  Scheduler --> TradingCore
  Scheduler --> AsyncTasks
  TradingCore --> Snapshot
  TradingCore --> Storage
  TradingCore --> InfraLayer
  InfraLayer --> RateLimit
  Dashboard --> Storage
```

---

## 二、架构设计亮点与实战成果

系统在实盘严苛环境下沉淀了大量优质的工程设计，并在最新提交中完成了关键升级：

1. **出色的交易所权重控制与分层缓存**：
   - **进程级限流协调**：在 [`infra/binance_rate_limit.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/infra/binance_rate_limit.py) 中实现了跨账户共享的限流协调器（`RateLimitCoordinator`），为交易请求与后台请求分别划分预算（如 300 vs 200 weight/min），并在收到 `429/418` 时触发统一冷却。
   - **账户快照与无锁 I/O**：通过 [`core/account_snapshot.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/core/account_snapshot.py)，同一账户每分钟仅调用一次 REST 账户接口；最新修复确保了**在发起 HTTP 请求时不占用内部锁**，不再阻塞并发到达的 WebSocket 用户流。
   - **流式感知与严格状态机**：通过 [`infra/binance_user_stream.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/infra/binance_user_stream.py) 依靠 WebSocket 维护订单状态；在 [`core/state_store.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/core/state_store.py) 中建立了终态保护（FILLED/CANCELED 不会被迟到的 REST 覆盖回 NEW），`executed_qty` 保持单调递增。

2. **强幂等锚点与非阻塞调度**：
   - `runs` 表以 `(account_id, trade_day_utc)` 作为复合唯一键，保障日内入场逻辑幂等；资金流水使用持久游标和 `tranId/unique_key` 去重。
   - 调度器通过单飞（Single-flight）异步调度将流水同步等慢 I/O 移出主调度循环，保障了核心入场与保护巡检的准点触发。

3. **扎实的自动化测试防线**：
   - 包含 23 个测试模块，总计 **353 个通过测试（353 passed）**，包含完整的探针回归测试 [`tests/test_audit_probe_regression.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_audit_probe_regression.py)，为接下来的重构提供了高密度的安全气囊。

---

## 三、深层架构痛点与真实风险边界

经过 9-22 线上审计数据过滤，排除了“虚假风险”，当前阻碍系统进一步迭代的核心痛点聚焦在以下三项：

### 1. 核心模块过深，业务逻辑膨胀至临界点
全工程生产 Python 代码中，约 73% 集中在四个巨型文件中：
- [`dashboard_server.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/dashboard_server.py)（**9,485 行**）：内嵌 **5,500+ 行原生 HTML/CSS/JS 字符串**，混合了 SQL、数据聚合与页面拼装。
- [`core/strategy_top10_short.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/core/strategy_top10_short.py)（**5,005 行**）：近期连续增加了“先阳后阴（bullish-then-bearish）”、“分批加仓（scale-in）”、“信号独立模式”等，状态机逻辑极度厚重，选币、状态跟踪与订单执行紧耦合。
- [`core/position_manager.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py)（**4,450 行**）：平铺了 7 大类巡检与保护逻辑（`run_once`, `run_noon_protection_stop`, `run_morning_protection_stop`, `run_hourly_exchange_take_profit`, `run_daily_loss_cut`, `run_portfolio_loss_cut`, `run_portfolio_take_profit`）。规则判断与网络下单、落库混织在一起，无法做纯粹的单元测试。
- [`core/state_store.py`](file:///Users/zhangshuai/PycharmProjects/bubble_buster/core/state_store.py)（**2,401 行**）：77 个方法，职责过宽。

### 2. 反模式：依赖日志正则解析推断运行状态（Log Scraping）
- 看板为了获知入场进度，依然通过 `_task_log_parse_state` 正则扫描本地 `strategy.log` 文件。
- 日志一旦微调文案，前端监控直接失效，属于典型的高耦合反模式。

### 3. 弱类型与贫血数据结构
- 虽然有部分 `@dataclass`，但在模块间流转时仍大量充斥着 `row.get("symbol")`、`order.get("updateTime")` 等弱类型字典，重命名或字段访问失误极难在静态分析期被捕捉。

---

## 四、关键架构模式纠偏（特别附录）

重构推进中，必须坚守以下三个经过推演的设计红线：

### 1. 为什么坚决不用“责任链模式（Chain of Responsibility）”？
* 责任链将对象沿链条传递，每个节点自主决定处理或退出。但在量化风控中：
  - **组合风控不是单仓决策**：组合止损（-3.5%）是跨全仓位的宏观聚合计算；
  - **动作不只是退出**：很多保护动作是“收紧止损限价（Tighten Stop Loss）”；
  - **副作用不可失控**：各规则如果直接调用交易所 API，会造成严重的挂单冲突与竞态。
* **终极解法：函数式核心，指令式外壳（Functional Core, Imperative Shell）**：
  $$\text{只读不可变快照 (Context)} \xrightarrow{\text{纯风控决策 (Pure Evaluation)}} \text{风险意图列表 (ExitIntents)} \xrightarrow{\text{统一执行器 (Exit Executor)}} \text{合并/排序/下单/落库}$$

```mermaid
flowchart LR
  subgraph Input["只读不可变快照"]
    Snap["PositionRiskContext\nPortfolioRiskContext"]
  end

  subgraph PureCore["纯风控决策层 (Pure Evaluation - 无副作用)"]
    R1["TP/SL 规则"]
    R2["早盘/午盘保护规则"]
    R3["组合止盈/止损规则"]
    R4["小时级回撤保护规则"]
  end

  subgraph OutputIntents["纯数据意图"]
    Intents["List[ExitIntent]\n(全平 / 减仓 / 收紧止损)"]
  end

  subgraph CentralExecutor["中央退出执行器 (Central Exit Executor)"]
    Exec["1. 意图冲突合并与优先级排序\n2. 精度规格截断\n3. 统一调用币安 API (含重试/降级)\n4. 原子持久化与对账更新"]
  end

  Input --> R1 & R2 & R3 & R4
  R1 & R2 & R3 & R4 --> Intents
  Intents --> CentralExecutor
```

### 2. 避免无脑拆分 Repository 破坏事务完整性
- 订单事件（`order_events`）、仓位更新（`positions`）与成交流水（`fills`）必须在同一事务中原子更新。
- 仓储治理应以**业务聚合根（Aggregate Root）**与 **Unit of Work** 为边界，绝不搞“一张表对应一个类”的教条拆分。

### 3. 工具库按语义拆分，拒绝垃圾抽屉
- 提取 `core/time_utils.py` 与 `core/math_utils.py`，客户订单号等业务标识符生成保留在交易域。

---

## 五、最新校准的演进路线图 (P1 为主轴)

```mermaid
timeline
    title 调整后的 Bubble Buster 架构演进路线
    已完成 (a276cc6) : 订单终态与增量防倒退 : 调度器解耦慢 I/O : 1.5M 历史全表扫描优化 : 353 项回归测试绿灯
    P1.1 已完成 (dd2c7d8) : PositionManager 纯决策与执行器解耦 : 提取纯风控评估模块 : 建立 CentralExitExecutor : 397 项测试通过
    P1.2 已完成 (50c094f) : Top10ShortStrategy 领域解耦 : 提取 MarketRankScanner : 提取 RebalanceCalculator : 提取 TimingController : 415 项测试全绿
    P2.1 已完成 (e6be3e5) : 新建 task_executions 表替代日志爬取 : 调度任务结构化落库 : 彻底解除 Dashboard 日志 IO 瓶颈 : 430 项测试全绿
    P2.2 已完成 (dfabe33) : 抽离 Dashboard 5500 行 HTML 静态化与模板解耦 : 提取 templates/ 目录 : 建立带 mtime 缓存热重载的 TemplateLoader : 433 项测试全绿
    P3 已完成 (当前) : 仓储层轻量 Unit of Work 规范化与类型系统强化 : 提取 core/storage/ : 引入原子事务边界与双访问模型 : 442 项测试全绿
```

---

## 六、P1.1 改造方案详案：PositionManager 纯函数化

作为接下来首要落地的任务，P1.1 的实施方案如下：

### 目标
在保持 `PositionManager` 外部公开接口（`run_once`, `run_noon_protection_stop` 等）签名和运行时行为 **100% 不变** 的前提下，将其内部深重耦合的代码解耦为**决策**与**执行**两大部分。

### 实施成果与模块交付 (已完成)

1. **不可变领域模型 (`core/risk/models.py`)**：
   - 退出意图类型枚举 `ExitActionType` (`NOOP`, `UPDATE_STOP_LOSS`, `CLOSE_POSITION`, `REDUCE_POSITION`)
   - 不可变意图载体 `ExitIntent`（支持标的、动作、目标价、数量、买卖方向、持仓模式、原因、元数据）
   - 各风控规则的输入与结果数据结构（`NoonProtectionEvaluationInput/Result`、`MorningProtectionEvaluationInput/Result`、`DailyLossCutEvaluationInput/Result`、`HourlyExchangeTakeProfitEvaluationInput/Result`、`PortfolioLossCutEvaluationResult`、`PortfolioTakeProfitEvaluationResult`）

2. **纯计算与决策引擎 (`core/risk/evaluators.py`)**：
   - `calculate_noon_protection_window_start(...)`
   - `calculate_merged_stop_loss(...)`
   - `evaluate_noon_protection_candidate(...)`
   - `is_morning_protection_hold_satisfied(...)`
   - `resolve_morning_protection_old_sl(...)`
   - `evaluate_morning_protection_candidate(...)`
   - `calculate_portfolio_cycle_window(...)`
   - `evaluate_daily_loss_cut_candidate(...)`
   - `evaluate_hourly_take_profit_candidate(...)`
   - `evaluate_portfolio_loss_cut_threshold(...)`
   - `evaluate_portfolio_take_profit_threshold(...)`
   - `calculate_dynamic_stop_price(...)`
   - `evaluate_dynamic_stop_candidate(...)`
   *零网络 I/O、零数据库依赖、零副作用，毫秒级纯数学与规则判定。*

3. **集中退出执行器 (`core/risk/executor.py` -> `CentralExitExecutor`)**：
   - `execute_stop_loss_intent(...)`：负责止损单下发、-2021 立即触发降级市价平仓、DB 事务落库与失败回滚、旧单取消
   - `close_position(...)` / `close_protection_immediate(...)`：统一市价平仓、对账记录、本地仓位标记与保护撤单
   - `cancel_exit_orders(...)` / `cancel_order_if_exists(...)`：旧止损/止盈单原子撤回

4. **`PositionManager` 成为精简的调度 Façade**：
   - `run_noon_protection_stop`：仅负责上下文提取、调用纯判定引擎、委托 `exit_executor`
   - `run_morning_protection_stop`：同上
   - `run_daily_loss_cut`：委托纯判定引擎与 `exit_executor.close_position`
   - `run_hourly_exchange_take_profit`：委托纯判定引擎与 `exit_executor.close_position`
   - `run_portfolio_loss_cut`：委托 `calculate_portfolio_cycle_window` 与 `evaluate_portfolio_loss_cut_threshold`
   - `run_portfolio_take_profit`：委托 `evaluate_portfolio_take_profit_threshold`
   - `_update_dynamic_stop`：委托 `evaluate_dynamic_stop_candidate` 与 `exit_executor.execute_stop_loss_intent`
   - `_close_timeout`：委托 `exit_executor.close_position`

5. **自动化测试与回归保障**：
   - 新增 `tests/test_risk_evaluators.py`（38 个纯函数单元测试）
   - 新增 `tests/test_risk_executor.py`（6 个执行器单测）
   - 全量回归测试：**397 项测试全部通过（0 失败，100% 绿灯）**。

---

## 七、P1.2 改造方案详案：Top10ShortStrategy 领域解耦

### 目标
在保持 `Top10ShortStrategy` 外部公开接口（`run_entry`, `run_rebalance` 等）和所有内部委托方法签名 **100% 不变** 的前提下，将其超长代码（5005 行）中的选币、权重再平衡计算、K线形态确认状态机解耦为纯领域子模块。

### 实施成果与模块交付 (已完成)

1. **不可变实体与领域模型 (`core/strategy/models.py`)**：
   - `RankEntry`：提取自榜单的标的行情模型
   - `PlannedOrder`：入场开仓计算出的初始委托意图
   - `ReadyEntry`：通过形态确认后的就绪标的（含预收盘、正式收盘和批次状态）
   - `EntryStructureWindow`：结构高点止损参考窗口
   - `RebalancePlan`：持仓调仓调整方案计划

2. **选币扫描与过滤模块 (`core/strategy/scanner.py` -> `MarketRankScanner`)**：
   - `build_ranked_entries(top_gainers)`：安全解析交易所 ticker 榜单
   - `filter_and_sort_ranked_entries(ranked, volume_threshold)`：按涨幅降序与成交额阈值过滤
   - `select_entry_candidates(ranked, open_symbols, target_count)`：排除现有持仓并补齐至目标席位数

3. **仓位再平衡计算引擎 (`core/strategy/rebalance.py` -> `RebalanceCalculator`)**：
   - `parse_iso_utc(text)`：标准化时区感知解析
   - `position_age_hours(pos, now_utc)`：仓位持有小时纯函数
   - `age_decay_weight(age_hours, half_life_hours)`：指数衰减半衰期权重计算
   - `build_target_notional_map(...)`：支持 equal_risk 与 age_decay 模式的名义价值分配
   - `build_rebalance_plan(...)`：偏差度量、死区过滤、单次最大调整限制与委托数量规格化

4. **K线形态确认状态机 (`core/strategy/timing.py` -> `TimingController`)**：
   - `is_bearish` / `is_bullish`：纯 K 线阴阳线判定
   - `floor_to_utc_hour` / `closed_hour_boundary` / `resolve_entry_fill_time`：确定性时间算子
   - `classify_due_states(...)`：将待确认标的按预收盘与整点收盘智能分类及调度下一次唤醒
   - `evaluate_preclose_candle(...)`：预收盘阴线就绪判定与阶段扭转
   - `evaluate_single_bearish_final_candle(...)`：单次阴线入场确认
   - `advance_bullish_bearish_final_candle(...)`：先阳后阴分步加仓状态机（INITIAL -> WAIT_BULLISH -> WAIT_BEARISH -> COMPLETE）

5. **`Top10ShortStrategy` 成为精简的协调 Façade**：
   - 保持所有模块层级与测试层级向后兼容（`from core.strategy_top10_short import RankEntry, ReadyEntry, RebalancePlan...` 依然完全有效）
   - 原有方法一键委托至各纯函数模块，策略主体精简 311 行。

6. **自动化测试与回归保障**：
   - 新增 `tests/test_strategy_scanner.py`（3 个单元测试）
   - 新增 `tests/test_strategy_rebalance_calc.py`（8 个单元测试）
   - 新增 `tests/test_strategy_timing.py`（7 个单元测试）
   - 全量回归测试：**415 项测试全部通过（0 失败，100% 绿灯）**。

---

## 八、P2.1 改造方案详案：新建 task_executions 表彻底替代日志状态爬取

### 目标
彻底解决 `dashboard_server.py` 扫描巨量轮转日志文件（`strategy.log.*`）以及通过脆弱的 Python 正则表达式与 `ast.literal_eval` 解析各调度任务（`entry`, `daily_loss_cut`, `noon_protection`, `manage`, `equity_recovery_take_profit`）执行状态的架构性能与可靠性隐患。

### 实施成果与模块交付 (已完成)

1. **统一任务状态归一化库 (`core/task_status.py`)**：
   - 提取纯函数 `format_task_status(task_name, payload, time_local)`，支持所有五类核心任务的数据清洗与归一化；
   - 提取 `task_status_template()`、`status_from_error_count()`、`format_symbol_field()` 等纯工具，使得服务层与前端层共享统一的语义标准。

2. **数据库结构与索引设计 (`schema.sql` / `core/state_store.py`)**：
   - 新建 `task_executions` 核心审计表（记录 `task_name`, `task_cycle`, `status`, `summary`, `time_local`, `payload_json`, `error`, `account_id` 等）；
   - 建立复合覆盖索引：
     - `idx_task_executions_account_task_id (account_id, task_name, id DESC)`：支撑 Dashboard 极速提取最新状态；
     - `idx_task_executions_account_cycle (account_id, task_name, task_cycle)`：支撑按交易日周期幂等审计；
     - `idx_task_executions_created (created_at_utc)`：支撑时间序列历史审计。
   - `StateStore` 增加 `record_task_execution(...)`, `get_latest_task_executions(...)`, `list_task_executions(...)`。

3. **调度服务结构化落库 (`core/runtime_service.py`)**：
   - `StrategyRuntimeService` 增加 `_get_store_for_account` 与 `_record_task_execution`；
   - 调度任务执行后自动记录结构化执行审计：
     - 开仓任务：`_collect_entry_futures` 完成与异常时自动落库；
     - 日常止损：`_run_daily_loss_cut_if_due` 执行结果自动落库；
     - 午盘保护：`_run_noon_protection_if_due` 执行结果自动落库；
     - 仓位管理：`_run_manage_if_due` 周期心跳与组合止盈/权益恢复触发及异常时自动落库。

4. **Dashboard 极速查询与零停机平滑降级 (`dashboard_server.py`)**：
   - `_latest_task_statuses_for_accounts` 改造为优先合并 `task_executions` 数据库表数据；
   - 移除原 `_task_status_from_payload` 内部上百行冗余重复逻辑，委托统一规范 `format_task_status`；
   - 保留日志缓存比对机制作为平滑降级底座，确保历史老日志无缝向下兼容。

5. **自动化测试与回归保障**：
   - 新增 `tests/test_task_status.py`（10 个格式化与归一化单元测试）；
   - 新增 `tests/test_task_executions_store.py`（3 个数据库持久化与高效查询单测）；
   - 增强 `tests/test_dashboard_server.py`（验证 DB 最新状态对日志的优先合并）；
   - 增强 `tests/test_runtime_service.py`（验证调度服务生命周期内自动审计落库）；
   - 全量回归测试：**430 项测试全部通过（0 失败，100% 绿灯）**。

---

## 九、P2.2 改造方案详案：抽离 Dashboard 5500 行 HTML 静态化与模板解耦

### 目标
解决 `dashboard_server.py` 内部长达 5500+ 行的超长内嵌 HTML/CSS/JS 字符串（`DASHBOARD_HTML` 与 `ACCOUNTS_OVERVIEW_HTML`），消除 Python 转义与字符串拼装导致的维护困难，使 `dashboard_server.py` 从 9400+ 行大幅精简至 3900 行，并提供现代模板加载与热重载支持。

### 实施成果与模块交付 (已完成)

1. **独立模板资源目录 (`templates/`)**：
   - 提取 `templates/dashboard.html` (2284 行，86.6KB)：单账户控制台看板独立模板；
   - 提取 `templates/accounts_overview.html` (3233 行，139KB)：多账户总览看板独立模板；
   - 模板支持现代编辑器原生 HTML/CSS/JS 语法高亮、语法检查与格式化。

2. **带 mtime 缓存热重载的模板加载器 (`core/template_loader.py`)**：
   - 实现 `load_template(name, base_dir=None)`：采用基于文件修改时间（`mtime`）的内存缓存机制；
   - **零运行时开销**：生产环境下常驻内存快速渲染；
   - **开发即时生效**：前端修改 HTML/CSS/JS 文件无需重启服务，刷新浏览器即可自动加载最新页面。

3. **`dashboard_server.py` 瘦身与无缝向后兼容**：
   - 移除内嵌的 5522 行 HTML 字符串字面量，`dashboard_server.py` 从 9449 行减至 3942 行（**净减少 58% 代码体积**）；
   - 保留 `get_dashboard_html()`, `get_accounts_overview_html()` 以及模块级属性 `DASHBOARD_HTML`, `ACCOUNTS_OVERVIEW_HTML`，保证已有调用方和测试完全无感知兼容。

4. **自动化测试与回归保障**：
   - 新增 `tests/test_template_loader.py`（3 个单元测试，覆盖模板加载、异常处理与 mtime 热重载）；
   - 全量回归测试：**433 项测试全部通过（0 失败，100% 绿灯）**。

---

## 十、P3 改造方案详案：仓储层轻量 Unit of Work 规范化与类型系统强化

### 目标
解决原有仓储层单表分散提交破坏事务原子性的隐患（如更新仓位状态、记录订单事件与成交流水跨多个独立连接执行）。以**交易聚合根（Trading Aggregate Root）**为边界，建立支持上下文自动传播与嵌套 Savepoint 的轻量 `UnitOfWork`，并引入双访问强类型领域模型（兼具属性访问与字典下标），保证 100% 生产兼容性。

### 实施成果与模块交付 (已完成)

1. **强类型双访问领域模型 (`core/storage/models.py`)**：
   - `DualAccessRecord`：继承自原生 `dict`，同时支持属性读取/赋值（`rec.symbol = ...`）、字典下标（`rec["symbol"]`）、安全检索（`rec.get(...)`）与标准字典转换（`rec.to_dict()`）；
   - `PositionRecord` (兼容别名 `PositionState`)：持仓生命周期聚合模型，内建 `is_open`, `is_active`, `is_closed` 计算属性，支持位置参数无缝兼容旧测试；
   - `RunRecord` (兼容别名 `RunState`)：交易轮次聚合模型，统一 `reason` 与 `message` 字段双向兼容，内建 `is_running`, `is_success`, `is_failed`；
   - `OrderEventRecord` 与 `FillRecord`：订单事件与成交记录模型，内建 `parsed_payload()` 强壮解析；
   - `TaskExecutionRecord` 与 `WalletSnapshotRecord`：调度审计与钱包快照模型。

2. **轻量 Unit of Work 事务管理器 (`core/storage/uow.py`)**：
   - 实现 `UnitOfWork`：基于 Python `ContextVar` 实现环境上下文（Ambient Context）隐式传播；
   - **嵌套事务保护**：自动检测已有活动 UoW，同库嵌套时动态创建与释放 SQLite `SAVEPOINT`，实现细粒度局部失败回滚；
   - **生命周期回调钩子**：支持 `add_after_commit(fn)` 与 `add_after_rollback(fn)`；
   - 提供便捷入口：`store.unit_of_work()` 与 `store.uow()`。

3. **仓储层与执行器无缝协作 (`core/state_store.py` / `core/risk/executor.py`)**：
   - `StateStore._connect_ctx(conn=None)`：优先复用当前上下文或显式传入的 `conn`，未开启 UoW 时自管理连接与事务，**实现 100% 签名与运行时向后兼容**；
   - 核心变动接口（`insert_position`, `update_position_orders`, `update_stop_loss`, `mark_position_closed`, `add_order_event`, `update_order_event` 等）均支持显式及隐式上下文汇入；
   - 新增领域查询接口：`get_order_event`, `list_order_events_for_position`, `get_fill_by_order_event_id`, `list_fills_for_position`；
   - `CentralExitExecutor`（市价退出与止损下发）原子包裹 `unit_of_work()`，订单事件、仓位终态与成交流水同事务落库。

4. **自动化测试与回归保障**：
   - 新增 `tests/test_storage_uow.py`（9 个单元测试，覆盖模型双访问模式、原子事务提交、异常整事务回滚、嵌套 Savepoint 隔离、提交/回滚回调、三表原子落库）；
   - 全量回归测试：**442 项测试全部通过（0 失败，100% 绿灯）**。

---

## 十一、目标架构全景图

```mermaid
flowchart TD
  subgraph SchedulerLayer["运行时调度 (Runtime Service)"]
    Service["StrategyRuntimeService\n(内置时间窗口与心跳调度)"]
  end

  subgraph StrategyDomain["策略领域 (Strategy Core)"]
    FacadeS["Top10ShortStrategy (Façade)"]
    Scanner["RankScanner (选币)"]
    Timing["TimingController (形态确认状态机)"]
    Rebalance["RebalanceCalculator (权重计算)"]
    FacadeS --> Scanner & Timing & Rebalance
  end

  subgraph RiskDomain["风控领域 (Risk Domain - P1.1 核心成果)"]
    FacadePM["PositionManager (Façade)"]
    PureEval["纯风控规则引擎 (Pure Evaluators)\n- evaluate_tpsl\n- evaluate_noon_protection\n- evaluate_portfolio_cut\n- evaluate_hourly_drawdown"]
    Executor["CentralExitExecutor (统一执行器)\n- 意图合并与排序\n- 下单/撤单/降级\n- 事务原子落库"]
    FacadePM --> PureEval
    PureEval -->|List of ExitIntent| Executor
  end

  subgraph StorageLayer["持久化与仓储 (Storage & UoW - P3 核心成果)"]
    UoW["Unit of Work (事务上下文)\n- Ambient ContextVar\n- SQLite Savepoints\n- DualAccess Models"]
    DB[(SQLite: state.db\nPRAGMA journal_mode=WAL)]
    UoW --> DB
  end

  subgraph PresentationLayer["监控与运维 (Presentation - P2 核心成果)"]
    FastAPI["FastAPI 路由 (含 Cycle Readiness 指标)"]
    StaticUI["独立 HTML/JS 前端 (淘汰内嵌字符串)"]
    FastAPI --> UoW
    StaticUI --> FastAPI
  end

  Service --> StrategyDomain
  Service --> RiskDomain
  StrategyDomain --> UoW
  Executor --> UoW
```

---

## 十二、总结与工程准则

通过 9-22 审计报告的客观核验与 `a276cc6` 的成功合入，Bubble Buster 证明了其极高的实战可靠性。

接下来的重构将严格遵循：
1. **接口不变性（Preserve External Interface）**：所有对外界（`main.py` / `runtime_service.py`）暴露的方法签名和返回值结构严格保持兼容；
2. **纯函数先行（Pure Functions First）**：先把业务规则写成纯函数，建立 100% 单测，再把老代码替换为对纯函数的调用；
3. **保持测试绿灯（Keep Green）**：每个小步骤提交前必须确保所有（当前 442 个）测试全部通过。
