# 对 `ARCHITECTURE_REVIEW.md` 的复核意见

复核日期：2026-09-04

## 总结

Gemini 对项目主要架构痛点的判断基本正确，但把现状盘点、风险评估和未来重构方案混在了一起。我的判断是：这份评审约七成可靠，可以作为重构候选清单，但不应按其中的 P0-P3 顺序原样执行。

最重要的风险不是代码文件长或前端嵌在 Python 中，而是交易所副作用、本地持久化、恢复逻辑和并发写入之间的一致性。

## 一、判断成立的部分

### 1. 核心模块确实过深，职责聚集明显

`dashboard_server.py`、`core/strategy_top10_short.py`、`core/position_manager.py` 和 `core/state_store.py` 都很大，分别混合了展示/查询、策略状态机、风险管理与持久化等多类职责。

这不是单纯的“行数问题”。它们的外部接口也比较宽，调用者需要了解较多隐含前置条件、状态和异常行为，因此模块的深度和局部性都不够理想。拆分时应优先寻找真正的 seam（接缝），让复杂行为隐藏在更小的 interface 后面，而不是机械地按文件或类切割。

### 2. `StateStore` 需要收窄，但并不是“没有 Repository 层”

`StateStore` 已经是一个持久化 adapter，只是目前承担了很多领域的读写，接口约有 77 个方法。将来可以按业务不变量逐步提取订单、仓位、钱包、再平衡等模块。

拆分不能只按数据库表拆。订单事件、成交、交易所订单状态和仓位状态之间存在事务关系，应保留事务性的一致更新能力，必要时使用统一的 Unit of Work 或 façade。

参考：[core/state_store.py](../core/state_store.py)

### 3. 策略与仓位管理器值得渐进式拆分

`Top10ShortStrategy` 和 `PositionManager` 中确实同时包含编排、交易所交互、恢复、风险判断、订单执行和状态持久化。把市场排名、入场状态机、恢复逻辑、风险决策和执行器分开，会提升测试性和维护局部性。

不过它们当前也承担了重要的账户级编排职责。更稳妥的做法是先保留原有 façade，在内部提取小而有明确 interface 的模块，避免一次性重写交易流程。

参考：[core/strategy_top10_short.py](../core/strategy_top10_short.py)、[core/position_manager.py](../core/position_manager.py)

### 4. Dashboard 的展示层与状态读取值得分离

Dashboard 中仍有大量内嵌 HTML/CSS/JavaScript，日志解析也和数据查询、页面上下文构造放在同一个大型实现中。将静态资源、模板、查询 adapter 和日志解析器分开是合理的维护性改进。

但这属于中低优先级。当前已经有 FastAPI 路由和部分静态页面，应该在不影响交易运行时的前提下逐步迁移。

参考：[dashboard_server.py](../dashboard_server.py)、[dashboard_fastapi.py](../dashboard_fastapi.py)

### 5. SQLite 写竞争是需要验证的风险

WAL、`synchronous=NORMAL` 和 30 秒 `busy_timeout` 已经配置，WAL 本身不能消除 SQLite 的单写者约束。运行时、用户流、Dashboard 后台刷新等路径都可能访问同一数据库，因此写入延迟和锁等待值得被观测和压测。

目前更准确的表述是“存在潜在并发风险”，还不能仅凭代码断言线上已经出现严重阻塞。应先增加锁等待、任务耗时和超时任务指标，再决定是否需要写入队列或迁移 PostgreSQL。

## 二、报告中需要修正的事实或表述

| Gemini 的说法 | 代码核对结果 | 更准确的表述 |
|---|---|---|
| 四个大文件超过生产代码 75% | 按当前生产 Python 文件总量约为 73% | 集中度很高，但具体百分比取决于统计口径 |
| 数据库有 13 张表 | `schema.sql` 当前有 17 张表 | 表数量不是 13，但数据模型已经相当丰富 |
| 有 25 个测试套件 | 约 22 个测试模块，实际 `343 passed, 1 skipped` | 测试基础比报告描述得更好 |
| 还需要启用 WAL 和 busy timeout | 这些配置已经存在 | 下一步应做并发压测和指标化，而不是重复配置 |
| 缺少结构化实体定义 | 已有多个 dataclass，如 `AccountSnapshot`、`RunState`、`RankEntry`、`ServiceRuntimeConfig` | 类型模型存在，但没有贯穿数据库、交易所 payload 和业务层 |
| 完全没有 Repository 层 | `StateStore` 已承担持久化 adapter 的角色 | 问题是 interface 过宽，而不是从零缺失 |
| Dashboard 100% 只读 SQLite、完全不访问 Binance | 正常页面主要读本地状态，但存在可选的后台交易统计 fetcher | 应区分 HTTP 请求路径和后台刷新路径 |
| 300/200 是 IP 级统一限流 | 目前主要是单进程内按 scope 共享 | 多进程或多实例部署时不能视为真正的全局 IP 协调 |
| 已经实现绝对幂等 | 每日 run 有唯一约束，日志/游标/恢复机制也较完整 | 具备重要幂等锚点，但交易所副作用跨进程崩溃时仍不能保证绝对 exactly-once |

## 三、对重构建议的评价

### 1. `task_executions` 或结构化执行状态：值得做

这是报告里比较有价值的建议。日志适合诊断，不适合作为任务状态的唯一事实来源。新的执行状态应至少考虑：账户、任务类型、业务日期或周期、状态、开始/结束时间、attempt、错误摘要、幂等键和最后一次心跳。

它应与现有 `runs`、`locks` 和恢复逻辑明确分工，不能只是再建一张“任务日志表”。

### 2. “Guardian Chain”：需要改造为策略集合 + 执行器

当前风险处理不只是单仓位规则的线性链：组合级止损和止盈会影响多个仓位，某些策略会收紧止损上限，订单执行还要处理幂等、部分失败、持久化和交易所 reconciliation。

更合适的 seam 是：

1. 纯风险决策模块读取不可变的 `PositionSnapshot` 或 `RiskContext`；
2. 返回多个 `RiskDecision`、`ExitIntent` 或止损约束；
3. 中央执行器负责排序、合并、调用交易所、写入状态和恢复。

这样可以保留 `PositionManager.run_once` 作为 façade，同时逐步让规则模块变成可独立测试的 deep module。还应明确区分 `PositionRiskContext` 和 `PortfolioRiskContext`。

### 3. `StateStore` 拆分：方向正确，但必须围绕不变量

可以逐步提取 `PositionRepository`、`OrderRepository`、`WalletRepository` 等 adapter，但不要为了“一个表一个类”而拆分。首先要列出哪些状态必须在同一事务中更新，并保留账户 scope、迁移和并发语义。

### 4. `core/utils.py`：避免形成新的垃圾抽屉

重复辅助逻辑值得消除，但所有内容都塞进一个通用 `utils.py` 会制造新的浅层模块。时间处理、client-order-id 生成、数值转换应按语义放入聚焦的模块；交易相关 ID 甚至可能属于交易域，而不是通用工具。

### 5. 事件总线、多个数据库、多通道告警：暂缓到有指标之后

用户流和本地 `order_events` 已经提供了一部分事件驱动基础。若当前没有多个独立消费者、吞吐瓶颈或明确的跨模块异步需求，引入进程内 event bus 可能只是增加隐式控制流。

多个 SQLite 数据库也不应以“超过 10 个账户”作为固定阈值，应由写入 QPS、锁等待、任务 p95 延迟和恢复需求决定。多通道告警有价值，但应先设计 severity、去重、限流和 channel adapter，而不是简单增加几个通知 API。

## 四、建议的实际优先级

### P0：先保护交易正确性

- 记录交易所请求与本地状态更新之间的未知窗口，并验证崩溃恢复；
- 明确订单、成交、仓位和交易日 run 的幂等键与状态转移；
- 增加 SQLite 锁等待、任务超时、用户流断连和快照过期指标；
- 增加多账户、多线程、多写者的数据库压力测试。

### P1：在保持行为不变的前提下收窄 interface

- 从 `PositionManager` 提取纯风险决策模块和统一退出执行器；
- 从 `Top10ShortStrategy` 提取市场数据、入场状态机和恢复模块；
- 在数据库 adapter、交易所 adapter 与业务模块之间补齐结构化类型；
- 消除重复 helper，但用语义明确的模块承载它们。

### P2：改善维护体验

- 抽离 Dashboard 模板和静态资源；
- 增加告警去重与多通道 adapter；
- 根据实际指标评估写入队列、PostgreSQL 或事件总线。

## 最终判断

Gemini 对“代码集中、职责过多、需要逐步形成更清晰模块”的诊断是对的；对数字、已存在的基础设施和部分绝对化结论则不够严谨。最稳妥的路线是保留现有运行行为和恢复机制，先建立可观测性，再沿着真实的 seam 做渐进式拆分，而不是进行一次大规模架构重写。

本次复核只新增了这份文档，没有修改生产代码或现有评审文件。
