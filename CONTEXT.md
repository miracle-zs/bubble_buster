# Bubble Buster Strategy Context

This context defines the shared vocabulary for understanding the trading strategy, portfolio exits, and position sizing. Detailed production observations and formulas are recorded in [the strategy behavior note](./docs/strategy-understanding-20260821.md).

## Language

### Portfolio cycle and exits

**账户周期**：一个账户从本地日切换时间开始，到下一次日切换前结束的策略运行周期。
_Avoid_: 交易日（除非明确指数据库中的 `trade_day_utc`）

**组合止盈周期**：账户权益基准、峰值和移动触发线共同所属的一个账户周期。

**组合止盈触发**：组合权益达到启动条件并执行当前持仓的组合级减仓或清仓；触发后本周期状态会锁存。

**组合止盈减仓比例**：组合止盈触发时，针对触发瞬间已有仓位执行的减仓比例。它不等于后续新仓的开仓比例。

**后触发新仓**：组合止盈已经触发后、同一账户周期内继续建立的仓位。后触发新仓不自动继承已经完成的那次组合止盈计划。

### Position sizing

**原始开仓金额**：某个 symbol 的入场订单实际建立的名义金额，应从入场成交数量和成交价计算。

**当前剩余金额**：经过组合止盈、单仓退出或再平衡后，仓位当前仍然持有的名义金额。

**目标单仓金额**：当前再平衡周期为一个仓位分配的目标名义金额；它可能因账户权益和目标持仓数变化而变化。

**目标持仓数**：当前再平衡或入场批次用于分摊目标总名义金额的仓位数量，不必然等于固定的 Top-N 配置。

**再平衡**：根据账户权益、杠杆、利用率和目标持仓数，检查并调整已有仓位或为后续新仓计算目标金额的过程。

**仓位金额口径**：描述金额时必须注明是“原始开仓金额”还是“当前剩余金额”；单独说“普通仓位”容易混淆两者。
_Avoid_: 把 `positions.qty` 直接当成原始开仓数量
