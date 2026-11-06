# Code Cleanup Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement and verify each batch. The user's instructions override commit, handoff, and additional-test defaults.

**Goal:** 删除用途已核实的冗余代码，让现有运行方式保持直接、可理解。

**Architecture:** 保持 Python 单体、SQLite 和现有组件组装方式。只删除无调用私有方法、未接入的内部接口及无效计算，不补完交易核心迁移，不重组目录。

**Tech Stack:** Python 3.13、pytest、FastAPI、SQLite；在仓库外临时虚拟环境安装现有 requirements 和检查工具。

**Spec:** 本次用户请求：先调查实际用途，分批清理验证，保留有效行为和已有改动；不提交、推送或部署。

## Global Constraints

- 初始工作区干净；不操作实际账户、运行中服务或本地 state.db。
- 不根据命名、年龄或静态检查结果单独认定废弃。
- 保留动态注册的 FastAPI 路由、脚本入口、外部接口和持久化恢复兼容。
- 简单删除复用现有测试；只在关键行为缺少覆盖时补测试。
- 不为推测中的竞态或安全边界增加复杂度。

## 调查范围与运行链路

- `main.py` 的 entry/manage/loss-cut/service 经 `create_components()` 和每账户 `AccountCoordinator.step()` 执行；coordinator 仍调用现有 strategy/manager。
- `main.py dashboard` 和 `uvicorn.example` 都启动 `app.main:app`，实际应用来自 `dashboard_fastapi.py`，读取 `dashboard_server.py` 的数据提供器及模板渲染函数。
- `cron.example` 保留单次 loss-cut；`infra/binance_top10_monitor.py` 有独立 `__main__` 和 `[Settings]` 配置。
- 检查源码、配置样例、README/CLAUDE.md、脚本 import/CLI、测试、模块导出及非固定名称的 getattr/hasattr；研究和回填脚本不能按主服务引用情况删除。
- 仓库无打包声明或插件加载器。FastAPI decorator、`__all__` 和框架回调不作为未调用代码。

## Review Focus

- 历史 `reduction_plan` 仍会从数据库状态恢复执行，只删已无调用的计划生成器。
- 新执行核心没有完整接管策略/manager，不删除现有业务路径或 kernel 独立评估模式。
- `core.state_store.RunState` 仍被测试导入，保留显式导出。
- 仪表盘保留 readonly 统计抓取客户端及后台服务；只删从未使用的全局客户端对象。
- 监控脚本兼容配置及历史迁移脚本仍保留；用户已确认旧独立 HTTP 服务和客户端注入没有仓库外调用者。

## Task 1: 删除不可达私有辅助代码

**Files:** `core/position_manager.py`、`core/strategy_top10_short.py`、`dashboard_server.py`、`core/runtime_components.py`、`core/ports.py`。

**Interfaces:** 不改在用业务方法、CLI、HTTP 路由或配置契约。

- [x] 删除没有直接、字符串或动态调用的旧私有方法；任务格式化在 `core/task_status.py`、再平衡在 `RebalanceCalculator`、退出和成交处理在 `CentralExitExecutor` / `MarketFillReconciler` 中继续执行。
- [x] 删除无导入、无注册、无配置引用的内部 ports 文件及路径解析辅助函数。
- [x] 运行 manager、组合止盈、再平衡、任务状态、组件组装和成交补录的现有相关测试。

## Task 2: 清理无效计算和未使用导入

**Files:** `app/`、`core/`、`infra/`、`web/`、主入口、仪表盘、`scripts/`、`tests/` 中静态检查已定位的位置。

**Interfaces:** 保留会产生业务副作用的调用，仅移除结果未使用的绑定；不引入新依赖声明。

- [x] 去掉 coordinator 未消费的 active-intent 数据库读取、重复的早盘持仓时长计算及再平衡废弃订单参数构造。
- [x] 删除仪表盘未使用的全局 Binance 客户端构造和配置读取。
- [x] 按导出与调用证据清理未使用导入/局部变量，保留在用 re-export。
- [x] 运行相关测试及 Ruff F401/F811/F841、F821/F822/F823 检查。

## Task 3: 同步过时断言和直接相关文档，完成验证

**Files:** `tests/test_dashboard_fastapi.py`、`tests/test_dashboard_server.py`、`README.md`、本文件。

**Interfaces:** 保留当前页面行为，断言精确匹配已有界面文案和说明属性。

- [x] 对照提交历史确认页面断言失败原因；只替换已被明确界面变更淘汰的期望，不删除测试或放宽有效断言。
- [x] 修正 README 中不存在的配置模板路径；不改写历史设计文档。
- [x] 全量 pytest、入口 `--help`、源码编译、依赖检查和 diff 检查；记录失败及环境边界。
- [x] 独立审阅本次未提交 diff，补全清理证据、保留项及验证结果。

## 基线

- 首次全量测试：两个仪表盘断言失败（旧曲线文案、全页禁止 `title` 属性）。
- 再次完整记录：572 passed、3 failed、13 subtests passed；额外出现 `test_native_entry_completed_is_terminal_and_not_resubmitted` 的后台调度时序失败。均发生在源码修改前，不能通过删除测试或新增兜底消除。
- Ruff F401/F811/F841：76 项，包含实际 re-export，逐项核对后处理。

## 执行证据和最终验证

### 逐项清理依据与影响

以下删除位置使用清理前 HEAD 中的方法名定位；替代实现和现有入口继续保留。

| 位置 | 用途与调用证据 | 处理及影响 |
| --- | --- | --- |
| `core/position_manager.py` 的 `_build_portfolio_take_profit_reduction_plan` | 已没有调用点；新触发使用 `_build_portfolio_take_profit_limit_plan`，历史状态直接交给旧执行器 | 只删旧计划生成器；保留 `_execute_portfolio_take_profit_reduction_plan` 及状态分流 |
| 同文件 `_close_protection_immediate`、`_is_immediate_trigger_error`、`_build_close_fill_payload_from_user_trades` | 无方法或字符串调用；当前退出由 `CentralExitExecutor` 执行，成交由 `_record_close_order_event` 交给 `MarketFillReconciler` | 删除空转发和旧 BUY 成交拼装；不修改在用退出、补录逻辑 |
| `core/strategy_top10_short.py` 的 `_age_decay_weight`、`_position_age_hours`、`_is_cooling_off_error` | 均无调用；再平衡直接在 `RebalanceCalculator` 计算年龄和权重，冷却重试由 executor 判断 | 删除私有转发、旧判断及其专用常量；保留实际冷却参数和重试行为 |
| 同文件 `non_blocking_entry_wait` | 参数仅在构造函数声明并写入属性；源码、脚本、测试均无传入或读取，不控制任何入场路径 | 删除未接入的构造选项，不补完尚未实施的迁移 |
| `dashboard_server.py` 的 `_account_position_index`、`_status_from_error_count`、`_format_symbol_field`、`_append_summary_part`、`_read_log_lines` | 无调用；当前持仓索引从本地快照构建，任务格式化通过 `format_task_status`，日志使用增量读取 | 删除五个旧私有实现；保留日志兼容解析与当前缓存 |
| `core/runtime_components.py:resolve_runtime_paths`、`core/ports.py` | 无导入、导出、注册、配置或动态引用；入口直接使用 `resolve_path`，未使用这些 Protocol 装配组件 | 删除未使用的路径封装和未接入的接口文件；`infra/protocols.py` 的在用类型保持 |
| coordinator 的 `active_intents` 读取 | `list_active_intents()` 只读数据库并生成列表，结果不参与 `AccountView` | 删除一次无消费的读取，不改交易仲裁 |
| 早盘 `min_hold_seconds`、再平衡 `order_params`、其他未消费变量 | 时长在 evaluator 重新计算；订单参数未传给 executor，数量在实际提交处格式化；未使用绑定不影响调用副作用 | 删除重复计算和废弃参数；下单、事务、插入操作仍保留 |
| `dashboard_fastapi.py:create_dashboard_context` | 全局 `client` 创建后没有存入上下文或 provider；`run_with_dashboard` 局部值无读取，实际开关仍在后台服务启动中使用 | 删除废弃余额轮询准备；readonly 的 `TradeStatsFetcher` 客户端保持 |
| `infra/binance_user_stream.py:_last_event_monotonic` | 只有初始化及事件写入，没有读者或状态持久化字段 | 删除两次无消费的赋值，不改流状态判定 |
| `core/runtime_service.py` 入场前写入的 `portfolio_loss_cut_reset_hour/minute` | `4648e2f` 将入场解锁时间改为次日 07:30，同时移除了策略对这两个属性的读取；仓库内没有其他读者。组合止损任务仍直接读取自身配置 | 删除两个失效属性写入及其专用配置查询；保留止损周期配置和入场解锁规则 |
| `core/runtime_components.py` 的代理配置 | 账户组件和仪表盘各自解析相同的 `http_proxy` / `https_proxy`，逻辑完全一致；账户配置仍需支持合并覆盖 | 共用既有解析函数，保留两个入口和代理不完整时的原有行为 |
| `dashboard_server.py` 旧独立 HTTP 服务 | 当前入口和部署样例均使用 FastAPI；用户确认仓库外没有旧服务调用者 | 删除旧配置类、HTTP 处理器和只服务于旧处理器的解析辅助函数及其测试；保留 FastAPI 共享的数据与曲线处理 |
| `DashboardDataProvider.live_position_clients` 参数和 setter | 用户确认仓库外没有调用者；当前持仓及订单数据从 SQLite 读取，注入对象没有读者 | 删除无效参数和 setter；测试继续验证本地状态，并通过 socket 监测确认没有网络连接 |
| 未使用导入与测试局部绑定 | Ruff 定位后核对跨模块导入、字符串引用和 `__all__`；唯一仍有外部导入的候选是 `core.state_store.RunState` | 清理未使用项，RunState 改为显式 re-export；不删除有效测试或断言 |
| `core/execution/models.py` 的 `List` | 扩展静态检查发现已有两处类型标注未导入；`get_type_hints` 需要解析这些名称 | 补齐已有标注所需导入，不改模型字段 |
| 页面测试 | `76f6a38` 已把曲线标签改为“本周期账户权益曲线”；`5cfc674` 更新对比页标题和名义价值说明；`4f47a52` 增加权益口径 title | 更新四处精确期望，包含完整说明属性；不改页面或放宽数据/交易断言 |
| README 配置示例和目录 | `config.ini.example`、预置 `config_b.ini` 均不存在；当前实际模板为 `config.production.multi.ini.example`，逐账户配置会覆盖全局凭证 | 改为真实文件、复制说明、启用列表、模式及逐账户凭证填写步骤，运行命令保持 |

### 已运行验证

- 第一批：208 passed、1 failed，失败仍为基线中的全页禁止 title 断言。
- 第二批：241 passed、1 failed、11 subtests passed，失败仍为基线中的页面断言。
- 页面断言同步后的相关测试：133 passed。
- Ruff F401/F811/F841/F821/F822/F823 通过；117 个受版本控制的 Python 源文件解析和编译通过；`AccountView` / `MarketView` 标注可解析。
- 临时虚拟环境 `pip check`、`git diff --check` 通过。
- 清理后的全量：575 passed、13 subtests passed、17 个既有弃用警告（FastAPI on_event 及 Starlette/httpx）；无失败、无跳过。
- 删除旧 HTTP 服务后的全量：574 passed、13 subtests passed、17 个既有弃用警告；减少的一项测试只覆盖已删除的旧 HTTP 查询参数解析函数。
- 主 CLI 和五个子命令 `--help` 通过；`scripts/` 的 21 个 argparse 入口 `--help` 通过；`app.main:app` 导入及主要路由注册通过。
- 独立只读审阅未发现源代码回归，发现多账户模板对应的凭证填写说明不完整；已核对配置覆盖逻辑并补全文档。

### 审阅后的取舍

- 不增加仓库外 Python 调用的推测性兼容层：本项目没有发布的 Python 包/SDK 声明，已删除项没有仓库内调用、配置或动态注册，保留有真实消费者的导出。若存在未登记的仓库外导入，仍需调用方核验。
- 不更改实盘恢复、历史回填或交易语义：缺少生产状态与交易所验证，错误删除会影响订单与恢复，因此保留在用分支。
- 后续排查确认基线调度测试的 mock 未配置待唤醒计划返回值：默认 MagicMock 被当作待处理计划。测试现在显式返回 None，并检查第二轮未提交后台任务；实际调度逻辑未修改。

### 保留项与需要的证据

- 旧组合市价减仓执行器、数据库迁移和旧任务日志解析：都有持久化恢复或运维用途，缺少所有运行实例完成迁移的证据。
- `[Settings]` 及监控通知：`infra/binance_top10_monitor.py` 有独立 `__main__`，并非无用旧配置。
- `core/execution` 的独立 kernel 路径：生产 coordinator 仍依赖 strategy/manager，新核心尚未完整接管，保留双方路径。
- 研究与回填脚本：主要核对入口、交叉导入和静态引用，未逐个执行回测、历史数据修复或核验研究结论；保留全部脚本入口。
- `httpx` 虽非交易生产代码直接导入，但仪表盘 TestClient 使用，保留依赖声明；其他四项依赖均有直接运行时消费者。
- 后台调度测试偶发失败：清理前已出现；后续使用可重复场景确认测试替身误报，并修正替身配置与异步断言，保留生产实现。

### 验证边界

未访问交易所、未执行下单或历史回填、未启动线上服务；没有线上部署配置和外部 Python 调用者清单。验证覆盖本地模拟/临时数据库、框架路由和源码入口，不能据此宣称生产行为已全面核验。
