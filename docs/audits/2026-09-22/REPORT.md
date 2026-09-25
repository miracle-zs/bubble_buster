# Storage / Transport / Compute / Resilience 审计

审计时间：2026-09-22，北京时间约 23:02–23:10。

结论：存在三个已在本地复现的正确性/调度问题、一个有线上测量依据的 SQL 优化点，以及通知配置和健康检查方面的弹性缺口。当前没有证据表明需要扩容服务器、迁移 PostgreSQL 或大规模架构重写。

## 范围与证据边界

- 服务器只读检查：资源、systemd 元数据、当天策略日志、SQLite 元数据及 SELECT、三个本机 HTTP GET。未重启服务、改配置、发通知或调用交易接口。
- 核对了 `core/state_store.py`、`core/account_snapshot.py`、`core/runtime_service.py`、`infra/binance_rate_limit.py`、`infra/binance_user_stream.py`、`infra/binance_futures_client.py`，六个文件本地/线上 SHA256 一致。
- 本地和线上均存在既有未提交改动；线上 Git HEAD 为 `cbaeeec`。Dashboard 文件不能仅按 HEAD 假定一致，线上另确认存在本文讨论的 MAX(id) 分组查询。
- 用户未指定某个已发生的故障，因此先盘点代码，再用独立故障注入构建反馈环。复现表明代码存在缺陷，不代表已经证明线上发生了资金或订单损失。
- 本次只新增本报告和隔离探针，没有修复或部署业务代码。探针不进入正常测试自动发现路径。

## 1. Storage & State：新账户状态可被旧 REST 覆盖（P1）

证据：`core/account_snapshot.py:71`、`:169`，`core/state_store.py:1572`。

`capture()` 持有 provider 锁期间执行网络请求，返回后全量替换账户和持仓。`apply_stream_update()` 却先写数据库、读取数据库，再获取同一把锁更新缓存。

可复现时序：

1. 初始钱包余额 100；线程 A 发起 REST，响应被暂停。
2. 线程 B 收到较新用户流，钱包余额 200，已成功写库；随后等待 provider 锁。
3. A 返回较旧的余额 100，全量替换数据库。
4. 两线程结束后数据库为 100，而应保留较新的 200。

数据库账户与持仓由后台采样和 Dashboard 消费；丢失新状态会影响这些读取，内存缓存还可能与数据库暂时不一致。不能只因为有 RLock 就认为整个更新已经串行化。

建议：以账户为边界统一提交快照与增量；保留单次 REST 请求的基线版本，合并期间产生的增量，使用版本条件写入。数据库与缓存的发布应遵循同一顺序。不要简单将数据库写入也搬进一个长时间持锁的网络区间，否则会阻塞 WebSocket 消费。

## 2. Transport：旧回报/迟到 REST 能将订单终态倒退（P1）

证据：`core/state_store.py:1805`、`:1889`，`infra/binance_user_stream.py:296`。

`upsert_exchange_order_state()` 在冲突时无条件覆盖 status、executed_qty、event_time。两个独立探针均失败：

- 先写较新的 FILLED/成交量 1，再写较旧 NEW/成交量 0，最终为 NEW/0。
- REST 获取活动订单开始后订单成交，用户流先记录 FILLED；迟到 REST 列表通过真实 `reconcile_open_order_state()` 写入后，最终仍退回 NEW。

第二种不要求 WebSocket 自身乱序，只需要网络请求与用户流正常并发。`reconcile_open_order_state()` 使用本地处理时间写入，也意味着仅比较当前 event_time 字段不足以解决问题。此外 `_event_time_iso()` 丢弃毫秒，同秒回报需要额外版本或状态规则。

影响：活动单集合、成交终态判断和后续对账可能短暂错误。尚未证明该路径在线上造成重复下单或错平仓。

建议：显式建模订单状态转移；阻止真实成交终态退回活动态，累计成交量保持单调；保留交易所毫秒时间，区分事件时间、请求基线版本和接收时间。REST 对账与用户流共用订单状态提交入口，并针对取消、部分成交、algo 母子单建立状态规则。

## 3. Compute：后台流水 I/O 阻塞主调度（P1）

证据：`core/runtime_service.py:1325`、`:1342`；`infra/binance_rate_limit.py:97`、`:116`。

`run_cycle()` 首先同步执行 `_run_cashflow_sync_if_due()`；后者直接调用 `sync_cashflows_once()`。此调用不受账户 executor 的 timeout 保护。网络重试、共享冷却或权重预算等待都会阻止后续入场、定时保护与 manage 的调度。

探针将 account_task_timeout_sec 设置为 0.1 秒，暂停现金流 I/O；0.25 秒后保护任务仍未得到分派，释放 I/O 后才继续。这证明是主线程依赖，不是 worker 数量不够。

线上当天 20:00:09 出现 income 网络重试，说明该依赖上的网络异常确实发生过；未据此推断某次保护已经错过执行窗口。

建议：现金流同步、readonly 快照、孤儿订单清理使用有界后台任务；每账户/任务单飞防止重入，跨周期复用在途任务。主循环只判断到期与分派。为限流等待提供截止时间和停止信号，超时不能仅返回后继续无限排队。

现有 entry/manage/scheduled executor 和在途任务去重值得保留。当前 `shutdown(wait=True)` 仍会等待运行中的任务，而限流等待没有停止信号；应纳入停机故障测试，本文未进行线上停机验证。

## 4. Storage 性能：最新权益查询扫描历史（P2）

证据：`dashboard_server.py:3580` 的 `_configured_accounts_summary_rows()`，以及无显式账户列表的同类查询。

线上 wallet_snapshots 有 1,505,329 行，数据库约 404 MiB。现有 `GROUP BY account_id / MAX(id)` 虽使用 `idx_wallet_snapshots_account_id_id`，仍遍历所选账户的历史索引区间并检查 error。

只读测量（六个账户）：

| 查询 | 第一次 | 第二次 |
|---|---:|---:|
| 分组 MAX(id)，error IS NULL | 1.3582 秒 | 0.9810 秒 |
| 每账户 ORDER BY id DESC LIMIT 1，error IS NULL | 0.0002 秒 | 0.0001 秒 |

第二轮在同一只读事务内执行两种查询，所有账户的结果 ID 相同。使用 3 秒 SQLite progress-handler 截止保护。两轮是定向测量，不是 p95/p99 压测，也不代表接口整体能获得同比例加速。

建议：已知账户集合直接逐账户倒序索引 seek，取需要的完整行；保留相同 error 过滤和无记录处理。已有缓存减少了前台影响，但仍应降低周期后台计算量。先改 SQL，无需仅为此迁移数据库。后续按查询用途设计历史降采样与保留策略，避免直接删除审计数据。

## 5. Resilience：通知被跳过，健康检查不能识别停滞（P1/P2）

### 线上通知配置缺口（P1，若这些通知应当送达）

当天策略日志有 10 条 `ServerChan enabled but sendkey is empty; skip notification`。`infra/notifier.py:30` 对此直接返回。

这是实际发生的通知跳过，不是推测。尚不能说明所有账户或所有外部告警渠道均不可用。

建议：启动时校验“enabled 但缺少 key”，在健康/配置状态中明确显示不可用；若本来无需发送，则明确关闭。实际配置密钥和发送验证不属于本次只读审计，未执行。

### 健康检查只有存活判断（P2）

`dashboard_fastapi.py:906` 的 `/healthz` 固定返回 `ok: true`，service_running 只检查线程 is_alive；主循环卡在现金流请求时仍然会被视为运行中。systemd 目前监护的是嵌入交易 runtime 的 uvicorn 进程，HTTP 存活也不能证明交易循环在推进。

建议：保留 liveness；增加 readiness，按部署模式判断主循环最后完成时间、账户快照年龄、用户流确定性、在途任务年龄、通知配置可用性。任务失败与卡住分开统计。需要失效告警与明确恢复路径后再考虑拆进程；拆进程时必须同时处理当前进程内限流协调范围。

## 线上基线与没有证据支持的结论

- systemd 自 2026-09-07 07:53:48 启动，NRestarts=0；34 个任务，服务 cgroup MemoryCurrent 约 513 MiB。
- 服务器 RAM 约 1.92 GiB，available 约 1.04 GiB；swap 已用约 1.34 GiB，但两次 vmstat 实时间隔采样 si/so 均为 0。不能仅凭 swap 占用断言当前内存抖动。
- 根分区约 48% 使用；load average 约 0.33/0.31/0.26，短采样 CPU 大部分空闲。
- SQLite 已为 WAL。当天日志至约 23:03，无 database is locked、hard scheduler timeout、global cooldown 或 cashflow sync failed；有一次 entry soft-timeout。无匹配明确状态码的 429/418 告警。直接搜索数字会误命中价格、时间及订单数据，不能作为限流次数。
- 当天一次 readonly01 WebSocket 断连错误；仅凭此不能称为持续故障。
- 六账户快照当时均在约一分钟内。HTTP 单次本机测量：healthz 69ms、summary/fast 8ms、summary/details 20ms，均为 200。不是端到端互联网延迟或负载基准。
- 没有证据支持“数据库锁竞争已成为线上瓶颈”“必须换异步框架”“必须用消息队列”或“立即加 CPU/内存”。

## 可复现验证与实施顺序

在项目根目录执行：

```sh
rtk proxy /Users/zhangshuai/anaconda3/bin/python docs/audits/2026-09-22/probe.py
```

结果：`Ran 4 tests in 0.318s; FAILED (failures=4)`。四个失败是三个问题的健康断言；网络由 mock 替代，数据库创建于 TemporaryDirectory，线程使用 Event 控制时序，退出时清理。该脚本作为故障证据保留，不是修复后应继续失败的正式回归套件。

相关现有测试：

```sh
rtk proxy /Users/zhangshuai/anaconda3/bin/python -m pytest -q tests/test_runtime_service.py tests/test_binance_weight_optimization.py tests/test_state_store.py tests/test_state_store_multi_account.py tests/test_client_utils.py
```

结果：`99 passed in 4.62s`。不是全量测试；现有通过说明这些边界还未被当前用例捕获。

建议顺序：先将上述探针转成正式回归并修复账户状态合并、订单状态倒退、主调度阻塞；核实通知意图并完善 readiness；随后改最新权益 SQL。架构提取围绕账户状态提交、订单状态机、任务分派三个边界进行，避免机械拆大文件或一次性替换运行时。
