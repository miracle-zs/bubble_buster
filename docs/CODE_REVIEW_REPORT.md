# 代码审查报告

**审查日期**: 2026-04-16  
**修复日期**: 2026-04-16  
**审查范围**: 核心交易逻辑、API调用、状态管理

---

## 一、已修复的问题

### 1. ✅ [已修复] 缺少币安错误码 -4117 的处理

**文件**: `infra/binance_futures_client.py:77`

**修复内容**: 在 `RETRIABLE_ERROR_CODES` 中添加 `-4117` 错误码。

```python
# 修复后
RETRIABLE_ERROR_CODES = {-1001, -1003, -1006, -1007, -1008, -1021, -4117}
```

---

### 2. ✅ [已修复] `_is_expired` 时区处理

**文件**: `core/position_manager.py:1556-1563`

**修复内容**: 在比较前确保时区一致。

```python
# 修复后
@staticmethod
def _is_expired(expire_at_utc: str) -> bool:
    expire_time = datetime.fromisoformat(expire_at_utc)
    if expire_time.tzinfo is None:
        expire_time = expire_time.replace(tzinfo=timezone.utc)
    now_utc = datetime.now(timezone.utc)
    return now_utc >= expire_time
```

---

### 3. ✅ [已修复] 条件单触发后状态同步延迟

**文件**: `core/strategy_top10_short.py:1942-1965`

**修复内容**: 增加重试次数到8次，使用指数退避策略。

```python
# 修复后
def _load_short_position(self, symbol: str) -> Optional[Dict[str, str]]:
    max_retries = 8
    base_delay = 0.2
    for attempt in range(max_retries):
        # ... 查询逻辑
        delay = min(base_delay * (2 ** attempt), 5.0)
        time.sleep(delay)
```

---

### 4. ✅ [已修复] K线数据缺失处理不完整

**文件**: `core/position_manager.py:359-406`

**修复内容**: 添加完善的错误日志和异常处理。

```python
# 修复后
def _get_previous_closed_hour_open_and_close(...):
    try:
        rows = self.client.get_klines(...)
    except Exception as exc:
        LOGGER.warning("Failed to fetch hourly klines for symbol=%s: %s", symbol, exc)
        return None, None
    
    if row is None:
        LOGGER.warning("No hourly kline returned for symbol=%s", symbol)
        return None, None
    
    if len(row) < 5:
        LOGGER.warning("Incomplete kline data for symbol=%s: row_length=%s", symbol, len(row))
        return None, None
```

---

### 5. ✅ [已修复] 缺少 `priceProtect` 参数

**文件**: `core/strategy_top10_short.py` 和 `core/position_manager.py`

**修复内容**: 在条件单创建时添加 `priceProtect=True` 参数。

```python
# strategy_top10_short.py 修复后
return self.client.create_order(
    ...
    priceProtect=True,
    ...
)

# position_manager.py 修复后
create_order_params: Dict[str, object] = {
    ...
    "priceProtect": True,
    ...
}
```

---

## 二、未修复的问题（风险较低）

### 1. [低] SQL 动态拼接

**文件**: `core/state_store.py:213-215`

**风险**: `table_name` 来自硬编码，`default_account_id` 经过转义，仅在迁移时执行一次。

**决定**: 风险极低，暂不修复。

---

### 2. [低] 浮点数精度

**文件**: `core/strategy_top10_short.py:665-667`

**风险**: 已有 `threshold_eps` 容错处理。

**决定**: 当前实现可接受，暂不修复。

---

### 3. [低] 算法订单响应字段映射

**文件**: `infra/binance_futures_client.py:482-501`

**风险**: 映射逻辑正确，仅缺少调试日志。

**决定**: 可选优化，暂不修复。

---

## 三、API 使用正确性确认

### ✅ 正确：`closePosition` 参数使用

代码正确实现了 `closePosition=true` 不与 `quantity`/`reduceOnly` 同时使用。

### ✅ 正确：Hedge Mode 处理

单向持仓和双向持仓模式处理正确。

### ✅ 正确：算法订单迁移

正确实现了币安条件单迁移到算法订单服务的处理。

---

## 四、修复总结

| 项目 | 修复前 | 修复后 |
|------|--------|--------|
| 错误码覆盖 | 6个 | 7个（新增-4117） |
| 状态同步重试 | 5次固定间隔 | 8次指数退避 |
| 时区处理 | 可能出错 | 强制UTC |
| K线错误日志 | 无 | 完善 |
| 价格保护 | 无 | 启用 |

**语法验证**: 所有修改的文件通过 Python 语法检查。
