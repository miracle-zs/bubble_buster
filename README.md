# Bubble Buster

币安 U 本位合约 Top10 涨幅做空策略（主网）+ 常驻服务调度 + 可视化 Dashboard。

## 功能概览

- 每天 `07:40 (Asia/Shanghai)` 执行一次入场（按 UTC 开盘价口径筛选涨幅榜）。
- 固定按可用余额 `10` 份分配，`2x` 杠杆做空。
- 单仓风控：
  - 止盈：价格下跌 `20%`
  - 止损：`liq_price * 0.99`
  - 超时：持仓 `47.5h` 平仓
- 幂等与可追溯：
  - 同一 `trade_day_utc` 只执行一次 entry
  - SQLite 记录 run/position/order event
- 后台常驻服务（不依赖 cron 也可运行）。
- Dashboard（FastAPI + ECharts）：
  - 资金曲线（可拖动/缩放）
  - 回撤统计
  - 运行状态、仓位、事件、日志
- 余额快照：后台每个管理周期（默认 `60s`）采集并写入 `wallet_snapshots`，即使没人打开前端也会落库。

## 目录结构

- `/Users/zhangshuai/PycharmProjects/bubble_buster/main.py`：CLI 入口（entry/manage/service/dashboard）
- `/Users/zhangshuai/PycharmProjects/bubble_buster/dashboard_fastapi.py`：Dashboard 服务
- `/Users/zhangshuai/PycharmProjects/bubble_buster/dashboard_server.py`：Dashboard 数据与页面
- `/Users/zhangshuai/PycharmProjects/bubble_buster/core/`：核心策略与运行时逻辑
  - `strategy_top10_short.py`：建仓与出场挂单
  - `position_manager.py`：巡检、动态止损、超时平仓
  - `runtime_service.py`：常驻调度
  - `runtime_components.py`：组件装配
  - `state_store.py`：SQLite 存储层
  - `balance_sampler.py`：余额快照采集
- `/Users/zhangshuai/PycharmProjects/bubble_buster/infra/`：基础设施适配
  - `binance_futures_client.py`：交易所 API 客户端
  - `binance_top10_monitor.py`：涨幅榜数据计算
  - `notifier.py`：Server 酱通知
- `/Users/zhangshuai/PycharmProjects/bubble_buster/schema.sql`：数据库表结构
- `/Users/zhangshuai/PycharmProjects/bubble_buster/config.ini.example`：配置模板

## 环境准备

1. Python 3.11+（推荐使用 `conda base`）
2. 安装依赖

```bash
cd /Users/zhangshuai/PycharmProjects/bubble_buster
pip install -r requirements.txt
```

3. 配置

```bash
cp /Users/zhangshuai/PycharmProjects/bubble_buster/config.ini.example /Users/zhangshuai/PycharmProjects/bubble_buster/config.ini
```

填写 `config.ini` 中至少以下字段：

- `[binance]`：`api_key` / `api_secret` / `base_url`
- `[notify]`：`enabled` / `serverchan_sendkey`（可选）

## 运行方式

### 1) 常驻服务（推荐）

```bash
cd /Users/zhangshuai/PycharmProjects/bubble_buster
python main.py service --config config.ini
```

说明：

- service 会按配置自动执行 entry + manage
- 同时每个 manage 周期都会采集余额并写入 `wallet_snapshots`

### 2) Web Dashboard（含内置后台服务）

```bash
cd /Users/zhangshuai/PycharmProjects/bubble_buster
BUBBLE_BUSTER_CONFIG=/Users/zhangshuai/PycharmProjects/bubble_buster/config.ini \
uvicorn app.main:app --host 127.0.0.1 --port 8787 --reload
```

访问：`http://127.0.0.1:8787`

### 3) 手动命令

```bash
# 手动执行一次 entry
python main.py entry --config config.ini

# 手动执行一次 manage
python main.py manage --config config.ini

# manage 循环
python main.py manage --config config.ini --loop
```

## 配置重点

`/Users/zhangshuai/PycharmProjects/bubble_buster/config.ini`

- `[strategy]`
  - `leverage=2`
  - `top_n=10`
  - `tp_price_drop_pct=20`
  - `sl_liq_buffer_pct=1`
  - `max_hold_hours=47.5`
- `[runtime]`
  - `timezone=Asia/Shanghai`
  - `entry_hour=7`
  - `entry_minute=40`
  - `manager_interval_sec=60`
  - `run_service_with_dashboard=true`
  - `wallet_snapshot_asset=USDT`

## 数据库表

- `runs`：每日入场任务状态
- `positions`：策略仓位状态
- `order_events`：下单/撤单/成交事件
- `wallet_snapshots`：余额快照（默认每 60s 一条）
- `locks`：锁信息

默认数据库：`/Users/zhangshuai/PycharmProjects/bubble_buster/state.db`

## 测试

```bash
cd /Users/zhangshuai/PycharmProjects/bubble_buster
conda run -n base python -m unittest discover -s tests -p 'test_*.py'
```

## 注意事项

- 本项目默认主网，请先小资金验证。
- 请确保币安账户处于单向模式（非对冲双向）。
- `config.ini` 包含密钥，不要提交到仓库。
- 交易有风险，策略和代码均不保证收益。

## 免责声明

仅供研究与学习用途。任何实盘操作风险由使用者自行承担。
