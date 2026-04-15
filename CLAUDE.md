# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bubble Buster is a cryptocurrency trading system for Binance USDT-M Futures. Core strategy: daily top-N gainer shorting with position management, take-profit/stop-loss automation, and equity recovery mechanisms.

## Development Commands

```bash
# Run all tests
python -m pytest -q

# Run a specific test file
python -m pytest tests/test_position_manager.py -v

# Run a single test
python -m pytest tests/test_position_manager.py::PositionManagerTest::test_dynamic_stop_loss_update -v
```

## Running the Application

```bash
# Start the built-in scheduler service (production)
python main.py --config config.ini service

# Start the FastAPI dashboard
python main.py --config config.ini dashboard --host 0.0.0.0 --port 8787

# Single entry execution
python main.py --config config.ini entry

# Single position management cycle
python main.py --config config.ini manage

# Continuous position management loop
python main.py --config config.ini manage --loop

# Single daily loss-cut
python main.py --config config.ini loss-cut
```

## Architecture

```
main.py                    # CLI entry point with subcommands
core/
  runtime_components.py    # Dependency injection - assembles all components
  runtime_service.py       # Built-in scheduler (replaces cron)
  strategy_top10_short.py  # Daily entry logic - TopN selection, position sizing, TP/SL
  position_manager.py      # Position lifecycle - TP/SL handling, timeout, dynamic stops
  balance_sampler.py       # Wallet equity snapshots
  state_store.py           # SQLite persistence layer
  account_config.py        # Multi-account configuration parsing
infra/
  binance_futures_client.py  # Binance API wrapper with retry, signing, rate limiting
  binance_top10_monitor.py   # Top gainer ranking logic
  notifier.py                # ServerChan notification
app/main.py                  # FastAPI app entry (imports dashboard_fastapi)
dashboard_fastapi.py         # Dashboard API endpoints
dashboard_server.py          # Dashboard frontend rendering
schema.sql                   # SQLite schema
```

## Key Design Patterns

**Component Assembly**: `core/runtime_components.py` is the central wiring point. `create_components()` builds `BinanceFuturesClient`, `StateStore`, `Top10ShortStrategy`, `PositionManager`, and `WalletSnapshotSampler` with merged config from global and account-specific sections.

**Multi-Account Support**: Config uses `[accounts]` section with `enabled` list. Account-specific overrides go in `[account.<id>.binance]`, `[account.<id>.strategy]`, `[account.<id>.runtime]`. Account mode can be `full` (complete strategy) or `loss_cut_only` (only daily loss-cut).

**Idempotency**: Entry uses `trade_day_utc` as idempotency key. Each day's entry can only run once. State store tracks runs, positions, order events, fills.

**Process Isolation**: File lock (`runtime.lock`) prevents concurrent execution. Each account instance should have separate `lock_file`, `db_path`, `log_dir`.

## Configuration

INI-style config files. Key sections:
- `[binance]` / `[account.<id>.binance]` - API credentials, base URL, timeouts
- `[strategy]` / `[account.<id>.strategy]` - `top_n`, `leverage`, `tp_price_drop_pct`, `max_hold_hours`, rebalance params
- `[runtime]` / `[account.<id>.runtime]` - Schedule times, intervals, `daily_loss_cut_*`, `morning_protection_*`, `hourly_exchange_take_profit_*`
- `[accounts]` - Multi-account setup with `enabled` list and `mode.<id>` settings
- `[notify]` - ServerChan notifications

See `config.production.multi.ini.example` for complete example.

## Important Classes

- `Top10ShortStrategy` (`core/strategy_top10_short.py`): Entry logic with rebalance support, equity recovery take-profit
- `PositionManager` (`core/position_manager.py`): TP/SL tracking, daily loss-cut, morning/noon protection stops, hourly exchange take-profit
- `BinanceFuturesClient` (`infra/binance_futures_client.py`): HTTP client with signature, retry, connection pooling
- `StateStore` (`core/state_store.py`): SQLite operations with account scoping

## Database Tables

Key tables in `schema.sql`:
- `runs`: Daily entry execution records
- `positions`: Position lifecycle with TP/SL prices, order IDs
- `order_events`: Order event stream
- `fills`: Execution details from order fills
- `wallet_snapshots`: Equity snapshots
- `rebalance_cycles` / `rebalance_actions`: Rebalancing logs
- `equity_recovery_events`: Equity recovery take-profit events
- `locks`: Runtime state locks for idempotent operations

## Code Conventions

- Chinese comments and log messages are used throughout
- UTC timestamps in database, local timezone for display/scheduling
- Error codes from Binance API: `-2019`, `-2027`, `-2028` for insufficient margin; `-4192` for cooling-off period
- Protection exempt symbols: whitelist in `[account.<id>.runtime]` to skip all automated TP/SL
