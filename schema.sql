CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT 'default',
    trade_day_utc TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    status TEXT NOT NULL,
    message TEXT,
    UNIQUE(account_id, trade_day_utc)
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    entry_price REAL NOT NULL,
    liq_price_open REAL,
    liq_price_latest REAL,
    tp_price REAL,
    sl_price REAL,
    tp_order_id INTEGER,
    sl_order_id INTEGER,
    tp_client_order_id TEXT,
    sl_client_order_id TEXT,
    opened_at_utc TEXT NOT NULL,
    expire_at_utc TEXT NOT NULL,
    closed_at_utc TEXT,
    close_order_id INTEGER,
    status TEXT NOT NULL,
    close_reason TEXT,
    last_error TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_symbol_status ON positions(symbol, status);
CREATE INDEX IF NOT EXISTS idx_positions_status_opened ON positions(status, opened_at_utc);

CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL DEFAULT 'default',
    position_id INTEGER,
    symbol TEXT NOT NULL,
    order_id INTEGER,
    client_order_id TEXT,
    type TEXT,
    side TEXT,
    price REAL,
    qty REAL,
    status TEXT,
    event_time_utc TEXT NOT NULL,
    raw_json TEXT,
    FOREIGN KEY(position_id) REFERENCES positions(id)
);

CREATE INDEX IF NOT EXISTS idx_order_events_position ON order_events(position_id);
CREATE INDEX IF NOT EXISTS idx_order_events_symbol ON order_events(symbol);
CREATE INDEX IF NOT EXISTS idx_order_events_account_id_id ON order_events(account_id, id);
CREATE INDEX IF NOT EXISTS idx_order_events_position_order_id_id ON order_events(position_id, order_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_order_events_position_side_status_id ON order_events(position_id, side, status, id DESC);

CREATE TABLE IF NOT EXISTS wallet_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL DEFAULT 'default',
    captured_at_utc TEXT NOT NULL,
    balance_usdt REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'API',
    error TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_snapshots_captured_at ON wallet_snapshots(captured_at_utc);
CREATE INDEX IF NOT EXISTS idx_wallet_snapshots_account_captured_at ON wallet_snapshots(account_id, captured_at_utc);
CREATE INDEX IF NOT EXISTS idx_wallet_snapshots_account_id_id ON wallet_snapshots(account_id, id);

CREATE TABLE IF NOT EXISTS cashflow_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL DEFAULT 'default',
    unique_key TEXT NOT NULL UNIQUE,
    event_time_utc TEXT NOT NULL,
    asset TEXT NOT NULL,
    amount REAL NOT NULL,
    income_type TEXT NOT NULL,
    symbol TEXT,
    tran_id TEXT,
    info TEXT,
    raw_json TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cashflow_events_time ON cashflow_events(event_time_utc);
CREATE INDEX IF NOT EXISTS idx_cashflow_events_account_tran
    ON cashflow_events(account_id, tran_id);
CREATE INDEX IF NOT EXISTS idx_cashflow_events_asset_time ON cashflow_events(asset, event_time_utc);
CREATE INDEX IF NOT EXISTS idx_cashflow_events_account_asset_time ON cashflow_events(account_id, asset, event_time_utc);

-- Latest account state shared by the scheduler, position manager and dashboard.
-- Wallet history remains in wallet_snapshots; these tables intentionally keep
-- only the current exchange view so dashboard requests never call Binance.
CREATE TABLE IF NOT EXISTS account_state (
    account_id TEXT PRIMARY KEY,
    captured_at_utc TEXT NOT NULL,
    wallet_balance REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    equity REAL NOT NULL,
    available_balance REAL NOT NULL,
    stream_status TEXT NOT NULL DEFAULT 'REST',
    raw_json TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS account_position_state (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    position_side TEXT NOT NULL DEFAULT 'BOTH',
    position_amt REAL NOT NULL DEFAULT 0,
    entry_price REAL,
    break_even_price REAL,
    mark_price REAL,
    unrealized_pnl REAL,
    liquidation_price REAL,
    leverage REAL,
    notional REAL,
    isolated_margin REAL,
    initial_margin REAL,
    captured_at_utc TEXT NOT NULL,
    raw_json TEXT,
    PRIMARY KEY(account_id, symbol, position_side)
);

CREATE INDEX IF NOT EXISTS idx_account_position_state_account_amt
    ON account_position_state(account_id, position_amt);

CREATE TABLE IF NOT EXISTS exchange_order_state (
    account_id TEXT NOT NULL,
    order_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    order_id TEXT,
    client_order_id TEXT,
    type TEXT,
    side TEXT,
    position_side TEXT,
    status TEXT,
    execution_type TEXT,
    price REAL,
    stop_price REAL,
    avg_price REAL,
    original_qty REAL,
    executed_qty REAL,
    reduce_only INTEGER,
    close_position INTEGER,
    event_time_utc TEXT NOT NULL,
    source TEXT NOT NULL,
    raw_json TEXT,
    PRIMARY KEY(account_id, order_key)
);

CREATE INDEX IF NOT EXISTS idx_exchange_order_state_lookup
    ON exchange_order_state(account_id, symbol, order_id, client_order_id);
CREATE INDEX IF NOT EXISTS idx_exchange_order_state_status
    ON exchange_order_state(account_id, status);

-- Raw readonly statistics ledger.  The dashboard aggregates these local rows;
-- Binance is touched only by the background incremental synchronizer.
CREATE TABLE IF NOT EXISTS binance_income_records (
    account_id TEXT NOT NULL,
    unique_key TEXT NOT NULL,
    tran_id TEXT,
    trade_id TEXT,
    symbol TEXT,
    income_type TEXT NOT NULL,
    asset TEXT,
    income REAL NOT NULL,
    event_time_ms INTEGER NOT NULL,
    raw_json TEXT,
    created_at_utc TEXT NOT NULL,
    PRIMARY KEY(account_id, unique_key)
);

CREATE INDEX IF NOT EXISTS idx_binance_income_account_time
    ON binance_income_records(account_id, event_time_ms);
CREATE INDEX IF NOT EXISTS idx_binance_income_account_type_time
    ON binance_income_records(account_id, income_type, event_time_ms);
CREATE INDEX IF NOT EXISTS idx_binance_income_account_tran
    ON binance_income_records(account_id, tran_id);

CREATE TABLE IF NOT EXISTS binance_user_trades (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    order_id TEXT,
    event_time_ms INTEGER NOT NULL,
    realized_pnl REAL NOT NULL DEFAULT 0,
    commission REAL NOT NULL DEFAULT 0,
    commission_asset TEXT,
    side TEXT,
    qty REAL,
    price REAL,
    quote_qty REAL,
    raw_json TEXT,
    created_at_utc TEXT NOT NULL,
    PRIMARY KEY(account_id, symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_binance_user_trades_account_time
    ON binance_user_trades(account_id, event_time_ms);
CREATE INDEX IF NOT EXISTS idx_binance_user_trades_account_order
    ON binance_user_trades(account_id, symbol, order_id);

-- Market ranking inputs are project-wide (not account scoped).
CREATE TABLE IF NOT EXISTS daily_open_prices (
    day_utc TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open_price REAL NOT NULL,
    source TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY(day_utc, symbol)
);

CREATE TABLE IF NOT EXISTS market_data_cache (
    cache_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    expires_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rebalance_cycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    reason_tag TEXT NOT NULL,
    mode TEXT NOT NULL,
    reduce_only INTEGER NOT NULL,
    target_count INTEGER NOT NULL,
    open_positions INTEGER NOT NULL DEFAULT 0,
    virtual_slots INTEGER NOT NULL DEFAULT 0,
    equity_usdt REAL NOT NULL DEFAULT 0,
    target_gross_notional_usdt REAL NOT NULL DEFAULT 0,
    target_notional_per_position_usdt REAL NOT NULL DEFAULT 0,
    planned_count INTEGER NOT NULL DEFAULT 0,
    adjusted_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    reduced_notional_usdt REAL NOT NULL DEFAULT 0,
    added_notional_usdt REAL NOT NULL DEFAULT 0,
    skip_reason TEXT,
    started_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_rebalance_cycles_run ON rebalance_cycles(run_id);
CREATE INDEX IF NOT EXISTS idx_rebalance_cycles_started ON rebalance_cycles(started_at_utc);
CREATE INDEX IF NOT EXISTS idx_rebalance_cycles_reason ON rebalance_cycles(reason_tag, started_at_utc);

CREATE TABLE IF NOT EXISTS rebalance_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id INTEGER NOT NULL,
    run_id TEXT,
    position_id INTEGER,
    symbol TEXT NOT NULL,
    action_side TEXT,
    reduce_only INTEGER NOT NULL,
    ref_price REAL,
    current_notional_usdt REAL,
    target_notional_usdt REAL,
    deviation_notional_usdt REAL,
    deadband_notional_usdt REAL,
    max_adjust_notional_usdt REAL,
    requested_adjust_notional_usdt REAL,
    qty REAL,
    est_notional_usdt REAL,
    status TEXT NOT NULL,
    skip_reason TEXT,
    order_id INTEGER,
    client_order_id TEXT,
    error TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    FOREIGN KEY(cycle_id) REFERENCES rebalance_cycles(id),
    FOREIGN KEY(run_id) REFERENCES runs(run_id),
    FOREIGN KEY(position_id) REFERENCES positions(id)
);

CREATE INDEX IF NOT EXISTS idx_rebalance_actions_cycle ON rebalance_actions(cycle_id);
CREATE INDEX IF NOT EXISTS idx_rebalance_actions_status ON rebalance_actions(status);
CREATE INDEX IF NOT EXISTS idx_rebalance_actions_symbol ON rebalance_actions(symbol);
CREATE INDEX IF NOT EXISTS idx_rebalance_actions_position ON rebalance_actions(position_id);

CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_event_id INTEGER NOT NULL UNIQUE,
    position_id INTEGER,
    symbol TEXT NOT NULL,
    order_id INTEGER,
    client_order_id TEXT,
    side TEXT,
    reduce_only INTEGER,
    status TEXT,
    executed_qty REAL NOT NULL,
    quote_qty REAL,
    avg_price REAL,
    realized_pnl REAL,
    commission REAL,
    commission_asset TEXT,
    event_time_utc TEXT NOT NULL,
    raw_json TEXT,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY(order_event_id) REFERENCES order_events(id),
    FOREIGN KEY(position_id) REFERENCES positions(id)
);

CREATE INDEX IF NOT EXISTS idx_fills_time ON fills(event_time_utc);
CREATE INDEX IF NOT EXISTS idx_fills_symbol_time ON fills(symbol, event_time_utc);
CREATE INDEX IF NOT EXISTS idx_fills_position ON fills(position_id);

CREATE TABLE IF NOT EXISTS locks (
    lock_name TEXT PRIMARY KEY,
    holder TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS equity_recovery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL DEFAULT 'default',
    cycle_key TEXT NOT NULL,
    cycle_min_captured_at_utc TEXT NOT NULL,
    cycle_min_equity_usdt REAL NOT NULL,
    current_captured_at_utc TEXT NOT NULL,
    current_equity_usdt REAL NOT NULL,
    trigger_pct REAL NOT NULL,
    threshold_equity_usdt REAL NOT NULL,
    reduce_ratio REAL NOT NULL,
    open_positions INTEGER NOT NULL DEFAULT 0,
    adjusted_positions INTEGER NOT NULL DEFAULT 0,
    reduced_notional_usdt REAL NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    details_json TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_equity_recovery_cycle ON equity_recovery_events(cycle_key);
CREATE INDEX IF NOT EXISTS idx_equity_recovery_created ON equity_recovery_events(created_at_utc);
CREATE INDEX IF NOT EXISTS idx_equity_recovery_account_created ON equity_recovery_events(account_id, created_at_utc);
