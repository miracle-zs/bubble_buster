CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    trade_day_utc TEXT NOT NULL UNIQUE,
    started_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    status TEXT NOT NULL,
    message TEXT
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

CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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

CREATE TABLE IF NOT EXISTS wallet_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at_utc TEXT NOT NULL,
    balance_usdt REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'API',
    error TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_snapshots_captured_at ON wallet_snapshots(captured_at_utc);

CREATE TABLE IF NOT EXISTS cashflow_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
CREATE INDEX IF NOT EXISTS idx_cashflow_events_asset_time ON cashflow_events(asset, event_time_utc);

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
