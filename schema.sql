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

CREATE TABLE IF NOT EXISTS locks (
    lock_name TEXT PRIMARY KEY,
    holder TEXT,
    updated_at_utc TEXT NOT NULL
);
