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
    episode_id TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_symbol_status ON positions(symbol, status);
CREATE INDEX IF NOT EXISTS idx_positions_status_opened ON positions(status, opened_at_utc);
CREATE INDEX IF NOT EXISTS idx_positions_episode_id ON positions(episode_id);

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

CREATE TABLE IF NOT EXISTS entry_structure_protections (
    position_id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT 'default',
    stop_price REAL NOT NULL,
    bearish_close_time_utc TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    window_end_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_entry_structure_protections_account ON entry_structure_protections(account_id);

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

CREATE TABLE IF NOT EXISTS task_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL DEFAULT 'default',
    task_name TEXT NOT NULL,
    task_cycle TEXT,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    summary TEXT,
    payload_json TEXT,
    error TEXT,
    started_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    last_heartbeat_utc TEXT,
    time_local TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_executions_account_task_id
    ON task_executions(account_id, task_name, id DESC);
CREATE INDEX IF NOT EXISTS idx_task_executions_account_cycle
    ON task_executions(account_id, task_name, task_cycle);
CREATE INDEX IF NOT EXISTS idx_task_executions_created
    ON task_executions(created_at_utc DESC);

-- Execution Engine domain tables
CREATE TABLE IF NOT EXISTS order_intents (
    intent_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT 'default',
    client_intent_key TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    target_qty REAL,
    target_price REAL,
    intent_scope TEXT NOT NULL,
    position_id INTEGER,
    episode_id TEXT,
    status TEXT NOT NULL,
    reason TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_intents_account_status ON order_intents(account_id, status);
CREATE INDEX IF NOT EXISTS idx_order_intents_symbol ON order_intents(symbol);

CREATE TABLE IF NOT EXISTS order_attempts (
    attempt_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT 'default',
    symbol TEXT NOT NULL,
    client_order_id TEXT NOT NULL UNIQUE,
    exchange_order_id TEXT,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    submitted_qty REAL,
    executed_qty REAL NOT NULL DEFAULT 0,
    cumulative_quote_qty REAL,
    avg_price REAL,
    error_message TEXT,
    parent_attempt_id TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    FOREIGN KEY(intent_id) REFERENCES order_intents(intent_id)
);

CREATE INDEX IF NOT EXISTS idx_order_attempts_intent ON order_attempts(intent_id);
CREATE INDEX IF NOT EXISTS idx_order_attempts_client_order_id ON order_attempts(client_order_id);
CREATE INDEX IF NOT EXISTS idx_order_attempts_exchange_order_id ON order_attempts(account_id, symbol, exchange_order_id);
CREATE INDEX IF NOT EXISTS idx_order_attempts_status ON order_attempts(status);
CREATE INDEX IF NOT EXISTS idx_order_attempts_parent ON order_attempts(parent_attempt_id);

CREATE TABLE IF NOT EXISTS position_episodes (
    episode_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT 'default',
    symbol TEXT NOT NULL,
    position_side TEXT NOT NULL DEFAULT 'SHORT',
    status TEXT NOT NULL,
    opened_at_utc TEXT NOT NULL,
    closed_at_utc TEXT,
    target_qty REAL,
    current_qty REAL NOT NULL DEFAULT 0,
    realized_pnl REAL NOT NULL DEFAULT 0,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_position_episodes_account_symbol ON position_episodes(account_id, symbol, status);

CREATE TABLE IF NOT EXISTS risk_cycle_target_sets (
    target_set_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT 'default',
    cycle_type TEXT NOT NULL,
    cycle_key TEXT NOT NULL,
    status TEXT NOT NULL,
    targets_json TEXT NOT NULL,
    summary_json TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_risk_cycle_target_sets_account_cycle ON risk_cycle_target_sets(account_id, cycle_type, cycle_key);

CREATE TABLE IF NOT EXISTS protection_policy_states (
    policy_key TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT 'default',
    policy_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_protection_policy_states_account ON protection_policy_states(account_id, policy_type);

CREATE TABLE IF NOT EXISTS entry_plans (
    plan_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL DEFAULT 'default',
    symbol TEXT NOT NULL,
    status TEXT NOT NULL,
    hour_open_utc TEXT NOT NULL,
    next_wakeup_utc TEXT NOT NULL,
    plan_payload_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_entry_plans_account_status ON entry_plans(account_id, status);

CREATE TABLE IF NOT EXISTS execution_fills (
    fill_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT 'default',
    symbol TEXT NOT NULL,
    exchange_trade_id TEXT NOT NULL UNIQUE,
    exchange_order_id TEXT,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    commission REAL NOT NULL DEFAULT 0,
    commission_asset TEXT NOT NULL DEFAULT 'USDT',
    trade_time_utc TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY(attempt_id) REFERENCES order_attempts(attempt_id),
    FOREIGN KEY(intent_id) REFERENCES order_intents(intent_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_fills_attempt ON execution_fills(attempt_id);
CREATE INDEX IF NOT EXISTS idx_execution_fills_intent ON execution_fills(intent_id);
CREATE INDEX IF NOT EXISTS idx_execution_fills_trade_id ON execution_fills(exchange_trade_id);

CREATE TABLE IF NOT EXISTS ingestion_cursor_states (
    cursor_key TEXT NOT NULL,
    account_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (cursor_key, account_id)
);

CREATE TABLE IF NOT EXISTS task_occurrences (
    task_occurrence_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    cycle_key TEXT NOT NULL,
    status TEXT NOT NULL,
    due_at_utc TEXT NOT NULL,
    executed_at_utc TEXT,
    completed_at_utc TEXT,
    payload TEXT,
    error_message TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (account_id, task_type, cycle_key)
);

CREATE INDEX IF NOT EXISTS idx_task_occurrences_due ON task_occurrences(account_id, status, due_at_utc);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at_utc TEXT NOT NULL,
    description TEXT
);
