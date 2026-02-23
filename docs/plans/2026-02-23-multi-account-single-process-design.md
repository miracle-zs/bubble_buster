# Multi-Account Single-Process Concurrency Design

Date: 2026-02-23
Status: Implemented (2026-02-23)

## Implementation Notes

- Added account config parser and validation (`core/account_config.py`) for `enabled` + `mode.<account_id>` + per-account binance overrides.
- Runtime service now supports account runtime contexts with concurrent manage dispatch (`max_account_workers`).
- Added per-account resilience controls:
  - `account_failure_threshold`
  - `account_cooldown_cycles`
  - `account_task_timeout_sec`
- `StateStore` supports account-scoped access via `store.scoped(account_id)` and scoped queries for runs/positions/wallet/cashflow/equity-recovery/locks.
- Dashboard added:
  - `GET /api/accounts/summary`
  - `GET /api/account/{account_id}/snapshot`
  with account-filtered snapshot query path.

## 1. Context

Current system supports only one Binance account (`api_key/api_secret`) and one strategy parameter set in a single runtime instance. The target is to support multiple accounts with per-account hyperparameters, while running inside one process with concurrent account execution.

Confirmed constraints:
- Single process, concurrent execution across accounts.
- Single config file (`config.ini`) for all accounts.
- Single SQLite database, partitioned by `account_id`.
- Dashboard must provide aggregated account overview and account drill-down detail.
- Existing legacy startup scripts can be retired.

## 2. Goals and Non-Goals

### Goals
- Run `entry/manage/loss-cut/wallet snapshot` for multiple accounts concurrently.
- Isolate data logically by `account_id` in DB and all queries.
- Allow per-account overrides for binance/strategy/runtime/notify settings.
- Provide account-level health/metrics on Dashboard.
- Ensure one account failure does not stop other accounts in the same cycle.

### Non-Goals
- Multi-process orchestration in this phase.
- Database engine migration (stay on SQLite).
- Strategy algorithm redesign.

## 3. Architecture Overview

### 3.1 Runtime model
- Keep one `service` main loop.
- Build `AccountRuntimeContext` per account at startup:
  - `client`
  - `strategy`
  - `manager`
  - `wallet_sampler`
  - account-scoped `StateStore`
- Scheduler generates task list by account and task type.
- Execute tasks with `ThreadPoolExecutor` (bounded workers).
- Aggregate cycle results (success/failure/latency) and continue loop.

### 3.2 Account mode
- `full`: run entry/manage/loss-cut/snapshot.
- `loss_cut_only`: run only loss-cut in scheduled flow.

### 3.3 Failure handling
- Per-account exception isolation.
- Per-account timeout (`runtime.task_timeout_sec`).
- Per-account circuit breaker:
  - consecutive failures threshold
  - cool-down cycles before retry

## 4. Configuration Design

### 4.1 Config structure
- Keep global defaults:
  - `[binance]`
  - `[strategy]`
  - `[runtime]`
  - `[notify]`
- Add accounts registry:
  - `[accounts]`
- Add account override sections:
  - `[account.<id>.binance]`
  - `[account.<id>.strategy]`
  - `[account.<id>.runtime]`
  - `[account.<id>.notify]`

### 4.2 Accounts section keys
- `enabled = acc01,acc02,55`
- `mode.acc01 = full`
- `mode.acc02 = full`
- `mode.55 = loss_cut_only`

### 4.3 Inheritance rule
Read value priority:
1. `account.<id>.<section>.<key>`
2. `<section>.<key>` global default
3. hardcoded fallback

### 4.4 Validation
For each enabled account:
- effective `api_key` and `api_secret` must be non-empty.
- mode must be one of `{full, loss_cut_only}`.
- runtime schedule fields must be parseable.
- invalid account is skipped with explicit startup error log.

## 5. Database Design (Single DB + account_id)

### 5.1 Tables requiring `account_id`
- `runs`
- `positions`
- `order_events`
- `fills`
- `wallet_snapshots`
- `cashflow_events`
- `rebalance_cycles`
- `rebalance_actions`
- `equity_recovery_events`
- `locks`

### 5.2 Constraints and index updates
- Replace `runs.trade_day_utc` global unique with account-scoped unique:
  - `UNIQUE(account_id, trade_day_utc)`
- `cashflow_events` uniqueness becomes account-scoped:
  - `UNIQUE(account_id, unique_key)`
- Add account-leading indexes for hot queries, including:
  - `positions(account_id, status)`
  - `runs(account_id, trade_day_utc)`
  - `wallet_snapshots(account_id, captured_at_utc)`
  - event/time indexes prefixed by `account_id`
- `locks` primary key becomes composite `(account_id, lock_name)`.

### 5.3 Migration strategy
Implement `migrate_to_multi_account(default_account_id)` in `StateStore`:
1. backup DB file (`state.db.bak.<timestamp>`)
2. add nullable `account_id` columns
3. backfill historical rows with `default_account_id`
4. rebuild tables requiring changed UNIQUE/PK in SQLite
5. recreate indexes
6. run `PRAGMA integrity_check`

If migration fails, rollback transaction and keep original DB.

## 6. Service Concurrency Design

### 6.1 New runtime controls
Add under `[runtime]`:
- `max_account_workers` (default `min(account_count, 4)`)
- `task_timeout_sec` (default e.g. 120)
- `account_failure_threshold` (default e.g. 3)
- `account_cooldown_cycles` (default e.g. 5)

### 6.2 Execution pattern
- Entry window: dispatch accounts concurrently in one entry cycle.
- Manage loop: periodic concurrent dispatch.
- Loss-cut: dispatch only `full + loss_cut_only` accounts.
- Snapshot: dispatch only `full` accounts.

### 6.3 Logging
All runtime logs include account context prefix:
- `[account=acc01] ...`
Cycle summary logs include account success/failure counts and latency.

## 7. Dashboard Design

### 7.1 New views
- Account overview page (aggregate across all accounts).
- Click-through to existing detail page with `account_id` filter.

### 7.2 API additions
- `GET /api/accounts/summary`
- `GET /api/account/{account_id}/snapshot`

### 7.3 Query rules
- Aggregate APIs: `GROUP BY account_id`.
- Detail APIs: every query must include `WHERE account_id = ?`.
- optional short cache (2-5 seconds) for summary endpoint.

### 7.4 Health status
Define per-account status for overview cards:
- `healthy`
- `degraded`
- `tripped`

Derived from recent task success/failure and breaker state.

## 8. Implementation Phases

1. Config and runtime scaffolding
- parse account list/mode/overrides
- construct per-account contexts
- concurrent task executor skeleton

2. DB migration + StateStore account scoping
- schema migration
- account-aware store APIs
- remove unscoped queries from runtime path

3. Strategy/manager/sampler multi-account integration
- wire all scheduled tasks to account contexts
- add timeout/breaker behavior

4. Dashboard aggregate + drill-down
- new endpoints
- front-end account overview and navigation

5. Hardening and tests
- config parse/validation tests
- migration tests
- concurrency isolation tests
- dashboard account API tests

## 9. Risks and Mitigations

1. SQLite lock contention under concurrent writes
- Mitigation: short transactions, retry-on-lock, bounded worker count.

2. Cross-account data leakage
- Mitigation: strict account-scoped query API, no default unscoped read in runtime path.

3. One slow account delaying cycle completion
- Mitigation: per-account timeout + breaker.

4. Observability complexity
- Mitigation: mandatory account-tagged logs and cycle-level summary metrics.

## 10. Acceptance Criteria

- Multiple accounts execute concurrently in one process.
- Each account can have different strategy hyperparameters.
- DB data is queryable by account and no cross-account contamination is observed.
- Dashboard homepage shows all accounts; drill-down retains existing detail behavior.
- Failure in one account does not stop execution of other accounts in same cycle.
