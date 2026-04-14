# Account Protection Exempt Symbols Design

## Goal

Add an account-scoped symbol whitelist so specific symbols can bypass all automatic take-profit and stop-loss protections for one account only.

Primary use case:

- Account `55` needs to hold a long-lived manual short position, such as `XAUUSDT`.
- That position must not be affected by any automated TP/SL or protection tasks.
- The position must remain visible in dashboard, logs, and exchange/tracking sync flows.

## Requirements

- Whitelist is account-scoped, not global.
- Matching is by exact symbol, normalized to uppercase.
- Whitelisted symbols skip all automated take-profit / stop-loss behaviors.
- Whitelisted symbols must also skip initial TP/SL order placement after entry.
- Visibility and manual lifecycle handling must remain intact.

## Non-Goals

- No wildcard symbol matching.
- No database-backed per-position exemption flags.
- No hiding of exempt positions from dashboard or notifications.
- No change to manual close detection or state synchronization.

## Proposed Configuration

Add a new runtime-level config field:

```ini
[account.55.runtime]
protection_exempt_symbols = XAUUSDT
```

Rules:

- Comma-separated list.
- Normalize with `strip()` and `upper()`.
- Empty values are ignored.
- Scope is only the account where the config is defined.

This field belongs under runtime/account runtime config rather than strategy config because most affected logic lives in `RuntimeService` and `PositionManager`.

## Behavioral Model

For a whitelisted symbol in a specific account:

- Do not place initial TP order.
- Do not place initial SL order.
- Do not refresh dynamic stop-loss.
- Do not trigger fixed TP handling.
- Do not apply daily loss-cut.
- Do not apply morning protection stop.
- Do not apply noon protection stop.
- Do not apply hourly exchange take-profit.
- Do not include the symbol in equity recovery take-profit reductions.
- Do continue to show the position in dashboard and runtime state.
- Do continue to detect manual/external close and sync state.

In short: exempt symbols are visible but unmanaged by automatic protection logic.

## Affected Components

### `core/runtime_components.py`

- Parse `protection_exempt_symbols` from selected runtime config.
- Pass normalized symbol set into `Top10ShortStrategy`.
- Pass normalized symbol set into `PositionManager`.

### `core/strategy_top10_short.py`

- Store the exempt symbol set on the strategy instance.
- In `_place_exit_orders()`, return early when `symbol` is exempt.
- In `run_equity_recovery_take_profit()`, skip exempt symbols during reduction.

### `core/position_manager.py`

- Store the exempt symbol set on the manager instance.
- Add a small helper such as `_is_protection_exempt(symbol)`.
- Skip exempt symbols in:
  - `run_daily_loss_cut()`
  - `run_noon_protection_stop()`
  - `run_morning_protection_stop()`
  - `run_hourly_exchange_take_profit()`
  - `_manage_position()`
  - `_update_dynamic_stop()`

`_manage_position()` should skip all automated TP/SL/timeout management for exempt symbols while preserving external-close detection.

Recommended behavior:

- Still query current exchange position risk.
- If position no longer exists, continue to mark tracked position closed as external.
- Otherwise return a skip result and do not inspect TP/SL order state or timeout logic.

This avoids leaving stale tracked OPEN positions after a manual close.

## Detailed Runtime Semantics

### Entry / Initial Orders

If a strategy-created position uses an exempt symbol:

- Position record may still be created.
- Exit orders are not placed.
- `tp_*` and `sl_*` fields remain empty.

This is intentional because the symbol is explicitly opted out of all protection handling.

### Manage Loop

For exempt tracked positions:

- Do not treat missing TP/SL orders as errors.
- Do not run timeout close.
- Do not run dynamic SL refresh.
- Do still reconcile external/manual close if exchange position is absent.

### Exchange-Wide Tasks

For exchange-scanning tasks on account `55`, exempt symbols must be filtered before any close/update action:

- daily loss-cut
- morning protection
- noon protection
- hourly exchange take-profit

### Equity Recovery

Equity recovery remains enabled at account level, but exempt symbols are excluded from adjustment. Other non-exempt positions in the same account still participate normally.

## Notifications and Observability

Notifications should remain concise. No new notification type is required.

Recommended minimal observability:

- Include exempt skips in debug/info logs.
- Optionally include a `skipped_exempt` counter in summaries where convenient.

This is useful but not required for initial implementation if it adds too much churn.

## Edge Cases

### Exempt symbol with tracked position but no exchange position

- Mark as `CLOSED_EXTERNAL` as usual.
- Reason remains unchanged.

### Exempt symbol in accounts other than `55`

- Only exempt if that specific account config includes the symbol.
- Same symbol in another account is unaffected.

### Exempt symbol in `loss_cut_only` account

- Still skip all protection logic for that symbol.
- Other non-exempt positions in the same account continue to use account tasks normally.

### Symbol formatting

- Normalize incoming config and runtime symbol strings to uppercase trimmed values.
- Do not support aliases or fuzzy matching.

## Alternatives Considered

### 1. Global symbol whitelist

Rejected because it would accidentally disable protections for the same symbol across all accounts.

### 2. Database per-position exemption flag

Rejected for now because it requires broader lifecycle changes and is unnecessary for the current account-specific use case.

### 3. Strategy-level config instead of runtime-level config

Rejected because the affected behavior spans strategy placement, manage loop, and service-triggered manager tasks. Runtime/account runtime config is the clearest ownership point.

## Testing Strategy

Add targeted tests for:

- Runtime config parsing of `protection_exempt_symbols`.
- Strategy skips initial TP/SL placement for exempt symbol.
- Position manager skips dynamic stop/timeout/TP/SL handling for exempt tracked symbol but still closes externally when position disappears.
- Daily loss-cut skips exempt symbol.
- Morning protection skips exempt symbol.
- Noon protection skips exempt symbol.
- Hourly exchange take-profit skips exempt symbol.
- Equity recovery skips exempt symbol while still reducing non-exempt positions.
- Account scoping: exempt symbol in account `55` does not affect another account.

## Recommendation

Implement the account-scoped runtime whitelist now.

It solves the immediate account `55` long-hold short-position use case with minimal structural change, preserves observability, and avoids leaking special-case behavior into other accounts.
