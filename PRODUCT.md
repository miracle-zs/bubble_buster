# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

- **Primary user (Quant Operator / Solo Developer)**: Operates and monitors an automated Binance USDT-M Futures top-N gainer shorting system across multiple live managed accounts (`full` / `loss_cut_only`) and read-only benchmark accounts (`readonly`).
- **Situations**:
  - **Desk check-in (Desktop)**: Daily cycle review around local entry time (`07:40` / `08:00` UTC+8), checking fleet health, portfolio stop distance, intraday entry progress, open positions, TP/SL order alignment, and cross-account normalized equity curves.
  - **On-call triage (Mobile 390px)**: Fast one-handed checks when alerted to task failures, portfolio stop-loss triggers, or stale snapshot telemetry.

## Product Purpose

Bubble Buster Dashboard provides a strictly read-only web operations console for inspecting live multi-account strategy equity, portfolio risk budgets, two-stage entry progress, position protection status, and execution anomalies without ever exposing trading mutation actions in the browser. Success means the operator can verify fleet health, distinguish real risk from stale snapshots, and drill into any anomalous account or symbol within seconds.

## Positioning

Unlike generic exchange portfolios or trading terminals, Bubble Buster is purpose-built around a single automated daily-cycle shorting mechanism: differential cross-account strategy highlighting, cycle-anchored portfolio stop-loss budget tracking (backed by backend lock state rather than curve guesswork), two-stage bearish/bullish candle entry timelines, and strict separation between managed strategy accounts and read-only benchmark accounts.

## Operating Context

- **Runtime & Delivery**: Served directly by FastAPI (`dashboard_fastapi.py` + `dashboard_server.py`) reading local SQLite state (`state.db`), logs (`logs/strategy.log`), and cached exchange telemetry. Zero frontend build pipeline; static assets in `app_static/` are versioned with SHA-256 `?v=` query hashes (`web/template_loader.py`).
- **Primary Surfaces**:
  - `/` — Accounts Overview (command bar, conditional fleet incident banner, managed account cards, readonly strip, daily entry progress board, exception-first task board).
  - `/account/{account_id}/` — Account Detail (account command bar, strategy strip, KPI summary rail, interactive ECharts equity curve, cycle risk or readonly 30-day trade stats panel, sortable open positions, tabbed activity logs, lazy diagnostics).
  - `/accounts/comparison/` — Multi-Account Equity Comparison (normalized relative % / absolute USDT ECharts overlay, multi-pattern legend toggles, sortable comparison metrics matrix).
- **Time & Locale**: Simplified Chinese (`zh-CN`) interface; database stores UTC timestamps while UI displays Beijing time (`Asia/Shanghai`, UTC+8) except explicit `trade_day_utc`.

## Capabilities and Constraints

- **Strict Read-Only Surface**: HTTP middleware rejects any method outside `GET` / `HEAD` / `OPTIONS` (`405`), and CSP enforces `script-src 'self'` and `form-action 'none'`. No order placement, cancellation, or config editing exists in the UI.
- **Canonical Domain Terminology** (governed by `CONTEXT.md`):
  - Use **账户周期** (not generic 交易日, except explicit `trade_day_utc`).
  - Always qualify position notional as **原始开仓金额** vs **当前剩余金额**; never label `positions.qty` as original entry quantity.
  - Distinguish **策略权益** (excluding window deposits/withdrawals) from **账户权益** (wallet balance including unrealized PnL & cashflows).
  - Readonly accounts never display managed portfolio stop-loss thresholds or automated entry schedules.
- **Data Fidelity**: Carried-forward fallback points preserve real `wallet.as_of_utc`; UI must surface snapshot age explicitly and never fabricate intraday trajectories from a single historical snapshot.

## Brand Commitments

- **Product Name**: Bubble Buster (`Binance USDT-M` venue badge).
- **Voice & Copy**: Direct, calm, operational Simplified Chinese. State exact numbers, timestamps, and failure reasons; avoid marketing fluff, raw storage acronyms (`DB`), or untranslated internal log strings where user-facing labels apply.

## Evidence on Hand

- Live/historical multi-account SQLite telemetry (`state.db` covering `acc01`–`acc04`, `readonly01`, `readonly02`).
- Automated regression tests (`tests/test_dashboard_server.py`, `tests/test_dashboard_fastapi.py`, `tests/test_template_loader.py`, `tests/test_overview_curve_metrics.py`).
- No external marketing copy, customer testimonials, or third-party analytics exist and none should be fabricated.

## Product Principles

1. **Truth Over Visual Drama**: Surface data staleness, carried-forward points, and backend lock states honestly; never interpolate misleading curves or hide stale timestamps.
2. **Exception-First Triage**: Default to surfacing active anomalies, stop triggers, and pending signals while keeping quiet/unstarted boilerplate collapsed.
3. **Strict Mode Separation**: Keep managed strategy controls/risk telemetry cleanly separated from read-only benchmark accounts across overview and detail views.
4. **Zero-Friction Keyboard & Mobile Parity**: Every routine inspection path works equally well via desktop hotkeys (`R`, `/`, `0`, `C`, `1-9`, `?`) and 390px one-handed mobile viewports.

## Accessibility & Inclusion

- Target **WCAG 2.1 AA** across dark surfaces (≥4.5:1 text contrast), full keyboard operability (skip link, visible `:focus-visible` rings, Escape dismiss with focus restoration, ARIA-wired tooltips and tablists), `prefers-reduced-motion`, `forced-colors` high-contrast support, and `pointer: coarse` 44px touch targets.
