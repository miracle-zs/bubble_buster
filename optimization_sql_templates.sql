-- Optimization SQL templates for rebalance strategy (SQLite).
-- Database: state.db
-- Purpose: evaluate rebalance behavior and generate features for parameter tuning.
--
-- Usage:
-- 1) Open this file and edit date range in each `params` CTE.
-- 2) Run with sqlite3:
--    sqlite3 state.db < optimization_sql_templates.sql
-- 3) Keep only the query blocks you need for faster output.

-- ============================================================
-- Q1) Data coverage and health check
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
)
SELECT
    (SELECT COUNT(*) FROM runs r, params p WHERE r.started_at_utc BETWEEN p.from_utc AND p.to_utc) AS run_count,
    (SELECT COUNT(*) FROM positions x, params p WHERE x.opened_at_utc BETWEEN p.from_utc AND p.to_utc) AS position_count,
    (SELECT COUNT(*) FROM order_events e, params p WHERE e.event_time_utc BETWEEN p.from_utc AND p.to_utc) AS order_event_count,
    (SELECT COUNT(*) FROM fills f, params p WHERE f.event_time_utc BETWEEN p.from_utc AND p.to_utc) AS fill_count,
    (SELECT COUNT(*) FROM rebalance_cycles c, params p WHERE c.started_at_utc BETWEEN p.from_utc AND p.to_utc) AS rebalance_cycle_count,
    (SELECT COUNT(*) FROM rebalance_actions a, params p WHERE a.created_at_utc BETWEEN p.from_utc AND p.to_utc) AS rebalance_action_count,
    (
        SELECT ROUND(100.0 * SUM(CASE WHEN a.order_id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
        FROM rebalance_actions a, params p
        WHERE a.created_at_utc BETWEEN p.from_utc AND p.to_utc
          AND a.status = 'ADJUSTED'
    ) AS adjusted_with_order_id_pct;

-- ============================================================
-- Q2) Cycle KPI by day / stage / mode
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
)
SELECT
    substr(c.started_at_utc, 1, 10) AS trade_day_utc,
    c.reason_tag,
    c.mode,
    COUNT(*) AS cycles,
    SUM(c.planned_count) AS planned_total,
    SUM(c.adjusted_count) AS adjusted_total,
    ROUND(100.0 * SUM(c.adjusted_count) / NULLIF(SUM(c.planned_count), 0), 2) AS adjust_rate_pct,
    SUM(c.error_count) AS errors_total,
    ROUND(AVG(c.error_count), 4) AS avg_errors_per_cycle,
    ROUND(SUM(c.reduced_notional_usdt), 6) AS reduced_notional_total,
    ROUND(SUM(c.added_notional_usdt), 6) AS added_notional_total
FROM rebalance_cycles c, params p
WHERE c.started_at_utc BETWEEN p.from_utc AND p.to_utc
GROUP BY substr(c.started_at_utc, 1, 10), c.reason_tag, c.mode
ORDER BY trade_day_utc DESC, c.reason_tag, c.mode;

-- ============================================================
-- Q3) Why actions are skipped (skip reason ranking)
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
)
SELECT
    COALESCE(a.skip_reason, 'UNKNOWN') AS skip_reason,
    COUNT(*) AS cnt,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct,
    ROUND(AVG(ABS(COALESCE(a.deviation_notional_usdt, 0))), 6) AS avg_abs_deviation
FROM rebalance_actions a, params p
WHERE a.created_at_utc BETWEEN p.from_utc AND p.to_utc
  AND a.status = 'SKIPPED'
GROUP BY COALESCE(a.skip_reason, 'UNKNOWN')
ORDER BY cnt DESC, skip_reason;

-- ============================================================
-- Q4) Planned -> Adjusted funnel by side
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
)
SELECT
    COALESCE(a.action_side, 'NA') AS action_side,
    SUM(CASE WHEN a.status = 'PLANNED' THEN 1 ELSE 0 END) AS planned,
    SUM(CASE WHEN a.status = 'ADJUSTED' THEN 1 ELSE 0 END) AS adjusted,
    SUM(CASE WHEN a.status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
    ROUND(
        100.0 * SUM(CASE WHEN a.status = 'ADJUSTED' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN a.status IN ('PLANNED', 'ADJUSTED', 'ERROR') THEN 1 ELSE 0 END), 0),
        2
    ) AS adjusted_rate_pct
FROM rebalance_actions a, params p
WHERE a.created_at_utc BETWEEN p.from_utc AND p.to_utc
GROUP BY COALESCE(a.action_side, 'NA')
ORDER BY action_side;

-- ============================================================
-- Q5) Execution quality (action joined with fill)
-- Note:
-- - `slippage_bps_signed` > 0 means worse execution than ref price.
-- - BUY: (avg_price - ref_price) / ref_price
-- - SELL: (ref_price - avg_price) / ref_price
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
),
action_fill AS (
    SELECT
        a.id AS action_id,
        a.action_side,
        a.status,
        a.symbol,
        a.ref_price,
        a.est_notional_usdt,
        f.executed_qty,
        f.avg_price,
        f.realized_pnl,
        f.commission,
        CASE
            WHEN a.ref_price IS NULL OR a.ref_price <= 0 OR f.avg_price IS NULL THEN NULL
            WHEN a.action_side = 'BUY' THEN (f.avg_price - a.ref_price) * 10000.0 / a.ref_price
            WHEN a.action_side = 'SELL' THEN (a.ref_price - f.avg_price) * 10000.0 / a.ref_price
            ELSE NULL
        END AS slippage_bps_signed
    FROM rebalance_actions a
    LEFT JOIN fills f
      ON f.order_id = a.order_id
     AND f.symbol = a.symbol
    JOIN params p
      ON a.created_at_utc BETWEEN p.from_utc AND p.to_utc
    WHERE a.status = 'ADJUSTED'
)
SELECT
    COALESCE(action_side, 'NA') AS action_side,
    COUNT(*) AS adjusted_actions,
    SUM(CASE WHEN executed_qty IS NOT NULL THEN 1 ELSE 0 END) AS with_fill_rows,
    ROUND(100.0 * SUM(CASE WHEN executed_qty IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS fill_link_rate_pct,
    ROUND(AVG(slippage_bps_signed), 4) AS avg_slippage_bps_signed,
    ROUND(SUM(COALESCE(realized_pnl, 0.0)), 6) AS sum_realized_pnl,
    ROUND(SUM(COALESCE(commission, 0.0)), 6) AS sum_commission
FROM action_fill
GROUP BY COALESCE(action_side, 'NA')
ORDER BY action_side;

-- ============================================================
-- Q6) Age bucket effect (older positions vs newer positions)
-- This helps evaluate if older positions should be down-weighted more aggressively.
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
),
base AS (
    SELECT
        a.id AS action_id,
        a.action_side,
        a.symbol,
        a.est_notional_usdt,
        a.created_at_utc AS action_time_utc,
        p.opened_at_utc,
        p.entry_price,
        f.executed_qty,
        f.avg_price,
        f.realized_pnl,
        f.commission,
        (julianday(a.created_at_utc) - julianday(p.opened_at_utc)) * 24.0 AS age_hours,
        CASE
            WHEN f.realized_pnl IS NOT NULL THEN f.realized_pnl
            WHEN a.action_side = 'BUY'
             AND p.entry_price IS NOT NULL
             AND p.entry_price > 0
             AND f.avg_price IS NOT NULL
             AND f.executed_qty IS NOT NULL
            THEN (p.entry_price - f.avg_price) * f.executed_qty
            ELSE NULL
        END AS realized_pnl_proxy
    FROM rebalance_actions a
    JOIN positions p
      ON p.id = a.position_id
    LEFT JOIN fills f
      ON f.order_id = a.order_id
     AND f.symbol = a.symbol
    JOIN params x
      ON a.created_at_utc BETWEEN x.from_utc AND x.to_utc
    WHERE a.status = 'ADJUSTED'
),
bucketed AS (
    SELECT
        action_id,
        action_side,
        symbol,
        est_notional_usdt,
        COALESCE(age_hours, 0.0) AS age_hours,
        CASE
            WHEN COALESCE(age_hours, 0.0) < 12 THEN '00_<12h'
            WHEN COALESCE(age_hours, 0.0) < 24 THEN '01_12-24h'
            WHEN COALESCE(age_hours, 0.0) < 48 THEN '02_24-48h'
            ELSE '03_>=48h'
        END AS age_bucket,
        realized_pnl_proxy,
        commission
    FROM base
)
SELECT
    age_bucket,
    action_side,
    COUNT(*) AS actions,
    ROUND(AVG(est_notional_usdt), 6) AS avg_est_notional,
    ROUND(AVG(COALESCE(realized_pnl_proxy, 0.0)), 6) AS avg_realized_pnl_proxy,
    ROUND(SUM(COALESCE(realized_pnl_proxy, 0.0)), 6) AS sum_realized_pnl_proxy,
    ROUND(SUM(COALESCE(commission, 0.0)), 6) AS sum_commission
FROM bucketed
GROUP BY age_bucket, action_side
ORDER BY age_bucket, action_side;

-- ============================================================
-- Q7) Mode comparison (equal_risk vs age_decay)
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
),
cycle_cost AS (
    SELECT
        c.id AS cycle_id,
        c.mode,
        c.reason_tag,
        c.started_at_utc,
        c.planned_count,
        c.adjusted_count,
        c.error_count,
        c.reduced_notional_usdt,
        c.added_notional_usdt,
        SUM(COALESCE(f.realized_pnl, 0.0)) AS realized_pnl_sum,
        SUM(COALESCE(f.commission, 0.0)) AS commission_sum
    FROM rebalance_cycles c
    LEFT JOIN rebalance_actions a
      ON a.cycle_id = c.id
     AND a.status = 'ADJUSTED'
    LEFT JOIN fills f
      ON f.order_id = a.order_id
     AND f.symbol = a.symbol
    JOIN params p
      ON c.started_at_utc BETWEEN p.from_utc AND p.to_utc
    GROUP BY
        c.id, c.mode, c.reason_tag, c.started_at_utc,
        c.planned_count, c.adjusted_count, c.error_count,
        c.reduced_notional_usdt, c.added_notional_usdt
)
SELECT
    mode,
    reason_tag,
    COUNT(*) AS cycles,
    ROUND(AVG(planned_count), 4) AS avg_planned,
    ROUND(AVG(adjusted_count), 4) AS avg_adjusted,
    ROUND(AVG(error_count), 4) AS avg_errors,
    ROUND(SUM(realized_pnl_sum), 6) AS realized_pnl_sum,
    ROUND(SUM(commission_sum), 6) AS commission_sum,
    ROUND(SUM(realized_pnl_sum) - SUM(commission_sum), 6) AS net_pnl_after_fee_proxy,
    ROUND(AVG(reduced_notional_usdt + added_notional_usdt), 6) AS avg_turnover
FROM cycle_cost
GROUP BY mode, reason_tag
ORDER BY mode, reason_tag;

-- ============================================================
-- Q8) Symbol ranking under rebalance actions
-- Helps identify symbols where rebalancing tends to be expensive or ineffective.
-- ============================================================
WITH params AS (
    SELECT
        '2026-01-01T00:00:00+00:00' AS from_utc,
        '2026-12-31T23:59:59+00:00' AS to_utc
),
base AS (
    SELECT
        a.symbol,
        a.status,
        a.action_side,
        a.est_notional_usdt,
        ABS(COALESCE(a.deviation_notional_usdt, 0.0)) AS abs_deviation,
        COALESCE(f.realized_pnl, 0.0) AS realized_pnl,
        COALESCE(f.commission, 0.0) AS commission
    FROM rebalance_actions a
    LEFT JOIN fills f
      ON f.order_id = a.order_id
     AND f.symbol = a.symbol
    JOIN params p
      ON a.created_at_utc BETWEEN p.from_utc AND p.to_utc
)
SELECT
    symbol,
    COUNT(*) AS action_count,
    SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped_count,
    SUM(CASE WHEN status = 'ADJUSTED' THEN 1 ELSE 0 END) AS adjusted_count,
    ROUND(AVG(est_notional_usdt), 6) AS avg_est_notional,
    ROUND(AVG(abs_deviation), 6) AS avg_abs_deviation,
    ROUND(SUM(realized_pnl), 6) AS sum_realized_pnl,
    ROUND(SUM(commission), 6) AS sum_commission,
    ROUND(SUM(realized_pnl) - SUM(commission), 6) AS net_pnl_after_fee_proxy
FROM base
GROUP BY symbol
HAVING COUNT(*) >= 5
ORDER BY net_pnl_after_fee_proxy ASC, action_count DESC
LIMIT 100;

-- ============================================================
-- Q9) Training dataset export (row-level sample)
-- Optional: create a reusable view for offline model / optimizer.
-- ============================================================
DROP VIEW IF EXISTS v_rebalance_training_samples;
CREATE VIEW v_rebalance_training_samples AS
SELECT
    c.id AS cycle_id,
    c.run_id,
    c.reason_tag,
    c.mode,
    c.reduce_only,
    c.target_count,
    c.open_positions,
    c.virtual_slots,
    c.equity_usdt,
    c.target_notional_per_position_usdt,
    c.started_at_utc AS cycle_time_utc,
    a.id AS action_id,
    a.position_id,
    a.symbol,
    a.action_side,
    a.status AS action_status,
    a.ref_price,
    a.current_notional_usdt,
    a.target_notional_usdt,
    a.deviation_notional_usdt,
    a.deadband_notional_usdt,
    a.max_adjust_notional_usdt,
    a.requested_adjust_notional_usdt,
    a.qty,
    a.est_notional_usdt,
    p.opened_at_utc,
    p.entry_price,
    (julianday(a.created_at_utc) - julianday(p.opened_at_utc)) * 24.0 AS age_hours,
    f.event_time_utc AS fill_time_utc,
    f.executed_qty,
    f.avg_price,
    f.realized_pnl,
    f.commission,
    CASE
        WHEN f.realized_pnl IS NOT NULL THEN f.realized_pnl
        WHEN a.action_side = 'BUY'
         AND p.entry_price IS NOT NULL
         AND p.entry_price > 0
         AND f.avg_price IS NOT NULL
         AND f.executed_qty IS NOT NULL
        THEN (p.entry_price - f.avg_price) * f.executed_qty
        ELSE NULL
    END AS realized_pnl_proxy,
    CASE
        WHEN a.ref_price IS NULL OR a.ref_price <= 0 OR f.avg_price IS NULL THEN NULL
        WHEN a.action_side = 'BUY' THEN (f.avg_price - a.ref_price) * 10000.0 / a.ref_price
        WHEN a.action_side = 'SELL' THEN (a.ref_price - f.avg_price) * 10000.0 / a.ref_price
        ELSE NULL
    END AS slippage_bps_signed
FROM rebalance_cycles c
JOIN rebalance_actions a
  ON a.cycle_id = c.id
LEFT JOIN positions p
  ON p.id = a.position_id
LEFT JOIN fills f
  ON f.order_id = a.order_id
 AND f.symbol = a.symbol;

-- Example:
-- SELECT * FROM v_rebalance_training_samples
-- WHERE cycle_time_utc >= '2026-01-01T00:00:00+00:00'
-- ORDER BY cycle_time_utc DESC, action_id DESC
-- LIMIT 500;
