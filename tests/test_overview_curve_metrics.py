"""Run the actual overview calculation in app_static/ops.js, not a Python copy of its formulas."""
import shutil
import subprocess
import unittest
from pathlib import Path


class OverviewCurveMetricsTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js required for browser calculation test")
    def test_risk_baseline_and_peak_drawdown(self):
        ops_path = (Path(__file__).resolve().parents[1] / "app_static/ops.js").resolve()
        script = f"""
const assert = require('node:assert/strict');
require({str(ops_path)!r});
const ops = globalThis.BBOps;
const cfg = {{ stopHour: 8, stopMinute: 0, stopPct: 3.5 }};
const stop = {{ baseline_equity: 276.70522059, baseline_captured_at_utc: '2026-10-09T00:00:00Z',
               current_equity: 276.5609, loss_pct: 3.5 }};
const result = ops.computeCycleRiskMetrics([{{ t: '2026-10-09T00:01:40Z', equity: 276.9200458 }}], 'full', stop, cfg);
assert.ok(Math.abs(result.currentReturnPct - (-0.0521568005)) < 1e-8);
assert.equal(result.values[0], 0);
assert.ok(Math.abs(result.stopDistancePct - 3.4478431995) < 1e-8);
const peak = ops.computeCycleRiskMetrics([{{ t: '2026-10-09T00:01:00Z', equity: 120 }}], 'full',
                                         {{ ...stop, baseline_equity: 100, current_equity: 110 }}, cfg);
assert.ok(Math.abs(peak.currentDrawdownPct - (-100 / 12)) < 1e-8);
assert.equal(ops.computeCycleRiskMetrics([{{ t: '2026-10-09T00:01:00Z', equity: 120 }}], 'full', null, cfg), null);
const parsed = ops.parseSummaryPairs('reason=failed error=Binance API Error 0: Service unavailable');
assert.equal(parsed.reason, 'failed');
assert.equal(parsed.error, 'Binance API Error 0: Service unavailable');
assert.deepEqual(ops.parseSummaryPairs('opened=12 failed=0 skipped=0'), {{ opened: '12', failed: '0', skipped: '0' }});

// Shared portfolio stop evaluator (single source of truth for overview + detail)
const disabledState = ops.evaluatePortfolioStopState(null, null, {{ enabled: false, fallbackPct: 3.5 }});
assert.equal(disabledState.tone, 'warn');
assert.equal(disabledState.distanceText, '未启用');

const okState = ops.evaluatePortfolioStopState({{ status: 'MONITORING', distance_pct: 2.8, current_return_pct: -0.7, loss_pct: 3.5 }}, null, {{ enabled: true, fallbackPct: 3.5 }});
assert.equal(okState.tone, 'ok');
assert.equal(okState.stateText, '风险正常');
assert.equal(okState.consumedPct, 20);

const nearState = ops.evaluatePortfolioStopState({{ status: 'MONITORING', distance_pct: 0.6, current_return_pct: -2.9, loss_pct: 3.5 }}, null, {{ enabled: true, fallbackPct: 3.5 }});
assert.equal(nearState.tone, 'warn');
assert.equal(nearState.stateText, '接近组合止损');

const triggeredState = ops.evaluatePortfolioStopState({{ status: 'TRIGGERED_COMPLETE', distance_pct: -0.2, current_return_pct: -3.7, loss_pct: 3.5 }}, null, {{ enabled: true, fallbackPct: 3.5 }});
assert.equal(triggeredState.tone, 'bad');
assert.equal(triggeredState.consumedPct, 100);
assert.equal(triggeredState.distanceText, '已完成');

// Cached theme and date formatting
const theme1 = ops.getChartTheme();
const theme2 = ops.getChartTheme();
assert.strictEqual(theme1, theme2);
assert.equal(ops.formatBeijingTime('2026-10-09T00:01:40Z', {{ withYear: true, withSeconds: true }}), '2026-10-09 08:01:40');
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
