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
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
