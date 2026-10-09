"""Run the actual overview calculation, not a Python copy of its formulas."""
import shutil
import subprocess
import unittest
from pathlib import Path


class OverviewCurveMetricsTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js required for browser calculation test")
    def test_risk_baseline_and_peak_drawdown(self):
        source = (Path(__file__).resolve().parents[1] / "templates/accounts_overview.html").read_text()
        functions = source[source.index("  function cycleStartMs("):source.index("  function chartGeometry(")]
        script = "var portfolioStopHour=8, portfolioStopMinute=0, portfolioStopPct=3.5;\n" + functions + """
const assert = require('node:assert/strict');
const stop = {baseline_equity:276.70522059, baseline_captured_at_utc:'2026-10-09T00:00:00Z',
              current_equity:276.5609, loss_pct:3.5};
const result = curveMetrics([{t:'2026-10-09T00:01:40Z', equity:276.9200458}], 'full', stop);
assert.ok(Math.abs(result.currentReturnPct - (-0.0521568005)) < 1e-8);
assert.equal(result.values[0], 0);
assert.ok(Math.abs(result.stopDistancePct - 3.4478431995) < 1e-8);
const peak = curveMetrics([{t:'2026-10-09T00:01:00Z', equity:120}], 'full',
                         {...stop, baseline_equity:100, current_equity:110});
assert.ok(Math.abs(peak.currentDrawdownPct - (-100/12)) < 1e-8);
assert.equal(curveMetrics([{t:'2026-10-09T00:01:00Z', equity:120}], 'full', null), null);
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
