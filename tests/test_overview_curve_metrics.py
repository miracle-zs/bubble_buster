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

// Carried-forward single point yields realPointCount === 0 (prevents synthetic crash ramp)
const carried = ops.computeCycleRiskMetrics([{{ t: '2026-10-09T00:01:40Z', equity: 276.56, carried_forward: true }}], 'full', stop, cfg);
assert.equal(carried.realPointCount, 0);
assert.equal(result.realPointCount, 1);

// Wallet source localization
assert.equal(ops.formatWalletSource('DB'), '本地快照');
assert.equal(ops.formatWalletSource('API'), '交易所实时');
assert.equal(ops.formatWalletSource(''), '--');

// Tone classifiers: managed small dip stays neutral/ok while readonly negative is bad
assert.equal(ops.classifyReturnTone(-0.1, 3.5, 'MONITORING'), '');
assert.equal(ops.classifyReturnTone(-1.5, 3.5, 'MONITORING'), 'warn');
assert.equal(ops.classifyReturnTone(-2.6, 3.5, 'MONITORING'), 'bad');
assert.equal(ops.classifyReturnTone(-0.1, null, ''), 'bad');
assert.equal(ops.classifyDrawdownTone(-0.2, 3.5), 'ok');
assert.equal(ops.classifyDrawdownTone(-1.4, 3.5), 'warn');
assert.equal(ops.classifyDrawdownTone(-2.6, 3.5), 'bad');

// Pure overview_render.js module tests
require({str((Path(__file__).resolve().parents[1] / "app_static/overview_render.js").resolve())!r});
const ov = globalThis.BBOverviewRender;
assert.equal(ov.taskStatusCls('READONLY'), 'is-readonly');
assert.equal(ov.worstAccountTaskStatus({{ tasks: {{ entry: {{ status: 'SUCCESS' }}, manage: {{ status: 'FAILED' }} }} }}), 'FAILED');
assert.equal(ov.anomalyTaskCount([{{ tasks: {{ a: {{ status: 'FAILED' }}, b: {{ status: 'PARTIAL' }}, c: {{ status: 'SUCCESS' }} }} }}]), 2);
const parsedTask = ov.formatTaskResultLines('entry', {{ summary: 'opened=3 failed=1 skipped=2 failed_symbols=BTCUSDT,ETHUSDT' }});
assert.equal(parsedTask.statLine, '开仓 3 · 失败 1 · 跳过 2');
assert.equal(parsedTask.symbolLines.length, 1);
assert.deepEqual(parsedTask.symbolLines[0].symbols, ['BTCUSDT', 'ETHUSDT']);
const tl = ov.buildCommonEntryTimeline([
  {{ entry_actions: [{{ entry_stage: 'INITIAL', symbol: 'BTCUSDT', opened_at_local: '2026-09-05 08:00:10' }}] }},
  {{ entry_actions: [{{ entry_stage: 'INITIAL', symbol: 'BTCUSDT', opened_at_local: '2026-09-05 08:00:55' }}] }}
]);
assert.equal(tl.commonActionCount, 1);
assert.equal(tl.groups[0].time, '08:00');
assert.equal(ov.computeVenueState(true, {{}}).cls, 'status-bad');

// Pure dashboard_render.js module tests
require({str((Path(__file__).resolve().parents[1] / "app_static/dashboard_render.js").resolve())!r});
const dr = globalThis.BBDashboardRender;
assert.equal(dr.fmtAdaptive(123.456), '123.46');
assert.equal(dr.fmtAdaptive(0.00123456), '0.00123456');
assert.equal(dr.summarizeRunMessage('All symbols already have open strategy positions'), '目标交易对均已有策略持仓（已跳过）');
assert.equal(dr.summarizeRunMessage('opened=10,failed=0,planned=2'), '开仓 10 · 失败 0 · 计划 2');
assert.equal(dr.liveOrderLabel('ACTIVE'), '挂单中');
const opt = dr.buildEquityChartOption({{
  seriesData: [[Date.parse('2026-09-05T08:23:51Z'), 275.4, 0, 0]],
  markLines: [{{ yAxis: 285.4 }}, {{ yAxis: 275.41 }}],
  timeZone: 'Asia/Shanghai'
}});
assert.equal(opt.yAxis.min, 272);
assert.equal(opt.yAxis.max, 289);
assert.equal(opt.dataZoom.length, 0);
assert.equal(opt.xAxis.axisLabel.fontFamily, ops.getChartTheme().fontMono);
assert.ok(!dr.cashflowRowHtml({{ amount: 0, income_type: 'FUNDING_FEE', asset: 'USDT', symbol: 'BTCUSDT', id: 1, event_time_utc: '2026-10-09T00:00:00Z' }}, 'Asia/Shanghai').includes('numeric warn'));
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
