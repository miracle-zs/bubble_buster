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

// Base prefix resolution (including /account/legacy/)
assert.equal(ops.resolveBasePrefix('/account/legacy/'), '');
assert.equal(ops.resolveBasePrefix('/proxy/account/legacy/'), '/proxy');
assert.equal(ops.resolveBasePrefix('/proxy/legacy/'), '/proxy');
assert.equal(ops.resolveBasePrefix('/proxy/accounts/comparison/'), '/proxy');

// Pure overview_render.js module tests
require({str((Path(__file__).resolve().parents[1] / "app_static/overview_render.js").resolve())!r});
const ov = globalThis.BBOverviewRender;
assert.equal(ov.fmt('N/A<tag>', 2), 'N/A<tag>');
assert.equal(ov.taskStatusCls('READONLY'), 'is-readonly');
const rowMarkup = ov.taskRowHtml('acc01', 'manage', '仓位巡检', {{ status: 'FAILED', summary: 'error=boom ' + 'x'.repeat(120) }});
assert.ok(rowMarkup.includes('class="task-row is-anomaly-row" role="none"'));
assert.ok(rowMarkup.includes('class="task-row-main" role="row"'));
assert.ok(!rowMarkup.includes('<span class="task-result"'));
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
assert.deepEqual(ops.parseSummaryPairs('opened=10,failed=0,planned=2'), {{ opened: '10', failed: '0', planned: '2' }});
const epView = ov.buildEntryProgressHtml([{{
  account_id: 'acc01',
  mode: 'full',
  entry_progress: {{
    is_today: true,
    status: 'IN_PROGRESS',
    target_count: 10,
    opened_count: 4,
    entry_action_target_count: 10,
    entry_action_completed_count: 4,
    entry_action_initial_count: 3,
    entry_action_scale_in_count: 1,
    entry_action_failed_count: 1,
    entry_action_skipped_count: 1,
    entry_action_pending_count: 4,
    waiting_count: 4,
    next_check_local: '2026-09-05 09:00:00',
    updated_at_local: '2026-09-05 08:30:00'
  }}
}}], {{ entryDetailsUserOverride: null }});
assert.equal(epView.waitingTotal, 4);
assert.equal(epView.showDetails, true);
assert.ok(epView.html.includes('entry-progress-summary-meter'));
assert.ok(epView.html.includes('entry-progress-meter-initial'));
assert.ok(epView.html.includes('entry-progress-meter-skipped'));
assert.ok(!epView.html.includes('summary-meter-'));
const tbHtml = ov.buildTaskBoardHtml([{{ account_id: 'acc01', mode: 'full', tasks: {{ entry: {{ status: 'FAILED' }} }} }}], 'anomaly');
assert.ok(tbHtml.includes('class="task-account-card"'));
assert.ok(!tbHtml.includes('task-account-anomaly'));
assert.ok(ov.buildTaskBoardHtml([], 'anomaly').includes('暂无异常'));
assert.equal(dr.liveOrderLabel('ACTIVE'), '挂单中');
const runMarkup = dr.runRowHtml({{ id: 42, trade_day_utc: '2026-09-05', status: 'SUCCESS', started_at_utc: '2026-09-05T08:00:00Z', message: 'opened=1' }}, 'Asia/Shanghai');
assert.ok(runMarkup.includes('data-detail-key="42_2026-09-05_2026-09-05T08:00:00Z"'));
assert.ok(runMarkup.includes('<summary>原始记录（2026-09-05）</summary>'));
const opt = dr.buildEquityChartOption({{
  seriesData: [[Date.parse('2026-09-05T08:23:51Z'), 275.4, 0, 0]],
  markLines: [{{ yAxis: 285.4 }}, {{ yAxis: 275.41 }}],
  timeZone: 'Asia/Shanghai'
}});
assert.equal(opt.yAxis.min, 272);
assert.equal(opt.yAxis.max, 289);
assert.equal(opt.dataZoom.length, 0);
assert.equal(opt.grid.containLabel, true);
assert.equal(opt.xAxis.axisLabel.fontFamily, ops.getChartTheme().fontMono);
const pts30 = Array.from({{ length: 30 }}, (_, i) => [Date.parse('2026-09-05T08:00:00Z') + i * 60000, 275 + i * 0.1, 0, 0]);
const zoomedOpt = dr.buildEquityChartOption({{ seriesData: pts30, zoomStart: 25, zoomEnd: 75, timeZone: 'Asia/Shanghai' }});
assert.equal(zoomedOpt.dataZoom[0].start, 25);
assert.equal(zoomedOpt.dataZoom[0].end, 75);
assert.ok(!dr.cashflowRowHtml({{ amount: 0, income_type: 'FUNDING_FEE', asset: 'USDT', symbol: 'BTCUSDT', id: 1, event_time_utc: '2026-10-09T00:00:00Z' }}, 'Asia/Shanghai').includes('numeric warn'));

// Fixes verification
assert.equal(ops.formatBeijingTime('2026-10-08T16:00:00Z', {{ withYear: true, withSeconds: true }}), '2026-10-09 00:00:00');
assert.equal(ops.parseTimestampMs('2026-10-09T08:00:00'), Date.parse('2026-10-09T08:00:00+08:00'));
const synthStop = {{ baseline_equity: 100, baseline_captured_at_utc: '2026-10-09T00:00:00Z', current_equity: 105, updated_at_utc: '2026-10-09T00:05:00Z', loss_pct: 3.5 }};
const synthRes = ops.computeCycleRiskMetrics([{{ t: '2026-10-09T00:01:00Z', equity: 101 }}], 'full', synthStop, cfg);
assert.equal(synthRes.points[synthRes.points.length - 1].t, '2026-10-09T00:05:00Z');
assert.equal(dr.trimFixed(-1e-9, 2), '0');
assert.ok(!dr.readonlyStatsFactsHtml({{ win_rate_pct: 'bad', profit_factor: NaN, total_trades: null }}).includes('NaN'));
const negOpt = dr.buildEquityChartOption({{ seriesData: [[Date.parse('2026-09-05T08:00:00Z'), -50, -50, -10]], timeZone: 'Asia/Shanghai' }});
assert.ok(negOpt.yAxis.min < -50);
assert.equal(dr.clsForStatus('RUNNING'), 'ok');
assert.equal(dr.liveOrderClass('FILLED'), 'ok');
const readonlyRow = ov.taskRowsForAccount({{ account_id: 'readonly01', mode: 'readonly', trade_stats: null }});
assert.ok(readonlyRow.includes('class="task-result" role="cell"'));

// Pure comparison_render.js module tests
require({str((Path(__file__).resolve().parents[1] / "app_static/comparison_render.js").resolve())!r});
const cr = globalThis.BBComparisonRender;
assert.ok(Math.abs(cr.computeRowMaxDrawdownPct({{ points: [{{ equity: 100 }}, {{ equity: 120 }}, {{ equity: 90 }}, {{ equity: 110 }}] }}) - (-25)) < 1e-8);
assert.equal(cr.computeRowMaxDrawdownPct({{ points: [{{ equity: 0 }}, {{ equity: null }}] }}), null);
const cardHtml = cr.buildSummaryCardHtml(
  {{ account_id: 'acc<1>', status: 'OK', change_pct: 2.5, latest_equity: 1234.5, min_change_pct: -1.1, max_change_pct: 3.2 }},
  0,
  {{ isReadonly: true, pathPrefix: '/proxy', note: 'TP 9%', allNotes: ['TP 9%'] }}
);
assert.ok(cardHtml.includes('summary-card is-readonly'));
assert.ok(cardHtml.includes('acc&lt;1&gt;'));
assert.ok(cardHtml.includes('href="/proxy/account/acc%3C1%3E/"'));
const legendHtml = cr.buildLegendHtml(
  [
    {{ account_id: 'acc01', status: 'OK', points: [{{ t: '2026-10-09T00:00:00Z', equity: 100 }}], change_pct: 1.5 }},
    {{ account_id: 'acc02', status: 'NO_DATA', points: [], change_pct: null }}
  ],
  {{ visible: {{ acc01: true }}, isReadonlyFn: (id) => id === 'acc02' }}
);
assert.ok(legendHtml.includes('data-account="acc01" aria-pressed="true"'));
assert.ok(legendHtml.includes('legend-button is-muted is-no-data is-readonly'));
assert.ok(legendHtml.includes('disabled'));
const sorted = cr.sortAndEnrichComparisonRows(
  [
    {{ account_id: 'acc02', change_pct: null, latest_equity: 100, points: [] }},
    {{ account_id: 'acc01', change_pct: 1.2, latest_equity: 200, points: [{{ equity: 100 }}, {{ equity: 98 }}] }},
    {{ account_id: 'acc03', change_pct: 3.4, latest_equity: 300, points: [{{ equity: 100 }}, {{ equity: 97 }}] }}
  ],
  {{ key: 'change_pct', dir: 'desc' }}
);
assert.deepEqual(sorted.map((item) => item.account_id), ['acc03', 'acc01', 'acc02']);
const metricsRow = cr.buildMetricsRowHtml(sorted[2], {{ isReadonly: true, pathPrefix: '' }});
assert.ok(metricsRow.includes('class="is-readonly"'));
assert.ok(metricsRow.includes('未配置'));
const cmpOpt = cr.buildComparisonChartOption({{
  activeRows: [{{
    account_id: 'acc01',
    status: 'OK',
    points: [
      {{ t: '2026-10-09T00:00:00Z', relative_pct: 0, equity: 100 }},
      {{ t: '2026-10-09T00:05:00Z', relative_pct: null, equity: 100 }}
    ]
  }}],
  mode: 'relative',
  range: 'all',
  animate: false,
  isReadonlyFn: () => false
}});
assert.equal(cmpOpt.series[0].data[1][1], null);
assert.equal(cmpOpt.dataZoom.length, 0);

// Shared ops.js utilities: status labels, sorting, timestamp selection, ECharts chrome
assert.equal(ops.formatStatusLabel('CLOSED_TP'), '止盈平仓');
assert.equal(ops.formatStatusLabel('', '--'), '--');
assert.equal(ops.statusTone('READONLY'), 'readonly');
assert.equal(ops.statusTone('PARTIAL'), 'warn');
assert.equal(ops.compareNullableNumbers(null, 5, true), 1);
assert.equal(ops.compareNullableNumbers(10, 5, true), -5);
assert.equal(ops.pickLatestTimestamp(['2026-10-09T00:00:00Z', '2026-10-09T02:00:00Z', '2026-10-09T01:00:00Z']), '2026-10-09T02:00:00Z');
const chrome = ops.buildEchartsChrome(ops.getChartTheme(), {{ showSlider: true, zoomStart: 10, zoomEnd: 90, xSplitLine: true }});
assert.equal(chrome.dataZoom.length, 1);
assert.equal(chrome.dataZoom[0].start, 10);
assert.equal(chrome.xAxis.splitLine.show, true);
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)

    @unittest.skipUnless(shutil.which("node"), "Node.js required for browser DOM reconciliation test")
    def test_keyed_reconciliation_and_reorder_preserve_nodes_and_state(self):
        ops_path = (Path(__file__).resolve().parents[1] / "app_static/ops.js").resolve()
        script = f"""
const assert = require('node:assert/strict');

// Minimal DOM stub for testing ops.syncPressedButtons, reorderChildrenByKey, and reconcileKeyedHtml
class FakeClassList {{
  constructor(el) {{ this.el = el; }}
  toggle(cls, force) {{
    const set = new Set((this.el.className || '').split(/\\s+/).filter(Boolean));
    const has = set.has(cls);
    const next = force === undefined ? !has : !!force;
    if (next) set.add(cls); else set.delete(cls);
    this.el.className = Array.from(set).join(' ');
    return next;
  }}
}}

class FakeElement {{
  constructor(tagName) {{
    this.tagName = String(tagName || 'DIV').toUpperCase();
    this.children = [];
    this.parentNode = null;
    this.attrs = {{}};
    this.className = '';
    this.classList = new FakeClassList(this);
    this._innerHTML = '';
    this.moveBeforeCalls = 0;
  }}
  setAttribute(k, v) {{ this.attrs[k] = String(v); }}
  getAttribute(k) {{ return Object.prototype.hasOwnProperty.call(this.attrs, k) ? this.attrs[k] : null; }}
  removeAttribute(k) {{ delete this.attrs[k]; }}
  appendChild(child) {{
    if (child.parentNode) child.parentNode.removeChild(child);
    child.parentNode = this;
    this.children.push(child);
    return child;
  }}
  removeChild(child) {{
    const idx = this.children.indexOf(child);
    if (idx >= 0) {{
      this.children.splice(idx, 1);
      child.parentNode = null;
    }}
    return child;
  }}
  insertBefore(node, ref) {{
    if (node.parentNode) node.parentNode.removeChild(node);
    node.parentNode = this;
    if (!ref) {{
      this.children.push(node);
      return node;
    }}
    const idx = this.children.indexOf(ref);
    if (idx < 0) this.children.push(node);
    else this.children.splice(idx, 0, node);
    return node;
  }}
  moveBefore(node, ref) {{
    this.moveBeforeCalls += 1;
    return this.insertBefore(node, ref);
  }}
  replaceChild(next, old) {{
    const idx = this.children.indexOf(old);
    if (idx < 0) return null;
    old.parentNode = null;
    next.parentNode = this;
    this.children[idx] = next;
    return old;
  }}
  contains(node) {{
    let cur = node;
    while (cur) {{
      if (cur === this) return true;
      cur = cur.parentNode;
    }}
    return false;
  }}
  querySelectorAll(selector) {{
    const out = [];
    const wantDetailsOpen = selector.includes('details[open]');
    const mAttr = selector.match(/\\[([a-zA-Z0-9_-]+)\\]$/);
    const attrName = mAttr ? mAttr[1] : null;
    const walk = (el) => {{
      for (const ch of el.children) {{
        if (selector.startsWith('details') && ch.tagName === 'DETAILS') {{
          if ((!wantDetailsOpen || Object.prototype.hasOwnProperty.call(ch.attrs, 'open'))
              && (!attrName || ch.getAttribute(attrName) != null)) {{
            out.push(ch);
          }}
        }} else if (!selector.startsWith('details') && attrName && ch.getAttribute(attrName) != null) {{
          out.push(ch);
        }}
        walk(ch);
      }}
    }};
    walk(this);
    return out;
  }}
  set innerHTML(html) {{
    this._innerHTML = html;
    this.children = [];
    const trMatches = html.match(/<tr[^>]*>.*?<\\/tr>/g) || [];
    for (const chunk of trMatches) {{
      const tr = new FakeElement('tr');
      tr._raw = chunk;
      const detMatch = chunk.match(/<details([^>]*)>/);
      if (detMatch) {{
        const det = new FakeElement('details');
        const dk = detMatch[1].match(/data-detail-key="([^"]+)"/);
        if (dk) det.setAttribute('data-detail-key', dk[1]);
        if (/\\bopen\\b/.test(detMatch[1])) det.setAttribute('open', '');
        tr.appendChild(det);
      }}
      this.appendChild(tr);
    }}
  }}
  get innerHTML() {{ return this._innerHTML; }}
}}

globalThis.document = {{
  activeElement: null,
  createElement(tag) {{
    if (tag === 'template') {{
      const holder = new FakeElement('template');
      return {{
        content: {{
          get firstElementChild() {{ return holder.children[0] || null; }}
        }},
        set innerHTML(v) {{ holder.innerHTML = v; }}
      }};
    }}
    return new FakeElement(tag);
  }}
}};

require({str(ops_path)!r});
const ops = globalThis.BBOps;

// 1. syncPressedButtons
const b1 = new FakeElement('button');
const b2 = new FakeElement('button');
ops.syncPressedButtons([b1, b2], (btn) => btn === b2);
assert.equal(b1.className, '');
assert.equal(b1.getAttribute('aria-pressed'), 'false');
assert.equal(b2.className, 'active');
assert.equal(b2.getAttribute('aria-pressed'), 'true');

// 2. reconcileKeyedHtml: initial render, no-op short-circuit, single-row patch, open-details preservation, moveBefore reorder
const tbody = new FakeElement('tbody');
const rowA1 = '<tr><td>A</td><td>10<details data-detail-key="dA"></details></td></tr>';
const rowB1 = '<tr><td>B</td><td>20<details data-detail-key="dB"></details></td></tr>';
assert.equal(ops.reconcileKeyedHtml(tbody, [{{ key: 'A', html: rowA1 }}, {{ key: 'B', html: rowB1 }}], {{ keyAttr: 'data-row-key', detailAttr: 'data-detail-key' }}), true);
const origA = tbody.children[0];
const origB = tbody.children[1];
assert.equal(origA.getAttribute('data-row-key'), 'A');
assert.equal(origB.getAttribute('data-row-key'), 'B');

// Exact same items => no-op
assert.equal(ops.reconcileKeyedHtml(tbody, [{{ key: 'A', html: rowA1 }}, {{ key: 'B', html: rowB1 }}], {{ keyAttr: 'data-row-key', detailAttr: 'data-detail-key' }}), false);
assert.strictEqual(tbody.children[0], origA);
assert.strictEqual(tbody.children[1], origB);

// User opens details in row A; row A updates, row B stays unchanged (same node!)
origA.children[0].setAttribute('open', '');
const rowA2 = '<tr><td>A</td><td>15<details data-detail-key="dA"></details></td></tr>';
assert.equal(ops.reconcileKeyedHtml(tbody, [{{ key: 'A', html: rowA2 }}, {{ key: 'B', html: rowB1 }}], {{ keyAttr: 'data-row-key', detailAttr: 'data-detail-key' }}), true);
assert.notStrictEqual(tbody.children[0], origA);
assert.strictEqual(tbody.children[1], origB);
assert.equal(tbody.children[0].children[0].getAttribute('open'), '');

// Reorder B before A uses moveBefore and keeps both node identities
const nodeA = tbody.children[0];
const nodeB = tbody.children[1];
assert.equal(ops.reorderChildrenByKey(tbody, 'data-row-key', ['B', 'A']), true);
assert.strictEqual(tbody.children[0], nodeB);
assert.strictEqual(tbody.children[1], nodeA);
assert.ok(tbody.moveBeforeCalls >= 1);

// Empty state resets keyed state cleanly
assert.equal(ops.reconcileKeyedHtml(tbody, [], {{ keyAttr: 'data-row-key', emptyHtml: '<tr><td colspan="2">empty</td></tr>' }}), true);
assert.equal(tbody.children.length, 1);
assert.equal(tbody.children[0].getAttribute('data-row-key'), null);
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
