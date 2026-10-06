#!/usr/bin/env python3
"""Build the inline HTML visual for the observed two-entry comparison."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build two-entry equity curve visual")
    parser.add_argument("--input-json", required=True)
    parser.add_argument("--output-html", required=True)
    args = parser.parse_args()

    data = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    embedded = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    embedded = embedded.replace("<", "\\u003c").replace(">", "\\u003e")
    html = f'''<div id="equity-curve-split-vs-original" class="equity-visual">
  <style>
    #equity-curve-split-vs-original {{
      --bg: #07131d;
      --panel: #0b202c;
      --panel-2: #0e2938;
      --line: #1d4c61;
      --text: #e7f3f8;
      --muted: #88a7b5;
      --grid: #153746;
      --actual: #55d5f5;
      --original: #ffb84a;
      --positive: #42df9b;
      --negative: #ff7d7d;
      width: 100%;
      min-height: 640px;
      box-sizing: border-box;
      padding: 20px 22px 24px;
      color: var(--text);
      background: var(--bg);
      border: 1px solid #1d5268;
      border-radius: 18px;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    #equity-curve-split-vs-original * {{ box-sizing: border-box; }}
    #equity-curve-split-vs-original .eyebrow {{
      color: var(--actual);
      font-size: 11px;
      font-weight: 800;
      letter-spacing: .16em;
      text-transform: uppercase;
    }}
    #equity-curve-split-vs-original h2 {{ margin: 5px 0 4px; font-size: clamp(21px, 3vw, 31px); letter-spacing: -.02em; }}
    #equity-curve-split-vs-original .subtitle {{ margin: 0; color: var(--muted); font-size: 12px; line-height: 1.55; }}
    #equity-curve-split-vs-original .toolbar {{
      display: flex; align-items: center; justify-content: space-between; gap: 14px; flex-wrap: wrap;
      margin: 17px 0 13px; padding: 10px 12px; border: 1px solid var(--line); border-radius: 12px; background: rgba(14,41,56,.72);
    }}
    #equity-curve-split-vs-original .legend {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    #equity-curve-split-vs-original button.legend-button {{
      appearance: none; border: 1px solid var(--line); border-radius: 999px; padding: 7px 11px; cursor: pointer;
      color: var(--text); background: #0b202c; font: inherit; font-size: 12px; line-height: 1;
    }}
    #equity-curve-split-vs-original button.legend-button[aria-pressed="true"] {{ background: #143847; border-color: currentColor; }}
    #equity-curve-split-vs-original button.legend-button .dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; background: currentColor; }}
    #equity-curve-split-vs-original .actual-button {{ color: var(--actual); }}
    #equity-curve-split-vs-original .original-button {{ color: var(--original); }}
    #equity-curve-split-vs-original .note {{ color: var(--muted); font-size: 11px; }}
    #equity-curve-split-vs-original .section-title {{ margin: 17px 0 8px; font-size: 13px; font-weight: 750; letter-spacing: .02em; }}
    #equity-curve-split-vs-original .chart-panel {{ border: 1px solid var(--line); border-radius: 14px; background: var(--panel); overflow: hidden; }}
    #equity-curve-split-vs-original .chart-wrap {{ position: relative; width: 100%; }}
    #equity-curve-split-vs-original svg {{ display: block; width: 100%; height: auto; }}
    #equity-curve-split-vs-original .axis path, #equity-curve-split-vs-original .axis line {{ stroke: #2b596b; shape-rendering: crispEdges; }}
    #equity-curve-split-vs-original .axis text {{ fill: var(--muted); font-size: 10px; }}
    #equity-curve-split-vs-original .grid line {{ stroke: var(--grid); stroke-dasharray: 2 4; }}
    #equity-curve-split-vs-original .grid path {{ stroke-width: 0; }}
    #equity-curve-split-vs-original .series-line {{ fill: none; stroke-width: 2.6; stroke-linecap: round; stroke-linejoin: round; }}
    #equity-curve-split-vs-original .actual-line {{ stroke: var(--actual); }}
    #equity-curve-split-vs-original .original-line {{ stroke: var(--original); }}
    #equity-curve-split-vs-original .zero-line {{ stroke: #476775; stroke-dasharray: 4 4; }}
    #equity-curve-split-vs-original .hover-line {{ stroke: #a8d0db; stroke-width: 1; stroke-dasharray: 3 3; opacity: .75; }}
    #equity-curve-split-vs-original .hover-dot {{ stroke: var(--bg); stroke-width: 2; }}
    #equity-curve-split-vs-original .axis-label {{ fill: var(--muted); font-size: 10px; }}
    #equity-curve-split-vs-original .tooltip {{
      position: absolute; pointer-events: none; z-index: 3; min-width: 172px; padding: 9px 10px;
      border: 1px solid #4a7788; border-radius: 9px; background: rgba(5,17,26,.96); box-shadow: 0 8px 26px rgba(0,0,0,.3);
      color: var(--text); font-size: 11px; line-height: 1.55; opacity: 0; transition: opacity .12s ease;
    }}
    #equity-curve-split-vs-original .tooltip strong {{ font-size: 12px; }}
    #equity-curve-split-vs-original .metric-strip {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; padding: 10px 12px 12px; border-top: 1px solid var(--line); }}
    #equity-curve-split-vs-original .metric {{ min-width: 0; }}
    #equity-curve-split-vs-original .metric-label {{ color: var(--muted); font-size: 10px; }}
    #equity-curve-split-vs-original .metric-value {{ margin-top: 2px; font-size: 14px; font-weight: 760; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    #equity-curve-split-vs-original .accounts-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }}
    #equity-curve-split-vs-original .account-panel {{ min-width: 0; }}
    #equity-curve-split-vs-original .account-head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 8px; padding: 10px 12px 0; }}
    #equity-curve-split-vs-original .account-title {{ font-weight: 780; font-size: 13px; }}
    #equity-curve-split-vs-original .account-meta {{ color: var(--muted); font-size: 10px; white-space: nowrap; }}
    #equity-curve-split-vs-original .footer-note {{ margin-top: 13px; color: var(--muted); font-size: 10px; line-height: 1.6; }}
    @media (max-width: 760px) {{
      #equity-curve-split-vs-original {{ padding: 16px 13px 18px; }}
      #equity-curve-split-vs-original .accounts-grid {{ grid-template-columns: 1fr; }}
      #equity-curve-split-vs-original .metric-strip {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 460px) {{
      #equity-curve-split-vs-original {{ border-radius: 12px; }}
      #equity-curve-split-vs-original .toolbar {{ align-items: flex-start; }}
      #equity-curve-split-vs-original .note {{ width: 100%; }}
      #equity-curve-split-vs-original .account-meta {{ display: none; }}
    }}
  </style>
  <div class="eyebrow">Equity attribution / observed exits</div>
  <h2>一次满仓 vs 两次建仓</h2>
  <p class="subtitle">现行两次建仓使用服务器真实钱包余额；原策略线只替换入场分配，沿用同一批真实平仓路径。指数以各图起点 = 100。</p>
  <div class="toolbar">
    <div class="legend" role="group" aria-label="显示或隐藏曲线">
      <button class="legend-button actual-button" data-series="actual" aria-pressed="true"><span class="dot"></span>现行两次建仓</button>
      <button class="legend-button original-button" data-series="original" aria-pressed="true"><span class="dot"></span>原策略一次满仓</button>
    </div>
    <div class="note" id="equity-curve-split-vs-original-range"></div>
  </div>
  <div class="section-title">共同区间 · 四账户平均归一化指数</div>
  <div class="chart-panel">
    <div class="chart-wrap" id="equity-curve-split-vs-original-aggregate-wrap"><svg id="equity-curve-split-vs-original-aggregate" role="img" aria-label="四账户平均归一化钱包权益曲线"></svg><div class="tooltip" id="equity-curve-split-vs-original-aggregate-tip"></div></div>
    <div class="metric-strip" id="equity-curve-split-vs-original-aggregate-metrics"></div>
  </div>
  <div class="section-title">账户明细 · 从各自独立建仓启动时归一化</div>
  <div class="accounts-grid" id="equity-curve-split-vs-original-accounts"></div>
  <div class="footer-note">口径：钱包余额为已实现权益，不含历史未实现浮盈亏；原策略的第二笔入场被移回首笔信号，平仓数量按现行仓位实际关闭比例缩放。因此这是入场分配归因，不代表原策略一定会触发完全相同的止损/止盈。手续费优先采用服务器成交记录，缺失时按双边 0.05% 估算。</div>
</div>
<script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
<script>
(() => {{
  const DATA = {embedded};
  const root = document.getElementById('equity-curve-split-vs-original');
  if (!root || !window.d3) return;
  const d3 = window.d3;
  const colors = {{ actual: '#55d5f5', original: '#ffb84a' }};
  const state = {{ actual: true, original: true }};
  const fmt = d3.format('.2f');
  const fmtPct = value => `${{value >= 0 ? '+' : ''}}${{fmt(value)}}%`;
  const dateFmt = d3.timeFormat('%m/%d %H:%M');
  const localFmt = new Intl.DateTimeFormat('zh-CN', {{ timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }});
  const parseTime = value => new Date(value);
  const localTime = value => localFmt.format(parseTime(value)).replace(',', ' ');
  const numberOrNull = value => value == null ? null : +value;

  const accountRows = Object.entries(DATA.accounts).map(([account, value]) => ({{ account, ...value }}));
  const aggregate = DATA.aggregate.points.map(point => ({{ ...point, date: parseTime(point.t) }}));
  const commonStart = localTime(DATA.meta.common_start_utc);
  const endLocal = localTime(DATA.meta.end_utc);
  document.getElementById('equity-curve-split-vs-original-range').textContent = `北京时间 ${{commonStart}} – ${{endLocal}} · ${{accountRows.length}} 个账户`;

  function visibleKeys() {{ return Object.keys(state).filter(key => state[key]); }}
  function drawAxes(g, x, y, innerWidth, innerHeight, xTicks, yTicks, xFormat) {{
    g.append('g').attr('class', 'grid').attr('transform', `translate(0,${{innerHeight}})`).call(d3.axisBottom(x).tickValues(xTicks).tickSize(-innerHeight).tickFormat(''));
    g.append('g').attr('class', 'grid').call(d3.axisLeft(y).tickValues(yTicks).tickSize(-innerWidth).tickFormat(''));
    g.append('g').attr('class', 'axis').attr('transform', `translate(0,${{innerHeight}})`).call(d3.axisBottom(x).tickValues(xTicks).tickFormat(xFormat));
    g.append('g').attr('class', 'axis').call(d3.axisLeft(y).tickValues(yTicks).tickFormat(d => `${{d}}`));
  }}
  function linePath(data, x, y, key) {{ return d3.line().defined(d => d[key] != null).x(d => x(d.date || d.hours)).y(d => y(+d[key])).curve(d3.curveMonotoneX)(data); }}
  function addTooltip(wrap, svg, data, x, inner, keys, valueForKey, labelForKey) {{
    const tip = wrap.querySelector('.tooltip');
    const bisect = d3.bisector(d => d.date || d.hours).center;
    const overlay = svg.append('rect').attr('x', inner.left).attr('y', inner.top).attr('width', inner.width).attr('height', inner.height).attr('fill', 'transparent').style('cursor', 'crosshair');
    const hover = svg.append('g').style('display', 'none');
    hover.append('line').attr('class', 'hover-line').attr('y1', inner.top).attr('y2', inner.top + inner.height);
    const dots = {{}};
    keys.forEach(key => dots[key] = hover.append('circle').attr('class', 'hover-dot').attr('r', 4).attr('fill', colors[key]));
    overlay.on('mouseenter', () => {{ hover.style('display', null); tip.style.opacity = 1; }})
      .on('mouseleave', () => {{ hover.style('display', 'none'); tip.style.opacity = 0; }})
      .on('mousemove', function(event) {{
        const point = d3.pointer(event, this);
        const domainValue = x.invert(point[0] - inner.left);
        let index = bisect(data, domainValue);
        index = Math.max(0, Math.min(data.length - 1, index));
        const row = data[index];
        const px = x(row.date || row.hours) + inner.left;
        hover.select('line').attr('x1', px).attr('x2', px);
        keys.forEach(key => {{ const value = valueForKey(row, key); dots[key].attr('cx', px).attr('cy', y(value) + inner.top).style('display', value == null ? 'none' : null); }});
        const tooltipRows = keys.map(key => '<span style="color:' + colors[key] + '">●</span> ' + (key === 'actual' ? '现行' : '原策略') + '：' + fmt(valueForKey(row, key))).join('<br>');
        tip.innerHTML = '<strong>' + labelForKey(row) + '</strong><br>' + tooltipRows;
        const left = Math.min(Math.max(8, px + 10), wrap.clientWidth - 190);
        tip.style.left = `${{left}}px`;
        tip.style.top = `${{Math.max(8, inner.top + 8)}}px`;
      }});
  }}
  function metricsHtml(items) {{ return items.map(item => `<div class="metric"><div class="metric-label">${{item.label}}</div><div class="metric-value" style="color:${{item.color || 'var(--text)'}}">${{item.value}}</div></div>`).join(''); }}

  function drawAggregate() {{
    const svg = d3.select('#equity-curve-split-vs-original-aggregate');
    const wrap = document.getElementById('equity-curve-split-vs-original-aggregate-wrap');
    svg.selectAll('*').remove();
    const width = Math.max(300, wrap.clientWidth);
    const height = width < 560 ? 300 : 350;
    svg.attr('viewBox', `0 0 ${{width}} ${{height}}`).attr('height', height);
    const margin = {{ top: 18, right: 18, bottom: 32, left: 45 }};
    const inner = {{ left: margin.left, top: margin.top, width: width - margin.left - margin.right, height: height - margin.top - margin.bottom }};
    const x = d3.scaleTime().domain(d3.extent(aggregate, d => d.date)).range([0, inner.width]);
    const allValues = aggregate.flatMap(d => [d.actual_index, d.original_index]).filter(v => v != null);
    const y = d3.scaleLinear().domain([Math.floor(d3.min(allValues) - 1), Math.ceil(d3.max(allValues) + 1)]).nice().range([inner.height, 0]);
    const g = svg.append('g').attr('transform', `translate(${{inner.left}},${{inner.top}})`);
    const xTicks = x.ticks(width < 560 ? 4 : 7); const yTicks = y.ticks(5);
    drawAxes(g, x, y, inner.width, inner.height, xTicks, yTicks, dateFmt);
    g.append('text').attr('class', 'axis-label').attr('x', 0).attr('y', -5).text('归一化钱包余额指数');
    visibleKeys().forEach(key => g.append('path').datum(aggregate).attr('class', `series-line ${{key}}-line`).attr('d', linePath(aggregate, x, y, `${{key}}_index`)).attr('stroke', colors[key]));
    addTooltip(wrap, svg, aggregate, x, {{ ...inner }}, visibleKeys(), (row, key) => numberOrNull(row[`${{key}}_index`]), row => `${{localTime(row.t)}} · ${{row.accounts}} 账户`);
    const last = aggregate[aggregate.length - 1];
    const actualFinal = last.actual_index; const originalFinal = last.original_index;
    document.getElementById('equity-curve-split-vs-original-aggregate-metrics').innerHTML = metricsHtml([
      {{ label: '现行终点', value: fmt(actualFinal), color: colors.actual }},
      {{ label: '原策略终点', value: fmt(originalFinal), color: colors.original }},
      {{ label: '原策略相对差', value: `${{originalFinal - actualFinal >= 0 ? '+' : ''}}${{fmt(originalFinal - actualFinal)}} 指数点`, color: originalFinal >= actualFinal ? 'var(--positive)' : 'var(--negative)' }},
      {{ label: '参与账户', value: `${{last.accounts}} / ${{accountRows.length}}` }}
    ]);
  }}

  function drawAccount(accountData, index) {{
    const panel = document.createElement('div'); panel.className = 'account-panel chart-panel';
    const metrics = accountData.metrics;
    const deltaColor = metrics.delta_final_usdt >= 0 ? 'var(--positive)' : 'var(--negative)';
    panel.innerHTML = `<div class="account-head"><div class="account-title">${{accountData.account}}</div><div class="account-meta">启动 ${{localTime(accountData.meta.rollout_start_utc)}} · 结束 ${{localTime(DATA.meta.end_utc)}}</div></div><div class="chart-wrap"><svg role="img" aria-label="${{accountData.account}} 一次满仓与两次建仓曲线"></svg><div class="tooltip"></div></div><div class="metric-strip"><div class="metric"><div class="metric-label">现行</div><div class="metric-value" style="color:${{colors.actual}}">${{fmtPct(metrics.actual_return_pct)}}</div></div><div class="metric"><div class="metric-label">原策略</div><div class="metric-value" style="color:${{colors.original}}">${{fmtPct(metrics.original_return_pct)}}</div></div><div class="metric"><div class="metric-label">原−现 USDT</div><div class="metric-value" style="color:${{deltaColor}}">${{metrics.delta_final_usdt >= 0 ? '+' : ''}}${{fmt(metrics.delta_final_usdt)}}</div></div><div class="metric"><div class="metric-label">最大回撤</div><div class="metric-value">${{fmt(metrics.actual_max_drawdown_pct)}}% / ${{fmt(metrics.original_max_drawdown_pct)}}%</div></div></div>`;
    document.getElementById('equity-curve-split-vs-original-accounts').appendChild(panel);
    const wrap = panel.querySelector('.chart-wrap'); const svg = d3.select(panel.querySelector('svg'));
    const points = accountData.points.map(point => ({{ ...point, date: +point.hours }}));
    const width = Math.max(300, wrap.clientWidth); const height = width < 520 ? 235 : 255;
    svg.attr('viewBox', `0 0 ${{width}} ${{height}}`).attr('height', height);
    const margin = {{ top: 14, right: 12, bottom: 29, left: index % 2 === 0 ? 42 : 34 }};
    const inner = {{ left: margin.left, top: margin.top, width: width - margin.left - margin.right, height: height - margin.top - margin.bottom }};
    const x = d3.scaleLinear().domain([0, d3.max(points, d => d.hours)]).range([0, inner.width]);
    const values = points.flatMap(d => [d.actual, d.original]).filter(v => v != null).map(v => v / points[0].actual * 100);
    const y = d3.scaleLinear().domain([Math.floor(d3.min(values) - 1), Math.ceil(d3.max(values) + 1)]).nice().range([inner.height, 0]);
    points.forEach(point => {{ point.actual_index = point.actual / points[0].actual * 100; point.original_index = point.original / points[0].actual * 100; }});
    const g = svg.append('g').attr('transform', `translate(${{inner.left}},${{inner.top}})`);
    const xTicks = x.ticks(width < 520 ? 4 : 6); const yTicks = y.ticks(4);
    drawAxes(g, x, y, inner.width, inner.height, xTicks, yTicks, d => `${{Math.round(d)}}h`);
    if (y.domain()[0] < 100 && y.domain()[1] > 100) g.append('line').attr('class', 'zero-line').attr('x1', 0).attr('x2', inner.width).attr('y1', y(100)).attr('y2', y(100));
    visibleKeys().forEach(key => g.append('path').datum(points).attr('class', `series-line ${{key}}-line`).attr('d', linePath(points, x, y, `${{key}}_index`)).attr('stroke', colors[key]));
    addTooltip(wrap, svg, points, x, {{ ...inner }}, visibleKeys(), (row, key) => numberOrNull(row[`${{key}}_index`]), row => `启动后 ${{fmt(row.hours)}} 小时`);
  }}

  function redraw() {{
    d3.select('#equity-curve-split-vs-original-accounts').selectAll('*').remove();
    drawAggregate(); accountRows.forEach(drawAccount);
  }}
  root.querySelectorAll('.legend-button').forEach(button => button.addEventListener('click', () => {{
    const key = button.dataset.series; const activeCount = visibleKeys().length;
    if (state[key] && activeCount === 1) return;
    state[key] = !state[key]; button.setAttribute('aria-pressed', String(state[key])); redraw();
  }}));
  redraw();
  window.addEventListener('resize', (() => {{ let timer; return () => {{ clearTimeout(timer); timer = setTimeout(redraw, 120); }}; }})());
}})();
</script>
'''
    output = Path(args.output_html)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    print(f"wrote {output} bytes={output.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
