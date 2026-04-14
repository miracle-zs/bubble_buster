#!/usr/bin/env python3
import json
import sqlite3
import subprocess
import tempfile
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "state.db"
DEFAULT_OUT = ROOT / "reports" / "order_time_to_nextday_0730_report.html"
SH = ZoneInfo("Asia/Shanghai")
WINDOW_END_DAY_OFFSET = 1
WINDOW_END_HOUR = 7
WINDOW_END_MINUTE = 30
WINDOW_END_LABEL = "次日 07:30"


HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>开单到次日 07:30 下单窗口分析</title>
  <style>
    :root {
      --bg: #f4efe5;
      --paper: rgba(255,255,255,0.82);
      --ink: #1f1a17;
      --muted: #6f645d;
      --line: rgba(31,26,23,0.1);
      --up: #0f8b6d;
      --down: #d04f3e;
      --flat: #9b8d84;
      --accent: #d6a94f;
      --shadow: 0 18px 42px rgba(58, 41, 24, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 10% 10%, rgba(214,169,79,0.18), transparent 35%),
        radial-gradient(circle at 85% 0%, rgba(15,139,109,0.12), transparent 30%),
        linear-gradient(180deg, #f9f3e8 0%, #f1eadf 100%);
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Palatino, serif;
    }
    .wrap {
      width: min(1220px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 64px;
    }
    .hero {
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 28px;
      background: linear-gradient(135deg, rgba(255,255,255,0.88), rgba(252,246,237,0.9));
      box-shadow: var(--shadow);
      overflow: hidden;
      position: relative;
    }
    .hero::after {
      content: "";
      position: absolute;
      inset: auto -80px -80px auto;
      width: 240px;
      height: 240px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(214,169,79,0.28), rgba(214,169,79,0));
      pointer-events: none;
    }
    .eyebrow {
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 10px;
    }
    h1 {
      margin: 0;
      font-size: clamp(32px, 4vw, 56px);
      line-height: 0.98;
      max-width: 860px;
    }
    .hero p {
      max-width: 760px;
      margin: 16px 0 0;
      font-size: 18px;
      line-height: 1.55;
      color: #3d342f;
    }
    .section {
      margin-top: 22px;
      padding: 24px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: var(--paper);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    .section h2 {
      margin: 0 0 16px;
      font-size: 24px;
    }
    .grid {
      display: grid;
      gap: 16px;
    }
    .metrics {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    .metric, .account-card {
      background: rgba(255,255,255,0.68);
      border: 1px solid rgba(31,26,23,0.08);
      border-radius: 18px;
      padding: 18px;
    }
    .metric-label {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .metric-value {
      margin-top: 8px;
      font-size: 34px;
      line-height: 1;
    }
    .metric-note {
      margin-top: 10px;
      color: var(--muted);
      font-size: 14px;
    }
    .takeaways {
      margin: 0;
      padding-left: 18px;
      color: #403632;
      line-height: 1.7;
    }
    .bars {
      display: grid;
      gap: 14px;
    }
    .bar-row {
      display: grid;
      grid-template-columns: 76px 1.1fr 96px 96px;
      gap: 14px;
      align-items: center;
    }
    .bar-label {
      font-size: 20px;
      font-weight: 700;
    }
    .bar-track {
      position: relative;
      height: 16px;
      border-radius: 999px;
      background: rgba(31,26,23,0.08);
      overflow: hidden;
    }
    .bar-fill {
      position: absolute;
      inset: 0 auto 0 0;
      border-radius: 999px;
      background: linear-gradient(90deg, #b05741, #d6a94f, #0f8b6d);
    }
    .bar-stat {
      text-align: right;
      font-variant-numeric: tabular-nums;
      font-size: 15px;
    }
    .candidate-best {
      color: var(--up);
      font-weight: 700;
    }
    .accounts {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    .viz-grid {
      grid-template-columns: 1.3fr 0.9fr;
      align-items: start;
    }
    .chart-shell, .mini-panel {
      background: rgba(255,255,255,0.68);
      border: 1px solid rgba(31,26,23,0.08);
      border-radius: 18px;
      padding: 16px;
    }
    .small-multiples {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .small-chart h3 {
      margin: 0 0 12px;
      font-size: 20px;
    }
    .interactive-grid {
      display: grid;
      gap: 18px;
    }
    .interactive-account {
      border: 1px solid rgba(31,26,23,0.08);
      border-radius: 20px;
      background: rgba(255,255,255,0.64);
      overflow: hidden;
    }
    .interactive-head {
      padding: 16px 18px;
      border-bottom: 1px solid rgba(31,26,23,0.08);
      background: rgba(247,240,229,0.92);
    }
    .interactive-head h3 {
      margin: 0;
      font-size: 22px;
    }
    .interactive-head p {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }
    .interactive-body {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) 280px;
      gap: 0;
    }
    .interactive-canvas {
      padding: 14px;
      border-right: 1px solid rgba(31,26,23,0.08);
      background: rgba(255,255,255,0.35);
    }
    .interactive-canvas svg {
      width: 100%;
      height: auto;
      display: block;
    }
    .sample-info {
      padding: 16px;
      display: grid;
      align-content: start;
      gap: 12px;
      background: rgba(255,255,255,0.4);
    }
    .sample-info h4 {
      margin: 0;
      font-size: 18px;
    }
    .sample-kv {
      display: grid;
      gap: 8px;
    }
    .sample-kv div {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      padding-top: 8px;
      border-top: 1px solid rgba(31,26,23,0.08);
      font-size: 14px;
      font-variant-numeric: tabular-nums;
    }
    .sample-kv div:first-child { border-top: 0; padding-top: 0; }
    .sample-note {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .chart-shell svg {
      width: 100%;
      height: auto;
      display: block;
    }
    .chart-legend {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 10px;
    }
    .chart-legend span {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .chart-legend i {
      width: 16px;
      height: 3px;
      display: inline-block;
      border-radius: 999px;
    }
    .mini-metrics {
      display: grid;
      gap: 12px;
    }
    .mini-metric {
      padding: 12px 0;
      border-top: 1px solid rgba(31,26,23,0.08);
    }
    .mini-metric:first-child { border-top: 0; padding-top: 0; }
    .mini-label {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .mini-value {
      margin-top: 6px;
      font-size: 26px;
      line-height: 1;
    }
    .mini-note {
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
    }
    .time-bars {
      display: grid;
      gap: 10px;
    }
    .time-bar-row {
      display: grid;
      grid-template-columns: 56px 1fr 48px;
      align-items: center;
      gap: 12px;
    }
    .time-bar-track {
      height: 10px;
      border-radius: 999px;
      background: rgba(31,26,23,0.08);
      overflow: hidden;
    }
    .time-bar-fill {
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, #d6a94f, #0f8b6d);
    }
    .account-card h3 {
      margin: 0 0 14px;
      font-size: 22px;
    }
    .account-line {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 8px 0;
      border-top: 1px solid rgba(31,26,23,0.08);
      font-variant-numeric: tabular-nums;
    }
    .account-line:first-of-type { border-top: 0; }
    .heatmap {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(12px, 1fr));
      gap: 8px;
      align-items: start;
    }
    .dot {
      position: relative;
      width: 100%;
      aspect-ratio: 1 / 1;
      border-radius: 4px;
      background: var(--flat);
      border: 1px solid rgba(31,26,23,0.08);
      cursor: default;
    }
    .dot.up { background: color-mix(in srgb, var(--up) 84%, white); }
    .dot.down { background: color-mix(in srgb, var(--down) 84%, white); }
    .dot.flat { background: color-mix(in srgb, var(--flat) 84%, white); }
    .dot:hover::after {
      content: attr(data-tip);
      position: absolute;
      left: 50%;
      bottom: calc(100% + 8px);
      transform: translateX(-50%);
      min-width: 190px;
      max-width: 260px;
      padding: 8px 10px;
      border-radius: 10px;
      background: rgba(24,20,18,0.94);
      color: white;
      font-size: 12px;
      line-height: 1.45;
      z-index: 10;
      white-space: pre-line;
      pointer-events: none;
      box-shadow: 0 10px 28px rgba(0,0,0,0.24);
    }
    .legend {
      display: flex;
      gap: 18px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 14px;
      margin-bottom: 14px;
    }
    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .legend i {
      display: inline-block;
      width: 12px;
      height: 12px;
      border-radius: 3px;
    }
    .tables-grid {
      display: grid;
      gap: 18px;
    }
    .table-group {
      border: 1px solid rgba(31,26,23,0.08);
      border-radius: 18px;
      background: rgba(255,255,255,0.58);
      overflow: hidden;
    }
    .table-group-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      padding: 14px 16px;
      background: rgba(247,240,229,0.92);
      border-bottom: 1px solid rgba(31,26,23,0.08);
    }
    .table-group-head h3 {
      margin: 0;
      font-size: 20px;
    }
    .table-group-head span {
      color: var(--muted);
      font-size: 13px;
    }
    .table-shell {
      overflow: auto;
      background: rgba(255,255,255,0.7);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-variant-numeric: tabular-nums;
      min-width: 760px;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid rgba(31,26,23,0.08);
      text-align: right;
      font-size: 14px;
    }
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) {
      text-align: left;
    }
    thead th {
      position: sticky;
      top: 0;
      background: #f7f0e5;
      z-index: 1;
    }
    .pos { color: var(--up); }
    .neg { color: var(--down); }
    .footer {
      margin-top: 16px;
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 960px) {
      .metrics, .accounts, .viz-grid, .small-multiples { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .bar-row { grid-template-columns: 70px 1fr; }
      .bar-stat { text-align: left; }
      .interactive-body { grid-template-columns: 1fr; }
      .interactive-canvas { border-right: 0; border-bottom: 1px solid rgba(31,26,23,0.08); }
    }
    @media (max-width: 640px) {
      .wrap { width: min(100vw - 20px, 1220px); }
      .hero, .section { padding: 18px; border-radius: 20px; }
      .metrics, .accounts, .viz-grid, .small-multiples { grid-template-columns: 1fr; }
      .bar-row { grid-template-columns: 1fr; }
      .time-bar-row { grid-template-columns: 56px 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Wallet Snapshot Lens</div>
      <h1>开单到次日 07:30，先看路径再看终点。</h1>
      <p id="hero-copy"></p>
    </section>

    <section class="section">
      <h2>核心结论</h2>
      <ul class="takeaways" id="takeaways"></ul>
    </section>

    <section class="section">
      <h2>总览</h2>
      <div class="grid metrics" id="metrics"></div>
    </section>

    <section class="section">
      <h2>开单后到次日 07:30 的时序路径</h2>
      <div class="grid viz-grid">
        <div class="chart-shell">
          <div class="chart-legend">
            <span><i style="background:#0f8b6d"></i>中位路径</span>
            <span><i style="background:#d6a94f"></i>平均路径</span>
            <span><i style="background:#c8b59f; height:10px; width:10px; border-radius:50%"></i>10%-90% 区间</span>
          </div>
          <div id="path-chart"></div>
        </div>
        <div class="mini-panel">
          <div class="mini-metrics" id="path-metrics"></div>
        </div>
      </div>
    </section>

    <section class="section">
      <h2>每个账号单独路径</h2>
      <div class="grid small-multiples" id="account-path-grid"></div>
      <div class="footer">每张图都把各自账户的每次开单时刻归一到 0%，然后聚合到次日 07:30。</div>
    </section>

    <section class="section">
      <h2>每个账号的所有样本线</h2>
      <div class="interactive-grid" id="account-sample-grid"></div>
      <div class="footer">横轴是开单后经过的分钟数，纵轴是相对开单时刻的收益率。点击任意一根线可以单独查看该样本。</div>
    </section>

    <section class="section">
      <h2>最低点通常出现在哪一分钟</h2>
      <div class="time-bars" id="min-time-bars"></div>
      <div class="footer">这里统计的是每次真实下单窗口里，从开单到次日 07:30 之间的最低权益点出现时刻。</div>
    </section>

    <section class="section">
      <h2>07:40 到次日 07:30 候选时点</h2>
      <div class="bars" id="candidate-bars"></div>
      <div class="footer">条形长度按到次日 07:30 的平均收益率缩放。绿色更优，红色更弱。</div>
    </section>

    <section class="section">
      <h2>账户拆分</h2>
      <div class="grid accounts" id="accounts"></div>
    </section>

    <section class="section">
      <h2>每次真实下单窗口的最终结果</h2>
      <div class="legend">
        <span><i style="background: var(--up)"></i>上涨</span>
        <span><i style="background: var(--down)"></i>下跌</span>
        <span><i style="background: var(--flat)"></i>持平</span>
      </div>
      <div class="heatmap" id="heatmap"></div>
      <div class="footer">每个色块代表一次真实下单样本。悬停可看账户、日期、起始余额、次日 07:30 余额与收益率。</div>
    </section>

    <section class="section">
      <h2>样本明细</h2>
      <div class="table-shell">
        <table>
          <thead>
            <tr>
              <th>日期</th>
              <th>账户</th>
              <th>实际下单</th>
              <th>起始余额</th>
              <th>次日 07:30 余额</th>
              <th>变化</th>
              <th>收益率</th>
            </tr>
          </thead>
          <tbody id="detail-table"></tbody>
        </table>
      </div>
    </section>
  </div>

  <script>
    const data = __DATA__;
    const windowEndLabel = data.window_end_label;

    const pct = (v) => `${v >= 0 ? "+" : ""}${v.toFixed(4)}%`;
    const num = (v) => `${v >= 0 ? "+" : ""}${v.toFixed(4)}`;
    const cls = (v) => v > 0 ? "pos" : (v < 0 ? "neg" : "");

    const overall = data.overall;
    const downRatio = overall.down_count / overall.sample_count * 100;
    const upRatio = overall.up_count / overall.sample_count * 100;

    document.getElementById("hero-copy").textContent =
      `实际样本 ${overall.sample_count} 次，涨到 ${windowEndLabel} 的占 ${upRatio.toFixed(2)}%，跌到 ${windowEndLabel} 的占 ${downRatio.toFixed(2)}%。` +
      ` 这份报告把开单到 ${windowEndLabel} 的分钟级路径也展开了，用来判断是否真的存在“总会先大回撤再反弹”。`;

    document.getElementById("takeaways").innerHTML = data.key_takeaways
      .map((item) => `<li>${item}</li>`).join("");

    const metricCards = [
      ["样本数", `${overall.sample_count}`, `真实下单后到 ${windowEndLabel} 的窗口`],
      ["上涨占比", `${upRatio.toFixed(2)}%`, `${overall.up_count} 次上涨`],
      ["下跌占比", `${downRatio.toFixed(2)}%`, `${overall.down_count} 次下跌`],
      ["平均收益率", pct(overall.avg_ret_pct), `中位数 ${pct(overall.med_ret_pct)}`],
    ];
    document.getElementById("metrics").innerHTML = metricCards.map(([label, value, note]) => `
      <article class="metric">
        <div class="metric-label">${label}</div>
        <div class="metric-value">${value}</div>
        <div class="metric-note">${note}</div>
      </article>
    `).join("");

    const renderPathChart = (containerId, points) => {
      const width = 760;
      const height = 320;
      const margin = { top: 12, right: 16, bottom: 34, left: 52 };
      const innerW = width - margin.left - margin.right;
      const innerH = height - margin.top - margin.bottom;
      const values = points.flatMap((item) => [item.avg_ret_pct, item.med_ret_pct, item.p10_ret_pct, item.p90_ret_pct]);
      const minY = Math.min(...values, -0.5);
      const maxY = Math.max(...values, 0.5);
      const yPad = Math.max((maxY - minY) * 0.12, 0.15);
      const y0 = minY - yPad;
      const y1 = maxY + yPad;
      const xOf = (idx) => margin.left + (idx / Math.max(points.length - 1, 1)) * innerW;
      const yOf = (v) => margin.top + (1 - (v - y0) / (y1 - y0)) * innerH;
      const linePath = (key) => points.map((item, idx) => `${idx === 0 ? "M" : "L"}${xOf(idx).toFixed(2)},${yOf(item[key]).toFixed(2)}`).join(" ");
      const bandTop = points.map((item, idx) => `${idx === 0 ? "M" : "L"}${xOf(idx).toFixed(2)},${yOf(item.p90_ret_pct).toFixed(2)}`).join(" ");
      const bandBottom = points.slice().reverse().map((item, revIdx) => {
        const idx = points.length - 1 - revIdx;
        return `L${xOf(idx).toFixed(2)},${yOf(item.p10_ret_pct).toFixed(2)}`;
      }).join(" ");
      const yTicks = [y0, (y0 + y1) / 2, y1];
      const xTickEvery = Math.max(1, Math.floor(points.length / 5));
      const svg = `
        <svg viewBox="0 0 ${width} ${height}" aria-label="path chart">
          ${yTicks.map((tick) => `
            <g>
              <line x1="${margin.left}" x2="${width - margin.right}" y1="${yOf(tick)}" y2="${yOf(tick)}" stroke="rgba(31,26,23,0.10)" />
              <text x="0" y="${yOf(tick) + 4}" fill="#6f645d" font-size="12">${tick.toFixed(2)}%</text>
            </g>
          `).join("")}
          ${points.map((item, idx) => idx % xTickEvery === 0 || idx === points.length - 1 ? `
            <g>
              <line x1="${xOf(idx)}" x2="${xOf(idx)}" y1="${margin.top}" y2="${height - margin.bottom}" stroke="rgba(31,26,23,0.06)" />
              <text x="${xOf(idx)}" y="${height - 10}" text-anchor="middle" fill="#6f645d" font-size="12">${item.time}</text>
            </g>
          ` : "").join("")}
          <path d="${bandTop} ${bandBottom} Z" fill="rgba(200,181,159,0.32)"></path>
          <path d="${linePath("avg_ret_pct")}" fill="none" stroke="#d6a94f" stroke-width="2.5"></path>
          <path d="${linePath("med_ret_pct")}" fill="none" stroke="#0f8b6d" stroke-width="3"></path>
        </svg>
      `;
      document.getElementById(containerId).innerHTML = svg;
    };

    const renderPathChartInline = (points, width = 520, height = 240) => {
      const margin = { top: 12, right: 14, bottom: 32, left: 50 };
      const innerW = width - margin.left - margin.right;
      const innerH = height - margin.top - margin.bottom;
      const values = points.flatMap((item) => [item.avg_ret_pct, item.med_ret_pct, item.p10_ret_pct, item.p90_ret_pct]);
      const minY = Math.min(...values, -0.5);
      const maxY = Math.max(...values, 0.5);
      const yPad = Math.max((maxY - minY) * 0.12, 0.15);
      const y0 = minY - yPad;
      const y1 = maxY + yPad;
      const xOf = (idx) => margin.left + (idx / Math.max(points.length - 1, 1)) * innerW;
      const yOf = (v) => margin.top + (1 - (v - y0) / (y1 - y0)) * innerH;
      const linePath = (key) => points.map((item, idx) => `${idx === 0 ? "M" : "L"}${xOf(idx).toFixed(2)},${yOf(item[key]).toFixed(2)}`).join(" ");
      const bandTop = points.map((item, idx) => `${idx === 0 ? "M" : "L"}${xOf(idx).toFixed(2)},${yOf(item.p90_ret_pct).toFixed(2)}`).join(" ");
      const bandBottom = points.slice().reverse().map((item, revIdx) => {
        const idx = points.length - 1 - revIdx;
        return `L${xOf(idx).toFixed(2)},${yOf(item.p10_ret_pct).toFixed(2)}`;
      }).join(" ");
      const yTicks = [y0, (y0 + y1) / 2, y1];
      const step = Math.max(1, Math.floor(points.length / 4));
      return `
        <svg viewBox="0 0 ${width} ${height}" aria-label="account path chart">
          ${yTicks.map((tick) => `
            <g>
              <line x1="${margin.left}" x2="${width - margin.right}" y1="${yOf(tick)}" y2="${yOf(tick)}" stroke="rgba(31,26,23,0.10)" />
              <text x="0" y="${yOf(tick) + 4}" fill="#6f645d" font-size="12">${tick.toFixed(2)}%</text>
            </g>
          `).join("")}
          ${points.map((item, idx) => idx % step === 0 || idx === points.length - 1 ? `
            <g>
              <line x1="${xOf(idx)}" x2="${xOf(idx)}" y1="${margin.top}" y2="${height - margin.bottom}" stroke="rgba(31,26,23,0.06)" />
              <text x="${xOf(idx)}" y="${height - 8}" text-anchor="middle" fill="#6f645d" font-size="11">${item.time}</text>
            </g>
          ` : "").join("")}
          <path d="${bandTop} ${bandBottom} Z" fill="rgba(200,181,159,0.28)"></path>
          <path d="${linePath("avg_ret_pct")}" fill="none" stroke="#d6a94f" stroke-width="2"></path>
          <path d="${linePath("med_ret_pct")}" fill="none" stroke="#0f8b6d" stroke-width="2.5"></path>
        </svg>
      `;
    };

    const renderSamplePathPanel = (accountData, panelId) => {
      const width = 760;
      const height = 320;
      const margin = { top: 12, right: 16, bottom: 34, left: 54 };
      const innerW = width - margin.left - margin.right;
      const innerH = height - margin.top - margin.bottom;
      const series = accountData.samples;
      const allValues = series.flatMap((sample) => sample.returns);
      const minY = Math.min(...allValues, -0.5);
      const maxY = Math.max(...allValues, 0.5);
      const yPad = Math.max((maxY - minY) * 0.12, 0.2);
      const y0 = minY - yPad;
      const y1 = maxY + yPad;
      const maxMinute = accountData.max_minutes || 1;
      const xOf = (minute) => margin.left + (minute / maxMinute) * innerW;
      const yOf = (v) => margin.top + (1 - (v - y0) / (y1 - y0)) * innerH;
      const ticks = [];
      for (let m = 0; m <= maxMinute; m += 30) ticks.push(m);
      if (ticks[ticks.length - 1] !== maxMinute) ticks.push(maxMinute);
      const yTicks = [y0, (y0 + y1) / 2, y1];

      const panel = document.getElementById(panelId);
      panel.innerHTML = `
        <article class="interactive-account">
          <div class="interactive-head">
            <h3>${accountData.account_id}</h3>
            <p>${series.length} 条样本线。默认展示全部路径，点击任意一根线高亮并查看该样本。</p>
          </div>
          <div class="interactive-body">
            <div class="interactive-canvas">
              <svg viewBox="0 0 ${width} ${height}" aria-label="${accountData.account_id} sample paths">
                ${yTicks.map((tick) => `
                  <g>
                    <line x1="${margin.left}" x2="${width - margin.right}" y1="${yOf(tick)}" y2="${yOf(tick)}" stroke="rgba(31,26,23,0.10)" />
                    <text x="0" y="${yOf(tick) + 4}" fill="#6f645d" font-size="12">${tick.toFixed(2)}%</text>
                  </g>
                `).join("")}
                ${ticks.map((minute) => `
                  <g>
                    <line x1="${xOf(minute)}" x2="${xOf(minute)}" y1="${margin.top}" y2="${height - margin.bottom}" stroke="rgba(31,26,23,0.06)" />
                    <text x="${xOf(minute)}" y="${height - 8}" text-anchor="middle" fill="#6f645d" font-size="11">${minute}m</text>
                  </g>
                `).join("")}
                ${series.map((sample, idx) => {
                  const path = sample.returns.map((ret, minute) => `${minute === 0 ? "M" : "L"}${xOf(minute).toFixed(2)},${yOf(ret).toFixed(2)}`).join(" ");
                  return `<path d="${path}" data-sample-index="${idx}" fill="none" stroke="rgba(15,139,109,0.18)" stroke-width="1.1" style="cursor:pointer" />`;
                }).join("")}
              </svg>
            </div>
            <aside class="sample-info" id="${panelId}-info"></aside>
          </div>
        </article>
      `;

      const info = document.getElementById(`${panelId}-info`);
      const paths = [...panel.querySelectorAll("[data-sample-index]")];
      const setActive = (idx) => {
        const sample = series[idx];
        paths.forEach((path, pathIdx) => {
          if (pathIdx === idx) {
            path.setAttribute("stroke", "#d04f3e");
            path.setAttribute("stroke-width", "2.8");
            path.setAttribute("opacity", "1");
          } else {
            path.setAttribute("stroke", "rgba(15,139,109,0.16)");
            path.setAttribute("stroke-width", "1.0");
            path.setAttribute("opacity", "0.9");
          }
        });
        info.innerHTML = `
          <h4>${sample.date}</h4>
          <div class="sample-kv">
            <div><span>实际开单</span><strong>${sample.run_time}</strong></div>
            <div><span>到 ${windowEndLabel} 收益</span><strong class="${cls(sample.final_ret_pct)}">${pct(sample.final_ret_pct)}</strong></div>
            <div><span>最低点</span><strong>${sample.min_time}</strong></div>
            <div><span>最低回撤</span><strong class="${cls(sample.min_drawdown_pct)}">${pct(sample.min_drawdown_pct)}</strong></div>
            <div><span>样本编号</span><strong>#${idx + 1}</strong></div>
          </div>
          <div class="sample-note">如果想切换样本，直接点击左侧图里的任意一根线。</div>
        `;
      };

      paths.forEach((path) => {
        path.addEventListener("click", () => setActive(Number(path.dataset.sampleIndex)));
      });
      setActive(0);
    };

    const dd = data.drawdown_summary;
    const pathCards = [
      ["最低点中位回撤", pct(dd.med_min_drawdown_pct), "如果经常先深跌，这里会明显更负"],
      ["最低点平均回撤", pct(dd.avg_min_drawdown_pct), `${dd.pct_le_minus_1_0.toFixed(2)}% 的样本低于 -1%`],
      ["低于 -0.5%", `${dd.pct_le_minus_0_5.toFixed(2)}%`, `低于 -2% 的只有 ${dd.pct_le_minus_2_0.toFixed(2)}%`],
      ["最低点 hindsight 优势", pct(dd.med_hindsight_edge_pct), `中位数。如果能买在最低点，到 ${windowEndLabel} 理论上多 ${pct(dd.med_hindsight_edge_pct)}`],
    ];
    document.getElementById("path-metrics").innerHTML = pathCards.map(([label, value, note]) => `
      <div class="mini-metric">
        <div class="mini-label">${label}</div>
        <div class="mini-value ${cls(parseFloat(value))}">${value}</div>
        <div class="mini-note">${note}</div>
      </div>
    `).join("");
    renderPathChart("path-chart", data.path_curve);

    document.getElementById("account-path-grid").innerHTML = data.account_path_curves.map((item) => `
      <article class="chart-shell small-chart">
        <h3>${item.account_id}</h3>
        ${renderPathChartInline(item.points)}
      </article>
    `).join("");

    document.getElementById("account-sample-grid").innerHTML = data.account_sample_paths
      .map((item, idx) => `<div id="account-sample-panel-${idx}"></div>`)
      .join("");
    data.account_sample_paths.forEach((item, idx) => renderSamplePathPanel(item, `account-sample-panel-${idx}`));

    const maxMinCount = Math.max(...data.min_time_distribution.map((item) => item.count), 1);
    document.getElementById("min-time-bars").innerHTML = data.min_time_distribution
      .slice()
      .sort((a, b) => b.count - a.count)
      .slice(0, 12)
      .map((item) => `
        <div class="time-bar-row">
          <div>${item.time}</div>
          <div class="time-bar-track"><div class="time-bar-fill" style="width:${item.count / maxMinCount * 100}%"></div></div>
          <div>${item.count}</div>
        </div>
      `).join("");

    const candidateBest = [...data.candidate_windows].sort((a, b) => b.avg_ret_pct - a.avg_ret_pct)[0].time;
    const maxAbs = Math.max(...data.candidate_windows.map((item) => Math.abs(item.avg_ret_pct))) || 1;
    document.getElementById("candidate-bars").innerHTML = data.candidate_windows.map((item) => {
      const width = Math.max(8, Math.abs(item.avg_ret_pct) / maxAbs * 100);
      const fill = item.avg_ret_pct >= 0 ? `linear-gradient(90deg, #d6a94f, #0f8b6d)` : `linear-gradient(90deg, #d28b6d, #d04f3e)`;
      return `
        <div class="bar-row">
          <div class="bar-label ${item.time === candidateBest ? "candidate-best" : ""}">${item.time}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${width}%; background:${fill};"></div></div>
          <div class="bar-stat ${cls(item.avg_ret_pct)}">${pct(item.avg_ret_pct)}</div>
          <div class="bar-stat">${item.down_ratio_pct.toFixed(2)}% down</div>
        </div>
      `;
    }).join("");

    document.getElementById("accounts").innerHTML = data.by_account.map((item) => {
      const downRatioAccount = item.down_count / item.sample_count * 100;
      return `
        <article class="account-card">
          <h3>${item.account_id}</h3>
          <div class="account-line"><span>样本</span><strong>${item.sample_count}</strong></div>
          <div class="account-line"><span>上涨 / 下跌</span><strong>${item.up_count} / ${item.down_count}</strong></div>
          <div class="account-line"><span>下跌占比</span><strong>${downRatioAccount.toFixed(2)}%</strong></div>
          <div class="account-line"><span>平均变化</span><strong class="${cls(item.avg_delta)}">${num(item.avg_delta)}</strong></div>
          <div class="account-line"><span>平均收益率</span><strong class="${cls(item.avg_ret_pct)}">${pct(item.avg_ret_pct)}</strong></div>
        </article>
      `;
    }).join("");

    document.getElementById("heatmap").innerHTML = data.actual_run_samples.map((item) => {
      const tip = `${item.date} ${item.account_id}\\n实际下单 ${item.run_time}\\n起始 ${item.start_balance}\\n${windowEndLabel} ${item.end_balance_window}\\n变化 ${num(item.delta)}\\n收益率 ${pct(item.ret_pct)}`;
      return `<div class="dot ${item.direction}" data-tip="${tip}"></div>`;
    }).join("");

    const groupedRows = data.actual_run_samples.reduce((acc, item) => {
      if (!acc[item.account_id]) acc[item.account_id] = [];
      acc[item.account_id].push(item);
      return acc;
    }, {});
    document.getElementById("detail-table").parentElement.parentElement.outerHTML = `
      <div class="tables-grid">
        ${Object.entries(groupedRows)
          .sort((a, b) => a[0].localeCompare(b[0]))
          .map(([accountId, rows]) => {
            const sortedRows = rows.slice().sort((a, b) => a.run_ts < b.run_ts ? 1 : -1);
            return `
              <section class="table-group">
                <div class="table-group-head">
                  <h3>${accountId}</h3>
                  <span>${rows.length} 个样本</span>
                </div>
                <div class="table-shell">
                  <table>
                    <thead>
                      <tr>
                        <th>日期</th>
                        <th>账户</th>
                        <th>实际下单</th>
                        <th>起始余额</th>
                        <th>${windowEndLabel} 余额</th>
                        <th>变化</th>
                        <th>收益率</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${sortedRows.map((item) => `
                        <tr>
                          <td>${item.date}</td>
                          <td>${item.account_id}</td>
                          <td>${item.run_time}</td>
                          <td>${item.start_balance.toFixed(4)}</td>
                          <td>${item.end_balance_window.toFixed(4)}</td>
                          <td class="${cls(item.delta)}">${num(item.delta)}</td>
                          <td class="${cls(item.ret_pct)}">${pct(item.ret_pct)}<br><span style="color:#6f645d;font-size:12px">最低点 ${item.min_time} / ${pct(item.min_drawdown_pct)}</span></td>
                        </tr>
                      `).join("")}
                    </tbody>
                  </table>
                </div>
              </section>
            `;
          }).join("")}
      </div>
    `;
  </script>
</body>
</html>
"""


def recover_database(source: Path) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="state_recovered_"))
    sql_path = temp_dir / "recovered.sql"
    db_path = temp_dir / "recovered.db"
    result = subprocess.run(
        ["sqlite3", str(source), ".recover --ignore-freelist"],
        check=True,
        capture_output=True,
        text=True,
    )
    sql_path.write_text(result.stdout, encoding="utf-8")
    subprocess.run(["sqlite3", str(db_path)], input=result.stdout, text=True, check=True)
    return db_path


def load_wallet_data(db_path: Path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    wallet = defaultdict(list)
    runs = defaultdict(list)
    try:
        cur.execute(
            "select account_id, captured_at_utc, balance_usdt from wallet_snapshots "
            "order by account_id, captured_at_utc"
        )
        for acc, ts_s, bal in cur:
            ts = datetime.fromisoformat(ts_s).astimezone(SH)
            wallet[acc].append((ts, float(bal)))

        cur.execute("select account_id, started_at_utc from runs order by account_id, started_at_utc")
        for acc, ts_s in cur:
            ts = datetime.fromisoformat(ts_s).astimezone(SH)
            runs[acc].append(ts)
    finally:
        conn.close()
    return wallet, runs


def first_after(index_map, account_id: str, target: datetime):
    ts_list, bal_list = index_map[account_id]
    pos = bisect_left(ts_list, target)
    if pos >= len(ts_list):
        return None
    return ts_list[pos], bal_list[pos]


def percentile(values, fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * fraction
    left = int(pos)
    right = min(left + 1, len(ordered) - 1)
    weight = pos - left
    return ordered[left] * (1 - weight) + ordered[right] * weight


def window_end_ts(run_ts: datetime) -> datetime:
    return run_ts.replace(
        hour=WINDOW_END_HOUR,
        minute=WINDOW_END_MINUTE,
        second=0,
        microsecond=0,
    ) + timedelta(days=WINDOW_END_DAY_OFFSET)


def build_report(wallet, runs):
    index_map = {acc: ([ts for ts, _ in pts], [bal for _, bal in pts]) for acc, pts in wallet.items()}

    actual_rows = []
    path_buckets = defaultdict(list)
    account_path_buckets = defaultdict(lambda: defaultdict(list))
    account_sample_paths = defaultdict(list)
    min_time_counter = defaultdict(int)
    hindsight_edges = []
    for acc, run_list in runs.items():
        for run_ts in run_list:
            end_ts = window_end_ts(run_ts)
            if run_ts >= end_ts:
                continue
            start_obs = first_after(index_map, acc, run_ts)
            end_obs = first_after(index_map, acc, end_ts)
            if not start_obs or not end_obs:
                continue
            start_obs_ts, start_bal = start_obs
            end_obs_ts, end_bal = end_obs
            if start_obs_ts.date() != run_ts.date() or end_obs_ts.date() != end_ts.date():
                continue
            delta = end_bal - start_bal
            ret_pct = delta / start_bal * 100 if start_bal else 0.0
            minute_points = []
            cursor = run_ts.replace(second=0, microsecond=0)
            while cursor <= end_ts:
                point_obs = first_after(index_map, acc, cursor)
                if not point_obs or point_obs[0] > end_ts:
                    break
                point_ret = (point_obs[1] - start_bal) / start_bal * 100 if start_bal else 0.0
                minute_key = cursor.strftime("%H:%M")
                minute_points.append((minute_key, point_ret, point_obs[1]))
                path_buckets[minute_key].append(point_ret)
                account_path_buckets[acc][minute_key].append(point_ret)
                cursor += timedelta(minutes=1)
            if len(minute_points) < 2:
                continue
            min_time, min_drawdown_pct, min_balance = min(minute_points, key=lambda item: item[1])
            hindsight_ret_pct = (end_bal - min_balance) / min_balance * 100 if min_balance else 0.0
            hindsight_edge_pct = hindsight_ret_pct - ret_pct
            hindsight_edges.append(hindsight_edge_pct)
            min_time_counter[min_time] += 1
            actual_rows.append(
                {
                    "account_id": acc,
                    "run_time": run_ts.strftime("%H:%M"),
                    "run_ts": run_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "date": run_ts.strftime("%Y-%m-%d"),
                    "start_balance": round(start_bal, 4),
                    "end_balance_window": round(end_bal, 4),
                    "delta": round(delta, 4),
                    "ret_pct": round(ret_pct, 4),
                    "min_time": min_time,
                    "min_drawdown_pct": round(min_drawdown_pct, 4),
                    "hindsight_edge_pct": round(hindsight_edge_pct, 4),
                    "direction": "down" if delta < 0 else ("up" if delta > 0 else "flat"),
                }
            )
            account_sample_paths[acc].append(
                {
                    "date": run_ts.strftime("%Y-%m-%d"),
                    "run_time": run_ts.strftime("%H:%M"),
                    "final_ret_pct": round(ret_pct, 4),
                    "min_time": min_time,
                    "min_drawdown_pct": round(min_drawdown_pct, 4),
                    "returns": [round(item[1], 4) for item in minute_points],
                }
            )

    overall = {
        "sample_count": len(actual_rows),
        "down_count": sum(1 for item in actual_rows if item["delta"] < 0),
        "up_count": sum(1 for item in actual_rows if item["delta"] > 0),
        "flat_count": sum(1 for item in actual_rows if item["delta"] == 0),
        "avg_delta": round(mean(item["delta"] for item in actual_rows), 4),
        "med_delta": round(median(item["delta"] for item in actual_rows), 4),
        "avg_ret_pct": round(mean(item["ret_pct"] for item in actual_rows), 4),
        "med_ret_pct": round(median(item["ret_pct"] for item in actual_rows), 4),
    }

    by_account = []
    for acc in sorted(wallet):
        rows = [item for item in actual_rows if item["account_id"] == acc]
        if not rows:
            continue
        by_account.append(
            {
                "account_id": acc,
                "sample_count": len(rows),
                "down_count": sum(1 for item in rows if item["delta"] < 0),
                "up_count": sum(1 for item in rows if item["delta"] > 0),
                "avg_delta": round(mean(item["delta"] for item in rows), 4),
                "med_delta": round(median(item["delta"] for item in rows), 4),
                "avg_ret_pct": round(mean(item["ret_pct"] for item in rows), 4),
                "med_ret_pct": round(median(item["ret_pct"] for item in rows), 4),
            }
        )

    candidate_windows = []
    for candidate in ("07:40", "07:45", "07:50", "07:55"):
        returns = []
        hh, mm = map(int, candidate.split(":"))
        for acc in sorted(wallet):
            dates = sorted({ts.date() for ts, _ in wallet[acc]})
            for day in dates:
                start_target = datetime(day.year, day.month, day.day, hh, mm, tzinfo=SH)
                start_obs = first_after(index_map, acc, start_target)
                if not start_obs or start_obs[0].date() != day:
                    continue
                end_target = window_end_ts(start_target)
                end_obs = first_after(index_map, acc, end_target)
                if not end_obs or end_obs[0].date() != end_target.date() or start_obs[0] > end_obs[0]:
                    continue
                start_bal = start_obs[1]
                end_bal = end_obs[1]
                returns.append((end_bal - start_bal) / start_bal * 100 if start_bal else 0.0)
        candidate_windows.append(
            {
                "time": candidate,
                "sample_count": len(returns),
                "down_ratio_pct": round(sum(1 for item in returns if item < 0) / len(returns) * 100, 2),
                "up_ratio_pct": round(sum(1 for item in returns if item > 0) / len(returns) * 100, 2),
                "avg_ret_pct": round(mean(returns), 4),
                "med_ret_pct": round(median(returns), 4),
            }
        )

    path_curve = []
    for minute_key in sorted(path_buckets):
        values = path_buckets[minute_key]
        path_curve.append(
            {
                "time": minute_key,
                "sample_count": len(values),
                "avg_ret_pct": round(mean(values), 4),
                "med_ret_pct": round(median(values), 4),
                "p10_ret_pct": round(percentile(values, 0.10), 4),
                "p90_ret_pct": round(percentile(values, 0.90), 4),
            }
        )

    account_path_curves = []
    for acc in sorted(account_path_buckets):
        points = []
        for minute_key in sorted(account_path_buckets[acc]):
            values = account_path_buckets[acc][minute_key]
            points.append(
                {
                    "time": minute_key,
                    "sample_count": len(values),
                    "avg_ret_pct": round(mean(values), 4),
                    "med_ret_pct": round(median(values), 4),
                    "p10_ret_pct": round(percentile(values, 0.10), 4),
                    "p90_ret_pct": round(percentile(values, 0.90), 4),
                }
            )
        account_path_curves.append({"account_id": acc, "points": points})

    account_sample_path_list = []
    for acc in sorted(account_sample_paths):
        max_minutes = max(len(sample["returns"]) - 1 for sample in account_sample_paths[acc]) if account_sample_paths[acc] else 0
        account_sample_path_list.append(
            {
                "account_id": acc,
                "max_minutes": max_minutes,
                "samples": account_sample_paths[acc],
            }
        )

    drawdowns = [item["min_drawdown_pct"] for item in actual_rows]
    drawdown_summary = {
        "avg_min_drawdown_pct": round(mean(drawdowns), 4),
        "med_min_drawdown_pct": round(median(drawdowns), 4),
        "pct_le_minus_0_5": round(sum(1 for item in drawdowns if item <= -0.5) / len(drawdowns) * 100, 2),
        "pct_le_minus_1_0": round(sum(1 for item in drawdowns if item <= -1.0) / len(drawdowns) * 100, 2),
        "pct_le_minus_2_0": round(sum(1 for item in drawdowns if item <= -2.0) / len(drawdowns) * 100, 2),
        "avg_hindsight_edge_pct": round(mean(hindsight_edges), 4),
        "med_hindsight_edge_pct": round(median(hindsight_edges), 4),
    }

    min_time_distribution = [
        {"time": minute_key, "count": count}
        for minute_key, count in sorted(min_time_counter.items())
    ]

    best_candidate = max(candidate_windows, key=lambda item: item["avg_ret_pct"])

    trend_summary = "多数样本上涨" if overall["up_count"] > overall["down_count"] else "多数样本下跌"
    drawdown_summary_text = (
        f"窗口内最低点的中位回撤为 {drawdown_summary['med_min_drawdown_pct']:.4f}%，"
        "回撤并不轻。"
        if drawdown_summary["med_min_drawdown_pct"] <= -1.0
        else f"窗口内最低点的中位回撤只有 {drawdown_summary['med_min_drawdown_pct']:.4f}%，并不存在几乎每次都会先大回撤。"
    )

    return {
        "title": f"Order Time To {WINDOW_END_LABEL} Analysis",
        "window_end_label": WINDOW_END_LABEL,
        "generated_at": datetime.now(SH).strftime("%Y-%m-%d %H:%M:%S"),
        "overall": overall,
        "by_account": by_account,
        "candidate_windows": candidate_windows,
        "path_curve": path_curve,
        "account_path_curves": account_path_curves,
        "account_sample_paths": account_sample_path_list,
        "drawdown_summary": drawdown_summary,
        "min_time_distribution": min_time_distribution,
        "actual_run_samples": actual_rows,
        "key_takeaways": [
            f"实际下单后到 {WINDOW_END_LABEL}，{trend_summary}。",
            drawdown_summary_text,
            f"如果用事后最低点进场，理论上到 {WINDOW_END_LABEL} 的中位提升约 {drawdown_summary['med_hindsight_edge_pct']:.4f}%；但这是 hindsight，不能直接当作实盘下单时点。",
            f"在 07:40 到 {WINDOW_END_LABEL} 的候选点里，当前样本下 {best_candidate['time']} 的平均表现最好。",
        ],
    }


def main():
    db_path = DEFAULT_DB
    try:
      wallet, runs = load_wallet_data(db_path)
    except sqlite3.DatabaseError:
      db_path = recover_database(db_path)
      wallet, runs = load_wallet_data(db_path)

    report = build_report(wallet, runs)
    DEFAULT_OUT.parent.mkdir(parents=True, exist_ok=True)
    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(report, ensure_ascii=False))
    DEFAULT_OUT.write_text(html, encoding="utf-8")
    print(DEFAULT_OUT)


if __name__ == "__main__":
    main()
