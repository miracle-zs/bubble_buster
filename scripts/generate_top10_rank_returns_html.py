#!/usr/bin/env python3
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT / "reports" / "top10_short_rank_returns.csv"
DEFAULT_OUT = ROOT / "reports" / "top10_short_rank_returns_report.html"


HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>涨幅榜做空收益可视化</title>
  <style>
    :root {
      --bg: #f2eee6;
      --paper: rgba(255,255,255,0.82);
      --ink: #181512;
      --muted: #73685e;
      --line: rgba(24,21,18,0.1);
      --up: #0f8b6d;
      --down: #bf4d3b;
      --amber: #cc9b39;
      --slate: #8d8379;
      --accent: #112a46;
      --shadow: 0 18px 48px rgba(43, 31, 20, 0.12);
      --font-body: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      --font-display: "Baskerville", "Times New Roman", serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 0% 0%, rgba(17,42,70,0.14), transparent 28%),
        radial-gradient(circle at 100% 10%, rgba(204,155,57,0.18), transparent 24%),
        linear-gradient(180deg, #f8f4ec 0%, #efe9de 100%);
      font-family: var(--font-body);
    }
    .wrap {
      width: min(1280px, calc(100vw - 28px));
      margin: 0 auto;
      padding: 24px 0 56px;
    }
    .hero, .panel {
      border: 1px solid var(--line);
      border-radius: 28px;
      background: var(--paper);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    .hero {
      padding: 30px;
      position: relative;
      overflow: hidden;
      background:
        linear-gradient(135deg, rgba(255,255,255,0.92), rgba(245,237,224,0.9)),
        var(--paper);
    }
    .hero::after {
      content: "";
      position: absolute;
      inset: auto -80px -80px auto;
      width: 240px;
      height: 240px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(17,42,70,0.18), rgba(17,42,70,0));
    }
    .eyebrow {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      color: var(--muted);
      margin-bottom: 12px;
    }
    h1 {
      margin: 0;
      max-width: 900px;
      font-family: var(--font-display);
      font-size: clamp(34px, 4vw, 60px);
      line-height: 0.98;
      font-weight: 600;
    }
    .hero p {
      margin: 16px 0 0;
      max-width: 760px;
      color: #3c342d;
      line-height: 1.6;
      font-size: 17px;
    }
    .stack {
      display: grid;
      gap: 18px;
      margin-top: 18px;
    }
    .panel {
      padding: 22px;
    }
    .panel h2 {
      margin: 0 0 14px;
      font-size: 22px;
    }
    .subtle {
      margin-top: -4px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }
    .metrics {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    .metric {
      padding: 16px;
      border: 1px solid rgba(24,21,18,0.08);
      border-radius: 18px;
      background: rgba(255,255,255,0.68);
    }
    .metric .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.1em;
    }
    .metric .value {
      margin-top: 8px;
      font-size: 32px;
      line-height: 1;
      font-weight: 700;
    }
    .metric .note {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }
    .twocol {
      display: grid;
      gap: 18px;
      grid-template-columns: 1.25fr 1fr;
    }
    .card {
      padding: 16px;
      border: 1px solid rgba(24,21,18,0.08);
      border-radius: 20px;
      background: rgba(255,255,255,0.68);
    }
    .card h3 {
      margin: 0 0 12px;
      font-size: 18px;
    }
    .bars, .trend-list, .basis-list {
      display: grid;
      gap: 12px;
    }
    .bar-row {
      display: grid;
      gap: 12px;
      grid-template-columns: 68px 1fr 86px;
      align-items: center;
      font-variant-numeric: tabular-nums;
    }
    .track {
      position: relative;
      height: 14px;
      border-radius: 999px;
      background: rgba(24,21,18,0.08);
      overflow: hidden;
    }
    .fill {
      position: absolute;
      inset: 0 auto 0 0;
      border-radius: 999px;
      background: linear-gradient(90deg, #c45b46, #cc9b39, #0f8b6d);
    }
    .heatmap {
      overflow: auto;
    }
    .heatmap table {
      width: 100%;
      border-collapse: collapse;
      min-width: 520px;
      font-variant-numeric: tabular-nums;
    }
    .heatmap th, .heatmap td {
      padding: 10px 12px;
      text-align: center;
      border-bottom: 1px solid rgba(24,21,18,0.08);
    }
    .heat-cell {
      border-radius: 12px;
      color: #fff;
      font-weight: 700;
      padding: 10px 8px;
    }
    .heat-note {
      display: block;
      font-size: 11px;
      opacity: 0.84;
      margin-top: 4px;
      font-weight: 500;
    }
    .trend-row, .basis-row {
      display: grid;
      grid-template-columns: 74px 1fr 72px;
      gap: 12px;
      align-items: center;
      font-variant-numeric: tabular-nums;
    }
    .dotline {
      height: 10px;
      position: relative;
      background: linear-gradient(90deg, rgba(191,77,59,0.16), rgba(204,155,57,0.12), rgba(15,139,109,0.16));
      border-radius: 999px;
    }
    .dot {
      position: absolute;
      top: 50%;
      width: 14px;
      height: 14px;
      transform: translate(-50%, -50%);
      border-radius: 50%;
      background: var(--accent);
      border: 2px solid rgba(255,255,255,0.9);
      box-shadow: 0 6px 16px rgba(17,42,70,0.24);
    }
    .filters {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      margin-bottom: 16px;
    }
    .field {
      display: grid;
      gap: 6px;
    }
    .field label {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }
    .field select, .field input {
      width: 100%;
      padding: 10px 12px;
      border-radius: 14px;
      border: 1px solid rgba(24,21,18,0.12);
      background: rgba(255,255,255,0.86);
      color: var(--ink);
      font: inherit;
    }
    .detail-summary {
      margin-bottom: 12px;
      color: var(--muted);
      font-size: 14px;
    }
    .table-shell {
      overflow: auto;
      border: 1px solid rgba(24,21,18,0.08);
      border-radius: 18px;
      background: rgba(255,255,255,0.68);
    }
    table.detail {
      width: 100%;
      min-width: 1040px;
      border-collapse: collapse;
      font-variant-numeric: tabular-nums;
    }
    .detail th, .detail td {
      padding: 10px 12px;
      border-bottom: 1px solid rgba(24,21,18,0.08);
      text-align: right;
      font-size: 13px;
    }
    .detail th:nth-child(-n+4), .detail td:nth-child(-n+4) {
      text-align: left;
    }
    .detail thead th {
      position: sticky;
      top: 0;
      background: #f6f0e4;
      z-index: 1;
    }
    .pos { color: var(--up); }
    .neg { color: var(--down); }
    .muted { color: var(--muted); }
    @media (max-width: 980px) {
      .metrics, .twocol, .filters { grid-template-columns: 1fr 1fr; }
    }
    @media (max-width: 640px) {
      .wrap { width: min(100vw - 18px, 1280px); }
      .hero, .panel { border-radius: 22px; }
      .hero, .panel { padding: 18px; }
      .metrics, .twocol, .filters { grid-template-columns: 1fr; }
      .bar-row, .trend-row, .basis-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Top10 Short Return Atlas</div>
      <h1>涨幅榜做空收益可视化</h1>
      <p id="hero-copy"></p>
    </section>

    <div class="stack">
      <section class="panel">
        <h2>总览仪表板</h2>
        <div class="metrics" id="metrics"></div>
      </section>

      <section class="panel">
        <h2>结构观察</h2>
        <div class="twocol">
          <div class="card">
            <h3>排名表现</h3>
            <div class="subtle">只统计存在 `return_pct` 的真实收益记录。</div>
            <div class="bars" id="rank-bars"></div>
          </div>
          <div class="card">
            <h3>收益口径状态</h3>
            <div class="subtle">把未开仓、未平仓和缺失平仓成交单独拆开看。</div>
            <div class="basis-list" id="basis-bars"></div>
          </div>
        </div>
      </section>

      <section class="panel">
        <h2>账号 x 排名热力图</h2>
        <div class="subtle">颜色表示平均收益率，卡片脚注是可计算收益率的样本数。</div>
        <div class="heatmap" id="heatmap"></div>
      </section>

      <section class="panel">
        <h2>账号日度趋势</h2>
        <div class="twocol">
          <div class="card">
            <h3>账号平均收益率</h3>
            <div class="trend-list" id="account-trends"></div>
          </div>
          <div class="card">
            <h3>最近 12 天</h3>
            <div class="subtle">按天聚合每个账号有收益率的记录均值。</div>
            <div class="table-shell">
              <table class="detail">
                <thead>
                  <tr><th>日期</th><th>账号</th><th>样本</th><th>平均收益率</th></tr>
                </thead>
                <tbody id="daily-trend-table"></tbody>
              </table>
            </div>
          </div>
        </div>
      </section>

      <section class="panel">
        <h2>明细穿透</h2>
        <div class="filters">
          <div class="field"><label>账号</label><select id="filter-account"></select></div>
          <div class="field"><label>排名</label><select id="filter-rank"></select></div>
          <div class="field"><label>收益口径</label><select id="filter-basis"></select></div>
          <div class="field"><label>开始日期</label><input id="filter-start" type="date"></div>
          <div class="field"><label>结束日期</label><input id="filter-end" type="date"></div>
          <div class="field"><label>关键词</label><input id="filter-keyword" type="text" placeholder="symbol / 状态"></div>
        </div>
        <div class="detail-summary" id="detail-summary"></div>
        <div class="table-shell">
          <table class="detail">
            <thead>
              <tr>
                <th>日期</th>
                <th>账号</th>
                <th>排名</th>
                <th>Symbol</th>
                <th>榜单涨幅</th>
                <th>收益率</th>
                <th>仓位状态</th>
                <th>收益口径</th>
                <th>开仓价</th>
                <th>平仓均价</th>
              </tr>
            </thead>
            <tbody id="detail-body"></tbody>
          </table>
        </div>
      </section>
    </div>
  </div>

  <script>
    const data = __DATA__;
    const pct = (v) => {
      if (v === null || v === undefined || Number.isNaN(v)) return "—";
      return `${v >= 0 ? "+" : ""}${v.toFixed(2)}%`;
    };
    const num = (v) => (v === null || v === undefined || Number.isNaN(v) ? "—" : Number(v).toFixed(4));
    const cls = (v) => {
      if (v === null || v === undefined || Number.isNaN(v)) return "muted";
      return v > 0 ? "pos" : (v < 0 ? "neg" : "muted");
    };

    document.getElementById("hero-copy").textContent =
      `共 ${data.overview.total_rows} 条记录，其中 ${data.overview.realized_rows} 条可直接计算真实收益率。` +
      ` 页面把排名效果、账号差异和缺失口径分开展示，避免把空收益率误读成 0。`;

    const metrics = [
      ["总记录", data.overview.total_rows, `${data.overview.account_count} 个账号，${data.overview.date_count} 个交易日`],
      ["真实收益记录", data.overview.realized_rows, `占比 ${pct(data.overview.realized_ratio_pct)}`],
      ["未开仓", data.overview.not_opened_rows, `占比 ${pct(data.overview.not_opened_ratio_pct)}`],
      ["仍未闭合/缺失平仓", data.overview.non_realized_rows, `中位收益率 ${pct(data.overview.median_return_pct)}`],
    ];
    document.getElementById("metrics").innerHTML = metrics.map(([label, value, note]) => `
      <article class="metric">
        <div class="label">${label}</div>
        <div class="value">${value}</div>
        <div class="note">${note}</div>
      </article>
    `).join("");

    const maxAbsRankAvg = Math.max(...data.rank_summary.map((item) => Math.abs(item.avg_return_pct || 0)), 1);
    document.getElementById("rank-bars").innerHTML = data.rank_summary.map((item) => `
      <div class="bar-row">
        <strong>#${item.rank}</strong>
        <div class="track"><div class="fill" style="width:${Math.max(Math.abs(item.avg_return_pct || 0) / maxAbsRankAvg * 100, 2)}%"></div></div>
        <span class="${cls(item.avg_return_pct)}">${pct(item.avg_return_pct)}</span>
      </div>
    `).join("");

    const basisMax = Math.max(...data.return_basis_summary.map((item) => item.count), 1);
    document.getElementById("basis-bars").innerHTML = data.return_basis_summary.map((item) => `
      <div class="basis-row">
        <strong>${item.label}</strong>
        <div class="track"><div class="fill" style="width:${item.count / basisMax * 100}%"></div></div>
        <span>${item.count}</span>
      </div>
    `).join("");

    const heatColor = (value) => {
      if (value === null || value === undefined) return "rgba(141,131,121,0.58)";
      if (value >= 0) {
        const alpha = Math.min(0.88, 0.28 + Math.abs(value) / 20);
        return `rgba(15,139,109,${alpha})`;
      }
      const alpha = Math.min(0.88, 0.28 + Math.abs(value) / 20);
      return `rgba(191,77,59,${alpha})`;
    };
    document.getElementById("heatmap").innerHTML = `
      <table>
        <thead>
          <tr>
            <th>账号</th>
            ${data.ranks.map((rank) => `<th>#${rank}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
          ${data.account_rank_heatmap.map((row) => `
            <tr>
              <th>${row.account_id}</th>
              ${row.cells.map((cell) => `
                <td>
                  <div class="heat-cell" style="background:${heatColor(cell.avg_return_pct)}">
                    ${pct(cell.avg_return_pct)}
                    <span class="heat-note">${cell.sample_count} 条</span>
                  </div>
                </td>
              `).join("")}
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;

    const trendMin = Math.min(...data.account_trends.map((item) => item.avg_return_pct ?? 0), -1);
    const trendMax = Math.max(...data.account_trends.map((item) => item.avg_return_pct ?? 0), 1);
    const trendSpan = Math.max(trendMax - trendMin, 1);
    document.getElementById("account-trends").innerHTML = data.account_trends.map((item) => {
      const left = ((item.avg_return_pct - trendMin) / trendSpan) * 100;
      return `
        <div class="trend-row">
          <strong>${item.account_id}</strong>
          <div class="dotline"><span class="dot" style="left:${left}%"></span></div>
          <span class="${cls(item.avg_return_pct)}">${pct(item.avg_return_pct)}</span>
        </div>
      `;
    }).join("");

    document.getElementById("daily-trend-table").innerHTML = data.daily_trend_rows.map((item) => `
      <tr>
        <td>${item.date}</td>
        <td>${item.account_id}</td>
        <td>${item.sample_count}</td>
        <td class="${cls(item.avg_return_pct)}">${pct(item.avg_return_pct)}</td>
      </tr>
    `).join("");

    const detailRows = data.detail_rows;
    const fillSelect = (id, values, allLabel) => {
      const node = document.getElementById(id);
      node.innerHTML = [`<option value="">${allLabel}</option>`]
        .concat(values.map((value) => `<option value="${value}">${value}</option>`))
        .join("");
    };
    fillSelect("filter-account", data.accounts, "全部账号");
    fillSelect("filter-rank", data.ranks.map(String), "全部排名");
    fillSelect("filter-basis", data.return_basis_summary.map((item) => item.key), "全部口径");

    const accountFilter = document.getElementById("filter-account");
    const rankFilter = document.getElementById("filter-rank");
    const basisFilter = document.getElementById("filter-basis");
    const startFilter = document.getElementById("filter-start");
    const endFilter = document.getElementById("filter-end");
    const keywordFilter = document.getElementById("filter-keyword");
    startFilter.value = data.date_range.min;
    endFilter.value = data.date_range.max;

    const renderDetailTable = () => {
      const keyword = keywordFilter.value.trim().toUpperCase();
      const rows = detailRows.filter((row) => {
        if (accountFilter.value && row.account_id !== accountFilter.value) return false;
        if (rankFilter.value && String(row.rank) !== rankFilter.value) return false;
        if (basisFilter.value && row.return_basis !== basisFilter.value) return false;
        if (startFilter.value && row.ranking_local_date < startFilter.value) return false;
        if (endFilter.value && row.ranking_local_date > endFilter.value) return false;
        if (keyword) {
          const hay = `${row.symbol} ${row.position_status || ""} ${row.return_basis}`.toUpperCase();
          if (!hay.includes(keyword)) return false;
        }
        return true;
      });
      document.getElementById("detail-summary").textContent =
        `当前筛选命中 ${rows.length} 条；其中可计算真实收益率 ${rows.filter((row) => row.return_pct !== null).length} 条。`;
      document.getElementById("detail-body").innerHTML = rows.map((row) => `
        <tr>
          <td>${row.ranking_local_date}</td>
          <td>${row.account_id}</td>
          <td>#${row.rank}</td>
          <td>${row.symbol}</td>
          <td class="${cls(row.rank_pct_change)}">${pct(row.rank_pct_change)}</td>
          <td class="${cls(row.return_pct)}">${pct(row.return_pct)}</td>
          <td>${row.position_status || "—"}</td>
          <td>${row.return_basis_label}</td>
          <td>${num(row.entry_price)}</td>
          <td>${num(row.exit_fill_avg_price)}</td>
        </tr>
      `).join("");
    };
    [accountFilter, rankFilter, basisFilter, startFilter, endFilter, keywordFilter].forEach((node) => {
      node.addEventListener("input", renderDetailTable);
      node.addEventListener("change", renderDetailTable);
    });
    renderDetailTable();
  </script>
</body>
</html>
"""


RETURN_BASIS_LABELS = {
    "entry_to_exit_fill": "已计算真实收益",
    "rank_symbol_not_opened": "榜单币未开仓",
    "open_position_no_exit_fill": "仍未平仓",
    "missing_exit_fill": "缺少平仓成交",
}


def parse_float(value: str) -> Optional[float]:
    text = (value or "").strip()
    if not text:
        return None
    return float(text)


def load_rows(csv_path: Path) -> List[Dict[str, object]]:
    with csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows: List[Dict[str, object]] = []
        for raw in reader:
            row = {
                "ranking_local_date": raw["ranking_local_date"],
                "account_id": raw["account_id"],
                "rank": int(raw["rank"]),
                "symbol": raw["symbol"],
                "rank_pct_change": parse_float(raw["rank_pct_change"]),
                "position_status": (raw.get("position_status") or "").strip(),
                "entry_price": parse_float(raw.get("entry_price", "")),
                "exit_fill_avg_price": parse_float(raw.get("exit_fill_avg_price", "")),
                "return_pct": parse_float(raw.get("return_pct", "")),
                "return_basis": raw["return_basis"],
            }
            row["return_basis_label"] = RETURN_BASIS_LABELS.get(row["return_basis"], row["return_basis"])
            rows.append(row)
    return rows


def avg(values: List[float]) -> Optional[float]:
    return mean(values) if values else None


def med(values: List[float]) -> Optional[float]:
    return median(values) if values else None


def build_payload(rows: List[Dict[str, object]]) -> Dict[str, object]:
    accounts = sorted({str(row["account_id"]) for row in rows})
    ranks = sorted({int(row["rank"]) for row in rows})
    dates = sorted({str(row["ranking_local_date"]) for row in rows})
    realized_rows = [row for row in rows if row["return_pct"] is not None]
    not_opened_rows = [row for row in rows if row["return_basis"] == "rank_symbol_not_opened"]
    non_realized_rows = [row for row in rows if row["return_pct"] is None and row["return_basis"] != "rank_symbol_not_opened"]

    rank_summary = []
    for rank in ranks:
        values = [float(row["return_pct"]) for row in realized_rows if row["rank"] == rank]
        rank_summary.append(
            {
                "rank": rank,
                "sample_count": len(values),
                "avg_return_pct": avg(values),
                "median_return_pct": med(values),
            }
        )

    basis_counter = Counter(str(row["return_basis"]) for row in rows)
    return_basis_summary = [
        {"key": key, "label": RETURN_BASIS_LABELS.get(key, key), "count": count}
        for key, count in basis_counter.most_common()
    ]

    heatmap = []
    for account_id in accounts:
        cells = []
        for rank in ranks:
            values = [
                float(row["return_pct"])
                for row in realized_rows
                if row["account_id"] == account_id and row["rank"] == rank
            ]
            cells.append(
                {
                    "rank": rank,
                    "sample_count": len(values),
                    "avg_return_pct": avg(values),
                }
            )
        heatmap.append({"account_id": account_id, "cells": cells})

    account_trends = []
    for account_id in accounts:
        values = [float(row["return_pct"]) for row in realized_rows if row["account_id"] == account_id]
        account_trends.append(
            {
                "account_id": account_id,
                "sample_count": len(values),
                "avg_return_pct": avg(values),
                "median_return_pct": med(values),
            }
        )

    daily_group: Dict[tuple[str, str], List[float]] = defaultdict(list)
    for row in realized_rows:
        daily_group[(str(row["ranking_local_date"]), str(row["account_id"]))].append(float(row["return_pct"]))
    daily_trend_rows = [
        {
            "date": date,
            "account_id": account_id,
            "sample_count": len(values),
            "avg_return_pct": avg(values),
        }
        for (date, account_id), values in sorted(daily_group.items(), reverse=True)
    ][:48]

    overview = {
        "total_rows": len(rows),
        "realized_rows": len(realized_rows),
        "non_realized_rows": len(non_realized_rows),
        "not_opened_rows": len(not_opened_rows),
        "account_count": len(accounts),
        "date_count": len(dates),
        "realized_ratio_pct": len(realized_rows) / len(rows) * 100 if rows else 0.0,
        "not_opened_ratio_pct": len(not_opened_rows) / len(rows) * 100 if rows else 0.0,
        "median_return_pct": med([float(row["return_pct"]) for row in realized_rows]) or 0.0,
    }

    detail_rows = sorted(
        rows,
        key=lambda row: (
            str(row["ranking_local_date"]),
            str(row["account_id"]),
            int(row["rank"]),
        ),
        reverse=True,
    )

    return {
        "overview": overview,
        "accounts": accounts,
        "ranks": ranks,
        "date_range": {"min": dates[0], "max": dates[-1]} if dates else {"min": "", "max": ""},
        "rank_summary": rank_summary,
        "return_basis_summary": return_basis_summary,
        "account_rank_heatmap": heatmap,
        "account_trends": account_trends,
        "daily_trend_rows": daily_trend_rows,
        "detail_rows": detail_rows,
    }


def render_html(payload: Dict[str, object]) -> str:
    return HTML_TEMPLATE.replace("__DATA__", json.dumps(payload, ensure_ascii=False))


def generate_report(csv_path: Path = DEFAULT_CSV, output_path: Path = DEFAULT_OUT) -> None:
    rows = load_rows(csv_path)
    payload = build_payload(rows)
    html = render_html(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def main() -> None:
    generate_report()
    print(f"wrote {DEFAULT_OUT}")


if __name__ == "__main__":
    main()
