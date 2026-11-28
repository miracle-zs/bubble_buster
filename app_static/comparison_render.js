/* Bubble Buster — Account Comparison pure formatters, comparators, HTML builders & ECharts option builder */
(function (global) {
  "use strict";

  var ops = global.BBOps;
  if (!ops) return;

  var LINE_PATTERNS = [
    { type: "solid", swatchClass: "" },
    { type: "dashed", swatchClass: "is-dashed" },
    { type: "dotted", swatchClass: "is-dotted" },
    { type: [8, 3, 2, 3], swatchClass: "is-dashdot" }
  ];

  var SORT_LABELS = {
    account_id: "账户",
    change_pct: "区间收益率",
    latest_equity: "当前权益",
    max_drawdown_pct: "区间最大回撤"
  };

  var NAMED_ACCOUNT_SLOTS = { acc01: 0, acc02: 1, acc03: 2, acc04: 3 };

  function compareText(a, b) {
    return ops.compareText(a, b);
  }

  function accountSlot(accountId, index) {
    return Object.prototype.hasOwnProperty.call(NAMED_ACCOUNT_SLOTS, accountId)
      ? NAMED_ACCOUNT_SLOTS[accountId]
      : index;
  }

  function colorFor(accountId, index, isReadonly) {
    var theme = ops.getChartTheme();
    if (isReadonly) return theme.readonly;
    var slot = accountSlot(accountId, index);
    return theme.series[slot % theme.series.length];
  }

  function patternFor(accountId, index) {
    var slot = accountSlot(accountId, index);
    return LINE_PATTERNS[slot % LINE_PATTERNS.length];
  }

  function formatLocalTime(value, withSeconds) {
    return ops.formatBeijingTime(value, { withYear: true, withSeconds: !!withSeconds });
  }

  function rangeLabel(range) {
    if (range === "all") return "上线以来";
    var hours = Number(range);
    if (hours < 48) return "最近 24 小时";
    if (hours < 168) return "最近 3 天";
    if (hours < 720) return "最近 7 天";
    return "最近 30 天";
  }

  function computeRowMaxDrawdownPct(row) {
    var pts = (row && row.points) || [];
    var peak = -Infinity;
    var maxDd = 0;
    for (var i = 0; i < pts.length; i += 1) {
      var eq = Number(pts[i] && pts[i].equity);
      if (!Number.isFinite(eq) || eq <= 0) continue;
      if (eq > peak) peak = eq;
      if (peak > 0) {
        var dd = ((eq - peak) / peak) * 100;
        if (dd < maxDd) maxDd = dd;
      }
    }
    return peak > 0 ? maxDd : null;
  }

  function buildSummaryCardHtml(row, index, opts) {
    var options = opts || {};
    var isReadonly = !!options.isReadonly;
    var color = colorFor(row.account_id, index, isReadonly);
    var change = row.change_pct;
    var changeClass = change == null ? "" : (Number(change) >= 0 ? "is-positive" : "is-negative");
    var status = "暂无数据";
    if (row.status === "OK") {
      var rowFreshness = ops.evaluateDataFreshness(row.latest_at_utc, { warnSec: 180, staleSec: 900 });
      if (rowFreshness.state === "live") status = "实时快照";
      else if (rowFreshness.state === "warn") status = "延迟快照";
      else if (rowFreshness.state === "offline") status = "离线";
      else status = "历史快照";
    }
    var pathPrefix = options.pathPrefix || "";
    var detailHref = pathPrefix + "/account/" + encodeURIComponent(row.account_id) + "/";
    var note = options.note || "";
    var allNotes = options.allNotes || [];
    var strategyHtml = note ? ops.formatStrategyHighlights(note, allNotes, 2, "summary-strategy-tag") : "";
    return '<article class="summary-card' + (isReadonly ? " is-readonly" : "") + '" style="--account-color:' + color + '">'
      + '<div class="summary-head"><a class="account-label" href="' + ops.escapeHtml(detailHref) + '" aria-label="查看 ' + ops.escapeHtml(row.account_id) + ' 账户详情"><i class="account-dot" aria-hidden="true"></i><span>' + ops.escapeHtml(row.account_id) + '</span><span class="account-jump" aria-hidden="true">' + ops.ARROW_ICON + '</span></a><span class="summary-status">' + status + '</span></div>'
      + '<div class="summary-change ' + changeClass + '">' + ops.fmtPct(change) + '</div>'
      + '<div class="summary-equity">当前权益 ' + (row.latest_equity == null ? "--" : ops.fmtUsdt(row.latest_equity, 2) + " USDT") + '</div>'
      + (strategyHtml ? '<div class="summary-strategy">' + strategyHtml + '</div>' : "")
      + '<div class="summary-range"><span>区间低点 <strong>' + ops.fmtPct(row.min_change_pct) + '</strong></span><span>高点 <strong>' + ops.fmtPct(row.max_change_pct) + '</strong></span></div>'
      + "</article>";
  }

  function buildLegendHtml(rows, opts) {
    var options = opts || {};
    var visible = options.visible || {};
    var isReadonlyFn = options.isReadonlyFn || function () { return false; };
    return (rows || []).map(function (row, index) {
      var hasSeries = row.status === "OK" && row.points && row.points.length > 0;
      var muted = !hasSeries || visible[row.account_id] === false;
      var isReadonly = !!isReadonlyFn(row.account_id);
      var color = colorFor(row.account_id, index, isReadonly);
      var pat = patternFor(row.account_id, index);
      return '<button class="legend-button' + (muted ? " is-muted" : "") + (!hasSeries ? " is-no-data" : "") + (isReadonly ? " is-readonly" : "") + '" data-account="' + ops.escapeHtml(row.account_id) + '" aria-pressed="' + (hasSeries && !muted) + '"' + (hasSeries ? "" : " disabled") + ' style="--account-color:' + color + '" type="button">'
        + '<i class="legend-swatch ' + pat.swatchClass + '" aria-hidden="true"></i><span>' + ops.escapeHtml(row.account_id) + '</span><span class="legend-value">' + (hasSeries ? ops.fmtPct(row.change_pct) : "暂无数据") + "</span></button>";
    }).join("");
  }

  function sortAndEnrichComparisonRows(rows, sortState) {
    var enriched = (rows || []).map(function (row, idx) {
      return {
        row: row,
        index: idx,
        account_id: String(row.account_id || ""),
        change_pct: ops.toNum(row.change_pct),
        latest_equity: ops.toNum(row.latest_equity),
        min_change_pct: ops.toNum(row.min_change_pct),
        max_change_pct: ops.toNum(row.max_change_pct),
        max_drawdown_pct: computeRowMaxDrawdownPct(row)
      };
    });
    var state = sortState || { key: "change_pct", dir: "desc" };
    var isDesc = state.dir !== "asc";
    enriched.sort(function (a, b) {
      var k = state.key;
      if (k === "account_id") return compareText(a.account_id, b.account_id) * (isDesc ? -1 : 1);
      var cmp = ops.compareNullableNumbers(a[k], b[k], isDesc);
      return cmp !== 0 ? cmp : compareText(a.account_id, b.account_id);
    });
    return enriched;
  }

  function buildMetricsRowHtml(item, opts) {
    var options = opts || {};
    var row = item.row;
    var isReadonly = !!options.isReadonly;
    var color = colorFor(row.account_id, item.index, isReadonly);
    var pathPrefix = options.pathPrefix || "";
    var detailHref = pathPrefix + "/account/" + encodeURIComponent(row.account_id) + "/";
    var changeCls = item.change_pct === null ? "" : (item.change_pct >= 0 ? "ok" : "bad");
    var ddCls = item.max_drawdown_pct === null ? "" : (item.max_drawdown_pct <= -2.5 ? "bad" : (item.max_drawdown_pct < 0 ? "warn" : "ok"));
    var note = options.note || "";
    var allNotes = options.allNotes || [];
    var strategyHtml = note
      ? ops.formatStrategyHighlights(note, allNotes, 2, "summary-strategy-tag")
      : '<span class="strategy-tag strategy-tag-empty">未配置</span>';
    return '<tr data-account-row="' + ops.escapeHtml(item.account_id) + '"' + (isReadonly ? ' class="is-readonly"' : "") + '>'
      + '<th scope="row"><a class="account-label" style="--account-color:' + color + '" href="' + detailHref + '"><i class="account-dot" aria-hidden="true"></i>' + ops.escapeHtml(row.account_id) + "</a></th>"
      + '<td class="numeric ' + changeCls + '">' + ops.escapeHtml(ops.fmtPct(item.change_pct)) + "</td>"
      + '<td class="numeric">' + (item.latest_equity === null ? "--" : ops.escapeHtml(ops.fmtUsdt(item.latest_equity, 2))) + "</td>"
      + '<td class="numeric ' + ddCls + '">' + (item.max_drawdown_pct === null ? "--" : ops.escapeHtml(ops.fmtSigned(item.max_drawdown_pct, 2) + "%")) + "</td>"
      + '<td class="numeric">' + ops.escapeHtml(ops.fmtPct(item.min_change_pct) + " / " + ops.fmtPct(item.max_change_pct)) + "</td>"
      + '<td class="strategy-col"><div class="summary-strategy metrics-strategy-tags">' + strategyHtml + "</div></td>"
      + "</tr>";
  }

  function buildComparisonChartOption(opts) {
    var options = opts || {};
    var activeRows = options.activeRows || [];
    var mode = options.mode === "absolute" ? "absolute" : "relative";
    var range = options.range || "all";
    var isReadonlyFn = options.isReadonlyFn || function () { return false; };
    var theme = ops.getChartTheme();
    var totalPoints = activeRows.reduce(function (acc, r) { return acc + (r.points ? r.points.length : 0); }, 0);
    var showSlider = totalPoints > 40;

    var chartSeries = activeRows.map(function (row, index) {
      var color = colorFor(row.account_id, index, !!isReadonlyFn(row.account_id));
      var pat = patternFor(row.account_id, index);
      var seriesPoints = [];
      (row.points || []).forEach(function (point) {
        var tMs = ops.parseTimestampMs(point.t);
        if (!Number.isFinite(tMs)) return;
        var yVal = mode === "relative" ? ops.toNum(point.relative_pct) : ops.toNum(point.equity);
        var eqVal = ops.toNum(point.equity);
        seriesPoints.push([tMs, yVal, eqVal]);
      });
      return {
        name: row.account_id,
        type: "line",
        showSymbol: seriesPoints.length <= 2,
        smooth: false,
        connectNulls: true,
        data: seriesPoints,
        lineStyle: { color: color, width: 2.2, type: pat.type },
        itemStyle: { color: color },
        emphasis: { focus: "series", lineStyle: { width: 3.2 } },
        markLine: index === 0 ? {
          silent: true,
          symbol: ["none", "none"],
          lineStyle: { color: theme.borderStrong, type: "dashed", width: 1 },
          label: {
            position: "insideEndTop",
            distance: [6, 6],
            color: theme.textSecondary,
            backgroundColor: theme.surfaceTooltip,
            borderColor: theme.border,
            borderWidth: 1,
            borderRadius: 4,
            padding: [3, 7],
            fontSize: 12,
            fontFamily: theme.fontMono,
            formatter: range === "all" ? "0% 基准 · 上线后首快照" : "0% 区间基准"
          },
          data: mode === "relative" ? [{ yAxis: 0 }] : []
        } : undefined
      };
    });

    var reduceMotion = ops.prefersReducedMotion();
    var shouldAnimate = !!options.animate && !reduceMotion;
    var chrome = ops.buildEchartsChrome(theme, {
      showSlider: showSlider,
      zoomStart: options.zoomStart,
      zoomEnd: options.zoomEnd,
      sliderBottom: 10,
      zoomDataBackground: true,
      xSplitLine: true,
      yScale: mode === "absolute",
      formatXAxis: function (value) { return formatLocalTime(value, false); },
      formatYAxis: function (value) {
        return mode === "relative" ? (value >= 0 ? "+" : "") + Number(value).toFixed(1) + "%" : ops.fmtUsdt(value, 0);
      },
      tooltipFormatter: function (items) {
        if (!items || !items.length) return "";
        var html = '<div style="margin-bottom:6px;color:' + theme.muted + '">' + ops.escapeHtml(formatLocalTime(items[0].axisValue, true)) + "</div>";
        items.forEach(function (item) {
          var val = Array.isArray(item.value) ? item.value[1] : item.value;
          var equity = Array.isArray(item.value) ? item.value[2] : null;
          var suffix = mode === "relative" && equity != null ? " · " + ops.fmtUsdt(equity, 2) + " USDT" : "";
          html += '<div style="line-height:1.6"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + ops.escapeHtml(item.color) + ';margin-right:6px"></span>'
            + ops.escapeHtml(item.seriesName) + " <strong>"
            + (mode === "relative" ? ops.fmtPct(val) : ops.fmtUsdt(val, 2) + " USDT")
            + "</strong>" + suffix + "</div>";
        });
        return html;
      }
    });

    return {
      animation: shouldAnimate,
      animationDuration: shouldAnimate ? 280 : 0,
      animationEasing: "cubicOut",
      backgroundColor: "transparent",
      grid: { top: 32, right: 24, bottom: showSlider ? 78 : 48, left: 16, containLabel: true },
      dataZoom: chrome.dataZoom,
      tooltip: chrome.tooltip,
      xAxis: chrome.xAxis,
      yAxis: chrome.yAxis,
      series: chartSeries
    };
  }

  global.BBComparisonRender = {
    LINE_PATTERNS: LINE_PATTERNS,
    SORT_LABELS: SORT_LABELS,
    compareText: compareText,
    accountSlot: accountSlot,
    colorFor: colorFor,
    patternFor: patternFor,
    formatLocalTime: formatLocalTime,
    rangeLabel: rangeLabel,
    computeRowMaxDrawdownPct: computeRowMaxDrawdownPct,
    buildSummaryCardHtml: buildSummaryCardHtml,
    buildLegendHtml: buildLegendHtml,
    sortAndEnrichComparisonRows: sortAndEnrichComparisonRows,
    buildMetricsRowHtml: buildMetricsRowHtml,
    buildComparisonChartOption: buildComparisonChartOption
  };
})(typeof window !== "undefined" ? window : globalThis);
