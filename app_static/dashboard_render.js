/* Bubble Buster — Account Detail pure formatters, row renderers & chart option builder */
(function (global) {
  "use strict";

  var ops = global.BBOps;
  if (!ops) return;

  function txt(v) {
    return (v === null || v === undefined || v === "") ? "--" : String(v);
  }

  function escapeHtml(v) {
    return ops.escapeHtml(txt(v));
  }

  function trimFixed(value, digits) {
    var n = ops.toNum(value);
    if (n === null) return "--";
    return n.toFixed(digits).replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "");
  }

  function fmtAdaptive(value) {
    var n = ops.toNum(value);
    if (n === null) return "--";
    var magnitude = Math.abs(n);
    var digits = magnitude >= 100 ? 2 : (magnitude >= 1 ? 4 : (magnitude >= 0.01 ? 6 : 8));
    return trimFixed(n, digits);
  }

  function fmtQuantity(value) {
    var n = ops.toNum(value);
    if (n === null) return "--";
    var digits = Math.abs(n) >= 100 ? 2 : (Math.abs(n) >= 1 ? 4 : 8);
    return trimFixed(n, digits);
  }

  function notionalSourceLabel(value) {
    var labels = {
      EXCHANGE_NOTIONAL: "交易所实时",
      MARK_PRICE: "按标记价",
      ENTRY_PRICE_ESTIMATE: "按入场价估算"
    };
    return labels[String(value || "").toUpperCase()] || "";
  }

  function fmtAxisTime(isoText, timeZone) {
    return ops.formatBeijingTime(isoText, { timeZone: timeZone || "Asia/Shanghai" });
  }

  function fmtMetaTime(isoText, timeZone) {
    return ops.formatBeijingTime(isoText, { timeZone: timeZone || "Asia/Shanghai", withSeconds: true });
  }

  function fmtDateOnly(isoText) {
    var raw = txt(isoText);
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    return ops.formatBeijingTime(raw, { timeZone: "UTC", dateOnly: true });
  }

  function clsForStatus(status) {
    var s = (status || "").toUpperCase();
    if (s.indexOf("SUCCESS") >= 0 || s.indexOf("CLOSED_TP") >= 0) return "ok";
    if (s.indexOf("RUNNING") >= 0 || s.indexOf("SKIPPED") >= 0) return "warn";
    if (s.indexOf("FAILED") >= 0 || s.indexOf("ERROR") >= 0 || s.indexOf("CLOSED_SL") >= 0) return "bad";
    return "";
  }

  function statusLabel(status) {
    var s = String(status || "").toUpperCase();
    var labels = {
      SUCCESS: "成功",
      RUNNING: "运行中",
      FAILED: "失败",
      PARTIAL: "部分完成",
      SKIPPED: "已跳过",
      NEW: "挂单中",
      FILLED: "已成交",
      CANCELED: "已撤销",
      CANCELLED: "已撤销",
      EXPIRED: "已过期",
      REJECTED: "已拒绝",
      CLOSED_TP: "止盈平仓",
      CLOSED_SL: "止损平仓",
      CLOSED_TIMEOUT: "到期平仓",
      CLOSED_EXTERNAL: "外部平仓"
    };
    return labels[s] || s || "--";
  }

  function statusBadge(status) {
    var cls = clsForStatus(status);
    return '<span class="status-badge ' + cls + '">' + escapeHtml(statusLabel(status)) + "</span>";
  }

  function orderTypeLabel(value) {
    var s = String(value || "").toUpperCase();
    var labels = {
      MARKET: "市价",
      LIMIT: "限价",
      STOP: "止损",
      STOP_MARKET: "止损市价",
      TAKE_PROFIT: "止盈",
      TAKE_PROFIT_MARKET: "止盈市价"
    };
    return labels[s] || s || "--";
  }

  function sideLabel(value) {
    var s = String(value || "").toUpperCase();
    if (s === "BUY") return "买入";
    if (s === "SELL") return "卖出";
    if (s === "SHORT") return "空单";
    if (s === "LONG") return "多单";
    if (s === "BOTH") return "双向";
    return s || "--";
  }

  function liveOrderLabel(value) {
    var s = String(value || "").toUpperCase();
    var labels = {
      ACTIVE: "挂单中",
      NEW: "挂单中",
      PARTIALLY_FILLED: "部分成交",
      FILLED: "已成交",
      CANCELED: "已撤销",
      CANCELLED: "已撤销",
      EXPIRED: "已过期",
      REJECTED: "已拒绝",
      READ_ERROR: "读取失败",
      NOT_FOUND: "未发现",
      MISSING: "缺失",
      NOT_SET: "未设置"
    };
    return labels[s] || s || "待同步";
  }

  function liveOrderClass(value) {
    var s = String(value || "").toUpperCase();
    if (s === "NEW" || s === "ACTIVE" || s === "PARTIALLY_FILLED") return "ok";
    if (s === "READ_ERROR" || s === "MISSING" || s === "CANCELED" || s === "CANCELLED" || s === "EXPIRED" || s === "REJECTED") return "bad";
    return "warn";
  }

  function liveOrderBadge(value) {
    return '<span class="order-state ' + liveOrderClass(value) + '">' + escapeHtml(liveOrderLabel(value)) + "</span>";
  }

  function cashflowTypeLabel(value) {
    var s = String(value || "").toUpperCase();
    var labels = {
      TRANSFER: "划转",
      REALIZED_PNL: "已实现盈亏",
      COMMISSION: "手续费",
      FUNDING_FEE: "资金费",
      INSURANCE_CLEAR: "保险清算"
    };
    return labels[s] || s || "--";
  }

  function summarizeRunMessage(message) {
    var raw = String(message || "").trim();
    if (!raw) return "无执行摘要";
    var lower = raw.toLowerCase();
    if (lower.indexOf("all symbols already have open strategy positions") >= 0) {
      return "目标交易对均已有策略持仓（已跳过）";
    }
    if (lower.indexOf("no eligible symbols") >= 0) {
      return "无符合条件的入场交易对";
    }
    var values = {};
    raw.split(",").forEach(function (part) {
      var text = String(part || "").trim();
      var eq = text.indexOf("=");
      if (eq > 0) values[text.slice(0, eq)] = text.slice(eq + 1);
    });
    var items = [];
    if (values.opened !== undefined) items.push("开仓 " + values.opened);
    if (values.failed !== undefined) items.push("失败 " + values.failed);
    if (values.entry_failed !== undefined && Number(values.entry_failed) > 0) items.push("入场失败 " + values.entry_failed);
    if (values.skipped_existing !== undefined && Number(values.skipped_existing) > 0) items.push("已有持仓 " + values.skipped_existing);
    if (values.planned !== undefined) items.push("计划 " + values.planned);
    if (values.adjusted !== undefined) items.push("调整 " + values.adjusted);
    if (values.errors !== undefined && Number(values.errors) > 0) items.push("异常 " + values.errors);
    return items.length ? items.join(" · ") : raw.slice(0, 140);
  }

  function positionRowHtml(p, timeZone) {
    var errorText = String(p.last_error || "").trim();
    var liveAvailable = p.live_data_available === true;
    var readonlyLive = p.readonly_live === true;
    var liveError = String(p.live_data_error || p.order_data_error || p.margin_data_error || "").trim();
    var pnl = ops.toNum(p.unrealized_pnl);
    var pnlPct = ops.toNum(p.unrealized_pnl_pct);
    var notionalPnlPct = ops.toNum(p.unrealized_pnl_notional_pct);
    var marginPnlPct = ops.toNum(p.unrealized_pnl_margin_pct);
    var notional = ops.toNum(p.notional);
    var leverage = ops.toNum(p.leverage);
    var notionalSub = "剩余数量 " + fmtQuantity(p.qty);
    if (leverage !== null) notionalSub += " · 杠杆 " + fmtAdaptive(leverage) + "x";
    var notionalSource = notionalSourceLabel(p.notional_source);
    if (notionalSource) notionalSub += " · " + notionalSource;
    var pnlClass = pnl === null ? "neutral" : (pnl > 0 ? "positive" : (pnl < 0 ? "negative" : "neutral"));
    var pnlMain = pnl === null ? "实时不可用" : ops.fmtSignedUsdt(pnl, 2) + " USDT";
    var pnlSub = readonlyLive
      ? "名义收益率 " + (notionalPnlPct === null ? "--" : ops.fmtSigned(notionalPnlPct, 2) + "%")
        + " · 实际保证金收益率 " + (marginPnlPct === null ? "--" : ops.fmtSigned(marginPnlPct, 2) + "%")
      : (pnlPct === null ? "收益率 --" : "收益率 " + ops.fmtSigned(pnlPct, 2) + "%");
    var stateHtml = errorText
      ? '<span class="status-badge bad">异常</span><div class="error-cell">' + escapeHtml(errorText) + "</div>"
      : (readonlyLive
        ? '<span class="status-badge readonly">只读实时</span>'
        : '<span class="status-badge ok">正常</span>');
    if (liveError) {
      stateHtml += '<small class="live-unavailable">' + escapeHtml(liveError) + "</small>";
    } else if (liveAvailable) {
      stateHtml += '<small class="live-sync">' + (readonlyLive ? "交易所实时" : "实时已同步") + "</small>";
    }
    var sourceLabel = readonlyLive ? "交易所实时" : "#" + p.id;
    var protectionHtml = '<div class="exit-stack"><div class="exit-line"><span class="exit-key tp">TP</span><span class="exit-price">' + escapeHtml(fmtAdaptive(p.tp_price)) + "</span>" + liveOrderBadge(p.tp_order_status) + '</div><div class="exit-line"><span class="exit-key sl">SL</span><span class="exit-price">' + escapeHtml(fmtAdaptive(p.sl_price)) + "</span>" + liveOrderBadge(p.sl_order_status) + "</div></div>";
    if (readonlyLive) {
      protectionHtml += '<small class="readonly-protection-note">交易所订单读取</small>';
    }
    return (
      "<tr>"
      + '<th scope="row" class="symbol-cell"><strong>' + escapeHtml(p.symbol) + "</strong><small>" + escapeHtml(sideLabel(p.side)) + " · " + escapeHtml(sourceLabel) + "</small></th>"
      + '<td class="numeric"><div class="position-stack"><strong class="position-primary">' + escapeHtml(notional === null ? "--" : ops.fmtUsdt(notional, 2) + " USDT") + '</strong><small class="position-secondary">' + escapeHtml(notionalSub) + "</small></div></td>"
      + '<td class="numeric"><div class="position-stack"><strong class="position-primary">' + escapeHtml(fmtAdaptive(p.entry_price)) + '</strong><small class="position-secondary">标记 <strong>' + escapeHtml(liveAvailable ? fmtAdaptive(p.mark_price) : "--") + "</strong></small></div></td>"
      + '<td class="numeric live-pnl ' + pnlClass + '"><div class="position-stack"><strong class="position-primary">' + escapeHtml(pnlMain) + '</strong><small class="position-secondary">' + escapeHtml(pnlSub) + "</small></div></td>"
      + "<td>" + protectionHtml + "</td>"
      + '<td class="numeric">' + escapeHtml(fmtAxisTime(p.expire_at_utc, timeZone)) + "</td>"
      + "<td>" + stateHtml + "</td>"
      + "</tr>"
    );
  }

  function runRowHtml(r, timeZone) {
    var rawMessage = String(r.message || "");
    return (
      "<tr>"
      + '<th scope="row" class="numeric">' + escapeHtml(fmtDateOnly(r.trade_day_utc)) + "</th>"
      + "<td>" + statusBadge(r.status) + "</td>"
      + '<td class="numeric">' + escapeHtml(fmtAxisTime(r.started_at_utc, timeZone)) + "</td>"
      + '<td class="message-cell"><div class="message-summary">' + escapeHtml(summarizeRunMessage(rawMessage)) + '</div><details class="row-details" data-detail-key="' + escapeHtml(r.trade_day_utc || "") + "_" + escapeHtml(r.started_at_utc || "") + '"><summary>原始记录</summary><div class="raw-message">' + escapeHtml(rawMessage) + "</div></details></td>"
      + "</tr>"
    );
  }

  function eventRowHtml(e, timeZone) {
    return (
      "<tr>"
      + '<th scope="row" class="numeric">' + escapeHtml(fmtAxisTime(e.event_time_utc, timeZone)) + "</th>"
      + "<td>" + escapeHtml(e.symbol) + "</td>"
      + "<td>" + escapeHtml(orderTypeLabel(e.type)) + "</td>"
      + "<td>" + escapeHtml(sideLabel(e.side)) + "</td>"
      + "<td>" + statusBadge(e.status) + "</td>"
      + '<td class="numeric cell-muted">#' + escapeHtml(e.id) + "</td>"
      + "</tr>"
    );
  }

  function cashflowRowHtml(c, timeZone) {
    var amount = ops.toNum(c.amount);
    var amountClass = amount === null ? "" : (amount > 0 ? "ok" : (amount < 0 ? "bad" : ""));
    return (
      "<tr>"
      + '<th scope="row" class="numeric">' + escapeHtml(fmtAxisTime(c.event_time_utc, timeZone)) + "</th>"
      + "<td>" + escapeHtml(cashflowTypeLabel(c.income_type)) + "</td>"
      + '<td class="numeric ' + amountClass + '">' + escapeHtml(ops.fmtSignedUsdt(c.amount, 4)) + "</td>"
      + "<td>" + escapeHtml(c.asset) + "</td>"
      + "<td>" + escapeHtml(c.symbol) + "</td>"
      + '<td class="numeric cell-muted">#' + escapeHtml(c.id) + "</td>"
      + "</tr>"
    );
  }

  function unpricedRowHtml(u, timeZone) {
    return (
      "<tr>"
      + '<th scope="row" class="symbol-cell"><strong>' + escapeHtml(u.symbol) + "</strong><small>#" + escapeHtml(u.id) + "</small></th>"
      + "<td>" + statusBadge(u.status) + "</td>"
      + "<td>" + escapeHtml(u.close_reason) + "</td>"
      + "<td>" + escapeHtml(u.detected_reason) + "</td>"
      + '<td class="numeric">' + escapeHtml(u.close_order_id) + "</td>"
      + '<td class="numeric">' + escapeHtml(fmtAxisTime(u.closed_at_utc, timeZone)) + "</td>"
      + "</tr>"
    );
  }

  function readonlyStatsFactsHtml(stats) {
    var rows = [
      ["胜率", stats.win_rate_pct != null ? ops.fmtNum(stats.win_rate_pct, 1) + "%" : "--", ""],
      ["盈亏比", stats.profit_factor != null ? ops.fmtNum(stats.profit_factor, 2) : "--", ""],
      ["平仓订单数", stats.total_trades != null ? ops.fmtNum(stats.total_trades, 0) + " 笔" : "--", ""],
      ["胜 / 负场次", (stats.win_count != null ? ops.fmtNum(stats.win_count, 0) : "--") + " / " + (stats.loss_count != null ? ops.fmtNum(stats.loss_count, 0) : "--"), ""],
      ["平均盈利", stats.avg_win != null ? "+" + ops.fmtUsdt(Math.abs(stats.avg_win), 2) + " USDT" : "--", "ok"],
      ["平均亏损", stats.avg_loss != null ? "-" + ops.fmtUsdt(Math.abs(stats.avg_loss), 2) + " USDT" : "--", "bad"]
    ];
    return rows.map(function (r) {
      return '<div class="risk-fact"><span class="risk-fact-label">' + ops.escapeHtml(r[0]) + '</span><strong class="risk-fact-value ' + r[2] + '">' + ops.escapeHtml(r[1]) + "</strong></div>";
    }).join("");
  }

  function drawdownStatsFactsHtml(stats) {
    var s = stats || {};
    var rows = [
      ["窗口权益变化", ops.fmtWithUnit(s.total_realized_pnl, ops.fmtSignedUsdt, 2, " USDT")],
      ["全部权益变化", ops.fmtWithUnit(s.all_time_account_pnl, ops.fmtSignedUsdt, 2, " USDT")],
      ["窗口最大回撤", ops.fmtWithUnit(s.max_drawdown_pct, ops.fmtNum, 2, "%")],
      ["窗口出入金", ops.fmtWithUnit(s.net_cashflow_usdt, ops.fmtSignedUsdt, 2, " USDT")]
    ];
    return rows.map(function (r) {
      return '<div class="risk-fact"><span class="risk-fact-label">' + ops.escapeHtml(r[0]) + '</span><strong class="risk-fact-value">' + ops.escapeHtml(r[1]) + "</strong></div>";
    }).join("");
  }

  function buildEquityChartOption(params) {
    var seriesData = params.seriesData || [];
    var markLines = params.markLines || [];
    var theme = params.theme || ops.getChartTheme();
    var timeZone = params.timeZone || "Asia/Shanghai";
    var lineColor = params.lineColor || theme.ok;
    var areaTop = params.areaTop || theme.areaOk;
    var areaBottom = params.areaBottom || theme.areaFade;
    var echartsGraphic = params.echartsGraphic || null;

    var yMin = Infinity;
    var yMax = -Infinity;
    for (var k = 0; k < seriesData.length; k += 1) {
      if (seriesData[k][1] < yMin) yMin = seriesData[k][1];
      if (seriesData[k][1] > yMax) yMax = seriesData[k][1];
    }
    for (var m = 0; m < markLines.length; m += 1) {
      var mlVal = Number(markLines[m].yAxis);
      if (Number.isFinite(mlVal)) {
        if (mlVal < yMin) yMin = mlVal;
        if (mlVal > yMax) yMax = mlVal;
      }
    }
    var yAxisCfg = {
      type: "value",
      scale: true,
      axisLabel: { color: theme.muted, fontSize: 12, fontFamily: theme.fontMono, formatter: function (value) { return fmtAdaptive(value); } },
      axisLine: { show: false },
      splitLine: { lineStyle: { color: theme.splitLine, type: "dashed" } }
    };
    if (Number.isFinite(yMin) && Number.isFinite(yMax) && (seriesData.length <= 2 || markLines.length > 0)) {
      var rawRange = Math.max(yMax - yMin, Math.abs(yMax || yMin) * 0.02, 1);
      var pad = rawRange * 0.28;
      if (rawRange >= 2) {
        yAxisCfg.min = Math.max(0, Math.floor(yMin - pad));
        yAxisCfg.max = Math.ceil(yMax + pad);
      } else {
        yAxisCfg.min = Math.max(0, Number((yMin - pad).toFixed(2)));
        yAxisCfg.max = Number((yMax + pad).toFixed(2));
      }
    }

    var showSlider = seriesData.length > 24;
    var zoomStart = Number.isFinite(Number(params.zoomStart)) ? Number(params.zoomStart) : 0;
    var zoomEnd = Number.isFinite(Number(params.zoomEnd)) ? Number(params.zoomEnd) : 100;
    var areaColor = echartsGraphic && echartsGraphic.LinearGradient
      ? new echartsGraphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: areaTop },
          { offset: 1, color: areaBottom }
        ])
      : areaTop;

    return {
      animation: false,
      grid: { left: 16, right: 24, top: 20, bottom: showSlider ? 68 : 36, containLabel: true },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "line", lineStyle: { color: theme.borderStrong } },
        backgroundColor: theme.surfaceTooltip,
        borderColor: theme.borderStrong,
        borderWidth: 1,
        extraCssText: "box-shadow: " + theme.shadowOverlay + "; font-variant-numeric: tabular-nums;",
        textStyle: { color: theme.text, fontSize: 12, fontFamily: theme.fontMono },
        formatter: function (items) {
          if (!items || !items.length) return "";
          var v = items[0].value || [];
          return [
            '<div style="margin-bottom:4px;color:' + theme.muted + '">' + escapeHtml(fmtAxisTime(v[0], timeZone)) + "</div>",
            "<div>权益：<strong>" + escapeHtml(ops.fmtUsdt(v[1], 2)) + " USDT</strong></div>",
            "<div>窗口变化：" + escapeHtml(ops.fmtSigned(v[2], 2)) + " USDT</div>",
            "<div>回撤：" + escapeHtml(ops.fmtNum(v[3], 2)) + "%</div>"
          ].join("");
        }
      },
      dataZoom: showSlider ? [{
        type: "slider",
        xAxisIndex: 0,
        start: zoomStart,
        end: zoomEnd,
        height: 22,
        bottom: 8,
        borderColor: theme.border,
        backgroundColor: theme.surfaceInset,
        fillerColor: theme.zoomFill,
        handleStyle: { color: theme.accent, borderColor: theme.accent },
        textStyle: { color: theme.muted, fontSize: 12 }
      }] : [],
      xAxis: {
        type: "time",
        boundaryGap: false,
        axisLabel: {
          color: theme.muted,
          hideOverlap: true,
          alignMinLabel: "left",
          alignMaxLabel: "right",
          fontSize: 12,
          fontFamily: theme.fontMono,
          margin: 10,
          formatter: function (value) { return fmtAxisTime(value, timeZone); }
        },
        axisLine: { lineStyle: { color: theme.border } },
        axisTick: { show: false }
      },
      yAxis: yAxisCfg,
      series: [
        {
          name: "权益",
          type: "line",
          showSymbol: seriesData.length <= 2,
          symbolSize: 7,
          smooth: false,
          data: seriesData,
          itemStyle: { color: lineColor },
          lineStyle: { width: 2.2, color: lineColor },
          areaStyle: { color: areaColor },
          markLine: markLines.length ? {
            silent: true,
            symbol: ["none", "none"],
            data: markLines
          } : undefined
        }
      ]
    };
  }

  global.BBDashboardRender = {
    txt: txt,
    escapeHtml: escapeHtml,
    trimFixed: trimFixed,
    fmtAdaptive: fmtAdaptive,
    fmtQuantity: fmtQuantity,
    notionalSourceLabel: notionalSourceLabel,
    fmtAxisTime: fmtAxisTime,
    fmtMetaTime: fmtMetaTime,
    fmtDateOnly: fmtDateOnly,
    clsForStatus: clsForStatus,
    statusLabel: statusLabel,
    statusBadge: statusBadge,
    orderTypeLabel: orderTypeLabel,
    sideLabel: sideLabel,
    liveOrderLabel: liveOrderLabel,
    liveOrderClass: liveOrderClass,
    liveOrderBadge: liveOrderBadge,
    cashflowTypeLabel: cashflowTypeLabel,
    summarizeRunMessage: summarizeRunMessage,
    positionRowHtml: positionRowHtml,
    runRowHtml: runRowHtml,
    eventRowHtml: eventRowHtml,
    cashflowRowHtml: cashflowRowHtml,
    unpricedRowHtml: unpricedRowHtml,
    readonlyStatsFactsHtml: readonlyStatsFactsHtml,
    drawdownStatsFactsHtml: drawdownStatsFactsHtml,
    buildEquityChartOption: buildEquityChartOption
  };
})(typeof window !== "undefined" ? window : globalThis);
