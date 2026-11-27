/* Bubble Buster — Account Detail Dashboard controller (Obsidian Desk Terminal) */
(function () {
  "use strict";

  document.addEventListener("DOMContentLoaded", function () {
    var ops = window.BBOps;
    var R = window.BBDashboardRender;
    if (!ops || !R) return;

    var cfg = ops.readPageConfig("page-config");
    var REFRESH_SEC = Number(cfg.refreshSec) || 15;
    var ECHARTS_SRC = String(cfg.echartsSrc || "");
    var accountId = String(cfg.accountId || "");
    var accountMode = String(cfg.accountMode || "full").toLowerCase();
    var accountStrategyNote = String(cfg.strategyNote || "");
    var navAccounts = Array.isArray(cfg.navAccounts) ? cfg.navAccounts : [];
    var portfolioStopEnabled = !!cfg.portfolioStopEnabled;
    var portfolioStopPct = ops.toNum(cfg.portfolioStopPct) !== null ? ops.toNum(cfg.portfolioStopPct) : 3.5;
    var portfolioStopHour = ops.toNum(cfg.portfolioStopHour) !== null ? ops.toNum(cfg.portfolioStopHour) : 8;
    var portfolioStopMinute = ops.toNum(cfg.portfolioStopMinute) !== null ? ops.toNum(cfg.portfolioStopMinute) : 0;

    function detectMobile() {
      return !!(window.matchMedia && window.matchMedia("(max-width: 900px)").matches);
    }
    var isMobile = detectMobile();
    window.addEventListener("resize", function () {
      isMobile = detectMobile();
    }, { passive: true });

    var apiBase = ops.resolveBasePrefix();
    var api = accountId
      ? apiBase + "/api/account/" + encodeURIComponent(accountId)
      : apiBase + "/api/dashboard";

    var equityChart = null;
    var chartResizeObs = null;
    var chartZoomRange = null;
    var coreTimer = null;
    var curveTimer = null;
    var VALID_WINDOWS = { 1: true, 24: true, 168: true, 720: true, 8760: true };
    var VALID_ACTIVITIES = { runs: "activity-runs", events: "activity-events", cashflow: "activity-cashflow" };
    var initialQuery = ops.readQueryParams();
    var defaultCurveTab = accountMode === "readonly" ? "balance" : "strategy";
    var requestedTab = initialQuery.tab === "balance" ? "balance" : (initialQuery.tab === "strategy" ? "strategy" : defaultCurveTab);
    var currentCurveTab = accountMode === "readonly" ? "balance" : requestedTab;
    var requestedWindow = Number(initialQuery.window);
    var currentWindowHours = VALID_WINDOWS[requestedWindow] ? requestedWindow : 24;
    var defaultActivityKey = accountMode === "readonly" ? "events" : "runs";
    var currentActivityKey = VALID_ACTIVITIES[initialQuery.activity] ? initialQuery.activity : defaultActivityKey;

    var displayTimeZone = "Asia/Shanghai";
    var riskSnapshot = null;
    var latestData = null;
    var lastChartSignature = "";
    var refreshTick = 0;
    var fullLoadedOnce = false;
    var detailsLoaded = false;
    var detailsInFlight = false;
    var detailsRefreshPending = false;
    var detailsRequestToken = 0;
    var logsLoadedOnce = false;
    var manualRefreshPendingCount = 0;
    var cachedPositions = [];
    var cachedEvents = [];
    var positionSortKey = "default";
    var eventSearchQuery = "";
    var eventStatusFilter = "ALL";

    var el = {
      accountTitle: document.getElementById("accountTitle"),
      accountMode: document.getElementById("accountMode"),
      meta: document.getElementById("meta"),
      freshnessPill: document.getElementById("detailFreshnessPill"),
      freshnessText: document.getElementById("detailFreshnessText"),
      updatedAt: document.getElementById("updatedAt"),
      strategyTags: document.getElementById("strategyTags"),
      nextEntryLabel: document.getElementById("nextEntryLabel"),
      nextEntry: document.getElementById("nextEntry"),
      serviceState: document.getElementById("serviceState"),
      summaryRail: document.getElementById("summaryRail"),
      walletLabel: document.getElementById("walletLabel"),
      walletHelp: document.getElementById("walletHelp"),
      walletBalance: document.getElementById("walletBalance"),
      cycleReturn: document.getElementById("cycleReturn"),
      cycleReturnLabel: document.getElementById("cycleReturnLabel"),
      cycleReturnHelp: document.getElementById("cycleReturnHelp"),
      currentDrawdown: document.getElementById("currentDrawdown"),
      stopDistanceMetric: document.getElementById("stopDistanceMetric"),
      stopDistance: document.getElementById("stopDistance"),
      stopDistanceHelp: document.getElementById("stopDistanceHelp"),
      openCount: document.getElementById("openCount"),
      symbolCount: document.getElementById("symbolCount"),
      errorCount: document.getElementById("errorCount"),
      lastRunStatus: document.getElementById("lastRunStatus"),
      lastRunMetric: document.getElementById("lastRunMetric"),
      curveTitle: document.getElementById("curveTitle"),
      curveTabGroup: document.getElementById("curveTabGroup"),
      tabStrategy: document.getElementById("tabStrategy"),
      tabBalance: document.getElementById("tabBalance"),
      windowRow: document.getElementById("windowRow"),
      chartResetZoomBtn: document.getElementById("chartResetZoomBtn"),
      equityChart: document.getElementById("equityChart"),
      chartEmpty: document.getElementById("chartEmpty"),
      chartSparseHint: document.getElementById("chartSparseHint"),
      detailRefreshBtn: document.getElementById("detailRefreshBtn"),
      riskPanel: document.getElementById("riskPanel"),
      readonlyStatsPanel: document.getElementById("readonlyStatsPanel"),
      readonlyNetPnl: document.getElementById("readonlyNetPnl"),
      readonlyStatsSubtitle: document.getElementById("readonlyStatsSubtitle"),
      readonlyStatsFacts: document.getElementById("readonlyStatsFacts"),
      riskCycleStart: document.getElementById("riskCycleStart"),
      riskState: document.getElementById("riskState"),
      riskThreshold: document.getElementById("riskThreshold"),
      riskDescription: document.getElementById("riskDescription"),
      riskMeterLabel: document.getElementById("riskMeterLabel"),
      riskMeterTrack: document.getElementById("riskMeterTrack"),
      riskMeterFill: document.getElementById("riskMeterFill"),
      riskMeterLimit: document.getElementById("riskMeterLimit"),
      drawdownStats: document.getElementById("drawdownStats"),
      positionsCount: document.getElementById("positionsCount"),
      positionSortSelect: document.getElementById("positionSortSelect"),
      runsTab: document.getElementById("tab-runs"),
      eventsTab: document.getElementById("tab-events"),
      runsCount: document.getElementById("runsCount"),
      eventsCount: document.getElementById("eventsCount"),
      cashflowCount: document.getElementById("cashflowCount"),
      unpricedCount: document.getElementById("unpricedCount"),
      positionsTable: document.getElementById("positionsTable"),
      positionsBody: document.getElementById("positionsBody"),
      runsTable: document.getElementById("runsTable"),
      runsBody: document.getElementById("runsBody"),
      eventsTable: document.getElementById("eventsTable"),
      eventsBody: document.getElementById("eventsBody"),
      eventSearchInput: document.getElementById("eventSearchInput"),
      eventStatusSelect: document.getElementById("eventStatusSelect"),
      eventFilterCount: document.getElementById("eventFilterCount"),
      cashflowTable: document.getElementById("cashflowTable"),
      cashflowBody: document.getElementById("cashflowBody"),
      diagnosticsPanel: document.getElementById("diagnosticsPanel"),
      unpricedTable: document.getElementById("unpricedTable"),
      unpricedBody: document.getElementById("unpricedBody"),
      logTail: document.getElementById("logTail"),
      detailAlert: document.getElementById("detailAlert"),
      detailAlertText: document.getElementById("detailAlertText"),
      detailRetryBtn: document.getElementById("detailRetryBtn")
    };

    var detailErrors = { core: "", curve: "", details: "" };

    function setText(node, value) {
      if (node) node.textContent = value;
    }

    var txt = R.txt;
    var clsForStatus = R.clsForStatus;
    var statusLabel = R.statusLabel;

    function setDetailError(slot, message) {
      detailErrors[slot] = message || "";
      if (!el.detailAlert || !el.detailAlertText) return;
      var activeList = [detailErrors.core, detailErrors.curve, detailErrors.details].filter(Boolean);
      if (activeList.length) {
        el.detailAlertText.textContent = activeList.join("；");
        el.detailAlert.hidden = false;
      } else {
        el.detailAlertText.textContent = "";
        el.detailAlert.hidden = true;
      }
    }

    function fmtAxisTime(isoText) {
      return R.fmtAxisTime(isoText, displayTimeZone);
    }

    function fmtMetaTime(isoText) {
      return R.fmtMetaTime(isoText, displayTimeZone);
    }

    function applyAccountContext() {
      var displayId = accountId || "运行账户";
      var modeText = "完整策略";
      var modeClass = "mode-badge";
      if (accountMode === "readonly") {
        modeText = "只读监控";
        modeClass += " mode-readonly";
      } else if (accountMode === "loss_cut_only") {
        modeText = "止损监控";
        modeClass += " mode-loss-cut";
      }
      setText(el.accountTitle, displayId + " 账户详情");
      if (el.accountMode) {
        el.accountMode.textContent = modeText;
        el.accountMode.className = modeClass;
      }
      if (el.strategyTags) {
        el.strategyTags.innerHTML = ops.formatStrategyTags(accountStrategyNote, "strategy-chip");
      }

      var navItems = navAccounts.length
        ? navAccounts
        : (accountId ? [{ id: accountId, mode: accountMode }] : []);
      ops.initTopNav({
        basePrefix: apiBase,
        activePage: "account",
        activeAccountId: accountId,
        accounts: navItems
      });
      document.title = displayId + " · Bubble Buster";

      var cycleText = String(portfolioStopHour).padStart(2, "0") + ":" + String(portfolioStopMinute).padStart(2, "0");
      if (accountMode === "readonly") {
        setText(el.walletLabel, "账户余额 (USDT)");
        if (el.walletHelp) {
          el.walletHelp.textContent = "含未实现盈亏";
          el.walletHelp.hidden = false;
        }
        setText(el.nextEntryLabel, "监控模式");
        setText(el.nextEntry, "只读（无自动交易）");
        setText(el.cycleReturnLabel, "窗口收益");
        setText(el.cycleReturnHelp, "跟随所选曲线范围");
        if (el.stopDistanceMetric) el.stopDistanceMetric.hidden = true;
        if (el.lastRunMetric) {
          var lbl = el.lastRunMetric.querySelector(".summary-label");
          var hlp = el.lastRunMetric.querySelector(".summary-help");
          if (lbl) lbl.textContent = "近30日胜率";
          if (hlp) hlp.textContent = "等待交易统计";
        }
        if (el.summaryRail) el.summaryRail.classList.add("is-readonly-rail");
        if (el.riskPanel) el.riskPanel.hidden = true;
        if (el.readonlyStatsPanel) el.readonlyStatsPanel.hidden = false;
        if (el.curveTabGroup) el.curveTabGroup.hidden = true;
        if (el.tabStrategy) el.tabStrategy.hidden = true;
        if (el.runsTab) el.runsTab.hidden = true;
        if (el.diagnosticsPanel) el.diagnosticsPanel.hidden = true;
      } else {
        setText(el.walletLabel, "策略权益 (USDT)");
        if (el.walletHelp) {
          el.walletHelp.textContent = "";
          el.walletHelp.hidden = true;
        }
        setText(el.nextEntryLabel, "下次开单");
        setText(el.cycleReturnLabel, cycleText + " 以来收益");
        setText(el.cycleReturnHelp, "UTC+8 周期");
        setText(el.stopDistanceHelp, portfolioStopEnabled ? "距离 -" + portfolioStopPct + "% 止损阈值" : "组合止损未启用");
        setText(el.riskCycleStart, "每日 " + cycleText + " 切换账户周期 · 北京时间 UTC+8");
        if (el.riskThreshold) {
          el.riskThreshold.textContent = portfolioStopEnabled ? "-" + portfolioStopPct + "%" : "未启用";
          el.riskThreshold.classList.toggle("is-neutral", !portfolioStopEnabled);
        }
        setText(el.riskMeterLimit, portfolioStopEnabled ? "止损 -" + portfolioStopPct + "%" : "未启用");
        if (el.riskPanel) el.riskPanel.hidden = false;
        if (el.readonlyStatsPanel) el.readonlyStatsPanel.hidden = true;
      }
    }

    function renderRows(tableNode, tbodyNode, rows, mapper, emptyCols, emptyMessage) {
      if (!tbodyNode) return;
      if (!rows || rows.length === 0) {
        if (tableNode) tableNode.classList.add("is-empty");
        var emptyHtml = '<tr><td colspan="' + emptyCols + '" class="empty-row">' + ops.escapeHtml(emptyMessage || "暂无数据") + "</td></tr>";
        ops.patchHtmlIfChanged(tbodyNode, emptyHtml);
        return;
      }
      if (tableNode) tableNode.classList.remove("is-empty");

      var html = "";
      for (var i = 0; i < rows.length; i += 1) {
        html += mapper(rows[i]);
      }
      if (tbodyNode.__bbLastHtml === html) return;

      var openKeys = {};
      try {
        var openEls = tbodyNode.querySelectorAll("details[open][data-detail-key]");
        for (var k = 0; k < openEls.length; k += 1) {
          var key = openEls[k].getAttribute("data-detail-key");
          if (key) openKeys[key] = true;
        }
      } catch (e) {}

      tbodyNode.__bbLastHtml = html;
      tbodyNode.innerHTML = html;

      if (Object.keys(openKeys).length > 0) {
        try {
          var newDetails = tbodyNode.querySelectorAll("details[data-detail-key]");
          for (var j = 0; j < newDetails.length; j += 1) {
            var dKey = newDetails[j].getAttribute("data-detail-key");
            if (dKey && openKeys[dKey]) newDetails[j].setAttribute("open", "");
          }
        } catch (e) {}
      }
    }

    function curvePointsForWindow(hours) {
      var h = Number(hours);
      if (!Number.isFinite(h) || h <= 0) return 600;
      var estimatedPoints = Math.ceil(h * 60);
      var capped = Math.max(600, Math.min(5000, estimatedPoints));
      if (isMobile) return Math.max(320, Math.min(1200, Math.ceil(capped * 0.5)));
      return capped;
    }

    var fetchControllers = {};

    function fetchDashboard(apiUrl, options, callback) {
      var opts = options || {};
      var kind = opts.kind || "full";
      var q = ["_=" + encodeURIComponent(String(Date.now()))];
      if (kind === "curve" || kind === "full") {
        var curvePoints = curvePointsForWindow(currentWindowHours);
        if (!opts.lite && currentWindowHours >= 24) {
          curvePoints = Math.max(curvePoints, 1200);
        }
        q.push("window_hours=" + encodeURIComponent(String(currentWindowHours)));
        q.push("curve_points=" + encodeURIComponent(String(curvePoints)));
      }
      if (kind === "details" || kind === "full") {
        var logLines = opts.includeLogs ? 80 : 0;
        q.push("log_lines=" + encodeURIComponent(String(logLines)));
      }
      if (opts.curveType === "strategy" || opts.curveType === "balance" || opts.curveType === "both") {
        q.push("curve_type=" + encodeURIComponent(opts.curveType));
      }
      var targetUrl = apiUrl + (apiUrl.indexOf("?") >= 0 ? "&" : "?") + q.join("&");
      var reqKey = apiUrl.split("?")[0];

      if (fetchControllers[reqKey]) {
        try { fetchControllers[reqKey].superseded = true; fetchControllers[reqKey].controller.abort(); } catch (e) {}
      }
      var controller = (typeof AbortController !== "undefined") ? new AbortController() : null;
      var reqState = { controller: controller, superseded: false };
      if (controller) fetchControllers[reqKey] = reqState;

      ops.fetchJsonWithTimeout(targetUrl, {
        signal: controller ? controller.signal : null,
        timeoutMs: opts.timeout || 15000
      })
        .then(function (data) {
          if (fetchControllers[reqKey] === reqState) delete fetchControllers[reqKey];
          if (reqState.superseded) {
            callback(null, null, true);
            return;
          }
          callback(null, data);
        })
        .catch(function (err) {
          if (fetchControllers[reqKey] === reqState) delete fetchControllers[reqKey];
          if (reqState.superseded || (err && err.name === "AbortError")) {
            callback(null, null, true);
            return;
          }
          callback(err || new Error("网络异常"));
        });
    }

    function mergeObjectFields(base, extra) {
      var merged = {};
      if (base && typeof base === "object") {
        Object.keys(base).forEach(function (k) { merged[k] = base[k]; });
      }
      if (extra && typeof extra === "object") {
        Object.keys(extra).forEach(function (k) { merged[k] = extra[k]; });
      }
      return merged;
    }

    function applyTradeOutcomeStats() {
      if (!latestData || !latestData.trade_outcome_stats || typeof latestData.trade_outcome_stats !== "object") return;
      latestData.drawdown_stats_strategy = mergeObjectFields(
        latestData.drawdown_stats_strategy || {},
        latestData.trade_outcome_stats
      );
      if (currentCurveTab === "strategy") {
        latestData.drawdown_stats = latestData.drawdown_stats_strategy;
      }
    }

    function mergeLatest(partial) {
      if (!partial || typeof partial !== "object") return;
      if (!latestData || typeof latestData !== "object") latestData = {};
      Object.keys(partial).forEach(function (k) { latestData[k] = partial[k]; });
      applyTradeOutcomeStats();
    }

    function mergeCurveOnly(curvePayload) {
      if (!curvePayload || typeof curvePayload !== "object") return;
      if (!latestData || typeof latestData !== "object") latestData = {};
      latestData.curve_window_hours = curvePayload.curve_window_hours;
      latestData.curve_points = curvePayload.curve_points;
      if (curvePayload.balance_curve_cycle_baseline !== undefined) {
        latestData.balance_curve_cycle_baseline = curvePayload.balance_curve_cycle_baseline;
      }
      if (curvePayload.portfolio_loss_cut) {
        latestData.portfolio_loss_cut = curvePayload.portfolio_loss_cut;
      }
      if (curvePayload.curve_type === "strategy") {
        latestData.strategy_equity_curve = curvePayload.strategy_equity_curve || [];
        latestData.equity_curve = curvePayload.equity_curve || latestData.strategy_equity_curve;
        if (curvePayload.drawdown_stats_strategy) latestData.drawdown_stats_strategy = curvePayload.drawdown_stats_strategy;
      } else if (curvePayload.curve_type === "balance") {
        latestData.balance_curve = curvePayload.balance_curve || [];
        latestData.equity_curve = curvePayload.equity_curve || latestData.balance_curve;
        if (curvePayload.drawdown_stats_balance) latestData.drawdown_stats_balance = curvePayload.drawdown_stats_balance;
      } else {
        latestData.strategy_equity_curve = curvePayload.strategy_equity_curve || [];
        latestData.balance_curve = curvePayload.balance_curve || [];
        latestData.equity_curve = curvePayload.equity_curve || latestData.strategy_equity_curve || [];
        if (curvePayload.drawdown_stats_strategy) latestData.drawdown_stats_strategy = curvePayload.drawdown_stats_strategy;
        if (curvePayload.drawdown_stats_balance) latestData.drawdown_stats_balance = curvePayload.drawdown_stats_balance;
      }
      if (curvePayload.drawdown_stats) latestData.drawdown_stats = curvePayload.drawdown_stats;
      applyTradeOutcomeStats();
    }

    function activeStats(data) {
      if (!data) return {};
      if (currentCurveTab === "balance") return data.drawdown_stats_balance || data.drawdown_stats || {};
      return data.drawdown_stats_strategy || data.drawdown_stats || {};
    }

    function activeCurve(data) {
      if (!data) return [];
      if (currentCurveTab === "balance") return data.balance_curve || data.equity_curve || [];
      return data.strategy_equity_curve || data.equity_curve || [];
    }

    function latestUnderlyingTimestamp(data) {
      if (!data) return "";
      if (data.wallet && data.wallet.as_of_utc) return data.wallet.as_of_utc;
      if (data.wallet && data.wallet.recorded_at_utc) return data.wallet.recorded_at_utc;
      if (data.wallet && data.wallet.ts_utc) return data.wallet.ts_utc;
      var curve = activeCurve(data);
      if (curve.length > 1) {
        var lastPoint = curve[curve.length - 1];
        if (lastPoint && lastPoint.t) return lastPoint.t;
      }
      if (data.trade_stats && data.trade_stats.last_updated_utc) return data.trade_stats.last_updated_utc;
      return "";
    }

    function updateFreshnessDisplay(data) {
      var ts = latestUnderlyingTimestamp(data);
      var freshness = ops.evaluateDataFreshness(ts, { warnSec: 180, staleSec: 900 });
      ops.applyFreshnessPill(el.freshnessPill, el.freshnessText, freshness);
      if (el.updatedAt) {
        el.updatedAt.textContent = ts ? fmtMetaTime(ts) : "--";
      }
    }

    function showChartEmpty(message, isError) {
      if (el.chartSparseHint) el.chartSparseHint.hidden = true;
      if (el.chartResetZoomBtn) el.chartResetZoomBtn.hidden = true;
      if (!el.chartEmpty) return;
      el.chartEmpty.textContent = message || "暂无曲线";
      el.chartEmpty.classList.toggle("is-error", !!isError);
      el.chartEmpty.hidden = false;
    }

    var throttledChartResize = ops.rafThrottle(function () {
      if (equityChart) equityChart.resize();
    });

    function attachChartResizeObserver() {
      if (!el.equityChart) return;
      if (typeof window.ResizeObserver !== "undefined" && !chartResizeObs) {
        chartResizeObs = new ResizeObserver(function (entries) {
          if (!equityChart) return;
          for (var rIdx = 0; rIdx < entries.length; rIdx += 1) {
            if (entries[rIdx].contentRect && entries[rIdx].contentRect.width > 0) {
              throttledChartResize();
              break;
            }
          }
        });
        chartResizeObs.observe(el.equityChart);
      }
    }

    function renderEquityChart(curve, forceRebuild) {
      if (!el.equityChart) return;
      if (typeof window.echarts === "undefined") {
        showChartEmpty("图表组件加载中...");
        return;
      }

      var raw = curve || [];
      var stop = latestData && latestData.portfolio_loss_cut;
      var stopSig = stop
        ? (String(stop.status || "") + ":" + (stop.baseline_equity != null ? String(stop.baseline_equity) : "") + ":" + (stop.loss_pct != null ? String(stop.loss_pct) : "") + ":" + (stop.current_return_pct != null ? String(stop.current_return_pct) : ""))
        : "";
      var firstT = raw.length ? String((raw[0] && raw[0].t) || "") : "";
      var lastT = raw.length ? String((raw[raw.length - 1] && raw[raw.length - 1].t) || "") : "";
      var lastEq = raw.length && raw[raw.length - 1] && raw[raw.length - 1].equity != null
        ? String(raw[raw.length - 1].equity)
        : "";
      var nextSig = currentCurveTab + "|" + currentWindowHours + "|" + displayTimeZone + "|" + (portfolioStopEnabled ? "1" : "0") + "|" + raw.length + "|" + firstT + "|" + lastT + "|" + lastEq + "|" + stopSig;
      if (!forceRebuild && nextSig === lastChartSignature && equityChart) {
        return;
      }
      lastChartSignature = nextSig;

      if (!equityChart) {
        equityChart = window.echarts.init(el.equityChart, null, { renderer: "canvas" });
        equityChart.on("datazoom", function (evt) {
          var batch = evt && evt.batch && evt.batch[0] ? evt.batch[0] : evt;
          var start = batch && Number.isFinite(Number(batch.start)) ? Number(batch.start) : 0;
          var end = batch && Number.isFinite(Number(batch.end)) ? Number(batch.end) : 100;
          var zoomed = start > 0.5 || end < 99.5;
          chartZoomRange = zoomed ? { start: start, end: end } : null;
          if (el.chartResetZoomBtn) el.chartResetZoomBtn.hidden = !zoomed;
        });
        attachChartResizeObserver();
      }

      if (!raw.length) {
        equityChart.clear();
        showChartEmpty("所选范围暂无权益曲线");
        return;
      }

      var seriesData = [];
      for (var i = 0; i < raw.length; i += 1) {
        var item = raw[i] || {};
        var equity = ops.toNum(item.equity);
        var tMs = ops.parseTimestampMs(item.t);
        if (equity === null || !Number.isFinite(tMs)) continue;
        seriesData.push([tMs, equity, ops.toNum(item.cum_pnl), ops.toNum(item.drawdown_pct)]);
      }

      if (!seriesData.length) {
        equityChart.clear();
        showChartEmpty("所选范围暂无有效数据");
        return;
      }

      if (el.chartEmpty) el.chartEmpty.hidden = true;
      if (el.chartSparseHint) {
        el.chartSparseHint.hidden = !(seriesData.length <= 1);
      }

      var theme = ops.getChartTheme();
      var firstEq = seriesData[0][1];
      var lastEqVal = seriesData[seriesData.length - 1][1];
      var isStrategyCurve = accountMode !== "readonly" && currentCurveTab === "strategy";
      var stopStatus = isStrategyCurve && stop ? String(stop.status || "").toUpperCase() : "";
      var backendReturn = isStrategyCurve && stop ? ops.toNum(stop.current_return_pct) : null;
      var effectiveReturn = seriesData.length <= 1 && backendReturn !== null
        ? backendReturn
        : (firstEq > 0 ? ((lastEqVal - firstEq) / firstEq) * 100 : 0);
      var tone = ops.classifyReturnTone(effectiveReturn, isStrategyCurve ? portfolioStopPct : null, stopStatus);
      var isNegative = tone === "bad";
      var lineColor = isNegative ? theme.bad : theme.ok;
      var areaTop = isNegative ? theme.areaBad : theme.areaOk;
      var areaBottom = theme.areaFade;

      var markLines = [];
      if (accountMode !== "readonly" && portfolioStopEnabled && stop && ops.toNum(stop.baseline_equity) > 0) {
        var baseEq = ops.toNum(stop.baseline_equity);
        var lossPct = ops.toNum(stop.loss_pct) || portfolioStopPct;
        var stopEq = baseEq * (1 - lossPct / 100);
        markLines.push({
          yAxis: baseEq,
          lineStyle: { color: theme.borderStrong, type: "dotted", width: 1 },
          label: {
            position: "insideStartTop",
            color: theme.muted,
            backgroundColor: theme.surface,
            padding: [2, 6],
            borderRadius: 4,
            fontSize: 12,
            formatter: "周期起点 " + ops.fmtUsdt(baseEq, 2)
          }
        });
        markLines.push({
          yAxis: stopEq,
          lineStyle: { color: theme.bad, type: "dashed", width: 1.4 },
          label: {
            position: "insideStartBottom",
            color: theme.badText,
            backgroundColor: theme.surface,
            borderColor: theme.bad,
            borderWidth: 1,
            padding: [2, 6],
            borderRadius: 4,
            fontSize: 12,
            formatter: "组合止损 -" + ops.fmtNum(lossPct, 2) + "% (" + ops.fmtUsdt(stopEq, 2) + ")"
          }
        });
      }

      el.equityChart.setAttribute(
        "aria-label",
        (accountId || "账户") + " " + (currentCurveTab === "balance" ? "账户权益" : "策略权益")
        + "曲线，共 " + seriesData.length + " 个快照点，最新权益 " + ops.fmtUsdt(lastEqVal, 2) + " USDT"
      );

      equityChart.setOption(R.buildEquityChartOption({
        seriesData: seriesData,
        markLines: markLines,
        lineColor: lineColor,
        areaTop: areaTop,
        areaBottom: areaBottom,
        theme: theme,
        timeZone: displayTimeZone,
        zoomStart: chartZoomRange ? chartZoomRange.start : undefined,
        zoomEnd: chartZoomRange ? chartZoomRange.end : undefined,
        echartsGraphic: window.echarts && window.echarts.graphic
      }), true);
      if (el.chartResetZoomBtn) {
        el.chartResetZoomBtn.hidden = !(seriesData.length > 24 && chartZoomRange && (chartZoomRange.start > 0.5 || chartZoomRange.end < 99.5));
      }
    }

    function ensureEcharts() {
      var resolvedSrc = ECHARTS_SRC || (apiBase + "/static/vendor/echarts.min.js");
      if (resolvedSrc.charAt(0) === "/" && resolvedSrc.indexOf("://") < 0 && apiBase && apiBase !== "/") {
        resolvedSrc = apiBase + resolvedSrc;
      }
      ops.ensureEcharts(
        resolvedSrc,
        function () {
          if (latestData) renderEquityChart(activeCurve(latestData), true);
        },
        function () {
          showChartEmpty("图表组件加载失败", true);
        }
      );
    }

    function setSummaryMetric(node, value, cls) {
      if (!node) return;
      node.textContent = value;
      node.className = "summary-value" + (cls ? " " + cls : "");
    }

    function renderReadonlyStats(data) {
      if (accountMode !== "readonly") return;
      var stats = (data && data.trade_stats) || null;
      if (!stats) {
        if (el.readonlyNetPnl) {
          el.readonlyNetPnl.textContent = "--";
          el.readonlyNetPnl.className = "readonly-hero-pnl";
        }
        if (el.lastRunStatus) {
          el.lastRunStatus.textContent = "--";
          el.lastRunStatus.className = "summary-value";
        }
        setText(el.readonlyStatsSubtitle, "暂无近 30 日成交统计（或未配置只读成交拉取）");
        if (el.readonlyStatsFacts) {
          ops.patchHtmlIfChanged(el.readonlyStatsFacts, '<div class="risk-fact"><span class="risk-fact-label">胜率</span><strong class="risk-fact-value">--</strong></div><div class="risk-fact"><span class="risk-fact-label">盈亏比</span><strong class="risk-fact-value">--</strong></div>');
        }
        return;
      }
      var netPnl = ops.toNum(stats.net_realized_pnl != null ? stats.net_realized_pnl : stats.total_realized_pnl);
      var pnlCls = netPnl === null ? "" : (netPnl >= 0 ? "ok" : "bad");
      if (el.readonlyNetPnl) {
        el.readonlyNetPnl.textContent = netPnl === null ? "--" : ops.fmtSignedUsdt(netPnl, 2) + " USDT";
        el.readonlyNetPnl.className = "readonly-hero-pnl " + pnlCls;
      }
      if (el.lastRunStatus) {
        el.lastRunStatus.textContent = stats.win_rate_pct != null ? ops.fmtNum(stats.win_rate_pct, 1) + "%" : "--";
        el.lastRunStatus.className = "summary-value " + pnlCls;
      }
      if (el.lastRunMetric) {
        var helpNode = el.lastRunMetric.querySelector(".summary-help");
        if (helpNode) {
          helpNode.textContent = stats.total_trades != null ? ("近30日平仓 " + ops.fmtNum(stats.total_trades, 0) + " 笔") : "近30日统计";
        }
      }
      var updatedLabel = stats.last_updated_utc ? fmtAxisTime(stats.last_updated_utc) : "--";
      setText(el.readonlyStatsSubtitle, "近 30 日实盘已实现统计 · 更新于 " + updatedLabel);
      ops.patchHtmlIfChanged(el.readonlyStatsFacts, R.readonlyStatsFactsHtml(stats));
    }

    function renderRiskMetrics(data) {
      var useCycle = accountMode !== "readonly";
      var stop = data && data.portfolio_loss_cut;
      var sourceCurve = useCycle
        ? ((data && data.strategy_equity_curve) || (data && data.balance_curve) || [])
        : activeCurve(data || {});

      var metrics = {};
      var currentReturn = null;
      var currentDrawdown = null;
      var stopStatus = "";
      if (!useCycle) {
        var validPts = [];
        for (var pIdx = 0; pIdx < sourceCurve.length; pIdx += 1) {
          var eqNum = ops.toNum(sourceCurve[pIdx] && sourceCurve[pIdx].equity);
          if (eqNum !== null && eqNum > 0) validPts.push(eqNum);
        }
        if (validPts.length >= 2) {
          var winFirst = validPts[0];
          var winLast = validPts[validPts.length - 1];
          var winPeak = validPts[0];
          for (var wIdx = 1; wIdx < validPts.length; wIdx += 1) {
            if (validPts[wIdx] > winPeak) winPeak = validPts[wIdx];
          }
          currentReturn = ((winLast - winFirst) / winFirst) * 100;
          currentDrawdown = winPeak > 0 ? ((winLast - winPeak) / winPeak) * 100 : 0;
        }
        riskSnapshot = null;
      } else {
        var calculated = ops.computeCycleRiskMetrics(
          sourceCurve,
          accountMode,
          stop,
          { stopHour: portfolioStopHour, stopMinute: portfolioStopMinute, stopPct: portfolioStopPct },
          data && data.balance_curve_cycle_baseline
        );
        riskSnapshot = calculated || null;
        metrics = calculated || {};
        var backendReturn = stop ? ops.toNum(stop.current_return_pct) : null;
        currentReturn = backendReturn !== null ? backendReturn : ops.toNum(metrics.currentReturnPct);
        currentDrawdown = ops.toNum(metrics.currentDrawdownPct);
        stopStatus = stop ? String(stop.status || "").toUpperCase() : "";
      }

      var returnCls = ops.classifyReturnTone(currentReturn, useCycle ? portfolioStopPct : null, stopStatus);
      var drawdownCls = ops.classifyDrawdownTone(currentDrawdown, portfolioStopPct);

      setSummaryMetric(el.cycleReturn, currentReturn === null ? "--" : ops.fmtSigned(currentReturn, 2) + "%", returnCls);
      setSummaryMetric(el.currentDrawdown, currentDrawdown === null ? "--" : ops.fmtSigned(currentDrawdown, 2) + "%", drawdownCls);

      if (accountMode === "readonly") {
        renderReadonlyStats(data);
        return;
      }

      var state = ops.evaluatePortfolioStopState(stop, metrics, {
        enabled: portfolioStopEnabled,
        fallbackPct: portfolioStopPct
      });
      setSummaryMetric(el.stopDistance, state.distanceText, state.tone);
      if (el.riskState) el.riskState.className = "risk-state " + state.tone;
      setText(el.riskState, state.stateText);
      setText(el.riskDescription, state.description);
      setText(el.riskMeterLabel, state.meterText);
      if (el.riskMeterTrack) {
        el.riskMeterTrack.value = Math.round(state.consumedPct);
        el.riskMeterTrack.className = "ops-progress risk-meter-track " + state.tone;
        el.riskMeterTrack.setAttribute("aria-valuetext", state.stateText + "，" + state.meterText);
      }
    }

    function renderCurveTabState() {
      if (el.tabStrategy) {
        el.tabStrategy.classList.toggle("active", currentCurveTab === "strategy");
        el.tabStrategy.setAttribute("aria-pressed", currentCurveTab === "strategy" ? "true" : "false");
      }
      if (el.tabBalance) {
        el.tabBalance.classList.toggle("active", currentCurveTab === "balance");
        el.tabBalance.setAttribute("aria-pressed", currentCurveTab === "balance" ? "true" : "false");
      }
      if (el.curveTitle) {
        el.curveTitle.textContent = currentCurveTab === "strategy"
          ? "策略权益（不含出入金）"
          : "账户权益（含未实现盈亏与出入金）";
      }
      if (el.windowRow) {
        var buttons = el.windowRow.querySelectorAll("[data-window-hours]");
        for (var i = 0; i < buttons.length; i += 1) {
          var b = buttons[i];
          var h = Number(b.getAttribute("data-window-hours"));
          b.classList.toggle("active", h === currentWindowHours);
          b.setAttribute("aria-pressed", h === currentWindowHours ? "true" : "false");
        }
      }
    }

    function renderDrawdownStats(stats) {
      if (!el.drawdownStats) return;
      ops.patchHtmlIfChanged(el.drawdownStats, R.drawdownStatsFactsHtml(stats));
    }

    function rerenderFromLatest(options) {
      if (!latestData) return;
      var opts = options || {};
      var d = latestData;
      var summary = d.summary || {};
      var wallet = d.wallet || {};
      var stats = activeStats(d);

      var walletDisplay = stats.wallet_balance_usdt;
      if (walletDisplay === null || walletDisplay === undefined) walletDisplay = wallet.balance_usdt;
      setText(el.walletBalance, ops.fmtUsdt(walletDisplay, 2));
      renderCurveTabState();
      if (opts.updateChart) {
        renderEquityChart(activeCurve(d), !!opts.forceChart);
      }
      renderRiskMetrics(d);
      renderDrawdownStats(stats);
      updateFreshnessDisplay(d);
      setText(el.openCount, txt(summary.open_positions));
      setText(el.symbolCount, txt(summary.open_symbols));
      setText(el.errorCount, txt(summary.recent_errors));
    }

    function finishManualRefreshSlot() {
      if (manualRefreshPendingCount > 0) {
        manualRefreshPendingCount -= 1;
        if (manualRefreshPendingCount === 0) setDetailRefreshBusy(false);
      }
    }

    function refreshCore(options) {
      var opts = options || {};
      var trackManual = !!opts.manual;
      fetchDashboard(api + "/core", { kind: "core" }, function (err, d, aborted) {
        if (trackManual) finishManualRefreshSlot();
        if (aborted) return;
        if (err) {
          var coreMsg = "账户状态刷新失败：" + (err && err.message ? err.message : String(err));
          setText(el.meta, coreMsg);
          setDetailError("core", coreMsg);
          if (!latestData) {
            if (el.freshnessText) el.freshnessText.textContent = "读取失败";
            if (el.freshnessPill) el.freshnessPill.className = "ops-freshness is-stale";
            if (el.serviceState) {
              el.serviceState.textContent = "读取失败";
              el.serviceState.className = "command-cell-value bad";
            }
          }
          return;
        }
        setDetailError("core", "");

        var previousOpenCount = latestData && latestData.summary ? ops.toNum(latestData.summary.open_positions) : null;
        mergeLatest(d);
        d = latestData || d;
        var summary = d.summary || {};
        var wallet = d.wallet || {};
        var svc = d.service || {};
        if (d.timezone) displayTimeZone = String(d.timezone);
        var svcStatus = "DISABLED";
        if (svc.enabled) {
          svcStatus = svc.running ? "RUNNING" : (svc.error ? "ERROR (" + svc.error + ")" : "STOPPED");
        }

        setText(el.meta, "UTC+8 · 数据来源 " + ops.formatWalletSource(wallet.source));
        if (accountMode !== "readonly") {
          setText(el.nextEntry, fmtAxisTime(d.next_entry_local));
        }
        var svcLabel = svcStatus === "RUNNING" ? "运行中" : (svcStatus === "DISABLED" ? "未启用" : (svcStatus === "STOPPED" ? "已停止" : "异常"));
        setText(el.serviceState, svcLabel);
        if (el.serviceState) {
          var svcClass = svcStatus.indexOf("RUNNING") >= 0 ? "ok" : (svcStatus.indexOf("ERROR") >= 0 || svcStatus === "STOPPED" ? "bad" : "");
          el.serviceState.className = "command-cell-value" + (svcClass ? " " + svcClass : "");
        }
        if (accountMode !== "readonly") {
          setText(el.lastRunStatus, statusLabel(summary.last_run_status));
          if (el.lastRunStatus) {
            el.lastRunStatus.className = "summary-value " + clsForStatus(summary.last_run_status);
          }
        }

        // Do NOT rebuild ECharts during the core poll; chart updates only via refreshCurveFast or tab/window change.
        rerenderFromLatest({ updateChart: false });

        var currentOpenCount = ops.toNum(summary.open_positions);
        if (previousOpenCount !== null && currentOpenCount !== null && previousOpenCount !== currentOpenCount) {
          refreshDetails();
        }
      });
    }

    function refreshCurveFast(options) {
      var opts = options || {};
      var trackManual = !!opts.manual;
      fetchDashboard(api + "/curve", { kind: "curve", lite: true, curveType: currentCurveTab }, function (err, d, aborted) {
        if (trackManual) finishManualRefreshSlot();
        if (aborted) return;
        if (err) {
          if (el.curveTitle) el.curveTitle.textContent = "权益曲线（加载失败，请稍后重试）";
          if (!latestData || !activeCurve(latestData).length) {
            if (equityChart) equityChart.clear();
            showChartEmpty("权益曲线读取失败，请稍后重试", true);
          }
          setDetailError("curve", "权益曲线读取失败：" + (err && err.message ? err.message : String(err)));
          return;
        }
        setDetailError("curve", "");
        mergeCurveOnly(d);
        rerenderFromLatest({ updateChart: true, forceChart: !!opts.forceChart });
      });
    }

    function compareNullableNumbers(aVal, bVal, desc) {
      if (aVal === null && bVal === null) return 0;
      if (aVal === null) return 1;
      if (bVal === null) return -1;
      return desc ? (bVal - aVal) : (aVal - bVal);
    }

    function renderPositionsTable() {
      var sorted = cachedPositions.slice();
      if (positionSortKey === "pnl_asc") {
        sorted.sort(function (a, b) { return compareNullableNumbers(ops.toNum(a.unrealized_pnl), ops.toNum(b.unrealized_pnl), false); });
      } else if (positionSortKey === "pnl_desc") {
        sorted.sort(function (a, b) { return compareNullableNumbers(ops.toNum(a.unrealized_pnl), ops.toNum(b.unrealized_pnl), true); });
      } else if (positionSortKey === "notional_desc") {
        sorted.sort(function (a, b) { return compareNullableNumbers(ops.toNum(a.notional), ops.toNum(b.notional), true); });
      }

      renderRows(el.positionsTable, el.positionsBody, sorted, function (p) {
        return R.positionRowHtml(p, displayTimeZone);
      }, 7, "当前无持仓");
    }

    function renderEventsTable() {
      var q = eventSearchQuery.trim().toUpperCase();
      var filtered = cachedEvents.filter(function (e) {
        if (eventStatusFilter !== "ALL" && String(e.status || "").toUpperCase() !== eventStatusFilter) return false;
        if (q && String(e.symbol || "").toUpperCase().indexOf(q) < 0) return false;
        return true;
      });
      if (el.eventFilterCount) {
        el.eventFilterCount.hidden = false;
        el.eventFilterCount.textContent = filtered.length === cachedEvents.length
          ? "共 " + cachedEvents.length + " 条"
          : "匹配 " + filtered.length + " / " + cachedEvents.length + " 条";
      }
      renderRows(el.eventsTable, el.eventsBody, filtered, function (e) {
        return R.eventRowHtml(e, displayTimeZone);
      }, 6, cachedEvents.length ? "无匹配筛选条件的订单事件" : "暂无订单事件");
    }

    function renderDetails(d, includedLogs) {
      cachedPositions = d.open_positions || [];
      var runs = d.runs || [];
      cachedEvents = d.events || [];
      var cashflows = d.cashflow_events || [];
      var unpriced = d.unpriced_closed_details || [];

      setText(el.positionsCount, String(cachedPositions.length));
      setText(el.runsCount, String(runs.length));
      setText(el.eventsCount, String(cachedEvents.length));
      setText(el.cashflowCount, String(cashflows.length));
      setText(el.unpricedCount, String(unpriced.length));
      if (el.unpricedCount) {
        el.unpricedCount.classList.toggle("warn", unpriced.length > 0);
      }

      if (accountMode === "readonly" && el.runsTab) {
        el.runsTab.hidden = runs.length === 0;
        if (el.runsTab.hidden && currentActivityKey === "runs" && activateActivityTab && el.eventsTab) {
          activateActivityTab(el.eventsTab, false, false);
        }
      }

      renderPositionsTable();

      renderRows(el.runsTable, el.runsBody, runs, function (r) {
        return R.runRowHtml(r, displayTimeZone);
      }, 4, "暂无策略运行记录");

      renderEventsTable();

      renderRows(el.cashflowTable, el.cashflowBody, cashflows, function (c) {
        return R.cashflowRowHtml(c, displayTimeZone);
      }, 6, "暂无资金流水");

      renderRows(el.unpricedTable, el.unpricedBody, unpriced, function (u) {
        return R.unpricedRowHtml(u, displayTimeZone);
      }, 6, "暂无待核对平仓记录");

      if (includedLogs && el.logTail) {
        var logLines = Array.isArray(d.log_tail) ? d.log_tail : [];
        el.logTail.textContent = logLines.join("\n") || "暂无日志";
        logsLoadedOnce = true;
      }
      fullLoadedOnce = true;
    }

    function setDetailsErrorRows(message) {
      var targets = [
        [el.positionsTable, el.positionsBody, 7, "持仓明细"],
        [el.runsTable, el.runsBody, 4, "运行记录"],
        [el.eventsTable, el.eventsBody, 6, "订单事件"],
        [el.cashflowTable, el.cashflowBody, 6, "资金流水"],
        [el.unpricedTable, el.unpricedBody, 6, "待核对记录"]
      ];
      for (var i = 0; i < targets.length; i += 1) {
        var tbl = targets[i][0];
        var body = targets[i][1];
        var cols = targets[i][2];
        var label = targets[i][3];
        if (tbl) tbl.classList.add("is-empty");
        if (body) {
          ops.patchHtmlIfChanged(body, '<tr><td colspan="' + cols + '" class="empty-row bad">' + ops.escapeHtml(label) + "加载失败，请稍后重试</td></tr>");
        }
      }
      if (el.logTail && !logsLoadedOnce) {
        el.logTail.textContent = message || "策略日志加载失败，请稍后重试";
      }
    }

    function refreshDetails(options) {
      var opts = options || {};
      var trackManual = !!opts.manual;
      var includeLogs = opts.includeLogs !== undefined
        ? !!opts.includeLogs
        : !!(el.diagnosticsPanel && el.diagnosticsPanel.open);
      if (detailsInFlight && !opts.force) {
        detailsRefreshPending = true;
        if (trackManual) finishManualRefreshSlot();
        return;
      }
      detailsRequestToken += 1;
      var requestToken = detailsRequestToken;
      detailsInFlight = true;
      detailsRefreshPending = false;
      fetchDashboard(api + "/details", { kind: "details", includeLogs: includeLogs }, function (err, d, aborted) {
        if (requestToken === detailsRequestToken) detailsInFlight = false;
        if (trackManual) finishManualRefreshSlot();
        if (aborted || requestToken !== detailsRequestToken) return;
        if (err) {
          if (!detailsLoaded) setDetailsErrorRows("策略日志加载失败，请稍后重试");
          setDetailError("details", "持仓与活动明细读取失败：" + (err && err.message ? err.message : String(err)));
          return;
        }
        setDetailError("details", "");
        renderDetails(d || {}, includeLogs);
        detailsLoaded = true;
        if (detailsRefreshPending) refreshDetails();
      });
    }

    function setupDetailsLazyLoad() {
      var targets = [];
      if (el.positionsBody && el.positionsBody.closest) {
        var panel = el.positionsBody.closest(".positions-panel");
        if (panel) targets.push(panel);
      }

      var fallbackTimer = setTimeout(function () {
        fallbackTimer = 0;
        if (!detailsLoaded && !detailsInFlight) refreshDetails();
      }, isMobile ? 350 : 200);
      if (!targets.length || !window.IntersectionObserver) return;

      var loaded = false;
      var obs = new IntersectionObserver(function (entries) {
        if (loaded) return;
        for (var i = 0; i < entries.length; i += 1) {
          if (entries[i].isIntersecting) {
            loaded = true;
            if (fallbackTimer) {
              clearTimeout(fallbackTimer);
              fallbackTimer = 0;
            }
            refreshDetails();
            obs.disconnect();
            return;
          }
        }
      }, { rootMargin: "240px 0px" });
      for (var j = 0; j < targets.length; j += 1) obs.observe(targets[j]);
    }

    function syncDetailUrl() {
      ops.updateQueryParams({
        tab: currentCurveTab === defaultCurveTab ? null : currentCurveTab,
        window: currentWindowHours === 24 ? null : String(currentWindowHours),
        activity: currentActivityKey === defaultActivityKey ? null : currentActivityKey
      });
    }

    function setDetailRefreshBusy(busy) {
      if (!el.detailRefreshBtn) return;
      el.detailRefreshBtn.disabled = !!busy;
      var lbl = el.detailRefreshBtn.querySelector(".btn-label");
      if (lbl) lbl.textContent = busy ? "刷新中..." : "刷新";
      else el.detailRefreshBtn.textContent = busy ? "刷新中..." : "刷新";
    }

    var activateActivityTab = null;

    function setupActivityTabs() {
      var tablist = document.querySelector(".activity-tabs");
      if (!tablist) return;
      var tabs = Array.prototype.slice.call(tablist.querySelectorAll("[data-activity-target]"));

      function visibleTabs() {
        return tabs.filter(function (t) { return !t.hidden; });
      }

      function activateTab(tab, shouldFocus, persist) {
        if (!tab) return;
        var targetId = tab.getAttribute("data-activity-target");
        if (!targetId) return;
        for (var j = 0; j < tabs.length; j += 1) {
          var t = tabs[j];
          var isTarget = t === tab;
          t.classList.toggle("active", isTarget);
          t.setAttribute("aria-selected", isTarget ? "true" : "false");
          t.setAttribute("tabindex", isTarget ? "0" : "-1");
          var view = document.getElementById(t.getAttribute("data-activity-target"));
          if (view) view.hidden = !isTarget;
        }
        var nextKey = targetId.replace(/^activity-/, "");
        if (VALID_ACTIVITIES[nextKey]) currentActivityKey = nextKey;
        if (persist) syncDetailUrl();
        if (shouldFocus) tab.focus();
      }
      activateActivityTab = activateTab;

      var initialTargetId = VALID_ACTIVITIES[currentActivityKey];
      var initialTab = initialTargetId ? tablist.querySelector('[data-activity-target="' + initialTargetId + '"]') : null;
      if (initialTab && initialTab.hidden) {
        var visible = visibleTabs();
        initialTab = visible.length ? visible[0] : null;
      }
      if (initialTab) activateTab(initialTab, false, false);

      tablist.addEventListener("click", function (evt) {
        var tab = evt.target && evt.target.closest ? evt.target.closest("[data-activity-target]") : null;
        if (tab) activateTab(tab, false, true);
      });

      tablist.addEventListener("keydown", function (evt) {
        var activeTabs = visibleTabs();
        if (!activeTabs.length) return;
        var activeIndex = -1;
        for (var i = 0; i < activeTabs.length; i += 1) {
          if (activeTabs[i].getAttribute("aria-selected") === "true") {
            activeIndex = i;
            break;
          }
        }
        if (activeIndex < 0) activeIndex = 0;
        if (evt.key === "ArrowRight" || evt.key === "ArrowDown") {
          evt.preventDefault();
          activateTab(activeTabs[(activeIndex + 1) % activeTabs.length], true, true);
        } else if (evt.key === "ArrowLeft" || evt.key === "ArrowUp") {
          evt.preventDefault();
          activateTab(activeTabs[(activeIndex - 1 + activeTabs.length) % activeTabs.length], true, true);
        } else if (evt.key === "Home") {
          evt.preventDefault();
          activateTab(activeTabs[0], true, true);
        } else if (evt.key === "End") {
          evt.preventDefault();
          activateTab(activeTabs[activeTabs.length - 1], true, true);
        }
      });
    }

    function triggerManualRefresh() {
      if (manualRefreshPendingCount > 0) return;
      manualRefreshPendingCount = 3;
      setDetailRefreshBusy(true);
      refreshCore({ manual: true });
      refreshCurveFast({ manual: true, forceChart: true });
      refreshDetails({ manual: true, force: true });
    }

    function focusEventSearch() {
      if (el.eventsTab && activateActivityTab) {
        activateActivityTab(el.eventsTab, false, true);
      }
      if (el.eventSearchInput) {
        el.eventSearchInput.focus();
        el.eventSearchInput.select();
      }
    }

    applyAccountContext();
    setupActivityTabs();
    renderCurveTabState();

    ops.bindKeyboardShortcuts({
      onRefresh: triggerManualRefresh,
      onSearch: focusEventSearch
    });

    if (el.detailRetryBtn) el.detailRetryBtn.addEventListener("click", triggerManualRefresh);
    if (el.detailRefreshBtn) el.detailRefreshBtn.addEventListener("click", triggerManualRefresh);

    if (el.diagnosticsPanel) {
      el.diagnosticsPanel.addEventListener("toggle", function () {
        if (el.diagnosticsPanel.open && !logsLoadedOnce) {
          if (el.logTail) el.logTail.textContent = "日志加载中...";
          refreshDetails({ force: true, includeLogs: true });
        }
      });
    }

    if (el.chartResetZoomBtn) {
      el.chartResetZoomBtn.addEventListener("click", function () {
        chartZoomRange = null;
        if (!equityChart) return;
        equityChart.dispatchAction({ type: "dataZoom", start: 0, end: 100 });
        el.chartResetZoomBtn.hidden = true;
      });
    }

    if (el.chartSparseHint) {
      el.chartSparseHint.addEventListener("click", function (evt) {
        var btn = evt.target && evt.target.closest ? evt.target.closest("[data-sparse-window]") : null;
        if (!btn) return;
        var parsed = Number(btn.getAttribute("data-sparse-window"));
        if (!VALID_WINDOWS[parsed]) return;
        chartZoomRange = null;
        currentWindowHours = parsed;
        syncDetailUrl();
        renderCurveTabState();
        refreshCurveFast({ forceChart: true });
      });
    }

    if (el.positionSortSelect) {
      el.positionSortSelect.addEventListener("change", function () {
        positionSortKey = el.positionSortSelect.value || "default";
        renderPositionsTable();
      });
    }
    var eventSearchTimer = 0;
    if (el.eventSearchInput) {
      el.eventSearchInput.addEventListener("input", function () {
        eventSearchQuery = el.eventSearchInput.value || "";
        if (eventSearchTimer) clearTimeout(eventSearchTimer);
        eventSearchTimer = setTimeout(function () {
          eventSearchTimer = 0;
          renderEventsTable();
        }, 150);
      });
    }
    if (el.eventStatusSelect) {
      el.eventStatusSelect.addEventListener("change", function () {
        eventStatusFilter = el.eventStatusSelect.value || "ALL";
        renderEventsTable();
      });
    }

    if (el.tabStrategy) {
      el.tabStrategy.addEventListener("click", function () {
        chartZoomRange = null;
        currentCurveTab = "strategy";
        syncDetailUrl();
        rerenderFromLatest({ updateChart: true, forceChart: true });
        refreshCurveFast({ forceChart: true });
      });
    }
    if (el.tabBalance) {
      el.tabBalance.addEventListener("click", function () {
        chartZoomRange = null;
        currentCurveTab = "balance";
        syncDetailUrl();
        rerenderFromLatest({ updateChart: true, forceChart: true });
        refreshCurveFast({ forceChart: true });
      });
    }
    if (el.windowRow) {
      el.windowRow.addEventListener("click", function (evt) {
        var btn = evt.target && evt.target.closest ? evt.target.closest("[data-window-hours]") : null;
        if (!btn) return;
        var parsed = Number(btn.getAttribute("data-window-hours"));
        if (!Number.isFinite(parsed) || parsed <= 0) return;
        chartZoomRange = null;
        currentWindowHours = parsed;
        syncDetailUrl();
        renderCurveTabState();
        refreshCurveFast({ forceChart: true });
      });
    }

    function startPolling() {
      if (!coreTimer) {
        coreTimer = setInterval(function () {
          if (document.hidden) return;
          refreshTick += 1;
          if (isMobile) {
            var fullEvery = 3;
            refreshCore();
            if (fullLoadedOnce && (refreshTick % fullEvery) === 0) refreshDetails();
            return;
          }
          refreshCore();
          if (fullLoadedOnce && (refreshTick % 2) === 0) refreshDetails();
        }, Math.max(2000, REFRESH_SEC * 1000));
      }
      if (!curveTimer) {
        curveTimer = setInterval(function () {
          if (!document.hidden) refreshCurveFast();
        }, Math.max(60000, REFRESH_SEC * 1000));
      }
    }

    function stopPolling(disposeChart) {
      if (coreTimer) { clearInterval(coreTimer); coreTimer = null; }
      if (curveTimer) { clearInterval(curveTimer); curveTimer = null; }
      Object.keys(fetchControllers).forEach(function (reqKey) {
        var state = fetchControllers[reqKey];
        if (state && state.controller) {
          state.superseded = true;
          try { state.controller.abort(); } catch (e) {}
        }
        delete fetchControllers[reqKey];
      });
      detailsInFlight = false;
      detailsRefreshPending = false;
      manualRefreshPendingCount = 0;
      setDetailRefreshBusy(false);
      if (disposeChart) {
        if (chartResizeObs) {
          try { chartResizeObs.disconnect(); } catch (e) {}
          chartResizeObs = null;
        }
        if (equityChart) {
          try { equityChart.dispose(); } catch (e) {}
          equityChart = null;
          lastChartSignature = "";
        }
      }
    }

    refreshCurveFast({ forceChart: true });
    refreshCore();
    ensureEcharts();
    setupDetailsLazyLoad();
    startPolling();

    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) {
        refreshCurveFast();
        refreshCore();
        if (fullLoadedOnce) refreshDetails();
      }
    });

    window.addEventListener("online", function () { updateFreshnessDisplay(latestData); });
    window.addEventListener("offline", function () { updateFreshnessDisplay(latestData); });

    window.addEventListener("pagehide", function (event) {
      stopPolling(!(event && event.persisted));
    });

    window.addEventListener("pageshow", function (event) {
      if (event && event.persisted) {
        startPolling();
        attachChartResizeObserver();
        if (equityChart) equityChart.resize();
        else if (latestData) renderEquityChart(activeCurve(latestData), true);
        refreshCurveFast();
        refreshCore();
        if (fullLoadedOnce || detailsLoaded) refreshDetails();
      }
    });
  });
})();
