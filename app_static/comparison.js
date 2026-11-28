/* Bubble Buster — Account Equity Comparison controller (Obsidian Desk Terminal) */
(function () {
  "use strict";

  document.addEventListener("DOMContentLoaded", function () {
    var ops = window.BBOps;
    var renderLib = window.BBComparisonRender;
    if (!ops || !renderLib) return;

    var pageConfig = ops.readPageConfig("page-config");
    var initialNavAccounts = Array.isArray(pageConfig.navAccounts) ? pageConfig.navAccounts : [];
    var echartsSrc = String(pageConfig.echartsSrc || "");

    var chart = null;
    var resizeObserver = null;
    var chartZoomRange = null;
    var payload = null;
    var VALID_RANGES = { "24": true, "72": true, "168": true, "720": true, "all": true };
    var VALID_MODES = { relative: true, absolute: true };
    var initialParams = ops.readQueryParams();
    var range = VALID_RANGES[initialParams.range] ? initialParams.range : "all";
    var mode = VALID_MODES[initialParams.mode] ? initialParams.mode : "relative";
    var visible = {};
    if (initialParams.hide) {
      String(initialParams.hide).split(",").forEach(function (aid) {
        var trimmed = aid.trim();
        if (trimmed) visible[trimmed] = false;
      });
    }
    var sortState = { key: "change_pct", dir: "desc" };
    var activeAbortController = null;
    var activeRequestToken = 0;
    var navAccountsAbortController = null;
    var navAccountsRequestToken = 0;
    var pollTimer = null;
    var echartsLoadFailed = false;
    var lastLoadedAt = 0;
    var navAccountsHydrated = initialNavAccounts.length > 0;
    var accountModesById = {};
    initialNavAccounts.forEach(function (acc) {
      var aid = String((acc && (acc.id || acc.account_id)) || "").trim();
      var modeVal = String((acc && acc.mode) || "").trim().toLowerCase();
      if (aid && modeVal) accountModesById[aid] = modeVal;
    });
    var strategyNotesByAccount = {};
    var lastMetricsContentSig = "";
    var lastChartSignature = "";
    var lastChartModeRange = "";
    var lastChartHasSlider = false;
    var pathPrefix = ops.resolveBasePrefix();

    ops.initTopNav({
      basePrefix: pathPrefix,
      activePage: "comparison",
      accounts: initialNavAccounts
    });

    function el(id) { return document.getElementById(id); }

    function isReadonlyAccount(accountId) {
      return accountModesById[String(accountId || "")] === "readonly";
    }

    function rangeLabel() {
      return renderLib.rangeLabel(range);
    }

    function setFreshnessPill(text, stateClass) {
      var pill = el("freshness-pill");
      var textEl = el("status-text");
      if (textEl) textEl.textContent = text;
      if (pill) pill.className = "ops-freshness" + (stateClass ? " " + stateClass : "");
    }

    var setNoticeAlert = ops.createAlertController(el("notice"), el("notice-text"), ["load"]);
    function setNotice(message) {
      setNoticeAlert("load", message);
    }

    function seriesRows() {
      return (payload && payload.series) || [];
    }

    function loadNavAccounts() {
      navAccountsRequestToken += 1;
      var token = navAccountsRequestToken;
      if (navAccountsAbortController) {
        try { navAccountsAbortController.abort(); } catch (e) {}
      }
      navAccountsAbortController = typeof AbortController !== "undefined" ? new AbortController() : null;
      ops.fetchJsonWithTimeout(pathPrefix + "/api/accounts/summary/fast", {
        signal: navAccountsAbortController ? navAccountsAbortController.signal : null,
        timeoutMs: 12000
      })
        .then(function (data) {
          if (token !== navAccountsRequestToken) return;
          if (!data || !Array.isArray(data.accounts) || !data.accounts.length) return;
          navAccountsHydrated = true;
          var notesChanged = false;
          data.accounts.forEach(function (acc) {
            var aid = String((acc && (acc.account_id || acc.id)) || "").trim();
            var modeVal = String((acc && acc.mode) || "").trim().toLowerCase();
            if (aid && modeVal) accountModesById[aid] = modeVal;
            var note = String((acc && acc.strategy_note) || "").trim();
            if (aid && note && strategyNotesByAccount[aid] !== note) {
              strategyNotesByAccount[aid] = note;
              notesChanged = true;
            }
          });
          ops.renderNavAccounts(data.accounts);
          if (notesChanged && payload) {
            renderSummary();
            renderMetricsTable(false);
          }
        })
        .catch(function (err) {
          if (token !== navAccountsRequestToken) return;
          if (err && err.name === "AbortError") return;
          if (!navAccountsHydrated && payload) {
            ops.renderNavAccounts(seriesRows().map(function (r) {
              return { id: r.account_id, mode: accountModesById[r.account_id] || "full" };
            }));
          }
        });
    }

    function renderSummary() {
      var target = el("summary-grid");
      if (!target) return;
      var rows = seriesRows();
      var allNotes = rows.map(function (row) {
        return strategyNotesByAccount[row.account_id] || "";
      }).filter(Boolean);
      var items = rows.map(function (row, index) {
        return {
          key: String(row.account_id || index),
          html: renderLib.buildSummaryCardHtml(row, index, {
            isReadonly: isReadonlyAccount(row.account_id),
            pathPrefix: pathPrefix,
            note: strategyNotesByAccount[row.account_id] || "",
            allNotes: allNotes
          })
        };
      });
      ops.reconcileKeyedHtml(target, items, { keyAttr: "data-account-card" });
    }

    function renderLegend() {
      var legendEl = el("legend");
      if (!legendEl) return;
      var rows = seriesRows();
      var items = rows.map(function (row, index) {
        return {
          key: String(row.account_id || index),
          html: renderLib.buildLegendHtml([row], {
            visible: visible,
            isReadonlyFn: isReadonlyAccount
          })
        };
      });
      ops.reconcileKeyedHtml(legendEl, items, { keyAttr: "data-account" });
    }

    function syncLegendItemStates() {
      var legendEl = el("legend");
      if (!legendEl) return;
      Array.prototype.forEach.call(legendEl.querySelectorAll("[data-account]"), function (button) {
        if (button.disabled) return;
        var aid = button.getAttribute("data-account");
        var isShown = visible[aid] !== false;
        button.classList.toggle("is-muted", !isShown);
        button.setAttribute("aria-pressed", isShown ? "true" : "false");
      });
    }

    function syncSortHeaderAria() {
      var sortButtons = document.querySelectorAll("#comparison-metrics-table [data-sort-key]");
      Array.prototype.forEach.call(sortButtons, function (btn) {
        var th = btn.closest ? btn.closest("th") : btn.parentElement;
        if (!th) return;
        var key = btn.getAttribute("data-sort-key");
        if (key === sortState.key) {
          th.setAttribute("aria-sort", sortState.dir === "asc" ? "ascending" : "descending");
        } else {
          th.removeAttribute("aria-sort");
        }
      });
    }

    function announceMetricsSort(totalCount) {
      var statusNode = el("metrics-sort-status");
      if (!statusNode) return;
      var label = renderLib.SORT_LABELS[sortState.key] || sortState.key;
      var dirText = sortState.dir === "asc" ? "升序" : "降序";
      statusNode.textContent = "已按" + label + dirText + "排列，共 " + totalCount + " 个账户";
    }

    function renderMetricsTable(announceSort) {
      var tbody = el("comparison-metrics-body");
      var table = el("comparison-metrics-table");
      if (!tbody) return;
      var rows = seriesRows().slice();
      var allNotes = rows.map(function (r) { return strategyNotesByAccount[r.account_id] || ""; }).filter(Boolean);

      syncSortHeaderAria();

      if (!rows.length) {
        if (table) table.classList.add("is-empty");
        lastMetricsContentSig = "";
        ops.reconcileKeyedHtml(tbody, [], {
          keyAttr: "data-account-row",
          emptyHtml: '<tr><td colspan="6" class="table-empty-cell">当前窗口暂无账户对比数据</td></tr>'
        });
        return;
      }
      if (table) table.classList.remove("is-empty");

      var enriched = renderLib.sortAndEnrichComparisonRows(rows, sortState);

      var contentSig = enriched.slice().sort(function (a, b) {
        return renderLib.compareText(a.account_id, b.account_id);
      }).map(function (item) {
        var r = item.row;
        return item.account_id + ":" + String(isReadonlyAccount(item.account_id) ? 1 : 0)
          + ":" + String(r.change_pct) + ":" + String(r.latest_equity)
          + ":" + String(r.min_change_pct) + ":" + String(r.max_change_pct)
          + ":" + String(item.max_drawdown_pct) + ":" + String((r.points || []).length)
          + ":" + String(strategyNotesByAccount[r.account_id] || "");
      }).join("|");

      var orderedIds = enriched.map(function (item) { return item.account_id; });
      if (contentSig === lastMetricsContentSig && ops.reorderChildrenByKey(tbody, "data-account-row", orderedIds)) {
        if (announceSort) announceMetricsSort(enriched.length);
        return;
      }

      lastMetricsContentSig = contentSig;
      var items = enriched.map(function (item) {
        var row = item.row;
        return {
          key: item.account_id,
          html: renderLib.buildMetricsRowHtml(item, {
            isReadonly: isReadonlyAccount(row.account_id),
            pathPrefix: pathPrefix,
            note: strategyNotesByAccount[row.account_id] || "",
            allNotes: allNotes
          })
        };
      });
      ops.reconcileKeyedHtml(tbody, items, { keyAttr: "data-account-row" });

      if (announceSort) announceMetricsSort(enriched.length);
    }

    function updateZoomResetVisibility(show) {
      var btn = el("reset-zoom-button");
      if (btn) btn.hidden = !show;
    }

    function attachChartResizeObserver() {
      var chartContainer = el("equity-chart");
      if (!chartContainer || resizeObserver) return;
      resizeObserver = ops.createChartResizeObserver(chartContainer, function () {
        return chart;
      });
    }

    function renderChart(animate) {
      var rows = seriesRows();
      var chartElement = el("equity-chart");
      var emptyElement = el("chart-empty");
      var emptyText = el("chart-empty-text");
      var emptyAction = el("chart-empty-action");
      var okRows = rows.filter(function (row) {
        return row.status === "OK" && (row.points || []).length;
      });
      var activeRows = okRows.filter(function (row) {
        return visible[row.account_id] !== false;
      });
      var allMuted = okRows.length > 0 && activeRows.length === 0;
      chartElement.style.display = activeRows.length ? "block" : "none";
      emptyElement.style.display = activeRows.length ? "none" : "flex";
      if (!activeRows.length) {
        lastChartSignature = "";
        if (chart) chart.clear();
        if (emptyText) {
          emptyText.textContent = allMuted
            ? "已隐藏全部账户曲线"
            : (range === "all"
                ? "当前筛选下没有可用权益快照"
                : "「" + rangeLabel() + "」窗口内暂无权益快照");
        }
        if (emptyAction) {
          if (allMuted) {
            emptyAction.textContent = "显示全部账户";
            emptyAction.setAttribute("data-empty-action", "show-all");
            emptyAction.hidden = false;
          } else {
            emptyAction.textContent = "切换至上线以来";
            emptyAction.setAttribute("data-empty-action", "all-time");
            emptyAction.hidden = range === "all";
          }
        }
        updateZoomResetVisibility(false);
      }

      el("chart-title").textContent = mode === "relative" ? "相对权益变化（%）" : "账户权益（USDT）";
      el("chart-subtitle").textContent = mode === "relative"
        ? (range === "all"
            ? "组合止盈上线后首个有效快照 = 0%；策略事件不叠加到主图。"
            : "所选时间区间（" + rangeLabel() + "）首快照归一化 0%；策略事件不叠加到主图。")
        : "账户权益快照走势；策略事件不叠加到主图。";

      if (!activeRows.length) return;

      if (typeof window.echarts === "undefined") {
        lastChartSignature = "";
        chartElement.style.display = "none";
        if (emptyText) emptyText.textContent = echartsLoadFailed ? "图表组件加载失败" : "图表组件加载中...";
        if (emptyAction) emptyAction.hidden = true;
        emptyElement.style.display = "flex";
        return;
      }

      var chartSig = [
        range,
        mode,
        activeRows.map(function (r) {
          var pts = r.points || [];
          var lastPt = pts.length ? pts[pts.length - 1] : null;
          return r.account_id + ":" + (isReadonlyAccount(r.account_id) ? 1 : 0) + ":" + pts.length
            + ":" + (lastPt ? String(lastPt.t || "") + ":" + String(lastPt.equity) : "");
        }).join(",")
      ].join("|");
      if (chart && chartSig === lastChartSignature) {
        return;
      }
      lastChartSignature = chartSig;

      if (!chart) {
        chart = window.echarts.init(chartElement, null, { renderer: "canvas" });
        chart.on("datazoom", ops.createEchartsZoomHandler(function (nextZoom, zoomed) {
          chartZoomRange = nextZoom;
          updateZoomResetVisibility(zoomed);
        }));
        attachChartResizeObserver();
      }

      chartElement.setAttribute("aria-label", activeRows.length + " 个账户权益对比曲线（" + rangeLabel() + "）");
      var totalPoints = activeRows.reduce(function (acc, r) { return acc + (r.points ? r.points.length : 0); }, 0);
      var showSlider = totalPoints > 40;
      var zoomStart = chartZoomRange ? chartZoomRange.start : 0;
      var zoomEnd = chartZoomRange ? chartZoomRange.end : 100;
      var chartModeRange = range + "|" + mode;
      var needsFullMerge = !lastChartModeRange || lastChartModeRange !== chartModeRange || lastChartHasSlider !== showSlider;
      lastChartModeRange = chartModeRange;
      lastChartHasSlider = showSlider;
      chart.setOption(renderLib.buildComparisonChartOption({
        activeRows: activeRows,
        mode: mode,
        range: range,
        animate: animate,
        zoomStart: zoomStart,
        zoomEnd: zoomEnd,
        isReadonlyFn: isReadonlyAccount
      }), needsFullMerge ? true : { replaceMerge: ["series"] });
      updateZoomResetVisibility(!!(showSlider && chartZoomRange && (chartZoomRange.start > 0.5 || chartZoomRange.end < 99.5)));
    }

    function latestSnapshotTimestamp(rows) {
      return ops.pickLatestTimestamp((rows || []).map(function (r) {
        return r && r.latest_at_utc;
      }));
    }

    function renderMeta() {
      var rows = seriesRows().filter(function (row) { return row.status === "OK" && row.points && row.points.length; });
      var count = rows.reduce(function (total, row) { return total + row.points.length; }, 0);
      var latest = latestSnapshotTimestamp(rows);
      var comparisonStart = payload && payload.comparison_start_at_utc
        ? renderLib.formatLocalTime(payload.comparison_start_at_utc, true)
        : "--";
      el("chart-meta").textContent = rangeLabel() + " · " + rows.length + " 个账户 · " + count + " 个点";
      el("data-as-of").textContent = "组合止盈上线：" + comparisonStart + " · 数据截至 " + (latest ? renderLib.formatLocalTime(latest, true) : "--") + "（北京时间 UTC+8）";
    }

    function updateMetricsTableOverflow() {
      var wrap = document.querySelector(".metrics-panel .table-wrap");
      var panel = wrap && wrap.closest ? wrap.closest(".metrics-panel") : null;
      if (!wrap || !panel) return;
      var hasRight = (wrap.scrollWidth - wrap.clientWidth - wrap.scrollLeft) > 4;
      panel.classList.toggle("has-table-overflow", hasRight);
    }

    function render(animateChart) {
      renderSummary();
      renderLegend();
      renderChart(animateChart);
      renderMetricsTable(false);
      renderMeta();
      updateMetricsTableOverflow();
    }

    function setActiveControl(groupId, key, value) {
      var group = el(groupId);
      if (!group) return;
      ops.syncPressedButtons(group.querySelectorAll(".control"), function (button) {
        return button.getAttribute("data-" + key) === value;
      });
    }

    function ensureVisibility(rows) {
      var validIds = {};
      rows.forEach(function (row) {
        if (!row || !row.account_id) return;
        validIds[row.account_id] = true;
        if (visible[row.account_id] === undefined) visible[row.account_id] = true;
      });
      if (!rows.length) return;
      var pruned = false;
      Object.keys(visible).forEach(function (aid) {
        if (!validIds[aid]) {
          delete visible[aid];
          pruned = true;
        }
      });
      if (pruned) syncComparisonUrl();
    }

    function updateFreshnessStatus() {
      var rows = seriesRows().filter(function (row) { return row.status === "OK" && row.points && row.points.length; });
      if (!rows.length) {
        setFreshnessPill("当前窗口无快照", "is-warn");
        return;
      }
      var latest = latestSnapshotTimestamp(rows);
      var freshness = ops.evaluateDataFreshness(latest, { warnSec: 180, staleSec: 900 });
      ops.applyFreshnessPill(el("freshness-pill"), el("status-text"), freshness);
    }

    function syncComparisonUrl() {
      var hiddenIds = Object.keys(visible).filter(function (aid) {
        return visible[aid] === false;
      }).sort();
      ops.updateQueryParams({
        range: range === "all" ? null : range,
        mode: mode === "relative" ? null : mode,
        hide: hiddenIds.length ? hiddenIds.join(",") : null
      });
    }

    function setRefreshBusy(busy) {
      ops.setButtonBusy(el("refresh-button"), busy, "刷新", "刷新中...");
    }

    function load(options) {
      var opts = options || {};
      var animateChart = !!opts.animate;
      activeRequestToken += 1;
      var requestToken = activeRequestToken;
      if (activeAbortController) {
        try { activeAbortController.abort(); } catch (e) {}
      }
      activeAbortController = typeof AbortController !== "undefined" ? new AbortController() : null;
      setRefreshBusy(true);
      setFreshnessPill("正在读取快照", "");
      setNotice("");
      if (!payload) {
        var chartElement = el("equity-chart");
        var emptyElement = el("chart-empty");
        var emptyText = el("chart-empty-text");
        var emptyAction = el("chart-empty-action");
        if (chartElement) chartElement.style.display = "none";
        if (emptyElement) {
          emptyElement.classList.remove("is-error");
          emptyElement.style.display = "flex";
        }
        if (emptyText) emptyText.textContent = "正在加载对比曲线...";
        if (emptyAction) emptyAction.hidden = true;
      }
      var query = range === "all" ? "all_time=true" : "window_hours=" + encodeURIComponent(range);
      ops.fetchJsonWithTimeout(
        pathPrefix + "/api/accounts/equity-comparison?" + query + "&curve_points=600&include_events=false",
        {
          signal: activeAbortController ? activeAbortController.signal : null,
          timeoutMs: 15000
        }
      )
        .then(function (nextPayload) {
          if (requestToken !== activeRequestToken) return;
          lastLoadedAt = Date.now();
          setRefreshBusy(false);
          payload = nextPayload || {};
          seriesRows().forEach(function (row) {
            if (!row || !row.account_id) return;
            if (row.mode) accountModesById[row.account_id] = String(row.mode).toLowerCase();
            if (row.strategy_note) strategyNotesByAccount[row.account_id] = String(row.strategy_note);
          });
          ensureVisibility(seriesRows());
          if (!navAccountsHydrated) {
            ops.renderNavAccounts(seriesRows().map(function (r) {
              return { id: r.account_id, mode: accountModesById[r.account_id] || r.mode || "full" };
            }));
          }
          var emptyEl = el("chart-empty");
          if (emptyEl) emptyEl.classList.remove("is-error");
          render(animateChart);
          updateFreshnessStatus();
        })
        .catch(function (error) {
          if (requestToken !== activeRequestToken) return;
          if (error && error.name === "AbortError") return;
          setRefreshBusy(false);
          setFreshnessPill("读取失败", "is-stale");
          var errMsg = error && error.message ? error.message : "网络异常";
          setNotice("账户对比数据读取失败：" + errMsg);
          if (!payload) {
            var chartNode = el("equity-chart");
            var emptyNode = el("chart-empty");
            var emptyTextNode = el("chart-empty-text");
            var emptyActionNode = el("chart-empty-action");
            if (chartNode) chartNode.style.display = "none";
            if (emptyNode) {
              emptyNode.classList.add("is-error");
              emptyNode.style.display = "flex";
            }
            if (emptyTextNode) emptyTextNode.textContent = "对比曲线加载失败，请点击“立即重试”";
            if (emptyActionNode) emptyActionNode.hidden = true;
            var metricsTable = el("comparison-metrics-table");
            var metricsBody = el("comparison-metrics-body");
            if (metricsTable) metricsTable.classList.add("is-empty");
            if (metricsBody) {
              ops.patchHtmlIfChanged(metricsBody, '<tr><td colspan="6" class="table-empty-cell bad">指标数据加载失败，请点击“立即重试”</td></tr>');
            }
          }
        });
    }

    setActiveControl("range-controls", "range", range);
    setActiveControl("mode-controls", "mode", mode);

    function retryEchartsIfNeeded() {
      if (typeof window.echarts === "undefined" && echartsLoadFailed) {
        echartsLoadFailed = false;
        ops.ensureEcharts(
          echartsSrc || (pathPrefix + "/static/vendor/echarts.min.js"),
          function () {
            echartsLoadFailed = false;
            if (payload) renderChart(true);
          },
          function () {
            echartsLoadFailed = true;
            if (payload) renderChart(false);
          }
        );
      }
    }

    function triggerManualLoad() {
      var btn = el("refresh-button");
      if (btn && btn.disabled) return;
      retryEchartsIfNeeded();
      if (!navAccountsHydrated) loadNavAccounts();
      load({ animate: true });
    }

    ops.bindKeyboardShortcuts({ onRefresh: triggerManualLoad });

    Array.prototype.forEach.call(document.querySelectorAll("[data-range]"), function (button) {
      button.addEventListener("click", function () {
        chartZoomRange = null;
        range = button.getAttribute("data-range") || "168";
        setActiveControl("range-controls", "range", range);
        syncComparisonUrl();
        ops.announceStatus("时间范围已切换为 " + rangeLabel());
        load({ animate: true });
      });
    });

    Array.prototype.forEach.call(document.querySelectorAll("[data-mode]"), function (button) {
      button.addEventListener("click", function () {
        mode = button.getAttribute("data-mode") || "relative";
        setActiveControl("mode-controls", "mode", mode);
        syncComparisonUrl();
        ops.announceStatus("图表模式已切换为 " + (mode === "relative" ? "相对变化" : "账户权益"));
        if (payload) render(true);
      });
    });

    var refreshBtn = el("refresh-button");
    if (refreshBtn) refreshBtn.addEventListener("click", triggerManualLoad);

    var retryBtn = el("notice-retry-btn");
    if (retryBtn) retryBtn.addEventListener("click", triggerManualLoad);

    var metricsWrap = document.querySelector(".metrics-panel .table-wrap");
    if (metricsWrap) {
      var throttledMetricsOverflow = ops.rafThrottle(updateMetricsTableOverflow);
      metricsWrap.addEventListener("scroll", throttledMetricsOverflow, { passive: true });
      window.addEventListener("resize", throttledMetricsOverflow, { passive: true });
    }

    var emptyActionBtn = el("chart-empty-action");
    if (emptyActionBtn) {
      emptyActionBtn.addEventListener("click", function () {
        if (emptyActionBtn.getAttribute("data-empty-action") === "show-all") {
          seriesRows().forEach(function (row) {
            if (row && row.account_id) visible[row.account_id] = true;
          });
          syncComparisonUrl();
          syncLegendItemStates();
          renderChart(true);
          ops.announceStatus("已显示全部账户曲线");
          return;
        }
        chartZoomRange = null;
        range = "all";
        setActiveControl("range-controls", "range", range);
        syncComparisonUrl();
        load({ animate: true });
      });
    }

    var resetZoomBtn = el("reset-zoom-button");
    if (resetZoomBtn) {
      resetZoomBtn.addEventListener("click", function () {
        chartZoomRange = null;
        if (!chart) return;
        chart.dispatchAction({ type: "dataZoom", start: 0, end: 100 });
        updateZoomResetVisibility(false);
        ops.announceStatus("已重置图表缩放");
      });
    }

    var legendEl = el("legend");
    if (legendEl) {
      legendEl.addEventListener("click", function (event) {
        var button = event.target && event.target.closest ? event.target.closest("[data-account]") : null;
        if (!button || button.disabled) return;
        var accountId = button.getAttribute("data-account");
        var nextVisible = visible[accountId] === false;
        visible[accountId] = nextVisible;
        syncComparisonUrl();
        syncLegendItemStates();
        renderChart(true);
        ops.announceStatus(accountId + (nextVisible ? " 曲线已显示" : " 曲线已隐藏"));
      });
    }

    var metricsTable = el("comparison-metrics-table");
    if (metricsTable) {
      metricsTable.addEventListener("click", function (event) {
        var sortBtn = event.target && event.target.closest ? event.target.closest("[data-sort-key]") : null;
        if (!sortBtn) return;
        var key = sortBtn.getAttribute("data-sort-key");
        if (!key) return;
        if (sortState.key === key) {
          sortState.dir = sortState.dir === "desc" ? "asc" : "desc";
        } else {
          sortState.key = key;
          sortState.dir = key === "account_id" ? "asc" : "desc";
        }
        renderMetricsTable(true);
      });
    }

    document.addEventListener("visibilitychange", function () {
      if (document.hidden) {
        if (pollTimer) {
          clearInterval(pollTimer);
          pollTimer = null;
        }
      } else {
        startPolling();
        if ((Date.now() - lastLoadedAt) >= 30000) load({ animate: false });
      }
    });

    window.addEventListener("online", function () {
      updateFreshnessStatus();
      if (payload) renderSummary();
    });
    window.addEventListener("offline", function () {
      updateFreshnessStatus();
      if (payload) renderSummary();
    });

    attachChartResizeObserver();

    function startPolling() {
      if (!pollTimer) {
        pollTimer = setInterval(function () {
          if (!document.hidden) load();
        }, 60000);
      }
    }

    function stopPolling(disposeChart) {
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
      if (activeAbortController) {
        try { activeAbortController.abort(); } catch (e) {}
        activeAbortController = null;
      }
      if (navAccountsAbortController) {
        try { navAccountsAbortController.abort(); } catch (e) {}
        navAccountsAbortController = null;
      }
      setRefreshBusy(false);
      if (disposeChart) {
        if (resizeObserver) {
          try { resizeObserver.disconnect(); } catch (e) {}
          resizeObserver = null;
        }
        if (chart) {
          try { chart.dispose(); } catch (e) {}
          chart = null;
        }
      }
    }

    window.addEventListener("pagehide", function (event) {
      stopPolling(!(event && event.persisted));
    });

    window.addEventListener("pageshow", function (event) {
      if (event && event.persisted) {
        startPolling();
        attachChartResizeObserver();
        if (chart) chart.resize();
        else if (payload) renderChart();
        if ((Date.now() - lastLoadedAt) >= 30000) load();
      }
    });

    var resolvedEchartsSrc = echartsSrc || (pathPrefix + "/static/vendor/echarts.min.js");
    ops.ensureEcharts(
      resolvedEchartsSrc,
      function () {
        echartsLoadFailed = false;
        if (payload) renderChart();
      },
      function () {
        echartsLoadFailed = true;
        if (payload) renderChart();
      }
    );
    if (!navAccountsHydrated) loadNavAccounts();
    load();
    startPolling();
  });
})();
