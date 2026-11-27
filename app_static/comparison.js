/* Bubble Buster — Account Equity Comparison controller (Obsidian Desk Terminal) */
(function () {
  "use strict";

  document.addEventListener("DOMContentLoaded", function () {
    var ops = window.BBOps;
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

    var collator = typeof Intl !== "undefined" && Intl.Collator
      ? new Intl.Collator(undefined, { numeric: true, sensitivity: "base" })
      : null;

    var pageConfig = ops.readPageConfig("page-config");
    var initialNavAccounts = Array.isArray(pageConfig.navAccounts) ? pageConfig.navAccounts : [];
    var echartsSrc = String(pageConfig.echartsSrc || "");

    var chart = null;
    var resizeObserver = null;
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
    var pathPrefix = ops.resolveBasePrefix();

    ops.initTopNav({
      basePrefix: pathPrefix,
      activePage: "comparison",
      accounts: initialNavAccounts
    });

    function el(id) { return document.getElementById(id); }

    function compareText(a, b) {
      return collator ? collator.compare(a, b) : String(a).localeCompare(String(b));
    }

    function isReadonlyAccount(accountId) {
      return accountModesById[String(accountId || "")] === "readonly";
    }

    function colorFor(accountId, index) {
      var theme = ops.getChartTheme();
      if (isReadonlyAccount(accountId)) return theme.readonly;
      var namedIndex = { acc01: 0, acc02: 1, acc03: 2, acc04: 3 };
      var slot = Object.prototype.hasOwnProperty.call(namedIndex, accountId) ? namedIndex[accountId] : index;
      return theme.series[slot % theme.series.length];
    }

    function patternFor(accountId, index) {
      var namedIndex = { acc01: 0, acc02: 1, acc03: 2, acc04: 3 };
      var slot = Object.prototype.hasOwnProperty.call(namedIndex, accountId) ? namedIndex[accountId] : index;
      return LINE_PATTERNS[slot % LINE_PATTERNS.length];
    }

    function formatLocalTime(value, withSeconds) {
      return ops.formatBeijingTime(value, { withYear: true, withSeconds: !!withSeconds });
    }

    function rangeLabel() {
      if (range === "all") return "上线以来";
      var hours = Number(range);
      if (hours < 48) return "最近 24 小时";
      if (hours < 168) return "最近 3 天";
      if (hours < 720) return "最近 7 天";
      return "最近 30 天";
    }

    function setFreshnessPill(text, stateClass) {
      var pill = el("freshness-pill");
      var textEl = el("status-text");
      if (textEl) textEl.textContent = text;
      if (pill) pill.className = "ops-freshness" + (stateClass ? " " + stateClass : "");
    }

    function setNotice(message) {
      var box = el("notice");
      var copy = el("notice-text");
      if (!box || !copy) return;
      if (message) {
        copy.textContent = message;
        box.hidden = false;
      } else {
        copy.textContent = "";
        box.hidden = true;
      }
    }

    function seriesRows() {
      return (payload && payload.series) || [];
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

    function loadNavAccounts() {
      navAccountsRequestToken += 1;
      var token = navAccountsRequestToken;
      if (navAccountsAbortController) {
        try { navAccountsAbortController.abort(); } catch (e) {}
      }
      navAccountsAbortController = typeof AbortController !== "undefined" ? new AbortController() : null;
      ops.fetchJsonWithTimeout(pathPrefix + "/api/accounts/summary/fast?_=" + Date.now(), {
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
      var arrowIcon = '<svg class="ops-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 11L11 5"/><path d="M6.5 5H11v4.5"/></svg>';
      var html = rows.map(function (row, index) {
        var color = colorFor(row.account_id, index);
        var isReadonly = isReadonlyAccount(row.account_id);
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
        var detailHref = pathPrefix + "/account/" + encodeURIComponent(row.account_id) + "/";
        var note = strategyNotesByAccount[row.account_id] || "";
        var strategyHtml = note ? ops.formatStrategyHighlights(note, allNotes, 2, "summary-strategy-tag") : "";
        return '<article class="summary-card' + (isReadonly ? " is-readonly" : "") + '" style="--account-color:' + color + '">'
          + '<div class="summary-head"><a class="account-label" href="' + detailHref + '" aria-label="查看 ' + ops.escapeHtml(row.account_id) + ' 账户详情"><i class="account-dot" aria-hidden="true"></i><span>' + ops.escapeHtml(row.account_id) + '</span><span class="account-jump" aria-hidden="true">' + arrowIcon + '</span></a><span class="summary-status">' + status + '</span></div>'
          + '<div class="summary-change ' + changeClass + '">' + ops.fmtPct(change) + '</div>'
          + '<div class="summary-equity">当前权益 ' + (row.latest_equity == null ? "--" : ops.fmtUsdt(row.latest_equity, 2) + " USDT") + '</div>'
          + (strategyHtml ? '<div class="summary-strategy">' + strategyHtml + '</div>' : "")
          + '<div class="summary-range"><span>区间低点 <strong>' + ops.fmtPct(row.min_change_pct) + '</strong></span><span>高点 <strong>' + ops.fmtPct(row.max_change_pct) + '</strong></span></div>'
          + "</article>";
      }).join("");
      ops.patchHtmlIfChanged(target, html);
    }

    function renderLegend() {
      var legendEl = el("legend");
      if (!legendEl) return;
      var html = seriesRows().map(function (row, index) {
        var muted = visible[row.account_id] === false;
        var isReadonly = isReadonlyAccount(row.account_id);
        var color = colorFor(row.account_id, index);
        var pat = patternFor(row.account_id, index);
        return '<button class="legend-button' + (muted ? " is-muted" : "") + (isReadonly ? " is-readonly" : "") + '" data-account="' + ops.escapeHtml(row.account_id) + '" aria-pressed="' + (!muted) + '" style="--account-color:' + color + '" type="button">'
          + '<i class="legend-swatch ' + pat.swatchClass + '" aria-hidden="true"></i><span>' + ops.escapeHtml(row.account_id) + '</span><span class="legend-value">' + ops.fmtPct(row.change_pct) + "</span></button>";
      }).join("");
      ops.patchHtmlIfChanged(legendEl, html);
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

    function reorderExistingRows(tbody, orderedAccountIds) {
      var rowMap = {};
      var currentRows = tbody.querySelectorAll("tr[data-account-row]");
      if (currentRows.length !== orderedAccountIds.length) return false;
      for (var i = 0; i < currentRows.length; i += 1) {
        rowMap[currentRows[i].getAttribute("data-account-row")] = currentRows[i];
      }
      var orderedNodes = [];
      for (var j = 0; j < orderedAccountIds.length; j += 1) {
        var tr = rowMap[orderedAccountIds[j]];
        if (!tr) return false;
        orderedNodes.push(tr);
      }
      var useMoveBefore = typeof tbody.moveBefore === "function";
      for (var k = 0; k < orderedNodes.length; k += 1) {
        if (useMoveBefore) {
          try {
            tbody.moveBefore(orderedNodes[k], null);
            continue;
          } catch (e) {}
        }
        tbody.appendChild(orderedNodes[k]);
      }
      return true;
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
        ops.patchHtmlIfChanged(tbody, '<tr><td colspan="6" class="table-empty-cell">当前窗口暂无账户对比数据</td></tr>');
        return;
      }
      if (table) table.classList.remove("is-empty");

      var enriched = rows.map(function (row, idx) {
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

      var dirMult = sortState.dir === "asc" ? 1 : -1;
      enriched.sort(function (a, b) {
        var k = sortState.key;
        if (k === "account_id") return compareText(a.account_id, b.account_id) * dirMult;
        var va = a[k];
        var vb = b[k];
        if (va === null && vb === null) return compareText(a.account_id, b.account_id);
        if (va === null) return 1;
        if (vb === null) return -1;
        if (va === vb) return compareText(a.account_id, b.account_id);
        return (va - vb) * dirMult;
      });

      var contentSig = enriched.map(function (item) {
        var r = item.row;
        return item.account_id + ":" + String(r.change_pct) + ":" + String(r.latest_equity)
          + ":" + String(r.min_change_pct) + ":" + String(r.max_change_pct)
          + ":" + String(item.max_drawdown_pct) + ":" + String((r.points || []).length)
          + ":" + String(strategyNotesByAccount[r.account_id] || "");
      }).join("|");

      var orderedIds = enriched.map(function (item) { return item.account_id; });
      if (contentSig === lastMetricsContentSig && reorderExistingRows(tbody, orderedIds)) {
        if (announceSort) {
          var statusEl = el("metrics-sort-status");
          if (statusEl) {
            var label = SORT_LABELS[sortState.key] || sortState.key;
            var dirText = sortState.dir === "asc" ? "升序" : "降序";
            statusEl.textContent = "已按" + label + dirText + "排列，共 " + enriched.length + " 个账户";
          }
        }
        return;
      }

      lastMetricsContentSig = contentSig;
      var html = enriched.map(function (item) {
        var row = item.row;
        var isReadonly = isReadonlyAccount(row.account_id);
        var color = colorFor(row.account_id, item.index);
        var detailHref = pathPrefix + "/account/" + encodeURIComponent(row.account_id) + "/";
        var changeCls = item.change_pct === null ? "" : (item.change_pct >= 0 ? "ok" : "bad");
        var ddCls = item.max_drawdown_pct === null ? "" : (item.max_drawdown_pct <= -2.5 ? "bad" : (item.max_drawdown_pct < 0 ? "warn" : "ok"));
        var note = strategyNotesByAccount[row.account_id] || "";
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
      }).join("");
      tbody.__bbLastHtml = html;
      tbody.innerHTML = html;

      if (announceSort) {
        var statusNode = el("metrics-sort-status");
        if (statusNode) {
          var sortLabel = SORT_LABELS[sortState.key] || sortState.key;
          var sortDirText = sortState.dir === "asc" ? "升序" : "降序";
          statusNode.textContent = "已按" + sortLabel + sortDirText + "排列，共 " + enriched.length + " 个账户";
        }
      }
    }

    function updateZoomResetVisibility(show) {
      var btn = el("reset-zoom-button");
      if (btn) btn.hidden = !show;
    }

    var throttledChartResize = ops.rafThrottle(function () {
      if (chart) chart.resize();
    });

    function attachChartResizeObserver() {
      var chartContainer = el("equity-chart");
      if (typeof ResizeObserver !== "undefined" && chartContainer && !resizeObserver) {
        resizeObserver = new ResizeObserver(throttledChartResize);
        resizeObserver.observe(chartContainer);
      }
    }

    function renderChart() {
      var rows = seriesRows();
      var chartElement = el("equity-chart");
      var emptyElement = el("chart-empty");
      var emptyText = el("chart-empty-text");
      var emptyAction = el("chart-empty-action");
      var activeRows = rows.filter(function (row) {
        return row.status === "OK" && visible[row.account_id] !== false && (row.points || []).length;
      });
      chartElement.style.display = activeRows.length ? "block" : "none";
      emptyElement.style.display = activeRows.length ? "none" : "flex";
      if (!activeRows.length) {
        if (chart) chart.clear();
        if (emptyText) {
          emptyText.textContent = range === "all"
            ? "当前筛选下没有可用权益快照"
            : "「" + rangeLabel() + "」窗口内暂无权益快照";
        }
        if (emptyAction) emptyAction.hidden = range === "all";
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
        chartElement.style.display = "none";
        if (emptyText) emptyText.textContent = echartsLoadFailed ? "图表组件加载失败" : "图表组件加载中...";
        if (emptyAction) emptyAction.hidden = true;
        emptyElement.style.display = "flex";
        return;
      }

      if (!chart) {
        chart = window.echarts.init(chartElement, null, { renderer: "canvas" });
        chart.on("datazoom", function (evt) {
          var batch = evt && evt.batch && evt.batch[0] ? evt.batch[0] : evt;
          var start = batch && Number.isFinite(Number(batch.start)) ? Number(batch.start) : 0;
          var end = batch && Number.isFinite(Number(batch.end)) ? Number(batch.end) : 100;
          updateZoomResetVisibility(start > 0.5 || end < 99.5);
        });
        attachChartResizeObserver();
      }

      var theme = ops.getChartTheme();
      chartElement.setAttribute("aria-label", activeRows.length + " 个账户权益对比曲线（" + rangeLabel() + "）");
      var totalPoints = activeRows.reduce(function (acc, r) { return acc + (r.points ? r.points.length : 0); }, 0);
      var showSlider = totalPoints > 40;

      var chartSeries = activeRows.map(function (row, index) {
        var color = colorFor(row.account_id, index);
        var pat = patternFor(row.account_id, index);
        var seriesPoints = [];
        (row.points || []).forEach(function (point) {
          var tMs = ops.parseTimestampMs(point.t);
          if (!Number.isFinite(tMs)) return;
          var yVal = mode === "relative" ? Number(point.relative_pct) : Number(point.equity);
          var eqVal = Number(point.equity);
          seriesPoints.push([tMs, Number.isFinite(yVal) ? yVal : null, Number.isFinite(eqVal) ? eqVal : null]);
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
      chart.setOption({
        animation: !reduceMotion,
        animationDuration: reduceMotion ? 0 : 280,
        animationEasing: "cubicOut",
        backgroundColor: "transparent",
        grid: { top: 32, right: 36, bottom: showSlider ? 78 : 48, left: 68 },
        dataZoom: showSlider ? [{
          type: "slider",
          xAxisIndex: 0,
          height: 22,
          bottom: 10,
          borderColor: theme.border,
          backgroundColor: theme.surfaceInset,
          fillerColor: theme.zoomFill,
          handleStyle: { color: theme.accent, borderColor: theme.accent },
          dataBackground: {
            lineStyle: { color: theme.borderStrong, width: 1 },
            areaStyle: { color: theme.zoomArea }
          },
          textStyle: { color: theme.muted, fontSize: 12 }
        }] : [],
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
        },
        xAxis: {
          type: "time",
          boundaryGap: false,
          axisLine: { lineStyle: { color: theme.border } },
          axisTick: { show: false },
          axisLabel: {
            color: theme.muted,
            fontSize: 12,
            fontFamily: theme.fontMono,
            hideOverlap: true,
            alignMinLabel: "left",
            alignMaxLabel: "right",
            formatter: function (value) { return formatLocalTime(value, false); }
          },
          splitLine: { show: true, lineStyle: { color: theme.splitLineSubtle, type: "dashed" } }
        },
        yAxis: {
          type: "value",
          scale: mode === "absolute",
          axisLine: { show: false },
          axisTick: { show: false },
          axisLabel: {
            color: theme.muted,
            fontSize: 12,
            fontFamily: theme.fontMono,
            formatter: function (value) {
              return mode === "relative" ? (value >= 0 ? "+" : "") + Number(value).toFixed(1) + "%" : ops.fmtUsdt(value, 0);
            }
          },
          splitLine: { lineStyle: { color: theme.splitLine, type: "dashed" } }
        },
        series: chartSeries
      }, true);
      updateZoomResetVisibility(false);
    }

    function latestSnapshotTimestamp(rows) {
      var best = "";
      var bestMs = -Infinity;
      for (var i = 0; i < (rows || []).length; i += 1) {
        var val = rows[i] && rows[i].latest_at_utc;
        if (!val) continue;
        var ms = ops.parseTimestampMs(val);
        if (Number.isFinite(ms)) {
          if (ms > bestMs) { bestMs = ms; best = val; }
        } else if (!best || String(val) > String(best)) {
          best = val;
        }
      }
      return best;
    }

    function renderMeta() {
      var rows = seriesRows().filter(function (row) { return row.status === "OK" && row.points && row.points.length; });
      var count = rows.reduce(function (total, row) { return total + row.points.length; }, 0);
      var latest = latestSnapshotTimestamp(rows);
      var comparisonStart = payload && payload.comparison_start_at_utc
        ? formatLocalTime(payload.comparison_start_at_utc, true)
        : "--";
      el("chart-meta").textContent = rangeLabel() + " · " + rows.length + " 个账户 · " + count + " 个点";
      el("data-as-of").textContent = "组合止盈上线：" + comparisonStart + " · 数据截至 " + (latest ? formatLocalTime(latest, true) : "--") + "（北京时间 UTC+8）";
    }

    function updateMetricsTableOverflow() {
      var wrap = document.querySelector(".metrics-panel .table-wrap");
      var panel = wrap && wrap.closest ? wrap.closest(".metrics-panel") : null;
      if (!wrap || !panel) return;
      var hasRight = (wrap.scrollWidth - wrap.clientWidth - wrap.scrollLeft) > 4;
      panel.classList.toggle("has-table-overflow", hasRight);
    }

    function render() {
      renderSummary();
      renderLegend();
      renderChart();
      renderMetricsTable(false);
      renderMeta();
      updateMetricsTableOverflow();
    }

    function setActiveControl(groupId, key, value) {
      var group = el(groupId);
      if (!group) return;
      Array.prototype.forEach.call(group.querySelectorAll(".control"), function (button) {
        var isActive = button.getAttribute("data-" + key) === value;
        button.classList.toggle("active", isActive);
        button.setAttribute("aria-pressed", isActive ? "true" : "false");
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
      var btn = el("refresh-button");
      if (!btn) return;
      btn.disabled = !!busy;
      var lbl = btn.querySelector(".btn-label");
      if (lbl) lbl.textContent = busy ? "刷新中..." : "刷新";
      else btn.textContent = busy ? "刷新中..." : "刷新";
    }

    function load() {
      activeRequestToken += 1;
      var requestToken = activeRequestToken;
      if (activeAbortController) {
        try { activeAbortController.abort(); } catch (e) {}
      }
      activeAbortController = typeof AbortController !== "undefined" ? new AbortController() : null;
      setRefreshBusy(true);
      setFreshnessPill("正在读取快照", "is-warn");
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
        pathPrefix + "/api/accounts/equity-comparison?" + query + "&curve_points=600&include_events=false&_=" + Date.now(),
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
          render();
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

    function triggerManualLoad() {
      var btn = el("refresh-button");
      if (btn && btn.disabled) return;
      if (!Object.keys(strategyNotesByAccount).length) loadNavAccounts();
      load();
    }

    ops.bindKeyboardShortcuts({ onRefresh: triggerManualLoad });

    Array.prototype.forEach.call(document.querySelectorAll("[data-range]"), function (button) {
      button.addEventListener("click", function () {
        range = button.getAttribute("data-range") || "168";
        setActiveControl("range-controls", "range", range);
        syncComparisonUrl();
        load();
      });
    });

    Array.prototype.forEach.call(document.querySelectorAll("[data-mode]"), function (button) {
      button.addEventListener("click", function () {
        mode = button.getAttribute("data-mode") || "relative";
        setActiveControl("mode-controls", "mode", mode);
        syncComparisonUrl();
        if (payload) render();
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
        range = "all";
        setActiveControl("range-controls", "range", range);
        syncComparisonUrl();
        load();
      });
    }

    var resetZoomBtn = el("reset-zoom-button");
    if (resetZoomBtn) {
      resetZoomBtn.addEventListener("click", function () {
        if (!chart) return;
        chart.dispatchAction({ type: "dataZoom", start: 0, end: 100 });
        updateZoomResetVisibility(false);
      });
    }

    var legendEl = el("legend");
    if (legendEl) {
      legendEl.addEventListener("click", function (event) {
        var button = event.target && event.target.closest ? event.target.closest("[data-account]") : null;
        if (!button) return;
        var accountId = button.getAttribute("data-account");
        visible[accountId] = visible[accountId] === false;
        syncComparisonUrl();
        renderLegend();
        renderChart();
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
      if (!document.hidden && (Date.now() - lastLoadedAt) >= 30000) {
        load();
      }
    });

    window.addEventListener("online", updateFreshnessStatus);
    window.addEventListener("offline", updateFreshnessStatus);

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
