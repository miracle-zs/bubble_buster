/* Bubble Buster — Accounts Overview controller */
(function () {
  "use strict";

  document.addEventListener("DOMContentLoaded", function () {
    var ops = window.BBOps;
    if (!ops) return;

    var cfg = ops.readPageConfig("page-config");
    var refreshSec = Math.max(15, Number(cfg.refreshSec) || 15);
    var pathPrefix = ops.resolveBasePrefix();
    var summaryApi = pathPrefix + "/api/accounts/summary";
    var summaryFastApi = summaryApi + "/fast";
    var summaryDetailsApi = summaryApi + "/details";
    var healthApi = pathPrefix + "/healthz";
    var portfolioStopEnabled = !!cfg.portfolioStopEnabled;
    var portfolioStopPct = Number(cfg.portfolioStopPct) || 3.5;
    var portfolioStopHour = Number(cfg.portfolioStopHour) || 0;
    var portfolioStopMinute = Number(cfg.portfolioStopMinute) || 0;
    var navAccounts = Array.isArray(cfg.navAccounts) ? cfg.navAccounts : [];

    ops.initTopNav({ basePrefix: pathPrefix, activePage: "overview", accounts: navAccounts });

    var cards = document.getElementById("cards");
    var entryProgressBoard = document.getElementById("entry-progress-board");
    var entryProgressUpdated = document.getElementById("entry-progress-updated");
    var entryProgressToggle = document.getElementById("entry-progress-toggle");
    var taskBoard = document.getElementById("task-board");
    var taskUpdatedAt = document.getElementById("task-updated-at");
    var serviceState = document.getElementById("service-state");
    var overviewUpdatedAt = document.getElementById("overview-updated-at");
    var overviewFreshnessPill = document.getElementById("overview-freshness-pill");
    var overviewFreshnessText = document.getElementById("overview-freshness-text");
    var overviewAnomalyCount = document.getElementById("overview-anomaly-count");
    var overviewAlert = document.getElementById("overview-alert");
    var overviewAlertText = document.getElementById("overview-alert-text");
    var overviewRetryBtn = document.getElementById("overview-retry-btn");
    var overviewRefreshBtn = document.getElementById("overview-refresh-btn");
    var incidentBanner = document.getElementById("fleet-incident-banner");
    var incidentPill = document.getElementById("fleet-incident-pill");
    var incidentText = document.getElementById("fleet-incident-text");
    var incidentJumpBtn = document.getElementById("fleet-incident-jump");

    var overviewErrors = { summary: "", details: "" };
    var serviceHealthState = { enabled: false, running: false, loaded: false };
    var curveCache = {};
    var curveErrors = {};
    var curveInFlight = {};
    var curveRenderData = {};
    var curveObserver = null;
    var accountModes = {};
    var portfolioStopStates = {};
    var summaryRows = [];
    var latestSummaryPayload = null;
    var initialQuery = ops.readQueryParams();
    var validTaskFilters = { all: true, anomaly: true, symbols: true };
    var taskFilter = validTaskFilters[initialQuery.task_filter] ? initialQuery.task_filter : "anomaly";
    var entryDetailsUserOverride = null;
    var nextEntryCheckRaw = "";
    var countdownTimer = null;
    var curveTtlMs = Math.max(60000, refreshSec * 3000);
    var summaryRequestInFlight = false;
    var summaryDetailsRequestInFlight = false;
    var detailsHydratedOnce = false;
    var lastDetailsFetchAt = 0;
    var detailsRetryTimer = null;

    function escapeHtml(text) {
      return ops.escapeHtml(text);
    }

    function fmt(v, digits) {
      if (v === null || v === undefined || v === "") return "--";
      var n = Number(v);
      if (!Number.isFinite(n)) return escapeHtml(String(v));
      return n.toFixed(digits);
    }

    function setOverviewError(slot, message) {
      overviewErrors[slot] = message || "";
      if (!overviewAlert || !overviewAlertText) return;
      var active = overviewErrors.summary || overviewErrors.details;
      if (active) {
        overviewAlertText.textContent = active;
        overviewAlert.hidden = false;
      } else {
        overviewAlertText.textContent = "";
        overviewAlert.hidden = true;
      }
    }

    function taskStatusText(status) {
      var s = String(status || "UNKNOWN").toUpperCase();
      var map = {
        SUCCESS: "成功",
        FAILED: "失败",
        PARTIAL: "部分完成",
        SKIPPED: "已跳过",
        RUNNING: "运行中",
        READONLY: "只读",
        UNKNOWN: "未知"
      };
      return map[s] || s;
    }

    function taskStatusCls(status) {
      var s = String(status || "UNKNOWN").toUpperCase();
      if (s === "SUCCESS" || s === "RUNNING") return "status-ok";
      if (s === "PARTIAL" || s === "SKIPPED" || s === "UNKNOWN" || s === "READONLY") return "status-warn";
      return "status-bad";
    }

    function worstAccountTaskStatus(row) {
      var tasks = (row && row.tasks) || {};
      var keys = Object.keys(tasks);
      var sawPartial = false;
      var sawSuccess = false;
      var sawRunning = false;
      for (var i = 0; i < keys.length; i += 1) {
        var st = String(((tasks[keys[i]] || {}).status) || "").toUpperCase();
        if (st === "FAILED") return "FAILED";
        if (st === "PARTIAL") sawPartial = true;
        if (st === "RUNNING") sawRunning = true;
        if (st === "SUCCESS") sawSuccess = true;
      }
      if (sawPartial) return "PARTIAL";
      if (sawRunning) return "RUNNING";
      if (sawSuccess) return "SUCCESS";
      var fallback = String((row && row.last_run_status) || "").toUpperCase();
      return fallback || "UNKNOWN";
    }

    function isAccountStopped(aid, row) {
      var stop = (row && row.portfolio_loss_cut) || portfolioStopStates[aid] || null;
      var st = String((stop && stop.status) || "").toUpperCase();
      return st === "TRIGGERED_COMPLETE" || st === "CLOSING" || st === "THRESHOLD_REACHED";
    }

    function anomalyTaskCount(rows) {
      var total = 0;
      for (var i = 0; i < (rows || []).length; i += 1) {
        var tasks = ((rows[i] || {}).tasks) || {};
        var keys = Object.keys(tasks);
        for (var j = 0; j < keys.length; j += 1) {
          var status = String(((tasks[keys[j]] || {}).status) || "").toUpperCase();
          if (status === "FAILED" || status === "PARTIAL") total += 1;
        }
      }
      return total;
    }

    function latestUnderlyingTimestamp(rows) {
      var candidates = [];
      for (var i = 0; i < (rows || []).length; i += 1) {
        var r = rows[i] || {};
        var aid = String(r.account_id || "");
        var cached = curveCache[aid];
        if (cached && cached.walletAsOfUtc) {
          candidates.push(ops.parseTimestampMs(cached.walletAsOfUtc));
        } else if (cached && cached.points && cached.points.length > 1) {
          var lastPt = cached.points[cached.points.length - 1];
          if (lastPt && lastPt.t) candidates.push(ops.parseTimestampMs(lastPt.t));
        }
        if (r.portfolio_loss_cut && r.portfolio_loss_cut.updated_at_utc) {
          candidates.push(ops.parseTimestampMs(r.portfolio_loss_cut.updated_at_utc));
        }
        if (r.entry_progress && r.entry_progress.updated_at_local) {
          candidates.push(ops.parseTimestampMs(r.entry_progress.updated_at_local));
        }
        var taskTs = latestTaskTimeForAccount(r);
        if (taskTs) candidates.push(ops.parseTimestampMs(taskTs));
      }
      var valid = candidates.filter(Number.isFinite);
      if (valid.length) return Math.max.apply(null, valid);
      return NaN;
    }

    function renderFleetIncidentBanner(freshness) {
      if (!incidentBanner || !incidentPill || !incidentText) return;
      var managedRows = (summaryRows || []).filter(function (r) {
        var m = String((r && r.mode) || "full").toLowerCase();
        return m !== "readonly" && m !== "loss_cut_only";
      });
      var stoppedCount = 0;
      for (var i = 0; i < managedRows.length; i += 1) {
        if (isAccountStopped(String(managedRows[i].account_id || ""), managedRows[i])) stoppedCount += 1;
      }
      var anomalies = anomalyTaskCount(summaryRows);
      var serviceDown = serviceHealthState.loaded && serviceHealthState.enabled && !serviceHealthState.running;
      var isStale = freshness && (freshness.state === "stale" || freshness.state === "offline");

      var alerts = [];
      var severity = "warn";
      if (stoppedCount > 0) {
        severity = "bad";
        alerts.push("组合止损已触发 " + stoppedCount + "/" + managedRows.length + " 个托管账户");
      }
      if (anomalies > 0) {
        severity = "bad";
        alerts.push("存在 " + anomalies + " 项任务执行异常");
      }
      if (serviceDown) {
        severity = "bad";
        alerts.push("策略调度服务已停止");
      }
      if (isStale) {
        alerts.push(freshness.detailText);
      }

      if (!alerts.length) {
        incidentBanner.hidden = true;
        return;
      }
      incidentBanner.hidden = false;
      incidentBanner.className = "ops-incident-banner" + (severity === "warn" ? " is-warn" : "");
      incidentPill.textContent = severity === "bad" ? "风险关注" : "数据时效提示";
      incidentText.textContent = alerts.join(" · ");
      if (incidentJumpBtn) incidentJumpBtn.hidden = anomalies === 0;
    }

    function renderCommandBar(payload) {
      if (payload) latestSummaryPayload = payload;
      var latestMs = latestUnderlyingTimestamp(summaryRows, latestSummaryPayload);
      var freshness = ops.evaluateDataFreshness(latestMs, { warnSec: 180, staleSec: 900 });

      if (overviewUpdatedAt) {
        overviewUpdatedAt.textContent = Number.isFinite(latestMs)
          ? ops.formatBeijingTime(latestMs, { withYear: freshness.ageSec >= 86400, withSeconds: freshness.ageSec < 3600 })
          : "--";
      }
      if (overviewFreshnessText) overviewFreshnessText.textContent = freshness.badgeText;
      if (overviewFreshnessPill) {
        var fCls = freshness.state === "live" ? "is-live" : (freshness.state === "warn" ? "is-warn" : (freshness.state === "stale" || freshness.state === "offline" ? "is-stale" : ""));
        overviewFreshnessPill.className = "ops-freshness" + (fCls ? " " + fCls : "");
        overviewFreshnessPill.title = freshness.detailText;
      }

      if (overviewAnomalyCount) {
        var count = anomalyTaskCount(summaryRows);
        overviewAnomalyCount.textContent = String(count);
        overviewAnomalyCount.className = "command-value " + (count > 0 ? "status-bad" : "status-ok");
      }

      var stopCmd = document.getElementById("portfolio-stop-command");
      if (stopCmd && portfolioStopEnabled) {
        var managedCount = 0;
        var triggeredCount = 0;
        for (var i = 0; i < (summaryRows || []).length; i += 1) {
          var row = summaryRows[i] || {};
          var mode = String(row.mode || "full").toLowerCase();
          if (mode === "readonly" || mode === "loss_cut_only") continue;
          managedCount += 1;
          if (isAccountStopped(String(row.account_id || ""), row)) triggeredCount += 1;
        }
        stopCmd.classList.toggle("is-triggered", triggeredCount > 0);
        if (triggeredCount > 0) {
          stopCmd.textContent = "组合止损已触发 " + triggeredCount + "/" + managedCount;
        } else {
          stopCmd.textContent = "组合止损 -" + portfolioStopPct + "% 已启用";
        }
      }

      renderFleetIncidentBanner(freshness);
    }

    var healthAbortController = null;
    function fetchHealth() {
      if (!serviceState || document.hidden) return;
      if (healthAbortController) healthAbortController.abort();
      healthAbortController = new AbortController();
      fetch(healthApi + "?_=" + Date.now(), { signal: healthAbortController.signal, cache: "no-store" })
        .then(function (res) {
          if (!res.ok) throw new Error("status " + res.status);
          return res.json();
        })
        .then(function (payload) {
          var running = !!payload.service_running;
          var enabled = !!payload.service_enabled;
          serviceHealthState = { enabled: enabled, running: running, loaded: true };
          serviceState.innerHTML = '<span class="status-dot" aria-hidden="true"></span>'
            + (running ? "运行中" : (enabled ? "已停止" : "未启用"));
          serviceState.className = "command-value " + (running ? "status-ok" : (enabled ? "status-bad" : "status-warn"));
          renderCommandBar();
        })
        .catch(function (err) {
          if (err && err.name === "AbortError") return;
          serviceState.innerHTML = '<span class="status-dot" aria-hidden="true"></span>未知';
          serviceState.className = "command-value status-warn";
        });
    }

    function taskTimeValue(task) {
      return String((task && task.time_local) || "");
    }

    function latestTaskTimeForAccount(row) {
      var tasks = (row && row.tasks) || {};
      var keys = Object.keys(tasks);
      var best = "";
      for (var i = 0; i < keys.length; i += 1) {
        var val = taskTimeValue(tasks[keys[i]]);
        if (val && (!best || val > best)) best = val;
      }
      return best;
    }

    function latestTaskTime(rows) {
      var best = "";
      for (var i = 0; i < rows.length; i += 1) {
        var val = latestTaskTimeForAccount(rows[i]);
        if (val && (!best || val > best)) best = val;
      }
      return best;
    }

    function hasAnomalyTask(row) {
      var tasks = (row && row.tasks) || {};
      var keys = Object.keys(tasks);
      for (var i = 0; i < keys.length; i += 1) {
        var status = String(((tasks[keys[i]] || {}).status) || "UNKNOWN").toUpperCase();
        if (status === "FAILED" || status === "PARTIAL") return true;
      }
      return false;
    }

    function hasSymbolDetails(row) {
      var tasks = (row && row.tasks) || {};
      var keys = Object.keys(tasks);
      for (var i = 0; i < keys.length; i += 1) {
        var pairs = ops.parseSummaryPairs((tasks[keys[i]] || {}).summary || "--");
        var pairKeys = Object.keys(pairs);
        for (var j = 0; j < pairKeys.length; j += 1) {
          if (pairKeys[j].indexOf("_symbols") > 0) return true;
        }
      }
      return false;
    }

    function visibleTaskAccounts(rows) {
      var sorted = (rows || []).slice();
      if (taskFilter === "anomaly") return sorted.filter(hasAnomalyTask);
      if (taskFilter === "symbols") return sorted.filter(hasSymbolDetails);
      return sorted;
    }

    function taskMetaLabel(taskKey) {
      if (taskKey === "equity_recovery_take_profit") return "巡检内触发";
      if (taskKey === "manage") return "例行巡检";
      return "";
    }

    function symbolList(rawValue) {
      var raw = String(rawValue || "").trim();
      if (!raw || raw === "-" || raw === "--") return [];
      return raw.split(",").map(function (item) {
        return String(item || "").trim();
      }).filter(Boolean);
    }

    function formatTaskResultLines(taskKey, task) {
      var t = task || {};
      var rawSummary = String(t.summary || "--");
      var pairs = ops.parseSummaryPairs(rawSummary);
      var statParts = [];
      var symbolLines = [];
      var errorDetail = "";

      function addSymbols(label, key) {
        var items = symbolList(pairs[key]);
        if (!items.length) return;
        symbolLines.push({ label: label, key: key, symbols: items });
      }

      if (taskKey === "entry") {
        statParts.push("开仓 " + (pairs.opened || "0"));
        statParts.push("失败 " + (pairs.failed || "0"));
        statParts.push("跳过 " + (pairs.skipped || "0"));
        addSymbols("失败交易对", "failed_symbols");
        addSymbols("跳过交易对", "skipped_symbols");
      } else if (taskKey === "daily_loss_cut") {
        statParts.push("检查 " + (pairs.total || "0"));
        statParts.push("平仓 " + (pairs.closed || "0"));
        statParts.push("异常 " + (pairs.errors || "0"));
        addSymbols("平仓交易对", "closed_symbols");
        addSymbols("失败交易对", "failed_symbols");
      } else if (taskKey === "noon_protection") {
        statParts.push("检查 " + (pairs.total || "0"));
        statParts.push("更新 " + (pairs.updated || "0"));
        statParts.push("跳过 " + (pairs.skipped || "0"));
        statParts.push("异常 " + (pairs.errors || "0"));
        addSymbols("失败交易对", "failed_symbols");
      } else if (taskKey === "manage") {
        if (pairs.reason) statParts.push("原因：" + pairs.reason);
        if (pairs.error) {
          errorDetail = pairs.error;
          statParts.push("错误：" + (pairs.error.length > 90 ? pairs.error.slice(0, 90) + "…" : pairs.error));
        }
        if (pairs.total) statParts.push("持仓 " + pairs.total);
        if (pairs.tp) statParts.push("止盈 " + pairs.tp);
        if (pairs.sl) statParts.push("止损 " + pairs.sl);
        if (pairs.timeout) statParts.push("到期 " + pairs.timeout);
        if (pairs.updated) statParts.push("更新 " + pairs.updated);
        if (pairs.errors) statParts.push("异常 " + pairs.errors);
      } else if (taskKey === "equity_recovery_take_profit") {
        statParts.push("调整 " + (pairs.adjusted || "0"));
        statParts.push("异常 " + (pairs.errors || "0"));
        if (pairs.reduced) statParts.push("减仓 " + pairs.reduced);
      } else {
        Object.keys(pairs).forEach(function (k) { statParts.push(k + "=" + pairs[k]); });
      }
      if (!errorDetail && rawSummary.length > 110) {
        errorDetail = rawSummary;
      }

      return {
        statLine: statParts.join(" · ") || "--",
        symbolLines: symbolLines,
        errorDetail: errorDetail
      };
    }

    function taskResultHtml(accountId, taskKey, task) {
      var formatted = formatTaskResultLines(taskKey, task);
      var html = '<div class="task-result-lines">'
        + '<div class="task-stat-line">' + escapeHtml(formatted.statLine) + "</div>";
      for (var i = 0; i < formatted.symbolLines.length; i += 1) {
        var line = formatted.symbolLines[i];
        html += '<div class="task-symbol-line">'
          + '<span class="task-symbol-label">' + escapeHtml(line.label + "：") + "</span>"
          + '<span class="task-symbol-text">' + escapeHtml(line.symbols.join(", ")) + "</span>"
          + "</div>";
      }
      if (formatted.errorDetail) {
        html += '<details class="task-error-details"><summary>查看完整详情</summary><div class="task-error-full">' + escapeHtml(formatted.errorDetail) + "</div></details>";
      }
      html += "</div>";
      return html;
    }

    function taskRowHtml(accountId, taskKey, name, task) {
      var t = task || {};
      var status = String(t.status || "UNKNOWN");
      var statusText = taskStatusText(status);
      var cls = taskStatusCls(status);
      var timeText = String(t.time_local || "") || "--";
      var metaText = taskMetaLabel(taskKey);
      return '<div class="task-row">'
        + '<div class="task-row-main">'
        + '<div class="task-name-wrap"><span class="task-name">' + escapeHtml(name) + "</span>"
        + (metaText ? '<span class="task-task-tag">' + escapeHtml(metaText) + "</span>" : "")
        + "</div>"
        + '<span class="task-meta"><span class="task-badge ' + cls + '">' + escapeHtml(statusText) + "</span></span>"
        + '<span class="task-time">' + escapeHtml(timeText) + "</span>"
        + '<span class="task-result">' + taskResultHtml(accountId, taskKey, t) + "</span>"
        + "</div>"
        + "</div>";
    }

    function taskRowsForAccount(row) {
      var mode = String((row && row.mode) || "full").toLowerCase();
      var tasks = (row && row.tasks) || {};
      var rows = [];
      if (mode === "readonly") {
        var stats = row && row.trade_stats;
        if (stats) {
          var profitFactor = fmt(stats.profit_factor, 2);
          return '<div class="task-row">'
            + '<div class="task-row-main">'
            + '<div class="task-name-wrap"><span class="task-name">近 30 日交易统计</span>'
            + '<span class="task-task-tag">只读监控</span></div>'
            + '<span class="task-meta"><span class="task-badge status-ok">只读</span></span>'
            + '<span class="task-time">' + escapeHtml(ops.formatBeijingTime(stats.last_updated_utc, { withYear: true })) + "</span>"
            + '<div class="task-result-lines">'
            + '<div class="task-stat-line">净盈亏 ' + ops.fmtSignedUsdt(stats.net_realized_pnl != null ? stats.net_realized_pnl : stats.total_realized_pnl, 2) + " USDT · 胜率 " + fmt(stats.win_rate_pct, 1) + "% · 盈亏比 " + profitFactor + "</div>"
            + '<div class="task-stat-line">胜 ' + fmt(stats.win_count, 0) + " / 负 " + fmt(stats.loss_count, 0) + " · 平均盈利 " + ops.fmtUsdt(stats.avg_win, 2) + " · 平均亏损 " + ops.fmtUsdt(stats.avg_loss, 2) + "</div>"
            + "</div></div></div>";
        }
        return '<div class="task-row">'
          + '<div class="task-row-main">'
          + '<div class="task-name-wrap"><span class="task-name">账户监控</span>'
          + '<span class="task-task-tag">只读监控</span></div>'
          + '<span class="task-meta"><span class="task-badge status-warn">只读</span></span>'
          + '<span class="task-time">--</span>'
          + '<div class="task-result-lines"><div class="task-stat-line">暂无近 30 日交易统计数据</div></div>'
          + "</div></div>";
      }
      if (mode === "full") rows.push({ key: "entry", name: "每日开仓", task: tasks.entry });
      rows.push({ key: "daily_loss_cut", name: "浮亏砍仓", task: tasks.daily_loss_cut });
      rows.push({ key: "noon_protection", name: "中午保护", task: tasks.noon_protection });
      if (mode === "full") {
        rows.push({ key: "manage", name: "仓位巡检", task: tasks.manage });
        if (row && row.equity_recovery_take_profit_enabled) {
          rows.push({ key: "equity_recovery_take_profit", name: "组合止盈监控", task: tasks.equity_recovery_take_profit });
        }
      }
      var html = "";
      for (var i = 0; i < rows.length; i += 1) {
        html += taskRowHtml(String((row && row.account_id) || ""), rows[i].key, rows[i].name, rows[i].task);
      }
      return html;
    }

    function entryProgressStatus(status) {
      var raw = String(status || "UNKNOWN").toUpperCase();
      if (raw === "COMPLETED") return { text: "已完成", cls: "status-ok" };
      if (raw === "WAITING") return { text: "等待先阳后阴信号", cls: "status-warn" };
      if (raw === "RUNNING") return { text: "执行中", cls: "status-warn" };
      if (raw === "PARTIAL") return { text: "部分完成", cls: "status-warn" };
      if (raw === "SKIPPED") return { text: "已跳过", cls: "status-warn" };
      if (raw === "FAILED") return { text: "失败", cls: "status-bad" };
      return { text: "未开始", cls: "status-warn" };
    }

    function compactLocalTime(value) {
      var raw = String(value || "");
      return raw.length >= 16 ? raw.slice(11, 16) : raw;
    }

    function formatEntryCountdown(milliseconds) {
      var totalSeconds = Math.max(0, Math.ceil(Number(milliseconds || 0) / 1000));
      var hours = Math.floor(totalSeconds / 3600);
      var minutes = Math.floor((totalSeconds % 3600) / 60);
      var seconds = totalSeconds % 60;
      if (hours > 0) return "剩余 " + hours + "小时" + String(minutes).padStart(2, "0") + "分";
      return "剩余 " + String(minutes).padStart(2, "0") + "分" + String(seconds).padStart(2, "0") + "秒";
    }

    function syncCountdownTimer() {
      if (nextEntryCheckRaw && !countdownTimer) {
        countdownTimer = setInterval(updateEntryProgressCountdown, 1000);
      } else if (!nextEntryCheckRaw && countdownTimer) {
        clearInterval(countdownTimer);
        countdownTimer = null;
      }
    }

    function updateEntryProgressCountdown() {
      if (document.hidden) return;
      var countdown = document.getElementById("entry-progress-countdown");
      if (!countdown) return;
      var targetMs = ops.parseTimestampMs(nextEntryCheckRaw);
      if (!Number.isFinite(targetMs)) {
        countdown.textContent = nextEntryCheckRaw ? "等待时间同步" : "今日无待检查任务";
        return;
      }
      var remainingMs = targetMs - Date.now();
      countdown.textContent = remainingMs <= 0 ? "正在检查" : formatEntryCountdown(remainingMs);
    }

    function progressItems(value) {
      return Array.isArray(value) ? value : [];
    }

    function entryPhaseMeta(item) {
      var phase = String((item || {}).entry_phase || "INITIAL").toUpperCase();
      if (phase === "WAIT_BULLISH") return { label: "第二笔 · 等待阳线", chipLabel: "等待阳线", note: "下一根阳线后才等阴线", cls: "warn", variant: "wait-bullish" };
      if (phase === "WAIT_BEARISH") return { label: "第二笔 · 阳线已确认，等阴线", chipLabel: "阳后等阴线", note: "下一根阴线触发独立开仓", cls: "warn", variant: "wait-bearish" };
      if (phase === "POST_INITIAL_CANDLE") return { label: "首笔 · 确认中", chipLabel: "确认中", note: "等待阴线收盘", cls: "warn", variant: "confirming" };
      if (phase === "COMPLETE") return { label: "已完成", chipLabel: "已完成", note: "信号已消费", cls: "ok", variant: "complete" };
      return { label: "第一笔 · 等阴线", chipLabel: "等待阴线", note: "等待首根1h阴线", cls: "warn", variant: "initial" };
    }

    function entryProgressPhase(progress) {
      var waitingRows = progressItems(progress && progress.waiting_symbols);
      if (waitingRows.length) {
        var phaseCounts = {};
        for (var i = 0; i < waitingRows.length; i += 1) {
          var phase = String((waitingRows[i] || {}).entry_phase || "INITIAL").toUpperCase();
          phaseCounts[phase] = Number(phaseCounts[phase] || 0) + 1;
        }
        if (phaseCounts.WAIT_BEARISH) return { label: "第二笔 · 阳线已确认，等阴线", note: "下一根阴线触发独立开仓", cls: "warn", variant: "wait-bearish" };
        if (phaseCounts.WAIT_BULLISH) return { label: "第二笔 · 等待阳线", note: "下一根阳线后才等阴线", cls: "warn", variant: "wait-bullish" };
        if (phaseCounts.POST_INITIAL_CANDLE) return { label: "首笔 · 确认中", note: "等待当前1h K线收盘", cls: "warn", variant: "confirming" };
        if (phaseCounts.INITIAL) return { label: "第一笔 · 等待阴线", note: "首根1h阴线确认后开仓", cls: "warn", variant: "initial" };
        return { label: "等待信号", note: "策略信号尚未确认", cls: "warn", variant: "unknown" };
      }
      var status = String((progress && progress.status) || "UNKNOWN").toUpperCase();
      if (status === "FAILED" || Number((progress || {}).failed_count || 0) > 0) {
        return { label: "需要处理", note: "存在失败或部分完成任务", cls: "bad" };
      }
      if (status === "COMPLETED") return { label: "今日已完成", note: "没有待处理的开仓信号", cls: "ok", variant: "complete" };
      if (status === "PARTIAL") return { label: "部分完成", note: "请查看异常与任务", cls: "warn", variant: "unknown" };
      return { label: "执行中", note: "等待后端状态同步", cls: "warn", variant: "unknown" };
    }

    function entryProgressWaitingHtml(progress) {
      var waitingRows = progressItems(progress && progress.waiting_symbols);
      if (!waitingRows.length) return '<div class="entry-progress-waiting-empty">无待处理信号</div>';
      var html = '<div class="entry-progress-waiting-list">';
      for (var i = 0; i < waitingRows.length; i += 1) {
        var item = waitingRows[i] || {};
        var meta = entryPhaseMeta(item);
        var next = compactLocalTime(item.next_check_local || (progress && progress.next_check_local));
        var title = meta.label + (next ? "，下次检查 " + next : "");
        html += '<span class="entry-progress-waiting-chip is-' + escapeHtml(meta.variant || "unknown") + '" aria-label="' + escapeHtml(title) + '">'
          + '<span class="entry-progress-waiting-chip-head"><i aria-hidden="true"></i><strong>' + escapeHtml(String(item.symbol || "--")) + "</strong></span>"
          + "<small>" + escapeHtml(meta.chipLabel + (next ? " · " + next : "")) + "</small>"
          + "</span>";
      }
      return html + "</div>";
    }

    function entryActionMeta(item) {
      var row = item || {};
      var stage = String(row.entry_stage || "INITIAL").toUpperCase();
      if (stage === "SCALE_IN") {
        return row.signal_independent ? { label: "二次独立", cls: "is-independent" } : { label: "二次", cls: "is-scale-in" };
      }
      return { label: "首笔", cls: "is-initial" };
    }

    function entryProgressKey(item) {
      var row = item || {};
      return String(row.entry_stage || "INITIAL").toUpperCase()
        + "|" + String(row.symbol || "") + "|" + compactLocalTime(row.opened_at_local);
    }

    function buildCommonEntryTimeline(progressRows) {
      var counts = {};
      var samples = {};
      for (var i = 0; i < progressRows.length; i += 1) {
        var openedRows = progressItems(progressRows[i].entry_actions);
        var seen = {};
        for (var j = 0; j < openedRows.length; j += 1) {
          var item = openedRows[j] || {};
          var key = entryProgressKey(item);
          if (!item.symbol || seen[key]) continue;
          seen[key] = true;
          counts[key] = Number(counts[key] || 0) + 1;
          samples[key] = item;
        }
      }
      var commonKeys = {};
      var grouped = {};
      var commonActionCount = 0;
      var expected = progressRows.length;
      Object.keys(counts).forEach(function (key) {
        if (!expected || counts[key] !== expected) return;
        commonKeys[key] = true;
        commonActionCount += 1;
        var item = samples[key] || {};
        var time = compactLocalTime(item.opened_at_local) || "--";
        if (!grouped[time]) grouped[time] = [];
        grouped[time].push({
          symbol: String(item.symbol || ""),
          entry_stage: String(item.entry_stage || "INITIAL").toUpperCase(),
          signal_independent: !!item.signal_independent
        });
      });
      var groups = Object.keys(grouped).sort().map(function (time) {
        return {
          time: time,
          actions: grouped[time].sort(function (left, right) {
            return String(left.symbol || "").localeCompare(String(right.symbol || ""));
          })
        };
      });
      return { groups: groups, commonKeys: commonKeys, commonActionCount: commonActionCount };
    }

    function entryProgressWindow(progress) {
      var times = progressItems(progress.entry_actions).map(function (item) {
        return compactLocalTime((item || {}).opened_at_local);
      }).filter(Boolean).sort();
      if (!times.length) return "--";
      if (times[0] === times[times.length - 1]) return "开仓 " + times[0];
      return "首单 " + times[0] + " · 末单 " + times[times.length - 1];
    }

    function entryProgressDetail(progress, commonKeys) {
      var waitingRows = progressItems(progress.waiting_symbols);
      if (waitingRows.length) {
        var waitingSymbols = waitingRows.map(function (item) { return String((item || {}).symbol || ""); }).filter(Boolean);
        var nextCheck = compactLocalTime(progress.next_check_local);
        return {
          text: "等待 " + (waitingSymbols.join(" · ") || String(progress.waiting_count || 0))
            + (nextCheck ? " · 下次 " + nextCheck : ""),
          cls: "warn"
        };
      }
      var failed = Math.max(0, Number(progress.failed_count || 0));
      if (failed > 0) return { text: "失败 " + failed + "，请查看任务日志", cls: "bad" };
      var differences = [];
      var openedRows = progressItems(progress.entry_actions);
      for (var i = 0; i < openedRows.length; i += 1) {
        var item = openedRows[i] || {};
        var key = entryProgressKey(item);
        if (!item.symbol || commonKeys[key]) continue;
        var meta = entryActionMeta(item);
        differences.push(meta.label + " " + String(item.symbol) + " " + (compactLocalTime(item.opened_at_local) || "--"));
      }
      if (differences.length) return { text: "差异入场 " + differences.join(" · "), cls: "warn" };
      return { text: "与共同榜单一致", cls: "ok" };
    }

    function entryTimelineActionHtml(item) {
      var meta = entryActionMeta(item);
      return '<span class="entry-progress-timeline-symbol ' + meta.cls + '"><i aria-hidden="true"></i>'
        + escapeHtml(meta.label) + " · " + escapeHtml(String((item || {}).symbol || "--")) + "</span>";
    }

    function renderEntryProgress() {
      if (!entryProgressBoard) return;
      var rows = summaryRows || [];
      var accountRows = [];
      var todayRows = [];
      var latest = "";
      for (var i = 0; i < rows.length; i += 1) {
        var row = rows[i] || {};
        if (String(row.mode || "full").toLowerCase() !== "full") continue;
        accountRows.push(row);
        var progress = row.entry_progress || null;
        if (progress && progress.is_today !== false) todayRows.push(progress);
        var updated = String((progress && progress.updated_at_local) || "");
        if (updated && (!latest || updated > latest)) latest = updated;
      }
      if (!accountRows.length) {
        nextEntryCheckRaw = "";
        syncCountdownTimer();
        ops.patchHtmlIfChanged(entryProgressBoard, '<div class="entry-progress-empty">暂无完整策略账号</div>');
        if (entryProgressUpdated) entryProgressUpdated.textContent = "数据更新时间 --";
        if (entryProgressToggle) entryProgressToggle.hidden = true;
        return;
      }

      var completedAccounts = 0;
      var targetTotal = 0;
      var entryActionTotal = 0;
      var initialActionTotal = 0;
      var scaleInActionTotal = 0;
      var positionTotal = 0;
      var waitingTotal = 0;
      var failedTotal = 0;
      var pendingActionTotal = 0;
      var skippedTotal = 0;
      var nextCheckRaw = "";
      for (var j = 0; j < todayRows.length; j += 1) {
        var summary = todayRows[j];
        if (String(summary.status || "").toUpperCase() === "COMPLETED") completedAccounts += 1;
        var completedActions = summary.entry_action_completed_count == null ? Number(summary.opened_count || 0) : Number(summary.entry_action_completed_count || 0);
        var targetActions = summary.entry_action_target_count == null ? Number(summary.target_count || 0) : Number(summary.entry_action_target_count || 0);
        entryActionTotal += Math.max(0, completedActions);
        targetTotal += Math.max(0, targetActions);
        initialActionTotal += Math.max(0, Number(summary.entry_action_initial_count || 0));
        scaleInActionTotal += Math.max(0, Number(summary.entry_action_scale_in_count || 0));
        positionTotal += Math.max(0, Number(summary.position_count == null ? summary.opened_count : summary.position_count));
        waitingTotal += Math.max(0, Number(summary.waiting_count || 0));
        failedTotal += Math.max(0, Number(summary.entry_action_failed_count == null ? summary.failed_count : summary.entry_action_failed_count));
        pendingActionTotal += Math.max(0, Number(summary.entry_action_pending_count || 0));
        skippedTotal += Math.max(0, Number(summary.entry_action_skipped_count == null ? summary.skipped_count : summary.entry_action_skipped_count));
        var summaryNextCheck = String(summary.next_check_local || "");
        if (summaryNextCheck && (!nextCheckRaw || summaryNextCheck < nextCheckRaw)) nextCheckRaw = summaryNextCheck;
      }
      nextEntryCheckRaw = waitingTotal > 0 ? nextCheckRaw : "";
      syncCountdownTimer();

      var timeline = buildCommonEntryTimeline(todayRows);
      var hasIssues = todayRows.length !== accountRows.length
        || completedAccounts !== accountRows.length
        || waitingTotal > 0
        || failedTotal > 0
        || !timeline.groups.length;
      // Default expanded when hasIssues is true, but ALWAYS let operator collapse/expand!
      var showDetails = entryDetailsUserOverride === null ? hasIssues : entryDetailsUserOverride;
      if (entryProgressToggle) {
        entryProgressToggle.hidden = false;
        entryProgressToggle.textContent = showDetails ? "收起账号明细" : "查看账号明细";
        entryProgressToggle.setAttribute("aria-expanded", showDetails ? "true" : "false");
      }

      var nextCheckLabel = nextCheckRaw ? compactLocalTime(nextCheckRaw) : "--";
      var nextActionLabel = waitingTotal > 0 ? "等待信号检查" : (completedAccounts === accountRows.length ? "今日已完成" : "等待状态同步");
      var overallPct = targetTotal > 0 ? Math.round(Math.min(100, (entryActionTotal / targetTotal) * 100)) : 0;
      var anomalyStatClass = failedTotal > 0 ? "bad" : "clear";
      var anomalyStatNote = failedTotal > 0 ? "需要处理" : "当前无异常";
      var timelineCaption = timeline.groups.length
        ? timeline.groups.length + " 个共同窗口 · " + timeline.commonActionCount + " 个共同入场动作"
        : "暂无共同完成窗口";

      var initialMeterPct = targetTotal > 0 ? Math.min(100, (initialActionTotal / targetTotal) * 100) : 0;
      var scaleInMeterPct = targetTotal > 0 ? Math.min(Math.max(0, 100 - initialMeterPct), (scaleInActionTotal / targetTotal) * 100) : 0;
      var failedMeterPct = targetTotal > 0 ? Math.min(Math.max(0, 100 - initialMeterPct - scaleInMeterPct), (failedTotal / targetTotal) * 100) : 0;
      var skippedMeterPct = targetTotal > 0 ? Math.min(Math.max(0, 100 - initialMeterPct - scaleInMeterPct - failedMeterPct), (skippedTotal / targetTotal) * 100) : 0;
      var pendingMeterPct = Math.min(Math.max(0, 100 - initialMeterPct - scaleInMeterPct - failedMeterPct - skippedMeterPct), (pendingActionTotal / Math.max(1, targetTotal)) * 100);

      var html = '<div class="entry-progress-overview">'
        + '<div class="entry-progress-overview-primary">'
        + '<div class="entry-progress-hero-row"><div><span class="entry-progress-hero-label">开仓动作</span><strong>' + entryActionTotal + "<small> / " + targetTotal + '</small></strong></div><span class="entry-progress-hero-percent">' + overallPct + "%</span></div>"
        + '<div class="entry-progress-hero-caption">首笔 ' + initialActionTotal + " · 二次 " + scaleInActionTotal + " · 持仓记录 " + positionTotal + " · " + completedAccounts + " / " + accountRows.length + " 个账号完成</div>"
        + '<div class="entry-progress-summary-meter" role="img" aria-label="' + entryActionTotal + " 个入场动作已完成，共 " + targetTotal + " 个；" + waitingTotal + " 个等待信号，" + failedTotal + ' 个异常">'
        + '<span class="summary-meter-initial" style="width:' + initialMeterPct.toFixed(2) + '%"></span>'
        + '<span class="summary-meter-scale-in" style="width:' + scaleInMeterPct.toFixed(2) + '%"></span>'
        + '<span class="summary-meter-waiting" style="width:' + pendingMeterPct.toFixed(2) + '%"></span>'
        + '<span class="summary-meter-failed" style="width:' + failedMeterPct.toFixed(2) + '%"></span>'
        + '<span class="summary-meter-skipped" style="width:' + skippedMeterPct.toFixed(2) + '%"></span>'
        + "</div>"
        + '<div class="entry-progress-summary-legend"><span><i class="legend-dot initial" aria-hidden="true"></i>' + initialActionTotal + ' 首笔</span><span><i class="legend-dot scale-in" aria-hidden="true"></i>' + scaleInActionTotal + ' 二次</span><span><i class="legend-dot waiting" aria-hidden="true"></i>' + waitingTotal + ' 待触发信号</span><span><i class="legend-dot failed" aria-hidden="true"></i>' + failedTotal + " 异常</span></div>"
        + "</div>"
        + '<div class="entry-progress-stat warn"><span class="entry-progress-stat-label">待触发</span><strong>' + waitingTotal + '</strong><span class="entry-progress-stat-note">等待信号确认</span></div>'
        + '<div class="entry-progress-stat ' + anomalyStatClass + '"><span class="entry-progress-stat-label">异常</span><strong>' + failedTotal + '</strong><span class="entry-progress-stat-note">' + anomalyStatNote + "</span></div>"
        + '<div class="entry-progress-stat neutral"><span class="entry-progress-stat-label">账号状态</span><strong>' + completedAccounts + " / " + accountRows.length + '</strong><span class="entry-progress-stat-note">今日完成账号</span></div>'
        + '<div class="entry-progress-next-action"><span class="entry-progress-next-label">下一检查</span><strong>' + escapeHtml(nextCheckLabel) + '</strong><span class="entry-progress-next-state"><i class="entry-progress-next-pulse" aria-hidden="true"></i>' + escapeHtml(nextActionLabel) + ' · <span id="entry-progress-countdown">--</span></span></div>'
        + "</div>";

      html += '<div class="entry-progress-timeline"><div class="entry-progress-timeline-head"><div class="entry-progress-timeline-title"><strong>共同入场窗口</strong><span>' + escapeHtml(timelineCaption) + '</span></div><div class="entry-progress-timeline-legend"><span class="entry-progress-phase-legend is-initial"><i aria-hidden="true"></i>首笔入场</span><span class="entry-progress-phase-legend is-scale-in"><i aria-hidden="true"></i>二次入场</span><span class="entry-progress-phase-legend is-wait-bullish"><i aria-hidden="true"></i>等待阳线</span><span class="entry-progress-phase-legend is-wait-bearish"><i aria-hidden="true"></i>阳线已确认 · 等阴线</span><span class="entry-progress-zone">北京时间 UTC+8</span></div></div><div class="entry-progress-timeline-track">';
      if (timeline.groups.length) {
        for (var k = 0; k < timeline.groups.length; k += 1) {
          var group = timeline.groups[k];
          var groupActions = group.actions || [];
          var scaleOnly = groupActions.length > 0 && groupActions.every(function (item) {
            return String((item || {}).entry_stage || "INITIAL").toUpperCase() === "SCALE_IN";
          });
          html += '<div class="entry-progress-timeline-event is-complete' + (scaleOnly ? " is-scale-in" : "") + '"><div class="entry-progress-timeline-event-top"><span class="entry-progress-timeline-event-state">已完成</span><time>' + escapeHtml(group.time) + '</time></div><span class="entry-progress-timeline-actions">'
            + groupActions.map(entryTimelineActionHtml).join("") + "</span></div>";
        }
      } else {
        html += '<div class="entry-progress-detail warn">各账号榜单存在差异或尚未开始，见下方账号明细</div>';
      }
      if (nextCheckRaw) {
        html += '<div class="entry-progress-timeline-event entry-progress-timeline-next is-next"><div class="entry-progress-timeline-event-top"><span class="entry-progress-timeline-event-state">下一检查</span><time>' + escapeHtml(nextCheckLabel) + "</time></div><span>待检查 " + waitingTotal + " 个信号</span></div>";
      }
      html += "</div></div>"
        + '<div class="entry-progress-details' + (showDetails ? "" : " is-collapsed") + '">'
        + '<div class="entry-progress-header"><span>账号与进度</span><span>当前判断</span><span>等待信号</span><span>下一动作</span></div>';

      for (var n = 0; n < accountRows.length; n += 1) {
        var account = accountRows[n] || {};
        var accProgress = account.entry_progress || null;
        var rawAid = String(account.account_id || "");
        var aid = escapeHtml(rawAid);
        if (!accProgress || accProgress.is_today === false) {
          html += '<div class="entry-progress-row">'
            + '<div class="entry-progress-account progress-account-cell"><div class="entry-progress-account-head"><strong>' + aid + '</strong><span class="entry-progress-account-status status-warn">未开始</span></div><span class="entry-progress-account-meta">今日尚未执行</span><div class="entry-progress-progress-line"><strong>0</strong><span>/ 0 入场动作</span><em>0%</em></div><div class="entry-progress-meter" role="progressbar" aria-label="' + escapeHtml(rawAid + " 入场动作进度") + '" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100"></div></div>'
            + '<div class="entry-progress-phase"><span class="entry-progress-phase-label phase-initial">未开始</span><span class="entry-progress-phase-note">等待今日榜单</span></div>'
            + '<div class="entry-progress-waiting-cell"><div class="entry-progress-waiting-list"><div class="entry-progress-waiting-empty">等待今日榜单</div></div></div>'
            + '<div class="entry-progress-next-cell"><strong>--</strong><span>尚未开始</span></div></div>';
          continue;
        }
        var target = Math.max(0, Number(accProgress.entry_action_target_count == null ? accProgress.target_count : accProgress.entry_action_target_count));
        var opened = Math.max(0, Number(accProgress.entry_action_completed_count == null ? accProgress.opened_count : accProgress.entry_action_completed_count));
        var positionCount = Math.max(0, Number(accProgress.position_count == null ? accProgress.opened_count : accProgress.position_count));
        var waiting = Math.max(0, Number(accProgress.waiting_count || 0));
        var failed = Math.max(0, Number(accProgress.entry_action_failed_count == null ? accProgress.failed_count : accProgress.entry_action_failed_count));
        var skipped = Math.max(0, Number(accProgress.entry_action_skipped_count == null ? accProgress.skipped_count : accProgress.entry_action_skipped_count));
        var pendingActions = Math.max(0, Number(accProgress.entry_action_pending_count == null ? target - opened - failed - skipped : accProgress.entry_action_pending_count));
        var hasActionData = progressItems(accProgress.entry_actions).length > 0;
        var initialActions = Math.max(0, Number(accProgress.entry_action_initial_count || 0));
        var scaleInActions = Math.max(0, Number(accProgress.entry_action_scale_in_count || 0));
        if (!hasActionData && initialActions === 0) initialActions = opened;
        var denom = target > 0 ? target : Math.max(1, opened + waiting + failed);
        var openedPct = Math.min(100, (opened / denom) * 100);
        var initialPct = Math.min(100, (initialActions / denom) * 100);
        var scaleInPct = Math.min(Math.max(0, 100 - initialPct), (scaleInActions / denom) * 100);
        var failedPct = Math.min(Math.max(0, 100 - initialPct - scaleInPct), (failed / denom) * 100);
        var skippedPct = Math.min(Math.max(0, 100 - initialPct - scaleInPct - failedPct), (skipped / denom) * 100);
        var waitingPct = Math.min(Math.max(0, 100 - initialPct - scaleInPct - failedPct - skippedPct), (pendingActions / denom) * 100);
        var st = entryProgressStatus(accProgress.status);
        var phase = entryProgressPhase(accProgress);
        var nextText = accProgress.next_check_local ? compactLocalTime(accProgress.next_check_local) : "--";
        var detail = entryProgressDetail(accProgress, timeline.commonKeys);
        var waitingHtml = waiting ? entryProgressWaitingHtml(accProgress) : '<div class="entry-progress-detail ' + detail.cls + '">' + escapeHtml(detail.text) + "</div>";
        var nextNote = waiting ? "按小时收盘检查" : (failed ? "请查看任务日志" : "无需动作");
        var progressNoteParts = ["首笔 " + initialActions, "二次 " + scaleInActions, "记录 " + positionCount];
        if (waiting) progressNoteParts.push("待触发 " + waiting);
        if (failed) progressNoteParts.push("异常 " + failed);
        var progressNoteClass = failed ? "bad" : (waiting ? "warn" : "ok");
        var accountStatusText = waiting ? "等待中" : st.text;
        var accountMeta = "启动 " + compactLocalTime(accProgress.started_at_local) + " · " + entryProgressWindow(accProgress);
        html += '<div class="entry-progress-row">'
          + '<div class="entry-progress-account progress-account-cell"><div class="entry-progress-account-head"><strong>' + aid + '</strong><span class="entry-progress-account-status ' + st.cls + '">' + escapeHtml(accountStatusText) + '</span></div><span class="entry-progress-account-meta">' + escapeHtml(accountMeta) + '</span><div class="entry-progress-progress-line"><strong>' + opened + "</strong><span>/ " + target + " 入场动作</span><em>" + Math.round(openedPct) + '%</em></div><div class="entry-progress-meter" role="progressbar" aria-label="' + escapeHtml(rawAid + " 入场动作进度") + '" aria-valuenow="' + Math.round(openedPct) + '" aria-valuemin="0" aria-valuemax="100"><span class="entry-progress-meter-initial" style="width:' + initialPct.toFixed(2) + '%"></span><span class="entry-progress-meter-scale-in" style="width:' + scaleInPct.toFixed(2) + '%"></span><span class="entry-progress-meter-waiting" style="width:' + waitingPct.toFixed(2) + '%"></span><span class="entry-progress-meter-failed" style="width:' + failedPct.toFixed(2) + '%"></span><span class="entry-progress-meter-skipped" style="width:' + skippedPct.toFixed(2) + '%"></span></div><div class="entry-progress-progress-foot"><span>入场动作</span><span class="' + progressNoteClass + '">' + escapeHtml(progressNoteParts.join(" · ")) + "</span></div></div>"
          + '<div class="entry-progress-phase" aria-label="' + escapeHtml(st.text) + '"><span class="entry-progress-phase-label ' + phase.cls + " phase-" + escapeHtml(phase.variant || "unknown") + '">' + escapeHtml(phase.label) + '</span><span class="entry-progress-phase-note">' + escapeHtml(phase.note) + "</span></div>"
          + '<div class="entry-progress-waiting-cell">' + waitingHtml + "</div>"
          + '<div class="entry-progress-next-cell' + (waiting ? " is-waiting" : "") + '"><span class="entry-progress-next-cell-label">下一检查</span><strong>' + escapeHtml(nextText) + "</strong><span>" + escapeHtml(nextNote) + "</span></div></div>";
      }
      html += "</div>";
      ops.patchHtmlIfChanged(entryProgressBoard, html);
      updateEntryProgressCountdown();
      if (entryProgressUpdated) entryProgressUpdated.textContent = "数据更新时间 " + (latest || "--");
    }

    function createObserver() {
      if (curveObserver || !window.IntersectionObserver) return;
      curveObserver = new IntersectionObserver(function (entries) {
        for (var i = 0; i < entries.length; i += 1) {
          var ent = entries[i];
          if (!ent.isIntersecting) continue;
          var aid = ent.target.getAttribute("data-account-id") || "";
          if (aid) fetchCurve(aid);
          curveObserver.unobserve(ent.target);
        }
      }, { rootMargin: "200px 0px" });
    }

    function accountElement(className, aid) {
      if (!aid) return null;
      var card = document.getElementById("account-card-" + aid);
      if (card) {
        if (card.classList && card.classList.contains(className)) return card;
        var found = card.querySelector("." + className);
        if (found) return found;
      }
      return null;
    }

    function setAccountMetric(className, aid, text, cls) {
      var node = accountElement(className, aid);
      if (!node) return;
      node.textContent = text;
      node.classList.remove("status-ok", "status-warn", "status-bad", "spark-up", "spark-down", "spark-flat");
      if (cls) node.classList.add(cls);
      node.setAttribute("data-account-id", aid);
    }

    function chartGeometry(metrics, width, height, includeStop, stopPct) {
      var values = metrics.values;
      var pts = metrics.points || [];
      stopPct = Number.isFinite(stopPct) ? stopPct : portfolioStopPct;
      var min = 0;
      var max = 0;
      for (var i = 0; i < values.length; i += 1) {
        if (values[i] < min) min = values[i];
        if (values[i] > max) max = values[i];
      }
      if (includeStop) min = Math.min(min, -stopPct);
      var rawSpan = max - min;
      var padding = Math.max(0.2, rawSpan * 0.14);
      min -= padding;
      max += padding;
      var span = Math.max(0.4, max - min);
      function yFor(value) {
        return height - ((value - min) / span) * height;
      }

      // Map X coordinates by real timestamps when available so sparse data never fakes uniform ramps.
      var times = pts.map(function (p) { return p && Number.isFinite(p.ms) ? p.ms : NaN; });
      var validTimes = times.filter(Number.isFinite);
      var useTimeAxis = validTimes.length === values.length && validTimes.length >= 2;
      var minT = useTimeAxis ? Math.min.apply(null, validTimes) : 0;
      var maxT = useTimeAxis ? Math.max.apply(null, validTimes) : 1;
      var tSpan = maxT - minT;
      if (!useTimeAxis || tSpan <= 0) useTimeAxis = false;

      var padX = 6;
      var usableW = Math.max(1, width - padX * 2);
      var coords = [];
      var renderedPoints = [];
      for (var j = 0; j < values.length; j += 1) {
        var ratio = useTimeAxis
          ? ((times[j] - minT) / tSpan)
          : (values.length > 1 ? j / (values.length - 1) : 0.5);
        var x = padX + ratio * usableW;
        var y = yFor(values[j]);
        coords.push(x.toFixed(2) + "," + y.toFixed(2));
        renderedPoints.push({
          x: x,
          y: y,
          ratio: ratio,
          val: values[j],
          equity: pts[j] ? pts[j].equity : null,
          t: pts[j] ? pts[j].t : ""
        });
      }

      var zeroY = yFor(0);
      var firstX = renderedPoints.length ? renderedPoints[0].x : padX;
      var lastX = renderedPoints.length ? renderedPoints[renderedPoints.length - 1].x : (width - padX);
      var areaPoints = firstX.toFixed(2) + "," + zeroY.toFixed(2) + " "
        + coords.join(" ") + " "
        + lastX.toFixed(2) + "," + zeroY.toFixed(2);

      return {
        points: coords.join(" "),
        areaPoints: areaPoints,
        renderedPoints: renderedPoints,
        zeroY: zeroY,
        stopY: yFor(-stopPct)
      };
    }

    function renderPortfolioStopState(aid, stop, metrics) {
      var meterLabel = accountElement("stop-meter-label", aid);
      var meter = accountElement("stop-meter-fill", aid);
      var configuredLossPct = stop && ops.toNum(stop.loss_pct) > 0 ? ops.toNum(stop.loss_pct) : portfolioStopPct;

      if (metrics) {
        var ddTone = ops.classifyDrawdownTone(metrics.currentDrawdownPct, configuredLossPct);
        setAccountMetric("current-drawdown", aid, fmt(metrics.currentDrawdownPct, 2) + "%", ddTone ? "status-" + ddTone : "");
      } else {
        setAccountMetric("current-drawdown", aid, "--", "");
      }

      if (!portfolioStopEnabled) {
        setAccountMetric("stop-distance", aid, "未启用", "status-warn");
        setAccountMetric("risk-state", aid, "组合止损未启用", "status-warn");
        if (meterLabel) { meterLabel.textContent = "未启用"; meterLabel.className = "stop-meter-label status-warn"; }
        if (meter) { meter.style.setProperty("--meter-scale", "0"); meter.className = "stop-meter-fill status-warn"; }
        return;
      }
      if (!stop || !stop.status) {
        setAccountMetric("stop-distance", aid, "待同步", "status-warn");
        setAccountMetric("risk-state", aid, "等待后端止损状态", "status-warn");
        if (meterLabel) { meterLabel.textContent = "待同步"; meterLabel.className = "stop-meter-label status-warn"; }
        if (meter) { meter.style.setProperty("--meter-scale", "0"); meter.className = "stop-meter-fill status-warn"; }
        return;
      }

      var status = String(stop.status || "").toUpperCase();
      var distance = ops.toNum(stop.distance_pct);
      var backendReturn = ops.toNum(stop.current_return_pct);
      var currentReturn = backendReturn !== null ? backendReturn : (metrics ? metrics.currentReturnPct : null);
      var riskCls = "status-ok";
      var riskText = "风险正常";
      var stopDistanceText = distance !== null ? fmt(distance, 2) + "%" : "--";
      // Unified consumed stop-loss budget (0% safe -> 100% triggered), matching Detail page!
      var consumedPct = currentReturn !== null && currentReturn < 0
        ? Math.min(100, (Math.abs(currentReturn) / configuredLossPct) * 100)
        : 0;
      var meterText = consumedPct <= 0 ? "未消耗" : "已耗 " + fmt(consumedPct, 0) + "%";

      if (status === "TRIGGERED_COMPLETE") {
        riskCls = "status-bad";
        riskText = "组合止损已完成";
        stopDistanceText = "已完成";
        meterText = "已止损 100%";
        consumedPct = 100;
      } else if (status === "CLOSING") {
        riskCls = "status-bad";
        riskText = "组合止损执行中";
        stopDistanceText = "执行中";
        meterText = stop.state_inconsistent ? "等待对账 100%" : "平仓中 100%";
        consumedPct = 100;
      } else if (status === "THRESHOLD_REACHED") {
        riskCls = "status-bad";
        riskText = "已到止损阈值";
        stopDistanceText = distance !== null ? "已超出 " + fmt(Math.abs(distance), 2) + "%" : "已到阈值";
        meterText = "已达阈值 100%";
        consumedPct = 100;
      } else if (status === "MONITORING" && distance !== null) {
        riskCls = distance <= 1 ? "status-warn" : "status-ok";
        riskText = distance <= 1 ? "接近组合止损" : "风险正常";
      } else {
        riskCls = "status-warn";
        riskText = "等待后端止损状态";
        stopDistanceText = "待同步";
        meterText = "待同步";
        consumedPct = 0;
      }

      setAccountMetric("stop-distance", aid, stopDistanceText, riskCls);
      setAccountMetric("risk-state", aid, riskText, riskCls);
      if (meterLabel) {
        meterLabel.textContent = meterText;
        meterLabel.className = "stop-meter-label " + riskCls;
      }
      if (meter) {
        meter.style.setProperty("--meter-scale", (consumedPct / 100).toFixed(4));
        meter.className = "stop-meter-fill " + riskCls;
      }
    }

    function renderCurve(aid) {
      var box = accountElement("spark-box", aid);
      var deltaEl = accountElement("spark-delta", aid);
      if (!box || !deltaEl) return;
      var cached = curveCache[aid];
      var errMsg = curveErrors[aid];
      var mode = String(accountModes[aid] || "full").toLowerCase();
      var hasSummaryStop = Object.prototype.hasOwnProperty.call(portfolioStopStates, aid);
      var stop = hasSummaryStop ? portfolioStopStates[aid] : ((cached && cached.portfolioLossCut) || null);
      var backendReturn = stop ? ops.toNum(stop.current_return_pct) : null;

      if (errMsg && !cached) {
        box.className = "spark-box";
        box.innerHTML = '<div class="spark-empty is-error"><span>曲线加载失败</span><button class="ops-btn" type="button" data-retry-curve="' + escapeHtml(aid) + '">重试</button></div>';
        if (mode !== "readonly") renderPortfolioStopState(aid, stop, null);
        return;
      }

      var metrics = cached
        ? ops.computeCycleRiskMetrics(
            cached.points,
            mode,
            stop,
            { stopHour: portfolioStopHour, stopMinute: portfolioStopMinute, stopPct: portfolioStopPct },
            cached.cycleBaseline
          )
        : null;

      if (!metrics) {
        box.className = "spark-box";
        box.innerHTML = '<div class="spark-empty">' + (curveInFlight[aid] ? "加载中..." : "暂无曲线") + "</div>";
        if (mode !== "readonly" && backendReturn !== null) {
          var tone = ops.classifyReturnTone(backendReturn, portfolioStopPct, stop && stop.status);
          var backendCls = tone === "ok" ? "spark-up" : (tone === "bad" ? "spark-down" : "spark-flat");
          setAccountMetric("spark-delta", aid, (backendReturn >= 0 ? "+" : "") + fmt(backendReturn, 2) + "%", backendCls);
          renderPortfolioStopState(aid, stop, null);
        } else {
          setAccountMetric("spark-delta", aid, "--", "");
          if (mode !== "readonly") renderPortfolioStopState(aid, stop, null);
        }
        return;
      }

      var changePct = mode !== "readonly" && backendReturn !== null ? backendReturn : metrics.currentReturnPct;
      var tone2 = ops.classifyReturnTone(changePct, portfolioStopPct, stop && stop.status);
      var deltaCls = tone2 === "ok" ? "spark-up" : (tone2 === "bad" ? "spark-down" : "spark-flat");
      var width = mode === "readonly" ? 300 : 320;
      var height = mode === "readonly" ? 50 : 126;
      var includeStop = mode !== "readonly" && portfolioStopEnabled;
      var geometry = chartGeometry(metrics, width, height, includeStop, metrics.configuredLossPct);
      curveRenderData[aid] = geometry.renderedPoints;

      var signedReturn = (changePct >= 0 ? "+" : "") + fmt(changePct, 2) + "%";
      var svgLabel = aid + " 本账户周期权益曲线，当前变化 " + signedReturn + "，回撤 " + fmt(metrics.currentDrawdownPct, 2) + "%";
      var lastPt = geometry.renderedPoints[geometry.renderedPoints.length - 1];

      box.className = "spark-box " + deltaCls;
      box.innerHTML = ''
        + '<svg class="spark-svg" viewBox="0 0 ' + width + " " + height + '" preserveAspectRatio="none" role="img" aria-label="' + escapeHtml(svgLabel) + '">'
        + "<title>" + escapeHtml(svgLabel) + "</title>"
        + '<line class="spark-zero-line" x1="0" x2="' + width + '" y1="' + geometry.zeroY.toFixed(2) + '" y2="' + geometry.zeroY.toFixed(2) + '"></line>'
        + (includeStop ? '<line class="spark-stop-line" x1="0" x2="' + width + '" y1="' + geometry.stopY.toFixed(2) + '" y2="' + geometry.stopY.toFixed(2) + '"></line>' : "")
        + '<polygon class="spark-area" points="' + geometry.areaPoints + '"></polygon>'
        + '<polyline class="spark-path" points="' + geometry.points + '"></polyline>'
        + (lastPt ? '<circle class="spark-point-dot" cx="' + lastPt.x.toFixed(2) + '" cy="' + lastPt.y.toFixed(2) + '" r="3"></circle>' : "")
        + "</svg>"
        + '<div class="spark-hover-tip" hidden></div>';

      setAccountMetric("spark-delta", aid, signedReturn, deltaCls);
      if (mode === "readonly") return;
      renderPortfolioStopState(aid, stop, metrics);
    }

    var curveAbortControllers = {};
    function fetchCurve(aid, force) {
      if (!aid) return;
      var now = Date.now();
      var cached = curveCache[aid];
      if (!force && cached && cached.ts && (now - cached.ts) < curveTtlMs) {
        renderCurve(aid);
        return;
      }
      if (curveInFlight[aid] && !force) {
        renderCurve(aid);
        return;
      }
      curveInFlight[aid] = true;
      curveErrors[aid] = "";
      var url = pathPrefix + "/api/account/" + encodeURIComponent(aid) + "/curve?window_hours=24&curve_points=160&_=" + now;
      if (curveAbortControllers[aid]) {
        try { curveAbortControllers[aid].abort(); } catch (e) {}
      }
      var controller = new AbortController();
      curveAbortControllers[aid] = controller;
      fetch(url, { signal: controller.signal, cache: "no-store" })
        .then(function (res) {
          if (!res.ok) throw new Error("HTTP " + res.status);
          return res.json();
        })
        .then(function (payload) {
          curveInFlight[aid] = false;
          curveErrors[aid] = "";
          var mode = String(accountModes[aid] || "full").toLowerCase();
          var curve = mode === "readonly"
            ? (payload.balance_curve || payload.equity_curve || [])
            : (payload.balance_curve || []);
          var points = [];
          for (var i = 0; i < curve.length; i += 1) {
            var p = curve[i] || {};
            var n = Number(p.equity);
            if (Number.isFinite(n)) points.push({ t: p.t || "", equity: n });
          }
          if (payload.portfolio_loss_cut) portfolioStopStates[aid] = payload.portfolio_loss_cut;
          curveCache[aid] = {
            points: points.slice(-160),
            walletAsOfUtc: (payload.wallet && payload.wallet.as_of_utc) || "",
            cycleBaseline: payload.balance_curve_cycle_baseline || null,
            portfolioLossCut: payload.portfolio_loss_cut || portfolioStopStates[aid] || null,
            ts: Date.now()
          };
          renderCurve(aid);
          renderCommandBar();
        })
        .catch(function (err) {
          curveInFlight[aid] = false;
          if (err && err.name === "AbortError") return;
          curveErrors[aid] = (err && err.message) || "网络异常";
          renderCurve(aid);
        });
    }

    function primeCurveLoad() {
      createObserver();
      var nodes = document.querySelectorAll(".spark-box");
      for (var i = 0; i < nodes.length; i += 1) {
        var node = nodes[i];
        var aid = node.getAttribute("data-account-id") || "";
        renderCurve(aid);
        var cached = curveCache[aid];
        var fresh = !!(cached && cached.ts && (Date.now() - cached.ts) < curveTtlMs);
        if (!aid || fresh) continue;
        if (curveObserver) curveObserver.observe(node);
        else fetchCurve(aid);
      }
    }

    function managedStrategyNotes(rows) {
      var notes = [];
      for (var i = 0; i < (rows || []).length; i += 1) {
        var item = rows[i] || {};
        var m = String(item.mode || "full").toLowerCase();
        if (m === "readonly" || m === "loss_cut_only") continue;
        if (item.strategy_note) notes.push(String(item.strategy_note));
      }
      return notes;
    }

    function computeVenueState(aid, row) {
      var stopped = isAccountStopped(aid, row);
      var worst = worstAccountTaskStatus(row);
      if (stopped) return { cls: "status-bad", label: "Binance Futures · 组合止损已触发" };
      if (worst === "FAILED") return { cls: "status-bad", label: "Binance Futures · 存在失败任务" };
      if (worst === "PARTIAL") return { cls: "status-warn", label: "Binance Futures · 部分任务异常" };
      if (worst === "SUCCESS" || worst === "RUNNING") return { cls: "status-ok", label: "Binance Futures · 运行正常" };
      return { cls: "status-warn", label: "Binance Futures · 状态待同步" };
    }

    function patchExistingCards(rows) {
      var allNotes = managedStrategyNotes(rows);
      for (var i = 0; i < rows.length; i += 1) {
        var r = rows[i] || {};
        var mode = String(r.mode || "full").toLowerCase();
        if (mode === "loss_cut_only") continue;
        var aid = String(r.account_id || "");
        var card = document.getElementById("account-card-" + aid);
        if (!card) return false;
        var balanceEl = card.querySelector("[data-field='wallet-balance']");
        if (balanceEl) balanceEl.textContent = ops.fmtUsdt(r.wallet_balance_usdt, 2);
        if (mode === "readonly") {
          var stats = r.trade_stats || {};
          var netPnl = stats.net_realized_pnl;
          var pnlNumber = ops.toNum(netPnl);
          var pnlCls = pnlNumber !== null ? (pnlNumber >= 0 ? "status-ok" : "status-bad") : "";
          var pnlEl = card.querySelector("[data-field='readonly-pnl']");
          if (pnlEl) {
            pnlEl.textContent = pnlNumber !== null ? ops.fmtSignedUsdt(pnlNumber, 2) : "--";
            pnlEl.className = "metric-value pnl-value" + (pnlCls ? " " + pnlCls : "");
          }
          var winRateEl = card.querySelector("[data-field='readonly-win-rate']");
          if (winRateEl) winRateEl.textContent = stats.win_rate_pct != null ? fmt(stats.win_rate_pct, 1) + "%" : "--";
          var tradesEl = card.querySelector("[data-field='readonly-trades']");
          if (tradesEl) tradesEl.textContent = stats.total_trades != null ? fmt(stats.total_trades, 0) : "--";
          var pfEl = card.querySelector("[data-field='readonly-pf']");
          if (pfEl) pfEl.textContent = stats.profit_factor != null ? fmt(stats.profit_factor, 2) : "--";
        } else {
          var venueInfo = computeVenueState(aid, r);
          var venueEl = card.querySelector(".venue-state");
          if (venueEl) {
            venueEl.className = "venue-state " + venueInfo.cls;
            venueEl.setAttribute("title", venueInfo.label);
            venueEl.setAttribute("aria-label", venueInfo.label);
          }
          var openEl = card.querySelector("[data-field='open-positions']");
          if (openEl) openEl.textContent = fmt(r.open_positions, 0);
          var tagsEl = card.querySelector(".strategy-popover-tags");
          var noteKey = String(r.strategy_note || "");
          if (tagsEl && tagsEl.getAttribute("data-note") !== noteKey) {
            tagsEl.innerHTML = ops.formatStrategyTags(r.strategy_note, "strategy-tag");
            tagsEl.setAttribute("data-note", noteKey);
          }
          var highlightsEl = card.querySelector("[data-field='strategy-highlights']");
          if (highlightsEl && highlightsEl.getAttribute("data-note") !== noteKey) {
            highlightsEl.innerHTML = ops.formatStrategyHighlights(r.strategy_note, allNotes, 2, "strategy-tag");
            highlightsEl.setAttribute("data-note", noteKey);
          }
          var stopThresholdEl = card.querySelector("[data-field='stop-threshold-label']");
          if (stopThresholdEl) {
            stopThresholdEl.textContent = portfolioStopEnabled
              ? ("止损阈值 -" + fmt(r.portfolio_loss_cut ? r.portfolio_loss_cut.loss_pct : portfolioStopPct, 2) + "%")
              : "组合止损未启用";
          }
        }
      }
      return true;
    }

    function renderCards() {
      var rows = summaryRows || [];
      var managedHtml = "";
      var readonlyHtml = "";
      var sigParts = [];
      accountModes = {};
      portfolioStopStates = {};
      for (var i = 0; i < rows.length; i += 1) {
        var r = rows[i] || {};
        var mode = String(r.mode || "full").toLowerCase();
        if (mode === "loss_cut_only") continue;
        var aid = String(r.account_id || "");
        accountModes[aid] = mode;
        portfolioStopStates[aid] = r.portfolio_loss_cut || null;
        sigParts.push(aid + ":" + mode);
      }
      var nextSig = sigParts.join("|");
      if (nextSig && cards.getAttribute("data-card-sig") === nextSig && patchExistingCards(rows)) {
        primeCurveLoad();
        return;
      }

      if (curveObserver) curveObserver.disconnect();
      var allManagedNotes = managedStrategyNotes(rows);
      for (var j = 0; j < rows.length; j += 1) {
        var row = rows[j] || {};
        var rowMode = String(row.mode || "full").toLowerCase();
        if (rowMode === "loss_cut_only") continue;
        var rowAid = String(row.account_id || "");
        var safeAid = escapeHtml(rowAid);
        var base = pathPrefix + "/account/" + encodeURIComponent(rowAid) + "/";

        if (rowMode === "readonly") {
          var stats = row.trade_stats || {};
          var netPnl = ops.toNum(stats.net_realized_pnl);
          var pnlText = netPnl !== null ? ops.fmtSignedUsdt(netPnl, 2) : "--";
          var winRateText = stats.win_rate_pct != null ? fmt(stats.win_rate_pct, 1) + "%" : "--";
          var tradeCountText = stats.total_trades != null ? fmt(stats.total_trades, 0) : "--";
          var profitFactorText = stats.profit_factor != null ? fmt(stats.profit_factor, 2) : "--";
          var pnlCls = netPnl !== null ? (netPnl >= 0 ? "status-ok" : "status-bad") : "";
          readonlyHtml += '<article class="readonly-strip" id="account-card-' + safeAid + '" data-account-id="' + safeAid + '">'
            + '<div class="readonly-identity"><div><span class="readonly-id">' + safeAid + '</span><span class="readonly-badge">只读监控</span></div><div class="readonly-source">Binance Futures</div></div>'
            + '<div class="readonly-metric"><span class="metric-label">余额 (USDT)</span><strong class="metric-value" data-field="wallet-balance">' + ops.fmtUsdt(row.wallet_balance_usdt, 2) + "</strong></div>"
            + '<div class="readonly-metric"><span class="metric-label">近30日净盈亏</span><strong class="metric-value pnl-value ' + pnlCls + '" data-field="readonly-pnl">' + pnlText + "</strong></div>"
            + '<div class="readonly-metric"><span class="metric-label">胜率</span><strong class="metric-value" data-field="readonly-win-rate">' + winRateText + "</strong></div>"
            + '<div class="readonly-metric readonly-secondary"><span class="metric-label">平仓订单数</span><strong class="metric-value" data-field="readonly-trades">' + tradeCountText + "</strong></div>"
            + '<div class="readonly-metric readonly-secondary"><span class="metric-label">盈亏比</span><strong class="metric-value" data-field="readonly-pf">' + profitFactorText + "</strong></div>"
            + '<div class="readonly-chart"><div>'
            + '<div class="spark-box" data-account-id="' + safeAid + '"><div class="spark-empty">加载中...</div></div>'
            + '</div><div class="readonly-chart-meta"><span class="metric-label" title="按账户周期首条权益快照计算，含浮盈亏，不等同已实现净盈亏">本周期权益变化</span><strong class="metric-value spark-delta" data-account-id="' + safeAid + '">--</strong></div></div>'
            + '<div class="readonly-action"><a class="detail-link" href="' + base + '" aria-label="查看 ' + safeAid + ' 余额曲线与交易统计">余额曲线 / 交易统计</a></div>'
            + "</article>";
        } else {
          var venueInfo = computeVenueState(rowAid, row);
          var popoverId = "strategy-popover-" + j;
          var noteAttr = escapeHtml(String(row.strategy_note || ""));
          var highlightsHtml = ops.formatStrategyHighlights(row.strategy_note, allManagedNotes, 2, "strategy-tag");
          managedHtml += '<article class="account-card" id="account-card-' + safeAid + '" data-account-id="' + safeAid + '">'
            + '<header class="account-card-head"><span class="aid">' + safeAid + '</span><div class="account-head-actions">'
            + '<div class="strategy-popover-wrap"><button class="strategy-trigger" type="button" aria-expanded="false" aria-controls="' + popoverId + '" aria-label="' + safeAid + ' 策略配置">策略</button>'
            + '<div id="' + popoverId + '" class="strategy-popover" hidden aria-label="' + safeAid + ' 策略配置">'
            + '<div class="strategy-popover-head"><div class="strategy-popover-meta"><strong class="strategy-popover-title">策略配置</strong><span class="strategy-popover-subtitle">' + safeAid + '</span></div><button class="strategy-popover-close" type="button" aria-label="关闭策略说明">×</button></div>'
            + '<div class="strategy-popover-tags" data-note="' + noteAttr + '">' + ops.formatStrategyTags(row.strategy_note, "strategy-tag") + "</div></div></div>"
            + '<span class="venue-state ' + venueInfo.cls + '" title="' + escapeHtml(venueInfo.label) + '" aria-label="' + escapeHtml(venueInfo.label) + '">Binance Futures<span class="status-dot" aria-hidden="true"></span></span>'
            + '<a class="detail-link" href="' + base + '" aria-label="查看 ' + safeAid + ' 账户详情">详情</a></div></header>'
            + '<div class="account-card-body">'
            + (highlightsHtml ? '<div class="card-strategy-highlights" data-field="strategy-highlights" data-note="' + noteAttr + '">' + highlightsHtml + "</div>" : "")
            + '<div class="account-primary">'
            + '<div class="metric"><span class="metric-label">账户权益（USDT）</span><strong class="metric-value" data-field="wallet-balance">' + ops.fmtUsdt(row.wallet_balance_usdt, 2) + "</strong></div>"
            + '<div class="metric"><span class="metric-label">' + String(portfolioStopHour).padStart(2, "0") + ":" + String(portfolioStopMinute).padStart(2, "0") + ' 以来收益</span><strong class="metric-value return-value spark-delta" data-account-id="' + safeAid + '">--</strong></div>'
            + "</div>"
            + '<div class="account-secondary">'
            + '<div class="metric"><span class="metric-label">当前持仓数</span><strong class="metric-value" data-field="open-positions">' + fmt(row.open_positions, 0) + "</strong></div>"
            + '<div class="metric"><span class="metric-label">距高点回撤</span><strong class="metric-value current-drawdown" data-account-id="' + safeAid + '">--</strong></div>'
            + '<div class="metric"><span class="metric-label">距组合止损</span><strong class="metric-value stop-distance" data-account-id="' + safeAid + '">--</strong></div>'
            + "</div>"
            + '<div class="risk-summary"><span>账户周期风险</span><strong class="risk-state" data-account-id="' + safeAid + '">计算中</strong></div>'
            + '<div class="spark-block">'
            + '<div class="spark-title"><span class="label">本账户周期权益曲线</span><span class="label" data-field="stop-threshold-label">' + (portfolioStopEnabled ? ("止损阈值 -" + fmt(row.portfolio_loss_cut ? row.portfolio_loss_cut.loss_pct : portfolioStopPct, 2) + "%") : "组合止损未启用") + "</span></div>"
            + '<div class="spark-box" data-account-id="' + safeAid + '"><div class="spark-empty">加载中...</div></div>'
            + '<div class="stop-meter"><span>已耗止损</span><span class="stop-meter-track"><span class="stop-meter-fill" data-account-id="' + safeAid + '"></span></span><span class="stop-meter-label" data-account-id="' + safeAid + '">计算中</span></div>'
            + "</div></div>"
            + "</article>";
        }
      }
      var html = managedHtml ? '<div class="managed-grid">' + managedHtml + "</div>" : "";
      html += readonlyHtml;
      cards.setAttribute("data-card-sig", nextSig);
      cards.innerHTML = html || '<div class="task-empty"><strong>暂无账户数据</strong><span>等待账户快照写入</span></div>';
      primeCurveLoad();
    }

    function renderTaskBoardHeader(rows) {
      if (!taskUpdatedAt) return;
      var latest = latestTaskTime(rows || []);
      taskUpdatedAt.textContent = "数据更新时间 " + (latest || "--");
      ["all", "anomaly", "symbols"].forEach(function (key) {
        var btn = document.getElementById("task-filter-" + key);
        if (!btn) return;
        var isActive = key === taskFilter;
        btn.classList.toggle("active", isActive);
        btn.setAttribute("aria-pressed", isActive ? "true" : "false");
      });
    }

    function toggleTaskFilter(nextFilter) {
      taskFilter = nextFilter || "all";
      ops.updateQueryParams({
        task_filter: taskFilter === "anomaly" ? null : taskFilter
      });
      renderTaskBoard();
    }

    function renderTaskBoard() {
      var rows = visibleTaskAccounts(summaryRows || []);
      if (!taskBoard) return;
      renderTaskBoardHeader(summaryRows || []);
      var html = "";
      for (var i = 0; i < rows.length; i += 1) {
        var r = rows[i] || {};
        var aid = String(r.account_id || "");
        var safeAid = escapeHtml(aid);
        var mode = String(r.mode || "full").toLowerCase();
        var modeLabel = mode === "full" ? "完整策略" : (mode === "loss_cut_only" ? "止损保护" : "只读监控");
        var modeBadgeClass = "task-mode-badge" + (mode === "readonly" ? " mode-readonly" : "");
        var isAnomaly = hasAnomalyTask(r);
        var headerStatus = mode === "readonly" ? "READONLY" : worstAccountTaskStatus(r);
        var accountCls = isAnomaly ? "task-account-card task-account-anomaly" : "task-account-card";
        html += '<article class="' + accountCls + '">'
          + '<div class="task-account-head">'
          + '<div class="task-account-title">'
          + '<span class="task-account-id">' + safeAid + "</span>"
          + '<span class="' + modeBadgeClass + '">' + escapeHtml(modeLabel) + "</span>"
          + (r && r.equity_recovery_take_profit_enabled ? '<span class="task-feature-badge">组合止盈监控</span>' : "")
          + "</div>"
          + '<span class="task-last-run ' + taskStatusCls(headerStatus) + '">' + escapeHtml(taskStatusText(headerStatus)) + "</span>"
          + "</div>"
          + '<div class="task-table">'
          + '<div class="task-head">'
          + '<span class="task-col-h">任务</span>'
          + '<span class="task-col-h">状态</span>'
          + '<span class="task-col-h">时间</span>'
          + '<span class="task-col-h">结果</span>'
          + "</div>"
          + taskRowsForAccount(r)
          + "</div>"
          + "</article>";
      }
      var finalHtml = html
        || (taskFilter === "anomaly"
          ? '<div class="task-empty"><strong>暂无异常</strong><span>所有系统任务正常运行</span></div>'
          : '<div class="task-empty"><strong>暂无匹配任务</strong><span>切换筛选条件查看其他任务</span></div>');
      ops.patchHtmlIfChanged(taskBoard, finalHtml);
    }

    function mergeFastRows(rows) {
      var previousByAccount = {};
      for (var i = 0; i < (summaryRows || []).length; i += 1) {
        var previous = summaryRows[i] || {};
        var previousAid = String(previous.account_id || "");
        if (previousAid) previousByAccount[previousAid] = previous;
      }
      return (rows || []).map(function (row) {
        var next = row || {};
        var old = previousByAccount[String(next.account_id || "")];
        if (old) {
          if (!Object.prototype.hasOwnProperty.call(next, "tasks") && old.tasks) next.tasks = old.tasks;
          if (!Object.prototype.hasOwnProperty.call(next, "entry_progress") && old.entry_progress) next.entry_progress = old.entry_progress;
        }
        return next;
      });
    }

    function mergeSummaryDetails(payload) {
      if (!payload || payload.ready !== true) {
        if (!detailsRetryTimer) {
          detailsRetryTimer = window.setTimeout(function () {
            detailsRetryTimer = null;
            fetchSummaryDetails();
          }, 1000);
        }
        return;
      }
      if (detailsRetryTimer) {
        window.clearTimeout(detailsRetryTimer);
        detailsRetryTimer = null;
      }
      var detailsByAccount = {};
      var detailRows = payload.accounts || [];
      for (var i = 0; i < detailRows.length; i += 1) {
        var detail = detailRows[i] || {};
        var aid = String(detail.account_id || "");
        if (aid) detailsByAccount[aid] = detail;
      }
      for (var j = 0; j < summaryRows.length; j += 1) {
        var row = summaryRows[j] || {};
        var rowDetail = detailsByAccount[String(row.account_id || "")];
        if (!rowDetail) continue;
        if (Object.prototype.hasOwnProperty.call(rowDetail, "tasks")) row.tasks = rowDetail.tasks;
        if (Object.prototype.hasOwnProperty.call(rowDetail, "entry_progress")) row.entry_progress = rowDetail.entry_progress;
      }
      renderCommandBar(payload);
      patchExistingCards(summaryRows);
      renderEntryProgress();
      renderTaskBoard();
    }

    var summaryDetailsAbortController = null;
    function fetchSummaryDetails(options) {
      var force = !!(options && options.force);
      if (document.hidden && !force) return;
      if (summaryDetailsRequestInFlight && !force) return;
      if (summaryDetailsAbortController) {
        try { summaryDetailsAbortController.abort(); } catch (e) {}
      }
      summaryDetailsRequestInFlight = true;
      lastDetailsFetchAt = Date.now();
      summaryDetailsAbortController = new AbortController();
      fetch(summaryDetailsApi + "?_=" + Date.now(), { signal: summaryDetailsAbortController.signal, cache: "no-store" })
        .then(function (res) {
          if (!res.ok) throw new Error("HTTP " + res.status);
          return res.json();
        })
        .then(function (payload) {
          summaryDetailsRequestInFlight = false;
          detailsHydratedOnce = true;
          setOverviewError("details", "");
          mergeSummaryDetails(payload);
        })
        .catch(function (err) {
          summaryDetailsRequestInFlight = false;
          if (err && err.name === "AbortError") return;
          setOverviewError("details", "任务与开单进度读取失败：" + (err && err.message ? err.message : "网络异常"));
        });
    }

    function setOverviewRefreshBusy(busy) {
      if (!overviewRefreshBtn) return;
      overviewRefreshBtn.disabled = !!busy;
      overviewRefreshBtn.textContent = busy ? "刷新中..." : "刷新 ↻";
    }

    var summaryAbortController = null;
    function fetchSummary(options) {
      var force = !!(options && options.force);
      if (document.hidden && !force) return;
      if (summaryRequestInFlight && !force) return;
      if (summaryAbortController) {
        try { summaryAbortController.abort(); } catch (e) {}
      }
      summaryRequestInFlight = true;
      setOverviewRefreshBusy(true);
      summaryAbortController = new AbortController();
      fetch(summaryFastApi + "?_=" + Date.now(), { signal: summaryAbortController.signal, cache: "no-store" })
        .then(function (res) {
          if (!res.ok) throw new Error("HTTP " + res.status);
          return res.json();
        })
        .then(function (payload) {
          summaryRequestInFlight = false;
          setOverviewRefreshBusy(false);
          setOverviewError("summary", "");
          summaryRows = mergeFastRows(payload.accounts || []);
          ops.renderNavAccounts(summaryRows);
          renderCommandBar(payload);
          renderCards();
          renderEntryProgress();
          renderTaskBoard();
          if (force || !detailsHydratedOnce || (Date.now() - lastDetailsFetchAt) > Math.max(5000, refreshSec * 800)) {
            fetchSummaryDetails({ force: force });
          }
        })
        .catch(function (err) {
          summaryRequestInFlight = false;
          setOverviewRefreshBusy(false);
          if (err && err.name === "AbortError") return;
          setOverviewError("summary", "账户总览刷新失败：" + (err && err.message ? err.message : "网络异常") + "，页面可能不是最新数据");
        });
    }

    function triggerOverviewRefresh() {
      lastDetailsFetchAt = 0;
      curveCache = {};
      curveErrors = {};
      fetchSummary({ force: true });
      fetchHealth();
    }

    function jumpToAnomalies() {
      toggleTaskFilter("anomaly");
      var panel = document.getElementById("task-panel");
      if (panel) {
        var reduce = ops.prefersReducedMotion();
        panel.scrollIntoView({ behavior: reduce ? "auto" : "smooth", block: "start" });
        try { panel.focus({ preventScroll: true }); } catch (e) {}
      }
    }

    function closeStrategyPopovers(exceptWrap, restoreFocus) {
      var wraps = cards ? cards.querySelectorAll(".strategy-popover-wrap.is-open") : [];
      var focusTarget = null;
      for (var i = 0; i < wraps.length; i += 1) {
        if (wraps[i] === exceptWrap) continue;
        wraps[i].classList.remove("is-open");
        var card = wraps[i].closest ? wraps[i].closest(".account-card") : null;
        if (card) card.classList.remove("has-open-popover");
        var button = wraps[i].querySelector(".strategy-trigger");
        if (button) {
          button.setAttribute("aria-expanded", "false");
          if (restoreFocus && !focusTarget) focusTarget = button;
        }
        var pop = wraps[i].querySelector(".strategy-popover");
        if (pop) pop.hidden = true;
      }
      if (restoreFocus && focusTarget && focusTarget.focus) {
        focusTarget.focus();
      }
    }

    if (cards) {
      cards.addEventListener("click", function (event) {
        var retryCurveBtn = event.target && event.target.closest ? event.target.closest("[data-retry-curve]") : null;
        if (retryCurveBtn) {
          var retryAid = retryCurveBtn.getAttribute("data-retry-curve");
          if (retryAid) fetchCurve(retryAid, true);
          return;
        }
        var closeBtn = event.target && event.target.closest ? event.target.closest(".strategy-popover-close") : null;
        if (closeBtn) {
          event.stopPropagation();
          closeStrategyPopovers(null, true);
          return;
        }
        var trigger = event.target && event.target.closest ? event.target.closest(".strategy-trigger") : null;
        if (!trigger) return;
        event.stopPropagation();
        var wrap = trigger.closest(".strategy-popover-wrap");
        if (!wrap) return;
        var card = wrap.closest ? wrap.closest(".account-card") : null;
        var pop = wrap.querySelector(".strategy-popover");
        var shouldOpen = !wrap.classList.contains("is-open");
        closeStrategyPopovers(wrap, false);
        wrap.classList.toggle("is-open", shouldOpen);
        if (card) card.classList.toggle("has-open-popover", shouldOpen);
        if (pop) pop.hidden = !shouldOpen;
        trigger.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
      });

      // Lightweight hover readout on SVG sparklines
      cards.addEventListener("pointermove", function (event) {
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (!box) return;
        var aid = box.getAttribute("data-account-id");
        var pts = aid ? curveRenderData[aid] : null;
        var tip = box.querySelector(".spark-hover-tip");
        if (!pts || !pts.length || !tip) return;
        var rect = box.getBoundingClientRect();
        if (!rect.width) return;
        var ratio = Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width));
        var nearest = pts[0];
        var bestDist = Infinity;
        for (var i = 0; i < pts.length; i += 1) {
          var d = Math.abs(pts[i].ratio - ratio);
          if (d < bestDist) { bestDist = d; nearest = pts[i]; }
        }
        var timePart = nearest.t ? ops.formatBeijingTime(nearest.t) + " · " : "";
        var eqPart = nearest.equity != null ? ops.fmtUsdt(nearest.equity, 2) + " USDT · " : "";
        tip.textContent = timePart + eqPart + ops.fmtPct(nearest.val);
        tip.hidden = false;
      });

      cards.addEventListener("pointerout", function (event) {
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (!box) return;
        if (event.relatedTarget && box.contains(event.relatedTarget)) return;
        var tip = box.querySelector(".spark-hover-tip");
        if (tip) tip.hidden = true;
      });
    }

    document.addEventListener("click", function (event) {
      if (event.target && event.target.closest && event.target.closest(".strategy-popover-wrap")) return;
      closeStrategyPopovers(null, false);
    });

    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") return;
      var hasOpen = cards && cards.querySelector(".strategy-popover-wrap.is-open");
      if (hasOpen) {
        event.preventDefault();
        closeStrategyPopovers(null, true);
      }
    });

    if (overviewRetryBtn) overviewRetryBtn.addEventListener("click", triggerOverviewRefresh);
    if (overviewRefreshBtn) overviewRefreshBtn.addEventListener("click", triggerOverviewRefresh);
    if (incidentJumpBtn) incidentJumpBtn.addEventListener("click", jumpToAnomalies);

    var overviewAnomalyBtn = document.getElementById("overview-anomaly-btn");
    if (overviewAnomalyBtn) overviewAnomalyBtn.addEventListener("click", jumpToAnomalies);

    var filterAll = document.getElementById("task-filter-all");
    var filterAnomaly = document.getElementById("task-filter-anomaly");
    var filterSymbols = document.getElementById("task-filter-symbols");
    if (filterAll) filterAll.addEventListener("click", function () { toggleTaskFilter("all"); });
    if (filterAnomaly) filterAnomaly.addEventListener("click", function () { toggleTaskFilter("anomaly"); });
    if (filterSymbols) filterSymbols.addEventListener("click", function () { toggleTaskFilter("symbols"); });

    if (entryProgressToggle) {
      entryProgressToggle.addEventListener("click", function () {
        var currentlyExpanded = entryProgressToggle.getAttribute("aria-expanded") === "true";
        entryDetailsUserOverride = !currentlyExpanded;
        renderEntryProgress();
      });
    }

    window.toggleTaskFilter = toggleTaskFilter;

    fetchSummary();
    fetchHealth();

    var summaryTimer = setInterval(function () { fetchSummary(); }, refreshSec * 1000);
    var healthTimer = setInterval(fetchHealth, Math.max(30, refreshSec * 2) * 1000);

    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) {
        fetchSummary();
        fetchHealth();
        updateEntryProgressCountdown();
      }
    });

    window.addEventListener("online", function () { renderCommandBar(); });
    window.addEventListener("offline", function () { renderCommandBar(); });

    window.addEventListener("pagehide", function () {
      clearInterval(summaryTimer);
      clearInterval(healthTimer);
      if (countdownTimer) clearInterval(countdownTimer);
      if (curveObserver) {
        try { curveObserver.disconnect(); } catch (e) {}
        curveObserver = null;
      }
    });
  });
})();
