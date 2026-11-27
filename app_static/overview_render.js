/* Bubble Buster — Accounts Overview pure formatters, aggregators & HTML builders */
(function (global) {
  "use strict";

  var ops = global.BBOps;
  if (!ops) return;

  function escapeHtml(text) {
    return ops.escapeHtml(text);
  }

  function fmt(v, digits) {
    if (v === null || v === undefined || v === "") return "--";
    var n = Number(v);
    if (!Number.isFinite(n)) return String(v);
    return n.toFixed(digits);
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
    if (s === "READONLY") return "is-readonly";
    if (s === "SUCCESS" || s === "RUNNING") return "status-ok";
    if (s === "PARTIAL" || s === "SKIPPED" || s === "UNKNOWN") return "status-warn";
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

  function taskTimeValue(task) {
    return String((task && task.time_local) || "");
  }

  function latestTaskTimeForAccount(row) {
    var tasks = (row && row.tasks) || {};
    var keys = Object.keys(tasks);
    var best = "";
    var bestMs = -Infinity;
    for (var i = 0; i < keys.length; i += 1) {
      var val = taskTimeValue(tasks[keys[i]]);
      if (!val) continue;
      var ms = ops.parseTimestampMs(val);
      if (Number.isFinite(ms)) {
        if (ms > bestMs) { bestMs = ms; best = val; }
      } else if (!best || val > best) {
        best = val;
      }
    }
    return best;
  }

  function latestTaskTime(rows) {
    var best = "";
    var bestMs = -Infinity;
    for (var i = 0; i < (rows || []).length; i += 1) {
      var val = latestTaskTimeForAccount(rows[i]);
      if (!val) continue;
      var ms = ops.parseTimestampMs(val);
      if (Number.isFinite(ms)) {
        if (ms > bestMs) { bestMs = ms; best = val; }
      } else if (!best || val > best) {
        best = val;
      }
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

  function visibleTaskAccounts(rows, taskFilter) {
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
      html += '<details class="task-error-details" data-task-detail-key="' + escapeHtml(String(accountId || "") + ":" + String(taskKey || "")) + '"><summary>查看完整详情</summary><div class="task-error-full">' + escapeHtml(formatted.errorDetail) + "</div></details>";
    }
    html += "</div>";
    return html;
  }

  function taskRowHtml(accountId, taskKey, name, task) {
    var t = task || {};
    var status = String(t.status || "UNKNOWN");
    var upperStatus = status.toUpperCase();
    var isAnomalyRow = upperStatus === "FAILED" || upperStatus === "PARTIAL";
    var statusText = taskStatusText(status);
    var cls = taskStatusCls(status);
    var timeText = String(t.time_local || "") || "--";
    var metaText = taskMetaLabel(taskKey);
    return '<div class="task-row' + (isAnomalyRow ? " is-anomaly-row" : "") + '" role="none">'
      + '<div class="task-row-main" role="row">'
      + '<div class="task-name-wrap" role="cell"><span class="task-name">' + escapeHtml(name) + "</span>"
      + (metaText ? '<span class="task-task-tag">' + escapeHtml(metaText) + "</span>" : "")
      + "</div>"
      + '<div class="task-meta" role="cell"><span class="task-badge ' + cls + '">' + escapeHtml(statusText) + "</span></div>"
      + '<div class="task-time" role="cell">' + escapeHtml(timeText) + "</div>"
      + '<div class="task-result" role="cell">' + taskResultHtml(accountId, taskKey, t) + "</div>"
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
        return '<div class="task-row" role="none">'
          + '<div class="task-row-main" role="row">'
          + '<div class="task-name-wrap" role="cell"><span class="task-name">近 30 日交易统计</span>'
          + '<span class="task-task-tag">只读监控</span></div>'
          + '<div class="task-meta" role="cell"><span class="task-badge is-readonly">只读</span></div>'
          + '<div class="task-time" role="cell">' + escapeHtml(ops.formatBeijingTime(stats.last_updated_utc, { withYear: true })) + "</div>"
          + '<div class="task-result-lines" role="cell">'
          + '<div class="task-stat-line">净盈亏 ' + escapeHtml(ops.fmtSignedUsdt(stats.net_realized_pnl != null ? stats.net_realized_pnl : stats.total_realized_pnl, 2)) + " USDT · 胜率 " + escapeHtml(fmt(stats.win_rate_pct, 1)) + "% · 盈亏比 " + escapeHtml(profitFactor) + "</div>"
          + '<div class="task-stat-line">胜 ' + escapeHtml(fmt(stats.win_count, 0)) + " / 负 " + escapeHtml(fmt(stats.loss_count, 0)) + " · 平均盈利 " + escapeHtml(ops.fmtUsdt(stats.avg_win, 2)) + " · 平均亏损 " + escapeHtml(ops.fmtUsdt(stats.avg_loss, 2)) + "</div>"
          + "</div></div></div>";
      }
      return '<div class="task-row" role="none">'
        + '<div class="task-row-main" role="row">'
        + '<div class="task-name-wrap" role="cell"><span class="task-name">账户监控</span>'
        + '<span class="task-task-tag">只读监控</span></div>'
        + '<div class="task-meta" role="cell"><span class="task-badge is-readonly">只读</span></div>'
        + '<div class="task-time" role="cell">--</div>'
        + '<div class="task-result-lines" role="cell"><div class="task-stat-line">暂无近 30 日交易统计数据</div></div>'
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

  function chartGeometry(metrics, width, height, includeStop, stopPct, defaultStopPct) {
    var values = metrics.values;
    var pts = metrics.points || [];
    stopPct = Number.isFinite(stopPct) ? stopPct : (Number.isFinite(defaultStopPct) ? defaultStopPct : 3.5);
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
        xPct: (x / width) * 100,
        yPct: (y / height) * 100,
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

  function computeVenueState(stopped, row) {
    var worst = worstAccountTaskStatus(row);
    if (stopped) return { cls: "status-bad", label: "组合止损已触发" };
    if (worst === "FAILED") return { cls: "status-bad", label: "存在失败任务" };
    if (worst === "PARTIAL") return { cls: "status-warn", label: "部分任务异常" };
    if (worst === "SUCCESS" || worst === "RUNNING") return { cls: "status-ok", label: "运行正常" };
    return { cls: "status-warn", label: "状态待同步" };
  }

  function nonNegNum(primary, fallback) {
    var v = ops.toNum(primary);
    if (v === null && fallback !== undefined) v = ops.toNum(fallback);
    return v === null ? 0 : Math.max(0, v);
  }

  function computeEntryMeterPcts(counts, denom) {
    var safeDenom = denom > 0 ? denom : 1;
    var initial = Math.min(100, (counts.initial / safeDenom) * 100);
    var scaleIn = Math.min(Math.max(0, 100 - initial), (counts.scaleIn / safeDenom) * 100);
    var failed = Math.min(Math.max(0, 100 - initial - scaleIn), (counts.failed / safeDenom) * 100);
    var skipped = Math.min(Math.max(0, 100 - initial - scaleIn - failed), (counts.skipped / safeDenom) * 100);
    var waiting = Math.min(Math.max(0, 100 - initial - scaleIn - failed - skipped), (counts.waiting / safeDenom) * 100);
    return { initial: initial, scaleIn: scaleIn, failed: failed, skipped: skipped, waiting: waiting };
  }

  function entryMeterHtml(meterClass, label, pctNow, pcts) {
    return '<div class="' + meterClass + '" role="progressbar" aria-label="' + escapeHtml(label) + '" aria-valuenow="' + pctNow + '" aria-valuemin="0" aria-valuemax="100">'
      + '<span class="entry-progress-meter-initial" style="width:' + pcts.initial.toFixed(2) + '%"></span>'
      + '<span class="entry-progress-meter-scale-in" style="width:' + pcts.scaleIn.toFixed(2) + '%"></span>'
      + '<span class="entry-progress-meter-waiting" style="width:' + pcts.waiting.toFixed(2) + '%"></span>'
      + '<span class="entry-progress-meter-failed" style="width:' + pcts.failed.toFixed(2) + '%"></span>'
      + '<span class="entry-progress-meter-skipped" style="width:' + pcts.skipped.toFixed(2) + '%"></span>'
      + "</div>";
  }

  function buildEntryProgressHtml(accountRows, options) {
    var opts = options || {};
    var rows = accountRows || [];
    var todayRows = [];
    var latest = "";
    for (var i = 0; i < rows.length; i += 1) {
      var progress = (rows[i] || {}).entry_progress || null;
      if (progress && progress.is_today !== false) todayRows.push(progress);
      var updated = String((progress && progress.updated_at_local) || "");
      if (updated && (!latest || updated > latest)) latest = updated;
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
      entryActionTotal += nonNegNum(summary.entry_action_completed_count, summary.opened_count);
      targetTotal += nonNegNum(summary.entry_action_target_count, summary.target_count);
      initialActionTotal += nonNegNum(summary.entry_action_initial_count);
      scaleInActionTotal += nonNegNum(summary.entry_action_scale_in_count);
      positionTotal += nonNegNum(summary.position_count, summary.opened_count);
      waitingTotal += nonNegNum(summary.waiting_count);
      failedTotal += nonNegNum(summary.entry_action_failed_count, summary.failed_count);
      pendingActionTotal += nonNegNum(summary.entry_action_pending_count);
      skippedTotal += nonNegNum(summary.entry_action_skipped_count, summary.skipped_count);
      var summaryNextCheck = String(summary.next_check_local || "");
      if (summaryNextCheck && (!nextCheckRaw || summaryNextCheck < nextCheckRaw)) nextCheckRaw = summaryNextCheck;
    }

    var timeline = buildCommonEntryTimeline(todayRows);
    var hasIssues = failedTotal > 0;
    var override = opts.entryDetailsUserOverride;
    var showDetails = override === null || override === undefined ? hasIssues : !!override;
    var nextCheckLabel = nextCheckRaw ? compactLocalTime(nextCheckRaw) : "--";
    var nextActionLabel = waitingTotal > 0 ? "等待信号检查" : (completedAccounts === rows.length ? "今日已完成" : "等待状态同步");
    var overallPct = targetTotal > 0 ? Math.round(Math.min(100, (entryActionTotal / targetTotal) * 100)) : 0;
    var anomalyStatClass = failedTotal > 0 ? "bad" : "neutral";
    var anomalyStatNote = failedTotal > 0 ? "需要处理" : "当前无异常";
    var timelineCaption = timeline.groups.length
      ? timeline.groups.length + " 个共同窗口 · " + timeline.commonActionCount + " 个共同入场动作"
      : "暂无共同完成窗口";

    var summaryDenom = targetTotal > 0
      ? targetTotal
      : Math.max(1, initialActionTotal + scaleInActionTotal + failedTotal + skippedTotal + pendingActionTotal);
    var summaryPcts = computeEntryMeterPcts({
      initial: targetTotal > 0 ? initialActionTotal : 0,
      scaleIn: targetTotal > 0 ? scaleInActionTotal : 0,
      failed: targetTotal > 0 ? failedTotal : 0,
      skipped: targetTotal > 0 ? skippedTotal : 0,
      waiting: pendingActionTotal
    }, summaryDenom);

    var html = '<div class="entry-progress-overview">'
      + '<div class="entry-progress-overview-primary">'
      + '<div class="entry-progress-hero-row"><div><span class="entry-progress-hero-label">开仓动作</span><strong>' + entryActionTotal + "<small> / " + targetTotal + '</small></strong></div><span class="entry-progress-hero-percent">' + overallPct + "%</span></div>"
      + '<div class="entry-progress-hero-caption">首笔 ' + initialActionTotal + " · 二次 " + scaleInActionTotal + " · 持仓记录 " + positionTotal + " · " + completedAccounts + " / " + rows.length + " 个账号完成</div>"
      + entryMeterHtml("entry-progress-summary-meter", "今日总体入场动作进度", overallPct, summaryPcts)
      + '<div class="entry-progress-summary-legend"><span><i class="legend-dot initial" aria-hidden="true"></i>' + initialActionTotal + ' 首笔</span><span><i class="legend-dot scale-in" aria-hidden="true"></i>' + scaleInActionTotal + ' 二次</span><span><i class="legend-dot waiting" aria-hidden="true"></i>' + waitingTotal + ' 待触发信号</span><span><i class="legend-dot failed" aria-hidden="true"></i>' + failedTotal + " 异常</span></div>"
      + "</div>"
      + '<div class="entry-progress-stat ' + (waitingTotal > 0 ? "warn" : "neutral") + '"><span class="entry-progress-stat-label">待触发</span><strong>' + waitingTotal + '</strong><span class="entry-progress-stat-note">' + (waitingTotal > 0 ? "等待信号确认" : "暂无待触发信号") + "</span></div>"
      + '<div class="entry-progress-stat ' + anomalyStatClass + '"><span class="entry-progress-stat-label">异常</span><strong>' + failedTotal + '</strong><span class="entry-progress-stat-note">' + anomalyStatNote + "</span></div>"
      + '<div class="entry-progress-stat neutral"><span class="entry-progress-stat-label">账号状态</span><strong>' + completedAccounts + " / " + rows.length + '</strong><span class="entry-progress-stat-note">今日完成账号</span></div>'
      + '<div class="entry-progress-next-action"><span class="entry-progress-next-label">下一检查</span><strong>' + escapeHtml(nextCheckLabel) + '</strong><span class="entry-progress-next-state"><i class="entry-progress-next-pulse" aria-hidden="true"></i>' + escapeHtml(nextActionLabel) + ' · <span id="entry-progress-countdown">--</span></span></div>'
      + "</div>";

    html += '<div class="entry-progress-timeline"><div class="entry-progress-timeline-head"><div class="entry-progress-timeline-title"><strong>共同入场窗口</strong><span>' + escapeHtml(timelineCaption) + '</span></div></div><div class="entry-progress-timeline-track">';
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
    if (waitingTotal > 0 && nextCheckRaw) {
      html += '<div class="entry-progress-timeline-event entry-progress-timeline-next is-next"><div class="entry-progress-timeline-event-top"><span class="entry-progress-timeline-event-state">下一检查</span><time>' + escapeHtml(nextCheckLabel) + "</time></div><span>待检查 " + waitingTotal + " 个信号</span></div>";
    }
    html += "</div></div>"
      + '<div id="entry-progress-details-table" class="entry-progress-details' + (showDetails ? "" : " is-collapsed") + '" role="table" aria-label="各账号今日开单明细">'
      + '<div class="entry-progress-header" role="row"><span role="columnheader">账号与进度</span><span role="columnheader">当前判断</span><span role="columnheader">等待信号</span><span role="columnheader">下一动作</span></div>';

    for (var n = 0; n < rows.length; n += 1) {
      var account = rows[n] || {};
      var accProgress = account.entry_progress || null;
      var rawAid = String(account.account_id || "");
      var aid = escapeHtml(rawAid);
      if (!accProgress || accProgress.is_today === false) {
        html += '<div class="entry-progress-row" role="row">'
          + '<div class="entry-progress-account progress-account-cell" role="cell"><div class="entry-progress-account-head"><strong>' + aid + '</strong><span class="entry-progress-account-status status-warn">未开始</span></div><span class="entry-progress-account-meta">今日尚未执行</span><div class="entry-progress-progress-line"><strong>0</strong><span>/ 0 入场动作</span><em>0%</em></div><div class="entry-progress-meter" role="progressbar" aria-label="' + escapeHtml(rawAid + " 入场动作进度") + '" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100"></div></div>'
          + '<div class="entry-progress-phase" role="cell"><span class="entry-progress-phase-label neutral">未开始</span><span class="entry-progress-phase-note">等待今日榜单</span></div>'
          + '<div class="entry-progress-waiting-cell" role="cell"><div class="entry-progress-waiting-list"><div class="entry-progress-waiting-empty">等待今日榜单</div></div></div>'
          + '<div class="entry-progress-next-cell" role="cell"><strong>--</strong><span>尚未开始</span></div></div>';
        continue;
      }
      var target = nonNegNum(accProgress.entry_action_target_count, accProgress.target_count);
      var opened = nonNegNum(accProgress.entry_action_completed_count, accProgress.opened_count);
      var positionCount = nonNegNum(accProgress.position_count, accProgress.opened_count);
      var waiting = nonNegNum(accProgress.waiting_count);
      var failed = nonNegNum(accProgress.entry_action_failed_count, accProgress.failed_count);
      var skipped = nonNegNum(accProgress.entry_action_skipped_count, accProgress.skipped_count);
      var pendingRaw = ops.toNum(accProgress.entry_action_pending_count);
      var pendingActions = pendingRaw !== null ? Math.max(0, pendingRaw) : Math.max(0, target - opened - failed - skipped);
      var hasActionData = progressItems(accProgress.entry_actions).length > 0;
      var initialActions = nonNegNum(accProgress.entry_action_initial_count);
      var scaleInActions = nonNegNum(accProgress.entry_action_scale_in_count);
      if (!hasActionData && initialActions === 0) initialActions = opened;
      var denom = target > 0 ? target : Math.max(1, opened + waiting + failed);
      var openedPct = Math.round(Math.min(100, (opened / denom) * 100));
      var rowPcts = computeEntryMeterPcts({
        initial: initialActions,
        scaleIn: scaleInActions,
        failed: failed,
        skipped: skipped,
        waiting: pendingActions
      }, denom);
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
      html += '<div class="entry-progress-row" role="row">'
        + '<div class="entry-progress-account progress-account-cell" role="cell"><div class="entry-progress-account-head"><strong>' + aid + '</strong><span class="entry-progress-account-status ' + st.cls + '">' + escapeHtml(accountStatusText) + '</span></div><span class="entry-progress-account-meta">' + escapeHtml(accountMeta) + '</span><div class="entry-progress-progress-line"><strong>' + opened + "</strong><span>/ " + target + " 入场动作</span><em>" + openedPct + "%</em></div>"
        + entryMeterHtml("entry-progress-meter", rawAid + " 入场动作进度", openedPct, rowPcts)
        + '<div class="entry-progress-progress-foot"><span>入场动作</span><span class="' + progressNoteClass + '">' + escapeHtml(progressNoteParts.join(" · ")) + "</span></div></div>"
        + '<div class="entry-progress-phase" role="cell" aria-label="' + escapeHtml(st.text) + '"><span class="entry-progress-phase-label ' + phase.cls + " phase-" + escapeHtml(phase.variant || "unknown") + '">' + escapeHtml(phase.label) + '</span><span class="entry-progress-phase-note">' + escapeHtml(phase.note) + "</span></div>"
        + '<div class="entry-progress-waiting-cell" role="cell">' + waitingHtml + "</div>"
        + '<div class="entry-progress-next-cell' + (waiting ? " is-waiting" : "") + '" role="cell"><span class="entry-progress-next-cell-label">下一检查</span><strong>' + escapeHtml(nextText) + "</strong><span>" + escapeHtml(nextNote) + "</span></div></div>";
    }
    html += "</div>";

    return {
      html: html,
      latest: latest,
      waitingTotal: waitingTotal,
      nextCheckRaw: waitingTotal > 0 ? nextCheckRaw : "",
      showDetails: showDetails
    };
  }

  function buildTaskBoardHtml(rows, taskFilter) {
    var list = rows || [];
    var html = "";
    for (var i = 0; i < list.length; i += 1) {
      var r = list[i] || {};
      var aid = String(r.account_id || "");
      var safeAid = escapeHtml(aid);
      var mode = String(r.mode || "full").toLowerCase();
      var modeLabel = mode === "full" ? "完整策略" : (mode === "loss_cut_only" ? "止损保护" : "只读监控");
      var modeBadgeClass = "task-mode-badge" + (mode === "readonly" ? " mode-readonly" : (mode === "loss_cut_only" ? " mode-loss-cut" : ""));
      var headerStatus = mode === "readonly" ? "READONLY" : worstAccountTaskStatus(r);
      html += '<article class="task-account-card" aria-labelledby="task-card-title-' + safeAid + '">'
        + '<div class="task-account-head">'
        + '<div class="task-account-title">'
        + '<h3 class="card-title-heading task-account-id" id="task-card-title-' + safeAid + '">' + safeAid + "</h3>"
        + '<span class="' + modeBadgeClass + '">' + escapeHtml(modeLabel) + "</span>"
        + (r && r.equity_recovery_take_profit_enabled ? '<span class="task-feature-badge">组合止盈监控</span>' : "")
        + "</div>"
        + '<span class="task-last-run ' + taskStatusCls(headerStatus) + '">' + escapeHtml(taskStatusText(headerStatus)) + "</span>"
        + "</div>"
        + '<div class="task-table" role="table" aria-label="' + safeAid + ' 任务执行记录">'
        + '<div class="task-head" role="row">'
        + '<span class="task-col-h" role="columnheader">任务</span>'
        + '<span class="task-col-h" role="columnheader">状态</span>'
        + '<span class="task-col-h" role="columnheader">时间</span>'
        + '<span class="task-col-h" role="columnheader">结果</span>'
        + "</div>"
        + taskRowsForAccount(r)
        + "</div>"
        + "</article>";
    }
    return html
      || (taskFilter === "anomaly"
        ? '<div class="task-empty"><strong>暂无异常</strong><span>所有系统任务正常运行</span></div>'
        : '<div class="task-empty"><strong>暂无匹配任务</strong><span>切换筛选条件查看其他任务</span></div>');
  }

  global.BBOverviewRender = {
    fmt: fmt,
    taskStatusText: taskStatusText,
    taskStatusCls: taskStatusCls,
    worstAccountTaskStatus: worstAccountTaskStatus,
    anomalyTaskCount: anomalyTaskCount,
    taskTimeValue: taskTimeValue,
    latestTaskTimeForAccount: latestTaskTimeForAccount,
    latestTaskTime: latestTaskTime,
    hasAnomalyTask: hasAnomalyTask,
    hasSymbolDetails: hasSymbolDetails,
    visibleTaskAccounts: visibleTaskAccounts,
    taskMetaLabel: taskMetaLabel,
    symbolList: symbolList,
    formatTaskResultLines: formatTaskResultLines,
    taskResultHtml: taskResultHtml,
    taskRowHtml: taskRowHtml,
    taskRowsForAccount: taskRowsForAccount,
    entryProgressStatus: entryProgressStatus,
    compactLocalTime: compactLocalTime,
    formatEntryCountdown: formatEntryCountdown,
    progressItems: progressItems,
    entryPhaseMeta: entryPhaseMeta,
    entryProgressPhase: entryProgressPhase,
    entryProgressWaitingHtml: entryProgressWaitingHtml,
    entryActionMeta: entryActionMeta,
    entryProgressKey: entryProgressKey,
    buildCommonEntryTimeline: buildCommonEntryTimeline,
    entryProgressWindow: entryProgressWindow,
    entryProgressDetail: entryProgressDetail,
    entryTimelineActionHtml: entryTimelineActionHtml,
    chartGeometry: chartGeometry,
    managedStrategyNotes: managedStrategyNotes,
    computeVenueState: computeVenueState,
    buildEntryProgressHtml: buildEntryProgressHtml,
    buildTaskBoardHtml: buildTaskBoardHtml
  };
})(typeof window !== "undefined" ? window : globalThis);
