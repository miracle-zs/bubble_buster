/* Bubble Buster — Shared Obsidian Desk utilities, domain calculations & navigation */
(function (global) {
  "use strict";

  var SHANGHAI_TZ = "Asia/Shanghai";
  var SHANGHAI_OFFSET_MS = 8 * 60 * 60 * 1000;
  var ARROW_ICON = '<svg class="ops-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 11L11 5"/><path d="M6.5 5H11v4.5"/></svg>';
  var chartThemeCache = null;
  var dateFormatterCache = {};

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  var collator = typeof Intl !== "undefined" && Intl.Collator
    ? new Intl.Collator(undefined, { numeric: true, sensitivity: "base" })
    : null;

  function compareText(a, b) {
    return collator ? collator.compare(a, b) : String(a).localeCompare(String(b));
  }

  function compareNullableNumbers(aVal, bVal, desc) {
    if (aVal === null && bVal === null) return 0;
    if (aVal === null) return 1;
    if (bVal === null) return -1;
    return desc ? (bVal - aVal) : (aVal - bVal);
  }

  function toNum(value) {
    if (value === null || value === undefined || value === "") return null;
    var n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function fmtNum(value, digits) {
    var n = toNum(value);
    return n === null ? "--" : n.toFixed(digits);
  }

  function fmtSigned(value, digits) {
    var n = toNum(value);
    if (n === null) return "--";
    return (n > 0 ? "+" : "") + n.toFixed(digits);
  }

  function fmtUsdt(value, digits) {
    var n = toNum(value);
    if (n === null) return "--";
    var d = digits == null ? 2 : digits;
    try {
      return n.toLocaleString("zh-CN", {
        minimumFractionDigits: d,
        maximumFractionDigits: d
      });
    } catch (e) {
      return n.toFixed(d);
    }
  }

  function fmtSignedUsdt(value, digits) {
    var n = toNum(value);
    if (n === null) return "--";
    var formatted = fmtUsdt(Math.abs(n), digits);
    if (n > 0) return "+" + formatted;
    if (n < 0) return "-" + formatted;
    return formatted;
  }

  function fmtPct(value) {
    var n = toNum(value);
    if (n === null) return "--";
    return (n >= 0 ? "+" : "") + n.toFixed(2) + "%";
  }

  function fmtWithUnit(value, formatter, digits, suffix) {
    var formatted = formatter(value, digits);
    return formatted === "--" ? "--" : formatted + suffix;
  }

  function prefersReducedMotion() {
    return !!(global.matchMedia && global.matchMedia("(prefers-reduced-motion: reduce)").matches);
  }

  function getChartTheme(forceRefresh) {
    if (chartThemeCache && !forceRefresh) return chartThemeCache;
    var styles = null;
    try {
      if (global.getComputedStyle && typeof document !== "undefined" && document.documentElement) {
        styles = global.getComputedStyle(document.documentElement);
      }
    } catch (e) {
      styles = null;
    }
    function readVar(name, fallback) {
      if (!styles) return fallback;
      var val = styles.getPropertyValue(name).trim();
      return val || fallback;
    }
    chartThemeCache = {
      bg: readVar("--bg", "#080a0f"),
      surface: readVar("--surface", "#10141c"),
      surfaceElevated: readVar("--surface-elevated", "#151b26"),
      surfaceSubtle: readVar("--surface-subtle", "#131822"),
      surfaceInset: readVar("--surface-inset", "#0b0e14"),
      surfaceTooltip: readVar("--surface-tooltip", "rgba(11, 14, 20, 0.94)"),
      border: readVar("--border", "#222a38"),
      borderStrong: readVar("--border-strong", "#344054"),
      shadowOverlay: readVar("--shadow-overlay", "0 14px 36px rgba(2, 4, 10, 0.72), 0 2px 8px rgba(2, 4, 10, 0.5)"),
      fontMono: readVar("--font-mono", 'ui-monospace, SFMono-Regular, "SF Mono", "JetBrains Mono", Menlo, Monaco, Consolas, monospace'),
      text: readVar("--text", "#f3f6fb"),
      textSecondary: readVar("--text-secondary", "#c2ccd9"),
      muted: readVar("--muted", "#8b99ad"),
      accent: readVar("--accent", "#38bdf8"),
      ok: readVar("--ok", "#10b981"),
      okText: readVar("--ok-text", "#34d399"),
      warn: readVar("--warn", "#f59e0b"),
      warnText: readVar("--warn-text", "#fbbf24"),
      bad: readVar("--bad", "#f43f5e"),
      badText: readVar("--bad-text", "#fb7185"),
      readonly: readVar("--readonly", "#a78bfa"),
      splitLine: readVar("--chart-split-line", "rgba(34, 42, 56, 0.85)"),
      splitLineSubtle: readVar("--chart-split-line-subtle", "rgba(34, 42, 56, 0.65)"),
      zoomFill: readVar("--chart-zoom-fill", "rgba(56, 189, 248, 0.16)"),
      zoomArea: readVar("--chart-zoom-area", "rgba(56, 189, 248, 0.08)"),
      areaOk: readVar("--chart-area-ok", "rgba(16, 185, 129, 0.17)"),
      areaBad: readVar("--chart-area-bad", "rgba(244, 63, 94, 0.17)"),
      areaFade: readVar("--chart-area-fade", "rgba(8, 10, 15, 0)"),
      series: [
        readVar("--series-1", "#38bdf8"),
        readVar("--series-2", "#f59e0b"),
        readVar("--series-3", "#10b981"),
        readVar("--series-4", "#fb7185"),
        readVar("--series-5", "#2dd4bf"),
        readVar("--series-6", "#a3e635")
      ]
    };
    return chartThemeCache;
  }

  function formatStrategyDisplay(part) {
    var text = String(part || "").trim();
    if (!text) return "";
    return text
      .replace(/(组合止盈|组合止损|盈利|回撤|减仓|止盈|止损)(\d)/g, "$1 $2")
      .replace(/(%)(回撤|减仓|清仓)/g, "$1 · $2")
      .replace(/(ON|OFF)$/i, function (m) { return " " + m.toUpperCase(); })
      .replace(/\s{2,}/g, " ")
      .trim();
  }

  function splitStrategyParts(note) {
    var raw = String(note || "").trim();
    if (!raw) return [];
    var parts = [];
    var buf = "";
    var depth = 0;
    for (var i = 0; i < raw.length; i += 1) {
      var ch = raw.charAt(i);
      if (ch === "(" || ch === "（") {
        depth += 1;
        buf += ch;
      } else if (ch === ")" || ch === "）") {
        depth = Math.max(0, depth - 1);
        buf += ch;
      } else if (ch === "/" && depth === 0) {
        var trimmed = buf.trim();
        if (trimmed) parts.push(trimmed);
        buf = "";
      } else {
        buf += ch;
      }
    }
    var tail = buf.trim();
    if (tail) parts.push(tail);
    return parts;
  }

  function classifyStrategyPart(part, clsBase) {
    var upper = part.toUpperCase();
    var isTag = clsBase === "strategy-tag";
    if (upper.indexOf("OFF") >= 0) {
      return isTag ? "strategy-tag strategy-tag-off" : clsBase + " off";
    }
    if (
      upper.indexOf("TP") >= 0
      || part.indexOf("减仓") >= 0
      || part.indexOf("清仓") >= 0
      || part.indexOf("组合止盈") >= 0
    ) {
      return isTag ? "strategy-tag strategy-tag-primary" : clsBase + " primary";
    }
    return isTag ? "strategy-tag strategy-tag-protection" : clsBase;
  }

  function formatStrategyTags(note, baseClass) {
    var clsBase = baseClass || "strategy-chip";
    var emptyClass = clsBase === "strategy-tag" ? "strategy-tag strategy-tag-empty" : clsBase;
    var parts = splitStrategyParts(note);
    if (!parts.length) return '<span class="' + emptyClass + '">未配置策略说明</span>';
    var html = "";
    for (var i = 0; i < parts.length; i += 1) {
      var part = parts[i];
      var display = formatStrategyDisplay(part);
      html += '<span class="' + classifyStrategyPart(part, clsBase) + '">' + escapeHtml(display) + "</span>";
    }
    return html;
  }

  function formatStrategyHighlights(note, allNotes, maxCount, baseClass) {
    var limit = Math.max(1, Number(maxCount) || 2);
    var clsBase = baseClass || "strategy-tag";
    var parts = splitStrategyParts(note);
    if (!parts.length) return "";
    var pool = Array.isArray(allNotes) ? allNotes.filter(function (n) { return String(n || "").trim(); }) : [];
    var freq = {};
    if (pool.length >= 2) {
      for (var i = 0; i < pool.length; i += 1) {
        var seenInNote = {};
        var np = splitStrategyParts(pool[i]);
        for (var j = 0; j < np.length; j += 1) {
          if (seenInNote[np[j]]) continue;
          seenInNote[np[j]] = true;
          freq[np[j]] = (freq[np[j]] || 0) + 1;
        }
      }
    }
    var candidates = pool.length >= 2
      ? parts.filter(function (p) { return (freq[p] || 0) < pool.length; })
      : parts.slice();
    if (!candidates.length) candidates = parts.slice();
    var selected = [];
    for (var k = 0; k < candidates.length; k += 1) {
      var c = candidates[k];
      var cls = classifyStrategyPart(c, clsBase);
      if (cls.indexOf("primary") >= 0 || cls.indexOf("off") >= 0) {
        selected.push(c);
        break;
      }
    }
    var remaining = candidates.filter(function (c) { return selected.indexOf(c) < 0; });
    remaining.sort(function (a, b) {
      return (freq[a] || 0) - (freq[b] || 0);
    });
    while (selected.length < limit && remaining.length) {
      selected.push(remaining.shift());
    }
    var html = "";
    for (var m = 0; m < selected.length; m += 1) {
      var item = selected[m];
      var display = formatStrategyDisplay(item);
      html += '<span class="' + classifyStrategyPart(item, clsBase) + '" title="' + escapeHtml(display) + '">' + escapeHtml(display) + "</span>";
    }
    return html;
  }

  var STATUS_LABELS = {
    SUCCESS: "成功",
    RUNNING: "运行中",
    FAILED: "失败",
    PARTIAL: "部分完成",
    SKIPPED: "已跳过",
    READONLY: "只读",
    UNKNOWN: "未知",
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

  function formatStatusLabel(status, fallback) {
    var s = String(status || "").toUpperCase();
    if (!s) return fallback !== undefined ? fallback : "未知";
    return STATUS_LABELS[s] || s;
  }

  function statusTone(status) {
    var s = String(status || "").toUpperCase();
    if (s === "READONLY") return "readonly";
    if (s.indexOf("SUCCESS") >= 0 || s.indexOf("CLOSED_TP") >= 0 || s.indexOf("RUNNING") >= 0) return "ok";
    if (s.indexOf("PARTIAL") >= 0 || s.indexOf("SKIPPED") >= 0 || s === "UNKNOWN") return "warn";
    if (s.indexOf("FAILED") >= 0 || s.indexOf("ERROR") >= 0 || s.indexOf("CLOSED_SL") >= 0) return "bad";
    return "";
  }

  function pickLatestTimestamp(values) {
    var best = "";
    var bestMs = -Infinity;
    var list = values || [];
    for (var i = 0; i < list.length; i += 1) {
      var val = list[i];
      if (!val) continue;
      var text = String(val);
      var ms = parseTimestampMs(text);
      if (Number.isFinite(ms)) {
        if (ms > bestMs) { bestMs = ms; best = text; }
      } else if (!best || text > best) {
        best = text;
      }
    }
    return best;
  }

  function parseTimestampMs(value) {
    if (typeof value === "number") return Number.isFinite(value) ? value : NaN;
    var raw = String(value || "").trim();
    if (!raw || raw === "--") return NaN;
    var normalized = raw;
    if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(normalized) && !/[zZ]|[+-]\d{2}:?\d{2}$/.test(normalized)) {
      normalized = normalized.replace(" ", "T") + "+08:00";
    }
    var ms = Date.parse(normalized);
    return Number.isFinite(ms) ? ms : NaN;
  }

  function getCachedDateFormatter(tz, withYear, withSeconds, dateOnly) {
    var key = tz + "|" + (withYear ? "1" : "0") + "|" + (withSeconds ? "1" : "0") + "|" + (dateOnly ? "1" : "0");
    if (Object.prototype.hasOwnProperty.call(dateFormatterCache, key)) {
      return dateFormatterCache[key];
    }
    try {
      var intlOpts = dateOnly
        ? { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }
        : { timeZone: tz, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false, hourCycle: "h23" };
      if (!dateOnly && withYear) intlOpts.year = "numeric";
      if (!dateOnly && withSeconds) intlOpts.second = "2-digit";
      dateFormatterCache[key] = new Intl.DateTimeFormat("zh-CN", intlOpts);
    } catch (e) {
      dateFormatterCache[key] = null;
    }
    return dateFormatterCache[key];
  }

  function formatBeijingTime(value, options) {
    if (value === null || value === undefined || value === "") return "--";
    var opts = options || {};
    var ms = parseTimestampMs(value);
    if (!Number.isFinite(ms)) {
      var raw = String(value);
      return raw.replace("T", " ").slice(0, opts.withSeconds ? 19 : 16);
    }
    var tz = opts.timeZone || SHANGHAI_TZ;
    var formatter = getCachedDateFormatter(tz, !!opts.withYear, !!opts.withSeconds, !!opts.dateOnly);
    if (formatter) {
      try {
        return formatter.format(new Date(ms)).replace(/\//g, "-");
      } catch (e) {}
    }
    var local = new Date(ms + SHANGHAI_OFFSET_MS).toISOString().replace("T", " ");
    if (opts.dateOnly) return local.slice(0, 10);
    var start = opts.withYear ? 0 : 5;
    var end = opts.withSeconds ? 19 : 16;
    return local.slice(start, end);
  }

  function evaluateDataFreshness(latestValue, options) {
    var opts = options || {};
    var warnSec = Number(opts.warnSec) || 180;
    var staleSec = Number(opts.staleSec) || 900;
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      return { state: "offline", ageSec: null, badgeText: "离线", detailText: "浏览器当前离线" };
    }
    var ms = parseTimestampMs(latestValue);
    if (!Number.isFinite(ms)) {
      return { state: "unknown", ageSec: null, badgeText: "待同步", detailText: "尚无快照时间" };
    }
    var ageSec = Math.max(0, Math.floor((Date.now() - ms) / 1000));
    var ageText;
    if (ageSec < 60) ageText = ageSec + "秒前";
    else if (ageSec < 3600) ageText = Math.floor(ageSec / 60) + "分钟前";
    else if (ageSec < 86400) ageText = Math.floor(ageSec / 3600) + "小时前";
    else ageText = Math.floor(ageSec / 86400) + "天前";

    var stamp = formatBeijingTime(ms, { withYear: ageSec >= 86400, withSeconds: ageSec < 3600 });
    if (ageSec >= staleSec) {
      return {
        state: "stale",
        ageSec: ageSec,
        badgeText: "历史快照 · 滞后 " + ageText,
        detailText: "最新数据截至 " + stamp + "（滞后 " + ageText + "）"
      };
    }
    if (ageSec >= warnSec) {
      return {
        state: "warn",
        ageSec: ageSec,
        badgeText: "延迟 · " + ageText,
        detailText: "最新数据截至 " + stamp + "（" + ageText + "）"
      };
    }
    return {
      state: "live",
      ageSec: ageSec,
      badgeText: "实时 · " + ageText,
      detailText: "最新数据截至 " + stamp
    };
  }

  function freshnessClassForState(state) {
    if (state === "live") return "is-live";
    if (state === "warn") return "is-warn";
    if (state === "stale" || state === "offline") return "is-stale";
    return "";
  }

  function ensureLiveAnnouncer() {
    if (typeof document === "undefined" || !document.body) return null;
    var node = document.getElementById("ops-live-announcer");
    if (!node) {
      node = document.createElement("div");
      node.id = "ops-live-announcer";
      node.className = "sr-only";
      node.setAttribute("role", "status");
      node.setAttribute("aria-live", "polite");
      node.setAttribute("aria-atomic", "true");
      document.body.appendChild(node);
    }
    return node;
  }

  var announceTimer = 0;
  function announceStatus(message) {
    var node = ensureLiveAnnouncer();
    if (!node || !message) return;
    var text = String(message);
    node.textContent = "";
    if (announceTimer) {
      if (typeof cancelAnimationFrame === "function") cancelAnimationFrame(announceTimer);
      else clearTimeout(announceTimer);
      announceTimer = 0;
    }
    var schedule = typeof requestAnimationFrame === "function"
      ? requestAnimationFrame
      : function (fn) { return setTimeout(fn, 16); };
    announceTimer = schedule(function () {
      announceTimer = 0;
      node.textContent = text;
    });
  }

  function formatWalletSource(source) {
    var raw = String(source || "").trim().toUpperCase();
    var map = {
      DB: "本地快照",
      API: "交易所实时",
      CACHE: "内存缓存",
      STALE: "缓存快照",
      COOLDOWN: "冷却回退",
      ERROR: "读取异常"
    };
    return map[raw] || (source ? String(source) : "--");
  }

  function applyFreshnessPill(pillEl, textEl, freshness) {
    if (!freshness) return;
    if (textEl) textEl.textContent = freshness.badgeText;
    if (pillEl) {
      var prevState = pillEl.getAttribute("data-freshness-state") || "";
      var cls = freshnessClassForState(freshness.state);
      pillEl.className = "ops-freshness" + (cls ? " " + cls : "");
      pillEl.title = freshness.detailText || "";
      pillEl.setAttribute("data-freshness-state", freshness.state || "");
      // Only announce meaningful state transitions (not ticking seconds)
      if (prevState && prevState !== freshness.state && (freshness.state === "stale" || freshness.state === "offline" || freshness.state === "live")) {
        announceStatus("数据同步状态：" + freshness.badgeText);
      }
    }
  }

  function cycleStartMs(referenceMs, stopHour, stopMinute) {
    var h = Number.isFinite(Number(stopHour)) ? Number(stopHour) : 8;
    var m = Number.isFinite(Number(stopMinute)) ? Number(stopMinute) : 0;
    var local = new Date(referenceMs + SHANGHAI_OFFSET_MS);
    var start = Date.UTC(
      local.getUTCFullYear(),
      local.getUTCMonth(),
      local.getUTCDate(),
      h,
      m
    ) - SHANGHAI_OFFSET_MS;
    if (referenceMs < start) start -= 24 * 60 * 60 * 1000;
    return start;
  }

  function computeCycleRiskMetrics(rawPoints, mode, stop, config, cycleBaseline) {
    var cfg = config || {};
    var stopHour = Number.isFinite(Number(cfg.stopHour)) ? Number(cfg.stopHour) : 8;
    var stopMinute = Number.isFinite(Number(cfg.stopMinute)) ? Number(cfg.stopMinute) : 0;
    var fallbackStopPct = Number.isFinite(Number(cfg.stopPct)) ? Number(cfg.stopPct) : 3.5;

    var points = (rawPoints || []).filter(function (point) {
      return point && Number.isFinite(Number(point.equity));
    });
    var riskBaseline = mode !== "readonly" && stop ? Number(stop.baseline_equity) : NaN;
    if (mode !== "readonly" && !(riskBaseline > 0)) return null;
    if (!points.length) return null;

    var exactCycleBaseline = null;
    if (mode === "readonly" && cycleBaseline) {
      var cbEq = Number(cycleBaseline.equity);
      var cbMs = parseTimestampMs(cycleBaseline.t);
      if (Number.isFinite(cbEq) && Number.isFinite(cbMs)) {
        exactCycleBaseline = { t: cycleBaseline.t, equity: cbEq, ms: cbMs };
      }
    }

    var startIndex = 0;
    var latestMs = parseTimestampMs(points[points.length - 1].t);
    var capturedMs = stop ? parseTimestampMs(stop.baseline_captured_at_utc) : NaN;
    var targetMs = NaN;
    if (Number.isFinite(latestMs)) {
      targetMs = exactCycleBaseline
        ? exactCycleBaseline.ms
        : (Number.isFinite(capturedMs) ? capturedMs : cycleStartMs(latestMs, stopHour, stopMinute));
      var afterIndex = -1;
      for (var i = 0; i < points.length; i += 1) {
        var pointMs = parseTimestampMs(points[i].t);
        if (!Number.isFinite(pointMs)) continue;
        if (afterIndex < 0 && pointMs >= targetMs) afterIndex = i;
      }
      if (afterIndex < 0) return null;
      startIndex = afterIndex;
    }

    var scoped = points.slice(startIndex).map(function (pt) {
      return {
        t: pt.t || "",
        ms: parseTimestampMs(pt.t),
        equity: Number(pt.equity),
        carriedForward: !!(pt.carried_forward || pt.carriedForward)
      };
    });
    if (!scoped.length) return null;

    var realPointCount = scoped.filter(function (pt) { return !pt.carriedForward; }).length;

    if (exactCycleBaseline) {
      scoped = scoped.filter(function (pt) { return pt.ms > exactCycleBaseline.ms; });
      scoped.unshift({ t: exactCycleBaseline.t, ms: exactCycleBaseline.ms, equity: exactCycleBaseline.equity });
    }

    var baseline = mode !== "readonly"
      ? riskBaseline
      : (exactCycleBaseline ? exactCycleBaseline.equity : scoped[0].equity);

    if (mode !== "readonly") {
      var baseMs = Number.isFinite(capturedMs)
        ? capturedMs
        : (Number.isFinite(targetMs) ? targetMs : (Number.isFinite(scoped[0].ms) ? scoped[0].ms - 60000 : NaN));
      scoped.unshift({
        t: (stop && stop.baseline_captured_at_utc) || "",
        ms: baseMs,
        equity: baseline,
        synthetic: true
      });
      if (stop && stop.current_equity !== null && stop.current_equity !== undefined && Number.isFinite(Number(stop.current_equity))) {
        var lastReal = scoped[scoped.length - 1];
        var currEq = Number(stop.current_equity);
        if (Math.abs(currEq - lastReal.equity) > 1e-9 || scoped.length < 2) {
          var stopUpdMs = stop.updated_at_utc ? parseTimestampMs(stop.updated_at_utc) : NaN;
          var useStopTs = Number.isFinite(stopUpdMs) && (!Number.isFinite(lastReal.ms) || stopUpdMs >= lastReal.ms);
          var currMs = useStopTs
            ? stopUpdMs
            : (Number.isFinite(lastReal.ms) ? lastReal.ms + 1000 : NaN);
          var currT = useStopTs ? String(stop.updated_at_utc) : lastReal.t;
          scoped.push({ t: currT, ms: currMs, equity: currEq, synthetic: true });
        }
      }
    }

    if (scoped.length < 2 || !Number.isFinite(baseline) || baseline === 0) return null;

    var values = scoped.map(function (pt) {
      return ((pt.equity - baseline) / baseline) * 100;
    });
    var current = values[values.length - 1];
    var peak = values[0];
    for (var j = 1; j < values.length; j += 1) {
      if (values[j] > peak) peak = values[j];
    }
    var configuredLossPct = stop && Number.isFinite(Number(stop.loss_pct)) && Number(stop.loss_pct) > 0
      ? Number(stop.loss_pct)
      : fallbackStopPct;
    var stopStatus = stop ? String(stop.status || "").toUpperCase() : "";
    var isStopped = stopStatus === "TRIGGERED_COMPLETE" || stopStatus === "CLOSING" || stopStatus === "THRESHOLD_REACHED";
    var consumedStopPct = isStopped
      ? 100
      : (current < 0 ? Math.min(100, (Math.abs(current) / configuredLossPct) * 100) : 0);

    return {
      values: values,
      points: scoped,
      realPointCount: realPointCount,
      baselineEquity: baseline,
      stopEquity: baseline * (1 - configuredLossPct / 100),
      configuredLossPct: configuredLossPct,
      currentReturnPct: current,
      peakReturnPct: peak,
      currentDrawdownPct: ((1 + current / 100) / (1 + peak / 100) - 1) * 100,
      stopDistancePct: current + configuredLossPct,
      consumedStopPct: consumedStopPct
    };
  }

  function evaluatePortfolioStopState(stop, metrics, options) {
    var opts = options || {};
    var enabled = !!opts.enabled;
    var fallbackPct = Number.isFinite(Number(opts.fallbackPct)) && Number(opts.fallbackPct) > 0 ? Number(opts.fallbackPct) : 3.5;
    var configuredLossPct = stop && toNum(stop.loss_pct) > 0 ? toNum(stop.loss_pct) : fallbackPct;

    if (!enabled) {
      return {
        tone: "warn",
        stateText: "组合止损未启用",
        distanceText: "未启用",
        meterText: "未启用",
        consumedPct: 0,
        configuredLossPct: configuredLossPct,
        description: "账户周期收益与回撤仍会持续计算，但当前账户不会按组合阈值触发整体平仓。"
      };
    }

    if (!stop || !stop.status) {
      return {
        tone: "warn",
        stateText: "等待后端止损状态",
        distanceText: "待同步",
        meterText: "待同步",
        consumedPct: 0,
        configuredLossPct: configuredLossPct,
        description: "后端尚未写入本账户周期组合止损状态；页面不会仅凭权益曲线判定已经触发。"
      };
    }

    var status = String(stop.status || "").toUpperCase();
    var distance = toNum(stop.distance_pct);
    var backendReturn = toNum(stop.current_return_pct);
    var currentReturn = backendReturn !== null ? backendReturn : (metrics ? toNum(metrics.currentReturnPct) : null);
    var consumedPct = currentReturn !== null && currentReturn < 0
      ? Math.min(100, (Math.abs(currentReturn) / configuredLossPct) * 100)
      : 0;

    if (status === "TRIGGERED_COMPLETE") {
      return {
        tone: "bad",
        stateText: "组合止损已完成",
        distanceText: "已完成",
        meterText: "已止损 100%",
        consumedPct: 100,
        configuredLossPct: configuredLossPct,
        description: "后端已完成本账户周期组合止损，当前受管持仓为 0。"
      };
    }
    if (status === "CLOSING") {
      var openPos = stop.open_positions == null ? "--" : String(stop.open_positions);
      return {
        tone: "bad",
        stateText: "组合止损执行中",
        distanceText: "执行中",
        meterText: stop.state_inconsistent ? "等待对账 100%" : "平仓中 100%",
        consumedPct: 100,
        configuredLossPct: configuredLossPct,
        description: stop.state_inconsistent
          ? "后端已记录平仓完成，但本地仍有 " + openPos + " 个受管持仓，正在等待对账。"
          : "后端已触发组合止损，正在处理剩余 " + openPos + " 个受管持仓。"
      };
    }
    if (status === "THRESHOLD_REACHED") {
      return {
        tone: "bad",
        stateText: "已到止损阈值",
        distanceText: distance !== null ? "已超出 " + fmtNum(Math.abs(distance), 2) + "%" : "已到阈值",
        meterText: "已达阈值 100%",
        consumedPct: 100,
        configuredLossPct: configuredLossPct,
        description: "最新权益已达到止损阈值，等待策略管理器确认并执行。"
      };
    }
    if (status === "MONITORING" && distance !== null) {
      var nearStop = distance <= 1;
      return {
        tone: nearStop ? "warn" : "ok",
        stateText: nearStop ? "接近组合止损" : "风险正常",
        distanceText: fmtNum(distance, 2) + "%",
        meterText: consumedPct <= 0 ? "未消耗" : "已耗 " + fmtNum(consumedPct, 0) + "%",
        consumedPct: consumedPct,
        configuredLossPct: configuredLossPct,
        description: currentReturn === null
          ? "后端正在监控本账户周期组合止损。"
          : "当前周期收益 " + fmtSigned(currentReturn, 2) + "%，距离 -" + fmtNum(configuredLossPct, 2) + "% 止损阈值仍有 " + fmtNum(distance, 2) + " 个百分点。"
      };
    }
    return {
      tone: "warn",
      stateText: "等待后端止损状态",
      distanceText: "待同步",
      meterText: "待同步",
      consumedPct: 0,
      configuredLossPct: configuredLossPct,
      description: "后端组合止损基准尚未就绪；页面不会仅凭权益曲线判定已经触发。"
    };
  }

  function classifyReturnTone(returnPct, stopPct, stopStatus) {
    var n = toNum(returnPct);
    var st = String(stopStatus || "").toUpperCase();
    if (st === "TRIGGERED_COMPLETE" || st === "CLOSING" || st === "THRESHOLD_REACHED") return "bad";
    if (n === null) return "";
    if (n > 0) return "ok";
    if (n < 0) {
      var limit = Number(stopPct);
      if (Number.isFinite(limit) && limit > 0) {
        if (n <= -(limit * 0.7)) return "bad";
        if (n <= -(limit * 0.35)) return "warn";
        return "";
      }
      return "bad";
    }
    return "";
  }

  function classifyDrawdownTone(drawdownPct, stopPct) {
    var n = toNum(drawdownPct);
    var limit = Number.isFinite(Number(stopPct)) && Number(stopPct) > 0 ? Number(stopPct) : 3.5;
    if (n === null) return "";
    if (n <= -(limit * 0.7)) return "bad";
    if (n <= -(limit * 0.35)) return "warn";
    return "ok";
  }

  function parseSummaryPairs(summary) {
    var raw = String(summary || "--").trim();
    var pairs = {};
    if (!raw || raw === "--") return pairs;
    var matcher = /(?:^|[\s,])([A-Za-z0-9_.-]+)=/g;
    var tokens = [];
    var match;
    while ((match = matcher.exec(raw)) !== null) {
      tokens.push({
        key: match[1],
        valueStart: match.index + match[0].length,
        matchStart: match.index
      });
    }
    for (var i = 0; i < tokens.length; i += 1) {
      var endIndex = i + 1 < tokens.length ? tokens[i + 1].matchStart : raw.length;
      var val = raw.slice(tokens[i].valueStart, endIndex).trim();
      if (val) pairs[tokens[i].key] = val;
    }
    return pairs;
  }

  function fetchJsonWithTimeout(url, options) {
    var opts = options || {};
    var timeoutMs = Number(opts.timeoutMs) || 15000;
    var externalSignal = opts.signal || null;
    var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    var timedOut = false;
    var timer = null;

    if (controller && externalSignal) {
      if (externalSignal.aborted) {
        try { controller.abort(); } catch (e) {}
      } else {
        externalSignal.addEventListener("abort", function () {
          try { controller.abort(); } catch (e) {}
        }, { once: true });
      }
    }

    if (controller && timeoutMs > 0) {
      timer = setTimeout(function () {
        timedOut = true;
        try { controller.abort(); } catch (e) {}
      }, timeoutMs);
    }

    return fetch(url, {
      method: "GET",
      cache: "no-store",
      signal: controller ? controller.signal : externalSignal || undefined,
      headers: { Accept: "application/json" }
    })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.json();
      })
      .then(function (data) {
        if (timer) clearTimeout(timer);
        if (data && typeof data === "object" && data.db_error) {
          throw new Error("数据库读取失败：" + data.db_error);
        }
        return data;
      }, function (err) {
        if (timer) clearTimeout(timer);
        if (timedOut) {
          throw new Error("请求超时（" + Math.round(timeoutMs / 1000) + "s）");
        }
        throw err;
      });
  }

  function setButtonBusy(btn, busy, idleText, busyText) {
    if (!btn) return;
    btn.disabled = !!busy;
    var text = busy ? (busyText || "刷新中...") : (idleText || "刷新");
    var label = btn.querySelector(".btn-label");
    if (label) label.textContent = text;
    else btn.textContent = text;
  }

  function createAlertController(alertEl, textEl, slotOrder, separator) {
    var errors = {};
    var sep = separator == null ? "；" : String(separator);
    return function setError(slot, message) {
      errors[slot] = message || "";
      if (!alertEl || !textEl) return;
      var keys = Array.isArray(slotOrder) && slotOrder.length ? slotOrder : Object.keys(errors);
      var active = keys.map(function (k) { return errors[k]; }).filter(Boolean);
      if (active.length) {
        textEl.textContent = active.join(sep);
        alertEl.hidden = false;
      } else {
        textEl.textContent = "";
        alertEl.hidden = true;
      }
    };
  }

  function syncPressedButtons(nodes, isActiveFn) {
    if (!nodes) return;
    for (var i = 0; i < nodes.length; i += 1) {
      var btn = nodes[i];
      if (!btn) continue;
      var active = !!isActiveFn(btn, i);
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    }
  }

  function createEchartsZoomHandler(onChange) {
    return function (evt) {
      var batch = evt && evt.batch && evt.batch[0] ? evt.batch[0] : evt;
      var start = batch && Number.isFinite(Number(batch.start)) ? Number(batch.start) : 0;
      var end = batch && Number.isFinite(Number(batch.end)) ? Number(batch.end) : 100;
      var zoomed = start > 0.5 || end < 99.5;
      onChange(zoomed ? { start: start, end: end } : null, zoomed);
    };
  }

  function buildEchartsChrome(theme, options) {
    var t = theme || getChartTheme();
    var opts = options || {};
    var timeZone = opts.timeZone || SHANGHAI_TZ;
    var formatXAxis = opts.formatXAxis || function (value) {
      return formatBeijingTime(value, { timeZone: timeZone });
    };
    var formatYAxis = opts.formatYAxis || function (value) { return String(value); };

    var tooltip = {
      trigger: "axis",
      axisPointer: { type: "line", lineStyle: { color: t.borderStrong } },
      backgroundColor: t.surfaceTooltip,
      borderColor: t.borderStrong,
      borderWidth: 1,
      extraCssText: "box-shadow: " + t.shadowOverlay + "; font-variant-numeric: tabular-nums;",
      textStyle: { color: t.text, fontSize: 12, fontFamily: t.fontMono },
      formatter: opts.tooltipFormatter
    };

    var xAxis = {
      type: "time",
      boundaryGap: false,
      axisLine: { lineStyle: { color: t.border } },
      axisTick: { show: false },
      axisLabel: {
        color: t.muted,
        fontSize: 12,
        fontFamily: t.fontMono,
        hideOverlap: true,
        alignMinLabel: "left",
        alignMaxLabel: "right",
        margin: opts.xLabelMargin != null ? opts.xLabelMargin : 8,
        formatter: formatXAxis
      }
    };
    if (opts.xSplitLine) {
      xAxis.splitLine = { show: true, lineStyle: { color: t.splitLineSubtle, type: "dashed" } };
    }

    var yAxis = {
      type: "value",
      scale: opts.yScale !== undefined ? !!opts.yScale : true,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        color: t.muted,
        fontSize: 12,
        fontFamily: t.fontMono,
        formatter: formatYAxis
      },
      splitLine: { lineStyle: { color: t.splitLine, type: "dashed" } }
    };

    var dataZoom = opts.showSlider ? [{
      type: "slider",
      xAxisIndex: 0,
      start: Number.isFinite(Number(opts.zoomStart)) ? Number(opts.zoomStart) : 0,
      end: Number.isFinite(Number(opts.zoomEnd)) ? Number(opts.zoomEnd) : 100,
      height: 22,
      bottom: opts.sliderBottom != null ? opts.sliderBottom : 8,
      borderColor: t.border,
      backgroundColor: t.surfaceInset,
      fillerColor: t.zoomFill,
      handleStyle: { color: t.accent, borderColor: t.accent },
      dataBackground: opts.zoomDataBackground ? {
        lineStyle: { color: t.borderStrong, width: 1 },
        areaStyle: { color: t.zoomArea }
      } : undefined,
      textStyle: { color: t.muted, fontSize: 12 }
    }] : [];

    return { tooltip: tooltip, xAxis: xAxis, yAxis: yAxis, dataZoom: dataZoom };
  }

  function captureOpenDetails(root, detailAttr) {
    var openKeys = {};
    if (!root || !detailAttr) return openKeys;
    try {
      var openEls = root.querySelectorAll("details[open][" + detailAttr + "]");
      for (var i = 0; i < openEls.length; i += 1) {
        var key = openEls[i].getAttribute(detailAttr);
        if (key) openKeys[key] = true;
      }
    } catch (e) {}
    return openKeys;
  }

  function restoreOpenDetails(root, detailAttr, openKeys) {
    if (!root || !detailAttr || !openKeys) return;
    var keys = Object.keys(openKeys);
    if (!keys.length) return;
    try {
      var nextEls = root.querySelectorAll("details[" + detailAttr + "]");
      for (var i = 0; i < nextEls.length; i += 1) {
        var key = nextEls[i].getAttribute(detailAttr);
        if (key && openKeys[key]) nextEls[i].setAttribute("open", "");
      }
    } catch (e) {}
  }

  function captureFocusToken(container) {
    if (typeof document === "undefined" || !container || !container.contains) return null;
    var active = document.activeElement;
    if (!active || active === container || !container.contains(active)) return null;
    if (active.id) return { attr: "id", value: active.id };
    var attrs = ["data-account", "data-account-card", "data-detail-key", "data-task-detail-key", "data-row-key"];
    for (var i = 0; i < attrs.length; i += 1) {
      var attr = attrs[i];
      var val = active.getAttribute ? active.getAttribute(attr) : "";
      if (val) return { attr: attr, value: val };
      var parent = active.closest ? active.closest("[" + attr + "]") : null;
      if (parent && container.contains(parent)) {
        var pVal = parent.getAttribute(attr);
        if (pVal) {
          var sub = active.tagName === "SUMMARY" ? "summary" : (active.classList && active.classList[0] ? "." + active.classList[0] : "");
          return { attr: attr, value: pVal, sub: sub };
        }
      }
    }
    return null;
  }

  function restoreFocusToken(container, token) {
    if (!container || !token || typeof document === "undefined") return;
    if (container.contains(document.activeElement)) return;
    var target = null;
    if (token.attr === "id") {
      target = document.getElementById(token.value);
    } else if (container.querySelector) {
      var list = container.querySelectorAll("[" + token.attr + "]");
      for (var i = 0; i < list.length; i += 1) {
        if (list[i].getAttribute(token.attr) === token.value) {
          target = token.sub && list[i].querySelector ? (list[i].querySelector(token.sub) || list[i]) : list[i];
          break;
        }
      }
    }
    if (target && target.focus) {
      try { target.focus({ preventScroll: true }); } catch (e) { target.focus(); }
    }
  }

  function patchHtmlIfChanged(node, html, options) {
    if (!node) return false;
    if (node.__bbLastHtml === html) return false;
    var opts = options || {};
    var detailAttr = opts.detailAttr || "";
    var openKeys = detailAttr ? captureOpenDetails(node, detailAttr) : null;
    var focusToken = opts.preserveFocus !== false ? captureFocusToken(node) : null;
    node.__bbLastHtml = html;
    node.innerHTML = html;
    if (detailAttr && openKeys) restoreOpenDetails(node, detailAttr, openKeys);
    if (focusToken) restoreFocusToken(node, focusToken);
    return true;
  }

  function parseSingleElement(html, parentTag) {
    if (typeof document === "undefined") return null;
    var tpl = document.createElement("template");
    if (tpl && tpl.content) {
      tpl.innerHTML = html;
      return tpl.content.firstElementChild;
    }
    var wrap = document.createElement(parentTag || "div");
    wrap.innerHTML = html;
    return wrap.firstElementChild;
  }

  function moveChildNode(container, node, refNode) {
    if (typeof container.moveBefore === "function") {
      try {
        container.moveBefore(node, refNode || null);
        return;
      } catch (e) {}
    }
    container.insertBefore(node, refNode || null);
  }

  function reorderChildrenByKey(container, keyAttr, orderedKeys) {
    if (!container || !keyAttr || !orderedKeys) return false;
    var existing = {};
    var children = container.children;
    var keyedCount = 0;
    for (var i = 0; i < children.length; i += 1) {
      var k = children[i].getAttribute ? children[i].getAttribute(keyAttr) : "";
      if (k) {
        existing[k] = children[i];
        keyedCount += 1;
      }
    }
    if (keyedCount !== orderedKeys.length) return false;
    var orderedNodes = [];
    for (var j = 0; j < orderedKeys.length; j += 1) {
      var node = existing[orderedKeys[j]];
      if (!node) return false;
      orderedNodes.push(node);
    }
    for (var m = 0; m < orderedNodes.length; m += 1) {
      if (container.children[m] !== orderedNodes[m]) {
        moveChildNode(container, orderedNodes[m], container.children[m] || null);
      }
    }
    return true;
  }

  function reconcileKeyedHtml(container, items, options) {
    if (!container) return false;
    var opts = options || {};
    var keyAttr = opts.keyAttr || "data-row-key";
    var detailAttr = opts.detailAttr || "";
    var list = items || [];

    if (!list.length) {
      delete container.__bbKeyedReady;
      return patchHtmlIfChanged(container, opts.emptyHtml || "", opts);
    }

    var fullSig = "";
    for (var s = 0; s < list.length; s += 1) {
      fullSig += list[s].key + "\0" + list[s].html + "\n";
    }
    if (container.__bbLastHtml === fullSig && container.__bbKeyedReady) {
      return false;
    }

    var existingByKey = {};
    var existingCount = 0;
    var children = container.children;
    for (var i = 0; i < children.length; i += 1) {
      var child = children[i];
      var cKey = child.getAttribute ? child.getAttribute(keyAttr) : "";
      if (cKey) {
        existingByKey[cKey] = child;
        existingCount += 1;
      }
    }

    if (!container.__bbKeyedReady || existingCount === 0) {
      var joined = "";
      for (var j = 0; j < list.length; j += 1) joined += list[j].html;
      patchHtmlIfChanged(container, joined, opts);
      container.__bbLastHtml = fullSig;
      container.__bbKeyedReady = true;
      var newChildren = container.children;
      for (var n = 0; n < list.length && n < newChildren.length; n += 1) {
        newChildren[n].setAttribute(keyAttr, list[n].key);
        newChildren[n].__bbRowHtml = list[n].html;
      }
      return true;
    }

    var focusToken = captureFocusToken(container);
    var keepKeys = {};
    var desiredNodes = [];
    var parentTag = container.tagName || "div";

    for (var idx = 0; idx < list.length; idx += 1) {
      var item = list[idx];
      keepKeys[item.key] = true;
      var existing = existingByKey[item.key];
      if (existing && existing.__bbRowHtml === item.html) {
        desiredNodes.push(existing);
      } else {
        var nextNode = parseSingleElement(item.html, parentTag);
        if (!nextNode) {
          delete container.__bbKeyedReady;
          return patchHtmlIfChanged(container, list.map(function (x) { return x.html; }).join(""), opts);
        }
        nextNode.setAttribute(keyAttr, item.key);
        nextNode.__bbRowHtml = item.html;
        if (existing) {
          if (detailAttr) {
            restoreOpenDetails(nextNode, detailAttr, captureOpenDetails(existing, detailAttr));
          }
          container.replaceChild(nextNode, existing);
        }
        desiredNodes.push(nextNode);
      }
    }

    for (var r = container.children.length - 1; r >= 0; r -= 1) {
      var rem = container.children[r];
      var rKey = rem.getAttribute ? rem.getAttribute(keyAttr) : "";
      if (!rKey || !keepKeys[rKey]) {
        container.removeChild(rem);
      }
    }

    for (var p = 0; p < desiredNodes.length; p += 1) {
      var want = desiredNodes[p];
      var currentAt = container.children[p] || null;
      if (currentAt !== want) {
        moveChildNode(container, want, currentAt);
      }
    }

    container.__bbLastHtml = fullSig;
    container.__bbKeyedReady = true;
    if (focusToken) restoreFocusToken(container, focusToken);
    return true;
  }

  function createChartResizeObserver(container, getChart) {
    if (!container || typeof global.ResizeObserver === "undefined") return null;
    var throttledResize = rafThrottle(function () {
      var chart = getChart();
      if (chart) chart.resize();
    });
    var observer = new global.ResizeObserver(function (entries) {
      if (!getChart()) return;
      for (var i = 0; i < entries.length; i += 1) {
        if (entries[i].contentRect && entries[i].contentRect.width > 0) {
          throttledResize();
          break;
        }
      }
    });
    observer.observe(container);
    return observer;
  }

  function readPageConfig(scriptId) {
    var node = document.getElementById(scriptId || "page-config");
    if (!node) return {};
    try {
      return JSON.parse(node.textContent || "{}") || {};
    } catch (e) {
      return {};
    }
  }

  function resolveBasePrefix(pathname) {
    var raw = String(pathname || (global.location && global.location.pathname) || "/").replace(/\/+$/, "");
    if (!raw) return "";
    var compIdx = raw.indexOf("/accounts/comparison");
    if (compIdx >= 0) return raw.slice(0, compIdx).replace(/\/+$/, "");
    return raw.replace(/\/account\/[^/]+(?:\/legacy)?$/, "").replace(/\/legacy$/, "");
  }

  var navState = {
    basePrefix: "",
    activePage: "",
    activeAccountId: "",
    lastSig: ""
  };

  function updateScrollOverflowFade(scrollEl, wrapEl) {
    if (!scrollEl || !wrapEl) return;
    var hasLeft = scrollEl.scrollLeft > 4;
    var hasRight = (scrollEl.scrollWidth - scrollEl.clientWidth - scrollEl.scrollLeft) > 4;
    wrapEl.classList.toggle("has-overflow-left", hasLeft);
    wrapEl.classList.toggle("has-overflow-right", hasRight);
  }

  function bindScrollOverflowFade(scrollEl, wrapEl) {
    if (!scrollEl || !wrapEl) return;
    if (!scrollEl.__bbOverflowBound) {
      scrollEl.__bbOverflowBound = true;
      var throttledUpdate = rafThrottle(function () {
        updateScrollOverflowFade(scrollEl, wrapEl);
      });
      scrollEl.addEventListener("scroll", throttledUpdate, { passive: true });
      global.addEventListener("resize", throttledUpdate, { passive: true });
    }
    updateScrollOverflowFade(scrollEl, wrapEl);
  }

  function updateNavOverflowFade() {
    var nav = document.querySelector(".ops-nav");
    var wrap = nav && nav.parentElement && nav.parentElement.classList.contains("ops-nav-wrap")
      ? nav.parentElement
      : null;
    updateScrollOverflowFade(nav, wrap);
  }

  function rafThrottle(fn) {
    var rafId = 0;
    var lastArgs = null;
    var schedule = typeof requestAnimationFrame === "function"
      ? requestAnimationFrame
      : function (cb) { return setTimeout(cb, 16); };
    return function () {
      lastArgs = arguments;
      if (rafId) return;
      rafId = schedule(function () {
        rafId = 0;
        fn.apply(null, lastArgs);
      });
    };
  }

  var echartsWaiters = [];
  var echartsLoading = false;
  var echartsScriptEl = null;
  function ensureEcharts(src, onReady, onError) {
    if (typeof global.echarts !== "undefined") {
      if (typeof onReady === "function") onReady();
      return;
    }
    if (typeof document === "undefined") return;
    if (typeof onReady === "function" || typeof onError === "function") {
      echartsWaiters.push({ onReady: onReady, onError: onError });
    }
    if (echartsLoading) return;
    echartsLoading = true;
    if (echartsScriptEl && echartsScriptEl.parentNode) {
      echartsScriptEl.parentNode.removeChild(echartsScriptEl);
    }
    var script = document.createElement("script");
    script.src = src || "/static/vendor/echarts.min.js";
    script.async = true;
    echartsScriptEl = script;
    script.onload = function () {
      echartsLoading = false;
      var waiters = echartsWaiters.splice(0, echartsWaiters.length);
      for (var i = 0; i < waiters.length; i += 1) {
        if (typeof waiters[i].onReady === "function") waiters[i].onReady();
      }
    };
    script.onerror = function () {
      echartsLoading = false;
      if (script.parentNode) script.parentNode.removeChild(script);
      if (echartsScriptEl === script) echartsScriptEl = null;
      var waiters = echartsWaiters.splice(0, echartsWaiters.length);
      for (var i = 0; i < waiters.length; i += 1) {
        if (typeof waiters[i].onError === "function") waiters[i].onError();
      }
    };
    document.head.appendChild(script);
  }

  function bindNavOverflowListeners() {
    var nav = document.querySelector(".ops-nav");
    var wrap = nav && nav.parentElement && nav.parentElement.classList.contains("ops-nav-wrap")
      ? nav.parentElement
      : null;
    bindScrollOverflowFade(nav, wrap);
  }

  function renderNavAccounts(accounts) {
    var listEl = document.getElementById("ops-nav-accounts");
    var dividerEl = document.getElementById("ops-nav-divider");
    if (!listEl) return;
    var rows = Array.isArray(accounts) ? accounts : [];
    var seen = {};
    var managed = [];
    var readonly = [];
    for (var i = 0; i < rows.length; i += 1) {
      var item = rows[i] || {};
      var aid = String(item.id || item.account_id || "").trim();
      if (!aid || seen[aid]) continue;
      seen[aid] = true;
      var mode = String(item.mode || "full").toLowerCase();
      if (mode === "loss_cut_only") continue;
      (mode === "readonly" ? readonly : managed).push({ id: aid, mode: mode });
    }
    var ordered = managed.concat(readonly);
    var sig = navState.basePrefix + "|" + navState.activePage + "|" + navState.activeAccountId + "|"
      + ordered.map(function (entry) { return entry.id + ":" + entry.mode; }).join(",");
    if (sig === navState.lastSig && listEl.children.length === ordered.length) {
      return;
    }
    navState.lastSig = sig;

    var html = "";
    for (var j = 0; j < ordered.length; j += 1) {
      var entry = ordered[j];
      var cls = "ops-nav-link ops-nav-account" + (entry.mode === "readonly" ? " is-readonly" : "");
      var isCurrent = navState.activePage === "account" && navState.activeAccountId === entry.id;
      var href = (navState.basePrefix || "") + "/account/" + encodeURIComponent(entry.id) + "/";
      html += '<li><a class="' + cls + '" href="' + escapeHtml(href) + '"'
        + (isCurrent ? ' aria-current="page"' : "")
        + '><span class="ops-nav-dot" aria-hidden="true"></span>' + escapeHtml(entry.id) + "</a></li>";
    }
    listEl.innerHTML = html;
    if (dividerEl) dividerEl.hidden = !html;
    var activeLink = listEl.querySelector('[aria-current="page"]');
    if (activeLink && activeLink.scrollIntoView) {
      try {
        activeLink.scrollIntoView({ block: "nearest", inline: "nearest" });
      } catch (e) {}
    }
    updateNavOverflowFade();
  }

  function initTopNav(options) {
    var opts = options || {};
    navState.basePrefix = opts.basePrefix != null ? String(opts.basePrefix) : resolveBasePrefix();
    navState.activePage = String(opts.activePage || "");
    navState.activeAccountId = String(opts.activeAccountId || "");
    navState.lastSig = "";

    var brandLink = document.getElementById("ops-nav-brand");
    var overviewLink = document.getElementById("ops-nav-overview");
    var comparisonLink = document.getElementById("ops-nav-comparison");
    var homeHref = (navState.basePrefix || "") + "/";
    var compHref = (navState.basePrefix || "") + "/accounts/comparison/";
    if (brandLink) brandLink.setAttribute("href", homeHref);
    if (overviewLink) {
      overviewLink.setAttribute("href", homeHref);
      if (navState.activePage === "overview") overviewLink.setAttribute("aria-current", "page");
      else overviewLink.removeAttribute("aria-current");
    }
    if (comparisonLink) {
      comparisonLink.setAttribute("href", compHref);
      if (navState.activePage === "comparison") comparisonLink.setAttribute("aria-current", "page");
      else comparisonLink.removeAttribute("aria-current");
    }
    bindNavOverflowListeners();
    initTermTooltips();
    initHotkeyHelp();
    if (Array.isArray(opts.accounts) && opts.accounts.length) {
      renderNavAccounts(opts.accounts);
    }
  }

  var termTooltipsBound = false;
  var activeTooltipTerm = null;

  function ensureTooltipPortal() {
    if (typeof document === "undefined" || !document.body) return null;
    var portal = document.getElementById("ops-tooltip-portal");
    if (!portal) {
      portal = document.createElement("div");
      portal.id = "ops-tooltip-portal";
      portal.className = "ops-tooltip-portal";
      portal.setAttribute("role", "tooltip");
      portal.hidden = true;
      document.body.appendChild(portal);
    }
    return portal;
  }

  function positionTooltipPortal(term, portal) {
    if (!term || !portal || !term.getBoundingClientRect || typeof window === "undefined") return;
    var rect = term.getBoundingClientRect();
    var vw = window.innerWidth || document.documentElement.clientWidth || 1024;
    var vh = window.innerHeight || document.documentElement.clientHeight || 768;
    var margin = 12;
    var gap = 6;
    var popRect = portal.getBoundingClientRect();
    var preferBelow = term.classList.contains("tip-below");

    var top = preferBelow ? (rect.bottom + gap) : (rect.top - popRect.height - gap);
    if (!preferBelow && top < margin) {
      top = rect.bottom + gap;
    } else if (preferBelow && (top + popRect.height > vh - margin)) {
      var above = rect.top - popRect.height - gap;
      if (above >= margin) top = above;
    }

    var left = rect.left;
    if (left + popRect.width > vw - margin) {
      left = vw - margin - popRect.width;
    }
    if (left < margin) left = margin;

    portal.style.left = Math.round(left) + "px";
    portal.style.top = Math.round(Math.max(margin, top)) + "px";
  }

  function showTermTooltip(term) {
    if (!term) return;
    var tip = term.getAttribute("data-tip");
    if (!tip) return;
    var portal = ensureTooltipPortal();
    if (!portal) return;
    if (activeTooltipTerm && activeTooltipTerm !== term) {
      activeTooltipTerm.classList.remove("is-tip-open");
      activeTooltipTerm.removeAttribute("aria-describedby");
    }
    activeTooltipTerm = term;
    portal.textContent = tip;
    portal.hidden = false;
    term.classList.add("is-tip-open");
    term.setAttribute("aria-describedby", portal.id);
    positionTooltipPortal(term, portal);
  }

  function hideTermTooltip(targetTerm) {
    if (targetTerm && activeTooltipTerm && targetTerm !== activeTooltipTerm) return;
    var portal = document.getElementById("ops-tooltip-portal");
    if (portal) portal.hidden = true;
    if (activeTooltipTerm) {
      activeTooltipTerm.classList.remove("is-tip-open");
      activeTooltipTerm.removeAttribute("aria-describedby");
      activeTooltipTerm = null;
    }
  }

  function initTermTooltips() {
    if (termTooltipsBound || typeof document === "undefined") return;
    termTooltipsBound = true;

    document.addEventListener("pointerenter", function (evt) {
      if (evt.pointerType === "touch") return;
      var term = evt.target && evt.target.closest ? evt.target.closest(".ops-term[data-tip]") : null;
      if (!term) return;
      showTermTooltip(term);
    }, true);

    document.addEventListener("focusin", function (evt) {
      var term = evt.target && evt.target.closest ? evt.target.closest(".ops-term[data-tip]") : null;
      if (!term) return;
      showTermTooltip(term);
    });

    document.addEventListener("pointerleave", function (evt) {
      if (evt.pointerType === "touch") return;
      var term = evt.target && evt.target.closest ? evt.target.closest(".ops-term[data-tip]") : null;
      if (!term) return;
      if (document.activeElement === term) return;
      hideTermTooltip(term);
    }, true);

    document.addEventListener("focusout", function (evt) {
      var term = evt.target && evt.target.closest ? evt.target.closest(".ops-term[data-tip]") : null;
      if (!term) return;
      hideTermTooltip(term);
    });

    document.addEventListener("click", function (evt) {
      var term = evt.target && evt.target.closest ? evt.target.closest(".ops-term[data-tip]") : null;
      if (!term) {
        hideTermTooltip();
        return;
      }
      if (activeTooltipTerm === term) {
        hideTermTooltip(term);
      } else {
        showTermTooltip(term);
      }
    });

    document.addEventListener("keydown", function (evt) {
      if (evt.key === "Escape" && activeTooltipTerm) {
        hideTermTooltip();
        return;
      }
      if ((evt.key === "Enter" || evt.key === " ") && evt.target && evt.target.closest) {
        var term = evt.target.closest(".ops-term[data-tip]");
        if (!term) return;
        evt.preventDefault();
        if (activeTooltipTerm === term) hideTermTooltip(term);
        else showTermTooltip(term);
      }
    });

    global.addEventListener("scroll", function () {
      if (activeTooltipTerm) hideTermTooltip();
    }, { passive: true, capture: true });

    global.addEventListener("resize", function () {
      if (activeTooltipTerm) hideTermTooltip();
    }, { passive: true });
  }

  var hotkeyHelpBound = false;
  function initHotkeyHelp() {
    if (hotkeyHelpBound || typeof document === "undefined") return;
    var btn = document.getElementById("ops-hotkey-btn");
    var pop = document.getElementById("ops-hotkey-popover");
    if (!btn || !pop) return;
    hotkeyHelpBound = true;

    function setOpen(nextOpen, restoreFocus) {
      pop.hidden = !nextOpen;
      btn.setAttribute("aria-expanded", nextOpen ? "true" : "false");
      if (nextOpen && pop.focus) {
        try { pop.focus({ preventScroll: true }); } catch (e) { pop.focus(); }
      } else if (!nextOpen && restoreFocus && btn.focus) {
        btn.focus();
      }
    }

    btn.addEventListener("click", function (evt) {
      evt.stopPropagation();
      var focusInside = !pop.hidden && pop.contains(document.activeElement);
      setOpen(pop.hidden, focusInside);
    });

    document.addEventListener("click", function (evt) {
      if (pop.hidden) return;
      if (pop.contains(evt.target) || btn.contains(evt.target)) return;
      setOpen(false, false);
    });

    document.addEventListener("focusin", function (evt) {
      if (pop.hidden) return;
      if (pop.contains(evt.target) || btn.contains(evt.target)) return;
      setOpen(false, false);
    });

    document.addEventListener("keydown", function (evt) {
      if (evt.key === "Escape" && !pop.hidden) {
        evt.preventDefault();
        setOpen(false, true);
      }
    });
  }

  function isEditableTarget(target) {
    if (!target || !target.tagName) return false;
    var tag = target.tagName.toUpperCase();
    return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || target.isContentEditable;
  }

  function navigateToElementHref(linkEl) {
    if (!linkEl) return false;
    var href = linkEl.getAttribute("href");
    if (!href) return false;
    if (linkEl.getAttribute("aria-current") === "page") return true;
    global.location.assign(href);
    return true;
  }

  var boundShortcutListener = null;
  function bindKeyboardShortcuts(handlers) {
    if (typeof document === "undefined") return;
    if (boundShortcutListener) {
      document.removeEventListener("keydown", boundShortcutListener);
    }
    var cfg = handlers || {};
    boundShortcutListener = function (evt) {
      if (evt.metaKey || evt.ctrlKey || evt.altKey) return;
      if (isEditableTarget(evt.target)) return;
      var lowerKey = evt.key ? String(evt.key).toLowerCase() : "";
      if (evt.key === "?") {
        var hotkeyBtn = document.getElementById("ops-hotkey-btn");
        if (hotkeyBtn) {
          evt.preventDefault();
          hotkeyBtn.click();
        }
      } else if (lowerKey === "r" && typeof cfg.onRefresh === "function") {
        evt.preventDefault();
        cfg.onRefresh();
      } else if (evt.key === "/" && typeof cfg.onSearch === "function") {
        evt.preventDefault();
        cfg.onSearch();
      } else if (evt.key === "0") {
        var overviewLink = document.getElementById("ops-nav-overview");
        if (navigateToElementHref(overviewLink)) evt.preventDefault();
      } else if (lowerKey === "c") {
        var comparisonLink = document.getElementById("ops-nav-comparison");
        if (navigateToElementHref(comparisonLink)) evt.preventDefault();
      } else if (/^[1-9]$/.test(evt.key)) {
        var idx = Number(evt.key) - 1;
        var accountLinks = document.querySelectorAll("#ops-nav-accounts a.ops-nav-account");
        if (accountLinks && accountLinks[idx] && navigateToElementHref(accountLinks[idx])) {
          evt.preventDefault();
        }
      }
    };
    document.addEventListener("keydown", boundShortcutListener);
  }

  function readQueryParams() {
    var out = {};
    if (!global.location || !global.location.search) return out;
    if (typeof URLSearchParams === "function") {
      var params = new URLSearchParams(global.location.search);
      params.forEach(function (value, key) {
        out[key] = value;
      });
    }
    return out;
  }

  function updateQueryParams(updates) {
    if (!global.history || !global.history.replaceState || typeof URLSearchParams !== "function") return;
    var params = new URLSearchParams((global.location && global.location.search) || "");
    var keys = Object.keys(updates || {});
    for (var i = 0; i < keys.length; i += 1) {
      var key = keys[i];
      var val = updates[key];
      if (val === null || val === undefined || val === "") params.delete(key);
      else params.set(key, String(val));
    }
    var qs = params.toString();
    var nextUrl = ((global.location && global.location.pathname) || "/")
      + (qs ? "?" + qs : "")
      + ((global.location && global.location.hash) || "");
    try {
      global.history.replaceState(null, "", nextUrl);
    } catch (e) {}
  }

  global.BBOps = {
    ARROW_ICON: ARROW_ICON,
    escapeHtml: escapeHtml,
    compareText: compareText,
    compareNullableNumbers: compareNullableNumbers,
    toNum: toNum,
    fmtNum: fmtNum,
    fmtSigned: fmtSigned,
    fmtUsdt: fmtUsdt,
    fmtSignedUsdt: fmtSignedUsdt,
    fmtPct: fmtPct,
    fmtWithUnit: fmtWithUnit,
    prefersReducedMotion: prefersReducedMotion,
    rafThrottle: rafThrottle,
    ensureEcharts: ensureEcharts,
    getChartTheme: getChartTheme,
    buildEchartsChrome: buildEchartsChrome,
    createChartResizeObserver: createChartResizeObserver,
    splitStrategyParts: splitStrategyParts,
    formatStrategyTags: formatStrategyTags,
    formatStrategyHighlights: formatStrategyHighlights,
    formatStatusLabel: formatStatusLabel,
    statusTone: statusTone,
    pickLatestTimestamp: pickLatestTimestamp,
    parseTimestampMs: parseTimestampMs,
    formatBeijingTime: formatBeijingTime,
    evaluateDataFreshness: evaluateDataFreshness,
    announceStatus: announceStatus,
    formatWalletSource: formatWalletSource,
    applyFreshnessPill: applyFreshnessPill,
    cycleStartMs: cycleStartMs,
    computeCycleRiskMetrics: computeCycleRiskMetrics,
    evaluatePortfolioStopState: evaluatePortfolioStopState,
    classifyReturnTone: classifyReturnTone,
    classifyDrawdownTone: classifyDrawdownTone,
    parseSummaryPairs: parseSummaryPairs,
    fetchJsonWithTimeout: fetchJsonWithTimeout,
    setButtonBusy: setButtonBusy,
    syncPressedButtons: syncPressedButtons,
    createAlertController: createAlertController,
    createEchartsZoomHandler: createEchartsZoomHandler,
    bindScrollOverflowFade: bindScrollOverflowFade,
    patchHtmlIfChanged: patchHtmlIfChanged,
    reorderChildrenByKey: reorderChildrenByKey,
    reconcileKeyedHtml: reconcileKeyedHtml,
    readPageConfig: readPageConfig,
    resolveBasePrefix: resolveBasePrefix,
    readQueryParams: readQueryParams,
    updateQueryParams: updateQueryParams,
    initTopNav: initTopNav,
    renderNavAccounts: renderNavAccounts,
    bindKeyboardShortcuts: bindKeyboardShortcuts
  };
})(typeof window !== "undefined" ? window : globalThis);
