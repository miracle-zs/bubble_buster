/* Bubble Buster — Shared Primer Quiet Ops utilities, domain calculations & navigation */
(function (global) {
  "use strict";

  var SHANGHAI_TZ = "Asia/Shanghai";
  var SHANGHAI_OFFSET_MS = 8 * 60 * 60 * 1000;

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
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

  function readCssVar(name, fallback) {
    try {
      var val = global.getComputedStyle(document.documentElement).getPropertyValue(name).trim();
      return val || fallback;
    } catch (e) {
      return fallback;
    }
  }

  function getChartTheme() {
    return {
      bg: readCssVar("--bg", "#0d1117"),
      surface: readCssVar("--surface", "#161b22"),
      surfaceSubtle: readCssVar("--surface-subtle", "#1c2128"),
      surfaceInset: readCssVar("--surface-inset", "#0d1117"),
      border: readCssVar("--border", "#30363d"),
      borderStrong: readCssVar("--border-strong", "#484f58"),
      text: readCssVar("--text", "#f0f6fc"),
      textSecondary: readCssVar("--text-secondary", "#c9d1d9"),
      muted: readCssVar("--muted", "#919ba6"),
      accent: readCssVar("--accent", "#58a6ff"),
      ok: readCssVar("--ok", "#3fb950"),
      okText: readCssVar("--ok-text", "#56d364"),
      warn: readCssVar("--warn", "#d29922"),
      warnText: readCssVar("--warn-text", "#e3b341"),
      bad: readCssVar("--bad", "#f85149"),
      badText: readCssVar("--bad-text", "#ff7b72"),
      readonly: readCssVar("--readonly", "#bc8cff"),
      splitLine: readCssVar("--chart-split-line", "rgba(48, 54, 61, 0.65)"),
      splitLineSubtle: readCssVar("--chart-split-line-subtle", "rgba(48, 54, 61, 0.55)"),
      zoomFill: readCssVar("--chart-zoom-fill", "rgba(88, 166, 255, 0.16)"),
      zoomArea: readCssVar("--chart-zoom-area", "rgba(88, 166, 255, 0.08)"),
      areaOk: readCssVar("--chart-area-ok", "rgba(63, 185, 80, 0.16)"),
      areaBad: readCssVar("--chart-area-bad", "rgba(248, 81, 73, 0.16)"),
      areaFade: readCssVar("--chart-area-fade", "rgba(13, 17, 23, 0)"),
      series: [
        readCssVar("--series-1", "#58a6ff"),
        readCssVar("--series-2", "#f0883e"),
        readCssVar("--series-3", "#3fb950"),
        readCssVar("--series-4", "#e3b341"),
        readCssVar("--series-5", "#39c5cf"),
        readCssVar("--series-6", "#f778ba")
      ]
    };
  }

  function formatStrategyDisplay(part) {
    var text = String(part || "").trim();
    if (!text) return "";
    // Insert clean spacing around compound clauses like 组合止盈2.5%回撤15%减仓50%
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

  function parseTimestampMs(value) {
    if (typeof value === "number") return Number.isFinite(value) ? value : NaN;
    var raw = String(value || "").trim();
    if (!raw || raw === "--") return NaN;
    var normalized = raw;
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(normalized) && !/[zZ]|[+-]\d{2}:?\d{2}$/.test(normalized)) {
      normalized = normalized.replace(" ", "T") + "+08:00";
    }
    var ms = Date.parse(normalized);
    return Number.isFinite(ms) ? ms : NaN;
  }

  function formatBeijingTime(value, options) {
    if (value === null || value === undefined || value === "") return "--";
    var opts = options || {};
    var ms = parseTimestampMs(value);
    if (!Number.isFinite(ms)) {
      var raw = String(value);
      return raw.replace("T", " ").slice(0, opts.withSeconds ? 19 : 16);
    }
    var date = new Date(ms);
    var tz = opts.timeZone || SHANGHAI_TZ;
    try {
      var intlOpts = {
        timeZone: tz,
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      };
      if (opts.withYear) intlOpts.year = "numeric";
      if (opts.withSeconds) intlOpts.second = "2-digit";
      if (opts.dateOnly) {
        intlOpts = { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" };
      }
      return new Intl.DateTimeFormat("zh-CN", intlOpts).format(date).replace(/\//g, "-");
    } catch (e) {
      var local = new Date(ms + SHANGHAI_OFFSET_MS).toISOString().replace("T", " ");
      if (opts.dateOnly) return local.slice(0, 10);
      var start = opts.withYear ? 0 : 5;
      var end = opts.withSeconds ? 19 : 16;
      return local.slice(start, end);
    }
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
      return { t: pt.t || "", ms: parseTimestampMs(pt.t), equity: Number(pt.equity) };
    });
    if (!scoped.length) return null;

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
          var currMs = Number.isFinite(lastReal.ms) ? lastReal.ms + 1000 : NaN;
          scoped.push({ t: lastReal.t, ms: currMs, equity: currEq, synthetic: true });
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

  function classifyReturnTone(returnPct, stopPct, stopStatus) {
    var n = toNum(returnPct);
    var st = String(stopStatus || "").toUpperCase();
    if (st === "TRIGGERED_COMPLETE" || st === "CLOSING" || st === "THRESHOLD_REACHED") return "bad";
    if (n === null) return "";
    if (n > 0) return "ok";
    if (n < 0) return "bad";
    return "";
  }

  function classifyDrawdownTone(drawdownPct, stopPct) {
    var n = toNum(drawdownPct);
    var limit = Number.isFinite(Number(stopPct)) && Number(stopPct) > 0 ? Number(stopPct) : 3.5;
    if (n === null) return "";
    if (n <= -(limit * 0.7)) return "bad";
    if (n < 0) return "warn";
    return "ok";
  }

  function parseSummaryPairs(summary) {
    var raw = String(summary || "--").trim();
    var pairs = {};
    if (!raw || raw === "--") return pairs;
    var matcher = /(?:^|\s)([A-Za-z0-9_.-]+)=/g;
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

  function patchHtmlIfChanged(node, html) {
    if (!node) return false;
    if (node.__bbLastHtml === html) return false;
    node.__bbLastHtml = html;
    node.innerHTML = html;
    return true;
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
    return raw.replace(/\/legacy$/, "").replace(/\/account\/[^/]+$/, "");
  }

  var navState = {
    basePrefix: "",
    activePage: "",
    activeAccountId: "",
    lastSig: ""
  };

  function updateNavOverflowFade() {
    var nav = document.querySelector(".ops-nav");
    var wrap = nav && nav.parentElement && nav.parentElement.classList.contains("ops-nav-wrap")
      ? nav.parentElement
      : null;
    if (!nav || !wrap) return;
    var hasRight = (nav.scrollWidth - nav.clientWidth - nav.scrollLeft) > 4;
    wrap.classList.toggle("has-overflow-right", hasRight);
  }

  function bindNavOverflowListeners() {
    var nav = document.querySelector(".ops-nav");
    if (!nav || nav.__bbOverflowBound) return;
    nav.__bbOverflowBound = true;
    nav.addEventListener("scroll", updateNavOverflowFade, { passive: true });
    global.addEventListener("resize", updateNavOverflowFade, { passive: true });
    updateNavOverflowFade();
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
      html += '<li><a class="' + cls + '" href="' + href + '"'
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
    if (Array.isArray(opts.accounts) && opts.accounts.length) {
      renderNavAccounts(opts.accounts);
    }
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
    escapeHtml: escapeHtml,
    toNum: toNum,
    fmtNum: fmtNum,
    fmtSigned: fmtSigned,
    fmtUsdt: fmtUsdt,
    fmtSignedUsdt: fmtSignedUsdt,
    fmtPct: fmtPct,
    fmtWithUnit: fmtWithUnit,
    prefersReducedMotion: prefersReducedMotion,
    getChartTheme: getChartTheme,
    formatStrategyDisplay: formatStrategyDisplay,
    splitStrategyParts: splitStrategyParts,
    formatStrategyTags: formatStrategyTags,
    formatStrategyHighlights: formatStrategyHighlights,
    parseTimestampMs: parseTimestampMs,
    formatBeijingTime: formatBeijingTime,
    evaluateDataFreshness: evaluateDataFreshness,
    cycleStartMs: cycleStartMs,
    computeCycleRiskMetrics: computeCycleRiskMetrics,
    classifyReturnTone: classifyReturnTone,
    classifyDrawdownTone: classifyDrawdownTone,
    parseSummaryPairs: parseSummaryPairs,
    patchHtmlIfChanged: patchHtmlIfChanged,
    readPageConfig: readPageConfig,
    resolveBasePrefix: resolveBasePrefix,
    readQueryParams: readQueryParams,
    updateQueryParams: updateQueryParams,
    initTopNav: initTopNav,
    renderNavAccounts: renderNavAccounts
  };
})(typeof window !== "undefined" ? window : globalThis);
