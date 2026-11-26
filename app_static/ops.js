/* Bubble Buster — Shared Primer Quiet Ops utilities and cross-account top navigation */
(function (global) {
  "use strict";

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
      html += '<span class="' + classifyStrategyPart(part, clsBase) + '">' + escapeHtml(part) + "</span>";
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
      html += '<span class="' + classifyStrategyPart(item, clsBase) + '" title="' + escapeHtml(item) + '">' + escapeHtml(item) + "</span>";
    }
    return html;
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
    activeAccountId: ""
  };

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
  }

  function initTopNav(options) {
    var opts = options || {};
    navState.basePrefix = opts.basePrefix != null ? String(opts.basePrefix) : resolveBasePrefix();
    navState.activePage = String(opts.activePage || "");
    navState.activeAccountId = String(opts.activeAccountId || "");

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
    fmtPct: fmtPct,
    fmtWithUnit: fmtWithUnit,
    prefersReducedMotion: prefersReducedMotion,
    splitStrategyParts: splitStrategyParts,
    formatStrategyTags: formatStrategyTags,
    formatStrategyHighlights: formatStrategyHighlights,
    resolveBasePrefix: resolveBasePrefix,
    readQueryParams: readQueryParams,
    updateQueryParams: updateQueryParams,
    initTopNav: initTopNav,
    renderNavAccounts: renderNavAccounts
  };
})(window);
