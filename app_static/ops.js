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

  function formatStrategyTags(note, baseClass) {
    var clsBase = baseClass || "strategy-chip";
    var emptyClass = clsBase === "strategy-tag" ? "strategy-tag strategy-tag-empty" : clsBase;
    var raw = String(note || "").trim();
    if (!raw) return '<span class="' + emptyClass + '">未配置策略说明</span>';
    var parts = raw.split("/").map(function (s) { return String(s || "").trim(); }).filter(Boolean);
    var html = "";
    for (var i = 0; i < parts.length; i += 1) {
      var part = parts[i];
      var upper = part.toUpperCase();
      var cls = clsBase === "strategy-tag" ? "strategy-tag strategy-tag-protection" : clsBase;
      if (upper.indexOf("OFF") >= 0) {
        cls = clsBase === "strategy-tag" ? "strategy-tag strategy-tag-off" : clsBase + " off";
      } else if (
        upper.indexOf("TP") >= 0
        || part.indexOf("减仓") >= 0
        || part.indexOf("清仓") >= 0
        || part.indexOf("组合止盈") >= 0
      ) {
        cls = clsBase === "strategy-tag" ? "strategy-tag strategy-tag-primary" : clsBase + " primary";
      }
      html += '<span class="' + cls + '">' + escapeHtml(part) + "</span>";
    }
    return html || '<span class="' + emptyClass + '">未配置策略说明</span>';
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

  global.BBOps = {
    escapeHtml: escapeHtml,
    toNum: toNum,
    fmtNum: fmtNum,
    fmtSigned: fmtSigned,
    fmtPct: fmtPct,
    fmtWithUnit: fmtWithUnit,
    prefersReducedMotion: prefersReducedMotion,
    formatStrategyTags: formatStrategyTags,
    resolveBasePrefix: resolveBasePrefix,
    initTopNav: initTopNav,
    renderNavAccounts: renderNavAccounts
  };
})(window);
