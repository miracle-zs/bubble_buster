/* Bubble Buster — Accounts Overview controller (Obsidian Desk Terminal) */
(function () {
  "use strict";

  document.addEventListener("DOMContentLoaded", function () {
    var ops = window.BBOps;
    var R = window.BBOverviewRender;
    if (!ops || !R) return;

    var cfg = ops.readPageConfig("page-config");
    var refreshSec = Math.max(15, Number(cfg.refreshSec) || 15);
    var pathPrefix = ops.resolveBasePrefix();
    var summaryApi = pathPrefix + "/api/accounts/summary";
    var summaryFastApi = summaryApi + "/fast";
    var summaryDetailsApi = summaryApi + "/details";
    var curvesBatchApi = pathPrefix + "/api/accounts/curves";
    var healthApi = pathPrefix + "/healthz";
    var portfolioStopEnabled = !!cfg.portfolioStopEnabled;
    var portfolioStopPct = ops.toNum(cfg.portfolioStopPct) !== null ? ops.toNum(cfg.portfolioStopPct) : 3.5;
    var portfolioStopHour = ops.toNum(cfg.portfolioStopHour) !== null ? ops.toNum(cfg.portfolioStopHour) : 8;
    var portfolioStopMinute = ops.toNum(cfg.portfolioStopMinute) !== null ? ops.toNum(cfg.portfolioStopMinute) : 0;
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
    var setAlertError = ops.createAlertController(overviewAlert, overviewAlertText, ["summary", "details"]);
    var serviceHealthState = { enabled: false, running: false, loaded: false };
    var curveCache = {};
    var curveErrors = {};
    var curveInFlight = {};
    var curveRenderData = {};
    var curveRenderSig = {};
    var curveInputSig = {};
    var curveObserver = null;
    var accountModes = {};
    var portfolioStopStates = {};
    var summaryRows = [];
    var summaryGeneratedAtUtc = "";
    var initialQuery = ops.readQueryParams();
    var validTaskFilters = { all: true, anomaly: true, symbols: true };
    var taskFilter = validTaskFilters[initialQuery.task_filter] ? initialQuery.task_filter : "anomaly";
    var entryDetailsUserOverride = null;
    var nextEntryCheckRaw = "";
    var countdownTimer = null;
    var summaryTimer = null;
    var healthTimer = null;
    var curveTtlMs = Math.max(60000, refreshSec * 3000);
    var summaryRequestInFlight = false;
    var summaryDetailsRequestInFlight = false;
    var summaryHydratedOnce = false;
    var detailsHydratedOnce = false;
    var lastDetailsFetchAt = 0;
    var detailsRetryTimer = null;
    var detailsRetryAttempts = 0;
    var commandBarRafId = 0;

    function escapeHtml(text) {
      return ops.escapeHtml(text);
    }

    var fmt = R.fmt;
    var taskStatusText = R.taskStatusText;
    var taskStatusCls = R.taskStatusCls;
    var worstAccountTaskStatus = R.worstAccountTaskStatus;
    var anomalyTaskCount = R.anomalyTaskCount;

    function setOverviewError(slot, message) {
      overviewErrors[slot] = message || "";
      setAlertError(slot, message);
    }

    function mergePortfolioStopState(aid, nextStop) {
      if (!aid) return;
      if (!nextStop) {
        if (!Object.prototype.hasOwnProperty.call(portfolioStopStates, aid)) {
          portfolioStopStates[aid] = null;
        }
        return;
      }
      var current = portfolioStopStates[aid];
      if (!current || !current.updated_at_utc) {
        portfolioStopStates[aid] = nextStop;
        return;
      }
      var prevMs = ops.parseTimestampMs(current.updated_at_utc);
      var nextMs = nextStop.updated_at_utc ? ops.parseTimestampMs(nextStop.updated_at_utc) : NaN;
      if (!Number.isFinite(prevMs) || (Number.isFinite(nextMs) && nextMs >= prevMs)) {
        portfolioStopStates[aid] = nextStop;
      }
    }

    function isAccountStopped(aid, row) {
      var stop = (row && row.portfolio_loss_cut) || portfolioStopStates[aid] || null;
      var st = String((stop && stop.status) || "").toUpperCase();
      return st === "TRIGGERED_COMPLETE" || st === "CLOSING" || st === "THRESHOLD_REACHED";
    }

    function latestUnderlyingTimestamp(rows) {
      var candidates = [];
      for (var i = 0; i < (rows || []).length; i += 1) {
        var r = rows[i] || {};
        var aid = String(r.account_id || "");
        var cached = curveCache[aid];
        if (cached && cached.walletAsOfUtc) {
          candidates.push(ops.parseTimestampMs(cached.walletAsOfUtc));
        } else if (cached && cached.points && cached.points.length) {
          var lastPt = cached.points[cached.points.length - 1];
          if (lastPt && lastPt.t && !lastPt.carried_forward) {
            candidates.push(ops.parseTimestampMs(lastPt.t));
          }
        }
        if (r.portfolio_loss_cut && r.portfolio_loss_cut.updated_at_utc) {
          candidates.push(ops.parseTimestampMs(r.portfolio_loss_cut.updated_at_utc));
        }
        if (r.live_position_as_of_utc) {
          candidates.push(ops.parseTimestampMs(r.live_position_as_of_utc));
        }
        if (r.trade_stats && r.trade_stats.last_updated_utc) {
          candidates.push(ops.parseTimestampMs(r.trade_stats.last_updated_utc));
        }
      }
      var valid = candidates.filter(Number.isFinite);
      if (valid.length) return Math.max.apply(null, valid);
      if (summaryGeneratedAtUtc) {
        var genMs = ops.parseTimestampMs(summaryGeneratedAtUtc);
        if (Number.isFinite(genMs)) return genMs;
      }
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
      var sig = severity + "|" + stoppedCount + "|" + anomalies + "|" + (serviceDown ? 1 : 0) + "|" + (freshness ? freshness.state : "");
      if (incidentBanner.getAttribute("data-incident-sig") !== sig) {
        incidentBanner.setAttribute("data-incident-sig", sig);
        ops.announceStatus((severity === "bad" ? "风险关注：" : "数据时效提示：") + alerts.join(" · "));
      }
      incidentBanner.hidden = false;
      incidentBanner.className = "ops-incident-banner" + (severity === "warn" ? " is-warn" : "");
      incidentPill.textContent = severity === "bad" ? "风险关注" : "数据时效提示";
      incidentText.textContent = alerts.join(" · ");
      if (incidentJumpBtn) incidentJumpBtn.hidden = anomalies === 0;
    }

    function renderCommandBar() {
      var latestMs = latestUnderlyingTimestamp(summaryRows);
      var freshness = ops.evaluateDataFreshness(latestMs, { warnSec: 180, staleSec: 900 });

      if (overviewUpdatedAt) {
        overviewUpdatedAt.textContent = Number.isFinite(latestMs)
          ? ops.formatBeijingTime(latestMs, { withYear: freshness.ageSec >= 86400, withSeconds: freshness.ageSec < 3600 })
          : "--";
      }
      ops.applyFreshnessPill(overviewFreshnessPill, overviewFreshnessText, freshness);

      if (overviewAnomalyCount) {
        var anomalyCue = document.getElementById("overview-anomaly-cue");
        if (!summaryHydratedOnce) {
          overviewAnomalyCount.textContent = "--";
          overviewAnomalyCount.className = "command-value";
          if (anomalyCue) anomalyCue.hidden = true;
        } else {
          var count = anomalyTaskCount(summaryRows);
          overviewAnomalyCount.textContent = String(count);
          overviewAnomalyCount.className = "command-value" + (count > 0 ? " status-bad" : "");
          if (anomalyCue) anomalyCue.hidden = count === 0;
        }
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

    function scheduleCommandBarRender() {
      if (commandBarRafId) return;
      var schedule = window.requestAnimationFrame || function (fn) { return setTimeout(fn, 16); };
      commandBarRafId = schedule(function () {
        commandBarRafId = 0;
        renderCommandBar();
      });
    }

    var healthAbortController = null;
    function fetchHealth() {
      if (!serviceState || document.hidden) return;
      if (healthAbortController) {
        try { healthAbortController.abort(); } catch (e) {}
      }
      var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      healthAbortController = controller;
      ops.fetchJsonWithTimeout(healthApi, {
        signal: controller ? controller.signal : null,
        timeoutMs: 10000
      })
        .then(function (payload) {
          if (healthAbortController !== controller) return;
          healthAbortController = null;
          var running = !!payload.service_running;
          var enabled = !!payload.service_enabled;
          serviceHealthState = { enabled: enabled, running: running, loaded: true };
          var nextHealthSig = running ? "ok" : (enabled ? "bad" : "off");
          if (serviceState.getAttribute("data-health-sig") !== nextHealthSig) {
            serviceState.setAttribute("data-health-sig", nextHealthSig);
            serviceState.innerHTML = '<span class="status-dot" aria-hidden="true"></span>'
              + (running ? "运行中" : (enabled ? "已停止" : "未启用"));
            serviceState.className = "command-value" + (running ? " status-ok" : (enabled ? " status-bad" : ""));
          }
          scheduleCommandBarRender();
        })
        .catch(function (err) {
          if (healthAbortController !== controller) return;
          healthAbortController = null;
          if (err && err.name === "AbortError") return;
          if (serviceState.getAttribute("data-health-sig") !== "unknown") {
            serviceState.setAttribute("data-health-sig", "unknown");
            serviceState.innerHTML = '<span class="status-dot" aria-hidden="true"></span>未知';
            serviceState.className = "command-value status-warn";
          }
        });
    }

    var latestTaskTime = R.latestTaskTime;
    var formatEntryCountdown = R.formatEntryCountdown;

    function visibleTaskAccounts(rows) {
      return R.visibleTaskAccounts(rows, taskFilter);
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

    function renderEntryProgress() {
      if (!entryProgressBoard) return;
      if (!detailsHydratedOnce) {
        nextEntryCheckRaw = "";
        syncCountdownTimer();
        var hasErr = !!(overviewErrors.details || (!summaryHydratedOnce && overviewErrors.summary));
        ops.patchHtmlIfChanged(
          entryProgressBoard,
          hasErr
            ? '<div class="entry-progress-empty is-error">开单进度加载失败，请点击上方“立即重试”</div>'
            : '<div class="entry-progress-empty">正在同步今日开单进度...</div>'
        );
        if (entryProgressUpdated) entryProgressUpdated.textContent = "更新 --";
        if (entryProgressToggle) entryProgressToggle.hidden = true;
        return;
      }
      var rows = summaryRows || [];
      var accountRows = [];
      for (var i = 0; i < rows.length; i += 1) {
        var row = rows[i] || {};
        if (String(row.mode || "full").toLowerCase() === "full") accountRows.push(row);
      }
      if (!accountRows.length) {
        nextEntryCheckRaw = "";
        syncCountdownTimer();
        ops.patchHtmlIfChanged(entryProgressBoard, '<div class="entry-progress-empty">暂无完整策略账号</div>');
        if (entryProgressUpdated) entryProgressUpdated.textContent = "更新 --";
        if (entryProgressToggle) entryProgressToggle.hidden = true;
        return;
      }

      var view = R.buildEntryProgressHtml(accountRows, {
        entryDetailsUserOverride: entryDetailsUserOverride
      });
      nextEntryCheckRaw = view.nextCheckRaw;
      syncCountdownTimer();

      if (entryProgressToggle) {
        entryProgressToggle.hidden = false;
        entryProgressToggle.textContent = view.showDetails ? "收起账号明细" : "查看账号明细";
        entryProgressToggle.setAttribute("aria-expanded", view.showDetails ? "true" : "false");
      }

      var prevTrack = entryProgressBoard.querySelector(".entry-progress-timeline-track");
      var prevScrollLeft = prevTrack ? prevTrack.scrollLeft : 0;
      ops.patchHtmlIfChanged(entryProgressBoard, view.html);
      var nextTrack = entryProgressBoard.querySelector(".entry-progress-timeline-track");
      if (nextTrack) {
        if (prevScrollLeft > 0) nextTrack.scrollLeft = prevScrollLeft;
        ops.bindScrollOverflowFade(nextTrack, nextTrack.parentElement);
      }
      updateEntryProgressCountdown();
      if (entryProgressUpdated) entryProgressUpdated.textContent = "更新 " + (view.latest || "--");
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
      return R.chartGeometry(metrics, width, height, includeStop, stopPct, portfolioStopPct);
    }

    function renderPortfolioStopState(aid, stop, metrics) {
      var meterLabel = accountElement("stop-meter-label", aid);
      var meterTrack = accountElement("stop-meter-track", aid);
      var state = ops.evaluatePortfolioStopState(stop, metrics, {
        enabled: portfolioStopEnabled,
        fallbackPct: portfolioStopPct
      });

      if (metrics) {
        var ddTone = ops.classifyDrawdownTone(metrics.currentDrawdownPct, state.configuredLossPct);
        setAccountMetric("current-drawdown", aid, fmt(metrics.currentDrawdownPct, 2) + "%", ddTone ? "status-" + ddTone : "");
      } else {
        setAccountMetric("current-drawdown", aid, "--", "");
      }

      var riskCls = "status-" + state.tone;
      setAccountMetric("stop-distance", aid, state.distanceText, riskCls);
      setAccountMetric("risk-state", aid, state.stateText, riskCls);
      if (meterLabel) {
        meterLabel.textContent = state.meterText;
        meterLabel.className = "stop-meter-label " + riskCls;
      }
      if (meterTrack) {
        meterTrack.value = Math.round(state.consumedPct);
        meterTrack.className = "ops-progress stop-meter-track " + riskCls;
        meterTrack.setAttribute("aria-valuetext", state.stateText + "，" + state.meterText);
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
      var stopSig = stop
        ? [stop.status, stop.baseline_equity, stop.loss_pct, stop.current_return_pct, stop.current_equity, stop.distance_pct, stop.updated_at_utc].join(":")
        : "";
      var inputSig = [mode, portfolioStopEnabled ? 1 : 0, errMsg || "", cached ? cached.ts : 0, curveInFlight[aid] ? 1 : 0, stopSig].join("|");
      if (curveInputSig[aid] === inputSig && box.querySelector(".spark-svg, .spark-empty")) {
        return;
      }
      curveInputSig[aid] = inputSig;

      if (errMsg && !cached) {
        var errSig = "err:" + errMsg;
        if (curveRenderSig[aid] !== errSig) {
          curveRenderSig[aid] = errSig;
          box.className = "spark-box";
          box.removeAttribute("data-active-index");
          box.innerHTML = '<div class="spark-empty is-error"><span>曲线加载失败</span><button class="ops-btn" type="button" data-retry-curve="' + escapeHtml(aid) + '">重试</button></div>';
        }
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
        var emptySig = "empty:" + (curveInFlight[aid] ? "1" : "0");
        if (curveRenderSig[aid] !== emptySig) {
          curveRenderSig[aid] = emptySig;
          box.className = "spark-box";
          box.removeAttribute("data-active-index");
          box.innerHTML = '<div class="spark-empty">' + (curveInFlight[aid] ? "加载中..." : "暂无曲线") + "</div>";
        }
        if (mode !== "readonly" && backendReturn !== null) {
          var effectiveLossPct = (stop && ops.toNum(stop.loss_pct)) || portfolioStopPct;
          var tone = ops.classifyReturnTone(backendReturn, effectiveLossPct, stop && stop.status);
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
      var tone2 = ops.classifyReturnTone(changePct, mode === "readonly" ? null : metrics.configuredLossPct, stop && stop.status);
      var deltaCls = tone2 === "ok" ? "spark-up" : (tone2 === "bad" ? "spark-down" : "spark-flat");
      var signedReturn = (changePct >= 0 ? "+" : "") + fmt(changePct, 2) + "%";

      if (metrics.realPointCount < 2) {
        var sparseSig = "sparse:" + deltaCls + "|" + signedReturn;
        if (curveRenderSig[aid] !== sparseSig) {
          curveRenderSig[aid] = sparseSig;
          delete curveRenderData[aid];
          box.className = "spark-box " + deltaCls;
          box.removeAttribute("tabindex");
          box.removeAttribute("role");
          box.removeAttribute("aria-label");
          box.removeAttribute("data-active-index");
          box.innerHTML = '<div class="spark-empty">当前窗口暂无日内曲线轨迹</div>';
        }
        setAccountMetric("spark-delta", aid, signedReturn, deltaCls);
        if (mode !== "readonly") renderPortfolioStopState(aid, stop, metrics);
        return;
      }

      var width = mode === "readonly" ? 300 : 320;
      var height = mode === "readonly" ? 50 : 98;
      var includeStop = mode !== "readonly" && portfolioStopEnabled;
      var geometry = chartGeometry(metrics, width, height, includeStop, metrics.configuredLossPct);
      curveRenderData[aid] = geometry.renderedPoints;

      var nextSig = deltaCls + "|" + signedReturn + "|" + geometry.points + "|" + (includeStop ? geometry.stopY.toFixed(1) : "0");
      if (curveRenderSig[aid] !== nextSig) {
        curveRenderSig[aid] = nextSig;
        var svgLabel = aid + " 本账户周期权益曲线，当前变化 " + signedReturn + "，回撤 " + fmt(metrics.currentDrawdownPct, 2) + "%";
        var lastPt = geometry.renderedPoints[geometry.renderedPoints.length - 1];
        var endDotHtml = lastPt
          ? '<span class="spark-end-dot" aria-hidden="true" style="left:' + lastPt.xPct.toFixed(2) + "%;top:" + lastPt.yPct.toFixed(2) + '%"></span>'
          : "";

        box.className = "spark-box " + deltaCls;
        box.setAttribute("tabindex", "0");
        box.setAttribute("role", "group");
        box.setAttribute("aria-label", svgLabel + "，使用左右方向键查看时间点");
        box.innerHTML = ''
          + '<svg class="spark-svg" viewBox="0 0 ' + width + " " + height + '" preserveAspectRatio="none" aria-hidden="true">'
          + "<title>" + escapeHtml(svgLabel) + "</title>"
          + '<line class="spark-zero-line" x1="0" x2="' + width + '" y1="' + geometry.zeroY.toFixed(2) + '" y2="' + geometry.zeroY.toFixed(2) + '"></line>'
          + (includeStop ? '<line class="spark-stop-line" x1="0" x2="' + width + '" y1="' + geometry.stopY.toFixed(2) + '" y2="' + geometry.stopY.toFixed(2) + '"></line>' : "")
          + '<polygon class="spark-area" points="' + geometry.areaPoints + '"></polygon>'
          + '<polyline class="spark-path" points="' + geometry.points + '"></polyline>'
          + "</svg>"
          + endDotHtml
          + '<span class="spark-crosshair" aria-hidden="true" hidden></span>'
          + '<span class="spark-active-dot" aria-hidden="true" hidden></span>'
          + '<div class="spark-hover-tip" aria-hidden="true" hidden></div>';
      }

      setAccountMetric("spark-delta", aid, signedReturn, deltaCls);
      if (mode === "readonly") return;
      renderPortfolioStopState(aid, stop, metrics);
    }

    function storeCurvePayload(aid, payload) {
      if (!aid || !payload || typeof payload !== "object") return;
      var curve = payload.balance_curve || payload.equity_curve || [];
      var points = [];
      for (var i = 0; i < curve.length; i += 1) {
        var p = curve[i] || {};
        var n = Number(p.equity);
        if (Number.isFinite(n)) points.push({ t: p.t || "", equity: n, carried_forward: !!p.carried_forward });
      }
      if (payload.portfolio_loss_cut) mergePortfolioStopState(aid, payload.portfolio_loss_cut);
      delete curveInputSig[aid];
      curveCache[aid] = {
        points: points.slice(-160),
        walletAsOfUtc: (payload.wallet && payload.wallet.as_of_utc) || "",
        cycleBaseline: payload.balance_curve_cycle_baseline || null,
        portfolioLossCut: payload.portfolio_loss_cut || portfolioStopStates[aid] || null,
        ts: Date.now()
      };
      curveErrors[aid] = "";
    }

    var curveAbortControllers = {};
    var batchCurvesAbortController = null;
    var batchCurvesInFlight = false;
    var forceCurveRefreshPending = false;

    function fetchCurvesBatch(accountIds, force) {
      var ids = (accountIds || []).filter(Boolean);
      if (!ids.length) return;
      if (batchCurvesInFlight && !force) return;
      var now = Date.now();
      for (var i = 0; i < ids.length; i += 1) {
        var bid = ids[i];
        if (curveAbortControllers[bid]) {
          try { curveAbortControllers[bid].abort(); } catch (e) {}
          delete curveAbortControllers[bid];
        }
        curveInFlight[bid] = true;
        curveErrors[bid] = "";
      }
      batchCurvesInFlight = true;
      if (batchCurvesAbortController) {
        try { batchCurvesAbortController.abort(); } catch (e) {}
      }
      var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      batchCurvesAbortController = controller;
      var url = curvesBatchApi
        + "?window_hours=24&curve_points=160&accounts=" + encodeURIComponent(ids.join(","));
      ops.fetchJsonWithTimeout(url, {
        signal: controller ? controller.signal : null,
        timeoutMs: 15000
      })
        .then(function (data) {
          if (batchCurvesAbortController !== controller) return;
          batchCurvesAbortController = null;
          batchCurvesInFlight = false;
          var curvesMap = (data && data.curves) || {};
          for (var j = 0; j < ids.length; j += 1) {
            var aid = ids[j];
            if (!curveAbortControllers[aid]) curveInFlight[aid] = false;
            var item = Object.prototype.hasOwnProperty.call(curvesMap, aid) ? curvesMap[aid] : null;
            if (item && item.db_error) {
              curveErrors[aid] = "数据库读取失败：" + item.db_error;
              delete curveInputSig[aid];
              renderCurve(aid);
            } else if (item) {
              storeCurvePayload(aid, item);
              renderCurve(aid);
            } else {
              fetchCurve(aid, true);
            }
          }
          scheduleCommandBarRender();
        })
        .catch(function (err) {
          if (batchCurvesAbortController !== controller) return;
          batchCurvesAbortController = null;
          batchCurvesInFlight = false;
          if (err && err.name === "AbortError") {
            for (var a = 0; a < ids.length; a += 1) {
              if (!curveAbortControllers[ids[a]]) curveInFlight[ids[a]] = false;
            }
            return;
          }
          var batchErrMsg = (err && err.message) || "网络异常";
          for (var k = 0; k < ids.length; k += 1) {
            var failAid = ids[k];
            if (!curveAbortControllers[failAid]) curveInFlight[failAid] = false;
            curveErrors[failAid] = batchErrMsg;
            renderCurve(failAid);
          }
        });
    }

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
      delete curveInputSig[aid];
      var url = pathPrefix + "/api/account/" + encodeURIComponent(aid) + "/curve?window_hours=24&curve_points=160&curve_type=balance";
      if (curveAbortControllers[aid]) {
        try { curveAbortControllers[aid].abort(); } catch (e) {}
      }
      var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      curveAbortControllers[aid] = controller;
      ops.fetchJsonWithTimeout(url, {
        signal: controller ? controller.signal : null,
        timeoutMs: 15000
      })
        .then(function (payload) {
          if (curveAbortControllers[aid] !== controller) return;
          delete curveAbortControllers[aid];
          curveInFlight[aid] = false;
          storeCurvePayload(aid, payload);
          renderCurve(aid);
          scheduleCommandBarRender();
        })
        .catch(function (err) {
          if (curveAbortControllers[aid] !== controller) return;
          delete curveAbortControllers[aid];
          curveInFlight[aid] = false;
          if (err && err.name === "AbortError") return;
          curveErrors[aid] = (err && err.message) || "网络异常";
          renderCurve(aid);
        });
    }

    function primeCurveLoad() {
      var forceAll = forceCurveRefreshPending;
      forceCurveRefreshPending = false;
      var nodes = document.querySelectorAll(".spark-box");
      var staleIds = [];
      for (var i = 0; i < nodes.length; i += 1) {
        var node = nodes[i];
        var aid = node.getAttribute("data-account-id") || "";
        renderCurve(aid);
        var cached = curveCache[aid];
        var fresh = !!(cached && cached.ts && (Date.now() - cached.ts) < curveTtlMs);
        if (!aid || (!forceAll && fresh) || (!forceAll && curveInFlight[aid])) continue;
        if (staleIds.indexOf(aid) < 0) staleIds.push(aid);
      }
      if (staleIds.length > 1) {
        fetchCurvesBatch(staleIds, forceAll);
      } else if (staleIds.length === 1) {
        if (forceAll) {
          fetchCurve(staleIds[0], true);
          return;
        }
        createObserver();
        if (curveObserver && nodes.length) {
          for (var j = 0; j < nodes.length; j += 1) {
            if (nodes[j].getAttribute("data-account-id") === staleIds[0]) {
              curveObserver.observe(nodes[j]);
              break;
            }
          }
        } else {
          fetchCurve(staleIds[0]);
        }
      }
    }

    var managedStrategyNotes = R.managedStrategyNotes;

    function computeVenueState(aid, row) {
      return R.computeVenueState(isAccountStopped(aid, row), row);
    }

    function patchExistingCards(rows) {
      var allNotes = managedStrategyNotes(rows);
      var allNotesSig = allNotes.join("\n");
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
          var netPnl = stats.net_realized_pnl != null ? stats.net_realized_pnl : stats.total_realized_pnl;
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
          var aidLink = card.querySelector(".aid-link");
          if (aidLink) {
            aidLink.className = "aid-link " + venueInfo.cls;
            aidLink.setAttribute("title", aid + " · " + venueInfo.label);
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
          var highlightsKey = noteKey + "|" + allNotesSig;
          if (highlightsEl && highlightsEl.getAttribute("data-note") !== highlightsKey) {
            var nextHighlights = ops.formatStrategyHighlights(r.strategy_note, allNotes, 2, "strategy-tag");
            highlightsEl.innerHTML = nextHighlights;
            highlightsEl.hidden = !nextHighlights;
            highlightsEl.setAttribute("data-note", highlightsKey);
          }
        }
      }
      return true;
    }

    function buildReadonlyStripHtml(row, safeAid, safeBase) {
      var stats = row.trade_stats || {};
      var netPnl = ops.toNum(stats.net_realized_pnl != null ? stats.net_realized_pnl : stats.total_realized_pnl);
      var pnlText = netPnl !== null ? ops.fmtSignedUsdt(netPnl, 2) : "--";
      var winRateText = stats.win_rate_pct != null ? escapeHtml(fmt(stats.win_rate_pct, 1)) + "%" : "--";
      var tradeCountText = stats.total_trades != null ? escapeHtml(fmt(stats.total_trades, 0)) : "--";
      var profitFactorText = stats.profit_factor != null ? escapeHtml(fmt(stats.profit_factor, 2)) : "--";
      var pnlCls = netPnl !== null ? (netPnl >= 0 ? "status-ok" : "status-bad") : "";
      return '<article class="readonly-strip" id="account-card-' + safeAid + '" data-account-id="' + safeAid + '" aria-labelledby="card-title-' + safeAid + '">'
        + '<div class="readonly-identity"><h3 class="card-title-heading" id="card-title-' + safeAid + '"><a class="readonly-id-link" href="' + safeBase + '">' + safeAid + '<span class="readonly-badge">只读监控</span></a></h3></div>'
        + '<div class="readonly-metric readonly-balance"><span class="metric-label">余额 (USDT)</span><strong class="metric-value" data-field="wallet-balance">' + ops.fmtUsdt(row.wallet_balance_usdt, 2) + "</strong></div>"
        + '<div class="readonly-metric"><span class="metric-label">近30日净盈亏</span><strong class="metric-value pnl-value ' + pnlCls + '" data-field="readonly-pnl">' + pnlText + "</strong></div>"
        + '<div class="readonly-metric"><span class="metric-label">胜率</span><strong class="metric-value" data-field="readonly-win-rate">' + winRateText + "</strong></div>"
        + '<div class="readonly-metric readonly-secondary"><span class="metric-label">平仓订单数</span><strong class="metric-value" data-field="readonly-trades">' + tradeCountText + "</strong></div>"
        + '<div class="readonly-metric readonly-secondary"><span class="metric-label"><button type="button" class="ops-term" data-tip="平均单笔盈利金额 / 平均单笔亏损金额">盈亏比</button></span><strong class="metric-value" data-field="readonly-pf">' + profitFactorText + "</strong></div>"
        + '<div class="readonly-chart"><div>'
        + '<div class="spark-box" data-account-id="' + safeAid + '"><div class="spark-empty">加载中...</div></div>'
        + '</div><div class="readonly-chart-meta"><span class="metric-label" title="按账户周期首条权益快照计算，含浮盈亏">本周期变化</span><strong class="metric-value spark-delta" data-account-id="' + safeAid + '">--</strong></div></div>'
        + '<div class="readonly-action"><a class="detail-link" href="' + safeBase + '" aria-label="查看 ' + safeAid + ' 余额曲线与交易统计">详情 ' + ops.ARROW_ICON + "</a></div>"
        + "</article>";
    }

    function buildManagedCardHtml(row, rowAid, safeAid, safeBase, idx, allManagedNotes, allNotesSig) {
      var venueInfo = computeVenueState(rowAid, row);
      var popoverId = "strategy-popover-" + idx;
      var noteRaw = String(row.strategy_note || "");
      var noteAttr = escapeHtml(noteRaw);
      var highlightsAttr = escapeHtml(noteRaw + "|" + allNotesSig);
      var highlightsHtml = ops.formatStrategyHighlights(row.strategy_note, allManagedNotes, 2, "strategy-tag");
      return '<article class="account-card" id="account-card-' + safeAid + '" data-account-id="' + safeAid + '" aria-labelledby="card-title-' + safeAid + '">'
        + '<header class="account-card-head">'
        + '<h3 class="card-title-heading" id="card-title-' + safeAid + '"><a class="aid-link ' + venueInfo.cls + '" href="' + safeBase + '" title="' + safeAid + ' · ' + escapeHtml(venueInfo.label) + '"><span class="venue-dot" aria-hidden="true"></span>' + safeAid + '</a></h3>'
        + '<div class="account-head-actions">'
        + '<div class="strategy-popover-wrap"><button class="strategy-trigger" type="button" aria-expanded="false" aria-controls="' + popoverId + '" aria-label="' + safeAid + ' 策略配置">策略</button>'
        + '<div id="' + popoverId + '" class="strategy-popover" hidden role="region" aria-label="' + safeAid + ' 策略配置">'
        + '<div class="strategy-popover-head"><div class="strategy-popover-meta"><strong class="strategy-popover-title">策略配置</strong><span class="strategy-popover-subtitle">' + safeAid + '</span></div><button class="strategy-popover-close" type="button" aria-label="关闭策略说明">×</button></div>'
        + '<div class="strategy-popover-tags" data-note="' + noteAttr + '">' + ops.formatStrategyTags(row.strategy_note, "strategy-tag") + "</div></div></div>"
        + '<a class="detail-link" href="' + safeBase + '" aria-label="查看 ' + safeAid + ' 账户详情">详情 ' + ops.ARROW_ICON + "</a></div></header>"
        + '<div class="account-card-body">'
        + '<div class="card-strategy-highlights" data-field="strategy-highlights" data-note="' + highlightsAttr + '"' + (highlightsHtml ? "" : " hidden") + '>' + highlightsHtml + "</div>"
        + '<div class="account-primary">'
        + '<div class="metric"><span class="metric-label">策略权益 (USDT)</span><strong class="metric-value" data-field="wallet-balance">' + ops.fmtUsdt(row.wallet_balance_usdt, 2) + "</strong></div>"
        + '<div class="metric"><span class="metric-label">' + String(portfolioStopHour).padStart(2, "0") + ":" + String(portfolioStopMinute).padStart(2, "0") + ' 以来收益</span><strong class="metric-value return-value spark-delta" data-account-id="' + safeAid + '">--</strong></div>'
        + "</div>"
        + '<div class="account-secondary">'
        + '<div class="metric"><span class="metric-label">当前持仓</span><strong class="metric-value" data-field="open-positions">' + escapeHtml(fmt(row.open_positions, 0)) + "</strong></div>"
        + '<div class="metric"><span class="metric-label"><button type="button" class="ops-term" data-tip="当前权益相对本账户周期最高点的回落幅度">高点回撤</button></span><strong class="metric-value current-drawdown" data-account-id="' + safeAid + '">--</strong></div>'
        + '<div class="metric"><span class="metric-label"><button type="button" class="ops-term" data-tip="当前周期收益距离触发全账户组合止损还剩的百分点">距组合止损</button></span><strong class="metric-value stop-distance" data-account-id="' + safeAid + '">--</strong></div>'
        + "</div>"
        + '<div class="spark-block">'
        + '<div class="spark-box" data-account-id="' + safeAid + '"><div class="spark-empty">加载中...</div></div>'
        + '<div class="stop-meter"><strong class="risk-state" data-account-id="' + safeAid + '">计算中</strong><progress class="ops-progress stop-meter-track" data-account-id="' + safeAid + '" max="100" value="0" aria-label="' + safeAid + ' 组合止损预算消耗"></progress><span class="stop-meter-label" data-account-id="' + safeAid + '">--</span></div>'
        + "</div></div>"
        + "</article>";
    }

    function renderCards() {
      var rows = summaryRows || [];
      var managedHtml = "";
      var readonlyHtml = "";
      var sigParts = [];
      accountModes = {};
      for (var i = 0; i < rows.length; i += 1) {
        var r = rows[i] || {};
        var mode = String(r.mode || "full").toLowerCase();
        if (mode === "loss_cut_only") continue;
        var aid = String(r.account_id || "");
        accountModes[aid] = mode;
        mergePortfolioStopState(aid, r.portfolio_loss_cut || null);
        sigParts.push(aid + ":" + mode);
      }
      var nextSig = sigParts.join("|");
      if (nextSig && cards.getAttribute("data-card-sig") === nextSig && patchExistingCards(rows)) {
        primeCurveLoad();
        return;
      }

      if (hoverState.rafId && window.cancelAnimationFrame) {
        window.cancelAnimationFrame(hoverState.rafId);
      }
      hoverState.rafId = 0;
      hoverState.box = null;
      hoverState.rect = null;
      if (curveObserver) curveObserver.disconnect();
      curveRenderSig = {};
      curveInputSig = {};
      var allManagedNotes = managedStrategyNotes(rows);
      var allNotesSig = allManagedNotes.join("\n");

      for (var j = 0; j < rows.length; j += 1) {
        var row = rows[j] || {};
        var rowMode = String(row.mode || "full").toLowerCase();
        if (rowMode === "loss_cut_only") continue;
        var rowAid = String(row.account_id || "");
        var safeAid = escapeHtml(rowAid);
        var safeBase = escapeHtml(pathPrefix + "/account/" + encodeURIComponent(rowAid) + "/");
        if (rowMode === "readonly") {
          readonlyHtml += buildReadonlyStripHtml(row, safeAid, safeBase);
        } else {
          managedHtml += buildManagedCardHtml(row, rowAid, safeAid, safeBase, j, allManagedNotes, allNotesSig);
        }
      }
      var html = managedHtml ? '<div class="managed-grid">' + managedHtml + "</div>" : "";
      html += readonlyHtml;
      cards.setAttribute("data-card-sig", nextSig);
      var fallbackHtml = (!summaryHydratedOnce && overviewErrors.summary)
        ? '<div class="task-empty is-error"><strong>账户概览加载失败</strong><span>请点击上方“立即重试”重新同步</span></div>'
        : '<div class="task-empty"><strong>暂无账户数据</strong><span>等待账户快照写入</span></div>';
      cards.innerHTML = html || fallbackHtml;
      primeCurveLoad();
    }

    function renderTaskBoardHeader(rows) {
      if (!taskUpdatedAt) return;
      var latest = latestTaskTime(rows || []);
      taskUpdatedAt.textContent = "更新 " + (latest || "--");
      var filterNodes = ["all", "anomaly", "symbols"].map(function (key) {
        return document.getElementById("task-filter-" + key);
      });
      ops.syncPressedButtons(filterNodes, function (btn) {
        return btn.id === "task-filter-" + taskFilter;
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
      if (!detailsHydratedOnce) {
        var hasErr = !!(overviewErrors.details || (!summaryHydratedOnce && overviewErrors.summary));
        ops.reconcileKeyedHtml(taskBoard, [], {
          keyAttr: "data-account-card",
          emptyHtml: hasErr
            ? '<div class="task-empty is-error"><strong>任务执行记录加载失败</strong><span>请点击上方“立即重试”或稍后刷新</span></div>'
            : '<div class="task-empty"><strong>正在同步任务执行记录...</strong><span>等待后端返回最新巡检详情</span></div>'
        });
        return;
      }
      var items = rows.map(function (row) {
        return {
          key: String((row && row.account_id) || ""),
          html: R.buildTaskAccountCardHtml(row)
        };
      });
      ops.reconcileKeyedHtml(taskBoard, items, {
        keyAttr: "data-account-card",
        detailAttr: "data-task-detail-key",
        emptyHtml: R.buildTaskEmptyHtml(taskFilter)
      });
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
        if (detailsRetryAttempts >= 6) {
          setOverviewError("details", "任务与开单进度正在后台预热，请稍后手动刷新");
          return;
        }
        var delayMs = Math.min(16000, 1000 * Math.pow(2, detailsRetryAttempts));
        detailsRetryAttempts += 1;
        if (!detailsRetryTimer) {
          detailsRetryTimer = window.setTimeout(function () {
            detailsRetryTimer = null;
            fetchSummaryDetails();
          }, delayMs);
        }
        return;
      }
      detailsRetryAttempts = 0;
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
      scheduleCommandBarRender();
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
      var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      summaryDetailsAbortController = controller;
      ops.fetchJsonWithTimeout(summaryDetailsApi, {
        signal: controller ? controller.signal : null,
        timeoutMs: 15000
      })
        .then(function (payload) {
          if (summaryDetailsAbortController !== controller) return;
          summaryDetailsAbortController = null;
          summaryDetailsRequestInFlight = false;
          if (payload && payload.ready === true) detailsHydratedOnce = true;
          setOverviewError("details", "");
          mergeSummaryDetails(payload);
        })
        .catch(function (err) {
          if (summaryDetailsAbortController !== controller) return;
          summaryDetailsAbortController = null;
          summaryDetailsRequestInFlight = false;
          if (err && err.name === "AbortError") return;
          setOverviewError("details", "任务与开单进度读取失败：" + (err && err.message ? err.message : "网络异常"));
          if (!detailsHydratedOnce) {
            renderEntryProgress();
            renderTaskBoard();
          }
        });
    }

    function setOverviewRefreshBusy(busy) {
      ops.setButtonBusy(overviewRefreshBtn, busy, "刷新", "刷新中...");
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
      if (force) setOverviewRefreshBusy(true);
      var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      summaryAbortController = controller;
      ops.fetchJsonWithTimeout(summaryFastApi, {
        signal: controller ? controller.signal : null,
        timeoutMs: 15000
      })
        .then(function (payload) {
          if (summaryAbortController !== controller) return;
          summaryAbortController = null;
          summaryRequestInFlight = false;
          setOverviewRefreshBusy(false);
          summaryHydratedOnce = true;
          summaryGeneratedAtUtc = (payload && payload.generated_at_utc) || "";
          setOverviewError("summary", "");
          summaryRows = mergeFastRows(payload.accounts || []);
          ops.renderNavAccounts(summaryRows);
          scheduleCommandBarRender();
          renderCards();
          renderEntryProgress();
          renderTaskBoard();
          if (force || !detailsHydratedOnce || (Date.now() - lastDetailsFetchAt) > Math.max(5000, refreshSec * 800)) {
            fetchSummaryDetails({ force: force });
          }
        })
        .catch(function (err) {
          if (summaryAbortController !== controller) return;
          summaryAbortController = null;
          summaryRequestInFlight = false;
          setOverviewRefreshBusy(false);
          if (err && err.name === "AbortError") return;
          setOverviewError("summary", "账户总览刷新失败：" + (err && err.message ? err.message : "网络异常") + "，页面可能不是最新数据");
          if (!summaryHydratedOnce) {
            renderCards();
            renderEntryProgress();
            renderTaskBoard();
          } else if (forceCurveRefreshPending) {
            primeCurveLoad();
          }
        });
    }

    function triggerOverviewRefresh() {
      if (overviewRefreshBtn && overviewRefreshBtn.disabled) return;
      lastDetailsFetchAt = 0;
      detailsRetryAttempts = 0;
      forceCurveRefreshPending = true;
      curveErrors = {};
      batchCurvesInFlight = false;
      if (batchCurvesAbortController) {
        try { batchCurvesAbortController.abort(); } catch (e) {}
        batchCurvesAbortController = null;
      }
      Object.keys(curveAbortControllers).forEach(function (aid) {
        try { curveAbortControllers[aid].abort(); } catch (e) {}
        delete curveAbortControllers[aid];
        curveInFlight[aid] = false;
      });
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

    function clampPopoverToViewport(pop) {
      if (!pop) return;
      pop.style.setProperty("--popover-shift-x", "0px");
      var rect = pop.getBoundingClientRect();
      var margin = 12;
      var shift = 0;
      if (rect.left < margin) {
        shift = margin - rect.left;
      } else if (rect.right > window.innerWidth - margin) {
        shift = (window.innerWidth - margin) - rect.right;
      }
      if (shift !== 0) {
        pop.style.setProperty("--popover-shift-x", Math.round(shift) + "px");
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
        if (pop) {
          pop.hidden = true;
          pop.style.setProperty("--popover-shift-x", "0px");
        }
      }
      if (restoreFocus && focusTarget && focusTarget.focus) {
        focusTarget.focus();
      }
    }

    var hoverState = { box: null, rect: null, rafId: 0, clientX: 0 };

    function getSparkRefs(box) {
      if (!box.__bbSparkRefs || !box.__bbSparkRefs.tip || !box.contains(box.__bbSparkRefs.tip)) {
        box.__bbSparkRefs = {
          tip: box.querySelector(".spark-hover-tip"),
          crosshair: box.querySelector(".spark-crosshair"),
          activeDot: box.querySelector(".spark-active-dot")
        };
      }
      return box.__bbSparkRefs;
    }

    function showSparkPoint(box, pts, index, announce) {
      if (!box || !pts || !pts.length) return;
      var clampedIdx = Math.max(0, Math.min(pts.length - 1, index));
      var prevIdx = box.getAttribute("data-active-index");
      if (!announce && prevIdx !== null && prevIdx !== "" && Number(prevIdx) === clampedIdx) {
        return;
      }
      var nearest = pts[clampedIdx];
      var refs = getSparkRefs(box);
      var tip = refs.tip;
      var crosshair = refs.crosshair;
      var activeDot = refs.activeDot;
      if (!tip || !nearest) return;
      var timePart = nearest.t ? ops.formatBeijingTime(nearest.t) + " · " : "";
      var eqPart = nearest.equity != null ? ops.fmtUsdt(nearest.equity, 2) + " · " : "";
      var text = timePart + eqPart + ops.fmtPct(nearest.val);
      tip.textContent = text;
      tip.classList.toggle("is-right", nearest.xPct < 48);
      tip.hidden = false;
      if (box.closest && box.closest(".readonly-chart")) {
        var boxRect = (hoverState.box === box && hoverState.rect) ? hoverState.rect : box.getBoundingClientRect();
        var tipRect = tip.getBoundingClientRect();
        var margin = 8;
        var vw = window.innerWidth || document.documentElement.clientWidth || 1024;
        var anchorX = boxRect.left + (boxRect.width * nearest.xPct) / 100;
        var left = Math.max(margin, Math.min(vw - margin - tipRect.width, anchorX - tipRect.width / 2));
        var top = Math.max(margin, boxRect.top - tipRect.height - 6);
        tip.style.left = Math.round(left) + "px";
        tip.style.top = Math.round(top) + "px";
      } else {
        tip.style.left = "";
        tip.style.top = "";
      }
      if (crosshair) {
        crosshair.style.left = nearest.xPct.toFixed(2) + "%";
        crosshair.hidden = false;
      }
      if (activeDot) {
        activeDot.style.left = nearest.xPct.toFixed(2) + "%";
        activeDot.style.top = nearest.yPct.toFixed(2) + "%";
        activeDot.hidden = false;
      }
      box.setAttribute("data-active-index", String(clampedIdx));
      if (announce) {
        var aid = box.getAttribute("data-account-id") || "";
        ops.announceStatus(aid + " " + text + "（第 " + (clampedIdx + 1) + "/" + pts.length + " 点）");
      }
    }

    function renderSparkHover() {
      hoverState.rafId = 0;
      var box = hoverState.box;
      if (!box) return;
      var aid = box.getAttribute("data-account-id");
      var pts = aid ? curveRenderData[aid] : null;
      if (!pts || !pts.length) return;
      var rect = hoverState.rect || box.getBoundingClientRect();
      if (!rect.width) return;
      var ratio = Math.max(0, Math.min(1, (hoverState.clientX - rect.left) / rect.width));
      var bestIdx = 0;
      var bestDist = Infinity;
      for (var i = 0; i < pts.length; i += 1) {
        var d = Math.abs(pts[i].ratio - ratio);
        if (d < bestDist) { bestDist = d; bestIdx = i; }
      }
      showSparkPoint(box, pts, bestIdx, false);
    }

    function clearSparkHover(box) {
      if (!box) return;
      var refs = getSparkRefs(box);
      if (refs.tip) refs.tip.hidden = true;
      if (refs.crosshair) refs.crosshair.hidden = true;
      if (refs.activeDot) refs.activeDot.hidden = true;
      box.removeAttribute("data-active-index");
      if (hoverState.box === box) {
        if (hoverState.rafId && window.cancelAnimationFrame) {
          window.cancelAnimationFrame(hoverState.rafId);
        }
        hoverState.rafId = 0;
        hoverState.box = null;
        hoverState.rect = null;
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
        if (pop) {
          pop.hidden = !shouldOpen;
          if (shouldOpen) clampPopoverToViewport(pop);
        }
        trigger.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
      });

      cards.addEventListener("pointerdown", function (event) {
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (!box) return;
        if (hoverState.box && hoverState.box !== box) clearSparkHover(hoverState.box);
        hoverState.box = box;
        hoverState.rect = box.getBoundingClientRect();
        hoverState.clientX = event.clientX;
        renderSparkHover();
      });

      cards.addEventListener("pointermove", function (event) {
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (!box) return;
        if (hoverState.box !== box) {
          if (hoverState.box) clearSparkHover(hoverState.box);
          hoverState.box = box;
          hoverState.rect = box.getBoundingClientRect();
        }
        hoverState.clientX = event.clientX;
        if (!hoverState.rafId) {
          hoverState.rafId = (window.requestAnimationFrame || function (fn) { return setTimeout(fn, 16); })(renderSparkHover);
        }
      });

      cards.addEventListener("pointerout", function (event) {
        if (event.pointerType === "touch") return;
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (!box) return;
        if (event.relatedTarget && box.contains(event.relatedTarget)) return;
        clearSparkHover(box);
      });

      cards.addEventListener("pointercancel", function (event) {
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (box) clearSparkHover(box);
      });

      cards.addEventListener("keydown", function (event) {
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (!box) return;
        var aid = box.getAttribute("data-account-id");
        var pts = aid ? curveRenderData[aid] : null;
        if (!pts || !pts.length) return;
        var currentRaw = box.getAttribute("data-active-index");
        var currentIdx = currentRaw !== null && currentRaw !== "" ? Number(currentRaw) : (pts.length - 1);
        if (event.key === "ArrowLeft") {
          event.preventDefault();
          showSparkPoint(box, pts, currentIdx - 1, true);
        } else if (event.key === "ArrowRight") {
          event.preventDefault();
          showSparkPoint(box, pts, currentIdx + 1, true);
        } else if (event.key === "Home") {
          event.preventDefault();
          showSparkPoint(box, pts, 0, true);
        } else if (event.key === "End") {
          event.preventDefault();
          showSparkPoint(box, pts, pts.length - 1, true);
        } else if (event.key === "Escape") {
          clearSparkHover(box);
        }
      });

      cards.addEventListener("focusout", function (event) {
        var box = event.target && event.target.closest ? event.target.closest(".spark-box") : null;
        if (box) clearSparkHover(box);
      });
    }

    window.addEventListener("resize", ops.rafThrottle(function () {
      hoverState.rect = null;
      var openPop = cards && cards.querySelector(".strategy-popover-wrap.is-open .strategy-popover");
      if (openPop) clampPopoverToViewport(openPop);
    }), { passive: true });

    document.addEventListener("click", function (event) {
      if (hoverState.box && !(event.target && event.target.closest && event.target.closest(".spark-box") === hoverState.box)) {
        clearSparkHover(hoverState.box);
      }
      if (event.target && event.target.closest && event.target.closest(".strategy-popover-wrap")) return;
      closeStrategyPopovers(null, false);
    });

    document.addEventListener("focusin", function (event) {
      if (!cards) return;
      var openWrap = cards.querySelector(".strategy-popover-wrap.is-open");
      if (openWrap && !(event.target && openWrap.contains(event.target))) {
        closeStrategyPopovers(null, false);
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") return;
      var openWrap = cards && cards.querySelector(".strategy-popover-wrap.is-open");
      if (openWrap) {
        var focusInsidePopover = document.activeElement && openWrap.contains(document.activeElement);
        event.preventDefault();
        closeStrategyPopovers(null, focusInsidePopover);
      }
    });

    ops.bindKeyboardShortcuts({ onRefresh: triggerOverviewRefresh });

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
        var nextExpanded = !currentlyExpanded;
        entryDetailsUserOverride = nextExpanded;
        var tableEl = document.getElementById("entry-progress-details-table");
        if (tableEl) {
          tableEl.classList.toggle("is-collapsed", !nextExpanded);
          entryProgressToggle.setAttribute("aria-expanded", nextExpanded ? "true" : "false");
          entryProgressToggle.textContent = nextExpanded ? "收起账号明细" : "查看账号明细";
        } else {
          renderEntryProgress();
        }
      });
    }

    function startPolling() {
      if (!summaryTimer) {
        summaryTimer = setInterval(function () { fetchSummary(); }, refreshSec * 1000);
      }
      if (!healthTimer) {
        healthTimer = setInterval(fetchHealth, Math.max(30, refreshSec * 2) * 1000);
      }
      syncCountdownTimer();
    }

    function stopPolling() {
      if (summaryTimer) { clearInterval(summaryTimer); summaryTimer = null; }
      if (healthTimer) { clearInterval(healthTimer); healthTimer = null; }
      if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }
      if (detailsRetryTimer) { clearTimeout(detailsRetryTimer); detailsRetryTimer = null; }
      if (summaryAbortController) {
        try { summaryAbortController.abort(); } catch (e) {}
        summaryAbortController = null;
      }
      if (summaryDetailsAbortController) {
        try { summaryDetailsAbortController.abort(); } catch (e) {}
        summaryDetailsAbortController = null;
      }
      if (healthAbortController) {
        try { healthAbortController.abort(); } catch (e) {}
        healthAbortController = null;
      }
      if (batchCurvesAbortController) {
        try { batchCurvesAbortController.abort(); } catch (e) {}
        batchCurvesAbortController = null;
      }
      Object.keys(curveAbortControllers).forEach(function (aid) {
        try { if (curveAbortControllers[aid]) curveAbortControllers[aid].abort(); } catch (e) {}
        delete curveAbortControllers[aid];
      });
      summaryRequestInFlight = false;
      summaryDetailsRequestInFlight = false;
      batchCurvesInFlight = false;
      curveInFlight = {};
      setOverviewRefreshBusy(false);
      if (curveObserver) {
        try { curveObserver.disconnect(); } catch (e) {}
        curveObserver = null;
      }
    }

    fetchSummary();
    fetchHealth();
    startPolling();

    document.addEventListener("visibilitychange", function () {
      if (document.hidden) {
        if (summaryTimer) { clearInterval(summaryTimer); summaryTimer = null; }
        if (healthTimer) { clearInterval(healthTimer); healthTimer = null; }
        if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }
      } else {
        startPolling();
        fetchSummary();
        fetchHealth();
        updateEntryProgressCountdown();
      }
    });

    window.addEventListener("online", function () { scheduleCommandBarRender(); });
    window.addEventListener("offline", function () { scheduleCommandBarRender(); });

    window.addEventListener("pagehide", function () {
      stopPolling();
    });

    window.addEventListener("pageshow", function (event) {
      if (event && event.persisted) {
        startPolling();
        fetchSummary();
        fetchHealth();
      }
    });
  });
})();
