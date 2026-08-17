import configparser
import glob
import logging
import os
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from core.runtime_components import build_proxies, create_components, resolve_path
from core.runtime_service import StrategyRuntimeService
from core.state_store import StateStore
from dashboard_server import (
    DashboardDataProvider,
    _strip_curve_payload,
    render_account_dashboard_html,
    render_accounts_overview_html,
)
from infra.binance_futures_client import BinanceFuturesClient
from infra.trade_stats_fetcher import TradeStatsFetcher

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


@dataclass(frozen=True)
class DashboardRuntimeContext:
    config_path: str
    db_path: str
    log_file: str
    timezone_name: str
    entry_hour: int
    entry_minute: int
    portfolio_loss_cut_enabled: bool
    portfolio_loss_cut_pct: float
    portfolio_loss_cut_hour: int
    portfolio_loss_cut_minute: int
    refresh_sec: int
    trade_stats_refresh_sec: int
    echarts_src: str
    provider: DashboardDataProvider


class TradeStatsBackgroundRefresher:
    """Refresh readonly trade statistics outside the dashboard request path."""

    def __init__(
        self,
        fetchers: dict,
        *,
        refresh_interval_sec: int = 300,
        lookback_days: int = 30,
    ):
        self.fetchers = {
            str(account_id).strip(): fetcher
            for account_id, fetcher in (fetchers or {}).items()
            if str(account_id).strip() and fetcher is not None
        }
        self.refresh_interval_sec = max(60, int(refresh_interval_sec))
        self.lookback_days = max(1, int(lookback_days))
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.fetchers or (self._thread is not None and self._thread.is_alive()):
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="bubble-buster-trade-stats-cache",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout_sec: float = 10.0) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, float(timeout_sec)))
        self._thread = None

    def _run(self) -> None:
        next_refresh_at = {account_id: 0.0 for account_id in sorted(self.fetchers)}
        while not self._stop_event.is_set():
            did_refresh = False
            now = time.monotonic()
            for account_id in sorted(self.fetchers):
                if self._stop_event.is_set():
                    break
                if now < next_refresh_at[account_id]:
                    continue
                fetcher = self.fetchers[account_id]
                try:
                    fetcher.refresh_stats(
                        account_id=account_id,
                        lookback_days=self.lookback_days,
                    )
                except Exception as exc:  # noqa: BLE001
                    logging.getLogger(__name__).warning(
                        "Background trade stats refresh failed for account=%s: %s",
                        account_id,
                        exc,
                    )
                next_refresh_at[account_id] = time.monotonic() + self.refresh_interval_sec
                did_refresh = True

            if did_refresh:
                self._stop_event.wait(0.1)
            else:
                self._stop_event.wait(1.0)


class RuntimeFileLock:
    def __init__(self, lock_file: str, wait_sec: int = 30, poll_sec: float = 0.2):
        self.lock_file = lock_file
        self.wait_sec = max(0, wait_sec)
        self.poll_sec = max(0.05, poll_sec)
        self._fp = None

    def acquire(self) -> None:
        if fcntl is None:
            return

        os.makedirs(os.path.dirname(os.path.abspath(self.lock_file)), exist_ok=True)
        self._fp = open(self.lock_file, "a+", encoding="utf-8")
        start = time.time()
        while True:
            try:
                fcntl.flock(self._fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() - start >= self.wait_sec:
                    raise RuntimeError(f"Lock busy timeout after {self.wait_sec}s: {self.lock_file}")
                time.sleep(self.poll_sec)

        self._fp.write(f"pid={os.getpid()} time={int(time.time())}\n")
        self._fp.flush()

    def release(self) -> None:
        if not self._fp:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._fp.fileno(), fcntl.LOCK_UN)
        finally:
            self._fp.close()
            self._fp = None


def _load_config(config_path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if not cfg.read(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")
    return cfg


def create_dashboard_context(config_path: str) -> DashboardRuntimeContext:
    cfg = _load_config(config_path)
    runtime_cfg = cfg["runtime"] if cfg.has_section("runtime") else {}
    account_cfg = cfg["accounts"] if cfg.has_section("accounts") else {}

    base_dir = str(Path(config_path).resolve().parent)
    db_path = resolve_path(runtime_cfg.get("db_path", "state.db"), base_dir)
    log_dir = resolve_path(runtime_cfg.get("log_dir", "logs"), base_dir)
    log_file = os.path.join(log_dir, "strategy.log")

    timezone_name = runtime_cfg.get("timezone", "Asia/Shanghai").strip()
    entry_hour = int(runtime_cfg.get("entry_hour", 7))
    entry_minute = int(runtime_cfg.get("entry_minute", 40))
    portfolio_loss_cut_enabled = str(
        runtime_cfg.get("portfolio_loss_cut_enabled", "false")
    ).strip().lower() in {"1", "true", "yes", "on"}
    portfolio_loss_cut_pct = min(
        100.0,
        max(0.001, float(runtime_cfg.get("portfolio_loss_cut_pct", 3.5))),
    )
    portfolio_loss_cut_hour = int(runtime_cfg.get("portfolio_loss_cut_hour", 8)) % 24
    portfolio_loss_cut_minute = int(runtime_cfg.get("portfolio_loss_cut_minute", 0)) % 60
    refresh_sec = max(15, int(runtime_cfg.get("dashboard_refresh_sec", 15)))
    trade_stats_refresh_sec = max(
        60,
        int(runtime_cfg.get("dashboard_trade_stats_refresh_sec", 300)),
    )
    curve_points = max(100, int(runtime_cfg.get("dashboard_curve_points", 600)))
    balance_refresh_sec = max(5, int(runtime_cfg.get("manager_interval_sec", 60)))
    run_with_dashboard = runtime_cfg.get("run_service_with_dashboard", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    default_account_id = runtime_cfg.get("default_account_id", "default").strip() or "default"
    enabled_accounts_raw = account_cfg.get("enabled", fallback="") if account_cfg else ""
    enabled_accounts = [x.strip() for x in enabled_accounts_raw.split(",") if x.strip()]
    overview_account_ids = list(enabled_accounts)
    account_modes = {
        aid: account_cfg.get(f"mode.{aid}", fallback="full", raw=True).strip().lower() or "full"
        for aid in overview_account_ids
    }
    account_strategy_notes = {
        aid: account_cfg.get(f"strategy_note.{aid}", fallback="", raw=True).strip()
        for aid in overview_account_ids
    }
    global_equity_recovery_enabled = str(
        cfg.get("strategy", "equity_recovery_take_profit_enabled", fallback="false")
    ).strip().lower() in {"1", "true", "yes", "on"}
    global_portfolio_take_profit_enabled = str(
        runtime_cfg.get("portfolio_take_profit_enabled", "false")
    ).strip().lower() in {"1", "true", "yes", "on"}
    account_equity_recovery_enabled = {}
    for aid in overview_account_ids:
        strategy_section_name = f"account.{aid}.strategy"
        if cfg.has_option(strategy_section_name, "equity_recovery_take_profit_enabled"):
            raw = cfg.get(
                strategy_section_name,
                "equity_recovery_take_profit_enabled",
                fallback="false",
            )
            legacy_enabled = str(raw).strip().lower() in {"1", "true", "yes", "on"}
        else:
            legacy_enabled = global_equity_recovery_enabled

        runtime_section_name = f"account.{aid}.runtime"
        if cfg.has_option(runtime_section_name, "portfolio_take_profit_enabled"):
            raw = cfg.get(runtime_section_name, "portfolio_take_profit_enabled", fallback="false")
            daily_enabled = str(raw).strip().lower() in {"1", "true", "yes", "on"}
        else:
            daily_enabled = global_portfolio_take_profit_enabled
        # Keep the existing dashboard field/API stable while representing both
        # the legacy recovery rule and the new fixed daily-baseline rule.
        account_equity_recovery_enabled[aid] = legacy_enabled or daily_enabled

    balance_fetcher = None
    close_price_fetcher = None
    live_position_clients = {}
    # If runtime service is enabled, wallet snapshots are persisted in background.
    # In that mode dashboard reads DB snapshots and skips direct balance polling.
    if cfg.has_section("binance"):
        binance_cfg = cfg["binance"]
        api_key = binance_cfg.get("api_key", "").strip()
        api_secret = binance_cfg.get("api_secret", "").strip()
        if api_key and api_secret:
            default_pool_size = max(32, int(binance_cfg.get("http_pool_maxsize", fallback=64)))
            client = BinanceFuturesClient(
                api_key=api_key,
                api_secret=api_secret,
                base_url=binance_cfg.get("base_url", "https://fapi.binance.com").strip(),
                timeout_sec=binance_cfg.getint("timeout_sec", fallback=10),
                retry_count=binance_cfg.getint("retry_count", fallback=3),
                retry_delay_sec=binance_cfg.getfloat("retry_delay_sec", fallback=1.0),
                recv_window=binance_cfg.getint("recv_window", fallback=5000),
                http_pool_maxsize=default_pool_size,
                proxies=build_proxies(cfg),
            )
            live_position_clients[default_account_id] = client

            def _fetch_wallet_balance_usdt() -> float:
                balances = client.get_balance()
                wallet_balance = None
                for item in balances:
                    if str(item.get("asset", "")).upper() == "USDT":
                        raw = item.get("balance")
                        if raw is None:
                            raw = item.get("crossWalletBalance")
                        if raw is None:
                            raw = item.get("availableBalance")
                        wallet_balance = float(raw or 0.0)
                        break
                if wallet_balance is None:
                    raise ValueError("USDT balance not found from /fapi/v2/balance")
                unrealized = 0.0
                try:
                    positions = client.get_position_risk()
                    for row in positions:
                        unrealized += float(row.get("unRealizedProfit") or 0.0)
                except Exception as exc:  # noqa: BLE001
                    logging.getLogger(__name__).warning(
                        "Dashboard direct fetch failed to get position risk, fallback wallet balance only: %s",
                        exc,
                    )
                return wallet_balance + unrealized
            if not run_with_dashboard:
                balance_fetcher = _fetch_wallet_balance_usdt

            def _avg_price_from_order_trades(symbol: str, target_order_id: int) -> Optional[float]:
                trades = client.get_user_trades(symbol=symbol, order_id=target_order_id, limit=1000)
                if not trades:
                    return None
                total_qty = 0.0
                total_quote = 0.0
                for tr in trades:
                    qty = float(tr.get("qty") or tr.get("executedQty") or 0.0)
                    price = float(tr.get("price") or 0.0)
                    quote = float(tr.get("quoteQty") or 0.0)
                    if qty <= 0:
                        continue
                    if quote > 0:
                        total_quote += quote
                    elif price > 0:
                        total_quote += price * qty
                    total_qty += qty
                if total_qty <= 0 or total_quote <= 0:
                    return None
                return total_quote / total_qty

            def _fetch_close_price(symbol: str, order_id: int) -> Optional[float]:
                tried = set()

                def _try(order_id_candidate: Optional[int]) -> Optional[float]:
                    if order_id_candidate is None or order_id_candidate <= 0:
                        return None
                    if order_id_candidate in tried:
                        return None
                    tried.add(order_id_candidate)
                    return _avg_price_from_order_trades(symbol=symbol, target_order_id=order_id_candidate)

                direct = _try(int(order_id))
                if direct is not None:
                    return direct

                # For algo conditional orders, /fapi/v1/algoOrder returns actualOrderId.
                # We must resolve and then query /fapi/v1/userTrades by that real order id.
                try:
                    order_payload = client.get_order(symbol=symbol, order_id=int(order_id))
                except Exception:  # noqa: BLE001
                    return None

                actual_order_id = order_payload.get("actualOrderId")
                try:
                    parsed_actual_order_id = int(actual_order_id) if actual_order_id is not None else None
                except (TypeError, ValueError):
                    parsed_actual_order_id = None

                resolved = _try(parsed_actual_order_id)
                if resolved is not None:
                    return resolved
                return None

            close_price_fetcher = _fetch_close_price

    # 为 readonly 账户创建 TradeStatsFetcher
    trade_stats_fetchers = {}
    for aid in overview_account_ids:
        if account_modes.get(aid, "full") != "readonly":
            continue
        # 获取账户特定的 Binance 配置
        binance_section = f"account.{aid}.binance"
        if cfg.has_section(binance_section):
            api_key = cfg.get(binance_section, "api_key", fallback="").strip()
            api_secret = cfg.get(binance_section, "api_secret", fallback="").strip()
        else:
            api_key = ""
            api_secret = ""
        if not api_key or not api_secret:
            continue
        try:
            readonly_client = BinanceFuturesClient(
                api_key=api_key,
                api_secret=api_secret,
                base_url=cfg.get(binance_section, "base_url", fallback="https://fapi.binance.com").strip()
                if cfg.has_section(binance_section)
                else "https://fapi.binance.com",
                timeout_sec=cfg.getint(binance_section, "timeout_sec", fallback=10)
                if cfg.has_section(binance_section)
                else 10,
                retry_count=cfg.getint(binance_section, "retry_count", fallback=3)
                if cfg.has_section(binance_section)
                else 3,
                retry_delay_sec=cfg.getfloat(binance_section, "retry_delay_sec", fallback=1.0)
                if cfg.has_section(binance_section)
                else 1.0,
                recv_window=cfg.getint(binance_section, "recv_window", fallback=5000)
                if cfg.has_section(binance_section)
                else 5000,
                http_pool_maxsize=32,
                proxies=build_proxies(cfg),
            )
            live_position_clients[aid] = readonly_client
            trade_stats_fetchers[aid] = TradeStatsFetcher(client=readonly_client, cache_ttl_sec=300)
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "Failed to create TradeStatsFetcher for readonly account=%s: %s",
                aid,
                exc,
            )

    schema_path = str((Path(__file__).parent / "schema.sql").resolve())
    StateStore(
        db_path=db_path,
        schema_path=schema_path,
        account_id=default_account_id,
    ).init_schema()

    provider = DashboardDataProvider(
        db_path=db_path,
        log_file=log_file,
        timezone_name=timezone_name,
        entry_hour=entry_hour,
        entry_minute=entry_minute,
        balance_fetcher=balance_fetcher,
        close_price_fetcher=close_price_fetcher,
        balance_cache_ttl_sec=balance_refresh_sec,
        default_curve_points=curve_points,
        account_strategy_notes=account_strategy_notes,
        account_modes=account_modes,
        account_equity_recovery_enabled=account_equity_recovery_enabled,
        overview_account_ids=overview_account_ids,
        live_wallet_account_id=default_account_id,
        trade_stats_fetchers=trade_stats_fetchers,
        live_position_clients=live_position_clients,
    )

    echarts_src = _ensure_local_echarts_asset()

    return DashboardRuntimeContext(
        config_path=str(Path(config_path).resolve()),
        db_path=db_path,
        log_file=log_file,
        timezone_name=timezone_name,
        entry_hour=entry_hour,
        entry_minute=entry_minute,
        portfolio_loss_cut_enabled=portfolio_loss_cut_enabled,
        portfolio_loss_cut_pct=portfolio_loss_cut_pct,
        portfolio_loss_cut_hour=portfolio_loss_cut_hour,
        portfolio_loss_cut_minute=portfolio_loss_cut_minute,
        refresh_sec=refresh_sec,
        trade_stats_refresh_sec=trade_stats_refresh_sec,
        echarts_src=echarts_src,
        provider=provider,
    )


def _default_config_path() -> str:
    env_path = os.getenv("BUBBLE_BUSTER_CONFIG", "").strip()
    if env_path:
        return str(Path(env_path).resolve())
    return str((Path.cwd() / "config.ini").resolve())


def _ensure_local_echarts_asset() -> str:
    static_root = (Path(__file__).resolve().parent / "app_static").resolve()
    target = static_root / "vendor" / "echarts.min.js"
    if target.exists():
        return "/static/vendor/echarts.min.js"

    candidates = []
    for base in sys.path:
        if not base or not os.path.isdir(base):
            continue
        candidates.extend(
            glob.glob(os.path.join(base, "**", "echarts.min.js"), recursive=True)
        )
    for src in candidates:
        if "site-packages" not in src:
            continue
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, target)
            return "/static/vendor/echarts.min.js"
        except OSError:
            continue
    # Fallback to CDN if local asset unavailable.
    return "https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"


def _build_service_state(enabled: bool = False) -> dict:
    return {
        "enabled": enabled,
        "running": False,
        "error": None,
        "thread": None,
        "stop_event": None,
        "lock": None,
        "service": None,
    }


def _read_entry_catchup_from_config(config_path: str) -> bool:
    cfg = _load_config(config_path)
    runtime_cfg = cfg["runtime"] if cfg.has_section("runtime") else {}
    return str(runtime_cfg.get("entry_catchup_enabled", "true")).strip().lower() in {"1", "true", "yes", "on"}


def _runtime_entry_catchup_state(app: FastAPI) -> dict:
    config_path = str(getattr(app.state, "config_path", "") or _default_config_path())
    service_state = getattr(app.state, "service_state", {}) or {}
    service = service_state.get("service")
    if service is not None and getattr(service, "cfg", None) is not None:
        return {
            "entry_catchup_enabled": bool(getattr(service.cfg, "entry_catchup_enabled", True)),
            "source": "RUNTIME",
            "mutable": False,
        }
    return {
        "entry_catchup_enabled": _read_entry_catchup_from_config(config_path),
        "source": "CONFIG",
        "mutable": False,
    }


def _ensure_strategy_log_handler(log_file: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
    root = logging.getLogger()
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    abs_target = os.path.abspath(log_file)
    for handler in root.handlers:
        filename = getattr(handler, "baseFilename", None)
        if filename and os.path.abspath(filename) == abs_target:
            return

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    file_handler = TimedRotatingFileHandler(
        filename=abs_target,
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def _startup_background_service(app: FastAPI, config_path: str) -> None:
    cfg = _load_config(config_path)
    runtime_cfg = cfg["runtime"] if cfg.has_section("runtime") else {}
    run_with_dashboard = runtime_cfg.get("run_service_with_dashboard", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    service_state = _build_service_state(enabled=run_with_dashboard)
    app.state.service_state = service_state
    if not run_with_dashboard:
        return

    base_dir = str(Path(config_path).resolve().parent)
    lock_file = resolve_path(runtime_cfg.get("lock_file", "runtime.lock"), base_dir)
    lock_wait_sec = int(runtime_cfg.get("lock_wait_sec", 30))

    try:
        runtime_lock = RuntimeFileLock(lock_file=lock_file, wait_sec=lock_wait_sec)
        runtime_lock.acquire()

        strategy, manager, wallet_sampler, _runtime_cfg, service_cfg, account_runtimes = create_components(
            cfg,
            base_dir=base_dir,
        )
        service = StrategyRuntimeService(
            strategy=strategy,
            manager=manager,
            cfg=service_cfg,
            balance_sampler=wallet_sampler,
            account_runtimes=account_runtimes,
        )
        app.state.ctx.provider.set_live_position_clients(
            {
                aid: runtime_ctx.get("manager").client
                if runtime_ctx.get("manager") is not None
                else runtime_ctx.get("balance_sampler").client
                if runtime_ctx.get("balance_sampler") is not None
                else None
                for aid, runtime_ctx in account_runtimes.items()
            }
        )
        stop_event = threading.Event()
        thread = threading.Thread(
            target=service.run_forever,
            kwargs={"stop_event": stop_event},
            name="bubble-buster-runtime",
            daemon=True,
        )
        thread.start()

        service_state["running"] = True
        service_state["thread"] = thread
        service_state["stop_event"] = stop_event
        service_state["lock"] = runtime_lock
        service_state["service"] = service
    except Exception as exc:  # noqa: BLE001
        service_state["error"] = str(exc)
        lock_obj = service_state.get("lock")
        if lock_obj:
            lock_obj.release()
            service_state["lock"] = None


def _shutdown_background_service(app: FastAPI) -> None:
    service_state = getattr(app.state, "service_state", None)
    if not isinstance(service_state, dict):
        return

    stop_event = service_state.get("stop_event")
    thread = service_state.get("thread")
    lock_obj = service_state.get("lock")

    try:
        if stop_event:
            stop_event.set()
        if thread and thread.is_alive():
            thread.join(timeout=10)
    finally:
        thread_still_running = bool(thread and thread.is_alive())
        if lock_obj and not thread_still_running:
            lock_obj.release()
        elif thread_still_running:
            service_state["error"] = "runtime thread did not stop within 10s; lock retained"
        service_state["running"] = False
        service_state["service"] = None


def _start_trade_stats_background_refresh(app: FastAPI) -> None:
    ctx: DashboardRuntimeContext = app.state.ctx
    refresher = TradeStatsBackgroundRefresher(
        ctx.provider.trade_stats_fetchers,
        refresh_interval_sec=ctx.trade_stats_refresh_sec,
        lookback_days=30,
    )
    refresher.start()
    app.state.trade_stats_refresher = refresher


def _shutdown_trade_stats_background_refresh(app: FastAPI) -> None:
    refresher = getattr(app.state, "trade_stats_refresher", None)
    if refresher is not None:
        refresher.stop()
    app.state.trade_stats_refresher = None


def create_app(config_path: Optional[str] = None) -> FastAPI:
    app = FastAPI(title="Bubble Buster Dashboard", version="1.1.0")
    static_root = (Path(__file__).resolve().parent / "app_static").resolve()
    static_root.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_root)), name="static")

    @app.middleware("http")
    async def enforce_read_only_dashboard(request: Request, call_next):
        if request.method.upper() not in {"GET", "HEAD", "OPTIONS"}:
            return JSONResponse(
                status_code=405,
                content={"detail": "Dashboard is read-only"},
                headers={"Allow": "GET, HEAD, OPTIONS"},
            )
        return await call_next(request)

    @app.on_event("startup")
    def _startup() -> None:
        path = config_path or _default_config_path()
        app.state.config_path = path
        app.state.ctx = create_dashboard_context(path)
        _ensure_strategy_log_handler(app.state.ctx.log_file)
        _start_trade_stats_background_refresh(app)
        _startup_background_service(app, path)

    @app.on_event("shutdown")
    def _shutdown() -> None:
        _shutdown_trade_stats_background_refresh(app)
        _shutdown_background_service(app)

    @app.get("/", response_class=HTMLResponse)
    def overview_page(request: Request):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        return HTMLResponse(
            render_accounts_overview_html(
                ctx.refresh_sec,
                entry_hour=ctx.entry_hour,
                entry_minute=ctx.entry_minute,
                portfolio_loss_cut_enabled=ctx.portfolio_loss_cut_enabled,
                portfolio_loss_cut_pct=ctx.portfolio_loss_cut_pct,
                portfolio_loss_cut_hour=ctx.portfolio_loss_cut_hour,
                portfolio_loss_cut_minute=ctx.portfolio_loss_cut_minute,
            )
        )

    @app.get("/account/{account_id}/", response_class=HTMLResponse)
    def account_detail_page(
        request: Request,
        account_id: str,
    ):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        return HTMLResponse(
            render_account_dashboard_html(
                ctx.refresh_sec,
                account_id=account_id,
                echarts_src=ctx.echarts_src,
                account_mode=ctx.provider.account_modes.get(account_id, "full"),
                strategy_note=ctx.provider.account_strategy_notes.get(account_id, ""),
                portfolio_loss_cut_enabled=ctx.portfolio_loss_cut_enabled,
                portfolio_loss_cut_pct=ctx.portfolio_loss_cut_pct,
                portfolio_loss_cut_hour=ctx.portfolio_loss_cut_hour,
                portfolio_loss_cut_minute=ctx.portfolio_loss_cut_minute,
            )
        )

    @app.get("/api/dashboard")
    def dashboard_data(
        request: Request,
        log_lines: int = Query(default=80, ge=0, le=300),
        window_hours: Optional[float] = Query(default=24.0, gt=0.0, le=8784.0),
        curve_points: Optional[int] = Query(default=None, ge=100, le=5000),
    ):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        try:
            payload = ctx.provider.snapshot(
                log_lines=log_lines,
                window_hours=window_hours,
                curve_points=curve_points,
            )

            service_state = getattr(request.app.state, "service_state", {}) or {}
            thread = service_state.get("thread")
            payload["service"] = {
                "enabled": bool(service_state.get("enabled", False)),
                "running": bool(service_state.get("running", False)) and bool(thread and thread.is_alive()),
                "error": service_state.get("error"),
            }
            payload["runtime_settings"] = _runtime_entry_catchup_state(request.app)
            return JSONResponse(payload)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"dashboard snapshot failed: {exc}") from exc

    @app.get("/api/accounts/summary")
    def accounts_summary(request: Request):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        try:
            return JSONResponse(ctx.provider.accounts_summary())
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"accounts summary failed: {exc}") from exc

    @app.get("/api/account/{account_id}/snapshot")
    def account_snapshot(
        request: Request,
        account_id: str,
        log_lines: int = Query(default=80, ge=0, le=300),
        window_hours: Optional[float] = Query(default=24.0, gt=0.0, le=8784.0),
        curve_points: Optional[int] = Query(default=None, ge=100, le=5000),
    ):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        try:
            payload = ctx.provider.snapshot(
                log_lines=log_lines,
                window_hours=window_hours,
                curve_points=curve_points,
                account_id=account_id,
            )
            payload["account_id"] = account_id
            service_state = getattr(request.app.state, "service_state", {}) or {}
            thread = service_state.get("thread")
            payload["service"] = {
                "enabled": bool(service_state.get("enabled", False)),
                "running": bool(service_state.get("running", False)) and bool(thread and thread.is_alive()),
                "error": service_state.get("error"),
            }
            return JSONResponse(payload)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"account snapshot failed: {exc}") from exc

    @app.get("/api/account/{account_id}/core")
    def account_core(
        request: Request,
        account_id: str,
        window_hours: Optional[float] = Query(default=24.0, gt=0.0, le=8784.0),
        curve_points: Optional[int] = Query(default=None, ge=100, le=5000),
    ):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        try:
            payload = ctx.provider.snapshot(
                log_lines=0,
                window_hours=window_hours,
                curve_points=curve_points,
                account_id=account_id,
                include_details=False,
                include_log=False,
                include_curves=False,
                include_balance_curve=False,
                include_trade_stats=True,
            )
            payload = _strip_curve_payload(payload)
            payload["account_id"] = account_id
            service_state = getattr(request.app.state, "service_state", {}) or {}
            thread = service_state.get("thread")
            payload["service"] = {
                "enabled": bool(service_state.get("enabled", False)),
                "running": bool(service_state.get("running", False)) and bool(thread and thread.is_alive()),
                "error": service_state.get("error"),
            }
            return JSONResponse(payload)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"account core failed: {exc}") from exc

    @app.get("/api/account/{account_id}/curve")
    def account_curve(
        request: Request,
        account_id: str,
        window_hours: Optional[float] = Query(default=24.0, gt=0.0, le=8784.0),
        curve_points: Optional[int] = Query(default=None, ge=100, le=5000),
    ):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        try:
            payload = ctx.provider.snapshot(
                log_lines=0,
                window_hours=window_hours,
                curve_points=curve_points,
                account_id=account_id,
                include_details=False,
                include_log=False,
                include_curves=True,
                include_balance_curve=True,
                include_trade_stats=False,
            )
            payload["account_id"] = account_id
            return JSONResponse(payload)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"account curve failed: {exc}") from exc

    @app.get("/api/account/{account_id}/details")
    def account_details(
        request: Request,
        account_id: str,
        log_lines: int = Query(default=80, ge=0, le=300),
    ):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        try:
            payload = ctx.provider.snapshot(
                log_lines=log_lines,
                window_hours=None,
                curve_points=100,
                account_id=account_id,
                include_details=True,
                include_log=True,
                include_curves=False,
            )
            payload["account_id"] = account_id
            return JSONResponse(payload)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"account details failed: {exc}") from exc

    @app.get("/api/runtime/settings")
    def runtime_settings(request: Request):
        try:
            return _runtime_entry_catchup_state(request.app)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"runtime settings read failed: {exc}") from exc

    @app.get("/healthz")
    def healthz(request: Request):
        service_state = getattr(request.app.state, "service_state", {}) or {}
        thread = service_state.get("thread")
        return {
            "ok": True,
            "service_enabled": bool(service_state.get("enabled", False)),
            "service_running": bool(service_state.get("running", False)) and bool(thread and thread.is_alive()),
            "service_error": service_state.get("error"),
        }

    return app


app = create_app()
