import configparser
import logging
import os
import threading
import time
from dataclasses import dataclass, replace
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from core.runtime_components import build_proxies, create_components, resolve_path
from core.runtime_service import StrategyRuntimeService
from core.state_store import StateStore
from dashboard_server import DashboardDataProvider, render_dashboard_html
from infra.binance_futures_client import BinanceFuturesClient

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
    refresh_sec: int
    provider: DashboardDataProvider


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

    base_dir = str(Path(config_path).resolve().parent)
    db_path = resolve_path(runtime_cfg.get("db_path", "state.db"), base_dir)
    log_dir = resolve_path(runtime_cfg.get("log_dir", "logs"), base_dir)
    log_file = os.path.join(log_dir, "strategy.log")

    timezone_name = runtime_cfg.get("timezone", "Asia/Shanghai").strip()
    entry_hour = int(runtime_cfg.get("entry_hour", 7))
    entry_minute = int(runtime_cfg.get("entry_minute", 40))
    refresh_sec = max(2, int(runtime_cfg.get("dashboard_refresh_sec", 5)))
    curve_points = max(100, int(runtime_cfg.get("dashboard_curve_points", 600)))
    balance_refresh_sec = max(5, int(runtime_cfg.get("manager_interval_sec", 60)))
    run_with_dashboard = runtime_cfg.get("run_service_with_dashboard", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    balance_fetcher = None
    close_price_fetcher = None
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

            def _fetch_close_price(symbol: str, order_id: int) -> Optional[float]:
                trades = client.get_user_trades(symbol=symbol, order_id=order_id, limit=1000)
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

            close_price_fetcher = _fetch_close_price

    schema_path = str((Path(__file__).parent / "schema.sql").resolve())
    StateStore(db_path=db_path, schema_path=schema_path).init_schema()

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
    )

    return DashboardRuntimeContext(
        config_path=str(Path(config_path).resolve()),
        db_path=db_path,
        log_file=log_file,
        timezone_name=timezone_name,
        refresh_sec=refresh_sec,
        provider=provider,
    )


def _default_config_path() -> str:
    env_path = os.getenv("BUBBLE_BUSTER_CONFIG", "").strip()
    if env_path:
        return str(Path(env_path).resolve())
    return str((Path.cwd() / "config.ini").resolve())


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


def _persist_entry_catchup_to_config(config_path: str, enabled: bool) -> None:
    cfg = _load_config(config_path)
    if not cfg.has_section("runtime"):
        cfg.add_section("runtime")
    cfg["runtime"]["entry_catchup_enabled"] = "true" if enabled else "false"
    with open(config_path, "w", encoding="utf-8") as f:
        cfg.write(f)


def _runtime_entry_catchup_state(app: FastAPI) -> dict:
    config_path = str(getattr(app.state, "config_path", "") or _default_config_path())
    service_state = getattr(app.state, "service_state", {}) or {}
    service = service_state.get("service")
    if service is not None and getattr(service, "cfg", None) is not None:
        return {
            "entry_catchup_enabled": bool(getattr(service.cfg, "entry_catchup_enabled", True)),
            "source": "RUNTIME",
            "mutable": True,
            "config_path": config_path,
        }
    return {
        "entry_catchup_enabled": _read_entry_catchup_from_config(config_path),
        "source": "CONFIG",
        "mutable": True,
        "config_path": config_path,
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

        strategy, manager, wallet_sampler, _runtime_cfg, service_cfg = create_components(cfg, base_dir=base_dir)
        service = StrategyRuntimeService(
            strategy=strategy,
            manager=manager,
            cfg=service_cfg,
            balance_sampler=wallet_sampler,
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
        if lock_obj:
            lock_obj.release()
        service_state["running"] = False
        service_state["service"] = None


def create_app(config_path: Optional[str] = None) -> FastAPI:
    app = FastAPI(title="Bubble Buster Dashboard", version="1.1.0")

    @app.on_event("startup")
    def _startup() -> None:
        path = config_path or _default_config_path()
        app.state.config_path = path
        app.state.ctx = create_dashboard_context(path)
        _ensure_strategy_log_handler(app.state.ctx.log_file)
        _startup_background_service(app, path)

    @app.on_event("shutdown")
    def _shutdown() -> None:
        _shutdown_background_service(app)

    @app.get("/", response_class=HTMLResponse)
    def dashboard_page(request: Request):
        ctx: DashboardRuntimeContext = request.app.state.ctx
        return HTMLResponse(render_dashboard_html(ctx.refresh_sec))

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
            payload["config_path"] = ctx.config_path
            payload["db_path"] = ctx.db_path

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

    @app.get("/api/runtime/settings")
    def runtime_settings(request: Request):
        try:
            return _runtime_entry_catchup_state(request.app)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"runtime settings read failed: {exc}") from exc

    @app.post("/api/runtime/settings/entry-catchup")
    def update_entry_catchup(
        request: Request,
        enabled: bool = Query(...),
        persist: bool = Query(default=True),
    ):
        service_state = getattr(request.app.state, "service_state", {}) or {}
        service = service_state.get("service")
        applied_runtime = False
        if service is not None and getattr(service, "cfg", None) is not None:
            service.cfg = replace(service.cfg, entry_catchup_enabled=bool(enabled))
            applied_runtime = True

        config_path = str(getattr(request.app.state, "config_path", "") or _default_config_path())
        persisted = False
        if persist:
            _persist_entry_catchup_to_config(config_path, bool(enabled))
            persisted = True

        state = _runtime_entry_catchup_state(request.app)
        state["applied_runtime"] = applied_runtime
        state["persisted"] = persisted
        return state

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
