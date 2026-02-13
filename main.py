import argparse
import configparser
import fcntl
import logging
import os
import signal
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import Dict, Iterator, Optional
from zoneinfo import ZoneInfo

from core.runtime_components import create_components, resolve_path
from core.runtime_service import StrategyRuntimeService

LOGGER = logging.getLogger(__name__)


class GracefulExit(SystemExit):
    pass


def _handle_signal(signum, _frame):  # type: ignore[no-untyped-def]
    raise GracefulExit(f"Received signal: {signum}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance Top10 short strategy runner")
    parser.add_argument("--config", default="config.ini", help="Path to config.ini")

    subparsers = parser.add_subparsers(dest="command", required=True)

    entry_parser = subparsers.add_parser("entry", help="Run daily entry flow once")
    entry_parser.add_argument(
        "--trade-day-utc",
        default=None,
        help="Optional idempotency key override (e.g. 2026-02-13-manual)",
    )

    manage_parser = subparsers.add_parser("manage", help="Run position management")
    manage_parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously with runtime.manager_interval_sec",
    )
    subparsers.add_parser("service", help="Run built-in scheduler service (no cron)")
    dashboard_parser = subparsers.add_parser("dashboard", help="Run local web dashboard")
    dashboard_parser.add_argument("--host", default=None, help="Dashboard bind host override")
    dashboard_parser.add_argument("--port", type=int, default=None, help="Dashboard bind port override")
    dashboard_parser.add_argument("--reload", action="store_true", help="Enable uvicorn reload for dashboard")

    return parser.parse_args()


def load_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    read_ok = cfg.read(path)
    if not read_ok:
        raise FileNotFoundError(f"Config not found: {path}")
    return cfg


def setup_logging(log_dir: str, level: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level.upper())

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    # Reset handlers for deterministic behavior in cron invocations.
    root.handlers = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "strategy.log"),
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


@contextmanager
def file_lock(lock_file: str, wait_sec: int = 30, poll_sec: float = 0.2) -> Iterator[None]:
    os.makedirs(os.path.dirname(os.path.abspath(lock_file)), exist_ok=True)
    with open(lock_file, "a+", encoding="utf-8") as lock_fp:
        start_time = time.time()
        while True:
            try:
                fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() - start_time >= wait_sec:
                    raise RuntimeError(f"Lock busy timeout after {wait_sec}s: {lock_file}")
                time.sleep(max(0.05, poll_sec))
        lock_fp.write(f"pid={os.getpid()} time={int(time.time())}\n")
        lock_fp.flush()
        try:
            yield
        finally:
            fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)


def warn_if_outside_entry_window(runtime_cfg: configparser.SectionProxy, tolerance_min: int = 10) -> None:
    timezone_name = runtime_cfg.get("timezone", fallback="Asia/Shanghai").strip()
    entry_hour = runtime_cfg.getint("entry_hour", fallback=7)
    entry_minute = runtime_cfg.getint("entry_minute", fallback=40)

    try:
        now_local = datetime.now(ZoneInfo(timezone_name))
    except Exception:  # noqa: BLE001
        LOGGER.warning("Invalid runtime.timezone=%s, skip schedule window check", timezone_name)
        return

    target_local = now_local.replace(hour=entry_hour, minute=entry_minute, second=0, microsecond=0)
    delta_sec = abs((now_local - target_local).total_seconds())
    if delta_sec > tolerance_min * 60:
        LOGGER.warning(
            "Entry is running outside configured time window: now=%s target=%02d:%02d %s tolerance=%smin",
            now_local.strftime("%Y-%m-%d %H:%M:%S"),
            entry_hour,
            entry_minute,
            timezone_name,
            tolerance_min,
        )


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    args = parse_args()
    config_path = resolve_path(args.config)
    cfg = load_config(config_path)
    base_dir = os.path.dirname(config_path)

    runtime_cfg = cfg["runtime"]
    log_dir = runtime_cfg.get("log_dir", "logs").strip()
    if not os.path.isabs(log_dir):
        log_dir = resolve_path(log_dir, base_dir)
    setup_logging(log_dir=log_dir, level=runtime_cfg.get("log_level", fallback="INFO"))

    lock_file = runtime_cfg.get("lock_file", "runtime.lock").strip()
    if not os.path.isabs(lock_file):
        lock_file = resolve_path(lock_file, base_dir)
    lock_wait_sec = runtime_cfg.getint("lock_wait_sec", fallback=30)

    if args.command == "dashboard":
        os.environ["BUBBLE_BUSTER_CONFIG"] = config_path
        host = (args.host or runtime_cfg.get("dashboard_host", fallback="127.0.0.1")).strip()
        port = args.port or runtime_cfg.getint("dashboard_port", fallback=8787)
        try:
            import uvicorn
        except ImportError as exc:
            LOGGER.error("uvicorn is required for dashboard mode. Install dependencies from requirements.txt")
            raise exc

        uvicorn.run(
            "app.main:app",
            host=host,
            port=port,
            reload=bool(args.reload),
            log_level=runtime_cfg.get("log_level", fallback="info").lower(),
        )
        return 0

    try:
        with file_lock(lock_file, wait_sec=lock_wait_sec):
            strategy, manager, wallet_sampler, runtime_cfg, service_cfg = create_components(cfg, base_dir=base_dir)

            if args.command == "entry":
                warn_if_outside_entry_window(runtime_cfg)
                result = strategy.run_entry(trade_day_utc=args.trade_day_utc)
                LOGGER.info("entry result: %s", result)
                return 0 if result.get("status") != "FAILED" else 1

            if args.command == "manage":
                interval = runtime_cfg.getint("manager_interval_sec", fallback=60)
                if args.loop:
                    LOGGER.info("manage loop started, interval=%ss", interval)
                    while True:
                        summary = manager.run_once()
                        LOGGER.info("manage summary: %s", summary)
                        time.sleep(max(1, interval))
                else:
                    summary = manager.run_once()
                    LOGGER.info("manage summary: %s", summary)
                return 0

            if args.command == "service":
                service = StrategyRuntimeService(
                    strategy=strategy,
                    manager=manager,
                    cfg=service_cfg,
                    balance_sampler=wallet_sampler,
                )
                service.run_forever()
                return 0

            raise ValueError(f"Unsupported command: {args.command}")
    except GracefulExit as exc:
        LOGGER.info("Graceful exit: %s", exc)
        return 0
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
