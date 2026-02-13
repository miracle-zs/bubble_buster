import configparser
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

from balance_sampler import WalletSnapshotSampler
from binance_futures_client import BinanceFuturesClient
from notifier import ServerChanNotifier
from position_manager import PositionManager
from runtime_service import ServiceRuntimeConfig
from state_store import StateStore
from strategy_top10_short import Top10ShortStrategy


def resolve_path(raw: str, base_dir: Optional[str] = None) -> str:
    value = raw.strip()
    if os.path.isabs(value):
        return value
    base = Path(base_dir).resolve() if base_dir else Path.cwd().resolve()
    return str((base / value).resolve())


def build_proxies(cfg: configparser.ConfigParser) -> Optional[Dict[str, str]]:
    notify_section = cfg["notify"] if cfg.has_section("notify") else {}
    http_proxy = notify_section.get("http_proxy", "").strip()
    https_proxy = notify_section.get("https_proxy", "").strip()
    if http_proxy and https_proxy:
        return {"http": http_proxy, "https": https_proxy}
    return None


def resolve_runtime_paths(
    runtime_cfg: configparser.SectionProxy,
    base_dir: Optional[str] = None,
) -> Tuple[str, str, str]:
    db_path = resolve_path(runtime_cfg.get("db_path", "state.db"), base_dir)
    log_dir = resolve_path(runtime_cfg.get("log_dir", "logs"), base_dir)
    lock_file = resolve_path(runtime_cfg.get("lock_file", "runtime.lock"), base_dir)
    return db_path, log_dir, lock_file


def create_components(
    cfg: configparser.ConfigParser,
    base_dir: Optional[str] = None,
):
    proxies = build_proxies(cfg)

    binance_cfg = cfg["binance"]
    runtime_cfg = cfg["runtime"]
    strategy_cfg = cfg["strategy"]
    notify_cfg = cfg["notify"]
    ranker_max_workers = max(1, strategy_cfg.getint("ranker_max_workers", fallback=24))
    default_pool_size = max(32, ranker_max_workers * 2)

    client = BinanceFuturesClient(
        api_key=binance_cfg.get("api_key", "").strip(),
        api_secret=binance_cfg.get("api_secret", "").strip(),
        base_url=binance_cfg.get("base_url", "https://fapi.binance.com").strip(),
        timeout_sec=binance_cfg.getint("timeout_sec", fallback=10),
        retry_count=binance_cfg.getint("retry_count", fallback=3),
        retry_delay_sec=binance_cfg.getfloat("retry_delay_sec", fallback=1.0),
        recv_window=binance_cfg.getint("recv_window", fallback=5000),
        http_pool_maxsize=binance_cfg.getint("http_pool_maxsize", fallback=default_pool_size),
        proxies=proxies,
    )

    db_path = resolve_path(runtime_cfg.get("db_path", "state.db"), base_dir)
    schema_path = str((Path(__file__).parent / "schema.sql").resolve())

    store = StateStore(db_path=db_path, schema_path=schema_path)
    store.init_schema()

    notifier = ServerChanNotifier(
        enabled=notify_cfg.getboolean("enabled", fallback=True),
        sendkey=notify_cfg.get("serverchan_sendkey", "").strip(),
        proxies=proxies,
        timeout_sec=10,
    )

    strategy = Top10ShortStrategy(
        client=client,
        store=store,
        notifier=notifier,
        leverage=strategy_cfg.getint("leverage", fallback=2),
        top_n=strategy_cfg.getint("top_n", fallback=10),
        volume_threshold=strategy_cfg.getfloat("volume_threshold", fallback=0.0),
        tp_price_drop_pct=strategy_cfg.getfloat("tp_price_drop_pct", fallback=20.0),
        sl_liq_buffer_pct=strategy_cfg.getfloat("sl_liq_buffer_pct", fallback=1.0),
        max_hold_hours=strategy_cfg.getfloat("max_hold_hours", fallback=47.5),
        trigger_price_type=strategy_cfg.get("trigger_price_type", fallback="CONTRACT_PRICE").strip(),
        allocation_splits=strategy_cfg.getint("allocation_splits", fallback=10),
        entry_fee_buffer_pct=max(
            0.0,
            strategy_cfg.getfloat("entry_fee_buffer_pct", fallback=1.0),
        ),
        entry_shrink_retry_count=max(
            0,
            strategy_cfg.getint("entry_shrink_retry_count", fallback=3),
        ),
        entry_shrink_step_pct=max(
            1.0,
            strategy_cfg.getfloat("entry_shrink_step_pct", fallback=10.0),
        ),
        entry_rank_fetch_multiplier=max(
            1,
            strategy_cfg.getint("entry_rank_fetch_multiplier", fallback=3),
        ),
        ranker_max_workers=ranker_max_workers,
        ranker_weight_limit_per_minute=max(
            100,
            strategy_cfg.getint("ranker_weight_limit_per_minute", fallback=1000),
        ),
        ranker_min_request_interval_ms=max(
            0,
            strategy_cfg.getint("ranker_min_request_interval_ms", fallback=20),
        ),
    )

    manager = PositionManager(
        client=client,
        store=store,
        notifier=notifier,
        sl_liq_buffer_pct=strategy_cfg.getfloat("sl_liq_buffer_pct", fallback=1.0),
        trigger_price_type=strategy_cfg.get("trigger_price_type", fallback="CONTRACT_PRICE").strip(),
    )

    wallet_sampler = WalletSnapshotSampler(
        client=client,
        store=store,
        asset=runtime_cfg.get("wallet_snapshot_asset", fallback="USDT").strip() or "USDT",
    )

    service_cfg = ServiceRuntimeConfig(
        timezone_name=runtime_cfg.get("timezone", fallback="Asia/Shanghai").strip(),
        entry_hour=runtime_cfg.getint("entry_hour", fallback=7),
        entry_minute=runtime_cfg.getint("entry_minute", fallback=40),
        entry_misfire_grace_min=runtime_cfg.getint("entry_misfire_grace_min", fallback=120),
        manager_interval_sec=max(1, runtime_cfg.getint("manager_interval_sec", fallback=60)),
        manager_max_catch_up_runs=max(1, runtime_cfg.getint("manager_max_catch_up_runs", fallback=3)),
        loop_sleep_sec=max(0.2, runtime_cfg.getfloat("service_loop_sleep_sec", fallback=1.0)),
        run_manage_on_startup=runtime_cfg.getboolean("run_manage_on_startup", fallback=True),
    )

    return strategy, manager, wallet_sampler, runtime_cfg, service_cfg
