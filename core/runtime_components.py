import configparser
import io
import logging
import os
import threading
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

from core.account_config import parse_account_settings
from core.balance_sampler import WalletSnapshotSampler
from core.position_manager import PositionManager
from core.runtime_service import ServiceRuntimeConfig
from core.state_store import StateStore
from core.strategy_top10_short import Top10ShortStrategy
from infra.binance_futures_client import BinanceFuturesClient
from infra.notifier import ServerChanNotifier

LOGGER = logging.getLogger(__name__)


class MergedSection:
    def __init__(self, values: Dict[str, str]):
        self.values = dict(values)

    def get(self, key: str, fallback: Optional[str] = None) -> str:
        value = self.values.get(key)
        if value is None:
            return "" if fallback is None else str(fallback)
        return str(value).strip()

    def getint(self, key: str, fallback: int = 0) -> int:
        raw = self.values.get(key)
        if raw is None or str(raw).strip() == "":
            return int(fallback)
        return int(str(raw).strip())

    def getfloat(self, key: str, fallback: float = 0.0) -> float:
        raw = self.values.get(key)
        if raw is None or str(raw).strip() == "":
            return float(fallback)
        return float(str(raw).strip())

    def getboolean(self, key: str, fallback: bool = False) -> bool:
        raw = self.values.get(key)
        if raw is None or str(raw).strip() == "":
            return bool(fallback)
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def resolve_path(raw: str, base_dir: Optional[str] = None) -> str:
    value = raw.strip()
    if os.path.isabs(value):
        return value
    base = Path(base_dir).resolve() if base_dir else Path.cwd().resolve()
    return str((base / value).resolve())


def _build_proxies_from_section(notify_cfg: MergedSection) -> Optional[Dict[str, str]]:
    http_proxy = notify_cfg.get("http_proxy", "").strip()
    https_proxy = notify_cfg.get("https_proxy", "").strip()
    if http_proxy and https_proxy:
        return {"http": http_proxy, "https": https_proxy}
    return None


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


def _normalize_items(section: configparser.SectionProxy) -> Dict[str, str]:
    return {k: str(v).strip() for k, v in section.items()}


def _parse_symbol_set(raw: str) -> Set[str]:
    return {part.strip().upper() for part in str(raw or "").split(",") if part.strip()}


def _merged_section(cfg: configparser.ConfigParser, account_id: str, section_name: str) -> MergedSection:
    values: Dict[str, str] = {}
    if cfg.has_section(section_name):
        values.update(_normalize_items(cfg[section_name]))
    account_section = f"account.{account_id}.{section_name}"
    if cfg.has_section(account_section):
        values.update(_normalize_items(cfg[account_section]))
    return MergedSection(values)


def _build_single_account_components(
    account_id: str,
    mode: str,
    binance_cfg: MergedSection,
    strategy_cfg: MergedSection,
    runtime_cfg: MergedSection,
    notify_cfg: MergedSection,
    root_store: StateStore,
) -> Dict[str, object]:
    ranker_max_workers = max(1, strategy_cfg.getint("ranker_max_workers", fallback=24))
    default_pool_size = max(32, ranker_max_workers * 2)
    proxies = _build_proxies_from_section(notify_cfg)

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

    scoped_store = root_store.scoped(account_id)
    protection_exempt_symbols = _parse_symbol_set(runtime_cfg.get("protection_exempt_symbols", fallback=""))

    # readonly 模式：只需要 client 和 wallet_sampler，不需要 strategy 和 manager
    if mode == "readonly":
        wallet_sampler = WalletSnapshotSampler(
            client=client,
            store=scoped_store,
            asset=runtime_cfg.get("wallet_snapshot_asset", fallback="USDT").strip() or "USDT",
            sync_cashflows=False,  # readonly 模式不同步资金流水
            cashflow_income_types=[],
            account_id=account_id,
        )
        return {
            "account_id": account_id,
            "mode": mode,
            "client": client,
            "balance_sampler": wallet_sampler,
        }

    notifier = ServerChanNotifier(
        enabled=notify_cfg.getboolean("enabled", fallback=True),
        sendkey=notify_cfg.get("serverchan_sendkey", "").strip(),
        proxies=proxies,
        timeout_sec=10,
    )
    mutation_lock = threading.RLock()

    strategy = Top10ShortStrategy(
        client=client,
        store=scoped_store,
        notifier=notifier,
        leverage=strategy_cfg.getint("leverage", fallback=2),
        top_n=strategy_cfg.getint("top_n", fallback=10),
        volume_threshold=strategy_cfg.getfloat("volume_threshold", fallback=0.0),
        tp_price_drop_pct=strategy_cfg.getfloat("tp_price_drop_pct", fallback=20.0),
        fixed_take_profit_enabled=strategy_cfg.getboolean("fixed_take_profit_enabled", fallback=True),
        sl_liq_buffer_pct=strategy_cfg.getfloat("sl_liq_buffer_pct", fallback=1.0),
        max_hold_hours=strategy_cfg.getfloat("max_hold_hours", fallback=47.5),
        trigger_price_type=strategy_cfg.get("trigger_price_type", fallback="CONTRACT_PRICE").strip(),
        allocation_splits=strategy_cfg.getint("allocation_splits", fallback=10),
        entry_fee_buffer_pct=max(0.0, strategy_cfg.getfloat("entry_fee_buffer_pct", fallback=1.0)),
        entry_shrink_retry_count=max(0, strategy_cfg.getint("entry_shrink_retry_count", fallback=3)),
        entry_shrink_step_pct=max(1.0, strategy_cfg.getfloat("entry_shrink_step_pct", fallback=10.0)),
        entry_rank_fetch_multiplier=max(1, strategy_cfg.getint("entry_rank_fetch_multiplier", fallback=3)),
        ranker_max_workers=ranker_max_workers,
        ranker_weight_limit_per_minute=max(100, strategy_cfg.getint("ranker_weight_limit_per_minute", fallback=1000)),
        ranker_min_request_interval_ms=max(0, strategy_cfg.getint("ranker_min_request_interval_ms", fallback=20)),
        rebalance_enabled=strategy_cfg.getboolean("rebalance_enabled", fallback=False),
        rebalance_pre_entry_reduce=strategy_cfg.getboolean("rebalance_pre_entry_reduce", fallback=True),
        rebalance_after_entry=strategy_cfg.getboolean("rebalance_after_entry", fallback=True),
        rebalance_utilization=strategy_cfg.getfloat("rebalance_utilization", fallback=0.9),
        rebalance_deadband_pct=strategy_cfg.getfloat("rebalance_deadband_pct", fallback=0.10),
        rebalance_min_adjust_notional_usdt=strategy_cfg.getfloat("rebalance_min_adjust_notional_usdt", fallback=20.0),
        rebalance_max_single_adjust_pct=strategy_cfg.getfloat("rebalance_max_single_adjust_pct", fallback=0.40),
        rebalance_max_adjust_orders=strategy_cfg.getint("rebalance_max_adjust_orders", fallback=30),
        rebalance_mode=strategy_cfg.get("rebalance_mode", fallback="equal_risk").strip(),
        rebalance_age_decay_half_life_hours=strategy_cfg.getfloat("rebalance_age_decay_half_life_hours", fallback=36.0),
        equity_recovery_take_profit_enabled=strategy_cfg.getboolean("equity_recovery_take_profit_enabled", fallback=False),
        equity_recovery_lookback_hours=strategy_cfg.getfloat("equity_recovery_lookback_hours", fallback=24.0),
        equity_recovery_trigger_pct=strategy_cfg.getfloat("equity_recovery_trigger_pct", fallback=0.10),
        equity_recovery_reduce_ratio=strategy_cfg.getfloat("equity_recovery_reduce_ratio", fallback=0.50),
        entry_initial_delay_sec=max(0, runtime_cfg.getint("entry_initial_delay_sec", fallback=0)),
        entry_symbol_interval_sec=max(0, runtime_cfg.getint("entry_symbol_interval_sec", fallback=0)),
        entry_wait_bearish_hour_enabled=strategy_cfg.getboolean("entry_wait_bearish_hour_enabled", fallback=True),
        entry_wait_poll_sec=max(1, strategy_cfg.getint("entry_wait_poll_sec", fallback=30)),
        entry_wait_close_grace_sec=max(0.0, strategy_cfg.getfloat("entry_wait_close_grace_sec", fallback=1.0)),
        entry_wait_close_retry_sec=max(0.1, strategy_cfg.getfloat("entry_wait_close_retry_sec", fallback=1.0)),
        entry_wait_close_retry_count=max(1, strategy_cfg.getint("entry_wait_close_retry_count", fallback=5)),
        entry_wait_max_hours=max(1.0, strategy_cfg.getfloat("entry_wait_max_hours", fallback=16.0)),
        entry_preclose_sec=min(59.0, max(0.0, strategy_cfg.getfloat("entry_preclose_sec", fallback=10.0))),
        cooling_off_retry_count=max(0, runtime_cfg.getint("cooling_off_retry_count", fallback=0)),
        cooling_off_retry_delay_sec=max(0, runtime_cfg.getint("cooling_off_retry_delay_sec", fallback=0)),
        runtime_timezone=runtime_cfg.get("timezone", fallback="Asia/Shanghai").strip(),
        account_id=account_id,
        protection_exempt_symbols=protection_exempt_symbols,
        mutation_lock=mutation_lock,
    )

    manager = PositionManager(
        client=client,
        store=scoped_store,
        notifier=notifier,
        sl_liq_buffer_pct=strategy_cfg.getfloat("sl_liq_buffer_pct", fallback=1.0),
        trigger_price_type=strategy_cfg.get("trigger_price_type", fallback="CONTRACT_PRICE").strip(),
        daily_loss_cut_scope=runtime_cfg.get("daily_loss_cut_scope", fallback="tracked").strip(),
        account_id=account_id,
        protection_exempt_symbols=protection_exempt_symbols,
        mutation_lock=mutation_lock,
    )

    wallet_sampler = WalletSnapshotSampler(
        client=client,
        store=scoped_store,
        asset=runtime_cfg.get("wallet_snapshot_asset", fallback="USDT").strip() or "USDT",
        sync_cashflows=runtime_cfg.getboolean("sync_cashflows", fallback=True),
        cashflow_income_types=[
            x.strip().upper()
            for x in runtime_cfg.get("cashflow_income_types", fallback="TRANSFER,WELCOME_BONUS").split(",")
            if x.strip()
        ],
        account_id=account_id,
    )

    return {
        "account_id": account_id,
        "mode": mode,
        "entry_hour": runtime_cfg.getint("entry_hour", fallback=7),
        "entry_minute": runtime_cfg.getint("entry_minute", fallback=40),
        "entry_initial_delay_sec": max(0, runtime_cfg.getint("entry_initial_delay_sec", fallback=0)),
        "entry_symbol_interval_sec": max(0, runtime_cfg.getint("entry_symbol_interval_sec", fallback=0)),
        "entry_wait_close_grace_sec": max(
            0.0, strategy_cfg.getfloat("entry_wait_close_grace_sec", fallback=1.0)
        ),
        "entry_wait_close_retry_sec": max(
            0.1, strategy_cfg.getfloat("entry_wait_close_retry_sec", fallback=1.0)
        ),
        "entry_wait_close_retry_count": max(
            1, strategy_cfg.getint("entry_wait_close_retry_count", fallback=5)
        ),
        "entry_preclose_sec": min(
            59.0, max(0.0, strategy_cfg.getfloat("entry_preclose_sec", fallback=10.0))
        ),
        "cooling_off_retry_count": max(0, runtime_cfg.getint("cooling_off_retry_count", fallback=0)),
        "cooling_off_retry_delay_sec": max(0, runtime_cfg.getint("cooling_off_retry_delay_sec", fallback=0)),
        "daily_loss_cut_enabled": runtime_cfg.getboolean("daily_loss_cut_enabled", fallback=True),
        "portfolio_loss_cut_enabled": runtime_cfg.getboolean(
            "portfolio_loss_cut_enabled", fallback=False
        ),
        "portfolio_loss_cut_pct": min(
            100.0,
            max(0.001, runtime_cfg.getfloat("portfolio_loss_cut_pct", fallback=3.5)),
        ),
        "portfolio_loss_cut_hour": runtime_cfg.getint("portfolio_loss_cut_hour", fallback=8),
        "portfolio_loss_cut_minute": runtime_cfg.getint("portfolio_loss_cut_minute", fallback=0),
        "noon_protection_enabled": runtime_cfg.getboolean("noon_protection_enabled", fallback=True),
        "morning_protection_enabled": runtime_cfg.getboolean("morning_protection_enabled", fallback=False),
        "morning_protection_hour": runtime_cfg.getint("morning_protection_hour", fallback=7),
        "morning_protection_minute": runtime_cfg.getint("morning_protection_minute", fallback=55),
        "morning_protection_min_hold_hours": runtime_cfg.getfloat(
            "morning_protection_min_hold_hours", fallback=6.0
        ),
        "hourly_exchange_take_profit_enabled": runtime_cfg.getboolean(
            "hourly_exchange_take_profit_enabled", fallback=False
        ),
        "hourly_exchange_take_profit_minute": runtime_cfg.getint(
            "hourly_exchange_take_profit_minute", fallback=0
        ),
        "hourly_exchange_take_profit_drop_pct": runtime_cfg.getfloat(
            "hourly_exchange_take_profit_drop_pct", fallback=18.0
        ),
        "protection_exempt_symbols": protection_exempt_symbols,
        "strategy": strategy,
        "manager": manager,
        "balance_sampler": wallet_sampler,
    }


def _serialize_config(cfg: configparser.ConfigParser) -> str:
    buf = io.StringIO()
    cfg.write(buf)
    return buf.getvalue()


def create_components(
    cfg: configparser.ConfigParser,
    base_dir: Optional[str] = None,
):
    runtime_global = cfg["runtime"]
    db_path = resolve_path(runtime_global.get("db_path", "state.db"), base_dir)
    schema_path = str((Path(__file__).resolve().parents[1] / "schema.sql").resolve())

    default_account_id = runtime_global.get("default_account_id", fallback="default").strip() or "default"
    root_store = StateStore(db_path=db_path, schema_path=schema_path, account_id=default_account_id)
    root_store.init_schema()

    account_runtimes: Dict[str, Dict[str, object]] = {}
    if cfg.has_section("accounts"):
        parsed_accounts = parse_account_settings(_serialize_config(cfg))
        for account_id, settings in parsed_accounts.items():
            try:
                account_runtimes[account_id] = _build_single_account_components(
                    account_id=account_id,
                    mode=settings.mode,
                    binance_cfg=_merged_section(cfg, account_id, "binance"),
                    strategy_cfg=_merged_section(cfg, account_id, "strategy"),
                    runtime_cfg=_merged_section(cfg, account_id, "runtime"),
                    notify_cfg=_merged_section(cfg, account_id, "notify"),
                    root_store=root_store,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Skip account due to invalid config account=%s error=%s", account_id, exc)

    if not account_runtimes:
        if cfg.has_section("accounts"):
            raise RuntimeError("No valid account runtime can be created from [accounts] configuration")

        account_id = default_account_id
        account_runtimes[account_id] = _build_single_account_components(
            account_id=account_id,
            mode="full",
            binance_cfg=_merged_section(cfg, account_id, "binance"),
            strategy_cfg=_merged_section(cfg, account_id, "strategy"),
            runtime_cfg=_merged_section(cfg, account_id, "runtime"),
            notify_cfg=_merged_section(cfg, account_id, "notify"),
            root_store=root_store,
        )

    selected_account_id = default_account_id if default_account_id in account_runtimes else next(iter(account_runtimes))
    selected_ctx = account_runtimes[selected_account_id]
    runtime_cfg_selected = _merged_section(cfg, selected_account_id, "runtime")

    service_cfg = ServiceRuntimeConfig(
        timezone_name=runtime_cfg_selected.get("timezone", fallback="Asia/Shanghai").strip(),
        entry_hour=runtime_cfg_selected.getint("entry_hour", fallback=7),
        entry_minute=runtime_cfg_selected.getint("entry_minute", fallback=40),
        entry_misfire_grace_min=runtime_cfg_selected.getint("entry_misfire_grace_min", fallback=120),
        entry_catchup_enabled=runtime_cfg_selected.getboolean("entry_catchup_enabled", fallback=True),
        daily_loss_cut_enabled=runtime_cfg_selected.getboolean("daily_loss_cut_enabled", fallback=True),
        daily_loss_cut_hour=runtime_cfg_selected.getint("daily_loss_cut_hour", fallback=11),
        daily_loss_cut_minute=runtime_cfg_selected.getint("daily_loss_cut_minute", fallback=55),
        daily_loss_cut_grace_min=max(
            1,
            runtime_cfg_selected.getint("daily_loss_cut_grace_min", fallback=30),
        ),
        portfolio_loss_cut_enabled=runtime_cfg_selected.getboolean(
            "portfolio_loss_cut_enabled", fallback=False
        ),
        portfolio_loss_cut_pct=min(
            100.0,
            max(0.001, runtime_cfg_selected.getfloat("portfolio_loss_cut_pct", fallback=3.5)),
        ),
        portfolio_loss_cut_hour=runtime_cfg_selected.getint("portfolio_loss_cut_hour", fallback=8),
        portfolio_loss_cut_minute=runtime_cfg_selected.getint("portfolio_loss_cut_minute", fallback=0),
        noon_protection_enabled=runtime_cfg_selected.getboolean("noon_protection_enabled", fallback=True),
        noon_protection_hour=runtime_cfg_selected.getint("noon_protection_hour", fallback=12),
        noon_protection_minute=runtime_cfg_selected.getint("noon_protection_minute", fallback=0),
        noon_protection_retry_interval_sec=runtime_cfg_selected.getfloat(
            "noon_protection_retry_interval_sec", fallback=60.0
        ),
        morning_protection_enabled=runtime_cfg_selected.getboolean("morning_protection_enabled", fallback=False),
        morning_protection_hour=runtime_cfg_selected.getint("morning_protection_hour", fallback=7),
        morning_protection_minute=runtime_cfg_selected.getint("morning_protection_minute", fallback=55),
        morning_protection_min_hold_hours=runtime_cfg_selected.getfloat(
            "morning_protection_min_hold_hours", fallback=6.0
        ),
        hourly_exchange_take_profit_enabled=runtime_cfg_selected.getboolean(
            "hourly_exchange_take_profit_enabled", fallback=False
        ),
        hourly_exchange_take_profit_minute=runtime_cfg_selected.getint(
            "hourly_exchange_take_profit_minute", fallback=0
        ),
        hourly_exchange_take_profit_drop_pct=runtime_cfg_selected.getfloat(
            "hourly_exchange_take_profit_drop_pct", fallback=18.0
        ),
        orphan_exit_order_cleanup_enabled=runtime_cfg_selected.getboolean(
            "orphan_exit_order_cleanup_enabled", fallback=True
        ),
        orphan_exit_order_cleanup_hour=runtime_cfg_selected.getint(
            "orphan_exit_order_cleanup_hour", fallback=3
        ),
        orphan_exit_order_cleanup_minute=runtime_cfg_selected.getint(
            "orphan_exit_order_cleanup_minute", fallback=30
        ),
        manager_interval_sec=max(1, runtime_cfg_selected.getint("manager_interval_sec", fallback=60)),
        manager_max_catch_up_runs=max(1, runtime_cfg_selected.getint("manager_max_catch_up_runs", fallback=3)),
        loop_sleep_sec=max(0.2, runtime_cfg_selected.getfloat("service_loop_sleep_sec", fallback=1.0)),
        run_manage_on_startup=runtime_cfg_selected.getboolean("run_manage_on_startup", fallback=True),
        max_account_workers=max(1, runtime_cfg_selected.getint("max_account_workers", fallback=1)),
        account_failure_threshold=max(1, runtime_cfg_selected.getint("account_failure_threshold", fallback=3)),
        account_cooldown_cycles=max(1, runtime_cfg_selected.getint("account_cooldown_cycles", fallback=2)),
        account_task_timeout_sec=max(0.1, runtime_cfg_selected.getfloat("account_task_timeout_sec", fallback=30.0)),
        readonly_wallet_snapshot_interval_sec=max(
            1.0,
            runtime_cfg_selected.getfloat("readonly_wallet_snapshot_interval_sec", fallback=60.0),
        ),
    )

    return (
        selected_ctx["strategy"],
        selected_ctx["manager"],
        selected_ctx["balance_sampler"],
        cfg["runtime"],
        service_cfg,
        account_runtimes,
    )
