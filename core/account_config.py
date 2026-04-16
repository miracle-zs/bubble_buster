import configparser
from dataclasses import dataclass
from typing import Dict


VALID_ACCOUNT_MODES = {"full", "loss_cut_only", "readonly"}


@dataclass(frozen=True)
class AccountSettings:
    account_id: str
    mode: str
    binance: Dict[str, str]


def _normalize_items(section: configparser.SectionProxy) -> Dict[str, str]:
    return {k: v.strip() for k, v in section.items()}


def parse_account_settings(cfg_text: str) -> Dict[str, AccountSettings]:
    cfg = configparser.ConfigParser()
    cfg.read_string(cfg_text)

    if not cfg.has_section("accounts"):
        raise ValueError("Missing required [accounts] section")
    if not cfg.has_section("binance"):
        raise ValueError("Missing required [binance] section")

    enabled_raw = cfg.get("accounts", "enabled", fallback="")
    account_ids = [x.strip() for x in enabled_raw.split(",") if x.strip()]
    if not account_ids:
        raise ValueError("accounts.enabled must contain at least one account_id")

    base_binance = _normalize_items(cfg["binance"])
    result: Dict[str, AccountSettings] = {}
    for account_id in account_ids:
        mode = cfg.get("accounts", f"mode.{account_id}", fallback="full").strip() or "full"
        if mode not in VALID_ACCOUNT_MODES:
            raise ValueError(f"Invalid mode for account={account_id}: {mode}")

        merged_binance = dict(base_binance)
        account_binance_section = f"account.{account_id}.binance"
        if cfg.has_section(account_binance_section):
            merged_binance.update(_normalize_items(cfg[account_binance_section]))

        result[account_id] = AccountSettings(
            account_id=account_id,
            mode=mode,
            binance=merged_binance,
        )

    return result
