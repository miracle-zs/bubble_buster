import configparser

from core.runtime_components import create_components


def test_create_components_skips_invalid_account_and_keeps_valid_accounts(tmp_path) -> None:
    cfg_path = tmp_path / "config.ini"
    db_path = tmp_path / "state.db"
    cfg_path.write_text(
        f"""
[accounts]
enabled = good,bad
mode.good = full
mode.bad = full

[binance]
api_key =
api_secret =
base_url = https://fapi.binance.com
timeout_sec = 10
retry_count = 3
retry_delay_sec = 1
recv_window = 5000
http_pool_maxsize = 64

[account.good.binance]
api_key = good_key
api_secret = good_secret

[strategy]
leverage = 2
top_n = 10

[runtime]
db_path = {db_path}
default_account_id = good
timezone = UTC
entry_hour = 7
entry_minute = 40
manager_interval_sec = 60

[notify]
enabled = false
serverchan_sendkey =
""",
        encoding="utf-8",
    )

    cfg = configparser.ConfigParser()
    assert cfg.read(str(cfg_path))

    strategy, manager, wallet_sampler, runtime_cfg, service_cfg, account_runtimes = create_components(
        cfg,
        base_dir=str(tmp_path),
    )

    assert "good" in account_runtimes
    assert "bad" not in account_runtimes
    assert strategy is account_runtimes["good"]["strategy"]
    assert manager is account_runtimes["good"]["manager"]
    assert wallet_sampler is account_runtimes["good"]["balance_sampler"]
    assert runtime_cfg.get("default_account_id") == "good"
    assert service_cfg.max_account_workers >= 1
