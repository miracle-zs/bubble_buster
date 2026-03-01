import configparser
import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path

if importlib.util.find_spec("requests") is None:
    requests_stub = types.ModuleType("requests")

    class _DummySession:
        def __init__(self):
            self.headers = {}
            self.proxies = {}

        def mount(self, *_args, **_kwargs):
            return None

    class _DummyRequestException(Exception):
        pass

    requests_stub.Session = _DummySession
    requests_stub.RequestException = _DummyRequestException

    adapters_stub = types.ModuleType("requests.adapters")

    class _DummyHTTPAdapter:
        def __init__(self, *args, **kwargs):
            pass

    adapters_stub.HTTPAdapter = _DummyHTTPAdapter
    requests_stub.adapters = adapters_stub
    sys.modules["requests"] = requests_stub
    sys.modules["requests.adapters"] = adapters_stub

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


def test_create_components_applies_per_account_daily_loss_cut_enabled_override(tmp_path) -> None:
    cfg_path = tmp_path / "config.ini"
    db_path = tmp_path / "state.db"
    cfg_path.write_text(
        f"""
[accounts]
enabled = acc01,55
mode.acc01 = full
mode.55 = loss_cut_only

[binance]
api_key =
api_secret =
base_url = https://fapi.binance.com
timeout_sec = 10
retry_count = 3
retry_delay_sec = 1
recv_window = 5000
http_pool_maxsize = 64

[account.acc01.binance]
api_key = key1
api_secret = sec1

[account.55.binance]
api_key = key55
api_secret = sec55

[strategy]
leverage = 2
top_n = 10

[runtime]
db_path = {db_path}
default_account_id = acc01
timezone = UTC
entry_hour = 7
entry_minute = 40
manager_interval_sec = 60
daily_loss_cut_enabled = true

[account.55.runtime]
daily_loss_cut_enabled = false

[notify]
enabled = false
serverchan_sendkey =
""",
        encoding="utf-8",
    )

    cfg = configparser.ConfigParser()
    assert cfg.read(str(cfg_path))

    _, _, _, _, _, account_runtimes = create_components(
        cfg,
        base_dir=str(tmp_path),
    )

    assert account_runtimes["acc01"]["daily_loss_cut_enabled"] is True
    assert account_runtimes["55"]["daily_loss_cut_enabled"] is False


def test_create_components_exposes_account_runtime_entry_schedule_override(tmp_path) -> None:
    cfg_path = tmp_path / "config.ini"
    db_path = tmp_path / "state.db"
    cfg_path.write_text(
        f"""
[accounts]
enabled = acc01,acc02
mode.acc01 = full
mode.acc02 = full

[binance]
api_key =
api_secret =
base_url = https://fapi.binance.com
timeout_sec = 10
retry_count = 3
retry_delay_sec = 1
recv_window = 5000
http_pool_maxsize = 64

[account.acc01.binance]
api_key = key1
api_secret = sec1

[account.acc02.binance]
api_key = key2
api_secret = sec2

[strategy]
leverage = 2
top_n = 10

[runtime]
db_path = {db_path}
default_account_id = acc01
timezone = UTC
entry_hour = 7
entry_minute = 40
manager_interval_sec = 60

[account.acc02.runtime]
entry_hour = 7
entry_minute = 45

[notify]
enabled = false
serverchan_sendkey =
""",
        encoding="utf-8",
    )

    cfg = configparser.ConfigParser()
    assert cfg.read(str(cfg_path))

    _, _, _, _, _, account_runtimes = create_components(
        cfg,
        base_dir=str(tmp_path),
    )

    assert account_runtimes["acc01"]["entry_hour"] == 7
    assert account_runtimes["acc01"]["entry_minute"] == 40
    assert account_runtimes["acc02"]["entry_hour"] == 7
    assert account_runtimes["acc02"]["entry_minute"] == 45


class RuntimeComponentsEntryPacingTest(unittest.TestCase):
    def test_create_components_exposes_account_entry_symbol_interval_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cfg_path = tmp_path / "config.ini"
            db_path = tmp_path / "state.db"
            cfg_path.write_text(
                f"""
[accounts]
enabled = acc01,acc02
mode.acc01 = full
mode.acc02 = full

[binance]
api_key =
api_secret =
base_url = https://fapi.binance.com
timeout_sec = 10
retry_count = 3
retry_delay_sec = 1
recv_window = 5000
http_pool_maxsize = 64

[account.acc01.binance]
api_key = key1
api_secret = sec1

[account.acc02.binance]
api_key = key2
api_secret = sec2

[strategy]
leverage = 2
top_n = 10

[runtime]
db_path = {db_path}
default_account_id = acc01
timezone = UTC
entry_hour = 7
entry_minute = 40
manager_interval_sec = 60

[account.acc02.runtime]
entry_hour = 7
entry_minute = 45
entry_symbol_interval_sec = 30

[notify]
enabled = false
serverchan_sendkey =
""",
                encoding="utf-8",
            )

            cfg = configparser.ConfigParser()
            self.assertTrue(cfg.read(str(cfg_path)))

            _, _, _, _, _, account_runtimes = create_components(
                cfg,
                base_dir=str(tmp_path),
            )

            self.assertEqual(account_runtimes["acc01"]["entry_symbol_interval_sec"], 0)
            self.assertEqual(account_runtimes["acc02"]["entry_symbol_interval_sec"], 30)
            self.assertEqual(account_runtimes["acc02"]["strategy"].entry_symbol_interval_sec, 30)
