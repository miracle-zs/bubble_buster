"""Unit tests for CentralExitExecutor."""

from unittest.mock import MagicMock
import pytest

from core.risk.executor import CentralExitExecutor, is_immediate_trigger_error
from core.risk.models import ExitActionType, ExitIntent
from infra.binance_futures_client import BinanceAPIError


class TestImmediateTriggerError:
    def test_identifies_2021_error_code(self) -> None:
        exc = BinanceAPIError(code=-2021, message="Order would immediately trigger.")
        assert is_immediate_trigger_error(exc) is True

    def test_identifies_immediate_trigger_message(self) -> None:
        exc = BinanceAPIError(code=-1000, message="Price would immediately trigger liquidation")
        assert is_immediate_trigger_error(exc) is True

    def test_returns_false_for_other_errors(self) -> None:
        exc = BinanceAPIError(code=-2019, message="Margin is insufficient.")
        assert is_immediate_trigger_error(exc) is False


class TestCentralExitExecutor:
    def test_execute_stop_loss_intent_success(self) -> None:
        client = MagicMock()
        client.format_order_qty.return_value = "10.0"
        client.format_trigger_price.return_value = "9.50"
        client.create_order.return_value = {"orderId": 888, "clientOrderId": "nsl-test"}

        store = MagicMock()
        reconciler = MagicMock()

        executor = CentralExitExecutor(
            client=client,
            store=store,
            market_fill_reconciler=reconciler,
            new_client_id_fn=lambda tag, sym: f"{tag}-{sym}",
            now_iso_fn=lambda: "2026-09-25T12:00:00Z",
        )

        intent = ExitIntent(
            symbol="BTCUSDT",
            action=ExitActionType.UPDATE_STOP_LOSS,
            reason="NOON_PROTECTION",
            target_price=9.50,
            qty=10.0,
            close_side="BUY",
            position_id=123,
        )

        tracked_pos = {
            "symbol": "BTCUSDT",
            "sl_order_id": 777,
            "sl_client_order_id": "sl-old",
        }

        res = executor.execute_stop_loss_intent(
            intent=intent,
            tracked_pos=tracked_pos,
            liquidation_price=12.0,
        )

        assert res.status == "UPDATED"
        assert res.sl_order == {"orderId": 888, "clientOrderId": "nsl-test"}

        # Verify new order created
        client.create_order.assert_called_once()
        # Verify store updated
        store.update_stop_loss.assert_called_once_with(
            position_id=123,
            sl_order_id=888,
            sl_client_order_id="nsl-test",
            sl_price=9.50,
            liq_price_latest=12.0,
        )
        store.add_order_event.assert_called_once()
        # Verify old order canceled
        client.cancel_order.assert_called_once_with(
            symbol="BTCUSDT",
            order_id=777,
            orig_client_order_id="sl-old",
        )

    def test_execute_stop_loss_immediate_trigger_fallback(self) -> None:
        client = MagicMock()
        client.format_order_qty.return_value = "10.0"
        client.format_trigger_price.return_value = "9.50"
        # First call (stop order) raises -2021, second call (market close) succeeds
        client.create_order.side_effect = [
            BinanceAPIError(code=-2021, message="Order would immediately trigger."),
            {"orderId": 999, "clientOrderId": "nsi-test", "status": "FILLED", "executedQty": "10"},
        ]

        store = MagicMock()
        reconciler = MagicMock()

        executor = CentralExitExecutor(
            client=client,
            store=store,
            market_fill_reconciler=reconciler,
            new_client_id_fn=lambda tag, sym: f"{tag}-{sym}",
            now_iso_fn=lambda: "2026-09-25T12:00:00Z",
        )

        intent = ExitIntent(
            symbol="BTCUSDT",
            action=ExitActionType.UPDATE_STOP_LOSS,
            reason="NOON_PROTECTION",
            target_price=9.50,
            qty=10.0,
            close_side="BUY",
            position_id=123,
        )

        tracked_pos = {
            "symbol": "BTCUSDT",
            "tp_order_id": 666,
            "tp_client_order_id": "tp-old",
            "sl_order_id": 777,
            "sl_client_order_id": "sl-old",
        }

        res = executor.execute_stop_loss_intent(
            intent=intent,
            tracked_pos=tracked_pos,
        )

        assert res.status == "CLOSED_IMMEDIATE"
        assert res.close_info is not None
        assert res.close_info["close_order_id"] == 999

        # Reconciler and mark_position_closed called
        reconciler.record_market_order.assert_called_once()
        store.mark_position_closed.assert_called_once()

    def test_close_position_cancels_exit_orders_and_records_close(self) -> None:
        client = MagicMock()
        client.create_order.return_value = {"orderId": 888, "status": "FILLED"}
        client.format_order_qty.side_effect = lambda sym, qty: str(qty)
        store = MagicMock()
        reconciler = MagicMock()

        executor = CentralExitExecutor(
            client=client,
            store=store,
            market_fill_reconciler=reconciler,
            new_client_id_fn=lambda tag, sym: f"{tag}-{sym}",
            now_iso_fn=lambda: "2026-09-25T12:00:00Z",
        )

        cancel_pos = {
            "symbol": "ETHUSDT",
            "tp_order_id": 111,
            "sl_order_id": 222,
        }

        res = executor.close_position(
            symbol="ETHUSDT",
            qty=5.0,
            side="BUY",
            position_id=55,
            cancel_pos=cancel_pos,
            close_status="CLOSED_DAILY_LOSS_CUT",
            close_reason="DAILY_FLOATING_LOSS_CHECK",
            client_id_tag="dl",
        )

        assert res["qty"] == 5.0
        assert res["close_order_id"] == 888
        reconciler.record_market_order.assert_called_once()
        store.mark_position_closed.assert_called_once_with(
            position_id=55,
            status="CLOSED_DAILY_LOSS_CUT",
            close_reason="DAILY_FLOATING_LOSS_CHECK",
            close_order_id=888,
        )
        assert client.cancel_order.call_count == 2



def test_unknown_market_close_blocks_new_intent_until_reconciled(tmp_path):
    from pathlib import Path
    from core.state_store import StateStore
    from infra.binance_futures_client import OrderStateUnknownError

    store = StateStore(str(tmp_path / "state.db"), str(Path(__file__).resolve().parents[1] / "schema.sql"), account_id="acc01")
    store.init_schema()
    client = MagicMock()
    client.format_order_qty.return_value = "1"
    client.create_order.side_effect = [
        OrderStateUnknownError("BTCUSDT", "close-1", BinanceAPIError(-1000, "timeout")),
        {"orderId": 7, "status": "FILLED", "executedQty": "1", "avgPrice": "10"},
    ]
    executor = CentralExitExecutor(client, store, market_fill_reconciler=MagicMock())
    with pytest.raises(OrderStateUnknownError):
        executor.close_market_order("BTCUSDT", qty=1, client_order_id="close-1")
    with pytest.raises(OrderStateUnknownError):
        executor.close_market_order("BTCUSDT", qty=1, client_order_id="close-2")
    assert client.create_order.call_count == 1
    assert store.find_unresolved_market_exit("ETHUSDT", "BUY", "new") is None
    assert store.scoped("acc02").find_unresolved_market_exit("BTCUSDT", "BUY", "new") is None
    with store._connect_ctx() as conn:
        conn.execute("UPDATE order_attempts SET status = 'REJECTED' WHERE client_order_id = ?", ("close-1",))
    executor.close_market_order("BTCUSDT", qty=1, client_order_id="close-2")
    assert client.create_order.call_count == 2
