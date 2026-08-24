import logging
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core.account_snapshot import AccountSnapshot, AccountSnapshotProvider
from core.state_store import StateStore
from infra.binance_futures_client import BinanceFuturesClient

LOGGER = logging.getLogger(__name__)


class WalletSnapshotSampler:
    """Persist account equity snapshots on a fixed scheduler cadence."""

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        asset: str = "USDT",
        sync_cashflows: bool = False,
        cashflow_income_types: Optional[List[str]] = None,
        account_id: str = "default",
        snapshot_provider: Optional[AccountSnapshotProvider] = None,
        cashflow_overlap_minutes: int = 20,
        cashflow_inline: bool = True,
    ):
        self.client = client
        self.store = store
        self.asset = asset.upper().strip() or "USDT"
        self.account_id = (account_id or "").strip() or "default"
        self.sync_cashflows = bool(sync_cashflows)
        self.snapshot_provider = snapshot_provider
        self.cashflow_overlap_minutes = min(30, max(10, int(cashflow_overlap_minutes)))
        self.cashflow_inline = bool(cashflow_inline)
        income_types = cashflow_income_types or ["TRANSFER", "WELCOME_BONUS"]
        self.cashflow_income_types = [str(x).upper().strip() for x in income_types if str(x).strip()]

    def run_once(self, account_snapshot: Optional[AccountSnapshot] = None) -> Dict[str, object]:
        captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        snapshot_error: Optional[str] = None
        snapshot = account_snapshot
        if snapshot is None and self.snapshot_provider is not None:
            snapshot = self.snapshot_provider.capture()

        if snapshot is not None:
            captured_at = snapshot.captured_at_utc
            wallet_balance_usdt = float(snapshot.wallet_balance)
            unrealized_pnl_usdt = float(snapshot.unrealized_pnl)
            equity_usdt = float(snapshot.equity)
            available_balance_usdt = float(snapshot.available_balance)
        else:
            # Compatibility adapter for isolated tests/legacy callers.  Runtime
            # wiring always supplies AccountSnapshotProvider, so production does
            # not execute these two independent account reads.
            balances = self.client.get_balance()
            wallet_balance_usdt = self._extract_balance(balances)
            unrealized_pnl_usdt = 0.0
            try:
                position_rows = self.client.get_position_risk()
                unrealized_pnl_usdt = self._extract_unrealized_pnl(position_rows)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to fetch position risk for equity snapshot, fallback wallet balance only: %s", exc)
                snapshot_error = f"position_risk: {exc}"
            equity_usdt = wallet_balance_usdt + unrealized_pnl_usdt
            available_balance_usdt = self._extract_available_balance(balances)
        snapshot_id = self.store.add_wallet_snapshot(
            captured_at_utc=captured_at,
            balance_usdt=equity_usdt,
            source="API",
            error=snapshot_error,
        )
        cashflow_added = 0
        if self.sync_cashflows and self.cashflow_inline:
            cashflow_added = self.sync_cashflows_once()
        return {
            "snapshot_id": snapshot_id,
            "asset": self.asset,
            "wallet_balance": round(wallet_balance_usdt, 8),
            "unrealized_pnl": round(unrealized_pnl_usdt, 8),
            "equity": round(equity_usdt, 8),
            "balance": round(equity_usdt, 8),
            "available_balance": round(available_balance_usdt, 8),
            "captured_at_utc": captured_at,
            "cashflow_added": cashflow_added,
        }

    def _extract_balance(self, balances: list) -> float:
        for item in balances:
            if str(item.get("asset", "")).upper() != self.asset:
                continue
            raw = item.get("balance")
            if raw is None:
                raw = item.get("crossWalletBalance")
            if raw is None:
                raw = item.get("availableBalance")
            return float(raw or 0.0)
        raise ValueError(f"{self.asset} balance not found from /fapi/v2/balance")

    def _extract_unrealized_pnl(self, positions: list) -> float:
        total = 0.0
        for row in positions or []:
            # USD-M futures positionRisk is denominated in USDT for USDT contracts.
            value = row.get("unRealizedProfit")
            if value is None:
                continue
            total += float(value or 0.0)
        return total

    def _extract_available_balance(self, balances: list) -> float:
        for item in balances:
            if str(item.get("asset", "")).upper() == self.asset:
                return float(item.get("availableBalance") or 0.0)
        return 0.0

    def sync_cashflows_once(self, now_utc: Optional[datetime] = None) -> int:
        """Fetch one unfiltered income page and apply local type filtering."""
        if not self.cashflow_income_types:
            return 0

        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        request_now_ms = int(now.timestamp() * 1000)
        cursor_state = self.store.get_lock_state("cashflow_income_cursor_v2") or {}
        draining_full_page = bool(cursor_state.get("draining_full_page"))
        if draining_full_page:
            try:
                start_ms = int(
                    cursor_state.get("drain_start_ms")
                    or cursor_state.get("cursor_ms")
                    or 0
                )
                end_ms = int(cursor_state.get("drain_end_ms") or request_now_ms)
                page = max(1, int(cursor_state.get("drain_page") or 1))
            except (TypeError, ValueError):
                start_ms = self._resolve_cashflow_start_ms()
                end_ms = request_now_ms
                page = 1
                draining_full_page = False
        else:
            start_ms = self._resolve_cashflow_start_ms()
            end_ms = request_now_ms
            page = 1
        inserted = 0
        background_scope = getattr(self.client, "background_requests", None)
        with background_scope() if callable(background_scope) else nullcontext():
            rows = self.client.get_income_history(
                income_type=None,
                start_time=start_ms,
                end_time=end_ms,
                page=page,
                limit=1000,
            )
        rows_sorted = sorted(rows or [], key=lambda x: int(x.get("time") or 0))
        for row in rows_sorted:
            income_type = str(row.get("incomeType", "")).upper().strip()
            if income_type not in self.cashflow_income_types:
                continue
            asset = str(row.get("asset", "")).upper().strip()
            if asset != self.asset:
                continue
            event_ts_ms = int(row.get("time") or 0)
            if event_ts_ms <= 0:
                continue
            event_time_utc = datetime.fromtimestamp(event_ts_ms / 1000, tz=timezone.utc).replace(
                microsecond=0
            ).isoformat()
            created = self.store.add_cashflow_event(
                event_time_utc=event_time_utc,
                asset=asset,
                amount=float(row.get("income") or 0.0),
                income_type=income_type,
                symbol=str(row.get("symbol") or "").upper().strip() or None,
                tran_id=str(row.get("tranId") or "").strip() or None,
                info=str(row.get("info") or "").strip() or None,
                raw_json=row,
            )
            if created:
                inserted += 1

        # A short page proves the fixed interval was fully observed.  For a full
        # page, persist both interval and next page so the following minute can
        # continue without a second request now.  A page cursor also avoids
        # dropping records when more than 1000 events share one millisecond.
        full_page = len(rows_sorted) >= 1000
        previous_cursor_ms = int(cursor_state.get("cursor_ms") or 0)
        cursor_ms = end_ms if not full_page else previous_cursor_ms
        self.store.set_lock_state(
            "cashflow_income_cursor_v2",
            {
                "cursor_ms": int(cursor_ms),
                "draining_full_page": bool(full_page),
                "drain_start_ms": int(start_ms) if full_page else None,
                "drain_end_ms": int(end_ms) if full_page else None,
                "drain_page": int(page + 1) if full_page else None,
                "last_request_start_ms": int(start_ms),
                "last_request_end_ms": int(end_ms),
                "last_request_page": int(page),
                "last_row_count": len(rows_sorted),
                "updated_at_utc": now.replace(microsecond=0).isoformat(),
            },
        )
        return inserted

    # Backwards-compatible private name used by a few older callers/tests.
    def _sync_cashflows(self) -> int:
        return self.sync_cashflows_once()

    def _resolve_cashflow_start_ms(self, income_type: Optional[str] = None) -> int:
        cursor_state = self.store.get_lock_state("cashflow_income_cursor_v2") or {}
        try:
            cursor_ms = int(cursor_state.get("cursor_ms") or 0)
        except (TypeError, ValueError):
            cursor_ms = 0
        overlap_ms = self.cashflow_overlap_minutes * 60_000
        if cursor_ms > 0:
            if bool(cursor_state.get("draining_full_page")):
                # Continue a saturated interval across minutes. Once a short
                # page catches up, normal overlapped reads resume.
                return cursor_ms
            return max(0, cursor_ms - overlap_ms)

        latest_cashflow = self.store.get_latest_cashflow_event_time(asset=self.asset)
        if latest_cashflow:
            return max(0, int(_parse_iso_utc(latest_cashflow).timestamp() * 1000) - overlap_ms)

        earliest_snapshot = self.store.get_earliest_wallet_snapshot_time()
        if earliest_snapshot:
            dt = _parse_iso_utc(earliest_snapshot)
            return max(0, int(dt.timestamp() * 1000) - overlap_ms)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return max(0, now_ms - overlap_ms)


def _parse_iso_utc(text: str) -> datetime:
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
