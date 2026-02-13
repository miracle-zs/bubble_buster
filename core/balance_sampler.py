import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core.state_store import StateStore
from infra.binance_futures_client import BinanceFuturesClient

LOGGER = logging.getLogger(__name__)


class WalletSnapshotSampler:
    """Persist wallet balance snapshots on a fixed scheduler cadence."""

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        asset: str = "USDT",
        sync_cashflows: bool = False,
        cashflow_income_types: Optional[List[str]] = None,
    ):
        self.client = client
        self.store = store
        self.asset = asset.upper().strip() or "USDT"
        self.sync_cashflows = bool(sync_cashflows)
        income_types = cashflow_income_types or ["TRANSFER", "WELCOME_BONUS"]
        self.cashflow_income_types = [str(x).upper().strip() for x in income_types if str(x).strip()]

    def run_once(self) -> Dict[str, object]:
        captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        balances = self.client.get_balance()
        balance_usdt = self._extract_balance(balances)
        snapshot_id = self.store.add_wallet_snapshot(
            captured_at_utc=captured_at,
            balance_usdt=balance_usdt,
            source="API",
            error=None,
        )
        cashflow_added = 0
        if self.sync_cashflows:
            cashflow_added = self._sync_cashflows()
        return {
            "snapshot_id": snapshot_id,
            "asset": self.asset,
            "balance": round(balance_usdt, 8),
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

    def _sync_cashflows(self) -> int:
        if not self.cashflow_income_types:
            return 0

        start_ms = self._resolve_cashflow_start_ms()
        inserted = 0
        guard = 0
        while guard < 20:
            guard += 1
            rows = self.client.get_income_history(start_time=start_ms, limit=1000)
            if not rows:
                break
            rows_sorted = sorted(rows, key=lambda x: int(x.get("time") or 0))
            max_time = start_ms
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
                amount = float(row.get("income") or 0.0)
                created = self.store.add_cashflow_event(
                    event_time_utc=event_time_utc,
                    asset=asset,
                    amount=amount,
                    income_type=income_type,
                    symbol=str(row.get("symbol") or "").upper().strip() or None,
                    tran_id=str(row.get("tranId") or "").strip() or None,
                    info=str(row.get("info") or "").strip() or None,
                    raw_json=row,
                )
                if created:
                    inserted += 1
                if event_ts_ms > max_time:
                    max_time = event_ts_ms
            if len(rows_sorted) < 1000 or max_time <= start_ms:
                break
            start_ms = max_time + 1
        return inserted

    def _resolve_cashflow_start_ms(self) -> int:
        latest_cashflow = self.store.get_latest_cashflow_event_time(asset=self.asset)
        if latest_cashflow:
            dt = _parse_iso_utc(latest_cashflow)
            return max(0, int(dt.timestamp() * 1000) - 60_000)

        earliest_snapshot = self.store.get_earliest_wallet_snapshot_time()
        if earliest_snapshot:
            dt = _parse_iso_utc(earliest_snapshot)
            return max(0, int(dt.timestamp() * 1000) - 3_600_000)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return max(0, now_ms - 7 * 24 * 3600 * 1000)


def _parse_iso_utc(text: str) -> datetime:
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
