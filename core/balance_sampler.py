import logging
from datetime import datetime, timezone
from typing import Dict

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
    ):
        self.client = client
        self.store = store
        self.asset = asset.upper().strip() or "USDT"

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
        return {
            "snapshot_id": snapshot_id,
            "asset": self.asset,
            "balance": round(balance_usdt, 8),
            "captured_at_utc": captured_at,
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
