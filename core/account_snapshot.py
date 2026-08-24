"""One deep module for the account-state read shared by runtime callers."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.state_store import StateStore
from infra.binance_futures_client import BinanceFuturesClient


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else float(default)
    except (TypeError, ValueError):
        return float(default)


@dataclass(frozen=True)
class AccountSnapshot:
    account_id: str
    captured_at_utc: str
    wallet_balance: float
    unrealized_pnl: float
    equity: float
    available_balance: float
    positions: tuple[Dict[str, Any], ...]
    raw: Dict[str, Any]

    def position_risk_by_symbol(self) -> Dict[str, Dict[str, Any]]:
        """Select the short/negative row for each symbol, with a stable fallback."""
        result: Dict[str, Dict[str, Any]] = {}
        for row in self.positions:
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            existing = result.get(symbol)
            position_side = str(row.get("positionSide") or "BOTH").strip().upper()
            position_amt = _number(row.get("positionAmt"))
            if existing is None or position_side == "SHORT" or position_amt < 0:
                result[symbol] = row
        return result


class AccountSnapshotProvider:
    """Fetch, normalize, cache and persist at most one account read per UTC minute.

    The external interface deliberately exposes a single operation.  Wallet
    sampling, portfolio protection and position management all consume the
    returned immutable snapshot instead of learning Binance endpoint details.
    """

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        *,
        account_id: str,
        asset: str = "USDT",
    ) -> None:
        self.client = client
        self.store = store
        self.account_id = str(account_id or "default").strip() or "default"
        self.asset = str(asset or "USDT").strip().upper() or "USDT"
        self._lock = threading.RLock()
        self._cached_minute: Optional[str] = None
        self._cached: Optional[AccountSnapshot] = None

    def capture(
        self,
        *,
        force: bool = False,
        now_utc: Optional[datetime] = None,
    ) -> AccountSnapshot:
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        minute_key = now.strftime("%Y-%m-%dT%H:%M")
        with self._lock:
            if not force and self._cached is not None and self._cached_minute == minute_key:
                return self._cached

            payload = self.client.get_account()
            if (
                not isinstance(payload, dict)
                or not isinstance(payload.get("assets"), list)
                or not isinstance(payload.get("positions"), list)
            ):
                raise ValueError("invalid /fapi/v3/account snapshot payload")
            previous_positions = self.store.list_account_position_state()
            snapshot = self._from_payload(
                payload,
                captured_at=now,
                fallback_positions=previous_positions,
            )
            self.store.replace_account_state(
                captured_at_utc=snapshot.captured_at_utc,
                wallet_balance=snapshot.wallet_balance,
                unrealized_pnl=snapshot.unrealized_pnl,
                equity=snapshot.equity,
                available_balance=snapshot.available_balance,
                positions=list(snapshot.positions),
                raw_json=payload,
                stream_status="REST",
            )
            self._cached = snapshot
            self._cached_minute = minute_key
            return snapshot

    def invalidate(self) -> None:
        with self._lock:
            self._cached_minute = None

    def cached(self) -> Optional[AccountSnapshot]:
        with self._lock:
            return self._cached

    def merge_position_risks(
        self,
        rows: List[Dict[str, Any]],
        *,
        captured_at_utc: Optional[str] = None,
    ) -> Optional[AccountSnapshot]:
        """Merge one all-symbol positionRisk verification into local state.

        ``/fapi/v3/account`` is the minute snapshot source, but its position
        rows do not carry every risk field (notably liquidation/mark price).
        Startup/reconnect/5-minute REST verification supplies those fields in
        one all-symbol request and this method retains them for later minute
        snapshots without introducing symbol-level reads.
        """
        captured_at = captured_at_utc or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        normalized_rows = [dict(row) for row in rows or [] if isinstance(row, dict)]
        self.store.upsert_account_position_updates(
            normalized_rows,
            captured_at_utc=captured_at,
        )
        with self._lock:
            if self._cached is None:
                return None
            risk_by_key = {
                self._position_key(row): row
                for row in normalized_rows
                if self._position_key(row)[0]
            }
            merged_positions: List[Dict[str, Any]] = []
            seen: set[Tuple[str, str]] = set()
            for position in self._cached.positions:
                key = self._position_key(position)
                risk = risk_by_key.get(key)
                merged_positions.append(self._merge_position_fields(position, risk))
                seen.add(key)
            for key, risk in risk_by_key.items():
                if key not in seen:
                    merged_positions.append(dict(risk))
            cached = self._cached
            self._cached = AccountSnapshot(
                account_id=cached.account_id,
                captured_at_utc=cached.captured_at_utc,
                wallet_balance=cached.wallet_balance,
                unrealized_pnl=cached.unrealized_pnl,
                equity=cached.equity,
                available_balance=cached.available_balance,
                positions=tuple(merged_positions),
                raw=cached.raw,
            )
            return self._cached

    def apply_stream_update(
        self,
        *,
        balances: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        captured_at_utc: str,
    ) -> Optional[AccountSnapshot]:
        """Merge an ``ACCOUNT_UPDATE`` into storage and the minute cache.

        The user stream carries only changed rows.  StateStore first merges that
        subset into the complete local ledger; this method then refreshes the
        already-captured minute snapshot from that ledger without another REST
        account read.
        """
        self.store.apply_account_stream_update(
            balances=balances,
            positions=positions,
            captured_at_utc=captured_at_utc,
            asset=self.asset,
        )
        state = self.store.get_latest_account_state()
        stored_positions = self.store.list_account_position_state()
        with self._lock:
            if self._cached is None or state is None:
                return self._cached
            cached_by_key = {
                self._position_key(row): row
                for row in self._cached.positions
                if self._position_key(row)[0]
            }
            merged_positions: List[Dict[str, Any]] = []
            for stored in stored_positions:
                shaped = self._stored_position_payload(stored)
                key = self._position_key(shaped)
                merged = dict(cached_by_key.get(key) or {})
                merged.update({key: value for key, value in shaped.items() if value is not None})
                merged_positions.append(merged)
            cached = self._cached
            self._cached = AccountSnapshot(
                account_id=cached.account_id,
                captured_at_utc=str(state.get("captured_at_utc") or captured_at_utc),
                wallet_balance=_number(state.get("wallet_balance"), cached.wallet_balance),
                unrealized_pnl=_number(state.get("unrealized_pnl"), cached.unrealized_pnl),
                equity=_number(state.get("equity"), cached.equity),
                available_balance=_number(
                    state.get("available_balance"),
                    cached.available_balance,
                ),
                positions=tuple(merged_positions),
                raw=cached.raw,
            )
            return self._cached

    @staticmethod
    def _stored_position_payload(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "symbol": row.get("symbol"),
            "positionSide": row.get("position_side") or "BOTH",
            "positionAmt": row.get("position_amt"),
            "entryPrice": row.get("entry_price"),
            "breakEvenPrice": row.get("break_even_price"),
            "markPrice": row.get("mark_price"),
            "unRealizedProfit": row.get("unrealized_pnl"),
            "liquidationPrice": row.get("liquidation_price"),
            "leverage": row.get("leverage"),
            "notional": row.get("notional"),
            "isolatedMargin": row.get("isolated_margin"),
            "positionInitialMargin": row.get("initial_margin"),
        }

    @staticmethod
    def _position_key(row: Dict[str, Any]) -> Tuple[str, str]:
        return (
            str(row.get("symbol") or "").strip().upper(),
            str(row.get("positionSide") or row.get("position_side") or "BOTH").strip().upper()
            or "BOTH",
        )

    @classmethod
    def _merge_position_fields(
        cls,
        current: Dict[str, Any],
        fallback: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        merged = dict(current)
        if not fallback:
            return merged
        field_aliases = {
            "entryPrice": ("entryPrice", "entry_price"),
            "breakEvenPrice": ("breakEvenPrice", "break_even_price"),
            "markPrice": ("markPrice", "mark_price"),
            "liquidationPrice": ("liquidationPrice", "liquidation_price"),
            "leverage": ("leverage",),
            "marginType": ("marginType", "margin_type"),
            "isolatedMargin": ("isolatedMargin", "isolated_margin"),
            "positionInitialMargin": ("positionInitialMargin", "initial_margin"),
        }
        for destination, aliases in field_aliases.items():
            existing = merged.get(destination)
            if existing not in (None, ""):
                continue
            for alias in aliases:
                value = fallback.get(alias)
                if value not in (None, ""):
                    merged[destination] = value
                    break
        return merged

    def _from_payload(
        self,
        payload: Dict[str, Any],
        *,
        captured_at: datetime,
        fallback_positions: Optional[List[Dict[str, Any]]] = None,
    ) -> AccountSnapshot:
        assets = payload.get("assets") if isinstance(payload, dict) else []
        asset_row: Dict[str, Any] = {}
        for row in assets or []:
            if str(row.get("asset") or "").strip().upper() == self.asset:
                asset_row = row
                break

        wallet_balance = _number(
            asset_row.get("walletBalance"),
            _number(payload.get("totalWalletBalance")),
        )
        unrealized_pnl = _number(
            asset_row.get("unrealizedProfit"),
            _number(payload.get("totalUnrealizedProfit")),
        )
        equity = _number(
            asset_row.get("marginBalance"),
            _number(payload.get("totalMarginBalance"), wallet_balance + unrealized_pnl),
        )
        available_balance = _number(
            asset_row.get("availableBalance"),
            _number(payload.get("availableBalance")),
        )

        fallback_by_key = {
            self._position_key(row): row
            for row in (fallback_positions or [])
            if self._position_key(row)[0]
        }
        positions: List[Dict[str, Any]] = []
        for raw_position in payload.get("positions") or []:
            if not isinstance(raw_position, dict):
                continue
            normalized = dict(raw_position)
            symbol = str(normalized.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            normalized["symbol"] = symbol
            normalized["positionSide"] = (
                str(normalized.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            )
            if "unRealizedProfit" not in normalized:
                normalized["unRealizedProfit"] = normalized.get("unrealizedProfit", "0")
            position_amt = _number(normalized.get("positionAmt"))
            notional = _number(normalized.get("notional"))
            if not normalized.get("markPrice") and abs(position_amt) > 1e-12 and notional:
                normalized["markPrice"] = str(abs(notional / position_amt))
            if "isolatedMargin" not in normalized and "isolatedWallet" in normalized:
                normalized["isolatedMargin"] = normalized.get("isolatedWallet")
            normalized = self._merge_position_fields(
                normalized,
                fallback_by_key.get(self._position_key(normalized)),
            )
            positions.append(normalized)

        return AccountSnapshot(
            account_id=self.account_id,
            captured_at_utc=captured_at.replace(microsecond=0).isoformat(),
            wallet_balance=wallet_balance,
            unrealized_pnl=unrealized_pnl,
            equity=equity,
            available_balance=available_balance,
            positions=tuple(positions),
            raw=dict(payload),
        )
