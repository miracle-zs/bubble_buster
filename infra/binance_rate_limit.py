"""Shared Binance REST rate-limit coordination for one process/IP scope."""

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BinanceRequestBudget:
    """One admitted non-trading REST request."""

    path: str
    weight: int
    background: bool


class BinanceRateLimitTriggered(RuntimeError):
    """Raw market-data request was rejected by Binance rate limiting."""

    def __init__(self, http_status: Optional[int], retry_after_sec: float):
        self.http_status = http_status
        self.retry_after_sec = max(0.0, float(retry_after_sec))
        super().__init__(
            f"Binance REST rate limit triggered status={http_status or 'n/a'} "
            f"retry_after_sec={self.retry_after_sec:.2f}"
        )


class BinanceRateLimitCoordinator:
    """Coordinate REST cooldowns shared by multiple Binance clients.

    Binance applies request limits to the source IP rather than to an API key.
    The runtime therefore gives every account client the same coordinator.  A
    response-level cooldown is deliberately separate from the rolling request
    weight limiter used by the ranking code: this class is the circuit breaker
    that prevents retry storms after Binance has already returned 429/418.
    """

    def __init__(
        self,
        fallback_retry_after_sec: float = 60.0,
        ban_fallback_retry_after_sec: float = 120.0,
        non_trading_weight_per_minute: int = 300,
        background_weight_per_minute: int = 200,
    ) -> None:
        self.fallback_retry_after_sec = max(1.0, float(fallback_retry_after_sec))
        self.ban_fallback_retry_after_sec = max(
            self.fallback_retry_after_sec,
            float(ban_fallback_retry_after_sec),
        )
        self._blocked_until_monotonic = 0.0
        self.non_trading_weight_per_minute = max(1, int(non_trading_weight_per_minute))
        self.background_weight_per_minute = min(
            self.non_trading_weight_per_minute,
            max(1, int(background_weight_per_minute)),
        )
        self._non_trading_events: Deque[Tuple[float, int]] = deque()
        self._background_events: Deque[Tuple[float, int]] = deque()
        self._non_trading_weight = 0
        self._background_weight = 0
        self._lock = threading.RLock()

    @staticmethod
    def _prune_events(
        events: Deque[Tuple[float, int]],
        current_weight: int,
        now_monotonic: float,
    ) -> int:
        cutoff = now_monotonic - 60.0
        while events and events[0][0] <= cutoff:
            _event_at, event_weight = events.popleft()
            current_weight -= int(event_weight)
        return max(0, current_weight)

    def _prune(self, now_monotonic: float) -> None:
        self._non_trading_weight = self._prune_events(
            self._non_trading_events,
            self._non_trading_weight,
            now_monotonic,
        )
        self._background_weight = self._prune_events(
            self._background_events,
            self._background_weight,
            now_monotonic,
        )

    def wait_for_available(self, *, is_trading: bool = False) -> None:
        """Block non-trading callers until a shared 429 cooldown has elapsed.

        Order placement/cancellation stays available while observability and
        background REST are paused.  This is important for preserving existing
        exchange protection when Binance asks the project to back off.
        """
        if is_trading:
            return
        while True:
            with self._lock:
                remaining = self._blocked_until_monotonic - time.monotonic()
            if remaining <= 0:
                return
            # Short waits make an extended Retry-After responsive to process
            # shutdown and avoid holding the coordinator lock while sleeping.
            time.sleep(min(max(remaining, 0.01), 1.0))

    def acquire(
        self,
        *,
        path: str,
        weight: int,
        is_trading: bool = False,
        background: bool = False,
    ) -> BinanceRequestBudget:
        """Admit one request through the project-wide rolling weight budgets."""
        normalized_path = str(path or "").split("?", 1)[0] or "/"
        request_weight = max(1, int(weight))
        if is_trading:
            return BinanceRequestBudget(normalized_path, request_weight, False)

        while True:
            self.wait_for_available(is_trading=False)
            sleep_for = 0.0
            with self._lock:
                now_monotonic = time.monotonic()
                self._prune(now_monotonic)
                if (
                    self._non_trading_weight + request_weight
                    > self.non_trading_weight_per_minute
                    and self._non_trading_events
                ):
                    sleep_for = max(
                        sleep_for,
                        self._non_trading_events[0][0] + 60.0 - now_monotonic,
                    )
                if (
                    background
                    and self._background_weight + request_weight
                    > self.background_weight_per_minute
                    and self._background_events
                ):
                    sleep_for = max(
                        sleep_for,
                        self._background_events[0][0] + 60.0 - now_monotonic,
                    )
                if sleep_for <= 0:
                    self._non_trading_events.append((now_monotonic, request_weight))
                    self._non_trading_weight += request_weight
                    if background:
                        self._background_events.append((now_monotonic, request_weight))
                        self._background_weight += request_weight
                    LOGGER.debug(
                        "Binance REST admitted endpoint=%s weight=%s background=%s",
                        normalized_path,
                        request_weight,
                        background,
                    )
                    return BinanceRequestBudget(
                        normalized_path,
                        request_weight,
                        bool(background),
                    )
            time.sleep(min(max(sleep_for, 0.01), 1.0))

    def trip(
        self,
        retry_after_sec: Optional[float] = None,
        http_status: Optional[int] = None,
    ) -> float:
        """Open/extend the shared cooldown and return its effective delay."""
        try:
            delay = float(retry_after_sec) if retry_after_sec is not None else 0.0
        except (TypeError, ValueError):
            delay = 0.0
        if delay <= 0:
            delay = (
                self.ban_fallback_retry_after_sec
                if http_status == 418
                else self.fallback_retry_after_sec
            )

        now = time.monotonic()
        blocked_until = now + delay
        with self._lock:
            previous_until = self._blocked_until_monotonic
            self._blocked_until_monotonic = max(previous_until, blocked_until)
            effective_delay = max(0.0, self._blocked_until_monotonic - now)

        if blocked_until > previous_until:
            LOGGER.warning(
                "Binance REST global cooldown opened status=%s retry_after_sec=%.2f remaining_sec=%.2f",
                http_status or "n/a",
                delay,
                effective_delay,
            )
        return effective_delay

    def is_blocked(self) -> bool:
        with self._lock:
            return self._blocked_until_monotonic > time.monotonic()

    def cooldown_remaining_sec(self) -> float:
        with self._lock:
            return max(0.0, self._blocked_until_monotonic - time.monotonic())

    def usage(self) -> Dict[str, int]:
        """Return current rolling usage for diagnostics and acceptance tests."""
        with self._lock:
            self._prune(time.monotonic())
            return {
                "non_trading_weight_1m": int(self._non_trading_weight),
                "background_weight_1m": int(self._background_weight),
                "non_trading_limit_1m": int(self.non_trading_weight_per_minute),
                "background_limit_1m": int(self.background_weight_per_minute),
            }


_SHARED_COORDINATORS: Dict[str, BinanceRateLimitCoordinator] = {}
_SHARED_COORDINATORS_LOCK = threading.Lock()


def get_shared_rate_limit_coordinator(scope: str = "binance-futures-ip") -> BinanceRateLimitCoordinator:
    """Return the process-wide coordinator for a configured IP scope."""
    key = str(scope or "binance-futures-ip").strip() or "binance-futures-ip"
    with _SHARED_COORDINATORS_LOCK:
        coordinator = _SHARED_COORDINATORS.get(key)
        if coordinator is None:
            coordinator = BinanceRateLimitCoordinator()
            _SHARED_COORDINATORS[key] = coordinator
        return coordinator
