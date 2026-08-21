"""Shared Binance REST rate-limit coordination for one process/IP scope."""

import logging
import threading
import time
from typing import Dict, Optional


LOGGER = logging.getLogger(__name__)


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
    ) -> None:
        self.fallback_retry_after_sec = max(1.0, float(fallback_retry_after_sec))
        self.ban_fallback_retry_after_sec = max(
            self.fallback_retry_after_sec,
            float(ban_fallback_retry_after_sec),
        )
        self._blocked_until_monotonic = 0.0
        self._lock = threading.Lock()

    def wait_for_available(self) -> None:
        """Block one caller until the shared cooldown has elapsed."""
        while True:
            with self._lock:
                remaining = self._blocked_until_monotonic - time.monotonic()
            if remaining <= 0:
                return
            # Short waits make an extended Retry-After responsive to process
            # shutdown and avoid holding the coordinator lock while sleeping.
            time.sleep(min(max(remaining, 0.01), 1.0))

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
