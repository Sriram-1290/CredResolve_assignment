"""Provider health: a circuit breaker per provider, and a router that picks a healthy one."""
from __future__ import annotations

from collections import deque
from enum import StrEnum

from ..clock import Clock
from .base import TelecomProvider


class Health(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"      # elevated errors: safety falls back to progressive
    HALF_OPEN = "HALF_OPEN"    # recovering: one probe call at a time
    OPEN = "OPEN"              # failing: no new calls at all


class CircuitBreaker:
    def __init__(self, clock: Clock, *, window_s: float = 30.0, min_samples: int = 10,
                 degraded_error_rate: float = 0.10, open_error_rate: float = 0.50,
                 open_after_consecutive: int = 5, open_for_s: float = 15.0) -> None:
        self._clock = clock
        self._window_s = window_s
        self._min_samples = min_samples
        self._degraded = degraded_error_rate
        self._open = open_error_rate
        self._open_after_consecutive = open_after_consecutive
        self._open_for_s = open_for_s
        self._samples: deque[tuple[float, bool]] = deque()
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    def record(self, ok: bool) -> None:
        now = self._clock.now()
        if self._opened_at is not None:
            if now - self._opened_at < self._open_for_s:
                return                        # results of requests sent before opening
            if ok:                            # probe succeeded: close
                self._opened_at = None
                self._samples.clear()
                self._consecutive_failures = 0
            else:                             # probe failed: stay open for another period
                self._opened_at = now
            return
        self._samples.append((now, ok))
        self._prune(now)
        self._consecutive_failures = 0 if ok else self._consecutive_failures + 1
        n, err = self._error_rate()
        if self._consecutive_failures >= self._open_after_consecutive or \
                (n >= self._min_samples and err >= self._open):
            self._opened_at = now

    def state(self) -> Health:
        now = self._clock.now()
        if self._opened_at is not None:
            return Health.HALF_OPEN if now - self._opened_at >= self._open_for_s else Health.OPEN
        self._prune(now)
        n, err = self._error_rate()
        if n >= self._min_samples and err >= self._degraded:
            return Health.DEGRADED
        return Health.HEALTHY

    def _prune(self, now: float) -> None:
        while self._samples and self._samples[0][0] < now - self._window_s:
            self._samples.popleft()

    def _error_rate(self) -> tuple[int, float]:
        n = len(self._samples)
        if n == 0:
            return 0, 0.0
        return n, sum(1 for _, ok in self._samples if not ok) / n


_PREFERENCE = (Health.HEALTHY, Health.DEGRADED, Health.HALF_OPEN)


class ProviderRouter:
    """Chooses which provider new calls go to (in priority order, skipping unhealthy ones)
    and looks providers up by name for calls already placed."""

    def __init__(self, providers: list[TelecomProvider], clock: Clock, **breaker_kwargs) -> None:
        self._providers = {p.name: p for p in providers}
        self._order = [p.name for p in providers]
        self._breakers = {p.name: CircuitBreaker(clock, **breaker_kwargs) for p in providers}

    def get(self, name: str) -> TelecomProvider:
        return self._providers[name]

    def breaker(self, name: str) -> CircuitBreaker:
        return self._breakers[name]

    def record(self, name: str, ok: bool) -> None:
        self._breakers[name].record(ok)

    def pick(self) -> str | None:
        states = {n: self._breakers[n].state() for n in self._order}
        for wanted in _PREFERENCE:
            for name in self._order:
                if states[name] is wanted:
                    return name
        return None

    def health(self) -> Health:
        """Health of the provider new calls would go to; OPEN if there is none."""
        name = self.pick()
        return Health.OPEN if name is None else self._breakers[name].state()
