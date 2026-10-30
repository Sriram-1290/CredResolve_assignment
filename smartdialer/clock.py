"""Time sources and a tiny event loop.

Everything reads time through a Clock so the simulator can run an hour of dialing in seconds
using exactly the same code that would run against wall-clock time.
"""
from __future__ import annotations

import heapq
import itertools
import time
from typing import Callable, Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class RealClock:
    def now(self) -> float:
        return time.time()


class SimClock:
    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def advance_to(self, t: float) -> None:
        if t < self._t:
            raise ValueError("time cannot go backwards")
        self._t = t


class EventLoop:
    """A timer heap.

    With a SimClock, `run_until` jumps time from timer to timer (discrete-event simulation).
    With a RealClock, `run_due` fires whatever is due and is meant to be polled in a loop.
    """

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self._heap: list[tuple[float, int, Callable[[], None]]] = []
        self._seq = itertools.count()

    def call_at(self, t: float, fn: Callable[[], None]) -> None:
        heapq.heappush(self._heap, (t, next(self._seq), fn))

    def call_later(self, delay: float, fn: Callable[[], None]) -> None:
        self.call_at(self.clock.now() + max(0.0, delay), fn)

    def every(self, interval: float, fn: Callable[[], None], start_delay: float = 0.0) -> None:
        def run() -> None:
            fn()
            self.call_later(interval, run)
        self.call_later(start_delay, run)

    def run_due(self) -> int:
        n = 0
        now = self.clock.now()
        while self._heap and self._heap[0][0] <= now:
            _, _, fn = heapq.heappop(self._heap)
            fn()
            n += 1
        return n

    def run_until(self, t_end: float) -> None:
        if not isinstance(self.clock, SimClock):
            raise TypeError("run_until needs a SimClock")
        while self._heap and self._heap[0][0] <= t_end:
            t, _, fn = heapq.heappop(self._heap)
            self.clock.advance_to(max(t, self.clock.now()))
            fn()
        self.clock.advance_to(max(t_end, self.clock.now()))
