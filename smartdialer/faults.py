"""Fault injection: named crash points used by tests, the simulator and the failure demos."""
from __future__ import annotations


class SimulatedCrash(BaseException):
    """Stands in for the worker process dying at a specific line.

    It derives from BaseException so ordinary `except Exception` handlers cannot swallow it,
    and `Store.tx()` rolls back the open transaction exactly as a dead process would.
    """


class Faults:
    def __init__(self) -> None:
        self._armed: dict[str, int] = {}

    def arm(self, point: str, skip: int = 0) -> None:
        """Crash on the (skip+1)-th time `point` is reached."""
        self._armed[point] = skip

    def check(self, point: str) -> None:
        remaining = self._armed.get(point)
        if remaining is None:
            return
        if remaining == 0:
            del self._armed[point]
            raise SimulatedCrash(point)
        self._armed[point] = remaining - 1


NO_FAULTS = Faults()
