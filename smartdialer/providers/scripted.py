"""A provider that does nothing on its own: the caller scripts every event and status.

Used by tests and by the step-by-step failure demos, where we need to deliver exactly
"ANSWERED, ANSWERED, ANSWERED, COMPLETED" or "COMPLETED, ANSWERED, RINGING".
"""
from __future__ import annotations

import itertools

from .base import ProviderCallStatus, ProviderRejected, ProviderTimeout


class ScriptedProvider:
    def __init__(self, name: str = "S") -> None:
        self.name = name
        self.placed: dict[str, str] = {}          # call_id -> provider call id
        self.tombstones: set[str] = set()
        self.status: dict[str, ProviderCallStatus] = {}
        self.requests: list[tuple[str, str]] = []
        self.outage = False
        self.start_mode: str | None = None        # None | "reject" | "timeout" | "timeout_placed"
        self._ids = itertools.count(1)

    def start_call(self, call_id: str, phone: str, agent_id: str | None = None) -> str:
        self.requests.append(("start", call_id))
        if self.outage or self.start_mode == "timeout":
            raise ProviderTimeout("timeout")
        if call_id in self.tombstones or self.start_mode == "reject":
            raise ProviderRejected("rejected")
        if call_id not in self.placed:
            self.placed[call_id] = f"P{next(self._ids)}"
        if self.start_mode == "timeout_placed":
            raise ProviderTimeout("timeout after placing")
        return self.placed[call_id]

    def _op(self, op: str, call_id: str) -> None:
        self.requests.append((op, call_id))
        if self.outage:
            raise ProviderTimeout(op)

    def cancel_call(self, call_id: str) -> None:
        self._op("cancel", call_id)
        if call_id not in self.placed:
            self.tombstones.add(call_id)

    def hangup(self, call_id: str) -> None:
        self._op("hangup", call_id)

    def bridge(self, call_id: str, agent_id: str) -> None:
        self._op(f"bridge:{agent_id}", call_id)

    def get_call_status(self, call_id: str) -> ProviderCallStatus | None:
        self._op("status", call_id)
        if call_id in self.status:
            return self.status[call_id]
        if call_id in self.placed:
            return ProviderCallStatus(self.placed[call_id], answered=False, ended=False)
        return None

    def ops(self, op: str) -> list[str]:
        return [cid for o, cid in self.requests if o == op]
