"""Mock telecom providers.

A MockProvider simulates the *network*: it decides (via a BorrowerModel) whether the borrower
answers and how long they talk, and it reports progress through webhooks that can be delayed,
duplicated or reordered according to its MockProfile. The dialer sees only the TelecomProvider
interface and the events.

Realism that matters for safety: an answered borrower who is not bridged to an agent within
`bridge_grace_s` hangs up (COMPLETED reason=NO_AGENT). So a lost BRIDGE command, a slow ANSWERED
webhook or an outage during an answer all turn into real abandoned calls.
"""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, replace
from typing import Callable

from ..clock import EventLoop
from .base import (
    EndReason, EventSink, EventType, ProviderCallStatus, ProviderEvent, ProviderRejected,
    ProviderTimeout,
)

Schedule = float | Callable[[float], float]


def _at(value: Schedule, t: float) -> float:
    return value(t) if callable(value) else value


@dataclass(frozen=True)
class BorrowerModel:
    """How borrowers behave. Either field may be a function of simulated time (scenario D)."""
    answer_rate: Schedule = 0.3
    talk_mean_s: Schedule = 120.0
    answer_delay_s: tuple[float, float] = (3.0, 15.0)   # ringing -> picks up
    busy_rate: float = 0.03

    def answers(self, rng: random.Random, t: float) -> bool:
        return rng.random() < _at(self.answer_rate, t)

    def talk_seconds(self, rng: random.Random, t: float) -> float:
        mean = _at(self.talk_mean_s, t)
        return rng.gammavariate(2.0, mean / 2.0)        # mean `mean`, fewer extreme values than exp


@dataclass(frozen=True)
class MockProfile:
    name: str
    setup_s: tuple[float, float] = (0.5, 1.5)           # accepted -> ringing at the borrower
    delivery_s: tuple[float, float] = (0.05, 0.2)       # webhook delivery delay
    reject_rate: float = 0.005                          # start_call refused (definitive)
    timeout_rate: float = 0.0                           # start_call times out (ambiguous) ...
    placed_on_timeout: float = 0.5                      # ... and the call was placed anyway
    duplicate_rate: float = 0.0                         # a webhook is delivered twice
    straggler_rate: float = 0.0                         # a webhook arrives far behind later ones
    straggler_s: tuple[float, float] = (8.0, 30.0)
    ring_timeout_s: float = 30.0
    bridge_grace_s: float = 5.0


PROVIDER_A = MockProfile(name="A")
PROVIDER_B = MockProfile(
    name="B", setup_s=(2.0, 6.0), delivery_s=(0.2, 3.0), reject_rate=0.02, timeout_rate=0.03,
    duplicate_rate=0.15, straggler_rate=0.03,
)


@dataclass
class _Call:
    call_id: str
    pcid: str
    phase: str = "SETUP"            # SETUP | RINGING | ANSWERED | ENDED
    agent_on_answer: str | None = None
    answered: bool = False
    bridged_to: str | None = None
    end_type: EventType | None = None
    end_reason: str | None = None
    seq: int = 0


class MockProvider:
    def __init__(self, profile: MockProfile, loop: EventLoop, borrowers: BorrowerModel,
                 rng: random.Random, sink: EventSink | None = None) -> None:
        self.name = profile.name
        self.profile = profile
        self._loop = loop
        self._borrowers = borrowers
        self._rng = rng
        # Provider call ids (and so webhook event ids) must be globally unique, as a real
        # provider's are, even when several mock instances run side by side (load test).
        self._instance = f"{rng.getrandbits(32):08x}"
        self._sink = sink
        self._calls: dict[str, _Call] = {}
        self._tombstones: set[str] = set()
        self._outage = False
        self._held: list[ProviderEvent] = []
        self.stats: Counter[str] = Counter()

    # ------------------------------------------------------------------ test/sim controls
    def connect(self, sink: EventSink) -> None:
        self._sink = sink

    def set_outage(self, on: bool) -> None:
        """Outage: every API call times out and webhooks are held (delivered late on recovery).
        Calls already in the network keep progressing."""
        self._outage = on
        if not on:
            held, self._held = self._held, []
            for ev in held:
                self._loop.call_later(self._rng.uniform(0.5, 5.0), lambda ev=ev: self._deliver(ev))

    def set_borrowers(self, borrowers: BorrowerModel) -> None:
        self._borrowers = borrowers

    @property
    def live_calls(self) -> int:
        return sum(1 for c in self._calls.values() if c.phase != "ENDED")

    # ------------------------------------------------------------------ TelecomProvider
    def start_call(self, call_id: str, phone: str, agent_id: str | None = None) -> str:
        self.stats["start_call"] += 1
        if self._outage:
            raise ProviderTimeout(f"{self.name}: start_call timed out")
        existing = self._calls.get(call_id)
        if existing is not None:
            self.stats["start_call_idempotent_hit"] += 1
            return existing.pcid
        if call_id in self._tombstones:
            raise ProviderRejected(f"{self.name}: call {call_id} was cancelled")
        r = self._rng.random()
        if r < self.profile.reject_rate:
            self.stats["rejected"] += 1
            raise ProviderRejected(f"{self.name}: rejected")
        if r < self.profile.reject_rate + self.profile.timeout_rate:
            self.stats["timeout"] += 1
            if self._rng.random() < self.profile.placed_on_timeout:
                self.stats["timeout_but_placed"] += 1
                self._place(call_id, agent_id)
            raise ProviderTimeout(f"{self.name}: start_call timed out")
        return self._place(call_id, agent_id).pcid

    def cancel_call(self, call_id: str) -> None:
        self._api("cancel")
        call = self._calls.get(call_id)
        if call is None:
            self._tombstones.add(call_id)
            return
        if call.phase in ("SETUP", "RINGING"):
            self._end(call, EventType.FAILED, EndReason.CANCELLED)
        # answered or ended: too late to cancel, nothing to do

    def hangup(self, call_id: str) -> None:
        self._api("hangup")
        call = self._calls.get(call_id)
        if call is None or call.phase == "ENDED":
            return
        if call.phase == "ANSWERED":
            self._end(call, EventType.COMPLETED, EndReason.HANGUP)
        else:
            self._end(call, EventType.FAILED, EndReason.CANCELLED)

    def bridge(self, call_id: str, agent_id: str) -> None:
        self._api("bridge")
        call = self._calls.get(call_id)
        if call is None:
            raise ProviderRejected(f"{self.name}: unknown call {call_id}")
        if call.phase != "ANSWERED" or call.bridged_to == agent_id:
            return                                  # borrower already gone, or already bridged
        self._connect_agent(call, agent_id)         # first bridge, or transfer to another agent

    def get_call_status(self, call_id: str) -> ProviderCallStatus | None:
        self._api("status")
        call = self._calls.get(call_id)
        if call is None:
            return None
        return ProviderCallStatus(call.pcid, call.answered, call.phase == "ENDED",
                                  call.end_type, call.end_reason)

    # ------------------------------------------------------------------ network simulation
    def _api(self, op: str) -> None:
        self.stats[op] += 1
        if self._outage:
            raise ProviderTimeout(f"{self.name}: {op} timed out")

    def _place(self, call_id: str, agent_id: str | None) -> _Call:
        call = _Call(call_id=call_id, pcid=f"{self.name}-{self._instance}-{len(self._calls) + 1}",
                     agent_on_answer=agent_id)
        self._calls[call_id] = call
        self.stats["placed"] += 1
        now = self._loop.clock.now()
        setup = self._rng.uniform(*self.profile.setup_s)
        self._loop.call_later(setup, lambda: self._ring(call))
        if self._rng.random() < self._borrowers.busy_rate:
            self._loop.call_later(setup + 1.0, lambda: self._end(call, EventType.FAILED, EndReason.BUSY))
        elif self._borrowers.answers(self._rng, now):
            delay = setup + self._rng.uniform(*self._borrowers.answer_delay_s)
            self._loop.call_later(delay, lambda: self._answer(call))
        else:
            self._loop.call_later(setup + self.profile.ring_timeout_s,
                                  lambda: self._end(call, EventType.FAILED, EndReason.NO_ANSWER))
        return call

    def _ring(self, call: _Call) -> None:
        if call.phase == "SETUP":
            call.phase = "RINGING"
            self._emit(call, EventType.RINGING)

    def _answer(self, call: _Call) -> None:
        if call.phase not in ("SETUP", "RINGING"):
            return                                  # cancelled while ringing
        call.phase, call.answered = "ANSWERED", True
        self.stats["answered"] += 1
        if call.agent_on_answer is not None:
            self._connect_agent(call, call.agent_on_answer)
        self._emit(call, EventType.ANSWERED)
        self._loop.call_later(self.profile.bridge_grace_s, lambda: self._no_agent_check(call))

    def _connect_agent(self, call: _Call, agent_id: str) -> None:
        first = call.bridged_to is None
        call.bridged_to = agent_id
        if first:
            talk = self._borrowers.talk_seconds(self._rng, self._loop.clock.now())
            self._loop.call_later(talk, lambda: self._end(call, EventType.COMPLETED, EndReason.NORMAL))

    def _no_agent_check(self, call: _Call) -> None:
        if call.phase == "ANSWERED" and call.bridged_to is None:
            self.stats["borrower_hung_up_no_agent"] += 1
            self._end(call, EventType.COMPLETED, EndReason.NO_AGENT)

    def _end(self, call: _Call, end_type: EventType, reason: str) -> None:
        if call.phase == "ENDED":
            return
        call.phase, call.end_type, call.end_reason = "ENDED", end_type, reason
        self._emit(call, end_type, reason)

    def _emit(self, call: _Call, etype: EventType, reason: str | None = None) -> None:
        call.seq += 1
        ev = ProviderEvent(self.name, f"{call.pcid}-{call.seq}", call.call_id, etype,
                           self._loop.clock.now(), call.pcid, reason)
        self._send(ev)
        if self._rng.random() < self.profile.duplicate_rate:
            self.stats["duplicates"] += 1
            # Half are true redeliveries (same id), half are re-sends under a new id.
            dup = ev if self._rng.random() < 0.5 else replace(ev, event_id=ev.event_id + "-resend")
            self._send(dup)

    def _send(self, ev: ProviderEvent) -> None:
        delay = self._rng.uniform(*self.profile.delivery_s)
        if self._rng.random() < self.profile.straggler_rate:
            self.stats["stragglers"] += 1
            delay += self._rng.uniform(*self.profile.straggler_s)
        self._loop.call_later(delay, lambda: self._deliver(ev))

    def _deliver(self, ev: ProviderEvent) -> None:
        if self._outage:
            self._held.append(ev)
            return
        self.stats["delivered"] += 1
        if self._sink is not None:
            self._sink(ev)
