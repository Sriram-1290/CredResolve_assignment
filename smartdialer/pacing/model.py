"""Data exchanged between the worker, the pacers and the Safety Controller.

Both are frozen: a pacer receives a read-only view of the world and returns a proposal. It has
no handle on the database, the allocator or any provider, so the only thing it *can* do is ask.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PacingSnapshot:
    campaign_id: str
    now: float
    # agents (only agents with a fresh heartbeat are counted; stale ones are treated as gone)
    agents_logged_in: int
    agents_available: int
    agents_reserved: int            # RESERVED + DIALING: held for an un-answered agent-bound call
    agents_connected: int
    agents_wrap_up: int
    agents_paused: int
    # calls not yet answered
    bound_in_flight: int            # have an agent attached
    unbound_in_flight: int          # agentless: speculative, or orphaned by an agent who left
    unbound_cancelling: int         # ... of which we have already asked the provider to cancel
    dialable_borrowers: int         # due now (capped count)
    # recent history
    attempts_long: int              # resolved attempts (answered / no answer / busy), long window
    answers_long: int
    attempts_short: int             # same, short window
    answers_short: int
    mean_dial_to_answer_s: float | None
    mean_talk_s: float | None
    answered_live_window: int       # borrowers who picked up (abandon-rate denominator)
    abandoned_window: int
    events_recent: int = 0          # provider webhooks received recently ...
    events_late: int = 0            # ... of which arrived later than the late threshold
    cohort_dialed: int = 0          # calls dialed in the last ~30s ...
    cohort_answered: int = 0        # ... already answered: a hard lower bound on today's rate

    @property
    def abandon_rate(self) -> float:
        if self.answered_live_window == 0:
            return 0.0
        return self.abandoned_window / self.answered_live_window


@dataclass(frozen=True)
class PacingProposal:
    pacer: str
    bound: int                      # calls that reserve an agent before dialing
    speculative: int                # calls dialed ahead of agent availability
    explanation: tuple[str, ...]

    @property
    def total(self) -> int:
        return self.bound + self.speculative
