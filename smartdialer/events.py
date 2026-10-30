"""Provider event processing: inbox + idempotent, order-tolerant application.

receive(event)            webhook entry point
  1. INSERT into provider_events (PK provider+event_id). Duplicate delivery -> no-op.
     Committed on its own: once we have acknowledged an event we never lose it.
  2. Apply it in a second transaction: load the call, decide, write the call, agent, borrower
     and outbox rows, and mark the event DONE, all atomically.

Rules that make duplicates and reordering harmless:
  * an event may only move a call forward (CALL_RANK); anything else is recorded as STALE;
  * terminal states absorb everything;
  * agent updates are CAS'd on (state, call_id), so an event for an old call cannot move an agent
    who has since moved on;
  * facts are kept even from stale events (e.g. answered_at), so reporting stays accurate.

If the worker dies between 1 and 2, or in the middle of 2, the event is still PENDING and its
lease expires; `process_pending` (run by every worker's reconciler) applies it.
"""
from __future__ import annotations

import sqlite3

from . import repo
from .clock import Clock
from .commands import BRIDGE, HANGUP, CommandDispatcher
from .config import DialerConfig
from .db import Store
from .faults import NO_FAULTS, Faults
from .providers.base import EndReason, EventType, ProviderEvent
from .states import (
    AgentState, CALL_RANK, CallState, IllegalTransition, TERMINAL_CALL_STATES,
    UNANSWERED_CALL_STATES,
)

_TARGET = {
    EventType.RINGING: CallState.RINGING,
    EventType.ANSWERED: CallState.ANSWERED,
    EventType.COMPLETED: CallState.COMPLETED,
    EventType.FAILED: CallState.FAILED,
}


class EventProcessor:
    def __init__(self, store: Store, clock: Clock, dispatcher: CommandDispatcher,
                 config: DialerConfig, faults: Faults = NO_FAULTS) -> None:
        self._store = store
        self._clock = clock
        self._dispatcher = dispatcher
        self._cfg = config
        self._faults = faults

    # ------------------------------------------------------------------ entry points
    def receive(self, ev: ProviderEvent) -> str:
        if not self._ingest(ev):
            return "DUPLICATE"
        self._faults.check("after_event_ingested")
        return self._apply_one(ev.provider, ev.event_id)

    def process_pending(self, limit: int = 500) -> int:
        now = self._clock.now()
        with self._store.tx() as c:
            claimed = c.execute(
                """UPDATE provider_events SET lease_until=?
                   WHERE rowid IN (SELECT rowid FROM provider_events
                                   WHERE status='PENDING' AND lease_until < ?
                                   ORDER BY received_at LIMIT ?)
                   RETURNING provider, event_id""",
                (now + self._cfg.event_lease_s, now, limit)).fetchall()
        for row in claimed:
            try:
                self._apply_one(row["provider"], row["event_id"])
            except (IllegalTransition, ValueError, KeyError, AssertionError) as e:
                # A deterministic failure would fail again on every retry and abort every
                # worker's reconciliation pass. Park the event for review instead.
                with self._store.tx() as c:
                    c.execute("UPDATE provider_events SET status='DONE', outcome=? "
                              "WHERE provider=? AND event_id=? AND status='PENDING'",
                              (f"ERROR:{type(e).__name__}", row["provider"], row["event_id"]))
        return len(claimed)

    # ------------------------------------------------------------------ internals
    def _ingest(self, ev: ProviderEvent) -> bool:
        now = self._clock.now()
        with self._store.tx() as c:
            return c.execute(
                """INSERT INTO provider_events (provider, event_id, call_id, type, reason,
                       provider_call_id, occurred_at, received_at, lease_until)
                   VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                (ev.provider, ev.event_id, ev.call_id, str(ev.type), ev.reason,
                 ev.provider_call_id, ev.occurred_at, now, now + self._cfg.event_lease_s),
            ).rowcount == 1

    def _apply_one(self, provider: str, event_id: str) -> str:
        with self._store.tx() as c:
            ev = c.execute("SELECT * FROM provider_events WHERE provider=? AND event_id=?",
                           (provider, event_id)).fetchone()
            if ev is None or ev["status"] != "PENDING":
                return "ALREADY_DONE"
            call = repo.get_call(c, ev["call_id"])
            outcome = "UNKNOWN_CALL" if call is None else self._apply(c, ev, call)
            self._faults.check("before_event_commit")
            c.execute("UPDATE provider_events SET status='DONE', outcome=? "
                      "WHERE provider=? AND event_id=?", (outcome, provider, event_id))
        self._faults.check("after_event_commit")
        self._dispatcher.flush(call_id=ev["call_id"])
        return outcome

    def _apply(self, c: sqlite3.Connection, ev: sqlite3.Row, call: sqlite3.Row) -> str:
        now = self._clock.now()
        state = CallState(call["state"])
        etype = EventType(ev["type"])
        if call["provider_call_id"] is None and ev["provider_call_id"]:
            c.execute("UPDATE calls SET provider_call_id=? WHERE id=?",
                      (ev["provider_call_id"], call["id"]))
        if etype is EventType.ANSWERED and state in TERMINAL_CALL_STATES and \
                (call["answered_at"] is None or ev["occurred_at"] < call["answered_at"]):
            # A late ANSWERED refines the end-time estimate recorded when COMPLETED overtook it.
            c.execute("UPDATE calls SET answered_at=? WHERE id=?", (ev["occurred_at"], call["id"]))

        if state in TERMINAL_CALL_STATES:
            return "STALE"
        target = _TARGET[etype]
        if CALL_RANK[target] <= CALL_RANK[state]:
            return "STALE"

        if etype is EventType.RINGING:
            # The provider has the call, so the start is confirmed even if the worker that sent
            # it died before recording that: drop its lease and finish its agent bookkeeping.
            repo.move_call(c, call["id"], CallState.RINGING, expect={state}, now=now,
                           ringing_at=ev["occurred_at"], owner=None, lease_until=None)
            if call["agent_id"] is not None and state is CallState.RESERVED:
                repo.move_agent(c, call["agent_id"], AgentState.DIALING,
                                expect={AgentState.RESERVED}, now=now, only_if_call=call["id"])
            return "APPLIED"
        if etype is EventType.ANSWERED:
            return self._answered(c, call, state, ev["occurred_at"], now)
        if etype is EventType.COMPLETED:
            return self._completed(c, call, state, ev, now)
        reason = ev["reason"] or EndReason.ERROR
        # CANCELLED is only reachable before answer; a cancel-type failure on an answered call
        # is an ordinary failure (CONNECTED -> CANCELLED is not a legal transition).
        to = CallState.CANCELLED if reason == EndReason.CANCELLED and \
            state in UNANSWERED_CALL_STATES else CallState.FAILED
        repo.close_call(c, call["id"], to, reason, now, expect={state})
        return "APPLIED"

    def _answered(self, c: sqlite3.Connection, call: sqlite3.Row, state: CallState,
                  answered_at: float, now: float) -> str:
        call_id = call["id"]
        agent_id = call["agent_id"]
        if agent_id is not None:
            if repo.move_agent(c, agent_id, AgentState.CONNECTED,
                               expect={AgentState.RESERVED, AgentState.DIALING}, now=now,
                               only_if_call=call_id):
                return self._connect(c, call_id, state, agent_id, answered_at, now)
            # The reserved agent is gone; detach and treat it like an agentless call.
            c.execute("UPDATE calls SET agent_id=NULL, anomaly='AGENT_GONE_AT_ANSWER' WHERE id=?",
                      (call_id,))
        fresh_after = now - self._cfg.heartbeat_timeout_s
        claimed = repo.claim_agent_for_answer(c, call["campaign_id"], call_id, now, fresh_after)
        if claimed is not None:
            return self._connect(c, call_id, state, claimed, answered_at, now)
        # Nobody can take the call. Hang up promptly rather than leave the borrower in silence.
        repo.close_call(c, call_id, CallState.ABANDONED, "NO_AGENT", now, expect={state},
                        answered_at=answered_at)
        repo.enqueue_command(c, call_id, HANGUP, now)
        return "ABANDONED"

    def _connect(self, c: sqlite3.Connection, call_id: str, state: CallState, agent_id: str,
                 answered_at: float, now: float) -> str:
        repo.move_call(c, call_id, CallState.CONNECTED, expect={state}, via=CallState.ANSWERED,
                       now=now, agent_id=agent_id, answered_at=answered_at, connected_at=now,
                       owner=None, lease_until=None)
        repo.enqueue_command(c, call_id, BRIDGE, now, arg=agent_id)
        return "CONNECTED"

    def _completed(self, c: sqlite3.Connection, call: sqlite3.Row, state: CallState,
                   ev: sqlite3.Row, now: float) -> str:
        reason = ev["reason"] or EndReason.NORMAL
        if state is CallState.CONNECTED:
            if reason == EndReason.NO_AGENT:      # we assigned an agent, the bridge never landed
                repo.close_call(c, call["id"], CallState.ABANDONED, "BRIDGE_FAILED", now,
                                expect={state})
            else:
                repo.close_call(c, call["id"], CallState.COMPLETED, reason, now, expect={state})
            return "APPLIED"
        # Ended before we ever saw ANSWERED (the ANSWERED webhook is late or lost).
        assert state in UNANSWERED_CALL_STATES
        answered_at = call["answered_at"] or ev["occurred_at"]
        if reason == EndReason.NO_AGENT:
            repo.close_call(c, call["id"], CallState.ABANDONED, "ANSWER_NOT_SEEN", now,
                            expect={state}, answered_at=answered_at, anomaly="ANSWER_EVENT_LATE")
        else:
            repo.close_call(c, call["id"], CallState.COMPLETED, reason, now, expect={state},
                            answered_at=answered_at, anomaly="COMPLETED_BEFORE_ANSWERED")
        return "APPLIED"
