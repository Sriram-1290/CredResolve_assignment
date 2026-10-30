"""Reconciler: brings the database back in line with reality after crashes, lost webhooks and
provider outages. Every worker runs it every tick; all of its writes are CAS, so two reconcilers
working on the same row cannot both win.

What it repairs
---------------
1. Agents with a stale heartbeat              -> AgentService.logout (cancels their setup calls)
2. Calls whose start was never confirmed      -> lease expired in RESERVED/QUEUED: ask the provider
   (worker died around start_call, or the        * provider has it   -> adopt it (INITIATED) and
    start_call timed out)                                               replay its status as events
                                                 * provider doesn't  -> cancel the id (tombstone, so a
                                                                        delayed start can't create
                                                                        it), release agent+borrower
                                                 * provider unreachable -> retry; give up after
                                                                        max_unresolved_s
3. Calls stuck without progress               -> ask the provider, replay status as events
4. Events received but never applied          -> EventProcessor.process_pending
5. Outbox commands not yet sent               -> CommandDispatcher.flush
6. Wrap-up that never ended                   -> AVAILABLE
"""
from __future__ import annotations

from collections import Counter

from . import repo
from .agents import AgentService
from .clock import Clock
from .commands import CANCEL, CommandDispatcher
from .config import DialerConfig
from .db import Store
from .events import EventProcessor
from .providers.base import EndReason, EventType, ProviderCallStatus, ProviderEvent, ProviderTimeout
from .providers.health import Health, ProviderRouter
from .states import AgentState, CallState


class Reconciler:
    def __init__(self, store: Store, router: ProviderRouter, events: EventProcessor,
                 dispatcher: CommandDispatcher, agents: AgentService, clock: Clock,
                 config: DialerConfig) -> None:
        self._store = store
        self._router = router
        self._events = events
        self._dispatcher = dispatcher
        self._agents = agents
        self._clock = clock
        self._cfg = config
        self.stats: Counter[str] = Counter()

    def run_once(self) -> None:
        self._expire_agents()
        self._resolve_unconfirmed_starts()
        self._resolve_stuck_calls()
        self._events.process_pending()
        self._dispatcher.flush()
        self._expire_wrap_ups()

    # ------------------------------------------------------------------ 1
    def _expire_agents(self) -> None:
        cutoff = self._clock.now() - self._cfg.heartbeat_timeout_s
        for row in self._store.all(
                "SELECT id FROM agents WHERE state != 'OFFLINE' AND heartbeat_at < ?", (cutoff,)):
            if self._agents.logout(row["id"], reason="HEARTBEAT_LOST"):
                self.stats["agents_expired"] += 1

    # ------------------------------------------------------------------ 2
    def _resolve_unconfirmed_starts(self) -> None:
        now = self._clock.now()
        rows = self._store.all(
            """SELECT * FROM calls WHERE state IN ('RESERVED','QUEUED')
               AND lease_until IS NOT NULL AND lease_until < ? LIMIT 200""", (now,))
        for call in rows:
            if self._router.breaker(call["provider"]).state() is Health.OPEN:
                self._unreachable(call, now)             # don't hammer a provider that is down
                continue
            provider = self._router.get(call["provider"])
            try:
                status = provider.get_call_status(call["id"])
            except ProviderTimeout:
                self._router.record(call["provider"], ok=False)
                self._unreachable(call, now)
                continue
            self._router.record(call["provider"], ok=True)
            if status is None:
                with self._store.tx() as c:
                    # Mark the cancel as ours before tombstoning, so if the original worker's
                    # delayed start_call is refused by the tombstone, the allocator does not
                    # blame the provider (and trip its circuit breaker) for our own cancel.
                    c.execute("""UPDATE calls SET cancel_requested_at=COALESCE(cancel_requested_at, ?)
                                 WHERE id=? AND state IN ('RESERVED','QUEUED')""", (now, call["id"]))
                try:
                    provider.cancel_call(call["id"])        # tombstone the id first
                except ProviderTimeout:
                    self._router.record(call["provider"], ok=False)
                    self._unreachable(call, now)
                    continue
                with self._store.tx() as c:
                    if repo.close_call(c, call["id"], CallState.CANCELLED, "START_NOT_CONFIRMED",
                                       now, expect={CallState.RESERVED, CallState.QUEUED}):
                        self.stats["starts_cancelled"] += 1
                continue
            with self._store.tx() as c:
                if repo.confirm_start(c, call["id"], now, status.provider_call_id,
                                      anomaly="ADOPTED"):
                    self.stats["starts_adopted"] += 1
            self._replay(call, status)

    def _unreachable(self, call, now: float) -> None:
        """The provider cannot tell us what happened to this call. Release nothing: the borrower
        may be on the line, and for an agent-bound call may already be talking to the agent the
        provider bridged on answer. Releasing would risk double-booking that agent or re-dialing
        that borrower. While the provider is failing no new calls are placed anyway, so holding
        costs little. Only after max_unresolved_s is the call closed and flagged for review."""
        state = CallState(call["state"])
        with self._store.tx() as c:
            if state is not CallState.CONNECTED and \
                    now - call["created_at"] > self._cfg.max_unresolved_s:
                if repo.close_call(c, call["id"], CallState.FAILED, "UNRESOLVED", now,
                                   expect={state}, anomaly="NEEDS_REVIEW"):
                    repo.enqueue_command(c, call["id"], CANCEL, now)
                    self.stats["unresolved"] += 1
            elif state in (CallState.RESERVED, CallState.QUEUED):
                c.execute("UPDATE calls SET lease_until=? WHERE id=? AND state=?",
                          (now + self._cfg.start_recheck_s, call["id"], str(state)))
            else:                                       # ask again after start_recheck_s
                c.execute("UPDATE calls SET next_check_at=? WHERE id=? AND state=?",
                          (now + self._cfg.start_recheck_s, call["id"], str(state)))

    # ------------------------------------------------------------------ 3
    def _resolve_stuck_calls(self) -> None:
        now = self._clock.now()
        rows = self._store.all(
            """SELECT * FROM calls
               WHERE ((state IN ('INITIATED','RINGING') AND updated_at < ?)
                      OR (state = 'CONNECTED' AND updated_at < ?))
                 AND (next_check_at IS NULL OR next_check_at <= ?)
               LIMIT 200""",
            (now - self._cfg.max_unanswered_s, now - self._cfg.max_connected_s, now))
        for call in rows:
            if self._router.breaker(call["provider"]).state() is Health.OPEN:
                self._unreachable(call, now)             # don't hammer a provider that is down
                continue
            try:
                status = self._router.get(call["provider"]).get_call_status(call["id"])
            except ProviderTimeout:
                self._router.record(call["provider"], ok=False)
                self._unreachable(call, now)
                continue
            self._router.record(call["provider"], ok=True)
            self.stats["stuck_checked"] += 1
            if status is None:
                with self._store.tx() as c:
                    repo.close_call(c, call["id"], CallState.FAILED, "LOST_BY_PROVIDER", now,
                                    expect={CallState(call["state"])})
                continue
            with self._store.tx() as c:                  # checked: don't ask again immediately
                again = self._cfg.max_connected_s if call["state"] == CallState.CONNECTED \
                    else self._cfg.max_unanswered_s
                c.execute("UPDATE calls SET next_check_at=? WHERE id=? AND state=?",
                          (now + again, call["id"], call["state"]))
                if not status.answered and not status.ended:
                    repo.enqueue_command(c, call["id"], CANCEL, now)   # ringing far too long
            self._replay(call, status)

    def _replay(self, call, status: ProviderCallStatus) -> None:
        """Turn a provider status into events and feed them through the normal event path.
        Deterministic ids make repeated reconciliation idempotent; rank rules make them safe to
        interleave with the real (late) webhooks."""
        now = self._clock.now()
        name, cid = call["provider"], call["id"]
        events = []
        if status.answered:
            events.append(ProviderEvent(name, f"reconcile:{cid}:ANSWERED", cid,
                                        EventType.ANSWERED, now, status.provider_call_id))
        if status.ended:
            events.append(ProviderEvent(name, f"reconcile:{cid}:END", cid,
                                        status.end_type or EventType.FAILED, now,
                                        status.provider_call_id,
                                        status.end_reason or EndReason.ERROR))
        for ev in events:
            self._events.receive(ev)
            self.stats["replayed_events"] += 1

    # ------------------------------------------------------------------ 6
    def _expire_wrap_ups(self) -> None:
        now = self._clock.now()
        rows = self._store.all("SELECT id FROM agents WHERE state='WRAP_UP' AND state_since < ?",
                               (now - self._cfg.wrap_up_max_s,))
        if not rows:
            return
        with self._store.tx() as c:                      # one write transaction, not one per agent
            for row in rows:
                repo.move_agent(c, row["id"], AgentState.AVAILABLE, expect={AgentState.WRAP_UP},
                                now=now, call_id=None)
