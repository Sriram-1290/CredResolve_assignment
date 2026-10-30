"""Call Allocator: the only code that calls `provider.start_call`, and only under a DialPermit.

For each permitted call:

  tx 1  check fencing epoch (stale pacing leaders are refused)
        [bound]       reserve an agent           AVAILABLE -> RESERVED   (CAS)
        [speculative] check agentless calls in flight < permit ceiling   (atomic with the insert)
        claim a borrower                         PENDING -> IN_PROGRESS  (CAS)
        insert the call (RESERVED / QUEUED) with a lease owned by this worker
  ----  provider.start_call(call_id)  -- network I/O, never inside a transaction;
        call_id doubles as the idempotency key
  tx 2  RESERVED/QUEUED -> INITIATED, agent RESERVED -> DIALING, clear the lease

If the worker dies anywhere in between, the lease on the call row expires and the reconciler asks
the provider what happened to that call id (see recovery.py).
"""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass

from . import repo
from .clock import Clock
from .commands import CANCEL, CommandDispatcher
from .config import DialerConfig
from .db import Store
from .faults import NO_FAULTS, Faults
from .providers.base import ProviderRejected, ProviderTimeout
from .providers.health import ProviderRouter
from .safety import DialPermit
from .states import CallKind, CallState, UNANSWERED_CALL_STATES, sql_in

_UNANSWERED = sql_in(UNANSWERED_CALL_STATES)


@dataclass
class AllocationReport:
    reserved: int = 0
    started: int = 0
    rejected: int = 0
    timeouts: int = 0
    shed: int = 0
    stale_permit: bool = False
    integrity_conflicts: int = 0


@dataclass(frozen=True)
class _Reserved:
    id: str
    phone: str
    provider: str
    agent_id: str | None


class _NothingToDo(Exception):
    pass


class _StaleEpoch(Exception):
    pass


class CallAllocator:
    def __init__(self, store: Store, router: ProviderRouter, dispatcher: CommandDispatcher,
                 clock: Clock, config: DialerConfig, worker_id: str,
                 faults: Faults = NO_FAULTS) -> None:
        self._store = store
        self._router = router
        self._dispatcher = dispatcher
        self._clock = clock
        self._cfg = config
        self.worker_id = worker_id
        self._faults = faults

    def execute(self, permit: DialPermit) -> AllocationReport:
        report = AllocationReport()
        if not isinstance(permit, DialPermit) or not permit.valid_at(self._clock.now()):
            report.stale_permit = True
            return report
        for bound, count in ((True, permit.bound), (False, permit.speculative)):
            for _ in range(count):
                try:
                    call = self._reserve(permit, bound)
                except _StaleEpoch:
                    report.stale_permit = True
                    return report
                except sqlite3.IntegrityError:
                    # The unique indexes refused a second live call for an agent/borrower.
                    report.integrity_conflicts += 1
                    continue
                if call is None:
                    break
                report.reserved += 1
                self._start(call, report)
        return report

    def shed(self, permit: DialPermit) -> int:
        """Ask the provider to cancel the newest agentless calls that have not been answered,
        preferring ones that are not ringing at the borrower's phone yet."""
        if permit.shed <= 0 or not permit.valid_at(self._clock.now()):
            return 0
        now = self._clock.now()
        with self._store.tx() as c:
            epoch = c.execute("SELECT epoch FROM campaigns WHERE id=?",
                              (permit.campaign_id,)).fetchone()
            if epoch is None or epoch["epoch"] != permit.epoch:
                return 0                                # same fencing as execute()
            rows = c.execute(
                f"""SELECT id FROM calls
                    WHERE campaign_id=? AND agent_id IS NULL AND cancel_requested_at IS NULL
                      AND state IN {_UNANSWERED}
                    ORDER BY state='RINGING', created_at DESC LIMIT ?""",
                (permit.campaign_id, permit.shed)).fetchall()
            for r in rows:
                c.execute("UPDATE calls SET cancel_requested_at=?, anomaly='SHED' WHERE id=?",
                          (now, r["id"]))
                repo.enqueue_command(c, r["id"], CANCEL, now)
        for r in rows:
            self._dispatcher.flush(call_id=r["id"])
        return len(rows)

    # ------------------------------------------------------------------ steps
    def _reserve(self, permit: DialPermit, bound: bool) -> _Reserved | None:
        provider = self._router.pick()
        if provider is None:
            return None
        now = self._clock.now()
        call_id = uuid.uuid4().hex
        fresh_after = now - self._cfg.heartbeat_timeout_s
        try:
            with self._store.tx() as c:
                camp = c.execute("SELECT epoch, active FROM campaigns WHERE id=?",
                                 (permit.campaign_id,)).fetchone()
                if camp is None or camp["epoch"] != permit.epoch or not camp["active"]:
                    raise _StaleEpoch()
                agent_id = None
                if bound:
                    agent_id = repo.reserve_available_agent(c, permit.campaign_id, call_id, now,
                                                            fresh_after)
                    if agent_id is None:
                        raise _NothingToDo()
                else:
                    in_flight = c.execute(
                        f"""SELECT COUNT(*) FROM calls WHERE campaign_id=? AND agent_id IS NULL
                            AND state IN {_UNANSWERED} AND created_at >= ?""",
                        (permit.campaign_id, now - self._cfg.max_unanswered_s)).fetchone()[0]
                    if in_flight >= permit.unbound_ceiling:
                        raise _NothingToDo()
                borrower = repo.claim_borrower(c, permit.campaign_id, call_id, now)
                if borrower is None:
                    raise _NothingToDo()            # rolls back the agent reservation too
                self._faults.check("after_reserve")
                c.execute(
                    """INSERT INTO calls (id, campaign_id, borrower_id, agent_id, kind, state,
                           provider, owner, lease_until, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (call_id, permit.campaign_id, borrower["id"], agent_id,
                     str(CallKind.BOUND if bound else CallKind.SPECULATIVE),
                     str(CallState.RESERVED if bound else CallState.QUEUED), provider,
                     self.worker_id, now + self._cfg.start_lease_s, now, now))
        except _NothingToDo:
            return None
        return _Reserved(call_id, borrower["phone"], provider, agent_id)

    def _start(self, call: _Reserved, report: AllocationReport) -> None:
        provider = self._router.get(call.provider)
        self._faults.check("before_start_call")
        # Re-read right before dialing: if the agent left since the reservation, the call was
        # detached and a cancel requested. Don't dial it at all, and never hand the provider an
        # agent who is gone. (A logout *during* start_call is still possible; the cancel then
        # races the answer, and an answer is re-bridged to whichever agent claims it.)
        cur = self._store.one("SELECT agent_id, cancel_requested_at FROM calls WHERE id=?",
                              (call.id,))
        if cur is None or cur["cancel_requested_at"] is not None:
            with self._store.tx() as c:
                repo.close_call(c, call.id, CallState.CANCELLED, "CANCELLED_BEFORE_START",
                                self._clock.now(), expect={CallState.RESERVED, CallState.QUEUED})
            return
        try:
            pcid = provider.start_call(call.id, call.phone, cur["agent_id"])
        except ProviderTimeout:
            # Ambiguous: the call may exist. Keep everything reserved and let the reconciler ask
            # the provider soon; it will adopt the call or cancel it and release the agent.
            self._router.record(call.provider, ok=False)
            report.timeouts += 1
            now = self._clock.now()
            with self._store.tx() as c:
                c.execute("""UPDATE calls SET lease_until=?, anomaly='START_TIMEOUT', updated_at=?
                             WHERE id=? AND state IN ('RESERVED','QUEUED')""",
                          (now + self._cfg.start_recheck_s, now, call.id))
            return
        except ProviderRejected:
            with self._store.tx() as c:
                cur = repo.get_call(c, call.id)
                # Our own CANCEL (agent logout, shed) may have tombstoned the id before
                # start_call: that refusal is ours, not the provider's or the borrower's fault.
                ours = cur is not None and cur["cancel_requested_at"] is not None
                repo.close_call(c, call.id, CallState.CANCELLED if ours else CallState.FAILED,
                                "CANCELLED_BEFORE_START" if ours else "PROVIDER_REJECTED",
                                self._clock.now(), expect={CallState.RESERVED, CallState.QUEUED})
            if not ours:
                self._router.record(call.provider, ok=False)
                report.rejected += 1
            return
        self._router.record(call.provider, ok=True)
        report.started += 1
        self._faults.check("after_start_call")
        now = self._clock.now()
        with self._store.tx() as c:
            if not repo.confirm_start(c, call.id, now, pcid):
                # Provider webhooks overtook us (fine), or the call was closed meanwhile.
                c.execute("""UPDATE calls SET provider_call_id=COALESCE(provider_call_id, ?),
                                 initiated_at=COALESCE(initiated_at, ?), owner=NULL,
                                 lease_until=NULL WHERE id=?""", (pcid, now, call.id))
        self._dispatcher.flush(call_id=call.id)
