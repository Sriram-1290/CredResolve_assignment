"""Dialer worker: the composition root. Any number of these can run against the same database.

tick():
  1. reconcile (any worker; safe to run concurrently)
  2. for each active campaign this worker leads (lease + fencing epoch):
        snapshot -> pacer.propose -> safety.decide -> allocator.shed / allocator.execute

Webhooks can be delivered to any worker via receive_event().
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from . import repo
from .agents import AgentService
from .allocator import AllocationReport, CallAllocator
from .clock import Clock
from .commands import CommandDispatcher
from .config import DialerConfig, DialMode
from .db import Store
from .events import EventProcessor
from .faults import NO_FAULTS, Faults
from .observe import SnapshotBuilder
from .pacing import PredictivePacer, PredictiveParams, ProgressivePacer
from .providers.base import ProviderEvent
from .providers.health import ProviderRouter
from .recovery import Reconciler
from .safety import SafetyController, SafetyDecision, SafetyPolicy


@dataclass
class _Leadership:
    epoch: int
    safety: SafetyController


@dataclass(frozen=True)
class TickResult:
    campaign_id: str
    decision: SafetyDecision
    allocation: AllocationReport


class DialerWorker:
    def __init__(self, worker_id: str, store: Store, clock: Clock, router: ProviderRouter, *,
                 config: DialerConfig | None = None, safety_policy: SafetyPolicy | None = None,
                 predictive: PredictiveParams | None = None, faults: Faults = NO_FAULTS,
                 owns: Callable[[str], bool] | None = None) -> None:
        self.worker_id = worker_id
        self.store = store
        self.clock = clock
        self.router = router
        self.config = config or DialerConfig()
        self._policy = safety_policy or SafetyPolicy()
        self.faults = faults
        self.dispatcher = CommandDispatcher(store, router, clock, self.config)
        self.agents = AgentService(store, clock, self.dispatcher, self.config)
        self.events = EventProcessor(store, clock, self.dispatcher, self.config, faults)
        self.allocator = CallAllocator(store, router, self.dispatcher, clock, self.config,
                                       worker_id, faults)
        self.reconciler = Reconciler(store, router, self.events, self.dispatcher, self.agents,
                                     clock, self.config)
        self.snapshots = SnapshotBuilder(store, self.config,
                                         late_event_s=self._policy.late_event_s)
        self._pacers = {DialMode.PROGRESSIVE: ProgressivePacer(),
                        DialMode.PREDICTIVE: PredictivePacer(predictive)}
        self._leading: dict[str, _Leadership] = {}
        # Campaigns this worker paces by preference; others only if their leader looks dead.
        self._owns = owns or (lambda _campaign_id: True)
        self._started_at = clock.now()

    def receive_event(self, ev: ProviderEvent) -> str:
        return self.events.receive(ev)

    def tick(self) -> list[TickResult]:
        self.reconciler.run_once()
        results = []
        for row in self.store.all("SELECT id FROM campaigns WHERE active=1 ORDER BY id"):
            r = self._pace(row["id"])
            if r is not None:
                results.append(r)
        return results

    def _pace(self, campaign_id: str) -> TickResult | None:
        now = self.clock.now()
        owner = self._owns(campaign_id)
        takeover = 0.0 if owner else 2 * self.config.leader_lease_s
        if not owner and now - self._started_at < takeover:
            # A campaign that has never had a leader would otherwise go to whichever worker
            # ticks first. Give its owner the first claim; take it over only if nobody does.
            self._leading.pop(campaign_id, None)
            return None
        with self.store.tx() as c:
            lead = repo.acquire_leadership(c, campaign_id, self.worker_id, now,
                                           self.config.leader_lease_s, takeover)
        if lead is None:
            self._leading.pop(campaign_id, None)
            return None
        state = self._leading.get(campaign_id)
        if state is not None and lead["epoch"] == state.epoch + 1:
            # Our own lease lapsed (e.g. a slow tick) and we re-acquired it; nobody else led in
            # between (that would have taken the epoch past +1). Keep the controller, so active
            # cool-downs (abandon limit, agent drop, answer surge) are not forgotten.
            state.epoch = lead["epoch"]
        elif state is None or state.epoch != lead["epoch"]:
            # New term: start with a fresh controller (empty history -> warm-up, conservative).
            state = self._leading[campaign_id] = _Leadership(
                lead["epoch"], SafetyController(self._policy, self.clock))
        mode = DialMode(lead["mode"])
        snapshot = self.snapshots.build(campaign_id, now)
        proposal = self._pacers[mode].propose(snapshot)
        decision = state.safety.decide(proposal, snapshot, self.router.health(), state.epoch, mode)
        self._record(decision)
        shed = self.allocator.shed(decision.permit)
        report = self.allocator.execute(decision.permit)
        report.shed = shed
        return TickResult(campaign_id, decision, report)

    def _record(self, d: SafetyDecision) -> None:
        with self.store.tx() as c:
            c.execute(
                """INSERT INTO decisions (campaign_id, at, worker, epoch, pacer, proposed,
                       approved_bound, approved_spec, shed, action, explanation)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (d.permit.campaign_id, d.permit.issued_at, self.worker_id, d.permit.epoch,
                 d.proposal.pacer, d.proposal.total, d.permit.bound, d.permit.speculative,
                 d.permit.shed, str(d.action), d.explain()))
