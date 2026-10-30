"""Discrete-event simulation harness.

Runs the real DialerWorker code (real SQLite, real allocator/safety/events/reconciler) against
MockProviders on a simulated clock, so an hour of dialing takes seconds. The harness plays the
parts that are outside the dialer: agents' desktops (heartbeats, finishing wrap-up), the webhook
load balancer (delivering provider events to a live worker), and operators/chaos (outages,
agents vanishing, workers crashing).
"""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

from smartdialer import repo
from smartdialer.agents import AgentService
from smartdialer.clock import EventLoop, SimClock
from smartdialer.commands import CommandDispatcher
from smartdialer.config import CampaignConfig, DialerConfig, DialMode
from smartdialer.db import Store
from smartdialer.faults import Faults, SimulatedCrash
from smartdialer.invariants import check_invariants
from smartdialer.pacing import PredictiveParams
from smartdialer.providers.base import ProviderEvent
from smartdialer.providers.health import ProviderRouter
from smartdialer.providers.mock import PROVIDER_A, BorrowerModel, MockProfile, MockProvider, Schedule
from smartdialer.safety import SafetyPolicy
from smartdialer.worker import DialerWorker, TickResult

CAMPAIGN = "camp-1"


@dataclass
class ScenarioSpec:
    name: str
    mode: DialMode
    answer_rate: Schedule
    talk_mean_s: Schedule
    agents: int = 50
    duration_s: float = 3600.0
    providers: tuple[MockProfile, ...] = (PROVIDER_A,)
    wrap_s: tuple[float, float] = (6.0, 14.0)
    borrowers: int = 30000
    workers: int = 1
    seed: int = 7
    description: str = ""
    actions: tuple[tuple[float, Callable[["Simulation"], None]], ...] = ()
    policy: SafetyPolicy = field(default_factory=SafetyPolicy)
    predictive: PredictiveParams = field(default_factory=PredictiveParams)
    config: DialerConfig = field(default_factory=DialerConfig)


@dataclass
class Sample:
    t: float
    logged_in: int
    available: int
    reserved: int
    connected: int
    wrap_up: int
    unbound_in_flight: int
    bound_in_flight: int
    action: str
    proposed: int
    approved: int
    p_ucb: float
    capacity: int


class Simulation:
    def __init__(self, spec: ScenarioSpec) -> None:
        self.spec = spec
        self.clock = SimClock(0.0)
        self.loop = EventLoop(self.clock)
        self.store = Store(":memory:")
        self.rng = random.Random(spec.seed)
        self.borrowers = BorrowerModel(answer_rate=spec.answer_rate, talk_mean_s=spec.talk_mean_s)
        self.providers = [
            MockProvider(p, self.loop, self.borrowers, random.Random(spec.seed * 100 + i),
                         sink=self._webhook)
            for i, p in enumerate(spec.providers)
        ]
        repo.create_campaign(self.store, CampaignConfig(CAMPAIGN, spec.mode))
        self.agent_ids = [f"agent-{i:04d}" for i in range(spec.agents)]
        repo.add_agents(self.store, CAMPAIGN, self.agent_ids, 0.0)
        repo.add_borrowers(self.store, CAMPAIGN, spec.borrowers, random.Random(spec.seed))
        self.present: set[str] = set(self.agent_ids)          # agents whose desktop is alive
        self._wrap_deadline: dict[tuple[str, float], float] = {}
        self._held_webhooks: list[ProviderEvent] = []
        self._rr = 0
        self.decisions: list[TickResult] = []
        self.samples: list[Sample] = []
        self.state_seconds: Counter[str] = Counter()
        self.event_outcomes: Counter[str] = Counter()
        self.log: list[str] = []
        self.crashes: list[str] = []
        self.orphaned: dict[str, list[str]] = {}      # calls a crashed worker still held a lease on

        self.workers: list[DialerWorker] = []
        self.alive: list[DialerWorker] = []
        for i in range(spec.workers):
            self.start_worker(f"w{i}")
        desk_router = ProviderRouter(self.providers, self.clock)
        self.desk = AgentService(self.store, self.clock,
                                 CommandDispatcher(self.store, desk_router, self.clock, spec.config),
                                 spec.config)

        self.loop.every(spec.config.tick_s, self._tick_workers, start_delay=0.5)
        self.loop.every(1.0, self._agent_desk, start_delay=0.25)
        self.loop.every(1.0, self._sample, start_delay=0.75)
        for t, action in spec.actions:
            self.loop.call_at(t, lambda action=action: action(self))

    # ------------------------------------------------------------------ chaos controls
    def start_worker(self, worker_id: str) -> DialerWorker:
        w = DialerWorker(worker_id, self.store, self.clock, ProviderRouter(self.providers, self.clock),
                         config=self.spec.config, safety_policy=self.spec.policy,
                         predictive=self.spec.predictive, faults=Faults())
        self.workers.append(w)
        self.alive.append(w)
        self.note(f"worker {worker_id} started")
        for ev in self._held_webhooks:                 # provider retries webhooks it couldn't deliver
            self._webhook(ev)
        self._held_webhooks = []
        return w

    def crash_worker_at(self, worker_id: str, point: str) -> None:
        for w in self.alive:
            if w.worker_id == worker_id:
                w.faults.arm(point)
                self.note(f"armed crash of {worker_id} at '{point}'")

    def vanish_agents(self, n: int) -> list[str]:
        """Agents' desktops die silently: no logout, heartbeats just stop."""
        gone = [a for a in self.agent_ids if a in self.present][:n]
        for a in gone:
            self.present.discard(a)
        self.note(f"{len(gone)} agents vanished (heartbeats stopped)")
        return gone

    def set_outage(self, provider_index: int, on: bool) -> None:
        self.providers[provider_index].set_outage(on)
        self.note(f"provider {self.providers[provider_index].name} outage {'START' if on else 'END'}")

    def note(self, msg: str) -> None:
        self.log.append(f"t={self.clock.now():7.1f}s  {msg}")

    # ------------------------------------------------------------------ simulated surroundings
    def _kill(self, w: DialerWorker, exc: SimulatedCrash) -> None:
        if w in self.alive:
            self.alive.remove(w)
            self.crashes.append(f"{w.worker_id}@{exc}")
            self.orphaned[w.worker_id] = [r["id"] for r in self.store.all(
                "SELECT id FROM calls WHERE owner=?", (w.worker_id,))]
            self.note(f"worker {w.worker_id} CRASHED at '{exc}'")

    def _webhook(self, ev: ProviderEvent) -> None:
        if not self.alive:
            self._held_webhooks.append(ev)
            return
        w = self.alive[self._rr % len(self.alive)]
        self._rr += 1
        try:
            self.event_outcomes[w.receive_event(ev)] += 1
        except SimulatedCrash as exc:
            self._kill(w, exc)

    def _tick_workers(self) -> None:
        for w in list(self.alive):
            try:
                self.decisions.extend(w.tick())
            except SimulatedCrash as exc:
                self._kill(w, exc)

    def _agent_desk(self) -> None:
        now = self.clock.now()
        self.desk.heartbeat(self.present)
        for row in self.store.all("SELECT id, state_since FROM agents WHERE state='WRAP_UP'"):
            if row["id"] not in self.present:
                continue
            key = (row["id"], row["state_since"])
            deadline = self._wrap_deadline.get(key)
            if deadline is None:
                deadline = self._wrap_deadline[key] = row["state_since"] + \
                    self.rng.uniform(*self.spec.wrap_s)
            if now >= deadline:
                self.desk.finish_wrap_up(row["id"])
                del self._wrap_deadline[key]

    def _sample(self) -> None:
        counts = {r[0]: r[1] for r in self.store.all("SELECT state, COUNT(*) FROM agents GROUP BY state")}
        for state, n in counts.items():
            self.state_seconds[state] += n
        logged_in = sum(n for s, n in counts.items() if s != "OFFLINE")
        self.state_seconds["LOGGED_IN"] += logged_in
        if int(self.clock.now()) % 10 != 0:
            return
        flight = self.store.one(
            """SELECT COALESCE(SUM(agent_id IS NULL),0) u, COALESCE(SUM(agent_id IS NOT NULL),0) b
               FROM calls WHERE state IN ('QUEUED','RESERVED','INITIATED','RINGING')""")
        last = self.decisions[-1].decision if self.decisions else None
        self.samples.append(Sample(
            t=self.clock.now(), logged_in=logged_in, available=counts.get("AVAILABLE", 0),
            reserved=counts.get("RESERVED", 0) + counts.get("DIALING", 0),
            connected=counts.get("CONNECTED", 0), wrap_up=counts.get("WRAP_UP", 0),
            unbound_in_flight=flight["u"], bound_in_flight=flight["b"],
            action=str(last.action) if last else "", proposed=last.proposal.total if last else 0,
            approved=(last.permit.bound + last.permit.speculative) if last else 0,
            p_ucb=last.answer_rate_ucb if last else 0.0, capacity=last.capacity if last else 0))

    # ------------------------------------------------------------------ run
    def run(self) -> "SimResult":
        self.loop.run_until(self.spec.duration_s)
        return SimResult.collect(self)


@dataclass
class SimResult:
    name: str
    mode: str
    providers: str
    agents: int
    minutes: float
    utilization: float           # talking time / logged-in time
    occupancy: float             # (talking + wrap-up) / logged-in time
    idle: float                  # AVAILABLE time / logged-in time
    waiting: float               # (AVAILABLE + RESERVED + DIALING) / logged-in: not yet talking
    calls_initiated: int
    answered: int
    connected: int
    abandoned: int
    abandon_rate: float
    failed: dict[str, int]
    cancelled: int
    connects_per_agent_hour: float
    dials_per_connect: float
    actions: dict[str, int]
    mean_proposed: float
    mean_approved: float
    invariant_violations: list[str]
    crashes: list[str]
    log: list[str]
    samples: list[Sample]
    explained: list[str]

    @staticmethod
    def collect(sim: Simulation) -> "SimResult":
        st = sim.store
        secs = sim.state_seconds
        logged = max(secs["LOGGED_IN"], 1)
        initiated = st.scalar("SELECT COUNT(*) FROM calls WHERE provider_call_id IS NOT NULL")
        answered = st.scalar("SELECT COUNT(*) FROM calls WHERE answered_at IS NOT NULL")
        connected = st.scalar("SELECT COUNT(*) FROM calls WHERE connected_at IS NOT NULL")
        abandoned = st.scalar("SELECT COUNT(*) FROM calls WHERE state='ABANDONED'")
        answered_live = st.scalar("SELECT COUNT(*) FROM calls WHERE connected_at IS NOT NULL "
                                  "AND state != 'ABANDONED'")
        failed = {r[0]: r[1] for r in st.all(
            "SELECT end_reason, COUNT(*) FROM calls WHERE state='FAILED' GROUP BY end_reason")}
        cancelled = st.scalar("SELECT COUNT(*) FROM calls WHERE state='CANCELLED'")
        actions = Counter(str(d.decision.action) for d in sim.decisions)
        hours = sim.spec.duration_s / 3600
        interesting = [d for d in sim.decisions
                       if d.decision.proposal.total > 0 or d.decision.permit.shed > 0]
        step = max(1, len(interesting) // 4)
        return SimResult(
            name=sim.spec.name, mode=str(sim.spec.mode),
            providers="+".join(p.name for p in sim.providers), agents=sim.spec.agents,
            minutes=sim.spec.duration_s / 60,
            utilization=secs["CONNECTED"] / logged,
            occupancy=(secs["CONNECTED"] + secs["WRAP_UP"]) / logged,
            idle=secs["AVAILABLE"] / logged,
            waiting=(secs["AVAILABLE"] + secs["RESERVED"] + secs["DIALING"]) / logged,
            calls_initiated=initiated, answered=answered, connected=connected,
            # A call bridged and then abandoned (BRIDGE_FAILED) has connected_at set too:
            # count it once, as an abandon.
            abandoned=abandoned, abandon_rate=abandoned / max(answered_live + abandoned, 1),
            failed=failed, cancelled=cancelled,
            connects_per_agent_hour=connected / max(sim.spec.agents * hours, 1e-9),
            dials_per_connect=initiated / max(connected, 1),
            actions=dict(actions),
            mean_proposed=sum(d.decision.proposal.total for d in sim.decisions) / max(len(sim.decisions), 1),
            mean_approved=sum(d.decision.permit.bound + d.decision.permit.speculative
                              for d in sim.decisions) / max(len(sim.decisions), 1),
            invariant_violations=check_invariants(st),
            crashes=sim.crashes, log=sim.log, samples=sim.samples,
            explained=[f"t={d.decision.permit.issued_at:.0f}s\n{d.decision.explain()}"
                       for d in interesting[::step][:4]],
        )
