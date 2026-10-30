"""A tiny world around one campaign with a ScriptedProvider: every event is delivered by hand.

Used by the step-by-step failure demos (sim/failures.py) and by the tests.
"""
from __future__ import annotations

import itertools

from smartdialer import repo
from smartdialer.clock import SimClock
from smartdialer.config import CampaignConfig, DialerConfig, DialMode
from smartdialer.db import Store
from smartdialer.faults import Faults
from smartdialer.providers.base import EventType, ProviderEvent
from smartdialer.providers.health import ProviderRouter
from smartdialer.providers.scripted import ScriptedProvider
from smartdialer.safety import SafetyPolicy
from smartdialer.worker import DialerWorker

CAMPAIGN = "c1"


class ScriptedWorld:
    def __init__(self, agents: int = 3, borrowers: int = 20, mode: DialMode = DialMode.PROGRESSIVE,
                 policy: SafetyPolicy | None = None, config: DialerConfig | None = None,
                 store: Store | None = None) -> None:
        self.clock = SimClock(1000.0)
        self.store = store or Store(":memory:")
        self.provider = ScriptedProvider()
        self.config = config or DialerConfig()
        self.policy = policy or SafetyPolicy()
        repo.create_campaign(self.store, CampaignConfig(CAMPAIGN, mode, max_attempts=2))
        self.agent_ids = [f"a{i}" for i in range(agents)]
        repo.add_agents(self.store, CAMPAIGN, self.agent_ids, self.clock.now())
        repo.add_borrowers(self.store, CAMPAIGN, borrowers)
        self._eids = itertools.count(1)
        self.worker = self.new_worker("w1")

    def new_worker(self, worker_id: str, faults: Faults | None = None) -> DialerWorker:
        router = ProviderRouter([self.provider], self.clock)
        return DialerWorker(worker_id, self.store, self.clock, router, config=self.config,
                            safety_policy=self.policy, faults=faults or Faults())

    # --------------------------------------------------------------- driving
    def advance(self, seconds: float) -> None:
        self.clock.advance_to(self.clock.now() + seconds)

    def heartbeat_all(self) -> None:
        self.worker.agents.heartbeat(self.agent_ids)

    def ev(self, call_id: str, etype: EventType, reason: str | None = None,
           event_id: str | None = None) -> ProviderEvent:
        return ProviderEvent(self.provider.name, event_id or f"e{next(self._eids)}", call_id,
                             etype, self.clock.now(), self.provider.placed.get(call_id), reason)

    # --------------------------------------------------------------- reading
    def call(self, call_id: str):
        return self.store.one("SELECT * FROM calls WHERE id=?", (call_id,))

    def calls(self, where: str = "1=1"):
        return self.store.all(f"SELECT * FROM calls WHERE {where} ORDER BY created_at, id")

    def agent(self, agent_id: str):
        return self.store.one("SELECT * FROM agents WHERE id=?", (agent_id,))

    def borrower(self, borrower_id: str):
        return self.store.one("SELECT * FROM borrowers WHERE id=?", (borrower_id,))

    def agent_states(self) -> dict[str, str]:
        return {r["id"]: r["state"] for r in self.store.all("SELECT id, state FROM agents")}
