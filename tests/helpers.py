"""Test fixtures: the scripted world from sim/scripted.py, plus test-only permit minting."""
from __future__ import annotations

from sim.scripted import CAMPAIGN, ScriptedWorld
from smartdialer import repo
from smartdialer.providers.base import EventType
from smartdialer.safety import _SEAL, DialPermit


class World(ScriptedWorld):
    def permit(self, *, bound: int = 0, speculative: int = 0, ceiling: int = 0, shed: int = 0,
               epoch: int | None = None) -> DialPermit:
        """Tests only: mint a permit directly. Production code never touches _SEAL; the
        SafetyController is the only issuer (see test_safety.py::test_a_permit_cannot_be_forged)."""
        if epoch is None:
            with self.store.tx() as c:
                epoch = repo.acquire_leadership(c, CAMPAIGN, "test", self.clock.now(), 60)["epoch"]
        now = self.clock.now()
        return DialPermit(CAMPAIGN, epoch, bound, speculative, ceiling, shed, now, now + 2,
                          _seal=_SEAL)


__all__ = ["CAMPAIGN", "World", "EventType"]
