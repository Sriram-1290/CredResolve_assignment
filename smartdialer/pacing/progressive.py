from __future__ import annotations

from .model import PacingProposal, PacingSnapshot


class ProgressivePacer:
    """One available agent -> one agent-bound call."""

    name = "progressive"

    def propose(self, s: PacingSnapshot) -> PacingProposal:
        n = min(s.agents_available, s.dialable_borrowers)
        why = [f"{s.agents_available} agents AVAILABLE (fresh heartbeat, not reserved) "
               f"-> {n} agent-bound calls, one per agent"]
        if n < s.agents_available:
            why.append(f"limited by {s.dialable_borrowers} borrowers due now")
        return PacingProposal(self.name, bound=n, speculative=0, explanation=tuple(why))
