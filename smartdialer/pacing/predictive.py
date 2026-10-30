"""Predictive pacing: how many calls to start now so that answers line up with agents freeing up.

The model is deliberately simple and fully explainable:

    p          = answer rate: a Bayesian estimate (last 60s, using the last 5 min as the prior,
                 which in turn uses a configured campaign prior)
    T          = expected dial -> answer time (observed mean)
    capacity   = agents free now
               + agents in wrap-up expected to finish within T
               + talking agents expected to finish talking and wrap-up within T
                 (talk time treated as exponential, so P(done within x) = 1 - exp(-x / mean_talk))
    expected   = un-answered agentless calls already in flight * p
    proposal   = floor((capacity - expected) / p)        # dials needed to produce the missing answers

It optimises utilisation only. Whether the proposal is *allowed* is the Safety Controller's job.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from .model import PacingProposal, PacingSnapshot


@dataclass(frozen=True)
class PredictiveParams:
    prior_answer_rate: float = 0.3
    prior_weight: float = 20.0          # pseudo-attempts behind the prior
    short_weight: float = 30.0          # how strongly the 5-min rate anchors the 60-s rate
    default_dial_to_answer_s: float = 15.0
    default_talk_s: float = 120.0
    expected_wrap_s: float = 10.0
    max_ratio: float = 8.0              # never more agentless calls in flight than this x capacity


class PredictivePacer:
    name = "predictive"

    def __init__(self, params: PredictiveParams | None = None) -> None:
        self.params = params or PredictiveParams()

    def answer_rate(self, s: PacingSnapshot) -> float:
        prm = self.params
        p_long = (s.answers_long + prm.prior_answer_rate * prm.prior_weight) / \
                 (s.attempts_long + prm.prior_weight)
        p = (s.answers_short + p_long * prm.short_weight) / (s.attempts_short + prm.short_weight)
        return min(max(p, 0.01), 1.0)

    def propose(self, s: PacingSnapshot) -> PacingProposal:
        prm = self.params
        p = self.answer_rate(s)
        t_ans = s.mean_dial_to_answer_s or prm.default_dial_to_answer_s
        talk = s.mean_talk_s or prm.default_talk_s
        wrap = prm.expected_wrap_s

        wrap_soon = s.agents_wrap_up * min(1.0, t_ans / wrap) if wrap > 0 else s.agents_wrap_up
        p_talk_done = 1.0 - math.exp(-max(t_ans - wrap, 0.0) / talk) if talk > 0 else 1.0
        talk_soon = s.agents_connected * p_talk_done
        capacity = s.agents_available + wrap_soon + talk_soon

        expected = s.unbound_in_flight * p
        missing = capacity - expected
        raw = missing / p
        n = max(0, math.floor(raw))

        why = [
            f"answer rate p={p:.2f} (60s: {s.answers_short}/{s.attempts_short}, "
            f"5m: {s.answers_long}/{s.attempts_long}, prior {prm.prior_answer_rate:.2f})",
            f"dial->answer ~{t_ans:.0f}s; talk ~{talk:.0f}s; wrap ~{wrap:.0f}s",
            f"agents free within {t_ans:.0f}s: {s.agents_available} available"
            f" + {wrap_soon:.1f} of {s.agents_wrap_up} in wrap-up"
            f" + {talk_soon:.1f} of {s.agents_connected} talking = {capacity:.1f}",
            f"{s.unbound_in_flight} un-answered calls in flight x p = {expected:.1f} answers expected",
            f"missing {missing:.1f} answers / p = {raw:.1f} -> {n} calls",
        ]
        cap_ratio = max(0, math.ceil(prm.max_ratio * capacity) - s.unbound_in_flight)
        if n > cap_ratio:
            n = cap_ratio
            why.append(f"capped at {n} by max ratio {prm.max_ratio:g} calls per agent")
        if n > s.dialable_borrowers:
            n = s.dialable_borrowers
            why.append(f"capped at {n}: only {s.dialable_borrowers} borrowers due")
        return PacingProposal(self.name, bound=0, speculative=n, explanation=tuple(why))
