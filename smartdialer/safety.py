"""Safety Controller: the only component that can authorise calls.

Pacer -> PacingProposal (a wish) -> SafetyController.decide -> DialPermit (an authorisation)
-> CallAllocator (refuses anything that is not a valid, current-epoch permit).

The controller does not trust the pacer's model. It recomputes everything from the snapshot with
its own, deliberately pessimistic assumptions:

* answer rate = Wilson *upper* bound over the long and the short window, whichever is higher
  (no data -> 1.0, which collapses every bound below to exactly progressive dialing);
* capacity    = agents that are free *now* (+ agents in wrap-up, which the system can pre-empt);
  agents who are still talking are never counted, because we do not control when they finish;
* ceiling     = the largest number of agentless un-answered calls such that
                P(more of them answer than capacity) <= overflow_tolerance.

On top of the bound there are circuit breakers that force progressive (or zero) dialing: provider
health, too little data, realised abandon rate over the limit, a sudden drop in logged-in agents,
and a shift in answer rate between the short and long windows.

The policy is a frozen dataclass handed to the controller by the composition root. Neither the
pacer nor the allocator can reach or change it.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum

from .clock import Clock
from .config import DialMode
from .pacing.model import PacingProposal, PacingSnapshot
from .pacing.stats import (
    agents_needed, binom_sf, max_calls_within_risk, wilson_lower, wilson_upper,
)
from .providers.health import Health


class SafetyAction(StrEnum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_PROGRESSIVE = "FALLBACK_PROGRESSIVE"


@dataclass(frozen=True)
class SafetyPolicy:
    abandon_limit: float = 0.03          # rolling abandon rate that forces progressive (regulatory)
    abandon_soft_limit: float = 0.015    # above this the overflow tolerance is halved
    min_abandon_samples: int = 30        # answered calls before the abandon rate is trusted
    overflow_tolerance: float = 0.02     # max P(answers > capacity) when adding calls
    shed_tolerance: float = 0.10         # above this, cancel un-answered agentless calls
    confidence_z: float = 1.96
    min_samples: int = 50                # resolved attempts before predictive is allowed
    count_wrap_up: bool = True
    max_dial_ratio: float = 8.0
    max_new_calls_per_tick: int = 100
    agent_drop_fraction: float = 0.15
    agent_drop_window_s: float = 10.0
    answer_shift: float = 0.10           # a change in answer rate this large ...
    answer_shift_z: float = 3.5          # ... and this unlikely to be noise invalidates the model
    min_short_samples: int = 30
    late_event_s: float = 3.0            # webhook later than this: an agentless call can't be
                                         #   bridged before the borrower gives up
    max_late_event_rate: float = 0.005   # a late ANSWERED is ~one abandon, so even 1% late would
                                         #   spend a third of the 3% abandon budget on this alone
    min_event_samples: int = 50
    min_cohort: int = 20                 # recent dials needed for the answer-surge check
    surge_z: float = 2.58                # confidence for "answers already exceed the model"
    cooldown_s: float = 60.0
    warmup_s: float = 10.0               # a new pacing leader observes before dialing ahead


_SEAL = object()


@dataclass(frozen=True)
class DialPermit:
    """Authorisation to place calls. Only SafetyController can construct one."""
    campaign_id: str
    epoch: int
    bound: int                 # agent-bound calls (agent reserved before dialing)
    speculative: int           # agentless calls
    unbound_ceiling: int       # max agentless un-answered calls in flight (checked atomically)
    shed: int                  # agentless un-answered calls to cancel now
    issued_at: float
    expires_at: float
    _seal: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _SEAL:
            raise PermissionError("a DialPermit can only be issued by the SafetyController")

    def valid_at(self, now: float) -> bool:
        return self._seal is _SEAL and self.issued_at <= now <= self.expires_at


@dataclass(frozen=True)
class SafetyDecision:
    action: SafetyAction
    proposal: PacingProposal
    permit: DialPermit
    mode: str                   # "predictive" | "progressive" | "halt"
    answer_rate_ucb: float
    capacity: int
    overflow_risk: float        # P(answers > capacity) for what is in flight after this decision
    reasons: tuple[str, ...]
    health: str = ""
    snapshot: PacingSnapshot | None = None

    def explain(self) -> str:
        p = self.proposal
        lines = [f"pacer[{p.pacer}] proposed {p.total} "
                 f"(bound={p.bound}, speculative={p.speculative}):"]
        lines += [f"    {x}" for x in p.explanation]
        lines.append(f"safety: {self.action} -> bound={self.permit.bound}, "
                     f"speculative={self.permit.speculative}, shed={self.permit.shed} ({self.mode})")
        lines += [f"    {x}" for x in self.reasons]
        return "\n".join(lines)


class SafetyController:
    def __init__(self, policy: SafetyPolicy, clock: Clock, permit_ttl_s: float = 2.0) -> None:
        self._policy = policy
        self._clock = clock
        self._ttl = permit_ttl_s
        self._logged_in: deque[tuple[float, int]] = deque()
        self._cooldown_until = -math.inf
        self._cooldown_reason = ""
        self._shock_until = -math.inf
        self._surge_until = -math.inf
        self._started_at: float | None = None

    @property
    def policy(self) -> SafetyPolicy:
        return self._policy

    def decide(self, proposal: PacingProposal, s: PacingSnapshot, health: Health, epoch: int,
               mode: DialMode) -> SafetyDecision:
        pol, now = self._policy, s.now
        if self._started_at is None:
            self._started_at = now
        reasons: list[str] = []

        p_ucb = max(wilson_upper(s.answers_long, s.attempts_long, pol.confidence_z),
                    wilson_upper(s.answers_short, s.attempts_short, pol.confidence_z))
        # Recently dialed calls that are *already* answered bound today's answer rate from below
        # (answers only ever increase). If that floor is above our ceiling, the model is
        # provably wrong: use the floor, stop dialing ahead and shed.
        if s.cohort_dialed >= pol.min_cohort:
            floor = s.cohort_answered / s.cohort_dialed
            if wilson_lower(s.cohort_answered, s.cohort_dialed, pol.surge_z) > p_ucb:
                self._cooldown(now, f"answer surge: {s.cohort_answered} of {s.cohort_dialed} "
                                    f"recent calls already answered (>= {floor:.0%}), "
                                    f"model allowed <= {p_ucb:.0%}")
                self._shock_until = self._surge_until = now + pol.cooldown_s
            if floor > p_ucb:
                reasons.append(f"answer-rate bound raised {p_ucb:.2f} -> {floor:.2f}: "
                               f"already observed in recently dialed calls")
                p_ucb = floor
        self._watch(s)

        capacity = s.agents_available + (s.agents_wrap_up if pol.count_wrap_up else 0)
        in_flight = s.unbound_in_flight
        # After a surge no answer-rate estimate is trusted for calls already dialed: assume
        # every one may answer, i.e. hold in-flight agentless calls to progressive's invariant.
        p_shed = 1.0 if now < self._surge_until else p_ucb
        risk_now = binom_sf(capacity, in_flight, p_shed)

        def build(action, mode_name, bound=0, spec=0, ceiling=in_flight, shed=0):
            permit = DialPermit(s.campaign_id, epoch, bound, spec, ceiling, shed, now,
                                now + self._ttl, _seal=_SEAL)
            risk = binom_sf(capacity, in_flight + spec - shed, p_ucb)
            return SafetyDecision(action, proposal, permit, mode_name, p_ucb, capacity, risk,
                                  tuple(reasons), str(health), s)

        # 1. Provider down: nothing new, whatever the pacer thinks.
        if health is Health.OPEN:
            reasons.append("provider circuit OPEN: no new calls; calls in flight are left to "
                           "finish and are reconciled when the provider recovers")
            return build(SafetyAction.REJECT, "halt")

        # 2. Shed: after a shock (agents vanished, or answers surging past the model), calls
        #    already dialed have become too risky. Not in normal operation: capacity consumed by
        #    our own answers was priced in when the calls were dialed; shedding on it would churn.
        shed = 0
        if in_flight and now < self._shock_until and risk_now > pol.shed_tolerance:
            keep = max_calls_within_risk(capacity, p_shed, pol.shed_tolerance, in_flight)
            shed = max(0, in_flight - keep - s.unbound_cancelling)
            if shed:
                reasons.append(
                    f"SHED {shed}: with {capacity} agents free, P({in_flight} agentless calls "
                    f"produce more answers than agents) = {risk_now:.0%} > "
                    f"{pol.shed_tolerance:.0%}; cancelling not-yet-answered calls")

        # 3. Is predictive dialing allowed at all right now?
        fallback: list[str] = []
        if mode is DialMode.PREDICTIVE:
            if now - self._started_at < pol.warmup_s:
                fallback.append(f"new pacing leader warming up ({now - self._started_at:.0f}s "
                                f"< {pol.warmup_s:.0f}s)")
            if s.attempts_long < pol.min_samples:
                fallback.append(f"only {s.attempts_long} resolved attempts "
                                f"(< {pol.min_samples}): answer rate not yet trusted")
            if s.answered_live_window >= pol.min_abandon_samples and \
                    s.abandon_rate >= pol.abandon_limit:
                self._cooldown(now, f"abandon rate {s.abandon_rate:.1%} >= "
                                    f"{pol.abandon_limit:.0%} limit")
            if now < self._cooldown_until:
                fallback.append(f"{self._cooldown_reason} "
                                f"(cool-down {self._cooldown_until - now:.0f}s left)")
            if health in (Health.DEGRADED, Health.HALF_OPEN):
                fallback.append(f"provider {health}")
            if s.events_recent >= pol.min_event_samples and \
                    s.events_late / s.events_recent > pol.max_late_event_rate:
                fallback.append(
                    f"{s.events_late / s.events_recent:.1%} of provider webhooks arrive "
                    f">{pol.late_event_s:.0f}s late: an answered agentless call could not be "
                    f"bridged before the borrower gives up")
        per_tick = pol.max_new_calls_per_tick
        if health is Health.HALF_OPEN:
            per_tick = 1
            reasons.append("provider HALF_OPEN: one probe call per tick")

        if mode is DialMode.PREDICTIVE and not fallback:
            tol = pol.overflow_tolerance
            if s.answered_live_window >= pol.min_abandon_samples and \
                    s.abandon_rate >= pol.abandon_soft_limit:
                tol /= 2
                reasons.append(f"abandon rate {s.abandon_rate:.1%} above soft limit: "
                               f"tolerance halved to {tol:.1%}")
            hard_cap = math.floor(pol.max_dial_ratio * capacity)
            ceiling = max_calls_within_risk(capacity, p_ucb, tol, hard_cap)
            room = max(0, ceiling - in_flight)
            spec = min(proposal.speculative, room, per_tick, s.dialable_borrowers)
            reasons.append(
                f"capacity {capacity} = {s.agents_available} available + "
                f"{s.agents_wrap_up if pol.count_wrap_up else 0} in wrap-up (talking agents "
                f"never counted); answer-rate upper bound {p_ucb:.2f}")
            reasons.append(
                f"ceiling {ceiling} agentless calls keeps P(answers > {capacity}) <= {tol:.1%}; "
                f"{in_flight} in flight -> room for {room}")
            if spec == proposal.speculative:
                action = SafetyAction.APPROVE
            else:
                action = SafetyAction.REDUCE if spec > 0 else SafetyAction.REJECT
                reasons.append(f"proposal {proposal.speculative} cut to {spec}")
            return build(action, "predictive", spec=spec, ceiling=ceiling, shed=shed)

        # 4. Progressive (by configuration, or as the fallback). Agents that must stay free to
        #    absorb agentless calls still in flight are not handed out as reservations.
        reserve = agents_needed(in_flight - shed, p_shed, pol.overflow_tolerance)
        free_for_bound = max(0, s.agents_available - reserve)
        want = proposal.bound if mode is DialMode.PROGRESSIVE else free_for_bound
        bound = min(want, free_for_bound, per_tick, s.dialable_borrowers)
        if reserve:
            reasons.append(f"{reserve} available agents held back for {in_flight - shed} "
                           f"agentless calls still in flight")
        if mode is DialMode.PREDICTIVE:
            reasons.extend(f"fallback to progressive: {r}" for r in fallback)
            action = SafetyAction.FALLBACK_PROGRESSIVE
        elif bound == proposal.bound:
            action = SafetyAction.APPROVE
        else:
            action = SafetyAction.REDUCE if bound > 0 else SafetyAction.REJECT
            reasons.append(f"proposal {proposal.bound} cut to {bound}")
        return build(action, "progressive", bound=bound, ceiling=in_flight - shed, shed=shed)

    # ------------------------------------------------------------------ change detection
    def _watch(self, s: PacingSnapshot) -> None:
        pol, now = self._policy, s.now
        self._logged_in.append((now, s.agents_logged_in))
        while self._logged_in and self._logged_in[0][0] < now - pol.agent_drop_window_s:
            self._logged_in.popleft()
        peak = max(n for _, n in self._logged_in)
        if peak > 0 and (peak - s.agents_logged_in) / peak >= pol.agent_drop_fraction:
            self._cooldown(now, f"logged-in agents dropped {peak} -> {s.agents_logged_in} "
                                f"within {pol.agent_drop_window_s:.0f}s")
            self._shock_until = now + pol.cooldown_s
        if s.attempts_short >= pol.min_short_samples and s.attempts_long >= pol.min_samples:
            p_short = s.answers_short / s.attempts_short
            p_long = s.answers_long / s.attempts_long
            se = math.sqrt(max(p_long * (1 - p_long), 1e-6) / s.attempts_short)
            if abs(p_short - p_long) >= pol.answer_shift and \
                    abs(p_short - p_long) / se >= pol.answer_shift_z:
                self._cooldown(now, f"answer rate shifted {p_long:.0%} (5m) -> "
                                    f"{p_short:.0%} (60s): model no longer trusted")

    def _cooldown(self, now: float, reason: str) -> None:
        self._cooldown_until = now + self._policy.cooldown_s
        self._cooldown_reason = reason
