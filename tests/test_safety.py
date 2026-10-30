import ast
import dataclasses
from pathlib import Path

import pytest
from helpers import World

from smartdialer.clock import SimClock
from smartdialer.config import DialMode
from smartdialer.pacing.model import PacingProposal, PacingSnapshot
from smartdialer.pacing.stats import binom_sf
from smartdialer.providers.health import Health
from smartdialer.safety import DialPermit, SafetyAction, SafetyController, SafetyPolicy

PRED = DialMode.PREDICTIVE


def snap(now=100.0, **kw) -> PacingSnapshot:
    base = dict(campaign_id="c1", now=now, agents_logged_in=50, agents_available=10,
                agents_reserved=0, agents_connected=40, agents_wrap_up=0, agents_paused=0,
                bound_in_flight=0, unbound_in_flight=0, unbound_cancelling=0,
                dialable_borrowers=1000, attempts_long=500, answers_long=100,
                attempts_short=60, answers_short=12, mean_dial_to_answer_s=12.0,
                mean_talk_s=120.0, answered_live_window=300, abandoned_window=0)
    base.update(kw)
    return PacingSnapshot(**base)


def ask(n: int) -> PacingProposal:
    return PacingProposal("predictive", bound=0, speculative=n, explanation=("test",))


def warmed(policy: SafetyPolicy | None = None) -> SafetyController:
    ctl = SafetyController(policy or SafetyPolicy(), SimClock())
    ctl.decide(ask(0), snap(now=0.0), Health.HEALTHY, 1, PRED)     # start the warm-up clock
    return ctl


def test_predictive_is_capped_by_the_overflow_bound_not_by_the_pacer():
    ctl = warmed()
    d = ctl.decide(ask(1000), snap(), Health.HEALTHY, 1, PRED)
    assert d.action is SafetyAction.REDUCE
    assert d.mode == "predictive"
    spec = d.permit.speculative
    assert 10 < spec < 1000                                  # dials ahead, but bounded
    assert binom_sf(10, spec, d.answer_rate_ucb) <= ctl.policy.overflow_tolerance
    assert binom_sf(10, spec + 1, d.answer_rate_ucb) > ctl.policy.overflow_tolerance


def test_small_request_inside_the_bound_is_approved_as_is():
    d = warmed().decide(ask(5), snap(), Health.HEALTHY, 1, PRED)
    assert (d.action, d.permit.speculative) == (SafetyAction.APPROVE, 5)


def test_calls_already_in_flight_use_up_the_room():
    ctl = warmed()
    ceiling = ctl.decide(ask(1000), snap(), Health.HEALTHY, 1, PRED).permit.unbound_ceiling
    d = ctl.decide(ask(1000), snap(unbound_in_flight=ceiling), Health.HEALTHY, 1, PRED)
    assert (d.action, d.permit.speculative) == (SafetyAction.REJECT, 0)


def test_without_data_predictive_collapses_to_progressive():
    ctl = SafetyController(SafetyPolicy(), SimClock())
    d = ctl.decide(ask(50), snap(attempts_long=0, answers_long=0, attempts_short=0,
                                 answers_short=0), Health.HEALTHY, 1, PRED)
    assert d.action is SafetyAction.FALLBACK_PROGRESSIVE
    assert (d.permit.bound, d.permit.speculative) == (10, 0)
    assert d.answer_rate_ucb == 1.0
    assert any("not yet trusted" in r for r in d.reasons)


def test_realised_abandon_rate_over_the_limit_forces_progressive():
    d = warmed().decide(ask(50), snap(answered_live_window=200, abandoned_window=7),
                        Health.HEALTHY, 1, PRED)
    assert d.action is SafetyAction.FALLBACK_PROGRESSIVE
    assert any("abandon rate" in r for r in d.reasons)


def test_provider_open_rejects_everything():
    d = warmed().decide(ask(50), snap(), Health.OPEN, 1, PRED)
    assert d.action is SafetyAction.REJECT
    assert (d.permit.bound, d.permit.speculative) == (0, 0)
    d = warmed().decide(PacingProposal("progressive", 10, 0, ()), snap(), Health.OPEN, 1,
                        DialMode.PROGRESSIVE)
    assert (d.action, d.permit.bound) == (SafetyAction.REJECT, 0)


def test_provider_half_open_allows_a_single_probe_call():
    d = warmed().decide(ask(50), snap(), Health.HALF_OPEN, 1, PRED)
    assert d.action is SafetyAction.FALLBACK_PROGRESSIVE
    assert d.permit.bound + d.permit.speculative == 1


def test_late_webhooks_disable_dialing_ahead():
    d = warmed().decide(ask(50), snap(events_recent=1000, events_late=30), Health.HEALTHY, 1, PRED)
    assert d.action is SafetyAction.FALLBACK_PROGRESSIVE
    assert any("late" in r for r in d.reasons)


def test_forty_of_hundred_agents_vanish_dialer_falls_back_and_sheds():
    ctl = SafetyController(SafetyPolicy(), SimClock())
    s0 = dict(agents_logged_in=100, agents_available=20, agents_connected=80)
    ctl.decide(ask(0), snap(now=0.0, **s0), Health.HEALTHY, 1, PRED)
    ctl.decide(ask(0), snap(now=20.0, **s0), Health.HEALTHY, 1, PRED)
    # 3 seconds later: 40 agents gone, including most of the free ones, 40 agentless calls ringing.
    d = ctl.decide(ask(30), snap(now=23.0, agents_logged_in=60, agents_available=4,
                                 agents_connected=56, unbound_in_flight=40),
                   Health.HEALTHY, 1, PRED)
    assert d.action is SafetyAction.FALLBACK_PROGRESSIVE
    assert d.permit.speculative == 0 and d.permit.shed > 0
    assert d.permit.bound == 0                       # the 4 free agents are kept for answers
    assert any("dropped 100 -> 60" in r for r in d.reasons)


def test_answer_rate_surge_is_detected_and_tightens_the_bound():
    ctl = warmed()
    calm = ctl.decide(ask(1000), snap(), Health.HEALTHY, 1, PRED)
    surge = ctl.decide(ask(1000), snap(attempts_short=60, answers_short=42), Health.HEALTHY, 1, PRED)
    assert surge.action is SafetyAction.FALLBACK_PROGRESSIVE
    assert surge.answer_rate_ucb > calm.answer_rate_ucb
    assert any("shifted" in r for r in surge.reasons)


def test_a_permit_cannot_be_forged():
    with pytest.raises(PermissionError):
        DialPermit("c1", 1, bound=0, speculative=999, unbound_ceiling=999, shed=0,
                   issued_at=0, expires_at=1e18)


def test_allocator_refuses_an_expired_permit():
    w = World(agents=3)
    p = w.permit(bound=3)
    w.advance(10)
    assert w.worker.allocator.execute(p).stale_permit
    assert w.calls() == []


def test_safety_policy_is_immutable():
    ctl = SafetyController(SafetyPolicy(), SimClock())
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctl.policy.overflow_tolerance = 1.0
    with pytest.raises(AttributeError):
        ctl.policy = SafetyPolicy(overflow_tolerance=1.0)


def test_pacing_cannot_reach_providers_allocator_or_database():
    """The pacing package may import only its own modules and the standard library."""
    pkg = Path(__file__).resolve().parents[1] / "smartdialer" / "pacing"
    allowed_relative = {"model", "stats", "predictive", "progressive"}
    for path in pkg.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    assert node.level == 1 and (node.module or "") in allowed_relative, \
                        f"{path.name} imports .{node.module}"
                else:
                    assert not (node.module or "").startswith("smartdialer"), path.name
            elif isinstance(node, ast.Import):
                assert not any(a.name.startswith("smartdialer") for a in node.names), path.name
