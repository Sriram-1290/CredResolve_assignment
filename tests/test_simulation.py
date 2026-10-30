"""End-to-end: the simulator drives real workers against mock providers."""
import pytest

from sim import failures
from sim.engine import Simulation
from sim.scenarios import scenario
from smartdialer.config import DialMode


@pytest.fixture(scope="module")
def runs():
    return {mode: Simulation(scenario("A", mode, "A", minutes=10)).run()
            for mode in (DialMode.PROGRESSIVE, DialMode.PREDICTIVE)}


def test_progressive_never_abandons(runs):
    r = runs[DialMode.PROGRESSIVE]
    assert r.abandoned == 0
    assert r.invariant_violations == []


def test_predictive_beats_progressive_within_the_abandon_limit(runs):
    pro, pre = runs[DialMode.PROGRESSIVE], runs[DialMode.PREDICTIVE]
    assert pre.invariant_violations == []
    assert pre.utilization > pro.utilization + 0.1
    assert pre.abandon_rate <= 0.03


def test_provider_b_with_late_webhooks_keeps_predictive_off():
    sim = Simulation(scenario("B", DialMode.PREDICTIVE, "B", minutes=10))
    r = sim.run()
    assert r.invariant_violations == []
    assert r.abandon_rate <= 0.03
    # The lag gate is evidence-based: it needs a few minutes of webhooks to see that B's late
    # share is above the 0.5% gate. From then on, predictive stays off.
    late = [d.decision.action for d in sim.decisions if d.decision.permit.issued_at > 300]
    assert set(late) == {"FALLBACK_PROGRESSIVE"}


@pytest.mark.parametrize("name", list(failures.DEMOS))
def test_failure_demo(name):
    assert failures.DEMOS[name]()
