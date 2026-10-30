import math
import random

from test_safety import snap

from smartdialer.pacing import PredictivePacer, ProgressivePacer
from smartdialer.pacing.stats import agents_needed, binom_sf, max_calls_within_risk, wilson_upper


def test_binom_sf_matches_brute_force():
    for n in (0, 1, 7, 30):
        for p in (0.05, 0.3, 0.9):
            for k in range(-1, n + 2):
                exact = sum(math.comb(n, i) * p**i * (1 - p)**(n - i) for i in range(k + 1, n + 1))
                assert abs(binom_sf(k, n, p) - exact) < 1e-9


def test_binom_sf_large_n_does_not_underflow():
    assert 0.0 < binom_sf(450, 2000, 0.2) < 0.01


def test_wilson_upper_bound():
    assert wilson_upper(0, 0) == 1.0
    assert 0.2 < wilson_upper(20, 100) < 0.3
    assert wilson_upper(2000, 10000) - 0.2 < 0.01


def test_max_calls_within_risk_is_the_largest_safe_n():
    for cap, p, tol in ((10, 0.2, 0.02), (3, 0.5, 0.05), (50, 0.7, 0.01)):
        n = max_calls_within_risk(cap, p, tol, hard_cap=10_000)
        assert binom_sf(cap, n, p) <= tol < binom_sf(cap, n + 1, p)
        assert n >= cap
    assert max_calls_within_risk(0, 0.01, 0.5, 100) == 0
    assert max_calls_within_risk(10, 0.2, 0.02, hard_cap=12) == 12


def test_agents_needed_is_the_smallest_safe_capacity():
    for n, p in ((40, 0.2), (10, 0.9)):
        c = agents_needed(n, p, 0.02)
        assert binom_sf(c, n, p) <= 0.02 < binom_sf(c - 1, n, p)
    assert agents_needed(0, 0.5, 0.02) == 0


def test_progressive_pacer_one_call_per_available_agent():
    prop = ProgressivePacer().propose(snap(agents_available=7))
    assert (prop.bound, prop.speculative) == (7, 0)


def test_predictive_pacer_worked_example_17_calls():
    """The example used in docs/PACING.md: why 17 calls and not 10."""
    s = snap(agents_available=3, agents_wrap_up=2, agents_connected=45, unbound_in_flight=5,
             attempts_long=400, answers_long=120, attempts_short=50, answers_short=14,
             mean_dial_to_answer_s=15.0, mean_talk_s=150.0)
    prop = PredictivePacer().propose(s)
    assert prop.speculative == 17, prop.explanation
    text = "\n".join(prop.explanation)
    assert "p=0.29" in text and "-> 17 calls" in text


def test_predictive_pacer_reacts_to_answer_rate():
    low = PredictivePacer().propose(snap(attempts_short=100, answers_short=10,
                                         attempts_long=500, answers_long=50))
    high = PredictivePacer().propose(snap(attempts_short=100, answers_short=70,
                                          attempts_long=500, answers_long=350))
    assert low.speculative > 3 * high.speculative


def test_predictive_pacer_counts_calls_already_in_flight():
    base = PredictivePacer().propose(snap())
    busy = PredictivePacer().propose(snap(unbound_in_flight=20))
    assert busy.speculative < base.speculative


def test_estimates_are_deterministic_for_the_same_snapshot():
    s = snap(agents_available=random.Random(1).randint(1, 20))
    assert PredictivePacer().propose(s) == PredictivePacer().propose(s)
