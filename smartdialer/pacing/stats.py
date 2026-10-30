"""Small, dependency-free statistics used for pacing and safety."""
from __future__ import annotations

import math


def wilson_upper(successes: int, n: int, z: float = 1.96) -> float:
    """Upper end of the Wilson score interval for a binomial proportion.

    With no data it is 1.0 (assume everyone answers), which makes every bound that uses it
    collapse to progressive behaviour. As evidence accumulates it tightens towards the observed
    rate. This is how the system protects itself when it does not know the answer rate yet.
    """
    if n <= 0:
        return 1.0
    p = successes / n
    z2 = z * z
    centre = p + z2 / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    return min(1.0, (centre + margin) / (1 + z2 / n))


def wilson_lower(successes: int, n: int, z: float = 1.96) -> float:
    """Lower end of the Wilson score interval (0.0 with no data)."""
    if n <= 0:
        return 0.0
    p = successes / n
    z2 = z * z
    centre = p + z2 / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    return max(0.0, (centre - margin) / (1 + z2 / n))


def binom_sf(k: int, n: int, p: float) -> float:
    """P(X > k) for X ~ Binomial(n, p), computed in log space so large n does not underflow."""
    if k < 0:
        return 1.0
    if k >= n or p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0
    log_p, log_q = math.log(p), math.log1p(-p)
    lg_n1 = math.lgamma(n + 1)

    def pmf(i: int) -> float:
        return math.exp(lg_n1 - math.lgamma(i + 1) - math.lgamma(n - i + 1)
                        + i * log_p + (n - i) * log_q)

    if k + 1 > n * p:                       # upper tail is the small side: sum it directly
        total = 0.0
        for i in range(k + 1, n + 1):
            term = pmf(i)
            total += term
            if term < 1e-17 * max(total, 1e-300):
                break
        return min(1.0, total)
    lower = sum(pmf(i) for i in range(0, k + 1))
    return max(0.0, 1.0 - lower)


def max_calls_within_risk(capacity: int, p: float, tolerance: float, hard_cap: int) -> int:
    """Largest n <= hard_cap such that P(Binomial(n, p) > capacity) <= tolerance.

    "If n agentless calls are in flight and each is answered with probability p, the chance that
    more of them answer than there are agents to take them is at most `tolerance`."
    With zero capacity the answer is zero regardless of p.
    """
    if capacity <= 0 or hard_cap <= 0:
        return 0
    lo = min(capacity, hard_cap)            # n <= capacity can never overflow
    hi = hard_cap
    if binom_sf(capacity, hi, p) <= tolerance:
        return hi
    while hi - lo > 1:                      # invariant: lo is safe, hi is not
        mid = (lo + hi) // 2
        if binom_sf(capacity, mid, p) <= tolerance:
            lo = mid
        else:
            hi = mid
    return lo


def agents_needed(in_flight: int, p: float, tolerance: float) -> int:
    """Smallest capacity C such that P(Binomial(in_flight, p) > C) <= tolerance: how many agents
    must be kept free to absorb the agentless calls already in flight."""
    if in_flight <= 0:
        return 0
    lo, hi = 0, in_flight                   # hi always satisfies (P(X > n) = 0)
    while lo < hi:
        mid = (lo + hi) // 2
        if binom_sf(mid, in_flight, p) <= tolerance:
            hi = mid
        else:
            lo = mid + 1
    return lo
