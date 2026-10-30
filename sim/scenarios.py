"""Scenarios A-D from the assignment, plus the policy sensitivity sweep."""
from __future__ import annotations

from dataclasses import replace

from smartdialer.config import DialMode
from smartdialer.providers.mock import PROVIDER_A, PROVIDER_B, MockProfile
from smartdialer.safety import SafetyPolicy

from .engine import ScenarioSpec


def step(*points: tuple[float, float]):
    """Piecewise-constant schedule: step((0, 0.3), (900, 0.7)) is 0.3 until t=900, then 0.7."""
    def at(t: float) -> float:
        value = points[0][1]
        for start, v in points:
            if t >= start:
                value = v
        return value
    return at


# answer rate and talk time change every 15 / 20 minutes, including a sudden 70% -> 10% collapse
D_ANSWER = step((0, 0.30), (900, 0.70), (1800, 0.10), (2700, 0.45))
D_TALK = step((0, 90.0), (1200, 180.0), (2400, 60.0))

BASE = {
    "A": dict(answer_rate=0.20, talk_mean_s=120.0, description="20% answer, 120s talk"),
    "B": dict(answer_rate=0.50, talk_mean_s=90.0, description="50% answer, 90s talk"),
    "C": dict(answer_rate=0.70, talk_mean_s=180.0, description="70% answer, 180s talk"),
    "D": dict(answer_rate=D_ANSWER, talk_mean_s=D_TALK,
              description="answer 30%->70%->10%->45% every 15m; talk 90->180->60s every 20m"),
}

PROVIDERS: dict[str, MockProfile] = {"A": PROVIDER_A, "B": PROVIDER_B}


def scenario(name: str, mode: DialMode, provider: str = "A", minutes: float = 60,
             agents: int = 50, **overrides) -> ScenarioSpec:
    b = BASE[name]
    return ScenarioSpec(
        name=name, mode=mode, answer_rate=b["answer_rate"], talk_mean_s=b["talk_mean_s"],
        description=b["description"], agents=agents, duration_s=minutes * 60,
        providers=(PROVIDERS[provider],), **overrides)


def matrix(minutes: float = 60) -> list[ScenarioSpec]:
    return [scenario(name, mode, provider, minutes)
            for name in "ABCD"
            for provider in "AB"
            for mode in (DialMode.PROGRESSIVE, DialMode.PREDICTIVE)]


def sensitivity(minutes: float = 60) -> list[tuple[str, ScenarioSpec]]:
    """How the overflow tolerance trades utilisation against abandonment (scenario A)."""
    out = []
    for tol in (0.005, 0.02, 0.05, 0.10, 0.20):
        policy = SafetyPolicy(overflow_tolerance=tol, abandon_limit=1.0, abandon_soft_limit=1.0)
        out.append((f"tolerance {tol:.1%}",
                    replace(scenario("A", DialMode.PREDICTIVE, "A", minutes), policy=policy)))
    return out
