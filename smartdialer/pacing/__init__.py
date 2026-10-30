"""Pacing engines. This package must not import providers, the allocator or the database:
a pacer can only *propose* (enforced by tests/test_safety.py::test_pacing_cannot_reach_providers)."""
from .model import PacingProposal, PacingSnapshot
from .predictive import PredictivePacer, PredictiveParams
from .progressive import ProgressivePacer

__all__ = ["PacingProposal", "PacingSnapshot", "PredictivePacer", "PredictiveParams",
           "ProgressivePacer"]
