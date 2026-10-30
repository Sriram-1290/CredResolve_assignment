"""Process entry point for the 'N workers grab the same agents at the same instant' race.

Lives in an importable module because Windows/macOS start child processes with 'spawn'.
"""
from __future__ import annotations

import time

from smartdialer import repo
from smartdialer.db import Store


def race_reserve(db_path: str, campaign_id: str, worker_id: str, barrier, results,
                 attempts: int) -> None:
    store = Store(db_path)
    won = []
    barrier.wait()                                      # everyone starts at the same instant
    for i in range(attempts):
        with store.tx() as c:
            agent = repo.reserve_available_agent(c, campaign_id, f"{worker_id}-{i}", time.time(), 0)
        if agent is not None:
            won.append(agent)
    results.put((worker_id, won))
