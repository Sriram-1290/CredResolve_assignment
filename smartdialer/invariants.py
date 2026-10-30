"""Cross-table invariants. Checked by tests, the simulator after every run, and the load test.

They must hold between any two transactions, because every multi-row change is one transaction.
"""
from __future__ import annotations

from .db import Store
from .states import LIVE_CALL_STATES, sql_in

_LIVE = sql_in(LIVE_CALL_STATES)

CHECKS: dict[str, str] = {
    "agent has more than one live call": f"""
        SELECT agent_id FROM calls WHERE agent_id IS NOT NULL AND state IN {_LIVE}
        GROUP BY agent_id HAVING COUNT(*) > 1""",
    "borrower has more than one live call": f"""
        SELECT borrower_id FROM calls WHERE state IN {_LIVE}
        GROUP BY borrower_id HAVING COUNT(*) > 1""",
    "busy agent not attached to a matching live call": f"""
        SELECT a.id FROM agents a LEFT JOIN calls c ON c.id = a.call_id
        WHERE a.state IN ('RESERVED','DIALING','CONNECTED')
          AND (c.id IS NULL OR c.state NOT IN {_LIVE} OR c.agent_id IS NOT a.id)""",
    "live call's agent is not busy with it": f"""
        SELECT c.id FROM calls c JOIN agents a ON a.id = c.agent_id
        WHERE c.state IN {_LIVE}
          AND (a.call_id IS NOT c.id
               OR (c.state = 'CONNECTED' AND a.state != 'CONNECTED')
               OR (c.state != 'CONNECTED' AND a.state NOT IN ('RESERVED','DIALING')))""",
    "borrower IN_PROGRESS without its live call": f"""
        SELECT b.id FROM borrowers b LEFT JOIN calls c ON c.id = b.call_id
        WHERE b.status = 'IN_PROGRESS' AND (c.id IS NULL OR c.state NOT IN {_LIVE})""",
    "live call whose borrower is not IN_PROGRESS with it": f"""
        SELECT c.id FROM calls c JOIN borrowers b ON b.id = c.borrower_id
        WHERE c.state IN {_LIVE} AND (b.status != 'IN_PROGRESS' OR b.call_id IS NOT c.id)""",
}


def check_invariants(store: Store) -> list[str]:
    problems = []
    for name, sql in CHECKS.items():
        rows = store.all(sql)
        if rows:
            problems.append(f"{name}: {[tuple(r)[0] for r in rows[:5]]} ({len(rows)} rows)")
    return problems
