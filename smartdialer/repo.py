"""Transactional building blocks shared by the allocator, event processor and reconciler.

Every function here takes an open connection `c` that is inside `Store.tx()`. None of them
commit; the caller decides the transaction boundary. All state writes are compare-and-set.
"""
from __future__ import annotations

import random
import sqlite3
from typing import Iterable

from .config import CampaignConfig
from .states import (
    AgentState, BorrowerStatus, CallState, LIVE_CALL_STATES, TERMINAL_CALL_STATES,
    check_agent_transition, check_call_transition, sql_in,
)

_KEEP = object()

_CALL_FIELDS = frozenset({
    "agent_id", "provider_call_id", "owner", "lease_until", "initiated_at", "ringing_at",
    "answered_at", "connected_at", "ended_at", "end_reason", "anomaly", "cancel_requested_at",
})

# Outcomes that count against the borrower's attempt budget.
BORROWER_NO_CONTACT = frozenset({"NO_ANSWER", "BUSY"})


# --------------------------------------------------------------------------- agents

def move_agent(
    c: sqlite3.Connection, agent_id: str, to: AgentState, *, expect: Iterable[AgentState],
    now: float, call_id: object = _KEEP, only_if_call: str | None = None,
) -> bool:
    """CAS an agent from one of `expect` to `to`. Returns False if someone else got there first.

    `only_if_call` additionally requires the agent to still be attached to that call, so a late
    event for an old call can never move an agent who has since moved on to a new one.
    """
    expect = frozenset(expect)
    for src in expect:
        check_agent_transition(src, to)
    sets, params = ["state=?", "state_since=?", "version=version+1"], [str(to), now]
    if call_id is not _KEEP:
        sets.append("call_id=?")
        params.append(call_id)
    where = f"id=? AND state IN {sql_in(expect)}"
    params.append(agent_id)
    if only_if_call is not None:
        where += " AND call_id=?"
        params.append(only_if_call)
    return c.execute(f"UPDATE agents SET {', '.join(sets)} WHERE {where}", params).rowcount == 1


def reserve_available_agent(
    c: sqlite3.Connection, campaign_id: str, call_id: str, now: float, fresh_after: float,
) -> str | None:
    """AVAILABLE -> RESERVED for the longest-idle agent with a fresh heartbeat.

    The inner SELECT picks a candidate; the outer `AND state='AVAILABLE'` is the CAS. If two
    workers pick the same candidate, exactly one UPDATE matches; the other gets no row back.
    """
    check_agent_transition(AgentState.AVAILABLE, AgentState.RESERVED)
    row = c.execute(
        """UPDATE agents SET state='RESERVED', call_id=?, state_since=?, version=version+1
           WHERE id = (SELECT id FROM agents
                       WHERE campaign_id=? AND state='AVAILABLE' AND heartbeat_at >= ?
                       ORDER BY state_since LIMIT 1)
             AND state='AVAILABLE'
           RETURNING id""",
        (call_id, now, campaign_id, fresh_after),
    ).fetchone()
    return None if row is None else row[0]


def claim_agent_for_answer(
    c: sqlite3.Connection, campaign_id: str, call_id: str, now: float, fresh_after: float,
) -> str | None:
    """A borrower answered a call that has no agent: take the longest-idle AVAILABLE agent, or
    failing that pre-empt the agent who has been in WRAP_UP longest. Abandoning is the last resort."""
    for src in (AgentState.AVAILABLE, AgentState.WRAP_UP):
        check_agent_transition(src, AgentState.CONNECTED)
        row = c.execute(
            """UPDATE agents SET state='CONNECTED', call_id=?, state_since=?, version=version+1
               WHERE id = (SELECT id FROM agents
                           WHERE campaign_id=? AND state=? AND heartbeat_at >= ?
                           ORDER BY state_since LIMIT 1)
                 AND state=?
               RETURNING id""",
            (call_id, now, campaign_id, str(src), fresh_after, str(src)),
        ).fetchone()
        if row is not None:
            return row[0]
    return None


# --------------------------------------------------------------------------- calls

def get_call(c: sqlite3.Connection, call_id: str) -> sqlite3.Row | None:
    return c.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()


def move_call(
    c: sqlite3.Connection, call_id: str, to: CallState, *, expect: Iterable[CallState],
    now: float, via: CallState | None = None, **fields: object,
) -> bool:
    """CAS a call from one of `expect` to `to`, optionally validating the path src -> via -> to."""
    expect = frozenset(expect)
    for src in expect:
        if via is None:
            check_call_transition(src, to)
        else:
            check_call_transition(src, via)
            check_call_transition(via, to)
    unknown = set(fields) - _CALL_FIELDS
    if unknown:
        raise ValueError(f"unknown call fields {unknown}")
    sets = ["state=?", "updated_at=?", "version=version+1"] + [f"{k}=?" for k in fields]
    params = [str(to), now, *fields.values(), call_id]
    sql = f"UPDATE calls SET {', '.join(sets)} WHERE id=? AND state IN {sql_in(expect)}"
    return c.execute(sql, params).rowcount == 1


def confirm_start(c: sqlite3.Connection, call_id: str, now: float, provider_call_id: str | None,
                  **fields: object) -> bool:
    """The provider has the call: RESERVED/QUEUED -> INITIATED, drop the start lease, and move its
    agent (if any) RESERVED -> DIALING. Returns False if the call was no longer awaiting its start."""
    if not move_call(c, call_id, CallState.INITIATED, expect={CallState.RESERVED, CallState.QUEUED},
                     now=now, provider_call_id=provider_call_id, initiated_at=now, owner=None,
                     lease_until=None, **fields):
        return False
    agent_id = get_call(c, call_id)["agent_id"]
    if agent_id is not None:
        move_agent(c, agent_id, AgentState.DIALING, expect={AgentState.RESERVED}, now=now,
                   only_if_call=call_id)
    return True


def close_call(
    c: sqlite3.Connection, call_id: str, to: CallState, reason: str, now: float, *,
    expect: Iterable[CallState] = LIVE_CALL_STATES, **fields: object,
) -> bool:
    """Move a live call to a terminal state and, in the same transaction, release its agent and
    settle its borrower. Returns False (and changes nothing) if the call was no longer in `expect`."""
    assert to in TERMINAL_CALL_STATES
    call = get_call(c, call_id)
    if call is None or CallState(call["state"]) not in frozenset(expect):
        return False
    src = CallState(call["state"])
    if not move_call(c, call_id, to, expect={src}, now=now, ended_at=now, end_reason=reason,
                     owner=None, lease_until=None, **fields):
        return False
    call = get_call(c, call_id)
    _release_agent(c, call, now)
    _settle_borrower(c, call, to, reason, now)
    return True


def _release_agent(c: sqlite3.Connection, call: sqlite3.Row, now: float) -> None:
    agent_id = call["agent_id"]
    if not agent_id:
        return
    if call["connected_at"] is not None:
        move_agent(c, agent_id, AgentState.WRAP_UP, expect={AgentState.CONNECTED}, now=now,
                   only_if_call=call["id"])
    else:
        move_agent(c, agent_id, AgentState.AVAILABLE,
                   expect={AgentState.RESERVED, AgentState.DIALING}, now=now,
                   call_id=None, only_if_call=call["id"])


def _settle_borrower(c: sqlite3.Connection, call: sqlite3.Row, to: CallState, reason: str,
                     now: float) -> None:
    rules = c.execute("SELECT * FROM campaigns WHERE id=?", (call["campaign_id"],)).fetchone()
    b = c.execute("SELECT * FROM borrowers WHERE id=?", (call["borrower_id"],)).fetchone()
    if b is None or b["call_id"] != call["id"]:
        return
    attempts, failures = b["attempts"], b["provider_failures"]
    if to is CallState.COMPLETED:
        # Also when COMPLETED overtook a lost ANSWERED: an agent-bound call is bridged by the
        # provider on answer, so the conversation happened even though we never saw it start.
        status, next_at = BorrowerStatus.DONE, b["next_attempt_at"]
    elif to is CallState.ABANDONED or (to is CallState.FAILED and reason in BORROWER_NO_CONTACT):
        attempts += 1
        status = BorrowerStatus.EXHAUSTED if attempts >= rules["max_attempts"] else BorrowerStatus.PENDING
        next_at = now + rules["retry_after_s"]
    elif to is CallState.CANCELLED:
        # We withdrew the call ourselves; that is not the borrower's attempt.
        status, next_at = BorrowerStatus.PENDING, now + rules["provider_retry_after_s"]
    else:
        failures += 1
        status = (BorrowerStatus.EXHAUSTED if failures >= rules["max_provider_failures"]
                  else BorrowerStatus.PENDING)
        next_at = now + rules["provider_retry_after_s"]
    c.execute(
        """UPDATE borrowers SET status=?, attempts=?, provider_failures=?, next_attempt_at=?,
                  call_id=NULL, version=version+1
           WHERE id=? AND call_id=? AND status='IN_PROGRESS'""",
        (str(status), attempts, failures, next_at, b["id"], call["id"]),
    )


def claim_borrower(c: sqlite3.Connection, campaign_id: str, call_id: str,
                   now: float) -> sqlite3.Row | None:
    """PENDING -> IN_PROGRESS for the highest-priority borrower that is due. Same CAS shape as
    agent reservation, so two workers can never dial the same borrower."""
    return c.execute(
        """UPDATE borrowers SET status='IN_PROGRESS', call_id=?, version=version+1
           WHERE id = (SELECT id FROM borrowers
                       WHERE campaign_id=? AND status='PENDING' AND next_attempt_at <= ?
                       ORDER BY priority DESC, next_attempt_at LIMIT 1)
             AND status='PENDING'
           RETURNING id, phone""",
        (call_id, campaign_id, now),
    ).fetchone()


def enqueue_command(c: sqlite3.Connection, call_id: str, kind: str, now: float,
                    arg: str | None = None) -> None:
    """Outbox write. UNIQUE(call_id, kind) makes enqueueing idempotent; re-enqueueing a command
    that gave up (FAILED, e.g. during an outage) re-arms it instead of silently doing nothing."""
    c.execute(
        """INSERT INTO commands (call_id, kind, arg, next_attempt_at) VALUES (?, ?, ?, ?)
           ON CONFLICT (call_id, kind) DO UPDATE
               SET status='PENDING', attempts=0, arg=excluded.arg,
                   next_attempt_at=excluded.next_attempt_at, last_error=NULL
               WHERE commands.status='FAILED'""",
        (call_id, kind, arg, now),
    )


# --------------------------------------------------------------------------- campaigns

def acquire_leadership(c: sqlite3.Connection, campaign_id: str, worker_id: str, now: float,
                       lease_s: float, takeover_after_s: float = 0.0) -> sqlite3.Row | None:
    """Take or renew the per-campaign pacing lease. The epoch is a fencing token: it changes
    whenever leadership changes hands, and the allocator refuses permits from an old epoch.
    `takeover_after_s` > 0 only takes someone else's campaign once their lease has been expired
    that long (used for campaigns outside this worker's own partition)."""
    return c.execute(
        """UPDATE campaigns
           SET epoch = CASE WHEN leader = ? AND leader_until >= ? THEN epoch ELSE epoch + 1 END,
               leader = ?, leader_until = ?
           WHERE id = ? AND active = 1
             AND (leader IS NULL OR leader = ? OR leader_until < ?)
           RETURNING epoch, mode""",
        (worker_id, now, worker_id, now + lease_s, campaign_id, worker_id, now - takeover_after_s),
    ).fetchone()


# --------------------------------------------------------------------------- setup helpers

def create_campaign(store, cfg: CampaignConfig) -> None:
    with store.tx() as c:
        c.execute(
            """INSERT INTO campaigns (id, mode, max_attempts, retry_after_s,
                                      provider_retry_after_s, max_provider_failures)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (cfg.id, str(cfg.mode), cfg.max_attempts, cfg.retry_after_s,
             cfg.provider_retry_after_s, cfg.max_provider_failures),
        )


def set_campaign_mode(store, campaign_id: str, mode) -> None:
    with store.tx() as c:
        c.execute("UPDATE campaigns SET mode=? WHERE id=?", (str(mode), campaign_id))


def add_agents(store, campaign_id: str, agent_ids: Iterable[str], now: float,
               state: AgentState = AgentState.AVAILABLE) -> None:
    with store.tx() as c:
        c.executemany(
            "INSERT INTO agents (id, campaign_id, state, state_since, heartbeat_at) VALUES (?,?,?,?,?)",
            [(a, campaign_id, str(state), now, now) for a in agent_ids],
        )


def add_borrowers(store, campaign_id: str, count: int, rng: random.Random | None = None,
                  prefix: str = "b") -> None:
    rng = rng or random.Random(0)
    with store.tx() as c:
        c.executemany(
            "INSERT INTO borrowers (id, campaign_id, phone, priority) VALUES (?,?,?,?)",
            [(f"{prefix}{i}", campaign_id, f"+9190000{i:05d}", round(rng.random(), 4))
             for i in range(count)],
        )


__all__ = [
    "move_agent", "reserve_available_agent", "claim_agent_for_answer", "get_call", "move_call",
    "confirm_start", "close_call", "claim_borrower", "enqueue_command", "acquire_leadership", "create_campaign",
    "set_campaign_mode", "add_agents", "add_borrowers", "BORROWER_NO_CONTACT", "LIVE_CALL_STATES",
]
