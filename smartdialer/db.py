"""SQLite store: the single source of truth shared by all workers.

Concurrency model
-----------------
* Every write happens inside `Store.tx()`, which opens `BEGIN IMMEDIATE`: the transaction takes
  the database write lock up front, so a read-then-write inside it cannot interleave with another
  writer (other processes block on `busy_timeout`, then proceed and see the committed result).
* On top of that every state change is written as a compare-and-set
  (`UPDATE ... WHERE id=? AND state=?`) and the code checks `rowcount`. That makes the logic
  correct even on a database where two transactions *can* run concurrently (e.g. PostgreSQL at
  READ COMMITTED): the second UPDATE re-evaluates its WHERE clause after the first commits and
  matches zero rows.
* Partial unique indexes make "one live call per agent" and "one live call per borrower" a
  property of the data, not of the code. A bug that tried to double-book would fail with
  IntegrityError instead of dialing.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .states import LIVE_CALL_STATES, sql_in

_LIVE = sql_in(LIVE_CALL_STATES)

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS campaigns (
    id                    TEXT PRIMARY KEY,
    mode                  TEXT NOT NULL,
    active                INTEGER NOT NULL DEFAULT 1,
    max_attempts          INTEGER NOT NULL,
    retry_after_s         REAL NOT NULL,
    provider_retry_after_s REAL NOT NULL,
    max_provider_failures INTEGER NOT NULL,
    leader                TEXT,
    leader_until          REAL NOT NULL DEFAULT 0,
    epoch                 INTEGER NOT NULL DEFAULT 0   -- fencing token: +1 on every leader change
);

CREATE TABLE IF NOT EXISTS agents (
    id            TEXT PRIMARY KEY,
    campaign_id   TEXT NOT NULL,
    state         TEXT NOT NULL,
    call_id       TEXT,
    state_since   REAL NOT NULL,
    heartbeat_at  REAL NOT NULL,
    version       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS agents_pick ON agents (campaign_id, state, state_since);

CREATE TABLE IF NOT EXISTS borrowers (
    id                TEXT PRIMARY KEY,
    campaign_id       TEXT NOT NULL,
    phone             TEXT NOT NULL,
    priority          REAL NOT NULL DEFAULT 0,
    status            TEXT NOT NULL DEFAULT 'PENDING',
    attempts          INTEGER NOT NULL DEFAULT 0,
    provider_failures INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   REAL NOT NULL DEFAULT 0,
    call_id           TEXT,
    version           INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS borrowers_pick
    ON borrowers (campaign_id, status, priority DESC, next_attempt_at);

CREATE TABLE IF NOT EXISTS calls (
    id               TEXT PRIMARY KEY,           -- also the idempotency key sent to the provider
    campaign_id      TEXT NOT NULL,
    borrower_id      TEXT NOT NULL,
    agent_id         TEXT,
    kind             TEXT NOT NULL,
    state            TEXT NOT NULL,
    provider         TEXT NOT NULL,
    provider_call_id TEXT,
    owner            TEXT,                       -- worker that must confirm start_call
    lease_until      REAL,                       -- ... before this time, else the reconciler takes over
    next_check_at    REAL,                       -- reconciler: don't ask the provider again before this
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    initiated_at     REAL,
    ringing_at       REAL,
    answered_at      REAL,
    connected_at     REAL,
    ended_at         REAL,
    cancel_requested_at REAL,                    -- we asked the provider to withdraw it; until the
                                                 -- provider confirms, it can still be answered
    end_reason       TEXT,
    anomaly          TEXT,
    version          INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS one_live_call_per_borrower
    ON calls (borrower_id) WHERE state IN {_LIVE};
CREATE UNIQUE INDEX IF NOT EXISTS one_live_call_per_agent
    ON calls (agent_id) WHERE agent_id IS NOT NULL AND state IN {_LIVE};
CREATE INDEX IF NOT EXISTS calls_state ON calls (campaign_id, state);
CREATE INDEX IF NOT EXISTS calls_lease ON calls (state, lease_until);
CREATE INDEX IF NOT EXISTS calls_answered ON calls (campaign_id, answered_at);
CREATE INDEX IF NOT EXISTS calls_ended ON calls (campaign_id, ended_at);
CREATE INDEX IF NOT EXISTS calls_created ON calls (campaign_id, created_at);

-- Webhook inbox. The primary key is the dedupe: the same event delivered twice is one row.
CREATE TABLE IF NOT EXISTS provider_events (
    provider         TEXT NOT NULL,
    event_id         TEXT NOT NULL,
    call_id          TEXT NOT NULL,
    type             TEXT NOT NULL,
    reason           TEXT,
    provider_call_id TEXT,
    occurred_at      REAL NOT NULL,
    received_at      REAL NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | DONE
    outcome          TEXT,                              -- APPLIED | STALE | ABANDONED | ...
    lease_until      REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (provider, event_id)
);
CREATE INDEX IF NOT EXISTS events_pending ON provider_events (status, lease_until);
CREATE INDEX IF NOT EXISTS events_received ON provider_events (received_at);

-- Outbox of side effects decided inside a transaction and executed after it commits.
CREATE TABLE IF NOT EXISTS commands (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id         TEXT NOT NULL,
    kind            TEXT NOT NULL,                 -- BRIDGE | HANGUP | CANCEL
    arg             TEXT,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error      TEXT,
    UNIQUE (call_id, kind)
);
CREATE INDEX IF NOT EXISTS commands_due ON commands (status, next_attempt_at);

-- Every pacing round: what the pacer asked for, what safety allowed, and why.
CREATE TABLE IF NOT EXISTS decisions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id    TEXT NOT NULL,
    at             REAL NOT NULL,
    worker         TEXT NOT NULL,
    epoch          INTEGER NOT NULL,
    pacer          TEXT NOT NULL,
    proposed       INTEGER NOT NULL,
    approved_bound INTEGER NOT NULL,
    approved_spec  INTEGER NOT NULL,
    shed           INTEGER NOT NULL,
    action         TEXT NOT NULL,
    explanation    TEXT NOT NULL
);
"""


class Store:
    """Connection management. One SQLite connection per thread; ':memory:' is single-threaded."""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._local = threading.local()
        self.write_txns = 0                     # per-process counter, read by the load test
        self._memory_conn = self._connect() if path == ":memory:" else None
        self.conn().executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, isolation_level=None, timeout=30.0, check_same_thread=False)
        c.row_factory = sqlite3.Row
        if self.path != ":memory:":
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
        return c

    def conn(self) -> sqlite3.Connection:
        if self._memory_conn is not None:
            return self._memory_conn
        c = getattr(self._local, "conn", None)
        if c is None:
            c = self._local.conn = self._connect()
        return c

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        self.write_txns += 1
        try:
            yield c
            c.execute("COMMIT")
        except BaseException:          # includes SimulatedCrash: a crash mid-transaction loses it
            # Also covers a failed COMMIT; and SQLite may already have rolled back on its own,
            # in which case a bare ROLLBACK would raise and mask the original error.
            if c.in_transaction:
                c.execute("ROLLBACK")
            raise

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        """A read-only transaction: every query inside sees the same committed snapshot."""
        c = self.conn()
        c.execute("BEGIN")
        try:
            yield c
        finally:
            if c.in_transaction:
                c.execute("COMMIT")

    def all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        return self.conn().execute(sql, params).fetchall()

    def one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        return self.conn().execute(sql, params).fetchone()

    def scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        row = self.conn().execute(sql, params).fetchone()
        return None if row is None else row[0]

    def close(self) -> None:
        c = getattr(self._local, "conn", None)
        if c is not None:
            c.close()
            self._local.conn = None
