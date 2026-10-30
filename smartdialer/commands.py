"""Outbox dispatcher: executes provider side effects that were decided inside a transaction.

Decide-then-act without an outbox has a crash window: commit "CONNECTED", die, and the bridge
request is never sent. Here the BRIDGE/HANGUP/CANCEL row is committed with the decision, the
worker sends it right after commit, and any worker's reconciler re-sends rows still PENDING.
Provider operations are idempotent, so sending twice is harmless.
"""
from __future__ import annotations

from .clock import Clock
from .config import DialerConfig
from .db import Store
from .providers.base import ProviderRejected, ProviderTimeout
from .providers.health import ProviderRouter

BRIDGE, HANGUP, CANCEL = "BRIDGE", "HANGUP", "CANCEL"
_CLAIM_S = 5.0


class CommandDispatcher:
    def __init__(self, store: Store, router: ProviderRouter, clock: Clock,
                 config: DialerConfig) -> None:
        self._store = store
        self._router = router
        self._clock = clock
        self._cfg = config

    def flush(self, call_id: str | None = None, limit: int = 200) -> int:
        now = self._clock.now()
        sql = """SELECT m.id, m.call_id, m.kind, m.arg, m.attempts, c.provider
                 FROM commands m JOIN calls c ON c.id = m.call_id
                 WHERE m.status='PENDING' AND m.next_attempt_at <= ?"""
        params: tuple = (now,)
        if call_id is not None:
            sql += " AND m.call_id = ?"
            params += (call_id,)
        rows = self._store.all(sql + " ORDER BY m.id LIMIT ?", params + (limit,))
        done = 0
        for row in rows:
            if self._send(row, now):
                done += 1
        return done

    def _send(self, row, now: float) -> bool:
        # Claim by pushing next_attempt_at forward, so a concurrent dispatcher skips it.
        with self._store.tx() as c:
            claimed = c.execute(
                """UPDATE commands SET next_attempt_at=?, attempts=attempts+1
                   WHERE id=? AND status='PENDING' AND next_attempt_at <= ?""",
                (now + _CLAIM_S, row["id"], now)).rowcount
        if not claimed:
            return False
        provider = self._router.get(row["provider"])
        try:
            if row["kind"] == BRIDGE:
                provider.bridge(row["call_id"], row["arg"])
            elif row["kind"] == HANGUP:
                provider.hangup(row["call_id"])
            elif row["kind"] == CANCEL:
                provider.cancel_call(row["call_id"])
            else:
                raise ValueError(row["kind"])
        except ProviderTimeout as e:
            self._router.record(row["provider"], ok=False)
            attempts = row["attempts"] + 1
            with self._store.tx() as c:
                if attempts >= self._cfg.command_max_attempts:
                    c.execute("UPDATE commands SET status='FAILED', last_error=? WHERE id=?",
                              (str(e), row["id"]))
                else:
                    backoff = min(0.5 * 2 ** (attempts - 1), 10.0)
                    c.execute("UPDATE commands SET next_attempt_at=?, last_error=? WHERE id=?",
                              (now + backoff, str(e), row["id"]))
            return False
        except ProviderRejected as e:
            self._router.record(row["provider"], ok=True)   # provider answered; request invalid
            with self._store.tx() as c:
                c.execute("UPDATE commands SET status='FAILED', last_error=? WHERE id=?",
                          (str(e), row["id"]))
            return False
        self._router.record(row["provider"], ok=True)
        with self._store.tx() as c:
            c.execute("UPDATE commands SET status='DONE' WHERE id=?", (row["id"],))
        return True
