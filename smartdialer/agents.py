"""Agent desktop operations, and what happens when an agent disappears."""
from __future__ import annotations

from typing import Iterable

from . import repo
from .clock import Clock
from .commands import CANCEL, CommandDispatcher
from .config import DialerConfig
from .db import Store
from .states import AgentState, UNANSWERED_CALL_STATES, sql_in

_UNANSWERED = sql_in(UNANSWERED_CALL_STATES)


class AgentService:
    def __init__(self, store: Store, clock: Clock, dispatcher: CommandDispatcher,
                 config: DialerConfig) -> None:
        self._store = store
        self._clock = clock
        self._dispatcher = dispatcher
        self._cfg = config

    def login(self, agent_id: str) -> bool:
        now = self._clock.now()
        with self._store.tx() as c:
            ok = repo.move_agent(c, agent_id, AgentState.AVAILABLE, expect={AgentState.OFFLINE},
                                 now=now, call_id=None)
            if ok:
                c.execute("UPDATE agents SET heartbeat_at=? WHERE id=?", (now, agent_id))
            return ok

    def heartbeat(self, agent_ids: Iterable[str]) -> None:
        ids = list(agent_ids)
        if not ids:
            return
        now = self._clock.now()
        with self._store.tx() as c:
            c.executemany("UPDATE agents SET heartbeat_at=? WHERE id=? AND state != 'OFFLINE'",
                          [(now, a) for a in ids])

    def pause(self, agent_id: str) -> bool:
        with self._store.tx() as c:
            return repo.move_agent(c, agent_id, AgentState.PAUSED,
                                   expect={AgentState.AVAILABLE, AgentState.WRAP_UP},
                                   now=self._clock.now(), call_id=None)

    def resume(self, agent_id: str) -> bool:
        with self._store.tx() as c:
            return repo.move_agent(c, agent_id, AgentState.AVAILABLE, expect={AgentState.PAUSED},
                                   now=self._clock.now())

    def finish_wrap_up(self, agent_id: str) -> bool:
        with self._store.tx() as c:
            return repo.move_agent(c, agent_id, AgentState.AVAILABLE, expect={AgentState.WRAP_UP},
                                   now=self._clock.now(), call_id=None)

    def logout(self, agent_id: str, reason: str = "LOGOUT") -> str | None:
        """Agent leaves (logout, closed browser, lost heartbeat). Returns its previous state.

        * idle states            -> OFFLINE.
        * RESERVED / DIALING     -> OFFLINE, and the call is detached from the agent and a CANCEL
                                    is requested. Until the provider confirms the cancel, the
                                    call is an ordinary agentless call: if the borrower answers
                                    first, another agent is claimed, else it is abandoned.
        * CONNECTED              -> OFFLINE; the call carries on to its natural end, detached.
        """
        now = self._clock.now()
        call_to_cancel = None
        with self._store.tx() as c:
            a = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
            if a is None or a["state"] == AgentState.OFFLINE:
                return None
            state = AgentState(a["state"])
            if not repo.move_agent(c, agent_id, AgentState.OFFLINE, expect={state}, now=now,
                                   call_id=None):
                return None                     # changed concurrently; don't detach its call
            call_id = a["call_id"]
            if call_id and state in (AgentState.RESERVED, AgentState.DIALING):
                detached = c.execute(
                    f"""UPDATE calls SET agent_id=NULL, cancel_requested_at=?, anomaly=?,
                            updated_at=? WHERE id=? AND agent_id=? AND state IN {_UNANSWERED}""",
                    (now, f"AGENT_LOST:{reason}", now, call_id, agent_id)).rowcount
                if detached:
                    repo.enqueue_command(c, call_id, CANCEL, now)
                    call_to_cancel = call_id
            elif call_id and state is AgentState.CONNECTED:
                c.execute("UPDATE calls SET agent_id=NULL, anomaly=? WHERE id=? AND agent_id=?",
                          (f"AGENT_LOST_ON_CALL:{reason}", call_id, agent_id))
        if call_to_cancel:
            self._dispatcher.flush(call_id=call_to_cancel)
        return str(state)
