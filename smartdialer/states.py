"""Agent and call state machines.

The tables below are the single definition of which transitions are legal. Every write to
`agents.state` / `calls.state` goes through `repo.move_agent` / `repo.move_call`, which check
the transition here *and* use the expected current state as a compare-and-set guard in SQL.
"""
from __future__ import annotations

from enum import StrEnum


class AgentState(StrEnum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"      # claimed by the dialer for one specific call, call not yet at provider
    DIALING = "DIALING"        # its call is at the provider, borrower not answered yet
    CONNECTED = "CONNECTED"    # talking to a borrower
    WRAP_UP = "WRAP_UP"        # after-call work
    PAUSED = "PAUSED"


AGENT_TRANSITIONS: dict[AgentState, frozenset[AgentState]] = {
    AgentState.OFFLINE: frozenset({AgentState.AVAILABLE}),
    AgentState.AVAILABLE: frozenset({
        AgentState.RESERVED,      # progressive: reserved before dialing
        AgentState.CONNECTED,     # predictive: claimed at the moment a borrower answers
        AgentState.PAUSED, AgentState.OFFLINE,
    }),
    AgentState.RESERVED: frozenset({
        AgentState.DIALING,
        AgentState.CONNECTED,     # provider's ANSWERED overtook our own "call initiated" write
        AgentState.AVAILABLE,     # call could not be started
        AgentState.OFFLINE,       # agent disappeared during call setup
    }),
    AgentState.DIALING: frozenset({AgentState.CONNECTED, AgentState.AVAILABLE, AgentState.OFFLINE}),
    AgentState.CONNECTED: frozenset({AgentState.WRAP_UP, AgentState.OFFLINE}),
    AgentState.WRAP_UP: frozenset({
        AgentState.AVAILABLE,
        AgentState.CONNECTED,     # wrap-up pre-empted to take an answered call instead of abandoning it
        AgentState.PAUSED, AgentState.OFFLINE,
    }),
    AgentState.PAUSED: frozenset({AgentState.AVAILABLE, AgentState.OFFLINE}),
}


class CallState(StrEnum):
    QUEUED = "QUEUED"          # borrower claimed, no agent (predictive / speculative call)
    RESERVED = "RESERVED"      # borrower and agent claimed (progressive / agent-bound call)
    INITIATED = "INITIATED"    # provider accepted the call
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"      # transient: resolved to CONNECTED or ABANDONED in the same transaction
    CONNECTED = "CONNECTED"    # borrower bridged to an agent
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"          # no answer, busy, provider error, unresolved
    CANCELLED = "CANCELLED"    # we withdrew it before the borrower answered
    ABANDONED = "ABANDONED"    # borrower answered and no agent could take the call


class CallKind(StrEnum):
    BOUND = "BOUND"                # an agent was reserved before dialing (progressive)
    SPECULATIVE = "SPECULATIVE"    # agent is claimed only when the borrower answers (predictive)


class BorrowerStatus(StrEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    EXHAUSTED = "EXHAUSTED"


TERMINAL_CALL_STATES = frozenset({
    CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED, CallState.ABANDONED,
})
LIVE_CALL_STATES = frozenset(set(CallState) - TERMINAL_CALL_STATES)
UNANSWERED_CALL_STATES = frozenset({
    CallState.QUEUED, CallState.RESERVED, CallState.INITIATED, CallState.RINGING,
})

_PRE_ANSWER_EXITS = frozenset({
    CallState.ANSWERED, CallState.FAILED, CallState.CANCELLED,
    CallState.COMPLETED,   # provider says it ended without us ever seeing ANSWERED
    CallState.ABANDONED,   # ... and the reason was "nobody was bridged in"
})

CALL_TRANSITIONS: dict[CallState, frozenset[CallState]] = {
    CallState.QUEUED: frozenset({CallState.INITIATED, CallState.RINGING}) | _PRE_ANSWER_EXITS,
    CallState.RESERVED: frozenset({CallState.INITIATED, CallState.RINGING}) | _PRE_ANSWER_EXITS,
    CallState.INITIATED: frozenset({CallState.RINGING}) | _PRE_ANSWER_EXITS,
    CallState.RINGING: _PRE_ANSWER_EXITS,
    CallState.ANSWERED: frozenset({
        CallState.CONNECTED, CallState.ABANDONED, CallState.COMPLETED, CallState.FAILED,
    }),
    CallState.CONNECTED: frozenset({
        CallState.COMPLETED, CallState.FAILED,
        CallState.ABANDONED,      # an agent was assigned but the bridge never reached the provider
    }),
    CallState.COMPLETED: frozenset(),
    CallState.FAILED: frozenset(),
    CallState.CANCELLED: frozenset(),
    CallState.ABANDONED: frozenset(),
}

# Progress order. A provider event may only move a call *forward*; anything at or below the
# current rank is stale (duplicate or out of order) and is recorded but not applied.
CALL_RANK: dict[CallState, int] = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 0,
    CallState.INITIATED: 1,
    CallState.RINGING: 2,
    CallState.ANSWERED: 3,
    CallState.CONNECTED: 4,
    CallState.COMPLETED: 9,
    CallState.FAILED: 9,
    CallState.CANCELLED: 9,
    CallState.ABANDONED: 9,
}


class IllegalTransition(RuntimeError):
    """A programming error: code tried a transition the state machine does not allow."""


def check_agent_transition(src: AgentState, dst: AgentState) -> None:
    if dst not in AGENT_TRANSITIONS[src]:
        raise IllegalTransition(f"agent {src} -> {dst}")


def check_call_transition(src: CallState, dst: CallState) -> None:
    if dst not in CALL_TRANSITIONS[src]:
        raise IllegalTransition(f"call {src} -> {dst}")


def sql_in(states) -> str:
    """Render a set of enum members as a SQL IN list (values are our own constants, not input)."""
    return "(" + ",".join(f"'{s}'" for s in sorted(states)) + ")"
