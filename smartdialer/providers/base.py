"""Telecom provider contract. The dialer only ever sees this interface."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Protocol


class EventType(StrEnum):
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class EndReason:
    NORMAL = "NORMAL"          # conversation ended
    NO_AGENT = "NO_AGENT"      # borrower answered, nobody was bridged in, borrower hung up
    HANGUP = "HANGUP"          # we hung up (abandon handling)
    NO_ANSWER = "NO_ANSWER"
    BUSY = "BUSY"
    CANCELLED = "CANCELLED"    # we cancelled before answer
    ERROR = "ERROR"


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    event_id: str              # provider's id for this delivery; redelivery reuses it
    call_id: str               # our call id, echoed back (sent as the client reference)
    type: EventType
    occurred_at: float
    provider_call_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class ProviderCallStatus:
    provider_call_id: str
    answered: bool
    ended: bool
    end_type: EventType | None = None
    end_reason: str | None = None


class ProviderError(Exception):
    pass


class ProviderTimeout(ProviderError):
    """Outcome unknown: the request may or may not have taken effect."""


class ProviderRejected(ProviderError):
    """Definitive: the request did not take effect."""


class TelecomProvider(Protocol):
    name: str

    def start_call(self, call_id: str, phone: str, agent_id: str | None = None) -> str:
        """Place a call. Idempotent on call_id. Returns the provider's call id.

        With agent_id (agent-bound call) the provider connects the borrower to that agent itself
        the moment they answer, so the connection does not depend on our webhook round-trip.
        Without it (speculative call) the borrower waits until we send `bridge`."""

    def cancel_call(self, call_id: str) -> None:
        """Withdraw a call that has not been answered. If the provider has never seen call_id,
        it remembers the id so a delayed start_call for it is refused (a tombstone)."""

    def hangup(self, call_id: str) -> None: ...

    def bridge(self, call_id: str, agent_id: str) -> None:
        """Connect an answered call to an agent's endpoint. Idempotent."""

    def get_call_status(self, call_id: str) -> ProviderCallStatus | None:
        """None means the provider has no such call."""


EventSink = Callable[[ProviderEvent], Any]
