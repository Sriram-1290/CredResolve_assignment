from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class DialMode(StrEnum):
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


@dataclass(frozen=True)
class CampaignConfig:
    """Operator-owned campaign settings. Stored in the campaigns table so every worker sees them."""
    id: str
    mode: DialMode = DialMode.PROGRESSIVE
    max_attempts: int = 3               # borrower-caused attempts: no answer, busy, abandoned
    retry_after_s: float = 900.0
    provider_retry_after_s: float = 30.0
    max_provider_failures: int = 5      # our/provider-caused failures, counted separately


@dataclass(frozen=True)
class DialerConfig:
    """Timeouts shared by all workers. Leases are what turn a crashed worker into recoverable state."""
    tick_s: float = 1.0
    leader_lease_s: float = 5.0         # pacing leadership per campaign
    start_lease_s: float = 10.0         # a worker has this long to record the result of start_call
    start_recheck_s: float = 3.0        # after an ambiguous start_call timeout, ask the provider this soon
    heartbeat_timeout_s: float = 10.0   # agent desktop heartbeat
    event_lease_s: float = 5.0          # a received webhook not applied within this is re-processed
    max_unanswered_s: float = 60.0      # setup + provider ring timeout + slack: can no longer be answered
    max_connected_s: float = 3600.0
    max_unresolved_s: float = 900.0     # stop waiting for a provider that cannot tell us a call's
                                        # fate; the call is closed and flagged NEEDS_REVIEW
    wrap_up_max_s: float = 120.0
    command_max_attempts: int = 8
