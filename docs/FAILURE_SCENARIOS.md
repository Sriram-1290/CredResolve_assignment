# Failure scenarios

Run them all with `python -m sim.failures` (about 15s; the output also goes to
`results/failures.txt`), or one at a time with `python -m sim.failures crash`
(`race`, `crash`, `outage`, `drop`, `duplicates`, `order`, `setup`). Each demo ends by checking
every cross-table invariant (`smartdialer/invariants.py`), and each is also a pytest case
(`tests/test_simulation.py::test_failure_demo`). The step-by-step parts use a scripted provider,
where every webhook is delivered by hand. The statistical parts run the full simulator with chaos
injected. The excerpts below are from an actual run.

## 1. Worker crash: agent reserved > borrower reserved > call initiated > worker crashes

**Mechanism.** The allocator reserves the agent, claims the borrower and inserts the call in one
transaction. The call row carries a lease: `owner = worker`, `lease_until = now + 10s`. Then it
calls `start_call(call_id)` outside any transaction (our call id is the provider's idempotency
key). Only after the provider accepts does a second transaction move the call to INITIATED and
clear the lease.

**When the system comes back**, any worker's reconciler finds the expired lease and asks the
provider about that call id:

* the provider has it → **adopt**: INITIATED, agent DIALING, status replayed as events;
* the provider has never seen it → **cancel the id first** (a tombstone, so a delayed
  `start_call` can never create it), then CANCELLED, with agent and borrower released (no attempt
  charged);
* the provider is unreachable → hold everything and ask again (see outage).

If other workers are alive, the provider's RINGING webhook usually arrives first. The event
itself confirms the start and clears the dead worker's lease.

```
t=+0s   worker w-doomed crashed at 'after_start_call' (provider accepted the call, our DB was not updated)
        call=RESERVED agent_id=a0 | agents[a0=RESERVED, a1=AVAILABLE] | borrower b17=IN_PROGRESS
t=+11s  the whole system was down (no webhooks processed). A worker starts, its lease scan
        finds the expired lease, asks the provider about that call id and adopts the call
        call=INITIATED agent_id=a0 | agents[a0=DIALING, a1=DIALING] | borrower b17=IN_PROGRESS
        ... start_call was sent for the adopted call exactly 1 time
t=+15s  ANSWERED -> CONNECTED
t=+75s  COMPLETED -> APPLIED; call=COMPLETED/NORMAL ... agents[a0=WRAP_UP] | borrower b17=DONE

Variant: crash BEFORE the provider saw the call
t=+11s  provider has no such call -> cancel(id) leaves a tombstone, reservation released:
        call=CANCELLED/START_NOT_CONFIRMED ... agents[a0=AVAILABLE] | borrower b17=PENDING

Full simulation: 2 workers, the pacing leader w0 crashes at t=600s right after a start_call
        t=  600.5s  worker w0 CRASHED at 'after_start_call'
        pacing epochs seen: [1, 2]; first decision by the new leader at t=606.0s (lease 5s)
        calls w0 held mid-setup when it died: 1 -> final {'FAILED': 1}; still leased to w0 at the end: 0
        abandoned in the whole run: 0
```

**"The provider sends ANSWERED, the worker crashes, then COMPLETED arrives."** The webhook is
committed to the inbox *before* it is applied. If the worker dies before applying it, or in the
middle (the transaction rolls back), the event is still PENDING. When its lease expires, any
worker applies it. COMPLETED arriving at another worker in between is applied on its own
merits: if the call is still INITIATED it closes it, and the late ANSWERED then becomes STALE.
If the crash happens after the commit but before the BRIDGE request goes out, the request is
already in the outbox, and the next reconciler pass sends it
(`test_worker_crashes_after_receiving_answered_then_completed_arrives`,
`test_crash_while_applying_answered_rolls_back_and_is_retried`,
`test_crash_after_commit_before_bridge_is_sent_outbox_delivers_it`).

## 2. Provider outage

The only provider times out on every API call, and its webhooks are held, for 3 minutes
(t=600–780).

```
time    health     safety action          started  agentless-unanswered  agents dialing/talking
t= 600  HEALTHY    REDUCE                      10                    18  0/35
t= 610  OPEN       REJECT                       2                    19  0/35
t= 620  OPEN       REJECT                       0                    19  0/35
t= 740  OPEN       REJECT                       0                    19  2/35
t= 790  HEALTHY    FALLBACK_PROGRESSIVE        17                     1  20/2
t= 800  HEALTHY    FALLBACK_PROGRESSIVE        33                     0  41/8
```

* **Existing calls** are never released early. If we cannot learn whether a borrower answered, we
  must assume they might be on the line. For an agent-bound call they may even be talking to the
  agent the provider bridged. Releasing would risk double-booking that agent or re-dialing that
  borrower. No new calls are being placed anyway, so holding costs little. When the provider
  recovers, the held webhooks and status queries settle everything. After `max_unresolved_s`
  (15 min) a call is closed as `FAILED/UNRESOLVED` and flagged `NEEDS_REVIEW`.
* **New calls**: start_call and status timeouts open the circuit breaker within about 10s. After
  that, safety REJECTs every tick (173 of 180 decisions) and the provider gets no traffic. An
  unanswered call stops counting as risk once it is older than the longest possible ring, so
  the risk ceiling frees up and, with a second provider, traffic fails over:
  `calls started during the outage by provider: {'A2': 38}`.
* **Retries**: a call that failed because of the provider reschedules the borrower soon and
  counts against `provider_failures`, not against the borrower's attempt budget
  (`7 borrowers rescheduled ... exhausted: 0`). Outbox commands back off exponentially.
* **Pacing**: REJECT while OPEN, one probe call per tick when HALF_OPEN, then progressive until
  the late-webhook signal clears. The held webhooks arrive minutes late, so predictive returns
  only at t=1085.
* **Cost**: 4 speculative calls answered during the outage could not be bridged, because the
  bridge API was down (`ABANDONED`, 0.73% for the run). Agent-bound calls were unaffected
  because the provider bridges them itself. This is the irreducible risk of dialing ahead
  through a provider that can die. It is one reason provider health is a safety input, and why
  this design prefers agent-bound calls whenever the evidence is weak.

## 3. Agent availability suddenly drops (100 agents, 40 disappear)

```
-- logout: agents log out explicitly
   time    logged-in  available  agentless-unanswered  action                shed
   t= 899.5        100         24                    76  REDUCE                   0
   t= 900.5         60         12                    81  FALLBACK_PROGRESSIVE    31
   t= 901.5         60         12                    47  FALLBACK_PROGRESSIVE     0
   reaction: 0.5s after it ... cancelled 31 not-yet-answered agentless calls; abandoned in the next 3 min: 0

-- vanish: desktops die silently, heartbeats stop (timeout 10s)
   t= 902.5        100         23                    78  REDUCE                   0
   t= 909.5         60         16                    83  FALLBACK_PROGRESSIVE    23
   reaction: 9.5s after it ... cancelled 23 not-yet-answered agentless calls; abandoned in the next 3 min: 0
```

**How quickly does it react?** On the next pacing tick (1s) for anything the system is told
about. For silent disappearance, within the heartbeat timeout: the snapshot stops counting an
agent the moment its heartbeat is stale, even before the reconciler logs it out. The heartbeat
timeout is therefore the single most important knob for this failure. 10s is the default; a
softphone with 2s heartbeats could support 5–6s.

**What it does**: capacity is recomputed from the agents still present; a ≥15% drop within 10s
triggers fallback to progressive for 60s and *sheds* (cancels) not-yet-answered agentless calls,
preferring ones not yet ringing at the borrower's phone. Agents who were RESERVED or DIALING when
they vanished have their calls detached and cancelled (see scenario 6).

## 4. Duplicate events

```
ANSWERED evt-42                      -> CONNECTED  call=CONNECTED version=2
ANSWERED evt-42 (redelivery)         -> DUPLICATE  call=CONNECTED version=2
ANSWERED evt-43 (re-sent, new id)    -> STALE      call=CONNECTED version=2
COMPLETED evt-44                     -> APPLIED    call=COMPLETED version=3
bridge requests sent: 1

simulation, 50% of webhooks delivered twice: max transitions on any call: 4
```

No. Same id: the inbox primary key makes the second delivery a no-op. Same meaning under a new
id: the forward-only rank rule records it as STALE. The `version` column counts transitions and
does not move.

## 5. Out-of-order events

```
COMPLETED              -> APPLIED  call=COMPLETED/NORMAL ... agents[a0=AVAILABLE]
ANSWERED (90s late)    -> STALE
RINGING (90s late)     -> STALE
anomaly=COMPLETED_BEFORE_ANSWERED; answered_at kept from the late event: True

simulation, 30% of webhooks 8-30s late (progressive): abandoned 0, invariants OK
```

No. Terminal states absorb everything, and late events still contribute facts (`answered_at`).
For speculative calls a *late ANSWERED* is a real problem, because we cannot bridge what we have
not seen. That is why the Safety Controller measures webhook lateness and refuses to dial ahead
through a provider whose webhooks are late (provider B).

## 6. The agent disappears during call setup

```
dialing:              call=INITIATED agent_id=a0 | agents[a0=DIALING, a1=PAUSED]
a0 disappears:        call=INITIATED agent_id=None | agents[a0=OFFLINE, a1=PAUSED]; cancel requested=True, provider cancel sent=True
borrower answers before the cancel lands -> CONNECTED: call=CONNECTED agent_id=a1 | agents[a0=OFFLINE, a1=CONNECTED]
```

The agent goes OFFLINE, the call is detached and a cancel requested. The call still counts as an
agentless call until the provider confirms the cancel. If the borrower answers first, the
longest-idle available agent (or one in wrap-up) takes it; only if nobody can is it hung up and
counted as abandoned.

## 0. Two workers reserve the same agent at the same instant

```
20 threads, each with its own SQLite connection, released by one barrier
winners: 1 (thread [2]); agent a0 is now RESERVED for call-2
```

See [ARCHITECTURE.md](ARCHITECTURE.md#two-workers-see-the-same-available-agent-at-the-same-time).
`tests/test_concurrency.py` repeats it with 4 separate OS processes.
