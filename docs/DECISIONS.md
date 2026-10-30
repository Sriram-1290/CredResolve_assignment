# Architecture decisions

Each entry: what was chosen, why, what problem it solves, and what it makes harder.

### 1. Python, standard library only

* **Why**: the hard parts here are concurrency semantics and the pacing model, not raw speed.
  Python keeps the code short enough to read in one sitting, and `sqlite3` ships with it, so the
  project runs with `pip install pytest` and nothing else.
* **Makes harder**: CPU-bound pacing math at very large scale, and there is no async I/O for
  `start_call` yet (see SCALING.md).

### 2. One relational database as the single source of truth (SQLite here)

* **Why**: every core question is about shared state transitions: who holds this agent, did this
  event already happen, who owns this half-finished call. Transactions, compare-and-set updates
  and unique indexes answer all of them in one place.
* **Solves**: double booking (CAS + partial unique indexes), lost updates, and reasoning about
  consistency across agents, borrowers and calls. Multi-row changes are one transaction, so the
  invariants in `invariants.py` hold between any two transactions.
* **Makes harder**: SQLite has one writer, about 5,000 transactions/s in the load test, and it
  cannot be shared across hosts. The SQL is written CAS-style, so moving to PostgreSQL (with
  `SKIP LOCKED`) is a driver change. Sharding by campaign is the step after that.

### 3. No Redis, no Kafka, no cache

* **Why**: each would be a second source of truth that has to agree with the database. "The
  database says AVAILABLE, the cache says RESERVED" cannot happen if there is no cache.
* **Solves**: a whole class of consistency bugs, and operational complexity.
* **Makes harder**: read scaling for dashboards (add read replicas, never a cache that decisions
  depend on) and very high webhook burst rates (a log in front of the inbox at 100k+ agents).

### 4. Leases and a reconciler instead of distributed locks or a coordinator

* **Why**: a crashed worker must not leave anything stuck. Every in-flight responsibility is a
  row with an owner and an expiry: call start leases, event leases, pacing leadership, agent
  heartbeats. Any worker's reconciler takes over when one expires.
* **Solves**: worker crashes at any line (tested with named crash points), with no coordinator
  to fail.
* **Makes harder**: recovery is bounded by lease length (10s for call starts, 5s for
  leadership). Timeouts become product decisions.

### 5. One pacing leader per campaign, with a fencing epoch; ceilings as absolute levels

* **Why**: pacing reads a snapshot and decides a count. Two pacers acting on the same snapshot
  would both add N calls. Leadership is a lease, and the epoch increments when it changes hands.
  The allocator rejects permits from an old epoch inside the reservation transaction. The safety
  limit is expressed as "at most N agentless calls in flight", checked atomically per insert,
  not as "add N".
* **Solves**: duplicate pacing, and stale leaders after GC pauses or partitions. Even two valid
  permits cannot jointly exceed the ceiling.
* **Makes harder**: a very large campaign has one allocation loop. The fix is splitting it into
  agent pools, each with a leader.

### 6. Inbox for webhooks, outbox for side effects

* **Why**: "received" and "applied" are separate steps; so are "decided" and "told the
  provider". Each pair has a crash window between them.
* **Solves**: duplicate webhooks (primary-key dedupe), out-of-order webhooks (forward-only
  application), crash between receiving and applying (re-applied from the inbox), crash between
  deciding CONNECTED and sending the bridge (re-sent from the outbox).
* **Makes harder**: about 8 write transactions per call; more rows to retain or archive.

### 7. Two kinds of call: agent-bound and speculative

* **Why**: an agent-bound call has its agent reserved before dialing and handed to the provider,
  which bridges on answer without asking us. It cannot be abandoned because of anything on our
  side. Only speculative calls carry risk, so only they are what predictive pacing controls and
  what safety bounds.
* **Solves**: progressive keeps its deterministic safety even when webhooks are late, and the
  risk is isolated to a quantity we can bound.
* **Makes harder**: two code paths at answer time, and the provider must support
  "connect to agent on answer" (Plivo and Twilio do, through the answer URL).

### 8. A separate Safety Controller with its own pessimistic model

* **Why**: if the pacer's model is wrong, the pacer cannot be the one to notice. Safety uses the
  upper confidence bound of the answer rate, counts only agents it controls (never agents still
  talking), and caps P(answers > capacity) at 2%. With no data it degrades to exactly progressive.
  It falls back on evidence the model is wrong (answer surge, rate shift, agent drop, late
  webhooks, provider health, realised abandon rate).
* **Solves**: the pacer cannot turn safety off: it can only return a number, the policy is
  frozen and out of its reach, and only the controller can mint a permit.
* **Makes harder**: it leaves utilisation on the table. In scenario A predictive reaches 67%,
  where a model trusted fully could do more. That is intentional.

### 9. When the provider cannot tell us what happened, hold rather than release

* **Why**: an unanswered call in an outage might have been answered (and, for agent-bound calls,
  bridged to the agent). Releasing the agent or borrower could double-book a person who is
  actually on a call.
* **Solves**: no double booking during outages. No new calls go out while the circuit is open
  anyway, so holding costs little.
* **Makes harder**: agents can sit held for the length of an outage. After 15 minutes calls are
  closed as `UNRESOLVED / NEEDS_REVIEW`.

### 10. Simulate on the real code

* **Why**: the simulator drives the actual workers, SQLite schema, allocator and safety
  controller on a simulated clock. A result in the simulator is a result about this code, not
  about a model of it.
* **Solves**: an hour of dialing in about 4 seconds, reproducible from a seed. It found four real
  bugs during development: abandons from webhook latency on agent-bound calls; over-shedding;
  resolution-time bias in the answer rate; and a stuck lease when webhooks overtake a crashed
  worker.
* **Makes harder**: the mock world is simpler than reality (for example, talk time is
  independent of the agent).

## What I am least confident about

* **Silent agent disappearance.** Between an agent's desktop dying and its heartbeat expiring
  (up to 10s), the agent still counts as available, and an answered speculative call can be given
  to them. The real fix is a signal from the agent's softphone or SIP registration, not a shorter
  heartbeat.
* **Calls that were live when a provider died.** Holding them is safe for agents, but answered
  speculative calls that cannot be bridged are abandoned, and nothing can prevent that once the
  provider is down. The only mitigations are dialing ahead less through providers with poor
  health history, and failing over quickly.
* **The lag gate is evidence-based.** A provider with late webhooks gets a few minutes of
  predictive dialing before enough lateness is observed. Persisting each provider's lateness
  profile across campaigns and restarts would close that window.
* **In-process permit sealing.** Python cannot stop code from importing a private name. The
  boundary is enforced by structure and tests. In production the Safety Controller and allocator
  should be their own service, with signed permits.

## With another week

1. Concurrent `start_call` with a bounded pool, plus the incremental `campaign_stats` row, which
   are the first two scale limits.
2. PostgreSQL behind the same SQL, and a load test across real hosts.
3. A real provider (Plivo): answer-URL bridging for agent-bound calls, and webhook signature
   verification.
4. A per-provider lateness and health profile that persists across restarts.
5. Calling-window and regulatory rules (local time windows, max attempts per day, callback after
   abandonment) in the Safety Controller.
