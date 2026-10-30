# Scale: what breaks first, and how to fix it

## What the load test measured

`python -m loadtest.run --sweep --agents 2000 --seconds 12`: real worker processes sharing one
SQLite file (WAL mode), sub-second mock calls, one campaign per worker (each worker leads its
own). These numbers are from a Windows laptop; throughput repeated within a few percent across
runs, while tail latency varies more.

| worker processes | calls/s | webhooks/s | write txns/s (per call) | tick p50 / p95 / p99 ms | lock errors | invariants |
|---|---|---|---|---|---|---|
| 1 | 590 | 1367 | 4730 (8.0) | 77 / 105 / 134 | 0 | OK |
| 2 | 547 | 1212 | 4253 (7.8) | 139 / 227 / 254 | 0 | OK |
| 4 | 603 | 1276 | 4533 (7.5) | 124 / 553 / 1480 | 0 | OK |
| 8 | 625 | 1338 | 4918 (7.9) | 8 / 186 / 1387 | 0 | OK |

`python -m loadtest.run --scale`: cost of one pacing round's reads, and of one reservation, as
a single campaign grows (half the agents on calls, 10 historical calls per agent):

| agents in campaign | borrowers | snapshot ms | reserve + claim txn ms |
|---|---|---|---|
| 100 | 2,000 | 0.5 | 0.06 |
| 1,000 | 20,000 | 1.8 | 0.08 |
| 10,000 | 200,000 | 32 | 0.09 |
| 50,000 | 1,000,000 | 183 | 0.15 |

What these say:

* **Correctness held under contention**: zero double bookings, zero lock errors, invariants OK in
  every run.
* **Adding processes does not add throughput.** Throughput stays flat at about 600 calls/s,
  roughly 4,700 small write transactions per second, from one process to eight. That is what one
  SQLite file gives. The cost of more processes shows up as latency instead: they queue on the
  single write lock, and p99 tick time climbs from about 130ms to 1.4s. A slow tick is a slow
  reaction to an agent drop or a surge, so at that point the database is hurting safety
  responsiveness, not just throughput. SQLite's single writer is a hard ceiling, which is exactly
  what the load test was meant to find.
* **Each call costs about 8 write transactions**: reserve; record acceptance; ingest + apply for
  each of ~3 webhooks; bridge command claim and completion; finish wrap-up.
* **Reservation cost is flat** (indexed CAS, about 0.1ms). **The snapshot is not**: it scans the
  campaign's recent call history every tick, so it grows linearly.

## Demand, roughly

At a 20% answer rate and 2-minute talk time, an agent generates about 0.03 dial attempts per
second, and each attempt is about 8 write transactions and 3–4 webhooks.

| agents | dials/s | write txns/s | webhooks/s |
|---|---|---|---|
| 100 | 3 | 25 | 10 |
| 1,000 | 30 | 250 | 100 |
| 10,000 | 300 | 2,500 | 1,000 |
| 100,000 | 3,000 | 25,000 | 10,000 |

## What breaks, in order

### 100 → 1,000 agents: nothing structural

One worker process could do it alone (≈ 5% of measured capacity). Add a second worker for
availability, not throughput. Leadership is per campaign with sticky partitioning, so two workers
split campaigns and take over from each other in about 10s.

### 1,000 → 10,000 agents: the allocator's network I/O, then the snapshot

1. **Sequential `start_call`.** The allocator places calls one after another, and the mock
   answers instantly. A real provider takes 100–500ms per API call. At 300 dials/s that is
   30–150 seconds of waiting per second of wall clock, from one pacing leader. This breaks first,
   and well before the database does.
   *Fix:* keep the reservation transactions as they are (atomic, ~0.1ms), but issue `start_call`
   concurrently with a bounded pool (async I/O). The permit and the in-transaction ceiling
   already make concurrent execution safe; ordering does not matter.
2. **Snapshot cost.** 30ms per tick at 10k agents in one campaign is fine for one campaign, but
   it is O(history) per campaign per second.
   *Fix:* maintain rolling counters incrementally (attempts and answers per cohort bucket, abandon
   counts) in a `campaign_stats` row updated by the same transaction that applies each event.
   The snapshot then becomes O(1) reads.
3. **One pacing leader per giant campaign.** *Fix:* split a large campaign into agent pools
   (say 500 agents each), each with its own leader, ceiling and safety controller. Answers are
   bridged within the pool. Safety properties hold per pool, and the binomial bound tightens
   slightly because pools are smaller.

### 10,000 → 100,000 agents: the single database writer

25,000 write transactions/s is about 5 times what one SQLite file sustained here. *Fix:*

* **PostgreSQL**, with row-level locks and `SELECT ... FOR UPDATE SKIP LOCKED` for agent and
  borrower claiming. The CAS-style SQL is already written to be correct at READ COMMITTED, so
  this is a driver change, not a redesign.
* **Shard by campaign (or agent pool).** Agents, borrowers and calls of one pool never interact
  with another pool's rows, so the data partitions cleanly. Each shard is an independent
  instance of this exact design.
* **Webhook ingestion** at ~10,000/s: batch inbox inserts (many events per transaction), and
  route webhooks to the owning shard by the call id prefix. Only at this scale would a log
  (Kafka) in front of the inbox earn its place, as a buffer for ingestion bursts. It would not be
  a replacement for the inbox's dedupe.
* **Telecom limits become the real ceiling**: carrier calls-per-second caps and per-trunk
  concurrency. The Safety Controller should hold per-provider CPS budgets, the same way it holds
  the abandonment budget.

### "We just went from 1,000 to 100,000 agents. What breaks first?"

In this prototype: the sequential `start_call` loop of each campaign's pacing leader, because it
waits on the network for every call. Then the single SQLite writer, at about 5,000
transactions/s, around 15–20k agents. The reservation logic itself (indexed CAS) is not a
bottleneck at any of these sizes.
