# SmartDialer

A working prototype of an outbound dialer for collections with **both** modes:

- **progressive** (one available agent → one agent-bound call);
- **predictive** (dial ahead of agent availability).

In both modes a **Safety Controller** sits between pacing and the telecom provider, and the
pacing logic has no way to bypass it.

```
Campaign ─► Pacer ─► Safety Controller ─► Call Allocator ─► Provider Router ─► Provider A / B
           (proposal)   (sealed permit)    (only start_call)  (circuit breakers)      │ webhooks
Reconciler ─► Event Processor (inbox) ─► Command Dispatcher (outbox) ◄────────────────┘
      └──────────────── all state in one SQLite database, every write compare-and-set ────────┘
```

## Run it

Requires Python 3.11+. No dependencies beyond pytest.

```bash
python -m pip install -r requirements-dev.txt

python -m pytest                       # 79 tests, ~20s
python -m sim.run                      # scenarios A-D x provider A/B x both modes (~90s)
                                       #   -> docs/SIMULATION_RESULTS.md, results/timeseries_*.csv
python -m sim.run --scenario D --mode PREDICTIVE --provider A --explain 4
python -m sim.failures                 # the failure cases, step by step (~15s)
python -m sim.failures crash           # just one: race crash outage drop duplicates order setup
python -m loadtest.run                 # 4 worker processes on one database, real time
python -m loadtest.run --sweep         # 1/2/4/8 workers
python -m loadtest.run --scale         # per-tick cost vs campaign size
```

## What to read

| Document | Covers |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Architecture diagram, components, data model, **how two workers are prevented from reserving the same agent**, every distributed-systems concern from the brief |
| [docs/STATE_MACHINES.md](docs/STATE_MACHINES.md) | Agent and call state machines, and the rules for applying duplicate or out-of-order provider events |
| [docs/PACING.md](docs/PACING.md) | Progressive dialer, predictive pacing model, Safety Controller math, **"why 17 calls and not 10"**, what happens when the answer rate jumps or collapses |
| [docs/FAILURE_SCENARIOS.md](docs/FAILURE_SCENARIOS.md) | Worker crash, provider outage, agent drop, duplicate and out-of-order events, with real output |
| [docs/SIMULATION_RESULTS.md](docs/SIMULATION_RESULTS.md) | Generated: utilisation, calls, abandonment, safety decisions for every scenario |
| [docs/SCALING.md](docs/SCALING.md) | Load-test numbers; what breaks first at 1k / 10k / 100k agents and why |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Architecture decision record: what was chosen, why, and what it makes harder; least-confident parts; next week |

## Results in one table

60 simulated minutes, 50 agents, mock provider A (fast, reliable):

| Scenario | Progressive utilisation | Predictive utilisation | Predictive abandoned calls |
|---|---|---|---|
| A: 20% answer, 120s talk | 44% | **67%** | 0 |
| B: 50% answer, 90s talk | 63% | **70%** | 0 |
| C: 70% answer, 180s talk | 84% | **87%** | 0 |
| D: answer 30→70→10→45%, talk 90→180→60s | 53% | **67%** | 0 |

Across five random seeds per scenario (20 runs), predictive abandoned 2 calls in total, both in
one run of scenario D (0.16%). Against provider B (slow,
duplicate, late and out-of-order webhooks), the Safety Controller detects that webhooks arrive
too late to bridge an agentless call. It stays progressive, with at most 1 abandoned call per
hour (0.11%).

## Layout

```
smartdialer/
  states.py        agent and call state machines (the only definition of legal transitions)
  db.py, repo.py   schema; compare-and-set helpers every state change goes through
  pacing/          progressive and predictive pacers: pure functions, propose only
  safety.py        Safety Controller, SafetyPolicy (frozen), DialPermit (sealed)
  allocator.py     the only caller of provider.start_call
  events.py        webhook inbox: dedupe and forward-only application
  commands.py      outbox: bridge / hangup / cancel with retries
  recovery.py      reconciler: leases, heartbeats, provider status, pending work
  agents.py        agent desktop operations, agent disappearance
  observe.py       builds the pacing snapshot from the database
  worker.py        composition root: one tick of one worker
  providers/       interface, circuit breaker + router, mock providers A/B, scripted provider
  invariants.py    cross-table invariants checked everywhere
sim/               discrete-event simulator, scenarios A-D, failure demos
loadtest/          multi-process load test, process race
tests/             unit, concurrency, recovery, safety, end-to-end
```

## Final question

> How would you build a SmartDialer that gets as much of the utilisation benefit of predictive
> dialing as possible, while retaining the deterministic safety characteristics of progressive
> dialing?

Make predictive a **bounded, revocable extension of progressive**, not a separate mode.

1. **Keep two kinds of call.** An agent-bound call has its agent reserved before dialing and
   handed to the provider, which connects the borrower on answer without asking us. It is as
   safe as progressive by construction. A speculative call is the only thing that can be
   abandoned, so it is the only thing the predictive engine is allowed to ask for.
2. **The safety bound belongs to safety, not to the model.** Speculative calls in flight are
   capped so that P(more answers than agents) ≤ 2%. That uses the *upper* confidence bound of the
   answer rate, and counts only agents the system controls right now (free, or in wrap-up it can
   pre-empt), never agents still talking. With no data the bound is 1.0 and this is exactly
   progressive. Utilisation comes from dialing several calls per *free* agent when answer rates
   are low, which is where progressive wastes most agent time.
3. **Enforce it where it cannot be bypassed.** The pacer can only return a number. The cap is an
   absolute level checked in the same database transaction that creates the call, and is valid
   only for the current leadership epoch, so stale leaders and parallel workers cannot add up to
   more.
4. **Fall back the moment the model is wrong, and keep an exit open.** Revert to progressive on
   evidence rather than averages: an answer surge (recent dials already answered beyond the
   bound), agents disappearing, webhooks arriving late, provider health, or the realised abandon
   rate. Cancelling a call that has not been answered costs only a dial; abandoning one that has
   is a compliance event. So when safety is in doubt, it cancels unanswered speculative calls.

In the simulator this gives 67% utilisation against 44% at a 20% answer rate, with zero abandoned
calls. With a provider whose webhooks are too slow to bridge safely, it declines to dial ahead
and behaves exactly like progressive.
