# Pacing and the Safety Controller

Two separate questions, answered by two separate components:

1. **How many calls *should* we start?** The pacer. It optimises utilisation, and it is allowed
   to be wrong.
2. **How many calls *may* we start?** The Safety Controller. It assumes the pacer is wrong and
   only trusts what it can bound.

## Two kinds of call

| | Agent-bound (progressive) | Speculative (predictive) |
|---|---|---|
| Agent claimed | before dialing (`AVAILABLE → RESERVED`, CAS) | when the borrower answers |
| Who connects the borrower | the provider, immediately, because the agent was handed to it in `start_call` | us: ANSWERED webhook → claim agent → BRIDGE |
| Can it be abandoned? | no (unless the agent vanishes mid-setup *and* no one else is free) | yes, if more answer than there are free agents, or if the ANSWERED webhook is late |

Only speculative calls carry abandonment risk, and they are the only thing the predictive pacer
can ask for. Everything below is about bounding them.

## Progressive pacer

`proposal = agents AVAILABLE with a fresh heartbeat` (capped by borrowers due). Every call reserves
its agent before dialing, so 50 available agents can never produce more than 50 agent-bound
calls. The reservation is a CAS and the database has a unique index on live calls per agent
(`test_50_available_agents_never_produce_more_than_50_calls`).

Which borrower: highest `priority`, then earliest `next_attempt_at`, among borrowers that are
PENDING and due. Which agent: longest idle (`ORDER BY state_since`). When the agent becomes
reserved: in the same transaction that claims the borrower and inserts the call, before any
network I/O.

## Predictive pacer (`pacing/predictive.py`)

```
p        answer rate: 60s window, with the 5-min window as its prior, which uses the campaign prior
T        mean dial -> answer time
capacity agents free now
         + agents in wrap-up expected to finish within T
         + talking agents expected to finish talk + wrap-up within T   (exponential talk time)
expected un-answered agentless calls in flight × p
proposal floor((capacity − expected) / p)
```

Answer rates are measured on **mature dial cohorts**: calls dialed in a window that ended at
least 40s ago, so every outcome is known. Measuring "attempts resolved recently" is biased.
Answers resolve 3–15s after dialing but no-answers only at the 30s ring timeout, so whenever the
dial rate rises the answer rate looks inflated. The first version of this code had that bug, and
it produced false "answer rate shifted" alarms at a constant 20% rate.

## Safety Controller (`safety.py`)

It recomputes from the same snapshot with pessimistic assumptions:

* **Answer rate**: the Wilson *upper* bound (z = 1.96) over the long and short windows, whichever
  is higher. It is also raised to the share of recent dials that are *already answered* (a hard
  lower bound on today's rate, since answers can only increase). With no data the bound is 1.0,
  which makes everything below collapse to exactly progressive behaviour.
* **Capacity**: agents AVAILABLE now + agents in WRAP_UP (the system can pre-empt wrap-up).
  **Agents who are talking are never counted**, because we do not control when they finish. This
  is the main difference from the pacer's model, and the main reason the result is
  conservative.
* **Ceiling**: the largest number *n* of un-answered agentless calls such that
  `P(Binomial(n, p_ucb) > capacity) ≤ 2%`. The allocator enforces it inside the same transaction
  that inserts each call.

It then applies circuit breakers, in this order:

| Condition | Action |
|---|---|
| provider circuit OPEN | **REJECT** everything (calls in flight are left alone and reconciled later) |
| provider HALF_OPEN | one probe call per tick |
| fewer than 50 resolved attempts; pacing leader younger than 10s | **FALLBACK_PROGRESSIVE** |
| realised abandon rate (30 min) ≥ 3% | FALLBACK for a 60s cool-down; ≥ 1.5% halves the tolerance |
| > 0.5% of webhooks arrive more than 3s late | FALLBACK: an answered agentless call could not be bridged before the borrower gives up |
| logged-in agents fall ≥ 15% within 10s | FALLBACK + **SHED** (cancel not-yet-answered agentless calls) |
| answer surge: already-answered share of recent dials is significantly above the bound | FALLBACK + SHED down to progressive's invariant (agentless in flight ≤ free agents) |
| answer rate 60s vs 5min differs by ≥ 10 points and z ≥ 3.5 | FALLBACK for a 60s cool-down |
| otherwise | **APPROVE** the proposal, or **REDUCE** it to the room under the ceiling (**REJECT** if that room is 0) |

In FALLBACK_PROGRESSIVE, agents needed to absorb agentless calls still in flight are held back
and only the rest are reserved for agent-bound calls.

Shedding happens only after a shock. Capacity consumed by our own answers was already priced in
when the calls were dialed; cancelling on it would just churn calls. (The first version did shed
on it, and cancelled 230 calls in 15 minutes.) Cancelling an unanswered call costs a dial;
abandoning an answered one is a compliance event. That asymmetry is why shedding is the safe
exit.

### The boundary cannot be switched off by the pacer

* The pacer gets a frozen snapshot and returns a frozen proposal. The `pacing` package imports
  nothing that can reach the database, the allocator or a provider, and a test fails if it ever
  does.
* The policy is a frozen dataclass handed to the controller by the composition root. Nothing
  passes it to the pacer, and it cannot be mutated (`test_safety_policy_is_immutable`).
* The allocator accepts only a `DialPermit`, which only the controller can construct
  (`test_a_permit_cannot_be_forged`). The permit carries the leadership epoch and a 2s expiry.
  Its ceiling is re-checked atomically in the database, so even two valid permits cannot add up
  to more than the ceiling.
* Honest limit: in-process Python cannot stop determined code from importing a private name. In
  production the Safety Controller and allocator would run as their own service, and the permit
  would be a signed token.

## "Why did it initiate 17 calls instead of 10?"

Every decision stores this explanation in the `decisions` table. The pacer's side of the worked
example (`test_predictive_pacer_worked_example_17_calls`):

```
pacer[predictive] proposed 17 (bound=0, speculative=17):
    answer rate p=0.29 (60s: 14/50, 5m: 120/400, prior 0.30)
    dial->answer ~15s; talk ~150s; wrap ~10s
    agents free within 15s: 3 available + 2.0 of 2 in wrap-up + 1.5 of 45 talking = 6.5
    5 un-answered calls in flight x p = 1.4 answers expected
    missing 5.0 answers / p = 17.5 -> 17 calls
```

So 17 rather than 10: at a 29% answer rate, about 3.5 dials produce one answer, and about 5
answers are missing. Ten calls would leave roughly two of those agents idle for another cycle.

But the pacer does not get to decide. On this snapshot the Safety Controller says:

```
safety: REDUCE -> bound=0, speculative=1, shed=0 (predictive)
    capacity 5 = 3 available + 2 in wrap-up (talking agents never counted); answer-rate upper bound 0.42
    ceiling 6 agentless calls keeps P(answers > 5) <= 2.0%; 5 in flight -> room for 1
    proposal 17 cut to 1
```

It counts only the 5 agents it controls, uses p = 0.42 rather than 0.29, and already has 5 calls
in flight. With 8 agents available instead of 3, the pacer asks for 34 and safety allows 10
(ceiling 15). More sample decisions from a real run are in
[SIMULATION_RESULTS.md](SIMULATION_RESULTS.md).

## "The model predicted 70%. It suddenly drops to 10%."

That direction is safe: fewer borrowers answer than planned, so agents wait. Nothing is
abandoned. The system adapts as the mature cohorts move from 70% towards 10%, and the shift
detector forces a progressive cool-down while the model is untrustworthy. Utilisation dips; the
abandon rate does not move.

**The dangerous direction is the reverse** (10% → 45% in scenario D), where calls dialed for a
10% world are answered at 45%. Three layers handle it:

1. the ceiling uses the answer rate's *upper* bound, never the estimate;
2. within seconds, the share of recent dials that are *already answered* exceeds that bound. That
   is assumption-free evidence the model is wrong, so safety stops dialing ahead and cancels
   unanswered calls down to progressive's invariant;
3. if abandons happen anyway, the realised abandon rate trips the 3% breaker.

Before layer 2 existed, scenario D abandoned 44 calls (3.45%) in the minute after the surge.
With it: 0 on four of the five seeds tested, and 2 (0.16%) on the fifth.

## Results (60 simulated minutes, 50 agents, provider A)

| Scenario | Progressive util | Predictive util | Predictive abandoned |
|---|---|---|---|
| A: 20% answer, 120s talk | 44% | 67% | 0 |
| B: 50%, 90s | 63% | 70% | 0 |
| C: 70%, 180s | 84% | 87% | 0 |
| D: changing (30→70→10→45%, talk 90→180→60s) | 53% | 67% | 0 |

Robustness: across seeds 1–5, predictive utilisation was A 66–67%, B 71–72%, C 87–88%, D 66–67%.
Across all 20 runs there were **2 abandoned calls**, both in one run of scenario D (0.16%). With provider B (slow, late, duplicated webhooks),
safety detects the late webhooks and stays progressive: about the same utilisation as
progressive, and at most 1 abandon per hour (0.11%). The full table, including provider B and safety decision counts, is in
[SIMULATION_RESULTS.md](SIMULATION_RESULTS.md).

The gain is largest where progressive hurts most: low answer rates, where a progressive agent
spends half the hour listening to rings. At 70% answer rate there is little left to gain.

**Tolerance trade-off** (scenario A, abandon breaker disabled): overflow tolerance 0.5% / 2% / 5%
/ 10% / 20% gives 65% / 67% / 68% / 69% / 73% utilisation, with 0 abandons throughout in this
run. The default of 2% is deliberately conservative. Most of the remaining gap comes from never
counting talking agents as capacity, which is the price of keeping the safety bound independent
of talk-time predictions.
