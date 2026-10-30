"""Failure-case demonstrations.

    python -m sim.failures            # all of them; also writes results/failures.txt
    python -m sim.failures crash      # one of: race crash outage drop duplicates order setup

Each demo either scripts the exact sequence from the assignment (ScriptedWorld: every provider
event delivered by hand) or runs the full simulator with chaos injected, and checks the
cross-table invariants at the end.
"""
from __future__ import annotations

import re
import sys
import tempfile
import threading
from collections import Counter
from dataclasses import replace
from pathlib import Path

from smartdialer import repo
from smartdialer.config import DialMode
from smartdialer.db import Store
from smartdialer.faults import Faults, SimulatedCrash
from smartdialer.invariants import check_invariants
from smartdialer.providers.base import EndReason, EventType
from smartdialer.providers.mock import PROVIDER_A

from .engine import Simulation
from .scenarios import scenario
from .scripted import CAMPAIGN, ScriptedWorld

ROOT = Path(__file__).resolve().parents[1]
OUT: list[str] = []


def say(line: str = "") -> None:
    print(line, flush=True)
    OUT.append(line)


def header(title: str) -> None:
    say()
    say("=" * 100)
    say(title)
    say("=" * 100)


def state(w: ScriptedWorld, call_id: str | None = None) -> str:
    agents = ", ".join(f"{a}={s}" for a, s in sorted(w.agent_states().items()))
    if call_id is None:
        return f"agents[{agents}]"
    c = w.call(call_id)
    b = w.borrower(c["borrower_id"])
    return (f"call={c['state']}{'/' + c['end_reason'] if c['end_reason'] else ''} "
            f"agent_id={c['agent_id']} | agents[{agents}] | borrower {b['id']}={b['status']}")


def invariants(store: Store) -> bool:
    problems = check_invariants(store)
    say(f"invariants: {'OK' if not problems else problems}")
    return not problems


# ------------------------------------------------------------------------------------ demos
def demo_race() -> bool:
    header("0. Two workers see the same AVAILABLE agent at the same instant")
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "race.db")
        w = ScriptedWorld(agents=1, store=Store(path))
        barrier, winners = threading.Barrier(20), []

        def worker(i: int) -> None:
            store = Store(path)
            barrier.wait()
            with store.tx() as c:
                if repo.reserve_available_agent(c, CAMPAIGN, f"call-{i}", 1e12, 0):
                    winners.append(i)
            store.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        say("20 threads, each with its own SQLite connection, released by one barrier, run:")
        say("  UPDATE agents SET state='RESERVED' ... WHERE id=(SELECT ... AVAILABLE ...) "
            "AND state='AVAILABLE'")
        say(f"winners: {len(winners)} (thread {winners}); agent a0 is now "
            f"{w.agent('a0')['state']} for {w.agent('a0')['call_id']}")
        ok = len(winners) == 1
        w.store.close()
    return ok


def demo_crash() -> bool:
    header("1. Worker crash: agent reserved > borrower reserved > call initiated > worker crashes")
    w = ScriptedWorld(agents=2)
    faults = Faults()
    doomed = w.new_worker("w-doomed", faults)
    faults.arm("after_start_call")
    try:
        doomed.tick()
    except SimulatedCrash as e:
        say(f"t=+0s   worker w-doomed crashed at '{e}' (provider accepted the call, our DB "
            f"was not updated)")
    cid = w.calls()[0]["id"]
    c = w.call(cid)
    say(f"        {state(w, cid)}")
    say(f"        call lease owner={c['owner']} until t=+{c['lease_until'] - w.clock.now():.0f}s; "
        f"provider knows it as {w.provider.placed[cid]}")
    w.advance(11)
    w.heartbeat_all()
    say("t=+11s  the whole system was down (no webhooks processed). A worker starts, its lease scan")
    say("        finds the expired lease, asks the provider about that call id and adopts the call")
    back = w.new_worker("w-restarted")
    back.tick()
    c = w.call(cid)
    say(f"        call anomaly={c['anomaly']} provider_call_id={c['provider_call_id']} lease={c['lease_until']}")
    say(f"        {state(w, cid)}")
    say(f"        the other agent got a normal new call: {len(w.calls())} calls in total; "
        f"start_call was sent for the adopted call exactly {w.provider.ops('start').count(cid)} time")
    w.advance(4)
    say(f"t=+15s  ANSWERED -> {back.receive_event(w.ev(cid, EventType.ANSWERED))}; {state(w, cid)}")
    w.advance(60)
    w.heartbeat_all()
    say(f"t=+75s  COMPLETED -> {back.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NORMAL))}; "
        f"{state(w, cid)}")
    ok = invariants(w.store)

    say()
    say("Variant: other workers are alive, so the provider's RINGING webhook reaches one of them first")
    w3 = ScriptedWorld(agents=1)
    f3 = Faults()
    doomed = w3.new_worker("w-doomed", f3)
    f3.arm("after_start_call")
    try:
        doomed.tick()
    except SimulatedCrash:
        pass
    cid3 = w3.calls()[0]["id"]
    w3.advance(2)
    survivor = w3.new_worker("w-survivor")
    outcome = survivor.receive_event(w3.ev(cid3, EventType.RINGING))
    c3 = w3.call(cid3)
    say(f"        RINGING -> {outcome}: the event itself confirms the start; dead worker's lease "
        f"cleared (lease={c3['lease_until']}); {state(w3, cid3)}")
    ok &= invariants(w3.store) and c3["lease_until"] is None

    say()
    say("Variant: crash BEFORE the provider saw the call (crash point 'before_start_call')")
    w2 = ScriptedWorld(agents=1)
    f2 = Faults()
    doomed = w2.new_worker("w-doomed", f2)
    f2.arm("before_start_call")
    try:
        doomed.tick()
    except SimulatedCrash:
        pass
    cid2 = w2.calls()[0]["id"]
    say(f"        after crash: {state(w2, cid2)}")
    w2.advance(11)
    w2.heartbeat_all()
    w2.new_worker("w-restarted").reconciler.run_once()
    say("t=+11s  provider has no such call -> cancel(id) leaves a tombstone, reservation released:")
    say(f"        {state(w2, cid2)}; tombstoned={cid2 in w2.provider.tombstones}")
    ok &= invariants(w2.store)

    say()
    say("Full simulation: 2 workers, scenario B predictive; the pacing leader w0 crashes at t=600s "
        "right after a start_call")
    spec = replace(scenario("B", DialMode.PREDICTIVE, "A", minutes=20), workers=2,
                   actions=((600.0, lambda s: s.crash_worker_at("w0", "after_start_call")),))
    sim = Simulation(spec)
    r = sim.run()
    epochs = sorted({(d.decision.permit.epoch, d.decision.permit.issued_at // 1) for d in sim.decisions
                     if 595 <= d.decision.permit.issued_at <= 620})
    leaders = Counter()
    for d in sim.decisions:
        leaders[(d.decision.permit.epoch)] += 1
    orphans = sim.orphaned.get("w0", [])
    fates = Counter(f"{x['state']}{' (adopted)' if x['anomaly'] == 'ADOPTED' else ''}"
                    for x in (sim.store.one("SELECT state, anomaly FROM calls WHERE id=?", (cid,))
                              for cid in orphans))
    still_leased = sim.store.scalar("SELECT COUNT(*) FROM calls WHERE owner='w0'")
    for line in sim.log:
        say("        " + line)
    first_new = min((t for e, t in epochs if e > min(e for e, _ in epochs)), default=None)
    say(f"        pacing epochs seen: {sorted(leaders)}; first decision by the new leader at "
        f"t={first_new}s (lease {spec.config.leader_lease_s:.0f}s)")
    say(f"        calls w0 held mid-setup when it died: {len(orphans)} -> final {dict(fates)}; "
        f"still leased to w0 at the end: {still_leased}")
    say(f"        abandoned in the whole run: {r.abandoned}; utilization {r.utilization:.0%}")
    ok &= invariants(sim.store) and len(orphans) >= 1 and still_leased == 0
    return ok


def demo_outage() -> bool:
    header("2. Provider outage: the only provider times out on everything for 3 minutes")
    spec = replace(scenario("B", DialMode.PREDICTIVE, "A", minutes=25),
                   actions=((600.0, lambda s: s.set_outage(0, True)),
                            (780.0, lambda s: s.set_outage(0, False))))
    sim = Simulation(spec)
    r = sim.run()
    st = sim.store
    say("time    health     safety action          started  agentless-unanswered  agents dialing/talking")
    by_t = {int(d.decision.permit.issued_at): d for d in sim.decisions}
    samples = {int(s.t): s for s in sim.samples}
    for t in list(range(590, 640, 10)) + list(range(700, 781, 40)) + list(range(780, 900, 10)):
        d = by_t.get(t) or by_t.get(t + 1)
        s = samples.get(t)
        started = st.scalar("SELECT COUNT(*) FROM calls WHERE created_at >= ? AND created_at < ?",
                            (t - 10, t))
        say(f"t={t:4d}  {d.decision.health:9s}  {d.decision.action:21s}  {started:7d}  "
            f"{s.unbound_in_flight if s else '':>20}  {s.reserved if s else ''}/{s.connected if s else ''}")
    live_at_start = st.all(
        "SELECT state, end_reason, anomaly FROM calls WHERE created_at < 600 AND "
        "(ended_at IS NULL OR ended_at >= 600)")
    say()
    say(f"existing calls (live at t=600): {len(live_at_start)} -> final: "
        f"{dict(Counter((x['state'], x['end_reason']) for x in live_at_start).most_common(6))}")
    say("    they were never released early: the provider could not tell us whether the borrower "
        "answered, so agents/borrowers stayed held until it could.")
    during = st.all("SELECT state, end_reason FROM calls WHERE created_at >= 600 AND created_at < 780")
    say(f"new calls attempted during the outage: {len(during)} "
        f"{dict(Counter((x['state'], x['end_reason']) for x in during))}")
    during_d = [d.decision for d in sim.decisions if 600 <= d.decision.permit.issued_at < 780]
    opened = min((d.permit.issued_at for d in during_d if d.health == "OPEN"), default=None)
    say(f"    for the first ~{(opened or 600) - 600:.0f}s the agentless calls dialed before the outage "
        f"could still be answered and kept the safety ceiling full; then start_call and status "
        f"timeouts opened the circuit breaker (t={opened}).")
    say(f"safety decisions during the outage: {dict(Counter(str(d.action) for d in during_d))}; "
        f"provider health seen: {dict(Counter(d.health for d in during_d))}")
    ab = st.all("SELECT kind, end_reason, COUNT(*) n FROM calls WHERE state='ABANDONED' "
                "GROUP BY kind, end_reason")
    say(f"abandoned calls: {[(x['kind'], x['end_reason'], x['n']) for x in ab]}: agentless calls "
        f"answered during the outage could not be bridged (bridge requests time out). Agent-bound "
        f"calls are bridged by the provider itself and were unaffected.")
    retried = st.scalar("SELECT COUNT(*) FROM borrowers WHERE provider_failures > 0")
    exhausted = st.scalar("SELECT COUNT(*) FROM borrowers WHERE status='EXHAUSTED'")
    say(f"retries: {retried} borrowers rescheduled after provider-side failures "
        f"(not charged as borrower attempts); exhausted: {exhausted}")
    after = [d.decision for d in sim.decisions if 780 <= d.decision.permit.issued_at]
    why = Counter(re.sub(r"[\d.]+%?", "#", r.split(": ", 1)[1].split(":")[0])[:70]
                  for d in after for r in d.reasons if r.startswith("fallback"))
    back = min((d.permit.issued_at for d in after if d.mode == "predictive"), default=None)
    say(f"pacing after recovery: progressive fallback, top reasons {dict(why.most_common(3))}; "
        f"predictive again from t={back}")
    say(f"whole run: abandoned {r.abandoned} ({r.abandon_rate:.2%}), utilization {r.utilization:.0%}")
    ok = invariants(st)

    say()
    say("Variant: two providers (A primary, A2 backup); A goes dark for 3 minutes")
    spec2 = replace(spec, providers=(PROVIDER_A, replace(PROVIDER_A, name="A2")))
    sim2 = Simulation(spec2)
    r2 = sim2.run()
    split = Counter((x["provider"]) for x in sim2.store.all(
        "SELECT provider FROM calls WHERE created_at >= 610 AND created_at < 780"))
    say(f"calls started during the outage by provider: {dict(split)}; "
        f"utilization {r2.utilization:.0%} vs {r.utilization:.0%} without a backup; "
        f"abandoned {r2.abandoned}")
    ok &= invariants(sim2.store)
    return ok


def demo_drop() -> bool:
    header("3. Agent availability suddenly drops: 100 agents, 40 vanish at t=900s")
    ok = True
    for how in ("vanish", "logout"):
        def drop(s: Simulation, how=how) -> None:
            gone = s.vanish_agents(40)
            if how == "logout":
                for a in gone:
                    s.desk.logout(a, reason="LOGOUT")
        spec = replace(scenario("A", DialMode.PREDICTIVE, "A", minutes=25, agents=100),
                       actions=((900.0, drop),))
        sim = Simulation(spec)
        r = sim.run()
        say(f"-- {how}: " + ("desktops die silently, heartbeats stop (timeout "
                              f"{spec.config.heartbeat_timeout_s:.0f}s)" if how == "vanish"
                              else "agents log out explicitly"))
        say("   time    logged-in  available  agentless-unanswered  action                shed")
        detected = None
        for d in sim.decisions:
            dec, snap = d.decision, d.decision.snapshot
            t = dec.permit.issued_at
            if detected is None and t > 900 and snap.agents_logged_in < 100:
                detected = t
            if 898 <= t <= 903 or (detected is not None and detected - 1 <= t <= detected + 2):
                say(f"   t={t:6.1f}  {snap.agents_logged_in:9d}  {snap.agents_available:9d}  "
                    f"{snap.unbound_in_flight:20d}  {dec.action:20s}  {dec.permit.shed:4d}")
        ab = sim.store.scalar("SELECT COUNT(*) FROM calls WHERE state='ABANDONED' "
                              "AND ended_at BETWEEN 900 AND 1080")
        shed = sim.store.scalar("SELECT COUNT(*) FROM calls WHERE anomaly='SHED'")
        say(f"   reaction: safety saw the drop at t={detected} ({detected - 900:.1f}s after it), "
            f"fell back to progressive and cancelled {shed} not-yet-answered agentless calls; "
            f"abandoned in the next 3 min: {ab}; whole run {r.abandoned} ({r.abandon_rate:.2%})")
        ok &= invariants(sim.store)
    return ok


def demo_duplicates() -> bool:
    header("4. Duplicate events: does the same event create multiple transitions?")
    w = ScriptedWorld(agents=1)
    w.worker.tick()
    cid = w.calls()[0]["id"]
    first = w.ev(cid, EventType.ANSWERED, event_id="evt-42")
    for label, ev in (("ANSWERED evt-42", first), ("ANSWERED evt-42 (redelivery)", first),
                      ("ANSWERED evt-43 (re-sent, new id)", w.ev(cid, EventType.ANSWERED, event_id="evt-43")),
                      ("COMPLETED evt-44", w.ev(cid, EventType.COMPLETED, EndReason.NORMAL, "evt-44"))):
        outcome = w.worker.receive_event(ev)
        c = w.call(cid)
        say(f"{label:36s} -> {outcome:10s} call={c['state']:9s} version={c['version']} "
            f"agent={w.agent('a0')['state']}")
    say(f"bridge requests sent: {len(w.provider.ops('bridge:a0'))}")
    ok = invariants(w.store) and len(w.provider.ops("bridge:a0")) == 1

    say()
    noisy = replace(PROVIDER_A, name="DUP", duplicate_rate=0.5)
    spec = replace(scenario("B", DialMode.PREDICTIVE, "A", minutes=15), providers=(noisy,))
    sim = Simulation(spec)
    r = sim.run()
    rows = sim.store.all("SELECT outcome, COUNT(*) n FROM provider_events GROUP BY outcome")
    say(f"simulation, 50% of webhooks delivered twice: {sim.providers[0].stats['duplicates']} duplicates sent")
    say(f"   receive() outcomes: {dict(sim.event_outcomes)}")
    say(f"   stored events by outcome: {dict((x['outcome'], x['n']) for x in rows)}")
    maxv = sim.store.scalar("SELECT MAX(version) FROM calls")
    say(f"   max transitions on any call: {maxv} (initiated, ringing, connected, completed = 4)")
    say(f"   abandoned {r.abandoned}, utilization {r.utilization:.0%}")
    ok &= invariants(sim.store) and maxv <= 4
    return ok


def demo_order() -> bool:
    header("5. Out-of-order events: COMPLETED, ANSWERED, RINGING")
    w = ScriptedWorld(agents=1)
    w.worker.tick()
    cid = w.calls()[0]["id"]
    ringing, answered = w.ev(cid, EventType.RINGING), w.ev(cid, EventType.ANSWERED)
    w.advance(90)
    for label, ev in (("COMPLETED", w.ev(cid, EventType.COMPLETED, EndReason.NORMAL)),
                      ("ANSWERED (90s late)", answered), ("RINGING (90s late)", ringing)):
        say(f"{label:22s} -> {w.worker.receive_event(ev):8s} {state(w, cid)}")
    c = w.call(cid)
    say(f"anomaly={c['anomaly']}; answered_at kept from the late event: {c['answered_at'] is not None}")
    ok = invariants(w.store)

    say()
    messy = replace(PROVIDER_A, name="LATE", straggler_rate=0.3, delivery_s=(0.05, 2.0))
    spec = replace(scenario("B", DialMode.PROGRESSIVE, "A", minutes=15), providers=(messy,))
    sim = Simulation(spec)
    r = sim.run()
    rows = sim.store.all("SELECT outcome, COUNT(*) n FROM provider_events GROUP BY outcome")
    anomalies = sim.store.all("SELECT anomaly, COUNT(*) n FROM calls WHERE anomaly IS NOT NULL GROUP BY anomaly")
    say(f"simulation, 30% of webhooks 8-30s late (progressive): {sim.providers[0].stats['stragglers']} stragglers")
    say(f"   event outcomes: {dict((x['outcome'], x['n']) for x in rows)}")
    say(f"   call anomalies recorded: {dict((x['anomaly'], x['n']) for x in anomalies)}")
    say(f"   abandoned {r.abandoned} (agent-bound calls are bridged by the provider itself, so late "
        f"webhooks cannot abandon them)")
    ok &= invariants(sim.store)
    return ok


def demo_setup() -> bool:
    header("6. The agent disappears during call setup")
    w = ScriptedWorld(agents=2)
    w.worker.agents.pause("a1")
    w.worker.tick()
    cid = w.calls()[0]["id"]
    say(f"dialing:              {state(w, cid)}")
    w.worker.agents.logout("a0", reason="BROWSER_CLOSED")
    c = w.call(cid)
    say(f"a0 disappears:        {state(w, cid)}; cancel requested={c['cancel_requested_at'] is not None}, "
        f"provider cancel sent={w.provider.ops('cancel') == [cid]}")
    w.worker.agents.resume("a1")
    say(f"borrower answers before the cancel lands -> "
        f"{w.worker.receive_event(w.ev(cid, EventType.ANSWERED))}: {state(w, cid)}")
    return invariants(w.store)


DEMOS = {"race": demo_race, "crash": demo_crash, "outage": demo_outage, "drop": demo_drop,
         "duplicates": demo_duplicates, "order": demo_order, "setup": demo_setup}


def main() -> None:
    names = sys.argv[1:] or list(DEMOS)
    results = {n: DEMOS[n]() for n in names}
    header("Summary")
    for n, ok in results.items():
        say(f"{n:12s} {'OK' if ok else 'FAILED'}")
    out = ROOT / "results" / "failures.txt"
    out.parent.mkdir(exist_ok=True)
    out.write_text("\n".join(OUT) + "\n", encoding="utf-8")
    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
