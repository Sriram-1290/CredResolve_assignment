"""Basic load test: N dialer worker *processes* sharing one SQLite database, in real time.

    python -m loadtest.run                                  # 4 workers, 400 agents, 15s
    python -m loadtest.run --workers 8 --agents 1000 --campaigns 8 --seconds 20
    python -m loadtest.run --sweep                          # 1, 2, 4, 8 workers
    python -m loadtest.run --scale                          # per-tick cost vs campaign size

Each process runs the real DialerWorker (reconciler, pacing, safety, allocator, event processing)
against its own fast MockProvider (sub-second calls), plays the agent desktops for its share of
agents, and paces the campaigns in its partition. At the end the main process checks every
cross-table invariant on the shared database and reports throughput and latency.

Note: each process has its own mock provider, so a reconciler could not look up another
process's calls. Nothing crashes in this test, so no cross-process reconciliation is needed.
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import random
import sqlite3
import tempfile
import time
from collections import Counter
from pathlib import Path

from smartdialer import repo
from smartdialer.clock import EventLoop, RealClock
from smartdialer.config import CampaignConfig, DialerConfig, DialMode
from smartdialer.db import Store
from smartdialer.invariants import check_invariants
from smartdialer.observe import SnapshotBuilder
from smartdialer.providers.health import ProviderRouter
from smartdialer.providers.mock import BorrowerModel, MockProfile, MockProvider
from smartdialer.worker import DialerWorker

FAST = MockProfile(name="LT", setup_s=(0.02, 0.1), delivery_s=(0.005, 0.03), reject_rate=0.0,
                   ring_timeout_s=1.0, bridge_grace_s=1.0)
PEOPLE = BorrowerModel(answer_rate=0.5, talk_mean_s=1.0, answer_delay_s=(0.1, 0.4), busy_rate=0.03)
CONFIG = DialerConfig(heartbeat_timeout_s=5.0, max_unanswered_s=30.0)


def campaign_ids(n: int) -> list[str]:
    return [f"camp-{i}" for i in range(n)]


def setup(path: str, agents: int, campaigns: int, borrowers_per_campaign: int, mode: DialMode) -> None:
    store = Store(path)
    now = time.time()
    for i, cid in enumerate(campaign_ids(campaigns)):
        repo.create_campaign(store, CampaignConfig(cid, mode, retry_after_s=5.0))
        repo.add_agents(store, cid, [f"a{j}" for j in range(agents) if j % campaigns == i], now)
        repo.add_borrowers(store, cid, borrowers_per_campaign, random.Random(i), prefix=f"{cid}-b")
    store.close()


def run_worker(path: str, idx: int, workers: int, seconds: float, barrier, results) -> None:
    store = Store(path)
    clock = RealClock()
    loop = EventLoop(clock)
    provider = MockProvider(FAST, loop, PEOPLE, random.Random(idx))
    router = ProviderRouter([provider], clock)
    campaigns = [r["id"] for r in store.all("SELECT id FROM campaigns ORDER BY id")]
    mine = {c for i, c in enumerate(campaigns) if i % workers == idx}
    worker = DialerWorker(f"lt-{idx}", store, clock, router, config=CONFIG,
                          owns=lambda cid: cid in mine)
    provider.connect(worker.receive_event)
    my_agents = [r["id"] for r in store.all("SELECT id FROM agents ORDER BY id")][idx::workers]

    stats: Counter[str] = Counter()
    tick_ms: list[float] = []
    # Heartbeats written at setup are stale by now (spawning takes seconds on Windows). Refresh
    # ours before anyone's first reconcile, or the first worker to tick logs everyone out.
    worker.agents.heartbeat(my_agents)
    barrier.wait()
    deadline = time.time() + seconds
    next_desk = 0.0
    while time.time() < deadline:
        try:
            stats["timers"] += loop.run_due()
            t0 = time.perf_counter()
            for r in worker.tick():
                stats["decisions"] += 1
                stats["started"] += r.allocation.started
                stats["integrity_conflicts"] += r.allocation.integrity_conflicts
            tick_ms.append((time.perf_counter() - t0) * 1000)
            if time.time() >= next_desk:
                worker.agents.heartbeat(my_agents)
                done = store.all("SELECT id FROM agents WHERE state='WRAP_UP' AND state_since < ?",
                                 (time.time() - 0.2,))
                for row in done:
                    if row["id"] in my_agents:
                        worker.agents.finish_wrap_up(row["id"])
                next_desk = time.time() + 0.25
        except sqlite3.OperationalError as e:        # "database is locked" after busy_timeout
            stats[f"sqlite: {e}"] += 1
        time.sleep(0.001)
    stats["write_txns"] = store.write_txns
    results.put((idx, dict(stats), tick_ms))
    store.close()


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def run(workers: int, agents: int, campaigns: int, seconds: float, mode: DialMode,
        db: str | None = None) -> dict:
    tmp = None
    if db is None:
        tmp = tempfile.TemporaryDirectory()
        db = str(Path(tmp.name) / "loadtest.db")
    setup(db, agents, campaigns, borrowers_per_campaign=int(agents / campaigns * seconds * 6) + 100,
          mode=mode)
    ctx = mp.get_context("spawn")
    barrier, results = ctx.Barrier(workers + 1), ctx.Queue()
    procs = [ctx.Process(target=run_worker, args=(db, i, workers, seconds, barrier, results))
             for i in range(workers)]
    for p in procs:
        p.start()
    barrier.wait()
    t0 = time.time()
    outs = [results.get(timeout=seconds + 120) for _ in procs]
    for p in procs:
        p.join(timeout=60)
    elapsed = time.time() - t0

    store = Store(db)
    stats: Counter[str] = Counter()
    ticks: list[float] = []
    for _, s, t in outs:
        stats.update(s)
        ticks += t
    q = store.one("""SELECT COUNT(*) calls, SUM(connected_at IS NOT NULL) connected,
                            SUM(state='ABANDONED') abandoned FROM calls""")
    events = store.scalar("SELECT COUNT(*) FROM provider_events")
    leaders = store.scalar("SELECT COUNT(DISTINCT leader) FROM campaigns")
    problems = check_invariants(store)
    store.close()
    if tmp is not None:
        tmp.cleanup()
    return dict(workers=workers, agents=agents, campaigns=campaigns, seconds=round(elapsed, 1),
                calls=q["calls"], calls_per_s=q["calls"] / elapsed,
                txns_per_s=stats["write_txns"] / elapsed,
                txns_per_call=stats["write_txns"] / max(q["calls"], 1), connected=q["connected"] or 0,
                abandoned=q["abandoned"] or 0, events_per_s=events / elapsed, leaders=leaders,
                ticks=len(ticks), tick_p50=pct(ticks, 0.5), tick_p95=pct(ticks, 0.95),
                tick_p99=pct(ticks, 0.99), lock_errors=sum(v for k, v in stats.items()
                                                           if k.startswith("sqlite")),
                integrity_conflicts=stats["integrity_conflicts"],
                invariants="OK" if not problems else "; ".join(problems))


HEADER = ("| workers | agents | campaigns | calls/s | webhooks/s | write txns/s (per call) | "
          "connected | abandoned | tick p50 / p95 / p99 ms | lock errors | invariants |\n"
          "|---|---|---|---|---|---|---|---|---|---|---|")


def fmt(r: dict) -> str:
    return (f"| {r['workers']} | {r['agents']} | {r['campaigns']} | {r['calls_per_s']:.0f} | "
            f"{r['events_per_s']:.0f} | {r['txns_per_s']:.0f} ({r['txns_per_call']:.1f}) | "
            f"{r['connected']} | {r['abandoned']} | "
            f"{r['tick_p50']:.1f} / {r['tick_p95']:.1f} / {r['tick_p99']:.1f} | "
            f"{r['lock_errors']} | {r['invariants']} |")


def scale_bench() -> None:
    """Cost of one pacing round's reads (snapshot) and of one reservation, vs campaign size."""
    print("| agents in campaign | borrowers | snapshot ms | reserve+insert txn ms |\n|---|---|---|---|")
    for agents in (100, 1_000, 10_000, 50_000):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "scale.db")
            borrowers = agents * 20
            setup(path, agents, 1, borrowers, DialMode.PREDICTIVE)
            store = Store(path)
            now = time.time()
            # half the agents on calls, and an hour of call history (10 calls per agent)
            with store.tx() as c:
                c.execute("UPDATE agents SET state='CONNECTED' WHERE rowid % 2 = 0")
                c.executemany(
                    """INSERT INTO calls (id, campaign_id, borrower_id, kind, state, provider,
                           created_at, updated_at, answered_at, ended_at, end_reason)
                       VALUES (?, 'camp-0', ?, 'SPECULATIVE', 'COMPLETED', 'LT', ?, ?, ?, ?, 'NORMAL')""",
                    [(f"h{i}", f"camp-0-b{i}", now - i % 3600, now - i % 3600, now - i % 3600 + 9,
                      now - i % 3600 + 60) for i in range(agents * 10)])
            snap = SnapshotBuilder(store, CONFIG)
            t0 = time.perf_counter()
            for _ in range(20):
                snap.build("camp-0", now)
            snap_ms = (time.perf_counter() - t0) / 20 * 1000
            t0 = time.perf_counter()
            for i in range(50):
                with store.tx() as c:
                    repo.reserve_available_agent(c, "camp-0", f"x{i}", now, 0)
                    repo.claim_borrower(c, "camp-0", f"x{i}", now)
            res_ms = (time.perf_counter() - t0) / 50 * 1000
            print(f"| {agents:,} | {borrowers:,} | {snap_ms:.1f} | {res_ms:.2f} |", flush=True)
            store.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--agents", type=int, default=400)
    ap.add_argument("--campaigns", type=int, default=0, help="default: one per worker")
    ap.add_argument("--seconds", type=float, default=15)
    ap.add_argument("--mode", choices=[m.value for m in DialMode], default="PROGRESSIVE")
    ap.add_argument("--sweep", action="store_true", help="run with 1, 2, 4 and 8 workers")
    ap.add_argument("--scale", action="store_true", help="per-tick cost vs campaign size")
    args = ap.parse_args()
    if args.scale:
        scale_bench()
        return
    print(HEADER, flush=True)
    for w in ((1, 2, 4, 8) if args.sweep else (args.workers,)):
        r = run(w, args.agents, args.campaigns or max(w, 1), args.seconds, DialMode(args.mode))
        print(fmt(r), flush=True)


if __name__ == "__main__":
    main()
