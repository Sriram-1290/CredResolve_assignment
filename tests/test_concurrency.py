"""Two (or twenty) workers see the same AVAILABLE agent at the same moment."""
import multiprocessing as mp
import sqlite3
import threading

import pytest
from helpers import CAMPAIGN, World

from loadtest.race import race_reserve
from smartdialer import repo
from smartdialer.db import Store
from smartdialer.invariants import check_invariants


def _file_world(tmp_path, agents: int) -> tuple[World, str]:
    path = str(tmp_path / "dialer.db")
    return World(agents=agents, borrowers=200, store=Store(path)), path


def _thread_race(path: str, workers: int, attempts: int) -> list[str]:
    barrier = threading.Barrier(workers)
    won: list[str] = []
    lock = threading.Lock()

    def run(i: int) -> None:
        store = Store(path)                             # own connection, like a separate worker
        barrier.wait()
        for k in range(attempts):
            with store.tx() as c:
                agent = repo.reserve_available_agent(c, CAMPAIGN, f"t{i}-{k}", 1e12, 0)
            if agent is not None:
                with lock:
                    won.append(agent)
        store.close()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return won


def test_twenty_threads_one_agent_exactly_one_reservation(tmp_path):
    _, path = _file_world(tmp_path, agents=1)
    assert _thread_race(path, workers=20, attempts=1) == ["a0"]


def test_many_threads_many_agents_each_agent_reserved_once(tmp_path):
    _, path = _file_world(tmp_path, agents=40)
    won = _thread_race(path, workers=8, attempts=10)
    assert sorted(won) == sorted(f"a{i}" for i in range(40))


def test_separate_processes_race_for_the_same_agents(tmp_path):
    w, path = _file_world(tmp_path, agents=10)
    ctx = mp.get_context("spawn")
    n = 4
    barrier, results = ctx.Barrier(n), ctx.Queue()
    procs = [ctx.Process(target=race_reserve, args=(path, CAMPAIGN, f"p{i}", barrier, results, 5))
             for i in range(n)]
    for p in procs:
        p.start()
    won = [a for _ in procs for a in results.get(timeout=60)[1]]
    for p in procs:
        p.join(timeout=60)
    assert sorted(won) == sorted(f"a{i}" for i in range(10))     # each agent won exactly once
    assert w.store.scalar("SELECT COUNT(*) FROM agents WHERE state='RESERVED'") == 10


def test_database_refuses_a_second_live_call_for_an_agent_or_borrower():
    w = World(agents=1)
    w.worker.tick()
    (call,) = w.calls()
    insert = ("INSERT INTO calls (id, campaign_id, borrower_id, agent_id, kind, state, provider, "
              "created_at, updated_at) VALUES (?,?,?,?, 'BOUND', 'RESERVED', 'F', 0, 0)")
    with pytest.raises(sqlite3.IntegrityError):
        w.store.conn().execute(insert, ("x1", CAMPAIGN, "other-borrower", call["agent_id"]))
    with pytest.raises(sqlite3.IntegrityError):
        w.store.conn().execute(insert, ("x2", CAMPAIGN, call["borrower_id"], None))


def test_stale_pacing_leader_is_fenced_off():
    """Worker 1 pauses (GC, network) past its lease; worker 2 takes over. Worker 1 wakes up and
    tries to use the permit it got earlier: refused, because the epoch moved on."""
    w = World(agents=5)
    with w.store.tx() as c:
        old_epoch = repo.acquire_leadership(c, CAMPAIGN, "w1", w.clock.now(),
                                            w.config.leader_lease_s)["epoch"]
    w.advance(w.config.leader_lease_s + 1)
    w.heartbeat_all()
    with w.store.tx() as c:
        new_epoch = repo.acquire_leadership(c, CAMPAIGN, "w2", w.clock.now(),
                                            w.config.leader_lease_s)["epoch"]
    assert new_epoch == old_epoch + 1
    still_in_time = w.permit(bound=5, epoch=old_epoch)   # unexpired, but from the old term
    report = w.worker.allocator.execute(still_in_time)
    assert report.stale_permit and report.reserved == 0
    assert w.calls() == []
    assert w.worker.allocator.execute(w.permit(bound=5, epoch=new_epoch)).reserved == 5


def test_two_allocators_cannot_jointly_exceed_the_agentless_ceiling():
    """Ceilings are absolute levels checked inside the insert transaction, not deltas, so two
    permits computed from the same snapshot cannot add up to twice the risk."""
    w = World(agents=2, borrowers=50)
    p = w.permit(speculative=6, ceiling=6)
    w.worker.allocator.execute(p)
    w.new_worker("w2").allocator.execute(p)
    assert len(w.calls("agent_id IS NULL")) == 6
    assert check_invariants(w.store) == []


def test_campaign_owner_gets_first_claim_on_a_never_led_campaign():
    w = World(agents=2)
    outsider = w.new_worker("outsider")
    outsider._owns = lambda cid: False
    assert outsider._pace(CAMPAIGN) is None               # owner hasn't had its chance yet
    w.advance(2 * w.config.leader_lease_s + 1)
    w.heartbeat_all()
    assert outsider._pace(CAMPAIGN) is not None           # nobody claimed it: take it over


def test_mock_provider_ids_are_unique_across_instances():
    import random
    from smartdialer.clock import EventLoop, SimClock
    from smartdialer.providers.mock import PROVIDER_A, BorrowerModel, MockProvider
    loop = EventLoop(SimClock())
    a, b = (MockProvider(PROVIDER_A, loop, BorrowerModel(), random.Random(i)) for i in (1, 2))
    assert a.start_call("x", "+91") != b.start_call("y", "+91")
