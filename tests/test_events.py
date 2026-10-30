"""Duplicate, out-of-order and crash-interrupted provider events."""
from helpers import EventType, World

from smartdialer.faults import Faults, SimulatedCrash
from smartdialer.invariants import check_invariants
from smartdialer.providers.base import EndReason


def _dialing(w: World) -> str:
    w.worker.tick()
    return w.calls()[0]["id"]


def _versions(w, cid):
    return w.call(cid)["version"], w.agent("a0")["version"]


def test_same_event_delivered_three_times_applies_once():
    w = World(agents=1)
    cid = _dialing(w)
    ev = w.ev(cid, EventType.ANSWERED, event_id="dup-1")
    assert w.worker.receive_event(ev) == "CONNECTED"
    before = _versions(w, cid)
    assert w.worker.receive_event(ev) == "DUPLICATE"
    assert w.worker.receive_event(ev) == "DUPLICATE"
    assert _versions(w, cid) == before                 # no extra state transitions
    assert len(w.provider.ops("bridge:a0")) == 1
    assert w.store.scalar("SELECT COUNT(*) FROM provider_events") == 1


def test_answered_answered_answered_completed():
    """Re-sent ANSWERED under new ids are stale by rank; COMPLETED still lands."""
    w = World(agents=1)
    cid = _dialing(w)
    outcomes = [w.worker.receive_event(w.ev(cid, EventType.ANSWERED)) for _ in range(3)]
    assert outcomes == ["CONNECTED", "STALE", "STALE"]
    w.worker.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NORMAL))
    call = w.call(cid)
    assert call["state"] == "COMPLETED" and call["connected_at"] is not None
    assert w.agent("a0")["state"] == "WRAP_UP"
    assert w.borrower(call["borrower_id"])["status"] == "DONE"
    assert check_invariants(w.store) == []


def test_completed_answered_ringing_out_of_order():
    w = World(agents=1)
    cid = _dialing(w)
    answered_time = w.clock.now()
    late_answer = w.ev(cid, EventType.ANSWERED)
    late_ringing = w.ev(cid, EventType.RINGING)
    w.advance(30)
    assert w.worker.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NORMAL)) == "APPLIED"
    assert w.worker.receive_event(late_answer) == "STALE"
    assert w.worker.receive_event(late_ringing) == "STALE"
    call = w.call(cid)
    assert call["state"] == "COMPLETED"
    assert call["anomaly"] == "COMPLETED_BEFORE_ANSWERED"
    assert call["answered_at"] == answered_time          # fact kept from the stale event
    assert w.agent("a0")["state"] == "AVAILABLE"         # never connected -> no wrap-up
    assert check_invariants(w.store) == []


def test_borrower_hung_up_before_we_saw_answered_counts_as_abandoned():
    w = World(agents=1)
    cid = _dialing(w)
    w.worker.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NO_AGENT))
    call = w.call(cid)
    assert (call["state"], call["end_reason"]) == ("ABANDONED", "ANSWER_NOT_SEEN")
    assert call["answered_at"] is not None


def test_ringing_after_answered_is_ignored():
    w = World(agents=1)
    cid = _dialing(w)
    w.worker.receive_event(w.ev(cid, EventType.ANSWERED))
    assert w.worker.receive_event(w.ev(cid, EventType.RINGING)) == "STALE"
    assert w.call(cid)["state"] == "CONNECTED"


def test_late_event_for_an_old_call_cannot_move_an_agent_who_moved_on():
    w = World(agents=1, borrowers=5)
    first = _dialing(w)
    w.worker.receive_event(w.ev(first, EventType.FAILED, EndReason.NO_ANSWER))
    w.worker.tick()
    second = w.calls("state='INITIATED'")[0]["id"]
    assert w.agent("a0")["call_id"] == second
    # A re-sent FAILED for the first call (new event id) arrives late.
    assert w.worker.receive_event(w.ev(first, EventType.FAILED, EndReason.NO_ANSWER)) == "STALE"
    assert (w.agent("a0")["state"], w.agent("a0")["call_id"]) == ("DIALING", second)


def test_event_for_unknown_call_is_recorded_not_fatal():
    w = World(agents=1)
    assert w.worker.receive_event(w.ev("nope", EventType.ANSWERED)) == "UNKNOWN_CALL"


def test_worker_crashes_after_receiving_answered_then_completed_arrives():
    """ANSWERED is persisted to the inbox, the worker dies before applying it, then COMPLETED
    arrives at another worker. Final state must be consistent."""
    w = World(agents=1)
    cid = _dialing(w)
    faults = Faults()
    crashing = w.new_worker("w-crash", faults)
    faults.arm("after_event_ingested")
    try:
        crashing.receive_event(w.ev(cid, EventType.ANSWERED))
        raise AssertionError("expected crash")
    except SimulatedCrash:
        pass
    assert w.call(cid)["state"] == "INITIATED"           # nothing applied
    w.advance(w.config.event_lease_s + 1)
    w.heartbeat_all()
    survivor = w.new_worker("w2")
    survivor.tick()                                       # reconciler: process_pending
    assert w.call(cid)["state"] == "CONNECTED"
    assert w.agent("a0")["state"] == "CONNECTED"
    survivor.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NORMAL))
    assert w.call(cid)["state"] == "COMPLETED"
    assert w.agent("a0")["state"] == "WRAP_UP"
    assert check_invariants(w.store) == []


def test_crash_while_applying_answered_rolls_back_and_is_retried():
    w = World(agents=1)
    cid = _dialing(w)
    faults = Faults()
    crashing = w.new_worker("w-crash", faults)
    faults.arm("before_event_commit")
    try:
        crashing.receive_event(w.ev(cid, EventType.ANSWERED))
    except SimulatedCrash:
        pass
    assert w.call(cid)["state"] == "INITIATED" and w.agent("a0")["state"] == "DIALING"
    # COMPLETED arrives before anyone retried the ANSWERED.
    w.worker.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NORMAL))
    w.advance(w.config.event_lease_s + 1)
    w.heartbeat_all()
    w.worker.tick()
    call = w.call(cid)
    assert call["state"] == "COMPLETED"
    assert w.store.scalar("SELECT COUNT(*) FROM provider_events WHERE status='PENDING'") == 0
    assert check_invariants(w.store) == []


def test_crash_after_commit_before_bridge_is_sent_outbox_delivers_it():
    w = World(agents=1, borrowers=5)
    w.worker.allocator.execute(w.permit(speculative=1, ceiling=5))   # agentless call
    cid = w.calls()[0]["id"]
    faults = Faults()
    crashing = w.new_worker("w-crash", faults)
    faults.arm("after_event_commit")
    try:
        crashing.receive_event(w.ev(cid, EventType.ANSWERED))
    except SimulatedCrash:
        pass
    assert w.call(cid)["state"] == "CONNECTED"
    assert w.provider.ops("bridge:a0") == []              # decided, but never sent
    w.worker.tick()                                       # any worker's reconciler
    assert w.provider.ops("bridge:a0") == [cid]


def test_speculative_answer_takes_free_agent_else_abandons():
    w = World(agents=1, borrowers=5)
    w.worker.allocator.execute(w.permit(speculative=2, ceiling=5))
    first, second = (c["id"] for c in w.calls())
    assert w.worker.receive_event(w.ev(first, EventType.ANSWERED)) == "CONNECTED"
    assert w.worker.receive_event(w.ev(second, EventType.ANSWERED)) == "ABANDONED"
    assert w.provider.ops("hangup") == [second]
    assert check_invariants(w.store) == []


def test_speculative_answer_preempts_wrap_up_before_abandoning():
    w = World(agents=1, borrowers=5)
    w.store.conn().execute("UPDATE agents SET state='WRAP_UP'")
    w.worker.allocator.execute(w.permit(speculative=1, ceiling=5))
    cid = w.calls()[0]["id"]
    assert w.worker.receive_event(w.ev(cid, EventType.ANSWERED)) == "CONNECTED"
    assert w.agent("a0")["state"] == "CONNECTED"


def test_cancel_reported_for_a_connected_call_does_not_poison_the_workers():
    """Review fix: FAILED/CANCELLED on a CONNECTED call used to raise IllegalTransition forever."""
    w = World(agents=1)
    cid = _dialing(w)
    w.worker.receive_event(w.ev(cid, EventType.ANSWERED))
    w.worker.receive_event(w.ev(cid, EventType.FAILED, EndReason.CANCELLED))
    assert w.call(cid)["state"] == "FAILED"
    w.advance(w.config.event_lease_s + 1)
    w.heartbeat_all()
    w.worker.tick()                                      # must not raise
    assert check_invariants(w.store) == []


def test_completed_before_answered_does_not_redial_a_borrower_who_was_reached():
    w = World(agents=1)
    cid = _dialing(w)
    w.worker.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NORMAL))
    call = w.call(cid)
    assert call["answered_at"] is not None
    assert w.borrower(call["borrower_id"])["status"] == "DONE"
