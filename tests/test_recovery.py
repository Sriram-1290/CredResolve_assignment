"""Worker crashes, ambiguous provider responses, lost webhooks and provider outages."""
import pytest
from helpers import EventType, World

from smartdialer.faults import Faults, SimulatedCrash
from smartdialer.invariants import check_invariants
from smartdialer.providers.base import EndReason, ProviderCallStatus, ProviderRejected
from smartdialer.safety import SafetyAction


def _crash_tick(w: World, point: str) -> None:
    faults = Faults()
    doomed = w.new_worker("w-doomed", faults)
    faults.arm(point)
    with pytest.raises(SimulatedCrash):
        doomed.tick()


def _restart(w: World, after_s: float):
    w.advance(after_s)
    w.heartbeat_all()
    return w.new_worker("w-restarted")


def test_crash_after_call_initiated_system_adopts_the_call_when_it_comes_back():
    """Agent reserved > borrower reserved > call initiated > worker crashes."""
    w = World(agents=1)
    _crash_tick(w, "after_start_call")
    (call,) = w.calls()
    assert call["state"] == "RESERVED"                 # our record never saw the start succeed
    assert w.agent("a0")["state"] == "RESERVED"
    assert w.borrower(call["borrower_id"])["status"] == "IN_PROGRESS"
    assert call["id"] in w.provider.placed             # ... but the provider has the call

    back = _restart(w, w.config.start_lease_s + 1)
    back.tick()
    cur = w.call(call["id"])
    assert (cur["state"], cur["anomaly"]) == ("INITIATED", "ADOPTED")
    assert w.agent("a0")["state"] == "DIALING"
    assert w.provider.ops("start") == [call["id"]]     # never dialed twice
    back.receive_event(w.ev(call["id"], EventType.ANSWERED))
    back.receive_event(w.ev(call["id"], EventType.COMPLETED, EndReason.NORMAL))
    assert w.call(call["id"])["state"] == "COMPLETED"
    assert check_invariants(w.store) == []


def test_events_arriving_while_worker_is_dead_are_applied_by_the_survivor():
    w = World(agents=1)
    _crash_tick(w, "after_start_call")
    (call,) = w.calls()
    survivor = w.new_worker("w2")
    survivor.receive_event(w.ev(call["id"], EventType.RINGING))      # overtakes the adoption
    survivor.receive_event(w.ev(call["id"], EventType.ANSWERED))
    cur = w.call(call["id"])
    assert (cur["state"], cur["lease_until"]) == ("CONNECTED", None)
    assert w.agent("a0")["state"] == "CONNECTED"
    assert check_invariants(w.store) == []


def test_ringing_webhook_confirms_a_start_whose_worker_died():
    w = World(agents=1)
    _crash_tick(w, "after_start_call")
    (call,) = w.calls()
    w.new_worker("w2").receive_event(w.ev(call["id"], EventType.RINGING))
    cur = w.call(call["id"])
    assert (cur["state"], cur["owner"], cur["lease_until"]) == ("RINGING", None, None)
    assert w.agent("a0")["state"] == "DIALING"
    assert check_invariants(w.store) == []


def test_crash_before_start_call_reservation_is_released_and_id_tombstoned():
    w = World(agents=1)
    _crash_tick(w, "before_start_call")
    (call,) = w.calls()
    assert w.provider.placed == {}
    back = _restart(w, w.config.start_lease_s + 1)
    back.reconciler.run_once()
    cur = w.call(call["id"])
    assert (cur["state"], cur["end_reason"]) == ("CANCELLED", "START_NOT_CONFIRMED")
    assert w.agent("a0")["state"] == "AVAILABLE"
    assert w.borrower(call["borrower_id"])["status"] == "PENDING"
    with pytest.raises(ProviderRejected):               # a delayed start can't resurrect it
        w.provider.start_call(call["id"], "+91")


def test_ambiguous_start_timeout_where_call_was_placed_is_adopted():
    w = World(agents=1)
    w.provider.start_mode = "timeout_placed"
    w.worker.tick()
    (call,) = w.calls()
    assert (call["state"], call["anomaly"]) == ("RESERVED", "START_TIMEOUT")
    w.provider.start_mode = None
    w.advance(w.config.start_recheck_s + 1)
    w.heartbeat_all()
    w.worker.tick()
    assert w.call(call["id"])["state"] == "INITIATED"
    assert w.agent("a0")["state"] == "DIALING"


def test_ambiguous_start_timeout_where_call_was_not_placed_is_cancelled():
    w = World(agents=1)
    w.provider.start_mode = "timeout"
    w.worker.tick()
    (call,) = w.calls()
    w.provider.start_mode = None
    w.advance(w.config.start_recheck_s + 1)
    w.heartbeat_all()
    w.worker.reconciler.run_once()
    assert w.call(call["id"])["state"] == "CANCELLED"
    assert w.agent("a0")["state"] == "AVAILABLE"


def test_lost_webhooks_are_recovered_by_asking_the_provider():
    w = World(agents=1)
    w.worker.tick()
    (call,) = w.calls()
    w.provider.status[call["id"]] = ProviderCallStatus(
        w.provider.placed[call["id"]], answered=True, ended=True,
        end_type=EventType.COMPLETED, end_reason=EndReason.NORMAL)
    w.advance(w.config.max_unanswered_s + 1)
    w.heartbeat_all()
    w.worker.reconciler.run_once()
    cur = w.call(call["id"])
    assert cur["state"] == "COMPLETED" and cur["connected_at"] is not None
    assert w.agent("a0")["state"] == "WRAP_UP"


def test_provider_outage_holds_existing_calls_stops_new_ones_and_recovers():
    w = World(agents=4, borrowers=50)
    w.worker.tick()
    first_wave = [c["id"] for c in w.calls()]
    assert len(first_wave) == 4
    # One call finishes so an agent is free, then the provider goes dark.
    w.worker.receive_event(w.ev(first_wave[0], EventType.FAILED, EndReason.NO_ANSWER))
    w.provider.outage = True
    actions = []
    for _ in range(90):
        w.advance(1)
        w.heartbeat_all()
        actions += [r.decision.action for r in w.worker.tick()]
    # New calls: start_call timeouts trip the breaker; afterwards safety rejects outright.
    assert SafetyAction.REJECT in actions
    assert len(w.calls()) <= 4 + 6            # a handful of probes, never a flood
    # Existing calls: still live, agents still held. We cannot know if the borrower answered.
    for cid in first_wave[1:]:
        assert w.call(cid)["state"] == "INITIATED"
    assert sum(s == "DIALING" for s in w.agent_states().values()) == 3
    assert check_invariants(w.store) == []

    # Recovery: provider reports the held calls as never answered.
    w.provider.outage = False
    for cid, pcid in w.provider.placed.items():
        w.provider.status[cid] = ProviderCallStatus(pcid, answered=False, ended=True,
                                                    end_type=EventType.FAILED,
                                                    end_reason=EndReason.NO_ANSWER)
    for _ in range(40):
        w.advance(1)
        w.heartbeat_all()
        w.worker.tick()
    for cid in first_wave[1:]:
        assert w.call(cid)["state"] == "FAILED"
    assert w.store.scalar("SELECT COUNT(*) FROM calls WHERE state='INITIATED' "
                          "AND created_at > ?", (w.clock.now() - 40,)) > 0   # dialing again
    assert check_invariants(w.store) == []


def test_unresolvable_call_is_eventually_closed_for_review():
    w = World(agents=1)
    w.worker.tick()
    (call,) = w.calls()
    w.provider.outage = True
    for _ in range(int(w.config.max_unresolved_s) // 30 + 2):
        w.advance(30)
        w.heartbeat_all()
        w.worker.reconciler.run_once()
    cur = w.call(call["id"])
    assert (cur["state"], cur["end_reason"], cur["anomaly"]) == ("FAILED", "UNRESOLVED", "NEEDS_REVIEW")
    assert w.agent("a0")["state"] == "AVAILABLE"


def test_a_failed_command_can_be_enqueued_again_after_recovery():
    from smartdialer import repo
    w = World(agents=1)
    w.worker.tick()
    (call,) = w.calls()
    with w.store.tx() as c:
        repo.enqueue_command(c, call["id"], "CANCEL", w.clock.now())
        c.execute("UPDATE commands SET status='FAILED', attempts=8")
        repo.enqueue_command(c, call["id"], "CANCEL", w.clock.now())
    assert w.store.one("SELECT status FROM commands")["status"] == "PENDING"


def test_our_own_cancel_before_start_is_not_counted_as_a_provider_failure():
    w = World(agents=1)
    p = w.permit(bound=1)
    original = w.provider.start_call

    def cancel_then_start(call_id, phone, agent_id=None):
        w.worker.agents.logout(agent_id)                 # agent leaves; our CANCEL lands first
        return original(call_id, phone, agent_id)

    w.provider.start_call = cancel_then_start
    w.worker.allocator.execute(p)
    (call,) = w.calls()
    assert call["state"] == "CANCELLED"
    assert w.borrower(call["borrower_id"])["provider_failures"] == 0
    assert w.worker.router.breaker(w.provider.name).state() == "HEALTHY"


def test_call_cancelled_between_reservation_and_dial_is_never_dialed():
    w = World(agents=1)
    original = w.worker.allocator._faults.check

    def agent_leaves_before_dial(point):
        if point == "before_start_call":
            w.worker.agents.logout("a0")
        original(point)

    w.worker.allocator._faults.check = agent_leaves_before_dial
    w.worker.allocator.execute(w.permit(bound=1))
    (call,) = w.calls()
    assert (call["state"], call["end_reason"]) == ("CANCELLED", "CANCELLED_BEFORE_START")
    assert w.provider.ops("start") == []
    assert check_invariants(w.store) == []
