from helpers import CAMPAIGN, EventType, World

from smartdialer.invariants import check_invariants
from smartdialer.providers.base import EndReason


def test_50_available_agents_never_produce_more_than_50_calls():
    w = World(agents=50, borrowers=500)
    for _ in range(5):                       # repeated ticks must not add calls for busy agents
        w.worker.tick()
        w.advance(1)
        w.heartbeat_all()
    calls = w.calls()
    assert len(calls) == 50
    assert all(c["kind"] == "BOUND" and c["agent_id"] for c in calls)
    assert len({c["agent_id"] for c in calls}) == 50
    assert len({c["borrower_id"] for c in calls}) == 50
    assert set(w.agent_states().values()) == {"DIALING"}
    assert check_invariants(w.store) == []


def test_highest_priority_borrower_and_longest_idle_agent_first():
    w = World(agents=2, borrowers=10)
    w.store.conn().execute("UPDATE agents SET state_since=state_since-100 WHERE id='a1'")
    top = w.store.one("SELECT id FROM borrowers ORDER BY priority DESC LIMIT 1")["id"]
    w.worker.allocator.execute(w.permit(bound=1))
    call = w.calls()[0]
    assert call["agent_id"] == "a1"
    assert call["borrower_id"] == top


def test_no_answer_releases_agent_and_schedules_retry():
    w = World(agents=1)
    w.worker.tick()
    call = w.calls()[0]
    w.worker.receive_event(w.ev(call["id"], EventType.RINGING))
    w.worker.receive_event(w.ev(call["id"], EventType.FAILED, EndReason.NO_ANSWER))
    assert w.call(call["id"])["state"] == "FAILED"
    assert w.agent("a0")["state"] == "AVAILABLE"
    b = w.borrower(call["borrower_id"])
    assert (b["status"], b["attempts"]) == ("PENDING", 1)
    assert b["next_attempt_at"] > w.clock.now()


def test_borrower_exhausted_after_max_attempts():
    w = World(agents=1, borrowers=1)                 # campaign max_attempts=2
    for _ in range(2):
        w.store.conn().execute("UPDATE borrowers SET next_attempt_at=0")
        w.worker.tick()
        call = w.calls("state='INITIATED'")[0]
        w.worker.receive_event(w.ev(call["id"], EventType.FAILED, EndReason.BUSY))
    assert w.borrower("b0")["status"] == "EXHAUSTED"


def test_provider_rejection_releases_agent_without_charging_borrower_attempt():
    w = World(agents=1)
    w.provider.start_mode = "reject"
    w.worker.tick()
    call = w.calls()[0]
    assert (call["state"], call["end_reason"]) == ("FAILED", "PROVIDER_REJECTED")
    assert w.agent("a0")["state"] == "AVAILABLE"
    b = w.borrower(call["borrower_id"])
    assert (b["attempts"], b["provider_failures"], b["status"]) == (0, 1, "PENDING")


def test_paused_agent_is_never_reserved():
    w = World(agents=2)
    w.worker.agents.pause("a0")
    w.worker.tick()
    assert [c["agent_id"] for c in w.calls()] == ["a1"]


def test_agent_disappears_during_setup_call_is_cancelled():
    w = World(agents=1)
    w.worker.tick()
    call = w.calls()[0]
    assert w.agent("a0")["state"] == "DIALING"
    w.worker.agents.logout("a0")
    assert w.agent("a0")["state"] == "OFFLINE"
    cur = w.call(call["id"])
    assert cur["agent_id"] is None and cur["cancel_requested_at"] is not None
    assert w.provider.ops("cancel") == [call["id"]]
    w.worker.receive_event(w.ev(call["id"], EventType.FAILED, EndReason.CANCELLED))
    assert w.call(call["id"])["state"] == "CANCELLED"
    b = w.borrower(call["borrower_id"])
    assert (b["status"], b["attempts"]) == ("PENDING", 0)
    assert check_invariants(w.store) == []


def test_agent_disappears_but_borrower_answers_first_another_agent_takes_it():
    w = World(agents=2)
    w.worker.allocator.execute(w.permit(bound=1))
    call = w.calls()[0]
    lost = call["agent_id"]
    other = "a1" if lost == "a0" else "a0"
    w.worker.agents.logout(lost)
    w.worker.receive_event(w.ev(call["id"], EventType.ANSWERED))   # beat the cancel
    cur = w.call(call["id"])
    assert (cur["state"], cur["agent_id"]) == ("CONNECTED", other)
    assert w.provider.ops(f"bridge:{other}") == [call["id"]]
    assert check_invariants(w.store) == []


def test_agent_disappears_borrower_answers_and_nobody_is_free_call_is_abandoned_cleanly():
    w = World(agents=1)
    w.worker.tick()
    call = w.calls()[0]
    w.worker.agents.logout("a0")
    assert w.worker.receive_event(w.ev(call["id"], EventType.ANSWERED)) == "ABANDONED"
    cur = w.call(call["id"])
    assert (cur["state"], cur["end_reason"]) == ("ABANDONED", "NO_AGENT")
    assert w.provider.ops("hangup") == [call["id"]]
    assert check_invariants(w.store) == []


def test_silent_agent_is_logged_out_by_heartbeat_timeout():
    w = World(agents=2)
    w.advance(w.config.heartbeat_timeout_s + 1)
    w.worker.agents.heartbeat(["a1"])
    w.worker.tick()
    assert w.agent("a0")["state"] == "OFFLINE"
    assert [c["agent_id"] for c in w.calls()] == ["a1"]


def test_full_happy_path_connect_complete_wrap_up():
    w = World(agents=1)
    w.worker.tick()
    cid = w.calls()[0]["id"]
    for etype in (EventType.RINGING, EventType.ANSWERED):
        w.worker.receive_event(w.ev(cid, etype))
    assert (w.call(cid)["state"], w.agent("a0")["state"]) == ("CONNECTED", "CONNECTED")
    w.worker.receive_event(w.ev(cid, EventType.COMPLETED, EndReason.NORMAL))
    assert (w.call(cid)["state"], w.agent("a0")["state"]) == ("COMPLETED", "WRAP_UP")
    assert w.borrower(w.call(cid)["borrower_id"])["status"] == "DONE"
    w.worker.agents.finish_wrap_up("a0")
    assert w.agent("a0")["state"] == "AVAILABLE"
    assert w.store.scalar("SELECT COUNT(*) FROM calls WHERE campaign_id=?", (CAMPAIGN,)) == 1
