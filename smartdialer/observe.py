"""Builds the PacingSnapshot for a campaign from the database (the only source of truth).

Agents whose heartbeat is stale are not counted as available even before the reconciler marks
them OFFLINE, so the pacer and the safety controller react to a disappearing agent on the very
next tick.
"""
from __future__ import annotations

from .config import DialerConfig
from .db import Store
from .pacing.model import PacingSnapshot
from .states import UNANSWERED_CALL_STATES, sql_in

_UNANSWERED = sql_in(UNANSWERED_CALL_STATES)


class SnapshotBuilder:
    def __init__(self, store: Store, config: DialerConfig, *, long_window_s: float = 300.0,
                 short_window_s: float = 60.0, abandon_window_s: float = 1800.0,
                 borrower_scan_cap: int = 1000, late_event_s: float = 3.0,
                 cohort_s: float = 30.0, cohort_min_age_s: float = 4.0,
                 outcome_known_s: float = 40.0) -> None:
        self._store = store
        self._outcome_known_s = outcome_known_s     # setup + ring timeout: outcome surely known
        self._late_event_s = late_event_s
        self._cohort_s = cohort_s
        self._cohort_min_age_s = cohort_min_age_s   # too fresh to have been answered yet
        self._cfg = config
        self._long = long_window_s
        self._short = short_window_s
        self._abandon = abandon_window_s
        self._borrower_cap = borrower_scan_cap

    def build(self, campaign_id: str, now: float) -> PacingSnapshot:
        # One read transaction, so agent counts, calls in flight and rates come from the same
        # instant rather than from whatever other workers committed between the queries.
        with self._store.read():
            return self._build(campaign_id, now)

    def _build(self, campaign_id: str, now: float) -> PacingSnapshot:
        st = self._store
        fresh_after = now - self._cfg.heartbeat_timeout_s
        agents = {r[0]: r[1] for r in st.all(
            """SELECT state, COUNT(*) FROM agents
               WHERE campaign_id=? AND state != 'OFFLINE' AND heartbeat_at >= ?
               GROUP BY state""", (campaign_id, fresh_after))}

        # Un-answered calls older than the longest possible ring can no longer *produce* an
        # answer, so they stop counting as risk (they are still held and reconciled).
        flight = st.one(
            f"""SELECT COALESCE(SUM(agent_id IS NOT NULL), 0) AS bound,
                       COALESCE(SUM(agent_id IS NULL), 0) AS unbound,
                       COALESCE(SUM(agent_id IS NULL AND cancel_requested_at IS NOT NULL), 0)
                           AS cancelling
                FROM calls WHERE campaign_id=? AND state IN {_UNANSWERED}
                  AND created_at >= ?""",
            (campaign_id, now - self._cfg.max_unanswered_s))

        # Answer rate by *dial cohort*, only over cohorts old enough that every outcome is known.
        # Counting recently *resolved* attempts instead is biased: answers resolve in seconds,
        # no-answers only at the ring timeout, so a rising dial rate inflates the answer rate.
        mature = now - self._outcome_known_s
        long_t, short_t = mature - self._long, mature - self._short
        rate = st.one(
            """SELECT COUNT(*) AS n, COALESCE(SUM(answered_at IS NOT NULL), 0) AS answered,
                      COALESCE(SUM(created_at >= ?), 0) AS n_short,
                      COALESCE(SUM(created_at >= ? AND answered_at IS NOT NULL), 0) AS answered_short
               FROM calls WHERE campaign_id=? AND created_at >= ? AND created_at < ?
                 AND (answered_at IS NOT NULL
                      OR (state='FAILED' AND end_reason IN ('NO_ANSWER', 'BUSY')))""",
            (short_t, short_t, campaign_id, long_t, mature))
        dial_to_answer = st.scalar(
            """SELECT AVG(answered_at - created_at) FROM calls
               WHERE campaign_id=? AND answered_at >= ?""", (campaign_id, now - self._long))
        talk = st.scalar(
            """SELECT AVG(ended_at - connected_at) FROM calls
               WHERE campaign_id=? AND ended_at >= ? AND state='COMPLETED'
                 AND connected_at IS NOT NULL""",
            (campaign_id, now - 2 * self._long))

        abandon_t = now - self._abandon
        ab = st.one(
            """SELECT COALESCE(SUM(state='ABANDONED'), 0) AS abandoned,
                      COUNT(*) AS answered
               FROM calls WHERE campaign_id=? AND answered_at >= ?
                 AND state IN ('CONNECTED', 'COMPLETED', 'ABANDONED')""",
            (campaign_id, abandon_t))

        cohort = st.one(
            """SELECT COUNT(*) AS n, COALESCE(SUM(answered_at IS NOT NULL), 0) AS answered
               FROM calls WHERE campaign_id=? AND created_at >= ? AND created_at < ?
                 AND (provider_call_id IS NOT NULL OR answered_at IS NOT NULL)""",
            (campaign_id, now - self._cohort_s, now - self._cohort_min_age_s))

        lag = st.one(
            """SELECT COUNT(*) AS n, COALESCE(SUM(received_at - occurred_at > ?), 0) AS late
               FROM provider_events WHERE received_at >= ?
                 AND event_id NOT LIKE 'reconcile:%'   -- synthesized by us, never late""",
            (self._late_event_s, now - self._long))

        dialable = st.scalar(
            """SELECT COUNT(*) FROM (SELECT 1 FROM borrowers
               WHERE campaign_id=? AND status='PENDING' AND next_attempt_at <= ? LIMIT ?)""",
            (campaign_id, now, self._borrower_cap))

        return PacingSnapshot(
            campaign_id=campaign_id,
            now=now,
            agents_logged_in=sum(agents.values()),
            agents_available=agents.get("AVAILABLE", 0),
            agents_reserved=agents.get("RESERVED", 0) + agents.get("DIALING", 0),
            agents_connected=agents.get("CONNECTED", 0),
            agents_wrap_up=agents.get("WRAP_UP", 0),
            agents_paused=agents.get("PAUSED", 0),
            bound_in_flight=flight["bound"],
            unbound_in_flight=flight["unbound"],
            unbound_cancelling=flight["cancelling"],
            dialable_borrowers=dialable,
            attempts_long=rate["n"],
            answers_long=rate["answered"],
            attempts_short=rate["n_short"],
            answers_short=rate["answered_short"],
            mean_dial_to_answer_s=dial_to_answer,
            mean_talk_s=talk,
            answered_live_window=ab["answered"],
            abandoned_window=ab["abandoned"],
            events_recent=lag["n"],
            events_late=lag["late"],
            cohort_dialed=cohort["n"],
            cohort_answered=cohort["answered"],
        )
