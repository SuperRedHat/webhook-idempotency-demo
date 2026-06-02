"""A worker crash between the effect-commit and the mark-done-commit must
leave zero lost and zero duplicated effects after restart.
"""
from __future__ import annotations

from webhook_idempotency.engine import Engine
from webhook_idempotency.fixtures import chaotic_deliveries, make_events, signed

SECRET = "whsec_test"


def _ingest(eng, events):
    for ev in chaotic_deliveries(events, duplication=2.0):
        body, sig = signed(SECRET, ev)
        eng.receive(body, sig)


def test_crash_before_mark_resumes_exactly_once(tmp_path):
    db = str(tmp_path / "crash.db")
    events = make_events(80)
    victim = events[40]

    eng = Engine(db, secret=SECRET)
    _ingest(eng, events)

    hit = {"done": False}

    def crash_once(event_id):
        if event_id == victim.event_id and not hit["done"]:
            hit["done"] = True
            return True
        return False

    # First worker dies mid-stream after committing the victim's effect.
    try:
        eng.run_worker(crash_before_mark=crash_once)
    except Exception:
        pass
    # The victim's effect is durable but its outbox row is still pending.
    assert eng.ledger_total_for(victim.order_id) == victim.amount_cents
    pending_before = eng.reconcile().outbox_pending
    assert pending_before >= 1
    eng.close()

    # A fresh process resumes from the durable outbox.
    eng2 = Engine(db, secret=SECRET)
    eng2.run_worker()
    rec = eng2.reconcile()

    assert rec.balanced
    assert rec.outbox_pending == 0
    # Victim credited exactly once despite the crash + retry.
    assert eng2.ledger_total_for(victim.order_id) == victim.amount_cents
    assert rec.ledger_rows == len(events)
    eng2.close()


def test_redelivery_after_crash_still_once(tmp_path):
    db = str(tmp_path / "crash2.db")
    events = make_events(10)
    victim = events[5]
    eng = Engine(db, secret=SECRET)
    _ingest(eng, events)

    def crash_once(event_id):
        return event_id == victim.event_id and eng.reconcile().outbox_pending > 0

    try:
        eng.run_worker(crash_before_mark=lambda e: e == victim.event_id)
    except Exception:
        pass
    eng.close()

    # The provider redelivers the victim again after our crash...
    eng2 = Engine(db, secret=SECRET)
    body, sig = signed(SECRET, victim)
    eng2.receive(body, sig)          # dedup drops it at the inbox
    eng2.run_worker()
    assert eng2.ledger_total_for(victim.order_id) == victim.amount_cents
    assert eng2.reconcile().balanced
    eng2.close()
