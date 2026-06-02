"""The core invariant: N at-least-once deliveries => exactly 1 effect per event.

If you delete the inbox `INSERT OR IGNORE` dedup OR the ledger idempotency key,
`test_each_event_credited_exactly_once` fails — that is the planted-regression
guard the CI relies on (see README "Tests that have teeth").
"""
from __future__ import annotations

from webhook_idempotency.engine import Engine, naive_credit_total
from webhook_idempotency.fixtures import chaotic_deliveries, make_events, signed

SECRET = "whsec_test"


def _drive(db, events, repeats_stream):
    eng = Engine(db, secret=SECRET)
    for ev in repeats_stream:
        body, sig = signed(SECRET, ev)
        eng.receive(body, sig)
    eng.run_worker()
    return eng


def test_each_event_credited_exactly_once(tmp_path):
    events = make_events(120)
    stream = chaotic_deliveries(events, duplication=3.0)
    assert len(stream) > len(events)  # there really are duplicates/retries

    eng = _drive(str(tmp_path / "x.db"), events, stream)
    rec = eng.reconcile()

    assert rec.unique_events == len(events)
    assert rec.ledger_rows == len(events)        # exactly one effect per event
    assert rec.outbox_done == len(events)
    assert rec.outbox_pending == 0
    assert rec.total_credited_cents == sum(e.amount_cents for e in events)
    assert rec.balanced
    eng.close()


def test_explicit_redelivery_of_same_event_is_idempotent(tmp_path):
    [ev] = make_events(1)
    eng = Engine(str(tmp_path / "y.db"), secret=SECRET)
    body, sig = signed(SECRET, ev)
    for _ in range(50):                          # 50 identical retries
        eng.receive(body, sig)
    eng.run_worker()
    assert eng.ledger_total_for(ev.order_id) == ev.amount_cents
    assert eng.reconcile().ledger_rows == 1
    eng.close()


def test_naive_handler_would_double_credit():
    """Proves the suite has teeth: the wrong handler really does over-credit."""
    events = make_events(120)
    stream = chaotic_deliveries(events, duplication=3.0)
    correct = sum(e.amount_cents for e in events)
    naive = naive_credit_total(stream)
    assert naive > correct                       # duplicates inflate the total
