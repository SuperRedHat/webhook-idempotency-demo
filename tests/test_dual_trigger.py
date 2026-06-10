"""Dual-trigger ingestion: a provider webhook AND a polling fallback both
observe the same state transition; the downstream effect must fire once.

This is the classic payment-rail race. An on-ramp confirms a transfer via
webhook, but a polling fallback also discovers the same transfer as
`completed` — because the webhook was slow, or because it arrived and the
poller ran anyway. If the downstream effect (payout / disbursement / order
release) is keyed on the *delivery* that carried the news (webhook event id,
poll batch id), each path fires its own effect: double payout.

The fix demonstrated here: both paths normalize their observation into one
CANONICAL state-transition key — `{txn}:transfer.completed` — before touching
storage. The inbox PRIMARY KEY then absorbs whichever path loses the race,
and the ledger idempotency key absorbs worker retries on top of that. The
poller needs no knowledge of webhook delivery ids; it only needs to name the
provider state it saw, and the name is deterministic.
"""
from __future__ import annotations

from webhook_idempotency.engine import Engine, EventEnvelope
from webhook_idempotency.fixtures import signed

SECRET = "whsec_test"
TXN = "tx_4242"
AMOUNT = 125_00


def observed_completion(txn_id: str, amount_cents: int, observed_at: float) -> EventEnvelope:
    """The canonical 'transfer completed' fact, identical no matter which
    ingestion path observed it.

    event_id derives from the transaction + milestone, NOT from the delivery
    that carried the news — that is the whole trick. `observed_at` may differ
    between paths (each stamps its own clock); dedup must not care.
    """
    return EventEnvelope(
        event_id=f"{txn_id}:transfer.completed",
        type="payment_intent.succeeded",
        order_id=txn_id,
        amount_cents=amount_cents,
        created=observed_at,
    )


def test_webhook_then_poll_disburses_once(tmp_path):
    eng = Engine(str(tmp_path / "dual.db"), secret=SECRET)

    # 1. Webhook arrives first and the worker disburses.
    body, sig = signed(SECRET, observed_completion(TXN, AMOUNT, 1_700_000_000.0))
    assert eng.receive(body, sig).accepted
    assert eng.run_worker() == 1
    assert eng.ledger_total_for(TXN) == AMOUNT

    # 2. The polling fallback later sees the same transfer as completed.
    #    Different observation time -> different payload bytes; same canonical
    #    key -> dropped at the inbox, not at some fragile payload-hash layer.
    body2, sig2 = signed(SECRET, observed_completion(TXN, AMOUNT, 1_700_000_999.0))
    res = eng.receive(body2, sig2)
    assert res.accepted and res.duplicate

    # 3. Nothing new to do; the disbursement happened exactly once.
    assert eng.run_worker() == 0
    assert eng.ledger_total_for(TXN) == AMOUNT
    rec = eng.reconcile()
    assert rec.ledger_rows == 1 and rec.balanced
    eng.close()


def test_poll_then_late_webhook_disburses_once(tmp_path):
    """Same race, opposite winner: the poller beats a slow webhook."""
    eng = Engine(str(tmp_path / "dual2.db"), secret=SECRET)

    body, sig = signed(SECRET, observed_completion(TXN, AMOUNT, 1_700_000_500.0))
    assert eng.receive(body, sig).accepted          # poller's observation
    assert eng.run_worker() == 1

    body2, sig2 = signed(SECRET, observed_completion(TXN, AMOUNT, 1_700_000_000.0))
    res = eng.receive(body2, sig2)                  # webhook limps in late
    assert res.accepted and res.duplicate
    assert eng.run_worker() == 0

    assert eng.ledger_total_for(TXN) == AMOUNT
    assert eng.reconcile().ledger_rows == 1
    eng.close()


def test_poll_retrigger_after_crash_still_once(tmp_path):
    """Worst case: webhook handled, worker crashes after the effect-commit but
    before mark-done, process restarts, AND the poller re-triggers. The ledger
    idempotency key must absorb all of it — one disbursement, books balanced.
    """
    db = str(tmp_path / "dual3.db")
    eng = Engine(db, secret=SECRET)
    body, sig = signed(SECRET, observed_completion(TXN, AMOUNT, 1_700_000_000.0))
    eng.receive(body, sig)
    try:
        eng.run_worker(crash_before_mark=lambda e: True)
    except Exception:
        pass
    # Effect is durable, outbox row still pending — the dangerous window.
    assert eng.ledger_total_for(TXN) == AMOUNT
    assert eng.reconcile().outbox_pending == 1
    eng.close()

    eng2 = Engine(db, secret=SECRET)                # process restart
    body2, sig2 = signed(SECRET, observed_completion(TXN, AMOUNT, 1_700_001_000.0))
    assert eng2.receive(body2, sig2).duplicate      # poll fallback re-triggers
    eng2.run_worker()                               # drains the pending row

    assert eng2.ledger_total_for(TXN) == AMOUNT     # still exactly once
    rec = eng2.reconcile()
    assert rec.balanced and rec.ledger_rows == 1 and rec.outbox_pending == 0
    eng2.close()
