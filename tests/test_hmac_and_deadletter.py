"""Signature verification and the poison/dead-letter path."""
from __future__ import annotations

import json

from webhook_idempotency.engine import Engine, sign
from webhook_idempotency.fixtures import make_events, signed

SECRET = "whsec_test"


def test_valid_signature_is_accepted(tmp_path):
    [ev] = make_events(1)
    eng = Engine(str(tmp_path / "a.db"), secret=SECRET)
    body, sig = signed(SECRET, ev)
    res = eng.receive(body, sig)
    assert res.accepted and not res.dead_lettered
    assert eng.reconcile().dead_letters == 0
    eng.close()


def test_forged_signature_is_dead_lettered_not_processed(tmp_path):
    [ev] = make_events(1)
    eng = Engine(str(tmp_path / "b.db"), secret=SECRET)
    body, _ = signed(SECRET, ev)
    res = eng.receive(body, "deadbeef")          # attacker-supplied signature
    assert not res.accepted and res.dead_lettered
    eng.run_worker()
    assert eng.reconcile().ledger_rows == 0      # nothing applied
    assert eng.reconcile().dead_letters == 1
    eng.close()


def test_wrong_secret_does_not_verify(tmp_path):
    [ev] = make_events(1)
    eng = Engine(str(tmp_path / "c.db"), secret=SECRET)
    body = ev.body()
    res = eng.receive(body, sign("whsec_attacker", body))
    assert res.dead_lettered
    eng.close()


def test_malformed_payload_is_dead_lettered(tmp_path):
    eng = Engine(str(tmp_path / "d.db"), secret=SECRET)
    body = b'{"id": "evt_x", "type":'             # truncated JSON
    res = eng.receive(body, sign(SECRET, body))   # correctly signed but poison
    assert res.dead_lettered and res.reason == "malformed"
    assert eng.reconcile().ledger_rows == 0
    eng.close()


def test_replay_dead_letters_after_fix(tmp_path):
    """A payload that was poison only because of a transient bug can be replayed."""
    eng = Engine(str(tmp_path / "e.db"), secret=SECRET)
    [ev] = make_events(1)
    good = ev.body()
    # Simulate a row that landed in the DLQ as 'malformed' but is actually valid.
    eng.conn.execute(
        "INSERT INTO dead_letter(event_id, reason, payload, at) VALUES (?,?,?,?)",
        (ev.event_id, "malformed:test", good.decode(), 0.0),
    )
    replayed = eng.replay_dead_letters()
    eng.run_worker()
    assert replayed == 1
    assert eng.ledger_total_for(ev.order_id) == ev.amount_cents
    eng.close()
