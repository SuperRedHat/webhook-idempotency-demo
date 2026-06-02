"""Core exactly-once-EFFECT webhook engine (stdlib only).

Design in one paragraph
-----------------------
We cannot make the *external* side-effect (crediting a ledger / fulfilling an
order) and our own bookkeeping commit atomically — they live in different
systems. So we do NOT chase "exactly-once delivery" (impossible). Instead we
make the *effect* idempotent and reconstruct exactly-once-EFFECT from
at-least-once delivery:

  1. Receive: HMAC-verify, then INSERT-OR-IGNORE the event id into an `inbox`
     (dedup) and, in the SAME transaction, enqueue an `outbox` row. Duplicate
     deliveries are dropped here.
  2. Worker: for each pending outbox row, apply the effect via an idempotency
     key (INSERT-OR-IGNORE into `ledger`), commit, then mark the outbox row
     done. If we crash between those two commits, the row stays `pending`; on
     restart we re-apply, the ledger INSERT-OR-IGNORE no-ops, and we mark done.
     => the effect happens exactly once even though delivery/worker retried.
  3. Reconcile: unique inbox events == ledger rows == outbox done, and the
     credited total equals the sum of unique event amounts.

Everything is sqlite so the whole thing runs with zero credentials and zero
pip installs. A live FastAPI receiver (app.py) shows the same flow over HTTP.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

log = logging.getLogger("webhook_idempotency")


class SignatureError(Exception):
    """Raised / surfaced when an event's HMAC signature does not verify."""


class _Crash(Exception):
    """Internal: injected by tests/chaos to simulate a worker crash."""


def sign(secret: str, body: bytes) -> str:
    """Provider-style HMAC-SHA256 hex signature over the raw request body."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify(secret: str, body: bytes, signature: str) -> bool:
    """Constant-time signature check (mirrors Stripe/Shopify verification)."""
    return hmac.compare_digest(sign(secret, body), signature or "")


@dataclass(frozen=True)
class EventEnvelope:
    """A provider-shaped webhook event (Stripe `payment_intent.succeeded`-like)."""

    event_id: str          # provider event id -> the natural idempotency key
    type: str              # e.g. "payment_intent.succeeded"
    order_id: str          # business key the side-effect is keyed on
    amount_cents: int      # money is integer cents — never float
    created: float = field(default_factory=lambda: 0.0)

    def body(self) -> bytes:
        """Canonical JSON body the provider would have signed (sorted keys)."""
        return json.dumps(
            {
                "id": self.event_id,
                "type": self.type,
                "data": {"order_id": self.order_id, "amount_cents": self.amount_cents},
                "created": self.created,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()


_SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox (
    event_id   TEXT PRIMARY KEY,          -- dedup / idempotency key
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    received_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT NOT NULL,
    effect     TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending',   -- pending | done
    attempts   INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger (
    event_id   TEXT PRIMARY KEY,          -- effect-layer idempotency key
    order_id   TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    applied_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letter (
    event_id   TEXT,
    reason     TEXT NOT NULL,
    payload    TEXT,
    at         REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_status ON outbox(status, id);
"""


@dataclass
class ReceiveResult:
    accepted: bool
    duplicate: bool = False
    dead_lettered: bool = False
    reason: str = ""


@dataclass
class Reconciliation:
    unique_events: int
    ledger_rows: int
    outbox_done: int
    outbox_pending: int
    dead_letters: int
    total_credited_cents: int
    expected_credited_cents: int

    @property
    def balanced(self) -> bool:
        return (
            self.unique_events
            == self.ledger_rows
            == self.outbox_done
            and self.outbox_pending == 0
            and self.total_credited_cents == self.expected_credited_cents
        )

    def as_dict(self) -> dict:
        return {**self.__dict__, "balanced": self.balanced}


class Engine:
    """Idempotent webhook engine over a sqlite file (or :memory:).

    A fresh ``Engine`` pointed at the same ``db_path`` models a process
    restart: durable state (inbox/outbox/ledger) survives, in-flight work
    resumes from the outbox.
    """

    def __init__(self, db_path: str = ":memory:", secret: str = "whsec_demo"):
        self.secret = secret
        # isolation_level=None -> we control BEGIN/COMMIT explicitly so the
        # "effect commit" and the "mark-done commit" are two distinct commits.
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.executescript(_SCHEMA)

    # ----- 1. Receive (HTTP edge) -------------------------------------------
    def receive(self, body: bytes, signature: str) -> ReceiveResult:
        """Verify + dedup + enqueue. Returns fast; the worker does the effect.

        At-least-once safe: replaying the exact same delivery is a no-op.
        """
        if not verify(self.secret, body, signature):
            self._dead_letter(None, "bad_signature", body)
            log.warning("event rejected: bad signature")
            return ReceiveResult(accepted=False, dead_lettered=True, reason="bad_signature")

        try:
            doc = json.loads(body)
            event_id = doc["id"]
            etype = doc["type"]
        except (ValueError, KeyError) as exc:  # malformed / poison payload
            self._dead_letter(None, f"malformed:{exc}", body)
            log.warning("event dead-lettered: malformed payload (%s)", exc)
            return ReceiveResult(accepted=False, dead_lettered=True, reason="malformed")

        cid = uuid.uuid4().hex[:8]  # correlation id for structured logs
        self.conn.execute("BEGIN")
        try:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO inbox(event_id, type, payload, received_at) "
                "VALUES (?,?,?,?)",
                (event_id, etype, body.decode(), time.time()),
            )
            if cur.rowcount == 0:  # already seen -> dedup, ack without re-enqueue
                self.conn.execute("COMMIT")
                log.info("cid=%s event=%s DUPLICATE dropped", cid, event_id)
                return ReceiveResult(accepted=True, duplicate=True)
            # inbox + outbox enqueued atomically (transactional inbox/outbox)
            self.conn.execute(
                "INSERT INTO outbox(event_id, effect, created_at) VALUES (?,?,?)",
                (event_id, "credit_ledger", time.time()),
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        log.info("cid=%s event=%s accepted+enqueued", cid, event_id)
        return ReceiveResult(accepted=True)

    # ----- 2. Worker (async side-effect) ------------------------------------
    def run_worker(
        self,
        max_items: Optional[int] = None,
        crash_before_mark: Optional[Callable[[str], bool]] = None,
    ) -> int:
        """Drain pending outbox rows, applying each effect exactly once.

        ``crash_before_mark`` (test/chaos hook) may raise after the effect is
        committed but before the outbox row is marked done, simulating a worker
        that dies mid-flight. On the next run the row is still ``pending`` and
        is safely re-applied (the ledger idempotency key absorbs the retry).
        """
        processed = 0
        while True:
            row = self.conn.execute(
                "SELECT id, event_id FROM outbox WHERE status='pending' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                break
            outbox_id, event_id = row
            self.conn.execute(
                "UPDATE outbox SET attempts = attempts + 1 WHERE id=?", (outbox_id,)
            )
            ev = self.conn.execute(
                "SELECT payload FROM inbox WHERE event_id=?", (event_id,)
            ).fetchone()
            doc = json.loads(ev[0])
            data = doc["data"]

            # (a) Apply the EXTERNAL effect, idempotently, and commit it.
            self.conn.execute("BEGIN")
            self.conn.execute(
                "INSERT OR IGNORE INTO ledger(event_id, order_id, amount_cents, applied_at) "
                "VALUES (?,?,?,?)",
                (event_id, data["order_id"], int(data["amount_cents"]), time.time()),
            )
            self.conn.execute("COMMIT")

            # (b) Crash window: effect is durable but outbox is still pending.
            if crash_before_mark is not None and crash_before_mark(event_id):
                log.error("cid=worker event=%s CRASH before mark-done", event_id)
                raise _Crash(event_id)

            # (c) Mark the outbox row done (separate commit).
            self.conn.execute("BEGIN")
            self.conn.execute("UPDATE outbox SET status='done' WHERE id=?", (outbox_id,))
            self.conn.execute("COMMIT")

            processed += 1
            if max_items is not None and processed >= max_items:
                break
        return processed

    # ----- 3. Reconcile ------------------------------------------------------
    def reconcile(self) -> Reconciliation:
        q = self.conn.execute
        unique_events = q(
            "SELECT COUNT(*) FROM inbox WHERE type='payment_intent.succeeded'"
        ).fetchone()[0]
        ledger_rows = q("SELECT COUNT(*) FROM ledger").fetchone()[0]
        outbox_done = q("SELECT COUNT(*) FROM outbox WHERE status='done'").fetchone()[0]
        outbox_pending = q("SELECT COUNT(*) FROM outbox WHERE status='pending'").fetchone()[0]
        dead = q("SELECT COUNT(*) FROM dead_letter").fetchone()[0]
        total = q("SELECT COALESCE(SUM(amount_cents),0) FROM ledger").fetchone()[0]
        expected = q(
            "SELECT COALESCE(SUM(amount_cents),0) FROM ("
            "  SELECT json_extract(payload,'$.data.amount_cents') AS amount_cents"
            "  FROM inbox WHERE type='payment_intent.succeeded'"
            ")"
        ).fetchone()[0]
        return Reconciliation(
            unique_events=unique_events,
            ledger_rows=ledger_rows,
            outbox_done=outbox_done,
            outbox_pending=outbox_pending,
            dead_letters=dead,
            total_credited_cents=int(total),
            expected_credited_cents=int(expected or 0),
        )

    # ----- helpers -----------------------------------------------------------
    def _dead_letter(self, event_id, reason: str, payload: bytes) -> None:
        self.conn.execute(
            "INSERT INTO dead_letter(event_id, reason, payload, at) VALUES (?,?,?,?)",
            (event_id, reason, payload.decode(errors="replace"), time.time()),
        )

    def ledger_total_for(self, order_id: str) -> int:
        return self.conn.execute(
            "SELECT COALESCE(SUM(amount_cents),0) FROM ledger WHERE order_id=?",
            (order_id,),
        ).fetchone()[0]

    def replay_dead_letters(self) -> int:
        """Operational hook: re-feed dead-lettered payloads after a fix."""
        rows = self.conn.execute(
            "SELECT rowid, payload FROM dead_letter WHERE reason LIKE 'malformed%'"
        ).fetchall()
        replayed = 0
        for rowid, payload in rows:
            res = self.receive(payload.encode(), sign(self.secret, payload.encode()))
            if res.accepted and not res.dead_lettered:
                self.conn.execute("DELETE FROM dead_letter WHERE rowid=?", (rowid,))
                replayed += 1
        return replayed

    def close(self) -> None:
        self.conn.close()


def naive_credit_total(deliveries: Iterable[EventEnvelope]) -> int:
    """The WRONG handler: credit on every delivery (no dedup, no idempotency).

    Used by the demo to quantify how many duplicate effects the correct path
    suppresses. This is what a 'works in the happy path' competitor ships.
    """
    total = 0
    for ev in deliveries:
        total += ev.amount_cents
    return total
