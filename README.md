# webhook-idempotency-demo — exactly-once *effect* from at-least-once webhooks

[![CI](https://github.com/SuperRedHat/webhook-idempotency-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/SuperRedHat/webhook-idempotency-demo/actions/workflows/ci.yml)

A small, runnable proof that webhook side-effects (charging, fulfilling,
syncing a CRM) happen **exactly once** even though the provider delivers
**at-least-once**, out of order, with retries — and even when the worker
**crashes mid-processing**.

```bash
python demo.py          # ~60s, zero credentials, zero pip installs (stdlib only)
python -m pytest -q     # 10 tests, the invariants below
```

> Core engine is **stdlib only** (`sqlite3`). `pip install` is needed *only* for
> the optional live HTTP receiver (`app.py`) and to run the tests.

---

## The hard part — and how this handles it

Webhooks (Stripe, Shopify, HubSpot, Twilio…) are **at-least-once**: the same
event is redelivered on timeout, on retry, and on replay. A handler that just
"does the thing" on each POST **double-charges, double-fulfills, or double-syncs**.
And because the side-effect lives in *another* system, you cannot commit "the
effect happened" and "I recorded that it happened" atomically — so a crash in
between either **loses** the event or **repeats** the effect.

You cannot buy "exactly-once delivery" — it doesn't exist. You **engineer
exactly-once *effect*** out of at-least-once delivery:

```
   provider POST (at-least-once, out-of-order, retried)
          │
          ▼
   ┌──────────────┐   bad sig / poison    ┌────────────┐
   │  HMAC verify │ ─────────────────────▶│ dead-letter│
   └──────┬───────┘                       └────────────┘
          ▼   (one transaction)
   ┌──────────────────────────────┐
   │ INSERT OR IGNORE inbox(id)    │  ← dedup on the provider event id
   │ INSERT outbox(pending)        │  ← transactional inbox→outbox
   └──────┬───────────────────────┘
          ▼   async worker
   ┌──────────────────────────────┐
   │ (a) INSERT OR IGNORE ledger   │  ← effect-layer idempotency key  ── commit
   │ (b) [crash window]            │
   │ (c) UPDATE outbox = done      │  ── commit
   └──────┬───────────────────────┘
          ▼
   reconcile: unique inbox == ledger == outbox-done, balanced to the cent
```

If the worker dies between **(a)** and **(c)**, the outbox row stays `pending`;
on restart it is re-applied, the ledger `INSERT OR IGNORE` no-ops, and it is
marked done. **The effect is applied exactly once regardless of how many times
delivery or the worker retried.**

---

## What the demo prints (representative run, 312 events)

| | |
|---|---|
| Real payments | **312** |
| Webhook deliveries (at-least-once) | **847** + 1 forged |
| Duplicate deliveries **neutralised** | **535** |
| Forged signature → dead-letter | **1** |
| Worker **crash** mid-stream, then restart | resumes 174 rows |
| Reconciliation | `312 == 312 == 312`, **balanced to the cent** |
| Crash victim credited | **exactly once** |

A naive "credit on every delivery" handler over-credits by **$138,794.80** on
the same stream. That gap is the bug this design removes.

---

## Tests that have teeth

`tests/` asserts the invariants, not coverage:

- `test_each_event_credited_exactly_once` — N deliveries ⇒ exactly 1 ledger row
  per event, balanced total.
- `test_crash_before_mark_resumes_exactly_once` — kill the worker after the
  effect commit; a fresh process resumes with **0 lost, 0 duplicated**.
- `test_forged_signature_is_dead_lettered_not_processed`, `test_malformed_payload…`
  — bad input never reaches the effect.
- `test_naive_handler_would_double_credit` — proves the suite isn't vacuous.

**Planted-regression guard:** remove the inbox dedup *and* the ledger
idempotency key and `test_each_event_credited_exactly_once` goes red (verified:
the ledger balloons to 140 rows for 50 events). CI (`.github/workflows/ci.yml`)
runs the suite on every push.

---

## Design trade-offs (and why)

- **At-least-once + idempotency, *not* "exactly-once delivery".** Exactly-once
  delivery is impossible across a network; exactly-once *effect* is achievable
  and is what the business actually wants.
- **Transactional inbox → outbox.** Dedup and "work to do" commit in one local
  transaction, so we never enqueue a side-effect we didn't also record (or vice
  versa). The async worker decouples slow/failable effects from the fast HTTP ack.
- **Idempotency key at the *effect* layer too.** The inbox stops most duplicates,
  but the ledger key is the backstop that makes a *crash-induced* retry safe —
  belt and suspenders, because the two commits in the worker are not atomic.
- **Dead-letter, not silent `except: pass`.** Bad signatures and poison payloads
  are quarantined with a reason and a `replay_dead_letters()` hook.
- **Money is integer cents**, never float. Reconciliation balances exactly.

---

## How this maps to your posting

- *"event-driven, queue-backed, idempotent, resilient"* → exactly the
  inbox→outbox→idempotent-worker pattern here; swap sqlite→Postgres and the
  in-process worker→SQS/Kafka/PubSub.
- *"duplicate Stripe events" / "webhooks dropped during deploy"* → dedup inbox +
  durable outbox: a deploy that restarts the worker loses nothing (pending rows
  resume), and redelivered events are dropped.
- *Salesforce ↔ HubSpot sync, "no data drift", "conflict resolution"* → the
  ledger is the canonical store; bi-directional sync adds a per-record version /
  last-writer-wins (or merge) policy on top of the same idempotent upsert.

## How I'd productionize for your stack

- **Store:** Postgres — `inbox(event_id PK)`, `outbox` with `SELECT … FOR UPDATE
  SKIP LOCKED` (or `LISTEN/NOTIFY`), `ledger`/target with a unique idempotency key.
- **Transport:** keep the transactional outbox and relay to SQS/Kafka/PubSub so
  effects scale horizontally; consumers stay idempotent.
- **Resilience:** bounded retries with exponential backoff + jitter, a real DLQ,
  rate-limit/timeout handling per provider, structured logs with a correlation id
  (already stubbed), metrics on dedup-rate / DLQ-depth / outbox-lag.
- **Sync engines (SF/HubSpot/Ecwid→Supabase):** add Bulk/Streaming-API pagination,
  per-object cursors/watermarks, and conflict resolution; the idempotent core
  here is what stops re-runs and redeliveries from drifting your data.

## Assumptions & limitations (honest scope)

- A demo, not a framework: sqlite + an in-process worker. The *pattern* is
  production-grade; the *bindings* (Postgres, real queue, provider SDKs) are the
  paid work.
- One side-effect type (`credit_ledger`) for clarity; the outbox `effect` column
  is the extension point for more.
- No real provider keys: signatures are HMAC-SHA256 like Stripe/Shopify, over
  synthetic fixtures.

MIT licensed — see `LICENSE`.
