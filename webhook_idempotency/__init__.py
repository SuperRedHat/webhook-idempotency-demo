"""Exactly-once-EFFECT webhook processing engine.

The hard part: webhooks are delivered *at-least-once*, out of order, and
retried — so a naive handler double-charges / double-fulfills, and a crash
mid-processing either loses an event or applies its side-effect twice.

This package shows the correct pattern with stdlib only (sqlite3):
  HMAC verify -> dedup inbox -> transactional outbox -> idempotent worker
  -> reconciliation, with a dead-letter path for poison events.

See engine.Engine for the implementation and README.md for the design notes.
"""
from .engine import Engine, EventEnvelope, sign, SignatureError

__all__ = ["Engine", "EventEnvelope", "sign", "SignatureError"]
__version__ = "1.0.0"
