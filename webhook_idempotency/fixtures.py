"""Deterministic, provider-shaped event fixtures and a chaotic delivery stream.

No network and no credentials: we synthesize Stripe-shaped
`payment_intent.succeeded` events and then model how a real webhook source
*actually* delivers them — at-least-once, out of order, with retries.
"""
from __future__ import annotations

import random
from typing import List, Tuple

from .engine import EventEnvelope, sign


def make_events(n_unique: int, seed: int = 1337) -> List[EventEnvelope]:
    """`n_unique` distinct payment events with deterministic ids/amounts."""
    rnd = random.Random(seed)
    events = []
    for i in range(n_unique):
        events.append(
            EventEnvelope(
                event_id=f"evt_{i:05d}",
                type="payment_intent.succeeded",
                order_id=f"ord_{i:05d}",
                amount_cents=rnd.randint(500, 50_000),  # $5.00 .. $500.00
                created=float(1_700_000_000 + i),
            )
        )
    return events


def chaotic_deliveries(
    events: List[EventEnvelope],
    duplication: float = 2.2,
    seed: int = 7,
) -> List[EventEnvelope]:
    """Expand unique events into a realistic at-least-once delivery stream.

    Every event is delivered at least once; ``duplication`` controls the
    average number of deliveries (retries/replays), and the whole stream is
    shuffled so events arrive out of order.
    """
    rnd = random.Random(seed)
    stream: List[EventEnvelope] = []
    for ev in events:
        copies = max(1, int(rnd.expovariate(1 / duplication)) + 1)
        stream.extend([ev] * copies)
    rnd.shuffle(stream)
    return stream


def signed(secret: str, ev: EventEnvelope) -> Tuple[bytes, str]:
    """Return the (body, signature) pair a real provider POST would carry."""
    body = ev.body()
    return body, sign(secret, body)
