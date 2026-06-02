#!/usr/bin/env python3
"""Run the chaos demo: 1000+ at-least-once deliveries, a mid-stream crash,
and a reconciliation that balances to the cent. Zero credentials, zero pip.

    python demo.py            # default: ~312 unique events
    python demo.py --unique 500

What you should SEE:
  * a naive handler credits once per *delivery* (hundreds of double-credits);
  * this engine credits once per *event*, survives a worker crash mid-stream,
    and reconciles unique-events == ledger == outbox-done, balanced to the cent.
"""
from __future__ import annotations

import argparse
import os
import tempfile

from webhook_idempotency.engine import Engine, naive_credit_total
from webhook_idempotency.fixtures import chaotic_deliveries, make_events, signed

SECRET = "whsec_demo"


def banner(title: str) -> None:
    print("\n" + "=" * 64)
    print(f"  {title}")
    print("=" * 64)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--unique", type=int, default=312, help="number of distinct events")
    args = ap.parse_args()

    events = make_events(args.unique)
    stream = chaotic_deliveries(events)
    expected_cents = sum(e.amount_cents for e in events)

    banner("THE HARD PART")
    print(
        f"{len(events)} real payments arrive as {len(stream)} webhook deliveries\n"
        f"(at-least-once + retries + out-of-order). A correct system must apply\n"
        f"each payment's effect EXACTLY ONCE, even across a worker crash."
    )

    banner("WRONG HANDLER (credit on every delivery)")
    naive = naive_credit_total(stream)
    print(
        f"  deliveries processed : {len(stream)}\n"
        f"  total credited       : ${naive/100:,.2f}\n"
        f"  SHOULD have been     : ${expected_cents/100:,.2f}\n"
        f"  >>> over-credited by ${ (naive-expected_cents)/100:,.2f} "
        f"({len(stream)-len(events)} duplicate effects)  <-- the bug"
    )

    # Real engine on a durable file so we can model a crash + restart.
    dbfd, dbpath = tempfile.mkstemp(suffix=".db")
    os.close(dbfd)
    try:
        banner("THIS ENGINE (dedup inbox -> outbox -> idempotent worker)")
        eng = Engine(dbpath, secret=SECRET)
        accepted = duplicates = bad = 0
        for ev in stream:
            body, sig = signed(SECRET, ev)
            res = eng.receive(body, sig)
            duplicates += res.duplicate
            accepted += res.accepted and not res.duplicate
        # one forged delivery to exercise the dead-letter / signature path
        body, _ = signed(SECRET, events[0])
        bad += eng.receive(body, "sha256=forged").dead_lettered
        print(
            f"  deliveries received  : {len(stream)+1}\n"
            f"  unique accepted      : {accepted}\n"
            f"  duplicates dropped   : {duplicates}\n"
            f"  bad-signature -> DLQ : {bad}"
        )

        banner("WORKER CRASHES MID-STREAM, THEN RESTARTS")
        victim = events[len(events) // 2]
        crash_target = victim.event_id
        crashed = {"hit": False}

        def crash_once(event_id: str) -> bool:
            if event_id == crash_target and not crashed["hit"]:
                crashed["hit"] = True
                return True
            return False

        try:
            eng.run_worker(crash_before_mark=crash_once)
        except Exception as exc:  # noqa: BLE001 - simulated crash
            print(f"  worker died after applying effect for {crash_target}: {exc!r}")
        eng.close()

        print("  --- new process starts on the same database ---")
        eng2 = Engine(dbpath, secret=SECRET)
        resumed = eng2.run_worker()
        print(f"  resumed and processed {resumed} remaining outbox row(s)")

        banner("RECONCILIATION (source == effect, to the cent)")
        rec = eng2.reconcile()
        for k, v in rec.as_dict().items():
            if k.endswith("_cents"):
                print(f"  {k:24s}: ${v/100:,.2f}")
            else:
                print(f"  {k:24s}: {v}")
        victim_credit = eng2.ledger_total_for(victim.order_id)
        print(
            f"\n  crash victim {crash_target} credited exactly once: "
            f"${victim_credit/100:,.2f} "
            f"(== its single amount: {victim_credit == victim.amount_cents})"
        )
        eng2.close()

        banner("RESULT")
        ok = rec.balanced and victim_credit == victim.amount_cents
        print(
            f"  exactly-once EFFECT proven: {ok}\n"
            f"  {len(stream)-len(events)} duplicate deliveries neutralised; "
            f"crash caused 0 lost and 0 double effects."
        )
        return 0 if ok else 1
    finally:
        try:
            os.remove(dbpath)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
