"""Optional live HTTP receiver — shows the same engine behind a real webhook
endpoint. Not required to run the demo or tests (those are stdlib-only).

    pip install -r requirements.txt
    uvicorn app:app --reload
    # POST a signed event to /webhook ; run the worker on a schedule / loop.
"""
from __future__ import annotations

import os

try:
    from fastapi import FastAPI, Header, Request, Response
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
    raise SystemExit(
        "app.py needs FastAPI: pip install -r requirements.txt "
        "(the demo + tests run without it)"
    ) from exc

from webhook_idempotency.engine import Engine

SECRET = os.environ.get("WEBHOOK_SECRET", "whsec_demo")
DB = os.environ.get("WEBHOOK_DB", "webhook.db")

app = FastAPI(title="webhook-idempotency-demo")
engine = Engine(DB, secret=SECRET)


@app.post("/webhook")
async def webhook(request: Request, x_signature: str = Header(default="")):
    """Verify + dedup + enqueue, then ack fast (HTTP 200/202). At-least-once safe."""
    body = await request.body()
    res = engine.receive(body, x_signature)
    if res.dead_lettered:
        return Response(status_code=400, content=res.reason)
    return {"accepted": res.accepted, "duplicate": res.duplicate}


@app.post("/worker/run")
def run_worker():
    """Drain the outbox (in prod: a scheduled job / queue consumer)."""
    return {"processed": engine.run_worker()}


@app.get("/reconcile")
def reconcile():
    return engine.reconcile().as_dict()
