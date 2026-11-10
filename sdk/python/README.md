# graft-sdk (Python)

Typed client for the Graft API, sync and async. Early (v0.5, not published to PyPI yet).

```python
from graft import Graft, StepSpec

with Graft(token="...", base_url="http://localhost:8080") as g:
    br = g.branches.create("demo", name="feat-invoices")
    run = g.runs.create(br.id, steps=[
        StepSpec(id="migrate", max_attempts=3, backoff_ms=5, command=["graft-fake", "fail:10", "ok:10"]),
        StepSpec(id="seed", needs=["migrate"]),
    ])
    run = g.runs.wait(run.id)
    print(run.status, run.step("migrate").attempts)

    for b in g.branches.list("demo"):          # fetches pages as you iterate
        print(b.name)
    for e in g.runs.events(run.id):            # in seq order, resumable with after=
        print(e.seq, e.type, e.step)
```

```python
from graft import AsyncGraft

async with AsyncGraft(token="...") as g:
    br = await g.branches.create("demo", name="feat-x")
    async for b in g.branches.list("demo"):
        print(b.name)
    async for ev in g.stream(run_id):            # websocket: replay, then live; resumes across drops
        print(ev.seq, ev.type)
```

`GRAFT_TOKEN` and `GRAFT_URL` are read from the environment if not passed.

## What it does for you

- **Models generated from the contract** (`_models.py`, by `scripts/gen_models.sh`). CI regenerates them and fails on any difference. Unknown fields from a newer server are ignored, so an older SDK keeps working.
- **Typed errors** chosen from the problem `type`, each with `request_id`: `NotFound`, `Conflict`, `InvalidRequest`, `InvalidPipeline`, `IdempotencyKeyReused`, `RateLimited` (with `retry_after`), `ServerError`, `Unauthorized`; plus `NetworkError` and `RequestTimeout` when there is no response. All are `GraftError`.
- **Retries with full jitter, only when safe**: GET, and POST with an `Idempotency-Key`. Every create sends a fresh key and reuses it across its own retries, so a retried create never makes two. `Retry-After` is honoured. Turn retries off with `retry=RetryPolicy(max_retries=0)` (from `graft._retry`).
- **Realtime, async only**: `AsyncGraft.stream(run_id, after=0)` reconnects with backoff and jitter and resumes after the last seq it yielded; duplicates are dropped and holes are never yielded past (`stream.stats`). See docs/realtime.md for close codes.
- **Explicit timeouts**: connect 5 s, read and write 30 s.
- **Strict typing**: `mypy --strict` and pyright strict pass on the library and tests; no `Any` in the public API (`scripts/no_any.py`); `py.typed` ships in the wheel, and CI type-checks a sample app against the installed wheel.

## Changes from v0.2

Error classes were renamed (the SDK was never published): `NotFoundError` → `NotFound`, `ConflictError` → `Conflict`, `ValidationError` → `InvalidRequest`, `InvalidPipelineError` → `InvalidPipeline`, `AuthenticationError` → `Unauthorized`, `APIConnectionError` → `NetworkError`, `APITimeoutError` → `RequestTimeout`. Models are now pydantic (generated) instead of hand-written dataclasses.
