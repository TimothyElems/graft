# graft-sdk (Python)

Sync client for the Graft API. Early: v0.2. Async, generated models and realtime are planned (v0.4, v0.5).

```python
from graft import Graft, StepSpec

with Graft(token="...", base_url="http://localhost:8080") as g:
    br = g.branches.create("demo", name="feat-invoices")
    run = g.runs.create(br.id, steps=[
        StepSpec(id="migrate", max_attempts=3, backoff_ms=5, command=["graft-fake", "fail:10", "ok:10"]),
        StepSpec(id="seed", needs=["migrate"]),
    ])
    run = g.runs.wait(run.id)
    print(run.status, [(s.id, s.status, s.attempts) for s in run.steps])
```

`GRAFT_TOKEN` and `GRAFT_URL` are read from the environment if not passed.
