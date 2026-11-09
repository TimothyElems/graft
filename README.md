# Graft

A Go service that runs migration pipelines on short-lived database branches, and a typed Python SDK for it. One contract joins them.

Graft is an independent learning project, and it is early. v0.1 (contract and scheduling core) and v0.2 (a walking skeleton: Go service, sync Python SDK, end-to-end tests) exist. Everything else below is marked **planned**.

**Live demo:** https://graft-demo.fly.dev ([`/healthz`](https://graft-demo.fly.dev/healthz) is public; `/v1` needs a token). It keeps everything in memory and the step runner is a fake, so state resets whenever the machine sleeps. It is not production.

## Status

| Release | What | Status |
|---|---|---|
| v0.1 | Contract (OpenAPI + event schema), deterministic Go scheduler kernel, independent Python reference, 17 shared fixtures, random Go-vs-Python check | **Done** |
| v0.2 | Walking skeleton: Go HTTP service (in-memory store, fake runner over the kernel), sync Python SDK, end-to-end tests against `docker compose`, run in CI | **Done** ([what is not](#what-v02-does-not-do)) |
| v0.3 | Postgres job queue (`SKIP LOCKED`), leases, gap-free event log, cursor pages, Idempotency-Key | Planned |
| v0.4 | Full SDK: generated models + drift gate, async client, pagination, retries, strict typing | Planned |
| v0.5 | Realtime websocket with resume after `seq`, chaos test | Planned |
| v0.6 | Sandboxed Docker step runner, real clock shell, crash recovery, metrics/tracing, `graft.toml` | Planned |
| v0.7 | Support kit: `graft doctor`, repro bundles, ten case files | Planned |
| v0.8 | Release automation, v1 → v2 migration with codemod, `llms.txt` | Planned |
| v0.9 | TypeScript types, console, short-lived AWS demo stack | Planned |
| v1.0 | Docs pass and fresh-clone check | Planned |

Nothing in the Planned rows exists in this repository. Do not read them as done.

## What is here

```
contract/openapi.yaml        the API, OpenAPI 3.1
contract/events.schema.json  the event stream, JSON Schema 2020-12
kernel/                      Go: the deterministic scheduler core (stdlib only)
parity/                      Python: an independent reference, and the tests that compare the two
fixtures/pipeline/           17 cases with hand-derived results, read by both languages
service/                     Go: graftd, the HTTP service (v0.2)
sdk/python/                  Python: graft-sdk, the sync client (v0.2)
compose.yaml                 graftd in a container, for the end-to-end tests
db/schema.sql                Postgres tables, including the job queue (parses; not yet run against a database)
docs/contract-notes.md       one sentence per endpoint, and the gaps found in the contract
docs/kernel-map.md           where each scheduling rule lives in the Go and Python code
```

## What the tests prove

- **Contract** validates (`make contract`).
- **Kernel**: Go passes all 17 fixtures; the independent Python reference passes the same fixtures; every kernel event validates against the event schema (`make test`).
- **Parity**: 300 random valid pipelines give identical output from Go and Python, and 60 deliberately invalid ones give the identical error (`make diff`). Run more with `python3 parity/diff_random.py <N> <SEED>`.
- **Service** (`make test-service`, with the race detector): every `/v1` route needs the token; every error is `application/problem+json` with its own `type` and the same `request_id` as the `X-Request-Id` header; a run's result equals `pipeline.Simulate` on a hand-built kernel case (`service/internal/sched/shell_test.go`).
- **SDK** (`make test-sdk`, Python 3.10 and 3.13 in CI): errors are chosen by problem `type`, so the two kinds of 422 raise different exceptions; timeouts are explicit; `runs.wait()` polls with backoff.
- **End to end** (`make e2e`, or `make e2e-compose`, which is what CI runs): the SDK against a real `graftd`, and every raw response checked against `contract/openapi.yaml`.

## What v0.2 does not do

- No database: an in-memory store, lost on restart. Postgres arrives in v0.3.
- No real step execution: the fake runner scripts outcomes from `command: ["graft-fake", "fail:10", "ok:10"]` and treats any other command as "succeeds in 10 ms". The Docker runner arrives in v0.6.
- A static bearer token, not JWT.
- Only `healthz`, `createBranch`, `getBranch`, `createRun` and `getRun`. Listing, delete, cancel, events, bundle and the stream are in the contract but not built.
- `Idempotency-Key` is accepted and ignored until v0.3.

## Python SDK

```python
from graft import Graft, StepSpec

with Graft(token="...", base_url="http://localhost:8080") as g:
    br = g.branches.create("demo", name="feat-invoices")
    run = g.runs.create(br.id, steps=[
        StepSpec(id="migrate", max_attempts=3, backoff_ms=5, command=["graft-fake", "fail:10", "fail:10", "ok:10"]),
        StepSpec(id="seed", needs=["migrate"]),
    ])
    run = g.runs.wait(run.id)
    print(run.status, run.step("migrate").attempts)   # succeeded 3
```

## The scheduling rules

1. A step starts when every step it needs has succeeded and a worker slot is free. Free slots go to ready steps in list order.
2. An attempt longer than `timeout_ms` ends at the timeout, with reason `timeout`.
3. A failed attempt waits `backoff_ms` and retries, until `max_attempts` is used. Then the step is failed.
4. When a step has failed, been skipped or been cancelled, every step that needs it is skipped, with reason `dependency:<step>`, through the whole chain in one tick.
5. Cancel at time T: an attempt that finishes at T counts. Everything else not finished is cancelled.
6. A bad pipeline is refused before anything runs: `duplicate_step`, then `unknown_dependency`, then `cycle`.

Inside one tick: finish attempts that end now (in start order), then skip, then cancel, then start new steps, then jump to the next time anything can change.

## Run

```
python3 -m venv .venv && . .venv/bin/activate
pip install pytest pyyaml jsonschema openapi-spec-validator pglast
make all                 # contract, kernel, parity, service

python3 -m venv sdk/python/.venv && sdk/python/.venv/bin/pip install -e 'sdk/python[dev]'
make test-sdk            # SDK unit tests
make e2e                 # builds and starts graftd, runs the SDK e2e tests, stops it
make e2e-compose         # the same against docker compose (needs Docker)
```

Needs Go 1.24 and Python 3.10+. `.devcontainer/devcontainer.json` is included for Codespaces and has not been tried yet.

## The kernel in one paragraph

`Simulate(case)` takes steps, scripted outcomes, a worker limit and an optional cancel time. It returns every event with a virtual timestamp. It reads no clock and starts no process, so the same input always gives the same output. `graftd` wraps it in a shell (`service/internal/sched`) that turns a stored run into kernel input and records the kernel's result unchanged. In v0.6 the shell gets a real clock and a Docker runner.
