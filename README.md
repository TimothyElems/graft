# Graft

A Go service that runs migration pipelines on short-lived database branches, and a typed Python SDK for it. One contract joins them.

Graft is an independent learning project, and it is early. v0.1 (contract and scheduling core), v0.2 (walking skeleton), v0.3 (Postgres queue, leases, event log), v0.4 (the Python SDK: generated models, sync and async, strict typing) and v0.5 (realtime: a resumable websocket stream) exist. Everything else below is marked **planned**.

**Live demo:** https://graft-demo.fly.dev ([`/healthz`](https://graft-demo.fly.dev/healthz) is public; `/v1` needs a token). It still runs **v0.2** (in-memory, no database attached), so its state resets whenever the machine sleeps. It is not production.

## Status

| Release | What | Status |
|---|---|---|
| v0.1 | Contract (OpenAPI + event schema), deterministic Go scheduler kernel, independent Python reference, 17 shared fixtures, random Go-vs-Python check | **Done** |
| v0.2 | Walking skeleton: Go HTTP service, fake runner over the kernel, sync Python SDK, end-to-end tests against `docker compose`, run in CI | **Done** |
| v0.3 | Postgres: migrations, job queue (`SKIP LOCKED`) with leases and heartbeat, gap-free event log written with each state change, cursor pages, Idempotency-Key | **Done** ([what is not](#not-built-yet)) |
| v0.4 | Python SDK: models generated from the contract with a drift gate, sync and async clients, typed errors, pagination iterators, retries with jitter on safe calls, mypy strict + pyright strict, wheel checked from a clean venv | **Done** |
| v0.5 | Realtime: `/v1/stream` websocket (replay after a seq, then live, via Postgres NOTIFY), slow-consumer protection, SDK `AsyncGraft.stream()` with reconnect and resume, chaos test | **Done** ([docs/realtime.md](docs/realtime.md)) |
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
service/                     Go: graftd, the HTTP service and run workers
service/migrations/          numbered SQL migrations, applied by graftd at start
sdk/python/                  Python: graft-sdk, sync and async clients (models generated from the contract)
compose.yaml                 graftd in a container, for the end-to-end tests
db/schema.sql                the schema in one readable file; a test fails if it and the migrations differ
docs/contract-notes.md       one sentence per endpoint, and the gaps found in the contract
docs/kernel-map.md           where each scheduling rule lives in the Go and Python code
```

## What the tests prove

- **Contract** validates (`make contract`).
- **Kernel**: Go passes all 17 fixtures; the independent Python reference passes the same fixtures; every kernel event validates against the event schema (`make test`).
- **Parity**: 300 random valid pipelines give identical output from Go and Python, and 60 deliberately invalid ones give the identical error (`make diff`). Run more with `python3 parity/diff_random.py <N> <SEED>`.
- **Service** (`make test-service`, against a real Postgres, every test under the race detector): every `/v1` route needs the token; every error is `application/problem+json` with its own `type` and the same `request_id` as the `X-Request-Id` header; a run's result equals `pipeline.Simulate` on a hand-built kernel case (`service/internal/sched/shell_test.go`).
- **Queue** (`service/internal/queue`): 8 workers drain 1,000 runs and each is claimed exactly once; a worker process killed with `kill -9` loses its run when the lease lapses, another worker finishes it exactly once, and the dead worker's late result is refused.
- **Events** (`service/internal/events`): 20 concurrent writers to one run get seq 1..100 with no gaps or repeats; a rolled-back write leaves no gap; a run's state change, step results and events commit together or not at all.
- **Pages and retries** (`service/internal/api`): 120 branches with the same `created_at` page as 50, 50, 20 with no repeats or misses; the same Idempotency-Key and body replays the first response (12 concurrent retries create one run), a different body gets 422.
- **Migrations** (`service/internal/db`): concurrent starts apply each migration once; an edited, already-applied migration stops startup; a failing migration leaves nothing behind.
- **SDK** (`make test-sdk`, Python 3.10 and 3.13 in CI): every unit test runs on the sync AND the async client; errors are chosen by problem `type`; pagination is lazy and stops on a repeated cursor; retries happen only on GET and keyed POSTs, with the same Idempotency-Key on every attempt, against a flaky fake server.
- **Drift gate** (`make drift-check`): CI regenerates `sdk/python/src/graft/_models.py` from the contract and fails on any difference.
- **Typing** (`make sdk-typing`, `make check-wheel`): mypy `--strict` and pyright strict pass on the library and tests; `scripts/no_any.py` fails on `Any` in any public signature; the built wheel is installed alone in a fresh venv and a sample app is type-checked against it.
- **Realtime** (`service/internal/api/stream_test.go`, `sdk/python/tests/unit/test_stream.py`): replay equals the event log and every frame matches `events.schema.json`; `after=5` starts at 6; live events arrive through NOTIFY with polling disabled; a stalled reader is disconnected without slowing another reader or the worker; the client dedupes, never yields past a hole, and gives up only after repeated failures with no progress.
- **Chaos** (`sdk/python/tests/chaos/test_stream.py`): a proxy cuts the stream 200 times during a 502-event run; the client's events equal the server's log, with 0 duplicates and 0 gaps.
- **End to end** (`make e2e`, or `make e2e-compose`, which is what CI runs): the SDK, sync and async, against a real `graftd` and Postgres, and every raw response checked against `contract/openapi.yaml`.

## Not built yet

- No real step execution: the fake runner scripts outcomes from `command: ["graft-fake", "fail:10", "ok:10"]` and treats any other command as "succeeds in 10 ms". The Docker runner arrives in v0.6.
- A static bearer token, not JWT.
- Delete, cancel and the repro bundle are in the contract but not built. The stream is per run; a branch-wide stream would need its own sequence.
- The live demo has not been moved to v0.3 (it needs a database).

## Python SDK

Sync and async clients; see [sdk/python/README.md](sdk/python/README.md).

```python
from graft import Graft, StepSpec

with Graft(token="...", base_url="http://localhost:8080") as g:
    br = g.branches.create("demo", name="feat-invoices")
    run = g.runs.wait(g.runs.create(br.id, steps=[StepSpec(id="migrate"), StepSpec(id="seed", needs=["migrate"])]).id)
    print(run.status, [e.type for e in g.runs.events(run.id)])
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
eval "$(scripts/pg-dev.sh start)"   # throwaway local Postgres (needs initdb/pg_ctl), sets GRAFT_TEST_DATABASE_URL
make all                 # contract, kernel, parity, service

python3 -m venv sdk/python/.venv && sdk/python/.venv/bin/pip install -e 'sdk/python[dev]'
make test-sdk            # SDK unit tests
make e2e                 # graftd on a fresh database, SDK e2e tests, then a restart check
make e2e-compose         # the same against docker compose (needs Docker)
```

Needs Go 1.24 and Python 3.10+. `.devcontainer/devcontainer.json` is included for Codespaces and has not been tried yet.

## The kernel in one paragraph

`Simulate(case)` takes steps, scripted outcomes, a worker limit and an optional cancel time. It returns every event with a virtual timestamp. It reads no clock and starts no process, so the same input always gives the same output. `graftd` wraps it in a shell (`service/internal/sched`) that turns a stored run into kernel input and records the kernel's result unchanged. In v0.6 the shell gets a real clock and a Docker runner.
