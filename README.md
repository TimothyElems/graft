# Graft

A Go service that runs migration pipelines on short-lived database branches, and a typed Python SDK for it. One contract joins them.

Graft is an independent learning project. Only v0.1, the contract and the scheduling core, exists today. Everything else below is marked **planned**.

## Status

| Release | What | Status |
|---|---|---|
| v0.1 | Contract (OpenAPI + event schema), deterministic Go scheduler kernel, independent Python reference, 17 shared fixtures, random Go-vs-Python check | **Done** |
| v0.2 | Walking skeleton: Go HTTP server (in-memory), sync Python client, end-to-end test via `compose.yaml` | Planned |
| v0.3 | Postgres job queue (`SKIP LOCKED`), leases, gap-free event log, cursor pages, Idempotency-Key | Planned |
| v0.4 | Full SDK: generated models + drift gate, async client, typed errors, pagination, retries, strict typing | Planned |
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
db/schema.sql                Postgres tables, including the job queue (parses; not yet run against a database)
docs/contract-notes.md       one sentence per endpoint, and the gaps found in the contract
docs/kernel-map.md           where each scheduling rule lives in the Go and Python code
```

## What v0.1 proves (`make all`)

- The contract validates (`make contract`).
- The Go kernel passes all 17 fixtures (`make test-go`).
- The Python reference passes the same fixtures, and every kernel event validates against the event schema (`make test-py`).
- 300 random valid pipelines give identical output from Go and Python, and 60 deliberately invalid ones give the identical error (`make diff`). Run more with `python3 parity/diff_random.py <N> <SEED>`.

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
make all
```

Needs Go 1.24 and Python 3.10+. `.devcontainer/devcontainer.json` is included for Codespaces and has not been tried.

## The kernel in one paragraph

`Simulate(case)` takes steps, scripted outcomes, a worker limit and an optional cancel time. It returns every event with a virtual timestamp. It reads no clock and starts no process, so the same input always gives the same output. The real service (planned, v0.2 and v0.6) will wrap it in a shell that supplies time and runs containers.
