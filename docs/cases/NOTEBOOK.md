# Case notebook (the ten cases to work)

Copied from the Graft planning page's "Notebook" section, so every workspace has it. The likely causes are proposals; the real cause may differ once reproduced. For each case: build the bug on purpose, write the repro as a failing test, fix it, and write the reply a customer would receive, in `docs/cases/NN-name.md` (reply shape: what happened, what to do now, what changes so it does not repeat).

| # | Customer says | Likely cause (proposal) | Existing guard in the code, if any |
|---|---|---|---|
| 01 | "Run run_… has been running since this morning. Nothing moves." | A worker died and the lease never lapsed, or the reclaim query ignores expired leases. Look at `lease_until` and the last `step_started` event. Also found in v0.7: a per-run write deadline made every run longer than 30 s fail and restart forever. | `queue/lease_test.go` TestKillNineWorkerRunIsReclaimedAndFinishesExactlyOnce, TestARunLongerThanTheWriteTimeoutStillFinishes |
| 02 | "After my wifi dropped, the same step_succeeded came in two times." | Resume used `seq >= last` instead of `seq > last`. The client also needs to ignore a seq it already has. | `api/stream_test.go` (after=5 starts at 6), `sdk/python/tests/unit/test_stream.py` |
| 03 | "The timeline shows a step finishing before it started." | Events sorted by wall-clock time across two writers. Order by seq, which is per run and gap-free. | `events.List` orders by seq; a dedicated test is being added |
| 04 | "I set a backoff but it hammers the database." | Backoff counted from the first attempt, not from the failure. Or `backoff_ms` of 0 read as "unset" and replaced by a default. | kernel fixtures; `test_client.py` (explicit 0 is sent) |
| 05 | "The run says cancelled. docker ps still shows the step." | The context was cancelled for the scheduler but not passed to the runner. Kill the process tree, not only the parent. | `runner` TestCancelLeavesNoContainer (needs Docker) |
| 06 | "409 conflict on "feat-invoices", but I deleted it a minute ago." | Soft delete kept the row, and the unique index on (project, name) still covers it. Make the index partial on live rows. | `api/branches_test.go`, `branches_live_name` index |
| 07 | "Looping over all branches gives me repeats and misses some." | The cursor held only `created_at`. Ties break the page. Use (created_at, id) in the cursor and the ORDER BY. | `api/branches_test.go` (120 same-time branches) |
| 08 | "The SDK call never returns and never errors." | No default timeout on the HTTP client. Set explicit connect, read and write timeouts, and document them. | `test_client.py` test_default_timeout_is_explicit_and_finite |
| 09 | "mypy fails after I upgraded a minor version. A field I read is now Optional." | A type change shipped as a minor release is a breaking change. Revert, then carry it in v2 with a deprecation path. | `tests/typing/sample_app.py` under mypy (to be extended) |
| 10 | "I changed the steps but the run is the old one." (retried with the same Idempotency-Key) | Same key with a different body must be 422, not a silent replay. Store a hash of the body with the key. | `api/idempotency_test.go` (same key, different body is 422) |

The planning page suggested test paths `tests/cases/test_NN.py`; most guards here are Go tests, so each case file names its actual test.
