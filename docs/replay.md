# Replaying a support bundle

A customer says "step `seed` ran before `migrate` finished" or "my run failed and it shouldn't have". The bundle answers one question exactly: **did the service follow the scheduling rules?** If it did, the cause is in the pipeline definition or in the step itself, and the attempt logs say which. If it did not, the bug is ours, and replay shows the first event where the run went wrong.

```bash
graft doctor run_123                                   # customer side: writes graft-doctor-run_123.tar.gz
scripts/replay.sh graft-doctor-run_123.tar.gz          # our side
```

```
bundle run_uretuoaeazuc4npf from graftd v0.7.0-dev
MATCH: 9 events, same order and content, times within 50 ms. Run status failed.
The service followed the scheduling rules for these attempt durations.
```

## How it works

graftd stores what happened (events), not the script that caused it. `case.json` in the bundle is rebuilt from the events by `pipeline.CaseFromEvents` (`kernel/pipeline/replay.go`):

| Real event | Scripted attempt in case.json |
|---|---|
| `step_started` at S … `step_succeeded` at E | `ok`, E − S ms |
| … `step_retry` / `step_failed` (reason `failed`) at E | `fail`, E − S ms |
| … `step_retry` / `step_failed` (reason `timeout`) | `fail`, timeout_ms + 1 (it ran past the deadline) |
| … `step_cancelled` while running, at E | `ok`, E − S + 1 (it would have finished after the cancel) |
| `run_finished` (reason `cancelled`) at C | `cancel_at_ms: C` |

The two `+ 1`s are not decoration. An attempt that ends **exactly** at the cancel counts as finished (rule 5), so a cancelled attempt scripted to end at C would replay as a success. Both were proven by mutation: remove either one and the round-trip test fails.

`kernel/cmd/simulate -compare events.json` then runs that case and compares, position by position:

- **type, step, attempt and reason must be identical**, in the same order. Order is never jitter: the same events in another order is a different run.
- **times may differ by the tolerance** (default 50 ms). Real timers fire late: a 50 ms backoff wakes at 51 ms, and that lateness adds up along a chain of retries. The kernel's virtual clock is never late.

Only the **last claim** is compared (the events after the last `run_started`). If a worker died, claim 2 re-ran everything from time 0; claim 1's events are evidence of the crash, not part of the run that finished.

## What it proves, and what it does not

| Replay says | It means | Next step |
|---|---|---|
| MATCH | The service decided what the rules decide for these durations. | Read `attempts/`: why did the step fail or run long? Check the pipeline's `needs`, `max_attempts`, `timeout_ms`. |
| DIFFERENT at event N | The stored events break a rule: a step started before its needs, a retry before its backoff, an event lost or out of order. | A service bug. Event N is where to start; `run.json` has the claims and the lease owner. |
| no case.json (exit 2) | The run had not finished when the bundle was made. | The manifest says why. Ask for a new bundle after it finishes, or look at `events.json` for where it stopped. |

Replay does **not** reproduce what a step did (its exit code, its output). Those are evidence in `attempts/`, not something the kernel decides. It also cannot prove the durations themselves were right: it takes them as given.

## Checked by

- `kernel/pipeline/replay_test.go`: every fixture and 2,000 random pipelines (retries, timeouts, backoff, concurrency, cancels that tie with finishes) give the same events and result after `Simulate → CaseFromEvents → Simulate`.
- `kernel/cmd/simulate/main_test.go`: tolerance, order, missing events, last claim only.
- `service/internal/api/bundle_test.go`: a real run's bundle replays to the same events.
- By hand: swapping two events in a real bundle's `events.json` (what reading events by wall clock instead of seq does, notebook case 03) gives `DIFFERENT: event 6: kernel has step_succeeded migrate#2 at 251 ms; the real run has step_started seed#1 at 252 ms`.

## Safety

The bundle is input from a customer. `scripts/replay.sh` never extracts it: it streams `manifest.json`, `case.json` and `events.json` by name (`tar -xzOf`). `graft doctor` likewise reads the server's archive in memory and refuses links, absolute paths and `..`.
