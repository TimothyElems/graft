# Where each scheduler rule lives

Line numbers are `kernel/pipeline/sim.go` (Go) and `parity/reference.py` (Py) as of v0.1.

| # | Rule | Go | Py | How it is enforced |
|---|---|---|---|---|
| 1 | A step starts when every need has succeeded and a slot is free; free slots go to ready steps in list order. | 273–304 (slot cap 274, needs check 281–290) | 113–131 | Phase 4 walks `steps` by index, so list order *is* the priority. `len(run) >= conc` stops it when slots run out. |
| 2 | An attempt longer than `timeout_ms` ends at the timeout, reason `timeout`. | 298–300, reason picked at 219–222 | 125–127, 80 | Decided **at start**, not by watching a clock: if the scripted duration exceeds the timeout, the attempt is scheduled to end at `t + timeout_ms` with outcome `timeout`. Strictly greater: an attempt exactly equal to the timeout succeeds. |
| 3 | A failed attempt waits `backoff_ms` and retries until `max_attempts` is used; then the step fails. | 223–231; `readyAt` gate 278; wake-up 317–318 | 81–86; gate 117; wake 137 | Backoff counts **from the failure** (`readyAt = t + backoff`), not from the start. Phase 5 must consider `readyAt` as a wake-up time, or a step in backoff with nothing else running would never resume. |
| 4 | When a step has failed, been skipped or been cancelled, every step that needs it is skipped with `dependency:<step>`, spreading through the chain in one tick. | 236–253; `bad()` 195 | 91–102 | A fixed-point loop: repeat until a full pass changes nothing. One pass is not enough because list order need not be topological. The guard `attempts > 0` keeps a step that already ran from being skipped (its needs had already succeeded). |
| 5 | Cancel at T: an attempt that finishes at T counts; everything else not finished is cancelled. | 256–270 | 104–111 | The tie rule is **just phase order**: phase 1 (finish) runs before phase 3 (cancel) in the same tick, so a finish at T is recorded first. Nothing starts at T because phase 4 comes after the `break`. Phase 5 adds `cancel_at_ms` as a wake-up so the clock lands on T exactly. |
| 6 | A bad pipeline is refused before anything runs: `duplicate_step`, then `unknown_dependency`, then `cycle`. | `Validate` 79–125, called at 159 | `validate` 15–34, called at 39 | Three separate passes, in that order, so a pipeline with two problems always reports the same one. Cycle detection is Kahn's algorithm: if a topological sort can't place every step, there is a cycle. |

## The tick (sim.go 197–335)

1. Finish attempts with `end <= t`, sorted by `(end, start seq)`.
2. Skip, to a fixed point.
3. Cancel, if `cancel_at_ms <= t`, then stop.
4. Start ready steps in list order up to `concurrency`.
5. Jump `t` to the smallest of: running ends, backoff `readyAt`s, `cancel_at_ms`. Stop if nothing is pending or running.

## Things not in the six rules but in the code

- Unscripted steps default to `ok` in 10 ms (`defaultAttempt`, 76). A script shorter than the attempt count repeats its last entry (`attemptFor`, 144–153).
- `concurrency < 1` and `max_attempts < 1` are treated as 1.
- The kernel never emits `run_started`; the service adds it. It always ends with `run_finished`, whose `reason` is the run status.
- Run status: `cancelled` only if cancel actually cancelled something; else `succeeded` if every step succeeded; else `failed`.
