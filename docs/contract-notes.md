# Contract notes (v0.1)

One sentence per endpoint in `contract/openapi.yaml`, then the gaps I found reading it.

## Endpoints

| Operation | Call | In one sentence |
|---|---|---|
| `healthz` | `GET /healthz` | Tells a load balancer the process is up and how many runs are waiting, with no auth so a probe can call it. |
| `createBranch` | `POST /v1/projects/{project}/branches` | Asks for a new short-lived database copy by name, and a retried request with the same Idempotency-Key gets the first answer back instead of a second branch. |
| `listBranches` | `GET /v1/projects/{project}/branches` | Returns one page (default 50, max 200) of a project's branches newest first, plus an opaque cursor for the next page or `null` at the end. |
| `getBranch` | `GET /v1/branches/{id}` | Returns one branch, including its lifecycle status (`creating` to `deleted`) and when it expires. |
| `deleteBranch` | `DELETE /v1/branches/{id}` | Removes a branch and cancels any runs still going on it, answering 204 with no body. |
| `createRun` | `POST /v1/branches/{id}/runs` | Submits a pipeline of up to 64 steps and gets back a `queued` run with 202, because the work happens later on a worker, not inside the request. |
| `getRun` | `GET /v1/runs/{id}` | Returns a run's overall status and, per step, how many attempts it took, when it started and finished in run-relative ms, and why it ended. |
| `cancelRun` | `POST /v1/runs/{id}/cancel` | Asks a run to stop; calling it twice is the same as calling it once, so a client can retry it blindly. |
| `listRunEvents` | `GET /v1/runs/{id}/events?after=` | Returns the run's events oldest first starting after a given `seq`, so a client can catch up from where it left off. |
| `getRunBundle` | `GET /v1/runs/{id}/bundle` | Downloads a gzip with everything needed to replay a run in the kernel, secrets removed, for a support engineer. |
| `stream` | `GET /v1/stream?branch=&after=` (websocket) | Opens a live feed of every event on a branch, first replaying everything after `after`, so a reconnect sees no gaps and no duplicates. |

## Things the schemas pin down that are easy to miss

- **Two clocks.** `Run.created_at` / `Event.at` are wall-clock; `StepRun.started_ms` / `Event.at_ms` are milliseconds since the run started, the kernel's virtual clock. Only the second is deterministic.
- **Defaults that mean "off".** `timeout_ms: 0` means no timeout. `backoff_ms: 0` means retry immediately. `max_attempts: 1` means no retries. A client must not treat `0` as "unset".
- **Two different pattern rules.** Branch `name` may start with a digit (`^[a-z0-9]…`); `project` must start with a letter (`^[a-z]…`). Step ids must start with a letter and use `_`, not `-`.
- **Events are the source of truth for order.** `seq` is per run and gap-free; `at_ms` can tie (several events at the same ms), so order by `seq`, never by time.

## Gaps and questions (to fix when the server is written, v0.2/v0.3)

1. **No 401 on most operations.** Security is global (`bearerJwt`) but only `stream` lists a 401 response. Every authed operation can return 401.
2. **No 404 on `listRunEvents` or `getRunBundle`.** Both take a run id that may not exist; their siblings list 404.
3. **No 404/422 on `listBranches`.** An unknown project or a malformed cursor has no error response.
4. **`EventPage` has no "more" flag.** With `limit`, a client can only tell it is done when it gets an empty page. `last_seq` helps resume but does not say "this was everything".
5. **422 is overloaded.** It means both "body failed validation" and "Idempotency-Key reused with a different body". The `type` URI in the problem body must tell them apart, or the SDK cannot raise the right error class.
6. **`cancelRun` on a finished run.** "Safe to repeat" is stated, but not what happens when the run already succeeded: 202 with the unchanged run, or 409? Pick one and write it down.
7. **`Branch.status` has no `expired`.** TTL expiry presumably goes through `deleting` → `deleted`; worth saying so.
8. **`concurrency` lives on `CreateRun`, not on the step.** That matches the kernel (one `workers` number per run), but it is a run-level limit only; there is no global cap in the contract.
