# Realtime: `/v1/stream`

One run's events over a websocket: replayed after a seq, then live, ending after `run_finished`.

```
GET /v1/stream?run=<run id>&after=<seq>        Authorization: Bearer <token>
```

```python
async for ev in g.stream(run.id):          # graft-sdk, AsyncGraft
    print(ev.seq, ev.type, ev.step)
```

Each text frame is one event (`contract/events.schema.json`). `seq` is per run, starts at 1 and has no gaps, so `after=<last seq you saw>` resumes exactly: no gaps, no repeats.

**Per run, not per branch.** Every run's seq starts at 1, so "after seq 3" on a branch with two runs would be ambiguous. A branch-wide stream needs its own sequence; it is not built.

## Before the upgrade: ordinary HTTP errors

Everything that can be checked before upgrading is answered as `application/problem+json`, so a client gets an error it can act on, not a mysterious close.

| Status | Type | Meaning | SDK raises | SDK retries? |
|---|---|---|---|---|
| 401 | `unauthorized` | missing or wrong token | `Unauthorized` | no |
| 404 | `not-found` | no such run | `NotFound` | no |
| 422 | `validation` | `run` missing or malformed, `after` < 0 | `InvalidRequest` | no |
| 5xx, 429 | | server trouble | (kept internally) | yes, with backoff |

## After the upgrade: close codes

| Code | Sent when | What the client does |
|---|---|---|
| **1000** Normal | `run_finished` was sent: the stream is complete | stop |
| **1001** Going away | the server is shutting down | reconnect with backoff, `after=<last seq>` |
| **1011** Internal error | the server could not read the event log | reconnect with backoff |
| **4008** Slow consumer | a frame could not be written within the write deadline (10 s) | reconnect with backoff, `after=<last seq>` |
| *none* (1006) | the connection dropped with no close frame: network, proxy, or a slow consumer whose socket was too full for the close frame | reconnect with backoff, `after=<last seq>` |

**4008 is best effort.** A slow consumer is one whose socket buffer is full, so the close frame often cannot get through either. The server then just hangs up and the client sees 1006. Both mean the same thing: reconnect and resume.

## How the client reconnects

- Backoff with full jitter (the SDK's `RetryPolicy`), so many clients dropped together do not return together.
- The failure count resets whenever new events arrive. A long run with occasional drops never gives up; a dead server does, after `max_failures` tries in a row with no progress (default 10), with a `NetworkError` that says which seq to resume from.
- **Defence in depth.** A seq the client already has is dropped (`stats.duplicates_dropped`). A seq that skips ahead is never yielded: the client reconnects from its last good seq (`stats.gaps_detected`). A correct server makes both counters 0, and the chaos test asserts that, so the client's protection cannot hide a server bug.

## How the server stays safe

- **The event log is the truth; NOTIFY is a wake-up.** `events.Append` calls `pg_notify('graft_events', run_id)` inside its transaction. Postgres delivers it only at commit, after the rows are visible. Each connection then reads `seq > last` itself, in batches of 100. A missed notification costs at most the safety poll (2 s), never an event.
- **One LISTEN connection per process**, outside the pool. If it drops, the hub reconnects and wakes every subscriber, since notifications may have been missed in between.
- **Nothing can slow the scheduler.** The worker's only cost is one `pg_notify` per commit. The hub's fan-out is a non-blocking send on a 1-slot channel per subscriber (extra wake-ups coalesce). Each connection writes at its own pace under a per-frame deadline, and memory per connection is bounded by one batch.
- **Server timeouts are cleared on upgrade.** `http.Server`'s read and write deadlines survive a hijack and would cut every stream after 15 s; the hub's per-frame deadline replaces them.

## Proof

| Claim | Test |
|---|---|
| Replay equals the REST event log, every frame matches events.schema.json, close 1000 | `service/internal/api/stream_test.go` `TestStreamReplays...` |
| `after=5` starts at seq 6 | `TestResumeSendsStrictlyAfterTheGivenSeq` |
| Live events arrive through NOTIFY (safety poll set to one hour) | `TestLiveEventsArriveThroughNotifyNotPolling` |
| A stalled reader is disconnected; another reader and the worker are unaffected | `TestSlowConsumerIsDisconnectedAndBlocksNobody` |
| Shutdown closes with 1001 | `TestShutdownClosesWith1001` |
| Client: resume, dedupe, holes, 4008, typed handshake errors, give-up rule | `sdk/python/tests/unit/test_stream.py` |
| 200 drops during a 502-event run: client list equals server log, 0 duplicates, 0 gaps | `sdk/python/tests/chaos/test_stream.py` |

## Not built yet, and honest limits

- **Live is short today.** With the fake runner, a run's events are committed in one transaction, so most of a stream is replay. The real clock shell (v0.6) writes events as steps happen, making the live part long. The chaos test's drops fall mostly during replay delivery for this reason; the resume logic is the same either way.
- **Browsers cannot set an Authorization header on a websocket.** The console (v0.9) will need another way to authenticate (a short-lived token in the URL or a cookie).
- **No branch-wide stream** (see above).
