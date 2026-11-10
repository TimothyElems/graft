# pyright: reportPrivateUsage=false
"""EventStream against a real in-process websocket server that misbehaves on purpose."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.http11 import Request, Response

import graft
from graft import AsyncGraft
from graft._retry import RetryPolicy


def event(seq: int, last: int) -> str:
    kind = "run_finished" if seq == last else ("run_started" if seq == 1 else "step_started")
    return json.dumps({"id": f"evt_{seq}", "seq": seq, "run_id": "run_1", "branch_id": "br_1",
                       "at": "2026-11-10T09:00:00Z", "type": kind, "at_ms": seq,
                       "step": None if kind.startswith("run") else "a", "attempt": 0 if kind.startswith("run") else 1,
                       "reason": "succeeded" if kind == "run_finished" else None})


class FakeServer:
    """Serves events 1..n for run_1. Knobs make it drop, close with a code, resend, or skip."""

    def __init__(self, n: int, drop_every: int = 0, close_code: int | None = None, ge_bug: bool = False,
                 skip_once: int = 0, reject: int = 0, max_drops: int = 10**9, empty_every_other: bool = False) -> None:
        self.empty_every_other = empty_every_other
        self.n, self.drop_every, self.close_code, self.ge_bug = n, drop_every, close_code, ge_bug
        self.skip_once, self.reject, self.max_drops = skip_once, reject, max_drops
        self.afters: list[int] = []
        self.drops = 0

    def check(self, conn: ServerConnection, request: Request) -> Response | None:
        if request.headers.get("Authorization") != "Bearer t0ken":
            return conn.respond(401, json.dumps({"type": "https://graft.dev/problems/unauthorized", "title": "no", "status": 401}))
        if self.reject:
            kind = "not-found" if self.reject == 404 else "validation"
            body = json.dumps({"type": f"https://graft.dev/problems/{kind}", "title": "Rejected",
                               "status": self.reject, "request_id": "req_ws"})
            resp = conn.respond(self.reject, body)
            resp.headers = Headers([("Content-Type", "application/problem+json"), ("Content-Length", str(len(body)))])
            return resp
        return None

    async def handler(self, ws: ServerConnection) -> None:
        q = parse_qs(urlparse(ws.request.path if ws.request else "").query)
        after = int(q.get("after", ["0"])[0])
        self.afters.append(after)
        if self.empty_every_other and len(self.afters) % 2 == 1:
            ws.transport.abort()  # a connection that delivers nothing
            return
        start = after if self.ge_bug and after > 0 else after + 1  # ge_bug: resend `after` (seq >= after)
        sent = 0
        for seq in range(start, self.n + 1):
            if self.skip_once and seq == self.skip_once:
                self.skip_once = 0
                continue  # a server bug: a hole in the stream
            await ws.send(event(seq, self.n))
            sent += 1
            if self.drop_every and sent >= self.drop_every and seq < self.n and self.drops < self.max_drops:
                self.drops += 1
                if self.close_code:
                    await ws.close(self.close_code, "slow consumer")
                else:
                    ws.transport.abort()  # no close frame: like a proxy or Wi-Fi dropping the TCP connection
                return
        await ws.close(1000, "run finished")


def run(server: FakeServer, consume: Callable[[AsyncGraft], Awaitable[Any]], slept: list[float] | None = None) -> Any:
    async def main() -> Any:
        # Explicit Any, test-only: websockets' Server class is only partly typed for pyright on
        # Python < 3.13 (reproduced on 3.12 with websockets 17.4). The SDK itself only uses
        # websockets' client, which type-checks strictly on every supported version.
        srv = cast(Any, await serve(server.handler, "127.0.0.1", 0, process_request=server.check))
        try:
            port: int = next(iter(srv.sockets)).getsockname()[1]

            async def no_sleep(s: float) -> None:
                if slept is not None:
                    slept.append(s)

            policy = RetryPolicy(base=0.01, async_sleep=no_sleep)
            async with AsyncGraft(token="t0ken", base_url=f"http://127.0.0.1:{port}", retry=policy) as g:
                return await consume(g)
        finally:
            srv.close()
            await srv.wait_closed()
    return asyncio.run(main())


async def collect(g: AsyncGraft, after: int = 0, max_failures: int = 10) -> tuple[list[int], graft.EventStream]:
    s = g.stream("run_1", after=after, max_failures=max_failures)
    return [e.seq async for e in s], s


def test_plain_stream_ends_after_run_finished() -> None:
    seqs, s = run(FakeServer(12), collect)
    assert seqs == list(range(1, 13)) and s.stats.reconnects == 0 and s.stats.connects == 1


def test_drops_resume_from_the_last_seq_with_no_gaps_or_repeats() -> None:
    server = FakeServer(50, drop_every=3)
    slept: list[float] = []
    seqs, s = run(server, collect, slept)
    assert seqs == list(range(1, 51))
    assert server.afters == list(range(0, 49, 3)), "each reconnect must ask for after=<last seq seen>"
    assert s.stats.reconnects == server.drops == 16 and s.stats.duplicates_dropped == 0 and s.stats.gaps_detected == 0
    assert all(c is None for c in s.stats.close_codes), "abort() sends no close frame: the client sees 1006 (None)"
    assert len(slept) == 16  # it backs off before each reconnect


def test_slow_consumer_close_4008_reconnects() -> None:
    server = FakeServer(10, drop_every=4, close_code=4008)
    seqs, s = run(server, collect)
    assert seqs == list(range(1, 11)) and s.stats.close_codes == [4008, 4008]


def test_a_server_that_resends_is_deduplicated_and_counted() -> None:
    """Notebook case 02 on the server side (seq >= after): the client still yields each event once."""
    server = FakeServer(20, drop_every=5, ge_bug=True)
    seqs, s = run(server, collect)
    assert seqs == list(range(1, 21))
    assert s.stats.duplicates_dropped == server.drops > 0


def test_a_hole_is_never_yielded_past() -> None:
    server = FakeServer(10, skip_once=4)
    seqs, s = run(server, collect)
    assert seqs == list(range(1, 11)) and s.stats.gaps_detected == 1
    assert server.afters == [0, 3], "on a hole, reconnect from the last good seq"


def test_resume_from_a_given_seq() -> None:
    seqs, _ = run(FakeServer(10), lambda g: collect(g, after=7))
    assert seqs == [8, 9, 10]


@pytest.mark.parametrize("status,cls", [(404, graft.NotFound), (422, graft.InvalidRequest)])
def test_handshake_errors_are_typed_and_not_retried(status: int, cls: type[Exception]) -> None:
    server = FakeServer(5, reject=status)
    with pytest.raises(cls) as ei:
        run(server, collect)
    assert getattr(ei.value, "request_id", None) == "req_ws" and server.afters == []


def test_wrong_token_is_unauthorized() -> None:
    async def consume(g: AsyncGraft) -> list[int]:
        g._token = "wrong"
        return [e.seq async for e in g.stream("run_1")]
    with pytest.raises(graft.Unauthorized):
        run(FakeServer(5), consume)


def test_gives_up_after_repeated_failures_with_no_progress() -> None:
    server = FakeServer(10, drop_every=1)

    async def consume(g: AsyncGraft) -> list[int]:
        # Each connection delivers 1 event, so there IS progress: it must keep going to the end.
        return [e.seq async for e in g.stream("run_1", max_failures=2)]
    assert run(server, consume) == list(range(1, 11))

    dead = FakeServer(10, drop_every=1, max_drops=10**9)
    dead.n = 0  # sends nothing, closes at once: no progress ever

    async def consume_dead(g: AsyncGraft) -> list[int]:
        return [e.seq async for e in g.stream("run_1", max_failures=3)]
    with pytest.raises(graft.NetworkError, match="no new events"):
        run(dead, consume_dead)
    assert len(dead.afters) == 4  # the first try + 3 failures allowed, then give up


def test_failures_reset_whenever_events_arrive() -> None:
    """Empty, 1 event, empty, 1 event, ...: never two failures in a row, so it must not give up,
    even with max_failures=1. If failures added up across good connections, it would."""
    server = FakeServer(8, drop_every=1, empty_every_other=True)

    async def consume(g: AsyncGraft) -> list[int]:
        return [e.seq async for e in g.stream("run_1", max_failures=1)]
    assert run(server, consume) == list(range(1, 9))
