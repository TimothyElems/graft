"""Chaos: a proxy cuts the stream 200 times during a 500-event run. Nothing may be lost or repeated.

Runs only with GRAFT_E2E=1 against a real graftd (make e2e locally; CI runs it after
`docker compose up`). The proxy sits between the SDK and graftd and aborts the TCP
connection a random 100-500 bytes after each websocket handshake, 200 times, then
lets the stream finish. The client's events must EQUAL the server's event log:
same events, same order, every field, which proves no gaps and no repeats, and an
equal count alone would not (a duplicate plus a gap is still the right count).
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import pytest

from graft import AsyncGraft, Event
from graft._retry import RetryPolicy

pytestmark = pytest.mark.skipif(os.environ.get("GRAFT_E2E") != "1", reason="set GRAFT_E2E=1 with graftd running")

URL = os.environ.get("GRAFT_URL", "http://localhost:8080")
TOKEN = os.environ.get("GRAFT_TOKEN", "local-dev-token-change-me")
SCHEMA = Path(__file__).resolve().parents[4] / "contract" / "events.schema.json"
DROPS = 200


class ChaosProxy:
    """TCP proxy that aborts connections after a random number of post-handshake bytes."""

    def __init__(self, upstream: tuple[str, int], drops: int, seed: int = 5) -> None:
        self.upstream, self.remaining, self.rng = upstream, drops, random.Random(seed)
        self.drops = 0
        self.connections = 0

    async def handle(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter) -> None:
        self.connections += 1
        sr, sw = await asyncio.open_connection(*self.upstream)
        cut_after = self.rng.randint(100, 500) if self.remaining > 0 else None

        async def client_to_server() -> None:
            try:
                while data := await cr.read(4096):
                    sw.write(data)
                    await sw.drain()
            except (ConnectionError, OSError):
                pass

        async def server_to_client() -> None:
            seen, head_done, buf = 0, False, b""
            try:
                while data := await sr.read(4096):
                    if cut_after is not None:
                        if not head_done:
                            buf += data
                            if b"\r\n\r\n" in buf:  # end of the 101 response: count frames from here
                                head_done = True
                                seen = len(buf.split(b"\r\n\r\n", 1)[1])
                        else:
                            seen += len(data)
                        if head_done and seen >= cut_after:
                            keep = len(data) - (seen - cut_after)
                            cw.write(data[:max(keep, 0)])
                            self.remaining -= 1
                            self.drops += 1
                            cw.transport.abort()  # no close frame: like Wi-Fi or a proxy timing out
                            sw.transport.abort()
                            return
                    cw.write(data)
                    await cw.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                cw.close()

        await asyncio.gather(client_to_server(), server_to_client())


def big_run_steps() -> list[dict[str, Any]]:
    # 50 steps x (4 failures + 1 success) = 500 kernel events, plus run_started = 502 events
    return [{"id": f"s{i:02d}", "max_attempts": 5,
             "command": ["graft-fake", "fail:1", "fail:1", "fail:1", "fail:1", "ok:1"]} for i in range(50)]


def server_log(raw: httpx.Client, run_id: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    after = 0
    while True:
        page = raw.get(f"/v1/runs/{run_id}/events", params={"after": after, "limit": 200}).json()
        out += page["items"]
        if not page["items"]:
            return out
        after = page["last_seq"]


def test_200_drops_during_a_500_event_run_lose_and_repeat_nothing() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    validator = jsonschema.Draft202012Validator(json.loads(SCHEMA.read_text()), format_checker=jsonschema.FormatChecker())
    up = urlparse(URL)

    async def main() -> tuple[list[Event], Any, ChaosProxy, str]:
        proxy = ChaosProxy((up.hostname or "127.0.0.1", up.port or 80), DROPS)
        server = await asyncio.start_server(proxy.handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        async with server, httpx.AsyncClient(base_url=URL, headers={"Authorization": f"Bearer {TOKEN}"}) as raw:
            br = (await raw.post("/v1/projects/demo/branches", json={"name": f"chaos-{uuid.uuid4().hex[:8]}"})).json()
            run = (await raw.post(f"/v1/branches/{br['id']}/runs", json={"concurrency": 16, "steps": big_run_steps()})).json()
            # Subscribe at once, through the proxy, while the run may still be queued.
            policy = RetryPolicy(base=0.002, cap=0.05)
            async with AsyncGraft(token=TOKEN, base_url=f"http://127.0.0.1:{port}", retry=policy) as g:
                stream = g.stream(run["id"], max_failures=50)
                got = [e async for e in stream]
        return got, stream.stats, proxy, run["id"]

    got, stats, proxy, run_id = asyncio.run(main())
    with httpx.Client(base_url=URL, headers={"Authorization": f"Bearer {TOKEN}"}) as raw:
        log = server_log(raw, run_id)

    print(f"\nproxy cut the stream {proxy.drops} times over {proxy.connections} connections; "
          f"client reconnects={stats.reconnects} received={len(got)} server log={len(log)} "
          f"duplicates_dropped={stats.duplicates_dropped} gaps_detected={stats.gaps_detected}")
    assert proxy.drops == DROPS
    assert len(log) == 502
    received = [e.model_dump(mode="json") for e in got]
    expected = [Event.model_validate(e).model_dump(mode="json") for e in log]
    assert received == expected, "the client's events must equal the server's log: same events, same order"
    assert stats.duplicates_dropped == 0, "the server resent an event (resume used >= ?)"
    assert stats.gaps_detected == 0, "the server skipped an event"
    for e in received:
        errors = [x.message for x in validator.iter_errors(e)]
        assert not errors, (e["seq"], errors)
