"""Realtime: one run's events over a websocket, resumed across drops.

    async for ev in g.stream(run.id):            # replays, then live; ends after run_finished
        print(ev.seq, ev.type, ev.step)

On any drop (network, proxy, 1001 shutdown, 1011, 4008 slow consumer) it reconnects
with `after=<last seq yielded>`, backing off with full jitter. The failure count
resets whenever events arrive, so a long run with occasional drops never gives up,
while a dead server does, after `max_failures` tries with no progress.

Defence in depth: a seq already seen is dropped (and counted), and a seq that skips
ahead is never yielded: the client reconnects from its last good seq instead. The
counters in `.stats` let tests prove the SERVER sent neither.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from urllib.parse import urlencode

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from ._retry import RetryPolicy
from .errors import NetworkError, from_response
from .models import Event

# Handshake statuses a reconnect cannot fix: raise them as typed errors at once.
FINAL_STATUSES = frozenset({400, 401, 403, 404, 422})


@dataclass
class StreamStats:
    """What happened on the wire. Exposed for tests, logs and dashboards."""

    connects: int = 0
    reconnects: int = 0
    duplicates_dropped: int = 0  # a correct server never causes these
    gaps_detected: int = 0  # nor these
    close_codes: list[int | None] = field(default_factory=lambda: list[int | None]())


class EventStream:
    """Async iterable of a run's events. Create it with `AsyncGraft.stream(...)`."""

    def __init__(self, base_url: str, token: str, run_id: str, after: int, retry: RetryPolicy,
                 max_failures: int = 10, open_timeout: float = 10.0) -> None:
        self.run_id = run_id
        self.last_seq = after
        self.stats = StreamStats()
        self._ws_base = "ws" + base_url.removeprefix("http")  # http -> ws, https -> wss
        self._token = token
        self._retry = retry
        self._max_failures = max_failures
        self._open_timeout = open_timeout

    def _url(self) -> str:
        return f"{self._ws_base}/v1/stream?{urlencode({'run': self.run_id, 'after': self.last_seq})}"

    def __aiter__(self) -> AsyncIterator[Event]:
        return self._events()

    async def _events(self) -> AsyncIterator[Event]:
        failures = 0
        while True:
            progressed = False
            code: int | None = None
            try:
                async with connect(self._url(), additional_headers={"Authorization": f"Bearer {self._token}"},
                                   open_timeout=self._open_timeout, max_size=1 << 20) as ws:
                    self.stats.connects += 1
                    async for raw in ws:
                        ev = Event.model_validate_json(raw)
                        if ev.seq <= self.last_seq:
                            self.stats.duplicates_dropped += 1
                            continue
                        if ev.seq != self.last_seq + 1:
                            # The server skipped something. Never yield past a hole:
                            # reconnect and ask again from the last good seq.
                            self.stats.gaps_detected += 1
                            break
                        self.last_seq = ev.seq
                        progressed, failures = True, 0
                        yield ev
                        if ev.type == "run_finished":
                            return
                    code = ws.close_code
            except InvalidStatus as e:
                resp = e.response
                as_http = httpx.Response(resp.status_code, headers=dict(resp.headers.raw_items()), content=bytes(resp.body or b""))
                as_http.read()  # materialise the body so the problem+json can be parsed
                err = from_response(as_http)
                if resp.status_code in FINAL_STATUSES:
                    raise err from None
                code = resp.status_code
            except ConnectionClosed as e:
                code = e.rcvd.code if e.rcvd else None  # None: dropped with no close frame (1006)
            except (OSError, TimeoutError, InvalidHandshake):
                code = None
            self.stats.close_codes.append(code)
            if not progressed:
                failures += 1
                if failures > self._max_failures:
                    raise NetworkError(f"Stream for {self.run_id} failed {failures} times in a row with no new "
                                       f"events (last close code {code}); last seq {self.last_seq}. "
                                       f"Resume later with after={self.last_seq}.")
            self.stats.reconnects += 1
            await self._retry.async_sleep(self._retry.delay(min(failures, 6)))


__all__ = ["EventStream", "StreamStats"]
