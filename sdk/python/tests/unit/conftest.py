# pyright: reportPrivateUsage=false
# Tests deliberately exercise internals (_base, _send, _retry); the library itself is checked strictly.
"""Run every client test against BOTH clients.

`make_client(handler)` returns a Graft, or an AsyncGraft driven through `Syncify`,
which runs each coroutine on one event loop and turns async iterators into sync
ones. Tests are written once, in sync style, and a behaviour difference between the
two clients fails a test instead of hiding.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from typing import Any, Protocol, cast

import httpx
import pytest

from graft import AsyncGraft, Graft
from graft._retry import RetryPolicy

from ..syncify import Syncify

Handler = Callable[[httpx.Request], httpx.Response]


async def _no_async_sleep(seconds: float) -> None:
    return None


class MakeClient(Protocol):
    kind: str

    def __call__(self, handler: Handler, **kwargs: Any) -> Graft: ...


@pytest.fixture(params=["sync", "async"])
def make_client(request: pytest.FixtureRequest) -> Iterator[MakeClient]:
    loop = asyncio.new_event_loop()
    kind = cast(str, request.param)

    def make(handler: Handler, **kwargs: Any) -> Graft:
        transport = httpx.MockTransport(handler)
        # Retries really happen in these tests, but never really sleep.
        kwargs.setdefault("retry", RetryPolicy(sleep=lambda s: None, async_sleep=_no_async_sleep))
        if kind == "sync":
            return Graft(token="t0ken", base_url="http://graft.test", transport=transport, **kwargs)
        client = AsyncGraft(token="t0ken", base_url="http://graft.test", transport=transport, **kwargs)
        # Statically a Graft: Syncify exposes the same methods, run to completion.
        return cast(Graft, Syncify(client, loop))

    setattr(make, "kind", kind)
    yield cast(MakeClient, make)
    loop.close()
