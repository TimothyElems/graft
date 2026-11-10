"""Drive the async client through a sync surface, so one test body runs on both clients.

Syncify wraps AsyncGraft (and its .branches / .runs namespaces): each coroutine runs to
completion on one event loop, and async iterators become sync iterators. Test-only.
"""
from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Iterator
from typing import Any


class _SyncIter:
    def __init__(self, it: AsyncIterator[Any], loop: asyncio.AbstractEventLoop) -> None:
        self._it, self._loop = it, loop

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        try:
            return self._loop.run_until_complete(self._it.__anext__())
        except StopAsyncIteration:
            raise StopIteration from None


class Syncify:
    """Wraps the async client (and its .branches / .runs namespaces) with a sync surface."""

    def __init__(self, target: object, loop: asyncio.AbstractEventLoop) -> None:
        self._target, self._loop = target, loop

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)
        if inspect.ismethod(attr):
            def call(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                if inspect.iscoroutine(result):
                    return self._loop.run_until_complete(result)
                if hasattr(result, "__aiter__"):
                    return _SyncIter(result.__aiter__(), self._loop)
                return result
            return call
        if type(attr).__module__.startswith("graft.") and type(attr).__name__.startswith("Async"):
            return Syncify(attr, self._loop)
        return attr
