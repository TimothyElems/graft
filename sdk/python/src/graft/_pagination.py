"""Iterators that fetch the next page for you, lazily, sync and async.

    for b in g.branches.list("demo"):            # sync
    async for b in g.branches.list("demo"):      # async

A page is requested only when iteration reaches it. A server that hands back the
same cursor twice raises PaginationError instead of looping forever (the bug a
`created_at <=` cursor causes, notebook case 07).
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from typing import Generic, TypeVar

from .errors import GraftError

T = TypeVar("T")
C = TypeVar("C")  # the cursor's type: an opaque string for branches, a seq (int) for events


class PaginationError(GraftError):
    """The server returned a cursor it had already returned. Report it with the request ids."""


def _check(seen: set[C], cursor: C) -> None:
    if cursor in seen:
        raise PaginationError(f"The server repeated cursor {cursor!r}; stopping instead of looping forever.")
    seen.add(cursor)


class SyncPager(Generic[T, C]):
    """Iterate every item across pages. `fetch(cursor)` returns (items, next cursor or None)."""

    def __init__(self, fetch: Callable[[C | None], tuple[Sequence[T], C | None]], first: C | None = None) -> None:
        self._fetch = fetch
        self._first = first

    def __iter__(self) -> Iterator[T]:
        cursor, seen = self._first, set[C]()
        while True:
            items, nxt = self._fetch(cursor)
            yield from items
            if nxt is None:
                return
            _check(seen, nxt)
            cursor = nxt


class AsyncPager(Generic[T, C]):
    """Async twin of SyncPager: `async for item in pager`."""

    def __init__(self, fetch: Callable[[C | None], Awaitable[tuple[Sequence[T], C | None]]], first: C | None = None) -> None:
        self._fetch = fetch
        self._first = first

    async def __aiter__(self) -> AsyncIterator[T]:
        cursor, seen = self._first, set[C]()
        while True:
            items, nxt = await self._fetch(cursor)
            for item in items:
                yield item
            if nxt is None:
                return
            _check(seen, nxt)
            cursor = nxt


def events_next(after: int, last_seq: int, got: int, page_size: int) -> int | None:
    """Events page by seq. The contract has no 'more' flag, so a short page means the end."""
    return last_seq if got == page_size and last_seq > after else None
