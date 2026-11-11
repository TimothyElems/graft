"""Async client. The twin of client.Graft: same methods, same arguments, same _build."""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from types import TracebackType

import httpx

from ._base import DEFAULT_TIMEOUT, Request, default_headers, resolve_config
from ._base import build as _build
from ._retry import RetryPolicy, is_retryable, is_safe, retry_after
from ._stream import EventStream
from .errors import GraftError, NetworkError, RequestTimeout, from_response
from ._pagination import AsyncPager, events_next
from .models import Branch, BranchPage, Event, EventPage, Health, Run, StepSpec


class AsyncGraft:
    """Async client for one Graft service.

    >>> async with AsyncGraft(token="...") as g:  # doctest: +SKIP
    ...     br = await g.branches.create("demo", name="feat-invoices")
    """

    branches: AsyncBranches
    runs: AsyncRuns
    retry: RetryPolicy
    base_url: str

    def __init__(self, token: str | None = None, *, base_url: str | None = None,
                 timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
                 transport: httpx.AsyncBaseTransport | None = None, retry: RetryPolicy | None = None) -> None:
        token, self.base_url = resolve_config(token, base_url)
        self._token = token
        self.retry = retry or RetryPolicy()
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, transport=transport,
                                       headers=default_headers(token))
        self.branches = AsyncBranches(self._send)
        self.runs = AsyncRuns(self._send)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncGraft:
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                        tb: TracebackType | None) -> None:
        await self.aclose()

    def stream(self, run_id: str, *, after: int = 0, max_failures: int = 10) -> EventStream:
        """A run's events over a websocket: replayed after `after`, then live, ending after run_finished.

        Reconnects on drops with backoff and jitter, resuming from the last seq it yielded.
        Async only: a blocking iterator would tie up a thread for the whole run (the sync
        client has runs.events() for polling).

        >>> async for ev in g.stream(run.id):  # doctest: +SKIP
        ...     print(ev.seq, ev.type)
        """
        return EventStream(self.base_url, self._token, run_id, after, self.retry, max_failures=max_failures)

    async def health(self) -> Health:
        """Liveness and queue depth."""
        return Health.model_validate(await self._send(_build.health()))

    async def _send(self, req: Request) -> object:
        """Send once, retrying only when it is safe (see _retry) and might help."""
        attempt = 0
        while True:
            try:
                return await self._send_once(req)
            except GraftError as err:
                if attempt >= self.retry.max_retries or not is_safe(req) or not is_retryable(err):
                    raise
                await self.retry.async_sleep(self.retry.delay(attempt, retry_after(err)))
                attempt += 1

    async def _send_once(self, req: Request) -> object:
        try:
            resp = await self._http.request(req.method, req.path, json=req.json, params=dict(req.params),
                                            headers=dict(req.headers))
        except httpx.TimeoutException as e:
            raise RequestTimeout(f"{req.method} {req.path} timed out after the client timeout: {e}") from e
        except httpx.TransportError as e:
            raise NetworkError(f"Could not reach {self.base_url}: {e}") from e
        if resp.status_code >= 400:
            raise from_response(resp)
        return resp.content if req.raw else resp.json()


class AsyncBranches:
    def __init__(self, send: Callable[[Request], Awaitable[object]]) -> None:
        self._send = send

    async def create(self, project: str, *, name: str, git_ref: str | None = None, ttl_seconds: int | None = None,
                     idempotency_key: str | None = None) -> Branch:
        """Create a short-lived branch. Raises Conflict if a live branch has this name."""
        return Branch.model_validate(await self._send(_build.create_branch(project, name, git_ref, ttl_seconds, idempotency_key)))

    async def get(self, branch_id: str) -> Branch:
        """Fetch one branch. Raises NotFound if it does not exist."""
        return Branch.model_validate(await self._send(_build.get_branch(branch_id)))

    async def list_page(self, project: str, *, cursor: str | None = None, limit: int = 50) -> BranchPage:
        """One page of a project's branches, newest first. Pass `next_cursor` back for the next page."""
        return BranchPage.model_validate(await self._send(_build.list_branches(project, cursor, limit)))

    def list(self, project: str, *, page_size: int = 50) -> AsyncPager[Branch, str]:
        """Every branch in a project, newest first: `async for b in g.branches.list("demo")`."""
        async def fetch(cursor: str | None) -> tuple[list[Branch], str | None]:
            page = await self.list_page(project, cursor=cursor, limit=page_size)
            return page.items, page.next_cursor
        return AsyncPager(fetch)


class AsyncRuns:
    def __init__(self, send: Callable[[Request], Awaitable[object]]) -> None:
        self._send = send

    async def create(self, branch_id: str, *, steps: Sequence[StepSpec], concurrency: int | None = None,
                     idempotency_key: str | None = None) -> Run:
        """Queue a pipeline run. Returns at once with status "queued"; use wait() for the result."""
        return Run.model_validate(await self._send(_build.create_run(branch_id, steps, concurrency, idempotency_key)))

    async def get(self, run_id: str) -> Run:
        """Fetch a run and its step results."""
        return Run.model_validate(await self._send(_build.get_run(run_id)))

    async def cancel(self, run_id: str) -> Run:
        """Ask the service to cancel a run; returns it as it is now (202: the cancel is
        accepted; the run reaches "cancelled" shortly after). Safe to repeat, and on a
        finished run it changes nothing. Retried on network errors and 5xx."""
        return Run.model_validate(await self._send(_build.cancel_run(run_id)))

    async def bundle(self, run_id: str) -> bytes:
        """The run's support bundle (.tar.gz, secrets removed by the server). See `graft doctor`."""
        return _build.as_bytes(await self._send(_build.run_bundle(run_id)))

    def events(self, run_id: str, *, after: int = 0, page_size: int = 200) -> AsyncPager[Event, int]:
        """A run's events in seq order, from seq > `after`: `async for e in g.runs.events(run_id)`."""
        async def fetch(cursor: int | None) -> tuple[list[Event], int | None]:
            start = after if cursor is None else cursor
            page = EventPage.model_validate(await self._send(_build.list_events(run_id, start, page_size)))
            return page.items, events_next(start, page.last_seq, len(page.items), page_size)
        return AsyncPager(fetch)

    async def wait(self, run_id: str, *, timeout: float = 60.0, poll: float = 0.05) -> Run:
        """Poll until the run is succeeded, failed or cancelled. Raises TimeoutError after `timeout` seconds."""
        deadline = time.monotonic() + timeout
        delay = poll
        while True:
            run = await self.get(run_id)
            if run.done:
                return run
            if time.monotonic() + delay > deadline:
                raise TimeoutError(f"Run {run_id} still {run.status} after {timeout}s.")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 2.0)
