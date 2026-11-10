"""Sync client. Its async twin is _async_client.AsyncGraft; both share _build."""
from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from types import TracebackType

import httpx

from ._base import DEFAULT_TIMEOUT, Request, default_headers, resolve_config
from ._base import build as _build
from ._retry import RetryPolicy, is_retryable, is_safe, retry_after
from .errors import GraftError, NetworkError, RequestTimeout, from_response
from ._pagination import SyncPager, events_next
from .models import Branch, BranchPage, Event, EventPage, Health, Run, StepSpec


class Graft:
    """Client for one Graft service.

    >>> with Graft(token="...", base_url="http://localhost:8080") as g:  # doctest: +SKIP
    ...     br = g.branches.create("demo", name="feat-invoices")

    token and base_url fall back to the GRAFT_TOKEN and GRAFT_URL environment variables.
    """

    branches: Branches
    runs: Runs
    retry: RetryPolicy
    base_url: str

    def __init__(self, token: str | None = None, *, base_url: str | None = None,
                 timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
                 transport: httpx.BaseTransport | None = None, retry: RetryPolicy | None = None) -> None:
        token, self.base_url = resolve_config(token, base_url)
        self.retry = retry or RetryPolicy()
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout, transport=transport,
                                  headers=default_headers(token))
        self.branches = Branches(self._send)
        self.runs = Runs(self._send)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Graft:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        self.close()

    def health(self) -> Health:
        """Liveness and queue depth."""
        return Health.model_validate(self._send(_build.health()))

    def _send(self, req: Request) -> object:
        """Send once, retrying only when it is safe (see _retry) and might help."""
        attempt = 0
        while True:
            try:
                return self._send_once(req)
            except GraftError as err:
                if attempt >= self.retry.max_retries or not is_safe(req) or not is_retryable(err):
                    raise
                self.retry.sleep(self.retry.delay(attempt, retry_after(err)))
                attempt += 1

    def _send_once(self, req: Request) -> object:
        try:
            resp = self._http.request(req.method, req.path, json=req.json, params=dict(req.params),
                                      headers=dict(req.headers))
        except httpx.TimeoutException as e:
            raise RequestTimeout(f"{req.method} {req.path} timed out after the client timeout: {e}") from e
        except httpx.TransportError as e:
            raise NetworkError(f"Could not reach {self.base_url}: {e}") from e
        if resp.status_code >= 400:
            raise from_response(resp)
        return resp.json()


class Branches:
    def __init__(self, send: Callable[[Request], object]) -> None:
        self._send = send

    def create(self, project: str, *, name: str, git_ref: str | None = None, ttl_seconds: int | None = None,
               idempotency_key: str | None = None) -> Branch:
        """Create a short-lived branch. Raises Conflict if a live branch has this name."""
        return Branch.model_validate(self._send(_build.create_branch(project, name, git_ref, ttl_seconds, idempotency_key)))

    def get(self, branch_id: str) -> Branch:
        """Fetch one branch. Raises NotFound if it does not exist."""
        return Branch.model_validate(self._send(_build.get_branch(branch_id)))

    def list_page(self, project: str, *, cursor: str | None = None, limit: int = 50) -> BranchPage:
        """One page of a project's branches, newest first. Pass `next_cursor` back for the next page."""
        return BranchPage.model_validate(self._send(_build.list_branches(project, cursor, limit)))

    def list(self, project: str, *, page_size: int = 50) -> SyncPager[Branch, str]:
        """Every branch in a project, newest first, fetching pages as you iterate.

        >>> names = [b.name for b in g.branches.list("demo")]  # doctest: +SKIP
        """
        def fetch(cursor: str | None) -> tuple[list[Branch], str | None]:
            page = self.list_page(project, cursor=cursor, limit=page_size)
            return page.items, page.next_cursor
        return SyncPager(fetch)


class Runs:
    def __init__(self, send: Callable[[Request], object]) -> None:
        self._send = send

    def create(self, branch_id: str, *, steps: Sequence[StepSpec], concurrency: int | None = None,
               idempotency_key: str | None = None) -> Run:
        """Queue a pipeline run. Returns at once with status "queued"; use wait() for the result.

        Raises InvalidPipeline for a duplicate step, unknown dependency or cycle.
        """
        return Run.model_validate(self._send(_build.create_run(branch_id, steps, concurrency, idempotency_key)))

    def get(self, run_id: str) -> Run:
        """Fetch a run and its step results."""
        return Run.model_validate(self._send(_build.get_run(run_id)))

    def events(self, run_id: str, *, after: int = 0, page_size: int = 200) -> SyncPager[Event, int]:
        """A run's events in seq order, from seq > `after`, fetching pages as you iterate."""
        def fetch(cursor: int | None) -> tuple[list[Event], int | None]:
            start = after if cursor is None else cursor
            page = EventPage.model_validate(self._send(_build.list_events(run_id, start, page_size)))
            return page.items, events_next(start, page.last_seq, len(page.items), page_size)
        return SyncPager(fetch)

    def wait(self, run_id: str, *, timeout: float = 60.0, poll: float = 0.05) -> Run:
        """Poll until the run is succeeded, failed or cancelled.

        Raises TimeoutError if it is still queued or running after `timeout` seconds.
        Polling backs off from `poll` up to 2 s between requests.
        """
        deadline = time.monotonic() + timeout
        delay = poll
        while True:
            run = self.get(run_id)
            if run.done:
                return run
            if time.monotonic() + delay > deadline:
                raise TimeoutError(f"Run {run_id} still {run.status} after {timeout}s.")
            time.sleep(delay)
            delay = min(delay * 2, 2.0)
