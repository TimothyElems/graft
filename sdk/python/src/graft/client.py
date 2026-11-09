"""Sync client for the Graft API."""
from __future__ import annotations

import os
import time
from types import TracebackType
from typing import Any, Sequence

import httpx

from .errors import APIConnectionError, APITimeoutError, from_response
from .models import Branch, Health, Run, StepSpec

__version__ = "0.2.0.dev0"

DEFAULT_BASE_URL = "http://localhost:8080"
# Explicit, documented timeouts. A client with no read timeout hangs forever on a
# server that accepts the connection and never answers (notebook case 08).
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0)


class Graft:
    """Client for one Graft service.

    >>> with Graft(token="...", base_url="http://localhost:8080") as g:  # doctest: +SKIP
    ...     br = g.branches.create("demo", name="feat-invoices")

    token and base_url fall back to the GRAFT_TOKEN and GRAFT_URL environment variables.
    """

    def __init__(self, token: str | None = None, *, base_url: str | None = None,
                 timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
                 transport: httpx.BaseTransport | None = None) -> None:
        token = token or os.environ.get("GRAFT_TOKEN")
        if not token:
            raise ValueError("No token. Pass Graft(token=...) or set GRAFT_TOKEN.")
        self.base_url = (base_url or os.environ.get("GRAFT_URL") or DEFAULT_BASE_URL).rstrip("/")
        self._http = httpx.Client(
            base_url=self.base_url, timeout=timeout, transport=transport,
            headers={"Authorization": f"Bearer {token}", "User-Agent": f"graft-python/{__version__}",
                     "Accept": "application/json"},
        )
        self.branches = Branches(self)
        self.runs = Runs(self)

    # -- lifecycle
    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Graft:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        self.close()

    # -- calls
    def health(self) -> Health:
        """Liveness and queue depth. Needs no token on the server side."""
        return Health.from_json(self._request("GET", "/healthz"))

    def _request(self, method: str, path: str, json: Any = None) -> Any:
        try:
            resp = self._http.request(method, path, json=json)
        except httpx.TimeoutException as e:
            raise APITimeoutError(f"{method} {path} timed out after the client timeout: {e}") from e
        except httpx.TransportError as e:
            raise APIConnectionError(f"Could not reach {self.base_url}: {e}") from e
        if resp.status_code >= 400:
            try:
                body = resp.json()
            except ValueError:
                body = {"title": resp.reason_phrase, "detail": resp.text[:200]}
            raise from_response(resp.status_code, body, resp.headers.get("X-Request-Id"))
        return resp.json()


class Branches:
    def __init__(self, client: Graft) -> None:
        self._c = client

    def create(self, project: str, *, name: str, git_ref: str | None = None,
               ttl_seconds: int | None = None) -> Branch:
        """Create a short-lived branch in a project. Raises ConflictError if the name is taken."""
        body: dict[str, Any] = {"name": name}
        if git_ref is not None:
            body["git_ref"] = git_ref
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds
        return Branch.from_json(self._c._request("POST", f"/v1/projects/{project}/branches", body))

    def get(self, branch_id: str) -> Branch:
        """Fetch one branch. Raises NotFoundError if it does not exist."""
        return Branch.from_json(self._c._request("GET", f"/v1/branches/{branch_id}"))


class Runs:
    def __init__(self, client: Graft) -> None:
        self._c = client

    def create(self, branch_id: str, *, steps: Sequence[StepSpec], concurrency: int | None = None) -> Run:
        """Queue a pipeline run. Returns at once with status "queued"; use wait() for the result.

        Raises InvalidPipelineError for a duplicate step, unknown dependency or cycle.
        """
        body: dict[str, Any] = {"steps": [s.to_json() for s in steps]}
        if concurrency is not None:
            body["concurrency"] = concurrency
        return Run.from_json(self._c._request("POST", f"/v1/branches/{branch_id}/runs", body))

    def get(self, run_id: str) -> Run:
        """Fetch a run and its step results."""
        return Run.from_json(self._c._request("GET", f"/v1/runs/{run_id}"))

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
