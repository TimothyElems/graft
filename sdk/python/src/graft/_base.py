"""Everything the sync and async clients share. No I/O happens here.

Each API call is split into: build a Request (pure), send it (client-specific),
parse the response (pure). The two clients differ only in how they send and sleep,
so a behaviour can't be fixed in one and forgotten in the other.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import httpx

from ._models import CreateBranch, CreateRun
from .models import StepSpec

__version__ = "0.4.0.dev0"

DEFAULT_BASE_URL = "http://localhost:8080"
# Explicit, documented timeouts. A client with no read timeout hangs forever on a
# server that accepts the connection and never answers (notebook case 08).
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0)

JSONBody = Mapping[str, object]


@dataclass(frozen=True)
class Request:
    method: str
    path: str
    json: JSONBody | None = None
    params: Mapping[str, str | int] = field(default_factory=lambda: dict[str, "str | int"]())
    headers: Mapping[str, str] = field(default_factory=lambda: dict[str, str]())


def resolve_config(token: str | None, base_url: str | None) -> tuple[str, str]:
    token = token or os.environ.get("GRAFT_TOKEN")
    if not token:
        raise ValueError("No token. Pass Graft(token=...) or set GRAFT_TOKEN.")
    url = (base_url or os.environ.get("GRAFT_URL") or DEFAULT_BASE_URL).rstrip("/")
    return token, url


def default_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "User-Agent": f"graft-python/{__version__}",
            "Accept": "application/json"}


def new_idempotency_key() -> str:
    """A fresh key per call. Reused across that call's retries, never across calls."""
    return f"graft-py-{uuid.uuid4().hex}"


# ---------------------------------------------------------------- request builders

def health() -> Request:
    return Request("GET", "/healthz")


def create_branch(project: str, name: str, git_ref: str | None, ttl_seconds: int | None,
                  idempotency_key: str | None) -> Request:
    body = CreateBranch.model_validate(
        {k: v for k, v in {"name": name, "git_ref": git_ref, "ttl_seconds": ttl_seconds}.items() if v is not None})
    return Request("POST", f"/v1/projects/{project}/branches",
                   json=body.model_dump(mode="json", exclude_unset=True),
                   headers={"Idempotency-Key": idempotency_key or new_idempotency_key()})


def get_branch(branch_id: str) -> Request:
    return Request("GET", f"/v1/branches/{branch_id}")


def list_branches(project: str, cursor: str | None, limit: int) -> Request:
    params: dict[str, str | int] = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    return Request("GET", f"/v1/projects/{project}/branches", params=params)


def create_run(branch_id: str, steps: Sequence[StepSpec], concurrency: int | None,
               idempotency_key: str | None) -> Request:
    run = CreateRun(steps=list(steps)) if concurrency is None else CreateRun(steps=list(steps), concurrency=concurrency)
    # exclude_unset: send what the caller set, including an explicit 0 (notebook case 04),
    # and let the server apply its own defaults for the rest.
    return Request("POST", f"/v1/branches/{branch_id}/runs",
                   json=run.model_dump(mode="json", exclude_unset=True),
                   headers={"Idempotency-Key": idempotency_key or new_idempotency_key()})


def get_run(run_id: str) -> Request:
    return Request("GET", f"/v1/runs/{run_id}")


def list_events(run_id: str, after: int, limit: int) -> Request:
    return Request("GET", f"/v1/runs/{run_id}/events", params={"after": after, "limit": limit})


class _Builders:
    """The request builders as one object, so clients import a single public name."""
    health = staticmethod(health)
    create_branch = staticmethod(create_branch)
    get_branch = staticmethod(get_branch)
    list_branches = staticmethod(list_branches)
    create_run = staticmethod(create_run)
    get_run = staticmethod(get_run)
    list_events = staticmethod(list_events)


build = _Builders()
