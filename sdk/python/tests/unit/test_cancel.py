"""runs.cancel: in the contract and the service since v0.5; in the SDK since v0.7."""
from __future__ import annotations

import httpx

from .conftest import MakeClient
from .test_client import RUN_QUEUED, problem


def test_cancel_posts_and_returns_the_run(make_client: MakeClient) -> None:
    seen: list[str] = []

    def handle(req: httpx.Request) -> httpx.Response:
        seen.append(f"{req.method} {req.url.path}")
        return httpx.Response(202, json={**RUN_QUEUED, "status": "cancelled"})

    run = make_client(handle).runs.cancel("run_1")
    assert seen == ["POST /v1/runs/run_1/cancel"]
    assert run.status == "cancelled"


def test_cancel_is_retried_because_repeating_it_is_safe(make_client: MakeClient) -> None:
    """A POST, but the contract says "Safe to repeat": a cancel that times out or
    gets a 503 is sent again. (A create is retried only with an Idempotency-Key.)"""
    calls = 0

    def handle(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return problem(503, "unavailable")
        return httpx.Response(202, json=RUN_QUEUED)

    make_client(handle).runs.cancel("run_1")
    assert calls == 2
