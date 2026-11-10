# pyright: reportPrivateUsage=false
# Tests deliberately exercise internals (_base, _send, _retry); the library itself is checked strictly.
"""Retry with jitter against a flaky fake server, on both clients."""
from __future__ import annotations

import random
from collections.abc import Callable
from typing import Any

import httpx
import pytest

import graft
from graft import StepSpec
from graft._base import Request
from graft._retry import RetryPolicy, is_safe

from .conftest import MakeClient

OK_RUN: dict[str, Any] = {"id": "run_1", "branch_id": "br_1", "status": "queued", "created_at": "2026-11-10T09:00:00Z",
                          "finished_at": None, "steps": []}


def problem(status: int, kind: str) -> httpx.Response:
    return httpx.Response(status, json={"type": f"https://graft.dev/problems/{kind}", "title": kind, "status": status})


class Flaky:
    """Fails with each scripted failure in turn, then answers `ok`. Records every request."""

    def __init__(self, failures: list[Callable[[httpx.Request], httpx.Response]], ok: httpx.Response) -> None:
        self.failures, self.ok = list(failures), ok
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        if self.failures:
            return self.failures.pop(0)(req)
        return self.ok


def status(code: int, kind: str = "internal") -> Callable[[httpx.Request], httpx.Response]:
    return lambda req: problem(code, kind)


def drop(req: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection reset", request=req)


def timeout(req: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("read timed out", request=req)


class Recorder:
    def __init__(self) -> None:
        self.slept: list[float] = []

    def policy(self, **kw: Any) -> RetryPolicy:
        async def async_sleep(s: float) -> None:
            self.slept.append(s)
        return RetryPolicy(sleep=self.slept.append, async_sleep=async_sleep, **kw)


def test_get_is_retried_until_it_succeeds(make_client: MakeClient) -> None:
    rec, server = Recorder(), Flaky([status(503), drop], httpx.Response(200, json=OK_RUN))
    run = make_client(server, retry=rec.policy(rng=lambda: 1.0)).runs.get("run_1")
    assert run.id == "run_1" and len(server.requests) == 3
    assert rec.slept == [0.5, 1.0]  # rng=1.0 shows the ceiling: base * 2**attempt


def test_gives_up_after_max_retries(make_client: MakeClient) -> None:
    rec, server = Recorder(), Flaky([status(503)] * 10, httpx.Response(200, json=OK_RUN))
    with pytest.raises(graft.ServerError):
        make_client(server, retry=rec.policy(max_retries=2)).runs.get("run_1")
    assert len(server.requests) == 3 and len(rec.slept) == 2


def test_create_is_retried_with_the_same_idempotency_key(make_client: MakeClient) -> None:
    """The point of the key: the server sees ONE request, sent up to three times."""
    rec, server = Recorder(), Flaky([timeout, status(502)], httpx.Response(202, json=OK_RUN))
    make_client(server, retry=rec.policy()).runs.create("br_1", steps=[StepSpec(id="a")])
    keys = [r.headers.get("Idempotency-Key") for r in server.requests]
    assert len(keys) == 3 and len(set(keys)) == 1 and keys[0] is not None and keys[0].startswith("graft-py-")


def test_separate_creates_get_different_keys(make_client: MakeClient) -> None:
    server = Flaky([], httpx.Response(202, json=OK_RUN))
    g = make_client(server)
    g.runs.create("br_1", steps=[StepSpec(id="a")])
    g.runs.create("br_1", steps=[StepSpec(id="a")])
    k1, k2 = (r.headers["Idempotency-Key"] for r in server.requests)
    assert k1 != k2, "a key reused across calls would replay the first run instead of creating a second"


def test_caller_supplied_key_is_used(make_client: MakeClient) -> None:
    server = Flaky([], httpx.Response(202, json=OK_RUN))
    make_client(server).runs.create("br_1", steps=[StepSpec(id="a")], idempotency_key="order-42")
    assert server.requests[0].headers["Idempotency-Key"] == "order-42"


def test_post_without_a_key_is_never_retried(make_client: MakeClient) -> None:
    rec, server = Recorder(), Flaky([drop], httpx.Response(202, json=OK_RUN))
    g = make_client(server, retry=rec.policy())
    with pytest.raises(graft.NetworkError):
        g._send(Request("POST", "/v1/branches/br_1/runs", json={"steps": [{"id": "a"}]}))  # no key: unsafe
    assert len(server.requests) == 1 and rec.slept == []


@pytest.mark.parametrize("failure", [status(404, "not-found"), status(422, "validation"),
                                     status(409, "branch-name-taken"), status(401, "unauthorized")])
def test_errors_a_retry_cannot_fix_are_not_retried(make_client: MakeClient, failure: Callable[[httpx.Request], httpx.Response]) -> None:
    rec, server = Recorder(), Flaky([failure], httpx.Response(200, json=OK_RUN))
    with pytest.raises(graft.APIError):
        make_client(server, retry=rec.policy()).runs.get("run_1")
    assert len(server.requests) == 1 and rec.slept == []


def test_key_in_use_conflict_is_retried(make_client: MakeClient) -> None:
    rec, server = Recorder(), Flaky([status(409, "idempotency-key-in-use")], httpx.Response(202, json=OK_RUN))
    make_client(server, retry=rec.policy()).runs.create("br_1", steps=[StepSpec(id="a")])
    assert len(server.requests) == 2


def test_retry_after_is_honoured_exactly(make_client: MakeClient) -> None:
    rec = Recorder()
    server = Flaky([lambda req: httpx.Response(429, headers={"Retry-After": "3"})], httpx.Response(200, json=OK_RUN))
    make_client(server, retry=rec.policy()).runs.get("run_1")
    assert rec.slept == [3.0]


def test_max_retries_zero_turns_retries_off(make_client: MakeClient) -> None:
    server = Flaky([status(503)], httpx.Response(200, json=OK_RUN))
    with pytest.raises(graft.ServerError):
        make_client(server, retry=RetryPolicy(max_retries=0)).runs.get("run_1")
    assert len(server.requests) == 1


def test_full_jitter_stays_under_the_ceiling_and_varies() -> None:
    rng = random.Random(7)
    p = RetryPolicy(rng=rng.random, base=0.5, cap=8.0)
    for attempt in range(8):
        ceiling = min(8.0, 0.5 * 2 ** attempt)
        delays = [p.delay(attempt) for _ in range(200)]
        assert all(0.0 <= d <= ceiling for d in delays), attempt
        assert len({round(d, 6) for d in delays}) > 150, "jitter must spread clients out, not pick one value"
    assert RetryPolicy(rng=lambda: 1.0).delay(10) == 8.0  # capped
    assert RetryPolicy().delay(0, retry_after=999) == 60.0  # a server cannot make us sleep forever


def test_is_safe() -> None:
    assert is_safe(Request("GET", "/x"))
    assert is_safe(Request("POST", "/x", headers={"Idempotency-Key": "k"}))
    assert not is_safe(Request("POST", "/x"))
    assert not is_safe(Request("DELETE", "/x"))
