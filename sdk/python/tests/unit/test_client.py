# pyright: reportPrivateUsage=false
# Tests deliberately exercise internals (_base, _send, _retry); the library itself is checked strictly.
"""SDK behaviour against a fake transport. Every test runs on the sync AND the async client."""
from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

import graft
from graft import _base
from graft import Graft, StepSpec

from .conftest import MakeClient

RUN_QUEUED: dict[str, Any] = {
    "id": "run_1", "branch_id": "br_1", "status": "queued", "created_at": "2026-11-09T10:00:00Z",
    "finished_at": None,
    "steps": [{"id": "a", "status": "pending", "attempts": 0, "started_ms": None, "finished_ms": None, "reason": None}]}
RUN_DONE: dict[str, Any] = {
    **RUN_QUEUED, "status": "succeeded", "finished_at": "2026-11-09T10:00:01Z",
    "steps": [{"id": "a", "status": "succeeded", "attempts": 1, "started_ms": 0, "finished_ms": 10, "reason": None}]}
BRANCH: dict[str, Any] = {"id": "br_1", "project": "demo", "name": "feat-x", "status": "ready", "git_ref": None,
                          "created_at": "2026-11-09T10:00:00Z", "expires_at": "2026-11-10T10:00:00Z"}


def problem(status: int, kind: str, detail: str = "d", rid: str = "req_abc") -> httpx.Response:
    return httpx.Response(status, headers={"Content-Type": "application/problem+json"}, json={
        "type": f"https://graft.dev/problems/{kind}", "title": kind.title(), "status": status,
        "detail": detail, "request_id": rid})


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []

    def fake_sleep(s: float) -> None:
        slept.append(s)

    async def fake_async_sleep(s: float) -> None:
        slept.append(s)

    monkeypatch.setattr("time.sleep", fake_sleep)
    monkeypatch.setattr("graft._async_client.asyncio.sleep", fake_async_sleep)
    return slept


# ---------------------------------------------------------------- construction

@pytest.mark.parametrize("cls", [Graft, graft.AsyncGraft])
def test_token_is_required(monkeypatch: pytest.MonkeyPatch, cls: type[Any]) -> None:
    monkeypatch.delenv("GRAFT_TOKEN", raising=False)
    with pytest.raises(ValueError, match="GRAFT_TOKEN"):
        cls()


@pytest.mark.parametrize("cls", [Graft, graft.AsyncGraft])
def test_token_and_url_from_environment(monkeypatch: pytest.MonkeyPatch, cls: type[Any]) -> None:
    monkeypatch.setenv("GRAFT_TOKEN", "from-env")
    monkeypatch.setenv("GRAFT_URL", "http://env.test/")
    g = cls()
    assert g.base_url == "http://env.test"
    assert g._http.headers["Authorization"] == "Bearer from-env"


@pytest.mark.parametrize("cls", [Graft, graft.AsyncGraft])
def test_default_timeout_is_explicit_and_finite(cls: type[Any]) -> None:
    """Notebook case 08: a client with no read timeout hangs forever."""
    t = cls(token="t")._http.timeout
    assert (t.connect, t.read, t.write) == (5.0, 30.0, 30.0)


# ---------------------------------------------------------------- requests

def test_create_branch_sends_contract_shape(make_client: MakeClient) -> None:
    seen: dict[str, Any] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen.update(method=req.method, path=req.url.path, auth=req.headers["Authorization"],
                    key=req.headers.get("Idempotency-Key", ""), body=json.loads(req.content))
        return httpx.Response(201, json=BRANCH)

    br = make_client(handler).branches.create("demo", name="feat-x")
    assert (seen["method"], seen["path"], seen["auth"]) == ("POST", "/v1/projects/demo/branches", "Bearer t0ken")
    assert seen["body"] == {"name": "feat-x"}  # unset optional fields are omitted, not sent as null
    assert str(seen["key"]).startswith("graft-py-")  # creates always carry a key, so they are safe to retry
    assert br.id == "br_1" and br.git_ref is None and br.created_at.tzinfo is not None


def test_step_spec_sends_only_what_was_set_including_zero() -> None:
    """Notebook case 04: an explicit 0 must be sent, not dropped as 'unset'."""
    req = _base.create_run("br_1", [StepSpec(id="a"), StepSpec(id="m", needs=["a"], backoff_ms=0, max_attempts=3)], None, "k")
    assert req.json == {"steps": [{"id": "a"}, {"id": "m", "needs": ["a"], "backoff_ms": 0, "max_attempts": 3}]}


def test_step_spec_is_validated_before_sending() -> None:
    with pytest.raises(Exception, match="pattern"):
        StepSpec(id="Bad Id")


def test_unknown_response_fields_are_ignored(make_client: MakeClient) -> None:
    """Forward compatibility: a newer server may add fields; an older SDK must keep working."""
    g = make_client(lambda req: httpx.Response(200, json={**RUN_DONE, "added_in_v9": True}))
    assert g.runs.get("run_1").status == "succeeded"


# ---------------------------------------------------------------- errors

@pytest.mark.parametrize("status,kind,cls", [
    (401, "unauthorized", graft.Unauthorized),
    (404, "not-found", graft.NotFound),
    (409, "branch-name-taken", graft.Conflict),
    (409, "branch-limit", graft.Conflict),
    (422, "validation", graft.InvalidRequest),
    (422, "invalid-pipeline", graft.InvalidPipeline),
    (500, "internal", graft.ServerError),
])
def test_errors_map_by_problem_type_and_keep_request_id(make_client: MakeClient, status: int, kind: str, cls: type[Any]) -> None:
    g = make_client(lambda req: problem(status, kind, rid="req_xyz"))
    with pytest.raises(cls) as ei:
        g.runs.get("run_1")
    assert ei.value.status == status and ei.value.request_id == "req_xyz"
    assert "req_xyz" in str(ei.value)


def test_422_types_are_told_apart(make_client: MakeClient) -> None:
    g = make_client(lambda req: problem(422, "invalid-pipeline", detail="cycle"))
    with pytest.raises(graft.InvalidPipeline) as ei:
        g.runs.create("br_1", steps=[StepSpec(id="a")])
    assert not isinstance(ei.value, graft.InvalidRequest) and ei.value.detail == "cycle"


def test_non_json_error_body_still_raises_typed_error(make_client: MakeClient) -> None:
    g = make_client(lambda req: httpx.Response(502, text="<html>bad gateway</html>", headers={"X-Request-Id": "req_hdr"}))
    with pytest.raises(graft.ServerError) as ei:
        g.health()
    assert ei.value.request_id == "req_hdr"


def test_connection_and_timeout_errors_are_wrapped(make_client: MakeClient) -> None:
    def refuse(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=req)

    def slow(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=req)

    with pytest.raises(graft.NetworkError):
        make_client(refuse).health()
    with pytest.raises(graft.RequestTimeout):
        make_client(slow).health()


# ---------------------------------------------------------------- wait

def test_wait_polls_until_terminal(make_client: MakeClient, no_sleep: list[float]) -> None:
    answers = iter([RUN_QUEUED, {**RUN_QUEUED, "status": "running"}, RUN_DONE])
    calls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.path)
        return httpx.Response(200, json=next(answers))

    run = make_client(handler).runs.wait("run_1")
    assert run.status == "succeeded" and run.done and run.step("a").finished_ms == 10
    assert calls == ["/v1/runs/run_1"] * 3
    assert no_sleep == [0.05, 0.1]  # backoff doubles


def test_wait_times_out(make_client: MakeClient) -> None:
    g = make_client(lambda req: httpx.Response(200, json=RUN_QUEUED))
    with pytest.raises(TimeoutError, match="still queued"):
        g.runs.wait("run_1", timeout=0.05, poll=0.01)


def test_both_clients_have_the_same_public_methods() -> None:
    """The structural guarantee behind 'same tests on both': same names, same parameters."""
    import inspect

    from graft import _async_client, client
    pairs: list[tuple[type[object], type[object]]] = [
        (graft.Graft, graft.AsyncGraft),
        (client.Branches, _async_client.AsyncBranches),
        (client.Runs, _async_client.AsyncRuns),
    ]
    for sync_cls, async_cls in pairs:
        s = {n for n in dir(sync_cls) if not n.startswith("_")}
        a = {n for n in dir(async_cls) if not n.startswith("_")}
        # stream is async-only by design: a blocking websocket iterator would hold a thread.
        assert s - {"close"} == a - {"aclose", "stream"}, (sync_cls.__name__, s ^ a)
        for name in s - {"close"}:
            sp = inspect.signature(getattr(sync_cls, name)).parameters
            ap = inspect.signature(getattr(async_cls, name)).parameters
            assert list(sp) == list(ap), f"{sync_cls.__name__}.{name}: {list(sp)} vs {list(ap)}"
            assert inspect.iscoroutinefunction(getattr(async_cls, name)) or name in {"list", "events"}, name


def test_public_names_are_exported() -> None:
    for name in ["Graft", "AsyncGraft", "StepSpec", "Run", "Branch", "GraftError", "InvalidPipeline", "RateLimited", "NotFound", "Conflict", "ServerError"]:
        assert hasattr(graft, name), name


# ---------------------------------------------------------------- v0.4 error classes

@pytest.mark.parametrize("kind,cls", [
    ("idempotency-key-reused", graft.IdempotencyKeyReused),
    ("bad-cursor", graft.InvalidRequest),
    ("idempotency-key-in-use", graft.Conflict),
])
def test_more_problem_types(make_client: MakeClient, kind: str, cls: type[Any]) -> None:
    status = 409 if kind.endswith("in-use") else 422
    with pytest.raises(cls):
        make_client(lambda req: problem(status, kind)).runs.get("run_1")


def test_rate_limited_carries_retry_after_seconds_and_dates(make_client: MakeClient) -> None:
    g = make_client(lambda req: httpx.Response(429, headers={"Retry-After": "7", "X-Request-Id": "req_rl"}, text="slow down"))
    with pytest.raises(graft.RateLimited) as ei:
        g.health()
    assert ei.value.retry_after == 7.0 and ei.value.request_id == "req_rl"
    from datetime import datetime, timezone
    from graft.errors import parse_retry_after
    now = datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
    assert parse_retry_after("Tue, 10 Nov 2026 12:00:30 GMT", now) == 30.0
    assert parse_retry_after("soon", now) is None


def test_every_api_error_is_a_graft_error_and_says_what_to_do() -> None:
    for name in ["Unauthorized", "NotFound", "Conflict", "InvalidRequest", "InvalidPipeline",
                 "IdempotencyKeyReused", "RateLimited", "ServerError"]:
        cls = getattr(graft, name)
        assert issubclass(cls, graft.APIError) and issubclass(cls, graft.GraftError), name
        e = cls(400, "T", "d", None, "req_1")
        assert cls.hint and cls.hint in str(e) and "req_1" in str(e), (name, str(e))
    assert issubclass(graft.RequestTimeout, graft.NetworkError)
