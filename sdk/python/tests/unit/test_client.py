"""SDK behaviour against a fake transport: no server needed."""
from __future__ import annotations

import json
from typing import Any, Callable

import httpx
import pytest

import graft
from graft import Graft, StepSpec

RUN_QUEUED = {"id": "run_1", "branch_id": "br_1", "status": "queued", "created_at": "2026-11-09T10:00:00Z",
              "finished_at": None, "steps": [{"id": "a", "status": "pending", "attempts": 0, "started_ms": None,
                                              "finished_ms": None, "reason": None}]}
RUN_DONE = {**RUN_QUEUED, "status": "succeeded", "finished_at": "2026-11-09T10:00:01Z",
            "steps": [{"id": "a", "status": "succeeded", "attempts": 1, "started_ms": 0, "finished_ms": 10,
                       "reason": None}]}


def client(handler: Callable[[httpx.Request], httpx.Response]) -> Graft:
    return Graft(token="t0ken", base_url="http://graft.test", transport=httpx.MockTransport(handler))


def problem(status: int, kind: str, detail: str = "d", rid: str = "req_abc") -> httpx.Response:
    return httpx.Response(status, headers={"Content-Type": "application/problem+json"}, json={
        "type": f"https://graft.dev/problems/{kind}", "title": kind.title(), "status": status,
        "detail": detail, "request_id": rid})


def test_token_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRAFT_TOKEN", raising=False)
    with pytest.raises(ValueError, match="GRAFT_TOKEN"):
        Graft()


def test_token_and_url_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAFT_TOKEN", "from-env")
    monkeypatch.setenv("GRAFT_URL", "http://env.test/")
    g = Graft()
    assert g.base_url == "http://env.test"
    assert g._http.headers["Authorization"] == "Bearer from-env"


def test_default_timeout_is_explicit_and_finite() -> None:
    """Notebook case 08: a client with no read timeout hangs forever."""
    g = Graft(token="t")
    t = g._http.timeout
    assert t.connect == 5.0 and t.read == 30.0 and t.write == 30.0


def test_create_branch_sends_contract_shape() -> None:
    seen: dict[str, Any] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen.update(method=req.method, path=req.url.path, auth=req.headers["Authorization"], body=json.loads(req.content))
        return httpx.Response(201, json={"id": "br_1", "project": "demo", "name": "feat-x", "status": "ready",
                                         "git_ref": None, "created_at": "2026-11-09T10:00:00Z",
                                         "expires_at": "2026-11-10T10:00:00Z"})

    br = client(handler).branches.create("demo", name="feat-x")
    assert seen == {"method": "POST", "path": "/v1/projects/demo/branches", "auth": "Bearer t0ken",
                    "body": {"name": "feat-x"}}  # unset optional fields are omitted, not sent as null
    assert br.id == "br_1" and br.git_ref is None and br.created_at.tzinfo is not None


def test_step_spec_omits_defaults() -> None:
    assert StepSpec(id="a").to_json() == {"id": "a"}
    assert StepSpec(id="m", needs=["a"], max_attempts=3, backoff_ms=0, command=["graft-fake", "ok:1"]).to_json() == \
        {"id": "m", "needs": ["a"], "command": ["graft-fake", "ok:1"], "max_attempts": 3, "backoff_ms": 0}


def test_backoff_zero_is_sent_not_dropped() -> None:
    """Notebook case 04: 0 must not be read as "unset"."""
    assert StepSpec(id="m", backoff_ms=0, timeout_ms=0).to_json() == {"id": "m", "timeout_ms": 0, "backoff_ms": 0}


@pytest.mark.parametrize("status,kind,cls", [
    (401, "unauthorized", graft.AuthenticationError),
    (404, "not-found", graft.NotFoundError),
    (409, "branch-name-taken", graft.ConflictError),
    (409, "branch-limit", graft.ConflictError),
    (422, "validation", graft.ValidationError),
    (422, "invalid-pipeline", graft.InvalidPipelineError),
    (500, "internal", graft.ServerError),
])
def test_errors_map_by_problem_type_and_keep_request_id(status: int, kind: str, cls: type) -> None:
    g = client(lambda req: problem(status, kind, rid="req_xyz"))
    with pytest.raises(cls) as ei:
        g.runs.get("run_1")
    e = ei.value
    assert e.status == status and e.request_id == "req_xyz"
    assert "req_xyz" in str(e)


def test_422_types_are_told_apart() -> None:
    """Same status, different meaning: the class must come from the problem type."""
    g = client(lambda req: problem(422, "invalid-pipeline", detail="cycle"))
    with pytest.raises(graft.InvalidPipelineError) as ei:
        g.runs.create("br_1", steps=[StepSpec(id="a")])
    assert not isinstance(ei.value, graft.ValidationError)
    assert ei.value.detail == "cycle"


def test_non_json_error_body_still_raises_typed_error() -> None:
    g = client(lambda req: httpx.Response(502, text="<html>bad gateway</html>", headers={"X-Request-Id": "req_hdr"}))
    with pytest.raises(graft.ServerError) as ei:
        g.health()
    assert ei.value.request_id == "req_hdr"


def test_connection_and_timeout_errors_are_wrapped() -> None:
    def refuse(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=req)

    def slow(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=req)

    with pytest.raises(graft.APIConnectionError):
        client(refuse).health()
    with pytest.raises(graft.APITimeoutError):
        client(slow).health()


def test_wait_polls_until_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("time.sleep", lambda s: None)
    answers = iter([RUN_QUEUED, {**RUN_QUEUED, "status": "running"}, RUN_DONE])
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.path)
        return httpx.Response(200, json=next(answers))

    run = client(handler).runs.wait("run_1")
    assert run.status == "succeeded" and run.done and run.step("a").finished_ms == 10
    assert calls == ["/v1/runs/run_1"] * 3


def test_wait_times_out() -> None:
    g = client(lambda req: httpx.Response(200, json=RUN_QUEUED))
    with pytest.raises(TimeoutError, match="still queued"):
        g.runs.wait("run_1", timeout=0.05, poll=0.01)


def test_public_names_are_exported() -> None:
    for name in ["Graft", "StepSpec", "Run", "Branch", "GraftError", "InvalidPipelineError", "NotFoundError"]:
        assert hasattr(graft, name), name
