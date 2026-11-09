"""The SDK against a real graftd, end to end.

Runs only with GRAFT_E2E=1 (CI sets it after `docker compose up --wait`). Reads
GRAFT_URL (default http://localhost:8080) and GRAFT_TOKEN.

Besides checking the SDK, it validates the server's raw JSON against
contract/openapi.yaml, so neither side can drift from the contract unnoticed.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

import graft
from graft import Graft, StepSpec

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.environ.get("GRAFT_E2E") != "1", reason="set GRAFT_E2E=1 with graftd running"),
]

URL = os.environ.get("GRAFT_URL", "http://localhost:8080")
TOKEN = os.environ.get("GRAFT_TOKEN", "local-dev-token-change-me")
CONTRACT = Path(__file__).resolve().parents[4] / "contract" / "openapi.yaml"


@pytest.fixture(scope="module")
def g() -> Any:
    with Graft(token=TOKEN, base_url=URL) as client:
        client.health()  # fail loudly here, not in every test, if the server is not up
        yield client


@pytest.fixture(scope="module")
def raw() -> Any:
    with httpx.Client(base_url=URL, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=10) as c:
        yield c


@pytest.fixture(scope="module")
def contract() -> Any:
    yaml = pytest.importorskip("yaml")
    jsonschema = pytest.importorskip("jsonschema")
    spec = yaml.safe_load(CONTRACT.read_text())

    def check(component: str, doc: Any) -> None:
        schema = {"$ref": f"#/components/schemas/{component}", "components": spec["components"]}
        v = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
        errs = [f"{list(e.path)}: {e.message}" for e in v.iter_errors(doc)]
        assert not errs, f"{component} breaks the contract: {errs}"

    return check


def name() -> str:
    return f"e2e-{uuid.uuid4().hex[:10]}"


def test_health(g: Graft) -> None:
    h = g.health()
    assert h.status == "ok" and h.queue_depth >= 0


def test_branch_then_run_end_to_end(g: Graft, raw: httpx.Client, contract: Any) -> None:
    br = g.branches.create("demo", name=name(), git_ref="refs/heads/feat")
    assert br.status == "ready" and br.git_ref == "refs/heads/feat"
    assert g.branches.get(br.id) == br
    contract("Branch", raw.get(f"/v1/branches/{br.id}").json())

    run = g.runs.create(br.id, steps=[
        StepSpec(id="migrate", max_attempts=3, backoff_ms=5, command=["graft-fake", "fail:10", "fail:10", "ok:10"]),
        StepSpec(id="seed", needs=["migrate"]),
    ])
    assert run.status == "queued" and all(s.status == "pending" for s in run.steps)

    run = g.runs.wait(run.id, timeout=10)
    assert run.status == "succeeded" and run.finished_at is not None
    m, s = run.step("migrate"), run.step("seed")
    # Fixture 03's timeline, through HTTP: start 0, fail 10, start 15, fail 25, start 30, ok 40.
    assert (m.attempts, m.started_ms, m.finished_ms, m.reason) == (3, 0, 40, None)
    assert (s.attempts, s.started_ms, s.finished_ms) == (1, 40, 50)
    contract("Run", raw.get(f"/v1/runs/{run.id}").json())


def test_failure_skips_dependents(g: Graft, raw: httpx.Client, contract: Any) -> None:
    br = g.branches.create("demo", name=name())
    run = g.runs.wait(g.runs.create(br.id, steps=[
        StepSpec(id="migrate", command=["graft-fake", "fail:10"]),
        StepSpec(id="seed", needs=["migrate"]),
        StepSpec(id="deploy", needs=["seed"]),
    ]).id, timeout=10)
    assert run.status == "failed"
    assert (run.step("migrate").status, run.step("migrate").reason) == ("failed", "failed")
    assert (run.step("seed").status, run.step("seed").reason) == ("skipped", "dependency:migrate")
    assert (run.step("deploy").status, run.step("deploy").reason) == ("skipped", "dependency:seed")
    contract("Run", raw.get(f"/v1/runs/{run.id}").json())


def test_typed_errors_carry_request_id(g: Graft, raw: httpx.Client, contract: Any) -> None:
    with pytest.raises(graft.NotFoundError) as nf:
        g.runs.get("run_doesnotexist")
    assert nf.value.request_id and nf.value.request_id.startswith("req_")

    br = g.branches.create("demo", name=name())
    with pytest.raises(graft.InvalidPipelineError) as ip:
        g.runs.create(br.id, steps=[StepSpec(id="a", needs=["b"]), StepSpec(id="b", needs=["a"])])
    assert ip.value.detail == "cycle"

    with pytest.raises(graft.ConflictError):
        g.branches.create("demo", name=br.name)

    with pytest.raises(graft.ValidationError):
        g.runs.create(br.id, steps=[StepSpec(id="a", max_attempts=0)])

    body = raw.get("/v1/runs/run_doesnotexist").json()
    contract("Problem", body)


def test_wrong_token_is_rejected() -> None:
    with Graft(token="wrong-token", base_url=URL) as bad:
        with pytest.raises(graft.AuthenticationError):
            bad.runs.get("run_x")
