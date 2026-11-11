"""Every operation in the contract has an SDK method, on both clients.

The models drift gate (make drift-check) catches a changed schema. This catches a
new or forgotten operation: runs.cancel existed in the service and the contract
for two releases before the SDK had it. An operation marked `x-graft-status:
planned` is not built yet and is skipped.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from graft import AsyncGraft, Graft

CONTRACT = Path(__file__).resolve().parents[4] / "contract" / "openapi.yaml"

# operationId -> attribute path on the client. None: not on this client, with the reason.
SDK: dict[str, tuple[str | None, str | None]] = {
    # operationId:     (sync Graft,        AsyncGraft)
    "healthz":        ("health",          "health"),
    "createBranch":   ("branches.create", "branches.create"),
    "listBranches":   ("branches.list",   "branches.list"),
    "getBranch":      ("branches.get",    "branches.get"),
    "createRun":      ("runs.create",     "runs.create"),
    "getRun":         ("runs.get",        "runs.get"),
    "cancelRun":      ("runs.cancel",     "runs.cancel"),
    "listRunEvents":  ("runs.events",     "runs.events"),
    "getRunBundle":   ("runs.bundle",     "runs.bundle"),
    "stream":         (None,              "stream"),  # websocket: async only (docs/realtime.md)
}


def operations() -> list[tuple[str, dict[str, Any]]]:
    spec: dict[str, Any] = yaml.safe_load(CONTRACT.read_text())
    out: list[tuple[str, dict[str, Any]]] = []
    for item in spec["paths"].values():
        for method, op in item.items():
            if method in {"get", "post", "put", "patch", "delete"}:
                out.append((op["operationId"], op))
    return out


def resolve(client: object, path: str) -> object:
    obj = client
    for part in path.split("."):
        obj = getattr(obj, part, None)
    return obj


def clients() -> tuple[Graft, AsyncGraft]:
    t = httpx.MockTransport(lambda r: httpx.Response(500))
    return (Graft(token="t", base_url="http://x", transport=t),
            AsyncGraft(token="t", base_url="http://x", transport=httpx.MockTransport(lambda r: httpx.Response(500))))


@pytest.mark.parametrize("op_id,op", operations(), ids=[o for o, _ in operations()])
def test_every_operation_has_an_sdk_method(op_id: str, op: dict[str, Any]) -> None:
    if op.get("x-graft-status") == "planned":
        pytest.skip(f"{op_id} is planned, not built")
    assert op_id in SDK, f"{op_id} is in the contract but has no SDK method (add it, or mark it planned)"
    for client, path in zip(clients(), SDK[op_id]):
        if path is not None:
            assert callable(resolve(client, path)), f"{type(client).__name__}.{path} missing for {op_id}"


def test_no_stale_mappings() -> None:
    known = {o for o, _ in operations()}
    assert set(SDK) <= known, f"mapped but not in the contract: {set(SDK) - known}"
