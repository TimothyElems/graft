"""Response and request models, written by hand to match contract/openapi.yaml.

v0.4 replaces these with models generated from the contract, guarded by a drift
check in CI. Until then, the e2e test validates the server's raw JSON against
the contract so these can't silently disagree with it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping, Sequence, cast

BranchStatus = Literal["creating", "ready", "failed", "deleting", "deleted"]
RunStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]
StepStatus = Literal["pending", "succeeded", "failed", "skipped", "cancelled"]

TERMINAL: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})


def _dt(s: str) -> datetime:
    # Python 3.10's fromisoformat does not accept a trailing "Z".
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


@dataclass(frozen=True)
class Health:
    status: str
    queue_depth: int

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> Health:
        return cls(status=str(d["status"]), queue_depth=int(d["queue_depth"]))


@dataclass(frozen=True)
class Branch:
    id: str
    project: str
    name: str
    status: BranchStatus
    git_ref: str | None
    created_at: datetime
    expires_at: datetime

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> Branch:
        return cls(
            id=d["id"], project=d["project"], name=d["name"], status=cast(BranchStatus, d["status"]),
            git_ref=d.get("git_ref"), created_at=_dt(d["created_at"]), expires_at=_dt(d["expires_at"]),
        )


@dataclass(frozen=True)
class StepRun:
    id: str
    status: StepStatus
    attempts: int
    started_ms: int | None
    finished_ms: int | None
    reason: str | None

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> StepRun:
        return cls(id=d["id"], status=cast(StepStatus, d["status"]), attempts=int(d["attempts"]),
                   started_ms=d.get("started_ms"), finished_ms=d.get("finished_ms"), reason=d.get("reason"))


@dataclass(frozen=True)
class Run:
    id: str
    branch_id: str
    status: RunStatus
    created_at: datetime
    finished_at: datetime | None
    steps: tuple[StepRun, ...]

    @property
    def done(self) -> bool:
        """True once the run is succeeded, failed or cancelled."""
        return self.status in TERMINAL

    def step(self, step_id: str) -> StepRun:
        """The result for one step. Raises KeyError if the run has no such step."""
        for s in self.steps:
            if s.id == step_id:
                return s
        raise KeyError(step_id)

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> Run:
        fin = d.get("finished_at")
        return cls(id=d["id"], branch_id=d["branch_id"], status=cast(RunStatus, d["status"]),
                   created_at=_dt(d["created_at"]), finished_at=_dt(fin) if fin else None,
                   steps=tuple(StepRun.from_json(s) for s in d["steps"]))


@dataclass(frozen=True)
class StepSpec:
    """One step of a pipeline. Fields left as None take the server's default.

    With the v0.2 fake runner, `command=["graft-fake", "fail:10", "ok:10"]` scripts
    the attempts: fail after 10 ms, then succeed after 10 ms.
    """

    id: str
    needs: Sequence[str] = field(default_factory=tuple)
    image: str | None = None
    command: Sequence[str] = field(default_factory=tuple)
    max_attempts: int | None = None
    timeout_ms: int | None = None
    backoff_ms: int | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"id": self.id}
        if self.needs:
            out["needs"] = list(self.needs)
        if self.image is not None:
            out["image"] = self.image
        if self.command:
            out["command"] = list(self.command)
        for k in ("max_attempts", "timeout_ms", "backoff_ms"):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        return out
