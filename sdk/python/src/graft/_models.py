# GENERATED from contract/openapi.yaml by scripts/gen_models.sh. Do not edit by hand.

from __future__ import annotations

from typing import Literal

from pydantic import AnyUrl, AwareDatetime, BaseModel, ConfigDict, Field


class Health(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    status: Literal['ok']
    queue_depth: int = Field(..., ge=0)


class Problem(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    type: AnyUrl
    title: str
    status: int
    detail: str | None = None
    request_id: str | None = None


class CreateBranch(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    name: str = Field(..., pattern='^[a-z0-9][a-z0-9-]{0,62}$')
    git_ref: str | None = None
    ttl_seconds: int = Field(default=86400, ge=60, le=604800)


class Branch(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    id: str
    project: str
    name: str
    status: Literal['creating', 'ready', 'failed', 'deleting', 'deleted']
    git_ref: str | None = None
    created_at: AwareDatetime
    expires_at: AwareDatetime


class BranchPage(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    items: list[Branch]
    next_cursor: str | None


class StepSpec(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    id: str = Field(..., pattern='^[a-z][a-z0-9_]{0,31}$')
    needs: list[str] = []
    image: str | None = None
    """
    Container image. Sandboxed. No network. Read-only root.
    """
    command: list[str] | None = None
    max_attempts: int = Field(default=1, ge=1, le=10)
    timeout_ms: int = Field(default=0, ge=0)
    """
    0 means no timeout
    """
    backoff_ms: int = Field(default=0, ge=0)


class CreateRun(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    steps: list[StepSpec] = Field(..., max_length=64, min_length=1)
    concurrency: int = Field(default=1, ge=1, le=16)


class StepRun(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    id: str
    status: Literal['pending', 'succeeded', 'failed', 'skipped', 'cancelled']
    attempts: int = Field(..., ge=0)
    started_ms: int | None
    finished_ms: int | None
    reason: str | None


class Run(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    id: str
    branch_id: str
    status: Literal['queued', 'running', 'succeeded', 'failed', 'cancelled']
    created_at: AwareDatetime
    finished_at: AwareDatetime | None
    steps: list[StepRun]


class Event(BaseModel):
    """
    One event on a run. Sent by GET /v1/runs/{id}/events and on the /v1/stream websocket. The kernel produces type, at_ms, step, attempt and reason. The service adds id, seq, run_id, branch_id and at.
    """

    model_config = ConfigDict(
        extra='ignore',
    )
    id: str = Field(..., pattern='^evt_[A-Za-z0-9_]+$')
    seq: int = Field(..., ge=1)
    """
    Per-run, starts at 1, no gaps. Resume with ?after=<seq>.
    """
    run_id: str = Field(..., pattern='^run_[A-Za-z0-9_]+$')
    branch_id: str = Field(..., pattern='^br_[A-Za-z0-9_]+$')
    at: AwareDatetime
    """
    Wall-clock time the service recorded the event.
    """
    at_ms: int = Field(..., ge=0)
    """
    Milliseconds since the run started. Same clock the kernel uses.
    """
    type: Literal[
        'run_started',
        'step_started',
        'step_succeeded',
        'step_failed',
        'step_retry',
        'step_skipped',
        'step_cancelled',
        'run_finished',
    ]
    step: str | None
    """
    Step id. Null for run-level events.
    """
    attempt: int = Field(..., ge=0)
    reason: str | None
    """
    failed, timeout, cancelled, dependency:<step>, or the final run status on run_finished.
    """


class EventPage(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )
    items: list[Event]
    last_seq: int = Field(..., ge=0)
