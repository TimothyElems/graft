"""Public models.

Every field comes from `_models.py`, which is generated from contract/openapi.yaml
(scripts/gen_models.sh) and checked by the drift gate in CI. Classes here may add
behaviour (methods, properties) to a generated model, never fields.
"""
from __future__ import annotations

from . import _models
from ._models import Branch, BranchPage, Event, EventPage, Health, StepRun, StepSpec

__all__ = ["Branch", "BranchPage", "Event", "EventPage", "Health", "Run", "StepRun", "StepSpec", "TERMINAL"]

TERMINAL: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})


class Run(_models.Run):
    """A pipeline run and its step results. Fields are the contract's Run."""

    @property
    def done(self) -> bool:
        """True once the run is succeeded, failed or cancelled."""
        return self.status in TERMINAL

    def step(self, step_id: str) -> StepRun:
        """The result for one step. Raises KeyError if the run has no such step.

        >>> run.step("migrate").attempts  # doctest: +SKIP
        3
        """
        for s in self.steps:
            if s.id == step_id:
                return s
        raise KeyError(step_id)
