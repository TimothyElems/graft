"""A small app written the way a user would, type-checked against the INSTALLED wheel.

scripts/check_wheel.sh copies this file out of the repo, into a clean venv that has
only the built wheel, and runs mypy --strict and pyright --strict on it. assert_type
makes each check bite: if the SDK returned Any (say py.typed were missing), every
assert_type below would fail instead of passing silently.
"""
from __future__ import annotations

import asyncio
import datetime
from typing import Literal

from typing_extensions import assert_type

import graft
from graft import AsyncGraft, Branch, Event, Graft, Run, StepRun, StepSpec


def sync_app() -> None:
    with Graft(token="t", base_url="http://localhost:8080") as g:
        br = g.branches.create("demo", name="feat-invoices")
        assert_type(br, Branch)
        assert_type(br.created_at, datetime.datetime)
        assert_type(br.git_ref, str | None)

        run = g.runs.create(br.id, steps=[StepSpec(id="migrate", max_attempts=3), StepSpec(id="seed", needs=["migrate"])])
        assert_type(run, Run)
        run = g.runs.wait(run.id, timeout=30)
        assert_type(run.done, bool)
        assert_type(run.step("migrate"), StepRun)
        if run.status == "succeeded":  # a Literal: "succeded" (typo) would be flagged by pyright
            print("ok")

        for b in g.branches.list("demo"):
            assert_type(b, Branch)
        for e in g.runs.events(run.id, after=0):
            assert_type(e, Event)
            assert_type(e.seq, int)

        try:
            g.runs.get("run_missing")
        except graft.NotFound as err:
            assert_type(err.request_id, str | None)
        except graft.RateLimited as err:
            assert_type(err.retry_after, float | None)


async def async_app() -> None:
    async with AsyncGraft(token="t") as g:
        br = await g.branches.create("demo", name="feat-x")
        assert_type(br, Branch)
        run = await g.runs.wait((await g.runs.create(br.id, steps=[StepSpec(id="a")])).id)
        assert_type(run, Run)
        async for b in g.branches.list("demo"):
            assert_type(b, Branch)
        async for e in g.runs.events(run.id):
            assert_type(e, Event)
        stream = g.stream(run.id, after=0)
        async for e in stream:
            assert_type(e, Event)
            assert_type(e.type, Literal["run_started", "step_started", "step_succeeded", "step_failed",
                                        "step_retry", "step_skipped", "step_cancelled", "run_finished"])
        assert_type(stream.stats.duplicates_dropped, int)


if __name__ == "__main__":
    sync_app()
    asyncio.run(async_app())
