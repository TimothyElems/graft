# pyright: reportPrivateUsage=false
# Tests deliberately exercise internals (_base, _send, _retry); the library itself is checked strictly.
"""Pagination iterators, on both clients, against a fake paging server."""
from __future__ import annotations

import itertools
from typing import Any

import httpx
import pytest

from graft._pagination import PaginationError

from .conftest import MakeClient


def branch(i: int) -> dict[str, Any]:
    return {"id": f"br_{i:03d}", "project": "demo", "name": f"b-{i:03d}", "status": "ready", "git_ref": None,
            "created_at": "2026-11-10T09:00:00.123Z", "expires_at": "2026-11-11T09:00:00Z"}


class BranchServer:
    """Serves n branches with cursor pages. The cursor is the next start index, as text."""

    def __init__(self, n: int, repeat_cursor: bool = False) -> None:
        self.n, self.repeat_cursor = n, repeat_cursor
        self.calls: list[dict[str, str]] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        q = dict(req.url.params)
        self.calls.append(q)
        if len(self.calls) > 10:  # fail-safe: a looping client fails the test instead of hanging it
            raise AssertionError(f"client is looping: {len(self.calls)} requests")
        start = int(q.get("cursor", "0"))
        limit = int(q["limit"])
        items = [branch(i) for i in range(start, min(start + limit, self.n))]
        nxt = start + limit
        cursor = None if nxt >= self.n else ("0" if self.repeat_cursor else str(nxt))
        return httpx.Response(200, json={"items": items, "next_cursor": cursor})


def test_iterates_120_items_across_3_pages(make_client: MakeClient) -> None:
    server = BranchServer(120)
    names = [b.name for b in make_client(server).branches.list("demo", page_size=50)]
    assert len(names) == 120 and len(set(names)) == 120 and names[0] == "b-000" and names[-1] == "b-119"
    assert [c.get("cursor") for c in server.calls] == [None, "50", "100"]
    assert all(c["limit"] == "50" for c in server.calls)


def test_pages_are_fetched_lazily(make_client: MakeClient) -> None:
    server = BranchServer(120)
    first_ten = list(itertools.islice(make_client(server).branches.list("demo", page_size=50), 10))
    assert len(first_ten) == 10 and len(server.calls) == 1


def test_empty_project_makes_one_request(make_client: MakeClient) -> None:
    server = BranchServer(0)
    assert list(make_client(server).branches.list("demo")) == [] and len(server.calls) == 1


def test_a_repeated_cursor_stops_instead_of_looping(make_client: MakeClient) -> None:
    server = BranchServer(120, repeat_cursor=True)
    with pytest.raises(PaginationError, match="repeated cursor"):
        for _ in make_client(server).branches.list("demo", page_size=50):
            pass
    assert len(server.calls) <= 3


def test_list_page_returns_the_raw_page(make_client: MakeClient) -> None:
    page = make_client(BranchServer(120)).branches.list_page("demo", cursor="100", limit=50)
    assert len(page.items) == 20 and page.next_cursor is None


def event(seq: int) -> dict[str, Any]:
    return {"id": f"evt_{seq}", "seq": seq, "run_id": "run_1", "branch_id": "br_1", "at": "2026-11-10T09:00:00Z",
            "type": "step_started", "at_ms": seq, "step": "a", "attempt": 1, "reason": None}


class EventServer:
    def __init__(self, n: int) -> None:
        self.n = n
        self.calls: list[tuple[int, int]] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        after, limit = int(req.url.params["after"]), int(req.url.params["limit"])
        self.calls.append((after, limit))
        items = [event(s) for s in range(after + 1, min(after + limit, self.n) + 1)]
        return httpx.Response(200, json={"items": items, "last_seq": items[-1]["seq"] if items else after})


@pytest.mark.parametrize("n,expected_calls", [(450, [(0, 200), (200, 200), (400, 200)]),
                                              (400, [(0, 200), (200, 200), (400, 200)]),
                                              (0, [(0, 200)])])
def test_events_page_by_seq_until_a_short_page(make_client: MakeClient, n: int, expected_calls: list[tuple[int, int]]) -> None:
    server = EventServer(n)
    seqs = [e.seq for e in make_client(server).runs.events("run_1")]
    assert seqs == list(range(1, n + 1))
    assert server.calls == expected_calls


def test_events_resume_after_a_seq(make_client: MakeClient) -> None:
    server = EventServer(10)
    assert [e.seq for e in make_client(server).runs.events("run_1", after=7)] == [8, 9, 10]
