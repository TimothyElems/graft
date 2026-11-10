"""Retry with jitter, only where a retry cannot do harm.

Safe to retry: GET, and POST with an Idempotency-Key (the server replays the first
result instead of acting twice). The SDK sends a key on every create, and keeps the
same key across that call's retries. A POST without a key is never retried: if the
first attempt reached the server and only the response was lost, a retry would
create a second run.

Worth retrying: no response at all (network error, timeout), 5xx, 429, and 409
idempotency-key-in-use. Not worth it: other 4xx, which a retry cannot change.

Delay: "full jitter", a random wait in [0, min(cap, base * 2**attempt)], so many
clients that failed together do not come back together. A Retry-After from the
server is used as given.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from ._base import Request
from .errors import Conflict, GraftError, NetworkError, RateLimited, ServerError

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
MAX_RETRY_AFTER = 60.0  # never sleep longer than this on a server's say-so


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


async def _async_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


@dataclass(frozen=True)
class RetryPolicy:
    """How the clients retry. Pass `RetryPolicy(max_retries=0)` to turn retries off."""

    max_retries: int = 2
    base: float = 0.5
    cap: float = 8.0
    rng: Callable[[], float] = field(default=random.random)
    sleep: Callable[[float], None] = field(default=_sleep)
    async_sleep: Callable[[float], Awaitable[None]] = field(default=_async_sleep)

    def delay(self, attempt: int, retry_after: float | None = None) -> float:
        """Seconds to wait before retry number `attempt` (0-based)."""
        if retry_after is not None:
            return min(max(retry_after, 0.0), MAX_RETRY_AFTER)
        return self.rng() * min(self.cap, self.base * (2.0 ** attempt))  # 2.0: int ** int is typed Any


def is_safe(req: Request) -> bool:
    """A retry cannot cause a second effect."""
    return req.method in SAFE_METHODS or (req.method == "POST" and "Idempotency-Key" in req.headers)


def is_retryable(err: GraftError) -> bool:
    """A retry might succeed."""
    if isinstance(err, (NetworkError, ServerError, RateLimited)):
        return True
    return isinstance(err, Conflict) and (err.type or "").endswith("/idempotency-key-in-use")


def retry_after(err: GraftError) -> float | None:
    return err.retry_after if isinstance(err, RateLimited) else None
