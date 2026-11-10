"""Exceptions raised by the SDK.

    GraftError
    ├── NetworkError            no HTTP response at all (DNS, refused, TLS)
    │   └── RequestTimeout      the server did not answer within the client timeout
    └── APIError                the server answered with a problem+json (RFC 9457) error
        ├── Unauthorized         401
        ├── NotFound             404
        ├── Conflict             409  (name taken, branch limit, key in use)
        ├── InvalidRequest       422  type .../validation or .../bad-cursor
        ├── InvalidPipeline      422  type .../invalid-pipeline
        ├── IdempotencyKeyReused 422  type .../idempotency-key-reused
        ├── RateLimited          429  (.retry_after in seconds, if the server said)
        └── ServerError          5xx

The class comes from the problem's `type` first and the HTTP status second, because
one status can mean different things: three different 422s above. Every APIError
keeps the server's request_id, so a support ticket can name the exact request.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import cast

import httpx

PROBLEM_BASE = "https://graft.dev/problems/"


class GraftError(Exception):
    """Base class for every error this SDK raises."""


class NetworkError(GraftError):
    """The request never got an HTTP response. Check the URL and your network; safe calls are retried."""


class RequestTimeout(NetworkError):
    """The server did not answer within the client timeout. Raise `timeout=` on the client if calls are slow."""


class APIError(GraftError):
    """The server answered with an error. Carries the RFC 9457 problem fields."""

    #: What a caller should do next. Subclasses override it; it ends the message.
    hint: str = ""

    def __init__(self, status: int, title: str, detail: str | None = None, type: str | None = None,
                 request_id: str | None = None) -> None:
        self.status = status
        self.title = title
        self.detail = detail
        self.type = type
        self.request_id = request_id
        msg = f"{status} {title}"
        if detail:
            msg += f": {detail}"
        if self.hint:
            msg += f". {self.hint}"
        if request_id:
            msg += f" (request_id={request_id})"
        super().__init__(msg)


class Unauthorized(APIError):
    """401."""
    hint = "Check the token passed to Graft(token=...) or set in GRAFT_TOKEN."


class NotFound(APIError):
    """404."""
    hint = "Check the id; branches and their runs disappear when the branch expires."


class Conflict(APIError):
    """409: a live branch already has the name, the project is at its branch limit, or the key is in use."""
    hint = "Use another name, delete a branch, or retry shortly if the Idempotency-Key is in use."


class InvalidRequest(APIError):
    """422 .../validation or .../bad-cursor: the request does not match the contract."""
    hint = "Fix the request; the detail names the field."


class InvalidPipeline(APIError):
    """422 .../invalid-pipeline: duplicate_step, unknown_dependency or cycle (checked in that order)."""
    hint = "Fix the steps; the detail is the scheduler's own error."


class IdempotencyKeyReused(APIError):
    """422 .../idempotency-key-reused: this key was already used with a different request."""
    hint = "Use a new Idempotency-Key for a new request."


class RateLimited(APIError):
    """429. `retry_after` is the server's requested wait in seconds, or None."""
    hint = "Slow down; wait retry_after seconds before retrying."

    def __init__(self, status: int, title: str, detail: str | None = None, type: str | None = None,
                 request_id: str | None = None, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(status, title, detail, type, request_id)


class ServerError(APIError):
    """5xx. Safe to report with the request_id; safe calls are retried automatically."""
    hint = "This is on the server's side; retry later and quote the request_id if it persists."


_BY_TYPE: dict[str, type[APIError]] = {
    "unauthorized": Unauthorized,
    "not-found": NotFound,
    "branch-name-taken": Conflict,
    "branch-limit": Conflict,
    "idempotency-key-in-use": Conflict,
    "validation": InvalidRequest,
    "bad-cursor": InvalidRequest,
    "invalid-pipeline": InvalidPipeline,
    "idempotency-key-reused": IdempotencyKeyReused,
    "rate-limited": RateLimited,
}
_BY_STATUS: dict[int, type[APIError]] = {401: Unauthorized, 404: NotFound, 409: Conflict, 422: InvalidRequest,
                                         429: RateLimited}


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Retry-After is either seconds ("3") or an HTTP date. Returns seconds >= 0, or None."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - (now or datetime.now(timezone.utc))).total_seconds())


def _str(v: object) -> str | None:
    return v if isinstance(v, str) else None


def from_response(resp: httpx.Response) -> APIError:
    """Build the right APIError for an error response, even one without a JSON body."""
    try:
        raw: object = resp.json()
    except ValueError:
        raw = None
    body: Mapping[str, object]
    if isinstance(raw, dict):
        body = cast("dict[str, object]", raw)  # JSON objects have string keys
    else:
        body = {"title": resp.reason_phrase, "detail": resp.text[:200]}
    status = resp.status_code
    ptype = _str(body.get("type"))
    kind = ptype[len(PROBLEM_BASE):] if ptype and ptype.startswith(PROBLEM_BASE) else ""
    cls = _BY_TYPE.get(kind) or _BY_STATUS.get(status) or (ServerError if status >= 500 else APIError)
    title = _str(body.get("title")) or "HTTP error"
    detail = _str(body.get("detail"))
    rid = _str(body.get("request_id")) or resp.headers.get("X-Request-Id")
    if cls is RateLimited:
        return RateLimited(status, title, detail, ptype, rid, retry_after=parse_retry_after(resp.headers.get("Retry-After")))
    return cls(status, title, detail, ptype, rid)
