"""Exceptions raised by the client.

The class is chosen from the problem's `type` URI first and the HTTP status
second, because one status can mean different things: 422 is either a bad
request body (ValidationError) or a pipeline the kernel refuses
(InvalidPipelineError). Every API error keeps the server's request_id.
"""
from __future__ import annotations

from typing import Any, Mapping

PROBLEM_BASE = "https://graft.dev/problems/"


class GraftError(Exception):
    """Base class for every error this SDK raises."""


class APIConnectionError(GraftError):
    """The request never got an HTTP response (DNS, refused connection, TLS)."""


class APITimeoutError(APIConnectionError):
    """The server did not answer within the client's timeout."""


class APIError(GraftError):
    """The server answered with an error. Carries the RFC 9457 problem fields."""

    def __init__(self, status: int, title: str, detail: str | None = None,
                 type: str | None = None, request_id: str | None = None) -> None:
        self.status = status
        self.title = title
        self.detail = detail
        self.type = type
        self.request_id = request_id
        msg = f"{status} {title}"
        if detail:
            msg += f": {detail}"
        if request_id:
            msg += f" (request_id={request_id})"
        super().__init__(msg)


class AuthenticationError(APIError):
    """401. Check the token passed to Graft(...) or set in GRAFT_TOKEN."""


class NotFoundError(APIError):
    """404. The branch or run id does not exist (or has expired)."""


class ConflictError(APIError):
    """409. For example, a live branch with that name already exists, or the project is at its branch limit."""


class ValidationError(APIError):
    """422 with type .../validation. The request body does not match the contract."""


class InvalidPipelineError(APIError):
    """422 with type .../invalid-pipeline. The steps form a pipeline the scheduler refuses:
    duplicate_step, unknown_dependency or cycle (checked in that order)."""


class ServerError(APIError):
    """5xx. Safe to report with the request_id; retry later."""


_BY_TYPE: dict[str, type[APIError]] = {
    "unauthorized": AuthenticationError,
    "not-found": NotFoundError,
    "branch-name-taken": ConflictError,
    "branch-limit": ConflictError,
    "validation": ValidationError,
    "invalid-pipeline": InvalidPipelineError,
}
_BY_STATUS: dict[int, type[APIError]] = {401: AuthenticationError, 404: NotFoundError, 409: ConflictError,
                                         422: ValidationError}


def from_response(status: int, body: Mapping[str, Any] | None, header_request_id: str | None) -> APIError:
    body = body or {}
    ptype = body.get("type")
    kind = ptype[len(PROBLEM_BASE):] if isinstance(ptype, str) and ptype.startswith(PROBLEM_BASE) else None
    cls = _BY_TYPE.get(kind or "") or _BY_STATUS.get(status) or (ServerError if status >= 500 else APIError)
    return cls(status=status, title=str(body.get("title") or "HTTP error"), detail=body.get("detail"),
               type=ptype if isinstance(ptype, str) else None,
               request_id=body.get("request_id") or header_request_id)
