"""Secret scrubbing for support bundles: the client's copy of the server's rules.

service/internal/bundle/redact.go is the other copy. Both must turn every input in
fixtures/redaction/cases.json into exactly its expected output, so the two cannot
drift apart unnoticed.

Best effort: it catches values it is told about and credential-shaped text, not
every secret a step might print.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Iterable

REDACTED = "[REDACTED]"
#: Shorter literal secrets would mangle ordinary text; their shape still gives them away.
MIN_LITERAL = 8

Sub = Callable[["re.Match[str]"], str]


def _keep(n: int) -> Sub:
    """Keep groups 1..n, redact the rest of the match."""
    return lambda m: "".join(m.group(i) for i in range(1, n + 1)) + REDACTED


_RULES: list[tuple[re.Pattern[str], Sub]] = [
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
     lambda m: "[REDACTED PRIVATE KEY]"),
    (re.compile(r"""([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@"']+:)[^\s@"'/]+(@)"""),
     lambda m: m.group(1) + REDACTED + m.group(2)),
    (re.compile(r"(?i)(authorization:\s*basic\s+)[A-Za-z0-9+/]+=*"), _keep(1)),
    (re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"), _keep(1)),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), _keep(0)),  # JWT
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}"), _keep(0)),  # GitHub
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), _keep(0)),  # AWS key id
    (re.compile(r"\bsb_secret_[A-Za-z0-9_-]{8,}"), _keep(0)),  # Supabase secret key
    (re.compile(r"""(?i)\b([a-z0-9_]*(?:password|passwd|pwd|secret|token|api_?key|access_?key))("?\s*[=:]\s*"?)([^\s"'&,;]+)"""),
     _keep(2)),
]


class Redactor:
    """Removes the given literal secrets and anything credential-shaped."""

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        self._literals = sorted((s for s in secrets if len(s) >= MIN_LITERAL), key=len, reverse=True)

    def redact(self, s: str) -> tuple[str, int]:
        """Return s without secrets, and how many were removed."""
        n = 0
        for lit in self._literals:
            c = s.count(lit)
            if c:
                n += c
                s = s.replace(lit, REDACTED)
        for pattern, sub in _RULES:
            def counted(m: re.Match[str], sub: Sub = sub) -> str:
                nonlocal n
                out = sub(m)
                if out != m.group(0):
                    n += 1
                return out
            s = pattern.sub(counted, s)
        return s, n
