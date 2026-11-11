"""The `graft` command. One subcommand so far:

    graft doctor <run_id> [-o FILE]

downloads the run's support bundle, scrubs it again with this client's own secrets,
adds what only the client knows (SDK and Python versions, platform, how long the
service took to answer), and writes one .tar.gz to attach to a bug report.

GRAFT_URL and GRAFT_TOKEN configure it, as for the client.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import platform
import sys
import tarfile
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from ._base import __version__
from ._redact import Redactor
from .client import Graft
from .errors import GraftError

#: A bundle is one run's events and 8 KB per attempt; anything near this is not a bundle.
MAX_BUNDLE_BYTES = 64 << 20


class UnsafeBundle(Exception):
    """The archive holds something doctor will not copy: a link, a device, a path outside it."""


def read_members(raw: bytes) -> list[tuple[str, str]]:
    """The bundle's files as (name, text), read in memory, never extracted to disk.

    The archive comes from a server, so it is input: only regular files with plain
    relative names are accepted (no links, no absolute paths, no `..`).
    """
    out: list[tuple[str, str]] = []
    total = 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tf:
        for m in tf:
            p = PurePosixPath(m.name)
            if not m.isreg():
                raise UnsafeBundle(f"{m.name!r} is not a regular file")
            if p.is_absolute() or ".." in p.parts or "\\" in m.name or not m.name:
                raise UnsafeBundle(f"{m.name!r} is not a plain relative path")
            total += m.size
            if total > MAX_BUNDLE_BYTES:
                raise UnsafeBundle(f"larger than {MAX_BUNDLE_BYTES >> 20} MB")
            f = tf.extractfile(m)
            data = f.read() if f is not None else b""
            out.append((m.name, data.decode("utf-8", errors="replace")))
    return out


def _write_private(path: Path, files: list[tuple[str, str]]) -> int:
    """Write a .tar.gz readable only by this user (it may still hold a secret in an
    unknown shape). Written to a temporary name first: no half-written file on error."""
    buf = io.BytesIO()
    now = time.time()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, text in files:
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime = len(data), 0o644, int(now)
            tf.addfile(info, io.BytesIO(data))
    tmp = path.with_name(path.name + ".partial")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(buf.getvalue())
    os.replace(tmp, path)
    return len(buf.getvalue())


def doctor(run_id: str, out: Path, client: Graft | None = None) -> int:
    if out.exists():
        print(f"graft doctor: {out} already exists; not overwriting it (pass -o to choose another file)",
              file=sys.stderr)
        return 2
    try:
        g = client or Graft()
    except ValueError as e:
        print(f"graft doctor: {e}", file=sys.stderr)
        return 2
    try:
        health: dict[str, object]
        t0 = time.monotonic()
        try:
            h = g.health()
            health = {"status": h.status, "queue_depth": h.queue_depth}
        except GraftError as e:
            health = {"error": str(e)}
        health["latency_ms"] = round((time.monotonic() - t0) * 1000, 1)
        raw = g.runs.bundle(run_id)
    except GraftError as e:
        print(f"graft doctor: {e}", file=sys.stderr)
        return 1
    finally:
        if client is None:
            g.close()

    try:
        members = read_members(raw)
    except (UnsafeBundle, tarfile.TarError, EOFError, OSError) as e:
        print(f"graft doctor: unsafe or broken bundle, nothing written: {e}", file=sys.stderr)
        return 1

    # The server removed its own secrets. This client knows one more: its token.
    token = g._token  # pyright: ignore[reportPrivateUsage]  # same package; not public API
    red = Redactor([token])
    scrubbed: list[tuple[str, str]] = []
    removed = 0
    server_removed: object = None
    for name, text in members:
        clean, n = red.redact(text)
        removed += n
        scrubbed.append((name, clean))
        if name == "manifest.json":
            try:
                server_removed = json.loads(clean).get("redactions")
            except (ValueError, AttributeError):
                server_removed = None

    base_url, _ = red.redact(g.base_url)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "sdk_version": __version__,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "base_url": base_url,
        "health": health,
        "server_redactions": server_removed,
        "client_redactions": removed,
        "note": "Scrubbing is best effort. Read the attempt logs before you share this file.",
    }
    scrubbed.append(("doctor.json", json.dumps(report, indent=2) + "\n"))
    size = _write_private(out, scrubbed)
    print(f"wrote {out} ({len(scrubbed)} files, {size / 1024:.1f} KB)")
    print(f"  secrets removed: {server_removed if server_removed is not None else '?'} by the server, "
          f"{removed} by this client")
    print("  Read the attempt logs (attempts/) before you share it: scrubbing is best effort.")
    return 0


def main(argv: Sequence[str] | None = None, *, client: Graft | None = None) -> int:
    parser = argparse.ArgumentParser(prog="graft", description="Graft command line.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("doctor", help="write a support bundle for a run, secrets removed",
                       description="Download a run's support bundle, scrub it again, add client details, "
                                   "and write one .tar.gz to attach to a bug report.")
    d.add_argument("run_id")
    d.add_argument("-o", "--output", help="file to write (default: ./graft-doctor-<run_id>.tar.gz)")
    args = parser.parse_args(argv)
    run_id: str = args.run_id
    output: str | None = args.output
    return doctor(run_id, Path(output or f"graft-doctor-{run_id}.tar.gz"), client)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
