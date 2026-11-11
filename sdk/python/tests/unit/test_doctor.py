"""graft doctor: download the bundle, scrub it again, add client details, write one tar.gz."""
from __future__ import annotations

import io
import json
import os
import stat
import tarfile
from pathlib import Path
from typing import Any

import httpx
import pytest

from graft import Graft, NotFound
from graft._redact import Redactor
from graft.cli import main

from .conftest import Handler, MakeClient

CASES = Path(__file__).resolve().parents[4] / "fixtures" / "redaction" / "cases.json"
CLIENT_TOKEN = "client-token-abcdef123"


def test_redaction_matches_the_shared_cases() -> None:
    """Same file as the Go test: the two scrubbers must agree on every case."""
    data: dict[str, Any] = json.loads(CASES.read_text())
    r = Redactor(data["secrets"])
    for c in data["cases"]:
        got, n = r.redact(c["input"])
        assert got == c["want"], c["name"]
        assert (n > 0) == (got != c["input"]), c["name"]


def make_tar(files: dict[str, str], *, extra: tarfile.TarInfo | None = None) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, body in files.items():
            data = body.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        if extra is not None:
            tf.addfile(extra, io.BytesIO(b"x" * extra.size))
    return buf.getvalue()


SERVER_BUNDLE = {
    "manifest.json": json.dumps({"format": "graft-bundle/1", "run_id": "run_x", "redactions": 1}),
    "events.json": "[]",
    # An older server that did not know this client's token printed it bare.
    "attempts/claim-1/a-attempt-1.log": f"# step a\nretrying with {CLIENT_TOKEN}\nPGPASSWORD=[REDACTED]\n",
}


def handler(bundle: bytes) -> Handler:
    def handle(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/healthz":
            return httpx.Response(200, json={"status": "ok", "version": "v-test", "queue_depth": 0})
        if req.url.path == "/v1/runs/run_x/bundle":
            return httpx.Response(200, content=bundle, headers={
                "Content-Type": "application/gzip", "X-Graft-Redactions": "1"})
        return httpx.Response(404, headers={"Content-Type": "application/problem+json"}, json={
            "type": "https://graft.dev/problems/not-found", "title": "Run not found", "status": 404,
            "detail": "No run.", "request_id": "req_1"})
    return handle


def server(bundle: bytes) -> httpx.MockTransport:
    return httpx.MockTransport(handler(bundle))


def test_runs_bundle_returns_the_bytes(make_client: MakeClient) -> None:
    raw = make_tar({"manifest.json": "{}"})
    g = make_client(handler(raw))
    assert g.runs.bundle("run_x") == raw
    with pytest.raises(NotFound):
        g.runs.bundle("run_nope")


def read_tar(path: Path) -> dict[str, str]:
    with tarfile.open(path, "r:gz") as tf:
        return {m.name: tf.extractfile(m).read().decode() for m in tf.getmembers()}  # type: ignore[union-attr]


def test_doctor_scrubs_again_and_adds_client_details(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "out.tar.gz"
    g = Graft(token=CLIENT_TOKEN, base_url="http://graft.test", transport=server(make_tar(SERVER_BUNDLE)))
    code = main(["doctor", "run_x", "-o", str(out)], client=g)
    assert code == 0, capsys.readouterr()
    files = read_tar(out)
    assert set(files) == set(SERVER_BUNDLE) | {"doctor.json"}
    for name, body in files.items():
        assert CLIENT_TOKEN not in body, name
    doc = json.loads(files["doctor.json"])
    assert doc["server_redactions"] == 1 and doc["client_redactions"] == 1
    assert doc["health"]["status"] == "ok" and doc["health"]["latency_ms"] >= 0
    assert {"sdk_version", "python", "platform", "base_url", "created_at"} <= set(doc)
    assert stat.S_IMODE(os.stat(out).st_mode) == 0o600  # may still hold a secret in an unknown shape
    printed = capsys.readouterr().out
    assert str(out) in printed and "Read the attempt logs" in printed


def test_doctor_refuses_a_path_that_escapes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    evil = tarfile.TarInfo("../../evil.sh")
    evil.size = 4
    g = Graft(token=CLIENT_TOKEN, base_url="http://graft.test",
              transport=server(make_tar({"manifest.json": "{}"}, extra=evil)))
    out = tmp_path / "out.tar.gz"
    assert main(["doctor", "run_x", "-o", str(out)], client=g) != 0
    assert not out.exists()
    assert "unsafe" in capsys.readouterr().err


def test_doctor_refuses_a_link(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    link = tarfile.TarInfo("attempts/x.log")
    link.type, link.linkname, link.size = tarfile.SYMTYPE, "/etc/passwd", 0
    g = Graft(token=CLIENT_TOKEN, base_url="http://graft.test",
              transport=server(make_tar({"manifest.json": "{}"}, extra=link)))
    assert main(["doctor", "run_x", "-o", str(tmp_path / "o.tgz")], client=g) != 0
    assert "unsafe" in capsys.readouterr().err


def test_doctor_does_not_overwrite(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "out.tar.gz"
    out.write_text("keep me")
    g = Graft(token=CLIENT_TOKEN, base_url="http://graft.test", transport=server(make_tar(SERVER_BUNDLE)))
    assert main(["doctor", "run_x", "-o", str(out)], client=g) == 2
    assert out.read_text() == "keep me"


def test_doctor_unknown_run_says_so_with_the_request_id(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    g = Graft(token=CLIENT_TOKEN, base_url="http://graft.test", transport=server(b""))
    assert main(["doctor", "run_nope", "-o", str(tmp_path / "o.tgz")], client=g) == 1
    err = capsys.readouterr().err
    assert "No run." in err and "req_1" in err
