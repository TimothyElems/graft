"""Independent Python implementation of the Graft pipeline kernel.

Written from the rules, not translated from the Go code. If the two disagree on
any input, one of them is wrong, and the differential test (diff_random.py) finds it.
"""
from __future__ import annotations

from typing import Any


class CaseError(Exception):
    pass


def validate(steps: list[dict[str, Any]]) -> None:
    ids: set[str] = set()
    for s in steps:
        if s["id"] in ids:
            raise CaseError(f"duplicate_step:{s['id']}")
        ids.add(s["id"])
    for s in steps:
        for need in s.get("needs") or []:
            if need not in ids:
                raise CaseError(f"unknown_dependency:{need}")
    # Kahn's algorithm: if we cannot order every step there is a cycle.
    remaining = {s["id"]: set(s.get("needs") or []) for s in steps}
    while remaining:
        free = [k for k, v in remaining.items() if not v]
        if not free:
            raise CaseError("cycle")
        for k in free:
            del remaining[k]
        for v in remaining.values():
            v.difference_update(free)


def simulate(case: dict[str, Any]) -> dict[str, Any]:
    steps = case["steps"]
    validate(steps)
    script = case.get("script") or {}
    conc = max(1, case.get("concurrency") or 1)
    cancel_at = case.get("cancel_at_ms")
    order = [s["id"] for s in steps]
    spec = {s["id"]: s for s in steps}

    rec = {
        k: {"status": "pending", "attempts": 0, "started_ms": None, "finished_ms": None, "reason": ""}
        for k in order
    }
    ready_at = {k: 0 for k in order}
    inflight: list[dict[str, Any]] = []  # {id, end, outcome, seq}
    events: list[dict[str, Any]] = []
    seq = 0
    now = 0
    cancelled = False

    def emit(typ: str, step: str = "", attempt: int = 0, reason: str = "") -> None:
        events.append({"at_ms": now, "type": typ, "step": step, "attempt": attempt, "reason": reason})

    def attempt_plan(sid: str, n: int) -> tuple[int, str]:
        plan = script.get(sid) or []
        if not plan:
            return 10, "ok"
        a = plan[min(n, len(plan)) - 1]
        return a["dur_ms"], a["outcome"]

    def limit(sid: str) -> int:
        return max(1, spec[sid].get("max_attempts") or 1)

    while True:
        # finish what ends now
        done = sorted((f for f in inflight if f["end"] <= now), key=lambda f: (f["end"], f["seq"]))
        inflight = [f for f in inflight if f["end"] > now]
        for f in done:
            r = rec[f["id"]]
            if f["outcome"] == "ok":
                r["status"], r["finished_ms"] = "succeeded", now
                emit("step_succeeded", f["id"], r["attempts"])
                continue
            why = "timeout" if f["outcome"] == "timeout" else "failed"
            if r["attempts"] >= limit(f["id"]):
                r["status"], r["finished_ms"], r["reason"] = "failed", now, why
                emit("step_failed", f["id"], r["attempts"], why)
            else:
                ready_at[f["id"]] = now + (spec[f["id"]].get("backoff_ms") or 0)
                emit("step_retry", f["id"], r["attempts"], why)

        running_ids = {f["id"] for f in inflight}

        # skip anything that sits behind a dead step
        moved = True
        while moved:
            moved = False
            for sid in order:
                r = rec[sid]
                if r["status"] != "pending" or r["attempts"] > 0:
                    continue
                dead = next((n for n in (spec[sid].get("needs") or []) if rec[n]["status"] in ("failed", "skipped", "cancelled")), None)
                if dead is not None:
                    r["status"], r["finished_ms"], r["reason"] = "skipped", now, f"dependency:{dead}"
                    emit("step_skipped", sid, 0, r["reason"])
                    moved = True

        if cancel_at is not None and cancel_at <= now:
            for sid in order:
                r = rec[sid]
                if r["status"] == "pending":
                    r["status"], r["finished_ms"], r["reason"] = "cancelled", now, "cancelled"
                    emit("step_cancelled", sid, r["attempts"], "cancelled")
                    cancelled = True
            break

        for sid in order:
            if len(inflight) >= conc:
                break
            r = rec[sid]
            if r["status"] != "pending" or sid in running_ids or ready_at[sid] > now:
                continue
            if any(rec[n]["status"] != "succeeded" for n in (spec[sid].get("needs") or [])):
                continue
            r["attempts"] += 1
            if r["started_ms"] is None:
                r["started_ms"] = now
            dur, outcome = attempt_plan(sid, r["attempts"])
            tmo = spec[sid].get("timeout_ms") or 0
            if tmo > 0 and dur > tmo:
                dur, outcome = tmo, "timeout"
            seq += 1
            inflight.append({"id": sid, "end": now + dur, "outcome": outcome, "seq": seq})
            running_ids.add(sid)
            emit("step_started", sid, r["attempts"])

        pending = [k for k in order if rec[k]["status"] == "pending"]
        if not pending and not inflight:
            break
        wake = [f["end"] for f in inflight]
        wake += [ready_at[k] for k in pending if k not in running_ids and rec[k]["attempts"] > 0 and ready_at[k] > now]
        if cancel_at is not None and cancel_at > now:
            wake.append(cancel_at)
        if not wake:
            break
        now = min(wake)

    if cancelled:
        status = "cancelled"
    elif all(r["status"] == "succeeded" for r in rec.values()):
        status = "succeeded"
    else:
        status = "failed"
    emit("run_finished", "", 0, status)
    return {"run_status": status, "finished_ms": now, "steps": rec, "events": events}


def simulate_or_error(case: dict[str, Any]) -> dict[str, Any]:
    try:
        return simulate(case)
    except CaseError as e:
        return {"error": str(e)}
