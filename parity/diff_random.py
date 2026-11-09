"""Differential test: random cases through the Go binary and the Python reference.

    python parity/diff_random.py [N] [SEED]

Prints the first mismatch with the case that caused it, and exits 1.
"""
import json
import os
import random
import subprocess
import sys
from pathlib import Path

from reference import simulate_or_error

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "kernel" / "simulate.bin"


def build():
    env = {**os.environ, "PATH": os.environ["PATH"] + ":/usr/local/go/bin"}
    subprocess.run(["go", "build", "-o", str(BIN), "./cmd/simulate"], cwd=ROOT / "kernel", check=True, env=env)


def random_case(rng: random.Random) -> dict:
    n = rng.randint(1, 8)
    ids = [f"s{i}" for i in range(n)]
    steps, script = [], {}
    for i, sid in enumerate(ids):
        needs = [d for d in ids[:i] if rng.random() < 0.3]
        s = {"id": sid, "needs": needs}
        if rng.random() < 0.6:
            s["max_attempts"] = rng.randint(1, 4)
        if rng.random() < 0.4:
            s["timeout_ms"] = rng.choice([5, 10, 20, 50])
        if rng.random() < 0.5:
            s["backoff_ms"] = rng.choice([0, 1, 5, 10])
        steps.append(s)
        if rng.random() < 0.8:
            script[sid] = [
                {"outcome": rng.choice(["ok", "ok", "fail"]), "dur_ms": rng.choice([0, 1, 5, 10, 10, 25, 60])}
                for _ in range(rng.randint(1, 4))
            ]
    # shuffle list order but keep ids, so list order differs from dependency order
    if rng.random() < 0.5:
        rng.shuffle(steps)
    case = {"steps": steps, "script": script}
    if rng.random() < 0.8:
        case["concurrency"] = rng.randint(1, 4)
    if rng.random() < 0.35:
        case["cancel_at_ms"] = rng.choice([0, 3, 10, 25, 40, 80])
    return case


def break_case(rng: random.Random, case: dict) -> dict:
    """Make a valid case invalid in one to three ways at once.

    Several faults at once is the point: rule 6 says which error wins
    (duplicate_step, then unknown_dependency, then cycle), and only a case
    with more than one fault can tell a right order from a wrong one.
    """
    steps = [dict(s, needs=list(s["needs"])) for s in case["steps"]]
    faults = rng.sample(["dup", "unknown", "cycle", "self"], rng.randint(1, 3))
    for f in faults:
        if f == "dup":
            steps.insert(rng.randrange(len(steps) + 1), {"id": rng.choice(steps)["id"], "needs": []})
        elif f == "unknown":
            rng.choice(steps)["needs"].append(f"ghost{rng.randint(0, 9)}")
        elif f == "self":
            s = rng.choice(steps)
            s["needs"].append(s["id"])
        elif f == "cycle" and len(steps) >= 2:
            a, b = rng.sample(steps, 2)
            a["needs"].append(b["id"])
            b["needs"].append(a["id"])
    return {**case, "steps": steps}


def run_both(case: dict) -> tuple[dict, dict]:
    out = subprocess.run([str(BIN)], input=json.dumps(case), capture_output=True, text=True, check=True)
    return json.loads(out.stdout), simulate_or_error(case)


def mismatch(kind: str, i: int, seed: int, case: dict, go: dict, py: dict) -> None:
    print(f"MISMATCH at {kind} case {i} (seed {seed})")
    print("case:", json.dumps(case))
    print("go:  ", json.dumps(go))
    print("py:  ", json.dumps(py))
    sys.exit(1)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    build()
    rng = random.Random(seed)
    stats = {"succeeded": 0, "failed": 0, "cancelled": 0}
    for i in range(n):
        case = random_case(rng)
        go, py = run_both(case)
        if go != py:
            mismatch("valid", i, seed, case, go, py)
        stats[go["run_status"]] += 1

    # Invalid cases use their own RNG so the valid stream above is unchanged for any seed.
    bad_rng = random.Random(f"invalid-{seed}")
    errors: dict[str, int] = {}
    n_bad = max(1, n // 5)
    for i in range(n_bad):
        case = break_case(bad_rng, random_case(bad_rng))
        go, py = run_both(case)
        if go != py:
            mismatch("invalid", i, seed, case, go, py)
        kind = go["error"].split(":")[0] if "error" in go else "accepted"
        errors[kind] = errors.get(kind, 0) + 1
    print(f"ok: {n} random cases, Go == Python  {stats}")
    print(f"ok: {n_bad} invalid cases, Go == Python  {dict(sorted(errors.items()))}")


if __name__ == "__main__":
    main()
