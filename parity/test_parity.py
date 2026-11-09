"""Python side of the contract: the same fixtures the Go tests read, plus schema checks."""
import json
from pathlib import Path

import pytest

from reference import simulate_or_error

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = sorted((ROOT / "fixtures" / "pipeline").glob("*.json"))


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.name)
def test_python_reference_matches_fixture(path):
    fx = json.loads(path.read_text())
    assert simulate_or_error(fx["case"]) == fx["expect"]


def test_there_are_fixtures():
    assert len(FIXTURES) >= 15


def test_every_kernel_event_validates_against_the_event_schema():
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((ROOT / "contract" / "events.schema.json").read_text())
    validator = jsonschema.Draft202012Validator(schema)
    checked = 0
    for path in FIXTURES:
        fx = json.loads(path.read_text())
        if "events" not in fx["expect"]:
            continue
        for i, e in enumerate(fx["expect"]["events"], start=1):
            api = {
                "id": f"evt_{path.stem}_{i}",
                "seq": i,
                "run_id": "run_fixture",
                "branch_id": "br_fixture",
                "at": "2026-10-25T00:00:00Z",
                "type": e["type"],
                "at_ms": e["at_ms"],
                "step": e["step"] or None,
                "attempt": e["attempt"],
                "reason": e["reason"] or None,
            }
            errs = list(validator.iter_errors(api))
            assert not errs, (path.name, api, [x.message for x in errs])
            checked += 1
    assert checked > 50


@pytest.mark.parametrize("example,component", [("branch", "Branch"), ("run", "Run"), ("problem", "Problem")])
def test_examples_match_openapi_components(example, component):
    yaml = pytest.importorskip("yaml")
    jsonschema = pytest.importorskip("jsonschema")
    spec = yaml.safe_load((ROOT / "contract" / "openapi.yaml").read_text())
    schema = {"$ref": f"#/components/schemas/{component}", "components": spec["components"]}
    doc = json.loads((ROOT / "contract" / "examples" / f"{example}.json").read_text())
    errs = list(jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).iter_errors(doc))
    assert not errs, [e.message for e in errs]
