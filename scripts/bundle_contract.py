"""Inline contract/events.schema.json into openapi.yaml, so the model generator sees one document.

    python scripts/bundle_contract.py > bundled.yaml

The external $ref is the only one in the contract. If another appears, this fails loudly
rather than generating models that silently miss it.
"""
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = yaml.safe_load((ROOT / "contract" / "openapi.yaml").read_text())
event = json.loads((ROOT / "contract" / "events.schema.json").read_text())
for k in ("$schema", "$id"):
    event.pop(k, None)
# The if/then rules tie fields together (e.g. step is null on run events). Python types
# can't express them; the server and the schema test enforce them. Keep the plain fields.
event.pop("allOf", None)

schemas = spec["components"]["schemas"]
schemas["Event"] = event
spec["paths"]["/v1/stream"]["get"]["x-websocket"]["frame"] = {"$ref": "#/components/schemas/Event"}

leftover = [s for s in json.dumps(spec).split('"$ref": "')[1:] if not s.startswith("#/")]
if leftover:
    sys.exit(f"unresolved external $ref: {leftover[0][:60]}")
yaml.safe_dump(spec, sys.stdout, sort_keys=False)
