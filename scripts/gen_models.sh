#!/bin/sh
# Regenerate sdk/python/src/graft/_models.py from the contract.
# CI runs this and fails if the result differs from the committed file (the drift gate).
# Every option is pinned, and so is the generator version (sdk/python/pyproject.toml, extra "codegen"):
# the same contract must always give byte-identical output.
set -eu
cd "$(dirname "$0")/.."
PY="${PY:-sdk/python/.venv/bin/python}"
OUT=sdk/python/src/graft/_models.py
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

"$PY" -I scripts/bundle_contract.py > "$TMP/contract.yaml"
"$PY" -m datamodel_code_generator \
  --input "$TMP/contract.yaml" --input-file-type openapi \
  --output "$TMP/_models.py" \
  --output-model-type pydantic_v2.BaseModel \
  --formatters builtin \
  --target-python-version 3.10 \
  --use-union-operator --use-standard-collections \
  --enum-field-as-literal all \
  --field-constraints --strict-nullable --use-default-kwarg \
  --extra-fields ignore \
  --use-schema-description --use-field-description \
  --disable-timestamp \
  --custom-file-header "# GENERATED from contract/openapi.yaml by scripts/gen_models.sh. Do not edit by hand."

mv "$TMP/_models.py" "$OUT"
echo "wrote $OUT"
