#!/bin/sh
# Build the SDK wheel, install ONLY that wheel into a fresh venv, and type-check
# tests/typing/sample_app.py against it, outside the repo, as a user would.
set -eu
cd "$(dirname "$0")/.."
PYBIN="${PYBIN:-python3}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

"$PYBIN" -m build --wheel --outdir "$WORK/dist" sdk/python >/dev/null
WHEEL="$(ls "$WORK"/dist/*.whl)"
echo "built $(basename "$WHEEL")"
echo "wheel contains: $("$PYBIN" -m zipfile -l "$WHEEL" | awk 'NR>1{print $1}' | grep '^graft/' | tr '\n' ' ')"

"$PYBIN" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q "$WHEEL" 'mypy==2.4.1' 'pyright==1.1.414'
cp sdk/python/tests/typing/sample_app.py "$WORK/sample_app.py"   # outside the repo: no src/ on any path
cd "$WORK"
echo "--- mypy --strict (installed wheel only)"
./venv/bin/mypy --strict --python-version 3.10 sample_app.py
echo "--- pyright strict (installed wheel only)"
printf '{"typeCheckingMode": "strict", "pythonVersion": "3.10", "venvPath": ".", "venv": "venv"}' > pyrightconfig.json
./venv/bin/pyright sample_app.py
echo "--- the graft command is installed and runs"
./venv/bin/graft doctor --help | head -1
