#!/bin/sh
# Build graftd, start it, run the SDK e2e tests against it, stop it.
# For machines without Docker; CI runs the same tests against `docker compose up`.
set -eu
cd "$(dirname "$0")/.."
PORT="${PORT:-18080}"
BIN="$(mktemp -d)/graftd"
PY="${PY:-sdk/python/.venv/bin/python}"
[ -x "$PY" ] || PY=python3

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT is already in use; the tests would hit the wrong server. Stop it or set PORT." >&2
  exit 1
fi
(cd service && go build -o "$BIN" ./cmd/graftd)
GRAFT_TOKEN=local-e2e-token-0000 GRAFT_ADDR="127.0.0.1:$PORT" "$BIN" >/dev/null 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null || true' EXIT
for _ in 1 2 3 4 5 6 7 8 9 10; do
  GRAFT_ADDR="127.0.0.1:$PORT" "$BIN" -healthcheck >/dev/null 2>&1 && break
  sleep 0.2
done
cd sdk/python
GRAFT_E2E=1 GRAFT_URL="http://127.0.0.1:$PORT" GRAFT_TOKEN=local-e2e-token-0000 "../../$PY" -m pytest tests/e2e -v "$@"
