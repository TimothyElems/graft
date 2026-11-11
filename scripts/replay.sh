#!/usr/bin/env bash
# Replay a support bundle in the kernel: does the real run match what the scheduling
# rules decide for the same attempt durations? See docs/replay.md.
#
#   scripts/replay.sh graft-doctor-run_x.tar.gz [tolerance_ms]
#
# Exit 0: same events (the service followed the rules). 1: different (first
# difference printed). 2: the bundle has no case.json (the run had not finished;
# its manifest says why) or is not a bundle.
#
# The bundle is input from a customer: nothing is extracted to disk. Only case.json
# and events.json are read, by name, straight from the archive.
set -euo pipefail
BUNDLE="${1:?usage: scripts/replay.sh <bundle.tar.gz> [tolerance_ms]}"
TOL="${2:-50}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if ! tar -xzOf "$BUNDLE" manifest.json > "$WORK/manifest.json" 2>/dev/null; then
  echo "replay: $BUNDLE is not a Graft bundle (no manifest.json)" >&2; exit 2
fi
if ! tar -xzOf "$BUNDLE" case.json > "$WORK/case.json" 2>/dev/null; then
  echo "replay: no case.json in this bundle. From its manifest:" >&2
  python3 -c 'import json,sys; [print("  " + n, file=sys.stderr) for n in json.load(open(sys.argv[1]))["notes"]]' "$WORK/manifest.json"
  exit 2
fi
tar -xzOf "$BUNDLE" events.json > "$WORK/events.json"
python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print("bundle", m["run_id"], "from graftd", m["service_version"])' "$WORK/manifest.json"
(cd "$ROOT/kernel" && go build -o "$WORK/simulate" ./cmd/simulate)
"$WORK/simulate" -compare "$WORK/events.json" -tolerance-ms "$TOL" < "$WORK/case.json"
