#!/usr/bin/env bash
# Makes one node artificially slow and shows the scheduler starting a backup
# attempt elsewhere, taking whichever finishes first.
set -euo pipefail
cd "$(dirname "$0")/.."

SLOW_NODE="${SLOW_NODE:-3}"
SLOW_DELAY_MS="${SLOW_DELAY_MS:-80}"

./scripts/build.sh >/dev/null
trap './scripts/cluster-down.sh >/dev/null 2>&1 || true' EXIT

if [ ! -d build/synthetic ]; then
  java -jar build/linkmesh.jar gen --out build/synthetic --pages 12000 --partitions 12 \
    --links 8 --pagesPerFile 20 --skew 1.0 >/dev/null
fi

run_once() {
  local label="$1" extra="$2"
  ./scripts/cluster-down.sh >/dev/null 2>&1 || true
  sleep 1
  SLOW_NODE="$SLOW_NODE" SLOW_DELAY_MS="$SLOW_DELAY_MS" \
    CONTROLLER_ARGS="$extra" ./scripts/cluster-up.sh >/dev/null
  java -jar build/linkmesh.jar ingest --controller 127.0.0.1:9000 --prepared build/synthetic >/dev/null
  java -jar build/linkmesh.jar submit --controller 127.0.0.1:9000 --job "$label" >/dev/null
  local ms
  ms=$(grep "METRIC map_stage_ms" logs/controller.log | tail -1 | cut -d= -f2)
  echo "$ms"
  cp output/backlinks.tsv "output/$label.tsv"
  cp logs/controller.log "logs/controller-$label.log"
}

echo "node-$SLOW_NODE is slowed to ${SLOW_DELAY_MS}ms per file; the others run at full speed."
echo
WITHOUT=$(run_once "no-speculation" "--noSpeculation")
echo "map stage without speculation : ${WITHOUT} ms"
WITH=$(run_once "speculation" "--minSpeculativeMs 1000 --speculativeMultiplier 1.5")
echo "map stage with speculation    : ${WITH} ms"

echo
echo "== speculation decisions =="
grep -E "straggler|backup attempt|cancelled losing" logs/controller-speculation.log || echo "(none fired)"

echo
echo "== correctness =="
A=$(sort output/no-speculation.tsv | sha256sum | awk '{print $1}')
B=$(sort output/speculation.tsv | sha256sum | awk '{print $1}')
echo "without speculation sha256 : $A"
echo "with    speculation sha256 : $B"
if [ "$A" = "$B" ]; then
  echo "IDENTICAL - running a partition twice did not change the result."
  echo "(Safe because the reduce stores each edge in a Set, so re-applying an edge is a no-op.)"
else
  echo "MISMATCH - investigate."
  exit 1
fi
