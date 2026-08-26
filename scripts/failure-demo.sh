#!/usr/bin/env bash
# Kills a node in the middle of a job and shows that the job still finishes with
# byte-identical output, because every partition has a second replica.
set -euo pipefail
cd "$(dirname "$0")/.."

KILL_NODE="${KILL_NODE:-3}"
KILL_AFTER="${KILL_AFTER:-2}"
DELAY_MS="${DELAY_MS:-40}"

./scripts/build.sh >/dev/null
trap './scripts/cluster-down.sh >/dev/null 2>&1 || true' EXIT

if [ ! -d build/synthetic ]; then
  java -jar build/linkmesh.jar gen --out build/synthetic --pages 12000 --partitions 12 \
    --links 8 --pagesPerFile 20 --skew 1.0 >/dev/null
fi

echo "== baseline run, all nodes healthy =="
CONTROLLER_ARGS="--parseDelayMs $DELAY_MS --minSpeculativeMs 100000" ./scripts/cluster-up.sh
java -jar build/linkmesh.jar ingest --controller 127.0.0.1:9000 --prepared build/synthetic >/dev/null
java -jar build/linkmesh.jar submit --controller 127.0.0.1:9000 --job baseline >/dev/null
cp output/backlinks.tsv output/baseline.tsv
echo "baseline: $(wc -l < output/baseline.tsv) keys"
./scripts/cluster-down.sh >/dev/null
sleep 1

echo
echo "== failure run, node-$KILL_NODE killed ${KILL_AFTER}s into the job =="
CONTROLLER_ARGS="--parseDelayMs $DELAY_MS --minSpeculativeMs 100000" ./scripts/cluster-up.sh
java -jar build/linkmesh.jar ingest --controller 127.0.0.1:9000 --prepared build/synthetic >/dev/null

VICTIM=$(sed -n "$((KILL_NODE + 1))p" logs/pids)
java -jar build/linkmesh.jar submit --controller 127.0.0.1:9000 --job failure-demo >/dev/null &
SUBMIT=$!
sleep "$KILL_AFTER"
echo ">>> kill -9 node-$KILL_NODE (pid $VICTIM)"
kill -9 "$VICTIM" 2>/dev/null || true
wait $SUBMIT

echo
echo "== what the controller did =="
grep -E "declared DEAD|rescheduled|replicating|no surviving" logs/controller.log | head -12

echo
echo "== verdict =="
BASE=$(sort output/baseline.tsv | sha256sum | awk '{print $1}')
AFTER=$(sort output/backlinks.tsv | sha256sum | awk '{print $1}')
echo "baseline sha256 : $BASE  ($(wc -l < output/baseline.tsv) keys)"
echo "failure  sha256 : $AFTER  ($(wc -l < output/backlinks.tsv) keys)"
if [ "$BASE" = "$AFTER" ]; then
  echo "IDENTICAL - the node death did not change the result."
  echo
  echo "== recovery =="
  grep -E "METRIC (node_failures|failure_last_contact_ms|tasks_rescheduled|replications_issued|replication_recovery_ms)" logs/controller.log     | sed 's/METRIC /  /' || true
else
  echo "MISMATCH - results diverged, investigate."
  exit 1
fi
