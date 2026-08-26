#!/usr/bin/env bash
# Reproduces the publish/read race: a replica being deleted out from under a
# running map task.
#
# A node that rejoins still holding its old replicas pushes those partitions to
# RF+1 holders, so the planner schedules a DROP. If that lands on a node that is
# mid-scan, the task's files vanish and it dies with NoSuchFileException.
#
# JAR=build/linkmesh-before.jar ./scripts/reader-drop-demo.sh   # to see it fail
set -euo pipefail
cd "$(dirname "$0")/.."

JAR="${JAR:-build/linkmesh.jar}"
DELAY_MS="${DELAY_MS:-70}"
REJOIN_AFTER="${REJOIN_AFTER:-2}"

[ -f "$JAR" ] || { echo "no such jar: $JAR" >&2; exit 2; }
echo "== using $JAR =="

if [ ! -d build/synthetic ]; then
  java -jar "$JAR" gen --out build/synthetic --pages 12000 --partitions 12 \
    --links 8 --pagesPerFile 20 --skew 1.0 >/dev/null
fi

cleanup() { [ -f logs/pids ] && while read -r p; do kill -9 "$p" 2>/dev/null || true; done < logs/pids; }
trap cleanup EXIT

mkdir -p logs output
rm -rf linkmesh-data
: > logs/pids

start_node() {
  java -Xmx512m -jar "$JAR" worker --controller 127.0.0.1:9000 \
    --id "node-$1" --port "$((7100 + $1))" --advertise 127.0.0.1 \
    --data "linkmesh-data/node-$1" --parseDelayMs "$DELAY_MS" > "logs/node-$1.log" 2>&1 &
  echo $! >> logs/pids
}

java -Xmx512m -jar "$JAR" controller --port 9000 --replication 2 --reducers 2 \
  --output output/backlinks.tsv --minSpeculativeMs 100000 > logs/controller.log 2>&1 &
echo $! >> logs/pids
sleep 2

for i in 1 2 3; do start_node "$i"; done
for _ in $(seq 1 120); do
  [ "$(grep -c 'joined as' logs/controller.log || true)" -ge 3 ] && break; sleep 0.5
done

java -jar "$JAR" ingest --controller 127.0.0.1:9000 --prepared build/synthetic >/dev/null
echo "cluster up, corpus loaded"

# Kill node-3 but keep its data directory, then let re-replication settle.
NODE3=$(tail -1 logs/pids)
kill -9 "$NODE3" 2>/dev/null || true
echo ">>> node-3 killed (data left on disk)"
sleep 4

# Job starts, then node-3 comes back still holding its old replicas.
java -jar "$JAR" submit --controller 127.0.0.1:9000 --job reader-drop >/dev/null &
SUBMIT=$!
sleep "$REJOIN_AFTER"
echo ">>> node-3 rejoins mid-job, replicas now over-replicated"
start_node 3
wait $SUBMIT || true

echo
echo "== drops planned against a live reader =="
grep -E "dropped surplus|still in use|still reading it" logs/controller.log logs/node-*.log | head -8 || echo "  (none)"

echo
echo "== verdict =="
if grep -q "NoSuchFileException" logs/node-*.log; then
  echo "RACE HIT: a map task read a partition that was deleted underneath it"
  grep -h "NoSuchFileException" logs/node-*.log | head -3
  exit 1
fi

# A pass only means something if the rebalancer actually tried to drop a replica
# while the job was running. Timing can miss that on a slow runner, and a silent
# vacuous pass would look identical to a real one.
DROPS=$(grep -c "dropped surplus\|still in use" logs/controller.log || true)
REFUSALS=$(cat logs/node-*.log | grep -c "still reading it" || true)
if [ "$DROPS" -eq 0 ]; then
  echo "no task lost its files mid-scan"
  echo "NOTE: no drop was planned during the job, so this run did not exercise the race"
  exit 0
fi
echo "no task lost its files mid-scan ($DROPS drops planned, $REFUSALS refused by a live reader)"
