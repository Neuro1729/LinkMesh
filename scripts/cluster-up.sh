#!/usr/bin/env bash
# Starts a local controller plus N nodes for demos and benchmarking.
# On separate machines you do not need this: run the two commands it prints.
set -euo pipefail
cd "$(dirname "$0")/.."

NODES="${NODES:-3}"
REPLICATION="${REPLICATION:-2}"
REDUCERS="${REDUCERS:-2}"
CONTROLLER_PORT="${CONTROLLER_PORT:-9000}"
OUTPUT="${OUTPUT:-output/backlinks.tsv}"
CONTROLLER_ARGS="${CONTROLLER_ARGS:-}"
NODE_ARGS="${NODE_ARGS:-}"
SLOW_NODE="${SLOW_NODE:-}"
SLOW_DELAY_MS="${SLOW_DELAY_MS:-60}"
NODE_HEAP="${NODE_HEAP:-512m}"
NODE_JAVA_OPTS="${NODE_JAVA_OPTS:-}"
CONTROLLER_HEAP="${CONTROLLER_HEAP:-512m}"

mkdir -p logs output
rm -rf linkmesh-data
: > logs/pids

java "-Xmx${CONTROLLER_HEAP}" -jar build/linkmesh.jar controller \
  --port "$CONTROLLER_PORT" --replication "$REPLICATION" --reducers "$REDUCERS" \
  --output "$OUTPUT" $CONTROLLER_ARGS > logs/controller.log 2>&1 &
echo $! >> logs/pids
sleep 2

for i in $(seq 1 "$NODES"); do
  EXTRA=""
  if [ "$SLOW_NODE" = "$i" ]; then
    EXTRA="--parseDelayMs $SLOW_DELAY_MS"
    echo "  node-$i is the designated slow node (${SLOW_DELAY_MS}ms per file)"
  fi
  java "-Xmx${NODE_HEAP}" $NODE_JAVA_OPTS -jar build/linkmesh.jar worker \
    --controller "127.0.0.1:$CONTROLLER_PORT" \
    --id "node-$i" --port "$((7100 + i))" --advertise 127.0.0.1 \
    --data "linkmesh-data/node-$i" $NODE_ARGS $EXTRA > "logs/node-$i.log" 2>&1 &
  echo $! >> logs/pids
done

# Wait for every node to appear in the controller log rather than sleeping blindly.
for _ in $(seq 1 120); do
  joined=$(grep -c "joined as" logs/controller.log || true)
  [ "$joined" -ge "$NODES" ] && break
  sleep 0.5
done

echo "Cluster up: controller on 127.0.0.1:$CONTROLLER_PORT with $NODES nodes"
