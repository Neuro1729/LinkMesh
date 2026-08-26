#!/usr/bin/env bash
# Does parallel speedup get worse as the dataset grows?
#
# Comparing two different datasets cannot answer that, because the datasets
# differ in more than size. This takes ONE corpus and feeds it progressively more
# of itself, holding heap, reducers, partitions-per-slice and machine constant,
# so data volume is the only thing that changes.
#
# Corpora are sharded by hash of the source URL, so taking the first N partition
# directories is a uniform random sample of the whole graph, not a biased slice.
#
# GC time is recorded per run, because the obvious suspect for "parallelism stops
# helping once the data is big" is the collector eating the cores that the extra
# slots were supposed to use.
set -euo pipefail
cd "$(dirname "$0")/.."

CORPUS="${CORPUS:-build/corpus}"
FRACTIONS="${FRACTIONS:-16 32 64 128}"
SLOTS_LIST="${SLOTS_LIST:-1 2 4}"
TRIALS="${TRIALS:-1}"
NODE_HEAP="${NODE_HEAP:-11g}"
PARSER_THREADS="${PARSER_THREADS:-2}"

if [ ! -d "$CORPUS" ]; then
  echo "corpus not found: $CORPUS" >&2
  exit 2
fi

./scripts/build.sh >/dev/null
trap './scripts/cluster-down.sh >/dev/null 2>&1 || true' EXIT

TOTAL_PARTS=$(ls "$CORPUS" | wc -l | tr -d ' ')
CSV="output/scaling-vs-size.csv"
echo "partitions,edges,slots,map_ms,us_per_edge,speedup_vs_slots1,gc_ms,gc_percent" > "$CSV"

echo "corpus=$CORPUS ($TOTAL_PARTS partitions)  heap=$NODE_HEAP  cores=$(nproc 2>/dev/null || echo '?')"
echo

for N in $FRACTIONS; do
  [ "$N" -gt "$TOTAL_PARTS" ] && continue

  SLICE="build/slice-$N"
  rm -rf "$SLICE"; mkdir -p "$SLICE"
  ls "$CORPUS" | sort | head -n "$N" | while read -r part; do
    cp -r "$CORPUS/$part" "$SLICE/"
  done

  EDGES=$(grep -rc '^LINK ' "$SLICE" 2>/dev/null | awk -F: '{s+=$2} END {print s+0}')
  echo "=== $N partitions, $EDGES edges ==="

  BASE=""
  for S in $SLOTS_LIST; do
    ./scripts/cluster-down.sh >/dev/null 2>&1 || true
    sleep 1
    NODES=1 REPLICATION=1 REDUCERS=1 NODE_HEAP="$NODE_HEAP" CONTROLLER_HEAP=512m \
      NODE_JAVA_OPTS="-Xlog:gc:file=logs/gc-$N-$S.log" \
      NODE_ARGS="--parserThreads $PARSER_THREADS --slots $S" \
      CONTROLLER_ARGS="--minSpeculativeMs 100000" \
      ./scripts/cluster-up.sh >/dev/null

    java -jar build/linkmesh.jar ingest --controller 127.0.0.1:9000 --prepared "$SLICE" >/dev/null

    BEST=""
    for T in $(seq 1 "$TRIALS"); do
      java -jar build/linkmesh.jar submit --controller 127.0.0.1:9000 --job "size-$N-$S-$T" >/dev/null
      MS=$(grep 'METRIC map_stage_ms' logs/controller.log | tail -1 | cut -d= -f2)
      if [ -z "$BEST" ] || [ "$MS" -lt "$BEST" ]; then BEST="$MS"; fi
    done

    # Total pause time the collector reported. The duration is the last field of
    # each Pause line: "... Pause Young (Normal) (...) 51M->6M(92M) 5.035ms".
    GC_LOG="logs/gc-$N-$S.log"
    GC_MS=0
    if [ -f "$GC_LOG" ]; then
      GC_MS=$(awk '/Pause/ {d=$NF; sub(/ms$/, "", d); s+=d} END {printf "%.0f", s+0}' "$GC_LOG")
    fi
    [ -z "$GC_MS" ] && GC_MS=0
    ./scripts/cluster-down.sh >/dev/null 2>&1 || true

    [ -z "$BASE" ] && BASE="$BEST"
    SPEEDUP=$(awk -v b="$BASE" -v m="$BEST" 'BEGIN{printf "%.2f", b/m}')
    USPE=$(awk -v m="$BEST" -v e="$EDGES" 'BEGIN{if(e>0) printf "%.3f", m*1000/e; else print "0"}')
    GCPCT=$(awk -v g="$GC_MS" -v m="$BEST" 'BEGIN{if(m>0) printf "%.1f", 100*g/m; else print "0"}')

    printf '  slots=%-2s map %7s ms  %6s us/edge  %5sx  gc %6s ms (%s%%)\n' \
      "$S" "$BEST" "$USPE" "$SPEEDUP" "$GC_MS" "$GCPCT"
    echo "$N,$EDGES,$S,$BEST,$USPE,$SPEEDUP,$GC_MS,$GCPCT" >> "$CSV"
  done
  rm -rf "$SLICE"
  echo
done

echo "Wrote $CSV"
