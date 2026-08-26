#!/usr/bin/env bash
# Scaling and tuning benchmark against a prepared corpus.
#
# MODE=slots   One node, sweeping --slots. This is the single-machine tuning
#              curve, and on one box it is the knob that actually moves.
#
# MODE=fixed   Total concurrent tasks held constant, split across more and more
#              nodes. Same work in flight either way, so the difference is the
#              cost of spreading it out rather than the benefit of more CPU.
#
# MODE=nodes   N nodes each with a full slot allocation. This is the "add another
#              machine" curve. On a single host it stops meaning much once the
#              cores are saturated, since the nodes then share the same CPU.
#
# Ingest runs once per configuration and the job is then repeated TRIALS times,
# because jobs are repeatable against loaded data and re-loading every trial
# would swamp what is being measured.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${MODE:-fixed}"
CORPUS="${CORPUS:-build/wiki-corpus}"
TRIALS="${TRIALS:-3}"
PARSER_THREADS="${PARSER_THREADS:-2}"
TOTAL_SLOTS="${TOTAL_SLOTS:-4}"
SLOTS_PER_NODE="${SLOTS_PER_NODE:-4}"
NODE_LIST="${NODE_LIST:-1 2 4}"
SLOTS_LIST="${SLOTS_LIST:-1 2 4 8}"
NODE_HEAP="${NODE_HEAP:-900m}"

if [ ! -d "$CORPUS" ]; then
  echo "corpus not found: $CORPUS" >&2
  echo "build one with:  java -jar build/linkmesh.jar ingest --wikipedia DUMP --out $CORPUS --partitions 64" >&2
  exit 2
fi

./scripts/build.sh >/dev/null
trap './scripts/cluster-down.sh >/dev/null 2>&1 || true' EXIT

median() { sort -n | awk '{a[NR]=$1} END {if (NR%2) print a[(NR+1)/2]; else printf "%.0f\n", (a[NR/2]+a[NR/2+1])/2}'; }

# Runs one configuration and echoes "map_ms total_ms".
run_config() {
  local nodes="$1" slots="$2" tag="$3"
  ./scripts/cluster-down.sh >/dev/null 2>&1 || true
  sleep 1
  local rf=2 red=2
  if [ "$nodes" -lt 2 ]; then rf=1; red=1; fi

  NODES="$nodes" REPLICATION="$rf" REDUCERS="$red" NODE_HEAP="$NODE_HEAP" \
    NODE_ARGS="--parserThreads $PARSER_THREADS --slots $slots" \
    CONTROLLER_ARGS="--minSpeculativeMs 100000" \
    ./scripts/cluster-up.sh >/dev/null

  java -jar build/linkmesh.jar ingest --controller 127.0.0.1:9000 --prepared "$CORPUS" >/dev/null

  local maps="" totals=""
  for t in $(seq 1 "$TRIALS"); do
    java -jar build/linkmesh.jar submit --controller 127.0.0.1:9000 --job "bench-$tag-$t" >/dev/null
    maps="$maps$(grep 'METRIC map_stage_ms' logs/controller.log | tail -1 | cut -d= -f2)"$'\n'
    totals="$totals$(grep 'METRIC total_job_ms' logs/controller.log | tail -1 | cut -d= -f2)"$'\n'
  done
  cp output/backlinks.tsv "output/bench-$tag.tsv"
  echo "$(printf '%s' "$maps" | grep -v '^$' | median) $(printf '%s' "$totals" | grep -v '^$' | median)"
}

CSV="output/benchmark-$MODE.csv"
echo "mode=$MODE corpus=$CORPUS trials=$TRIALS cores=$(nproc 2>/dev/null || echo '?')"
echo

BASE=""
case "$MODE" in
  slots)
    echo "nodes,slots,total_tasks,median_map_ms,median_total_ms,speedup_vs_first,trials" > "$CSV"
    for S in $SLOTS_LIST; do
      read -r MMAP MTOT <<< "$(run_config 1 "$S" "slots-$S")"
      [ -z "$BASE" ] && BASE="$MMAP"
      SP=$(awk -v b="$BASE" -v m="$MMAP" 'BEGIN{printf "%.2f", b/m}')
      printf '1 node, slots=%-2s : map %6s ms  total %6s ms  %sx\n' "$S" "$MMAP" "$MTOT" "$SP"
      echo "1,$S,$S,$MMAP,$MTOT,$SP,$TRIALS" >> "$CSV"
    done
    ;;
  fixed)
    echo "nodes,slots_per_node,total_tasks,median_map_ms,median_total_ms,speedup_vs_first,trials" > "$CSV"
    for N in $NODE_LIST; do
      S=$(( TOTAL_SLOTS / N )); [ "$S" -lt 1 ] && S=1
      read -r MMAP MTOT <<< "$(run_config "$N" "$S" "fixed-$N")"
      [ -z "$BASE" ] && BASE="$MMAP"
      SP=$(awk -v b="$BASE" -v m="$MMAP" 'BEGIN{printf "%.2f", b/m}')
      printf '%d node(s) x %d slot(s) (=%d tasks) : map %6s ms  total %6s ms  %sx\n' \
        "$N" "$S" "$((N * S))" "$MMAP" "$MTOT" "$SP"
      echo "$N,$S,$((N * S)),$MMAP,$MTOT,$SP,$TRIALS" >> "$CSV"
    done
    ;;
  nodes)
    echo "nodes,slots_per_node,total_tasks,median_map_ms,median_total_ms,speedup_vs_1,trials" > "$CSV"
    for N in $NODE_LIST; do
      read -r MMAP MTOT <<< "$(run_config "$N" "$SLOTS_PER_NODE" "nodes-$N")"
      [ -z "$BASE" ] && BASE="$MMAP"
      SP=$(awk -v b="$BASE" -v m="$MMAP" 'BEGIN{printf "%.2f", b/m}')
      printf '%d node(s) x %d slots (=%d tasks) : map %6s ms  total %6s ms  %sx\n' \
        "$N" "$SLOTS_PER_NODE" "$((N * SLOTS_PER_NODE))" "$MMAP" "$MTOT" "$SP"
      echo "$N,$SLOTS_PER_NODE,$((N * SLOTS_PER_NODE)),$MMAP,$MTOT,$SP,$TRIALS" >> "$CSV"
    done
    ;;
  *)
    echo "unknown MODE: $MODE (expected slots, fixed, or nodes)" >&2
    exit 2
    ;;
esac

echo
echo "Wrote $CSV"
echo "Verifying every configuration produced the same index:"
./scripts/verify.sh output/bench-*.tsv
