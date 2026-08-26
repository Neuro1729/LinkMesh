#!/usr/bin/env bash
# End-to-end local demo: build, start a cluster, generate a corpus, ingest it,
# run a job, and show where the data actually landed.
set -euo pipefail
cd "$(dirname "$0")/.."

PAGES="${PAGES:-12000}"
PARTITIONS="${PARTITIONS:-12}"
NODES="${NODES:-3}"

./scripts/build.sh
trap './scripts/cluster-down.sh >/dev/null 2>&1 || true' EXIT

echo
echo "== 1. generate a synthetic corpus (power-law link targets) =="
java -jar build/linkmesh.jar gen --out build/synthetic \
  --pages "$PAGES" --partitions "$PARTITIONS" --links 8 --pagesPerFile 20 --skew 1.0

echo
echo "== 2. start a $NODES-node cluster =="
NODES="$NODES" ./scripts/cluster-up.sh

echo
echo "== 3. distribute the corpus (controller decides placement, RF=2) =="
java -jar build/linkmesh.jar ingest --controller 127.0.0.1:9000 --prepared build/synthetic

echo
echo "== 4. where the replicas landed =="
java -jar build/linkmesh.jar status --controller 127.0.0.1:9000

echo
echo "== 5. run the backlink job =="
java -jar build/linkmesh.jar submit --controller 127.0.0.1:9000 --job demo

echo
echo "== 6. results =="
grep METRIC logs/controller.log
echo
echo "Output: $(wc -l < output/backlinks.tsv) backlink keys in output/backlinks.tsv"
head -3 output/backlinks.tsv
