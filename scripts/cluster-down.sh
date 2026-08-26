#!/usr/bin/env bash
# Stops everything started by cluster-up.sh and waits for the processes to
# actually exit. Returning early leaves file handles open, which on Windows makes
# the next run fail to clear its data directory.
set -uo pipefail
cd "$(dirname "$0")/.."

if [ -f logs/pids ]; then
  PIDS=$(cat logs/pids)
  for pid in $PIDS; do
    kill "$pid" 2>/dev/null || true
  done

  for _ in $(seq 1 40); do
    alive=0
    for pid in $PIDS; do
      if kill -0 "$pid" 2>/dev/null; then alive=1; fi
    done
    [ "$alive" -eq 0 ] && break
    sleep 0.25
  done

  for pid in $PIDS; do
    kill -9 "$pid" 2>/dev/null || true
  done
  rm -f logs/pids
fi

echo "Cluster stopped."
