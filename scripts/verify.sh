#!/usr/bin/env bash
# Confirms that several runs produced the same logical index.
# Order differs between runs because reducers stream back concurrently, so the
# comparison sorts first.
set -euo pipefail
cd "$(dirname "$0")/.."

FILES=("$@")
if [ "${#FILES[@]}" -eq 0 ]; then
  mapfile -t FILES < <(ls output/*.tsv 2>/dev/null)
fi
if [ "${#FILES[@]}" -eq 0 ]; then
  echo "no result files to compare" >&2
  exit 2
fi

BASE=""
for f in "${FILES[@]}"; do
  [ -f "$f" ] || { echo "missing: $f" >&2; exit 2; }
  HASH=$(sort "$f" | sha256sum | awk '{print $1}')
  printf '%-36s %6s keys  %s\n' "$f" "$(wc -l < "$f" | tr -d ' ')" "$HASH"
  if [ -z "$BASE" ]; then
    BASE="$HASH"
  elif [ "$HASH" != "$BASE" ]; then
    echo "ERROR: outputs differ" >&2
    exit 1
  fi
done

echo "OK: all compared outputs are identical after sorting."
