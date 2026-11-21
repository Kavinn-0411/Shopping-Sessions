#!/bin/sh
# Interleaved A/B timing on one dataset: scripts/ab.sh DATASET RUNS "binA[:ENV=V]" "binB[:ENV=V]" ...
# Binaries live in the sessions-data volume under /d/bin (scripts/bench.sh puts them there).
set -eu
DS="$1"; N="$2"; shift 2
MSYS_NO_PATHCONV=1 docker run --rm --cpus 8 --memory 24g -v sessions-data:/d rust:1-slim-bookworm bash -c '
  cat /d/'"$DS"'/*/*.csv > /dev/null
  for r in $(seq 1 '"$N"'); do
    for spec in '"$*"'; do
      b=${spec%%:*}; e=""; [ "$spec" != "$b" ] && e=${spec#*:}
      s=$(date +%s%N); env $e DATA_DIR=/d/'"$DS"' OUTPUT_DIR=/tmp/o NCPU=8 /d/bin/$b 2>/dev/null
      echo "$spec $(( ($(date +%s%N)-s)/1000000 ))"
    done
  done' | awk '{t[$1]=t[$1]" "$2} END {for (k in t) {n=split(t[k],a," "); asort(a); printf "%-22s min %5d  med %5d  (n=%d)\n", k, a[1], a[int((n+1)/2)], n}}'
