#!/bin/sh
# Interleaved A/B timing on one dataset: scripts/ab.sh DATASET RUNS "binA[:ENV=V]" "binB[:ENV=V]" ...
# Reports wall and CPU (user+sys) min/median. Binaries live in the sessions-data volume under /d/bin.
set -eu
DS="$1"; N="$2"; shift 2
MSYS_NO_PATHCONV=1 docker run --rm --cpus 8 --memory 24g -v sessions-data:/d rust:1-slim-bookworm bash -c '
  cat /d/'"$DS"'/*/*.csv > /dev/null
  TIMEFORMAT="%R %U %S"
  for r in $(seq 1 '"$N"'); do
    for spec in '"$*"'; do
      b=${spec%%:*}; e=""; [ "$spec" != "$b" ] && e=${spec#*:}
      t=$( { time env $e DATA_DIR=/d/'"$DS"' OUTPUT_DIR=/tmp/o NCPU=8 /d/bin/$b 2>/dev/null; } 2>&1 )
      echo "$spec $t"
    done
  done' | awk '{w[$1]=w[$1]" "int($2*1000); c[$1]=c[$1]" "int(($3+$4)*1000)}
  END {for (k in w) {n=split(w[k],a," "); asort(a); split(c[k],b," "); asort(b);
       printf "%-22s wall min %5d med %5d | cpu min %6d med %6d (n=%d)\n", k, a[1], a[int((n+1)/2)], b[1], b[int((n+1)/2)], n}}'
