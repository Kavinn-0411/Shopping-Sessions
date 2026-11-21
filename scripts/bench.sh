#!/bin/sh
# Build the current source and run it on datasets in the sessions-data volume.
# Saves outputs to the volume under /d/out/<tag>/<dataset>; if REF is set, diffs each against /d/out/$REF/<dataset>.
# Usage: TAG=v2 REF=v1 scripts/bench.sh s1 s2 p20 ...
set -eu
cd "$(dirname "$0")/.."
ROOT="$(pwd -W 2>/dev/null || pwd)"
TAG="${TAG:-cur}"; REF="${REF:-}"; RUNS="${RUNS:-3}"
MSYS_NO_PATHCONV=1 docker run --rm --cpus 8 --memory 24g --memory-swap 24g \
  -v cargo-cache:/usr/local/cargo/registry -v sessions-target:/target -v sessions-data:/d \
  -v "$ROOT:/w" -w /w -e CARGO_TARGET_DIR=/target -e RUSTFLAGS="-C target-cpu=x86-64-v3" \
  rust:1-slim-bookworm bash -c '
    set -e
    cargo build --release 2>&1 | grep -E "^(error|warning)|-->|Finished" || true
    mkdir -p /d/bin && cp /target/release/sessions /d/bin/'"$TAG"'
    for ds in '"$*"'; do
      o=/d/out/'"$TAG"'/$ds; rm -rf $o; mkdir -p $o
      cat /d/$ds/events/*.csv /d/$ds/clicks/*.csv > /dev/null   # warm page cache
      for r in $(seq 1 '"$RUNS"'); do
        rm -rf $o/*; s=$(date +%s%N)
        DATA_DIR=/d/$ds OUTPUT_DIR=$o NCPU=8 /d/bin/'"$TAG"' 2> /tmp/log
        echo "$ds run$r: $(( ($(date +%s%N)-s)/1000000 )) ms | $(tail -1 /tmp/log)"
      done
    done'
if [ -n "$REF" ]; then
  MSYS_NO_PATHCONV=1 docker run --rm -v sessions-data:/d -v "$ROOT/scripts:/s" sessions-checker sh -c '
    for ds in '"$*"'; do echo "== $ds vs '"$REF"'"; python /s/compare.py /d/out/'"$REF"'/$ds /d/out/'"$TAG"'/$ds | grep -E "rows|MATCH|DIFFER"; done'
fi
