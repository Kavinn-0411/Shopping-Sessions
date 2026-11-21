#!/bin/sh
# Fast local loop: build in a Rust container (cached), run every fixture and the dev set, then check.
# Usage: scripts/dev.sh [fixtures|dev|all]   (default all)
set -eu
cd "$(dirname "$0")/.."
WHAT="${1:-all}"
ROOT="$(pwd -W 2>/dev/null || pwd)"
rm -rf .out && mkdir -p .out
MSYS_NO_PATHCONV=1 docker run --rm \
  -v cargo-cache:/usr/local/cargo/registry -v sessions-target:/target \
  -v "$ROOT:/w" -w /w -e CARGO_TARGET_DIR=/target -e RUSTFLAGS="-C target-cpu=x86-64-v3" \
  rust:1-slim-bookworm bash -c '
    set -e
    cargo build --release > /tmp/build.log 2>&1 || { grep -E "^error|-->" -A3 /tmp/build.log | head -30; echo BUILD FAILED; exit 1; }; tail -1 /tmp/build.log
    test -x /target/release/sessions
    if [ "'"$WHAT"'" != dev ]; then
      for c in $(ls fixtures); do [ -d fixtures/$c/input ] || continue
        DATA_DIR=fixtures/$c/input OUTPUT_DIR=.out/$c NCPU=8 /target/release/sessions 2>/dev/null || echo "CRASH $c"
      done
    fi
    if [ "'"$WHAT"'" != fixtures ]; then
      mkdir -p /tmp/devdata && cp -r dev/. /tmp/devdata/
      DATA_DIR=/tmp/devdata OUTPUT_DIR=.out/dev NCPU=8 /target/release/sessions
      echo "--- timing (warm):"; time sh -c "DATA_DIR=/tmp/devdata OUTPUT_DIR=/tmp/o NCPU=8 /target/release/sessions 2>/dev/null"
    fi'
if [ "$WHAT" != dev ]; then python scripts/check_outputs.py fixtures .out; fi
if [ "$WHAT" != fixtures ]; then python scripts/compare.py .out/dev dev-answer; fi
