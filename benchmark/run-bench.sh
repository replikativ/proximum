#!/bin/bash
# Quick benchmark runner wrapper
#
# Usage:
#   ./benchmark/run-bench.sh sift10k              # Quick test (proximum only)
#   ./benchmark/run-bench.sh sift10k --runs 3     # Multiple runs
#   ./benchmark/run-bench.sh sift1m               # Full suite (all libraries)
#   ./benchmark/run-bench.sh glove100 --only proximum  # Specific library

set -e

cd "$(dirname "$0")/.."

echo "Running benchmark with Clojure runner..."
clj -M:benchmark -m runner "$@"
