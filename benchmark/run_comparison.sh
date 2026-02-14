#!/bin/bash
# Run benchmark comparison between proximum and hnswlib
#
# Usage: ./benchmark/run_comparison.sh [n_vectors]
# Example: ./benchmark/run_comparison.sh 10000

set -e

N=${1:-10000}
DIM=128
M=16
EF_C=200

cd "$(dirname "$0")/.."

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║          HNSW Benchmark: proximum vs hnswlib          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Parameters: n=$N, dim=$DIM, M=$M, ef_construction=$EF_C"
echo ""

# Activate Python venv and run hnswlib benchmark
echo "=== Running hnswlib benchmark ==="
source benchmark-env/bin/activate
python benchmark/hnswlib_bench.py $N $DIM $M $EF_C > /tmp/hnswlib_results.json
echo ""
cat /tmp/hnswlib_results.json
echo ""

# Run proximum benchmark
echo ""
echo "=== Running proximum benchmark ==="
clj -M:dev -m comparison $N > /tmp/pvdb_results.json 2>&1 || {
    cat /tmp/pvdb_results.json
    exit 1
}
echo ""
cat /tmp/pvdb_results.json

# Summary comparison
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                         COMPARISON SUMMARY                        ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Use Python for JSON parsing and summary
python3 << 'EOF'
import json

with open('/tmp/hnswlib_results.json') as f:
    hnswlib = json.load(f)

# Parse proximum results (skip the header lines)
with open('/tmp/pvdb_results.json') as f:
    content = f.read()
    # Find the JSON part
    json_start = content.rfind('{')
    json_end = content.rfind('}') + 1
    pvdb = json.loads(content[json_start:json_end])

print(f"{'Metric':<30} {'hnswlib':>15} {'pvdb':>15} {'ratio':>10}")
print("-" * 72)

# Insert throughput
h_ins = hnswlib['insert_throughput']
p_ins = pvdb['insert_throughput']
print(f"{'Insert (vec/sec)':<30} {h_ins:>15.0f} {p_ins:>15.0f} {h_ins/p_ins:>10.1f}x")

# Search latency
h_lat = hnswlib['search_latency_mean_us']
p_lat = pvdb['search_latency_mean_us']
print(f"{'Search latency mean (µs)':<30} {h_lat:>15.1f} {p_lat:>15.1f} {p_lat/h_lat:>10.1f}x")

# QPS
h_qps = hnswlib['search_qps']
p_qps = pvdb['search_qps']
print(f"{'Search QPS':<30} {h_qps:>15.0f} {p_qps:>15.0f} {h_qps/p_qps:>10.1f}x")

# Recall
h_rec = hnswlib['recall_at_k']
p_rec = pvdb['recall_at_k']
print(f"{'Recall@10':<30} {h_rec:>14.1%} {p_rec:>14.1%} {'':>10}")

print("")
print("Note: hnswlib is C++ optimized with SIMD; pvdb is pure Clojure with CoW semantics")
EOF
