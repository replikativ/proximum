#!/usr/bin/env python3
"""
Benchmark hnswlib (reference C++ implementation) on SIFT/GloVe datasets.

Usage:
    python hnswlib_bench.py [dataset] [M] [ef_construction] [ef_search]

Example:
    python hnswlib_bench.py sift10k 16 200 100
    python hnswlib_bench.py sift1m 16 200 100
    python hnswlib_bench.py glove100 16 200 100
"""

import sys
import time
import json
import numpy as np
import hnswlib
from pathlib import Path

# Import dataset loader
sys.path.insert(0, str(Path(__file__).parent))
from benchmark_datasets import load_dataset, DATASETS, HDF5_DATASETS, HUGGINGFACE_DATASETS


def benchmark(dataset_name="sift10k", M=16, ef_construction=200, ef_search=100, k=10):
    """Run hnswlib benchmark on SIFT/GloVe datasets and return results."""

    # Load dataset with ground truth
    base, queries, groundtruth = load_dataset(dataset_name)
    n_vectors, dim = base.shape
    n_queries = len(queries)

    # Determine distance metric based on dataset
    # GloVe and DBpedia use angular/cosine similarity
    if dataset_name in HDF5_DATASETS and HDF5_DATASETS[dataset_name].get("metric") == "angular":
        space = "cosine"
    elif dataset_name in HUGGINGFACE_DATASETS and HUGGINGFACE_DATASETS[dataset_name].get("metric") == "angular":
        space = "cosine"
    else:
        space = "l2"

    results = {
        "library": "hnswlib",
        "dataset": dataset_name,
        "n_vectors": n_vectors,
        "dim": dim,
        "M": M,
        "ef_construction": ef_construction,
        "ef_search": ef_search,
        "k": k,
        "n_queries": n_queries,
        "space": space,
    }

    # Create index
    print(f"Creating index (M={M}, ef_construction={ef_construction}, space={space})...", file=sys.stderr)
    index = hnswlib.Index(space=space, dim=dim)
    index.init_index(max_elements=n_vectors, ef_construction=ef_construction, M=M)

    # Benchmark insertion
    print("Benchmarking insertion...", file=sys.stderr)
    start = time.perf_counter()
    index.add_items(base, np.arange(n_vectors))
    insert_time = time.perf_counter() - start

    results["insert_time_sec"] = insert_time
    results["insert_throughput"] = n_vectors / insert_time
    print(f"  Insert: {insert_time:.3f}s ({results['insert_throughput']:.0f} vec/sec)", file=sys.stderr)

    # Set ef for search
    index.set_ef(ef_search)

    # Warmup
    for _ in range(min(10, n_queries)):
        index.knn_query(queries[0:1], k=k)

    # Benchmark search
    print(f"Benchmarking search (ef={ef_search}, k={k}, {n_queries} queries)...", file=sys.stderr)
    latencies = []
    all_results = []
    for q in queries:
        start = time.perf_counter()
        labels, distances = index.knn_query(q.reshape(1, -1), k=k)
        latencies.append((time.perf_counter() - start) * 1e6)  # microseconds
        all_results.append(labels[0])

    results["search_latency_mean_us"] = float(np.mean(latencies))
    results["search_latency_p50_us"] = float(np.percentile(latencies, 50))
    results["search_latency_p99_us"] = float(np.percentile(latencies, 99))
    results["search_qps"] = 1e6 / results["search_latency_mean_us"]

    print(f"  Search: mean={results['search_latency_mean_us']:.1f}µs, "
          f"p50={results['search_latency_p50_us']:.1f}µs, "
          f"p99={results['search_latency_p99_us']:.1f}µs", file=sys.stderr)
    print(f"  QPS: {results['search_qps']:.0f}", file=sys.stderr)

    # Compute recall against ground truth
    print("Computing recall against ground truth...", file=sys.stderr)
    recalls = []
    for i in range(n_queries):
        hnsw_result = set(all_results[i][:k])
        gt_result = set(groundtruth[i][:k])
        recalls.append(len(hnsw_result & gt_result) / k)

    results["recall_at_k"] = float(np.mean(recalls))
    print(f"  Recall@{k}: {results['recall_at_k']:.2%}", file=sys.stderr)

    # Export for Clojure comparison
    out_dir = Path(__file__).parent / "data" / dataset_name
    out_dir.mkdir(parents=True, exist_ok=True)
    np.save(out_dir / "base.npy", base)
    np.save(out_dir / "queries.npy", queries)
    np.save(out_dir / "groundtruth.npy", groundtruth)
    print(f"  Data exported to {out_dir}/", file=sys.stderr)

    return results


if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else "sift10k"
    M = int(sys.argv[2]) if len(sys.argv) > 2 else 16
    ef_c = int(sys.argv[3]) if len(sys.argv) > 3 else 200
    ef_s = int(sys.argv[4]) if len(sys.argv) > 4 else 100

    all_datasets = set(DATASETS.keys()) | set(HDF5_DATASETS.keys()) | set(HUGGINGFACE_DATASETS.keys())
    if dataset not in all_datasets:
        print(f"Unknown dataset: {dataset}", file=sys.stderr)
        print(f"Available: {', '.join(sorted(all_datasets))}", file=sys.stderr)
        sys.exit(1)

    results = benchmark(dataset_name=dataset, M=M, ef_construction=ef_c, ef_search=ef_s)
    # Output single-line JSON for easy parsing by run_benchmarks.py
    print(json.dumps(results))
