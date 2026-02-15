#!/usr/bin/env python3
"""
Unified benchmark runner for HNSW implementations.

Runs benchmarks for:
- proximum (Clojure/Java)
- datalevin/usearch (native via javacpp)
- hnswlib (reference C++ via Python)

Usage:
    python benchmark/run_benchmarks.py [--dataset sift10k|sift1m] [--skip-download]

Results are saved to benchmark/results/<dataset>.json
"""

import argparse
import json
import subprocess
import sys
import statistics
from pathlib import Path

# Paths
BENCHMARK_DIR = Path(__file__).parent
PROJECT_DIR = BENCHMARK_DIR.parent
RESULTS_DIR = BENCHMARK_DIR / "results"


def run_command(cmd, description, cwd=None):
    """Run a command and return stdout, or None on error."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('='*60)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=cwd or PROJECT_DIR,
            timeout=3600  # 1 hour timeout for large datasets
        )

        if result.returncode != 0:
            print(f"STDERR:\n{result.stderr}", file=sys.stderr)
            print(f"FAILED with exit code {result.returncode}", file=sys.stderr)
            return None

        # Parse JSON from stdout (last line should be JSON)
        stdout_lines = result.stdout.strip().split('\n')
        json_line = stdout_lines[-1] if stdout_lines else ""

        try:
            return json.loads(json_line)
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON output: {e}", file=sys.stderr)
            print(f"Output was:\n{result.stdout}", file=sys.stderr)
            return None

    except subprocess.TimeoutExpired:
        print("TIMEOUT", file=sys.stderr)
        return None
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return None


def ensure_dataset(dataset):
    """Download and prepare dataset if needed."""
    data_dir = BENCHMARK_DIR / "data" / dataset
    if not (data_dir / "base.npy").exists():
        print(f"\nDataset {dataset} not found, downloading...")
        subprocess.run(
            ["python3", str(BENCHMARK_DIR / "benchmark_datasets.py"), dataset],
            check=True
        )
    else:
        print(f"\nDataset {dataset} already exists at {data_dir}")


def run_proximum(dataset, M, ef_c, ef_s, threads=8, warmup=0):
    """Run proximum benchmark using PersistentEdgeStore."""
    # Use consistent heap size with other Java implementations
    heap = "-Xmx8g" if "1m" in dataset else "-Xmx4g"
    cmd = [
        "clojure", f"-J{heap}",
        "-J--add-modules=jdk.incubator.vector",
        "-J--enable-native-access=ALL-UNNAMED",
        "-M:dev",
        "-m", "bench-proximum",
        dataset, str(M), str(ef_c), str(ef_s), str(threads), str(warmup)
    ]
    # Use cosine distance for GloVe and DBpedia (angular similarity)
    if "glove" in dataset or "dbpedia" in dataset:
        cmd.append("--cosine")
    return run_command(cmd, "proximum benchmark")


def run_datalevin(dataset, M, ef_c, ef_s):
    """Run datalevin/usearch benchmark in separate JVM."""
    # EDN format (not JSON) for clj -Sdeps
    deps = '{:paths ["benchmark"] :deps {datalevin/datalevin {:mvn/version "0.9.27"} org.clojure/data.json {:mvn/version "2.5.1"}}}'
    # Use consistent heap size with other Java implementations
    heap = "-Xmx8g" if "1m" in dataset else "-Xmx4g"

    cmd = [
        "clojure",
        "-Sdeps", deps,
        f"-J{heap}",
        "-J--enable-native-access=ALL-UNNAMED",
        "-M", "-m", "bench-datalevin",
        dataset, str(M), str(ef_c), str(ef_s)
    ]
    return run_command(cmd, "datalevin/usearch benchmark")


def run_hnswlib(dataset, M, ef_c, ef_s):
    """Run hnswlib benchmark via Python (using benchmark-env venv)."""
    # Use the benchmark venv's Python to ensure hnswlib is available
    venv_python = PROJECT_DIR / "benchmark-env" / "bin" / "python"
    if not venv_python.exists():
        venv_python = "python3"  # Fallback to system python
    cmd = [
        str(venv_python),
        str(BENCHMARK_DIR / "hnswlib_bench.py"),
        dataset, str(M), str(ef_c), str(ef_s)
    ]
    return run_command(cmd, "hnswlib benchmark")


def run_jvector(dataset, M, ef_c, ef_s):
    """Run JVector (DataStax) benchmark."""
    heap = "-Xmx8g" if "1m" in dataset else "-Xmx4g"
    cmd = [
        "clojure", f"-J{heap}",
        "-J--add-modules=jdk.incubator.vector",
        "-J--enable-native-access=ALL-UNNAMED",
        "-M:benchmark",
        "-m", "bench-jvector",
        dataset, str(M), str(ef_c), str(ef_s)
    ]
    return run_command(cmd, "JVector benchmark")


def run_lucene(dataset, M, ef_c, ef_s):
    """Run Lucene HNSW benchmark."""
    heap = "-Xmx8g" if "1m" in dataset else "-Xmx4g"
    cmd = [
        "clojure", f"-J{heap}", "-M:benchmark",
        "-m", "bench-lucene",
        dataset, str(M), str(ef_c), str(ef_s)
    ]
    return run_command(cmd, "Lucene HNSW benchmark")


def run_hnswlib_java(dataset, M, ef_c, ef_s):
    """Run hnswlib-java (jelmerk) benchmark."""
    heap = "-Xmx8g" if "1m" in dataset else "-Xmx4g"
    cmd = [
        "clojure", f"-J{heap}", "-M:benchmark",
        "-m", "bench-hnswlib-java",
        dataset, str(M), str(ef_c), str(ef_s)
    ]
    return run_command(cmd, "hnswlib-java benchmark")


def aggregate_runs(results_list):
    """Aggregate multiple benchmark runs, computing mean and stddev."""
    if not results_list:
        return None
    if len(results_list) == 1:
        return results_list[0]

    # Use first result as template
    aggregated = dict(results_list[0])

    # Metrics to aggregate
    metrics = ['insert_throughput', 'search_qps', 'search_latency_mean_us',
               'search_latency_p50_us', 'search_latency_p99_us', 'recall_at_k']

    for metric in metrics:
        values = [r[metric] for r in results_list if metric in r]
        if values:
            mean = statistics.mean(values)
            stddev = statistics.stdev(values) if len(values) > 1 else 0
            aggregated[metric] = mean
            aggregated[f"{metric}_stddev"] = stddev
            aggregated[f"{metric}_runs"] = values

    aggregated["n_runs"] = len(results_list)
    return aggregated


def run_with_variance(run_fn, n_runs, *args):
    """Run a benchmark multiple times and aggregate results."""
    results = []
    for run in range(n_runs):
        print(f"\n--- Run {run + 1}/{n_runs} ---")
        result = run_fn(*args)
        if result:
            results.append(result)
    return aggregate_runs(results)


def main():
    parser = argparse.ArgumentParser(description="Run HNSW benchmarks")
    parser.add_argument("--dataset", default="sift10k",
                        choices=["sift10k", "sift1m", "glove100", "glove10k",
                                 "dbpedia-openai-100k", "dbpedia-openai-1m"])
    parser.add_argument("--skip-download", action="store_true", help="Skip dataset download check")
    parser.add_argument("--M", type=int, default=16, help="Max neighbors per node")
    parser.add_argument("--ef-construction", type=int, default=200, help="ef during construction")
    parser.add_argument("--ef-search", type=int, default=100, help="ef during search")
    parser.add_argument("--runs", type=int, default=1, help="Number of runs for variance measurement")
    parser.add_argument("--threads", type=int, default=8, help="Number of threads for parallel insert")
    parser.add_argument("--warmup", type=int, default=1, help="Number of JVM warmup runs before measurement")
    parser.add_argument("--only", choices=["pv", "datalevin", "hnswlib", "jvector", "lucene", "hnswlib-java"], help="Run only one benchmark")
    args = parser.parse_args()

    print("="*60)
    print("HNSW Benchmark Suite")
    print("="*60)
    print(f"Dataset: {args.dataset}")
    print(f"Parameters: M={args.M}, ef_construction={args.ef_construction}, ef_search={args.ef_search}")
    print(f"Runs: {args.runs}, Threads: {args.threads}, Warmup: {args.warmup}")

    # Ensure dataset exists
    if not args.skip_download:
        ensure_dataset(args.dataset)

    # Run benchmarks
    results = []

    if args.only is None or args.only == "pv":
        pv_result = run_with_variance(run_proximum, args.runs,
                                       args.dataset, args.M, args.ef_construction, args.ef_search,
                                       args.threads, args.warmup)
        if pv_result:
            results.append(pv_result)
            stddev_str = f" (±{pv_result.get('search_qps_stddev', 0):.0f})" if args.runs > 1 else ""
            print(f"\nproximum: {pv_result['insert_throughput']:.0f} vec/sec insert, "
                  f"{pv_result['search_qps']:.0f}{stddev_str} QPS, {pv_result['recall_at_k']:.2%} recall")

    if args.only is None or args.only == "datalevin":
        dl_result = run_with_variance(run_datalevin, args.runs,
                                       args.dataset, args.M, args.ef_construction, args.ef_search)
        if dl_result:
            results.append(dl_result)
            stddev_str = f" (±{dl_result.get('search_qps_stddev', 0):.0f})" if args.runs > 1 else ""
            print(f"\ndatalevin/usearch: {dl_result['insert_throughput']:.0f} vec/sec insert, "
                  f"{dl_result['search_qps']:.0f}{stddev_str} QPS, {dl_result['recall_at_k']:.2%} recall")

    if args.only is None or args.only == "hnswlib":
        hn_result = run_with_variance(run_hnswlib, args.runs,
                                       args.dataset, args.M, args.ef_construction, args.ef_search)
        if hn_result:
            results.append(hn_result)
            stddev_str = f" (±{hn_result.get('search_qps_stddev', 0):.0f})" if args.runs > 1 else ""
            print(f"\nhnswlib: {hn_result['insert_throughput']:.0f} vec/sec insert, "
                  f"{hn_result['search_qps']:.0f}{stddev_str} QPS, {hn_result['recall_at_k']:.2%} recall")

    if args.only is None or args.only == "jvector":
        jv_result = run_with_variance(run_jvector, args.runs,
                                       args.dataset, args.M, args.ef_construction, args.ef_search)
        if jv_result:
            results.append(jv_result)
            stddev_str = f" (±{jv_result.get('search_qps_stddev', 0):.0f})" if args.runs > 1 else ""
            print(f"\njvector: {jv_result['insert_throughput']:.0f} vec/sec insert, "
                  f"{jv_result['search_qps']:.0f}{stddev_str} QPS, {jv_result['recall_at_k']:.2%} recall")

    if args.only is None or args.only == "lucene":
        lc_result = run_with_variance(run_lucene, args.runs,
                                       args.dataset, args.M, args.ef_construction, args.ef_search)
        if lc_result:
            results.append(lc_result)
            stddev_str = f" (±{lc_result.get('search_qps_stddev', 0):.0f})" if args.runs > 1 else ""
            print(f"\nlucene-hnsw: {lc_result['insert_throughput']:.0f} vec/sec insert, "
                  f"{lc_result['search_qps']:.0f}{stddev_str} QPS, {lc_result['recall_at_k']:.2%} recall")

    if args.only is None or args.only == "hnswlib-java":
        hj_result = run_with_variance(run_hnswlib_java, args.runs,
                                       args.dataset, args.M, args.ef_construction, args.ef_search)
        if hj_result:
            results.append(hj_result)
            stddev_str = f" (±{hj_result.get('search_qps_stddev', 0):.0f})" if args.runs > 1 else ""
            print(f"\nhnswlib-java: {hj_result['insert_throughput']:.0f} vec/sec insert, "
                  f"{hj_result['search_qps']:.0f}{stddev_str} QPS, {hj_result['recall_at_k']:.2%} recall")

    # Save results
    if results:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        results_file = RESULTS_DIR / f"{args.dataset}.json"
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n{'='*60}")
        print(f"Results saved to {results_file}")

        # Print summary table
        print(f"\n{'='*60}")
        print("SUMMARY")
        print("="*60)
        if args.runs > 1:
            print(f"\n{'Library':<25} {'Insert (vec/s)':<18} {'Search QPS':<18} {'Recall@10':<10}")
            print("-"*71)
            for r in results:
                insert_str = f"{r['insert_throughput']:.0f}±{r.get('insert_throughput_stddev', 0):.0f}"
                qps_str = f"{r['search_qps']:.0f}±{r.get('search_qps_stddev', 0):.0f}"
                print(f"{r['library']:<25} {insert_str:<18} {qps_str:<18} {r['recall_at_k']:<10.2%}")
        else:
            # Check if any result has storage metrics
            has_storage = any('total_storage_mb' in r for r in results)
            if has_storage:
                print(f"\n{'Library':<25} {'Insert (vec/s)':<12} {'QPS':<10} {'p50 (us)':<8} {'Storage (MB)':<12} {'Heap (MB)':<10} {'Recall':<8}")
                print("-"*95)
                for r in results:
                    storage = r.get('total_storage_mb', 0)
                    heap = r.get('heap_mb', 0)
                    storage_str = f"{storage:.1f}" if storage else "-"
                    heap_str = f"{heap:.1f}" if heap else "-"
                    print(f"{r['library']:<25} {r['insert_throughput']:<12.0f} {r['search_qps']:<10.0f} "
                          f"{r['search_latency_p50_us']:<8.1f} {storage_str:<12} {heap_str:<10} {r['recall_at_k']:<8.2%}")
            else:
                print(f"\n{'Library':<25} {'Insert (vec/s)':<15} {'Search QPS':<12} {'p50 (us)':<10} {'p99 (us)':<10} {'Recall@10':<10}")
                print("-"*82)
                for r in results:
                    print(f"{r['library']:<25} {r['insert_throughput']:<15.0f} {r['search_qps']:<12.0f} "
                          f"{r['search_latency_p50_us']:<10.1f} {r['search_latency_p99_us']:<10.1f} {r['recall_at_k']:<10.2%}")


if __name__ == "__main__":
    main()
