#!/usr/bin/env python3
"""
Generate benchmark comparison plots from JSON results.

Usage:
    python benchmark/plot_results.py [--dataset sift10k|sift1m|all]

Generates PNG plots in benchmark/results/
"""

import argparse
import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = Path(__file__).parent / "results"

# Color scheme
COLORS = {
    "proximum": "#2ecc71",  # Green
    "datalevin/usearch": "#3498db",    # Blue
    "hnswlib": "#e74c3c",              # Red
}

# Short names for plots
SHORT_NAMES = {
    "proximum": "proximum",
    "datalevin/usearch": "datalevin/usearch",
    "hnswlib": "hnswlib (C++)",
}


def load_results(dataset):
    """Load results JSON for a dataset."""
    results_file = RESULTS_DIR / f"{dataset}.json"
    if not results_file.exists():
        print(f"No results found for {dataset}")
        return None
    with open(results_file) as f:
        return json.load(f)


def plot_comparison(results, dataset, output_dir):
    """Generate comparison plots for a dataset."""
    if not results:
        return

    libraries = [r["library"] for r in results]
    colors = [COLORS.get(lib, "#95a5a6") for lib in libraries]
    names = [SHORT_NAMES.get(lib, lib) for lib in libraries]

    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle(f"HNSW Benchmark: {dataset.upper()}\n(M=16, ef_construction=200, ef_search=100)",
                 fontsize=14, fontweight='bold')

    # 1. Insert Throughput
    ax = axes[0, 0]
    values = [r["insert_throughput"] for r in results]
    bars = ax.bar(names, values, color=colors, edgecolor='black', linewidth=0.5)
    ax.set_ylabel("Vectors / second")
    ax.set_title("Insert Throughput")
    ax.set_ylim(0, max(values) * 1.2)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.02,
                f'{val:,.0f}', ha='center', va='bottom', fontsize=9)

    # 2. Search QPS
    ax = axes[0, 1]
    values = [r["search_qps"] for r in results]
    bars = ax.bar(names, values, color=colors, edgecolor='black', linewidth=0.5)
    ax.set_ylabel("Queries / second")
    ax.set_title("Search QPS (k=10)")
    ax.set_ylim(0, max(values) * 1.2)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.02,
                f'{val:,.0f}', ha='center', va='bottom', fontsize=9)

    # 3. Search Latency (p50 and p99)
    ax = axes[1, 0]
    x = np.arange(len(names))
    width = 0.35
    p50_values = [r["search_latency_p50_us"] for r in results]
    p99_values = [r["search_latency_p99_us"] for r in results]
    bars1 = ax.bar(x - width/2, p50_values, width, label='p50', color=colors, edgecolor='black', linewidth=0.5, alpha=0.8)
    bars2 = ax.bar(x + width/2, p99_values, width, label='p99', color=colors, edgecolor='black', linewidth=0.5, alpha=0.5, hatch='//')
    ax.set_ylabel("Latency (microseconds)")
    ax.set_title("Search Latency")
    ax.set_xticks(x)
    ax.set_xticklabels(names)
    ax.legend()
    ax.set_ylim(0, max(p99_values) * 1.3)

    # 4. Recall@10
    ax = axes[1, 1]
    values = [r["recall_at_k"] * 100 for r in results]
    bars = ax.bar(names, values, color=colors, edgecolor='black', linewidth=0.5)
    ax.set_ylabel("Recall (%)")
    ax.set_title("Recall@10")
    ax.set_ylim(95, 100.5)  # Focus on high recall range
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                f'{val:.1f}%', ha='center', va='bottom', fontsize=9)

    plt.tight_layout()

    # Save
    output_file = output_dir / f"{dataset}_comparison.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


def plot_combined(all_results, output_dir):
    """Generate a combined plot showing both datasets."""
    if len(all_results) < 2:
        return

    # Create figure
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle("HNSW Benchmark Comparison\n(M=16, ef_construction=200, ef_search=100)",
                 fontsize=14, fontweight='bold')

    datasets = list(all_results.keys())
    libraries = list(set(r["library"] for results in all_results.values() for r in results))
    libraries.sort()

    x = np.arange(len(datasets))
    width = 0.25
    offsets = np.linspace(-width, width, len(libraries))

    # 1. Insert Throughput
    ax = axes[0]
    for i, lib in enumerate(libraries):
        values = []
        for ds in datasets:
            result = next((r for r in all_results[ds] if r["library"] == lib), None)
            values.append(result["insert_throughput"] if result else 0)
        ax.bar(x + offsets[i], values, width * 0.9, label=SHORT_NAMES.get(lib, lib),
               color=COLORS.get(lib, "#95a5a6"), edgecolor='black', linewidth=0.5)
    ax.set_ylabel("Vectors / second")
    ax.set_title("Insert Throughput")
    ax.set_xticks(x)
    ax.set_xticklabels([d.upper() for d in datasets])
    ax.legend(loc='upper left')

    # 2. Search QPS
    ax = axes[1]
    for i, lib in enumerate(libraries):
        values = []
        for ds in datasets:
            result = next((r for r in all_results[ds] if r["library"] == lib), None)
            values.append(result["search_qps"] if result else 0)
        ax.bar(x + offsets[i], values, width * 0.9, label=SHORT_NAMES.get(lib, lib),
               color=COLORS.get(lib, "#95a5a6"), edgecolor='black', linewidth=0.5)
    ax.set_ylabel("Queries / second")
    ax.set_title("Search QPS")
    ax.set_xticks(x)
    ax.set_xticklabels([d.upper() for d in datasets])

    # 3. Recall
    ax = axes[2]
    for i, lib in enumerate(libraries):
        values = []
        for ds in datasets:
            result = next((r for r in all_results[ds] if r["library"] == lib), None)
            values.append(result["recall_at_k"] * 100 if result else 0)
        ax.bar(x + offsets[i], values, width * 0.9, label=SHORT_NAMES.get(lib, lib),
               color=COLORS.get(lib, "#95a5a6"), edgecolor='black', linewidth=0.5)
    ax.set_ylabel("Recall (%)")
    ax.set_title("Recall@10")
    ax.set_xticks(x)
    ax.set_xticklabels([d.upper() for d in datasets])
    ax.set_ylim(95, 100.5)

    plt.tight_layout()

    output_file = output_dir / "combined_comparison.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


def generate_markdown_table(all_results):
    """Generate markdown table for README."""
    print("\n## Benchmark Results\n")

    for dataset, results in all_results.items():
        print(f"### {dataset.upper()}\n")
        print("| Library | Insert (vec/s) | Search QPS | p50 (μs) | p99 (μs) | Recall@10 |")
        print("|---------|----------------|------------|----------|----------|-----------|")
        for r in sorted(results, key=lambda x: x["library"]):
            print(f"| {r['library']} | {r['insert_throughput']:,.0f} | {r['search_qps']:,.0f} | "
                  f"{r['search_latency_p50_us']:.1f} | {r['search_latency_p99_us']:.1f} | "
                  f"{r['recall_at_k']:.1%} |")
        print()


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark plots")
    parser.add_argument("--dataset", default="all", choices=["sift10k", "sift1m", "all"])
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    all_results = {}

    if args.dataset == "all":
        for ds in ["sift10k", "sift1m"]:
            results = load_results(ds)
            if results:
                all_results[ds] = results
                plot_comparison(results, ds, RESULTS_DIR)
    else:
        results = load_results(args.dataset)
        if results:
            all_results[args.dataset] = results
            plot_comparison(results, args.dataset, RESULTS_DIR)

    if len(all_results) >= 2:
        plot_combined(all_results, RESULTS_DIR)

    if all_results:
        generate_markdown_table(all_results)


if __name__ == "__main__":
    main()
