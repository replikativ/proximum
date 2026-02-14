# Proximum Benchmark Suite

Pure Clojure benchmark runner for HNSW implementations with minimal dependencies.

## Quick Start

**Datasets download automatically** - just run the benchmark:

```bash
# Quick test (10k vectors, ~1 minute)
./benchmark/run-bench.sh sift10k --only proximum

# Full test suite on SIFT10k (~5 minutes, all JVM libraries)
./benchmark/run-bench.sh sift10k

# Multiple runs for variance measurement
./benchmark/run-bench.sh sift10k --runs 3
```

**Note:** No Python needed for Proximum-only benchmarks. Python required for dataset download and optional hnswlib baseline.

## Benchmark Datasets

| Dataset | Vectors | Dimensions | Metric | Use Case |
|---------|---------|------------|--------|----------|
| **sift10k** | 10,000 | 128 | Euclidean | Quick validation |
| **sift1m** | 1,000,000 | 128 | Euclidean | Standard benchmark |
| **glove100** | 1,183,514 | 100 | Cosine | Text embeddings |
| **dbpedia-openai-100k** | 100,000 | 1536 | Cosine | LLM embeddings (OpenAI ada-002) |

## Implementations Tested

### Pure JVM (No external dependencies)
- **proximum** - This library
- **jvector** - DataStax HNSW with SIMD
- **lucene-hnsw** - Apache Lucene 10.x
- **hnswlib-java** - Pure Java port by jelmerk
- **datalevin/usearch** - Native via JavaCPP

### Optional Baseline (Requires Python)
- **hnswlib** - Reference C++ implementation (fastest, for comparison)

## Installation

### Minimal Setup (Proximum only)

Just Clojure - no additional dependencies:

```bash
clj -M:benchmark -m runner --dataset sift10k --only proximum
```

### Full Setup (All libraries including hnswlib baseline)

```bash
# Create Python environment for hnswlib
python3 -m venv benchmark-env
source benchmark-env/bin/activate
pip install -r benchmark/requirements.txt

# Download datasets
python3 benchmark/benchmark_datasets.py sift10k
python3 benchmark/benchmark_datasets.py glove100

# Run full benchmark suite
./benchmark/run-bench.sh sift10k
```

## Usage Examples

```bash
# Quick validation - Proximum only on SIFT10k
./benchmark/run-bench.sh sift10k --only proximum

# Specific library
./benchmark/run-bench.sh sift10k --only jvector

# All libraries (includes hnswlib if Python env exists)
./benchmark/run-bench.sh sift10k

# Multiple runs for statistical significance
./benchmark/run-bench.sh sift10k --runs 3

# Large dataset
./benchmark/run-bench.sh sift1m

# High-dimensional LLM embeddings
./benchmark/run-bench.sh dbpedia-openai-100k

# Custom HNSW parameters
./benchmark/run-bench.sh sift10k --M 32 --ef-construction 400 --ef-search 200

# Skip dataset download check
./benchmark/run-bench.sh sift10k --skip-download
```

## Command-Line Options

```
--dataset <name>         Dataset to benchmark (sift10k, sift1m, glove100, etc.)
--only <library>         Run only one library (proximum, jvector, lucene, etc.)
--runs <n>               Number of runs for variance measurement (default: 1)
--M <n>                  Max neighbors per node (default: 16)
--ef-construction <n>    Build beam width (default: 200)
--ef-search <n>          Search beam width (default: 100)
--threads <n>            Parallel insert threads (default: 8)
--skip-download          Skip dataset download check
```

## Output

Results are saved to `benchmark/results/<dataset>.json`:

```json
[
  {
    "library": "proximum",
    "dataset": "sift10k",
    "n_vectors": 10000,
    "dim": 128,
    "insert_throughput": 19025.06,
    "search_qps": 4017.61,
    "search_latency_p50_us": 243.4,
    "search_latency_p99_us": 500.2,
    "recall_at_k": 0.999,
    "heap_mb": 156.3,
    "total_storage_mb": 94.7
  }
]
```

## Architecture

### Pure Clojure Runner (`runner.clj`)

- Downloads datasets (delegates to Python script)
- Orchestrates all benchmarks
- Aggregates results with variance
- Zero Python dependency for JVM-only benchmarks

### Individual Benchmark Scripts

Each implementation has its own benchmark namespace:

- `bench-proximum.clj` - Main benchmarks for Proximum
- `bench-jvector.clj` - DataStax JVector
- `bench-lucene.clj` - Apache Lucene HNSW
- `bench-hnswlib-java.clj` - Pure Java port
- `bench-datalevin.clj` - Datalevin/uSearch

### Python Scripts (Optional)

- `benchmark_datasets.py` - Dataset download utility
- `hnswlib_bench.py` - C++ hnswlib baseline (requires Python bindings)
- `plot_results.py` - Visualization (optional)

## Advanced Benchmarks

### Parameter Sweeps

Test different ef_search values to generate recall-latency curves:

```bash
clj -M:dev -m bench-ef-sweep sift1m --ef-values 50,100,150,200,300,500
```

### Early Termination

Test timeout, distance budget, and patience controls:

```bash
clj -M:dev -m bench-early-termination sift1m --test all
```

## Troubleshooting

**"Dataset not found"**
- Run `python3 benchmark/benchmark_datasets.py <dataset>`
- Or use `--skip-download` if you know it exists

**"hnswlib benchmark failed"**
- Python environment not set up (this is optional)
- Skip with `--only proximum` to test just Proximum

**"OutOfMemoryError"**
- Increase heap: add to runner.clj or use larger machine
- Default: 4g for 10k/100k datasets, 8g for 1M datasets

**"Tests fail after async changes"**
- All storage operations (sync!, flush!, close!) are now async
- Must await with `(a/<!! (pv/close! idx))`
- Benchmarks have been updated to handle this

## Development

### Adding a New Benchmark

1. Create `benchmark/bench-mylib.clj`
2. Follow the template in `bench-proximum.clj`
3. Output JSON to stdout (last line)
4. Add runner function to `runner.clj`

### Re-running After Code Changes

```bash
# Clean rebuild
rm -rf benchmark/results/*

# Run fresh benchmarks
./benchmark/run-bench.sh sift10k --runs 3
```

## See Also

- [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) - Latest published results
- [../README.md](../README.md) - Main project documentation
- [../CLAUDE.md](../CLAUDE.md) - Development guide
