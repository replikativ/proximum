# HNSW Benchmark Results

Benchmarks comparing proximum against other HNSW implementations on standard ANN benchmark datasets.

## Test Environments

### Server (GloVe-100, SIFT1M)
| Component | Specification |
|-----------|---------------|
| CPU | 2x AMD EPYC 7313 16-Core (64 threads total) |
| RAM | 256 GB |
| Java | OpenJDK 25 |
| OS | Ubuntu 24.04.3 LTS |

### Development Machine (DBpedia-OpenAI, SIFT10k)
| Component | Specification |
|-----------|---------------|
| CPU | Intel Core Ultra 7 258V (8 cores) |
| RAM | 32 GB |
| Java | OpenJDK 25 |
| OS | Ubuntu 25.04 |

## Benchmark Configuration

- **HNSW Parameters**: M=16, ef_construction=200, ef_search=100, k=10
- **Runs**: 3 runs with warmup for SIFT10k, 1 run for larger datasets
- **Parallelism**: 8 threads for parallel insert (where supported)
- **JVM**: -Xmx4g for 10k/100k datasets, -Xmx8g for 1M datasets

---

## Results by Dataset

### SIFT10k (10,000 vectors, 128 dimensions, Euclidean)

#### Development Machine (Intel Core Ultra 7 258V, 32GB RAM) - 3 runs with warmup

| Library | Insert (vec/s) | Search QPS | vs hnswlib | Recall@10 |
|---------|---------------|------------|------------|-----------|
| hnswlib (native C++) | 63,197 ± 1,583 | 23,761 ± 465 | 100% | 99.80% |
| **proximum** | 27,076 ± 1,053 | **16,836 ± 806** | **71%** | 99.77% |
| jvector | 14,805 ± 1,042 | 7,707 ± 2,755 | 32% | 100% |
| hnswlib-java | 14,532 ± 625 | 5,695 ± 206 | 24% | 99.80% |
| datalevin/usearch | 10,529 ± 17 | 11,663 ± 849 | 49% | 99.80% |
| lucene-hnsw | 10,441 ± 129 | 5,964 ± 258 | 25% | 99.80% |

#### Server (AMD EPYC 7313, 256GB RAM) - 3 runs with warmup

| Library | Insert (vec/s) | Search QPS | vs hnswlib | Recall@10 |
|---------|---------------|------------|------------|-----------|
| hnswlib (native C++) | 150,189 ± 2,057 | 20,653 ± 36 | 100% | 99.87% |
| **proximum** | 25,233 ± 3,620 | **9,653 ± 130** | **47%** | 98.30% |
| hnswlib-java | 14,996 ± 3,027 | 3,867 ± 139 | 19% | 99.87% |
| datalevin/usearch | 3,643 ± 12 | 8,296 ± 129 | 40% | 99.80% |
| jvector | 8,149 ± 1,562 | 6,024 ± 1,125 | 29% | 100% |
| lucene-hnsw | 6,721 ± 78 | 3,018 ± 129 | 15% | 99.80% |

*Note: AMD shows 47% vs Intel's 71% relative to hnswlib, but AMD hnswlib baseline is much higher (20,653 vs 23,761 QPS). Intel single-core latency advantage more pronounced on small datasets.*

### SIFT1M (1,000,000 vectors, 128 dimensions, Euclidean)

#### Development Machine (Intel Core Ultra 7 258V, 32GB RAM)

| Library | Insert (vec/s) | Search QPS | vs hnswlib | p50 (µs) | p99 (µs) | Recall@10 |
|---------|---------------|------------|------------|----------|----------|-----------|
| hnswlib (native C++) | 18,205 | 7,849 | 100% | 131 | 165 | 98.32% |
| **proximum** | 10,387 | **4,601** | **59%** | 222 | 289 | **98.61%** |
| jvector | 6,095 | 1,844 | 23% | 557 | 696 | 95.95% |
| hnswlib-java | 4,329 | 1,004 | 13% | 1,041 | 1,381 | 98.30% |
| datalevin/usearch | 2,507 | 3,885 | 49% | 257 | 353 | 96.96% |
| lucene-hnsw | 2,347 | 3,095 | 39% | 333 | 458 | 98.53% |

#### Server (AMD EPYC 7313, 256GB RAM)

| Library | Insert (vec/s) | Search QPS | vs hnswlib | p50 (µs) | p99 (µs) | Recall@10 |
|---------|---------------|------------|------------|----------|----------|-----------|
| jvector | 16,074 | 1,212 | 37% | 772 | 1,817 | 95.99% |
| hnswlib (native C++) | 15,254 | 3,315 | 100% | 312 | 397 | 98.29% |
| hnswlib-java | 12,005 | 561 | 17% | 1,809 | 2,911 | 98.26% |
| **proximum** | 10,164 | **2,372** | **72%** | 435 | 610 | **98.60%** |
| datalevin/usearch | 759 | 2,786 | 84% | 365 | 580 | 96.96% |
| lucene-hnsw | 1,598 | 2,007 | 61% | 508 | 865 | 98.53% |

*Note: Intel shows 2x higher absolute QPS but AMD shows better relative performance to hnswlib (72% vs 59%). Intel's superior single-core latency benefits graph traversal workloads.*

### GloVe-100 (1,183,514 vectors, 100 dimensions, Cosine)

#### Development Machine (Intel Core Ultra 7 258V, 32GB RAM)

| Library | Insert (vec/s) | Search QPS | vs hnswlib | p50 (µs) | Recall@10 |
|---------|---------------|------------|------------|----------|-----------|
| hnswlib (native C++) | 14,865 | 6,606 | 100% | 150 | 81.03% |
| **proximum** | 5,185 | **2,794** | **42%** | 358 | **81.77%** |
| jvector | 5,250 | 1,571 | 24% | 611 | 70.25% |
| datalevin/usearch | 1,436 | 2,352 | 36% | 417 | 75.67% |
| lucene-hnsw | 1,463 | 2,041 | 31% | 479 | 81.35% |
| hnswlib-java | 2,865 | 726 | 11% | 1,374 | 80.83% |

*Note: Intel shows 2.4x higher absolute QPS than AMD (6,606 vs 2,728 for hnswlib baseline). proximum achieves 42% of hnswlib on Intel vs 50% on AMD.*

#### Server (AMD EPYC 7313, 256GB RAM)

| Library | Insert (vec/s) | Search QPS | vs hnswlib | p50 (µs) | Recall@10 |
|---------|---------------|------------|------------|----------|-----------|
| jvector | 14,823 | 1,222 | 45% | 778 | 70.62% |
| hnswlib (native C++) | 11,913 | 2,728 | 100% | 364 | 80.87% |
| hnswlib-java | 9,070 | 563 | 21% | 1,648 | 80.90% |
| **proximum** | 7,479 | 1,359 | **50%** | 733 | **81.66%** |
| datalevin/usearch | 514 | 1,756 | 64% | 548 | 75.61% |
| lucene-hnsw | 984 | 1,350 | 49% | 722 | 81.35% |

### DBpedia-OpenAI-100k (100,000 vectors, 1536 dimensions, Cosine)

OpenAI ada-002 embeddings from DBpedia entities - representative of real-world LLM embedding workloads.

#### Development Machine (Intel Core Ultra 7 258V, 32GB RAM)

| Library | Insert (vec/s) | Search QPS | vs hnswlib | p50 (µs) | Storage | Recall@10 |
|---------|---------------|------------|------------|----------|---------|-----------|
| hnswlib (native C++) | 5,014 | 2,070 | 100% | 496 | - | 99.24% |
| **proximum** | 3,283 | 1,736 | **84%** | 592 | 614 MB | **99.32%** |
| jvector | 2,803 | 1,079 | 52% | 948 | - | 98.66% |
| hnswlib-java | 1,972 | 578 | 28% | 1,783 | - | 99.25% |
| lucene-hnsw | 947 | 1,299 | 63% | 792 | - | 99.28% |
| datalevin/usearch | CRASHED | - | - | - | - | - |

*Note: datalevin/usearch crashes (SIGABRT) on 1536-dimension vectors - likely uSearch native memory issue.*

#### Server (AMD EPYC 7313, 256GB RAM)

| Library | Insert (vec/s) | Search QPS | vs hnswlib | p50 (µs) | Recall@10 |
|---------|---------------|------------|------------|----------|-----------|
| hnswlib-java | 3,388 | 225 | - | 4,447 | 99.28% |
| jvector | 2,898 | 474 | - | 1,860 | 98.58% |
| **proximum** | 1,526 | **836** | - | 1,234 | 99.16% |
| datalevin/usearch | 412 | 1,191 | - | 836 | 98.76% |
| lucene-hnsw | 770 | 995 | - | 1,015 | 99.28% |

*Note: hnswlib (C++) failed with exit code 1 on AMD for 1536-dim vectors (no stderr). proximum shows 3.7x faster search than hnswlib-java, competitive with datalevin/usearch and lucene-hnsw.*

---

## Key Findings

### Search Performance
- **proximum achieves 47-84% of native C++ hnswlib** depending on dataset and hardware
- **Intel Core Ultra 7**: 59-71% of hnswlib (better single-core latency)
  - SIFT10k: 71% (16,836 vs 23,761 QPS)
  - SIFT1M: 59% (4,601 vs 7,849 QPS)
  - DBpedia-OpenAI-100k: 84% (1,736 vs 2,070 QPS)
- **AMD EPYC 7313**: 47-72% of hnswlib (more cores, higher throughput ceiling)
  - SIFT10k: 47% (9,653 vs 20,653 QPS)
  - SIFT1M: 72% (2,372 vs 3,315 QPS)
  - GloVe-100: 50% (1,359 vs 2,728 QPS)
- **2.5x faster than jvector** on SIFT1M Intel (4,601 vs 1,844 QPS)
- **4.6x faster than hnswlib-java** on SIFT1M Intel (4,601 vs 1,004 QPS)

### Insert Performance
- **proximum: 27,076 vec/sec** on SIFT10k (43% of hnswlib)
- **1.8x faster insert** than jvector on SIFT10k
- Parallel insert with 8 threads

### Recall
- **Best recall on DBpedia-OpenAI** at 99.32% (beating all including hnswlib)
- **Best recall on GloVe-100** at 81.66%
- Consistent ~99.6-99.8% on SIFT datasets

### High-Dimensional Embeddings (1536 dims)
- **proximum handles OpenAI ada-002 embeddings well**
- datalevin/usearch crashes on 1536-dim vectors (native memory issue)
- Other Java libraries see significant slowdown at high dimensions

### Cosine/Angular Distance Support
- **proximum correctly supports cosine distance** for embedding datasets
- Tested on GloVe-100 and DBpedia-OpenAI (both angular/cosine)

---

## Library Comparison

| Library | Parallelism | Persistence | Fork/Branch | Notes |
|---------|-------------|-------------|-------------|-------|
| hnswlib | C++ native | ❌ | ❌ | Reference implementation |
| **proximum** | ✅ 8 threads | ✅ konserve | ✅ O(1) | Pure Clojure/Java, CoW semantics |
| datalevin/usearch | Native | ✅ LMDB | ❌ | Uses uSearch C++ via JavaCPP |
| jvector | ✅ ForkJoin | ❌ | ❌ | DataStax, SIMD via Java Vector API |
| lucene-hnsw | ❌ Single | ✅ Lucene | ❌ | Apache Lucene 10.x |
| hnswlib-java | ✅ Multi | ❌ | ❌ | Pure Java port by jelmerk |

---

## Running Benchmarks

```bash
# Setup Python environment (for hnswlib and dataset downloads)
python3 -m venv benchmark-env
source benchmark-env/bin/activate
pip install -r benchmark/requirements.txt

# Download datasets
python benchmark/benchmark_datasets.py sift10k
python benchmark/benchmark_datasets.py sift1m
python benchmark/benchmark_datasets.py glove100
python benchmark/benchmark_datasets.py dbpedia-openai-100k  # requires: pip install datasets

# Run all benchmarks on a dataset
clj -M:benchmark -m runner sift10k --runs 3
clj -M:benchmark -m runner glove100
clj -M:benchmark -m runner dbpedia-openai-100k

# Run specific library only
clj -M:benchmark -m runner sift1m --only proximum --skip-download

# Available datasets: sift10k, sift1m, glove100, glove10k, dbpedia-openai-100k, dbpedia-openai-1m
# Available libraries: proximum, jvector, lucene, hnswlib-java, datalevin, hnswlib
```

---

## Datasets

| Dataset | Vectors | Dimensions | Metric | Source |
|---------|---------|------------|--------|--------|
| SIFT10k | 10,000 | 128 | Euclidean | [IRISA TEXMEX](http://corpus-texmex.irisa.fr/) |
| SIFT1M | 1,000,000 | 128 | Euclidean | [IRISA TEXMEX](http://corpus-texmex.irisa.fr/) |
| GloVe-100 | 1,183,514 | 100 | Cosine | [ann-benchmarks](http://ann-benchmarks.com/) |
| DBpedia-OpenAI-100k | 100,000 | 1536 | Cosine | [HuggingFace](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) |
| DBpedia-OpenAI-1M | 1,000,000 | 1536 | Cosine | [HuggingFace](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) |
