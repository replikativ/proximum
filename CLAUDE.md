# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A high-performance persistent vector database with HNSW indexing, written in Clojure/Java. Features git-like versioning (snapshots, branches, time-travel queries) via immutable data structures.

**Key differentiators:**
- Pure JVM (no native dependencies)
- Version control for vector indices
- 79% of native C++ hnswlib performance
- Spring AI and LangChain4j integrations

## Common Commands

```bash
# Run all tests
clj -M:test

# Build JAR (compiles Java first)
clj -T:build jar

# Install locally
clj -T:build install

# Run benchmarks
cd benchmark && python3 run_benchmarks.py --dataset sift10k --only pv

# Start a REPL
clj -M:dev
```

## Project Structure

```
src/proximum/           # Clojure API
  core.clj                         # Main API (create-index, insert, search, etc.)
  metrics.clj                      # Index health metrics
  storage.clj                      # Konserve persistence
  vectors.clj                      # Vector storage abstraction

src-java/proximum/
  internal/                        # High-performance Java internals
    HnswInsertPES.java            # Parallel insert with SIMD
    HnswSearchPES.java            # Search with SIMD distance
    PersistentEdgeStore.java      # Chunked edge storage

src-java/io/replikativ/proximum/  # Java public API
  PersistentVectorIndex.java      # Builder-pattern Java API
  SearchResult.java               # Search result type

src-java-optional/                 # Optional integrations
  .../langchain4j/                # LangChain4j EmbeddingStore
  .../spring/                     # Spring AI VectorStore

src-commercial/                    # Commercial extensions (separate license)
  .../monitoring.clj              # Prometheus metrics export

benchmark/                         # Benchmarking suite
  run_benchmarks.py               # Main benchmark runner
  bench_*.clj                     # Per-library benchmarks
  BENCHMARK_RESULTS.md            # Latest results

test/proximum/          # Clojure tests
test/java/                         # Java API tests
```

## Architecture

**Data Flow:**
1. Vectors stored in memory-mapped file (MemorySegment)
2. HNSW graph edges in PersistentEdgeStore (chunked arrays with SoftReference caching)
3. Persistence via Konserve (async writes, structural sharing)

**Key Classes:**
- `HnswInsertPES`: Parallel insert using ForkJoinPool, concurrent candidate tracking
- `HnswSearchPES`: SIMD-accelerated search via Java Vector API
- `PersistentEdgeStore`: Edge storage with striped locks, dirty chunk tracking

## REPL Development

```bash
# Discover running nREPL servers
clj-nrepl-eval --discover-ports

# Evaluate code (session persists)
clj-nrepl-eval -p <port> "<clojure-code>"
```

Always use `:reload` when requiring namespaces:
```clojure
(require '[proximum.core :as core] :reload)
```

## Performance Notes

- SIMD via `jdk.incubator.vector` (requires `--add-modules`)
- ThreadLocal arrays avoid allocation in hot paths
- Striped locks (1024) reduce contention vs per-node locks
- Concurrent candidate tracking improves parallel insert graph quality

## Investigating Flaky Tests

When tests fail intermittently, especially fork/branching tests:

1. **Check structural sharing isolation**: Fork operations should create independent copies. If `test-fork-performance` fails with equal edge counts, it may indicate shared mutable state between original and forked index.

2. **Common culprits**:
   - `PersistentEdgeStore` sharing chunks between original and fork
   - Mmap file not properly copied (reflink vs full copy)
   - Race conditions in concurrent tests

3. **Debugging steps**:
   ```clojure
   ;; Check edge counts before/after operations
   (.countEdges (:pes-edges idx))

   ;; Verify independence
   (identical? (:pes-edges original) (:pes-edges forked))  ;; should be false
   ```

4. **Run specific test repeatedly**:
   ```bash
   for i in {1..10}; do clj -M:test -n proximum.core-test/test-fork-performance; done
   ```
