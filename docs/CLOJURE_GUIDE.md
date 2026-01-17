# Proximum Clojure Guide

Complete guide to using Proximum from Clojure.

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Collection Protocols](#collection-protocols)
5. [Basic Operations](#basic-operations)
6. [Search & Latency Controls](#search--latency-controls)
7. [Versioning & Branching](#versioning--branching)
8. [Metadata](#metadata)
9. [Advanced Features](#advanced-features)
10. [Performance Tips](#performance-tips)
11. [Troubleshooting](#troubleshooting)

---

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/org.replikativ/proximum.svg)](https://clojars.org/org.replikativ/proximum)

### deps.edn

```clojure
{:deps {org.replikativ/proximum {:mvn/version "LATEST"}}}
```

### Leiningen

```clojure
:dependencies [[org.replikativ/proximum "LATEST"]]
```

### Requirements

- **Java 21+** (for Vector API and Foreign Memory)
- **JVM Options**: Add these to your `:jvm-opts`:
  ```clojure
  :jvm-opts ["--add-modules=jdk.incubator.vector"
             "--enable-native-access=ALL-UNNAMED"]
  ```

---

## Quick Start

```clojure
(require '[proximum.core :as prox])

;; Create an index
(def idx (prox/create-index {:type :hnsw
                              :dim 384
                              :store-config {:backend :memory
                                            :id (random-uuid)}
                              :capacity 10000}))

;; Add vectors (feels like Clojure!)
(def idx2 (assoc idx "doc-1" (float-array (repeatedly 384 rand))))

;; Or use explicit API
(def idx3 (prox/insert idx2 (float-array (repeatedly 384 rand)) "doc-2"))

;; Search for nearest neighbors
(def results (prox/search idx3 (float-array (repeatedly 384 rand)) 5))
; => ({:id "doc-1", :distance 0.234...} {:id "doc-2", :distance 0.456...} ...)

;; Results have :id (external ID) and :distance
(map :id results)  ; => ("doc-1" "doc-2" ...)
```

That's it! Proximum feels like working with native Clojure data structures.

---

## Core Concepts

### Persistent Data Structures for Indices

Proximum brings Clojure's **persistent data structure philosophy** to vector search:

```clojure
(def idx1 (assoc idx "a" vec1))
(def idx2 (assoc idx1 "b" vec2))

;; idx and idx1 are still valid and queryable!
(count idx)   ; => 0
(count idx1)  ; => 1
(count idx2)  ; => 2
```

**Key properties:**

- ✅ **Immutable**: Operations return new versions, old versions unchanged
- ✅ **Structural Sharing**: Shared data between versions (copy-on-write)
- ✅ **Snapshot Semantics**: Each version is independently queryable
- ✅ **Safe Concurrency**: No locks needed for readers

### Git-Like Versioning

Proximum provides git-like version control for vector indices:

```clojure
;; Create a commit (like git commit)
(prox/sync! idx2)

;; Create a branch (like git branch)
(def experiment (prox/branch! idx2 :experiment))

;; Work on branch
(def experiment2 (assoc experiment "exp-doc" vec3))

;; Main branch is unchanged
(count idx2)        ; => 2
(count experiment2) ; => 3
```

**Use cases:**
- **A/B Testing**: Test new embedding models on a branch
- **Experiments**: Try different chunking strategies without affecting production
- **Staging**: Validate changes before promoting to main

### External vs Internal IDs

Proximum uses **two types of IDs**:

- **External IDs**: User-facing IDs (String, UUID, Long, keyword, etc.)
- **Internal IDs**: Implementation detail (integer node IDs in HNSW graph)

**You work with external IDs:**

```clojure
(assoc idx "my-doc-id" vector)        ; External ID: "my-doc-id"
(assoc idx 12345 vector)              ; External ID: 12345 (Datahike entity)
(assoc idx :doc/abc vector)           ; External ID: :doc/abc (keyword)

(prox/search idx query 10)
; => ({:id "my-doc-id", :distance ...} ...)  ; Returns external IDs
```

### When to Sync

Understanding when to persist is critical:

```clojure
;; In-memory operations (fast)
(def idx2 (assoc idx "a" vec1))      ; Instant
(def idx3 (assoc idx2 "b" vec2))     ; Instant
(prox/search idx3 query 10)          ; Instant

;; Persist to disk (slower)
(prox/sync! idx3)                    ; Writes to storage

;; After JVM restart, data is gone unless you sync!
```

**Rules:**
- **Development**: Sync rarely, use in-memory indices
- **Production**: Sync after batches, on shutdown, periodically
- **Experiments**: Use `fork` instead of `sync!` for O(1) copies

---

## Collection Protocols

Proximum implements standard Clojure protocols, making it feel like working with native data structures.

### IPersistentMap

```clojure
;; assoc - add/update vector
(def idx2 (assoc idx "doc-1" vector))
(def idx3 (assoc idx2 "doc-2" vector))

;; dissoc - delete vector (soft delete)
(def idx4 (dissoc idx3 "doc-1"))

;; count - number of live vectors (excluding deleted)
(count idx4)  ; => 1
```

### ILookup

```clojure
;; get - retrieve vector by ID
(get idx "doc-1")  ; => float array
(get idx "missing" :default)  ; => :default

;; contains? - check if ID exists
(contains? idx "doc-1")  ; => true

;; Index as function
(idx "doc-1")  ; => float array
```

### Seqable

```clojure
;; seq - iterate over [id vector] pairs
(doseq [[id vec] idx]
  (println id))
```

### ITransientMap

For building large indices efficiently:

```clojure
;; Explicit transient usage
(def idx2
  (persistent!
    (reduce (fn [t-idx [id vec]]
              (assoc! t-idx id vec))
            (transient idx)
            large-dataset)))

;; Or use 'into' (uses transient automatically)
(def idx3 (into idx large-dataset))  ; Fast!
```

**When to use transients:**
- Building an index from scratch with >1000 vectors
- Batch loading from a database
- Initial ingestion of large datasets

**Performance:**
- `into` uses transient internally (fast for batches)
- Manual transient gives you full control
- Persistent operations (`assoc`, `dissoc`) are fast for < 100 operations

---

## Basic Operations

### Creating an Index

**In-Memory Index** (for experiments):

```clojure
(def idx (prox/create-index {:type :hnsw
                              :dim 384
                              :M 16
                              :ef-construction 200
                              :store-config {:backend :memory
                                            :id (random-uuid)}
                              :capacity 10000}))
```

**Persistent Index** (for production):

```clojure
(def idx (prox/create-index {:type :hnsw
                              :dim 1536
                              :M 16
                              :ef-construction 200
                              :store-config {:backend :file
                                            :path "/var/data/vectors"
                                            :id (random-uuid)}
                              :mmap-dir "/var/data/mmap"
                              :capacity 1000000}))
```

**Configuration Options:**

| Option | Description | Default | Recommendation |
|--------|-------------|---------|----------------|
| `:dim` | Vector dimensions | Required | Match your embeddings |
| `:M` | Max neighbors per node | 16 | 16-32 for most use cases |
| `:ef-construction` | Build quality | 200 | 100-400 (higher = better recall) |
| `:ef-search` | Search quality | 50 | 50-200 (override per-query) |
| `:capacity` | Max vectors | 10M | Set to expected size |
| `:distance` | Metric | `:euclidean` | `:cosine` for normalized vectors |

### Inserting Vectors

**Single Insert (Explicit API):**

```clojure
(def idx2 (prox/insert idx vector "doc-1"))

;; With metadata
(def idx3 (prox/insert idx2 vector "doc-2" {:title "My Doc" :category :science}))
```

**Single Insert (Collection Protocol):**

```clojure
;; Just the vector
(def idx2 (assoc idx "doc-1" (float-array [...])))

;; With metadata
(def idx3 (assoc idx2 "doc-2" {:vector (float-array [...])
                                :metadata {:title "My Doc"}}))
```

**Batch Insert (Explicit API):**

```clojure
(def vectors [(float-array [...]) (float-array [...])])
(def ids ["doc-1" "doc-2"])

(def idx2 (prox/insert-batch idx vectors ids))
```

**Batch Insert (Collection Protocol):**

```clojure
(def docs [["doc-1" (float-array [...])]
           ["doc-2" (float-array [...])]
           ["doc-3" (float-array [...])]])

(def idx2 (into idx docs))  ; Fast! Uses transient internally
```

**Auto-Generated IDs:**

```clojure
;; Pass nil for auto-generated UUID
(def idx2 (prox/insert idx vector nil))

;; Or with assoc (generate yourself)
(def idx3 (assoc idx (random-uuid) vector))
```

### Retrieving Vectors

**Get by ID (ILookup protocol):**

```clojure
(get idx "doc-1")  ; => #object[...] (float array)
(seq (get idx "doc-1"))  ; => (0.1 0.2 0.3 ...)

;; With default
(get idx "nonexistent" :not-found)  ; => :not-found

;; Index as function
(idx "doc-1")  ; => #object[...]

;; Or use explicit API
(prox/get-vector idx "doc-1")  ; => #object[...]
```

### Deleting Vectors

**Soft Delete:**

```clojure
(def idx2 (prox/delete idx "doc-1"))
;; Or: (def idx2 (dissoc idx "doc-1"))

;; Vector is marked deleted but space not reclaimed
(count idx2)  ; Only counts live vectors

;; Check metrics
(prox/index-metrics idx2)
; => {:live-count 9, :deleted-count 1, :deletion-ratio 0.1, ...}
```

**Compaction (Reclaim Space):**

```clojure
;; When deletion ratio is high, compact
(when (prox/needs-compaction? idx2)
  (def compacted (prox/compact idx2 {:store-config {:backend :file
                                                     :path "/var/data/vectors-new"
                                                     :id (random-uuid)}
                                      :mmap-dir "/var/data/mmap-new"})))
```

---

## Search & Latency Controls

### Basic Search

```clojure
(def results (prox/search idx query-vector 10))
; => ({:id "doc-1", :distance 0.234}
;     {:id "doc-2", :distance 0.456}
;     ...)

;; Extract IDs
(map :id results)  ; => ("doc-1" "doc-2" ...)

;; Extract distances
(map :distance results)  ; => (0.234 0.456 ...)
```

### Latency Controls

Control search latency and quality with options:

```clojure
;; Higher ef = better recall, slower search
(def results (prox/search idx query-vector 10 {:ef 100}))

;; Timeout to prevent slow queries
(def results (prox/search idx query-vector 10 {:timeout-ms 100}))

;; Early stopping with patience (stop after N candidates with no improvement)
(def results (prox/search idx query-vector 10 {:patience 50
                                                 :patience-saturation 0.95}))

;; Minimum similarity threshold (0.0 to 1.0)
(def results (prox/search idx query-vector 10 {:min-similarity 0.8}))

;; Combine multiple controls
(def results (prox/search idx query-vector 10 {:ef 100
                                                 :timeout-ms 100
                                                 :min-similarity 0.7}))
```

**Available options:**

| Option | Type | Description |
|--------|------|-------------|
| `:ef` | int | Beam width (higher = better recall, slower) |
| `:timeout-ms` | int | Maximum search time in milliseconds |
| `:patience` | int | Early stopping: candidates to check with no improvement |
| `:patience-saturation` | double | Early stopping: similarity threshold (0.0-1.0) |
| `:min-similarity` | double | Minimum similarity to include in results (0.0-1.0) |

### Search with Metadata

Include metadata in search results:

```clojure
(def results (prox/search-with-metadata idx query-vector 10))
; => ({:id "doc-1", :distance 0.234, :metadata {:title "..." :category :science}}
;     ...)

;; Now metadata is included in results
(:metadata (first results))
```

### Filtered Search

Search only specific IDs (multi-tenant, per-user, etc.):

```clojure
;; Search only specific IDs
(def allowed-ids #{"doc-1" "doc-3" "doc-5"})
(def results (prox/search-filtered idx query-vector 10 allowed-ids))
; Only returns results from allowed-ids set

;; With options
(def results (prox/search-filtered idx query-vector 10 allowed-ids {:ef 100}))
```

---

## Versioning & Branching

### Snapshot Semantics

Every operation creates a new snapshot:

```clojure
(def v1 (assoc idx "a" vec-a))
(def v2 (assoc v1 "b" vec-b))
(def v3 (assoc v2 "c" vec-c))

;; All versions are independent
(count v1)  ; => 1
(count v2)  ; => 2
(count v3)  ; => 3

;; All versions are queryable
(prox/search v1 query 5)  ; Search snapshot v1
(prox/search v3 query 5)  ; Search snapshot v3
```

**Use case: Time travel**

```clojure
;; Save reference before modification
(def before-update idx)

;; Make changes
(def after-update (-> idx
                      (assoc "new-doc" vec)
                      (dissoc "old-doc")))

;; Compare search results
(prox/search before-update query 10)  ; Old results
(prox/search after-update query 10)   ; New results
```

### Commits and Sync

Create commits to persist state:

```clojure
;; Add vectors (in-memory)
(def idx2 (-> idx
              (assoc "doc-1" vec1)
              (assoc "doc-2" vec2)))

;; Create commit (persist to disk)
(prox/sync! idx2)

;; Get commit ID
(def commit-id (prox/get-commit-id idx2))
; => #uuid "550e8400-e29b-41d4-a716-446655440000"
```

### Branching

Create lightweight branches for experiments:

```clojure
;; Main branch
(def main (-> idx
              (assoc "doc-1" vec1)
              (assoc "doc-2" vec2)))

(prox/sync! main)

;; Create experiment branch (creates reflinked mmap copy)
(def experiment (prox/branch! main :experiment))

;; Modify experiment
(def experiment2 (-> experiment
                     (assoc "exp-doc-1" vec3)
                     (assoc "exp-doc-2" vec4)))

;; Main is unchanged
(count main)        ; => 2
(count experiment2) ; => 4

;; Search both
(prox/search main query 5)        ; Production results
(prox/search experiment2 query 5) ; Experimental results
```

**List branches:**

```clojure
(prox/branches idx)
; => #{:main :experiment :staging}
```

**Get current branch:**

```clojure
(prox/get-branch idx)
; => :main
```

**Delete a branch:**

```clojure
(prox/delete-branch! idx :experiment)
```

### Fork (In-Memory Copy)

For O(1) in-memory copies without persistence:

```clojure
(def original idx)
(def fork1 (prox/fork original))
(def fork2 (prox/fork original))

;; All three share structure, diverge on writes
(def fork1-modified (assoc fork1 "new-doc" vec))

(count original)       ; => 10
(count fork1-modified) ; => 11
(count fork2)          ; => 10
```

**Fork vs Branch:**
- `fork`: In-memory copy, no persistence, O(1)
- `branch!`: Persisted branch, requires `sync!`, creates mmap copy

### History and Commits

**View commit history:**

```clojure
(def history (prox/history idx))
; => ({:proximum/commit-id #uuid "...",
;      :proximum/created-at #inst "...",
;      :proximum/branch :main,
;      :proximum/vector-count 100}
;     ...)

;; Get recent commits
(take 5 history)
```

**Load historical commit:**

```clojure
(def old-version (prox/load-commit {:backend :file
                                    :path "/var/data/vectors"
                                    :id storage-id}
                                   commit-id))

(prox/search old-version query 10)  ; Search historical state!
```

**Commit info:**

```clojure
(prox/commit-info idx commit-id)
; => {:commit-id #uuid "...",
;     :parents #{#uuid "..."},
;     :created-at #inst "...",
;     :branch :main,
;     :vector-count 100,
;     :deleted-count 5}
```

---

## Metadata

Attach arbitrary data to vectors for filtering, display, and context.

### Adding Metadata

**With assoc:**

```clojure
(def idx2 (assoc idx "doc-1" {:vector (float-array [...])
                               :metadata {:title "Introduction to AI"
                                         :author "Alice"
                                         :category :ai
                                         :timestamp 1234567890}}))
```

**With insert:**

```clojure
(def idx3 (prox/insert idx2
                       (float-array [...])
                       "doc-2"
                       {:title "Vector Databases"
                        :author "Bob"
                        :tags [:database :search]}))
```

### Retrieving Metadata

```clojure
(prox/get-metadata idx "doc-1")
; => {:title "Introduction to AI", :author "Alice", :category :ai, :timestamp 1234567890}
```

### Updating Metadata

```clojure
(def idx4 (prox/with-metadata idx "doc-1" {:title "Updated Title"
                                            :author "Alice"
                                            :updated true}))
```

### Search with Metadata

Include metadata in search results:

```clojure
(def results (prox/search-with-metadata idx query-vec 10))

(doseq [{:keys [id distance metadata]} results]
  (println (:title metadata) "- Distance:" distance))
```

### Filtered Search by Metadata

Build ID sets from metadata predicates:

```clojure
;; Filter by category (build ID set from metadata)
(defn get-ids-by-category [idx category]
  (->> (seq idx)
       (keep (fn [[id _vec]]
               (when (= category (:category (prox/get-metadata idx id)))
                 id)))
       set))

(def ai-doc-ids (get-ids-by-category idx :ai))
(def results (prox/search-filtered idx query-vec 10 ai-doc-ids))
```

**Note**: For large-scale metadata filtering, consider maintaining external indices (like Datahike) that map metadata → external IDs.

---

## Advanced Features

### Crypto-Hash (Auditability)

Enable cryptographic hashing for compliance and verification:

```clojure
(def idx (prox/create-index {:type :hnsw
                              :dim 384
                              :crypto-hash? true  ; Enable SHA-512
                              :store-config {...}
                              ...}))

(def idx2 (assoc idx "doc-1" vec))
(prox/sync! idx2)

;; Get commit hash (chains like git)
(def hash1 (prox/get-commit-hash idx2))
; => #uuid "9d4a2f1c-..."

(def idx3 (assoc idx2 "doc-2" vec))
(prox/sync! idx3)

(def hash2 (prox/get-commit-hash idx3))
; => #uuid "a1b2c3d4-..." (different from hash1)

;; Verify integrity from cold storage
(prox/verify-from-cold {:backend :file :path "/var/data/vectors" :id storage-id})
; => {:valid? true, :vectors-verified 100, :edges-verified 5000}
```

**Use cases:**
- Compliance (HIPAA, GDPR audit trails)
- Backup verification (detect corruption)
- Supply chain integrity
- Reproducibility (exact hash ensures same state)

### Compaction

Reclaim space from deleted vectors:

```clojure
;; Check if compaction is needed
(prox/needs-compaction? idx)  ; => true (if deletion ratio > 0.3)

;; Get metrics
(def metrics (prox/index-metrics idx))
(:deletion-ratio metrics)  ; => 0.35

;; Compact (offline)
(def compacted (prox/compact idx {:store-config {:backend :file
                                                  :path "/var/data/vectors-new"
                                                  :id (random-uuid)}
                                   :mmap-dir "/var/data/mmap-new"}))

;; Result: smaller disk usage, faster searches
(prox/index-metrics compacted)
; => {:deletion-ratio 0.0, ...}
```

**When to compact:**
- Deletion ratio > 0.3 (30% deleted)
- Query performance degrading
- Disk space running low
- During maintenance window

### Garbage Collection

Remove unreachable commits and branches:

```clojure
;; GC unreferenced data
(def deleted-keys (prox/gc! idx))

(println "Freed" (count deleted-keys) "keys")
```

**What GC does:**
- Removes commits with no branch pointing to them
- Removes data from deleted branches
- Reclaims storage space

**When to GC:**
- After deleting branches
- Periodically (weekly/monthly)
- When storage is running low

### Index Metrics

Get comprehensive health metrics:

```clojure
(def metrics (prox/index-metrics idx))

;; Vector counts
(:vector-count metrics)   ; Total vectors (including deleted)
(:live-count metrics)     ; Live vectors
(:deleted-count metrics)  ; Deleted vectors

;; Health
(:deletion-ratio metrics)      ; 0.0 to 1.0
(:needs-compaction? metrics)   ; true/false
(:utilization metrics)         ; Percentage of capacity used

;; Graph statistics
(:edge-count metrics)          ; Total edges in HNSW graph
(:avg-edges-per-node metrics)  ; Average connectivity

;; Performance
(:cache-hits metrics)          ; Edge cache hits
(:cache-misses metrics)        ; Edge cache misses

;; Current state
(:branch metrics)     ; Current branch
(:commit-id metrics)  ; Current commit ID
```

---

## Performance Tips

### 1. Batch Operations

**Use `into` for batches:**

```clojure
;; Slow: Individual assoc calls
(def slow (reduce (fn [idx [id vec]]
                    (assoc idx id vec))
                  idx
                  large-dataset))

;; Fast: Uses transient internally
(def fast (into idx large-dataset))
```

**Or use `insert-batch`:**

```clojure
(def idx2 (prox/insert-batch idx vectors ids))
```

### 2. Transient for Building

When building from scratch with > 1000 vectors:

```clojure
(def idx2
  (persistent!
    (reduce (fn [t-idx [id vec]]
              (assoc! t-idx id vec))
            (transient idx)
            large-dataset)))
```

### 3. Tune HNSW Parameters

| Parameter | Higher = | Lower = | Sweet Spot |
|-----------|----------|---------|------------|
| **M** | Better recall, more memory | Faster build | 16-32 |
| **ef-construction** | Better graph quality | Faster inserts | 100-400 |
| **ef-search** | Better recall | Faster search | 50-200 |

```clojure
;; For recall-critical applications
{:M 32, :ef-construction 400, :ef-search 200}

;; For speed-critical applications
{:M 16, :ef-construction 100, :ef-search 50}

;; Balanced
{:M 16, :ef-construction 200, :ef-search 100}
```

### 4. Distance Metrics

Choose the right metric for your embeddings:

```clojure
;; For normalized embeddings (unit vectors)
{:distance :cosine}

;; For unnormalized embeddings
{:distance :euclidean}

;; For maximum inner product search
{:distance :inner-product}
```

### 5. Capacity Planning

Set capacity to expected size to avoid reallocation:

```clojure
;; If you expect 1M vectors
{:capacity 1000000}

;; Default is 10M (allocates memory even if not filled)
```

### 6. Memory-Mapped Files

Keep mmap files on fast storage:

```clojure
{:mmap-dir "/mnt/nvme/vectors"}  ; SSD
```

### 7. Sync Frequency

Balance durability vs performance:

```clojure
;; High-throughput ingestion: Sync in batches
(def idx2 (into idx batch1))
(def idx3 (into idx2 batch2))
(def idx4 (into idx3 batch3))
(prox/sync! idx4)  ; Sync once after all batches

;; Critical data: Sync more frequently
(def idx2 (assoc idx "critical-doc" vec))
(prox/sync! idx2)  ; Sync immediately
```

### 8. Fork vs Branch

```clojure
;; For in-memory experiments (no persistence)
(def fork (prox/fork idx))  ; O(1), instant

;; For durable branches (with persistence)
(prox/sync! idx)  ; Must sync first
(def branch (prox/branch! idx :experiment))  ; Slower, copies mmap
```

---

## Troubleshooting

### Issue: "Cannot find protocol method"

**Problem**: Using protocols on wrong object type.

**Solution**: Ensure you're calling on the index, not search results:

```clojure
;; Wrong
(def results (prox/search idx query 10))
(assoc results ...)  ; Error! results is a seq, not an index

;; Right
(def idx2 (assoc idx "new-doc" vec))
```

### Issue: "Index is read-only"

**Problem**: Trying to modify a loaded historical commit.

**Solution**: Historical commits are read-only. Fork if you need to modify:

```clojure
(def historical (prox/load-commit config commit-id))
(def writable (prox/fork historical))
(def modified (assoc writable "new-doc" vec))
```

### Issue: "Dimension mismatch"

**Problem**: Vectors don't match index dimensions.

**Solution**: Ensure all vectors have correct dimensionality:

```clojure
(def idx (prox/create-index {:dim 384 ...}))

;; Wrong
(assoc idx "doc" (float-array (range 512)))  ; Error! 512 ≠ 384

;; Right
(assoc idx "doc" (float-array (range 384)))  ; OK
```

### Issue: "Store already exists"

**Problem**: Trying to create index when storage path already exists.

**Solution**: Use `load` to reconnect, or delete existing store:

```clojure
;; Delete existing
(clojure.java.io/delete-file "/var/data/vectors" true)

;; Or reconnect
(def idx (prox/load {:backend :file :path "/var/data/vectors" :id storage-id}))
```

### Issue: "Out of capacity"

**Problem**: Exceeded `:capacity` setting.

**Solution**: Create new index with larger capacity, copy data:

```clojure
(def new-idx (prox/create-index {:capacity (* 2 old-capacity) ...}))
(def migrated (into new-idx (seq old-idx)))
```

### Issue: "Slow searches"

**Problem**: Poor recall or high deletion ratio.

**Solutions:**

1. **Increase ef-search**:
   ```clojure
   (prox/search idx query 10 {:ef 200})  ; Higher ef = better recall
   ```

2. **Compact if deletion ratio high**:
   ```clojure
   (when (> (:deletion-ratio (prox/index-metrics idx)) 0.3)
     (prox/compact idx ...))
   ```

3. **Tune M and ef-construction** when creating index.

### Issue: "High memory usage"

**Problem**: Large indices consuming too much RAM.

**Solutions:**

1. **Reduce cache-size**:
   ```clojure
   {:cache-size 1000}  ; Default is 10000
   ```

2. **Reduce M**:
   ```clojure
   {:M 8}  ; Default is 16
   ```

3. **Use mmap** for vector storage (automatic).

### Issue: "ClassNotFoundException" or "IllegalCallerException"

**Problem**: Missing JVM options for Vector API / Foreign Memory.

**Symptoms**:
- `ClassNotFoundException: jdk.incubator.vector.VectorSpecies`
- `IllegalCallerException: ... does not have native access enabled`
- Application exits with error (not a crash)

**Solution**: Add required JVM options:

```clojure
:jvm-opts ["--add-modules=jdk.incubator.vector"
           "--enable-native-access=ALL-UNNAMED"]
```

---

## Examples

Browse working examples in the [`examples/clojure/`](../examples/clojure/) directory:

- **quick_start.clj**: Basic usage with collection protocols
- **semantic_search.clj**: RAG-style semantic search
- **collection_protocols.clj**: Deep dive into Clojure collection API

---

**Questions?** Open an issue on [GitHub](https://github.com/replikativ/proximum/issues).
