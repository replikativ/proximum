# Proximum Persistence Design

This document describes the internal persistence mechanisms that enable Proximum's git-like versioning with zero-cost branching.

## Overview

Proximum achieves persistent (immutable) data structure semantics through **copy-on-write (CoW)** at the chunk level with **structural sharing** between versions. This means:

- Every mutation returns a new index version
- Unchanged data is shared between versions (no copying)
- Fork/branch operations are O(1) regardless of index size
- Historical versions remain accessible

The persistence layer consists of three main components:

| Component | Purpose | Storage |
|-----------|---------|---------|
| **PersistentEdgeStore (PES)** | HNSW graph edges | Chunked int arrays |
| **VectorStorage** | Raw vector data | Memory-mapped file |
| **PersistentSortedSet (PSS)** | Metadata & external IDs | Hitchhiker tree |

This document focuses on **PersistentEdgeStore**, the most complex component.

---

## PersistentEdgeStore (PES)

`PersistentEdgeStore` manages the HNSW graph structure - the neighbor lists for each node at each layer. It's implemented in Java for performance (`src-java/proximum/internal/PersistentEdgeStore.java`).

### Memory Layout

The graph is stored in **chunked arrays** for efficient structural sharing:

```
┌─────────────────────────────────────────────────────────────────┐
│                        CHUNK STRUCTURE                          │
├─────────────────────────────────────────────────────────────────┤
│ CHUNK_SIZE = 1024 nodes per chunk                               │
│ Each node gets (M + 1) int slots: [count, neighbor0, ..., neighborM-1]
│                                                                 │
│ Layer 0:  slotsPerNode = M0 + 1  (typically M0 = 2*M)          │
│ Layer 1+: slotsPerNode = M + 1                                  │
└─────────────────────────────────────────────────────────────────┘

Example with M=16, M0=32:

Layer 0 chunk (1024 nodes × 33 slots × 4 bytes = 135 KB):
┌────────────────────────────────────────────────────────────┐
│ Node 0: [count=5, n0, n1, n2, n3, n4, -, -, ..., -]       │
│ Node 1: [count=12, n0, n1, n2, ..., n11, -, ..., -]       │
│ ...                                                        │
│ Node 1023: [count=8, n0, n1, ..., n7, -, ..., -]          │
└────────────────────────────────────────────────────────────┘

Upper layer chunks are smaller (M+1 slots) and allocated lazily.
```

### Copy-on-Write Semantics

When modifying a node's neighbors, only the affected chunk is copied:

```
BEFORE MODIFICATION:
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ chunks[0] ──────→ │  Chunk A    │     │  Chunk B    │
│ chunks[1] ────────────────────────────→             │
│ chunks[2] ──────→ │  Chunk C    │     └─────────────┘
└─────────────┘     └─────────────┘

AFTER MODIFYING NODE IN CHUNK 1:
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ chunks[0] ──────→ │  Chunk A    │ ←─────── chunks[0] │ (shared)
│ chunks[1] ──────→ │  Chunk B'   │     │  Chunk B    │ ←── old, now unused
│ chunks[2] ──────→ │  Chunk C    │ ←─────── chunks[2] │ (shared)
└─────────────┘     └─────────────┘     └─────────────┘
   NEW VERSION                             OLD VERSION
```

The chunk array itself is also cloned (shallow copy), but unchanged chunks are shared.

### Fork Operation

`fork()` creates a new PES that shares all structure with the original:

```java
public PersistentEdgeStore fork() {
    // Clone the chunk index arrays (shallow - O(numChunks))
    int[][] newLayer0 = oldLayer0.clone();
    int[][][] newUpper = /* shallow clone each layer */;

    // Clone soft reference arrays
    SoftReference<int[]>[] newL0Refs = oldL0Refs.clone();

    // Clone deletion bitset
    long[] newDeleted = oldDeleted.clone();

    return new PersistentEdgeStore(..., newLayer0, newUpper, ...);
}
```

**Cost**: O(numChunks + maxLevel) - typically microseconds even for million-node graphs.

After forking, both PES instances share the same chunk data until one writes. The `dirtyChunks` set tracks which chunks have been modified in the current session.

### Transient vs Persistent Mode

PES supports two modes for different use cases:

| Mode | CoW Behavior | Use Case |
|------|--------------|----------|
| **Persistent** | Full CoW on every write | Normal operations, safe for sharing |
| **Transient** | In-place mutation (with CoW for inherited chunks) | Bulk insert, batch operations |

```java
// Bulk insert pattern
pes.asTransient();
for (vector : vectors) {
    insertNode(pes, vector);  // Fast in-place mutations
}
pes.asPersistent();  // Seal for safe sharing
```

In transient mode:
- Newly allocated chunks are mutated in place
- Inherited chunks (from fork) are still CoW'd before mutation
- The `dirtyChunks` set distinguishes owned vs inherited chunks

### Thread Safety

PES uses **striped locking** for concurrent insert:

```
┌─────────────────────────────────────────────────────────────────┐
│                    STRIPED LOCK DESIGN                          │
├─────────────────────────────────────────────────────────────────┤
│ 1024 ReentrantLock stripes (~50 KB total)                       │
│ Lock selection: stripedLocks[nodeId & 0x3FF]                    │
│                                                                 │
│ Benefits:                                                       │
│ - Much less memory than per-node locks (50KB vs 50MB for 1M)   │
│ - Low contention: nodes in same chunk rarely lock same stripe   │
│ - Allows parallel insert across different graph regions         │
└─────────────────────────────────────────────────────────────────┘
```

Reads are **lock-free** - they see a consistent snapshot via the immutable chunk references.

### Soft Reference Memory Management

For large indices, PES supports **lazy loading** with soft references:

```
┌─────────────────────────────────────────────────────────────────┐
│                 TWO-TIER MEMORY MANAGEMENT                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Hard Reference (chunks[i])     Soft Reference (refs[i])        │
│  ─────────────────────────      ────────────────────────        │
│  • Direct pointer to chunk      • SoftReference<int[]> wrapper  │
│  • Always in memory             • GC can clear under pressure   │
│  • Used for active/dirty data   • Used for cached/clean data    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

LIFECYCLE:

1. Create chunk     → chunks[i] = data, refs[i] = null
2. Persist to disk  → (no change yet)
3. Softify          → refs[i] = SoftRef(data), chunks[i] = null
4. GC clears it     → refs[i].get() = null
5. Next access      → reload from storage, cache in refs[i]
6. Write access     → resolve → clone → chunks[i] = copy
```

The `softifyChunk()` method converts a persisted chunk to soft reference:

```java
public void softifyChunk(long encodedAddress) {
    int[] chunk = chunks[chunkIdx];
    if (chunk != null) {
        refs[chunkIdx] = new SoftReference<>(chunk);
        chunks[chunkIdx] = null;  // Now GC-eligible
    }
}
```

**Important**: Write operations automatically resolve softified chunks before modification, preventing data loss. This is handled in `setNeighborsLayer0` and `setNeighborsUpperLayer`.

### Deletion Tracking

Deleted nodes are tracked with a **bitset** for O(1) lookup:

```java
// Mark node as deleted
long[] bits = deletedNodes.get();
long[] copy = bits.clone();  // CoW
copy[nodeId >> 6] |= (1L << (nodeId & 63));
deletedNodes.set(copy);
deletedCount.incrementAndGet();

// Check if deleted
boolean isDeleted = (bits[nodeId >> 6] & (1L << (nodeId & 63))) != 0;
```

Deleted nodes remain in the graph (soft delete) but are:
- Skipped during search
- Excluded from neighbor candidates
- Reclaimed during compaction

### Dirty Chunk Tracking

The `dirtyChunks` set tracks modified chunks for persistence:

```java
// Encoded as: (layer << 32) | chunkIdx
Set<Long> dirtyChunks = ConcurrentHashMap.newKeySet();

// On modification
dirtyChunks.add(encodePosition(layer, chunkIdx));

// On sync - only persist dirty chunks
for (long addr : dirtyChunks) {
    int[] chunk = getChunkByAddress(addr);
    storage.write(addr, chunk);
}
dirtyChunks.clear();
```

This enables **incremental persistence** - only changed chunks are written to storage.

### Storage Integration

PES integrates with Konserve (the storage layer) through the `ChunkStorage` interface:

```java
public interface ChunkStorage {
    // Restore chunk from storage (returns null if not persisted)
    int[] restore(long encodedPosition);

    // Called on cache hit (for LRU tracking)
    void accessed(long encodedPosition);
}
```

The actual persistence is handled in Clojure (`proximum.storage`), which:
- Maintains an address-map tracking persisted chunk locations
- Uses Konserve's merkle-tree for structural sharing across commits
- Supports multiple backends (file, memory, S3, etc.)

---

## Key Invariants

1. **Immutability**: Once a PES is in persistent mode, its visible state never changes
2. **Structural Sharing**: Forks share unchanged chunks (same object identity)
3. **CoW Isolation**: Writes never affect other versions or forks
4. **Soft Ref Safety**: Writes resolve softified chunks before allocating new ones
5. **Dirty Tracking**: `dirtyChunks` accurately reflects modifications since last sync

---

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| `fork()` | O(numChunks) | Shallow array clones |
| `getNeighbors()` | O(1) | Direct chunk access |
| `setNeighbors()` | O(chunkSize) | CoW copies one chunk |
| `isDeleted()` | O(1) | Bitset lookup |
| `countEdges()` | O(totalChunks) | Scans all chunks |

Memory per 1M nodes (M=16, M0=32):
- Layer 0: ~1000 chunks × 135 KB = ~132 MB
- Upper layers: Sparse, typically <10% of layer 0
- Deletion bitset: 125 KB
- Striped locks: 50 KB

---

## VectorStorage

`VectorStorage` manages raw vector data with a **dual storage** model: a memory-mapped file for fast SIMD access and Konserve for durability (`src/proximum/vectors.clj`).

### Dual Storage Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DUAL STORAGE MODEL                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   KONSERVE (source of truth)      MMAP (runtime cache)          │
│   ─────────────────────────       ────────────────────          │
│   • Chunked vector storage        • Memory-mapped file          │
│   • Async writes                  • Immediate writes            │
│   • Supports S3, GCS, etc.        • SIMD-friendly layout        │
│   • Structural sharing            • Fast reads for search       │
│                                                                  │
│   On sync: mmap → Konserve        On load: Konserve → mmap      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Memory Layout

Vectors are stored contiguously in a memory-mapped file with a 64-byte header:

```
MMAP FILE LAYOUT:

Offset 0-63:  Header
  ├── Bytes 0-3:   Magic "PVDB"
  ├── Bytes 4-7:   Version (int) = 1
  ├── Bytes 8-15:  Count (long) - number of vectors
  ├── Bytes 16-23: Dim (long) - vector dimensionality
  ├── Bytes 24-31: Chunk-size (long) - vectors per Konserve chunk
  └── Bytes 32-63: Reserved

Offset 64+:   Vector data
  ├── Vector 0: [dim floats × 4 bytes]
  ├── Vector 1: [dim floats × 4 bytes]
  ├── ...
  └── Vector N: [dim floats × 4 bytes]

Example: 384-dim vectors
  Vector offset = 64 + (node_id × 384 × 4) bytes
  Direct indexing for zero-copy SIMD access
```

### Chunked Persistence

Vectors are grouped into chunks for efficient Konserve storage:

```clojure
;; Default: 1000 vectors per chunk
CHUNK_SIZE = 1000

;; Write pipeline:
1. append! writes to mmap immediately (fast path)
2. Buffers vectors in write-buffer atom
3. On chunk completion (1000 vectors): flush async to Konserve
4. On sync!: flush partial chunk, wait for all pending writes
```

**Key insight**: Mmap provides immediate read availability while Konserve provides durability. The write buffer ensures chunk-aligned Konserve writes.

### Address Map

VectorStorage maintains a `chunk-address-map` tracking where each chunk is stored:

```clojure
;; chunk-id → storage-address (UUID)
{0 #uuid "a1b2c3..."
 1 #uuid "d4e5f6..."
 2 #uuid "g7h8i9..."}

;; Konserve key format:
[:vectors :chunk #uuid "a1b2c3..."]
```

This indirection enables:
- **Content-based addressing** (crypto mode): address = hash(chunk-content)
- **Structural sharing**: Two branches with same chunk share the same Konserve entry
- **Garbage collection**: Unreferenced chunks can be cleaned up

### Crypto-Hash Mode

When `crypto-hash?` is true, VectorStorage uses content-based addressing:

```clojure
;; Normal mode: random UUID
(generate-chunk-address) → #uuid "random..."

;; Crypto mode: SHA-512 hash of content
(hash-chunk chunk-bytes) → #uuid "hash-of-content..."

;; Commit chaining (like git):
commit-hash = hash(parent-hash + new-chunk-hashes)
```

This provides:
- **Tamper detection**: Content changes produce different addresses
- **Deduplication**: Identical content shares storage
- **Auditability**: Chain of hashes enables verification

### Reflink Support for Branching

When creating a branch, VectorStorage copies the mmap file:

```clojure
(copy-mmap-for-branch! src-path dst-path reflink-supported?)

;; If reflink supported (Btrfs, XFS, ZFS):
cp --reflink=auto src dst  → O(1) copy-on-write

;; Otherwise:
cp src dst  → O(file-size) full copy
```

The copied mmap gives the branch its own writable vector storage while sharing unchanged Konserve chunks.

### Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| `append!` | O(dim) | Write to mmap, buffer for Konserve |
| `get-vector` | O(dim) | Direct mmap read |
| `sync!` | O(pending-writes) | Wait for async Konserve writes |
| `distance-squared-to-node` | O(dim) | SIMD via MemorySegment |
| Branch copy (reflink) | O(1) | Copy-on-write file copy |
| Branch copy (no reflink) | O(file-size) | Full file copy |

---

## PersistentSortedSet (PSS)

Proximum uses Tonsky's `persistent-sorted-set` library for metadata storage. PSS provides an immutable B+ tree (hitchhiker tree variant) with structural sharing.

### Two PSS Instances

```
┌─────────────────────────────────────────────────────────────────┐
│                    PSS-BACKED METADATA                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   METADATA PSS                    EXTERNAL-ID INDEX PSS          │
│   ────────────                    ─────────────────────          │
│   node-id → {:data {...}}         external-id → node-id          │
│                                                                  │
│   Sorted by: node-id (long)       Sorted by: external-id (any)   │
│   Purpose: arbitrary metadata     Purpose: lookup internal ID    │
│                                                                  │
│   Example entries:                Example entries:               │
│   {:node-id 0                     {:external-id "doc-123"        │
│    :data {:label "cat"}}           :node-id 0}                   │
│   {:node-id 1                     {:external-id "doc-456"        │
│    :data {:label "dog"}}           :node-id 1}                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Comparators

```clojure
(defn metadata-comparator [a b]
  ;; Sort by node-id (long)
  (Long/compare (long (:node-id a)) (long (:node-id b))))

(defn external-id-comparator [a b]
  ;; Sort by external-id (any comparable type)
  ;; Mixed types: compare by class name, then pr-str
  (compare (:external-id a) (:external-id b)))
```

### PSS Operations

```clojure
;; Set metadata for a node
(set-metadata metadata-pss node-id {:label "cat"})
;; → Returns new PSS (CoW)

;; Lookup metadata
(lookup-metadata metadata-pss node-id)
;; → {:label "cat"} or nil

;; Set external ID mapping
(set-external-id external-id-pss "doc-123" node-id)
;; → Returns new PSS, throws if duplicate external-id

;; Lookup internal ID by external ID
(lookup-external-id external-id-pss "doc-123")
;; → node-id (long) or nil
```

### Storage Integration

PSS nodes are stored in Konserve via `CachedStorage`:

```clojure
;; Create PSS with Konserve backing
(pss/sorted-set* {:cmp metadata-comparator
                  :storage cached-storage
                  :branching-factor 512
                  :ref-type :weak})

;; On sync: store PSS and get root UUID
(pss/store meta-pss cached-storage)
;; → root-uuid

;; On restore: load PSS from root UUID
(pss/restore-by metadata-comparator root-uuid cached-storage)
;; → PSS instance
```

**Weak references** (`ref-type :weak`) allow the JVM to reclaim cached tree nodes under memory pressure, similar to PES soft references.

---

## HnswIndex: Bringing It All Together

`HnswIndex` (`src/proximum/hnsw.clj`) is the unified index type that combines PES, VectorStorage, and PSS into a cohesive persistent data structure.

### Index State

```clojure
(deftype HnswIndex [
  ;; Hot-path fields (direct access for performance)
  vectors                    ; VectorStorage
  ^PersistentEdgeStore pes-edges
  ^int dim
  ^int distance-type

  ;; All other state in a map
  ^IPersistentMap state]
  ...)

;; state map contains:
{:metadata          ; PSS for node metadata
 :external-id-index ; PSS for external-id → node-id
 :M                 ; HNSW parameter
 :ef-construction   ; construction beam width
 :storage           ; CachedStorage for PSS
 :edge-store        ; Konserve store
 :address-map       ; edge chunk addresses
 :branch            ; current branch name
 :commit-id         ; current commit UUID
 :vector-count      ; total vectors inserted
 :deleted-count     ; soft-deleted count
 ...}
```

### Immutable Operations

Every mutation returns a **new HnswIndex**:

```clojure
(p/insert idx vector {:external-id "doc-1"})
;; 1. Fork PES (O(chunks))
;; 2. Append vector to VectorStorage
;; 3. Insert into graph (mutates forked PES)
;; 4. Update metadata PSS (CoW)
;; 5. Update external-id-index PSS (CoW)
;; 6. Return new HnswIndex with updated fields

(p/delete idx internal-id)
;; 1. Fork PES
;; 2. Mark node deleted in PES (CoW bitset)
;; 3. Update neighbor lists
;; 4. Remove from metadata and external-id-index
;; 5. Return new HnswIndex
```

### Fork Operation

Creating a branch involves forking all three storage layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                       FORK OPERATION                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ORIGINAL INDEX                    FORKED INDEX                  │
│  ──────────────                    ────────────                  │
│                                                                  │
│  PES ─────────→ chunks[]    ←───── PES' (fork)                  │
│       (shared until write)                                       │
│                                                                  │
│  VectorStorage → mmap file  ──cp── VectorStorage' (new mmap)    │
│                  (reflink if supported)                          │
│                                                                  │
│  Metadata PSS ───────────── shared ─────────────→ Metadata PSS' │
│  (CoW tree nodes)                                                │
│                                                                  │
│  External-ID PSS ────────── shared ─────────────→ Ext-ID PSS'   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

Cost breakdown:
- **PES fork**: O(numChunks) - shallow clone arrays
- **VectorStorage**: O(1) with reflink, O(file-size) without
- **PSS**: O(1) - just share the root, CoW on write

### Sync Operation

`sync!` persists all dirty state and creates a commit:

```clojure
(p/sync! idx)
;; 1. Flush vector write buffer to Konserve (async)
;; 2. Flush dirty edge chunks to Konserve (async)
;; 3. Force mmap to disk, update header count
;; 4. Wait for all pending Konserve writes
;; 5. Compute commit hashes (if crypto-hash? enabled)
;; 6. Store PSS roots:
;;    - metadata-pss-root
;;    - external-id-pss-root
;;    - vectors-addr-pss-root
;;    - edges-addr-pss-root
;; 7. Write commit entry with snapshot
;; 8. Update branch head
;; 9. Return updated index with new commit-id
```

### Snapshot Structure

Each commit stores a snapshot in Konserve:

```clojure
{:commit-id          #uuid "..."
 :parents            [#uuid "..."]
 :branch             :main
 :created-at         #inst "2024-..."

 ;; PSS roots (UUIDs pointing to tree roots)
 :metadata-pss-root  #uuid "..."
 :external-id-pss-root #uuid "..."
 :vectors-addr-pss-root #uuid "..."
 :edges-addr-pss-root #uuid "..."

 ;; Graph state
 :entrypoint         123
 :current-max-level  4
 :deleted-nodes-bitset [0 0 0 ...]

 ;; Counts
 :branch-vector-count 10000
 :branch-deleted-count 50

 ;; Crypto hashes (if enabled)
 :vectors-commit-hash #uuid "..."
 :edges-commit-hash #uuid "..."}
```

### Restore Operation

Loading an index from a commit reverses the process:

```clojure
(p/restore-index snapshot edge-store opts)
;; 1. Read :index/config from store
;; 2. Restore PSS instances from roots
;; 3. Load vector address-map, open VectorStorage
;; 4. Create PES in transient mode
;; 5. Load edge chunks from address-map
;; 6. Restore deleted state
;; 7. Seal PES (asPersistent)
;; 8. Return HnswIndex
```

---

## Structural Sharing Across Commits

The key to efficient versioning is that **unchanged data is never copied**:

```
COMMIT 1                 COMMIT 2                 COMMIT 3
────────                 ────────                 ────────
┌──────────┐             ┌──────────┐             ┌──────────┐
│ Snapshot │             │ Snapshot │             │ Snapshot │
├──────────┤             ├──────────┤             ├──────────┤
│ PSS Root ─────┐        │ PSS Root ─────┐        │ PSS Root ──────┐
│ Edges Map     │        │ Edges Map     │        │ Edges Map      │
│ Vectors Map   │        │ Vectors Map   │        │ Vectors Map    │
└──────────┘    │        └──────────┘    │        └──────────┘     │
                │                        │                          │
                ▼                        ▼                          ▼
         ┌───────────┐            ┌───────────┐              ┌───────────┐
         │ PSS Tree  │◄───shared──│ PSS Tree  │◄────shared───│ PSS Tree  │
         │  Node A   │            │  Node A   │              │  Node A   │
         │  Node B   │            │  Node B   │              │  Node B   │
         │  ...      │            │  Node B'  │◄──new        │  Node B'  │
         └───────────┘            │  Node C   │◄──new        │  Node B'' │◄──new
                                  └───────────┘              │  Node C   │
                                                             └───────────┘
```

Each commit only stores:
1. New/modified PSS tree nodes
2. New/modified edge chunks
3. New/modified vector chunks
4. A new snapshot pointing to PSS roots and address maps

---

## Summary: Data Flow

```
                          USER OPERATION
                               │
                               ▼
                     ┌─────────────────┐
                     │   HnswIndex     │
                     │   (immutable)   │
                     └────────┬────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
   ┌────────────────┐ ┌──────────────┐ ┌────────────────┐
   │ VectorStorage  │ │     PES      │ │  PSS (×2)      │
   │ (mmap+chunks)  │ │ (edge graph) │ │ (metadata+ids) │
   └───────┬────────┘ └──────┬───────┘ └───────┬────────┘
           │                 │                 │
           ▼                 ▼                 ▼
       ┌──────────────────────────────────────────┐
       │              KONSERVE                     │
       │  [:vectors :chunk uuid]                   │
       │  [:edges :chunk uuid]                     │
       │  [:pss :node uuid]                        │
       │  [:commits uuid]                          │
       │  [:branches :main]                        │
       └──────────────────────────────────────────┘
```

All three storage layers converge at Konserve, which provides:
- **Durability**: Writes survive process crashes
- **Structural sharing**: Unchanged chunks are reused across commits
- **Backend flexibility**: File, memory, S3, GCS, etc.
- **Merkle-tree semantics**: Content-based addressing and integrity verification

