package proximum.internal;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Arrays;
import java.util.Set;

/**
 * Persistent edge store with copy-on-write semantics for HNSW graphs.
 *
 * <h2>Memory Layout</h2>
 * <p>Unified chunking for all layers:</p>
 * <ul>
 *   <li>All layers use chunked arrays for structural sharing</li>
 *   <li>Each chunk holds neighbors for CHUNK_SIZE (1024) nodes</li>
 *   <li>On modification: copy only the affected chunk (CoW)</li>
 *   <li>Upper layer chunks allocated lazily (sparse allocation)</li>
 * </ul>
 *
 * <p>Layer 0: slotsPerNode = M0 + 1 (count + M0 neighbors)<br>
 * Layer 1+: slotsPerNodeUpper = M + 1 (count + M neighbors)</p>
 *
 * <h2>Storage Modes</h2>
 * <ul>
 *   <li><b>In-memory</b> (storage=null): All chunks in heap</li>
 *   <li><b>Storage-backed</b>: Chunks can be evicted via SoftReference, reloaded on demand</li>
 * </ul>
 *
 * <h2>Soft Reference Memory Management</h2>
 * <p>When storage is configured, chunks can be "softified" to free memory:</p>
 * <ol>
 *   <li>After persisting a chunk, call {@link #softifyChunk} to clear the hard reference</li>
 *   <li>The chunk data is held only by a SoftReference (eligible for GC under memory pressure)</li>
 *   <li>On next access, if GC cleared the soft ref, chunk is reloaded from storage</li>
 *   <li>Write operations automatically resolve softified chunks before modification</li>
 * </ol>
 *
 * <h2>Thread Safety</h2>
 * <ul>
 *   <li>Striped locks (1024) for concurrent insert (low memory ~50KB, low contention)</li>
 *   <li>Lock-free reads (snapshot isolation via immutable chunks)</li>
 *   <li>AtomicBoolean 'edit' controls transient vs persistent mode</li>
 *   <li>LongAdder for accurate concurrent cache statistics</li>
 * </ul>
 *
 * <p><b>Internal API</b> - subject to change without notice.</p>
 */
public final class PersistentEdgeStore {

    // Chunk size for all layers (power of 2 for fast indexing)
    public static final int CHUNK_SIZE = 1024;
    public static final int CHUNK_SHIFT = 10;  // log2(1024)
    public static final int CHUNK_MASK = CHUNK_SIZE - 1;

    // Layer 0: Dense chunked storage
    // layer0Chunks[chunkIdx] = int[CHUNK_SIZE * slotsPerNode]
    // Each node gets slotsPerNode ints: [count, n0, n1, ..., n_{M0-1}]
    private final AtomicReference<int[][]> layer0Chunks;
    private final int slotsPerNode;  // M0 + 1 (count + neighbors)

    // Upper layers: Sparse chunked storage (same structure as layer 0)
    // upperLayerChunks[layer-1][chunkIdx] = int[CHUNK_SIZE * slotsPerNodeUpper] or null
    // Chunks allocated lazily when first node at that chunk is written
    private final AtomicReference<int[][][]> upperLayerChunks;
    private final int slotsPerNodeUpper;  // M + 1 (count + neighbors)

    // Configuration
    private final int maxNodes;
    private final int maxLevel;
    private final int M;   // max neighbors for upper layers
    private final int M0;  // max neighbors for layer 0
    private final int numChunks;  // total chunks per layer

    // Entry point and current max level
    private final AtomicInteger entrypoint;
    private final AtomicInteger currentMaxLevel;

    // Striped locks for concurrent insert (1024 stripes instead of per-node)
    // Reduces memory from ~50MB (1M locks) to ~50KB while maintaining low contention
    private static final int LOCK_STRIPES = 1024;
    private static final int LOCK_MASK = LOCK_STRIPES - 1;
    private final ReentrantLock[] stripedLocks;

    // Allocation locks for chunk creation (prevents races in transient mode)
    // Uses smaller stripe count since allocation is rare
    private static final int ALLOC_LOCK_STRIPES = 64;
    private static final int ALLOC_LOCK_MASK = ALLOC_LOCK_STRIPES - 1;
    private final Object[] allocLocks;

    // Transient/persistent mode
    private final AtomicBoolean edit;

    // Dirty chunk tracking for persistence
    // Encoded as: (layer << 32) | chunkIdx  (layer 0 = 0, upper layers = 1+)
    private final Set<Long> dirtyChunks;

    // =========================================================================
    // Lazy Loading Support (optional - null = in-memory only mode)
    // =========================================================================

    // Storage backend for lazy loading (null = all chunks in heap)
    // ChunkStorage checks address-map (in Clojure) to determine if chunk is persisted
    private volatile ChunkStorage storage;

    // When storage is set, chunks may be wrapped in SoftReference
    // layer0Refs[chunkIdx] = SoftReference<int[]> (null = not cached or use direct chunk)
    private final AtomicReference<SoftReference<int[]>[]> layer0Refs;
    private final AtomicReference<SoftReference<int[]>[][]> upperLayerRefs;

    // Statistics for monitoring (use LongAdder for accurate concurrent counting)
    private final java.util.concurrent.atomic.LongAdder cacheHits = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder cacheMisses = new java.util.concurrent.atomic.LongAdder();

    // Deletion tracking
    private final AtomicInteger deletedCount;
    private final AtomicReference<long[]> deletedNodes;  // Bitset for deleted nodes

    /**
     * Create a new empty edge store (in-memory mode).
     */
    public PersistentEdgeStore(int maxNodes, int maxLevel, int M, int M0) {
        this(maxNodes, maxLevel, M, M0, null);
    }

    /**
     * Create a new empty edge store with optional storage backend.
     *
     * @param storage Storage backend for lazy loading (null = in-memory only)
     */
    @SuppressWarnings("unchecked")
    public PersistentEdgeStore(int maxNodes, int maxLevel, int M, int M0, ChunkStorage storage) {
        this.maxNodes = maxNodes;
        this.maxLevel = maxLevel;
        this.M = M;
        this.M0 = M0;
        this.slotsPerNode = M0 + 1;  // count + M0 neighbors
        this.slotsPerNodeUpper = M + 1;  // count + M neighbors

        this.numChunks = (maxNodes + CHUNK_SIZE - 1) / CHUNK_SIZE;
        this.layer0Chunks = new AtomicReference<>(new int[numChunks][]);

        // Upper layers: array of chunk arrays, all initially null (lazy allocation)
        this.upperLayerChunks = new AtomicReference<>(new int[maxLevel][][]);

        this.entrypoint = new AtomicInteger(-1);
        this.currentMaxLevel = new AtomicInteger(-1);

        // Allocate striped locks (1024 stripes instead of per-node)
        this.stripedLocks = new ReentrantLock[LOCK_STRIPES];
        for (int i = 0; i < LOCK_STRIPES; i++) {
            stripedLocks[i] = new ReentrantLock();
        }

        // Allocate allocation locks for chunk creation
        this.allocLocks = new Object[ALLOC_LOCK_STRIPES];
        for (int i = 0; i < ALLOC_LOCK_STRIPES; i++) {
            allocLocks[i] = new Object();
        }

        this.edit = new AtomicBoolean(false);
        this.dirtyChunks = ConcurrentHashMap.newKeySet();

        // Lazy loading support
        this.storage = storage;
        this.layer0Refs = new AtomicReference<>(null);  // Allocated on first use
        this.upperLayerRefs = new AtomicReference<>(null);

        // Deletion tracking
        this.deletedCount = new AtomicInteger(0);
        this.deletedNodes = new AtomicReference<>(new long[(maxNodes + 63) / 64]);
    }

    /**
     * Private constructor for forking.
     */
    private PersistentEdgeStore(
            int maxNodes, int maxLevel, int M, int M0,
            int slotsPerNode, int slotsPerNodeUpper, int numChunks,
            int[][] layer0Chunks, int[][][] upperLayerChunks,
            int entrypoint, int currentMaxLevel,
            ReentrantLock[] stripedLocks, Object[] allocLocks,
            ChunkStorage storage,
            SoftReference<int[]>[] layer0Refs, SoftReference<int[]>[][] upperLayerRefs,
            int deletedCount, long[] deletedNodesBitset) {
        this.maxNodes = maxNodes;
        this.maxLevel = maxLevel;
        this.M = M;
        this.M0 = M0;
        this.slotsPerNode = slotsPerNode;
        this.slotsPerNodeUpper = slotsPerNodeUpper;
        this.numChunks = numChunks;

        // Shallow copy of chunk arrays (structural sharing)
        this.layer0Chunks = new AtomicReference<>(layer0Chunks.clone());

        // Shallow copy of upper layer chunk arrays
        int[][][] upperCopy = new int[maxLevel][][];
        for (int l = 0; l < maxLevel; l++) {
            if (upperLayerChunks[l] != null) {
                upperCopy[l] = upperLayerChunks[l].clone();
            }
        }
        this.upperLayerChunks = new AtomicReference<>(upperCopy);

        this.entrypoint = new AtomicInteger(entrypoint);
        this.currentMaxLevel = new AtomicInteger(currentMaxLevel);

        // Share striped locks (forks coordinate on same nodes)
        this.stripedLocks = stripedLocks;
        // Share allocation locks
        this.allocLocks = allocLocks;

        // Start in persistent mode
        this.edit = new AtomicBoolean(false);

        // Fork starts with empty dirty set
        this.dirtyChunks = ConcurrentHashMap.newKeySet();

        // Lazy loading support - share storage (address-map is in Clojure)
        this.storage = storage;
        // Share soft refs (they point to same chunks via structural sharing)
        this.layer0Refs = new AtomicReference<>(layer0Refs != null ? layer0Refs.clone() : null);
        this.upperLayerRefs = new AtomicReference<>(upperLayerRefs != null ? upperLayerRefs.clone() : null);

        // Copy deletion state (fork inherits deleted nodes)
        this.deletedCount = new AtomicInteger(deletedCount);
        this.deletedNodes = new AtomicReference<>(deletedNodesBitset.clone());
    }

    // =========================================================================
    // Transient/Persistent Mode
    // =========================================================================

    /**
     * Switch to transient mode for bulk mutations.
     * Mutations modify in place without copying.
     */
    public PersistentEdgeStore asTransient() {
        if (!edit.compareAndSet(false, true)) {
            throw new IllegalStateException("Already in transient mode");
        }
        return this;
    }

    /**
     * Switch to persistent mode, sealing for immutable sharing.
     */
    public PersistentEdgeStore asPersistent() {
        if (!edit.compareAndSet(true, false)) {
            throw new IllegalStateException("Not in transient mode");
        }
        return this;
    }

    /**
     * Check if in transient mode.
     */
    public boolean isTransient() {
        return edit.get();
    }

    /**
     * Ensure PES is in transient mode before mutation.
     * Throws if in persistent mode to prevent accidental shared state mutation.
     */
    private void ensureTransient() {
        if (!edit.get()) {
            throw new IllegalStateException(
                "Cannot mutate persistent PES. Fork first, then call asTransient().");
        }
    }

    /**
     * Set transient mode.
     * In transient mode, copy-on-write is disabled for better bulk insert performance.
     * Call with true before bulk inserts, and false after to re-enable COW.
     *
     * @param transient_ true to enable transient mode (no COW), false for persistent mode (COW enabled)
     * @return this for chaining
     */
    public PersistentEdgeStore setTransient(boolean transient_) {
        edit.set(transient_);
        return this;
    }

    /**
     * Create a fork with shared structure.
     * The fork starts in persistent mode.
     *
     * Shallow-clones the chunk arrays so the forked PES can independently
     * assign chunks without affecting the original. Individual chunks are
     * still shared until modified (CoW happens at chunk level).
     */
    public PersistentEdgeStore fork() {
        // Shallow clone layer0 chunks array
        int[][] oldLayer0 = layer0Chunks.get();
        int[][] newLayer0 = oldLayer0.clone();

        // Shallow clone upper layer chunks arrays
        int[][][] oldUpper = upperLayerChunks.get();
        int[][][] newUpper = new int[oldUpper.length][][];
        for (int i = 0; i < oldUpper.length; i++) {
            if (oldUpper[i] != null) {
                newUpper[i] = oldUpper[i].clone();
            }
        }

        // Clone refs arrays similarly (if present)
        SoftReference<int[]>[] oldL0Refs = layer0Refs.get();
        SoftReference<int[]>[] newL0Refs = oldL0Refs != null ? oldL0Refs.clone() : null;

        SoftReference<int[]>[][] oldUpperRefs = upperLayerRefs.get();
        SoftReference<int[]>[][] newUpperRefs = null;
        if (oldUpperRefs != null) {
            newUpperRefs = new SoftReference[oldUpperRefs.length][];
            for (int i = 0; i < oldUpperRefs.length; i++) {
                if (oldUpperRefs[i] != null) {
                    newUpperRefs[i] = oldUpperRefs[i].clone();
                }
            }
        }

        // Clone deleted nodes bitset
        long[] oldDeleted = deletedNodes.get();
        long[] newDeleted = oldDeleted != null ? oldDeleted.clone() : null;

        return new PersistentEdgeStore(
            maxNodes, maxLevel, M, M0,
            slotsPerNode, slotsPerNodeUpper, numChunks,
            newLayer0, newUpper,
            entrypoint.get(), currentMaxLevel.get(),
            stripedLocks, allocLocks,
            storage,
            newL0Refs, newUpperRefs,
            deletedCount.get(), newDeleted
        );
    }

    // =========================================================================
    // Storage Management (Lazy Loading)
    // =========================================================================

    /**
     * Set the storage backend for lazy loading.
     * Call this to enable larger-than-RAM operation.
     */
    public void setStorage(ChunkStorage storage) {
        this.storage = storage;
    }

    /**
     * Get the current storage backend.
     */
    public ChunkStorage getStorage() {
        return storage;
    }

    /**
     * Check if storage-backed lazy loading is enabled.
     */
    public boolean hasStorage() {
        return storage != null;
    }

    /**
     * Mark a chunk as persisted at the given address.
     * @deprecated No longer needed - address-map in Clojure tracks persistence.
     *             Kept for backward compatibility, does nothing.
     */
    @Deprecated
    public void markPersisted(long encodedAddress, long storageAddress) {
        // No-op: address tracking is now done entirely in Clojure address-map.
        // ChunkStorage.restore() checks if position exists in address-map.
    }

    /**
     * Convert a chunk to soft reference (allowing GC eviction).
     * Call after persisting to enable memory reclamation under memory pressure.
     *
     * <p><b>Soft Reference Contract:</b></p>
     * <ul>
     *   <li>Only call after the chunk has been persisted to storage</li>
     *   <li>The chunk data must be recoverable via {@link ChunkStorage#restore}</li>
     *   <li>After softifying, the chunk may be GC'd and reloaded on next access</li>
     *   <li>Write operations (setNeighbors) will automatically resolve softified chunks
     *       from soft refs or storage before modification, preventing data loss</li>
     * </ul>
     *
     * <p><b>Memory Management:</b></p>
     * <ul>
     *   <li>Hard reference (chunks array) is cleared, allowing GC of chunk data</li>
     *   <li>Soft reference is retained for fast re-access if still in memory</li>
     *   <li>If soft ref is cleared by GC, chunk is reloaded from storage on next access</li>
     * </ul>
     *
     * <p>Note: This is cache management, not graph mutation, so it doesn't require transient mode.</p>
     *
     * @param encodedAddress The encoded chunk position (layer << 32 | chunkIdx)
     */
    @SuppressWarnings("unchecked")
    public void softifyChunk(long encodedAddress) {
        if (storage == null) return;  // No lazy loading without storage

        int layer = decodeLayer(encodedAddress);
        int chunkIdx = decodeChunkIdx(encodedAddress);

        if (layer == 0) {
            int[][] chunks = layer0Chunks.get();
            int[] chunk = chunks[chunkIdx];
            if (chunk != null) {
                // Ensure refs array exists
                SoftReference<int[]>[] refs = layer0Refs.get();
                if (refs == null) {
                    refs = new SoftReference[numChunks];
                    layer0Refs.set(refs);
                }
                // Store soft reference, clear hard reference
                refs[chunkIdx] = new SoftReference<>(chunk);
                chunks[chunkIdx] = null;
            }
        } else {
            int layerIdx = layer - 1;
            int[][][] upper = upperLayerChunks.get();
            if (upper[layerIdx] != null) {
                int[] chunk = upper[layerIdx][chunkIdx];
                if (chunk != null) {
                    // Ensure refs array exists
                    SoftReference<int[]>[][] refs = upperLayerRefs.get();
                    if (refs == null) {
                        refs = new SoftReference[maxLevel][];
                        upperLayerRefs.set(refs);
                    }
                    if (refs[layerIdx] == null) {
                        refs[layerIdx] = new SoftReference[numChunks];
                    }
                    refs[layerIdx][chunkIdx] = new SoftReference<>(chunk);
                    upper[layerIdx][chunkIdx] = null;
                }
            }
        }
    }

    /**
     * Get cache statistics.
     */
    public long getCacheHits() { return cacheHits.sum(); }
    public long getCacheMisses() { return cacheMisses.sum(); }

    /**
     * Resolve a layer 0 chunk, loading from storage if needed.
     * ChunkStorage.restore() returns null if position is not in address-map.
     */
    @SuppressWarnings("unchecked")
    private int[] resolveLayer0Chunk(int chunkIdx) {
        int[][] chunks = layer0Chunks.get();
        if (chunkIdx >= chunks.length) return null;

        // Fast path: chunk in memory (common case, no storage)
        int[] chunk = chunks[chunkIdx];
        if (chunk != null) {
            return chunk;
        }

        // In-memory mode with no storage: nothing more to try
        if (storage == null) {
            return null;
        }

        // Storage mode: check soft reference cache
        SoftReference<int[]>[] refs = layer0Refs.get();
        long encodedPos = encodePosition(0, chunkIdx);  // layer 0
        if (refs != null && refs[chunkIdx] != null) {
            chunk = refs[chunkIdx].get();
            if (chunk != null) {
                cacheHits.increment();
                storage.accessed(encodedPos);
                return chunk;
            }
        }

        // Slow path: try to reload from storage
        // ChunkStorage checks address-map - returns null if not persisted
        cacheMisses.increment();
        chunk = storage.restore(encodedPos);
        if (chunk != null) {
            // Cache in soft reference
            if (refs == null) {
                refs = new SoftReference[numChunks];
                layer0Refs.set(refs);
            }
            refs[chunkIdx] = new SoftReference<>(chunk);
        }
        return chunk;
    }

    /**
     * Resolve an upper layer chunk, loading from storage if needed.
     * ChunkStorage.restore() returns null if position is not in address-map.
     */
    @SuppressWarnings("unchecked")
    private int[] resolveUpperLayerChunk(int layer, int chunkIdx) {
        int layerIdx = layer - 1;
        int[][][] upper = upperLayerChunks.get();
        if (layerIdx >= upper.length) return null;

        int[][] layerChunks = upper[layerIdx];
        if (layerChunks == null || chunkIdx >= layerChunks.length) return null;

        // Fast path: chunk in memory (common case, no storage)
        int[] chunk = layerChunks[chunkIdx];
        if (chunk != null) {
            return chunk;
        }

        // In-memory mode with no storage: nothing more to try
        if (storage == null) {
            return null;
        }

        // Storage mode: check soft reference cache
        SoftReference<int[]>[][] refs = upperLayerRefs.get();
        long encodedPos = encodePosition(layer, chunkIdx);
        if (refs != null && refs[layerIdx] != null && refs[layerIdx][chunkIdx] != null) {
            chunk = refs[layerIdx][chunkIdx].get();
            if (chunk != null) {
                cacheHits.increment();
                storage.accessed(encodedPos);
                return chunk;
            }
        }

        // Slow path: try to reload from storage
        // ChunkStorage checks address-map - returns null if not persisted
        cacheMisses.increment();
        chunk = storage.restore(encodedPos);
        if (chunk != null) {
            // Cache in soft reference
            if (refs == null) {
                refs = new SoftReference[maxLevel][];
                upperLayerRefs.set(refs);
            }
            if (refs[layerIdx] == null) {
                refs[layerIdx] = new SoftReference[numChunks];
            }
            refs[layerIdx][chunkIdx] = new SoftReference<>(chunk);
        }
        return chunk;
    }

    // =========================================================================
    // Entry Point Management
    // =========================================================================

    public int getEntrypoint() {
        return entrypoint.get();
    }

    public void setEntrypoint(int nodeId) {
        ensureTransient();
        entrypoint.set(nodeId);
    }

    public boolean compareAndSetEntrypoint(int expected, int update) {
        ensureTransient();
        return entrypoint.compareAndSet(expected, update);
    }

    public int getCurrentMaxLevel() {
        return currentMaxLevel.get();
    }

    public void setCurrentMaxLevel(int level) {
        ensureTransient();
        currentMaxLevel.set(level);
    }

    public boolean compareAndSetMaxLevel(int expected, int update) {
        ensureTransient();
        return currentMaxLevel.compareAndSet(expected, update);
    }

    // =========================================================================
    // Neighbor Access - Lock-free reads
    // =========================================================================

    /**
     * Get neighbors for a node at a specific layer.
     * Lock-free read - returns snapshot of current state.
     *
     * @return neighbor array (may be null if not set), or empty array
     */
    public int[] getNeighbors(int layer, int nodeId) {
        if (layer == 0) {
            return getNeighborsLayer0(nodeId);
        } else {
            return getNeighborsUpperLayer(layer, nodeId);
        }
    }

    /**
     * Get neighbors with lock - for use during construction to prevent
     * race conditions with concurrent writers.
     * Like hnswlib's searchBaseLayer which locks each node.
     */
    public int[] getNeighborsWithLock(int layer, int nodeId) {
        ReentrantLock lock = stripedLocks[nodeId & LOCK_MASK];
        lock.lock();
        try {
            return getNeighbors(layer, nodeId);
        } finally {
            lock.unlock();
        }
    }

    private int[] getNeighborsLayer0(int nodeId) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        // Use lazy loading resolution
        int[] chunk = resolveLayer0Chunk(chunkIdx);
        if (chunk == null) return null;

        int base = nodeOffset * slotsPerNode;
        int count = chunk[base];
        if (count == 0) return null;

        int[] neighbors = new int[count];
        System.arraycopy(chunk, base + 1, neighbors, 0, count);
        return neighbors;
    }

    private int[] getNeighborsUpperLayer(int layer, int nodeId) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        // Use lazy loading resolution
        int[] chunk = resolveUpperLayerChunk(layer, chunkIdx);
        if (chunk == null) return null;

        int base = nodeOffset * slotsPerNodeUpper;
        int count = chunk[base];
        if (count == 0) return null;

        int[] neighbors = new int[count];
        System.arraycopy(chunk, base + 1, neighbors, 0, count);
        return neighbors;
    }

    /**
     * Get neighbors into a provided buffer (allocation-free for caller).
     * Returns the number of neighbors copied into buffer.
     * Buffer must be at least M0 (layer 0) or M (upper layers) in size.
     *
     * @return number of neighbors, or -1 if node has no neighbors
     */
    public int getNeighborsInto(int layer, int nodeId, int[] buffer) {
        if (layer == 0) {
            return getNeighborsLayer0Into(nodeId, buffer);
        } else {
            return getNeighborsUpperLayerInto(layer, nodeId, buffer);
        }
    }

    private int getNeighborsLayer0Into(int nodeId, int[] buffer) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        int[] chunk = resolveLayer0Chunk(chunkIdx);
        if (chunk == null) return -1;

        int base = nodeOffset * slotsPerNode;
        int count = chunk[base];
        if (count == 0) return -1;

        System.arraycopy(chunk, base + 1, buffer, 0, count);
        return count;
    }

    private int getNeighborsUpperLayerInto(int layer, int nodeId, int[] buffer) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        int[] chunk = resolveUpperLayerChunk(layer, chunkIdx);
        if (chunk == null) return -1;

        int base = nodeOffset * slotsPerNodeUpper;
        int count = chunk[base];
        if (count == 0) return -1;

        System.arraycopy(chunk, base + 1, buffer, 0, count);
        return count;
    }

    /**
     * Iterate over neighbors without copying (zero-copy access).
     * Calls the consumer for each neighbor ID.
     */
    public void forEachNeighbor(int layer, int nodeId, java.util.function.IntConsumer consumer) {
        if (layer == 0) {
            forEachNeighborLayer0(nodeId, consumer);
        } else {
            forEachNeighborUpperLayer(layer, nodeId, consumer);
        }
    }

    private void forEachNeighborLayer0(int nodeId, java.util.function.IntConsumer consumer) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        // Use lazy loading resolution
        int[] chunk = resolveLayer0Chunk(chunkIdx);
        if (chunk == null) return;

        int base = nodeOffset * slotsPerNode;
        int count = chunk[base];

        for (int i = 0; i < count; i++) {
            consumer.accept(chunk[base + 1 + i]);
        }
    }

    private void forEachNeighborUpperLayer(int layer, int nodeId, java.util.function.IntConsumer consumer) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        // Use lazy loading resolution
        int[] chunk = resolveUpperLayerChunk(layer, chunkIdx);
        if (chunk == null) return;

        int base = nodeOffset * slotsPerNodeUpper;
        int count = chunk[base];

        for (int i = 0; i < count; i++) {
            consumer.accept(chunk[base + 1 + i]);
        }
    }

    /**
     * Get raw chunk data for layer 0 (zero-copy access for search).
     * Returns null if not allocated. Uses lazy loading if storage is set.
     */
    public int[] getLayer0Chunk(int chunkIdx) {
        return resolveLayer0Chunk(chunkIdx);
    }

    /**
     * Get raw chunk data for upper layer (zero-copy access for search).
     * Returns null if not allocated. Uses lazy loading if storage is set.
     */
    public int[] getUpperLayerChunk(int layer, int chunkIdx) {
        return resolveUpperLayerChunk(layer, chunkIdx);
    }

    /**
     * Get slot offset for a node in its chunk (layer 0).
     */
    public int getNodeSlotOffset(int nodeId) {
        return (nodeId & CHUNK_MASK) * slotsPerNode;
    }

    /**
     * Get slot offset for a node in its chunk (upper layers).
     */
    public int getNodeSlotOffsetUpper(int nodeId) {
        return (nodeId & CHUNK_MASK) * slotsPerNodeUpper;
    }

    public int getSlotsPerNode() {
        return slotsPerNode;
    }

    public int getSlotsPerNodeUpper() {
        return slotsPerNodeUpper;
    }

    /**
     * Get neighbor count for a node (faster than getNeighbors().length).
     * Uses lazy loading if storage is set.
     */
    public int getNeighborCount(int layer, int nodeId) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        if (layer == 0) {
            int[] chunk = resolveLayer0Chunk(chunkIdx);
            if (chunk == null) return 0;
            return chunk[nodeOffset * slotsPerNode];
        } else {
            int[] chunk = resolveUpperLayerChunk(layer, chunkIdx);
            if (chunk == null) return 0;
            return chunk[nodeOffset * slotsPerNodeUpper];
        }
    }

    // =========================================================================
    // Neighbor Modification - With CoW and locking
    // =========================================================================

    /**
     * Set neighbors for a node at a specific layer.
     * Thread-safe with striped locking (1024 stripes for low memory, low contention).
     * Requires transient mode - call fork().asTransient() first.
     */
    public void setNeighbors(int layer, int nodeId, int[] neighbors) {
        ensureTransient();
        ReentrantLock lock = stripedLocks[nodeId & LOCK_MASK];
        lock.lock();
        try {
            if (layer == 0) {
                setNeighborsLayer0(nodeId, neighbors);
            } else {
                setNeighborsUpperLayer(layer, nodeId, neighbors);
            }
        } finally {
            lock.unlock();
        }
    }

    private void setNeighborsLayer0(int nodeId, int[] neighbors) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;

        int[][] oldChunks = layer0Chunks.get();
        int[] oldChunk = oldChunks[chunkIdx];

        int[][] chunks;
        int[] chunk;

        if (edit.get()) {
            // Transient mode - mutate in place, but CoW inherited chunks
            chunks = oldChunks;
            long chunkAddr = encodePosition(0, chunkIdx);
            if (oldChunk == null) {
                // Hard ref is null - check soft ref / storage before allocating new
                // This prevents data loss if chunk was softified
                synchronized (allocLocks[chunkIdx & ALLOC_LOCK_MASK]) {
                    chunk = chunks[chunkIdx];  // Re-read under lock
                    if (chunk == null) {
                        // Try to resolve from soft ref or storage
                        int[] resolved = resolveLayer0Chunk(chunkIdx);
                        if (resolved != null) {
                            // Found existing data - must CoW before mutation
                            chunk = resolved.clone();
                        } else {
                            // Truly doesn't exist - allocate new
                            chunk = new int[CHUNK_SIZE * slotsPerNode];
                        }
                        chunks[chunkIdx] = chunk;
                    }
                }
            } else if (!dirtyChunks.contains(chunkAddr)) {
                // Chunk is inherited from fork - must CoW before mutation
                synchronized (allocLocks[chunkIdx & ALLOC_LOCK_MASK]) {
                    // Re-check under lock
                    if (!dirtyChunks.contains(chunkAddr)) {
                        chunk = oldChunk.clone();
                        chunks[chunkIdx] = chunk;
                    } else {
                        chunk = chunks[chunkIdx];
                    }
                }
            } else {
                chunk = oldChunk;
            }
        } else {
            // Persistent mode - CoW BEFORE any mutation
            chunks = oldChunks.clone();
            if (oldChunk == null) {
                // Check soft ref / storage before allocating new
                int[] resolved = resolveLayer0Chunk(chunkIdx);
                if (resolved != null) {
                    chunk = resolved.clone();
                } else {
                    chunk = new int[CHUNK_SIZE * slotsPerNode];
                }
            } else {
                chunk = oldChunk.clone();
            }

            // Write to our PRIVATE copy before publishing
            int base = nodeOffset * slotsPerNode;
            int count = Math.min(neighbors.length, M0);
            chunk[base] = count;
            System.arraycopy(neighbors, 0, chunk, base + 1, count);

            // NOW publish complete result - readers see old OR new, never partial
            chunks[chunkIdx] = chunk;
            layer0Chunks.set(chunks);

            markDirty(0, chunkIdx);
            return;
        }

        // Transient mode - write after allocation
        int base = nodeOffset * slotsPerNode;
        int count = Math.min(neighbors.length, M0);
        chunk[base] = count;
        System.arraycopy(neighbors, 0, chunk, base + 1, count);

        // Mark chunk as dirty
        markDirty(0, chunkIdx);
    }

    private void setNeighborsUpperLayer(int layer, int nodeId, int[] neighbors) {
        int chunkIdx = nodeId >> CHUNK_SHIFT;
        int nodeOffset = nodeId & CHUNK_MASK;
        int layerIdx = layer - 1;

        int[][][] oldUpper = upperLayerChunks.get();
        int[][] oldLayerChunks = oldUpper[layerIdx];
        int[] oldChunk = (oldLayerChunks != null) ? oldLayerChunks[chunkIdx] : null;

        int[][][] upper;
        int[][] layerChunks;
        int[] chunk;

        if (edit.get()) {
            // Transient mode - mutate in place, but CoW inherited chunks
            upper = oldUpper;
            long chunkAddr = encodePosition(layer, chunkIdx);
            if (oldLayerChunks == null || oldChunk == null) {
                // Hard ref is null - check soft ref / storage before allocating new
                // This prevents data loss if chunk was softified
                synchronized (allocLocks[(layerIdx * 31 + chunkIdx) & ALLOC_LOCK_MASK]) {
                    // Re-read under lock
                    layerChunks = upper[layerIdx];
                    if (layerChunks == null) {
                        layerChunks = new int[numChunks][];
                        upper[layerIdx] = layerChunks;
                    }
                    chunk = layerChunks[chunkIdx];
                    if (chunk == null) {
                        // Try to resolve from soft ref or storage
                        int[] resolved = resolveUpperLayerChunk(layer, chunkIdx);
                        if (resolved != null) {
                            // Found existing data - must CoW before mutation
                            chunk = resolved.clone();
                        } else {
                            // Truly doesn't exist - allocate new
                            chunk = new int[CHUNK_SIZE * slotsPerNodeUpper];
                        }
                        layerChunks[chunkIdx] = chunk;
                    }
                }
            } else if (!dirtyChunks.contains(chunkAddr)) {
                // Chunk is inherited from fork - must CoW before mutation
                synchronized (allocLocks[(layerIdx * 31 + chunkIdx) & ALLOC_LOCK_MASK]) {
                    // Re-check under lock
                    if (!dirtyChunks.contains(chunkAddr)) {
                        chunk = oldChunk.clone();
                        oldLayerChunks[chunkIdx] = chunk;
                        layerChunks = oldLayerChunks;
                    } else {
                        layerChunks = upper[layerIdx];
                        chunk = layerChunks[chunkIdx];
                    }
                }
            } else {
                layerChunks = oldLayerChunks;
                chunk = oldChunk;
            }
        } else {
            // Persistent mode - CoW BEFORE any mutation
            upper = oldUpper.clone();
            if (oldLayerChunks == null) {
                layerChunks = new int[numChunks][];
            } else {
                layerChunks = oldLayerChunks.clone();
            }
            if (oldChunk == null) {
                // Check soft ref / storage before allocating new
                int[] resolved = resolveUpperLayerChunk(layer, chunkIdx);
                if (resolved != null) {
                    chunk = resolved.clone();
                } else {
                    chunk = new int[CHUNK_SIZE * slotsPerNodeUpper];
                }
            } else {
                chunk = oldChunk.clone();
            }

            // Write to our PRIVATE copy before publishing
            int base = nodeOffset * slotsPerNodeUpper;
            int count = Math.min(neighbors.length, M);
            chunk[base] = count;
            System.arraycopy(neighbors, 0, chunk, base + 1, count);

            // NOW publish complete result - readers see old OR new, never partial
            layerChunks[chunkIdx] = chunk;
            upper[layerIdx] = layerChunks;
            upperLayerChunks.set(upper);

            markDirty(layer, chunkIdx);
            return;
        }

        // Transient mode - write after allocation
        int base = nodeOffset * slotsPerNodeUpper;
        int count = Math.min(neighbors.length, M);
        chunk[base] = count;
        System.arraycopy(neighbors, 0, chunk, base + 1, count);

        // Mark chunk as dirty
        markDirty(layer, chunkIdx);
    }

    /**
     * Add a neighbor to a node's list, with pruning if needed.
     * Returns the new neighbor list.
     */
    public int[] addNeighbor(int layer, int nodeId, int newNeighbor, int maxNeighbors) {
        ReentrantLock lock = stripedLocks[nodeId & LOCK_MASK];
        lock.lock();
        try {
            int[] current = getNeighbors(layer, nodeId);
            int currentCount = (current == null) ? 0 : current.length;

            // Check if already present
            for (int i = 0; i < currentCount; i++) {
                if (current[i] == newNeighbor) {
                    return current;  // Already present
                }
            }

            int[] newNeighbors;
            if (currentCount < maxNeighbors) {
                // Just append
                newNeighbors = new int[currentCount + 1];
                if (current != null) {
                    System.arraycopy(current, 0, newNeighbors, 0, currentCount);
                }
                newNeighbors[currentCount] = newNeighbor;
            } else {
                // Need pruning - caller should handle this externally
                // For now, just return current without adding
                return current;
            }

            // Update storage
            if (layer == 0) {
                setNeighborsLayer0(nodeId, newNeighbors);
            } else {
                setNeighborsUpperLayer(layer, nodeId, newNeighbors);
            }

            return newNeighbors;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove a neighbor from a node's list.
     * Requires transient mode.
     */
    public void removeNeighbor(int layer, int nodeId, int neighborToRemove) {
        ensureTransient();
        ReentrantLock lock = stripedLocks[nodeId & LOCK_MASK];
        lock.lock();
        try {
            int[] current = getNeighbors(layer, nodeId);
            if (current == null) return;

            int idx = -1;
            for (int i = 0; i < current.length; i++) {
                if (current[i] == neighborToRemove) {
                    idx = i;
                    break;
                }
            }

            if (idx < 0) return;  // Not found

            int[] newNeighbors = new int[current.length - 1];
            System.arraycopy(current, 0, newNeighbors, 0, idx);
            System.arraycopy(current, idx + 1, newNeighbors, idx, current.length - idx - 1);

            if (layer == 0) {
                setNeighborsLayer0(nodeId, newNeighbors);
            } else {
                setNeighborsUpperLayer(layer, nodeId, newNeighbors);
            }
        } finally {
            lock.unlock();
        }
    }

    // =========================================================================
    // Utility
    // =========================================================================

    public int getMaxNodes() {
        return maxNodes;
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    public int getM() {
        return M;
    }

    public int getM0() {
        return M0;
    }

    public int getNumChunks() {
        return numChunks;
    }

    /**
     * Count total edges in the graph.
     */
    public long countEdges() {
        long total = 0;

        // Layer 0
        int[][] chunks = layer0Chunks.get();
        for (int[] chunk : chunks) {
            if (chunk == null) continue;
            for (int i = 0; i < CHUNK_SIZE; i++) {
                total += chunk[i * slotsPerNode];
            }
        }

        // Upper layers
        int[][][] upper = upperLayerChunks.get();
        for (int[][] layerChunks : upper) {
            if (layerChunks == null) continue;
            for (int[] chunk : layerChunks) {
                if (chunk == null) continue;
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    total += chunk[i * slotsPerNodeUpper];
                }
            }
        }

        return total;
    }

    /**
     * Count allocated chunks (for memory statistics).
     */
    public int countAllocatedChunks() {
        int count = 0;

        // Layer 0
        int[][] chunks = layer0Chunks.get();
        for (int[] chunk : chunks) {
            if (chunk != null) count++;
        }

        // Upper layers
        int[][][] upper = upperLayerChunks.get();
        for (int[][] layerChunks : upper) {
            if (layerChunks == null) continue;
            for (int[] chunk : layerChunks) {
                if (chunk != null) count++;
            }
        }

        return count;
    }

    /**
     * Estimate storage size in bytes (for benchmarking).
     * Includes allocated chunk arrays only (not reference overhead).
     */
    public long getStorageBytes() {
        long bytes = 0;

        // Layer 0: each chunk is int[CHUNK_SIZE * (M0 + 1)]
        int[][] chunks = layer0Chunks.get();
        int layer0ChunkSize = CHUNK_SIZE * (M0 + 1);
        for (int[] chunk : chunks) {
            if (chunk != null) {
                bytes += chunk.length * 4L;  // int = 4 bytes
            }
        }

        // Upper layers: each chunk is int[CHUNK_SIZE * (M + 1)]
        int[][][] upper = upperLayerChunks.get();
        int upperChunkSize = CHUNK_SIZE * (M + 1);
        for (int[][] layerChunks : upper) {
            if (layerChunks == null) continue;
            for (int[] chunk : layerChunks) {
                if (chunk != null) {
                    bytes += chunk.length * 4L;
                }
            }
        }

        return bytes;
    }

    /**
     * Clear all edges (for testing).
     * Requires transient mode.
     */
    public void clear() {
        ensureTransient();
        layer0Chunks.set(new int[numChunks][]);
        upperLayerChunks.set(new int[maxLevel][][]);
        entrypoint.set(-1);
        currentMaxLevel.set(-1);
        dirtyChunks.clear();
        deletedCount.set(0);
    }

    // =========================================================================
    // Deletion Tracking (for metrics and compaction)
    // =========================================================================

    /**
     * Mark a node as deleted and increment the count.
     * Call this when removing a node from the graph.
     * Requires transient mode.
     */
    public void markDeleted(int nodeId) {
        ensureTransient();
        // Copy-on-write for deletedNodes bitset
        long[] current = deletedNodes.get();
        long[] copy = current.clone();
        int wordIdx = nodeId >> 6;
        long bit = 1L << (nodeId & 63);
        copy[wordIdx] |= bit;
        deletedNodes.set(copy);
        deletedCount.incrementAndGet();
    }

    /**
     * Increment the deleted count without marking specific node.
     * Requires transient mode.
     * @deprecated Use markDeleted(nodeId) instead for proper tracking.
     */
    @Deprecated
    public void incrementDeletedCount() {
        ensureTransient();
        deletedCount.incrementAndGet();
    }

    /**
     * Check if a node is deleted using the deleted bitset.
     */
    public boolean isDeleted(int nodeId) {
        long[] bits = deletedNodes.get();
        int wordIdx = nodeId >> 6;
        if (wordIdx >= bits.length) return false;
        long bit = 1L << (nodeId & 63);
        return (bits[wordIdx] & bit) != 0;
    }

    /**
     * Get the count of deleted nodes.
     */
    public int getDeletedCount() {
        return deletedCount.get();
    }

    /**
     * Set deleted count directly (for loading from storage).
     * Requires transient mode.
     */
    public void setDeletedCount(int count) {
        ensureTransient();
        deletedCount.set(count);
    }

    /**
     * Get the deleted nodes bitset (for persistence).
     * Returns a defensive copy to avoid external mutation.
     */
    public long[] getDeletedNodesBitset() {
        return deletedNodes.get().clone();
    }

    /**
     * Set the deleted nodes bitset (for loading from storage).
     * Takes a defensive copy to avoid external mutation.
     * Requires transient mode.
     */
    public void setDeletedNodesBitset(long[] bitset) {
        ensureTransient();
        if (bitset == null) {
            deletedNodes.set(new long[(maxNodes + 63) / 64]);
        } else {
            deletedNodes.set(bitset.clone());
        }
    }

    /**
     * Reset deleted state (after compaction).
     * Requires transient mode.
     */
    public void resetDeletedCount() {
        ensureTransient();
        deletedCount.set(0);
        // Reset the bitset
        long[] fresh = new long[(maxNodes + 63) / 64];
        deletedNodes.set(fresh);
    }

    // =========================================================================
    // Direct Chunk Access (for loading from storage)
    // =========================================================================

    /**
     * Set a layer 0 chunk directly (for loading from storage).
     * Does NOT mark as dirty - use only for initial load.
     * Requires transient mode.
     */
    public void setLayer0Chunk(int chunkIdx, int[] chunk) {
        ensureTransient();
        int[][] chunks = layer0Chunks.get();
        chunks[chunkIdx] = chunk;
    }

    /**
     * Set an upper layer chunk directly (for loading from storage).
     * Does NOT mark as dirty - use only for initial load.
     * Requires transient mode.
     */
    public void setUpperLayerChunk(int layer, int chunkIdx, int[] chunk) {
        ensureTransient();
        int layerIdx = layer - 1;
        int[][][] upper = upperLayerChunks.get();

        // Ensure layer chunk array exists
        if (upper[layerIdx] == null) {
            upper[layerIdx] = new int[numChunks][];
        }

        upper[layerIdx][chunkIdx] = chunk;
    }

    // =========================================================================
    // Dirty Chunk Tracking for Persistence
    // =========================================================================

    /**
     * Encode a chunk position as a long.
     * Layer 0 is encoded as layer=0, upper layers as layer=1,2,...
     * This is the identifier passed to ChunkStorage for lookup in address-map.
     */
    public static long encodePosition(int layer, int chunkIdx) {
        return ((long) layer << 32) | (chunkIdx & 0xFFFFFFFFL);
    }

    /**
     * Decode layer from encoded address.
     */
    public static int decodeLayer(long address) {
        return (int) (address >>> 32);
    }

    /**
     * Decode chunk index from encoded address.
     */
    public static int decodeChunkIdx(long address) {
        return (int) address;
    }

    /**
     * Mark a chunk as dirty (modified since last persistence).
     * Dirty chunks will get new storage addresses on next sync.
     */
    private void markDirty(int layer, int chunkIdx) {
        dirtyChunks.add(encodePosition(layer, chunkIdx));
    }

    /**
     * Get all dirty chunk positions.
     * Returns encoded positions - use decodeLayer/decodeChunkIdx to extract.
     */
    public Set<Long> getDirtyChunks() {
        return dirtyChunks;
    }

    /**
     * Get the raw chunk data for a given address.
     * Returns null if chunk doesn't exist.
     */
    public int[] getChunkByAddress(long address) {
        int layer = decodeLayer(address);
        int chunkIdx = decodeChunkIdx(address);

        if (layer == 0) {
            return getLayer0Chunk(chunkIdx);
        } else {
            return getUpperLayerChunk(layer, chunkIdx);
        }
    }

    /**
     * Set chunk data by encoded address (for loading from storage).
     * Does NOT mark as dirty - use only for initial load.
     * Unified handler for all layers.
     */
    public void setChunkByAddress(long address, int[] chunk) {
        int layer = decodeLayer(address);
        int chunkIdx = decodeChunkIdx(address);

        if (layer == 0) {
            setLayer0Chunk(chunkIdx, chunk);
        } else {
            setUpperLayerChunk(layer, chunkIdx, chunk);
        }
    }

    /**
     * Clear all dirty marks (call after successful persistence).
     * Note: This is bookkeeping, not graph mutation, so it doesn't require transient mode.
     */
    public void clearDirty() {
        dirtyChunks.clear();
    }

    /**
     * Check if there are any dirty chunks.
     */
    public boolean hasDirtyChunks() {
        return !dirtyChunks.isEmpty();
    }

    /**
     * Get count of dirty chunks.
     */
    public int countDirtyChunks() {
        return dirtyChunks.size();
    }
}
