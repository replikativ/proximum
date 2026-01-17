package proximum;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import proximum.internal.HnswInsert;
import proximum.internal.HnswSearch;
import proximum.internal.PersistentEdgeStore;

/**
 * High-performance HNSW index for approximumimate nearest neighbor search.
 *
 * This is the main entry point for low-level Java users. It provides:
 * - Simple insert/search/delete API
 * - Parallel bulk insert for high throughput
 * - Persistent edge storage with copy-on-write semantics
 * - SIMD-accelerated distance computation (AVX2/AVX-512)
 *
 * Example usage:
 * <pre>
 *   // Create index
 *   HnswIndex index = HnswIndex.create(
 *       Paths.get("/tmp/my-index"),
 *       128,      // dimension
 *       100000,   // capacity
 *       16,       // M (max neighbors per node)
 *       200       // efConstruction
 *   );
 *
 *   // Insert vectors
 *   float[] vector = new float[128];
 *   int id = index.insert(vector);
 *
 *   // Search
 *   SearchResult[] results = index.search(query, 10, 100);
 *
 *   // Close when done
 *   index.close();
 * </pre>
 *
 * Thread safety:
 * - insert() is thread-safe (uses per-node locking)
 * - search() is thread-safe (lock-free reads)
 * - insertBatch() is thread-safe and uses parallel execution
 */
public final class HnswIndex {

    private final VectorStorage vectors;
    private final PersistentEdgeStore edges;
    private final int dim;
    private final int M;
    private final int M0;
    private final int efConstruction;
    private final int maxLevel;
    private final double ml;  // 1/ln(M)
    private volatile boolean closed = false;

    /**
     * Create a new HNSW index.
     *
     * @param path directory for index files
     * @param dim vector dimensionality
     * @param capacity maximum number of vectors
     * @param M max neighbors per node (upper layers)
     * @param efConstruction beam width during construction
     */
    public static HnswIndex create(Path path, int dim, int capacity, int M, int efConstruction) {
        return create(path, dim, capacity, M, efConstruction, 16);
    }

    /**
     * Create a new HNSW index with custom max level.
     *
     * @param path directory for index files
     * @param dim vector dimensionality
     * @param capacity maximum number of vectors
     * @param M max neighbors per node (upper layers)
     * @param efConstruction beam width during construction
     * @param maxLevel maximum HNSW level (typically 16)
     */
    public static HnswIndex create(Path path, int dim, int capacity, int M, int efConstruction, int maxLevel) {
        Path vectorPath = path.resolve("vectors.mmap");
        MmapVectorStorage vectors = new MmapVectorStorage(vectorPath, dim, capacity);
        PersistentEdgeStore edges = new PersistentEdgeStore(capacity, maxLevel, M, 2 * M);
        return new HnswIndex(vectors, edges, dim, M, 2 * M, efConstruction, maxLevel);
    }

    /**
     * Create index with custom vector storage.
     * Use this when you want to manage vector storage separately.
     *
     * @param vectors custom VectorStorage implementation
     * @param M max neighbors per node (upper layers)
     * @param efConstruction beam width during construction
     */
    public static HnswIndex create(VectorStorage vectors, int M, int efConstruction) {
        return create(vectors, M, efConstruction, 16);
    }

    /**
     * Create index with custom vector storage and max level.
     */
    public static HnswIndex create(VectorStorage vectors, int M, int efConstruction, int maxLevel) {
        int capacity = vectors.getCapacity();
        int dim = vectors.getDim();
        PersistentEdgeStore edges = new PersistentEdgeStore(capacity, maxLevel, M, 2 * M);
        return new HnswIndex(vectors, edges, dim, M, 2 * M, efConstruction, maxLevel);
    }

    // Private constructor
    private HnswIndex(VectorStorage vectors, PersistentEdgeStore edges,
                      int dim, int M, int M0, int efConstruction, int maxLevel) {
        this.vectors = vectors;
        this.edges = edges;
        this.dim = dim;
        this.M = M;
        this.M0 = M0;
        this.efConstruction = efConstruction;
        this.maxLevel = maxLevel;
        this.ml = 1.0 / Math.log(M);
    }

    // =========================================================================
    // Insert Operations
    // =========================================================================

    /**
     * Insert a single vector.
     *
     * @param vector float array of length dim
     * @return node ID of the inserted vector
     */
    public int insert(float[] vector) {
        checkNotClosed();
        if (vector.length != dim) {
            throw new IllegalArgumentException(
                "Vector dimension mismatch: expected " + dim + ", got " + vector.length);
        }

        // Append vector to storage
        int nodeId = vectors.append(vector);

        // Generate random level
        int nodeLevel = HnswInsert.randomLevel(ml, maxLevel);

        // Insert into graph
        MemorySegment seg = vectors.getSegment();
        HnswInsert.insert(seg, edges, vector, nodeId, dim, nodeLevel, efConstruction);

        return nodeId;
    }

    /**
     * Insert multiple vectors in parallel.
     * This is more efficient than repeated single inserts.
     *
     * @param vecs array of vectors to insert
     * @return array of node IDs for the inserted vectors
     */
    public int[] insertBatch(float[][] vecs) {
        return insertBatch(vecs, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Insert multiple vectors in parallel with specified parallelism.
     *
     * @param vecs array of vectors to insert
     * @param parallelism number of threads to use
     * @return array of node IDs for the inserted vectors
     */
    public int[] insertBatch(float[][] vecs, int parallelism) {
        checkNotClosed();

        int n = vecs.length;
        int[] nodeIds = new int[n];
        int[] nodeLevels = new int[n];

        // Pre-append all vectors (sequential for thread safety)
        for (int i = 0; i < n; i++) {
            if (vecs[i].length != dim) {
                throw new IllegalArgumentException(
                    "Vector dimension mismatch at index " + i +
                    ": expected " + dim + ", got " + vecs[i].length);
            }
            nodeIds[i] = vectors.append(vecs[i]);
            nodeLevels[i] = HnswInsert.randomLevel(ml, maxLevel);
        }

        // Parallel insert into graph
        MemorySegment seg = vectors.getSegment();
        HnswInsert.insertBatch(seg, edges, vecs, nodeIds, dim, nodeLevels, efConstruction, parallelism);

        return nodeIds;
    }

    // =========================================================================
    // Search Operations
    // =========================================================================

    /**
     * Search result containing node ID and distance.
     */
    public static class SearchResult {
        public final int id;
        public final double distance;

        public SearchResult(int id, double distance) {
            this.id = id;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "SearchResult{id=" + id + ", distance=" + distance + "}";
        }
    }

    /**
     * Search for k nearest neighbors.
     *
     * @param query query vector
     * @param k number of neighbors to return
     * @param ef beam width (should be >= k)
     * @return array of SearchResult sorted by distance
     */
    public SearchResult[] search(float[] query, int k, int ef) {
        checkNotClosed();
        if (query.length != dim) {
            throw new IllegalArgumentException(
                "Query dimension mismatch: expected " + dim + ", got " + query.length);
        }

        MemorySegment seg = vectors.getSegment();
        double[] raw = HnswSearch.search(seg, edges, query, dim, k, ef);

        // Convert to SearchResult array
        int count = raw.length / 2;
        SearchResult[] results = new SearchResult[count];
        for (int i = 0; i < count; i++) {
            results[i] = new SearchResult((int) raw[i * 2], raw[i * 2 + 1]);
        }
        return results;
    }

    /**
     * Search for k nearest neighbors with default ef = k * 10.
     */
    public SearchResult[] search(float[] query, int k) {
        return search(query, k, Math.max(k * 10, 100));
    }

    // =========================================================================
    // Vector Access
    // =========================================================================

    /**
     * Get a vector by ID.
     */
    public float[] getVector(int nodeId) {
        checkNotClosed();
        return vectors.get(nodeId);
    }

    /**
     * Get the MemorySegment for direct SIMD access.
     * Advanced users only - use with care.
     */
    public MemorySegment getVectorSegment() {
        return vectors.getSegment();
    }

    // =========================================================================
    // Edge Access
    // =========================================================================

    /**
     * Get neighbors for a node at a specific layer.
     *
     * @param layer HNSW layer (0 = base layer)
     * @param nodeId node ID
     * @return array of neighbor IDs, or null if not set
     */
    public int[] getNeighbors(int layer, int nodeId) {
        return edges.getNeighbors(layer, nodeId);
    }

    /**
     * Get the entry point node.
     */
    public int getEntrypoint() {
        return edges.getEntrypoint();
    }

    /**
     * Get current maximum level of the graph.
     */
    public int getCurrentMaxLevel() {
        return edges.getCurrentMaxLevel();
    }

    // =========================================================================
    // Fork (Copy-on-Write)
    // =========================================================================

    /**
     * Create a fork of this index.
     * The fork shares structure with the original but can diverge.
     * Modifications to either don't affect the other.
     *
     * Note: Vector storage is shared (not forked). Only edges are forked.
     *
     * @return forked index
     */
    public HnswIndex fork() {
        PersistentEdgeStore forkedEdges = edges.fork();
        return new HnswIndex(vectors, forkedEdges, dim, M, M0, efConstruction, maxLevel);
    }

    // =========================================================================
    // Index Information
    // =========================================================================

    /**
     * Get vector dimensionality.
     */
    public int getDim() {
        return dim;
    }

    /**
     * Get current number of vectors.
     */
    public int size() {
        return vectors.getCount();
    }

    /**
     * Get maximum capacity.
     */
    public int capacity() {
        return vectors.getCapacity();
    }

    /**
     * Get M parameter.
     */
    public int getM() {
        return M;
    }

    /**
     * Get M0 parameter (layer 0 max neighbors).
     */
    public int getM0() {
        return M0;
    }

    /**
     * Get efConstruction parameter.
     */
    public int getEfConstruction() {
        return efConstruction;
    }

    /**
     * Count total edges in the graph.
     */
    public long countEdges() {
        return edges.countEdges();
    }

    /**
     * Check if index is empty.
     */
    public boolean isEmpty() {
        return edges.getEntrypoint() < 0;
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /**
     * Close the index and release resources.
     */
    public void close() {
        if (closed) return;
        closed = true;
        vectors.close();
    }

    /**
     * Check if the index has been closed.
     */
    public boolean isClosed() {
        return closed;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }
    }

    // =========================================================================
    // Transient Mode (for bulk operations)
    // =========================================================================

    /**
     * Switch edges to transient mode for bulk mutations.
     * In transient mode, edge updates are faster (no CoW copies).
     * Call asPersistent() when done to seal for sharing.
     */
    public void asTransient() {
        edges.asTransient();
    }

    /**
     * Switch edges to persistent mode.
     * Required before forking or sharing the index.
     */
    public void asPersistent() {
        edges.asPersistent();
    }

    /**
     * Check if edges are in transient mode.
     */
    public boolean isTransient() {
        return edges.isTransient();
    }
}
