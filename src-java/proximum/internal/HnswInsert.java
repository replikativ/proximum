package proximum.internal;

import java.lang.foreign.MemorySegment;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Physical core executor for NUMA-aware parallelism.
 * Uses only physical cores (not hyperthreads) to avoid memory bandwidth contention
 * and improve cache locality on multi-socket systems.
 */
class PhysicalCoreExecutor {
    // Default to half of available processors (physical cores on SMT systems)
    // Can be overridden via system property: -Dproximum.physical_cores=N
    private static final int PHYSICAL_CORE_COUNT = Integer.getInteger(
        "proximum.physical_cores",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 2)
    );

    // Dedicated ForkJoinPool sized to physical cores
    private static final ForkJoinPool POOL = new ForkJoinPool(
        PHYSICAL_CORE_COUNT,
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null,  // No uncaught exception handler
        true   // asyncMode for better work-stealing with many small tasks
    );

    static ForkJoinPool pool() {
        return POOL;
    }

    static int parallelism() {
        return PHYSICAL_CORE_COUNT;
    }
}

/**
 * High-performance parallel HNSW insert using PersistentEdgeStore.
 *
 * This implementation uses:
 * - PersistentEdgeStore for chunked copy-on-write edge storage
 * - Per-node locks from PersistentEdgeStore for concurrent updates
 * - Diversity heuristic for neighbor selection (hnswlib style)
 * - SIMD distance computation
 * - ForkJoinPool for parallel batch insert
 *
 * <p><b>Internal API</b> - subject to change without notice.
 */
public final class HnswInsert {

    private static final Logger logger = LoggerFactory.getLogger(HnswInsert.class);

    // ThreadLocal arrays to avoid allocation per search
    private static final ThreadLocal<long[]> VISITED_BITSET = new ThreadLocal<>();
    private static final ThreadLocal<int[]> VISITED_MAX_WORD = new ThreadLocal<>();  // [0]=maxWordIdx touched
    private static final ThreadLocal<int[]> CAND_IDS = new ThreadLocal<>();
    private static final ThreadLocal<double[]> CAND_DISTS = new ThreadLocal<>();
    private static final ThreadLocal<int[]> RES_IDS = new ThreadLocal<>();
    private static final ThreadLocal<double[]> RES_DISTS = new ThreadLocal<>();
    private static final ThreadLocal<int[]> NEIGHBOR_BUFFER = new ThreadLocal<>();
    // Fixed-size buffers for in-flight candidate tracking (supports up to 128 concurrent inserts)
    private static final ThreadLocal<int[]> IN_FLIGHT_IDS = ThreadLocal.withInitial(() -> new int[128]);
    private static final ThreadLocal<double[]> IN_FLIGHT_DISTS = ThreadLocal.withInitial(() -> new double[128]);

    // Distance type constants (aliases for convenience)
    public static final int DISTANCE_EUCLIDEAN = Distance.EUCLIDEAN;
    public static final int DISTANCE_COSINE = Distance.COSINE;
    public static final int DISTANCE_INNER_PRODUCT = Distance.INNER_PRODUCT;

    // Concurrent candidate tracking: in-flight inserts that can be considered as neighbors
    // Maps (edges instance, nodeId) -> vector for nodes currently being inserted
    // This allows parallel inserts to see each other as potential neighbors
    // IMPORTANT: Scoped per PersistentEdgeStore to prevent cross-index contamination during compaction
    private static final ConcurrentHashMap<InFlightKey, float[]> IN_FLIGHT_INSERTS = new ConcurrentHashMap<>();

    // Composite key for in-flight inserts: (edges instance identity, nodeId)
    private record InFlightKey(int edgesId, int nodeId) {}

    // =========================================================================
    // Vector Normalization (delegates to Distance)
    // =========================================================================

    /**
     * Normalize a single vector in place (L2 normalization).
     * After normalization, ||vector|| = 1.
     *
     * @param vector The vector to normalize (modified in place)
     */
    public static void normalizeVector(float[] vector) {
        Distance.normalizeVector(vector);
    }

    /**
     * Normalize multiple vectors in place (L2 normalization).
     * Each vector will have ||vector|| = 1 after normalization.
     *
     * @param vectors Array of vectors to normalize (each modified in place)
     */
    public static void normalizeVectors(float[][] vectors) {
        Distance.normalizeVectors(vectors);
    }

    /**
     * Insert a single vector into the HNSW graph (Euclidean distance).
     */
    public static void insert(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] vector,
            int nodeId,
            int dim,
            int nodeLevel,
            int efConstruction) {
        insert(seg, edges, vector, nodeId, dim, nodeLevel, efConstruction, DISTANCE_EUCLIDEAN);
    }

    // Debug logging is controlled via SLF4J log level configuration
    // Set org.slf4j.simpleLogger.log.proximum.internal.HnswInsert=debug to enable

    /**
     * Insert a single vector into the HNSW graph.
     *
     * @param seg           MemorySegment for vector storage
     * @param edges         PersistentEdgeStore for graph edges
     * @param vector        The vector to insert (float[])
     * @param nodeId        Pre-allocated node ID for this vector
     * @param dim           Vector dimensionality
     * @param nodeLevel     Level assigned to this node
     * @param efConstruction Beam width during construction
     * @param distanceType  Distance metric (DISTANCE_EUCLIDEAN, DISTANCE_COSINE, DISTANCE_INNER_PRODUCT)
     */
    public static void insert(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] vector,
            int nodeId,
            int dim,
            int nodeLevel,
            int efConstruction,
            int distanceType) {

        // Switch to transient mode for mutation
        edges.asTransient();
        try {
            insertInternal(seg, edges, vector, nodeId, dim, nodeLevel, efConstruction, distanceType);
        } finally {
            // Seal back to persistent mode
            edges.asPersistent();
        }
    }

    /**
     * Internal insert implementation (assumes PES is already in transient mode).
     */
    private static void insertInternal(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] vector,
            int nodeId,
            int dim,
            int nodeLevel,
            int efConstruction,
            int distanceType) {

        int M = edges.getM();
        int M0 = edges.getM0();

        logger.debug("Insert starting: nodeId={} level={} entrypoint={} maxLevel={}",
            nodeId, nodeLevel, edges.getEntrypoint(), edges.getCurrentMaxLevel());

        // Try to become entry point if graph is empty
        if (edges.compareAndSetEntrypoint(-1, nodeId)) {
            edges.setCurrentMaxLevel(nodeLevel);
            logger.debug("Insert nodeId={} became entry point (graph was empty)", nodeId);
            return;
        }

        // Use edges identity hash as part of key to scope per-index
        int edgesId = System.identityHashCode(edges);
        InFlightKey inFlightKey = new InFlightKey(edgesId, nodeId);

        // Register this insert as in-flight so concurrent inserts can see us
        IN_FLIGHT_INSERTS.put(inFlightKey, vector);

        try {
            // Read current state
            int ep = edges.getEntrypoint();
            int currentMaxLevel = edges.getCurrentMaxLevel();

            // Greedy descent from entry point to insertion level
            for (int level = currentMaxLevel; level > nodeLevel; level--) {
                ep = searchLayerGreedy(seg, edges, vector, dim, ep, level, distanceType);
            }

            // For each layer from node level down to 0
            int insertLevel = Math.min(nodeLevel, currentMaxLevel);
            for (int level = insertLevel; level >= 0; level--) {
                int maxNeighbors = (level == 0) ? M0 : M;

                // Search for candidates at this level
                SearchResult result = searchLayer(seg, edges, vector, dim, ep, level, efConstruction, distanceType);

                logger.debug("Insert nodeId={} level={} searchResult.count={} candidates={}",
                    nodeId, level, result.count, Arrays.toString(Arrays.copyOf(result.ids, result.count)));

                // Apply diversity heuristic for neighbor selection
                // Also consider in-flight inserts as candidates (scoped to this index)
                int[] selected = selectNeighborsHeuristic(seg, dim, edgesId, nodeId, vector, result,
                        maxNeighbors, distanceType);

                logger.debug("Insert nodeId={} level={} selected={}", nodeId, level, Arrays.toString(selected));

                if (selected.length == 0) {
                    logger.warn("Insert nodeId={} level={} - no neighbors selected", nodeId, level);
                    continue;
                }

                // Add forward edges (new node -> neighbors)
                edges.setNeighbors(level, nodeId, selected);

                // Add reverse edges (neighbors -> new node) with pruning
                for (int neighborId : selected) {
                    addReverseEdge(seg, dim, edges, level, neighborId, nodeId, maxNeighbors, distanceType);
                }

                // Update entry point for next layer
                ep = selected[0];
            }

            // Update entry point and max level if this node is higher
            if (nodeLevel > currentMaxLevel) {
                while (true) {
                    int current = edges.getCurrentMaxLevel();
                    if (nodeLevel <= current) break;
                    if (edges.compareAndSetMaxLevel(current, nodeLevel)) {
                        edges.setEntrypoint(nodeId);
                        logger.debug("Insert nodeId={} became new entry point, maxLevel={}", nodeId, nodeLevel);
                        break;
                    }
                }
            }

            if (logger.isDebugEnabled()) {
                int[] neighbors0 = edges.getNeighbors(0, nodeId);
                logger.debug("Insert DONE nodeId={} layer0_neighbors={}", nodeId,
                    (neighbors0 == null ? "null" : Arrays.toString(neighbors0)));
            }
        } finally {
            // Always unregister when done
            IN_FLIGHT_INSERTS.remove(inFlightKey);
        }
    }

    /**
     * Parallel batch insert using ForkJoinPool (Euclidean distance).
     */
    public static void insertBatch(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[][] vectors,
            int[] nodeIds,
            int dim,
            int[] nodeLevels,
            int efConstruction,
            int parallelism) {
        insertBatch(seg, edges, vectors, nodeIds, dim, nodeLevels, efConstruction, parallelism, DISTANCE_EUCLIDEAN);
    }

    /**
     * Parallel batch insert using ForkJoinPool.
     *
     * @param distanceType Distance metric (DISTANCE_EUCLIDEAN, DISTANCE_COSINE, DISTANCE_INNER_PRODUCT)
     */
    public static void insertBatch(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[][] vectors,
            int[] nodeIds,
            int dim,
            int[] nodeLevels,
            int efConstruction,
            int parallelism,
            int distanceType) {

        // Switch to transient mode for bulk insert
        edges.asTransient();

        try {
            // Use physical core executor for NUMA-aware parallelism
            // Only uses physical cores (not hyperthreads) to avoid memory bandwidth contention
            // On a 64-thread dual-socket EPYC, this uses 32 threads instead of 63
            ForkJoinPool pool = PhysicalCoreExecutor.pool();
            pool.invoke(new InsertTask(seg, edges, vectors, nodeIds, dim, nodeLevels, efConstruction, 0, vectors.length, distanceType));
            // Don't shutdown - it's a shared static pool
        } finally {
            // Seal back to persistent mode
            edges.asPersistent();
        }
    }

    /**
     * ForkJoin task for parallel insert.
     */
    private static class InsertTask extends RecursiveAction {
        private static final int THRESHOLD = 100;

        private final MemorySegment seg;
        private final PersistentEdgeStore edges;
        private final float[][] vectors;
        private final int[] nodeIds;
        private final int dim;
        private final int[] nodeLevels;
        private final int efConstruction;
        private final int start;
        private final int end;
        private final int distanceType;

        InsertTask(MemorySegment seg, PersistentEdgeStore edges, float[][] vectors,
                   int[] nodeIds, int dim, int[] nodeLevels, int efConstruction,
                   int start, int end, int distanceType) {
            this.seg = seg;
            this.edges = edges;
            this.vectors = vectors;
            this.nodeIds = nodeIds;
            this.dim = dim;
            this.nodeLevels = nodeLevels;
            this.efConstruction = efConstruction;
            this.start = start;
            this.end = end;
            this.distanceType = distanceType;
        }

        @Override
        protected void compute() {
            int size = end - start;
            if (size <= THRESHOLD) {
                // Sequential insert for small batches
                // Use insertInternal since batch already manages transient mode
                for (int i = start; i < end; i++) {
                    insertInternal(seg, edges, vectors[i], nodeIds[i], dim, nodeLevels[i], efConstruction, distanceType);
                }
            } else {
                // Split and fork
                int mid = start + size / 2;
                InsertTask left = new InsertTask(seg, edges, vectors, nodeIds, dim, nodeLevels, efConstruction, start, mid, distanceType);
                InsertTask right = new InsertTask(seg, edges, vectors, nodeIds, dim, nodeLevels, efConstruction, mid, end, distanceType);
                invokeAll(left, right);
            }
        }
    }

    // =========================================================================
    // Search Layer
    // =========================================================================

    /**
     * Search result - sorted array of (id, distance) pairs.
     */
    static class SearchResult {
        int[] ids;
        double[] distances;
        int count;

        SearchResult(int capacity) {
            ids = new int[capacity];
            distances = new double[capacity];
            count = 0;
        }
    }

    /**
     * Search a layer for nearest neighbors using beam search.
     * Uses lock-free reads for scalability - tolerates slightly stale neighbor data.
     */
    private static SearchResult searchLayer(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int entryPoint,
            int layer,
            int ef,
            int distanceType) {

        // Reuse thread-local arrays to avoid allocation per search
        int maxSize = Math.min(ef * 4, 4096);

        // Visited bitset with lazy clearing - only clear words touched in previous search
        int requiredBitsetSize = (edges.getMaxNodes() + 63) / 64;
        long[] visited = VISITED_BITSET.get();
        int[] maxWordHolder = VISITED_MAX_WORD.get();
        if (visited == null || visited.length < requiredBitsetSize) {
            visited = new long[requiredBitsetSize];
            maxWordHolder = new int[1];
            VISITED_BITSET.set(visited);
            VISITED_MAX_WORD.set(maxWordHolder);
        } else {
            // Lazy clear: only clear words 0..maxWordTouched from previous search
            int prevMax = maxWordHolder[0];
            for (int w = 0; w <= prevMax && w < visited.length; w++) {
                visited[w] = 0L;
            }
        }
        maxWordHolder[0] = 0;  // Reset max word tracker for this search

        // Candidate arrays (min-heap)
        int[] candIds = CAND_IDS.get();
        double[] candDists = CAND_DISTS.get();
        if (candIds == null || candIds.length < maxSize) {
            candIds = new int[maxSize];
            candDists = new double[maxSize];
            CAND_IDS.set(candIds);
            CAND_DISTS.set(candDists);
        }
        int candCount = 0;

        // Result arrays (max-heap)
        int[] resIds = RES_IDS.get();
        double[] resDists = RES_DISTS.get();
        if (resIds == null || resIds.length < maxSize) {
            resIds = new int[maxSize];
            resDists = new double[maxSize];
            RES_IDS.set(resIds);
            RES_DISTS.set(resDists);
        }
        int resCount = 0;

        // Neighbor buffer (M0 is always >= M, so use M0 size)
        int[] neighborBuf = NEIGHBOR_BUFFER.get();
        int neighborBufSize = edges.getM0();
        if (neighborBuf == null || neighborBuf.length < neighborBufSize) {
            neighborBuf = new int[neighborBufSize];
            NEIGHBOR_BUFFER.set(neighborBuf);
        }

        // Initialize with entry point
        double epDist = Distance.compute(seg, entryPoint, dim, query, distanceType);
        heapPush(candIds, candDists, candCount++, entryPoint, epDist);
        maxHeapPush(resIds, resDists, resCount++, entryPoint, epDist);
        setVisitedTracked(visited, entryPoint, maxWordHolder);

        while (candCount > 0) {
            // Pop closest candidate - O(log n)
            int current = candIds[0];
            double currentDist = candDists[0];
            heapPop(candIds, candDists, candCount--);

            // Early termination if candidate is further than worst result
            double furthestResult = resDists[0];  // Max-heap root is furthest
            if (currentDist > furthestResult && resCount >= ef) {
                break;
            }

            // Expand neighbors - lock-free read into reusable buffer
            int neighborCount = edges.getNeighborsInto(layer, current, neighborBuf);
            if (neighborCount < 0) continue;

            for (int i = 0; i < neighborCount; i++) {
                int neighborId = neighborBuf[i];
                if (isVisited(visited, neighborId)) continue;
                setVisitedTracked(visited, neighborId, maxWordHolder);

                double neighborDist = Distance.compute(seg, neighborId, dim, query, distanceType);

                // Add to candidates if better than worst result or room in results
                if (neighborDist < furthestResult || resCount < ef) {
                    // Add to candidates - O(log n)
                    if (candCount < maxSize) {
                        heapPush(candIds, candDists, candCount++, neighborId, neighborDist);
                    }

                    // Add to results
                    if (resCount < ef) {
                        maxHeapPush(resIds, resDists, resCount++, neighborId, neighborDist);
                    } else if (neighborDist < furthestResult) {
                        // Replace furthest - replace root and sift down O(log n)
                        resIds[0] = neighborId;
                        resDists[0] = neighborDist;
                        maxHeapSiftDown(resIds, resDists, resCount);
                    }
                }
            }
        }

        // Sort results by distance
        sortByDistance(resIds, resDists, resCount);

        SearchResult result = new SearchResult(resCount);
        result.count = resCount;
        System.arraycopy(resIds, 0, result.ids, 0, resCount);
        System.arraycopy(resDists, 0, result.distances, 0, resCount);
        return result;
    }

    /**
     * Greedy search to find nearest node at a layer.
     */
    private static int searchLayerGreedy(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int entryPoint,
            int layer,
            int distanceType) {

        // Get or create neighbor buffer
        int[] neighborBuf = NEIGHBOR_BUFFER.get();
        int neighborBufSize = edges.getM0();
        if (neighborBuf == null || neighborBuf.length < neighborBufSize) {
            neighborBuf = new int[neighborBufSize];
            NEIGHBOR_BUFFER.set(neighborBuf);
        }

        int current = entryPoint;
        double currentDist = Distance.compute(seg, current, dim, query, distanceType);

        boolean improved = true;
        while (improved) {
            improved = false;
            // Lock-free read into reusable buffer
            int neighborCount = edges.getNeighborsInto(layer, current, neighborBuf);
            if (neighborCount < 0) break;

            for (int i = 0; i < neighborCount; i++) {
                int neighborId = neighborBuf[i];
                double neighborDist = Distance.compute(seg, neighborId, dim, query, distanceType);
                if (neighborDist < currentDist) {
                    current = neighborId;
                    currentDist = neighborDist;
                    improved = true;
                }
            }
        }

        return current;
    }

    // =========================================================================
    // Neighbor Selection (Diversity Heuristic)
    // =========================================================================

    /**
     * Select neighbors using diversity heuristic.
     * For each candidate (sorted by distance):
     * - Keep if NOT closer to any already-selected neighbor than to query
     *
     * @param nodeId The node being inserted - used to prevent self-loops during concurrent batch insert
     */
    private static int[] selectNeighborsHeuristic(
            MemorySegment seg,
            int dim,
            int nodeId,
            SearchResult candidates,
            int maxNeighbors,
            int distanceType) {

        if (candidates.count == 0) return new int[0];

        int[] selected = new int[Math.min(candidates.count, maxNeighbors)];
        int selectedCount = 0;

        for (int i = 0; i < candidates.count && selectedCount < maxNeighbors; i++) {
            int candidateId = candidates.ids[i];

            // CRITICAL: Prevent self-loops - during concurrent batch insert,
            // searchLayer might find ourselves if another thread already inserted us
            if (candidateId == nodeId) {
                continue;
            }

            double candidateDist = candidates.distances[i];

            boolean tooClose = false;
            for (int j = 0; j < selectedCount; j++) {
                double interDist = Distance.computeNodes(seg, dim, candidateId, selected[j], distanceType);
                if (interDist < candidateDist) {
                    tooClose = true;
                    break;
                }
            }

            if (!tooClose) {
                selected[selectedCount++] = candidateId;
            }
        }

        if (selectedCount < selected.length) {
            return Arrays.copyOf(selected, selectedCount);
        }
        return selected;
    }

    /**
     * Select neighbors using diversity heuristic with in-flight insert candidates.
     * This allows concurrent inserts to see each other as potential neighbors,
     * improving graph quality during parallel construction.
     *
     * @param seg           MemorySegment for vector storage
     * @param dim           Vector dimensionality
     * @param edgesId       Identity hash of PersistentEdgeStore - used to scope in-flight lookups
     * @param nodeId        The node being inserted
     * @param vector        The vector being inserted
     * @param candidates    Search result candidates from graph traversal
     * @param maxNeighbors  Maximum neighbors to select
     * @param distanceType  Distance metric type
     */
    private static int[] selectNeighborsHeuristic(
            MemorySegment seg,
            int dim,
            int edgesId,
            int nodeId,
            float[] vector,
            SearchResult candidates,
            int maxNeighbors,
            int distanceType) {

        // Compute distances to in-flight inserts (excluding ourselves, same index only)
        // Fast path: no other in-flight inserts, just use candidates directly
        if (IN_FLIGHT_INSERTS.size() <= 1) {
            // Only ourselves or empty - skip merging entirely
            // Still pass nodeId to prevent self-loops during concurrent batch insert
            return selectNeighborsHeuristic(seg, dim, nodeId, candidates, maxNeighbors, distanceType);
        }

        // Reuse ThreadLocal buffers to avoid allocation
        int[] inFlightIds = IN_FLIGHT_IDS.get();
        double[] inFlightDists = IN_FLIGHT_DISTS.get();
        int inFlightCount = 0;

        for (var entry : IN_FLIGHT_INSERTS.entrySet()) {
            if (inFlightCount >= 128) break;  // Safety limit
            InFlightKey key = entry.getKey();
            // CRITICAL: Only consider in-flight inserts from the SAME index
            // During compaction, SOURCE and NEW indices have overlapping nodeIds
            if (key.edgesId() != edgesId) continue;
            if (key.nodeId() == nodeId) continue;  // Skip ourselves
            float[] otherVector = entry.getValue();

            double dist = Distance.computeVectors(vector, otherVector, distanceType);
            inFlightIds[inFlightCount] = key.nodeId();
            inFlightDists[inFlightCount] = dist;
            inFlightCount++;
        }

        // Merge candidates: graph traversal results + in-flight inserts
        int totalCandidates = candidates.count + inFlightCount;
        if (totalCandidates == 0) return new int[0];

        int[] mergedIds = new int[totalCandidates];
        double[] mergedDists = new double[totalCandidates];

        // Copy graph traversal candidates
        System.arraycopy(candidates.ids, 0, mergedIds, 0, candidates.count);
        System.arraycopy(candidates.distances, 0, mergedDists, 0, candidates.count);

        // Add in-flight candidates
        System.arraycopy(inFlightIds, 0, mergedIds, candidates.count, inFlightCount);
        System.arraycopy(inFlightDists, 0, mergedDists, candidates.count, inFlightCount);

        // Sort all by distance
        sortByDistance(mergedIds, mergedDists, totalCandidates);

        // Apply diversity heuristic
        int[] selected = new int[Math.min(totalCandidates, maxNeighbors)];
        int selectedCount = 0;

        for (int i = 0; i < totalCandidates && selectedCount < maxNeighbors; i++) {
            int candidateId = mergedIds[i];

            // CRITICAL: Prevent self-loops - a node cannot be its own neighbor
            if (candidateId == nodeId) {
                continue;
            }

            double candidateDist = mergedDists[i];

            boolean tooClose = false;
            for (int j = 0; j < selectedCount; j++) {
                // Need to handle distance between any two nodes (could be in-flight or in graph)
                double interDist = distanceAnyNodes(seg, dim, edgesId, candidateId, selected[j], distanceType);
                if (interDist < candidateDist) {
                    tooClose = true;
                    break;
                }
            }

            if (!tooClose) {
                selected[selectedCount++] = candidateId;
            }
        }

        if (selectedCount < selected.length) {
            return Arrays.copyOf(selected, selectedCount);
        }
        return selected;
    }

    /**
     * Add reverse edge with pruning.
     */
    private static void addReverseEdge(
            MemorySegment seg,
            int dim,
            PersistentEdgeStore edges,
            int layer,
            int neighborId,
            int newNodeId,
            int maxNeighbors,
            int distanceType) {

        // CRITICAL: Prevent self-loops - a node cannot be its own neighbor
        if (neighborId == newNodeId) {
            return;
        }

        int[] current = edges.getNeighbors(layer, neighborId);
        int currentCount = (current == null) ? 0 : current.length;

        // Check if already present
        for (int i = 0; i < currentCount; i++) {
            if (current[i] == newNodeId) return;
        }

        if (currentCount < maxNeighbors) {
            // Just append
            int[] newNeighbors = new int[currentCount + 1];
            if (current != null) {
                System.arraycopy(current, 0, newNeighbors, 0, currentCount);
            }
            newNeighbors[currentCount] = newNodeId;
            edges.setNeighbors(layer, neighborId, newNeighbors);
        } else {
            // Need to prune - add new and select best M
            int[] newNeighbors = pruneWithHeuristic(seg, dim, neighborId, current, newNodeId, maxNeighbors, distanceType);
            edges.setNeighbors(layer, neighborId, newNeighbors);
        }
    }

    /**
     * Prune neighbors using diversity heuristic.
     */
    private static int[] pruneWithHeuristic(
            MemorySegment seg,
            int dim,
            int refNodeId,
            int[] currentNeighbors,
            int newNodeId,
            int maxNeighbors,
            int distanceType) {

        int n = currentNeighbors.length + 1;
        int[] candidates = new int[n];
        double[] distances = new double[n];

        for (int i = 0; i < currentNeighbors.length; i++) {
            candidates[i] = currentNeighbors[i];
            distances[i] = Distance.computeNodes(seg, dim, refNodeId, currentNeighbors[i], distanceType);
        }
        candidates[n - 1] = newNodeId;
        distances[n - 1] = Distance.computeNodes(seg, dim, refNodeId, newNodeId, distanceType);

        sortByDistance(candidates, distances, n);

        int[] selected = new int[maxNeighbors];
        int selectedCount = 0;

        for (int i = 0; i < n && selectedCount < maxNeighbors; i++) {
            int candidateId = candidates[i];

            // Prevent self-loops - a node cannot be its own neighbor
            if (candidateId == refNodeId) {
                continue;
            }

            double candidateDist = distances[i];

            boolean tooClose = false;
            for (int j = 0; j < selectedCount; j++) {
                double interDist = Distance.computeNodes(seg, dim, candidateId, selected[j], distanceType);
                if (interDist < candidateDist) {
                    tooClose = true;
                    break;
                }
            }

            if (!tooClose) {
                selected[selectedCount++] = candidateId;
            }
        }

        if (selectedCount < maxNeighbors) {
            return Arrays.copyOf(selected, selectedCount);
        }
        return selected;
    }

    // =========================================================================
    // Distance Computation (delegates to Distance utility)
    // =========================================================================

    /**
     * Distance between any two nodes - handles in-flight vectors (not yet in segment).
     * If a node is in IN_FLIGHT_INSERTS, uses its vector; otherwise reads from segment.
     *
     * @param edgesId Identity hash of PersistentEdgeStore - used to scope in-flight lookups
     */
    private static double distanceAnyNodes(MemorySegment seg, int dim, int edgesId, int nodeA, int nodeB, int distanceType) {
        InFlightKey keyA = new InFlightKey(edgesId, nodeA);
        InFlightKey keyB = new InFlightKey(edgesId, nodeB);
        float[] vecA = IN_FLIGHT_INSERTS.get(keyA);
        float[] vecB = IN_FLIGHT_INSERTS.get(keyB);

        if (vecA != null && vecB != null) {
            // Both in-flight
            return Distance.computeVectors(vecA, vecB, distanceType);
        } else if (vecA != null) {
            // A in-flight, B in segment
            return Distance.compute(seg, nodeB, dim, vecA, distanceType);
        } else if (vecB != null) {
            // A in segment, B in-flight
            return Distance.compute(seg, nodeA, dim, vecB, distanceType);
        } else {
            // Both in segment
            return Distance.computeNodes(seg, dim, nodeA, nodeB, distanceType);
        }
    }

    // =========================================================================
    // Utility
    // =========================================================================

    private static void sortByDistance(int[] ids, double[] distances, int count) {
        // Insertion sort for small arrays
        for (int i = 1; i < count; i++) {
            int id = ids[i];
            double dist = distances[i];
            int j = i - 1;
            while (j >= 0 && distances[j] > dist) {
                ids[j + 1] = ids[j];
                distances[j + 1] = distances[j];
                j--;
            }
            ids[j + 1] = id;
            distances[j + 1] = dist;
        }
    }

    // =========================================================================
    // Binary Min-Heap for candidates (O(log n) operations)
    // =========================================================================

    private static void heapPush(int[] ids, double[] dists, int count, int id, double dist) {
        ids[count] = id;
        dists[count] = dist;
        // Sift up
        int i = count;
        while (i > 0) {
            int parent = (i - 1) >> 1;
            if (dists[parent] <= dists[i]) break;
            int tmpId = ids[parent]; double tmpDist = dists[parent];
            ids[parent] = ids[i]; dists[parent] = dists[i];
            ids[i] = tmpId; dists[i] = tmpDist;
            i = parent;
        }
    }

    private static void heapPop(int[] ids, double[] dists, int count) {
        // Move last to root
        ids[0] = ids[count - 1];
        dists[0] = dists[count - 1];
        // Sift down
        int i = 0;
        int size = count - 1;
        while (true) {
            int left = (i << 1) + 1;
            int right = left + 1;
            int smallest = i;
            if (left < size && dists[left] < dists[smallest]) smallest = left;
            if (right < size && dists[right] < dists[smallest]) smallest = right;
            if (smallest == i) break;
            int tmpId = ids[i]; double tmpDist = dists[i];
            ids[i] = ids[smallest]; dists[i] = dists[smallest];
            ids[smallest] = tmpId; dists[smallest] = tmpDist;
            i = smallest;
        }
    }

    // =========================================================================
    // Binary Max-Heap for results (O(log n) operations)
    // =========================================================================

    private static void maxHeapPush(int[] ids, double[] dists, int count, int id, double dist) {
        ids[count] = id;
        dists[count] = dist;
        // Sift up
        int i = count;
        while (i > 0) {
            int parent = (i - 1) >> 1;
            if (dists[parent] >= dists[i]) break;
            int tmpId = ids[parent]; double tmpDist = dists[parent];
            ids[parent] = ids[i]; dists[parent] = dists[i];
            ids[i] = tmpId; dists[i] = tmpDist;
            i = parent;
        }
    }

    private static void maxHeapPop(int[] ids, double[] dists, int count) {
        ids[0] = ids[count - 1];
        dists[0] = dists[count - 1];
        maxHeapSiftDown(ids, dists, count - 1);
    }

    /**
     * Sift down from root to restore max-heap property.
     * Used for replacing root element without changing heap size.
     */
    private static void maxHeapSiftDown(int[] ids, double[] dists, int size) {
        int i = 0;
        while (true) {
            int left = (i << 1) + 1;
            int right = left + 1;
            int largest = i;
            if (left < size && dists[left] > dists[largest]) largest = left;
            if (right < size && dists[right] > dists[largest]) largest = right;
            if (largest == i) break;
            int tmpId = ids[i]; double tmpDist = dists[i];
            ids[i] = ids[largest]; dists[i] = dists[largest];
            ids[largest] = tmpId; dists[largest] = tmpDist;
            i = largest;
        }
    }

    private static boolean isVisited(long[] visited, int nodeId) {
        int wordIdx = nodeId >> 6;
        long bit = 1L << (nodeId & 63);
        return (visited[wordIdx] & bit) != 0;
    }

    private static void setVisited(long[] visited, int nodeId) {
        int wordIdx = nodeId >> 6;
        long bit = 1L << (nodeId & 63);
        visited[wordIdx] |= bit;
    }

    /**
     * Set visited and track max word index for lazy clearing.
     */
    private static void setVisitedTracked(long[] visited, int nodeId, int[] maxWordHolder) {
        int wordIdx = nodeId >> 6;
        long bit = 1L << (nodeId & 63);
        visited[wordIdx] |= bit;
        if (wordIdx > maxWordHolder[0]) {
            maxWordHolder[0] = wordIdx;
        }
    }

    /**
     * Generate random level using exponential distribution.
     */
    public static int randomLevel(double ml, int maxLevel) {
        int level = (int) Math.floor(-Math.log(ThreadLocalRandom.current().nextDouble()) * ml);
        return (maxLevel >= 0) ? Math.min(level, maxLevel) : level;
    }

    // =========================================================================
    // Delete with Graph Repair
    // =========================================================================

    /**
     * Delete a node and repair graph connectivity using diversity heuristic (Euclidean distance).
     */
    public static void delete(
            MemorySegment seg,
            PersistentEdgeStore edges,
            int nodeId,
            int dim,
            int M,
            int M0) {
        delete(seg, edges, nodeId, dim, M, M0, DISTANCE_EUCLIDEAN);
    }

    /**
     * Delete a node and repair graph connectivity using diversity heuristic.
     *
     * For each neighbor of the deleted node:
     * 1. Remove deleted from their neighbor list
     * 2. Collect candidates from: their neighbors + deleted's neighbors
     * 3. Use diversity heuristic to select best M neighbors
     *
     * This is the standard HNSW delete approach - same heuristic used for insert.
     *
     * @param seg          MemorySegment for vector storage
     * @param edges        PersistentEdgeStore
     * @param nodeId       Node ID to delete
     * @param dim          Vector dimensionality
     * @param M            Max neighbors for upper layers
     * @param M0           Max neighbors for layer 0
     * @param distanceType Distance metric (DISTANCE_EUCLIDEAN, DISTANCE_COSINE, DISTANCE_INNER_PRODUCT)
     */
    public static void delete(
            MemorySegment seg,
            PersistentEdgeStore edges,
            int nodeId,
            int dim,
            int M,
            int M0,
            int distanceType) {

        // Switch to transient mode for mutation
        edges.asTransient();
        try {
            deleteInternal(seg, edges, nodeId, dim, M, M0, distanceType);
        } finally {
            // Seal back to persistent mode
            edges.asPersistent();
        }
    }

    /**
     * Internal delete implementation (assumes PES is already in transient mode).
     */
    private static void deleteInternal(
            MemorySegment seg,
            PersistentEdgeStore edges,
            int nodeId,
            int dim,
            int M,
            int M0,
            int distanceType) {

        int currentMaxLevel = edges.getCurrentMaxLevel();
        int entrypoint = edges.getEntrypoint();

        // Handle entrypoint deletion - find replacement
        if (nodeId == entrypoint) {
            int newEntry = findNewEntrypoint(edges, nodeId, currentMaxLevel);
            if (newEntry >= 0) {
                edges.setEntrypoint(newEntry);
                // Note: max level may need recalculation but we keep it for now
            } else {
                // Graph is now empty
                edges.setEntrypoint(-1);
                edges.setCurrentMaxLevel(0);
                edges.markDeleted(nodeId);
                return;
            }
        }

        // Process each layer
        for (int layer = 0; layer <= currentMaxLevel; layer++) {
            int[] deletedNeighbors = edges.getNeighbors(layer, nodeId);
            if (deletedNeighbors == null || deletedNeighbors.length == 0) {
                continue;
            }

            int maxNeighbors = (layer == 0) ? M0 : M;

            // For each neighbor of deleted node
            for (int neighborId : deletedNeighbors) {
                repairNeighbor(seg, edges, layer, neighborId, nodeId, deletedNeighbors, dim, maxNeighbors, distanceType);
            }

            // Clear deleted node's neighbors
            edges.setNeighbors(layer, nodeId, new int[0]);
        }

        // Mark node as deleted for filtering in search
        edges.markDeleted(nodeId);
    }

    /**
     * Repair a single neighbor after deletion.
     *
     * CRITICAL: This preserves existing connections to maintain graph connectivity.
     * Only new candidates (from deleted node's neighbors) go through diversity selection.
     * This prevents the diversity heuristic from dropping valid connections to nodes
     * that were inserted after initial graph construction.
     */
    private static void repairNeighbor(
            MemorySegment seg,
            PersistentEdgeStore edges,
            int layer,
            int neighborId,
            int deletedId,
            int[] deletedNeighbors,
            int dim,
            int maxNeighbors,
            int distanceType) {

        int[] theirNeighbors = edges.getNeighbors(layer, neighborId);
        if (theirNeighbors == null) return;

        // Remove deleted node from their list - these are preserved connections
        int[] existing = removeFromArray(theirNeighbors, deletedId);

        // If existing connections fill maxNeighbors, no room for new connections
        if (existing.length >= maxNeighbors) {
            edges.setNeighbors(layer, neighborId, existing);
            return;
        }

        // Collect NEW candidates from deleted's neighbors (not already connected)
        // Only these go through diversity selection
        int maxNewCandidates = deletedNeighbors.length;
        int[] newCandidates = new int[maxNewCandidates];
        double[] newDistances = new double[maxNewCandidates];
        int newCandCount = 0;

        for (int id : deletedNeighbors) {
            if (id != neighborId && id != deletedId && !contains(existing, id)) {
                newCandidates[newCandCount] = id;
                newDistances[newCandCount] = Distance.computeNodes(seg, dim, neighborId, id, distanceType);
                newCandCount++;
            }
        }

        // If no new candidates, just keep existing connections
        if (newCandCount == 0) {
            edges.setNeighbors(layer, neighborId, existing);
            return;
        }

        // Sort new candidates by distance
        sortByDistance(newCandidates, newDistances, newCandCount);

        // Build result: start with existing connections (preserved)
        int roomForNew = maxNeighbors - existing.length;
        int[] result = new int[Math.min(existing.length + newCandCount, maxNeighbors)];
        System.arraycopy(existing, 0, result, 0, existing.length);
        int resultCount = existing.length;

        // Select new candidates using diversity heuristic
        // The "selected" set for diversity check includes existing connections
        for (int i = 0; i < newCandCount && resultCount < maxNeighbors; i++) {
            int candidateId = newCandidates[i];

            // Prevent self-loops
            if (candidateId == neighborId) {
                continue;
            }

            double candidateDist = newDistances[i];

            // Check diversity against ALL current neighbors (existing + already added new)
            boolean tooClose = false;
            for (int j = 0; j < resultCount; j++) {
                double interDist = Distance.computeNodes(seg, dim, candidateId, result[j], distanceType);
                if (interDist < candidateDist) {
                    tooClose = true;
                    break;
                }
            }

            if (!tooClose) {
                result[resultCount++] = candidateId;
            }
        }

        if (resultCount < result.length) {
            result = Arrays.copyOf(result, resultCount);
        }

        edges.setNeighbors(layer, neighborId, result);
    }

    /**
     * Find new entrypoint when current is deleted.
     * Skips deleted nodes (those with no neighbors).
     */
    private static int findNewEntrypoint(PersistentEdgeStore edges, int deletedId, int maxLevel) {
        // Start from highest level, find first non-deleted neighbor
        for (int level = maxLevel; level >= 0; level--) {
            int[] neighbors = edges.getNeighbors(level, deletedId);
            if (neighbors != null) {
                for (int neighbor : neighbors) {
                    // Check if neighbor is not deleted (has neighbors)
                    if (!edges.isDeleted(neighbor)) {
                        return neighbor;
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Remove an element from an int array.
     */
    private static int[] removeFromArray(int[] arr, int toRemove) {
        int count = 0;
        for (int v : arr) {
            if (v != toRemove) count++;
        }
        if (count == arr.length) return arr;

        int[] result = new int[count];
        int idx = 0;
        for (int v : arr) {
            if (v != toRemove) {
                result[idx++] = v;
            }
        }
        return result;
    }

    /**
     * Check if array contains value.
     */
    private static boolean contains(int[] arr, int value) {
        for (int v : arr) {
            if (v == value) return true;
        }
        return false;
    }
}
