package proximum.internal;

import java.lang.foreign.MemorySegment;
import org.replikativ.proximum.SearchOptions;

/**
 * High-performance HNSW search using PersistentEdgeStore.
 *
 * Lock-free search with SIMD distance computation.
 *
 * <p><b>Internal API</b> - subject to change without notice.
 */
public final class HnswSearch {

    // ThreadLocal visited bitset to avoid allocation per search
    private static final ThreadLocal<long[]> VISITED_BITSET = new ThreadLocal<>();
    private static final ThreadLocal<int[]> VISITED_MAX_WORD = new ThreadLocal<>();  // [0]=maxWordIdx touched

    // Distance type constants (aliases for convenience)
    public static final int DISTANCE_EUCLIDEAN = Distance.EUCLIDEAN;
    public static final int DISTANCE_COSINE = Distance.COSINE;
    public static final int DISTANCE_INNER_PRODUCT = Distance.INNER_PRODUCT;

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
     * Search for k nearest neighbors (Euclidean distance).
     *
     * @param seg    MemorySegment for vector storage
     * @param edges  PersistentEdgeStore for graph edges
     * @param query  Query vector
     * @param dim    Vector dimensionality
     * @param k      Number of neighbors to return
     * @param ef     Beam width (exploration factor)
     * @return Array of [id, distance, id, distance, ...] sorted by distance
     */
    public static double[] search(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int k,
            int ef) {
        return search(seg, edges, query, dim, k, ef, Distance.EUCLIDEAN);
    }

    /**
     * Search for k nearest neighbors with configurable distance metric.
     *
     * @param seg          MemorySegment for vector storage
     * @param edges        PersistentEdgeStore for graph edges
     * @param query        Query vector
     * @param dim          Vector dimensionality
     * @param k            Number of neighbors to return
     * @param ef           Beam width (exploration factor)
     * @param distanceType Distance metric (Distance.EUCLIDEAN, Distance.COSINE, Distance.INNER_PRODUCT)
     * @return Array of [id, distance, id, distance, ...] sorted by distance
     */
    public static double[] search(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int k,
            int ef,
            int distanceType) {

        int entrypoint = edges.getEntrypoint();
        if (entrypoint < 0) {
            return new double[0];  // Empty index
        }

        int currentMaxLevel = edges.getCurrentMaxLevel();
        int ep = entrypoint;

        // Greedy descent from top level to level 1
        for (int level = currentMaxLevel; level > 0; level--) {
            ep = searchLayerGreedy(seg, edges, query, dim, ep, level, distanceType);
        }

        // Beam search at level 0
        return searchLayerBeam(seg, edges, query, dim, ep, 0, ef, k, distanceType,
                0, 0, 0, 0);
    }

    /**
     * Search for k nearest neighbors with search options for early termination.
     */
    public static double[] search(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int k,
            int ef,
            int distanceType,
            SearchOptions options) {

        int entrypoint = edges.getEntrypoint();
        if (entrypoint < 0) {
            return new double[0];
        }

        int currentMaxLevel = edges.getCurrentMaxLevel();
        int ep = entrypoint;

        for (int level = currentMaxLevel; level > 0; level--) {
            ep = searchLayerGreedy(seg, edges, query, dim, ep, level, distanceType);
        }

        // Extract early termination parameters
        long timeoutNanos = options != null ? options.getTimeoutNanos() : 0;
        int maxDistComps = options != null ? options.getMaxDistanceComputations() : 0;
        double patienceSat = options != null ? options.getPatienceSaturation() : 0;
        int patience = (options != null && patienceSat > 0) ? options.getEffectivePatience(k) : 0;

        return searchLayerBeam(seg, edges, query, dim, ep, 0, ef, k, distanceType,
                timeoutNanos, maxDistComps, patienceSat, patience);
    }

    /**
     * Search for k nearest neighbors with filtering.
     *
     * Uses hnswlib-style filtering: traverses all nodes for graph exploration,
     * but only adds allowed nodes to results. This ensures good recall while
     * respecting the filter.
     *
     * @param seg        MemorySegment for vector storage
     * @param edges      PersistentEdgeStore for graph edges
     * @param query      Query vector
     * @param dim        Vector dimensionality
     * @param k          Number of neighbors to return
     * @param ef         Beam width (exploration factor)
     * @param allowedIds Bitset of allowed node IDs (null = allow all)
     * @return Array of [id, distance, id, distance, ...] sorted by distance
     */
    public static double[] searchFiltered(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int k,
            int ef,
            ArrayBitSet allowedIds) {
        return searchFiltered(seg, edges, query, dim, k, ef, allowedIds, Distance.EUCLIDEAN);
    }

    /**
     * Search for k nearest neighbors with filtering and configurable distance.
     */
    public static double[] searchFiltered(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int k,
            int ef,
            ArrayBitSet allowedIds,
            int distanceType) {

        if (allowedIds == null) {
            return search(seg, edges, query, dim, k, ef, distanceType);
        }

        int entrypoint = edges.getEntrypoint();
        if (entrypoint < 0) {
            return new double[0];  // Empty index
        }

        int currentMaxLevel = edges.getCurrentMaxLevel();
        int ep = entrypoint;

        // Greedy descent from top level to level 1
        for (int level = currentMaxLevel; level > 0; level--) {
            ep = searchLayerGreedy(seg, edges, query, dim, ep, level, distanceType);
        }

        // Beam search at level 0 with filtering
        return searchLayerBeamFiltered(seg, edges, query, dim, ep, 0, ef, k, allowedIds, distanceType,
                0, 0, 0, 0);
    }

    /**
     * Search for k nearest neighbors with filtering and search options.
     */
    public static double[] searchFiltered(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int k,
            int ef,
            ArrayBitSet allowedIds,
            int distanceType,
            SearchOptions options) {

        if (allowedIds == null) {
            return search(seg, edges, query, dim, k, ef, distanceType, options);
        }

        int entrypoint = edges.getEntrypoint();
        if (entrypoint < 0) {
            return new double[0];
        }

        int currentMaxLevel = edges.getCurrentMaxLevel();
        int ep = entrypoint;

        for (int level = currentMaxLevel; level > 0; level--) {
            ep = searchLayerGreedy(seg, edges, query, dim, ep, level, distanceType);
        }

        // Extract early termination parameters
        long timeoutNanos = options != null ? options.getTimeoutNanos() : 0;
        int maxDistComps = options != null ? options.getMaxDistanceComputations() : 0;
        double patienceSat = options != null ? options.getPatienceSaturation() : 0;
        int patience = (options != null && patienceSat > 0) ? options.getEffectivePatience(k) : 0;

        return searchLayerBeamFiltered(seg, edges, query, dim, ep, 0, ef, k, allowedIds, distanceType,
                timeoutNanos, maxDistComps, patienceSat, patience);
    }

    /**
     * Greedy search at a layer - find single nearest node.
     * Uses zero-copy neighbor access for layer 0.
     */
    private static int searchLayerGreedy(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int entryPoint,
            int layer,
            int distanceType) {

        int current = entryPoint;
        double currentDist = Distance.compute(seg, current, dim, query, distanceType);

        boolean improved = true;
        while (improved) {
            improved = false;

            // Zero-copy neighbor iteration for layer 0
            if (layer == 0) {
                int chunkIdx = current >> PersistentEdgeStore.CHUNK_SHIFT;
                int[] chunk = edges.getLayer0Chunk(chunkIdx);
                if (chunk == null) break;

                int base = edges.getNodeSlotOffset(current);
                int count = chunk[base];

                for (int i = 0; i < count; i++) {
                    int neighborId = chunk[base + 1 + i];
                    double neighborDist = Distance.compute(seg, neighborId, dim, query, distanceType);
                    if (neighborDist < currentDist) {
                        current = neighborId;
                        currentDist = neighborDist;
                        improved = true;
                    }
                }
            } else {
                int[] neighbors = edges.getNeighbors(layer, current);
                if (neighbors == null) break;

                for (int neighborId : neighbors) {
                    double neighborDist = Distance.compute(seg, neighborId, dim, query, distanceType);
                    if (neighborDist < currentDist) {
                        current = neighborId;
                        currentDist = neighborDist;
                        improved = true;
                    }
                }
            }
        }

        return current;
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
            int parent = (i - 1) / 2;
            if (dists[parent] <= dists[i]) break;
            // Swap
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
        int n = count - 1;
        while (true) {
            int left = 2 * i + 1;
            int right = 2 * i + 2;
            int smallest = i;
            if (left < n && dists[left] < dists[smallest]) smallest = left;
            if (right < n && dists[right] < dists[smallest]) smallest = right;
            if (smallest == i) break;
            // Swap
            int tmpId = ids[smallest]; double tmpDist = dists[smallest];
            ids[smallest] = ids[i]; dists[smallest] = dists[i];
            ids[i] = tmpId; dists[i] = tmpDist;
            i = smallest;
        }
    }

    // Max-heap operations (for results - we want to evict furthest)
    private static void maxHeapPush(int[] ids, double[] dists, int count, int id, double dist) {
        ids[count] = id;
        dists[count] = dist;
        int i = count;
        while (i > 0) {
            int parent = (i - 1) / 2;
            if (dists[parent] >= dists[i]) break;
            int tmpId = ids[parent]; double tmpDist = dists[parent];
            ids[parent] = ids[i]; dists[parent] = dists[i];
            ids[i] = tmpId; dists[i] = tmpDist;
            i = parent;
        }
    }

    private static void maxHeapReplace(int[] ids, double[] dists, int count, int id, double dist) {
        ids[0] = id;
        dists[0] = dist;
        int i = 0;
        while (true) {
            int left = 2 * i + 1;
            int right = 2 * i + 2;
            int largest = i;
            if (left < count && dists[left] > dists[largest]) largest = left;
            if (right < count && dists[right] > dists[largest]) largest = right;
            if (largest == i) break;
            int tmpId = ids[largest]; double tmpDist = dists[largest];
            ids[largest] = ids[i]; dists[largest] = dists[i];
            ids[i] = tmpId; dists[i] = tmpDist;
            i = largest;
        }
    }

    /**
     * Beam search at a layer - find top-k nearest nodes.
     * Uses binary heaps for O(log n) candidate/result management.
     */
    private static double[] searchLayerBeam(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int entryPoint,
            int layer,
            int ef,
            int k,
            int distanceType,
            long timeoutNanos,
            int maxDistComps,
            double patienceSat,
            int patience) {

        int maxSize = Math.max(ef, k) * 4;

        // Early termination state
        long startTime = timeoutNanos > 0 ? System.nanoTime() : 0;
        int distCompCount = 0;
        int saturatedCount = 0;

        // Reuse thread-local visited bitset with lazy clearing
        int requiredSize = (edges.getMaxNodes() + 63) / 64;
        long[] visited = VISITED_BITSET.get();
        int[] maxWordHolder = VISITED_MAX_WORD.get();
        if (visited == null || visited.length < requiredSize) {
            visited = new long[requiredSize];
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

        // Candidates: min-heap by distance (closest first)
        int[] candIds = new int[maxSize];
        double[] candDists = new double[maxSize];
        int candCount = 0;

        // Results: max-heap by distance (furthest first for pruning)
        int[] resIds = new int[maxSize];
        double[] resDists = new double[maxSize];
        int resCount = 0;

        // Initialize with entry point (only add to results if not deleted)
        double epDist = Distance.compute(seg, entryPoint, dim, query, distanceType);
        distCompCount++;
        heapPush(candIds, candDists, candCount++, entryPoint, epDist);

        // Cache furthestResult - only update when heap changes
        double furthestResult = Double.MAX_VALUE;
        if (!edges.isDeleted(entryPoint)) {
            maxHeapPush(resIds, resDists, resCount++, entryPoint, epDist);
            furthestResult = epDist;  // First result sets the bound
        }
        setVisitedTracked(visited, entryPoint, maxWordHolder);

        while (candCount > 0) {
            // === Early termination checks ===

            // 1. Timeout check (every iteration for hard latency bounds)
            if (timeoutNanos > 0 && (System.nanoTime() - startTime) > timeoutNanos) {
                break;
            }

            // 2. Distance budget check
            if (maxDistComps > 0 && distCompCount >= maxDistComps) {
                break;
            }

            // Pop closest candidate (O(log n))
            int current = candIds[0];
            double currentDist = candDists[0];
            heapPop(candIds, candDists, candCount--);

            // Quality-based early termination
            if (currentDist > furthestResult && resCount >= ef) {
                break;
            }

            // Track displacements for patience (paper's metric)
            int displacementsThisIteration = 0;

            // Expand neighbors - zero-copy access for layer 0
            int chunkIdx = current >> PersistentEdgeStore.CHUNK_SHIFT;
            int[] chunk = edges.getLayer0Chunk(chunkIdx);
            if (chunk == null) continue;

            int base = edges.getNodeSlotOffset(current);
            int neighborCount = chunk[base];

            for (int i = 0; i < neighborCount; i++) {
                int neighborId = chunk[base + 1 + i];
                if (isVisited(visited, neighborId)) continue;
                setVisitedTracked(visited, neighborId, maxWordHolder);

                double neighborDist = Distance.compute(seg, neighborId, dim, query, distanceType);
                distCompCount++;

                // Always add to candidates for graph traversal (even deleted nodes help navigation)
                if (neighborDist < furthestResult || resCount < ef) {
                    if (candCount < maxSize) {
                        heapPush(candIds, candDists, candCount++, neighborId, neighborDist);
                    }
                }

                // Only add non-deleted nodes to results
                if (!edges.isDeleted(neighborId)) {
                    if (resCount < ef) {
                        maxHeapPush(resIds, resDists, resCount++, neighborId, neighborDist);
                        // Update cached furthest when heap grows
                        furthestResult = resDists[0];
                    } else if (neighborDist < furthestResult) {
                        // Replace furthest (O(log n))
                        maxHeapReplace(resIds, resDists, resCount, neighborId, neighborDist);
                        displacementsThisIteration++;
                        // Update cached furthest after replacement
                        furthestResult = resDists[0];
                    }
                }
            }

            // 3. Patience check (only after we have k results)
            if (patience > 0 && resCount >= k) {
                // Paper's metric: overlap = (k - displacements) / k
                double overlap = (double)(k - displacementsThisIteration) / k;
                if (overlap >= patienceSat) {
                    saturatedCount++;
                    if (saturatedCount > patience) {
                        break;
                    }
                } else {
                    saturatedCount = 0;
                }
            }
        }

        // Sort results by distance and return top k
        sortByDistance(resIds, resDists, resCount);

        int resultCount = Math.min(k, resCount);
        double[] result = new double[resultCount * 2];
        for (int i = 0; i < resultCount; i++) {
            result[i * 2] = resIds[i];
            result[i * 2 + 1] = resDists[i];
        }
        return result;
    }

    /**
     * Beam search with filtering - traverses all nodes but only adds allowed to results.
     *
     * Key insight from hnswlib: we still traverse through filtered-out nodes
     * (add to candidates) to maintain graph connectivity, but only add
     * allowed nodes to the result set.
     */
    private static double[] searchLayerBeamFiltered(
            MemorySegment seg,
            PersistentEdgeStore edges,
            float[] query,
            int dim,
            int entryPoint,
            int layer,
            int ef,
            int k,
            ArrayBitSet allowedIds,
            int distanceType,
            long timeoutNanos,
            int maxDistComps,
            double patienceSat,
            int patience) {

        int maxSize = Math.max(ef, k) * 4;

        // Early termination state
        long startTime = timeoutNanos > 0 ? System.nanoTime() : 0;
        int distCompCount = 0;
        int saturatedCount = 0;

        // Reuse thread-local visited bitset with lazy clearing
        int requiredSize = (edges.getMaxNodes() + 63) / 64;
        long[] visited = VISITED_BITSET.get();
        int[] maxWordHolder = VISITED_MAX_WORD.get();
        if (visited == null || visited.length < requiredSize) {
            visited = new long[requiredSize];
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

        // Candidates: min-heap by distance (closest first) - ALL nodes for traversal
        int[] candIds = new int[maxSize];
        double[] candDists = new double[maxSize];
        int candCount = 0;

        // Results: max-heap by distance (furthest first) - ONLY allowed nodes
        int[] resIds = new int[maxSize];
        double[] resDists = new double[maxSize];
        int resCount = 0;

        // Initialize with entry point
        double epDist = Distance.compute(seg, entryPoint, dim, query, distanceType);
        distCompCount++;
        heapPush(candIds, candDists, candCount++, entryPoint, epDist);
        setVisitedTracked(visited, entryPoint, maxWordHolder);

        // Cache furthestResult - only update when heap changes
        double furthestResult = Double.MAX_VALUE;

        // Only add to results if allowed AND not deleted
        if (allowedIds.contains(entryPoint) && !edges.isDeleted(entryPoint)) {
            maxHeapPush(resIds, resDists, resCount++, entryPoint, epDist);
            furthestResult = epDist;
        }

        while (candCount > 0) {
            // === Early termination checks ===

            // 1. Timeout check (every iteration for hard latency bounds)
            if (timeoutNanos > 0 && (System.nanoTime() - startTime) > timeoutNanos) {
                break;
            }

            // 2. Distance budget check
            if (maxDistComps > 0 && distCompCount >= maxDistComps) {
                break;
            }

            // Pop closest candidate (O(log n))
            int current = candIds[0];
            double currentDist = candDists[0];
            heapPop(candIds, candDists, candCount--);

            // Quality-based early termination: if we have enough results and current is worse
            if (resCount >= k && currentDist > furthestResult) {
                break;
            }

            // Track displacements for patience (paper's metric)
            int displacementsThisIteration = 0;

            // Expand neighbors - zero-copy access for layer 0
            int chunkIdx = current >> PersistentEdgeStore.CHUNK_SHIFT;
            int[] chunk = edges.getLayer0Chunk(chunkIdx);
            if (chunk == null) continue;

            int base = edges.getNodeSlotOffset(current);
            int neighborCount = chunk[base];

            for (int i = 0; i < neighborCount; i++) {
                int neighborId = chunk[base + 1 + i];
                if (isVisited(visited, neighborId)) continue;
                setVisitedTracked(visited, neighborId, maxWordHolder);

                double neighborDist = Distance.compute(seg, neighborId, dim, query, distanceType);
                distCompCount++;

                // Always consider for candidates (graph traversal)
                // Use ef as bound for exploration
                boolean dominated = resCount >= ef && neighborDist > furthestResult;
                if (!dominated && candCount < maxSize) {
                    heapPush(candIds, candDists, candCount++, neighborId, neighborDist);
                }

                // Only add to results if allowed AND not deleted
                if (allowedIds.contains(neighborId) && !edges.isDeleted(neighborId)) {
                    if (resCount < ef) {
                        maxHeapPush(resIds, resDists, resCount++, neighborId, neighborDist);
                        // Update cached furthest when heap grows
                        furthestResult = resDists[0];
                    } else if (neighborDist < furthestResult) {
                        // Replace furthest allowed result
                        maxHeapReplace(resIds, resDists, resCount, neighborId, neighborDist);
                        displacementsThisIteration++;
                        // Update cached furthest after replacement
                        furthestResult = resDists[0];
                    }
                }
            }

            // 3. Patience check (only after we have k results)
            if (patience > 0 && resCount >= k) {
                // Paper's metric: overlap = (k - displacements) / k
                double overlap = (double)(k - displacementsThisIteration) / k;
                if (overlap >= patienceSat) {
                    saturatedCount++;
                    if (saturatedCount > patience) {
                        break;
                    }
                } else {
                    saturatedCount = 0;
                }
            }
        }

        // Sort results by distance and return top k
        sortByDistance(resIds, resDists, resCount);

        int resultCount = Math.min(k, resCount);
        double[] result = new double[resultCount * 2];
        for (int i = 0; i < resultCount; i++) {
            result[i * 2] = resIds[i];
            result[i * 2 + 1] = resDists[i];
        }
        return result;
    }

    // =========================================================================
    // Utility
    // =========================================================================

    private static void sortByDistance(int[] ids, double[] distances, int count) {
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

    private static boolean isVisited(long[] visited, int nodeId) {
        int wordIdx = nodeId >> 6;
        long bit = 1L << (nodeId & 63);
        return (visited[wordIdx] & bit) != 0;
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
}
