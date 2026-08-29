package proximum.internal;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Generation-pinned, resumable level-0 HNSW traversal.
 *
 * <p>The ordinary HNSW search remains the allocation-minimal top-k fast path.
 * This cursor is for consumers whose primary predicates can reject candidates:
 * it retains the visited set and the discarded frontier so another page can
 * continue graph exploration instead of paging a frozen top-k vector.</p>
 *
 * <p>The cursor is deliberately mutable and single-consumer. Its immutable
 * {@code MemorySegment} and {@link PersistentEdgeIndex} references pin one
 * Proximum generation for the cursor lifetime; callers must keep that
 * generation open while paging.</p>
 */
public final class HnswCandidateCursor implements AutoCloseable {
    private static final Comparator<Node> NEAREST_FIRST = (left, right) -> {
        int distance = Double.compare(left.distance, right.distance);
        return distance != 0 ? distance : Integer.compare(left.id, right.id);
    };

    private static final Comparator<Node> FURTHEST_FIRST = (left, right) -> {
        int distance = Double.compare(right.distance, left.distance);
        return distance != 0 ? distance : Integer.compare(right.id, left.id);
    };

    private record Node(int id, double distance) {}

    private final MemorySegment vectors;
    private final PersistentEdgeIndex edges;
    private final float[] query;
    private final int dimension;
    private final int distanceType;
    private final int ef;
    private final boolean strictOrder;
    private final long maxVisited;
    private final long maxDistanceComputations;
    private final long timeoutNanos;
    private final long maxFrontierNodes;
    private final ArrayBitSet allowedIds;
    private final Runnable leaseRelease;
    private final long[] visited;
    private final long[] discardedMembership;
    private final long[] emitted;
    private final PriorityQueue<Node> discarded = new PriorityQueue<>(NEAREST_FIRST);
    private final PriorityQueue<Node> ready = new PriorityQueue<>(NEAREST_FIRST);

    private int entryPoint;
    private boolean firstBatch = true;
    private boolean terminal;
    private String stopReason;
    private Node previous;
    private long visitedCount;
    private long distanceComputations;
    private long emittedCount;
    private long strictOrderDrops;
    private long searchNanos;
    private int nextSequence;
    private int pendingSequence = -1;
    private double[] pendingPage;
    private boolean closed;

    public HnswCandidateCursor(
            MemorySegment vectors,
            PersistentEdgeIndex edges,
            float[] query,
            int dimension,
            int ef,
            int distanceType,
            boolean strictOrder,
            int nodeCount,
            long maxVisited,
            long maxDistanceComputations,
            long timeoutNanos,
            long maxFrontierNodes,
            ArrayBitSet allowedIds,
            Runnable leaseRelease) {
        if (ef <= 0) {
            throw new IllegalArgumentException("ef must be positive");
        }
        this.vectors = vectors;
        this.edges = edges;
        this.query = query.clone();
        this.dimension = dimension;
        this.ef = ef;
        this.distanceType = distanceType;
        this.strictOrder = strictOrder;
        this.maxVisited = maxVisited;
        this.maxDistanceComputations = maxDistanceComputations;
        this.timeoutNanos = timeoutNanos;
        this.maxFrontierNodes = maxFrontierNodes;
        // A continuation's logical universe is part of its pinned snapshot.
        // Do not allow mutation of an advanced caller's bitset to change it.
        this.allowedIds = allowedIds == null ? null : allowedIds.clone();
        this.leaseRelease = leaseRelease;

        int words = Math.max(1, (nodeCount + 63) / 64);
        this.visited = new long[words];
        this.discardedMembership = new long[words];
        this.emitted = new long[words];

        this.entryPoint = edges.getEntrypoint();
        if (entryPoint < 0) {
            finish("frontier-empty");
            return;
        }

        long started = System.nanoTime();
        try {
            int maxLevel = edges.getCurrentMaxLevel();
            for (int level = maxLevel; level > 0 && !terminal; level--) {
                entryPoint = greedy(entryPoint, level, started);
            }
        } finally {
            searchNanos += System.nanoTime() - started;
        }
    }

    /**
     * Stage and return one replayable page for the expected continuation
     * sequence. Call {@link #ack(int)} only after the caller has converted the
     * page successfully.
     */
    public synchronized double[] page(int sequence, int limit) {
        ensureOpen();
        if (sequence != nextSequence) {
            throw new IllegalStateException(
                    "stale or out-of-order candidate continuation: expected "
                            + nextSequence + ", got " + sequence);
        }
        if (pendingPage != null) {
            if (pendingSequence != sequence) {
                throw new IllegalStateException("another candidate page is pending acknowledgement");
            }
            return pendingPage.clone();
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }

        try {
            return producePage(sequence, limit);
        } catch (Throwable failure) {
            // Traversal mutates visited/frontier state incrementally, so a
            // native failure cannot be rolled back cheaply. Poison and close
            // the cursor instead of pretending that this sequence is replayable.
            finish("native-error");
            close();
            throw failure;
        }
    }

    private double[] producePage(int sequence, int limit) {

        List<Node> page = new ArrayList<>(limit);
        while (page.size() < limit) {
            ensureReady(limit);
            Node node = ready.poll();
            if (node == null) {
                break;
            }
            if (isSet(emitted, node.id)) {
                continue;
            }
            set(emitted, node.id);
            if (strictOrder && previous != null
                    && NEAREST_FIRST.compare(node, previous) < 0) {
                strictOrderDrops++;
                continue;
            }
            previous = node;
            emittedCount++;
            page.add(node);
        }

        if (ready.isEmpty() && !terminal && firstBatch == false && noDiscarded()) {
            finish("frontier-empty");
        }

        double[] result = new double[page.size() * 2];
        for (int i = 0; i < page.size(); i++) {
            Node node = page.get(i);
            result[i * 2] = node.id;
            result[i * 2 + 1] = node.distance;
        }
        pendingSequence = sequence;
        pendingPage = result;
        return result.clone();
    }

    /** Commit a successfully converted staged page. */
    public synchronized void ack(int sequence) {
        ensureOpen();
        if (pendingPage == null || pendingSequence != sequence || sequence != nextSequence) {
            throw new IllegalStateException("candidate page is not pending for sequence " + sequence);
        }
        pendingPage = null;
        pendingSequence = -1;
        nextSequence++;
        releaseIfFinished();
    }

    private void ensureReady(int requested) {
        while (ready.isEmpty() && !terminal) {
            List<Node> seeds = new ArrayList<>(ef);
            if (firstBatch) {
                firstBatch = false;
                Node entry = distanceNode(entryPoint);
                if (entry == null) {
                    return;
                }
                markVisited(entryPoint);
                seeds.add(entry);
            } else {
                int batchSize = ef;
                while (seeds.size() < batchSize) {
                    Node seed = pollDiscarded();
                    if (seed == null) {
                        break;
                    }
                    seeds.add(seed);
                }
                if (seeds.isEmpty()) {
                    finish("frontier-empty");
                    return;
                }
            }
            searchBatch(seeds, ef);
        }
    }

    private void searchBatch(List<Node> seeds, int beamWidth) {
        long started = System.nanoTime();
        PriorityQueue<Node> candidates = new PriorityQueue<>(NEAREST_FIRST);
        PriorityQueue<Node> results = new PriorityQueue<>(FURTHEST_FIRST);
        candidates.addAll(seeds);
        for (Node seed : seeds) {
            if (isAllowed(seed.id)
                    && !edges.isDeleted(seed.id) && !isSet(emitted, seed.id)) {
                results.add(seed);
            }
        }

        while (!candidates.isEmpty() && !terminal) {
            if (budgetReached(started)) {
                break;
            }

            Node current = candidates.poll();
            Node furthest = results.peek();
            if (furthest != null && results.size() >= beamWidth
                    && NEAREST_FIRST.compare(current, furthest) > 0) {
                addDiscarded(current);
                while (!candidates.isEmpty()) {
                    addDiscarded(candidates.poll());
                }
                break;
            }

            int[] chunk = edges.getLayer0Chunk(
                    current.id >> PersistentEdgeIndex.CHUNK_SHIFT);
            if (chunk == null) {
                continue;
            }
            int base = edges.getNodeSlotOffset(current.id);
            int neighborCount = chunk[base];
            for (int i = 0; i < neighborCount && !terminal; i++) {
                int neighborId = chunk[base + 1 + i];
                if (isSet(visited, neighborId)) {
                    continue;
                }
                if (maxVisited > 0 && visitedCount >= maxVisited) {
                    finish("visited-budget");
                    break;
                }
                if (maxFrontierNodes > 0 && visitedCount >= maxFrontierNodes) {
                    // Every retained Node originates from one level-0 visit;
                    // capping visits therefore conservatively bounds the union
                    // of ready, discarded, candidates, results, seeds, and a
                    // staged page even when queues share Node references.
                    finish("memory-budget");
                    break;
                }
                markVisited(neighborId);
                Node neighbor = distanceNode(neighborId);
                if (neighbor == null) {
                    break;
                }

                furthest = results.peek();
                boolean competitive = results.size() < beamWidth
                        || furthest == null
                        || NEAREST_FIRST.compare(neighbor, furthest) < 0;
                if (competitive) {
                    candidates.add(neighbor);
                    if (isAllowed(neighborId)
                            && !edges.isDeleted(neighborId) && !isSet(emitted, neighborId)) {
                        results.add(neighbor);
                        if (results.size() > beamWidth) {
                            addDiscarded(results.poll());
                        }
                    }
                } else {
                    addDiscarded(neighbor);
                }
            }
        }

        searchNanos += System.nanoTime() - started;
        while (!candidates.isEmpty()) {
            addDiscarded(candidates.poll());
        }
        while (!results.isEmpty()) {
            Node result = results.poll();
            if (!isSet(emitted, result.id)) {
                ready.add(result);
            }
        }
    }

    private int greedy(int start, int level, long started) {
        int current = start;
        Node currentNode = distanceNode(current);
        if (currentNode == null) {
            return current;
        }
        double currentDistance = currentNode.distance;
        boolean improved = true;
        while (improved && !terminal) {
            improved = false;
            int[] neighbors = edges.getNeighbors(level, current);
            if (neighbors == null) {
                break;
            }
            for (int neighbor : neighbors) {
                if (timeoutNanos > 0 && (System.nanoTime() - started) >= timeoutNanos) {
                    finish("timeout");
                    break;
                }
                Node neighborNode = distanceNode(neighbor);
                if (neighborNode == null) {
                    break;
                }
                if (NEAREST_FIRST.compare(neighborNode,
                        new Node(current, currentDistance)) < 0) {
                    current = neighbor;
                    currentDistance = neighborNode.distance;
                    improved = true;
                }
            }
        }
        return current;
    }

    private Node distanceNode(int id) {
        if (maxDistanceComputations > 0
                && distanceComputations >= maxDistanceComputations) {
            finish("distance-computation-budget");
            return null;
        }
        distanceComputations++;
        return new Node(id, Distance.compute(vectors, id, dimension, query, distanceType));
    }

    private boolean budgetReached(long batchStarted) {
        if (maxDistanceComputations > 0
                && distanceComputations >= maxDistanceComputations) {
            finish("distance-computation-budget");
            return true;
        }
        if (timeoutNanos > 0
                && searchNanos + (System.nanoTime() - batchStarted) >= timeoutNanos) {
            finish("timeout");
            return true;
        }
        return false;
    }

    private void addDiscarded(Node node) {
        if (!isSet(emitted, node.id) && !isSet(discardedMembership, node.id)) {
            set(discardedMembership, node.id);
            discarded.add(node);
        }
    }

    private Node pollDiscarded() {
        while (!discarded.isEmpty()) {
            Node node = discarded.poll();
            clear(discardedMembership, node.id);
            if (!isSet(emitted, node.id)) {
                return node;
            }
        }
        return null;
    }

    private boolean noDiscarded() {
        while (!discarded.isEmpty() && isSet(emitted, discarded.peek().id)) {
            Node node = discarded.poll();
            clear(discardedMembership, node.id);
        }
        return discarded.isEmpty();
    }

    private boolean isAllowed(int id) {
        return allowedIds == null || allowedIds.contains(id);
    }

    private void markVisited(int id) {
        if (!isSet(visited, id)) {
            set(visited, id);
            visitedCount++;
        }
    }

    private void finish(String reason) {
        terminal = true;
        if (stopReason == null) {
            stopReason = reason;
        }
    }

    private static boolean isSet(long[] words, int id) {
        return (words[id >> 6] & (1L << (id & 63))) != 0;
    }

    private static void set(long[] words, int id) {
        words[id >> 6] |= 1L << (id & 63);
    }

    private static void clear(long[] words, int id) {
        words[id >> 6] &= ~(1L << (id & 63));
    }

    public synchronized boolean isExhausted() {
        return terminal && ready.isEmpty() && pendingPage == null;
    }

    public synchronized String getStopReason() {
        return stopReason;
    }

    public synchronized long getVisitedCount() {
        return visitedCount;
    }

    public synchronized long getDistanceComputations() {
        return distanceComputations;
    }

    public synchronized long getEmittedCount() {
        return emittedCount;
    }

    public synchronized long getStrictOrderDrops() {
        return strictOrderDrops;
    }

    public synchronized long getSearchNanos() {
        return searchNanos;
    }

    public synchronized int getNextSequence() {
        return nextSequence;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("candidate cursor is closed");
        }
    }

    private void releaseIfFinished() {
        if (terminal && ready.isEmpty() && pendingPage == null) {
            close();
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        ready.clear();
        discarded.clear();
        pendingPage = null;
        if (leaseRelease != null) {
            leaseRelease.run();
        }
    }
}
