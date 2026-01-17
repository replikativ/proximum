package org.replikativ.proximum;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * ProximumVectorStore - Persistent Vector Store with versioning support.
 *
 * <p>This class is generated from proximum.specification and provides a 1-to-1
 * mapping with the Clojure API. All operations follow persistent/immutable
 * semantics - mutating operations return new store instances.</p>
 *
 * <h2>Basic Usage:</h2>
 * <pre>{@code
 * // Create a store
 * ProximumVectorStore store = ProximumVectorStore.createIndex(config);
 *
 * // Add vectors (returns new store)
 * store = store.add(embedding);
 * store = store.add(embedding, metadata);
 *
 * // Search
 * List<SearchResult> results = store.search(queryVector, 10);
 *
 * // Persist to storage
 * store.sync();
 * }</pre>
 *
 * <h2>Thread Safety:</h2>
 * <p>All operations are thread-safe. Mutating operations return new immutable
 * store instances, so the original store can still be used concurrently.</p>
 *
 * @see SearchResult
 * @see IndexConfig
 */
public class ProximumVectorStore implements AutoCloseable {

    // Clojure runtime initialization
    private static final Object LOCK = new Object();
    private static volatile boolean initialized = false;

    // Function references (loaded lazily)
    private static IFn compactionProgressFn;
    private static IFn syncFn;
    private static IFn indexMetricsFn;
    private static IFn searchWithMetadataFn;
    private static IFn getCommitHashFn;
    private static IFn indexTypeFn;
    private static IFn forkFn;
    private static IFn gcFn;
    private static IFn indexConfigFn;
    private static IFn closeFn;
    private static IFn loadFn;
    private static IFn verifyFromColdFn;
    private static IFn getMetadataFn;
    private static IFn remainingCapacityFn;
    private static IFn searchFilteredFn;
    private static IFn finishOnlineCompactionFn;
    private static IFn insertFn;
    private static IFn commitGraphFn;
    private static IFn capacityFn;
    private static IFn needsCompactionFn;
    private static IFn resetFn;
    private static IFn compactFn;
    private static IFn loadCommitFn;
    private static IFn deleteFn;
    private static IFn historyFn;
    private static IFn branchesFn;
    private static IFn getCommitIdFn;
    private static IFn deleteBranchFn;
    private static IFn getBranchFn;
    private static IFn searchFn;
    private static IFn getVectorFn;
    private static IFn ancestorsFn;
    private static IFn startOnlineCompactionFn;
    private static IFn withMetadataFn;
    private static IFn countVectorsFn;
    private static IFn createIndexFn;
    private static IFn lookupInternalIdFn;
    private static IFn cryptoHashFn;
    private static IFn parentsFn;
    private static IFn ancestorFn;
    private static IFn commonAncestorFn;
    private static IFn commitInfoFn;
    private static IFn branchFn;
    private static IFn insertBatchFn;

    // Instance state
    private volatile Object clojureIndex;
    private final IndexConfig config;

    /**
     * Private constructor - use static factory methods.
     */
    private ProximumVectorStore(Object clojureIndex, IndexConfig config) {
        this.clojureIndex = clojureIndex;
        this.config = config;
    }

    /**
     * Initialize Clojure runtime and load required namespaces.
     */
    private static void ensureInitialized() {
        if (!initialized) {
            synchronized (LOCK) {
                if (!initialized) {
                    IFn require = Clojure.var("clojure.core", "require");
                    require.invoke(Clojure.read("proximum.core"));

                    // Load all function references
            compactionProgressFn = Clojure.var("proximum.core", "compaction-progress");
            syncFn = Clojure.var("proximum.core", "sync!");
            indexMetricsFn = Clojure.var("proximum.core", "index-metrics");
            searchWithMetadataFn = Clojure.var("proximum.core", "search-with-metadata");
            getCommitHashFn = Clojure.var("proximum.core", "get-commit-hash");
            indexTypeFn = Clojure.var("proximum.core", "index-type");
            forkFn = Clojure.var("proximum.core", "fork");
            gcFn = Clojure.var("proximum.core", "gc!");
            indexConfigFn = Clojure.var("proximum.core", "index-config");
            closeFn = Clojure.var("proximum.core", "close!");
            loadFn = Clojure.var("proximum.core", "load");
            verifyFromColdFn = Clojure.var("proximum.core", "verify-from-cold");
            getMetadataFn = Clojure.var("proximum.core", "get-metadata");
            remainingCapacityFn = Clojure.var("proximum.core", "remaining-capacity");
            searchFilteredFn = Clojure.var("proximum.core", "search-filtered");
            finishOnlineCompactionFn = Clojure.var("proximum.core", "finish-online-compaction!");
            insertFn = Clojure.var("proximum.core", "insert");
            commitGraphFn = Clojure.var("proximum.core", "commit-graph");
            capacityFn = Clojure.var("proximum.core", "capacity");
            needsCompactionFn = Clojure.var("proximum.core", "needs-compaction?");
            resetFn = Clojure.var("proximum.core", "reset!");
            compactFn = Clojure.var("proximum.core", "compact");
            loadCommitFn = Clojure.var("proximum.core", "load-commit");
            deleteFn = Clojure.var("proximum.core", "delete");
            historyFn = Clojure.var("proximum.core", "history");
            branchesFn = Clojure.var("proximum.core", "branches");
            getCommitIdFn = Clojure.var("proximum.core", "get-commit-id");
            deleteBranchFn = Clojure.var("proximum.core", "delete-branch!");
            getBranchFn = Clojure.var("proximum.core", "get-branch");
            searchFn = Clojure.var("proximum.core", "search");
            getVectorFn = Clojure.var("proximum.core", "get-vector");
            ancestorsFn = Clojure.var("proximum.core", "ancestors");
            startOnlineCompactionFn = Clojure.var("proximum.core", "start-online-compaction");
            withMetadataFn = Clojure.var("proximum.core", "with-metadata");
            countVectorsFn = Clojure.var("proximum.core", "count-vectors");
            createIndexFn = Clojure.var("proximum.core", "create-index");
            lookupInternalIdFn = Clojure.var("proximum.core", "lookup-internal-id");
            cryptoHashFn = Clojure.var("proximum.core", "crypto-hash?");
            parentsFn = Clojure.var("proximum.core", "parents");
            ancestorFn = Clojure.var("proximum.core", "ancestor?");
            commonAncestorFn = Clojure.var("proximum.core", "common-ancestor");
            commitInfoFn = Clojure.var("proximum.core", "commit-info");
            branchFn = Clojure.var("proximum.core", "branch!");
            insertBatchFn = Clojure.var("proximum.core", "insert-batch");

                    initialized = true;
                }
            }
        }
    }

    /**
     * Get the underlying Clojure index object.
     * For advanced interop only.
     */
    public Object getClojureIndex() {
        return clojureIndex;
    }


    // ==========================================================================
    // Builder (generated from HnswConfig schema)
    // ==========================================================================

    /**
     * Create a new builder for constructing a ProximumVectorStore.
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating ProximumVectorStore instances.
     */
    public static class Builder {
        private int dim;
        private int M = 16;
        private int efConstruction = 200;
        private int efSearch = 50;
        private int capacity = 10000000;
        private DistanceMetric distance = DistanceMetric.EUCLIDEAN;
        private Map<String, Object> storeConfig;
        private String mmapDir;
        private String mmapPath;
        private String branch = "main";
        private boolean cryptoHash = false;
        private int chunkSize = 1000;
        private int cacheSize = 10000;
        private Integer maxLevels;

        private Builder() {}

        /** Vector dimensions (e.g., 1536 for OpenAI embeddings). */
        public Builder dim(int dim) {
            this.dim = dim;
            return this;
        }

        /** Max neighbors per node. Higher improves recall but increases memory and build time. */
        public Builder M(int M) {
            this.M = M;
            return this;
        }

        /** Beam width during index construction. Higher improves quality but slows builds. */
        public Builder efConstruction(int efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        /** Beam width during search. Higher improves recall but increases latency. Can override per-query. */
        public Builder efSearch(int efSearch) {
            this.efSearch = efSearch;
            return this;
        }

        /** Maximum number of vectors the index can hold. */
        public Builder capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        /** Distance metric: :euclidean, :cosine, or :inner-product. */
        public Builder distance(DistanceMetric distance) {
            this.distance = distance;
            return this;
        }

        /** Konserve store configuration map for custom backends. */
        public Builder storeConfig(Map<String, Object> storeConfig) {
            this.storeConfig = storeConfig;
            return this;
        }

        /** Directory for memory-mapped vector storage. Defaults to store-config path. */
        public Builder mmapDir(String mmapDir) {
            this.mmapDir = mmapDir;
            return this;
        }

        /** Explicit path for memory-mapped file (advanced). */
        public Builder mmapPath(String mmapPath) {
            this.mmapPath = mmapPath;
            return this;
        }

        /** Initial branch name for versioning. */
        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        /** Enable cryptographic commit hashing for auditability. */
        public Builder cryptoHash(boolean cryptoHash) {
            this.cryptoHash = cryptoHash;
            return this;
        }

        /** Edge storage chunk size. Affects memory/persistence granularity. */
        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        /** Number of edge chunks to cache in memory. */
        public Builder cacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        /** Maximum HNSW levels (auto-calculated if not set). */
        public Builder maxLevels(Integer maxLevels) {
            this.maxLevels = maxLevels;
            return this;
        }

        /** Alias for dim() - set vector dimensions. */
        public Builder dimensions(int dimensions) {
            return dim(dimensions);
        }

        /** Alias for M() - set max neighbors per node. */
        public Builder m(int m) {
            return M(m);
        }

        /**
         * Convenience method to set storage path with default file backend.
         * @param path directory path for storage
         * @return this builder
         */
        public Builder storagePath(String path) {
            this.storeConfig = Map.of(
                "backend", ":file",
                "path", path,
                "id", UUID.randomUUID()
            );
            if (this.mmapDir == null) {
                this.mmapDir = path;
            }
            return this;
        }

        /**
         * Build the ProximumVectorStore.
         * @return the configured store
         * @throws IllegalArgumentException if required fields are missing
         */
        public ProximumVectorStore build() {
            if (dim <= 0) {
                throw new IllegalArgumentException("dimensions (dim) must be set and positive");
            }
            ensureInitialized();

            // Build config map
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(":type", ":hnsw");
            configMap.put(":dim", dim);
            configMap.put(":M", M);
            configMap.put(":ef-construction", efConstruction);
            configMap.put(":ef-search", efSearch);
            configMap.put(":capacity", capacity);
            configMap.put(":distance", ":" + distance.name().toLowerCase());
            configMap.put(":branch", ":" + branch);
            configMap.put(":crypto-hash?", cryptoHash);
            configMap.put(":chunk-size", chunkSize);
            configMap.put(":cache-size", cacheSize);
            if (storeConfig != null) {
                configMap.put(":store-config", storeConfig);
            }
            if (mmapDir != null) {
                configMap.put(":mmap-dir", mmapDir);
            }
            if (mmapPath != null) {
                configMap.put(":mmap-path", mmapPath);
            }
            if (maxLevels != null) {
                configMap.put(":max-levels", maxLevels);
            }

            Object result = createIndexFn.invoke(toClojureMap(configMap));
            return new ProximumVectorStore(result, null);
        }
    }

    // Helper to convert Java map to Clojure map
    private static Object toClojureMap(Map<String, Object> map) {
        IFn hashMap = Clojure.var("clojure.core", "hash-map");
        IFn keyword = Clojure.var("clojure.core", "keyword");
        Object[] args = new Object[map.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String k = e.getKey();
            Object v = e.getValue();
            // Convert :keyword strings to actual keywords
            if (k.startsWith(":")) {
                args[i++] = keyword.invoke(k.substring(1));
            } else {
                args[i++] = keyword.invoke(k);
            }
            // Convert :value strings to keywords
            if (v instanceof String && ((String) v).startsWith(":")) {
                args[i++] = keyword.invoke(((String) v).substring(1));
            } else if (v instanceof Map) {
                args[i++] = toClojureMap((Map<String, Object>) v);
            } else {
                args[i++] = v;
            }
        }
        return hashMap.applyTo(clojure.lang.ArraySeq.create(args));
    }

    // Helper to convert Clojure search results to Java SearchResult list
    @SuppressWarnings("unchecked")
    private static List<SearchResult> toSearchResults(Iterable<Object> results) {
        IFn get = Clojure.var("clojure.core", "get");
        IFn keyword = Clojure.var("clojure.core", "keyword");
        Object idKw = keyword.invoke("id");
        Object distKw = keyword.invoke("distance");
        Object metaKw = keyword.invoke("metadata");

        List<SearchResult> list = new ArrayList<>();
        for (Object r : results) {
            Object id = get.invoke(r, idKw);
            Number dist = (Number) get.invoke(r, distKw);
            Object meta = get.invoke(r, metaKw);
            if (meta != null) {
                // Convert Clojure map with keyword keys to Java map with string keys
                Map<String, Object> metaMap = convertClojureMap((Map<Object, Object>) meta);
                list.add(new SearchResult(id, dist.doubleValue(), metaMap));
            } else {
                list.add(new SearchResult(id, dist.doubleValue()));
            }
        }
        return list;
    }


    // ==========================================================================
    // Static Methods (from specification)
    // ==========================================================================

    /**
     * Load an existing index from storage.
     * Loads the latest commit from the specified branch (default: :main).
     * 
     * Example:
     *   (load {:backend :file :path "/data/idx" :id #uuid "..."} {:branch :main})
     */
    public static ProximumVectorStore connect(Map<String, Object> storeConfig) {
        ensureInitialized();
        Object result = loadFn.invoke(toClojureMap(storeConfig));
        return new ProximumVectorStore(result, null);
    }

    /**
     * Verify index integrity from cold storage.
     * Returns: {:valid? bool :vectors-verified N :edges-verified N}
     */
    public static Map<String, Object> verifyFromCold(Map<String, Object> storeConfig) {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) verifyFromColdFn.invoke(toClojureMap(storeConfig));
        return result == null ? null : convertClojureMap(result);
    }

    /**
     * Load a historical commit by ID (time-travel query).
     * 
     * Example:
     *   (load-commit store-config #uuid "550e8400-..." {:branch :main})
     */
    public static ProximumVectorStore connectCommit(Map<String, Object> storeConfig, UUID commitId) {
        ensureInitialized();
        Object result = loadCommitFn.invoke(toClojureMap(storeConfig), commitId);
        return new ProximumVectorStore(result, null);
    }

    /**
     * Create a new vector index with the given configuration.
     * Dispatches on :type to create appropriate index implementation.
     * 
     * Example:
     *   (create-index {:type :hnsw
     *                  :dim 128
     *                  :M 16
     *                  :store-config {:backend :file :path "/data/idx" :id (random-uuid)}
     *                  :mmap-dir "/data/mmap"})
     */
    public static ProximumVectorStore createIndex(Map<String, Object> config) {
        ensureInitialized();
        Object result = createIndexFn.invoke(toClojureMap(config));
        return new ProximumVectorStore(result, null);
    }

    // ==========================================================================
    // Instance Methods (from specification)
    // ==========================================================================

    /**
     * Get current compaction progress.
     * Returns {:copying? bool :finished? bool :delta-count N :mapped-ids N}
     */
    public Map<String, Object> compactionProgress(Object arg0) {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) compactionProgressFn.invoke(clojureIndex, convertFilterArg(arg0));
        return result == null ? null : convertClojureMap(result);
    }

    /**
     * Persist current state to durable storage, creating a commit.
     * Waits for all pending writes to complete.
     * 
     * With {:sync? true}, blocks until complete.
     * Default returns channel that delivers index when done.
     */
    public synchronized ProximumVectorStore sync() {
        ensureInitialized();
        Object newIdx = syncFn.invoke(clojureIndex);
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Get comprehensive index health metrics.
     * Returns: {:vector-count :deleted-count :live-count :deletion-ratio
     *           :needs-compaction? :capacity :utilization :edge-count
     *           :avg-edges-per-node :branch :commit-id :cache-hits :cache-misses}
     */
    public Map<String, Object> getMetrics() {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) indexMetricsFn.invoke(clojureIndex);
        return result == null ? null : convertClojureMap(result);
    }

    /**
     * Search and include metadata in results.
     * Returns seq of {:id :distance :metadata}.
     * 
     * Example:
     *   (search-with-metadata idx query 10)
     */
    public List<SearchResult> searchWithMetadata(float[] vector, int k) {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Iterable<Object> results = (Iterable<Object>) searchWithMetadataFn.invoke(clojureIndex, vector, k);
        return toSearchResults(results);
    }

    /**
     * Get SHA-512 based commit hash (if crypto-hash? enabled).
     */
    public UUID getCommitHash() {
        ensureInitialized();
        return (UUID) getCommitHashFn.invoke(clojureIndex);
    }

    /**
     * Returns the index type keyword (e.g., :hnsw).
     */
    public String indexType() {
        ensureInitialized();
        Object result = indexTypeFn.invoke(clojureIndex);
        if (result instanceof clojure.lang.Keyword) {
            return ((clojure.lang.Keyword) result).getName();
        }
        return (String) result;
    }

    /**
     * Create O(1) fork with structural sharing.
     * Both indices share immutable structure, diverge on writes.
     */
    public synchronized ProximumVectorStore fork() {
        ensureInitialized();
        Object newIdx = forkFn.invoke(clojureIndex);
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Garbage collect unreachable data from storage.
     * Removes commits older than remove-before date.
     */
    public Set<Object> gc() {
        ensureInitialized();
        return (Set<Object>) gcFn.invoke(clojureIndex);
    }

    /**
     * Returns config map that can recreate this index via create-index.
     */
    public Map<String, Object> indexConfig() {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) indexConfigFn.invoke(clojureIndex);
        return result == null ? null : convertClojureMap(result);
    }

    /**
     * Close the index and release resources (mmap, caches, stores).
     */
    public void close() {
        ensureInitialized();
        closeFn.invoke(clojureIndex);
    }

    /**
     * Get metadata map for vector ID. Returns nil if not found.
     */
    public Map<String, Object> getMetadata(Object id) {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) getMetadataFn.invoke(clojureIndex, id);
        return result == null ? null : convertClojureMap(result);
    }

    /**
     * Remaining capacity (capacity - count-vectors).
     */
    public int remainingCapacity() {
        ensureInitialized();
        return ((Number) remainingCapacityFn.invoke(clojureIndex)).intValue();
    }

    /**
     * Search with filtering predicate or ID set.
     * Filter can be:
     *   - (fn [id metadata] boolean) - predicate receives external ID
     *   - Set of allowed external IDs
     * 
     * Example:
     *   (search-filtered idx query 10 #{"doc-1" "doc-2"})
     */
    public List<SearchResult> searchFiltered(float[] vector, int k, Object arg2) {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Iterable<Object> results = (Iterable<Object>) searchFilteredFn.invoke(clojureIndex, vector, k, convertFilterArg(arg2));
        return toSearchResults(results);
    }

    /**
     * Finish online compaction and return new index.
     */
    public synchronized ProximumVectorStore finishOnlineCompaction(Object arg0) {
        ensureInitialized();
        Object newIdx = finishOnlineCompactionFn.invoke(clojureIndex, convertFilterArg(arg0));
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Insert a vector with an ID and optional metadata. Returns new index.
     * ID can be any value (Long, String, UUID, etc.). Pass nil to auto-generate UUID.
     * This is a pure operation - no I/O until sync! is called.
     * 
     * Example:
     *   (insert idx (float-array [1.0 2.0 3.0]) 123)
     *   (insert idx (float-array [1.0 2.0 3.0]) "doc-abc" {:category :science})
     *   (insert idx vec nil)  ; auto-generates UUID
     */
    public synchronized ProximumVectorStore add(float[] vector, Object id) {
        ensureInitialized();
        Object newIdx = insertFn.invoke(clojureIndex, vector, id);
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Get full commit DAG for visualization.
     * Returns {:nodes {id {:parents :created-at :branch}} :branches {:main id} :roots #{id}}
     */
    public Map<String, Object> commitGraph() {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) commitGraphFn.invoke(clojureIndex);
        return result == null ? null : convertClojureMap(result);
    }

    /**
     * Maximum capacity of the index.
     */
    public int capacity() {
        ensureInitialized();
        return ((Number) capacityFn.invoke(clojureIndex)).intValue();
    }

    /**
     * Check if index needs compaction (deletion ratio > threshold).
     */
    public boolean isNeedsCompaction() {
        ensureInitialized();
        return (boolean) needsCompactionFn.invoke(clojureIndex);
    }

    /**
     * Reset current branch to a different commit. Like `git reset --hard`.
     * WARNING: Commits after target become unreachable.
     */
    public synchronized ProximumVectorStore reset(UUID commitId) {
        ensureInitialized();
        Object newIdx = resetFn.invoke(clojureIndex, commitId);
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Create compacted copy with only live vectors.
     * Target: {:store-config {...} :mmap-dir "..."}
     * Options: {:parallelism n}
     */
    public synchronized ProximumVectorStore compact(Map<String, Object> target) {
        ensureInitialized();
        Object newIdx = compactFn.invoke(clojureIndex, toClojureMap(target));
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Soft-delete vector by ID. Returns new index.
     * Vector is marked deleted but space not reclaimed until compact.
     */
    public synchronized ProximumVectorStore delete(Object id) {
        ensureInitialized();
        Object newIdx = deleteFn.invoke(clojureIndex, id);
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Get commit history for current branch (most recent first).
     */
    public List<Map<String, Object>> getHistory() {
        ensureInitialized();
        Object result = historyFn.invoke(clojureIndex);
        return convertClojureSeqToMapList(result);
    }

    /**
     * List all branches in the store.
     */
    public Set<String> listBranches() {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Set<Object> result = (Set<Object>) branchesFn.invoke(clojureIndex);
        return convertKeywordSetToStrings(result);
    }

    /**
     * Get the commit ID for current branch.
     */
    public UUID getCommitId() {
        ensureInitialized();
        return (UUID) getCommitIdFn.invoke(clojureIndex);
    }

    /**
     * Delete a branch. Like `git branch -d`. Cannot delete current or :main.
     */
    public synchronized ProximumVectorStore deleteBranch(String branchName) {
        ensureInitialized();
        Object newIdx = deleteBranchFn.invoke(clojureIndex, branchName);
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Get current branch name for this index.
     */
    public String getCurrentBranch() {
        ensureInitialized();
        Object result = getBranchFn.invoke(clojureIndex);
        if (result instanceof clojure.lang.Keyword) {
            return ((clojure.lang.Keyword) result).getName();
        }
        return (String) result;
    }

    /**
     * Search for k nearest neighbors.
     * Returns sequence of {:id :distance} sorted by distance (ascending).
     * IDs are external IDs as provided during insert.
     * 
     * Example:
     *   (search idx query-vec 10 {:ef 100})
     */
    public List<SearchResult> search(float[] vector, int k) {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Iterable<Object> results = (Iterable<Object>) searchFn.invoke(clojureIndex, vector, k);
        return toSearchResults(results);
    }

    /**
     * Retrieve vector by ID. Returns nil if not found or deleted.
     */
    public float[] getVector(Object id) {
        ensureInitialized();
        return (float[]) getVectorFn.invoke(clojureIndex, id);
    }

    /**
     * Get all ancestor commit IDs, most recent first. Like `git rev-list HEAD`.
     */
    public List<UUID> ancestors() {
        ensureInitialized();
        return (List<UUID>) ancestorsFn.invoke(clojureIndex);
    }

    /**
     * Start background compaction with zero downtime.
     * Returns CompactionState wrapper for use during compaction.
     */
    public Object startOnlineCompaction(Map<String, Object> target) {
        ensureInitialized();
        return (Object) startOnlineCompactionFn.invoke(clojureIndex, toClojureMap(target));
    }

    /**
     * Associate/update metadata for a vector. Returns new index.
     * 
     * Example:
     *   (with-metadata idx "doc-123" {:category :science})
     */
    public synchronized ProximumVectorStore withMetadata(Object id, Map<String, Object> metadata) {
        ensureInitialized();
        Object newIdx = withMetadataFn.invoke(clojureIndex, id, toClojureMap(metadata));
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Total vectors in index (includes soft-deleted).
     */
    public int count() {
        ensureInitialized();
        return ((Number) countVectorsFn.invoke(clojureIndex)).intValue();
    }

    /**
     * Look up internal node ID for an external ID. Returns nil if not found.
     */
    public Integer lookupInternalId(Object id) {
        ensureInitialized();
        return (Integer) lookupInternalIdFn.invoke(clojureIndex, id);
    }

    /**
     * Check if crypto-hash mode is enabled.
     */
    public boolean isCryptoHash() {
        ensureInitialized();
        return (boolean) cryptoHashFn.invoke(clojureIndex);
    }

    /**
     * Get parent commit IDs. Like `git rev-parse HEAD^@`.
     * First commit has #{:main} or similar branch keyword as parent.
     */
    public Set<Object> parents() {
        ensureInitialized();
        return (Set<Object>) parentsFn.invoke(clojureIndex);
    }

    /**
     * Check if first commit is ancestor of second. Like `git merge-base --is-ancestor`.
     */
    public boolean isAncestor(UUID commitId1, UUID commitId2) {
        ensureInitialized();
        return (boolean) ancestorFn.invoke(clojureIndex, commitId1, commitId2);
    }

    /**
     * Find common ancestor of two commits. Like `git merge-base`.
     */
    public UUID commonAncestor(UUID commitId1, UUID commitId2) {
        ensureInitialized();
        return (UUID) commonAncestorFn.invoke(clojureIndex, commitId1, commitId2);
    }

    /**
     * Get metadata for a commit. Like `git show --stat`.
     * Returns {:commit-id :parents :created-at :branch :vector-count :deleted-count}
     */
    public Map<String, Object> commitInfo(UUID commitId) {
        ensureInitialized();
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) commitInfoFn.invoke(clojureIndex, commitId);
        return result == null ? null : convertClojureMap(result);
    }

    /**
     * Create a new branch from current state.
     * Index must be synced first. Creates reflinked mmap copy.
     */
    public synchronized ProximumVectorStore branch(String branchName) {
        ensureInitialized();
        Object newIdx = branchFn.invoke(clojureIndex, branchName);
        this.clojureIndex = newIdx;
        return this;
    }

    /**
     * Insert multiple vectors with IDs efficiently.
     * IDs list must match vectors list length. Use nil for auto-generated UUIDs.
     * Options: {:metadata [m1 m2 ...], :parallelism n}
     * 
     * Example:
     *   (insert-batch idx [vec1 vec2] [id1 id2])
     *   (insert-batch idx [vec1 vec2] [nil nil] {:metadata [m1 m2]})
     */
    public synchronized ProximumVectorStore addBatch(List<float[]> arg0, List<Object> arg1) {
        ensureInitialized();
        Object newIdx = insertBatchFn.invoke(clojureIndex, arg0, arg1);
        this.clojureIndex = newIdx;
        return this;
    }


    // ==========================================================================
    // External ID API (public interface - hides internal IDs)
    // ==========================================================================

    private static IFn keywordFn;

    private static Object kw(String name) {
        if (keywordFn == null) {
            keywordFn = Clojure.var("clojure.core", "keyword");
        }
        return keywordFn.invoke(name);
    }

    /**
     * Add a vector with auto-generated UUID as ID.
     * <p>Convenience method that auto-generates a UUID and stores the vector.
     * Returns the new store instance (immutable pattern).</p>
     *
     * @param vector the embedding vector
     * @return new store with the vector added
     */
    public ProximumVectorStore addWithGeneratedId(float[] vector) {
        return addWithId(vector, UUID.randomUUID());
    }

    /**
     * Add a vector with the specified external ID.
     * <p>The ID is stored in metadata and can be any serializable type
     * (Long for Datahike, String, UUID, etc.).</p>
     *
     * @param vector the embedding vector
     * @param id the external ID to associate with this vector
     * @return new store with the vector added
     */
    public ProximumVectorStore addWithId(float[] vector, Object id) {
        ensureInitialized();
        Object result = insertFn.invoke(clojureIndex, vector, id);
        return new ProximumVectorStore(result, this.config);
    }

    /**
     * Add a vector with ID and additional metadata.
     * <p>The ID is stored in metadata under :external-id key.</p>
     *
     * @param vector the embedding vector
     * @param id the external ID to associate with this vector
     * @param metadata additional metadata to store
     * @return new store with the vector added
     */
    public ProximumVectorStore addWithId(float[] vector, Object id, Map<String, Object> metadata) {
        ensureInitialized();
        Object result = insertFn.invoke(clojureIndex, vector, id, toClojureMap(metadata));
        return new ProximumVectorStore(result, this.config);
    }

    /**
     * Search and return results with external IDs.
     * <p>Translates internal IDs to external IDs in results.
     * Vectors without external IDs will have null ID in results.</p>
     *
     * @param vector the query vector
     * @param k number of results to return
     * @return list of search results with external IDs
     */
    @SuppressWarnings("unchecked")
    public List<SearchResult> searchWithIds(float[] vector, int k) {
        ensureInitialized();
        Object results = searchFn.invoke(clojureIndex, vector, k);
        return translateResults((Iterable<Object>) results);
    }

    /**
     * Search with options and return results with external IDs.
     *
     * @param vector the query vector
     * @param k number of results to return
     * @param options search options (ef, min-similarity, etc.)
     * @return list of search results with external IDs
     */
    @SuppressWarnings("unchecked")
    public List<SearchResult> searchWithIds(float[] vector, int k, Map<String, Object> options) {
        ensureInitialized();
        Object results = searchFn.invoke(clojureIndex, vector, k, toClojureMap(options));
        return translateResults((Iterable<Object>) results);
    }

    /**
     * Look up the internal ID for an external ID.
     * <p>Returns null if the external ID is not found.</p>
     *
     * @param id the external ID
     * @return the internal ID, or null if not found
     */
    public Integer lookupId(Object id) {
        ensureInitialized();
        Object result = lookupInternalIdFn.invoke(clojureIndex, id);
        return result == null ? null : ((Number) result).intValue();
    }

    /**
     * Get vector by external ID.
     *
     * @param id the external ID
     * @return the vector, or null if not found
     */
    public float[] getVectorById(Object id) {
        ensureInitialized();
        return (float[]) getVectorFn.invoke(clojureIndex, id);
    }

    /**
     * Get metadata by external ID.
     *
     * @param id the external ID
     * @return the metadata map, or null if not found
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMetadataById(Object id) {
        ensureInitialized();
        return (Map<String, Object>) getMetadataFn.invoke(clojureIndex, id);
    }

    /**
     * Delete vector by external ID.
     *
     * @param id the external ID
     * @return new store with the vector deleted, or same store if ID not found
     */
    public ProximumVectorStore deleteById(Object id) {
        ensureInitialized();
        Object result = deleteFn.invoke(clojureIndex, id);
        return new ProximumVectorStore(result, this.config);
    }

    @SuppressWarnings("unchecked")
    private List<SearchResult> translateResults(Iterable<Object> results) {
        List<SearchResult> translated = new ArrayList<>();
        for (Object r : results) {
            Map<Object, Object> m = (Map<Object, Object>) r;
            int internalId = ((Number) m.get(kw("id"))).intValue();
            double distance = ((Number) m.get(kw("distance"))).doubleValue();

            // Get external ID from metadata
            Object meta = getMetadataFn.invoke(clojureIndex, internalId);
            Object externalId = null;
            if (meta != null) {
                Map<Object, Object> metaMap = (Map<Object, Object>) meta;
                externalId = metaMap.get(kw("external-id"));
            }

            // Include metadata in result if present
            Map<String, Object> metadata = null;
            Object metaObj = m.get(kw("metadata"));
            if (metaObj != null) {
                metadata = convertClojureMap((Map<Object, Object>) metaObj);
            }

            translated.add(new SearchResult(externalId, distance, metadata));
        }
        return translated;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> convertClojureMap(Map<Object, Object> clojureMap) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<Object, Object> e : clojureMap.entrySet()) {
            String key = e.getKey().toString();
            // Remove leading colon from keyword string representation
            if (key.startsWith(":")) {
                key = key.substring(1);
            }
            Object value = e.getValue();
            // Recursively convert nested maps
            if (value instanceof Map) {
                value = convertClojureMap((Map<Object, Object>) value);
            }
            result.put(key, value);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> convertClojureSeqToMapList(Object seq) {
        if (seq == null) {
            return null;
        }
        List<Map<String, Object>> result = new ArrayList<>();
        // Handle both Clojure seqs and Java iterables
        if (seq instanceof Iterable) {
            for (Object item : (Iterable<?>) seq) {
                if (item instanceof Map) {
                    result.add(convertClojureMap((Map<Object, Object>) item));
                }
            }
        }
        return result;
    }

    // Convert Java Set to Clojure persistent hash set
    private static Object toClojureSet(java.util.Set<?> javaSet) {
        if (javaSet == null) {
            return null;
        }
        IFn hashSet = Clojure.var("clojure.core", "hash-set");
        return hashSet.applyTo(clojure.lang.ArraySeq.create(javaSet.toArray()));
    }

    // Convert filter argument at runtime - handles Java Set, null, or pass-through
    private static Object convertFilterArg(Object arg) {
        if (arg == null) {
            return null;
        }
        if (arg instanceof java.util.Set) {
            return toClojureSet((java.util.Set<?>) arg);
        }
        // Already a Clojure set, fn, or ArrayBitSet - pass through
        return arg;
    }

    // Convert Clojure set of keywords to Java Set of Strings
    private static Set<String> convertKeywordSetToStrings(Set<Object> clojureSet) {
        if (clojureSet == null) {
            return null;
        }
        Set<String> result = new java.util.HashSet<>();
        for (Object item : clojureSet) {
            if (item instanceof clojure.lang.Keyword) {
                result.add(((clojure.lang.Keyword) item).getName());
            } else {
                result.add(String.valueOf(item));
            }
        }
        return result;
    }

    // ==========================================================================
    // Mutable Convenience Methods
    // ==========================================================================

    /**
     * Add a vector with auto-generated ID and return the ID (mutable convenience method).
     * <p>Thread-safe: synchronized on this instance.</p>
     *
     * @param vector the embedding vector
     * @return the generated UUID
     */
    public synchronized UUID addAndGetId(float[] vector) {
        UUID id = UUID.randomUUID();
        addAndGetId(vector, id);
        return id;
    }

    /**
     * Add a vector with specified ID (mutable convenience method).
     * <p>Mutates internal state. Thread-safe: synchronized on this instance.</p>
     *
     * @param vector the embedding vector
     * @param id the external ID
     * @return the ID (same as passed in)
     */
    public synchronized Object addAndGetId(float[] vector, Object id) {
        ensureInitialized();
        Object newIdx = insertFn.invoke(clojureIndex, vector, id);
        this.clojureIndex = newIdx;
        return id;
    }

    /**
     * Add a vector with ID and metadata (mutable convenience method).
     * <p>Thread-safe: synchronized on this instance.</p>
     *
     * @param vector the embedding vector
     * @param id the external ID
     * @param metadata additional metadata
     * @return the ID (same as passed in)
     */
    public synchronized Object addAndGetId(float[] vector, Object id, Map<String, Object> metadata) {
        ensureInitialized();
        Object newIdx = insertFn.invoke(clojureIndex, vector, id, toClojureMap(metadata));
        this.clojureIndex = newIdx;
        return id;
    }
}
