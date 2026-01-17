(ns proximum.protocols
  "Core protocols and multimethods for vector indices.

   This namespace defines the abstractions that all index types implement.
   It has no dependencies on specific implementations (HNSW, IVF, etc.),
   enabling index-agnostic persistence and compaction.

   Protocol Structure:
   - VectorIndex: Internal protocol for low-level operations (internal IDs)
   - NearestNeighborSearch: Domain-specific search protocol (external IDs)
   - IndexLifecycle: Lifecycle management (fork, sync!, flush!, close!)
   - IndexIntrospection: Type and config introspection

   The index implements Clojure collection protocols (IPersistentMap, ILookup, etc.)
   for map-like operations using external IDs as keys.")

;; -----------------------------------------------------------------------------
;; VectorIndex Protocol - Internal operations (internal node IDs)
;; This protocol is used by the implementation layer, not the public API.

(defprotocol VectorIndex
  "Internal protocol for vector index operations using internal node IDs.
   Public API uses collection protocols and NearestNeighborSearch instead.

   Note: fork, flush!, close!, index-type, index-config are in separate protocols."

  ;; Core operations (internal IDs)
  (insert [idx vector] [idx vector metadata]
    "Insert a vector with optional metadata. Returns new index with auto-assigned id.")
  (insert-batch [idx vectors] [idx vectors opts]
    "Insert multiple vectors. Options: {:metadata [m1 m2 ...], :parallelism n}.
     Returns new index.")
  (search [idx query k] [idx query k opts]
    "Search for k nearest neighbors. Returns seq of {:id :distance}.")
  (search-filtered [idx query k pred-fn] [idx query k pred-fn opts]
    "Search for k nearest neighbors with filtering.
     pred-fn: (fn [id metadata] boolean) - return true to include in results.
     Options are index-specific (e.g., :ef for HNSW).
     Returns seq of {:id :distance}.")
  (delete [idx id]
    "Delete vector by internal id. Returns new index.")

  ;; Accessors (internal IDs)
  (count-vectors [idx]
    "Get the number of vectors in the index.")
  (get-vector [idx id]
    "Retrieve a vector by its internal id.")
  (get-metadata [idx id]
    "Get metadata for a node by internal id. Returns nil if not set.")
  (set-metadata [idx id metadata]
    "Set/update metadata for a node by internal id. Returns new index.")
  (capacity [idx]
    "Get the maximum capacity of the index.")
  (remaining-capacity [idx]
    "Get remaining capacity (capacity - count-vectors)."))

;; -----------------------------------------------------------------------------
;; NearestNeighborSearch Protocol - Search operations (external IDs)

(defprotocol NearestNeighborSearch
  "Protocol for nearest neighbor search operations.
   Returns results with external IDs."

  (nearest [idx query k] [idx query k opts]
    "Find k nearest neighbors to query vector.
     Returns seq of {:id external-id :distance float}.
     Options: {:ef search-beam-width}")

  (nearest-filtered [idx query k pred-fn] [idx query k pred-fn opts]
    "Find k nearest neighbors with metadata filtering.
     pred-fn: (fn [external-id metadata] boolean) - return true to include.
     Returns seq of {:id external-id :distance float}."))

;; -----------------------------------------------------------------------------
;; IndexLifecycle Protocol - Lifecycle management

(defprotocol IndexLifecycle
  "Protocol for index lifecycle operations."

  (fork [idx]
    "Create a copy-on-write fork with shared structure. O(1) operation.")

  (sync! [idx]
    "Persist to durable storage, creating a commit. Returns updated index.")

  (flush! [idx]
    "Force pending writes to storage without creating a commit.")

  (close! [idx]
    "Close the index and release resources (mmap, file handles)."))

;; -----------------------------------------------------------------------------
;; IndexIntrospection Protocol - Type and config introspection

(defprotocol IndexIntrospection
  "Protocol for index type and configuration introspection."

  (index-type [idx]
    "Returns the index type keyword, e.g., :hnsw, :ivf.
     Used by restore-index multimethod to dispatch on type.")

  (index-config [idx]
    "Returns config map sufficient to recreate this index via create-index.
     Includes :type and all type-specific parameters."))

;; -----------------------------------------------------------------------------
;; IndexState Protocol - Access to index state

(defprotocol IndexState
  "Protocol for accessing index state (branch, commit, metrics).

   This protocol provides access to versioning and metrics state without
   exposing implementation details. Used by versioning, metrics, and GC."

  (storage [idx]
    "Get the CachedStorage wrapper for PSS operations, or nil if in-memory only.")

  (raw-storage [idx]
    "Get the raw Konserve store for branching/commit operations.
     Returns the underlying store used for branch snapshots and commits.
     Returns nil if in-memory only.")

  (current-branch [idx]
    "Get the current branch name keyword (e.g., :main, :feature-1).")

  (current-commit [idx]
    "Get the current commit UUID, or nil if no commits yet.")

  (vector-count-total [idx]
    "Get total vectors including deleted (for capacity calculation).")

  (deleted-count-total [idx]
    "Get number of deleted vectors (for compaction decision).")

  (mmap-dir [idx]
    "Get mmap directory for vector storage, or nil if not using mmap.
     Used by branching operations to copy mmap files.")

  (reflink-supported? [idx]
    "Check if filesystem supports reflink (copy-on-write) for O(1) branching.
     Returns true/false, or nil if not applicable.")

  (crypto-hash? [idx]
    "Check if index has content-addressed commit hashing enabled.
     When true, commits are identified by their merkle hash.")

  (external-id-index [idx]
    "Get the external-id index (maps external IDs to internal node IDs).
     This is an internal implementation detail exposed for debugging and
     advanced use cases. Returns an index-specific data structure.")

  (metadata-index [idx]
    "Get the metadata index (maps internal node IDs to metadata maps).
     This is an internal implementation detail exposed for debugging and
     advanced use cases. Returns an index-specific data structure.")

  (vector-storage [idx]
    "Get the underlying vector storage implementation.
     Returns index-specific vector storage (e.g., VectorStore for HNSW).
     Used by compaction, metrics, and serialization.")

  (edge-storage [idx]
    "Get the underlying edge/graph storage implementation.
     Returns index-specific edge storage (e.g., PersistentEdgeStore for HNSW).
     Returns nil for non-graph indices (e.g., flat indices).
     Used by compaction, metrics, and graph operations."))

;; -----------------------------------------------------------------------------
;; Algebraic Protocols - Operations with Laws
;;
;; These protocols represent fundamental capabilities with algebraic properties.
;; Each protocol defines laws that implementations must satisfy for correctness.

(defprotocol Snapshotable
  "Capability to extract serializable state for persistence.

   This is a read-only projection that extracts all mutable index state
   into a serializable snapshot for storage operations.

   Laws:
   1. Purity: (snapshot-graph-state idx) doesn't mutate idx
   2. Idempotence: Calling multiple times returns equivalent state
   3. Completeness: Snapshot contains all state needed to restore index"

  (snapshot-graph-state [idx]
    "Extract graph-specific state for persistence.

     Returns a map with index-type specific structure:
     - HNSW: {:entrypoint :max-level :deleted-nodes-bitset}
     - IVF: {:centroids :partition-assignments}

     This is used by sync! to create durable commits."))

(defprotocol Forkable
  "Capability to create copy-on-write independent branches.

   Forking creates a new index that shares structure with the original
   but can be modified independently. This is the foundation for git-like
   branching.

   Laws:
   1. Independence: (insert (fork idx) v) doesn't affect idx
   2. Preservation: (count-vectors (fork idx)) = (count-vectors idx)
   3. Structural sharing: fork is O(1) in graph size (COW semantics)
   4. Composition: branch! = fork × commit

   The three-step forking process:
   1. fork-graph-storage: Clone graph with structural sharing
   2. fork-vector-storage: Copy vector storage (with reflink if available)
   3. assemble-forked-index: Construct new index from forked components"

  (fork-graph-storage [idx]
    "Fork the graph/edge storage with structural sharing.

     Returns a new graph storage instance (e.g., PersistentEdgeStore)
     that shares immutable chunks with the original but can be mutated
     independently. Uses copy-on-write semantics.")

  (fork-vector-storage [idx branch-name]
    "Fork vector storage for a new branch.

     Copies the mmap file (using reflink if supported) and returns
     a new VectorStore instance pointing to the copied file.

     Args:
       branch-name - keyword name for new branch (e.g., :feature-x)

     Returns: new VectorStore instance")

  (assemble-forked-index [idx forked-vectors forked-graph new-branch new-commit-id]
    "Construct a new index from forked components.

     Takes the forked vector storage and graph storage and assembles
     them into a complete index instance with updated branch/commit metadata.

     Args:
       forked-vectors - VectorStore from fork-vector-storage
       forked-graph - Graph storage from fork-graph-storage
       new-branch - Branch name keyword
       new-commit-id - UUID for the new commit

     Returns: new index instance"))

(defprotocol GraphMetrics
  "Graph-specific introspection for monitoring and decision-making.

   These metrics are used for:
   - Monitoring index health
   - Deciding when to compact
   - Verifying graph connectivity

   Laws:
   1. Consistency: edge-count should match actual edges in graph
   2. Monotonicity: edge-count only increases with insertions (until compaction)
   3. Bounds: 0 <= edge-count <= vector-count * expected-connectivity"

  (edge-count [idx]
    "Total number of edges in the graph.

     For HNSW, this counts bidirectional edges across all layers.
     Returns Long.")

  (graph-entrypoint [idx]
    "Entry point node ID for graph traversal.

     For HNSW, this is the node at the highest level.
     Returns integer node ID, or nil if graph is empty.")

  (graph-max-level [idx]
    "Maximum level in the hierarchical graph.

     For HNSW, this is the highest layer that contains nodes.
     Returns integer level (0-based), or 0 if graph is empty.")

  (expected-connectivity [idx]
    "Expected number of edges per node.

     For HNSW, this is the M parameter.
     Used to calculate connectivity ratios for health monitoring.
     Returns integer."))

;; -----------------------------------------------------------------------------
;; create-index Multimethod

(defmulti create-index
  "Create a new vector index.

   Dispatches on :type key in config map.
   Available types: :hnsw (more coming)

   Common options:
     :dim            - Vector dimensionality (required)
     :distance       - Distance metric :euclidean, :cosine, :inner-product (default :euclidean)
     :store-config   - Konserve store config map (durable) (must include :id)
     :store          - Optional already-connected Konserve store instance
     :mmap-dir       - Directory for local mmap cache files (durable branching)
     :mmap-path      - Optional explicit mmap file path override
     :crypto-hash?   - Enable content-based hashing for auditability (default false)

   HNSW-specific options:
     :M              - Max neighbors per node (default 16)
     :ef-construction - Beam width during build (default: auto)
     :ef-search      - Default beam width during search (default: auto)
     :capacity       - Max vectors to store (default 10000000)
     :max-levels     - Max hierarchy levels (nil=unlimited, 0=flat)
     :chunk-size     - Vectors per konserve chunk (default 1000)
     :cache-size     - LRU cache size for PSS nodes (default 10000)

   Returns:
     VectorIndex implementation"
  (fn [config] (:type config)))

;; -----------------------------------------------------------------------------
;; restore-index Multimethod

(defmulti restore-index
  "Restore an index from a stored snapshot.

   Dispatches on :index-type key in snapshot.
   Called by connect/connect-commit to load indices from storage.

   Args:
     snapshot   - Map containing stored index state (from sync!)
     edge-store - Konserve store instance
     opts       - Options map:
                  :mmap-dir    - Directory for branch mmap files
                  :mmap-path   - Explicit mmap file path override
                  :cache-size  - LRU cache size for PSS nodes

   Returns:
     VectorIndex implementation restored to the snapshot state"
  (fn [snapshot _edge-store _opts] (:index-type snapshot)))
