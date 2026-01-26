(ns proximum.specification
  "Shared specification for different bindings.

   This namespace holds all semantic information such that individual bindings
   (Clojure API, Java API, HTTP routes) can be automatically derived from it.

   Following the Datahike pattern - the spec is purely declarative about semantics,
   not about how each binding should look. Names, routes, and method signatures
   are derived via conventions in the codegen modules.

   Each operation has:
     :args   - malli schema for function arguments (using :=> [:cat ...] form)
     :ret    - malli schema for return value
     :doc    - documentation string
     :impl   - symbol pointing to implementation function
     :referentially-transparent? - true if pure (no side effects, deterministic)
     :supports-remote? - true if can be exposed via HTTP/remote API"
  #?(:clj (:require [malli.core :as m]
                    [malli.util :as mu]
                    [malli.error :as me]
                    [clojure.string :as str])
     :cljs (:require [malli.core :as m]
                     [malli.util :as mu]
                     [malli.error :as me]
                     [clojure.string :as str])))

;; =============================================================================
;; Name Derivation Helpers
;; =============================================================================

(defn ->url
  "Turns an API endpoint name into a URL path segment.
   Removes ? and ! suffixes, uses kebab-case as-is."
  [op-name]
  (-> (name op-name)
      (str/replace #"[?!]$" "")))

(defn ->java-method
  "Derives Java method name from Clojure operation name.
   - kebab-case → camelCase
   - ? suffix → is prefix (Java boolean convention)
   - ! suffix → removed (side-effect marker not needed in Java)

   Examples:
     :insert → \"insert\"
     :insert-batch → \"insertBatch\"
     :count-vectors → \"countVectors\"
     :sync! → \"sync\"
     :needs-compaction? → \"isNeedsCompaction\"
     :crypto-hash? → \"isCryptoHash\""
  [op-name]
  (let [s (name op-name)
        predicate? (str/ends-with? s "?")
        clean (str/replace s #"[!?]$" "")
        parts (str/split clean #"-")
        camel (apply str (first parts)
                     (map str/capitalize (rest parts)))]
    (if predicate?
      (str "is" (str/upper-case (subs camel 0 1)) (subs camel 1))
      camel)))

(defn args->arglist
  "Extract arglist from malli function schema for defn metadata.
   Handles :=> schemas with :cat/:alt for arguments."
  [schema]
  (when schema
    (let [schema-form (if (m/schema? schema) (m/form schema) schema)]
      (cond
        ;; Function schema [:=> [:cat ...] output]
        (and (sequential? schema-form)
             (= :=> (first schema-form)))
        (let [[_ input-schema _] schema-form
              input-form (if (m/schema? input-schema)
                           (m/form input-schema)
                           input-schema)]
          (cond
            ;; [:cat Type1 Type2 ...]
            (and (sequential? input-form)
                 (= :cat (first input-form)))
            (let [args (rest input-form)]
              (list (vec (map-indexed
                          (fn [i _] (symbol (str "arg" i)))
                          args))))

            ;; [:alt [:cat ...] [:cat ...]] - multiple arities
            (and (sequential? input-form)
                 (= :alt (first input-form)))
            (for [alt-form (rest input-form)]
              (let [cat-form (if (and (sequential? alt-form)
                                      (= :cat (first alt-form)))
                               (rest alt-form)
                               [alt-form])]
                (vec (map-indexed
                      (fn [i _] (symbol (str "arg" i)))
                      cat-form))))

            :else
            '([& args])))

        :else
        '([& args])))))

;; =============================================================================
;; Schema Definitions (Malli)
;; =============================================================================

(def DistanceMetric
  "Distance metrics for vector similarity."
  [:enum :euclidean :cosine :inner-product])

;; =============================================================================
;; Semantic ID Types
;; =============================================================================

(def InternalId
  "Internal vector ID (0-indexed, up to capacity).
   Maps to long in Java for large index support."
  :int)

(def ExternalId
  "External/document ID provided by user.
   Can be any serializable value (Long for Datahike, String, UUID, etc.).
   Maps to Object in Java."
  :any)

(def CommitId
  "Commit identifier (UUID)."
  :uuid)

(def BranchName
  "Branch name for versioning.
   Keyword in Clojure, String in Java."
  :keyword)

;; =============================================================================
;; Data Types
;; =============================================================================

(def Vector
  "A vector - float array."
  [:fn {:error/message "must be a float array"}
   #?(:clj #(instance? (Class/forName "[F") %)
      :cljs #(instance? js/Float32Array %))])

(def Metadata
  "Arbitrary metadata map associated with vectors."
  [:map-of :keyword :any])

(def SearchResult
  "A single search result with distance and optional metadata."
  [:map
   [:id ExternalId]
   [:distance :double]
   [:metadata {:optional true} [:maybe Metadata]]])

(def SearchOptions
  "Options for search operations."
  [:map {:closed false}
   [:ef {:optional true} pos-int?]
   [:min-similarity {:optional true} [:double {:min 0 :max 1}]]
   [:timeout-ms {:optional true} pos-int?]
   [:patience {:optional true} pos-int?]
   [:patience-saturation {:optional true} [:double {:min 0 :max 1}]]])

(def StoreConfig
  "Konserve store configuration."
  [:map {:closed false}
   [:backend [:enum :file :mem :s3]]
   [:path {:optional true} :string]
   [:id :uuid]])

(def AsyncResult
  "Wrapper for operations that return core.async channels.
   [:async T] means 'returns channel that delivers T'.
   Maps to CompletableFuture<T> in Java.

   Example:
     [:async VectorIndex] -> CompletableFuture<ProximumVectorStore>
     [:async [:set :any]] -> CompletableFuture<Set<Object>>"
  [:fn {:error/message "async result wrapper"}
   (fn [_x] true)])  ;; Semantic marker, always valid

(def ConnectOptions
  "Options for connect operations."
  [:map {:closed false}
   [:branch {:optional true} :keyword]
   [:cache-size {:optional true} pos-int?]
   [:mmap-dir {:optional true} :string]
   [:mmap-path {:optional true} :string]
   [:store {:optional true} :any]
   [:sync? {:optional true} :boolean]])

(def HnswConfig
  "Configuration for HNSW index type."
  [:map {:closed false}
   [:type [:= :hnsw]]
   [:dim {:doc "Vector dimensions (e.g., 1536 for OpenAI embeddings)."} pos-int?]
   [:M {:optional true :default 16
        :doc "Max neighbors per node. Higher improves recall but increases memory and build time."}
    pos-int?]
   [:ef-construction {:optional true :default 200
                      :doc "Beam width during index construction. Higher improves quality but slows builds."}
    pos-int?]
   [:ef-search {:optional true :default 50
                :doc "Beam width during search. Higher improves recall but increases latency. Can override per-query."}
    pos-int?]
   [:capacity {:optional true :default 10000000
               :doc "Maximum number of vectors the index can hold."}
    pos-int?]
   [:distance {:optional true :default :euclidean
               :doc "Distance metric: :euclidean, :cosine, or :inner-product."}
    DistanceMetric]
   [:store-config {:optional true
                   :doc "Konserve store configuration map for custom backends."}
    :map]
   [:mmap-dir {:optional true
               :doc "Directory for memory-mapped vector storage. Defaults to store-config path."}
    :string]
   [:mmap-path {:optional true
                :doc "Explicit path for memory-mapped file (advanced)."}
    :string]
   [:branch {:optional true :default :main
             :doc "Initial branch name for versioning."}
    :keyword]
   [:crypto-hash? {:optional true :default false
                   :doc "Enable cryptographic commit hashing for auditability."}
    :boolean]
   [:chunk-size {:optional true :default 1000
                 :doc "Edge storage chunk size. Affects memory/persistence granularity."}
    pos-int?]
   [:cache-size {:optional true :default 10000
                 :doc "Number of edge chunks to cache in memory."}
    pos-int?]
   [:max-levels {:optional true
                 :doc "Maximum HNSW levels (auto-calculated if not set)."}
    [:maybe nat-int?]]])

(def IndexConfig
  "Polymorphic index configuration - dispatches on :type."
  [:multi {:dispatch :type}
   [:hnsw HnswConfig]])

;; Opaque type - actual implementation varies
(def VectorIndex
  "Reference to a vector index instance."
  :any)

(def CompactTarget
  "Target configuration for compaction."
  [:map
   [:store-config StoreConfig]
   [:mmap-dir :string]])

;; =============================================================================
;; API Specification
;; =============================================================================
;;
;; Each operation has ONLY semantic information:
;;   :args   - malli schema for function arguments
;;   :ret    - malli schema for return value
;;   :doc    - documentation string
;;   :impl   - symbol pointing to implementation function
;;   :referentially-transparent? - true if pure (deterministic, no side effects)
;;   :supports-remote? - true if can be exposed via HTTP/remote API

(def api-specification
  "Complete API specification for proximum.

   Operation names become:
   - Clojure function names (as-is)
   - Java method names (via ->java-method: kebab→camelCase, remove !?)
   - HTTP routes (via ->url: kebab-case path segments)"

  '{;; =========================================================================
    ;; Index Lifecycle
    ;; =========================================================================

    create-index
    {:args [:=> [:cat IndexConfig] VectorIndex]
     :ret  VectorIndex
     :doc  "Create a new vector index with the given configuration.
Dispatches on :type to create appropriate index implementation.

Example:
  (create-index {:type :hnsw
                 :dim 128
                 :M 16
                 :store-config {:backend :file :path \"/data/idx\" :id (random-uuid)}
                 :mmap-dir \"/data/mmap\"})"
     :impl proximum.protocols/create-index
     :referentially-transparent? false
     :supports-remote? true}

    restore-index
    {:args [:=> [:cat :map :any :map] VectorIndex]
     :ret  VectorIndex
     :doc  "Restore an index from a stored snapshot.
Dispatches on :index-type key in snapshot.
Called by connect/connect-commit to load indices from storage."
     :impl proximum.protocols/restore-index
     :referentially-transparent? false
     :supports-remote? false}

    load
    {:args [:=> [:cat StoreConfig [:? ConnectOptions]] VectorIndex]
     :ret  VectorIndex
     :doc  "Load an existing index from storage.
Loads the latest commit from the specified branch (default: :main).

Example:
  (load {:backend :file :path \"/data/idx\" :id #uuid \"...\"} {:branch :main})"
     :impl proximum.writing/load
     :referentially-transparent? false
     :supports-remote? true}

    load-commit
    {:args [:=> [:cat StoreConfig CommitId [:? ConnectOptions]] VectorIndex]
     :ret  VectorIndex
     :doc  "Load a historical commit by ID (time-travel query).

Example:
  (load-commit store-config #uuid \"550e8400-...\" {:branch :main})"
     :impl proximum.writing/load-commit
     :referentially-transparent? false
     :supports-remote? true}

    close!
    {:args [:=> [:cat VectorIndex] [:async :nil]]
     :ret  [:async :nil]
     :doc  "Close the index and release resources (mmap, caches, stores).
Returns channel that delivers nil when cleanup completes.
Clojure: can ignore return value (fire-and-forget).
Java: close() blocks until cleanup is complete."
     :impl proximum.protocols/close!
     :referentially-transparent? false
     :supports-remote? false}

    ;; =========================================================================
    ;; Core Operations
    ;; =========================================================================

    insert
    {:args [:=> [:cat VectorIndex Vector ExternalId [:? Metadata]] VectorIndex]
     :ret  VectorIndex
     :doc  "Insert a vector with an ID and optional metadata. Returns new index.
ID can be any value (Long, String, UUID, etc.). Pass nil to auto-generate UUID.
This is a pure operation - no I/O until sync! is called.

Example:
  (insert idx (float-array [1.0 2.0 3.0]) 123)
  (insert idx (float-array [1.0 2.0 3.0]) \"doc-abc\" {:category :science})
  (insert idx vec nil)  ; auto-generates UUID"
     :impl proximum.api-impl/insert
     :referentially-transparent? true
     :supports-remote? true}

    insert-batch
    {:args [:=> [:cat VectorIndex [:sequential Vector] [:sequential ExternalId] [:? :map]] VectorIndex]
     :ret  VectorIndex
     :doc  "Insert multiple vectors with IDs efficiently.
IDs list must match vectors list length. Use nil for auto-generated UUIDs.
Options: {:metadata [m1 m2 ...], :parallelism n}

Example:
  (insert-batch idx [vec1 vec2] [id1 id2])
  (insert-batch idx [vec1 vec2] [nil nil] {:metadata [m1 m2]})"
     :impl proximum.api-impl/insert-batch
     :referentially-transparent? true
     :supports-remote? true}

    search
    {:args [:=> [:cat VectorIndex Vector pos-int? [:? SearchOptions]]
            [:sequential SearchResult]]
     :ret  [:sequential SearchResult]
     :doc  "Search for k nearest neighbors.
Returns sequence of {:id :distance} sorted by distance (ascending).
IDs are external IDs as provided during insert.

Example:
  (search idx query-vec 10 {:ef 100})"
     :impl proximum.api-impl/search
     :referentially-transparent? true
     :supports-remote? true}

    search-filtered
    {:args [:=> [:cat VectorIndex Vector pos-int? :any [:? SearchOptions]]
            [:sequential SearchResult]]
     :ret  [:sequential SearchResult]
     :doc  "Search with filtering predicate or ID set.
Filter can be:
  - (fn [id metadata] boolean) - predicate receives external ID
  - Set of allowed external IDs

Example:
  (search-filtered idx query 10 #{\"doc-1\" \"doc-2\"})"
     :impl proximum.api-impl/search-filtered
     :referentially-transparent? true
     :supports-remote? true}

    search-with-metadata
    {:args [:=> [:cat VectorIndex Vector pos-int? [:? SearchOptions]]
            [:sequential SearchResult]]
     :ret  [:sequential SearchResult]
     :doc  "Search and include metadata in results.
Returns seq of {:id :distance :metadata}.

Example:
  (search-with-metadata idx query 10)"
     :impl proximum.api-impl/search-with-metadata
     :referentially-transparent? true
     :supports-remote? true}

    delete
    {:args [:=> [:cat VectorIndex ExternalId] VectorIndex]
     :ret  VectorIndex
     :doc  "Soft-delete vector by ID. Returns new index.
Vector is marked deleted but space not reclaimed until compact."
     :impl proximum.api-impl/delete
     :referentially-transparent? true
     :supports-remote? true}

    fork
    {:args [:=> [:cat VectorIndex] VectorIndex]
     :ret  VectorIndex
     :doc  "Create O(1) fork with structural sharing.
Both indices share immutable structure, diverge on writes."
     :impl proximum.protocols/fork
     :referentially-transparent? true
     :supports-remote? false}

    ;; =========================================================================
    ;; Accessors
    ;; =========================================================================

    count-vectors
    {:args [:=> [:cat VectorIndex] nat-int?]
     :ret  nat-int?
     :doc  "Total vectors in index (includes soft-deleted)."
     :impl proximum.protocols/count-vectors
     :referentially-transparent? true
     :supports-remote? true}

    get-vector
    {:args [:=> [:cat VectorIndex ExternalId] [:maybe Vector]]
     :ret  [:maybe Vector]
     :doc  "Retrieve vector by ID. Returns nil if not found or deleted."
     :impl proximum.api-impl/get-vector
     :referentially-transparent? true
     :supports-remote? true}

    get-metadata
    {:args [:=> [:cat VectorIndex ExternalId] [:maybe Metadata]]
     :ret  [:maybe Metadata]
     :doc  "Get metadata map for vector ID. Returns nil if not found."
     :impl proximum.api-impl/get-metadata
     :referentially-transparent? true
     :supports-remote? true}

    lookup-internal-id
    {:args [:=> [:cat VectorIndex ExternalId] [:maybe InternalId]]
     :ret  [:maybe InternalId]
     :doc  "Look up internal node ID for an external ID. Returns nil if not found."
     :impl proximum.api-impl/lookup-internal-id
     :referentially-transparent? true
     :supports-remote? true}

    with-metadata
    {:args [:=> [:cat VectorIndex ExternalId Metadata] VectorIndex]
     :ret  VectorIndex
     :doc  "Associate/update metadata for a vector. Returns new index.

Example:
  (with-metadata idx \"doc-123\" {:category :science})"
     :impl proximum.api-impl/with-metadata
     :referentially-transparent? true
     :supports-remote? true}

    capacity
    {:args [:=> [:cat VectorIndex] nat-int?]
     :ret  nat-int?
     :doc  "Maximum capacity of the index."
     :impl proximum.protocols/capacity
     :referentially-transparent? true
     :supports-remote? true}

    remaining-capacity
    {:args [:=> [:cat VectorIndex] nat-int?]
     :ret  nat-int?
     :doc  "Remaining capacity (capacity - count-vectors)."
     :impl proximum.protocols/remaining-capacity
     :referentially-transparent? true
     :supports-remote? true}

    index-type
    {:args [:=> [:cat VectorIndex] :keyword]
     :ret  :keyword
     :doc  "Returns the index type keyword (e.g., :hnsw)."
     :impl proximum.protocols/index-type
     :referentially-transparent? true
     :supports-remote? true}

    index-config
    {:args [:=> [:cat VectorIndex] :map]
     :ret  :map
     :doc  "Returns config map that can recreate this index via create-index."
     :impl proximum.protocols/index-config
     :referentially-transparent? true
     :supports-remote? true}

    ;; =========================================================================
    ;; Persistence
    ;; =========================================================================

    sync!
    {:args [:=> [:cat VectorIndex [:? :map]] [:async VectorIndex]]
     :ret  [:async VectorIndex]
     :doc  "Persist current state to durable storage, creating a commit.
Returns channel that delivers updated index when all pending writes complete.

In Clojure: use <! in go-block or <!! to block.
In Java: returns CompletableFuture<ProximumVectorStore>."
     :impl proximum.protocols/sync!
     :referentially-transparent? false
     :supports-remote? true}

    flush!
    {:args [:=> [:cat VectorIndex] [:async VectorIndex]]
     :ret  [:async VectorIndex]
     :doc  "Force pending writes to storage without creating commit.
Returns channel that delivers updated index when writes complete."
     :impl proximum.protocols/flush!
     :referentially-transparent? false
     :supports-remote? false}

    branch!
    {:args [:=> [:cat VectorIndex BranchName] VectorIndex]
     :ret  VectorIndex
     :doc  "Create a new branch from current state.
Index must be synced first. Creates reflinked mmap copy."
     :impl proximum.versioning/branch!
     :referentially-transparent? false
     :supports-remote? true}

    branches
    {:args [:=> [:cat VectorIndex] [:set BranchName]]
     :ret  [:set BranchName]
     :doc  "List all branches in the store."
     :impl proximum.versioning/branches
     :referentially-transparent? false
     :supports-remote? true}

    get-branch
    {:args [:=> [:cat VectorIndex] BranchName]
     :ret  BranchName
     :doc  "Get current branch name for this index."
     :impl proximum.versioning/get-branch
     :referentially-transparent? true
     :supports-remote? true}

    get-commit-id
    {:args [:=> [:cat VectorIndex] [:maybe CommitId]]
     :ret  [:maybe CommitId]
     :doc  "Get the commit ID for current branch."
     :impl proximum.versioning/get-commit-id
     :referentially-transparent? true
     :supports-remote? true}

    ;; =========================================================================
    ;; Compaction
    ;; =========================================================================

    compact
    {:args [:=> [:cat VectorIndex CompactTarget [:? :map]] VectorIndex]
     :ret  VectorIndex
     :doc  "Create compacted copy with only live vectors.
Target: {:store-config {...} :mmap-dir \"...\"}
Options: {:parallelism n}"
     :impl proximum.compaction/compact
     :referentially-transparent? false
     :supports-remote? true}

    start-online-compaction
    {:args [:=> [:cat VectorIndex CompactTarget [:? :map]] :any]
     :ret  :any
     :doc  "Start background compaction with zero downtime.
Returns CompactionState wrapper for use during compaction."
     :impl proximum.compaction/start-online-compaction
     :referentially-transparent? false
     :supports-remote? true}

    finish-online-compaction!
    {:args [:=> [:cat :any] [:async VectorIndex]]
     :ret  [:async VectorIndex]
     :doc  "Finish online compaction and return new index (async)."
     :impl proximum.compaction/finish-online-compaction!
     :referentially-transparent? false
     :supports-remote? true}

    abort-online-compaction!
    {:args [:=> [:cat :any] VectorIndex]
     :ret  VectorIndex
     :doc  "Abort online compaction and return source index."
     :impl proximum.compaction/abort-online-compaction!
     :referentially-transparent? false
     :supports-remote? false}

    compaction-progress
    {:args [:=> [:cat :any] :map]
     :ret  :map
     :doc  "Get current compaction progress.
Returns {:copying? bool :finished? bool :delta-count N :mapped-ids N}"
     :impl proximum.compaction/compaction-progress
     :referentially-transparent? true
     :supports-remote? true}

    ;; =========================================================================
    ;; Maintenance
    ;; =========================================================================

    gc!
    {:args [:=> [:cat VectorIndex [:? :any] [:? :map]] [:async [:set :any]]]
     :ret  [:async [:set :any]]
     :doc  "Garbage collect unreachable data from storage.
Returns channel that delivers set of deleted keys when GC completes.
Removes commits older than remove-before date."
     :impl proximum.gc/gc!
     :referentially-transparent? false
     :supports-remote? true}

    history
    {:args [:=> [:cat VectorIndex] [:sequential :map]]
     :ret  [:sequential :map]
     :doc  "Get commit history for current branch (most recent first)."
     :impl proximum.versioning/history
     :referentially-transparent? false
     :supports-remote? true}

    parents
    {:args [:=> [:cat VectorIndex] [:set :any]]
     :ret  [:set :any]
     :doc  "Get parent commit IDs. Like `git rev-parse HEAD^@`.
First commit has #{:main} or similar branch keyword as parent."
     :impl proximum.versioning/parents
     :referentially-transparent? false
     :supports-remote? true}

    ancestors
    {:args [:=> [:alt [:cat VectorIndex] [:cat VectorIndex CommitId]] [:sequential CommitId]]
     :ret  [:sequential CommitId]
     :doc  "Get all ancestor commit IDs, most recent first. Like `git rev-list HEAD`."
     :impl proximum.versioning/ancestors
     :referentially-transparent? false
     :supports-remote? true}

    ancestor?
    {:args [:=> [:cat VectorIndex CommitId CommitId] :boolean]
     :ret  :boolean
     :doc  "Check if first commit is ancestor of second. Like `git merge-base --is-ancestor`."
     :impl proximum.versioning/ancestor?
     :referentially-transparent? false
     :supports-remote? true}

    common-ancestor
    {:args [:=> [:cat VectorIndex CommitId CommitId] [:maybe CommitId]]
     :ret  [:maybe CommitId]
     :doc  "Find common ancestor of two commits. Like `git merge-base`."
     :impl proximum.versioning/common-ancestor
     :referentially-transparent? false
     :supports-remote? true}

    commit-info
    {:args [:=> [:cat VectorIndex CommitId] [:maybe :map]]
     :ret  [:maybe :map]
     :doc  "Get metadata for a commit. Like `git show --stat`.
Returns {:commit-id :parents :created-at :branch :vector-count :deleted-count}"
     :impl proximum.versioning/commit-info
     :referentially-transparent? false
     :supports-remote? true}

    commit-graph
    {:args [:=> [:cat VectorIndex] :map]
     :ret  :map
     :doc  "Get full commit DAG for visualization.
Returns {:nodes {id {:parents :created-at :branch}} :branches {:main id} :roots #{id}}"
     :impl proximum.versioning/commit-graph
     :referentially-transparent? false
     :supports-remote? true}

    delete-branch!
    {:args [:=> [:cat VectorIndex BranchName] VectorIndex]
     :ret  VectorIndex
     :doc  "Delete a branch. Like `git branch -d`. Cannot delete current or :main."
     :impl proximum.versioning/delete-branch!
     :referentially-transparent? false
     :supports-remote? true}

    reset!
    {:args [:=> [:cat VectorIndex CommitId] VectorIndex]
     :ret  VectorIndex
     :doc  "Reset current branch to a different commit. Like `git reset --hard`.
WARNING: Commits after target become unreachable."
     :impl proximum.versioning/reset!
     :referentially-transparent? false
     :supports-remote? true}

    ;; =========================================================================
    ;; Metrics & Health
    ;; =========================================================================

    index-metrics
    {:args [:=> [:cat VectorIndex [:? :map]] :map]
     :ret  :map
     :doc  "Get comprehensive index health metrics.
Returns: {:vector-count :deleted-count :live-count :deletion-ratio
          :needs-compaction? :capacity :utilization :edge-count
          :avg-edges-per-node :branch :commit-id :cache-hits :cache-misses}"
     :impl proximum.metrics/index-metrics
     :referentially-transparent? true
     :supports-remote? true}

    needs-compaction?
    {:args [:=> [:cat VectorIndex [:? :double]] :boolean]
     :ret  :boolean
     :doc  "Check if index needs compaction (deletion ratio > threshold)."
     :impl proximum.metrics/needs-compaction?
     :referentially-transparent? true
     :supports-remote? true}

    ;; =========================================================================
    ;; Crypto/Auditability
    ;; =========================================================================

    get-commit-hash
    {:args [:=> [:cat VectorIndex] [:maybe CommitId]]
     :ret  [:maybe CommitId]
     :doc  "Get SHA-512 based commit hash (if crypto-hash? enabled)."
     :impl proximum.crypto/get-commit-hash
     :referentially-transparent? true
     :supports-remote? true}

    crypto-hash?
    {:args [:=> [:cat VectorIndex] :boolean]
     :ret  :boolean
     :doc  "Check if crypto-hash mode is enabled."
     :impl proximum.crypto/crypto-hash?
     :referentially-transparent? true
     :supports-remote? true}

    hash-index-commit
    {:args [:=> [:cat VectorIndex] :uuid]
     :ret  :uuid
     :doc  "Compute combined SHA-512 based index commit hash.
Uses content-addressable hashing of index state."
     :impl proximum.crypto/hash-index-commit
     :referentially-transparent? true
     :supports-remote? false}

    verify-from-cold
    {:args [:=> [:cat StoreConfig [:? :keyword] [:? :map]] :map]
     :ret  :map
     :doc  "Verify index integrity from cold storage.
Returns: {:valid? bool :vectors-verified N :edges-verified N}"
     :impl proximum.crypto/verify-from-cold
     :referentially-transparent? false
     :supports-remote? true}

    ;; =========================================================================
    ;; HNSW-specific utilities
    ;; =========================================================================

    recommended-ef-construction
    {:args [:=> [:cat pos-int? pos-int?] pos-int?]
     :ret  pos-int?
     :doc  "Calculate recommended ef-construction based on expected index size and M."
     :impl proximum.hnsw/recommended-ef-construction
     :referentially-transparent? true
     :supports-remote? false}

    recommended-ef-search
    {:args [:=> [:cat pos-int? pos-int?] pos-int?]
     :ret  pos-int?
     :doc  "Calculate recommended ef-search for given k and index size."
     :impl proximum.hnsw/recommended-ef-search
     :referentially-transparent? true
     :supports-remote? false}

    ;; =========================================================================
    ;; Utilities
    ;; =========================================================================

    make-id-filter
    {:args [:=> [:cat nat-int? [:sequential :int]] :any]
     :ret  :any
     :doc  "Create reusable ID filter (ArrayBitSet) for search-filtered.
More efficient than passing Set for repeated searches.

Example:
  (let [filter (make-id-filter (count-vectors idx) #{1 5 10})]
    (search-filtered idx query1 10 filter)
    (search-filtered idx query2 10 filter))"
     :impl proximum.api-impl/make-id-filter
     :referentially-transparent? true
     :supports-remote? false}})

;; =============================================================================
;; Query Helpers
;; =============================================================================

(defn pure-operations
  "Get all operations that are referentially transparent (pure)."
  []
  (->> api-specification
       (filter (fn [[_ v]] (:referentially-transparent? v)))
       (into {})))

(defn io-operations
  "Get all operations with side effects (not referentially transparent)."
  []
  (->> api-specification
       (filter (fn [[_ v]] (not (:referentially-transparent? v))))
       (into {})))

(defn remote-operations
  "Get all operations that support remote/HTTP access."
  []
  (->> api-specification
       (filter (fn [[_ v]] (:supports-remote? v)))
       (into {})))

(defn local-only-operations
  "Get all operations that are local-only (not remotely accessible)."
  []
  (->> api-specification
       (filter (fn [[_ v]] (not (:supports-remote? v))))
       (into {})))

(defn operation-doc
  "Get documentation for a specific operation."
  [op-key]
  (get-in api-specification [op-key :doc]))

(defn operation-impl
  "Get implementation symbol for a specific operation."
  [op-key]
  (get-in api-specification [op-key :impl]))

;; =============================================================================
;; Validation
;; =============================================================================

(defn validate-index-config
  "Validate an index configuration map."
  [config]
  (m/validate IndexConfig config))

(defn explain-index-config
  "Explain validation errors for an index configuration."
  [config]
  (me/humanize (m/explain IndexConfig config)))

(defn validate-search-options
  "Validate search options map."
  [opts]
  (m/validate SearchOptions opts))

(defn validate-store-config
  "Validate store configuration map."
  [config]
  (m/validate StoreConfig config))
