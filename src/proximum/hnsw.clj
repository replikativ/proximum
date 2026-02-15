(ns proximum.hnsw
  "HNSW (Hierarchical Navigable Small World) index implementation.

   This namespace contains:
   - HnswIndex record definition
   - VectorIndex protocol implementation for HnswIndex
   - create-index and restore-index multimethods for :hnsw type
   - HNSW-specific operations (bulk-insert!, hnsw-search)

   Edge Storage:
   - PersistentEdgeStore for Java-based parallel insert with O(1) fork

   Core functions:
   - create-edge-store: Create edge storage
   - bulk-insert!: Parallel batch insert
   - hnsw-search: k-NN search with SIMD distance"
  (:require [proximum.protocols :as p]
            [proximum.vectors :as vectors]
            [proximum.storage :as storage]
            [proximum.edges :as edges]
            [proximum.metadata :as meta]
            [proximum.writing :as writing]
            [proximum.crypto :as crypto]
            [proximum.logging :as log]
            [org.replikativ.persistent-sorted-set :as pss]
            [konserve.core :as k]
            [clojure.core.async :as a])
  (:import [proximum.internal PersistentEdgeStore HnswInsert HnswSearch ArrayBitSet]
           [java.lang.foreign MemorySegment]))

;; -----------------------------------------------------------------------------
;; HnswIndex Type
;;
;; A persistent vector index implementing map-like collection protocols.
;; Key = external-id, Value = vector (float array)
;;
;; State map contains:
;;   :vectors        - VectorStore (konserve + mmap)
;;   :pes-edges      - PersistentEdgeStore (Java, fast CoW)
;;   :metadata       - PSS: {node-id -> metadata-map}
;;   :external-id-index - PSS: {external-id -> node-id}
;;   :M              - max neighbors per node (upper layers)
;;   :M0             - max neighbors at layer 0 (typically 2*M)
;;   :ef-construction - beam width during construction
;;   :ef-search      - default beam width during search
;;   :ml             - 1/ln(M) - level multiplier
;;   :max-levels     - max allowed levels (nil=unlimited, 0=flat single-layer)
;;   :dim            - vector dimensionality
;;   :distance       - keyword: :euclidean, :cosine, or :inner-product
;;   :distance-type  - int: 0=euclidean, 1=cosine, 2=inner-product
;;   :storage        - CachedStorage for PSS nodes
;;   :edge-store     - Konserve store for edges
;;   :address-map    - map for COW edge persistence
;;   :pending-edge-writes - atom for async edge writes
;;   :branch         - current branch keyword
;;   :commit-id      - UUID of current commit
;;   :vector-count   - total vectors inserted
;;   :deleted-count  - deleted vector count
;;   :mmap-dir       - directory for branch mmap files
;;   :reflink-supported? - filesystem reflink support
;;   :crypto-hash?   - enable content-based hashing

(declare ->HnswIndex)
(declare ->TransientHnswIndex)

;; Helper constructor to create HnswIndex from state map
(defn- make-hnsw-index
  "Create HnswIndex from state map, extracting hot-path fields for direct access."
  ([state]
   (make-hnsw-index state nil))
  ([state meta]
   (->HnswIndex
    (:vectors state)
    (:pes-edges state)
    (int (:dim state))
    (int (:distance-type state))
    (dissoc state :vectors :pes-edges :dim :distance-type)
    meta)))

(defn- update-hnsw-index
  "Update HnswIndex with new field values. Takes current index and map of updates.
   Creates a fresh :pending-edge-writes atom to ensure index versions don't share mutable state.
   Invalidates :commit-id unless explicitly set in updates (mutations invalidate, sync! sets it)."
  [idx updates]
  (let [new-vectors (get updates :vectors (.-vectors idx))
        new-pes (get updates :pes-edges (.-pes-edges idx))
        new-dim (get updates :dim (.-dim idx))
        new-distance-type (get updates :distance-type (.-distance-type idx))
        ;; Create fresh atom to avoid sharing mutable state between index versions
        ;; The old atom's pending writes should already have been consumed by sync!/flush!
        ;; Invalidate commit-id first, then merge - if updates has :commit-id it will override
        ;; This ensures mutations (which don't pass :commit-id) invalidate it, while sync! can set it
        new-state (-> (.-state idx)
                      (assoc :commit-id nil)
                      (merge (dissoc updates :vectors :pes-edges :dim :distance-type))
                      (assoc :pending-edge-writes (atom #{})))]
    (->HnswIndex new-vectors new-pes (int new-dim) (int new-distance-type) new-state (.-_meta idx))))

;; Forward reference holder for transient creation (set after TransientHnswIndex is defined)
(def ^:private ^:dynamic *make-transient* nil)

(deftype HnswIndex [;; Hot-path fields (direct access for performance)
                    vectors                      ; VectorStore
                    ^PersistentEdgeStore pes-edges ; PersistentEdgeStore
                    ^int dim                     ; dimension
                    ^int distance-type           ; 0=euclidean, 1=cosine, 2=inner-product

                    ;; All other state (20 fields)
                    ^clojure.lang.IPersistentMap state
                    ^clojure.lang.IPersistentMap _meta]

  ;; MapEquivalence marker - signals we implement proper map equality semantics
  clojure.lang.MapEquivalence

  ;; ========== Collection protocols - pure external-ID semantics ==========
  ;; The index behaves as a persistent map from external-ID to vector.
  ;; For internal state access, use proximum.hnsw.internal namespace.

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    ;; PURE external ID lookup - works with any key type including keywords
    ;; For internal state access, use proximum.hnsw.internal namespace
    (if-let [internal-id (meta/lookup-external-id (:external-id-index state) k)]
      (vectors/get-vector vectors internal-id)
      not-found))

  clojure.lang.IKeywordLookup
  (getLookupThunk [this k]
    ;; Optimize keyword lookups - delegates to valAt (external ID lookup)
    (reify clojure.lang.ILookupThunk
      (get [_ target]
        (.valAt ^HnswIndex target k nil))))

  clojure.lang.IPersistentMap
  (assoc [this k v]
    ;; PURE external ID insert - works with any key type including keywords
    ;; For internal state updates, use (assoc (.state idx) k v) directly
    ;; v can be vector or {:vector [...] :metadata {...}}
    (let [vector (if (map? v) (:vector v) v)
          metadata (when (map? v) (:metadata v))]
      (p/insert this vector (assoc (or metadata {}) :external-id k))))

  (assocEx [this k v]
    (if (contains? this k)
      (throw (ex-info "Key already exists" {:key k}))
      (.assoc this k v)))

  (without [this k]
    ;; PURE external ID delete - works with any key type including keywords
    ;; For internal state updates, use (dissoc (.state idx) k) directly
    (if-let [internal-id (meta/lookup-external-id (:external-id-index state) k)]
      (p/delete this internal-id)
      this))

  ;; Associative methods (inherited interface)
  (containsKey [this k]
    ;; PURE external ID check - works with any key type including keywords
    (boolean (meta/lookup-external-id (:external-id-index state) k)))

  (entryAt [this k]
    ;; PURE external ID entry - works with any key type including keywords
    (when-let [internal-id (meta/lookup-external-id (:external-id-index state) k)]
      (clojure.lang.MapEntry. k (vectors/get-vector vectors internal-id))))

  clojure.lang.Seqable
  (seq [this]
    ;; Iterate external-id-index in sorted order
    (let [live-count (- (:vector-count state) (:deleted-count state))]
      (when (pos? live-count)
        (map (fn [{:keys [external-id node-id]}]
               (clojure.lang.MapEntry. external-id
                                       (vectors/get-vector vectors node-id)))
             (seq (:external-id-index state))))))

  java.lang.Iterable
  (iterator [this]
    (.iterator ^java.lang.Iterable (or (seq this) [])))

  clojure.lang.Counted
  (count [this]
    ;; Live vectors (total - deleted)
    (- (:vector-count state) (:deleted-count state)))

  clojure.lang.IPersistentCollection
  (cons [this o]
    ;; o should be [external-id vector] or {:external-id ... :vector ...}
    (cond
      (vector? o) (.assoc this (first o) (second o))
      (map? o) (.assoc this (:external-id o) o)
      :else (throw (ex-info "cons expects [k v] pair or map with :external-id" {:entry o}))))

  (empty [this]
    ;; Return empty would require creating new index with same config
    ;; For now, throw as this is complex
    (throw (ex-info "empty not supported on HnswIndex - use create-index instead" {})))

  (equiv [this other]
    ;; Commit-based equality for crypto-hashed indices, identity otherwise
    (or (identical? this other)
        (and (instance? HnswIndex other)
             ;; Both must have crypto-hash enabled for content-based equality
             (:crypto-hash? state)
             (:crypto-hash? (.-state ^HnswIndex other))
             ;; Both must have commit-ids (not uncommitted)
             (some? (:commit-id state))
             (some? (:commit-id (.-state ^HnswIndex other)))
             ;; Same commit-id means same content (vectors, edges, metadata)
             ;; Branch doesn't matter - two commits with same hash are equal
             (= (:commit-id state) (:commit-id (.-state ^HnswIndex other))))))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))
  (invoke [this k not-found]
    (.valAt this k not-found))

  clojure.lang.IMeta
  (meta [this] _meta)

  clojure.lang.IObj
  (withMeta [this m]
    (->HnswIndex vectors pes-edges dim distance-type state m))

  clojure.lang.IEditableCollection
  (asTransient [this]
    ;; Reconstruct full state map for transient (includes hot-path fields)
    (let [full-state (assoc state
                            :vectors vectors
                            :pes-edges pes-edges
                            :dim dim
                            :distance-type distance-type)]
      (*make-transient* full-state _meta)))

  ;; Print representation
  Object
  (toString [this]
    (str "#HnswIndex{:dim " dim
         ", :count " (- (:vector-count state) (:deleted-count state))
         ", :branch " (:branch state)
         ", :commit-id " (:commit-id state) "}")))

;; Custom print method
(defmethod print-method HnswIndex [idx ^java.io.Writer w]
  (.write w (.toString idx)))

;; -----------------------------------------------------------------------------
;; TransientHnswIndex Type
;;
;; Mutable version of HnswIndex for efficient batch operations.
;; Used via (transient idx) -> (assoc! t k v) -> (persistent! t)
;;
;; The transient version:
;; - Forks PES and enables transient mode
;; - Batches inserts for parallel execution on persistent!
;; - Returns a sealed persistent HnswIndex on persistent!
;;
;; Performance note: valAt performs O(n) linear scan through pending inserts.
;; Transient mode is optimized for batch loading, not lookups during mutation.
;; If you need many lookups, call persistent! first.

(deftype TransientHnswIndex [^clojure.lang.Atom state-atom
                             ^clojure.lang.IPersistentMap _meta]

  clojure.lang.ITransientMap
  (assoc [this k v]
    ;; k = external-id, v = vector or {:vector [...] :metadata {...}}
    ;; Accumulate into pending inserts for batch execution on persistent!
    (let [vector (if (map? v) (:vector v) v)
          metadata (when (map? v) (:metadata v))
          meta-with-id (assoc (or metadata {}) :external-id k)]
      (swap! state-atom update :pending-inserts (fnil conj [])
             {:vector vector :metadata meta-with-id}))
    this)

  (without [this k]
    ;; Queue deletion for execution on persistent!
    (swap! state-atom update :pending-deletes (fnil conj #{}) k)
    this)

  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    ;; Check pending inserts first, then existing entries
    ;; PURE external ID lookup - works with any key type including keywords
    (let [s @state-atom
          pending (:pending-inserts s)]
      (if-let [entry (some #(when (= k (get-in % [:metadata :external-id])) %) pending)]
        (:vector entry)
        ;; Check existing entries via external-id index
        (if-let [internal-id (meta/lookup-external-id (:external-id-index s) k)]
          (vectors/get-vector (:vectors s) internal-id)
          not-found))))

  clojure.lang.ITransientCollection
  (conj [this entry]
    ;; entry must be [k v] pair or {:external-id ... :vector ...}
    (cond
      (vector? entry)
      (let [[k v] entry]
        (.assoc this k v))

      (map? entry)
      (let [ext-id (:external-id entry)
            vector (:vector entry)]
        (when-not ext-id
          (throw (ex-info "Entry must have :external-id" {:entry entry})))
        (.assoc this ext-id entry))

      :else
      (throw (ex-info "conj! expects [k v] pair or map with :external-id"
                      {:entry entry}))))

  (persistent [this]
    ;; Execute all pending operations and return persistent HnswIndex
    (let [s @state-atom
          pending-inserts (:pending-inserts s)
          pending-deletes (:pending-deletes s)

          ;; Get base state without pending queues
          base-state (dissoc s :pending-inserts :pending-deletes)

          ;; Create index from current state
          idx (make-hnsw-index base-state _meta)]

      ;; If nothing pending, just return the index
      (if (and (empty? pending-inserts) (empty? pending-deletes))
        idx
        ;; Apply batch operations
        (cond-> idx
          ;; Apply deletes first (using api functions)
          (seq pending-deletes)
          (as-> i (reduce (fn [idx ext-id]
                            (if-let [internal-id (meta/lookup-external-id
                                                  (:external-id-index (.-state idx)) ext-id)]
                              (p/delete idx internal-id)
                              idx))
                          i pending-deletes))

          ;; Then apply inserts as batch
          (seq pending-inserts)
          (p/insert-batch (mapv :vector pending-inserts)
                          {:metadata (mapv :metadata pending-inserts)})))))

  clojure.lang.Counted
  (count [this]
    (let [s @state-atom
          pending-inserts (count (:pending-inserts s))
          pending-deletes (count (:pending-deletes s))]
      (+ (- (:vector-count s) (:deleted-count s))
         pending-inserts
         (- pending-deletes)))))

;; Constructor for TransientHnswIndex
(defn ->TransientHnswIndex
  "Create a TransientHnswIndex from a state atom and metadata.
   Internal use - prefer (transient idx)."
  [state-atom meta]
  (TransientHnswIndex. state-atom meta))

;; Custom print method
(defmethod print-method TransientHnswIndex [idx ^java.io.Writer w]
  (let [s @(.state-atom ^TransientHnswIndex idx)]
    (.write w (str "#TransientHnswIndex{:dim " (:dim s)
                   ", :pending-inserts " (count (:pending-inserts s))
                   ", :pending-deletes " (count (:pending-deletes s)) "}"))))

;; Wire up the transient creation now that TransientHnswIndex is defined
(alter-var-root #'*make-transient*
                (constantly (fn [state meta]
                              (TransientHnswIndex. (atom state) meta))))

;; Note: ->HnswIndex is auto-generated by deftype and takes 6 args
;; These helpers provide backwards-compatible map-based construction

(defn hnsw-index
  "Create an HnswIndex from a state map (preferred constructor)."
  ([state] (make-hnsw-index state nil))
  ([state meta] (make-hnsw-index state meta)))

;; -----------------------------------------------------------------------------
;; Helper Functions

(defn- ensure-float-array
  "Convert to float array, always copying to avoid mutating user input.
   This is defensive - we don't know what the user will do with their array after passing it."
  ^floats [v]
  (if (instance? (Class/forName "[F") v)
    (aclone ^floats v)
    (float-array v)))

(defn- distance-keyword->type
  "Convert distance keyword to integer type for Java HNSW.

   Types match Distance.java constants:
     0 = EUCLIDEAN (L2 squared)
     1 = COSINE (requires normalized vectors)
     2 = INNER_PRODUCT"
  ^long [distance]
  (case distance
    :euclidean 0
    :cosine 1
    :inner-product 2
    0))

(defn recommended-ef-construction
  "Calculate recommended ef-construction based on expected index size."
  ^long [expected-size M]
  (let [base (* 2 M)]
    (cond
      (< expected-size 1000)   (max base 100)
      (< expected-size 10000)  (max base 150)
      (< expected-size 100000) (max base 200)
      :else                    (max base 400))))

(defn recommended-ef-search
  "Calculate recommended ef-search for a given k and index size."
  ^long [k index-size]
  (let [base (max k 50)]
    (cond
      (< index-size 1000)   base
      (< index-size 10000)  (max base 100)
      (< index-size 100000) (max base 150)
      :else                 (max base 200))))

;; -----------------------------------------------------------------------------
;; Store Connection Helpers

(defn- ->uuid
  "Best-effort conversion to UUID."
  [x]
  (cond
    (instance? java.util.UUID x) x
    (string? x) (try
                  (java.util.UUID/fromString x)
                  (catch Exception _ nil))
    :else nil))

(defn- normalize-store-config
  "Validate and normalize a Konserve store-config."
  [store-config]
  (when-not (map? store-config)
    (throw (ex-info ":store-config must be a map" {:store-config store-config})))
  (let [cfg-id (->uuid (:id store-config))]
    (when-not cfg-id
      (throw (ex-info "Konserve :id (UUID) is required in :store-config"
                      {:store-config store-config
                       :hint "Add :id #uuid \"...\" (or a UUID string) to your store config"})))
    (-> store-config
        (dissoc :opts)
        (assoc :id cfg-id))))

(defn- connect-store-sync
  "Connect to a Konserve store synchronously."
  [store-config]
  (when-not (map? store-config)
    (throw (ex-info ":store-config must be a map" {:store-config store-config})))
  (k/connect-store (dissoc store-config :opts) {:sync? true}))

(defn- create-store-sync
  "Create a Konserve store synchronously."
  [store-config]
  (when-not (map? store-config)
    (throw (ex-info ":store-config must be a map" {:store-config store-config})))
  (k/create-store (dissoc store-config :opts) {:sync? true}))

;; -----------------------------------------------------------------------------
;; PersistentEdgeStore Integration

(defn create-edge-store
  "Create a PersistentEdgeStore for an index.

   Uses chunked CoW arrays:
   - Layer 0: Dense chunks for structural sharing
   - Upper layers: Sparse per-node arrays

   Args:
     max-nodes   - Maximum number of nodes
     max-level   - Maximum HNSW level (typically 16)
     M           - Max neighbors for upper layers
     M0          - Max neighbors for layer 0 (typically 2*M)

   Returns:
     PersistentEdgeStore instance"
  ^PersistentEdgeStore [max-nodes max-level M M0]
  (PersistentEdgeStore. (int max-nodes) (int max-level) (int M) (int M0)))

(defn fork-edges
  "Create a fork of a PersistentEdgeStore.
   The fork shares structure with the original but can diverge."
  ^PersistentEdgeStore [^PersistentEdgeStore edges]
  (.fork edges))

;; Distance type constants matching Java HnswSearch
(def ^:const DISTANCE_EUCLIDEAN 0)
(def ^:const DISTANCE_COSINE 1)
(def ^:const DISTANCE_INNER_PRODUCT 2)

(defn distance-type->int
  "Convert distance keyword to Java distance type constant."
  [distance-type]
  (case distance-type
    :euclidean DISTANCE_EUCLIDEAN
    :cosine DISTANCE_COSINE
    :inner-product DISTANCE_INNER_PRODUCT
    DISTANCE_EUCLIDEAN))

(defn bulk-insert!
  "Insert multiple vectors in parallel using PersistentEdgeStore.

   This implementation uses:
   - PersistentEdgeStore for chunked CoW edge storage
   - Transient mode during bulk insert (mutates in place)
   - ForkJoinPool for parallel execution
   - Diversity heuristic for neighbor selection
   - SIMD distance computation

   Args:
     index         - HnswIndex (will use its vector store)
     edges         - PersistentEdgeStore to insert into
     vectors       - Collection of vectors to insert
     num-threads   - Number of threads (default: available processors)
     distance-type - Optional: :euclidean (default), :cosine, :inner-product

   Returns:
     PersistentEdgeStore with all vectors inserted"
  ([index edges vectors]
   (bulk-insert! index edges vectors (.availableProcessors (Runtime/getRuntime))))
  ([index edges vectors num-threads]
   (bulk-insert! index edges vectors num-threads :euclidean))
  ([index ^PersistentEdgeStore edges vectors num-threads distance-type]
   (let [state (.-state index)
         {:keys [M ef-construction max-levels]} state
         vectors-store (.-vectors index)
         dim (.-dim index)
         ml (/ 1.0 (Math/log M))
         max-level-limit (or max-levels 15)
         use-cosine? (= distance-type :cosine)

         ^MemorySegment seg (vectors/get-segment vectors-store)

         float-vectors (into-array (map (fn [v]
                                          (let [arr (ensure-float-array v)]
                                            (when use-cosine?
                                              (HnswInsert/normalizeVector arr))
                                            arr))
                                        vectors))

         node-ids (int-array
                   (map (fn [float-arr]
                          (vectors/append! vectors-store float-arr))
                        float-vectors))

         node-levels (int-array
                      (map (fn [_] (HnswInsert/randomLevel ml max-level-limit))
                           vectors))

         dist-int (int (case distance-type
                         :euclidean HnswInsert/DISTANCE_EUCLIDEAN
                         :cosine HnswInsert/DISTANCE_COSINE
                         :inner-product HnswInsert/DISTANCE_INNER_PRODUCT
                         HnswInsert/DISTANCE_EUCLIDEAN))]

     (HnswInsert/insertBatch
      seg
      edges
      float-vectors
      node-ids
      (int dim)
      node-levels
      (int ef-construction)
      (int num-threads)
      dist-int)

     edges)))

(defn hnsw-search
  "Search for k nearest neighbors using PersistentEdgeStore.

   Lock-free search with SIMD distance computation.

   Args:
     index     - HnswIndex (for vector store)
     edges     - PersistentEdgeStore containing the graph
     query     - Query vector
     k         - Number of results
     ef        - Search beam width (should be >= k)
     distance-type - Optional: :euclidean (default), :cosine, :inner-product

   Returns:
     Seq of {:id node-id, :distance distance}"
  ([index edges query k ef]
   (hnsw-search index edges query k ef :euclidean))
  ([index ^PersistentEdgeStore edges query k ef distance-type]
   (let [vectors-store (.-vectors index)
         ^MemorySegment seg (vectors/get-segment vectors-store)
         dim (.-dim index)
         float-arr (if (instance? (Class/forName "[F") query)
                     (aclone ^floats query)
                     (float-array query))
         _ (when (= distance-type :cosine)
             (HnswSearch/normalizeVector float-arr))
         dist-int (int (distance-type->int distance-type))
         ^doubles result (HnswSearch/search seg edges float-arr (int dim) (int k) (int ef) dist-int)]
     (loop [i 0
            acc (transient [])]
       (if (< i (alength result))
         (recur (+ i 2)
                (conj! acc {:id (long (aget result i))
                            :distance (aget result (inc i))}))
         (persistent! acc))))))

;; -----------------------------------------------------------------------------
;; VectorIndex Protocol Implementation

(extend-type HnswIndex
  p/VectorIndex
  (insert
    ([idx vector]
     (p/insert idx vector nil))
    ([idx vector meta-map]
     (let [state (.-state idx)
           vectors (.-vectors idx)
           pes-edges (.-pes-edges idx)
           dim (.-dim idx)
           distance-type (.-distance-type idx)
           {:keys [metadata external-id-index M ef-construction ml max-levels]} state
           cap (vectors/capacity vectors)
           cnt (vectors/count-vectors vectors)
           use-cosine? (= distance-type 1)]
       (when (>= cnt cap)
         (throw (ex-info "Index capacity exceeded. Create a new index with larger :capacity."
                         {:capacity cap
                          :current-count cnt
                          :hint "Use (create-index {:type :hnsw :dim dim :capacity larger-value ...})"})))
       ;; Fork PES for immutable semantics - new PES is private to this operation
       (let [new-pes (.fork ^PersistentEdgeStore pes-edges)
             float-arr (ensure-float-array vector)
             _ (when use-cosine?
                 (HnswInsert/normalizeVector float-arr))
             node-id (vectors/append! vectors float-arr)
             node-level (HnswInsert/randomLevel ml (or max-levels 16))
             ^MemorySegment seg (vectors/get-segment vectors)
             ;; HnswInsert/insert handles transient mode internally
             _ (HnswInsert/insert seg new-pes float-arr (int node-id)
                                  dim (int node-level) (int ef-construction)
                                  distance-type)
             new-metadata (meta/set-metadata metadata node-id meta-map)
             external-id (meta/external-id-from-meta meta-map)
             new-external-id-index (meta/set-external-id external-id-index external-id node-id)]
         (update-hnsw-index idx {:pes-edges new-pes
                                 :metadata new-metadata
                                 :external-id-index new-external-id-index
                                 :vector-count (inc cnt)})))))

  (insert-batch
    ([idx vectors]
     (p/insert-batch idx vectors {}))
    ([idx vecs opts]
     (let [state (.-state idx)
           vectors (.-vectors idx)
           pes-edges (.-pes-edges idx)
           dim (.-dim idx)
           distance-type (.-distance-type idx)
           {:keys [metadata external-id-index M ef-construction ml max-levels]} state
           {:keys [parallelism] :or {parallelism (.availableProcessors (Runtime/getRuntime))}} opts
           metadata-vec (:metadata opts)
           n (count vecs)
           cap (vectors/capacity vectors)
           cnt (vectors/count-vectors vectors)
           use-cosine? (= distance-type 1)]
       (when (> (+ cnt n) cap)
         (throw (ex-info "Index capacity exceeded. Create a new index with larger :capacity."
                         {:capacity cap
                          :current-count cnt
                          :batch-size n
                          :needed (+ cnt n)
                          :hint "Use (create-index {:type :hnsw :dim dim :capacity larger-value ...})"})))
       ;; Fork PES for immutable semantics - new PES is private to this operation
       (let [new-pes (.fork ^PersistentEdgeStore pes-edges)
             float-vecs (into-array (map ensure-float-array vecs))
             _ (when use-cosine?
                 (HnswInsert/normalizeVectors float-vecs))
             node-ids (int-array (map (fn [v] (vectors/append! vectors v)) float-vecs))
             max-level-limit (or max-levels 16)
             node-levels (int-array (map (fn [_] (HnswInsert/randomLevel ml max-level-limit)) vecs))
             ^MemorySegment seg (vectors/get-segment vectors)
             ;; HnswInsert/insertBatch handles transient mode internally
             _ (HnswInsert/insertBatch seg new-pes float-vecs node-ids
                                       dim node-levels (int ef-construction) (int parallelism)
                                       distance-type)
             [new-metadata new-external-id-index]
             (if metadata-vec
               (reduce (fn [[m ext] i]
                         (if-let [meta-map (nth metadata-vec i nil)]
                           (let [node-id (aget node-ids i)
                                 m' (meta/set-metadata m node-id meta-map)
                                 external-id (meta/external-id-from-meta meta-map)
                                 ext' (meta/set-external-id ext external-id node-id)]
                             [m' ext'])
                           [m ext]))
                       [metadata external-id-index]
                       (range n))
               [metadata external-id-index])]
         (update-hnsw-index idx {:pes-edges new-pes
                                 :metadata new-metadata
                                 :external-id-index new-external-id-index
                                 :vector-count (+ cnt n)})))))

  (search
    ([idx query k]
     (p/search idx query k {}))
    ([idx query k opts]
     (let [vectors (.-vectors idx)
           pes-edges (.-pes-edges idx)
           dim (.-dim idx)
           distance-type (.-distance-type idx)
           ef (max k (or (:ef opts) (:ef-search (.-state idx)) 50))
           float-arr (ensure-float-array query)
           _ (when (= distance-type 1)
               (HnswSearch/normalizeVector float-arr))
           ^MemorySegment seg (vectors/get-segment vectors)
           ^doubles result (HnswSearch/search seg pes-edges float-arr dim (int k) (int ef) distance-type)]
       (loop [i 0
              acc (transient [])]
         (if (< i (alength result))
           (recur (+ i 2)
                  (conj! acc {:id (long (aget result i))
                              :distance (aget result (inc i))}))
           (persistent! acc))))))

  (search-filtered
    ([idx query k pred-or-set]
     (p/search-filtered idx query k pred-or-set {}))
    ([idx query k pred-or-set opts]
     (let [vectors (.-vectors idx)
           pes-edges (.-pes-edges idx)
           dim (.-dim idx)
           distance-type (.-distance-type idx)
           ef (or (:ef opts) (* k 10))
           float-arr (ensure-float-array query)
           _ (when (= distance-type 1)
               (HnswSearch/normalizeVector float-arr))
           ^MemorySegment seg (vectors/get-segment vectors)
           n (vectors/count-vectors vectors)
           ;; Support multiple filter types:
           ;; - ArrayBitSet: use directly (fastest)
           ;; - Set/collection of IDs: build bitset from it
           ;; - Function (pred-fn): call on each vector to build bitset
           ^ArrayBitSet bitset
           (cond
             ;; Already an ArrayBitSet - use directly
             (instance? ArrayBitSet pred-or-set)
             pred-or-set

             ;; Set or collection of allowed IDs - build bitset
             (or (set? pred-or-set) (sequential? pred-or-set))
             (let [bs (ArrayBitSet. n)]
               (doseq [id pred-or-set]
                 (when (and (>= id 0) (< id n))
                   (.add bs (int id))))
               bs)

             ;; Function predicate - call on each vector
             (fn? pred-or-set)
             (let [bs (ArrayBitSet. n)]
               (dotimes [i n]
                 (when (pred-or-set i (p/get-metadata idx i))
                   (.add bs i)))
               bs)

             :else
             (throw (ex-info "search-filtered requires a predicate fn, Set, or ArrayBitSet"
                             {:type (type pred-or-set)})))

           ^doubles result (HnswSearch/searchFiltered seg pes-edges float-arr
                                                      dim (int k) (int ef)
                                                      bitset
                                                      distance-type)]
       (loop [i 0
              acc (transient [])]
         (if (< i (alength result))
           (recur (+ i 2)
                  (conj! acc {:id (long (aget result i))
                              :distance (aget result (inc i))}))
           (persistent! acc))))))

  (delete [idx id]
    (let [state (.-state idx)
          vectors (.-vectors idx)
          pes-edges (.-pes-edges idx)
          dim (.-dim idx)
          distance-type (.-distance-type idx)
          {:keys [metadata external-id-index M M0 deleted-count]} state
          ^MemorySegment seg (vectors/get-segment vectors)
          ;; Fork PES for immutable semantics - new PES is private to this operation
          new-pes (.fork ^PersistentEdgeStore pes-edges)
          ;; HnswInsert/delete handles transient mode internally
          _ (HnswInsert/delete seg new-pes (int id) dim (int M) (int M0) distance-type)
          old-meta (meta/lookup-metadata metadata id)
          old-external-id (meta/external-id-from-meta old-meta)
          new-metadata (disj metadata {:node-id id})
          new-external-id-index (meta/remove-external-id external-id-index old-external-id)]
      (update-hnsw-index idx {:pes-edges new-pes
                              :metadata new-metadata
                              :external-id-index new-external-id-index
                              :deleted-count (inc deleted-count)})))

  (count-vectors [idx]
    (- (:vector-count (.-state idx)) (:deleted-count (.-state idx))))

  (get-vector [idx id]
    (vectors/get-vector (.-vectors idx) id))

  (get-metadata [idx id]
    (meta/lookup-metadata (:metadata (.-state idx)) id))

  (set-metadata [idx id metadata]
    (let [state (.-state idx)
          new-metadata (meta/set-metadata (:metadata state) id metadata)]
      (update-hnsw-index idx {:metadata new-metadata})))

  (capacity [idx]
    (vectors/capacity (.-vectors idx)))

  (remaining-capacity [idx]
    (let [vectors (.-vectors idx)]
      (- (vectors/capacity vectors)
         (vectors/count-vectors vectors)))))  ; Close first extend-type

;; IndexLifecycle protocol implementation
(extend-type HnswIndex
  p/IndexLifecycle
  (fork [idx]
    (let [forked-pes (.fork ^PersistentEdgeStore (.-pes-edges idx))]
      ;; All fields are already plain values - just replace pes-edges
      ;; address-map, vector-count, deleted-count, commit-hash are copied by assoc
      ;; Preserve commit-id since fork doesn't change content
      (update-hnsw-index idx {:pes-edges forked-pes
                              :commit-id (:commit-id (.-state idx))})))

  (flush! [idx]
    ;; Capture state synchronously
    (let [vs (.-vectors idx)
          edge-store (:edge-store (.-state idx))
          pes (.-pes-edges idx)
          _ (vectors/flush-write-buffer-async! vs)
          new-address-map
          (if edge-store
            (if-let [{:keys [channels address-map]}
                     (edges/flush-dirty-chunks-async! edge-store pes (:address-map (.-state idx))
                                                      {:crypto-hash? (:crypto-hash? (.-state idx))})]
              (do
                (swap! (:pending-edge-writes (.-state idx)) into channels)
                address-map)
              (:address-map (.-state idx)))
            (:address-map (.-state idx)))
          _ (when-let [mmap-buf (:mmap-buf vs)]
              (.force ^java.nio.MappedByteBuffer mmap-buf))
          vec-channels-to-wait @(:pending-writes vs)
          edge-pending (:pending-edge-writes (.-state idx))
          edge-channels-to-wait (when edge-pending @edge-pending)]

      ;; Wait asynchronously
      (a/go
        (try
          ;; Wait for vector writes
          (doseq [ch vec-channels-to-wait]
            (a/<! ch))
          (swap! (:pending-writes vs) #(reduce disj % vec-channels-to-wait))

          ;; Wait for edge writes
          (when edge-channels-to-wait
            (doseq [ch edge-channels-to-wait]
              (a/<! ch))
            (swap! edge-pending #(reduce disj % edge-channels-to-wait)))

          ;; Clear dirty
          (when edge-store
            (.clearDirty ^PersistentEdgeStore pes))

          ;; Return updated index with new address-map
          (update-hnsw-index idx {:address-map new-address-map
                                  :commit-id (:commit-id (.-state idx))})
          (catch Exception e
            (throw e))))))  ; Close: try, go, let, flush!

  (sync!
    ([idx]
     (p/sync! idx {}))
    ([idx opts]
    ;; Sync the index to durable storage, creating a commit
    ;; See writing.clj for helper functions
     (let [state (.-state idx)
           vs (.-vectors idx)
           edge-store (:edge-store state)
           pes (.-pes-edges idx)
           branch (:branch state)
           crypto-hash? (:crypto-hash? state)
          ;; Read previous commit from storage (branch head), not in-memory state
          ;; This is correct even after mutations have invalidated the in-memory commit-id
           prev-snapshot (when edge-store (k/get edge-store branch nil {:sync? true}))
           parent-commit-hash (when crypto-hash? (:commit-id prev-snapshot))

           ;; 1. Fire async flush for vectors
           _ (vectors/flush-write-buffer-async! vs)

           ;; 2. Fire async flush for edges (COW: generates new storage addresses)
           edges-async-result
           (when edge-store
             (edges/flush-dirty-chunks-async! edge-store pes (:address-map state)
                                              {:crypto-hash? crypto-hash?}))

           ;; Track new address-map from edge flush
           new-address-map (if edges-async-result
                             (do
                               (swap! (:pending-edge-writes state) into (:channels edges-async-result))
                               (:address-map edges-async-result))
                             (:address-map state))

           ;; 3. Force mmap to disk and update header count
           _ (when-let [mmap-buf (:mmap-buf vs)]
               (.force ^java.nio.MappedByteBuffer mmap-buf)
               (vectors/update-header-count! mmap-buf (vectors/count-vectors vs))
               (.force ^java.nio.MappedByteBuffer mmap-buf))

           ;; Capture channels to wait for
           vec-channels-to-wait @(:pending-writes vs)
           edge-pending (:pending-edge-writes state)
           edge-channels-to-wait (when edge-pending @edge-pending)]

       ;; 4-7. Wait for writes and complete sync asynchronously
       (let [result-chan (a/go
                           (try
           ;; Wait for vector writes
                             (loop [[ch & r] vec-channels-to-wait]
                               (when ch
                                 (a/<! ch)
                                 (recur r)))
                             (swap! (:pending-writes vs) #(reduce disj % vec-channels-to-wait))

           ;; Wait for edge writes
                             (when edge-channels-to-wait
                               (loop [[ch & r] edge-channels-to-wait]
                                 (when ch
                                   (a/<! ch)
                                   (recur r)))
                               (swap! edge-pending #(reduce disj % edge-channels-to-wait)))

           ;; 6. Compute commit hashes and write metadata
                             (let [;; Vector commit hash
                                   vectors-pending-hashes (when crypto-hash? @(:pending-chunk-hashes vs))
                                   vectors-parent-hash (when crypto-hash? @(:commit-hash vs))
                                   vectors-commit-hash (when (and crypto-hash? (seq vectors-pending-hashes))
                                                         (vectors/hash-commit vectors-parent-hash vectors-pending-hashes))

                 ;; Edge commit hash (from async result)
                                   edges-chunk-hashes (when crypto-hash? (:chunk-hashes edges-async-result))
                                   edges-commit-hash (when (and crypto-hash? (seq edges-chunk-hashes))
                                                       (edges/hash-commit nil edges-chunk-hashes))

                 ;; Combined index commit hash (used as commit-id when crypto-hash? enabled)
                                   final-vectors-hash (or vectors-commit-hash vectors-parent-hash)
                                   new-commit-hash (when (and crypto-hash? (or final-vectors-hash edges-commit-hash))
                                                     (crypto/hash-index-commit parent-commit-hash final-vectors-hash edges-commit-hash))]

             ;; Update vectors commit-hash atom and atomically remove processed hashes
                               (when crypto-hash?
                                 (when vectors-commit-hash
                                   (reset! (:commit-hash vs) vectors-commit-hash))
               ;; Atomic removal: drop only the hashes we processed, keep any new ones
                                 (swap! (:pending-chunk-hashes vs)
                                        #(vec (drop (count vectors-pending-hashes) %))))

             ;; Clear dirty edges
                               (when edge-store
                                 (.clearDirty ^PersistentEdgeStore pes))

             ;; 7. Store all PSS structures and create commit
                               (if-let [store (:storage state)]
                                 (let [;; Store metadata PSS
                                       meta-pss (:metadata state)
                                       metadata-pss-root (pss/store meta-pss store)
                                       external-id-pss-root (pss/store (:external-id-index state) store)

                     ;; Convert vectors address map to PSS and store
                                       vectors-addr-map @(:chunk-address-map vs)
                                       vectors-addr-pss (storage/map-to-address-pss vectors-addr-map store)
                                       vectors-addr-pss-root (storage/store-address-pss! vectors-addr-pss store)

                     ;; Convert edges address map to PSS and store
                                       edges-addr-pss (storage/map-to-address-pss new-address-map store)
                                       edges-addr-pss-root (storage/store-address-pss! edges-addr-pss store)

                     ;; Flush all pending PSS writes
                                       _ (storage/flush-writes! store)

                     ;; Generate commit ID using helper
                                       commit-id (writing/generate-commit-id crypto-hash? new-commit-hash)
                                       now (java.util.Date.)

                     ;; Determine parents - use opts override or prev-snapshot
                                       prev-commit (:commit-id prev-snapshot)
                                       parents (or (:parents opts)
                                                   (writing/determine-parents prev-commit))

                     ;; Build the index snapshot with all PSS roots
                     ;; Use index with updated address-map for snapshot building
                                       index-for-snapshot (update-hnsw-index idx {:address-map new-address-map})
                                       snapshot (cond-> (writing/build-index-snapshot index-for-snapshot commit-id parents
                                                                                      metadata-pss-root external-id-pss-root
                                                                                      vectors-addr-pss-root edges-addr-pss-root
                                                                                      now)
                                                  (:message opts) (assoc :message (:message opts)))]

                 ;; Write commit entry and branch head using helper
                                   (writing/write-commit! edge-store commit-id branch snapshot)

                 ;; Return updated index with new values
                                   (update-hnsw-index idx {:address-map new-address-map
                                                           :commit-id commit-id}))

               ;; No storage - just return index with updated address-map
                                 (update-hnsw-index idx {:address-map new-address-map})))
                             (catch Exception e
                               (throw e))))]
         result-chan))))

  (close! [idx]
    ;; Return channel from vectors/close! for proper async cleanup
    (vectors/close! (.-vectors idx))))

;; IndexIntrospection protocol implementation
(extend-type HnswIndex
  p/IndexIntrospection
  (index-type [_] :hnsw)

  (index-config [idx]
    (let [state (.-state idx)]
      {:type :hnsw
       :dim (.-dim idx)
       :M (:M state)
       :ef-construction (:ef-construction state)
       :ef-search (:ef-search state)
       :distance (:distance state)
       :max-levels (:max-levels state)
       :crypto-hash? (:crypto-hash? state)})))

;; IndexState protocol implementation
(extend-type HnswIndex
  p/IndexState
  (storage [idx]
    (:storage (.-state idx)))

  (raw-storage [idx]
    (:edge-store (.-state idx)))

  (current-branch [idx]
    (:branch (.-state idx)))

  (current-commit [idx]
    (:commit-id (.-state idx)))

  (vector-count-total [idx]
    (:vector-count (.-state idx)))

  (deleted-count-total [idx]
    (:deleted-count (.-state idx)))

  (mmap-dir [idx]
    (:mmap-dir (.-state idx)))

  (reflink-supported? [idx]
    (:reflink-supported? (.-state idx)))

  (crypto-hash? [idx]
    (:crypto-hash? (.-state idx)))

  (external-id-index [idx]
    (:external-id-index (.-state idx)))

  (metadata-index [idx]
    (:metadata (.-state idx)))

  (vector-storage [idx]
    (.-vectors idx))

  (edge-storage [idx]
    (.-pes-edges idx)))

;; Algebraic Protocol Implementations

(extend-type HnswIndex
  p/Snapshotable
  (snapshot-graph-state [idx]
    (let [^PersistentEdgeStore pes (.-pes-edges idx)]
      {:entrypoint (.getEntrypoint pes)
       :max-level (.getCurrentMaxLevel pes)
       :deleted-nodes-bitset (vec (.getDeletedNodesBitset pes))}))

  p/Forkable
  (fork-graph-storage [idx]
    (.fork ^PersistentEdgeStore (.-pes-edges idx)))

  (fork-vector-storage [idx branch-name]
    (let [state (.-state idx)
          vs (.-vectors idx)
          edge-store (:edge-store state)
          mmap-dir (:mmap-dir state)
          _ (when-not mmap-dir
              (throw (ex-info "Cannot fork vector storage: mmap-dir is not configured"
                              {:hint "Specify :mmap-dir when creating the index to enable branching"
                               :branch-name branch-name})))
          crypto-hash? (:crypto-hash? state)
          src-mmap-path (:mmap-path vs)
          dst-mmap-path (vectors/branch-mmap-path mmap-dir branch-name)]
      ;; Force mmap to disk before copying
      (when-let [mmap-buf (:mmap-buf vs)]
        (.force ^java.nio.MappedByteBuffer mmap-buf))
      ;; Copy mmap file
      (vectors/copy-mmap-for-branch! src-mmap-path dst-mmap-path
                                     (:reflink-supported? state))
      ;; Get config for new VectorStore
      (let [config (k/get edge-store :index/config nil {:sync? true})
            {:keys [dim chunk-size]} config
            vectors-addr-map @(:chunk-address-map vs)
            vector-count (:vector-count state)
            commit-hash (when-let [ch (:commit-hash vs)] @ch)]
        ;; Open new VectorStore pointing at copied mmap
        (vectors/open-store* edge-store dim chunk-size crypto-hash?
                             dst-mmap-path vectors-addr-map
                             vector-count commit-hash))))

  (assemble-forked-index [idx forked-vectors forked-graph new-branch new-commit-id]
    (update-hnsw-index idx {:vectors forked-vectors
                            :pes-edges forked-graph
                            :branch new-branch
                            :commit-id new-commit-id}))

  p/GraphMetrics
  (edge-count [idx]
    (.countEdges ^PersistentEdgeStore (.-pes-edges idx)))

  (graph-entrypoint [idx]
    (.getEntrypoint ^PersistentEdgeStore (.-pes-edges idx)))

  (graph-max-level [idx]
    (.getCurrentMaxLevel ^PersistentEdgeStore (.-pes-edges idx)))

  (expected-connectivity [idx]
    (:M (.-state idx))))

;; -----------------------------------------------------------------------------
;; create-index :hnsw Implementation

(defmethod p/create-index :hnsw
  [{:keys [dim M ef-construction ef-search distance capacity
           max-levels chunk-size cache-size branch crypto-hash?
           store store-config mmap-dir mmap-path]
    :or {M 16
         distance :euclidean
         capacity 10000000
         max-levels nil
         chunk-size 1000
         cache-size 10000
         branch :main
         crypto-hash? false}}]
  (when-not dim
    (throw (ex-info ":dim is required" {})))
  (let [store-config (when store-config (normalize-store-config store-config))
        M0 (* 2 M)
        ml (/ 1.0 (Math/log M))
        ef-c (or ef-construction (recommended-ef-construction capacity M))
        ef-s (or ef-search (recommended-ef-search 10 capacity))
        max-level-int (or max-levels 16)
        base-store (or store
                       (when store-config (create-store-sync store-config))
                       (throw (ex-info "A Konserve store is required"
                                       {:hint "Pass either :store (instance) or :store-config (map including :id)"})))

        reflink-ok (when (and mmap-dir
                              (.exists (java.io.File. ^String mmap-dir)))
                     (vectors/test-reflink-support mmap-dir))
        _ (when (false? reflink-ok)
            (log/warn :proximum/connect "Filesystem does not support reflink (copy-on-write)"
                      {:hint "Branch operations will use full file copies. Consider Btrfs, XFS, or ZFS for O(1) branching."
                       :mmap-dir mmap-dir}))
        actual-mmap-path (or mmap-path
                             (when mmap-dir (vectors/branch-mmap-path mmap-dir branch)))
        ;; Write global immutable config (includes :index-type for restore-index dispatch)
        _ (when base-store
            (k/assoc base-store :index/config
                     {:index-type :hnsw  ;; NEW: for restore-index dispatch
                      :dim dim
                      :M M
                      :M0 M0
                      :max-nodes capacity
                      :max-level max-level-int
                      :chunk-size chunk-size
                      :distance distance
                      :crypto-hash? crypto-hash?}
                     {:sync? true}))
        _ (when base-store
            (k/assoc base-store :branches #{branch} {:sync? true}))
        pss-store (storage/create-storage base-store {:cache-size cache-size
                                                      :crypto-hash? crypto-hash?})
        meta-pss (meta/create-metadata-pss pss-store)
        external-id-pss (meta/create-external-id-pss pss-store)
        vs (vectors/create-store* base-store dim chunk-size capacity actual-mmap-path crypto-hash?)
        pes (PersistentEdgeStore. (int capacity) (int max-level-int) (int M) (int M0))
        edge-store base-store]
    (make-hnsw-index
     {:vectors vs
      :pes-edges pes
      :metadata meta-pss
      :external-id-index external-id-pss
      :M M
      :M0 M0
      :ef-construction ef-c
      :ef-search ef-s
      :ml ml
      :max-levels max-levels
      :dim dim
      :distance distance
      :distance-type (distance-keyword->type distance)
      :storage pss-store
      :edge-store edge-store
      :address-map {}
      :pending-edge-writes (atom #{})
      :branch branch
      :commit-id nil
      :vector-count 0
      :deleted-count 0
      :mmap-dir mmap-dir
      :reflink-supported? reflink-ok
      :crypto-hash? crypto-hash?})))

;; Default dispatch for backwards compatibility
(defmethod p/create-index :default
  [config]
  (if (map? config)
    (throw (ex-info (str "Unknown index type: " (:type config) ". Use :hnsw") {:config config}))
    (p/create-index {:type :hnsw :dim config})))

;; -----------------------------------------------------------------------------
;; restore-index :hnsw Implementation

(defmethod p/restore-index :hnsw
  [snapshot edge-store {:keys [mmap-dir mmap-path cache-size]
                        :or {cache-size 10000}}]
  (let [;; Read immutable config from :index/config
        config (k/get edge-store :index/config nil {:sync? true})
        {:keys [dim M M0 max-nodes max-level chunk-size distance crypto-hash?]} config

        ;; Read mutable state from branch snapshot
        {:keys [metadata-pss-root external-id-pss-root
                vectors-addr-pss-root edges-addr-pss-root
                entrypoint current-max-level
                branch-vector-count branch-deleted-count deleted-nodes-bitset
                branch
                commit-id]} snapshot

        ;; Determine mmap path
        ;; Priority: explicit > mmap-dir+branch > temp fallback
        actual-mmap-path (or mmap-path
                             (when mmap-dir
                               (vectors/branch-mmap-path mmap-dir branch))
                             (str (System/getProperty "java.io.tmpdir")
                                  "/pvdb-" (name branch) "-" (System/currentTimeMillis) ".mmap"))

        ;; Create PSS storage
        pss-store (storage/create-storage edge-store {:cache-size cache-size
                                                      :crypto-hash? crypto-hash?})

        ;; Restore vectors address map from PSS
        vectors-addr-pss (when vectors-addr-pss-root
                           (storage/restore-address-pss vectors-addr-pss-root pss-store))
        vectors-addr-map (or (storage/address-pss-to-map vectors-addr-pss) {})

        ;; Open vector store
        vs (vectors/open-store* edge-store dim chunk-size crypto-hash?
                                actual-mmap-path vectors-addr-map
                                (or branch-vector-count 0)
                                (:vectors-commit-hash snapshot))

        ;; Create PES and switch to transient mode for initialization
        max-level-int (or max-level 16)
        pes (PersistentEdgeStore. (int max-nodes) (int max-level-int) (int M) (int M0))
        _ (.asTransient pes)  ;; Enable mutation for initialization
        _ (when entrypoint
            (.setEntrypoint pes (int entrypoint)))
        _ (when current-max-level
            (.setCurrentMaxLevel pes (int current-max-level)))

        ;; Restore edges address map from PSS
        edges-addr-pss (when edges-addr-pss-root
                         (storage/restore-address-pss edges-addr-pss-root pss-store))
        edges-addr-map (or (storage/address-pss-to-map edges-addr-pss) {})

        ;; Load all edge chunks
        _ (doseq [[pos storage-addr] edges-addr-map]
            (let [chunk-key (if (instance? java.util.UUID storage-addr)
                              [:edges :chunk storage-addr]
                              [:edges :chunk (keyword (str storage-addr))])]
              (when-let [data (k/get edge-store chunk-key nil {:sync? true})]
                (.setChunkByAddress pes (long pos) (edges/bytes-to-chunk data)))))

        ;; Restore deleted state
        _ (when branch-deleted-count
            (.setDeletedCount pes (int branch-deleted-count)))
        _ (when deleted-nodes-bitset
            (.setDeletedNodesBitset pes (long-array deleted-nodes-bitset)))
        _ (.asPersistent pes)  ;; Seal after initialization complete

        ;; Restore metadata PSS
        meta-pss (if metadata-pss-root
                   (pss/restore-by meta/metadata-comparator metadata-pss-root pss-store)
                   (meta/create-metadata-pss pss-store))

        ;; Restore external-id index PSS
        external-id-pss (if external-id-pss-root
                          (pss/restore-by meta/external-id-comparator external-id-pss-root pss-store)
                          (meta/create-external-id-pss pss-store))

        ;; Test reflink support
        reflink-ok (boolean (and mmap-dir (vectors/test-reflink-support mmap-dir)))]

    (make-hnsw-index
     {:vectors vs
      :pes-edges pes
      :metadata meta-pss
      :external-id-index external-id-pss
      :M M
      :M0 M0
      :ef-construction (:ef-construction snapshot 200)
      :ef-search (:ef-search snapshot 200)
      :ml (:ml snapshot 0.36067)
      :max-levels max-level
      :dim dim
      :distance distance
      :distance-type (distance-keyword->type distance)
      :storage pss-store
      :edge-store edge-store
      :address-map edges-addr-map
      :pending-edge-writes (atom #{})
      :branch branch
      :commit-id commit-id
      :vector-count (or branch-vector-count 0)
      :deleted-count (or branch-deleted-count 0)
      :mmap-dir mmap-dir
      :reflink-supported? reflink-ok
      :crypto-hash? (boolean crypto-hash?)})))

;; -----------------------------------------------------------------------------
;; Internal Helpers

(defn lookup-internal-id
  "Resolve external ID to internal node ID. Returns nil if not found."
  [idx external-id]
  (meta/lookup-external-id (p/external-id-index idx) external-id))

(defn- get-external-id
  "Get external ID for an internal node ID."
  [idx internal-id]
  (when-let [m (p/get-metadata idx internal-id)]
    (:external-id m)))

;; -----------------------------------------------------------------------------
;; NearestNeighborSearch Protocol Implementation

(extend-type proximum.hnsw.HnswIndex
  p/NearestNeighborSearch
  (nearest
    ([idx query k]
     (p/nearest idx query k {}))
    ([idx query k opts]
     (let [results (p/search idx query k (or opts {}))]
       (mapv (fn [{:keys [id distance]}]
               {:id (get-external-id idx id)
                :distance distance})
             results))))

  (nearest-filtered
    ([idx query k filter-pred]
     (p/nearest-filtered idx query k filter-pred {}))
    ([idx query k filter-pred opts]
     (let [;; Build internal filter from external filter
           internal-filter
           (cond
             ;; Set of external IDs -> translate to internal IDs
             (set? filter-pred)
             (let [internal-ids (keep #(lookup-internal-id idx %) filter-pred)]
               (set internal-ids))

             ;; Predicate function - wrap to translate IDs
             (fn? filter-pred)
             (fn [internal-id metadata]
               (let [external-id (get-external-id idx internal-id)]
                 (filter-pred external-id metadata)))

             ;; ArrayBitSet or other - pass through (advanced use)
             :else filter-pred)

           results (p/search-filtered idx query k internal-filter (or opts {}))]
       (mapv (fn [{:keys [id distance]}]
               {:id (get-external-id idx id)
                :distance distance})
             results)))))
