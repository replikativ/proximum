(ns proximum.compaction
  "Index compaction operations.

   Provides both offline and online (zero-downtime) compaction:

   Offline compaction:
   - compact: Create compacted copy with only live vectors
   - compact!: Compact and sync in one step

   Online compaction (zero-downtime):
   - start-online-compaction: Begin background compaction
   - finish-online-compaction!: Complete compaction and switch to new index
   - abort-online-compaction!: Cancel and return to source index
   - compaction-progress: Check current progress

   All operations are index-type agnostic - they use index-config to
   recreate indices of the same type with the same parameters.

   IMPORTANT: CompactionState must be used linearly - thread the state
   through operations. Do not use an old state after creating a new one."
  (:require [proximum.protocols :as p]
            [proximum.vectors :as vectors]
            [proximum.writing :as writing]
            [proximum.storage :as storage]
            [proximum.metadata :as meta]
            [proximum.logging :as log]
            [clojure.core.async :as a]
            [clojure.java.io :as io]
            [konserve.core :as k])
  (:import [proximum.internal PersistentEdgeStore]))

;; -----------------------------------------------------------------------------
;; Offline Compaction

(defn compact
  "Create a compacted copy of the index with only live vectors.

   This creates a new index at the provided target containing only non-deleted
   vectors. The original index is unchanged and can still be used for
   historical queries.

   The new index will have:
   - Dense, contiguous node IDs (0, 1, 2, ...)
   - Freshly built HNSW graph (optimal quality)
   - No wasted space from deleted vectors
   - Metadata preserved with :old-id for ID mapping

   Args:
     idx    - Source index to compact (any VectorIndex implementation)
     target - Map describing the new index storage:
              {:store-config <konserve-config-with-:id>
               :mmap-dir <directory-for-branch-mmaps>}

   Options:
     :parallelism - Threads for batch insert (default: available processors)

   Returns:
     New VectorIndex at target (same type as source)

   Example:
     (when (needs-compaction? idx)
       (let [compacted (compact idx {:store-config {...} :mmap-dir \"/data/v2\"})]
         (sync! compacted)
         compacted))"
  ([idx target]
   (compact idx target {}))
  ([idx target opts]
   (let [{:keys [parallelism]
          :or {parallelism (.availableProcessors (Runtime/getRuntime))}} opts
         ^PersistentEdgeStore pes (p/edge-storage idx)
         vs (p/vector-storage idx)
         vector-count (vectors/count-vectors vs)

         {:keys [store-config mmap-dir]} target

         _ (when-not (map? store-config)
             (throw (ex-info "compact target requires :store-config (map)"
                             {:target target
                              :hint "Pass {:store-config {:backend ... :id ...} :mmap-dir \"/path\"}"})))

         _ (when-not (string? mmap-dir)
             (throw (ex-info "compact target requires :mmap-dir (string)"
                             {:target target
                              :hint "Pass {:store-config {:backend ... :id ...} :mmap-dir \"/path\"}"})))

         ;; Identify live vectors (not deleted)
         live-ids (into []
                        (filter #(not (.isDeleted pes %)))
                        (range vector-count))
         live-count (count live-ids)

         _ (log/info :proximum/compaction "Starting compaction"
                     {:live-count live-count
                      :total-count vector-count
                      :live-percent (* 100.0 (/ live-count (max 1 vector-count)))})

         ;; Get index config and create new index of same type
         ;; This is the key polymorphic operation - works for any index type
         base-config (p/index-config idx)
         new-idx (p/create-index (merge base-config
                                        {:store-config store-config
                                         :mmap-dir mmap-dir
                                         :capacity live-count}))

         ;; Collect vectors and metadata, adding old-id mapping
         vectors-and-meta
         (mapv (fn [old-id]
                 (let [vec (p/get-vector idx old-id)
                       old-meta (or (p/get-metadata idx old-id) {})
                       ;; Add old-id mapping for adapter reconstruction
                       new-meta (assoc old-meta :old-id old-id)]
                   {:vector vec :metadata new-meta}))
               live-ids)

         vectors (mapv :vector vectors-and-meta)
         metadata (mapv :metadata vectors-and-meta)]

     ;; Batch insert with metadata
     (p/insert-batch new-idx vectors {:metadata metadata
                                      :parallelism parallelism}))))

(defn compact!
  "Compact index and sync to storage.

   Convenience wrapper that calls compact followed by sync!.
   See compact for full documentation."
  ([idx target]
   (compact! idx target {}))
  ([idx target opts]
   (-> (compact idx target opts)
       (p/sync!))))

;; -----------------------------------------------------------------------------
;; Online Compaction (zero-downtime)

(def ^:dynamic *default-max-delta-size*
  "Default maximum number of delta operations before throwing overflow error."
  100000)

;; Forward declarations for helpers used in deftype
;; These access CompactionState fields via .- accessors

(defn- check-delta-limit!
  "Check if delta log has exceeded the configured limit.
   Throws ex-info with :compaction/delta-overflow if exceeded."
  [state]
  (let [max-size (get (.-config state) :max-delta-size *default-max-delta-size*)
        current (count @(.-delta-log state))]
    (when (>= current max-size)
      (throw (ex-info "Delta log overflow during online compaction"
                      {:error :compaction/delta-overflow
                       :max-delta-size max-size
                       :current-size current
                       :hint "Finish compaction sooner or increase :max-delta-size"})))))

(defn- check-compaction-failed!
  "Check if background compaction has failed. Throws the captured error if so."
  [state]
  (when-let [error @(.-error-atom state)]
    (throw (ex-info "Online compaction background copy failed"
                    {:error :compaction/copy-failed}
                    error))))

(deftype CompactionState
         [source-idx      ;; Original index (updated via assoc on operations)
          batch-state     ;; Atom: {:idx <new-index> :id-mapping {old-id -> new-id}}
          delta-log       ;; Atom: vector of {:op :insert/:delete, :vector, :metadata, :id}
          copy-future     ;; Future for background copy
          finished?       ;; Atom: true when copy complete
          error-atom      ;; Atom: exception if background copy failed
          config          ;; Compaction config including :max-delta-size
          _meta]          ;; Metadata support

  ;; Collection protocols for CompactionState - identical semantics to HnswIndex
  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    ;; PURE external ID lookup - delegates to source index
    ;; Works with any key type including keywords
    (get source-idx k not-found))

  clojure.lang.IPersistentMap
  (assoc [this k v]
    ;; Delegate to p/insert for unified delta logging
    ;; Extract vector/metadata and add :external-id, then let insert handle the rest
    (let [vector (if (map? v) (:vector v) v)
          metadata (when (map? v) (:metadata v))
          full-metadata (assoc (or metadata {}) :external-id k)]
      (p/insert this vector full-metadata)))

  (assocEx [this k v]
    (if (.containsKey this k)
      (throw (ex-info "Key already exists during compaction" {:key k}))
      (.assoc this k v)))

  (without [this k]
    ;; Delegate to p/delete for unified delta logging
    ;; Look up internal ID, then let delete handle the rest
    (if-let [internal-id (meta/lookup-external-id (p/external-id-index source-idx) k)]
      (p/delete this internal-id)
      ;; Key doesn't exist, no-op
      this))

  (containsKey [this k]
    ;; Check if external ID exists in source index
    (contains? source-idx k))

  (entryAt [this k]
    ;; Get MapEntry for external ID
    (.entryAt ^clojure.lang.Associative source-idx k))

  clojure.lang.Seqable
  (seq [this]
    ;; Delegate to source index for iteration
    (seq source-idx))

  java.lang.Iterable
  (iterator [this]
    (.iterator ^java.lang.Iterable (or (seq this) [])))

  clojure.lang.Counted
  (count [this]
    ;; Delegate to source index
    (count source-idx))

  clojure.lang.IPersistentCollection
  (cons [this o]
    ;; o should be [external-id vector] or {:external-id ... :vector ...}
    (cond
      (vector? o)
      (.assoc this (first o) (second o))

      (map? o)
      (.assoc this (:external-id o) o)

      :else
      (throw (ex-info "cons expects [k v] pair or map with :external-id"
                      {:entry o}))))

  (empty [this]
    ;; Can't empty during compaction
    (throw (ex-info "empty not supported during online compaction"
                    {:hint "Finish or abort compaction first"})))

  (equiv [this other]
    (and (instance? CompactionState other)
         (= source-idx (.-source-idx ^CompactionState other))))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))
  (invoke [this k not-found]
    (.valAt this k not-found))

  clojure.lang.IMeta
  (meta [this]
    _meta)

  clojure.lang.IObj
  (withMeta [this m]
    (CompactionState. source-idx batch-state delta-log copy-future
                      finished? error-atom config m))

  clojure.lang.IEditableCollection
  (asTransient [this]
    ;; Transient not supported during compaction
    (throw (ex-info "transient not supported during online compaction"
                    {:hint "Finish or abort compaction first"}))))

(defn- copy-live-vectors!
  "Background copy of live vectors to new index.
   Updates batch-state atomically after each batch for race-free access.

   Uses deleted-snapshot (captured at compaction start) to determine which
   vectors to copy, ensuring consistent behavior regardless of concurrent deletes."
  [source-idx batch-state finished? error-atom batch-size snapshot-count deleted-snapshot]
  (try
    (let [;; Identify live vectors using snapshot of deleted state
          live-ids (into []
                         (filter #(not (aget ^booleans deleted-snapshot (int %))))
                         (range snapshot-count))
          total (count live-ids)]

      (log/info :proximum/compaction "Online compaction: copying live vectors" {:total total})

      ;; Copy in batches
      (doseq [batch (partition-all batch-size live-ids)]
        (let [;; Collect vectors and metadata
              vectors-and-meta
              (mapv (fn [old-id]
                      (let [vec (p/get-vector source-idx old-id)
                            old-meta (or (p/get-metadata source-idx old-id) {})
                            new-meta (assoc old-meta :old-id old-id)]
                        {:old-id old-id :vector vec :metadata new-meta}))
                    batch)

              vectors (mapv :vector vectors-and-meta)
              metadata (mapv :metadata vectors-and-meta)
              old-ids (mapv :old-id vectors-and-meta)]

          ;; Atomically update batch-state with new index and id-mappings
          (swap! batch-state
                 (fn [{:keys [idx id-mapping]}]
                   (let [new-count-before (p/count-vectors idx)
                         idx-after (p/insert-batch idx vectors {:metadata metadata})
                         ;; Build new mappings for this batch
                         new-mappings (into {}
                                            (map-indexed
                                             (fn [i old-id]
                                               [old-id (+ new-count-before i)])
                                             old-ids))]
                     {:idx idx-after
                      :id-mapping (merge id-mapping new-mappings)})))))

      (log/info :proximum/compaction "Online compaction: copy complete" {:total total})
      (reset! finished? true))
    (catch Exception e
      (log/error :proximum/compaction "Online compaction copy failed"
                 {:error (.getMessage e)})
      (reset! error-atom e)
      (throw e))))

(defn- cleanup-partial-compaction!
  "Clean up partial compaction state on failure.
   Closes the new index and attempts to delete created files."
  [state]
  (let [{:keys [store-config mmap-dir]} (get-in state [:config :target])]
    ;; Close new index
    (when-let [batch-state (.-batch-state state)]
      (when-let [new-idx (:idx @batch-state)]
        (try
          (p/close! new-idx)
          (catch Exception e
            (log/warn :proximum/compaction "Error closing partial index during cleanup"
                      {:error (.getMessage e)})))))

    ;; Clean up mmap files
    (when mmap-dir
      (let [dir (io/file mmap-dir)]
        (when (.exists dir)
          (try
            (doseq [f (reverse (file-seq dir))]
              (.delete ^java.io.File f))
            (catch Exception e
              (log/warn :proximum/compaction "Error cleaning up mmap directory"
                        {:mmap-dir mmap-dir :error (.getMessage e)}))))))

    ;; Clean up storage keys (best effort - may miss some due to merkle sharing)
    (when store-config
      (try
        (let [store (storage/connect-store-sync (storage/normalize-store-config store-config))]
          ;; Delete known top-level keys
          (k/dissoc store :index/config {:sync? true})
          (k/dissoc store :main {:sync? true})
          (k/dissoc store :branches {:sync? true}))
          ;; Note: chunk keys may remain due to merkle sharing complexity
          ;; Konserve stores are closed automatically via GC
        (catch Exception e
          (log/warn :proximum/compaction "Error cleaning up storage keys"
                    {:error (.getMessage e)}))))))

(defn start-online-compaction
  "Start online compaction with zero downtime.

   Returns a CompactionState that wraps the source index. Use this wrapper
   for all operations during compaction - it will dual-write to both
   the source index and a delta log.

   IMPORTANT: Use the returned state linearly. Do not branch or use old
   state values after new ones are created.

   Reads are served from the source index (which has all data).
   Writes (insert/delete) are captured in a delta log.

   Background thread copies live vectors to the new index.

   Call finish-online-compaction! when ready to complete the swap.

   Args:
     idx    - Source index to compact (any VectorIndex implementation)
     target - Map describing the new index storage:
              {:store-config <konserve-config-with-:id>
               :mmap-dir <directory-for-branch-mmaps>}

   Options:
     :batch-size     - Vectors per batch during copy (default 1000)
     :parallelism    - Threads for batch insert (default: available processors)
     :max-delta-size - Max delta operations before error (default 100000)

   Returns:
     CompactionState wrapper

   Example:
     (let [state (start-online-compaction idx {:store-config {...} :mmap-dir \"/data/v2\"})
           ;; Continue using state for reads/writes during compaction
           state (insert state new-vector)
           state (delete state old-id)
           ;; When ready, complete the compaction
           new-idx (finish-online-compaction! state)]
       new-idx)"
  ([idx target]
   (start-online-compaction idx target {}))
  ([idx target opts]
   (let [{:keys [batch-size parallelism max-delta-size]
          :or {batch-size 1000
               parallelism (.availableProcessors (Runtime/getRuntime))
               max-delta-size *default-max-delta-size*}} opts
         {:keys [store-config mmap-dir]} target

         _ (when-not (map? store-config)
             (throw (ex-info "start-online-compaction target requires :store-config (map)"
                             {:target target
                              :hint "Pass {:store-config {:backend ... :id ...} :mmap-dir \"/path\"}"})))

         _ (when-not (string? mmap-dir)
             (throw (ex-info "start-online-compaction target requires :mmap-dir (string)"
                             {:target target
                              :hint "Pass {:store-config {:backend ... :id ...} :mmap-dir \"/path\"}"})))

         ^PersistentEdgeStore pes (p/edge-storage idx)
         vs (p/vector-storage idx)
         vector-count (vectors/count-vectors vs)
         deleted-count (.getDeletedCount pes)
         live-count (- vector-count deleted-count)

         ;; Get index config and create new index of same type (polymorphic!)
         base-config (p/index-config idx)
         new-capacity (max live-count (* 2 live-count))
         new-idx (p/create-index (merge base-config
                                        {:store-config store-config
                                         :mmap-dir mmap-dir
                                         :capacity new-capacity}))

         ;; Single atom for batch state (idx + id-mapping) to avoid race conditions
         batch-state (atom {:idx new-idx :id-mapping {}})
         delta-log (atom [])
         finished? (atom false)
         error-atom (atom nil)

         ;; Capture vector count at start time
         snapshot-count vector-count

         ;; Capture deleted state snapshot
         deleted-nodes-bitset (.getDeletedNodesBitset pes)
         deleted-snapshot (let [arr (boolean-array snapshot-count)]
                            (dotimes [i snapshot-count]
                              (let [word-idx (bit-shift-right i 6)
                                    bit (bit-shift-left 1 (bit-and i 63))]
                                (aset arr i (not= 0 (bit-and (aget deleted-nodes-bitset word-idx) bit)))))
                            arr)

         config {:batch-size batch-size
                 :parallelism parallelism
                 :max-delta-size max-delta-size
                 :target target}

         ;; Start background copy
         copy-future (future
                       (copy-live-vectors! idx batch-state finished? error-atom
                                           batch-size snapshot-count deleted-snapshot))]

     (CompactionState.
      idx
      batch-state
      delta-log
      copy-future
      finished?
      error-atom
      config
      nil))))  ;; metadata; closes: constructor, let, arity, defn

;; VectorIndex implementation for CompactionState
(extend-type CompactionState
  p/VectorIndex
  (insert
    ([state vector]
     (p/insert state vector nil))
    ([state vector meta-map]
     ;; Check for errors and limits before proceeding
     (check-compaction-failed! state)
     (check-delta-limit! state)
     ;; Dual-write: insert into source, log for later application
     (let [source-after (p/insert (.-source-idx state) vector meta-map)]
       ;; Log the insert for later application to new index
       (swap! (.-delta-log state) conj
              {:op :insert
               :vector vector
               :metadata meta-map
               :source-id (dec (p/count-vectors source-after))})
       (CompactionState. source-after (.-batch-state state) (.-delta-log state)
                         (.-copy-future state) (.-finished? state) (.-error-atom state)
                         (.-config state) (.-_meta state)))))

  (insert-batch
    ([state vectors]
     (p/insert-batch state vectors {}))
    ([state vecs opts]
     ;; Check for errors before proceeding
     (check-compaction-failed! state)
     ;; Check delta limit for batch
     (let [max-size (get (.-config state) :max-delta-size *default-max-delta-size*)
           current (count @(.-delta-log state))
           batch-size (count vecs)]
       (when (>= (+ current batch-size) max-size)
         (throw (ex-info "Delta log overflow during online compaction"
                         {:error :compaction/delta-overflow
                          :max-delta-size max-size
                          :current-size current
                          :batch-size batch-size
                          :hint "Finish compaction sooner or increase :max-delta-size"}))))
     (let [{:keys [metadata]} opts
           source-before (.-source-idx state)
           count-before (p/count-vectors source-before)
           source-after (p/insert-batch source-before vecs opts)
           ;; Build all delta entries at once for efficiency (single swap!)
           delta-entries (mapv (fn [i v]
                                 {:op :insert
                                  :vector v
                                  :metadata (when metadata (nth metadata i nil))
                                  :source-id (+ count-before i)})
                               (range (count vecs))
                               vecs)]
       ;; Log batch insert atomically
       (swap! (.-delta-log state) into delta-entries)
       (CompactionState. source-after (.-batch-state state) (.-delta-log state)
                         (.-copy-future state) (.-finished? state) (.-error-atom state)
                         (.-config state) (.-_meta state)))))

  (search
    ([state query k]
     (p/search state query k {}))
    ([state query k opts]
     ;; Reads go to source index
     (p/search (.-source-idx state) query k opts)))

  (search-filtered
    ([state query k pred-fn]
     (p/search-filtered state query k pred-fn {}))
    ([state query k pred-fn opts]
     ;; Reads go to source index
     (p/search-filtered (.-source-idx state) query k pred-fn opts)))

  (delete [state id]
    ;; Check for errors and limits before proceeding
    (check-compaction-failed! state)
    (check-delta-limit! state)
    ;; Dual-write: delete from source, log for later
    (let [source-after (p/delete (.-source-idx state) id)]
      (swap! (.-delta-log state) conj
             {:op :delete :id id})
      (CompactionState. source-after (.-batch-state state) (.-delta-log state)
                        (.-copy-future state) (.-finished? state) (.-error-atom state)
                        (.-config state) (.-_meta state))))

  (count-vectors [state]
    (p/count-vectors (.-source-idx state)))

  (get-vector [state id]
    (p/get-vector (.-source-idx state) id))

  (get-metadata [state id]
    (p/get-metadata (.-source-idx state) id))

  (set-metadata [state id metadata]
    ;; Update metadata in source index and log for delta replay
    ;; Must dual-write like insert/delete to ensure consistency
    (check-compaction-failed! state)
    (check-delta-limit! state)
    (let [updated-source (p/set-metadata (.-source-idx state) id metadata)]
      ;; Log the metadata update for later application to new index
      (swap! (.-delta-log state) conj
             {:op :set-metadata
              :id id
              :metadata metadata})
      (CompactionState. updated-source
                        (.-batch-state state)
                        (.-delta-log state)
                        (.-copy-future state)
                        (.-finished? state)
                        (.-error-atom state)
                        (.-config state)
                        (meta state))))

  (capacity [state]
    (p/capacity (.-source-idx state)))

  (remaining-capacity [state]
    (p/remaining-capacity (.-source-idx state))))

;; IndexLifecycle implementation for CompactionState
(extend-type CompactionState
  p/IndexLifecycle
  (fork [state]
    ;; Forking during compaction is not supported - too complex to maintain
    ;; consistent state across both source and new index
    (throw (ex-info "Cannot fork during online compaction"
                    {:hint "Finish or abort compaction first"})))

  (sync!
    ([state] (p/sync! state {}))
    ([state opts]
    ;; Sync delegates to source during compaction - returns channel
     (a/go
       (let [synced-source (a/<! (p/sync! (.-source-idx state) opts))]
         (CompactionState. synced-source (.-batch-state state) (.-delta-log state)
                           (.-copy-future state) (.-finished? state) (.-error-atom state)
                           (.-config state) (.-_meta state))))))

  (flush! [state]
    ;; Properly thread the source-idx through
    (let [flushed-source (p/flush! (.-source-idx state))]
      (CompactionState. flushed-source (.-batch-state state) (.-delta-log state)
                        (.-copy-future state) (.-finished? state) (.-error-atom state)
                        (.-config state) (.-_meta state))))

  (close! [state]
    ;; Cancel copy if still running, close both indices
    (future-cancel (.-copy-future state))
    (p/close! (.-source-idx state))
    (when-let [batch-state (.-batch-state state)]
      (when-let [new-idx (:idx @batch-state)]
        (p/close! new-idx)))
    nil))

;; IndexIntrospection implementation for CompactionState
(extend-type CompactionState
  p/IndexIntrospection
  (index-type [state]
    (p/index-type (.-source-idx state)))

  (index-config [state]
    (p/index-config (.-source-idx state))))

;; IndexState implementation for CompactionState
;; Delegates to source index - compaction doesn't change versioning state
(extend-type CompactionState
  p/IndexState
  (storage [state]
    (p/storage (.-source-idx state)))

  (raw-storage [state]
    (p/raw-storage (.-source-idx state)))

  (current-branch [state]
    (p/current-branch (.-source-idx state)))

  (current-commit [state]
    (p/current-commit (.-source-idx state)))

  (vector-count-total [state]
    (p/vector-count-total (.-source-idx state)))

  (deleted-count-total [state]
    (p/deleted-count-total (.-source-idx state)))

  (mmap-dir [state]
    (p/mmap-dir (.-source-idx state)))

  (reflink-supported? [state]
    (p/reflink-supported? (.-source-idx state)))

  (crypto-hash? [state]
    (p/crypto-hash? (.-source-idx state)))

  (external-id-index [state]
    (p/external-id-index (.-source-idx state)))

  (metadata-index [state]
    (p/metadata-index (.-source-idx state)))

  (vector-storage [state]
    (p/vector-storage (.-source-idx state)))

  (edge-storage [state]
    (p/edge-storage (.-source-idx state))))

(extend-type CompactionState
  p/Snapshotable
  (snapshot-graph-state [state]
    (p/snapshot-graph-state (.-source-idx state)))

  p/Forkable
  (fork-graph-storage [state]
    (p/fork-graph-storage (.-source-idx state)))

  (fork-vector-storage [state branch-name]
    (p/fork-vector-storage (.-source-idx state) branch-name))

  (assemble-forked-index [state forked-vectors forked-graph new-branch new-commit-id]
    (p/assemble-forked-index (.-source-idx state) forked-vectors forked-graph new-branch new-commit-id))

  p/GraphMetrics
  (edge-count [state]
    (p/edge-count (.-source-idx state)))

  (graph-entrypoint [state]
    (p/graph-entrypoint (.-source-idx state)))

  (graph-max-level [state]
    (p/graph-max-level (.-source-idx state)))

  (expected-connectivity [state]
    (p/expected-connectivity (.-source-idx state))))

;; NearestNeighborSearch implementation for CompactionState
(extend-type CompactionState
  p/NearestNeighborSearch
  (nearest
    ([state query k]
     (p/nearest (.-source-idx state) query k))
    ([state query k opts]
     (p/nearest (.-source-idx state) query k opts)))

  (nearest-filtered
    ([state query k pred-fn]
     (p/nearest-filtered (.-source-idx state) query k pred-fn))
    ([state query k pred-fn opts]
     (p/nearest-filtered (.-source-idx state) query k pred-fn opts))))

(defn compaction-progress
  "Get current compaction progress.

   Returns:
     {:copying?     - true if background copy still running
      :finished?    - true if copy complete
      :failed?      - true if background copy failed
      :error        - exception if failed, nil otherwise
      :delta-count  - number of pending delta operations
      :mapped-ids   - number of IDs already mapped to new index}"
  [^CompactionState state]
  (let [error @(.-error-atom state)]
    {:copying? (and (not (future-done? (.-copy-future state)))
                    (nil? error))
     :finished? @(.-finished? state)
     :failed? (some? error)
     :error error
     :delta-count (count @(.-delta-log state))
     :mapped-ids (count (:id-mapping @(.-batch-state state)))}))

(defn finish-online-compaction!
  "Finish online compaction and return the new compacted index.

   Waits for background copy to complete, applies all delta operations
   with proper ID remapping, and syncs the new index.

   On failure, cleans up partial state (closes index, deletes files).

   Args:
     state - CompactionState from start-online-compaction

   Returns:
     New VectorIndex at the configured storage path

   Throws:
     If background copy failed or delta application fails"
  [^CompactionState state]
  (try
    ;; Wait for copy to complete
    (log/info :proximum/compaction "Finishing online compaction")
    @(.-copy-future state)  ;; Will throw if copy failed

    (let [{:keys [idx id-mapping]} @(.-batch-state state)
          delta-log @(.-delta-log state)

          ;; Track new ID mappings for inserts during delta apply
          new-mappings (atom {})]

      (log/info :proximum/compaction "Applying delta operations" {:count (count delta-log)})

      ;; Apply deltas in order
      (let [final-idx
            (reduce
             (fn [idx {:keys [op] :as delta}]
               (case op
                 :insert
                 (let [{:keys [vector metadata source-id]} delta
                       count-before (p/count-vectors idx)
                       idx-after (p/insert idx vector metadata)]
                    ;; Track mapping for this new insert
                   (swap! new-mappings assoc source-id count-before)
                   idx-after)

                 :delete
                 (let [{:keys [id]} delta]
                    ;; Find the ID in new index
                   (if-let [new-id (get id-mapping id)]
                     (p/delete idx new-id)
                     (if-let [new-id (get @new-mappings id)]
                       (p/delete idx new-id)
                        ;; Vector was already deleted before compaction started
                       idx)))

                 :set-metadata
                 (let [{:keys [id metadata]} delta]
                    ;; Find the ID in new index and update metadata
                   (if-let [new-id (get id-mapping id)]
                     (p/set-metadata idx new-id metadata)
                     (if-let [new-id (get @new-mappings id)]
                       (p/set-metadata idx new-id metadata)
                        ;; Vector doesn't exist in new index (was deleted)
                       idx)))))
             idx
             delta-log)]

        (log/info :proximum/compaction "Online compaction complete, syncing")
        ;; Return channel from sync!
        (p/sync! final-idx)))

    (catch Exception e
      (log/error :proximum/compaction "Online compaction failed, cleaning up"
                 {:error (.getMessage e)})
      (cleanup-partial-compaction! state)
      (throw (ex-info "Online compaction failed"
                      {:error :compaction/failed
                       :phase (if @(.-finished? state) :delta-application :copy)
                       :delta-count (count @(.-delta-log state))}
                      e)))))

(defn abort-online-compaction!
  "Abort online compaction and clean up.

   Cancels background copy, closes partial new index, and cleans up files.

   Returns:
     Source index (unchanged)"
  [^CompactionState state]
  (log/info :proximum/compaction "Aborting online compaction")
  (future-cancel (.-copy-future state))
  (cleanup-partial-compaction! state)
  (.-source-idx state))
