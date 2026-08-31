(ns proximum.generations
  "Branch-free immutable generations for embedding Proximum in another owner.

   A generation builder owns private logical vector and graph state. Sealing writes an
   immutable commit entry but never updates `:main`, another native branch head,
   or the native branch registry.  The embedding owner records the returned id
   in its own durable root and then calls `rooted!`; until then a Konserve GC
   guard protects all unreferenced writes made by the builder.

   Linear descendants share the source's append-only mmap cache while retaining
   independent logical counts, durable chunk roots, graphs, and close lifetimes.
   Only true branch divergence copies the mmap."
  (:require [clojure.core.async :as a]
            [konserve.core :as k]
            [konserve.gc-guard :as guard]
            [konserve.protocols :as kp]
            [proximum.api-impl :as api]
            [proximum.gc :as gc]
            [proximum.hnsw]
            [proximum.hnsw.internal :as hi]
            [proximum.protocols :as p]
            [proximum.storage :as storage]
            [proximum.vectors :as vectors]
            [proximum.writing :as writing])
  (:import [java.lang.ref Cleaner Cleaner$Cleanable]
           [proximum.internal PersistentEdgeIndex]))

(defrecord GenerationBuilder
           [index-atom source-generation workspace-id mmap-path
            store-id guard-token status closed?])

(defrecord GenerationPublicationHold
           [generation-id store-id guard-token status completed?])

(defrecord SealedGeneration
           [index generation-id mmap-path store-id guard-token status closed?
            hold-transferred?])

(defrecord GenerationViewResource [index generation-id mmap-path store-id refs closed?])

(defrecord GenerationView
           [index generation-id mmap-path store-id resource cleanable* closed?])

(defonce ^:private ^Cleaner generation-view-cleaner (Cleaner/create))

(defn- release-view-resource! [^GenerationViewResource resource]
  (locking resource
    (when-not @(:closed? resource)
      (if (> (long @(:refs resource)) 1)
        (swap! (:refs resource) dec)
        (try
          (let [result (a/<!! (p/close! (:index resource)))]
            (when (instance? Throwable result)
              (throw result)))
          (finally
            ;; Native close is not safely retryable after partial progress.
            ;; A failed close may leave the disposable cache file behind, but
            ;; no later lease may reuse the handle.
            (reset! (:closed? resource) true)))))))

(defrecord GenerationViewCleanup [resource closed?]
  Runnable
  (run [_]
    (when (compare-and-set! closed? false true)
      (try
        (release-view-resource! resource)
        ;; Cleaner has no caller to receive a native close failure.
        (catch Throwable _)))))

(defn- generation-view-lease [^GenerationViewResource resource]
  (let [cleanable* (atom nil)
        closed? (atom false)
        view (->GenerationView (:index resource) (:generation-id resource)
                               (:mmap-path resource) (:store-id resource)
                               resource cleanable* closed?)
        cleanup (->GenerationViewCleanup resource closed?)]
    (try
      (reset! cleanable* (.register generation-view-cleaner view cleanup))
      view
      (catch Throwable failure
        (release-view-resource! resource)
        (throw failure)))))

(defn- ensure-status!
  [status expected operation]
  (when-not (= expected @status)
    (throw (ex-info (str operation " requires generation status " expected)
                    {:reason :invalid-generation-status
                     :operation operation
                     :expected expected
                     :actual @status}))))

(defn- source-snapshot!
  [source]
  (let [commit-id (p/current-commit source)
        raw-store (p/raw-storage source)
        state-count (p/vector-count-total source)
        vector-store (p/vector-storage source)
        physical-count (vectors/count-vectors vector-store)
        snapshot (when (and raw-store commit-id)
                   (k/get raw-store commit-id nil {:sync? true}))
        pending-vectors (count @(:pending-writes vector-store))
        buffered-vectors (count @(:write-buffer vector-store))
        pending-edges (count @(hi/pending-edge-writes source))
        dirty-edges (count (.getDirtyChunks
                            ^PersistentEdgeIndex (p/edge-storage source)))]
    (when-not raw-store
      (throw (ex-info "A generation source requires durable Konserve storage"
                      {:reason :generation-requires-storage})))
    (when-not commit-id
      (throw (ex-info "A generation source must be a sealed durable snapshot"
                      {:reason :generation-source-unsealed})))
    (when-not snapshot
      (throw (ex-info "The source generation does not exist in storage"
                      {:reason :generation-not-found
                       :generation-id commit-id})))
    ;; Legacy persistent HnswIndex values can share one mutable VectorStore.
    ;; Refuse such a contaminated handle. Generation descendants instead get a
    ;; distinct logical VectorStore handle over a ref-counted mmap resource.
    (when (or (not= state-count physical-count)
              (not= state-count (:branch-vector-count snapshot))
              (pos? pending-vectors)
              (pos? buffered-vectors)
              (pos? pending-edges)
              (pos? dirty-edges)
              (p/unsynced-metadata? source))
      (throw (ex-info "The source handle's shared writer state changed after it was sealed"
                      {:reason :generation-source-storage-mutated
                       :generation-id commit-id
                       :logical-vector-count state-count
                       :physical-vector-count physical-count
                       :snapshot-vector-count (:branch-vector-count snapshot)
                       :pending-vector-writes pending-vectors
                       :buffered-vectors buffered-vectors
                       :pending-edge-writes pending-edges
                       :dirty-edge-chunks dirty-edges
                       :unsynced-metadata? (p/unsynced-metadata? source)})))
    snapshot))

(defn begin-generation
  "Create an isolated private writer from the exact sealed `source` generation.

   The source must have `:mmap-dir` configured.  No native branch ref is created.
   Builder operations are intentionally mutable and linear; do not use a builder
   concurrently."
  [source]
  (let [raw-store (p/raw-storage source)
        store-id (when raw-store (kp/store-id raw-store))
        mmap-dir (p/mmap-dir source)]
    (when-not mmap-dir
      (throw (ex-info "Generation builders require :mmap-dir"
                      {:reason :generation-requires-mmap-dir})))
    (when-not store-id
      (throw (ex-info "Generation builders require a stable Konserve store id"
                      {:reason :generation-requires-store-id})))
    ;; Validation and zero-copy derivation synchronize on VectorStore, closing
    ;; the gap in which a legacy descendant could mutate the source handle.
    (locking (p/vector-storage source)
      (source-snapshot! source)
      (let [source-id (p/current-commit source)
            workspace-id (keyword "proximum.generation" (str (random-uuid)))
            token (guard/writing! store-id)
            forked-vectors* (atom nil)]
        (try
          (let [forked-vectors (vectors/fork-linear-store!
                                (p/vector-storage source))
                _ (reset! forked-vectors* forked-vectors)
                forked-graph (p/fork-graph-storage source)
                private-index (p/assemble-forked-index
                               source forked-vectors forked-graph
                               workspace-id source-id)]
            (->GenerationBuilder
             (atom private-index) source-id workspace-id
             (:mmap-path forked-vectors) store-id token (atom :open) (atom false)))
          (catch Throwable e
            (when-let [forked-vectors @forked-vectors*]
              (a/<!! (vectors/close! forked-vectors)))
            (guard/done! store-id token)
            (throw e)))))))

(defn- validate-store-owner!
  [config raw-store]
  (let [store-id (kp/store-id raw-store)]
    (when (= (:forbidden-store-id config) store-id)
      (throw
       (ex-info
        "The generation store must not be the embedding owner's primary store."
        {:type :proximum/generation-store-forbidden
         :reason :generation-store-forbidden
         :store-id store-id})))
    raw-store))

(defn- configured-store
  [{:keys [store store-config] :as config}]
  (validate-store-owner!
   config
   (or store
       (when store-config
         (storage/connect-store-sync
          (storage/normalize-store-config store-config)))
       (throw (ex-info "Generation config requires :store or :store-config"
                       {:reason :generation-requires-storage})))))

(defn- configured-bootstrap-store
  [{:keys [store store-config] :as generation-config}]
  (validate-store-owner!
   generation-config
   (or store
       (when store-config
         (let [config (dissoc (storage/normalize-store-config store-config) :opts)]
           (if (k/store-exists? config {:sync? true})
             (k/connect-store config {:sync? true})
             (try
               (k/create-store config {:sync? true})
               (catch Throwable creation-failure
                 ;; Another bootstrap may have won between exists? and create.
                 ;; Only recover when the store demonstrably exists now; a real
                 ;; creation failure must retain its original cause.
                 (if (k/store-exists? config {:sync? true})
                   (k/connect-store config {:sync? true})
                   (throw creation-failure)))))))
       (throw (ex-info "Generation config requires :store or :store-config"
                       {:reason :generation-requires-storage})))))

(defn begin-generation-from-config
  "Create a private, rootless generation builder from index configuration.

   Unlike ordinary `create-index`, this does not create a native branch, head,
   or branch-registry entry.  It does write the store-wide immutable index
   configuration needed to reopen a sealed generation.  The GC guard is held
   before that write and remains held until `rooted!` or `discard!`.

   Continuing from an existing generation shares its append-only mmap cache
   along one linear chain. Independent historical opens still use private
   caches, and native branch divergence still uses an independent mmap copy.

   Required config keys are the ordinary HNSW creation keys plus `:mmap-dir`
   and either `:store` or `:store-config`. An embedding owner can pass its own
   canonical Konserve id as `:forbidden-store-id`; the connected generation
   store is then refused when its live `PStoreConfig` identity matches."
  [config]
  (let [mmap-dir (:mmap-dir config)]
    (when-not mmap-dir
      (throw (ex-info "Generation builders require :mmap-dir"
                      {:reason :generation-requires-mmap-dir})))
    (let [raw-store (configured-bootstrap-store config)
          store-id (kp/store-id raw-store)]
      (when-not store-id
        (throw (ex-info "Generation builders require a stable Konserve store id"
                        {:reason :generation-requires-store-id})))
      (let [existing-config (k/get raw-store :index/config nil {:sync? true})
            incompatible
            (when existing-config
              (seq
               (keep (fn [[requested-key stored-key]]
                       (when (and (contains? config requested-key)
                                  (not= (get config requested-key)
                                        (get existing-config stored-key)))
                         {:key requested-key
                          :requested (get config requested-key)
                          :stored (get existing-config stored-key)}))
                     [[:dim :dim]
                      [:type :index-type]
                      [:distance :distance]
                      [:capacity :max-nodes]
                      [:M :M]
                      [:max-levels :max-level]
                      [:chunk-size :chunk-size]
                      [:crypto-hash? :crypto-hash?]
                      [:ef-construction :ef-construction]
                      [:ef-search :ef-search]
                      [:seed :seed]])))
            _ (when incompatible
                (throw (ex-info
                        "Generation config conflicts with this store's immutable index config"
                        {:reason :generation-config-conflict
                         :conflicts (vec incompatible)})))
            config (cond-> config
                     existing-config
                     (merge {:type (:index-type existing-config)
                             :dim (:dim existing-config)
                             :distance (:distance existing-config)
                             :capacity (:max-nodes existing-config)
                             :M (:M existing-config)
                             :max-levels (:max-level existing-config)
                             :chunk-size (:chunk-size existing-config)
                             :crypto-hash? (:crypto-hash? existing-config)
                             :ef-construction (:ef-construction existing-config)
                             :ef-search (:ef-search existing-config)
                             :seed (:seed existing-config)}))
            workspace-id (keyword "proximum.generation" (str (random-uuid)))
            token (guard/writing! store-id)]
        (try
          (let [private-index
                (p/create-index
                 (-> config
                     (dissoc :store-config :forbidden-store-id)
                     (assoc :store raw-store
                            :branch workspace-id
                            :register-branch? false)))
                vector-store (p/vector-storage private-index)
                _ (vectors/mark-mmap-disposable! vector-store)
                mmap-path (:mmap-path vector-store)]
            (->GenerationBuilder
             (atom private-index) nil workspace-id mmap-path
             store-id token (atom :open) (atom false)))
          (catch Throwable e
            (guard/done! store-id token)
            (throw e)))))))

(defn put!
  "Insert `id` into a private builder and return the same builder."
  ([builder id vector] (put! builder id vector nil))
  ([^GenerationBuilder builder id vector metadata]
   (locking builder
     (ensure-status! (:status builder) :open "put!")
     (swap! (:index-atom builder) api/insert vector id metadata)
     builder)))

(defn put-batch!
  "Insert one validated batch into a private builder with a single persistent
   HNSW fork. `vectors` and `ids` follow `proximum.core/insert-batch`; optional
   opts include `:metadata` and `:parallelism`."
  ([builder vectors ids]
   (put-batch! builder vectors ids nil))
  ([^GenerationBuilder builder vectors ids opts]
   (locking builder
     (ensure-status! (:status builder) :open "put-batch!")
     (swap! (:index-atom builder) api/insert-batch vectors ids opts)
     builder)))

(defn delete!
  "Delete `id` from a private builder and return the same builder."
  [^GenerationBuilder builder id]
  (locking builder
    (ensure-status! (:status builder) :open "delete!")
    (swap! (:index-atom builder) api/delete id)
    builder))

(defn builder-index
  "Return the builder's private query view.  Intended for diagnostics/tests."
  [^GenerationBuilder builder]
  @(:index-atom builder))

(defn seal!
  "Durably seal a builder without moving a native branch ref.

   This JVM API blocks for the existing asynchronous Proximum flush.  The
   returned generation continues to hold its GC guard.  After durably recording
   `generation-id` in the owning database root, call `rooted!`.  On failure the
   builder becomes `:failed`; call `discard!` to release its resources."
  [^GenerationBuilder builder]
  (locking builder
    (ensure-status! (:status builder) :open "seal!")
    (reset! (:status builder) :sealing)
    (try
      (let [parents (if-let [source (:source-generation builder)] #{source} #{})
            result (a/<!! (p/sync! @(:index-atom builder)
                                   {:parents parents
                                    :publish-branch? false
                                    :return-errors? true}))]
        (if (instance? Throwable result)
          (do
            (reset! (:status builder) :failed)
            (throw result))
          (if-not result
            (do
              (reset! (:status builder) :failed)
              (throw (ex-info "Generation sealing failed without a result"
                              {:reason :generation-seal-failed})))
            (do
              (reset! (:index-atom builder) result)
              (reset! (:status builder) :sealed-unrooted)
              (->SealedGeneration
               result (p/current-commit result) (:mmap-path builder)
               (:store-id builder) (:guard-token builder) (:status builder)
               (:closed? builder) (atom false))))))
      (catch Throwable e
        (when (= :sealing @(:status builder))
          (reset! (:status builder) :failed))
        (throw e)))))

(defn rooted!
  "Acknowledge that an owner durably recorded this generation id.

   Releases the unreferenced-write guard. This does not move any Proximum
   branch ref. Acknowledgement is idempotent because an embedding owner may
   prepare the same immutable generation for overlapping commit attempts; each
   successful attempt is allowed to acknowledge the generation it published."
  [^SealedGeneration generation]
  (locking generation
    (when @(:hold-transferred? generation)
      (throw (ex-info "The generation publication hold belongs to another owner"
                      {:reason :generation-publication-hold-transferred
                       :generation-id (:generation-id generation)})))
    (case @(:status generation)
      :sealed-unrooted
      (do
        (guard/done! (:store-id generation) (:guard-token generation))
        (reset! (:status generation) :rooted)
        generation)

      :rooted generation

      (ensure-status! (:status generation) :sealed-unrooted "rooted!"))))

(defn generation-id [generation]
  (:generation-id generation))

(defn generation-store-id
  "Return the canonical identity of the live Konserve generation store."
  [generation]
  (:store-id generation))

(defn generation-index [generation]
  (:index generation))

(defn take-publication-hold!
  "Detach the lightweight GC publication hold from a sealed generation.

   The hold can outlive the native index/mmap and can be carried across a chain
   of unpublished child generations without retaining every graph in memory.
   Complete it with `root-publication!` after the owner root lands or
   `abort-publication!` after publication is definitively known not to land.
   An ambiguous publication must retain the hold."
  [^SealedGeneration generation]
  (locking generation
    (ensure-status! (:status generation) :sealed-unrooted
                    "take-publication-hold!")
    (when-not (compare-and-set! (:hold-transferred? generation) false true)
      (throw (ex-info "The generation publication hold was already transferred"
                      {:reason :generation-publication-hold-transferred
                       :generation-id (:generation-id generation)})))
    (->GenerationPublicationHold
     (:generation-id generation) (:store-id generation)
     (:guard-token generation) (:status generation) (atom false))))

(defn root-publication!
  "Release a transferred hold after an owner durably publishes its generation."
  [^GenerationPublicationHold hold]
  (locking hold
    (when-not @(:completed? hold)
      (ensure-status! (:status hold) :sealed-unrooted "root-publication!")
      (guard/done! (:store-id hold) (:guard-token hold))
      (reset! (:status hold) :rooted)
      (reset! (:completed? hold) true)))
  hold)

(defn abort-publication!
  "Release a transferred hold after publication is definitively aborted."
  [^GenerationPublicationHold hold]
  (locking hold
    (when-not @(:completed? hold)
      (ensure-status! (:status hold) :sealed-unrooted "abort-publication!")
      (guard/done! (:store-id hold) (:guard-token hold))
      (reset! (:status hold) :discarded)
      (reset! (:completed? hold) true)))
  hold)

(defn take-generation-view!
  "Transfer a sealed generation's live native handle into an immutable view.

   This avoids closing and immediately restoring the generation after its
   commit has already been sealed.  The returned view becomes the sole owner
   of the mmap and native index handle; the sealed value retains only the GC
   guard acknowledgement lifecycle used by `rooted!` and `discard!`.

   Ownership transfer is one-shot.  On an aborted publication the caller must
   both `discard!` the sealed value (to release its guard) and `close-view!`
   the returned view (to release the transferred native resources)."
  [^SealedGeneration generation]
  (let [status (:status generation)
        closed? (:closed? generation)]
    (locking generation
      (when-not (#{:sealed-unrooted :rooted} @status)
        (throw (ex-info "Only a sealed generation can transfer its query view"
                        {:reason :invalid-generation-status
                         :operation "take-generation-view!"
                         :expected #{:sealed-unrooted :rooted}
                         :actual @status})))
      ;; The sealed value used to own this handle directly. Mark that ownership
      ;; closed before exposing the view, so close/discard on the guard handle
      ;; cannot unmap storage underneath its new owner.
      (when-not (compare-and-set! closed? false true)
        (throw (ex-info "The sealed generation's query view was already transferred or closed"
                        {:reason :generation-view-already-transferred
                         :generation-id (:generation-id generation)})))
      (let [resource (->GenerationViewResource
                      (:index generation) (:generation-id generation)
                      (:mmap-path generation) (:store-id generation)
                      (atom 1) (atom false))]
        (generation-view-lease resource)))))

(defn- index-handle [source]
  (cond
    (instance? GenerationBuilder source) @(:index-atom source)
    (or (instance? SealedGeneration source)
        (instance? GenerationView source)) (:index source)
    :else source))

(defn- generation-config?
  [source]
  (let [idx (index-handle source)]
    (and (map? source)
         (not (satisfies? p/IndexState idx))
         (or (contains? source :store)
             (contains? source :store-config)))))

(defn- source-store-and-storage
  [source]
  (if (generation-config? source)
    (let [raw-store (configured-store source)
          index-config (k/get raw-store :index/config nil {:sync? true})]
      (when-not index-config
        (throw (ex-info "Proximum index configuration not found"
                        {:reason :generation-config-not-found})))
      [raw-store
       (storage/create-storage raw-store
                               {:cache-size (or (:cache-size source) 10000)
                                :crypto-hash? (:crypto-hash? index-config)})])
    (let [idx (index-handle source)]
      [(p/raw-storage idx) (p/storage idx)])))

(defn reachable-keys
  "Return the exact Konserve key set needed to restore `generation-id`.

   `source` may be an index/generation handle or a config containing `:store`
   or `:store-config`."
  [source generation-id]
  (let [[raw-store pss-storage] (source-store-and-storage source)]
    (gc/generation-reachable-keys raw-store pss-storage generation-id)))

(defn- unique-generation-mmap
  [mmap-dir generation-id]
  (str mmap-dir "/generation-" generation-id "-" (random-uuid) ".bin"))

(defn open-generation
  "Open an exact immutable generation by id, never through a branch head.

  `source` supplies the store and immutable index configuration.  Every open
  receives a distinct mmap cache, so unrelated historical views cannot rewrite
  each other's local bytes. `retain-generation-view` creates another logical
  owner of one already-open read handle without copying that cache."
  [source generation-id]
  (let [config? (generation-config? source)
        idx (when-not config? (index-handle source))
        raw-store (if config? (configured-store source) (p/raw-storage idx))
        mmap-dir (or (when config? (:mmap-dir source))
                     (when idx (p/mmap-dir idx))
                     (System/getProperty "java.io.tmpdir"))
        mmap-path (unique-generation-mmap mmap-dir generation-id)
        idx (writing/load-commit nil generation-id
                                 :store raw-store
                                 :mmap-dir mmap-dir
                                 :mmap-path mmap-path)
        _ (vectors/mark-mmap-disposable! (p/vector-storage idx))
        resource (->GenerationViewResource idx (p/current-commit idx) mmap-path
                                           (kp/store-id raw-store)
                                           (atom 1) (atom false))]
    (generation-view-lease resource)))

(defn retain-generation-view
  "Return an independently closeable lease on an existing immutable view.

  This is for immutable wrapper transitions such as Datahike preparation: two
  DB values may own the same exact native query handle, but closing either must
  not unmap it underneath the other. Independent `open-generation` calls still
  receive private mmap caches, because the raw Proximum index API can derive
  mutable descendants and sharing those local bytes broadly would be unsafe."
  [^GenerationView view]
  (let [resource (:resource view)]
    (locking resource
      (when (or @(:closed? view) @(:closed? resource))
        (throw (ex-info "Cannot retain a closed generation view"
                        {:reason :generation-view-closed
                         :generation-id (:generation-id view)})))
      (swap! (:refs resource) inc))
    (generation-view-lease resource)))

(defn close-view!
  "Close a generation/view and release its mmap-cache reference.

   A private historical cache is deleted with its final reference. A linear
   descendant of a native branch shares that branch's reusable cache, so closing
   the generation does not delete the branch cache.

   A sealed-but-unrooted generation must first be acknowledged with `rooted!`
   or abandoned with `discard!`."
  [generation]
  (let [idx (:index generation)
        closed? (:closed? generation)]
    (when (and (instance? SealedGeneration generation)
               (= :sealed-unrooted @(:status generation))
               (not @(:hold-transferred? generation)))
      (throw (ex-info "Cannot close an unrooted generation; call rooted! or discard!"
                      {:reason :generation-unrooted
                       :generation-id (:generation-id generation)})))
    (cond
      (instance? GenerationView generation)
      (if (compare-and-set! closed? false true)
        (a/thread
          (try
            (release-view-resource! (:resource generation))
            (finally
              (.clean ^Cleaner$Cleanable @(:cleanable* generation))))
          nil)
        (doto (a/chan) a/close!))

      (compare-and-set! closed? false true)
      (a/go
        (a/<! (p/close! idx))
        nil)

      :else
      (doto (a/chan) a/close!))))

(defn discard!
  "Abandon a builder or an unrooted sealed generation and release its guard.

   Immutable objects already written remain harmless garbage for the next GC."
  [generation]
  (locking generation
    (let [status (:status generation)
          current @status
          idx (if (instance? GenerationBuilder generation)
                @(:index-atom generation)
                (:index generation))
          closed? (:closed? generation)]
      (when (and (not (#{:discarded :rooted} current))
                 (or (instance? GenerationBuilder generation)
                     (not @(:hold-transferred? generation))))
        (guard/done! (:store-id generation) (:guard-token generation))
        (reset! status :discarded))
      (if (compare-and-set! closed? false true)
        (a/go
          (a/<! (p/close! idx))
          nil)
        (doto (a/chan) a/close!)))))
