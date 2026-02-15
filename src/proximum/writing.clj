(ns proximum.writing
  "Index persistence operations.

   This namespace provides durable storage operations:
   - load/load-commit: Load indices from storage
   - sync!: Create durable commits
   - build-index-snapshot: Create snapshots for storage

   Branch management and versioning operations are in proximum.versioning.

   Note: This namespace shadows clojure.core/load."
  (:refer-clojure :exclude [load])
  (:require [proximum.protocols :as p]
            [proximum.vectors :as vectors]
            [proximum.storage :as storage]
            [proximum.edges :as edges]
            [proximum.crypto :as crypto]
            [proximum.logging :as log]
            [org.replikativ.persistent-sorted-set :as pss]
            [konserve.core :as k]
            [clojure.core.async :as a]))

;; Re-export store helpers for external use
(def normalize-store-config
  "Validate and normalize a Konserve store-config.
   See proximum.storage/normalize-store-config for details."
  storage/normalize-store-config)

(def connect-store-sync
  "Connect to a Konserve store synchronously.
   See proximum.storage/connect-store-sync for details."
  storage/connect-store-sync)

;; -----------------------------------------------------------------------------
;; Snapshot Building

(defn build-index-snapshot
  "Build an index snapshot for storage.
   Contains all mutable state needed to restore the index.

   Note: When crypto-hash? is enabled, the commit-id IS the merkle hash.
   No separate commit-hash field is stored in the snapshot.

   Args:
     index                 - VectorIndex implementation
     commit-id             - UUID for this commit (merkle hash if crypto-hash? enabled)
     parents               - Set of parent commit IDs
     metadata-pss-root     - PSS root for metadata
     external-id-pss-root  - PSS root for external-id index
     vectors-addr-pss-root - PSS root for vector chunk addresses
     edges-addr-pss-root   - PSS root for edge chunk addresses
     now                   - Timestamp"
  [index commit-id parents metadata-pss-root external-id-pss-root
   vectors-addr-pss-root edges-addr-pss-root now]
  (let [graph-state (p/snapshot-graph-state index)
        vs (p/vector-storage index)]
    (cond->
     {;; Index type for restore-index dispatch
      :index-type (p/index-type index)

       ;; Versioning
       ;; Note: commit-id is merkle hash when crypto-hash? enabled, random UUID otherwise
      :commit-id   commit-id
      :parents     parents
      :created-at  now
      :updated-at  now
      :branch              (p/current-branch index)

       ;; Vector state
      :vector-count         (p/vector-count-total index)
      :branch-vector-count  (p/vector-count-total index)
      :branch-deleted-count (p/deleted-count-total index)

       ;; Graph state (from Snapshotable protocol)
      :entrypoint           (:entrypoint graph-state)
      :current-max-level    (:max-level graph-state)
      :deleted-nodes-bitset (:deleted-nodes-bitset graph-state)

       ;; PSS roots (structural sharing)
      :metadata-pss-root     metadata-pss-root
      :external-id-pss-root  external-id-pss-root
      :vectors-addr-pss-root vectors-addr-pss-root
      :edges-addr-pss-root   edges-addr-pss-root}
       ;; Note: mmap-path is NOT stored in snapshots - it's derived from
       ;; mmap-dir + branch at load time for portability

      ;; Vectors commit hash if present (for vector-level verification)
      (and (:commit-hash vs) @(:commit-hash vs))
      (assoc :vectors-commit-hash @(:commit-hash vs)))))

;; -----------------------------------------------------------------------------
;; Generic Helpers for Persistence
;; These functions are used by index-specific sync! implementations

(defn write-commit!
  "Write a commit snapshot to storage atomically.

   Writes both the commit entry (by commit-id) and updates the branch head.

   Args:
     store      - Konserve store
     commit-id  - UUID for this commit
     branch     - Branch keyword
     snapshot   - Index snapshot map

   Returns: nil"
  [store commit-id branch snapshot]
  (k/assoc store commit-id snapshot {:sync? true})
  (k/assoc store branch snapshot {:sync? true}))

(defn generate-commit-id
  "Generate a commit ID based on crypto-hash settings.

   Args:
     crypto-hash? - Boolean, whether content-addressed IDs are enabled
     merkle-hash  - Merkle hash (or nil), used when crypto-hash? is true

   Returns: UUID (either merkle hash or random UUID)"
  [crypto-hash? merkle-hash]
  (if crypto-hash?
    (or merkle-hash (java.util.UUID/randomUUID))
    (java.util.UUID/randomUUID)))

(defn determine-parents
  "Determine parent commits for a new commit.

   Args:
     prev-commit - Previous commit UUID (or nil for first commit)

   Returns: Set of parent UUIDs (empty set for root commit)"
  [prev-commit]
  (if prev-commit
    #{prev-commit}
    #{}))

;; -----------------------------------------------------------------------------
;; load / load-commit - Index Loading

(defn load
  "Load an index branch from durable storage.

   Loads the index snapshot from the specified branch and restores
   all state. Dispatches on :index-type in snapshot for polymorphic restoration.

   Args:
     store-config - Konserve store config map (must include :id)

   Options:
     :branch       - Branch keyword to load (default :main)
     :cache-size   - LRU cache size (default 10000)
     :mmap-dir     - Directory for branch mmap files (recommended)
     :mmap-path    - Explicit mmap file path override (advanced)
     :store        - Pre-instantiated Konserve store (optional)

   Returns:
     VectorIndex implementation loaded from the specified branch"
  [store-config & {:keys [branch cache-size mmap-dir mmap-path store]
                   :or {branch :main
                        cache-size 10000}}]
  (let [store-config (when store-config (normalize-store-config store-config))
        edge-store (or store
                       (when store-config (connect-store-sync store-config))
                       (throw (ex-info "load requires either store-config (map) or :store (instance)"
                                       {:hint "Use (load {:backend ... :id ...} :mmap-dir \"/path\" ...) or pass :store"})))
        ;; Load snapshot from edge-store
        snapshot (k/get edge-store branch nil {:sync? true})]
    (when-not snapshot
      (throw (ex-info "Branch not found in storage"
                      {:branch branch
                       :available-branches (k/get edge-store :branches nil {:sync? true})})))

    ;; Get index-type from snapshot (new) or config (legacy fallback)
    (let [config (k/get edge-store :index/config nil {:sync? true})
          index-type (or (:index-type snapshot)
                         (:index-type config)
                         :hnsw)]  ;; Default to :hnsw for backwards compatibility
      ;; Dispatch to type-specific restoration
      (p/restore-index (assoc snapshot :index-type index-type)
                       edge-store
                       {:mmap-dir mmap-dir
                        :mmap-path mmap-path
                        :cache-size cache-size}))))

(defn load-commit
  "Load a historical commit from durable storage.

   Args:
     store-config - Konserve store config map (must include :id)
     commit-id    - UUID (or UUID string) of the commit to load

   Options:
     :branch       - Branch keyword to attach to returned index (default :main)
     :cache-size   - LRU cache size (default 10000)
     :mmap-dir     - Directory for branch mmap files (recommended)
     :mmap-path    - Explicit mmap file path override (advanced)
     :store        - Pre-instantiated Konserve store (optional)

   Returns:
     VectorIndex implementation restored to the specified commit"
  [store-config commit-id & {:keys [branch cache-size mmap-dir mmap-path store]
                             :or {branch :main
                                  cache-size 10000}}]
  (let [store-config (when store-config (normalize-store-config store-config))
        edge-store (or store
                       (when store-config (connect-store-sync store-config))
                       (throw (ex-info "load-commit requires either store-config (map) or :store (instance)"
                                       {:hint "Pass store-config or :store"})))
        commit-uuid (cond
                      (instance? java.util.UUID commit-id) commit-id
                      (string? commit-id) (java.util.UUID/fromString commit-id)
                      :else (throw (ex-info "Invalid commit-id (expected UUID or UUID string)"
                                            {:commit-id commit-id
                                             :commit-id-type (type commit-id)})))
        snapshot (k/get edge-store commit-uuid nil {:sync? true})]
    (when-not snapshot
      (throw (ex-info "Commit not found in storage"
                      {:commit-id commit-uuid
                       :hint "Use (history (load store-config :branch ...)) to enumerate commits"})))

    ;; Get index-type from snapshot (new) or config (legacy fallback)
    (let [config (k/get edge-store :index/config nil {:sync? true})
          index-type (or (:index-type snapshot)
                         (:index-type config)
                         :hnsw)
          snapshot-branch (:branch snapshot)
          actual-branch (or snapshot-branch branch)]
      (p/restore-index (assoc snapshot :index-type index-type :branch actual-branch)
                       edge-store
                       {:mmap-dir mmap-dir
                        :mmap-path mmap-path
                        :cache-size cache-size}))))

