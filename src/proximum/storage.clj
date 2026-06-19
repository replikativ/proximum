(ns proximum.storage
  "Storage backend for PersistentSortedSet edges.

  Provides lazy loading and incremental persistence for the HNSW edge index,
  following persistent-sorted-set's IStorage pattern.

  Edges are stored as {:key [layer node-id] :neighbors [neighbor-ids...]}

  When crypto-hash? is enabled, PSS node addresses are content-addressed using
  merkle tree hashing (like datahike). This provides automatic integrity:
  - Branch address = hash of child addresses (merkle)
  - Leaf address = hash of keys
  - Root address IS the merkle root - no separate hash tracking needed"
  (:require [clojure.core.cache :as cache]
            [konserve.core :as k]
            [konserve.utils :as k-utils]
            [konserve.serializers :refer [fressian-serializer]]
            [hasch.core :as hasch]
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           [org.replikativ.persistent_sorted_set IStorage Leaf Branch ANode Settings PersistentSortedSet]
           [java.util List UUID]))

;;; Address Generation

(defn gen-address
  "Generate UUID address for B-tree nodes.

   When crypto-hash? is true, computes content-based address (merkle tree):
   - Branch: hash of child addresses (merkle interior node)
   - Leaf: hash of keys (merkle leaf)

   When crypto-hash? is false, generates random UUID (faster, no integrity)."
  ([^ANode node crypto-hash?]
   (if crypto-hash?
     (if (instance? Branch node)
       ;; Branch: merkle hash of child addresses
       (hasch/uuid (vec (.addresses ^Branch node)))
       ;; Leaf: hash of keys (convert to vec for consistent hashing)
       (hasch/uuid (vec (.keys ^Leaf node))))
     (UUID/randomUUID)))
  ([]
   (UUID/randomUUID)))

;;; CachedStorage Implementation

(defrecord CachedStorage [store config cache stats pending-writes]
  IStorage
  (store [this node]
    (swap! stats update :writes inc)
    (let [address (gen-address node (:crypto-hash? config))]
      (swap! pending-writes conj [address node])
      (swap! cache cache/miss address node)
      address))

  (accessed [this address]
    (swap! stats update :accessed inc)
    (swap! cache cache/hit address)
    nil)

  (restore [this address]
    (if-let [cached (cache/lookup @cache address)]
      (do
        (swap! cache cache/hit address)
        cached)
      (let [node (k/get store address nil {:sync? true})]
        (when (nil? node)
          (throw (ex-info "Node not found in storage" {:address address
                                                       :crypto-hash? (:crypto-hash? config)})))
        (swap! stats update :reads inc)
        (swap! cache cache/miss address node)
        node))))

;;; Fressian Handlers for PSS Nodes

(def branching-factor 512)

(defn- map->settings ^Settings [m]
  (Settings. (int (or (:branching-factor m) branching-factor)) nil))

(defn create-fressian-handlers
  "Create Fressian handlers for PersistentSortedSet nodes.

   The Leaf/Branch NODE handlers are the canonical, shared ones from
   `org.replikativ.persistent-sorted-set.fressian` (one node codec across datahike /
   yggdrasil / proximum / stratum). Only the ROOT (PersistentSortedSet) handler is
   proximum-specific — it carries the storage back-reference (`storage-atom`) for lazy
   child loading and the flush invariant. Elements are fressian-native maps, so no
   element handler is needed.

   The storage-atom is used for circular reference during deserialization -
   the PersistentSortedSet needs a reference to its storage to lazy-load nodes."
  [storage-atom]
  (let [settings  (map->settings {:branching-factor branching-factor})
        pss-rh    (pss-fress/read-handlers settings)              ; pss/leaf + pss/branch
        ;; proximum has ONE local store, so storage resolves to a constant (the
        ;; circular-ref atom); comparator defaults to nil (the address-map sets'
        ;; ordering is stamped on descent).
        root-read (pss-fress/root-read-handler {:settings settings
                                                :resolve-storage (fn [_] @storage-atom)})]
    {:read-handlers
     (merge
      {pss-fress/set-tag root-read}                              ; pss/set
      pss-rh
      ;; BACKWARDS COMPAT: pre-canonical proximum.*-tagged root/leaf/branch blobs read
      ;; with the SAME canonical handlers — the old {:meta :address :count} /
      ;; {:keys} / {:level :keys :addresses} forms are subsets of the canonical maps.
      ;; New writes use the pss/ tags; existing stores read without migration.
      {"proximum.PersistentSortedSet"        root-read
       "proximum.PersistentSortedSet.Leaf"   (get pss-rh pss-fress/leaf-tag)
       "proximum.PersistentSortedSet.Branch" (get pss-rh pss-fress/branch-tag)})

     :write-handlers
     (merge
      {PersistentSortedSet {pss-fress/set-tag (pss-fress/root-write-handler)}}  ; pss/set
      pss-fress/write-handlers)}))                                ; pss/leaf + pss/branch

;;; Factory Functions

(defn create-storage
  "Create CachedStorage backed by a konserve store.

   Args:
     store - Konserve store (filestore, memory, etc.)

   Options:
     :cache-size   - LRU cache size (default 10000)
     :crypto-hash? - Enable merkle tree addressing for integrity (default false)

   When :crypto-hash? is true, PSS node addresses are content-based:
   - Automatic integrity: any corruption breaks the address chain
   - Root address IS the merkle root of the tree
   - Verification is implicit: if restore works, tree is valid"
  ([store] (create-storage store {}))
  ([store {:keys [cache-size crypto-hash?] :or {cache-size 10000 crypto-hash? false}}]
   (let [storage-atom (atom nil)
         handlers (create-fressian-handlers storage-atom)
         config {:crypto-hash? crypto-hash?}
         store (assoc store
                      :serializers {:FressianSerializer
                                    (fressian-serializer
                                     (:read-handlers handlers)
                                     (:write-handlers handlers))})
         storage (->CachedStorage
                  store
                  config
                  (atom (cache/lru-cache-factory {} :threshold cache-size))
                  (atom {:writes 0 :reads 0 :accessed 0})
                  (atom []))]
     (reset! storage-atom storage)
     storage)))

;;; Utilities

(defn storage-stats
  "Get storage statistics.

   Returns map with:
     :writes - Number of writes to storage
     :reads - Number of reads from storage
     :accessed - Number of accessed() calls"
  [^CachedStorage storage]
  @(.-stats storage))

(defn pending-write-count
  "Return number of pending writes waiting to be flushed."
  [^CachedStorage storage]
  (count @(.-pending-writes storage)))

(defn flush-writes!
  "Flush all pending writes to the underlying konserve store.

   This should be called periodically or before closing to persist changes.
   Uses atomic removal pattern to avoid losing writes added concurrently."
  [^CachedStorage storage]
  (let [store (.-store storage)
        pending @(.-pending-writes storage)]
    (when (seq pending)
      (if (k-utils/multi-key-capable? store)
        ;; Use multi-assoc for batch write - much faster with RocksDB WriteBatch
        (k/multi-assoc store (into {} pending) {:sync? true})
        ;; Fall back to individual writes
        (doseq [[address node] pending]
          (k/assoc store address node {:sync? true}))))
    ;; Atomic removal: drop only the items we processed, keep any new ones
    (swap! (.-pending-writes storage) #(vec (drop (count pending) %)))
    nil))

(defn clear-cache!
  "Clear the in-memory LRU cache.

   Nodes will be re-loaded from storage on next access.
   Useful for simulating cold starts or freeing memory."
  [^CachedStorage storage]
  (reset! (.-cache storage) (cache/lru-cache-factory {} :threshold 10000))
  nil)

(defn cache-size
  "Return number of nodes currently in cache."
  [^CachedStorage storage]
  (count @(.-cache storage)))

;; -----------------------------------------------------------------------------
;; Address Map PSS
;;
;; Instead of storing address maps as plain Clojure maps (O(n) write),
;; we use a PSS for structural sharing (O(log n) write).
;; Entries are {:pos <position> :addr <uuid>}

(defn addr-entry-comparator
  "Compare address map entries by position."
  [a b]
  (let [pos-a (if (map? a) (:pos a) a)
        pos-b (if (map? b) (:pos b) b)]
    (compare pos-a pos-b)))

(defn create-address-pss
  "Create a new address-map PSS, optionally with storage backing."
  [storage]
  (if storage
    (pss/sorted-set*
     {:cmp addr-entry-comparator
      :storage storage
      :branching-factor 512
      :ref-type :weak})
    (pss/sorted-set-by addr-entry-comparator)))

(defn address-pss-get
  "Look up a position in the address PSS. Returns the UUID or nil."
  [addr-pss position]
  (when addr-pss
    (when-let [entry (pss/slice addr-pss position position)]
      (when-let [e (first entry)]
        (:addr e)))))

(defn address-pss-assoc
  "Add or update a position -> UUID mapping in the address PSS."
  [addr-pss position uuid]
  (let [entry {:pos position :addr uuid}]
    ;; PSS disj + conj to update (PSS doesn't have native update)
    (-> addr-pss
        (disj {:pos position})  ;; Remove old entry if exists
        (conj entry))))

(defn address-pss-to-map
  "Convert address PSS to a plain map (for backward compatibility or debugging)."
  [addr-pss]
  (when addr-pss
    (into {} (map (fn [e] [(:pos e) (:addr e)])) addr-pss)))

(defn map-to-address-pss
  "Convert a plain map to an address PSS."
  [m storage]
  (reduce
   (fn [addr-pss [pos uuid]]
     (conj addr-pss {:pos pos :addr uuid}))
   (create-address-pss storage)
   m))

(defn store-address-pss!
  "Store address PSS and return root UUID."
  [addr-pss storage]
  (when addr-pss
    (let [root (pss/store addr-pss storage)]
      (flush-writes! storage)
      root)))

(defn restore-address-pss
  "Restore address PSS from root UUID."
  [root storage]
  (when root
    (pss/restore-by addr-entry-comparator root storage)))

;; -----------------------------------------------------------------------------
;; Konserve Store Connection Helpers

(defn- ->uuid
  "Best-effort conversion to UUID.
   Supports UUID values and UUID strings. Returns nil if conversion is not possible."
  [x]
  (cond
    (instance? java.util.UUID x) x
    (string? x) (try
                  (java.util.UUID/fromString x)
                  (catch Exception _ nil))
    :else nil))

(defn normalize-store-config
  "Validate and normalize a Konserve store-config.

  Proximum treats the store-config as an opaque map consumed by Konserve's
  multimethod constructors. The only semantic we rely on is the presence of
  a stable store identity under :id.

  Returns a normalized store-config with :id coerced to UUID and without any
  legacy :opts key."
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

(defn connect-store-sync
  "Connect to a Konserve store using the unified :backend dispatch API.

  Forces opts {:sync? true} regardless of the incoming config.
  Returns a store (not a channel)."
  [store-config]
  (when-not (map? store-config)
    (throw (ex-info ":store-config must be a map" {:store-config store-config})))
  (k/connect-store (dissoc store-config :opts) {:sync? true}))
