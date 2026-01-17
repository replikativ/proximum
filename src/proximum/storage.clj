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
            [me.tonsky.persistent-sorted-set :as pss])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           [me.tonsky.persistent_sorted_set IStorage Leaf Branch ANode Settings PersistentSortedSet]
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

   The storage-atom is used for circular reference during deserialization -
   the PersistentSortedSet needs a reference to its storage to lazy-load nodes."
  [storage-atom]
  (let [settings (map->settings {:branching-factor branching-factor})]
    {:read-handlers
     {"proximum.PersistentSortedSet"
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (let [{:keys [meta address count]} (.readObject reader)]
            (PersistentSortedSet. meta nil address @storage-atom nil count settings 0))))
      "proximum.PersistentSortedSet.Leaf"
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (let [{:keys [keys]} (.readObject reader)]
            (Leaf. ^List keys settings))))
      "proximum.PersistentSortedSet.Branch"
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (let [{:keys [keys level addresses]} (.readObject reader)]
            (Branch. (int level) ^List keys ^List (seq addresses) settings))))}

     :write-handlers
     {PersistentSortedSet
      {"proximum.PersistentSortedSet"
       (reify WriteHandler
         (write [_ writer pset]
           (when (nil? (.-_address ^PersistentSortedSet pset))
             (throw (ex-info "Must flush before serialization" {:type :must-be-flushed})))
           (.writeTag writer "proximum.PersistentSortedSet" 1)
           (.writeObject writer {:meta (meta pset)
                                 :address (.-_address ^PersistentSortedSet pset)
                                 :count (count pset)})))}
      Leaf
      {"proximum.PersistentSortedSet.Leaf"
       (reify WriteHandler
         (write [_ writer leaf]
           (.writeTag writer "proximum.PersistentSortedSet.Leaf" 1)
           (.writeObject writer {:keys (.keys ^Leaf leaf)})))}
      Branch
      {"proximum.PersistentSortedSet.Branch"
       (reify WriteHandler
         (write [_ writer node]
           (.writeTag writer "proximum.PersistentSortedSet.Branch" 1)
           (.writeObject writer {:level (.level ^Branch node)
                                 :keys (.keys ^Branch node)
                                 :addresses (.addresses ^Branch node)})))}}}))

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
