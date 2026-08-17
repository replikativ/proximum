(ns proximum.gc
  "Garbage collection for proximum.

   Implements mark-and-sweep garbage collection following the datahike pattern:
   1. Mark phase: Traverse from all branch heads, following parent chains
   2. Collect reachable keys (vector chunks, edge chunks, PSS nodes)
   3. Sweep phase: Delegate to konserve.gc/sweep! to remove unreachable data

   GC preserves:
   - All current branch heads (in :branches set)
   - Commit history back to remove-before date
   - All data referenced by preserved commits

   GC removes:
   - Old commit snapshots (before remove-before date)
   - Orphaned chunks no longer referenced by any commit
   - Unreachable PSS nodes"
  (:require [konserve.core :as k]
            [konserve.gc :as k-gc]
            [konserve.gc-guard :as guard]
            [konserve.protocols :as kp]
            [konserve.utils :as ku]
            [org.replikativ.persistent-sorted-set :as pss]
            [clojure.set :as set]
            [proximum.protocols :as p]
            [proximum.storage :as storage])
  (:import [proximum.storage CachedStorage]))

;; -----------------------------------------------------------------------------
;; Mark Phase Helpers

(defn- mark-pss-addresses
  "Walk PSS tree and collect all node addresses.

   Uses pss/walk-addresses to traverse the B-tree structure.
   Returns set of all addresses (UUIDs) used by the PSS."
  [pss-root storage cmp]
  (if (and pss-root storage)
    (try
      (let [addresses (atom #{})
            ;; Restore PSS from root to enable walking
            pss (pss/restore-by cmp pss-root storage)]
        (pss/walk-addresses pss
                            (fn [addr]
                              (when addr
                                (swap! addresses conj addr))
                              true))  ; Return true to continue walking
        @addresses)
      (catch Exception _
        ;; If PSS can't be restored, just return the root address
        #{pss-root}))
    #{}))

(defn- mark-address-pss
  "Walk address PSS tree and collect both node addresses and chunk keys.

   Returns {:pss-addrs #{...} :chunk-keys #{...}} where:
   - pss-addrs: UUIDs of PSS nodes (for GC preservation)
   - chunk-keys: Storage keys for chunks (e.g., [:vectors :chunk uuid])"
  [pss-root storage chunk-key-fn]
  (if (and pss-root storage)
    (try
      (let [pss-addrs (atom #{})
            chunk-keys (atom #{})
            ;; Restore PSS from root
            pss (pss/restore-by storage/addr-entry-comparator pss-root storage)]
        ;; Walk PSS nodes to collect their addresses
        (pss/walk-addresses pss
                            (fn [addr]
                              (when addr
                                (swap! pss-addrs conj addr))
                              true))
        ;; Walk entries to collect chunk keys
        (doseq [entry pss]
          (when-let [chunk-uuid (:addr entry)]
            (swap! chunk-keys conj (chunk-key-fn chunk-uuid))))
        {:pss-addrs @pss-addrs :chunk-keys @chunk-keys})
      (catch Exception _
        ;; If PSS can't be restored, just return the root address
        {:pss-addrs #{pss-root} :chunk-keys #{}}))
    {:pss-addrs #{} :chunk-keys #{}}))

(defn- mark-snapshot
  "Collect all storage keys referenced by a single snapshot.

   Returns set of keys including:
   - Vector chunk keys (from vectors-addr-pss)
   - Edge chunk keys (from edges-addr-pss)
   - Metadata PSS node addresses
   - External-id PSS node addresses
   - Address PSS node addresses"
  [snapshot storage]
  (let [;; Walk vectors address PSS
        vectors-result (mark-address-pss
                        (:vectors-addr-pss-root snapshot)
                        storage
                        (fn [uuid] [:vectors :chunk uuid]))
        ;; Walk edges address PSS
        ;; Edge storage addresses can be Long (non-crypto) or UUID (crypto)
        edges-result (mark-address-pss
                      (:edges-addr-pss-root snapshot)
                      storage
                      (fn [addr]
                        (if (instance? java.util.UUID addr)
                          [:edges :chunk addr]
                          [:edges :chunk (keyword (str addr))])))
        ;; Walk metadata PSS
        meta-pss-addrs (mark-pss-addresses
                        (:metadata-pss-root snapshot)
                        storage
                        (fn [a b]
                          (let [id-a (if (map? a) (:node-id a) a)
                                id-b (if (map? b) (:node-id b) b)]
                            (Long/compare (long id-a) (long id-b)))))
        ;; Walk external-id PSS
        ext-pss-addrs (mark-pss-addresses
                       (:external-id-pss-root snapshot)
                       storage
                       (fn [a b]
                         (let [id-a (if (map? a) (:external-id a) a)
                               id-b (if (map? b) (:external-id b) b)]
                           (compare (str id-a) (str id-b)))))]
    (set/union
     (:pss-addrs vectors-result)
     (:chunk-keys vectors-result)
     (:pss-addrs edges-result)
     (:chunk-keys edges-result)
     meta-pss-addrs
     ext-pss-addrs)))

;; -----------------------------------------------------------------------------
;; Reachability Analysis

(defn- before?
  "Check if date a is before date b."
  [^java.util.Date a ^java.util.Date b]
  (when (and a b)
    (< (.getTime a) (.getTime b))))

(defn mark-reachable
  "Mark all reachable keys starting from branch heads.

   Algorithm:
   1. Start from each branch in :branches set
   2. For each branch, walk parent chain
   3. Stop when:
      - Hit a branch keyword (cross-reference to another branch)
      - Hit a commit older than remove-before
      - Hit an already-visited commit
   4. Collect all keys from each visited snapshot

   Args:
     store         - Konserve store
     storage       - CachedStorage for PSS (may be nil)
     branches      - Set of branch keywords
     remove-before - Date cutoff (commits before this are candidates for removal)

   Returns:
     Set of all keys/addresses that should be preserved"
  [store storage branches remove-before]
  (reduce
   (fn [whitelist branch]
     (loop [to-check [branch]
            visited #{}
            wl whitelist]
       (if-let [ref (first to-check)]
         (if (visited ref)
            ;; Already visited, skip
           (recur (rest to-check) visited wl)
            ;; Load and process this ref
           (let [snapshot (k/get store ref nil {:sync? true})]
             (if-not snapshot
                ;; Ref not found, skip
               (recur (rest to-check) (conj visited ref) wl)
                ;; Check if this commit is too old
               (let [created (:created-at snapshot)]
                 (if (and (uuid? ref)
                          remove-before
                          (before? created remove-before))
                    ;; Too old, don't traverse further but keep visited
                   (recur (rest to-check) (conj visited ref) wl)
                    ;; Mark this snapshot and traverse parents
                   (let [parents (:parents snapshot #{})
                          ;; Filter out branch keywords from parents - they're tracked separately
                         parent-commits (remove keyword? parents)
                         snapshot-keys (mark-snapshot snapshot storage)
                         new-wl (-> wl
                                    (conj ref)  ; The commit/branch key itself
                                     ;; ...AND the commit it names. A branch head is
                                     ;; stored TWICE — once mutably under `:main`, once
                                     ;; content-addressed under its commit-id — and
                                     ;; only `:main` is `ref` here. Without this the
                                     ;; head commit of every branch was collected on
                                     ;; each GC, leaving `history` and `parents` to
                                     ;; dangle the moment they walked back into it.
                                     ;; Ancestors escaped only because they are
                                     ;; reached as `parent-commits` and become `ref`
                                     ;; in their own turn.
                                     ;;
                                     ;; Invisible until now: the sweep cutoff used to
                                     ;; be `remove-before`, epoch by default, so
                                     ;; nothing was ever deleted and the mark was
                                     ;; never actually tested.
                                    (conj (:commit-id snapshot))
                                    (into snapshot-keys))]
                     (recur (concat (rest to-check) parent-commits)
                            (conj visited ref)
                            new-wl)))))))
          ;; Done with this branch
         wl)))
    ;; Initial whitelist includes :branches and :index/config (global immutable)
   #{:branches :index/config}
   branches))

;; -----------------------------------------------------------------------------
;; GC Entry Point

(defn gc!
  "Garbage collect unreachable data from storage. Returns the set of deleted keys.

   Mark from every branch head, then sweep whatever the mark did not reach.

   TWO DATES, NOT ONE, and they want opposite values — conflating them was the
   bug here:

     `remove-before` is a RETENTION POLICY. It bounds the mark: history older
     than it stops being reachable and so becomes collectable. Callers want it
     in the past (\"keep thirty days\").

     the sweep CUTOFF is a SAFETY FLOOR. Nothing younger may be collected,
     because it may be a values-then-pointer sequence in flight. It wants to be
     NOW, pulled back only by `konserve.gc-guard`.

   Using `remove-before` for both meant recent garbage could never be reclaimed
   — it was always younger than the cutoff — and that pushing the date forward
   to reclaim it simultaneously dropped recent history from the mark, deleting
   live data. datahike separates them for the same reason (`datahike.gc`).

   THE GUARD IS READ BEFORE THE MARK. `konserve.gc/sweep!` cannot do this for
   us: it receives a finished whitelist, so any reading it did would come after
   the mark, and a sequence landing its pointer in between would be invisible to
   both. See `konserve.gc/sweep!`.

   Args:
     idx           - HnswIndex with storage
     remove-before - Date before which history stops being reachable
                     (default: epoch, i.e. keep all history)

   Options:
     :batch-size - Deletion batch size (default 1000)"
  ([idx] (gc! idx (java.util.Date. 0)))
  ([idx remove-before] (gc! idx remove-before {}))
  ([idx remove-before {:keys [batch-size] :or {batch-size 1000}}]
   (when-not (p/raw-storage idx)
     (throw (ex-info "Cannot GC in-memory index. Create the index with durable :store-config."
                     {:hint "Use (create-index {:type :hnsw :dim dim :store-config {...} :mmap-dir \"/path\"})"})))

   (let [edge-store (p/raw-storage idx)
         storage (p/storage idx)
         store-id (kp/store-id edge-store)
         ;; Guard first, mark second — see above.
         cutoff (if store-id
                  (guard/cutoff store-id (ku/now))
                  (ku/now))
         branches (or (k/get edge-store :branches nil {:sync? true}) #{})
         whitelist (mark-reachable edge-store storage branches remove-before)]
     (k-gc/sweep! edge-store whitelist cutoff batch-size {:sync? true}))))

;; Note: history function moved to proximum.versioning
