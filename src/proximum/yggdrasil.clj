(ns proximum.yggdrasil
  "Yggdrasil adapter for proximum vector indices.

  Uses VALUE SEMANTICS: mutating operations (branch!, checkout, merge!, etc.)
  return new ProximumSystem values. The underlying konserve store is shared
  (mutable), but the index state is immutable (each ProximumSystem holds
  a specific HnswIndex value).

  Snapshot IDs are UUIDs (as strings) from proximum's commit system."
  (:require [proximum.protocols :as p]
            [proximum.writing :as writing]
            [proximum.versioning :as versioning]
            [yggdrasil.protocols :as yp]
            [yggdrasil.types :as t]
            [konserve.core :as k]))

(declare ->ProximumSystem)

(defn- uuid-from-str
  "Parse a string to UUID, or return as-is if already a UUID."
  [s]
  (if (instance? java.util.UUID s)
    s
    (java.util.UUID/fromString (str s))))

(defrecord ProximumSystem [idx mmap-dir system-name]
  yp/SystemIdentity
  (system-id [_] (or system-name "proximum"))
  (system-type [_] :proximum)
  (capabilities [_]
    (t/->Capabilities true true true true false false))

  yp/Snapshotable
  (snapshot-id [_]
    (when-let [c (p/current-commit idx)]
      (str c)))

  (parent-ids [_]
    (when-let [edge-store (p/raw-storage idx)]
      (let [branch (p/current-branch idx)
            snapshot (k/get edge-store branch nil {:sync? true})]
        (if snapshot
          (set (map str (:parents snapshot #{})))
          #{}))))

  (as-of [this snap-id] (yp/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (let [uuid (uuid-from-str snap-id)
          edge-store (p/raw-storage idx)
          loaded-idx (writing/load-commit nil uuid
                                          :store edge-store
                                          :mmap-dir mmap-dir)]
      (->ProximumSystem loaded-idx mmap-dir system-name)))

  (snapshot-meta [this snap-id] (yp/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (when-let [edge-store (p/raw-storage idx)]
      (let [uuid (uuid-from-str snap-id)
            snapshot (k/get edge-store uuid nil {:sync? true})]
        (when snapshot
          {:snapshot-id (str (:commit-id snapshot))
           :parent-ids (set (map str (:parents snapshot #{})))
           :timestamp (:created-at snapshot)
           :message (:message snapshot)
           :branch (when-let [b (:branch snapshot)] (name b))}))))

  yp/Branchable
  (branches [this] (yp/branches this nil))
  (branches [_ _opts]
    (or (versioning/branches idx)
        #{:main}))

  (current-branch [_]
    (p/current-branch idx))

  (branch! [this bname]
    (let [branch-kw (keyword (clojure.core/name bname))
          forked-idx (versioning/branch! idx branch-kw)]
      (p/close! forked-idx)
      this))

  (branch! [this bname from] (yp/branch! this bname from nil))
  (branch! [this bname from _opts]
    (let [branch-kw (keyword (clojure.core/name bname))
          from-uuid (uuid-from-str from)
          edge-store (p/raw-storage idx)
          source-idx (writing/load-commit nil from-uuid
                                          :store edge-store
                                          :mmap-dir mmap-dir)
          forked-idx (versioning/branch! source-idx branch-kw)]
      (p/close! forked-idx)
      (p/close! source-idx)
      this))

  (delete-branch! [this bname] (yp/delete-branch! this bname nil))
  (delete-branch! [this bname _opts]
    (let [branch-kw (keyword (clojure.core/name bname))]
      (versioning/delete-branch! idx branch-kw)
      this))

  (checkout [this bname] (yp/checkout this bname nil))
  (checkout [_ bname _opts]
    (let [branch-kw (keyword (clojure.core/name bname))
          edge-store (p/raw-storage idx)
          loaded-idx (writing/load nil :branch branch-kw
                                   :store edge-store
                                   :mmap-dir mmap-dir)]
      ;; Note: old idx's mmap is NOT closed here because other system values
      ;; may share the same VectorStore due to structural sharing from insert.
      ;; The GC will reclaim unmapped buffers when no references remain.
      (->ProximumSystem loaded-idx mmap-dir system-name)))

  yp/Graphable
  (history [this] (yp/history this {}))
  (history [_ opts]
    (let [commits (versioning/history idx)
          ids (mapv #(str (:commit-id %)) commits)
          ids (if-let [limit (:limit opts)]
                (vec (take limit ids))
                ids)]
      ids))

  (ancestors [this snap-id] (yp/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [uuid (uuid-from-str snap-id)
          ancs (versioning/ancestors idx uuid)]
      ;; versioning/ancestors includes self; exclude it
      (vec (map str (remove #{uuid} ancs)))))

  (ancestor? [this a b] (yp/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [uuid-a (uuid-from-str a)
          uuid-b (uuid-from-str b)]
      (versioning/ancestor? idx uuid-a uuid-b)))

  (common-ancestor [this a b] (yp/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [uuid-a (uuid-from-str a)
          uuid-b (uuid-from-str b)]
      (when-let [ca (versioning/common-ancestor idx uuid-a uuid-b)]
        (str ca))))

  (commit-graph [this] (yp/commit-graph this nil))
  (commit-graph [_ _opts]
    (when-let [graph (versioning/commit-graph idx)]
      {:nodes (into {}
                (map (fn [[id info]]
                       [(str id) {:parent-ids (set (map str (:parents info #{})))
                                  :meta {:timestamp (:created-at info)
                                         :branch (when-let [b (:branch info)] (name b))}}]))
                (:nodes graph))
       :branches (into {}
                   (map (fn [[b commit-id]]
                          [b (str commit-id)]))
                   (:branches graph))
       :roots (set (map str (:roots graph)))}))

  (commit-info [this snap-id] (yp/commit-info this snap-id nil))
  (commit-info [_ snap-id _opts]
    (let [uuid (uuid-from-str snap-id)
          info (versioning/commit-info idx uuid)]
      (when info
        {:parent-ids (set (map str (:parents info #{})))
         :timestamp (:created-at info)
         :message (:message info)
         :branch (when-let [b (:branch info)] (name b))})))

  yp/Mergeable
  (merge! [this source] (yp/merge! this source {}))
  (merge! [_ source _opts]
    (let [source-kw (keyword (clojure.core/name source))
          synced-idx (versioning/merge! idx source-kw {:ids :all})]
      (->ProximumSystem synced-idx mmap-dir system-name)))

  (conflicts [this _a _b] (yp/conflicts this _a _b nil))
  (conflicts [_ _a _b _opts]
    ;; Proximum uses add-only merge, no structural conflicts
    [])

  (diff [this a b] (yp/diff this a b nil))
  (diff [_ a b _opts]
    {:snapshot-a (str a)
     :snapshot-b (str b)}))

(defn create
  "Create a ProximumSystem wrapping an existing proximum index.

  Args:
    idx         - A proximum HnswIndex (with konserve store configured)
    opts        - Options:
                  :mmap-dir    - directory for branch mmap files
                  :system-name - identifier for this system"
  [idx opts]
  (->ProximumSystem idx (:mmap-dir opts) (:system-name opts)))

(defn close!
  "Close the system's index, releasing mmap and file handles."
  [^ProximumSystem sys]
  (p/close! (:idx sys)))
