(ns proximum.metadata
  "Metadata and external-id management for vector indices.

   Provides PSS-backed storage for:
   - Node metadata: arbitrary maps associated with vector IDs
   - External ID index: string-based external IDs mapped to internal node IDs

   Both use PersistentSortedSet for O(log n) operations with structural sharing."
  (:require [org.replikativ.persistent-sorted-set :as pss]
            [clojure.string :as str]))

;; -----------------------------------------------------------------------------
;; Comparators

(defn metadata-comparator
  "Compare metadata entries by node-id."
  [a b]
  (let [id-a (if (map? a) (:node-id a) a)
        id-b (if (map? b) (:node-id b) b)]
    (Long/compare (long id-a) (long id-b))))

(defn external-id-comparator
  "Compare external-id entries by external-id.
   Supports any comparable types. Mixed types are compared by class name then pr-str."
  [a b]
  (let [id-a (if (map? a) (:external-id a) a)
        id-b (if (map? b) (:external-id b) b)]
    (if (= (type id-a) (type id-b))
      (compare id-a id-b)
      ;; Different types: compare by class name, then string representation
      (let [c (compare (.getName (class id-a)) (.getName (class id-b)))]
        (if (zero? c)
          (compare (pr-str id-a) (pr-str id-b))
          c)))))

;; -----------------------------------------------------------------------------
;; PSS Creation

(defn create-metadata-pss
  "Create a new metadata PSS, optionally with storage backing."
  [store]
  (if store
    (pss/sorted-set* {:cmp metadata-comparator
                      :storage store
                      :branching-factor 512
                      :ref-type :weak})
    (pss/sorted-set-by metadata-comparator)))

(defn create-external-id-pss
  "Create a new external-id index PSS (external-id -> node-id), optionally with storage backing."
  [store]
  (if store
    (pss/sorted-set* {:cmp external-id-comparator
                      :storage store
                      :branching-factor 512
                      :ref-type :weak})
    (pss/sorted-set-by external-id-comparator)))

;; -----------------------------------------------------------------------------
;; Metadata Operations

(defn set-metadata
  "Set metadata for a node in the PSS. Returns new PSS."
  [metadata-pss node-id meta-map]
  (if meta-map
    (let [entry {:node-id node-id :data meta-map}]
      (-> metadata-pss
          (disj {:node-id node-id})
          (conj entry)))
    metadata-pss))

(defn lookup-metadata
  "Look up metadata for a node. Returns nil if not found."
  [metadata-pss node-id]
  (when-let [entries (pss/slice metadata-pss node-id node-id)]
    (when-let [entry (first entries)]
      (:data entry))))

;; -----------------------------------------------------------------------------
;; External ID Operations

(defn external-id-from-meta
  "Extract external id from a metadata map.
   Returns the ID value as-is (preserves type: Long, String, UUID, etc.).
   Returns nil if not present or blank string."
  [meta-map]
  (let [v (when (map? meta-map) (get meta-map :external-id))]
    (when (and v (not (and (string? v) (str/blank? v))))
      v)))

(defn lookup-external-id
  "Lookup internal node id by external id from external-id-index PSS. Returns nil if not found."
  [external-id-pss external-id]
  (when (and external-id-pss external-id)
    ;; Normalize numerical external IDs to Long for consistent lookups
    ;; (Java methods return Integer, Clojure range produces Long)
    (let [normalized-id (if (number? external-id)
                          (long external-id)
                          external-id)]
      (when-let [entries (pss/slice external-id-pss normalized-id normalized-id)]
        (when-let [entry (first entries)]
          (:node-id entry))))))

(defn set-external-id
  "Set external id mapping in the PSS.

  Enforces uniqueness: throws if external-id already exists (and points to a different node)."
  [external-id-pss external-id node-id]
  (if external-id
    (let [existing (lookup-external-id external-id-pss external-id)]
      (when (and existing (not= (long existing) (long node-id)))
        (throw (ex-info "External id already exists" {:external-id external-id
                                                      :existing-node-id (long existing)
                                                      :new-node-id (long node-id)})))
      (-> external-id-pss
          (disj {:external-id external-id})
          (conj {:external-id external-id :node-id node-id})))
    external-id-pss))

(defn remove-external-id
  "Remove an external id mapping from the PSS."
  [external-id-pss external-id]
  (if external-id
    (disj external-id-pss {:external-id external-id})
    external-id-pss))
