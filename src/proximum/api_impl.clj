(ns proximum.api-impl
  "Implementation functions for proximum API.

   This namespace provides the public API that works with external IDs.
   Internally it translates to internal IDs used by the protocol layer.

   External IDs:
   - Can be any serializable value (Long, String, UUID, etc.)
   - Stored in metadata under :external-id key
   - Indexed for O(log n) lookup

   Generated API (proximum.core) references these implementations.

   Protocol implementations:
   - NearestNeighborSearch: Search with external ID results"
  (:require [proximum.protocols :as p]
            [proximum.metadata :as meta])
  (:import [proximum.internal ArrayBitSet]
           [java.util UUID]))

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

(defn- ensure-id
  "Ensure we have an ID - generate UUID if nil."
  [id]
  (or id (UUID/randomUUID)))

;; -----------------------------------------------------------------------------
;; Insert Operations

(defn insert
  "Insert a vector with an ID and optional metadata. Returns new index.
   ID can be any value (Long, String, UUID, etc.). Pass nil to auto-generate UUID."
  ([idx vector id]
   (insert idx vector id nil))
  ([idx vector id metadata]
   (let [actual-id (ensure-id id)
         meta-with-id (assoc (or metadata {}) :external-id actual-id)]
     (p/insert idx vector meta-with-id))))

(defn insert-batch
  "Insert multiple vectors with IDs efficiently.
   IDs list must match vectors length. Use nil for auto-generated UUIDs."
  ([idx vectors ids]
   (insert-batch idx vectors ids nil))
  ([idx vectors ids opts]
   (let [actual-ids (mapv ensure-id ids)
         metadata-list (or (:metadata opts) (repeat (count vectors) {}))
         metadata-with-ids (mapv (fn [m id] (assoc (or m {}) :external-id id))
                                 metadata-list
                                 actual-ids)]
     (p/insert-batch idx vectors (assoc opts :metadata metadata-with-ids)))))

;; -----------------------------------------------------------------------------
;; Search Operations

(defn search
  "Search for k nearest neighbors. Returns results with external IDs."
  ([idx query k]
   (search idx query k nil))
  ([idx query k opts]
   (let [results (p/search idx query k (or opts {}))]
     (mapv (fn [{:keys [id distance]}]
             {:id (get-external-id idx id)
              :distance distance})
           results))))

(defn search-filtered
  "Search with filtering. Filter receives external IDs."
  ([idx query k filter-pred]
   (search-filtered idx query k filter-pred nil))
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
           results))))

(defn search-with-metadata
  "Search and include metadata in results. Returns external IDs."
  ([idx query k]
   (search-with-metadata idx query k nil))
  ([idx query k opts]
   (let [results (p/search idx query k (or opts {}))]
     (mapv (fn [{:keys [id distance]}]
             {:id (get-external-id idx id)
              :distance distance
              :metadata (p/get-metadata idx id)})
           results))))

;; -----------------------------------------------------------------------------
;; Delete Operation

(defn delete
  "Soft-delete vector by external ID. Returns new index."
  [idx external-id]
  (if-let [internal-id (lookup-internal-id idx external-id)]
    (p/delete idx internal-id)
    idx))

;; -----------------------------------------------------------------------------
;; Accessor Operations

(defn get-vector
  "Retrieve vector by external ID. Returns nil if not found or deleted."
  [idx external-id]
  (when-let [internal-id (lookup-internal-id idx external-id)]
    (p/get-vector idx internal-id)))

(defn get-metadata
  "Get metadata map for vector by external ID. Returns nil if not found."
  [idx external-id]
  (when-let [internal-id (lookup-internal-id idx external-id)]
    (p/get-metadata idx internal-id)))

(defn with-metadata
  "Associate/update metadata for a vector by external ID. Returns new index."
  [idx external-id meta-map]
  (if-let [internal-id (lookup-internal-id idx external-id)]
    (let [;; Preserve the external-id in the metadata
          meta-with-id (assoc meta-map :external-id external-id)]
      ;; Use protocol method - works polymorphically for any index type
      (p/set-metadata idx internal-id meta-with-id))
    (throw (ex-info "External ID not found" {:external-id external-id}))))

;; -----------------------------------------------------------------------------
;; Utilities (keep for advanced use)

(defn make-id-filter
  "Create a reusable ID filter for search-filtered (advanced use with internal IDs).

   Returns an ArrayBitSet. Note: this uses internal IDs, not external IDs.
   For external ID filtering, pass a set of external IDs to search-filtered instead.

   Args:
     capacity - Maximum ID value + 1 (typically from count-vectors)
     ids      - Collection of internal IDs (integers)"
  [capacity ids]
  (let [^ArrayBitSet bs (ArrayBitSet. (int capacity))]
    (doseq [id ids]
      (when (and (>= id 0) (< id capacity))
        (.add bs (int id))))
    bs))

