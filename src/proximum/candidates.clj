(ns proximum.candidates
  "Paged HNSW candidate scans for secondary-index adapters.

   The default scan runs one approximate HNSW search and freezes that bounded
   candidate set. `:mode :iterative` instead pins an immutable generation and
   retains the native visited/discarded frontier, allowing later pages to
   discover more candidates after primary-store rejection."
  (:require [hasch.core :as hasch]
            [proximum.pgvector :as pg]
            [proximum.protocols :as p])
  (:import [proximum.internal HnswCandidateCursor]))

(defrecord CandidateScan [candidates offset page-size metadata])
(defrecord IterativeCandidateScan [cursor idx page-size page-number metadata])

(declare candidate)

(defn- external-id [idx internal-id]
  (:external-id (p/get-metadata idx internal-id)))

(defn start-candidate-scan
  "Materialize an approximate candidate set and return an immutable cursor.

   Options:
     :candidate-limit maximum candidates captured once (default 100)
     :page-size       candidates returned per page (default 25)
     :ef              optional HNSW beam override (at least candidate-limit);
                      omitted uses the index generation's configured/default
                      beam instead of weakening it to the candidate count
     :expected-index-commit-id fail if the index is not at this durable commit
     :primary-snapshot-id      opaque owner snapshot identity; required when
                               the index itself has no durable commit
     :query-id        stable caller identity; defaults to a content hash
     :mode            :materialized (default) or :iterative
     :strict-order?   iterative pages never regress in distance (default true)
     :max-visited     optional traversal budget; 0/unset means unlimited
     :max-distance-computations optional distance budget
     :timeout-ms      optional cumulative native search-time budget
     :max-frontier-nodes conservative cap on unique level-0 visits and thus
                         retained traversal nodes (default 100000)

   Result distances have both :rank-distance (the existing internal HNSW
   value) and :distance (pgvector-visible value). :distance-exact? means the
   metric is evaluated exactly for each returned candidate; :recall remains
   :approximate because HNSW may not discover every true neighbor."
  ([idx query] (start-candidate-scan idx query {}))
  ([idx query {:keys [candidate-limit page-size ef expected-index-commit-id
                      primary-snapshot-id query-id mode strict-order?
                      max-visited max-distance-computations timeout-ms
                      max-frontier-nodes]
               :or {candidate-limit 100 page-size 25 mode :materialized}}]
   (when-not (pos-int? candidate-limit)
     (throw (ex-info ":candidate-limit must be positive"
                     {:reason :invalid-candidate-limit :value candidate-limit})))
   (when-not (pos-int? page-size)
     (throw (ex-info ":page-size must be positive"
                     {:reason :invalid-page-size :value page-size})))
   (let [config (p/index-config idx)
         metric (:distance config)
         index-commit-id (p/current-commit idx)
         _ (when (and expected-index-commit-id
                      (not= expected-index-commit-id index-commit-id))
             (throw (ex-info "Candidate index is not at the expected durable commit"
                             {:reason :index-commit-mismatch
                              :expected-index-commit-id expected-index-commit-id
                              :actual-index-commit-id index-commit-id})))
         _ (when-not (or index-commit-id primary-snapshot-id)
             (throw (ex-info
                     "A dirty candidate index requires its owning primary snapshot identity"
                     {:reason :missing-primary-snapshot-id
                      :hint "Sync the index or pass :primary-snapshot-id"})))
         query-id (or query-id
                      (hasch/uuid {:index-commit-id index-commit-id
                                   :primary-snapshot-id primary-snapshot-id
                                   :metric metric
                                   :query (vec query)}))
         base-metadata {:metric metric
                        :index-commit-id index-commit-id
                        :primary-snapshot-id primary-snapshot-id
                        :query-id query-id
                        :recall :approximate
                        :complete-recall? false
                        :commit-id (p/current-commit idx)
                        :vector-count (p/vector-count-total idx)
                        :mutation-stable? true
                        :exhaustive? false}]
     (case mode
       :materialized
       (let [search-options (cond-> {}
                              ef (assoc :ef (max candidate-limit ef)))
             results (p/search idx query candidate-limit search-options)
             candidates (->> results
                             (map #(candidate idx metric index-commit-id
                                              primary-snapshot-id %))
                             (sort-by (juxt :rank-distance :internal-id))
                             vec)]
         (->CandidateScan
          candidates 0 page-size
          (assoc base-metadata
                 :candidate-count (count candidates)
                 :candidate-limit candidate-limit
                 :ordering [:rank-distance :internal-id]
                 :materialized? true
                 :scan-scope :fixed-ann-candidate-set)))

       :iterative
       (do
         (when-not (satisfies? p/CandidateSearch idx)
           (throw (ex-info "Index does not support iterative candidate search"
                           {:reason :iterative-candidate-search-unsupported
                            :index-type (p/index-type idx)})))
         (let [cursor (p/start-candidate-search
                       idx query
                       (cond-> {:strict-order? (not= false strict-order?)}
                         ef (assoc :ef ef)
                         max-visited (assoc :max-visited max-visited)
                         max-distance-computations
                         (assoc :max-distance-computations
                                max-distance-computations)
                         timeout-ms (assoc :timeout-ms timeout-ms)
                         max-frontier-nodes
                         (assoc :max-frontier-nodes max-frontier-nodes)))]
           (->IterativeCandidateScan
            cursor idx page-size 0
            (assoc base-metadata
                   :ordering (if (not= false strict-order?)
                               [:rank-distance :internal-id]
                               :approximate)
                   :materialized? false
                   :scan-scope :resumable-hnsw-frontier
                   :strict-order? (not= false strict-order?)))))

       (throw (ex-info ":mode must be :materialized or :iterative"
                       {:reason :invalid-candidate-mode :mode mode}))))))

(defn- candidate
  [idx metric index-commit-id primary-snapshot-id {:keys [id distance]}]
  (let [external-id (external-id idx id)]
    (when (nil? external-id)
      (throw (ex-info "Candidate has no stable external identity"
                      {:reason :missing-external-id
                       :internal-id id
                       :index-commit-id index-commit-id
                       :primary-snapshot-id primary-snapshot-id})))
    {:id external-id
     :internal-id id
     :rank-distance distance
     :distance (pg/internal-distance->pg-distance metric distance)
     :metric metric
     :distance-semantics :pgvector
     :distance-exact? true
     :recall :approximate}))

(defn candidate-page
  "Return the current page and a stable continuation cursor.

   Materialized scans own a frozen candidate vector. Iterative scans own an
   affine continuation over a generation-pinned native cursor. :continuation
   is nil when exhausted."
  [scan]
  (cond
    (instance? CandidateScan scan)
    (let [{:keys [candidates offset page-size metadata]} scan
          end (min (count candidates) (+ offset page-size))
          exhausted? (= end (count candidates))]
      {:candidates (subvec candidates offset end)
       :continuation (when-not exhausted? (assoc scan :offset end))
       :exhausted? exhausted?
       :exhaustion-scope :fixed-ann-candidate-set
       :stop-reason (when exhausted? :fixed-candidate-set)
       :metadata metadata})

    (instance? IterativeCandidateScan scan)
    (let [{:keys [^HnswCandidateCursor cursor idx page-size page-number metadata]} scan
          ^doubles raw (.page cursor (int page-number) (int page-size))
          metric (:metric metadata)
          candidates
          (loop [offset 0 result (transient [])]
            (if (< offset (alength raw))
              (recur (+ offset 2)
                     (conj! result
                            (candidate idx metric
                                       (:index-commit-id metadata)
                                       (:primary-snapshot-id metadata)
                                       {:id (long (aget raw offset))
                                        :distance (aget raw (inc offset))})))
              (persistent! result)))
          ;; Commit native emission only after every external identity and
          ;; distance conversion succeeded. Retrying the same continuation
          ;; after conversion failure receives the staged page byte-for-byte.
          _ (.ack cursor (int page-number))
          exhausted? (.isExhausted cursor)
          stop-reason (some-> (.getStopReason cursor) keyword)
          metadata (assoc metadata
                          :visited-count (.getVisitedCount cursor)
                          :distance-computations (.getDistanceComputations cursor)
                          :emitted-count (.getEmittedCount cursor)
                          :strict-order-drops (.getStrictOrderDrops cursor)
                          :search-nanos (.getSearchNanos cursor))]
      {:candidates candidates
       ;; The native cursor is intentionally shared and mutable, but every
       ;; public continuation token carries a monotone page identity so stale
       ;; or accidentally repeated tokens remain observable to validators.
       :continuation (when-not exhausted? (assoc scan :page-number (inc page-number)))
       :exhausted? exhausted?
       :exhaustion-scope :resumable-hnsw-frontier
       :stop-reason (when exhausted? stop-reason)
       :metadata metadata})

    :else
    (throw (ex-info "candidate-page requires a CandidateScan"
                    {:reason :invalid-candidate-scan :value scan}))))

(defn close-candidate-scan!
  "Cancel an iterative scan and release its generation lease.

   Materialized scans own no live native resources, so closing one is a no-op."
  [scan]
  (when (instance? IterativeCandidateScan scan)
    (.close ^HnswCandidateCursor (:cursor scan)))
  nil)
