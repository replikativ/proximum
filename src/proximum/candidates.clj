(ns proximum.candidates
  "Materialized, resumable HNSW candidate scans for secondary-index adapters.

   A scan runs one approximate HNSW search and freezes that bounded candidate
   set. Paging is stable even if the live index is subsequently mutated. It is
   not an unbounded cursor over the whole index and cannot improve recall after
   creation; callers must choose a sufficient :candidate-limit and recheck any
   primary-store predicate themselves."
  (:require [hasch.core :as hasch]
            [proximum.pgvector :as pg]
            [proximum.protocols :as p]))

(defrecord CandidateScan [candidates offset page-size metadata])

(defn- external-id [idx internal-id]
  (:external-id (p/get-metadata idx internal-id)))

(defn start-candidate-scan
  "Materialize an approximate candidate set and return an immutable cursor.

   Options:
     :candidate-limit maximum candidates captured once (default 100)
     :page-size       candidates returned per page (default 25)
     :ef              HNSW beam width (at least candidate-limit)
     :expected-index-commit-id fail if the index is not at this durable commit
     :primary-snapshot-id      opaque owner snapshot identity; required when
                               the index itself has no durable commit
     :query-id        stable caller identity; defaults to a content hash

   Result distances have both :rank-distance (the existing internal HNSW
   value) and :distance (pgvector-visible value). :distance-exact? means the
   metric is evaluated exactly for each returned candidate; :recall remains
   :approximate because HNSW may not discover every true neighbor."
  ([idx query] (start-candidate-scan idx query {}))
  ([idx query {:keys [candidate-limit page-size ef expected-index-commit-id
                      primary-snapshot-id query-id]
               :or {candidate-limit 100 page-size 25}}]
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
         results (p/search idx query candidate-limit
                           {:ef (max candidate-limit (or ef candidate-limit))})
         candidates (->> results
                         (map (fn [{:keys [id distance]}]
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
                                   :recall :approximate})))
                         ;; HNSW establishes distance order but does not promise a
                         ;; tie-break. Internal IDs are stable inside a generation.
                         (sort-by (juxt :rank-distance :internal-id))
                         vec)]
     (->CandidateScan
      candidates 0 page-size
      {:candidate-limit candidate-limit
       :candidate-count (count candidates)
       :metric metric
       :index-commit-id index-commit-id
       :primary-snapshot-id primary-snapshot-id
       :query-id query-id
       :ordering [:rank-distance :internal-id]
       :recall :approximate
       :materialized? true
       :scan-scope :fixed-ann-candidate-set
       :complete-recall? false
       :commit-id (p/current-commit idx)
       :vector-count (p/vector-count-total idx)
       :mutation-stable? true
       :exhaustive? false}))))

(defn candidate-page
  "Return the current page and a stable continuation cursor.

   The cursor owns the materialized candidate vector; it does not read `idx`
   again. :continuation is nil when exhausted."
  [^CandidateScan scan]
  (when-not (instance? CandidateScan scan)
    (throw (ex-info "candidate-page requires a CandidateScan"
                    {:reason :invalid-candidate-scan :value scan})))
  (let [{:keys [candidates offset page-size metadata]} scan
        end (min (count candidates) (+ offset page-size))
        exhausted? (= end (count candidates))]
    {:candidates (subvec candidates offset end)
     :continuation (when-not exhausted? (assoc scan :offset end))
     ;; Exhaustion applies only to the frozen ANN candidate set. It never
     ;; claims that every matching vector in the index has been visited.
     :exhausted? exhausted?
     :exhaustion-scope :fixed-ann-candidate-set
     :metadata metadata}))
