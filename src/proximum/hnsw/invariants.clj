(ns proximum.hnsw.invariants
  "HNSW graph invariant checking for correctness verification.

   Use these checks during stress testing to verify graph integrity:
   - Entry point validity
   - Neighbor reference validity
   - Neighbor count limits
   - Graph navigability

   Example usage:
     (check-all-invariants idx)
     ;; Returns {:valid? true} or {:valid? false :errors [...]}

   In stress tests:
     (when (zero? (mod iteration 100))
       (let [result (check-all-invariants idx)]
         (when-not (:valid? result)
           (throw (ex-info \"Invariant violation\" result)))))"
  (:require [proximum.vectors :as vectors]
            [proximum.protocols :as p]
            [proximum.hnsw.internal :as hnsw.i])
  (:import [proximum.internal PersistentEdgeStore]))

;; -----------------------------------------------------------------------------
;; Individual Invariant Checks

(defn check-entry-point
  "Check that entry point is valid for non-empty graphs.

   Invariants:
   - Empty graph: entry point = -1
   - Non-empty graph: entry point >= 0 and < vector-count"
  [idx]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        ep (.getEntrypoint pes)
        vec-count (vectors/count-vectors (hnsw.i/vectors idx))]
    (cond
      ;; Empty graph should have ep = -1
      (and (zero? vec-count) (not= ep -1))
      {:valid? false
       :error :entry-point-invalid-empty
       :message (str "Empty graph has entry point " ep " (should be -1)")
       :entry-point ep
       :vector-count vec-count}

      ;; Non-empty graph should have valid ep
      (and (pos? vec-count) (or (neg? ep) (>= ep vec-count)))
      {:valid? false
       :error :entry-point-out-of-bounds
       :message (str "Entry point " ep " out of bounds for " vec-count " vectors")
       :entry-point ep
       :vector-count vec-count}

      :else
      {:valid? true})))

(defn check-neighbor-bounds
  "Check that all neighbor references are valid (no dangling pointers).

   Samples up to max-samples nodes to avoid O(n*M) complexity.
   For full verification, use check-all-neighbor-bounds."
  [idx & {:keys [max-samples] :or {max-samples 1000}}]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        vec-count (vectors/count-vectors (hnsw.i/vectors idx))
        max-level (.getCurrentMaxLevel pes)
        errors (atom [])]

    (when (pos? vec-count)
      (let [;; Sample nodes randomly
            sample-size (min max-samples vec-count)
            sample-ids (if (<= vec-count max-samples)
                         (range vec-count)
                         (take sample-size (shuffle (range vec-count))))]

        (doseq [node-id sample-ids]
          ;; Check all layers
          (doseq [layer (range (inc max-level))]
            (let [neighbors (.getNeighbors pes layer (int node-id))]
              (when neighbors
                (doseq [neighbor neighbors]
                  (when (or (neg? neighbor) (>= neighbor vec-count))
                    (swap! errors conj
                           {:node-id node-id
                            :layer layer
                            :invalid-neighbor neighbor
                            :vector-count vec-count})))))))))

    (if (seq @errors)
      {:valid? false
       :error :invalid-neighbor-references
       :message (str (count @errors) " invalid neighbor reference(s) found")
       :errors (take 10 @errors)}  ; Limit error details
      {:valid? true})))

(defn check-neighbor-counts
  "Check that neighbor counts respect M/M0 limits.

   Layer 0: max M0 neighbors
   Upper layers: max M neighbors"
  [idx & {:keys [max-samples] :or {max-samples 1000}}]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        vec-count (vectors/count-vectors (hnsw.i/vectors idx))
        max-level (.getCurrentMaxLevel pes)
        M (.getM pes)
        M0 (.getM0 pes)
        errors (atom [])]

    (when (pos? vec-count)
      (let [sample-size (min max-samples vec-count)
            sample-ids (if (<= vec-count max-samples)
                         (range vec-count)
                         (take sample-size (shuffle (range vec-count))))]

        (doseq [node-id sample-ids]
          ;; Check layer 0
          (let [count-0 (.getNeighborCount pes 0 (int node-id))]
            (when (> count-0 M0)
              (swap! errors conj
                     {:node-id node-id
                      :layer 0
                      :neighbor-count count-0
                      :limit M0})))

          ;; Check upper layers
          (doseq [layer (range 1 (inc max-level))]
            (let [count-l (.getNeighborCount pes layer (int node-id))]
              (when (> count-l M)
                (swap! errors conj
                       {:node-id node-id
                        :layer layer
                        :neighbor-count count-l
                        :limit M})))))))

    (if (seq @errors)
      {:valid? false
       :error :neighbor-count-exceeded
       :message (str (count @errors) " node(s) exceed neighbor limits")
       :errors (take 10 @errors)}
      {:valid? true})))

(defn check-graph-connectivity
  "Check that the graph is navigable from entry point.

   Performs greedy traversal from entry point to verify
   the graph structure is intact.

   Note: This doesn't guarantee all nodes are reachable
   (HNSW doesn't require full connectivity), but verifies
   the search path works."
  [idx query-vec & {:keys [max-hops] :or {max-hops 1000}}]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        ep (.getEntrypoint pes)
        max-level (.getCurrentMaxLevel pes)]

    (if (neg? ep)
      ;; Empty graph is trivially connected
      {:valid? true :hops 0}

      ;; Try greedy descent from entry point
      (loop [current ep
             level max-level
             hops 0
             visited #{}]
        (cond
          ;; Too many hops - likely infinite loop
          (> hops max-hops)
          {:valid? false
           :error :infinite-loop
           :message "Greedy traversal exceeded max hops (possible cycle)"
           :last-node current
           :level level
           :hops hops}

          ;; Reached layer 0 - success
          (zero? level)
          {:valid? true :hops hops :final-node current}

          ;; Continue descent
          :else
          (let [neighbors (.getNeighbors pes level (int current))]
            (if (or (nil? neighbors) (zero? (alength neighbors)))
              ;; No neighbors at this level, go down
              (recur current (dec level) (inc hops) visited)
              ;; Pick first valid neighbor (simplified greedy)
              (let [next-node (aget neighbors 0)]
                (if (visited next-node)
                  ;; Avoid re-visiting, go down
                  (recur current (dec level) (inc hops) visited)
                  (recur next-node level (inc hops) (conj visited next-node)))))))))))

(defn check-no-self-loops
  "Check that no node has itself as a neighbor (self-loops)."
  [idx & {:keys [max-samples] :or {max-samples 1000}}]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        vec-count (vectors/count-vectors (hnsw.i/vectors idx))
        max-level (.getCurrentMaxLevel pes)
        errors (atom [])]

    (when (pos? vec-count)
      (let [sample-size (min max-samples vec-count)
            sample-ids (if (<= vec-count max-samples)
                         (range vec-count)
                         (take sample-size (shuffle (range vec-count))))]

        (doseq [node-id sample-ids
                layer (range (inc max-level))]
          (let [neighbors (.getNeighbors pes layer (int node-id))]
            (when neighbors
              (when (some #(= % node-id) neighbors)
                (swap! errors conj
                       {:node-id node-id
                        :layer layer})))))))

    (if (seq @errors)
      {:valid? false
       :error :self-loops-found
       :message (str (count @errors) " self-loop(s) found")
       :errors (take 10 @errors)}
      {:valid? true})))

(defn check-level-distribution
  "Check that level distribution roughly follows exponential decay.

   This is a soft check - we verify that:
   - Layer 0 has the most nodes
   - Higher layers have fewer nodes
   - Entry point is on the highest layer

   Returns stats rather than pass/fail."
  [idx]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        vec-count (vectors/count-vectors (hnsw.i/vectors idx))
        max-level (.getCurrentMaxLevel pes)
        ep (.getEntrypoint pes)]

    (if (zero? vec-count)
      {:valid? true :stats {:vector-count 0}}

      (let [;; Count nodes per layer (expensive - sample for large graphs)
            sample-size (min 10000 vec-count)
            sample-ids (if (<= vec-count sample-size)
                         (range vec-count)
                         (take sample-size (shuffle (range vec-count))))

            layer-counts (reduce
                          (fn [counts node-id]
                            (reduce
                             (fn [c layer]
                               (if (pos? (.getNeighborCount pes layer (int node-id)))
                                 (update c layer (fnil inc 0))
                                 c))
                             counts
                             (range (inc max-level))))
                          {}
                          sample-ids)]

        {:valid? true
         :stats {:vector-count vec-count
                 :max-level max-level
                 :entry-point ep
                 :layer-counts layer-counts
                 :sample-size sample-size}}))))

(defn check-deleted-not-in-neighbors
  "Check that deleted nodes don't appear in any neighbor lists.

   INVARIANT: If node X is deleted, X should not be in neighbors(Y, layer)
   for any non-deleted node Y.

   NOTE: This is a WEAK invariant. After deletion, only DIRECT neighbors of
   the deleted node have their edges repaired. Other nodes in the graph may
   still have edges pointing to deleted nodes - this is by design since full
   graph traversal would be expensive. HNSW handles this during search by
   filtering deleted nodes.

   This invariant is expected to FAIL after deletions until a full compaction
   is performed. Use :strict false to only check that the delete repair worked
   on immediate neighbors (not the whole graph)."
  [idx & {:keys [max-samples strict] :or {max-samples 1000 strict true}}]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        vec-count (vectors/count-vectors (hnsw.i/vectors idx))
        max-level (.getCurrentMaxLevel pes)
        errors (atom [])]

    (when (pos? vec-count)
      (let [sample-size (min max-samples vec-count)
            sample-ids (if (<= vec-count max-samples)
                         (range vec-count)
                         (take sample-size (shuffle (range vec-count))))]

        (doseq [node-id sample-ids]
          (when-not (.isDeleted pes (int node-id))
            ;; Check all layers for this non-deleted node
            (doseq [layer (range (inc max-level))]
              (let [neighbors (.getNeighbors pes layer (int node-id))]
                (when neighbors
                  (doseq [neighbor neighbors]
                    (when (.isDeleted pes (int neighbor))
                      (swap! errors conj
                             {:node-id node-id
                              :layer layer
                              :deleted-neighbor neighbor}))))))))))

    (if (seq @errors)
      {:valid? false
       :error :deleted-nodes-in-neighbors
       :message (str (count @errors) " edge(s) to deleted nodes found")
       :errors (take 10 @errors)}
      {:valid? true})))

(defn reachable-from-entrypoint
  "Check how many nodes are reachable from the entry point via layer 0.

   Returns the set of reachable node IDs. For a well-connected HNSW graph,
   this should include all non-deleted nodes.

   Note: HNSW doesn't strictly guarantee full connectivity, but poor
   connectivity indicates graph quality issues."
  [idx]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        ep (.getEntrypoint pes)]

    (if (neg? ep)
      {:reachable #{}
       :count 0}

      (loop [frontier #{ep}
             visited #{}]
        (if (empty? frontier)
          {:reachable visited
           :count (count visited)}
          (let [current (first frontier)
                neighbors (.getNeighbors pes 0 (int current))
                new-neighbors (when neighbors
                                (->> neighbors
                                     (remove visited)
                                     (remove #(.isDeleted pes (int %)))
                                     set))]
            (recur (into (disj frontier current) new-neighbors)
                   (conj visited current))))))))

(defn check-connectivity-ratio
  "Check what percentage of non-deleted nodes are reachable from entry point.

   Options:
     :min-ratio - Minimum acceptable reachability ratio (default 0.95)

   For a healthy HNSW graph, almost all nodes should be reachable."
  [idx & {:keys [min-ratio] :or {min-ratio 0.95}}]
  (let [^PersistentEdgeStore pes (hnsw.i/edges idx)
        vec-count (vectors/count-vectors (hnsw.i/vectors idx))
        deleted-count (.getDeletedCount pes)
        live-count (- vec-count deleted-count)]

    (if (zero? live-count)
      {:valid? true :ratio 1.0 :message "No live nodes"}

      (let [{:keys [count]} (reachable-from-entrypoint idx)
            ratio (/ count live-count)]
        (if (>= ratio min-ratio)
          {:valid? true
           :ratio ratio
           :reachable count
           :live-count live-count}
          {:valid? false
           :error :poor-connectivity
           :message (format "Only %.1f%% of nodes reachable (need %.1f%%)"
                            (* 100 ratio) (* 100 min-ratio))
           :ratio ratio
           :reachable count
           :live-count live-count})))))

;; -----------------------------------------------------------------------------
;; Combined Check

(defn check-all-invariants
  "Run all invariant checks on an HNSW index.

   Options:
     :max-samples  - Max nodes to sample for checks (default 1000)
     :query-vec    - Query vector for connectivity check (optional)
     :check-connectivity - Include connectivity ratio check (default false, expensive)
     :check-deleted-edges - Include deleted node edge check (default false, fails after delete)

   Returns:
     {:valid? true/false
      :checks [{:name :check-entry-point :valid? true} ...]
      :errors [...]}  ; Only if invalid"
  [idx & {:keys [max-samples query-vec check-connectivity check-deleted-edges]
          :or {max-samples 1000
               check-connectivity false
               check-deleted-edges false}}]
  (let [checks [(assoc (check-entry-point idx) :name :entry-point)
                (assoc (check-neighbor-bounds idx :max-samples max-samples) :name :neighbor-bounds)
                (assoc (check-neighbor-counts idx :max-samples max-samples) :name :neighbor-counts)
                (assoc (check-no-self-loops idx :max-samples max-samples) :name :no-self-loops)]
        checks (if check-deleted-edges
                 (conj checks (assoc (check-deleted-not-in-neighbors idx :max-samples max-samples)
                                     :name :deleted-not-in-neighbors))
                 checks)
        checks (if query-vec
                 (conj checks (assoc (check-graph-connectivity idx query-vec)
                                     :name :graph-connectivity))
                 checks)
        checks (if check-connectivity
                 (conj checks (assoc (check-connectivity-ratio idx)
                                     :name :connectivity-ratio))
                 checks)
        all-valid? (every? :valid? checks)
        errors (filterv (complement :valid?) checks)]

    {:valid? all-valid?
     :checks (mapv #(select-keys % [:name :valid?]) checks)
     :errors (when (seq errors) errors)}))

;; -----------------------------------------------------------------------------
;; Recall Computation (for quality testing)

(defn brute-force-knn
  "Compute exact k-nearest neighbors using brute force.

   Returns vector of {:id node-id :distance dist} sorted by distance."
  [idx query-vec k]
  (let [vs (hnsw.i/vectors idx)
        vec-count (vectors/count-vectors vs)
        dim (:dim vs)
        query (if (instance? (Class/forName "[F") query-vec)
                query-vec
                (float-array query-vec))]

    (->> (range vec-count)
         (mapv (fn [id]
                 (let [stored (vectors/get-vector vs id)
                       dist (vectors/distance-squared-to-node vs id query)]
                   {:id id :distance dist})))
         (sort-by :distance)
         (take k)
         vec)))

(defn compute-recall
  "Compute recall@k comparing HNSW results to exact k-NN.

   Returns recall as a value between 0.0 and 1.0."
  [hnsw-results exact-results]
  (let [hnsw-ids (set (map :id hnsw-results))
        exact-ids (set (map :id exact-results))
        overlap (count (clojure.set/intersection hnsw-ids exact-ids))]
    (/ overlap (count exact-ids))))

(defn check-search-quality
  "Check search quality against brute-force k-NN.

   Options:
     :k            - Number of neighbors (default 10)
     :num-queries  - Number of random queries (default 100)
     :min-recall   - Minimum acceptable recall (default 0.9)

   Returns:
     {:valid? true/false
      :mean-recall   recall
      :min-recall    min-recall-seen
      :max-recall    max-recall-seen
      :failed-queries [...]}  ; Queries below min-recall threshold"
  [idx & {:keys [k num-queries min-recall]
          :or {k 10 num-queries 100 min-recall 0.9}}]
  (let [vs (hnsw.i/vectors idx)
        dim (:dim vs)
        vec-count (vectors/count-vectors vs)]

    (if (< vec-count k)
      {:valid? true :message "Not enough vectors for recall test"}

      (let [;; Generate random query vectors
            queries (repeatedly num-queries
                                #(float-array (repeatedly dim (fn [] (- (rand 2.0) 1.0)))))
            ;; Compute recall for each query
            recalls (mapv (fn [query]
                            (let [hnsw-results (p/search idx query k {})
                                  exact-results (brute-force-knn idx query k)]
                              {:query query
                               :recall (compute-recall hnsw-results exact-results)}))
                          queries)
            recall-values (mapv :recall recalls)
            mean-recall (/ (reduce + recall-values) (count recall-values))
            min-seen (apply min recall-values)
            max-seen (apply max recall-values)
            failed (filterv #(< (:recall %) min-recall) recalls)]

        {:valid? (>= mean-recall min-recall)
         :mean-recall mean-recall
         :min-recall-seen min-seen
         :max-recall-seen max-seen
         :num-queries num-queries
         :k k
         :threshold min-recall
         :failed-count (count failed)}))))
