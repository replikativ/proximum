(ns proximum.metrics
  "Index health metrics for monitoring and compaction decisions.

   Open source API - returns point-in-time metrics as a map.
   Commercial extensions can add Prometheus export, historical trending, etc."
  (:require [proximum.vectors :as vectors]
            [proximum.protocols :as p])
  (:import [proximum.internal PersistentEdgeIndex]))

;; Default compaction threshold (10% deleted)
(def ^:dynamic *compaction-threshold* 0.10)

(defn index-metrics
  "Get current index health metrics.

   Returns a map with:
     :vector-count      - Total vectors in storage (includes deleted)
     :deleted-count     - Number of deleted vectors
     :live-count        - Vectors actually searchable (vector-count - deleted-count)
     :deletion-ratio    - Fraction deleted (deleted-count / vector-count)
     :needs-compaction? - True if deletion-ratio exceeds threshold
     :capacity          - Maximum index capacity
     :utilization       - Fraction of capacity used (vector-count / capacity)
     :edge-count        - Total edges in HNSW graph
     :avg-edges-per-node - Average connectivity (edge-count / live-count)
     :branch            - Current branch name
     :commit-id         - Last commit UUID (nil if never synced)
     :cache-hits        - Chunk cache hits (if storage-backed)
     :cache-misses      - Chunk cache misses (if storage-backed)

   Options:
     :compaction-threshold - Override default threshold (default 0.10)"
  ([idx]
   (index-metrics idx {}))
  ([idx {:keys [compaction-threshold]
         :or {compaction-threshold *compaction-threshold*}}]
   (let [vector-count (p/vector-count-total idx)
         deleted-count (p/deleted-count-total idx)
         live-count (- vector-count deleted-count)
         deletion-ratio (if (pos? vector-count)
                          (double (/ deleted-count vector-count))
                          0.0)
         edge-count (p/edge-count idx)
         capacity (p/capacity idx)
         ^PersistentEdgeIndex pes (p/edge-storage idx)]
     {:vector-count vector-count
      :deleted-count deleted-count
      :live-count live-count
      :deletion-ratio deletion-ratio
      :needs-compaction? (> deletion-ratio compaction-threshold)
      :capacity capacity
      :utilization (if (pos? capacity)
                     (double (/ vector-count capacity))
                     0.0)
      :edge-count edge-count
      :avg-edges-per-node (if (pos? live-count)
                            (double (/ edge-count live-count))
                            0.0)
      :branch (p/current-branch idx)
      :commit-id (p/current-commit idx)
      :cache-hits (.getCacheHits pes)
      :cache-misses (.getCacheMisses pes)})))

(defn deletion-ratio
  "Get just the deletion ratio (convenience function for threshold checks)."
  [idx]
  (let [vector-count (p/vector-count-total idx)
        deleted-count (p/deleted-count-total idx)]
    (if (pos? vector-count)
      (double (/ deleted-count vector-count))
      0.0)))

(defn needs-compaction?
  "Check if index needs compaction based on deletion ratio.

   Uses *compaction-threshold* dynamic var (default 0.10 = 10%)."
  ([idx]
   (needs-compaction? idx *compaction-threshold*))
  ([idx threshold]
   (> (deletion-ratio idx) threshold)))

(defn cache-hit-ratio
  "Get cache hit ratio for storage-backed indices.
   Returns nil for in-memory indices with no cache activity."
  [idx]
  (let [^PersistentEdgeIndex pes (p/edge-storage idx)
        hits (.getCacheHits pes)
        misses (.getCacheMisses pes)
        total (+ hits misses)]
    (when (pos? total)
      (double (/ hits total)))))

(defn graph-health
  "Get HNSW graph health indicators.

   Returns:
     :avg-connectivity - Average edges per node (should be ~M for healthy graph)
     :entrypoint       - Current graph entrypoint node ID
     :max-level        - Current maximum level in hierarchy
     :expected-M       - Configured M value for comparison"
  [idx]
  (let [vector-count (p/vector-count-total idx)
        deleted-count (p/deleted-count-total idx)
        live-count (- vector-count deleted-count)
        edge-count (p/edge-count idx)
        M (p/expected-connectivity idx)]
    {:avg-connectivity (if (pos? live-count)
                         (double (/ edge-count live-count))
                         0.0)
     :entrypoint (p/graph-entrypoint idx)
     :max-level (p/graph-max-level idx)
     :expected-M M
     :connectivity-ratio (if (pos? M)
                           (/ (if (pos? live-count)
                                (double (/ edge-count live-count))
                                0.0)
                              M)
                           0.0)}))

(defn reachability
  "Walk the graph and report which live vectors a search can reach.

   A search starts at the entry point and follows edges, so a live vector no path
   leads to is never returned, though it is stored and counted. This walks layer 0
   from the entry point the way a search does - a deleted node has no edges and
   is not a stepping stone - and compares what it reaches with what is live.

   Returns:
     :live-count        - vectors that are stored and not deleted
     :reachable-count   - of those, how many a search can reach
     :unreachable-count - the rest; 0 for a healthy index
     :unreachable       - their external ids, at most `:max-ids` of them
     :fully-reachable?  - true when nothing is unreachable
     :stale-edge-count  - edges on any layer that still point at a deleted
                          vector; each one takes a neighbor slot a live vector
                          could use, so recall falls as they accumulate

   Unreachable vectors and stale edges both come from deletes. `compact`
   rebuilds the graph from the live vectors and removes both; nothing else
   reconnects an unreachable vector.

   Cost: linear in the number of edges; it reads the whole graph. Meant for a
   health check or an operator, not for a request path.

   Options:
     :max-ids - how many unreachable external ids to list (default 100)"
  ([idx] (reachability idx {}))
  ([idx {:keys [max-ids] :or {max-ids 100}}]
   (let [^PersistentEdgeIndex pes (p/edge-storage idx)
         n (int (p/vector-count-total idx))
         entry (int (.getEntrypoint pes))
         max-level (int (.getCurrentMaxLevel pes))
         seen (java.util.BitSet. (max 1 n))
         queue (java.util.ArrayDeque.)
         live? (fn [id] (and (< -1 (int id) n) (not (.isDeleted pes (int id)))))]
     (when (live? entry)
       (.set seen entry)
       (.add queue entry))
     (loop []
       (when-some [current (.poll queue)]
         (when-some [neighbors (.getNeighbors pes 0 (int current))]
           (doseq [nb neighbors]
             (when (and (live? nb) (not (.get seen (int nb))))
               (.set seen (int nb))
               (.add queue nb))))
         (recur)))
     (let [live (into [] (filter live?) (range n))
           unreachable (into [] (remove #(.get seen (int %))) live)
           stale (reduce
                  (fn [acc id]
                    (loop [level 0 acc acc]
                      (if (> level max-level)
                        acc
                        (recur (inc level)
                               (if-some [neighbors (.getNeighbors pes (int level) (int id))]
                                 (+ acc (count (filter #(.isDeleted pes (int %)) neighbors)))
                                 acc)))))
                  0 live)]
       {:live-count (count live)
        :reachable-count (- (count live) (count unreachable))
        :unreachable-count (count unreachable)
        :unreachable (into [] (comp (take max-ids)
                                    (map #(:external-id (p/get-metadata idx %))))
                           unreachable)
        :fully-reachable? (empty? unreachable)
        :stale-edge-count stale}))))
