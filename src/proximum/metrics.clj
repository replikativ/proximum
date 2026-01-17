(ns proximum.metrics
  "Index health metrics for monitoring and compaction decisions.

   Open source API - returns point-in-time metrics as a map.
   Commercial extensions can add Prometheus export, historical trending, etc."
  (:require [proximum.vectors :as vectors]
            [proximum.protocols :as p])
  (:import [proximum.internal PersistentEdgeStore]))

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
         ^PersistentEdgeStore pes (p/edge-storage idx)]
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
  (let [^PersistentEdgeStore pes (p/edge-storage idx)
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
