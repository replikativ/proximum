(ns proximum.pgvector
  "Explicit translations between Proximum HNSW rank distances and pgvector.

   These helpers deliberately do not change Proximum's existing search result
   contract. HNSW continues to rank by its inexpensive monotonic metrics; an
   integration boundary can expose PostgreSQL-compatible operator values."
  (:require [proximum.protocols :as p]))

(defn internal-distance->pg-distance
  "Translate an HNSW rank distance to the value exposed by pgvector.

   :euclidean     squared L2 -> L2 (`<->`)
   :cosine        1 - cosine similarity, unchanged (`<=>`)
   :inner-product 1 - dot -> negative dot (`<#>`)

   Small negative Euclidean values caused by floating-point roundoff are
   clamped before taking the square root."
  [metric internal-distance]
  (case metric
    :euclidean (Math/sqrt (max 0.0 (double internal-distance)))
    :cosine (double internal-distance)
    :inner-product (- (double internal-distance) 1.0)
    (throw (ex-info "Unsupported distance metric"
                    {:reason :unsupported-distance-metric :metric metric}))))

(defn pg-distance->internal-distance
  "Inverse of `internal-distance->pg-distance`, useful for thresholds."
  [metric pg-distance]
  (case metric
    :euclidean (let [d (double pg-distance)] (* d d))
    :cosine (double pg-distance)
    :inner-product (+ (double pg-distance) 1.0)
    (throw (ex-info "Unsupported distance metric"
                    {:reason :unsupported-distance-metric :metric metric}))))

(defn visible-distance
  "Return pgvector-compatible distance for an index and internal rank value."
  [idx internal-distance]
  (internal-distance->pg-distance (:distance (p/index-config idx)) internal-distance))
