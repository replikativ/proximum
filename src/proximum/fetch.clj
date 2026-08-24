(ns proximum.fetch
  "Bounded parallel konserve fetch — the IO half of a restore, separated from
   the mutation half.

   Both of proximum's restore paths load their chunks EAGERLY (an HNSW search
   descends through arbitrary nodes, so unlike a B-tree there is no useful
   lazy subset), and both used to do it with one blocking `k/get` at a time:
   `chunks x RTT` with nothing overlapping, which against an object store is
   the entire cold-open cost. The addresses are all known up front — they come
   from the address map in the commit — so there is nothing to discover and no
   reason to wait: fetch them concurrently, bounded, and hand back the values.

   FETCH IN PARALLEL, APPLY SERIALLY. Only the konserve reads fan out; every
   caller applies the fetched values on its own single thread (`PES` chunk
   installation during transient init, mmap writes). Parallel IO against a
   konserve store is safe — independent keys, per-key locks — while parallel
   MUTATION of a half-built index is a bug nobody needs; this split keeps the
   speedup without buying the hazard.

   A per-call pool rather than a shared one: nothing to own, nothing to shut
   down on a failure path, and a restore is not hot enough for churn to
   matter. (The same reasoning, and the same shape, as
   persistent-sorted-set's warm fetch-wave.)"
  (:require [konserve.core :as k])
  (:import [java.util.concurrent Executors ExecutorService Callable Future TimeUnit]))

(def default-width
  "Concurrent in-flight fetches. Chosen to match persistent-sorted-set's warm
   default, which measured optimal against local MinIO at 64 (128 regressed).
   A starting point to measure against your store, not a constant to inherit."
  64)

(defn fetch-all!
  "Fetch `[tag konserve-key]` pairs from `store` with at most `width` reads in
   flight; returns `{tag value}` for the keys that exist (a missing key simply
   contributes no entry, matching the `when-let` the serial loops used).

   `width` <= 1 degrades to the serial behaviour this replaces — useful as a
   control, and as an escape hatch for a store that dislikes concurrency."
  ([store tagged-keys] (fetch-all! store tagged-keys default-width))
  ([store tagged-keys width]
   (let [tagged-keys (vec tagged-keys)]
     (cond
       (empty? tagged-keys) {}

       (<= (long (or width 1)) 1)
       (into {}
             (keep (fn [[tag kkey]]
                     (when-let [v (k/get store kkey nil {:sync? true})]
                       [tag v])))
             tagged-keys)

       :else
       (let [^ExecutorService pool (Executors/newFixedThreadPool
                                    (int (min (long width) (count tagged-keys))))]
         (try
           (let [futures (.invokeAll pool
                                     (mapv (fn [[tag kkey]]
                                             (reify Callable
                                               (call [_]
                                                 [tag (k/get store kkey nil {:sync? true})])))
                                           tagged-keys))]
             (into {}
                   (keep (fn [^Future f]
                           (let [[tag v] (.get f)]
                             (when (some? v) [tag v]))))
                   futures))
           (finally
             (.shutdown pool)
             (.awaitTermination pool 300 TimeUnit/SECONDS))))))))
