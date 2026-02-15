(ns profile-search
  "Profile HNSW search to identify bottlenecks."
  (:require [proximum.core :as core]
            [proximum.hnsw :as hnsw]
            [proximum.vectors :as vectors]
            [clj-async-profiler.core :as prof])
  (:import [proximum.internal PersistentEdgeStore HnswSearchPES]))

(defn profile-search [n-vectors n-queries]
  (println (format "\n=== Profiling HNSW Search ==="))
  (println (format "Index: %d vectors, Queries: %d\n" n-vectors n-queries))

  (let [dim 128
        M 16
        ef-construction 200
        ef-search 100
        k 10
        path (str "/tmp/profile-search-" (System/currentTimeMillis))

        ;; Generate random vectors
        _ (println "Generating vectors...")
        rand-vec (fn [] (float-array (repeatedly dim #(- (rand 2.0) 1.0))))
        base-vecs (vec (repeatedly n-vectors rand-vec))
        query-vecs (vec (repeatedly n-queries rand-vec))

        ;; Create index
        _ (println "Creating index...")
        vs (vectors/create-store path dim :capacity (+ n-vectors 100))
        pes (hnsw/create-edge-store (+ n-vectors 100) 16 M (* 2 M))
        index {:vectors vs :dim dim :M M :ef-construction ef-construction}

        ;; Insert vectors
        _ (println "Inserting vectors...")
        _ (time (hnsw/bulk-insert! index pes base-vecs 8 :euclidean))

        ;; Get memory segment for search
        seg (vectors/get-segment vs)

        ;; Warmup
        _ (println "Warming up search...")
        _ (dotimes [_ 100]
            (doseq [q query-vecs]
              (hnsw/hnsw-search index pes q k ef-search :euclidean)))]

    ;; Profile search
    (println "\nProfiling search (CPU)...")
    (prof/profile
      {:event :cpu}
      (dotimes [_ 10]  ;; 10 iterations of all queries
        (doseq [q query-vecs]
          (hnsw/hnsw-search index pes q k ef-search :euclidean))))

    (println "\nFlamegraph saved to /tmp/clj-async-profiler/results/")

    ;; Also time it
    (println "\nTiming 1000 searches...")
    (let [start (System/nanoTime)]
      (dotimes [_ 10]
        (doseq [q query-vecs]
          (hnsw/hnsw-search index pes q k ef-search :euclidean)))
      (let [elapsed (/ (- (System/nanoTime) start) 1e6)
            total-searches (* 10 n-queries)]
        (println (format "  Total: %.2f ms for %d searches" elapsed total-searches))
        (println (format "  Per search: %.2f µs" (/ (* elapsed 1000) total-searches)))
        (println (format "  QPS: %.0f" (/ (* total-searches 1000) elapsed)))))

    ;; Cleanup
    (vectors/close! vs)
    (doseq [f (.listFiles (java.io.File. path))]
      (.delete f))
    (.delete (java.io.File. path))))

(defn -main [& args]
  (profile-search 10000 100))
