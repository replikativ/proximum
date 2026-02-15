(ns profile-insert
  "Profile insert performance to find bottlenecks."
  (:require [proximum.core :as core]
            [proximum.hnsw :as hnsw]
            [proximum.vectors :as vectors]))

(defn profile-insert [n-vectors dim]
  (println (format "\n=== Profiling %d inserts (dim=%d) ===" n-vectors dim))

  ;; Generate random vectors
  (println "Generating vectors...")
  (let [vecs (vec (repeatedly n-vectors #(float-array (repeatedly dim rand))))

        ;; Create index
        _ (println "Creating index...")
        idx (core/create-index dim
                               :M 16
                               :ef-construction 200
                               :vectors-capacity (+ n-vectors 100))

        ;; Time insert
        _ (println "Inserting vectors...")
        start (System/nanoTime)
        final-idx (reduce (fn [i v]
                            (core/insert i v))
                          idx
                          vecs)
        elapsed-ms (/ (- (System/nanoTime) start) 1e6)
        vec-per-sec (/ n-vectors (/ elapsed-ms 1000))]

    (println (format "\nResults:"))
    (println (format "  Total: %.2f ms" elapsed-ms))
    (println (format "  Per vector: %.2f ms" (/ elapsed-ms n-vectors)))
    (println (format "  Throughput: %.0f vec/sec" vec-per-sec))

    (core/close! final-idx)

    {:total-ms elapsed-ms
     :per-vec-ms (/ elapsed-ms n-vectors)
     :vec-per-sec vec-per-sec}))

(defn -main [& args]
  (let [n (if (first args) (Integer/parseInt (first args)) 1000)]
    (profile-insert n 128)))
