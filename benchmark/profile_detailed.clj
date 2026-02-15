(ns profile-detailed
  "Detailed profiling of insert with async-profiler."
  (:require [proximum.core :as core]
            [proximum.hnsw :as hnsw]
            [clj-async-profiler.core :as prof]))

(defn profile-with-flamegraph [n-vectors dim]
  (println (format "\n=== Profiling %d inserts with flamegraph ===" n-vectors))

  ;; Generate random vectors
  (println "Generating vectors...")
  (let [vecs (vec (repeatedly n-vectors #(float-array (repeatedly dim rand))))

        ;; Create index
        _ (println "Creating index...")
        idx (core/create-index dim
                               :M 16
                               :ef-construction 200
                               :vectors-capacity (+ n-vectors 100))]

    ;; Profile insert
    (println "Profiling inserts...")
    (prof/profile
      {:event :cpu}
      (reduce core/insert idx vecs))

    (println "\nFlamegraph saved to /tmp/clj-async-profiler/results/")
    (println "Open the HTML file in a browser to view.")))

(defn -main [& args]
  (let [n (if (first args) (Integer/parseInt (first args)) 500)]
    (profile-with-flamegraph n 128)))
