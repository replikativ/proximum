(ns profile-pes-insert
  "Profile PES bulk insert with async-profiler."
  (:require [proximum.core :as core]
            [proximum.hnsw :as hnsw]
            [proximum.vectors :as vectors]
            [clj-async-profiler.core :as prof])
  (:import [proximum.internal PersistentEdgeStore]))

(defn profile-pes-insert [n-vectors dim]
  (println (format "\n=== Profiling PES bulk-insert of %d vectors ===" n-vectors))

  ;; Generate random vectors
  (println "Generating vectors...")
  (let [rand-float (fn [] (- (rand 2.0) 1.0))
        vecs (vec (repeatedly n-vectors (fn [] (float-array (repeatedly dim rand-float)))))
        M 16
        M0 (* 2 M)
        ef-construction 200
        max-levels 16
        path (str "/tmp/profile-pes-" (System/currentTimeMillis) ".bin")

        ;; Create vector store with temp file
        _ (println "Creating vector store...")
        vec-path (str "/tmp/profile-vecs-" (System/currentTimeMillis))
        vs (vectors/create-store vec-path dim :capacity (+ n-vectors 100))

        ;; Create index map (what hnsw/bulk-insert! expects)
        index {:vectors vs
               :dim dim
               :M M
               :M0 M0
               :ef-construction ef-construction
               :max-levels max-levels}

        ;; Create PES
        _ (println "Creating PersistentEdgeStore...")
        pes (hnsw/create-edge-store (+ n-vectors 100) max-levels M M0)]

    ;; Profile insert
    (println "Profiling bulk-insert-pes! ...")
    (prof/profile
      {:event :cpu}
      (hnsw/bulk-insert! index pes vecs 8 :euclidean))

    (println "\nFlamegraph saved to /tmp/clj-async-profiler/results/")
    (println "Open the HTML file in a browser to view.")

    ;; Cleanup
    (vectors/close! vs)
    (.delete (java.io.File. path))
    ;; Clean up vector store directory
    (doseq [f (.listFiles (java.io.File. vec-path))]
      (.delete f))
    (.delete (java.io.File. vec-path))))

(defn -main [& args]
  (let [n (if (first args) (Integer/parseInt (first args)) 10000)]
    (profile-pes-insert n 128)))
